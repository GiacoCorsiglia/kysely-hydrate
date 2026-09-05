import * as k from "kysely";

import { AliasHashCollisionError } from "./helpers/errors.ts";
import { truncateToBytes, utf8ByteLength, utf8Encode } from "./helpers/utils.ts";

/**
 * PostgreSQL's identifier limit: NAMEDATALEN - 1.
 */
export const POSTGRES_MAX_IDENTIFIER_BYTES = 63;

export interface FixLongAliasesOptions {
	/**
	 * Aliases longer than this many bytes are shortened. Defaults to 63, the
	 * PostgreSQL limit (`NAMEDATALEN - 1`). MySQL allows 64; SQLite has no limit.
	 */
	maxBytes?: number;
}

/**
 * The shortened alias is `<head><MARKER><hash>`: the start of the original
 * name for readability, followed by a token from which the original can be
 * looked up. The token alphabet is lowercase letters (plus the marker) so it
 * is a fixed point of every `CamelCasePlugin` transformation in both
 * directions, which matters because nested subqueries pass through
 * `transformQuery` more than once.
 */
const MARKER = "~";
const HASH_LENGTH = 14; // 26^14 > 2^64, so a 64-bit hash always fits.
const TOKEN_LENGTH = 1 + HASH_LENGTH;
const TOKEN_PATTERN = new RegExp(`${MARKER}[a-z]{${HASH_LENGTH}}`, "g");

interface Shortened {
	readonly original: string;
	/**
	 * Every shortened form of `original`: one per `maxBytes` it has been
	 * shortened under (normally just one), longest first so that restoring
	 * never matches a shorter form that is a suffix of a longer one.
	 */
	readonly shorts: string[];
}

/**
 * Module-level so that any plugin instance can restore an alias any other
 * instance shortened (the hash is deterministic, so the token is the same
 * everywhere). Bounded by the number of distinct over-long aliases the program
 * ever generates.
 */
const shortenedByToken = new Map<string, Shortened>();

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const U64 = 0xffffffffffffffffn;

function fnv1a64(input: string): bigint {
	let hash = FNV_OFFSET_BASIS;
	for (const byte of utf8Encode(input)) {
		hash ^= BigInt(byte);
		hash = (hash * FNV_PRIME) & U64;
	}
	return hash;
}

function toBase26(value: bigint): string {
	let out = "";
	for (let i = 0; i < HASH_LENGTH; i++) {
		out = String.fromCharCode(97 + Number(value % 26n)) + out;
		value /= 26n;
	}
	return out;
}

function shorten(name: string, maxBytes: number): string {
	if (utf8ByteLength(name) <= maxBytes) {
		return name;
	}

	const token = MARKER + toBase26(fnv1a64(name));
	const short = truncateToBytes(name, maxBytes - TOKEN_LENGTH) + token;

	let shortened = shortenedByToken.get(token);
	if (!shortened) {
		shortened = { original: name, shorts: [] };
		shortenedByToken.set(token, shortened);
	} else if (shortened.original !== name) {
		throw new AliasHashCollisionError(name, shortened.original);
	}
	if (!shortened.shorts.includes(short)) {
		shortened.shorts.push(short);
		shortened.shorts.sort((a, b) => b.length - a.length);
	}
	return short;
}

/**
 * Expands every shortened alias embedded in a result key back to its original
 * name. Loops because an original may itself embed a token (an alias that was
 * hoisted, prefixed, and shortened again at an outer level).
 */
function restoreKey(key: string): string {
	let restored = key;
	while (restored.includes(MARKER)) {
		const before = restored;
		for (const [token] of before.matchAll(TOKEN_PATTERN)) {
			const shortened = shortenedByToken.get(token);
			if (!shortened) {
				continue;
			}
			for (const short of shortened.shorts) {
				// A replacer function, because "$$" in a replacement string is an
				// escape sequence for String.prototype.replace.
				restored = restored.replace(short, () => shortened.original);
			}
		}
		if (restored === before) {
			break;
		}
	}
	return restored;
}

function restoreRow(row: k.UnknownRow): k.UnknownRow {
	const keys = Object.keys(row);
	if (!keys.some((key) => key.includes(MARKER))) {
		return row;
	}
	const restored: Record<string, unknown> = {};
	for (const key of keys) {
		restored[key.includes(MARKER) ? restoreKey(key) : key] = row[key];
	}
	return restored;
}

class ShortenAliasesTransformer extends k.OperationNodeTransformer {
	readonly #maxBytes: number;

	constructor(maxBytes: number) {
		super();
		this.#maxBytes = maxBytes;
	}

	protected override transformSelection(
		node: k.SelectionNode,
		queryId?: k.QueryId,
	): k.SelectionNode {
		node = super.transformSelection(node, queryId);
		const { selection } = node;
		if (k.AliasNode.is(selection) && k.IdentifierNode.is(selection.alias)) {
			const short = shorten(selection.alias.name, this.#maxBytes);
			if (short !== selection.alias.name) {
				return {
					...node,
					selection: k.AliasNode.create(selection.node, k.IdentifierNode.create(short)),
				};
			}
		}
		return node;
	}

	protected override transformColumn(node: k.ColumnNode, queryId?: k.QueryId): k.ColumnNode {
		node = super.transformColumn(node, queryId);
		const short = shorten(node.column.name, this.#maxBytes);
		return short === node.column.name ? node : k.ColumnNode.create(short);
	}
}

class FixLongAliasesPlugin implements k.KyselyPlugin {
	readonly #inner: k.KyselyPlugin | undefined;
	readonly #transformer: ShortenAliasesTransformer;

	constructor(inner: k.KyselyPlugin | undefined, options: FixLongAliasesOptions) {
		this.#inner = inner;
		this.#transformer = new ShortenAliasesTransformer(
			options.maxBytes ?? POSTGRES_MAX_IDENTIFIER_BYTES,
		);
	}

	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		// Shorten AFTER the inner plugin so we measure the identifiers the
		// database will actually see.
		const node = this.#inner ? this.#inner.transformQuery(args) : args.node;
		return this.#transformer.transformNode(node, args.queryId);
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		// Restore BEFORE the inner plugin so it sees the names it produced.
		let { result } = args;
		if (shortenedByToken.size > 0) {
			result = { ...result, rows: result.rows.map(restoreRow) };
		}
		return this.#inner ? this.#inner.transformResult({ ...args, result }) : result;
	}
}

function isPlugin(value: unknown): value is k.KyselyPlugin {
	return (
		typeof value === "object" &&
		value !== null &&
		typeof (value as k.KyselyPlugin).transformQuery === "function" &&
		typeof (value as k.KyselyPlugin).transformResult === "function"
	);
}

/**
 * Creates a Kysely plugin that keeps column aliases within the database's
 * identifier length limit.
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes. kysely-hydrate
 * builds prefixed aliases for nested relations (`parent$$child$$column`), so
 * deep nesting or long names can exceed the limit, and truncated aliases
 * corrupt hydration. This plugin shortens any over-long column alias (and any
 * reference to it) to a fixed-length form before the query is compiled, and
 * restores the original names in the result rows.
 *
 * Install it on the Kysely instance. If you use `CamelCasePlugin` (or any other
 * plugin that renames identifiers), pass it as the argument so the shortening
 * measures the identifiers the database will actually see:
 *
 * ```ts
 * const db = new Kysely<DB>({
 *   dialect,
 *   plugins: [fixLongAliases(new CamelCasePlugin())],
 * });
 * ```
 *
 * Put it last in the plugin list. Other plugins that run after it would see the
 * shortened names in queries but the restored names in results.
 *
 * The plugin is not specific to kysely-hydrate: it fixes any over-long alias in
 * any Kysely query. It rewrites column identifiers only (aliases, references,
 * column lists), never table names, table aliases, or schemas. A real column
 * whose name is over the limit (which PostgreSQL would have truncated in
 * `CREATE TABLE`) is rewritten too and therefore fails loudly instead of
 * matching by accident.
 *
 * `CamelCasePlugin({ upperCase: true })` is not supported: Kysely applies its
 * transformation twice to subqueries, which already breaks nested queries on
 * its own.
 */
export function fixLongAliases(options?: FixLongAliasesOptions): k.KyselyPlugin;
export function fixLongAliases(
	inner: k.KyselyPlugin,
	options?: FixLongAliasesOptions,
): k.KyselyPlugin;
export function fixLongAliases(
	innerOrOptions?: k.KyselyPlugin | FixLongAliasesOptions,
	options?: FixLongAliasesOptions,
): k.KyselyPlugin {
	if (isPlugin(innerOrOptions)) {
		return new FixLongAliasesPlugin(innerOrOptions, options ?? {});
	}
	return new FixLongAliasesPlugin(undefined, innerOrOptions ?? {});
}
