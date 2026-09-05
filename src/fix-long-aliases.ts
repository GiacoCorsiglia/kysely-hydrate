import * as k from "kysely";

import { AliasHashCollisionError } from "./helpers/errors.ts";
import { truncateToBytes, utf8ByteLength } from "./helpers/utils.ts";

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

// A shortened alias is `<head>~<hash>`: the start of the original name for
// readability, then a token the original can be looked up by. The token is
// lowercase letters only so that CamelCasePlugin, which Kysely runs more than
// once over nested subqueries, leaves it unchanged in both directions.
const HASH_LENGTH = 14; // 26^14 > 2^64, so a 64-bit hash always fits.
const TOKEN_LENGTH = 1 + HASH_LENGTH;
const TOKEN_PATTERN = /~[a-z]{14}/g;

// Module-level so any plugin instance can restore an alias any other instance
// shortened. `shorts` holds one shortened form per `maxBytes` the name has
// been shortened under, longest first so restoring never matches a shorter
// form that is a suffix of a longer one.
const shortenedByToken = new Map<string, { original: string; shorts: string[] }>();

function hashToken(name: string): string {
	let hash = 0xcbf29ce484222325n; // FNV-1a 64
	for (const byte of new TextEncoder().encode(name)) {
		hash = ((hash ^ BigInt(byte)) * 0x100000001b3n) & 0xffffffffffffffffn;
	}
	let token = "";
	for (let i = 0; i < HASH_LENGTH; i++) {
		token = String.fromCharCode(97 + Number(hash % 26n)) + token;
		hash /= 26n;
	}
	return "~" + token;
}

function shorten(name: string, maxBytes: number): string {
	if (utf8ByteLength(name) <= maxBytes) {
		return name;
	}
	const token = hashToken(name);
	const short = truncateToBytes(name, maxBytes - TOKEN_LENGTH) + token;

	let entry = shortenedByToken.get(token);
	if (!entry) {
		shortenedByToken.set(token, (entry = { original: name, shorts: [] }));
	} else if (entry.original !== name) {
		throw new AliasHashCollisionError(name, entry.original);
	}
	if (!entry.shorts.includes(short)) {
		entry.shorts.push(short);
		entry.shorts.sort((a, b) => b.length - a.length);
	}
	return short;
}

// Loops because an original may itself embed a token: an alias that was
// hoisted, prefixed, and shortened again at an outer level.
function restoreKey(key: string): string {
	for (let before = ""; before !== key; ) {
		before = key;
		for (const [token] of before.matchAll(TOKEN_PATTERN)) {
			const entry = shortenedByToken.get(token);
			if (!entry) {
				continue;
			}
			for (const short of entry.shorts) {
				// A replacer function: "$$" in a replacement string is an escape.
				key = key.replace(short, () => entry.original);
			}
		}
	}
	return key;
}

function restoreRow(row: k.UnknownRow): k.UnknownRow {
	if (!Object.keys(row).some((key) => key.includes("~"))) {
		return row;
	}
	return Object.fromEntries(Object.entries(row).map(([key, value]) => [restoreKey(key), value]));
}

class ShortenAliasesTransformer extends k.OperationNodeTransformer {
	readonly #maxBytes: number;
	// Memoized per identifier, like CamelCasePlugin: this runs on every
	// compilation, and a program's vocabulary of identifiers is small.
	readonly #shortened = new Map<string, string>();

	constructor(maxBytes: number) {
		super();
		this.#maxBytes = maxBytes;
	}

	#shorten(name: string): string {
		let short = this.#shortened.get(name);
		if (short === undefined) {
			this.#shortened.set(name, (short = shorten(name, this.#maxBytes)));
		}
		return short;
	}

	protected override transformSelection(
		node: k.SelectionNode,
		queryId?: k.QueryId,
	): k.SelectionNode {
		node = super.transformSelection(node, queryId);
		const { selection } = node;
		if (!k.AliasNode.is(selection) || !k.IdentifierNode.is(selection.alias)) {
			return node;
		}
		const short = this.#shorten(selection.alias.name);
		return short === selection.alias.name
			? node
			: { ...node, selection: k.AliasNode.create(selection.node, k.IdentifierNode.create(short)) };
	}

	protected override transformColumn(node: k.ColumnNode, queryId?: k.QueryId): k.ColumnNode {
		node = super.transformColumn(node, queryId);
		const short = this.#shorten(node.column.name);
		return short === node.column.name ? node : k.ColumnNode.create(short);
	}
}

function isPlugin(value: unknown): value is k.KyselyPlugin {
	const plugin = value as Partial<k.KyselyPlugin> | undefined;
	return (
		typeof plugin?.transformQuery === "function" && typeof plugin.transformResult === "function"
	);
}

function createPlugin(
	inner: k.KyselyPlugin | undefined,
	{ maxBytes = POSTGRES_MAX_IDENTIFIER_BYTES }: FixLongAliasesOptions = {},
): k.KyselyPlugin {
	const transformer = new ShortenAliasesTransformer(maxBytes);
	return {
		// After the inner plugin, so we measure the identifiers the database will see.
		transformQuery(args) {
			const node = inner ? inner.transformQuery(args) : args.node;
			return transformer.transformNode(node, args.queryId);
		},
		// Before the inner plugin, so it sees the names it produced.
		async transformResult(args) {
			let { result } = args;
			if (shortenedByToken.size > 0) {
				result = { ...result, rows: result.rows.map(restoreRow) };
			}
			return inner ? inner.transformResult({ ...args, result }) : result;
		},
	};
}

/**
 * Creates a Kysely plugin that keeps column aliases within the database's
 * identifier length limit.
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes. kysely-hydrate
 * builds prefixed aliases for nested relations (`parent$$child$$column`), so
 * deep nesting or long names can exceed the limit, and truncated aliases
 * corrupt hydration. This plugin shortens any over-long column alias (and any
 * reference to it) before the query is compiled, and restores the original
 * names in the result rows.
 *
 * Install it on the Kysely instance, last in the plugin list. If you use
 * `CamelCasePlugin` (or any other plugin that renames identifiers), pass it as
 * the argument so the shortening measures the identifiers the database will
 * actually see:
 *
 * ```ts
 * const db = new Kysely<DB>({
 *   dialect,
 *   plugins: [fixLongAliases(new CamelCasePlugin())],
 * });
 * ```
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
	return isPlugin(innerOrOptions)
		? createPlugin(innerOrOptions, options)
		: createPlugin(undefined, innerOrOptions);
}
