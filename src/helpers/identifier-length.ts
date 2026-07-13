/**
 * @module helpers/identifier-length
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1), emitting only a NOTICE. QuerySet builds prefixed column
 * aliases when nesting relations (`parent$$child$$column`), so with deep
 * nesting and/or long names the generated alias can exceed 63 bytes. Postgres
 * then truncates the alias in the result set and hydration silently produces
 * wrong output (mangled/missing fields, or collisions between two distinct
 * aliases that share the same first 63 bytes).
 *
 * This module works around the limit with a PAIR of Kysely plugins:
 *
 * - {@link ShortenLongIdentifiersPlugin} rewrites, on the way INTO the
 *   database, every generated identifier (identified by containing the
 *   {@link SEP} separator) whose UTF-8 encoding exceeds 63 bytes to a
 *   deterministic short form: a readable prefix of the original name plus a
 *   64-bit hash of the full name, always within the limit. It records the
 *   renames in per-query state keyed by Kysely's `queryId`.
 * - {@link RestoreLongIdentifiersPlugin} renames the shortened columns in
 *   result rows back to their original names on the way OUT.
 *
 * Why a pair? Kysely runs plugins top-to-bottom for BOTH `transformQuery` and
 * `transformResult` (see `QueryExecutorBase` in Kysely). With
 * `CamelCasePlugin` installed, identifiers are snake_cased by its
 * `transformQuery` (which makes them LONGER) and result keys are camelized by
 * its `transformResult`. Shortening must therefore run AFTER the camel
 * plugin's `transformQuery` (to see the final snake_cased identifiers), and
 * restoring must run BEFORE the camel plugin's `transformResult` (so the
 * restored full-length snake_case keys get camelized back to the aliases the
 * hydrator expects). Since both hooks run top-to-bottom, that means the
 * restore plugin must sit at the FRONT of the plugin list and the shorten
 * plugin at the END:
 *
 * ```
 * [RestoreLongIdentifiersPlugin, ...user plugins..., ShortenLongIdentifiersPlugin]
 * ```
 *
 * {@link withIdentifierLengthGuard} builds a `Kysely` instance with exactly
 * that arrangement. QuerySet applies it automatically to every query it
 * executes, so users need no setup.
 *
 * The two halves share state via a module-level `WeakMap` keyed on the
 * `queryId` object (the pattern recommended by Kysely's plugin docs): a
 * query's entry is garbage-collected with the query itself, so state cannot
 * leak even when `transformQuery` is never matched by a `transformResult`
 * (e.g. for queries that are compiled but never executed).
 */

import * as k from "kysely";

import { KyselyHydrateError } from "./errors.ts";
import { SEP } from "./prefixes.ts";

/**
 * PostgreSQL's identifier length limit: NAMEDATALEN - 1 bytes.
 */
export const MAX_IDENTIFIER_BYTES = 63;

/**
 * Number of hex characters of the hash included in a shortened identifier.
 * 16 hex chars = 64 bits.
 */
const HASH_CHARS = 16;

/**
 * Error thrown when two distinct over-long identifiers in the same query
 * shorten to the same name (a 64-bit hash collision). Practically
 * unreachable; failing loudly is preferable to silently merging columns.
 */
export class IdentifierShorteningCollisionError extends KyselyHydrateError {
	constructor(shortened: string, a: string, b: string) {
		super(
			`Shortened identifier collision: both "${a}" and "${b}" shorten to "${shortened}". ` +
				"Rename one of the involved collections or columns.",
		);
	}
}

const encoder = new TextEncoder();

function byteLength(str: string): number {
	return encoder.encode(str).length;
}

/**
 * Truncates a string so its UTF-8 encoding is at most `maxBytes` bytes,
 * without splitting a code point.
 */
function truncateToByteLength(str: string, maxBytes: number): string {
	if (byteLength(str) <= maxBytes) {
		return str;
	}

	let bytes = 0;
	let result = "";
	for (const char of str) {
		bytes += byteLength(char);
		if (bytes > maxBytes) {
			break;
		}
		result += char;
	}
	return result;
}

/**
 * FNV-1a 64-bit hash over the UTF-8 bytes of the input, as 16 hex characters.
 *
 * Dependency-free and deterministic across processes (unlike, say, keying a
 * counter), so the same query always compiles to the same SQL. FNV-1a is not
 * cryptographic, but these names are program-generated (not adversarial) and
 * 64 bits makes an accidental collision need ~2^32 distinct over-long
 * aliases before a birthday collision becomes likely. Collisions within a
 * single query are detected and throw {@link IdentifierShorteningCollisionError}.
 */
function hashIdentifier(name: string): string {
	const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
	const FNV_PRIME = 0x100000001b3n;
	const MASK_64 = 0xffffffffffffffffn;

	let hash = FNV_OFFSET_BASIS;
	for (const byte of encoder.encode(name)) {
		hash ^= BigInt(byte);
		hash = (hash * FNV_PRIME) & MASK_64;
	}
	return hash.toString(16).padStart(HASH_CHARS, "0");
}

/**
 * Shortens an identifier to at most `maxBytes` bytes: a readable prefix of
 * the original followed by `$` and a 64-bit hash of the FULL original name.
 * The `$` separator (legal in Postgres identifiers) cannot appear in the
 * hash, and the hash contains no underscores or uppercase characters, so the
 * shortened name is unaffected by `CamelCasePlugin`'s key mapping should it
 * ever see one.
 */
function shortenIdentifier(name: string, maxBytes: number): string {
	const prefix = truncateToByteLength(name, maxBytes - HASH_CHARS - 1);
	return `${prefix}$${hashIdentifier(name)}`;
}

/**
 * Per-query record of renames applied by the shorten plugin, consumed by the
 * restore plugin: shortened name -> original name.
 *
 * Keyed by the `queryId` object so entries are garbage-collected with the
 * query (never explicitly deleted: `transformResult` may run once per chunk
 * when streaming, and `transformQuery` is not always matched by a
 * `transformResult` at all).
 *
 * Module-level (rather than per plugin instance) so that multiple installed
 * instances of the pair cooperate instead of conflicting; shortening is
 * deterministic, so concurrent or repeated recordings agree.
 */
const shortenedNamesByQuery = new WeakMap<k.QueryId, Map<string, string>>();

function recordShortenedName(queryId: k.QueryId, shortened: string, original: string): void {
	let renames = shortenedNamesByQuery.get(queryId);
	if (!renames) {
		renames = new Map();
		shortenedNamesByQuery.set(queryId, renames);
	}

	const existing = renames.get(shortened);
	if (existing !== undefined && existing !== original) {
		throw new IdentifierShorteningCollisionError(shortened, existing, original);
	}
	renames.set(shortened, original);
}

/**
 * Rewrites every identifier that contains the {@link SEP} separator (i.e.
 * was generated by QuerySet's selection prefixing) and exceeds the byte
 * limit. Because ALL occurrences of the identifier — the aliased selection
 * in a subquery, references to it in outer queries, ORDER BY, GROUP BY, etc.
 * — rename to the same deterministic short form, the query stays internally
 * consistent.
 *
 * Identifiers without the separator are left alone: they are user-authored
 * (table names, column names, custom aliases) and renaming them is not this
 * plugin's business — and on dialects without a length limit (SQLite) a
 * genuinely long user identifier must pass through untouched.
 */
class ShortenIdentifiersTransformer extends k.OperationNodeTransformer {
	readonly #maxBytes: number;

	constructor(maxBytes: number) {
		super();
		this.#maxBytes = maxBytes;
	}

	protected override transformIdentifier(
		node: k.IdentifierNode,
		queryId?: k.QueryId,
	): k.IdentifierNode {
		node = super.transformIdentifier(node, queryId);

		const { name } = node;
		if (!name.includes(SEP) || byteLength(name) <= this.#maxBytes) {
			return node;
		}

		const shortened = shortenIdentifier(name, this.#maxBytes);
		if (queryId) {
			recordShortenedName(queryId, shortened, name);
		}

		return { ...node, name: shortened };
	}
}

/**
 * The "into the database" half of the identifier-length plugin pair. Must be
 * installed LAST (in particular, after `CamelCasePlugin`). See the module
 * docs; prefer {@link withIdentifierLengthGuard} over manual installation.
 */
export class ShortenLongIdentifiersPlugin implements k.KyselyPlugin {
	readonly #transformer: ShortenIdentifiersTransformer;

	constructor(maxBytes: number = MAX_IDENTIFIER_BYTES) {
		this.#transformer = new ShortenIdentifiersTransformer(maxBytes);
	}

	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		return this.#transformer.transformNode(args.node, args.queryId);
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		return args.result;
	}
}

/**
 * The "out of the database" half of the identifier-length plugin pair. Must
 * be installed FIRST (in particular, before `CamelCasePlugin`). See the
 * module docs; prefer {@link withIdentifierLengthGuard} over manual
 * installation.
 */
export class RestoreLongIdentifiersPlugin implements k.KyselyPlugin {
	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		return args.node;
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		const renames = shortenedNamesByQuery.get(args.queryId);
		if (!renames?.size) {
			return args.result;
		}

		return {
			...args.result,
			rows: args.result.rows.map((row) => restoreRow(row, renames)),
		};
	}
}

function restoreRow(row: k.UnknownRow, renames: Map<string, string>): k.UnknownRow {
	const keys = Object.keys(row);
	if (!keys.some((key) => renames.has(key))) {
		return row;
	}

	const restored: Record<string, unknown> = {};
	for (const key of keys) {
		restored[renames.get(key) ?? key] = row[key];
	}
	return restored;
}

const restorePlugin = new RestoreLongIdentifiersPlugin();
const shortenPlugin = new ShortenLongIdentifiersPlugin();

const guardedCache = new WeakMap<k.Kysely<any>, k.Kysely<any>>();

function isPairPlugin(plugin: k.KyselyPlugin): boolean {
	return (
		plugin instanceof RestoreLongIdentifiersPlugin || plugin instanceof ShortenLongIdentifiersPlugin
	);
}

/**
 * Returns a `Kysely` instance whose plugin list is
 * `[RestoreLongIdentifiersPlugin, ...existing plugins..., ShortenLongIdentifiersPlugin]`,
 * sharing the connection (and transaction, when given one) of the input.
 *
 * Idempotent: a db already in canonical form is returned as-is, and any
 * user-installed instances of the pair are removed before the canonical pair
 * is (re)applied, so misordered manual installations are corrected rather
 * than compounded. Results are cached per input instance.
 */
export function withIdentifierLengthGuard<DB>(db: k.Kysely<DB>): k.Kysely<DB> {
	const cached = guardedCache.get(db);
	if (cached) {
		return cached as k.Kysely<DB>;
	}

	const plugins = db.getExecutor().plugins;

	let guarded: k.Kysely<DB>;
	if (plugins[0] === restorePlugin && plugins[plugins.length - 1] === shortenPlugin) {
		guarded = db;
	} else {
		let rebuilt = db.withoutPlugins().withPlugin(restorePlugin);
		for (const plugin of plugins) {
			if (!isPairPlugin(plugin)) {
				rebuilt = rebuilt.withPlugin(plugin);
			}
		}
		guarded = rebuilt.withPlugin(shortenPlugin);
	}

	guardedCache.set(db, guarded);
	guardedCache.set(guarded, guarded);
	return guarded;
}
