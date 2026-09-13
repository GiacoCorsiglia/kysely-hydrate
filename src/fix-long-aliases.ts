import * as k from "kysely";

import { AliasHashCollisionError } from "./helpers/errors.ts";
import { byteLength } from "./helpers/utils.ts";

/** PostgreSQL truncates identifiers longer than this (NAMEDATALEN - 1). */
export const MAX_IDENTIFIER_BYTES = 63;

export interface FixLongAliasesOptions {
	/** Identifiers longer than this many bytes are shortened. Defaults to 63. */
	maxBytes?: number;
}

// A shortened identifier is the start of the original followed by "~" and a
// 14-letter hash. 26^14 > 2^64, so a 64-bit hash always fits. Lowercase
// letters only, so a second CamelCasePlugin pass (Kysely re-transforms
// embedded subqueries) leaves it alone.
const MARKER = "~";
const HASH_LENGTH = 14;

// Module level so that any plugin instance restores what any other shortened.
const originalByShort = new Map<string, string>();
const restoredByKey = new Map<string, string>();

// Queries whose rows may contain shortened names. Kysely passes the same
// QueryId object to transformQuery and transformResult.
const queriesToRestore = new WeakSet<k.QueryId>();

function hash(name: string): string {
	let h = 0xcbf29ce484222325n; // FNV-1a
	for (const byte of new TextEncoder().encode(name)) {
		h = ((h ^ BigInt(byte)) * 0x100000001b3n) & 0xffffffffffffffffn;
	}
	let out = "";
	for (let i = 0; i < HASH_LENGTH; i++) {
		out += String.fromCharCode(97 + Number(h % 26n));
		h /= 26n;
	}
	return out;
}

function shorten(name: string, maxBytes: number): string {
	const tail = MARKER + hash(name);
	let head = "";
	for (const char of name) {
		if (byteLength(head + char + tail) > maxBytes) {
			break;
		}
		head += char;
	}
	const short = head + tail;
	const other = originalByShort.get(short);
	if (other !== undefined && other !== name) {
		throw new AliasHashCollisionError(name, other);
	}
	originalByShort.set(short, name);
	return short;
}

// Repeats until nothing changes because an original can itself contain a
// shortened name: a subquery's alias, hoisted and prefixed by an outer query.
function restore(key: string): string {
	let restored = restoredByKey.get(key);
	if (restored === undefined) {
		restored = key;
		for (let previous = ""; previous !== restored; ) {
			previous = restored;
			for (const [short, original] of originalByShort) {
				restored = restored.split(short).join(original);
			}
		}
		restoredByKey.set(key, restored);
	}
	return restored;
}

function restoreRow(row: k.UnknownRow): k.UnknownRow {
	const restored: k.UnknownRow = {};
	for (const key in row) {
		restored[restore(key)] = row[key];
	}
	return restored;
}

class ShortenIdentifiers extends k.OperationNodeTransformer {
	readonly #maxBytes: number;
	readonly #shortByName = new Map<string, string>();

	constructor(maxBytes: number) {
		super();
		this.#maxBytes = maxBytes;
	}

	// A UTF-16 code unit is 1 to 3 bytes, so most names need no counting.
	#fits(name: string): boolean {
		return (
			name.length * 3 <= this.#maxBytes ||
			(name.length <= this.#maxBytes && byteLength(name) <= this.#maxBytes)
		);
	}

	/**
	 * Whether `node` has an identifier that needs shortening ("long") or that
	 * may already be shortened ("marked"). Kysely's transformer clones every
	 * node, so this read-only pass lets the common case skip it entirely.
	 */
	scan(node: unknown, found = { long: false, marked: false }): typeof found {
		if (Array.isArray(node)) {
			for (const item of node) {
				this.scan(item, found);
			}
		} else if (typeof node === "object" && node !== null && "kind" in node) {
			if (k.IdentifierNode.is(node as k.OperationNode)) {
				const { name } = node as k.IdentifierNode;
				found.long ||= !this.#fits(name);
				found.marked ||= name.includes(MARKER);
			} else if (!k.ValueNode.is(node as k.OperationNode)) {
				for (const key in node) {
					this.scan((node as Record<string, unknown>)[key], found);
				}
			}
		}
		return found;
	}

	protected override transformIdentifier(
		node: k.IdentifierNode,
		queryId?: k.QueryId,
	): k.IdentifierNode {
		node = super.transformIdentifier(node, queryId);
		if (this.#fits(node.name)) {
			return node;
		}
		let short = this.#shortByName.get(node.name);
		if (short === undefined) {
			this.#shortByName.set(node.name, (short = shorten(node.name, this.#maxBytes)));
		}
		return { ...node, name: short };
	}
}

/**
 * A Kysely plugin that shortens identifiers over PostgreSQL's 63-byte limit
 * (which PostgreSQL would otherwise silently truncate) and restores the
 * original names in result rows. Table and column names are rewritten too,
 * so a schema that relies on PostgreSQL's own truncation will not work.
 *
 * Install it last. If you use `CamelCasePlugin`, pass it in so that lengths
 * are measured on the snake_cased names the database sees:
 *
 * ```ts
 * plugins: [fixLongAliases(new CamelCasePlugin())]
 * ```
 */
export function fixLongAliases(
	inner?: k.KyselyPlugin,
	{ maxBytes = MAX_IDENTIFIER_BYTES }: FixLongAliasesOptions = {},
): k.KyselyPlugin {
	const transformer = new ShortenIdentifiers(maxBytes);
	return {
		transformQuery(args) {
			let node = inner ? inner.transformQuery(args) : args.node;
			const { long, marked } = transformer.scan(node);
			if (long) {
				node = transformer.transformNode(node, args.queryId);
			}
			if (long || marked) {
				queriesToRestore.add(args.queryId);
			}
			return node;
		},
		async transformResult(args) {
			let { result } = args;
			if (queriesToRestore.has(args.queryId)) {
				result = { ...result, rows: result.rows.map(restoreRow) };
			}
			return inner ? inner.transformResult({ ...args, result }) : result;
		},
	};
}
