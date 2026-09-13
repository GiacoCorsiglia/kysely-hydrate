import * as k from "kysely";

import { AliasHashCollisionError } from "./helpers/errors.ts";
import { byteLength, utf8 } from "./helpers/utils.ts";

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
const HASH_LENGTH = 14;

// Module level so that any plugin instance restores what any other shortened.
const originalByShort = new Map<string, string>();
const restoredByKey = new Map<string, string>();

function hash(name: string): string {
	let h = 0xcbf29ce484222325n; // FNV-1a
	for (const byte of utf8.encode(name)) {
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
	if (byteLength(name) <= maxBytes) {
		return name;
	}
	const tail = "~" + hash(name);
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
	return Object.fromEntries(Object.entries(row).map(([key, value]) => [restore(key), value]));
}

class ShortenIdentifiers extends k.OperationNodeTransformer {
	readonly #maxBytes: number;
	readonly #shortByName = new Map<string, string>();

	constructor(maxBytes: number) {
		super();
		this.#maxBytes = maxBytes;
	}

	protected override transformIdentifier(
		node: k.IdentifierNode,
		queryId?: k.QueryId,
	): k.IdentifierNode {
		node = super.transformIdentifier(node, queryId);
		let short = this.#shortByName.get(node.name);
		if (short === undefined) {
			this.#shortByName.set(node.name, (short = shorten(node.name, this.#maxBytes)));
		}
		return short === node.name ? node : { ...node, name: short };
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
			const node = inner ? inner.transformQuery(args) : args.node;
			return transformer.transformNode(node, args.queryId);
		},
		async transformResult(args) {
			let { result } = args;
			if (originalByShort.size > 0) {
				result = { ...result, rows: result.rows.map(restoreRow) };
			}
			return inner ? inner.transformResult({ ...args, result }) : result;
		},
	};
}
