import * as k from "kysely";

import { AliasHashCollisionError } from "./helpers/errors.ts";
import { byteLength } from "./helpers/utils.ts";

/** PostgreSQL truncates identifiers longer than this (NAMEDATALEN - 1). */
export const MAX_IDENTIFIER_BYTES = 63;

// A shortened identifier is the start of the original followed by "~" and a
// 14-letter hash. 26^14 > 2^64, so a 64-bit hash always fits. Lowercase
// letters only, so CamelCasePlugin (which Kysely re-runs over embedded
// subqueries) leaves the hash alone.
const HASH_LENGTH = 14;

// Module level so that any plugin instance restores what any other shortened.
const shortByName = new Map<string, string>();
const originalByShort = new Map<string, string>();
const restoredByKey = new Map<string, string>();

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

function shorten(name: string): string {
	let short = shortByName.get(name);
	if (short !== undefined) {
		return short;
	}
	short = name;
	if (byteLength(name) > MAX_IDENTIFIER_BYTES) {
		const tail = "~" + hash(name);
		let head = "";
		for (const char of name) {
			if (byteLength(head + char + tail) > MAX_IDENTIFIER_BYTES) {
				break;
			}
			head += char;
		}
		short = head + tail;
		const other = originalByShort.get(short);
		if (other !== undefined && other !== name) {
			throw new AliasHashCollisionError(name, other);
		}
		originalByShort.set(short, name);
	}
	shortByName.set(name, short);
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

class ShortenIdentifiersTransformer extends k.OperationNodeTransformer {
	protected override transformIdentifier(
		node: k.IdentifierNode,
		queryId?: k.QueryId,
	): k.IdentifierNode {
		node = super.transformIdentifier(node, queryId);
		const short = shorten(node.name);
		return short === node.name ? node : { ...node, name: short };
	}
}

/**
 * A Kysely plugin that shortens identifiers over PostgreSQL's 63-byte limit
 * (which PostgreSQL would otherwise silently truncate) and restores the
 * original names in result rows.
 *
 * Install it last. If you use `CamelCasePlugin`, pass it in so that lengths
 * are measured on the snake_cased names the database sees:
 *
 * ```ts
 * plugins: [fixLongAliases(new CamelCasePlugin())]
 * ```
 */
export function fixLongAliases(inner?: k.KyselyPlugin): k.KyselyPlugin {
	const transformer = new ShortenIdentifiersTransformer();
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
