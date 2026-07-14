/**
 * @module alias-encoding
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1).  The prefixed column aliases this library generates for
 * nested joins (`parent$$child$$column`) can exceed that limit, which would
 * mangle result-row keys and even collide two distinct columns that share the
 * same first 63 bytes.
 *
 * {@link encodeAlias} deterministically compresses over-long generated
 * aliases:
 *
 * - Aliases that are safely under the limit are returned unchanged, so SQL
 *   stays fully human-readable in the common case.
 * - Over-long aliases become `<head>$<hash>`: a readable head (the start of
 *   the alias, which preserves the relation-path context) plus a hash of the
 *   whole alias, together always under 63 bytes.
 *
 * ## Domain invariance
 *
 * With a `CamelCasePlugin`, the same alias exists in two spellings: the
 * TypeScript domain (`posts$$createdAt`, what users and the hydrator see) and
 * the SQL domain (`posts$$created_at`, what query nodes carry after the
 * plugin's `transformQuery` — which Kysely applies in `toOperationNode()`,
 * i.e. already at query-composition time).  Different call sites therefore
 * see different spellings of the same alias, and generated aliases pass
 * through `snakeCase` once per nesting level and through `camelCase` once on
 * the result row.  To make all call sites agree, the encoding is defined to
 * be invariant across these spellings:
 *
 * - The head and hash are computed over a *canonical form* (lowercased,
 *   underscores stripped), which is unchanged by camelCase <-> snake_case
 *   conversions.
 * - The length threshold uses a *worst-case byte length* under which
 *   `snakeCase` is length-neutral (an uppercase letter and its `_x`
 *   replacement both count 2 bytes), so both spellings make the same
 *   encode-or-not decision.
 * - The encoded output contains no underscores or uppercase letters, so it
 *   is a fixed point of every `snakeCase`/`camelCase` configuration: the
 *   result-row key equals the alias exactly, with or without the plugin.
 */

/**
 * PostgreSQL's identifier limit: NAMEDATALEN - 1 bytes.
 */
export const MAX_IDENTIFIER_BYTES = 63;

/**
 * Length of the hash suffix appended to compressed aliases.  Lowercase
 * letters only (base 26), so the suffix never expands under any camelCase or
 * snake_case transformation.  12 characters ~ 56 bits of the hash.
 */
const HASH_LENGTH = 12;

/**
 * Worst-case byte budget for the readable head of a compressed alias: the
 * limit minus the `"$"` marker and the hash suffix (all single-byte,
 * non-expanding characters).
 */
const HEAD_BUDGET = MAX_IDENTIFIER_BYTES - 1 - HASH_LENGTH;

const utf8 = new TextEncoder();

function isDigitChar(char: string): boolean {
	return char >= "0" && char <= "9";
}

function isUppercaseChar(char: string): boolean {
	return char.toLowerCase() !== char;
}

/**
 * The worst-case byte cost of `char` in SQL: its UTF-8 length, plus one byte
 * if `snakeCase` may expand it with an underscore — uppercase letters
 * (`aB` -> `a_b`), and digits starting a digit run (`a9` -> `a_9` with
 * `underscoreBeforeDigits`).  The first character of an identifier
 * (`prev === undefined`) never expands.
 */
function worstCaseCharBytes(char: string, prev: string | undefined): number {
	let bytes = utf8.encode(char).length;
	if (
		prev !== undefined &&
		(isUppercaseChar(char) || (isDigitChar(char) && prev !== "_" && !isDigitChar(prev)))
	) {
		bytes += 1;
	}
	return bytes;
}

/**
 * The worst-case byte length of `identifier` as it may appear in SQL under
 * any `CamelCasePlugin` configuration (see {@link worstCaseCharBytes}).
 *
 * The measure is invariant under `snakeCase` itself (an uppercase letter
 * counts 2 bytes, exactly like its `_x` replacement), so the camelCase and
 * snake_case spellings of an alias always yield the same result.
 */
export function worstCaseIdentifierBytes(identifier: string): number {
	let bytes = 0;
	let prev: string | undefined;
	for (const char of identifier) {
		bytes += worstCaseCharBytes(char, prev);
		prev = char;
	}
	return bytes;
}

/**
 * The canonical form of an alias: lowercased with underscores stripped.
 * Unchanged by camelCase <-> snake_case conversions, so every spelling of an
 * alias shares one canonical form.
 */
function canonicalAlias(alias: string): string {
	return alias.replaceAll("_", "").toLowerCase();
}

/**
 * FNV-1a 64-bit hash over the UTF-8 bytes of `str`.
 */
function fnv1a64(str: string): bigint {
	let hash = 0xcbf29ce484222325n;
	for (const byte of utf8.encode(str)) {
		hash ^= BigInt(byte);
		hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
	}
	return hash;
}

/**
 * Renders the low bits of `value` as `length` lowercase letters (base 26).
 */
function toLowercaseLetters(value: bigint, length: number): string {
	let out = "";
	for (let i = 0; i < length; i++) {
		out += String.fromCharCode(97 + Number(value % 26n));
		value /= 26n;
	}
	return out;
}

const encodeCache = new Map<string, string>();

/**
 * Encodes a generated column alias so it never exceeds PostgreSQL's 63-byte
 * identifier limit, under any `CamelCasePlugin` configuration.
 *
 * Aliases already within the limit are returned unchanged.  Over-long
 * aliases are compressed to `<head>$<hash>`, where `<head>` is the start of
 * the alias's canonical form (so the relation path stays recognizable in
 * SQL) and `<hash>` is a hash of the full canonical form.  Because the
 * canonical form and the length threshold are invariant across camelCase and
 * snake_case spellings (see the module docs), every spelling of an alias
 * encodes to the same identifier, which can therefore be independently
 * recomputed wherever the alias must be referenced (ORDER BY clauses,
 * result-row restoration).
 */
export function encodeAlias(alias: string): string {
	if (worstCaseIdentifierBytes(alias) <= MAX_IDENTIFIER_BYTES) {
		return alias;
	}

	const cached = encodeCache.get(alias);
	if (cached !== undefined) {
		return cached;
	}

	const canonical = canonicalAlias(alias);

	// Take as much of the canonical form as fits in the head budget, counting
	// worst-case bytes (the canonical form has no uppercase or underscores,
	// but digits may still expand under `underscoreBeforeDigits`).  Iterating
	// by code point (for..of) avoids splitting surrogate pairs.
	let head = "";
	let cost = 0;
	let prev: string | undefined;
	for (const char of canonical) {
		const charCost = worstCaseCharBytes(char, prev);
		if (cost + charCost > HEAD_BUDGET) {
			break;
		}
		head += char;
		cost += charCost;
		prev = char;
	}

	const encoded = `${head}$${toLowercaseLetters(fnv1a64(canonical), HASH_LENGTH)}`;
	encodeCache.set(alias, encoded);
	return encoded;
}
