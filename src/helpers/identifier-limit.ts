/**
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1), emitting only a NOTICE.  The prefixed column aliases
 * generated for nested relations (`parent$$child$$column`) can easily exceed
 * that, mangling result-row keys and even colliding two distinct columns that
 * share the same first 63 bytes.
 *
 * When a generated logical alias would not survive, it is replaced with a
 * hierarchical index path: `0/3/2` means relation 0 at the top level, its
 * relation 3, and that relation's column 2.  Index paths preserve the nesting
 * structure of the logical alias (the prefix `0/3/` identifies exactly one
 * relation's columns) and are effectively immune to the length limit: a path
 * of depth `d` with 3-digit indices at every level is `4d - 1` bytes, so 63
 * bytes allows 16 levels of nesting with up to 1000 relations/columns per
 * level (and 12 levels even under CamelCasePlugin's worst-case
 * `underscoreBeforeDigits` expansion, which adds one byte per segment).
 * Exceeding that throws `IdentifierTooLongError` rather than truncating.
 */

/**
 * The maximum identifier length in bytes: PostgreSQL's NAMEDATALEN - 1.
 * (SQLite and MySQL impose no comparable limit on result-column aliases.)
 */
const MAX_IDENTIFIER_BYTES = 63;

/**
 * Separator for hierarchical index-path aliases.  Distinct from the `$$`
 * logical separator so the two alias schemes can never be confused, and a
 * fixed point of CamelCasePlugin's snake_case/camelize transforms.
 */
export const INDEX_PATH_SEP = "/";

const INDEX_PATH_PATTERN = /^\d+(?:\/\d+)*$/;

/**
 * Tests whether a name is shaped like a hierarchical index path (`0/3/2`).
 */
export function isIndexPath(name: string): boolean {
	return INDEX_PATH_PATTERN.test(name);
}

const utf8 = new TextEncoder();

/**
 * Tests whether an alias is guaranteed to survive the SQL identifier limit,
 * conservatively accounting for the worst-case expansion CamelCasePlugin's
 * snake_case transform can apply before the identifier reaches the database:
 *
 * - one underscore per case-mapped (uppercase) character, and
 * - one underscore per digit run (the `underscoreBeforeDigits` option).
 *
 * The estimate is conservative on purpose: renaming an alias that would have
 * squeaked by is always safe, while keeping one that gets truncated is not.
 */
export function fitsWithinIdentifierLimit(name: string): boolean {
	let worstCaseBytes = utf8.encode(name).length;
	let previousWasDigit = false;

	for (const char of name) {
		const isDigit = char >= "0" && char <= "9";
		if (isDigit && !previousWasDigit) {
			worstCaseBytes++;
		}
		previousWasDigit = isDigit;

		if (!isDigit && char.toLowerCase() !== char) {
			worstCaseBytes++;
		}
	}

	return worstCaseBytes <= MAX_IDENTIFIER_BYTES;
}
