/**
 * @module identifier-renames
 *
 * QuerySet builds prefixed column aliases when nesting relations
 * (`parent$$child$$column`).  With deep nesting or long names, those generated
 * aliases can exceed PostgreSQL's 63-byte identifier limit (NAMEDATALEN - 1),
 * which Postgres silently truncates — mangling row keys and even collapsing
 * two distinct columns into one.
 *
 * This module renames any generated alias that could exceed the limit to a
 * short counter-based alias (`$c0`, `$c1`, …) and records the mapping so the
 * rows can be translated back to their logical (full-length) names before
 * hydration.
 *
 * The `$c` prefix (rather than a bare digit) is deliberate:
 * - Bare integer-like keys (`"0"`, `"1"`) are reordered ahead of other keys in
 *   JS objects, which would scramble row key order.
 * - `$` cannot appear in a camelCase/snake_case boundary, so Kysely's
 *   CamelCasePlugin round-trips `$c0` unchanged (even with
 *   `underscoreBeforeDigits`, `$c0` → `$c_0` in SQL → `$c0` in result rows).
 * - `$` makes accidental collision with real column names vanishingly
 *   unlikely (and collisions are avoided explicitly regardless).
 */

import { SEP } from "./prefixes.ts";

/**
 * PostgreSQL truncates identifiers longer than 63 bytes (NAMEDATALEN - 1).
 */
const MAX_IDENTIFIER_BYTES = 63;

const RENAMED_ALIAS_PREFIX = "$c";

/**
 * Bidirectional mapping between logical (full-length, prefixed) column names
 * and the short SQL aliases actually used in the generated query.  Names that
 * fit within the identifier limit are not renamed and do not appear in either
 * map.
 */
export interface IdentifierRenames {
	/** Logical name (`a$$b$$column`) → short SQL alias (`$c0`). */
	readonly toShort: ReadonlyMap<string, string>;
	/** Short SQL alias (`$c0`) → logical name (`a$$b$$column`). */
	readonly toLogical: ReadonlyMap<string, string>;
}

const EMPTY_MAP: ReadonlyMap<string, string> = new Map();

export const EMPTY_RENAMES: IdentifierRenames = {
	toShort: EMPTY_MAP,
	toLogical: EMPTY_MAP,
};

const textEncoder = new TextEncoder();

/**
 * Whether an alias could exceed PostgreSQL's 63-byte identifier limit once it
 * reaches the database.
 *
 * This must account for Kysely's CamelCasePlugin, which may be installed on
 * the user's database and snake_cases every identifier in the generated SQL:
 * each upper-case character can gain an underscore, and (with the
 * `underscoreBeforeDigits` option) so can each digit run.  A 58-character
 * camelCase alias can therefore still become a 64+ byte SQL identifier.  We
 * cannot detect the plugin from here, so we use the worst-case length.  This
 * over-approximation only means a few borderline names get renamed that
 * strictly needed it only when the plugin is present — harmless, and it keeps
 * the behavior deterministic and dialect-independent.
 */
export function mayExceedIdentifierLimit(name: string): boolean {
	// Fast path: a UTF-16 code unit encodes to at most 3 UTF-8 bytes and can
	// gain at most 1 byte of snake_case growth, so short names can never exceed
	// the limit.
	if (name.length * 4 <= MAX_IDENTIFIER_BYTES) {
		return false;
	}

	let worstCase = textEncoder.encode(name).length;
	let prevWasDigit = false;
	for (const char of name) {
		const isDigit = char >= "0" && char <= "9";
		if (char !== char.toLowerCase()) {
			// snake_case may insert an underscore before an upper-case character.
			worstCase++;
		} else if (isDigit && !prevWasDigit) {
			// snake_case with `underscoreBeforeDigits` inserts an underscore before
			// each digit run.
			worstCase++;
		}
		prevWasDigit = isDigit;
	}

	return worstCase > MAX_IDENTIFIER_BYTES;
}

/**
 * Computes the renames for one SELECT list, given its logical column names in
 * canonical order.  Every name that could exceed the identifier limit is
 * assigned the next `$cN` alias; short names keep their logical name.
 *
 * The numbering is a pure function of the (ordered) logical names, so any two
 * queries built from the same query-set structure agree on the aliases.
 */
export function computeIdentifierRenames(logicalNames: readonly string[]): IdentifierRenames {
	const toShort = new Map<string, string>();
	const toLogical = new Map<string, string>();
	let taken: Set<string> | undefined;
	let counter = 0;

	for (const name of logicalNames) {
		// Only generated (prefixed) aliases are rename candidates.  Plain column
		// names are the user's own; if one is over-long it would be equally
		// truncated in a plain Kysely query, and renaming it would change the
		// visible output of `toQuery()` in the common, non-nested case.
		if (!name.includes(SEP) || !mayExceedIdentifierLimit(name)) {
			continue;
		}

		// Guard against the (pathological) case of a real column named `$cN`.
		taken ??= new Set(logicalNames);

		let short: string;
		do {
			short = `${RENAMED_ALIAS_PREFIX}${counter++}`;
		} while (taken.has(short));

		toShort.set(name, short);
		toLogical.set(short, name);
	}

	return toShort.size === 0 ? EMPTY_RENAMES : { toShort, toLogical };
}

/**
 * Rebuilds a result row with any short (`$cN`) keys translated back to their
 * logical names, leaving other keys untouched.  Used as a pre-pass before
 * hydration so the hydrator can keep operating on full-length prefixed names.
 */
export function restoreRowLogicalNames(
	row: unknown,
	toLogical: ReadonlyMap<string, string>,
): unknown {
	if (typeof row !== "object" || row === null) {
		return row;
	}

	const out: Record<string, unknown> = {};
	for (const key of Object.keys(row)) {
		const name = toLogical.get(key) ?? key;
		const value = (row as Record<string, unknown>)[key];
		if (name === "__proto__") {
			// A plain assignment would set the prototype instead of a property.
			Object.defineProperty(out, name, {
				value,
				enumerable: true,
				writable: true,
				configurable: true,
			});
		} else {
			out[name] = value;
		}
	}
	return out;
}
