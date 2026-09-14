import type * as k from "kysely";

export interface OrderBy<T = Record<string, unknown>> {
	key: keyof T | ((input: T) => unknown);
	direction: "asc" | "desc";
	nulls?: "first" | "last" | undefined;
}

function nullsDefault(direction: "asc" | "desc"): "first" | "last" {
	// Default nulls behavior matches PostgreSQL/Oracle:
	// NULLS LAST for ASC, NULLS FIRST for DESC
	return direction === "asc" ? "last" : "first";
}

class MockOrderByItemBuilder {
	readonly orderBy: OrderBy;

	constructor(orderBy: OrderBy) {
		this.orderBy = orderBy;
	}

	#with(patch: Partial<OrderBy>): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({ ...this.orderBy, ...patch });
	}

	asc(): MockOrderByItemBuilder {
		return this.#with({ direction: "asc" });
	}

	desc(): MockOrderByItemBuilder {
		return this.#with({ direction: "desc" });
	}

	nullsFirst(): MockOrderByItemBuilder {
		return this.#with({ nulls: "first" });
	}

	nullsLast(): MockOrderByItemBuilder {
		return this.#with({ nulls: "last" });
	}

	collate(): MockOrderByItemBuilder {
		return this;
	}

	toOperationNode(): k.OperationNode {
		throw new Error("Not implemented");
	}
}

export function kyselyOrderByToOrderBy(expr: string, modifiers: k.OrderByModifiers): OrderBy<any> {
	if (typeof modifiers === "string") {
		return {
			key: expr,
			direction: modifiers,
			nulls: nullsDefault(modifiers),
		};
	}

	const builder = new MockOrderByItemBuilder({
		key: expr,
		direction: "asc",
	});
	const built = modifiers(
		builder as unknown as k.OrderByItemBuilder,
	) as unknown as MockOrderByItemBuilder;

	return built.orderBy;
}

function isNil(value: unknown): value is null | undefined {
	return value === null || value === undefined;
}

const TypeRank = {
	Boolean: 0,
	Numeric: 1,
	Date: 2,
	Temporal: 3,
	String: 4,
	/** `Buffer` and `Uint8Array`, which is how drivers return binary columns. */
	Bytes: 5,
	Array: 6,
	Other: 7,
} as const;

type TypeRank = (typeof TypeRank)[keyof typeof TypeRank];

function typeRankOf(value: unknown): TypeRank {
	switch (typeof value) {
		case "boolean":
			return TypeRank.Boolean;
		case "number":
		case "bigint":
			return TypeRank.Numeric;
		case "string":
			return TypeRank.String;
		default:
			if (value instanceof Date) {
				return TypeRank.Date;
			}
			if (temporalTag(value) !== undefined) {
				return TypeRank.Temporal;
			}
			if (value instanceof Uint8Array) {
				return TypeRank.Bytes;
			}
			if (Array.isArray(value)) {
				return TypeRank.Array;
			}
			return TypeRank.Other;
	}
}

/**
 * Lexicographic ordering shared by binary data and arrays, matching how SQL
 * orders both: compare position by position, and when one is a prefix of the
 * other the shorter sorts first.
 */
function compareLexicographic<T>(
	a: ArrayLike<T>,
	b: ArrayLike<T>,
	compareElement: (x: T, y: T) => number,
): number {
	const shared = Math.min(a.length, b.length);
	for (let i = 0; i < shared; i++) {
		const cmp = compareElement(a[i]!, b[i]!);
		if (cmp !== 0) {
			return cmp;
		}
	}
	return a.length - b.length;
}

/**
 * Every `Temporal.*` prototype carries its type name as `Symbol.toStringTag`.
 * Detecting by tag needs no `Temporal` global and recognizes polyfills too.
 */
function temporalTag(value: unknown): string | undefined {
	const tag = (value as { [Symbol.toStringTag]?: unknown })[Symbol.toStringTag];
	return typeof tag === "string" && tag.startsWith("Temporal.") ? tag : undefined;
}

/**
 * Temporal types order via a static `compare` that throws across types, so
 * unlike types are separated by tag first. `PlainMonthDay` has no `compare`
 * and falls back to string forms.
 */
function compareTemporal(a: object, b: object): number {
	const aTag = temporalTag(a)!;
	// Same constructor implies same tag.
	if (a.constructor !== b.constructor) {
		const bTag = temporalTag(b)!;
		if (aTag !== bTag) {
			return aTag < bTag ? -1 : 1;
		}
	}
	if (aTag === "Temporal.Duration") {
		return durationNanos(a) - durationNanos(b);
	}
	const compare = (a.constructor as { compare?: (x: object, y: object) => number }).compare;
	return typeof compare === "function" ? compare(a, b) : compareStringForms(a, b);
}

/**
 * Nominal length under Postgres's interval convention (30-day months, 12-month
 * years), which is how `ORDER BY` an `interval` column sorts.
 * `Temporal.Duration.compare` throws for calendar units, so it cannot give a
 * total order.
 */
function durationNanos(duration: object): number {
	const d = duration as Record<string, number | undefined>;
	const days = (d.years ?? 0) * 360 + (d.months ?? 0) * 30 + (d.weeks ?? 0) * 7 + (d.days ?? 0);
	const seconds = ((days * 24 + (d.hours ?? 0)) * 60 + (d.minutes ?? 0)) * 60 + (d.seconds ?? 0);
	return (
		seconds * 1e9 + (d.milliseconds ?? 0) * 1e6 + (d.microseconds ?? 0) * 1e3 + (d.nanoseconds ?? 0)
	);
}

/**
 * Last resort: compare `String()` forms. Distinct values sharing a form must
 * compare equal, or antisymmetry breaks.
 */
function compareStringForms(a: unknown, b: unknown): number {
	const aStr = String(a);
	const bStr = String(b);
	return aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
}

/**
 * Compares "not a value" values (NaN numbers, invalid Dates) against their
 * well-ordered peers: not-a-value sorts after every real value, and two
 * not-a-values compare equal.
 */
function compareNaNs(aIsNaN: boolean, bIsNaN: boolean): number {
	if (aIsNaN === bIsNaN) {
		return 0;
	}
	return aIsNaN ? 1 : -1;
}

/**
 * Numeric ordering with NaN pinned last. `<`, `>`, and the equality
 * fallthrough work correctly on mixed number/bigint operands, so e.g.
 * `sqlCompare(1, 1n) === 0`.
 */
function compareNumbers(a: number | bigint, b: number | bigint): number {
	if (a < b) {
		return -1;
	}
	if (a > b) {
		return 1;
	}
	// Neither ordered: equal, or at least one is NaN (bigint is never NaN).
	return compareNaNs(a !== a, b !== b);
}

/**
 * Total-order comparator emulating SQL ORDER BY semantics in JavaScript.
 *
 * - `null`/`undefined` compare equal to each other and less than everything
 *   else. (`sortBy` handles NULLS FIRST/LAST for top-level values itself, so
 *   this matters for array elements and direct `sqlCompare` callers.)
 * - Same-type comparisons match SQL: booleans (false < true), numbers and
 *   bigints numerically (including mixed number/bigint), Dates by timestamp,
 *   strings lexicographically by code unit, binary data (`Buffer`,
 *   `Uint8Array`) byte-wise, and arrays element-wise -- with a prefix sorting
 *   before the longer value it prefixes, as in SQL.
 * - `Temporal.*` values sort via their type's static `compare`, except
 *   `Duration`, which sorts by nominal length as a Postgres `interval` does.
 *   Different Temporal types are separated by type name.
 * - Cross-type comparisons (where SQL would error, but a JS comparator must
 *   still produce a total order) resolve by type rank:
 *   boolean < numeric (number/bigint) < Date < Temporal < string < binary <
 *   array < everything else. Values ranked "everything else" compare by their
 *   String() forms.
 * - `NaN` sorts after all other numerics, and invalid Dates sort after all
 *   valid Dates; NaN vs NaN and invalid Date vs invalid Date compare equal.
 *   (Returning NaN from a comparator, as `a - b` would, makes Array.sort
 *   behavior implementation-defined and can leave the array unsorted.)
 */
export function sqlCompare(a: unknown, b: unknown): number {
	if (a === b) {
		return 0;
	}
	if (isNil(a)) {
		// null and undefined compare equal to each other (they are not ===).
		return isNil(b) ? 0 : -1;
	}
	if (isNil(b)) {
		return 1;
	}

	const rank = typeRankOf(a);
	const rankDiff = rank - typeRankOf(b);
	if (rankDiff !== 0) {
		return rankDiff;
	}

	switch (rank) {
		case TypeRank.Boolean:
			// false < true; the equal cases returned 0 above.
			return a ? 1 : -1;

		case TypeRank.Numeric:
			return compareNumbers(a as number | bigint, b as number | bigint);

		case TypeRank.Date:
			// An invalid Date has a NaN timestamp, so it pins last like NaN.
			return compareNumbers((a as Date).getTime(), (b as Date).getTime());

		case TypeRank.Temporal:
			return compareTemporal(a as object, b as object);

		case TypeRank.String:
			// The equal case returned 0 above.
			return (a as string) < (b as string) ? -1 : 1;

		case TypeRank.Bytes:
			// Bytes are unsigned integers, so plain subtraction is a valid comparison.
			return compareLexicographic(a as Uint8Array, b as Uint8Array, (x, y) => x - y);

		case TypeRank.Array:
			// Elements recurse, so arrays of any supported type work, nested
			// arrays and nulls included.
			return compareLexicographic(a as unknown[], b as unknown[], sqlCompare);

		case TypeRank.Other:
			return compareStringForms(a, b);
	}
}

type GetValue<T> = (obj: T, key: keyof T | ((input: T) => unknown)) => unknown;

const defaultGetter = <T>(obj: T, key: keyof T | ((input: T) => unknown)) => {
	if (typeof key === "function") {
		return key(obj);
	}
	return (obj as any)[key];
};

/**
 * An ordering reduced to what the comparison loop needs, so direction and
 * null placement are resolved once rather than on every comparison.
 */
interface ColumnPlan {
	readonly direction: 1 | -1;
	readonly nullsFirst: boolean;
}

function planColumns<T>(orderings: readonly OrderBy<T>[]): ColumnPlan[] {
	return orderings.map(({ direction, nulls }) => ({
		direction: direction === "asc" ? 1 : -1,
		nullsFirst: (nulls ?? nullsDefault(direction)) === "first",
	}));
}

/**
 * Compares one column's values. Returns 0 when the two are indistinguishable
 * for this column (including both null), leaving the caller to move on to the
 * next ordering.
 */
function compareColumn(a: unknown, b: unknown, plan: ColumnPlan): number {
	const aNull = isNil(a);
	const bNull = isNil(b);

	if (aNull || bNull) {
		if (aNull && bNull) {
			return 0;
		}
		// NULLS FIRST/LAST is independent of ASC/DESC, so the ordering
		// direction deliberately does not apply here.
		const nullFirst = aNull ? -1 : 1;
		return plan.nullsFirst ? nullFirst : -nullFirst;
	}

	return sqlCompare(a, b) * plan.direction;
}

/**
 * Sorts rows by the given orderings, returning a new array.
 *
 * Rather than sorting with a comparator that extracts both rows' keys on
 * every call, every row's keys are extracted once up front, taking key
 * extraction from O(n log n) calls to O(n).
 *
 * That matters because `getValue` is not always cheap: for function keys the
 * hydrator builds a Proxy per extraction, which at 10k rows is the difference
 * between ~10k and ~218k Proxy allocations.
 */
export function sortBy<T>(
	rows: readonly T[],
	orderings: readonly OrderBy<T>[],
	getValue: GetValue<T> = defaultGetter,
): T[] {
	if (orderings.length === 0 || rows.length < 2) {
		return rows.slice();
	}

	const plans = planColumns(orderings);
	const n = rows.length;

	// Plain loops throughout rather than map/Array.from: the hydrator calls
	// this once per parent group, typically on 10-100 rows, where the closure
	// allocations measured ~1.5x the whole sort.
	//
	// One flat array per ordering, holding that column's key for every row.
	// Per-row key arrays measured 1.5-2x slower on 1e5 rows.
	const columns: unknown[][] = new Array(orderings.length);
	for (let c = 0; c < orderings.length; c++) {
		const key = orderings[c]!.key;
		const column = new Array<unknown>(n);
		for (let i = 0; i < n; i++) {
			column[i] = getValue(rows[i]!, key);
		}
		columns[c] = column;
	}

	const indices = new Array<number>(n);
	for (let i = 0; i < n; i++) {
		indices[i] = i;
	}
	indices.sort((x, y) => {
		for (let i = 0; i < columns.length; i++) {
			const cmp = compareColumn(columns[i]![x], columns[i]![y], plans[i]!);
			if (cmp !== 0) {
				return cmp;
			}
		}
		// Array.sort is stable, but sorting indices rather than the rows
		// themselves would leave equal rows in whatever order the sort put
		// their indices. Falling back to the original index restores it.
		return x - y;
	});

	const sorted = new Array<T>(n);
	for (let i = 0; i < n; i++) {
		sorted[i] = rows[indices[i]!]!;
	}
	return sorted;
}
