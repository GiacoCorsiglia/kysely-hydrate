import type * as k from "kysely";

export interface OrderBy<T = Record<string, unknown>> {
	key: keyof T | ((input: T) => unknown);
	direction: "asc" | "desc";
	nulls?: "first" | "last" | undefined;
}

function nullsDefault(direction: "asc" | "desc"): "first" | "last" {
	// Postgres/Oracle default: NULLS LAST for ASC, NULLS FIRST for DESC.
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
	Decimal: 2,
	Date: 3,
	Temporal: 4,
	String: 5,
	Bytes: 6,
	Array: 7,
	Other: 8,
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
			if (decimalCompareOf(value) !== undefined) {
				return TypeRank.Decimal;
			}
			return TypeRank.Other;
	}
}

/** Position by position, then shorter first, as SQL orders bytes and arrays. */
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
 * Decimal libraries are detected structurally, so none is a dependency:
 * decimal.js and big.js expose `cmp`, bignumber.js `comparedTo`.
 */
function decimalCompareOf(value: unknown): ((other: unknown) => unknown) | undefined {
	const shape = value as { cmp?: unknown; comparedTo?: unknown };
	if (typeof shape.cmp === "function") {
		return shape.cmp as (other: unknown) => unknown;
	}
	if (typeof shape.comparedTo === "function") {
		return shape.comparedTo as (other: unknown) => unknown;
	}
	return undefined;
}

/**
 * A comparison against a decimal NaN yields `NaN` (decimal.js) or `null`
 * (bignumber.js, which would coerce to 0), so any non-number result pins the
 * NaN operand last, like a `number` NaN.
 */
function compareDecimals(a: object, b: object): number {
	const result = decimalCompareOf(a)!.call(a, b);
	if (typeof result === "number" && !Number.isNaN(result)) {
		return result;
	}
	return compareNaNs(Number.isNaN(Number(String(a))), Number.isNaN(Number(String(b))));
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

/** Not-a-value (NaN, invalid Date) sorts last; two of them compare equal. */
function compareNaNs(aIsNaN: boolean, bIsNaN: boolean): number {
	if (aIsNaN === bIsNaN) {
		return 0;
	}
	return aIsNaN ? 1 : -1;
}

/** `<` and `>` work across number/bigint, so `1` and `1n` compare equal. */
function compareNumbers(a: number | bigint, b: number | bigint): number {
	if (a < b) {
		return -1;
	}
	if (a > b) {
		return 1;
	}
	// Equal, or at least one NaN.
	return compareNaNs(a !== a, b !== b);
}

/**
 * Total-order comparator emulating SQL ORDER BY.
 *
 * - `null`/`undefined` compare equal and sort first. (`sortBy` applies NULLS
 *   FIRST/LAST itself; this governs array elements and direct callers.)
 * - Same-type values compare as SQL does: booleans, numbers and bigints
 *   (mixed too), Dates by timestamp, strings by code unit, bytes and arrays
 *   lexicographically, decimals via their library's `cmp`/`comparedTo`, and
 *   `Temporal.*` via the type's static `compare` (`Duration` by nominal
 *   length, as a Postgres `interval`).
 * - Cross-type values, which SQL would reject, order by `TypeRank`. Different
 *   Temporal types order by type name; "other" values by `String()` form.
 * - NaN, decimal NaN, and invalid Dates sort after their peers and compare
 *   equal to each other. A comparator must never return NaN.
 */
export function sqlCompare(a: unknown, b: unknown): number {
	if (a === b) {
		return 0;
	}
	if (isNil(a)) {
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
			// Unequal, so false < true.
			return a ? 1 : -1;

		case TypeRank.Numeric:
			return compareNumbers(a as number | bigint, b as number | bigint);

		case TypeRank.Decimal:
			return compareDecimals(a as object, b as object);

		case TypeRank.Date:
			// An invalid Date's timestamp is NaN, which pins it last.
			return compareNumbers((a as Date).getTime(), (b as Date).getTime());

		case TypeRank.Temporal:
			return compareTemporal(a as object, b as object);

		case TypeRank.String:
			return (a as string) < (b as string) ? -1 : 1;

		case TypeRank.Bytes:
			return compareLexicographic(a as Uint8Array, b as Uint8Array, (x, y) => x - y);

		case TypeRank.Array:
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

/** An ordering with direction and null placement resolved up front. */
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

function compareColumn(a: unknown, b: unknown, plan: ColumnPlan): number {
	const aNull = isNil(a);
	const bNull = isNil(b);

	if (aNull || bNull) {
		if (aNull && bNull) {
			return 0;
		}
		// NULLS FIRST/LAST is independent of ASC/DESC.
		const nullFirst = aNull ? -1 : 1;
		return plan.nullsFirst ? nullFirst : -nullFirst;
	}

	return sqlCompare(a, b) * plan.direction;
}

/**
 * Sorts rows by the given orderings into a new array. Keys are extracted once
 * per row rather than on every comparison; for function keys the hydrator
 * builds a Proxy per extraction, so this is O(n) Proxies instead of O(n log n).
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

	// One key array per ordering. Plain loops: the hydrator calls this per
	// parent group of 10-100 rows, where map/Array.from closures measured
	// ~1.5x the whole sort. Per-row key arrays measured 1.5-2x slower.
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
		// Keep equal rows in input order.
		return x - y;
	});

	const sorted = new Array<T>(n);
	for (let i = 0; i < n; i++) {
		sorted[i] = rows[indices[i]!]!;
	}
	return sorted;
}
