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
	String: 3,
	Other: 4,
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
			return value instanceof Date ? TypeRank.Date : TypeRank.Other;
	}
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
 *   else. (`makeOrderByComparator` handles NULLS FIRST/LAST separately, so
 *   this branch only matters when `sqlCompare` is used directly.)
 * - Same-type comparisons match SQL: booleans (false < true), numbers and
 *   bigints numerically (including mixed number/bigint), Dates by timestamp,
 *   strings lexicographically by code unit.
 * - Cross-type comparisons (where SQL would error, but a JS comparator must
 *   still produce a total order) resolve by type rank:
 *   boolean < numeric (number/bigint) < Date < string < everything else.
 *   Values ranked "everything else" compare by their String() forms.
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

		case TypeRank.String:
			// The equal case returned 0 above.
			return (a as string) < (b as string) ? -1 : 1;

		case TypeRank.Other: {
			// Fallback: compare String() forms. Distinct values may share a
			// string form (e.g. two different objects), and those must compare
			// equal — returning 1 unconditionally would do so for both argument
			// orders, breaking the comparator contract.
			const aStr = String(a);
			const bStr = String(b);
			if (aStr === bStr) {
				return 0;
			}
			return aStr < bStr ? -1 : 1;
		}
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

export function makeOrderByComparator<T>(
	orderings: readonly OrderBy<T>[],
	getValue: GetValue<T> = defaultGetter,
) {
	const plans = planColumns(orderings);

	return (lhs: T, rhs: T): number => {
		for (let i = 0; i < orderings.length; i++) {
			const key = orderings[i]!.key;
			const cmp = compareColumn(getValue(lhs, key), getValue(rhs, key), plans[i]!);
			if (cmp !== 0) {
				return cmp;
			}
		}
		return 0;
	};
}

/**
 * Sorts rows by the given orderings, returning a new array.
 *
 * Equivalent to `rows.slice().sort(makeOrderByComparator(orderings, getValue))`
 * but extracts each row's sort keys once up front rather than on every
 * comparison, taking key extraction from O(n log n) calls to O(n).
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
	// One pass per ordering, extracting that column's key for every row.
	const columns = orderings.map(({ key }) => rows.map((row) => getValue(row, key)));

	const indices = Array.from(rows, (_, i) => i);
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

	return indices.map((i) => rows[i]!);
}
