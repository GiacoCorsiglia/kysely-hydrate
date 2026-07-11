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

	private with(patch: Partial<OrderBy>): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({ ...this.orderBy, ...patch });
	}

	asc(): MockOrderByItemBuilder {
		return this.with({ direction: "asc" });
	}

	desc(): MockOrderByItemBuilder {
		return this.with({ direction: "desc" });
	}

	nullsFirst(): MockOrderByItemBuilder {
		return this.with({ nulls: "first" });
	}

	nullsLast(): MockOrderByItemBuilder {
		return this.with({ nulls: "last" });
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

		case TypeRank.Numeric: {
			const aNum = a as number | bigint;
			const bNum = b as number | bigint;
			const aIsNaN = typeof aNum === "number" && Number.isNaN(aNum);
			const bIsNaN = typeof bNum === "number" && Number.isNaN(bNum);
			if (aIsNaN || bIsNaN) {
				return compareNaNs(aIsNaN, bIsNaN);
			}
			// <, >, and the equality fallthrough work correctly on mixed
			// number/bigint operands, so e.g. sqlCompare(1, 1n) === 0.
			if (aNum < bNum) {
				return -1;
			}
			return aNum > bNum ? 1 : 0;
		}

		case TypeRank.Date: {
			const aTime = (a as Date).getTime();
			const bTime = (b as Date).getTime();
			const aIsNaN = Number.isNaN(aTime);
			const bIsNaN = Number.isNaN(bTime);
			if (aIsNaN || bIsNaN) {
				return compareNaNs(aIsNaN, bIsNaN);
			}
			return aTime - bTime;
		}

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

const defaultGetter = <T>(obj: T, key: keyof T | ((input: T) => unknown)) => {
	if (typeof key === "function") {
		return key(obj);
	}
	return (obj as any)[key];
};

export function makeOrderByComparator<T>(
	orderings: readonly OrderBy<T>[],
	getValue: (obj: T, key: keyof T | ((input: T) => unknown)) => unknown = defaultGetter,
) {
	return (lhs: T, rhs: T): number => {
		for (const { key, direction, nulls } of orderings) {
			const a = getValue(lhs, key);
			const b = getValue(rhs, key);

			const aNull = isNil(a);
			const bNull = isNil(b);

			if (aNull || bNull) {
				if (aNull && bNull) {
					continue;
				}
				const dir = aNull ? -1 : 1;
				const effectiveNulls = nulls ?? nullsDefault(direction);
				return effectiveNulls === "first" ? dir : -dir;
			}

			const cmp = sqlCompare(a, b);
			if (cmp !== 0) {
				return direction === "asc" ? cmp : -cmp;
			}
		}
		return 0;
	};
}
