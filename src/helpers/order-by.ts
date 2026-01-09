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

	desc(): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({
			...this.orderBy,
			direction: "desc",
		});
	}

	nullsFirst(): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({
			...this.orderBy,
			nulls: "first",
		});
	}

	nullsLast(): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({
			...this.orderBy,
			nulls: "last",
		});
	}

	toOperationNode(): k.OperationNode {
		throw new Error("Not implemented");
	}

	asc(): MockOrderByItemBuilder {
		return new MockOrderByItemBuilder({
			...this.orderBy,
			direction: "asc",
		});
	}

	collate(): MockOrderByItemBuilder {
		return this;
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

export function sqlCompare(a: unknown, b: unknown): number {
	if (a === b) {
		return 0;
	}
	if (isNil(a)) {
		return -1;
	}
	if (isNil(b)) {
		return 1;
	}

	const aType = typeof a;
	const bType = typeof b;

	// numbers
	if (aType === "number" && bType === "number") {
		return (a as number) - (b as number);
	}

	if (aType === "bigint" && bType === "bigint") {
		return a < b ? -1 : 1;
	}

	if (aType === "boolean" && bType === "boolean") {
		// false < true
		return a ? 1 : -1;
	}

	// strings
	if (aType === "string" && bType === "string") {
		return a < b ? -1 : 1;
	}

	// dates
	if (a instanceof Date && b instanceof Date) {
		return a.getTime() - b.getTime();
	}

	// fallback (SQL would error; JS must total-order)
	return String(a) < String(b) ? -1 : 1;
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
