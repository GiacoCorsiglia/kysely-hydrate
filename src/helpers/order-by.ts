interface OrderBy<T = Record<string, unknown>> {
	key: keyof T;
	direction: "asc" | "desc";
	nulls: "first" | "last";
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

export function makeOrderByComparator<T extends object>(orderings: readonly OrderBy<T>[]) {
	return (lhs: T, rhs: T): number => {
		for (const { key, direction, nulls } of orderings) {
			const a = lhs[key];
			const b = rhs[key];

			const aNull = isNil(a);
			const bNull = isNil(b);

			if (aNull || bNull) {
				if (aNull && bNull) {
					continue;
				}
				const dir = aNull ? -1 : 1;
				return nulls === "first" ? dir : -dir;
			}

			const cmp = sqlCompare(a, b);
			if (cmp !== 0) {
				return direction === "asc" ? cmp : -cmp;
			}
		}
		return 0;
	};
}
