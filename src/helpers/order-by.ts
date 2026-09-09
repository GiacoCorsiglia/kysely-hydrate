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

////////////////////////////////////////////////////////////////////
// Type handlers
//
// Comparison is a two-step lookup rather than a chain of type tests: every
// value resolves to a `TypeHandler`, and the handler carries both its sort
// rank and the function that compares two values of that type.
//
// The point is that all type *detection* -- including the duck typing for
// Temporal, decimal libraries, and binary data -- happens once per
// constructor inside `classify`, whose result is memoized. The per-comparison
// path never re-tests a type; it does a table lookup and calls through.
// Supporting a new type means adding a handler, not adding a branch.
////////////////////////////////////////////////////////////////////

/**
 * Sort ranks for values of different types. SQL would reject most cross-type
 * comparisons outright, but a JavaScript comparator has to return *something*
 * and still be a total order, so unlike types are separated by rank.
 */
const Rank = {
	Boolean: 0,
	/** `number`, `bigint`, and decimal-library instances. */
	Numeric: 1,
	Date: 2,
	String: 3,
	/** `Buffer`, `Uint8Array`, and other typed arrays. */
	Bytes: 4,
	Array: 5,
	/** Any `Temporal.*` value. */
	Temporal: 6,
	Other: 7,
} as const;

interface TypeHandler {
	readonly rank: number;
	/** Distinguishes handlers that share a rank; also the tiebreak ordering. */
	readonly name: string;
	/** Compares two values that resolved to this same handler. */
	readonly compare: (a: any, b: any) => number;
	/**
	 * Compares values that share this handler's rank but resolved to different
	 * handlers (e.g. a `number` against a decimal instance).
	 */
	readonly mixed: (a: any, b: any, ha: TypeHandler, hb: TypeHandler) => number;
}

/**
 * Orders values of the same rank but different types by handler name. Names
 * are fixed strings, so the result is stable across runs -- which matters,
 * because an unstable tiebreak would make sort output depend on input order.
 */
function byHandlerName(_a: unknown, _b: unknown, ha: TypeHandler, hb: TypeHandler): number {
	return ha.name < hb.name ? -1 : ha.name > hb.name ? 1 : 0;
}

/**
 * Last-resort ordering: compare `String()` forms. Distinct values may share a
 * string form (two plain objects are both "[object Object]"), and those must
 * compare equal -- returning a nonzero constant would do so for both argument
 * orders, breaking antisymmetry.
 */
function compareStringForms(a: unknown, b: unknown): number {
	const aStr = stringForm(a);
	const bStr = stringForm(b);
	return aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
}

/**
 * `String(value)` throws for values that cannot be coerced -- an object with a
 * null prototype has no `toString`, and a Symbol refuses implicit conversion.
 * The comparator must still return an ordering for them.
 */
function stringForm(value: unknown): string {
	try {
		return String(value);
	} catch {
		return Object.prototype.toString.call(value);
	}
}

/**
 * Normalizes a third-party comparison result. Decimal libraries disagree on
 * what an unorderable comparison returns -- decimal.js gives `NaN`,
 * bignumber.js gives `null` -- and `null` is the dangerous one, because it
 * coerces to 0 and would silently report unequal values as equal. Anything
 * that is not a real number is rejected so the caller can fall back.
 */
function clampSign(result: unknown): number | undefined {
	if (typeof result !== "number" || result !== result) {
		return undefined;
	}
	return result < 0 ? -1 : result > 0 ? 1 : 0;
}

const booleanHandler: TypeHandler = {
	rank: Rank.Boolean,
	name: "boolean",
	// Equal values are resolved by the `a === b` check in sqlCompare.
	compare: (a: boolean) => (a ? 1 : -1),
	mixed: byHandlerName,
};

/**
 * Handles `number` and `bigint` together, including mixed pairs: `<` and `>`
 * are correct across the two, so `sqlCompare(1, 1n)` is 0.
 *
 * Falling through both comparisons means the values are either equal or at
 * least one is NaN; `value !== value` distinguishes them. NaN sorts after
 * every real number rather than propagating, because a comparator that
 * returns NaN (as `a - b` would) leaves Array.sort's behavior
 * implementation-defined and can leave the array unsorted.
 */
const numericHandler: TypeHandler = {
	rank: Rank.Numeric,
	name: "numeric",
	compare(a: number | bigint, b: number | bigint) {
		if (a < b) {
			return -1;
		}
		if (a > b) {
			return 1;
		}
		return compareUnordered(a !== a, b !== b);
	},
	mixed: compareDecimalMixed,
};

/**
 * Orders "not a value" results (NaN, invalid Dates) against their well-ordered
 * peers: unorderable sorts after everything real, and two unorderables compare
 * equal.
 */
function compareUnordered(aUnordered: boolean, bUnordered: boolean): number {
	if (aUnordered === bUnordered) {
		return 0;
	}
	return aUnordered ? 1 : -1;
}

const dateHandler: TypeHandler = {
	rank: Rank.Date,
	name: "Date",
	compare(a: Date, b: Date) {
		const aTime = a.getTime();
		const bTime = b.getTime();
		if (aTime < bTime) {
			return -1;
		}
		if (aTime > bTime) {
			return 1;
		}
		// Invalid Dates have a NaN timestamp; pin them after valid ones.
		return compareUnordered(aTime !== aTime, bTime !== bTime);
	},
	mixed: byHandlerName,
};

const stringHandler: TypeHandler = {
	rank: Rank.String,
	name: "string",
	compare: (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0),
	mixed: byHandlerName,
};

/**
 * Byte-wise ordering for binary columns (Postgres `bytea` arrives as a
 * `Buffer`), matching how SQL orders binary data: compare byte by byte, then
 * shorter-is-first when one is a prefix of the other.
 */
const bytesHandler: TypeHandler = {
	rank: Rank.Bytes,
	name: "bytes",
	compare(a: Uint8Array, b: Uint8Array) {
		const shared = a.length < b.length ? a.length : b.length;
		for (let i = 0; i < shared; i++) {
			const aByte = a[i]!;
			const bByte = b[i]!;
			if (aByte !== bByte) {
				return aByte < bByte ? -1 : 1;
			}
		}
		return a.length - b.length;
	},
	mixed: byHandlerName,
};

/**
 * Element-wise lexicographic ordering for array columns, matching SQL: compare
 * corresponding elements, and if one array is a prefix of the other, the
 * shorter sorts first. Elements recurse through `sqlCompare`, so arrays of any
 * supported type work, including nested arrays.
 */
const arrayHandler: TypeHandler = {
	rank: Rank.Array,
	name: "array",
	compare(a: readonly unknown[], b: readonly unknown[]) {
		const shared = a.length < b.length ? a.length : b.length;
		for (let i = 0; i < shared; i++) {
			const element = sqlCompare(a[i], b[i]);
			if (element !== 0) {
				return element;
			}
		}
		return a.length - b.length;
	},
	mixed: byHandlerName,
};

/**
 * Arbitrary-precision decimals (decimal.js, big.js, bignumber.js, and
 * anything else exposing the same shape). These share the numeric rank so a
 * column mixing decimals with plain numbers still sorts numerically.
 */
const decimalHandler: TypeHandler = {
	rank: Rank.Numeric,
	name: "decimal",
	compare: compareDecimals,
	mixed: compareDecimalMixed,
};

function compareDecimals(a: any, b: any): number {
	let result: number | undefined;
	try {
		result = clampSign(typeof a.cmp === "function" ? a.cmp(b) : a.comparedTo(b));
	} catch {
		// Decimal libraries throw on operands they cannot interpret at all.
		return compareStringForms(a, b);
	}
	if (result !== undefined) {
		return result;
	}
	// An unorderable result means a decimal NaN. Pin it after real values, the
	// same treatment numeric NaN gets, rather than letting it fall somewhere
	// else in the ordering.
	return compareUnordered(isUnorderableNumeric(a), isUnorderableNumeric(b));
}

/**
 * Whether a numeric-ranked value has no place in the numeric ordering. A
 * decimal NaN reveals itself by comparing unorderably against itself, which
 * works whichever decimal library produced it.
 */
function isUnorderableNumeric(value: any): boolean {
	if (typeof value === "number") {
		return value !== value;
	}
	if (typeof value === "bigint") {
		return false;
	}
	try {
		return (
			clampSign(typeof value.cmp === "function" ? value.cmp(value) : value.comparedTo(value)) ===
			undefined
		);
	} catch {
		return true;
	}
}

/**
 * Compares a decimal against a plain `number`/`bigint`. Every decimal library
 * checked accepts a plain number argument to `cmp`/`comparedTo`, so the
 * decimal side drives the comparison and the result is negated when the
 * decimal is the right-hand operand.
 */
function compareDecimalMixed(a: any, b: any, ha: TypeHandler, hb: TypeHandler): number {
	if (ha === decimalHandler) {
		return compareDecimals(a, b);
	}
	if (hb === decimalHandler) {
		return -compareDecimals(b, a);
	}
	return byHandlerName(a, b, ha, hb);
}

const otherHandler: TypeHandler = {
	rank: Rank.Other,
	name: "other",
	compare: compareStringForms,
	mixed: byHandlerName,
};

/**
 * Builds a handler for a `Temporal.*` value.
 *
 * Every Temporal type exposes ordering as a *static* `compare` on its
 * constructor rather than an instance method, so it is reached from the value
 * itself -- no reference to a `Temporal` global, which matters because
 * Temporal is not available on every supported runtime.
 *
 * Each Temporal type is its own handler: `Temporal.PlainDate.compare` throws
 * when handed a value of a different type, so only same-type pairs may reach
 * it. Unlike Temporal types fall to the shared-rank name tiebreak, ordering
 * them by tag ("Temporal.Instant" before "Temporal.PlainDate", etc.).
 *
 * `Temporal.PlainMonthDay` has no `compare` at all -- a month/day pair has no
 * meaningful order -- and `Temporal.Duration.compare` throws for durations
 * with calendar-ambiguous units (comparing months against days needs a
 * starting point). Both fall back to string forms.
 */
function makeTemporalHandler(value: object, tag: string): TypeHandler {
	const constructor = (
		Object.getPrototypeOf(value) as { constructor?: { compare?: unknown } } | null
	)?.constructor;
	const compare = constructor?.compare;

	if (typeof compare !== "function") {
		return { rank: Rank.Temporal, name: tag, compare: compareStringForms, mixed: byHandlerName };
	}

	return {
		rank: Rank.Temporal,
		name: tag,
		compare(a: unknown, b: unknown) {
			try {
				return clampSign(compare(a, b)) ?? compareStringForms(a, b);
			} catch {
				return compareStringForms(a, b);
			}
		},
		mixed: byHandlerName,
	};
}

/**
 * Determines the handler for an object. Only ever called once per
 * constructor -- `handlerFor` memoizes the result -- so the duck-type probing
 * here costs nothing per comparison.
 */
function classify(value: object): TypeHandler {
	if (value instanceof Date) {
		return dateHandler;
	}
	if (Array.isArray(value)) {
		return arrayHandler;
	}
	// Typed arrays and Buffers, but not DataView, which has no indexed access.
	if (ArrayBuffer.isView(value) && typeof (value as { length?: unknown }).length === "number") {
		return bytesHandler;
	}

	const tag = (value as { [Symbol.toStringTag]?: unknown })[Symbol.toStringTag];
	if (typeof tag === "string" && tag.startsWith("Temporal.")) {
		return makeTemporalHandler(value, tag);
	}

	const candidate = value as { cmp?: unknown; comparedTo?: unknown };
	if (typeof candidate.cmp === "function" || typeof candidate.comparedTo === "function") {
		return decimalHandler;
	}

	return otherHandler;
}

/**
 * Handlers for the primitive types, keyed by `typeof`. `number` and `bigint`
 * deliberately share one handler so mixed pairs take the same-handler path.
 * Types absent from this table (`object`, `function`) fall through to
 * `classify`.
 */
const primitiveHandlers: Partial<Record<string, TypeHandler>> = {
	boolean: booleanHandler,
	number: numericHandler,
	bigint: numericHandler,
	string: stringHandler,
	symbol: otherHandler,
	function: otherHandler,
};

/**
 * Memoized per constructor. Keyed weakly so classifying values from
 * dynamically created classes does not retain those classes.
 */
const handlerCache = new WeakMap<object, TypeHandler>();

/** Stands in for values whose `constructor` is missing (null-prototype objects). */
const noConstructor: object = {};

function handlerFor(value: NonNullable<unknown>): TypeHandler {
	const primitive = primitiveHandlers[typeof value];
	if (primitive !== undefined) {
		return primitive;
	}

	const constructor = (value as { constructor?: unknown }).constructor;
	// A non-object constructor (someone assigned to the property) cannot key a
	// WeakMap; treat it as unclassifiable rather than throwing.
	const key =
		typeof constructor === "function" || (typeof constructor === "object" && constructor !== null)
			? (constructor as object)
			: noConstructor;

	const cached = handlerCache.get(key);
	if (cached !== undefined) {
		return cached;
	}

	const handler = key === noConstructor ? otherHandler : classify(value as object);
	handlerCache.set(key, handler);
	return handler;
}

/**
 * Total-order comparator approximating SQL ORDER BY semantics in JavaScript.
 *
 * It is a best-effort emulation: SQL rejects most cross-type comparisons,
 * while a JavaScript comparator must return an answer for every pair and
 * satisfy Array.sort's contract (antisymmetry, transitivity, and an ordering
 * independent of input order). Where SQL would error, values are separated by
 * type rank instead.
 *
 * - `null`/`undefined` compare equal to each other and before everything else.
 *   (`makeOrderByComparator` applies NULLS FIRST/LAST separately, so this only
 *   matters when `sqlCompare` is used directly.)
 * - Same-type comparisons match SQL: booleans false < true, numbers and
 *   bigints numerically (including mixed pairs), Dates by timestamp, strings
 *   by code unit, binary data byte-wise, arrays element-wise.
 * - Duck-typed support, detected structurally so no library is a dependency:
 *   decimal instances (decimal.js, big.js, bignumber.js) sort within the
 *   numeric rank alongside plain numbers, and `Temporal.*` values sort via
 *   their type's static `compare`.
 * - Cross-type pairs resolve by rank: boolean < numeric < Date < string <
 *   binary < array < Temporal < everything else. Unrelated types that share a
 *   rank are ordered by a fixed type name. Anything unrecognized compares by
 *   its `String()` form.
 * - Values that are not orderable within their own type -- NaN, invalid Dates,
 *   decimal NaN -- sort after their well-ordered peers and compare equal to
 *   each other, rather than returning NaN and leaving the sort undefined.
 *
 * Note that Dates and `Temporal.Instant` occupy separate ranks: a column
 * mixing the two groups them by type rather than interleaving them
 * chronologically.
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

	// Fast path for same-typed primitives, which dominate real columns. This
	// duplicates what the numeric/string handlers do, but reaching them costs
	// two lookups and an indirect call -- measurably slower than the inline
	// comparison for the most common case.
	const type = typeof a;
	if (type === typeof b) {
		if (typeof a === "number" || typeof a === "bigint") {
			// bigint is never NaN, so the unordered check is a no-op for it.
			const other = b as number;
			return a < other ? -1 : a > other ? 1 : compareUnordered(a !== a, b !== b);
		}
		if (typeof a === "string") {
			// The equal case returned 0 above.
			return a < (b as string) ? -1 : 1;
		}
		if (type === "object" && a instanceof Date && b instanceof Date) {
			return dateHandler.compare(a, b);
		}
	}

	const ha = handlerFor(a);
	const hb = handlerFor(b);
	if (ha === hb) {
		return ha.compare(a, b);
	}
	if (ha.rank !== hb.rank) {
		return ha.rank - hb.rank;
	}
	return ha.mixed(a, b, ha, hb);
}

type GetValue<T> = (obj: T, key: keyof T | ((input: T) => unknown)) => unknown;

const defaultGetter = <T>(obj: T, key: keyof T | ((input: T) => unknown)) => {
	if (typeof key === "function") {
		return key(obj);
	}
	return (obj as any)[key];
};

/**
 * An ordering reduced to the two numbers the comparison loop actually needs,
 * so direction and null placement are resolved once rather than per
 * comparison.
 */
interface ColumnPlan {
	/** 1 for ascending, -1 for descending. */
	readonly direction: number;
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
 * comparison. Array.sort calls its comparator O(n log n) times, and each call
 * would otherwise re-extract both operands' keys -- so key extraction drops
 * from O(n log n) to O(n).
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
	const columns: unknown[][] = orderings.map(({ key }) => {
		const values = new Array<unknown>(rows.length);
		for (let i = 0; i < rows.length; i++) {
			values[i] = getValue(rows[i]!, key);
		}
		return values;
	});

	const indices = new Array<number>(rows.length);
	for (let i = 0; i < rows.length; i++) {
		indices[i] = i;
	}

	indices.sort((x, y) => {
		for (let i = 0; i < columns.length; i++) {
			const column = columns[i]!;
			const cmp = compareColumn(column[x], column[y], plans[i]!);
			if (cmp !== 0) {
				return cmp;
			}
		}
		// Array.sort is stable, but sorting indices rather than the rows
		// themselves discards that guarantee -- equal elements would be ordered
		// by whatever the sort implementation does with their indices. Falling
		// back to the original index restores it.
		return x - y;
	});

	const sorted = new Array<T>(rows.length);
	for (let i = 0; i < rows.length; i++) {
		sorted[i] = rows[indices[i]!]!;
	}
	return sorted;
}
