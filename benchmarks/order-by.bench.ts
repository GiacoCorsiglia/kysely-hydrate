/** `sortBy` and `sqlCompare` from `src/helpers/order-by.ts`, on plain arrays. */
import { DecimalStub, PlainDateStub } from "../src/helpers/order-by.test-stubs.ts";
import { type OrderBy, sortBy, sqlCompare } from "../src/helpers/order-by.ts";
import { createdPrefixedAccessor, getPrefixedValue } from "../src/helpers/prefixes.ts";
import { group, runSuite, type Workload } from "./lib/harness.ts";
import { range, shuffle } from "./lib/rows.ts";

type Row = Record<`k${0 | 1 | 2 | 3 | 4}` | "tied", number>;
type GetValue = (row: Row, key: keyof Row | ((row: Row) => unknown)) => unknown;

/**
 * Each `k` column is an independently shuffled permutation of `[0, n)`, so the
 * sorted result is unique and one column's order doesn't predict the next.
 */
function makeRows(n: number): Row[] {
	const [k0, k1, k2, k3, k4] = range(5, 1).map((seed) => shuffle(range(n), seed));
	return range(n).map((i) => ({
		k0: k0![i]!,
		k1: k1![i]!,
		k2: k2![i]!,
		k3: k3![i]!,
		k4: k4![i]!,
		tied: 0,
	}));
}

const PREFIX = "posts$$";

/**
 * A copy of the private `Hydrator#makePrefixedGetValue`, not a simplification:
 * the point is to measure the branch the hydrator runs, where a function key
 * sees the row through a Proxy.
 */
const prefixedGetValue: GetValue = (row, key) =>
	typeof key === "function"
		? key(createdPrefixedAccessor(PREFIX, row) as unknown as Row)
		: getPrefixedValue(PREFIX, row, key);

/** Every column prefixed, as a nested level receives them.  Only `prefixedGetValue` reads these. */
const asPrefixed = (rows: Row[]) =>
	rows.map((row) =>
		Object.fromEntries(Object.entries(row).map(([key, value]) => [PREFIX + key, value])),
	) as unknown as Row[];

/**
 * Every ordering below is decided by its first on anything but `tied`, always
 * `k0`, so a stable sort on it in that ordering's direction is the expected result.
 */
function sorting(rows: Row[], orderings: OrderBy<Row>[], getValue?: GetValue): Workload {
	const k0 = (row: Row) => (getValue ? getValue(row, "k0") : row.k0) as number;
	const sign = orderings.find(({ key }) => key !== "tied")!.direction === "desc" ? -1 : 1;
	return {
		run: () => sortBy(rows, orderings, getValue),
		expected: rows.slice().sort((a, b) => sign * (k0(a) - k0(b))),
	};
}

// The hydrator sorts one parent group of 10-100 rows at a time; a
// single-level `sort: "all"` query sorts every row at once.
const rows100 = makeRows(100);
const rows1k = makeRows(1000);
const rows1kSorted = rows1k.slice().sort((a, b) => a.k0 - b.k0);

const asc = (key: keyof Row): OrderBy<Row> => ({ key, direction: "asc" });
const byK0 = [asc("k0")];
const byK0Function: OrderBy<Row>[] = [{ key: (row) => row.k0, direction: "asc" }];
// Same columns extracted, but only "decided by the last" compares them all.
const decidedByFirst: OrderBy<Row>[] = [
	...byK0,
	asc("k1"),
	{ key: "k2", direction: "desc" },
	asc("k3"),
	asc("k4"),
];
// Descending, so a sort that ignored direction fails.
const decidedByLast = (n: number): OrderBy<Row>[] => [
	...Array<OrderBy<Row>>(n - 1).fill(asc("tied")),
	{ key: "k0", direction: "desc" },
];

group({
	"sortBy 100 rows": sorting(rows100, byK0),
	"sortBy 10 rows": sorting(makeRows(10), byK0),
	"sortBy 1k rows": sorting(rows1k, byK0),
	"sortBy 10k rows": sorting(makeRows(10_000), byK0),
	// What a key costs to read, at the size a real parent group pays it.
	"sortBy 100 rows, function key": sorting(rows100, byK0Function),
	"sortBy 100 rows, prefixed string key": sorting(asPrefixed(rows100), byK0, prefixedGetValue),
	"sortBy 100 rows, prefixed function key through a Proxy": sorting(
		asPrefixed(rows100),
		byK0Function,
		prefixedGetValue,
	),
});

// Ordered input costs TimSort a linear scan, and equal keys fall through
// every column: the cheapest of these is the floor of key extraction, index
// setup and rebuilding the output.
group({
	"sortBy 1k random rows, 1 ordering": sorting(rows1k, byK0),
	"sortBy 1k rows already in order": sorting(rows1kSorted, byK0),
	"sortBy 1k rows in reverse order": sorting(rows1kSorted.slice().reverse(), byK0),
	"sortBy 1k rows with equal keys": sorting(
		rows1k.map((row) => ({ ...row, k0: 0 })),
		byK0,
	),
	...Object.fromEntries(
		[3, 5].flatMap((n) => [
			[
				`sortBy 1k rows, ${n} orderings decided by the first`,
				sorting(rows1k, decidedByFirst.slice(0, n)),
			],
			[`sortBy 1k rows, ${n} orderings decided by the last`, sorting(rows1k, decidedByLast(n))],
		]),
	),
});

/**
 * One column per `sqlCompare` dispatch path, all the same length and the same
 * permutation.  `typeRankOf` tries types in turn, so position in its chain is
 * part of the cost; decimals, needing two property probes, come last.
 */
const COLUMN_SIZE = 480;
const date = (i: number) => new Date(Date.UTC(2024, 0, 1) + i * 86_400_000);

/** Each ascending in `i`, over `n` values. */
const builders = {
	// The baseline: `typeRankOf`'s first case, and two relational operators.
	numbers: (i: number) => i * 3,
	// Two distinct values, so `sqlCompare`'s `a === b` fast path answers most comparisons.
	booleans: (i: number, n: number) => i >= n / 2,
	bigints: (i: number) => BigInt(i) * 3n,
	decimals: (i: number) => new DecimalStub(i * 3),
	Dates: date,
	"Temporal-like values": (i: number) => new PlainDateStub(date(i).toISOString().slice(0, 10)),
	strings: (i: number) => `value-${String(i).padStart(4, "0")}`,
	"byte arrays": (i: number) => new Uint8Array([0, 0, 0, 0, (i >>> 8) & 0xff, i & 0xff, 7, 9]),
	// Each element is a full recursive `sqlCompare`: dispatch three times per pair.
	"number arrays": (i: number) => [Math.floor(i / 100), i % 100, i],
};

// Unlike types rank-compare and return, so this measures `typeRankOf` almost
// alone.  Listed in `sqlCompare`'s documented rank order, which verifying it
// then asserts in full.
const rankOrder = [
	"booleans",
	"numbers",
	"decimals",
	"Dates",
	"Temporal-like values",
	"strings",
	"byte arrays",
	"number arrays",
] as const;
const perRank = COLUMN_SIZE / rankOrder.length;

const columns: Record<string, (i: number) => unknown> = {
	...Object.fromEntries(
		Object.entries(builders).map(([name, build]) => [name, (i: number) => build(i, COLUMN_SIZE)]),
	),
	"values of mixed types": (i) =>
		builders[rankOrder[Math.floor(i / perRank)]!](i % perRank, perRank),
};

// Directly, not through `sortBy`, so the comparator is the only variable.
group(
	Object.fromEntries(
		Object.entries(columns).map(([name, build]) => {
			const ordered = range(COLUMN_SIZE).map((i) => build(i));
			const shuffled = shuffle(ordered, 17);
			return [
				`sqlCompare ${COLUMN_SIZE} ${name}`,
				{ run: () => shuffled.slice().sort(sqlCompare), expected: ordered },
			];
		}),
	),
);

// `sortBy` places nulls itself, ahead of `sqlCompare`, with or without any nulls present.
const nullable = shuffle(range(1000), 23);
const halfNull = nullable.map((v) => ({ v: v % 2 === 0 ? null : v }));
const allNull = range(500).map(() => ({ v: null }));
const odds = range(500).map((i) => ({ v: i * 2 + 1 }));
const byV = (rows: { v: number | null }[], nulls: "first" | "last", expected: unknown) => ({
	run: () => sortBy(rows, [{ key: "v", direction: "asc", nulls }]),
	expected,
});

group({
	"sortBy 1k rows with no nulls": byV(
		nullable.map((v) => ({ v })),
		"first",
		range(1000).map((v) => ({ v })),
	),
	"sortBy 1k rows half null, nulls first": byV(halfNull, "first", [...allNull, ...odds]),
	"sortBy 1k rows half null, nulls last": byV(halfNull, "last", [...odds, ...allNull]),
});

await runSuite();
