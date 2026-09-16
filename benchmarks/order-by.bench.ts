/**
 * Benchmarks for `src/helpers/order-by.ts`: `sortBy`, which orders a group of
 * rows, and `sqlCompare`, the total-order comparator underneath it.
 *
 *   npm run bench -- order-by              # just this suite
 *   npm run bench -- order-by --filter X   # only benchmarks matching /X/
 *
 * Nothing here builds join rows, a hydrator or a database.  Sorting is a leaf
 * of the hydration path, and the questions this suite answers — how the sort
 * scales, what key extraction costs against comparison, which `sqlCompare`
 * dispatch paths are expensive — are all answerable from plain arrays.  That is
 * also why `lib/rows.ts` is not imported: it pulls in the hydrator, and this
 * suite would then pay for it on every filtered run.
 *
 * Read the numbers as relative comparisons within a `summary()` group; across
 * runs the machine drifts far more than most of these differences.
 */
import assert from "node:assert/strict";

import { summary } from "mitata";

import { DecimalStub, PlainDateStub } from "../src/helpers/order-by.test-stubs.ts";
import { type OrderBy, sortBy, sqlCompare } from "../src/helpers/order-by.ts";
import { createdPrefixedAccessor, getPrefixedValue } from "../src/helpers/prefixes.ts";
import { benchSync, runSuite } from "./lib/harness.ts";

////////////////////////////////////////////////////////////
// Fixture building blocks.
////////////////////////////////////////////////////////////

/** Builds `[0, 1, ... n - 1]` worth of `T`. */
function times<T>(n: number, build: (i: number) => T): T[] {
	return Array.from({ length: n }, (_, i) => build(i));
}

/**
 * mulberry32.  Seeded rather than `Math.random` so every run sorts the same
 * permutation: a sort's cost depends on how disordered its input is, so an
 * input that varied run to run would add noise no baseline could absorb.
 */
function makeRandom(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state + 0x6d2b79f5) >>> 0;
		let t = Math.imul(state ^ (state >>> 15), 1 | state);
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
	};
}

function shuffle<T>(values: readonly T[], random: () => number): T[] {
	const result = values.slice();
	for (let i = result.length - 1; i > 0; i--) {
		const j = Math.floor(random() * (i + 1));
		const swap = result[i]!;
		result[i] = result[j]!;
		result[j] = swap;
	}
	return result;
}

////////////////////////////////////////////////////////////
// Rows.
////////////////////////////////////////////////////////////

interface Row {
	k0: number;
	k1: number;
	k2: number;
	k3: number;
	k4: number;
	/** The same in every row, so an ordering on it decides nothing. */
	tied: number;
	label: string;
}

/**
 * Each sort column holds a distinct integer in a shuffled order.  Distinct
 * keys make the expected result unique, so verification does not have to reason
 * about which ties the sort happened to preserve; independently shuffled
 * columns keep one column's order from predicting the next.
 */
function makeRows(n: number): Row[] {
	const identity = times(n, (i) => i);
	const columns = times(5, (c) => shuffle(identity, makeRandom(c + 1)));
	return times(n, (i) => ({
		k0: columns[0]![i]!,
		k1: columns[1]![i]!,
		k2: columns[2]![i]!,
		k3: columns[3]![i]!,
		k4: columns[4]![i]!,
		tied: 0,
		label: `row-${i}`,
	}));
}

// The hydrator sorts one parent group at a time, which `sortBy`'s own comment
// puts at 10-100 rows.  1k and 10k are here because a single-level query with
// `sort: "all"` sorts every row at once, and because the shape of the curve
// says whether the per-call setup or the comparisons dominate at each size.
const rows10 = makeRows(10);
const rows100 = makeRows(100);
const rows1k = makeRows(1000);
const rows10k = makeRows(10_000);

////////////////////////////////////////////////////////////
// Orderings.
////////////////////////////////////////////////////////////

const byK0: OrderBy<Row> = { key: "k0", direction: "asc" };
const byK0Function: OrderBy<Row> = { key: (row) => row.k0, direction: "asc" };
const byTied: OrderBy<Row> = { key: "tied", direction: "asc" };

const orderings1 = [byK0];
const orderings1Function = [byK0Function];

// `sortBy` extracts one key array per ordering up front, then compares columns
// left to right until one decides.  Pairing "decided by the first" against
// "decided by the last" separates those two costs: both extract the same number
// of columns, but only the second compares them all.
const orderings3First: OrderBy<Row>[] = [
	byK0,
	{ key: "k1", direction: "asc" },
	{ key: "k2", direction: "desc" },
];
const orderings5First: OrderBy<Row>[] = [
	...orderings3First,
	{ key: "k3", direction: "asc" },
	{ key: "k4", direction: "desc" },
];
const orderings3Last: OrderBy<Row>[] = [byTied, byTied, byK0];
const orderings5Last: OrderBy<Row>[] = [byTied, byTied, byTied, byTied, byK0];

////////////////////////////////////////////////////////////
// Input that is already ordered.
//
// A sorted or reversed run costs V8's TimSort a linear scan rather than
// n log n comparisons, and all-equal keys make every comparison fall through
// every column.  Against the random case they bracket how much of `sortBy` is
// comparison at all: whatever the cheapest of them still costs is the floor of
// key extraction, index setup and rebuilding the output array.
////////////////////////////////////////////////////////////

const rows1kSorted = sortBy(rows1k, orderings1);
const rows1kReversed = rows1kSorted.slice().reverse();
const rows1kAllEqual = rows1k.map((row) => ({ ...row, k0: 0 }));

////////////////////////////////////////////////////////////
// Keys read through the hydrator's getter.
//
// `sortBy`'s default getter indexes the row directly.  The hydrator passes its
// own, because at a nested level the row's columns carry a prefix: a string key
// becomes a concatenation, and a function key has to see an unprefixed view of
// the row, which means a Proxy per extraction.  `sortBy`'s comment claims that
// is O(n) Proxies rather than O(n log n); these measure what one of those n
// costs.
////////////////////////////////////////////////////////////

const PREFIX = "posts$$";

/**
 * A copy of `Hydrator#makePrefixedGetValue`, which is private and so cannot be
 * imported.  It stays a copy rather than a simplification: the point is to
 * measure the branch the hydrator actually runs.
 */
function prefixedGetValue(row: Row, key: keyof Row | ((input: Row) => unknown)): unknown {
	if (typeof key === "function") {
		return key(createdPrefixedAccessor(PREFIX, row) as unknown as Row);
	}
	return getPrefixedValue(PREFIX, row, key);
}

/**
 * Rows with every column prefixed, as a nested level receives them.  The cast
 * is the same gap the hydrator has: orderings name unprefixed columns while the
 * rows carry prefixed ones, which is why the hydrator types its own orderings
 * as `OrderBy<any>`.  Only `prefixedGetValue` ever reads these rows.
 */
function asPrefixedRows(rows: readonly Row[]): Row[] {
	return rows.map((row) =>
		Object.fromEntries(Object.entries(row).map(([key, value]) => [PREFIX + key, value])),
	) as unknown as Row[];
}

const prefixedRows100 = asPrefixedRows(rows100);

////////////////////////////////////////////////////////////
// Columns of one type, for `sqlCompare` dispatch.
//
// `typeRankOf` switches on `typeof` and then falls through a chain of
// `instanceof`/duck-type checks, so how far down the chain a type sits is part
// of what comparing it costs — and `Decimal`, which needs two property probes,
// sits last.  Every column is the same length and the same permutation, so the
// only variable between these benchmarks is the value type.
//
// Real `Temporal` values are behind a flag on Node 22, but detection is by
// `Symbol.toStringTag` precisely so that polyfills work, and the test stubs
// carry the same shape V8 implements.  Decimal libraries are likewise detected
// structurally and are not dependencies.
////////////////////////////////////////////////////////////

const COLUMN_SIZE = 500;
const BASE_MS = Date.UTC(2024, 0, 1);
const DAY_MS = 86_400_000;

/** Ascending in `i`, so string ordering by code unit agrees with numeric order. */
function padded(i: number): string {
	return String(i).padStart(4, "0");
}

function isoDate(i: number): string {
	return new Date(BASE_MS + i * DAY_MS).toISOString().slice(0, 10);
}

interface Column<T> {
	/** Ascending under `sqlCompare`, by construction: the expected result. */
	readonly ordered: readonly T[];
	readonly shuffled: T[];
}

/** `build` must be ascending in `i`; the shuffle is what the sort has to undo. */
function column<T>(build: (i: number) => T): Column<T> {
	const ordered = times(COLUMN_SIZE, build);
	// One permutation, shared by every column, so the columns are comparable.
	return { ordered, shuffled: shuffle(ordered, makeRandom(17)) };
}

// Half false then half true: only two distinct values, so `sqlCompare`'s
// `a === b` fast path answers most of these comparisons before any dispatch.
const booleans = column((i) => i >= COLUMN_SIZE / 2);
const numbers = column((i) => i * 3);
const bigints = column((i) => BigInt(i) * 3n);
const strings = column((i) => `value-${padded(i)}`);
const dates = column((i) => new Date(BASE_MS + i * DAY_MS));
const bytes = column((i) => new Uint8Array([0, 0, 0, 0, (i >>> 8) & 0xff, i & 0xff, 7, 9]));
// Lexicographic order agrees with `i`, and each element comparison is a full
// recursive `sqlCompare`, so this measures dispatch three times per pair.
const numberArrays = column((i) => [Math.floor(i / 100), i % 100, i]);
const decimals = column((i) => new DecimalStub(i * 3));
const temporals = column((i) => new PlainDateStub(isoDate(i)));

////////////////////////////////////////////////////////////
// A column of mixed types.
//
// SQL would reject this, but hydrated rows can carry it — a JSON column, or a
// union across a polymorphic join.  Unlike types never reach the same-type
// comparison at all: they rank-compare and return, which makes this a
// measurement of `typeRankOf` almost on its own.
//
// Written in the rank order `sqlCompare` documents, so this array is also the
// assertion of what that order is.
////////////////////////////////////////////////////////////

const MIXED_PER_KIND = 60;
const MIXED_KINDS = 8;
const MIXED_SIZE = MIXED_PER_KIND * MIXED_KINDS;

const mixed: Column<unknown> = (() => {
	const ordered: unknown[] = [
		...times(MIXED_PER_KIND, (i) => i >= MIXED_PER_KIND / 2),
		...times(MIXED_PER_KIND, (i) => i * 3),
		...times(MIXED_PER_KIND, (i) => new DecimalStub(i * 3)),
		...times(MIXED_PER_KIND, (i) => new Date(BASE_MS + i * DAY_MS)),
		...times(MIXED_PER_KIND, (i) => new PlainDateStub(isoDate(i))),
		...times(MIXED_PER_KIND, (i) => `value-${padded(i)}`),
		...times(MIXED_PER_KIND, (i) => new Uint8Array([i, 7, 9])),
		...times(MIXED_PER_KIND, (i) => [Math.floor(i / 10), i % 10]),
	];
	return { ordered, shuffled: shuffle(ordered, makeRandom(17)) };
})();

////////////////////////////////////////////////////////////
// A column that is half null.
//
// `sortBy` handles null placement itself, in `compareColumn`, ahead of
// `sqlCompare`: two extra `isNil` checks on every comparison whether or not the
// column has any nulls.  The no-null row is the control for that.
////////////////////////////////////////////////////////////

interface NullableRow {
	v: number | null;
}

const NULLABLE_SIZE = 1000;
const nullableValues = shuffle(
	times(NULLABLE_SIZE, (i) => i),
	makeRandom(23),
);

const rowsNoNulls: NullableRow[] = nullableValues.map((v) => ({ v }));
const rowsHalfNull: NullableRow[] = nullableValues.map((v) => ({ v: v % 2 === 0 ? null : v }));

const byVNullsFirst: OrderBy<NullableRow> = { key: "v", direction: "asc", nulls: "first" };
const byVNullsLast: OrderBy<NullableRow> = { key: "v", direction: "asc", nulls: "last" };

const orderingsNullsFirst = [byVNullsFirst];
const orderingsNullsLast = [byVNullsLast];

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/** The keys `orderings` would sort by, in the order the rows came back. */
function keys(rows: readonly Row[]): number[] {
	return rows.map((row) => row.k0);
}

function ascending(n: number): number[] {
	return times(n, (i) => i);
}

/**
 * Every workload runs once here and has its output asserted before anything is
 * timed.  A comparator that silently started returning 0 — or a `typeRankOf`
 * chain that stopped recognizing a type and fell through to `Other` — would
 * read as a large speedup rather than a failure, and several of these
 * benchmarks are precisely the ones that would stop doing their work.
 */
function verifyWorkloads(): void {
	// The sort columns are a permutation of [0, n), so a correct ascending sort
	// by `k0` puts them back in order, whatever the size.
	for (const rows of [rows10, rows100, rows1k, rows10k]) {
		const sorted = sortBy(rows, orderings1);
		assert.equal(sorted.length, rows.length);
		assert.deepEqual(keys(sorted), ascending(rows.length));
	}

	// Different input orders, one result: these three hold the same rows.
	assert.deepEqual(sortBy(rows1kSorted, orderings1), rows1kSorted);
	assert.deepEqual(sortBy(rows1kReversed, orderings1), rows1kSorted);
	// Nothing distinguishes all-equal keys, so the stable sort returns the input.
	assert.deepEqual(sortBy(rows1kAllEqual, orderings1), rows1kAllEqual);

	// Leading columns that tie decide nothing, so the trailing `k0` decides
	// everything and the result matches the single-ordering sort.
	for (const orderings of [orderings3First, orderings5First]) {
		assert.deepEqual(keys(sortBy(rows1k, orderings)), ascending(rows1k.length));
	}
	for (const orderings of [orderings3Last, orderings5Last]) {
		assert.deepEqual(keys(sortBy(rows1k, orderings)), ascending(rows1k.length));
	}

	// A function key must reach the same value the string key names, through
	// either getter.
	assert.deepEqual(keys(sortBy(rows100, orderings1Function)), ascending(rows100.length));
	for (const orderings of [orderings1, orderings1Function]) {
		const sorted = sortBy(prefixedRows100, orderings, prefixedGetValue);
		assert.deepEqual(
			sorted.map((row) => prefixedGetValue(row, "k0")),
			ascending(rows100.length),
			"the prefixed getter must read the same column the default getter does",
		);
	}

	// Each column is a shuffle of an array that is ascending by construction, so
	// sorting it has to reproduce that array exactly.  For `mixed` this asserts
	// the documented rank order in full: booleans, then numbers, decimals,
	// Dates, Temporal values, strings, bytes and arrays.
	const columns = [
		booleans,
		numbers,
		bigints,
		strings,
		dates,
		bytes,
		numberArrays,
		decimals,
		temporals,
		mixed,
	];
	for (const { ordered, shuffled } of columns) {
		assert.deepEqual(shuffled.slice().sort(sqlCompare), ordered);
	}
	assert.equal(mixed.ordered.length, MIXED_SIZE, "the mixed column must cover every rank");

	// Spot-check the two cases a rank comparison is easiest to get wrong: a
	// decimal is not a number, and it sorts between numbers and Dates.
	const [firstDecimal] = decimals.ordered;
	assert.equal(sqlCompare(firstDecimal, 1e9) > 0, true, "decimals sort after every number");
	assert.equal(sqlCompare(firstDecimal, new Date(0)) < 0, true, "decimals sort before Dates");
	assert.deepEqual(
		decimals.shuffled
			.slice()
			.sort(sqlCompare)
			.map((d) => d.n),
		times(COLUMN_SIZE, (i) => i * 3),
		"decimals order by value, through the stub's cmp",
	);

	// Null placement is independent of direction, so the two orderings differ
	// only in which end the nulls land on.
	const half = NULLABLE_SIZE / 2;
	const odds = times(half, (i) => i * 2 + 1);

	const nullsFirst = sortBy(rowsHalfNull, orderingsNullsFirst).map((row) => row.v);
	assert.deepEqual(nullsFirst.slice(0, half), new Array<null>(half).fill(null));
	assert.deepEqual(nullsFirst.slice(half), odds);

	const nullsLast = sortBy(rowsHalfNull, orderingsNullsLast).map((row) => row.v);
	assert.deepEqual(nullsLast.slice(0, half), odds);
	assert.deepEqual(nullsLast.slice(half), new Array<null>(half).fill(null));

	assert.deepEqual(
		sortBy(rowsNoNulls, orderingsNullsFirst).map((row) => row.v),
		ascending(NULLABLE_SIZE),
	);
}

verifyWorkloads();

////////////////////////////////////////////////////////////
// sortBy: how the sort scales.
//
// The baseline is 100 rows, the top of the range the hydrator calls this at, so
// the smaller entry reads as what a per-call fixed cost is worth and the larger
// ones as what the comparisons add.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sortBy 10 rows", () => sortBy(rows10, orderings1));
	benchSync("sortBy 100 rows", () => sortBy(rows100, orderings1)).baseline(true);
	benchSync("sortBy 1k rows", () => sortBy(rows1k, orderings1));
	benchSync("sortBy 10k rows", () => sortBy(rows10k, orderings1));
});

////////////////////////////////////////////////////////////
// sortBy: extraction against comparison, by input order.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sortBy 1k random rows", () => sortBy(rows1k, orderings1)).baseline(true);
	benchSync("sortBy 1k rows already in order", () => sortBy(rows1kSorted, orderings1));
	benchSync("sortBy 1k rows in reverse order", () => sortBy(rows1kReversed, orderings1));
	benchSync("sortBy 1k rows with equal keys", () => sortBy(rows1kAllEqual, orderings1));
});

////////////////////////////////////////////////////////////
// sortBy: extraction against comparison, by ordering count.
//
// Each extra ordering costs one more key array up front, always.  It costs an
// extra comparison per pair only when the columns before it tie, which is the
// difference between the two halves of this group.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sortBy 1k rows, 1 ordering", () => sortBy(rows1k, orderings1)).baseline(true);
	benchSync("sortBy 1k rows, 3 orderings decided by the first", () =>
		sortBy(rows1k, orderings3First),
	);
	benchSync("sortBy 1k rows, 5 orderings decided by the first", () =>
		sortBy(rows1k, orderings5First),
	);
	benchSync("sortBy 1k rows, 3 orderings decided by the last", () =>
		sortBy(rows1k, orderings3Last),
	);
	benchSync("sortBy 1k rows, 5 orderings decided by the last", () =>
		sortBy(rows1k, orderings5Last),
	);
});

////////////////////////////////////////////////////////////
// sortBy: what a key costs to read.
//
// 100 rows, the size the hydrator sorts at, so the Proxy count is the one a
// real parent group pays.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sortBy 100 rows, string key", () => sortBy(rows100, orderings1)).baseline(true);
	benchSync("sortBy 100 rows, function key", () => sortBy(rows100, orderings1Function));
	benchSync("sortBy 100 rows, prefixed string key", () =>
		sortBy(prefixedRows100, orderings1, prefixedGetValue),
	);
	benchSync("sortBy 100 rows, prefixed function key through a Proxy", () =>
		sortBy(prefixedRows100, orderings1Function, prefixedGetValue),
	);
});

////////////////////////////////////////////////////////////
// sqlCompare: dispatch by value type.
//
// These sort with `sqlCompare` directly rather than through `sortBy`, so the
// comparator is the only thing between the benchmarks.  Numbers are the
// baseline: they leave `typeRankOf` on its first switch case and compare with
// two relational operators, which is as cheap as this gets.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sqlCompare 500 numbers", () => numbers.shuffled.slice().sort(sqlCompare)).baseline(
		true,
	);
	benchSync("sqlCompare 500 booleans", () => booleans.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 bigints", () => bigints.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 strings", () => strings.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 Dates", () => dates.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 Temporal-like values", () =>
		temporals.shuffled.slice().sort(sqlCompare),
	);
	benchSync("sqlCompare 500 byte arrays", () => bytes.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 number arrays", () => numberArrays.shuffled.slice().sort(sqlCompare));
	benchSync("sqlCompare 500 decimals", () => decimals.shuffled.slice().sort(sqlCompare));
});

////////////////////////////////////////////////////////////
// sqlCompare: one type against many.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sqlCompare 480 values of one type", () =>
		numbers.shuffled.slice(0, MIXED_SIZE).sort(sqlCompare),
	).baseline(true);
	benchSync("sqlCompare 480 values of mixed types", () => mixed.shuffled.slice().sort(sqlCompare));
});

////////////////////////////////////////////////////////////
// sortBy: null handling.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("sortBy 1k rows with no nulls", () =>
		sortBy(rowsNoNulls, orderingsNullsFirst),
	).baseline(true);
	benchSync("sortBy 1k rows half null, nulls first", () =>
		sortBy(rowsHalfNull, orderingsNullsFirst),
	);
	benchSync("sortBy 1k rows half null, nulls last", () => sortBy(rowsHalfNull, orderingsNullsLast));
});

////////////////////////////////////////////////////////////
// Run.
////////////////////////////////////////////////////////////

await runSuite("order-by");
