import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { DurationStub, PlainDateStub, PlainMonthDayStub, PlainTimeStub } from "./order-by.stubs.ts";
import { type OrderBy, sortBy, sqlCompare } from "./order-by.ts";

describe("sqlCompare", () => {
	it("should return 0 for equal values", () => {
		assert.equal(sqlCompare(5, 5), 0);
		assert.equal(sqlCompare("hello", "hello"), 0);
		assert.equal(sqlCompare(null, null), 0);
		assert.equal(sqlCompare(undefined, undefined), 0);
		assert.equal(sqlCompare(true, true), 0);
	});

	it("should handle null/undefined as less than any value", () => {
		assert.equal(sqlCompare(null, 5), -1);
		assert.equal(sqlCompare(undefined, "hello"), -1);
		assert.equal(sqlCompare(5, null), 1);
		assert.equal(sqlCompare("hello", undefined), 1);
	});

	it("should compare numbers correctly", () => {
		// The contract is sign-only; don't over-specify exact return values
		assert.ok(sqlCompare(1, 2) < 0);
		assert.ok(sqlCompare(2, 1) > 0);
		assert.ok(sqlCompare(-5, 3) < 0);
		assert.equal(sqlCompare(0, 0), 0);
		assert.ok(sqlCompare(1.5, 1.2) > 0);
		assert.ok(sqlCompare(1.2, 1.5) < 0);
	});

	it("should compare bigints correctly", () => {
		assert.equal(sqlCompare(1n, 2n), -1);
		assert.equal(sqlCompare(2n, 1n), 1);
		assert.equal(sqlCompare(100n, 100n), 0);
		assert.equal(sqlCompare(9007199254740991n, 9007199254740992n), -1);
	});

	it("should compare booleans correctly (false < true)", () => {
		assert.equal(sqlCompare(false, true), -1);
		assert.equal(sqlCompare(true, false), 1);
		assert.equal(sqlCompare(false, false), 0);
		assert.equal(sqlCompare(true, true), 0);
	});

	it("should compare strings correctly (case-sensitive)", () => {
		assert.equal(sqlCompare("a", "b"), -1);
		assert.equal(sqlCompare("b", "a"), 1);
		assert.equal(sqlCompare("hello", "hello"), 0);
		assert.equal(sqlCompare("A", "a"), -1); // 'A' < 'a' in lexicographic order
		assert.equal(sqlCompare("apple", "banana"), -1);
	});

	it("should compare dates correctly", () => {
		const date1 = new Date("2024-01-01");
		const date2 = new Date("2024-01-02");
		const date3 = new Date("2024-01-01");

		assert.ok(sqlCompare(date1, date2) < 0);
		assert.ok(sqlCompare(date2, date1) > 0);
		assert.equal(sqlCompare(date1, date3), 0);
	});

	it("should resolve mixed types by type rank", () => {
		// Cross-type pairs resolve by type rank (see the TypeRank table), never
		// by stringification — comparing 10 vs "10" as equal while 2 vs "10"
		// compares lexicographically breaks transitivity (2 < 10 numerically but
		// "10" < "2" lexicographically), making sort output depend on input
		// order.
		assert.ok(sqlCompare(1, "1") < 0);
		assert.ok(sqlCompare("1", 1) > 0);
		assert.ok(sqlCompare(1, "2") < 0);
		assert.ok(sqlCompare("2", 1) > 0);
		assert.ok(sqlCompare(true, 0) < 0);
		assert.ok(sqlCompare(new Date(0), "a") < 0);
		assert.ok(sqlCompare(new Date(0), 1) > 0);
		assert.ok(sqlCompare("a", {}) < 0);

		// Unrecognized objects compare by String() form: distinct forms order,
		// identical forms are equal.
		assert.ok(sqlCompare({}, new Map()) > 0);
		assert.ok(sqlCompare(new Map(), {}) < 0);
		assert.equal(sqlCompare({}, { a: 1 }), 0);

		// number and bigint are the same rank and compare numerically.
		assert.equal(sqlCompare(1, 1n), 0);
		assert.equal(sqlCompare(1n, 2), -1);
		assert.equal(sqlCompare(2, 1n), 1);
	});

	it("should pin NaN after all other numerics", () => {
		assert.equal(sqlCompare(NaN, NaN), 0);
		assert.ok(sqlCompare(NaN, 3) > 0);
		assert.ok(sqlCompare(3, NaN) < 0);
		assert.ok(sqlCompare(NaN, Infinity) > 0);
		assert.ok(sqlCompare(NaN, 9007199254740993n) > 0);

		const values = [3, NaN, 1, NaN, 2];
		values.sort(sqlCompare);
		assert.deepEqual(values.slice(0, 3), [1, 2, 3]);
		assert.ok(Number.isNaN(values[3]));
		assert.ok(Number.isNaN(values[4]));
	});

	it("should pin invalid Dates after all valid Dates", () => {
		const invalid1 = new Date(NaN);
		const invalid2 = new Date("nope");
		const valid = new Date("2024-01-01");

		assert.equal(sqlCompare(invalid1, invalid2), 0);
		assert.ok(sqlCompare(invalid1, valid) > 0);
		assert.ok(sqlCompare(valid, invalid1) < 0);
	});

	it("should order binary data byte-wise, shorter prefix first", () => {
		assert.ok(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 3])) < 0);
		assert.ok(sqlCompare(Buffer.from([1, 3]), Buffer.from([1, 2])) > 0);
		assert.equal(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 2])), 0);
		// A prefix sorts before the longer value it prefixes.
		assert.ok(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 2, 0])) < 0);
		assert.ok(sqlCompare(Buffer.alloc(0), Buffer.from([0])) < 0);
		// Byte-wise, not stringified: String() of both of these is "1,2".
		assert.equal(sqlCompare(Buffer.from([1, 2]), new Uint8Array([1, 2])), 0);
		// High bytes compare as unsigned, where a signed read would invert them.
		assert.ok(sqlCompare(Buffer.from([0x7f]), Buffer.from([0x80])) < 0);
	});

	it("should order arrays element-wise, not by string form", () => {
		assert.ok(sqlCompare([1, 2], [1, 3]) < 0);
		assert.ok(sqlCompare([1, 3], [1, 2]) > 0);
		assert.equal(sqlCompare([1, 2], [1, 2]), 0);
		assert.ok(sqlCompare([], [0]) < 0);
		// The case stringification gets wrong: "10" < "2" lexicographically,
		// but 2 < 10 numerically.
		assert.ok(sqlCompare([2], [10]) < 0);
		// Nested arrays recurse.
		assert.ok(sqlCompare([[1, 2]], [[1, 3]]) < 0);
		// Elements of any supported type, including nulls (which sort first).
		assert.ok(sqlCompare([null, 1], [1, 1]) < 0);
		assert.ok(sqlCompare(["a", 1], ["a", 2]) < 0);
	});

	it("should keep binary data and arrays in separate ranks", () => {
		// Same String() form, different types: must not compare equal.
		assert.notEqual(sqlCompare(Buffer.from([1, 2]), [1, 2]), 0);
		assert.ok(sqlCompare("z", Buffer.from([1])) < 0);
		assert.ok(sqlCompare(Buffer.from([1]), [0]) < 0);
		assert.ok(sqlCompare([0], {}) < 0);
	});

	describe("Temporal", () => {
		it("should order values via their type's static compare", () => {
			const early = new PlainDateStub("2020-06-15");
			const late = new PlainDateStub("2024-01-01");
			assert.ok(sqlCompare(early, late) < 0);
			assert.ok(sqlCompare(late, early) > 0);
			assert.equal(sqlCompare(new PlainDateStub("2024-01-01"), new PlainDateStub("2024-01-01")), 0);
		});

		it("should never coerce a value with valueOf", () => {
			// Temporal throws from valueOf to block `a < b`. Any code path that
			// coerced instead of using compare would surface here.
			const a = new PlainDateStub("2020-06-15");
			const b = new PlainDateStub("2024-01-01");
			assert.throws(() => a.valueOf());
			assert.doesNotThrow(() => sqlCompare(a, b));
			assert.doesNotThrow(() => sqlCompare(a, 1));
			assert.doesNotThrow(() => sqlCompare(a, "x"));
			assert.doesNotThrow(() => [b, a].sort(sqlCompare));
		});

		it("should separate distinct Temporal types by name rather than comparing them", () => {
			// PlainDate.compare throws when handed a PlainTime, so unlike types
			// must never reach it.
			const date = new PlainDateStub("2024-01-01");
			const time = new PlainTimeStub("10:00:00");
			assert.doesNotThrow(() => sqlCompare(date, time));
			assert.ok(sqlCompare(date, time) < 0);
			assert.ok(sqlCompare(time, date) > 0);
		});

		it("should fall back to string forms for a type without compare", () => {
			// PlainMonthDay has no static compare at all.
			const a = new PlainMonthDayStub("01-01");
			const b = new PlainMonthDayStub("06-15");
			assert.ok(sqlCompare(a, b) < 0);
			assert.ok(sqlCompare(b, a) > 0);
			assert.equal(sqlCompare(a, new PlainMonthDayStub("01-01")), 0);
		});

		it("should order Durations by nominal length like a Postgres interval", () => {
			const duration = (iso: string) => new DurationStub(iso);
			assert.ok(sqlCompare(duration("PT90M"), duration("PT2H")) < 0);
			assert.equal(sqlCompare(duration("PT1H"), duration("PT60M")), 0);
			// Temporal.Duration.compare would throw here; a month is nominally
			// 30 days and a year 360, matching interval_cmp.
			assert.doesNotThrow(() => sqlCompare(duration("P1M"), duration("P30D")));
			assert.equal(sqlCompare(duration("P1M"), duration("P30D")), 0);
			assert.ok(sqlCompare(duration("P1M"), duration("P31D")) < 0);
			assert.equal(sqlCompare(duration("P1Y"), duration("P12M")), 0);
			assert.ok(sqlCompare(duration("P1Y"), duration("P365D")) < 0);
			assert.ok(sqlCompare(duration("P1W"), duration("P6DT23H")) > 0);
			assert.ok(sqlCompare(duration("PT1M"), duration("PT59S")) > 0);
		});

		it("should stay transitive across Durations whose native compare is partial", () => {
			const durations = ["P1M", "P5Y", "PT1H", "PT60M", "P10D", "P9D", "P2W"].map(
				(iso) => new DurationStub(iso),
			);
			for (const a of durations) {
				for (const b of durations) {
					assert.ok(Math.sign(sqlCompare(a, b)) === -Math.sign(sqlCompare(b, a)), `${a} vs ${b}`);
					for (const c of durations) {
						if (sqlCompare(a, b) <= 0 && sqlCompare(b, c) <= 0) {
							assert.ok(sqlCompare(a, c) <= 0, `${a} <= ${b} <= ${c} but ${a} > ${c}`);
						}
					}
				}
			}
		});

		it("should order subclass instances through the inherited static compare", () => {
			class MyDate extends PlainDateStub {}
			assert.ok(sqlCompare(new MyDate("2020-01-01"), new PlainDateStub("2024-01-01")) < 0);
			assert.equal(sqlCompare(new MyDate("2024-01-01"), new PlainDateStub("2024-01-01")), 0);
		});

		it("should rank Temporal between Date and string", () => {
			const date = new PlainDateStub("2024-01-01");
			assert.ok(sqlCompare(new Date("2030-01-01"), date) < 0);
			assert.ok(sqlCompare(date, "0") < 0);
			assert.ok(sqlCompare(date, Buffer.from([0])) < 0);
		});
	});

	// A pool of every value family the comparator must total-order together.
	const mixedPool = [
		null,
		undefined,
		false,
		true,
		-Infinity,
		-5,
		0,
		2,
		9.5,
		10,
		Infinity,
		NaN,
		3n,
		9007199254740993n,
		new Date("2020-06-15"),
		new Date("2024-01-01"),
		new Date(NaN),
		new DurationStub("P1M"),
		new DurationStub("PT1H"),
		new PlainDateStub("2020-06-15"),
		new PlainDateStub("2024-01-01"),
		new PlainMonthDayStub("06-15"),
		new PlainTimeStub("10:00:00"),
		"10",
		"2",
		"apple",
		Buffer.from([1, 2]),
		Buffer.from([1, 2, 3]),
		new Uint8Array([0x80]),
		[],
		[1, 2],
		[1, 10],
		[[1], [2]],
		{},
		new Map(),
	];

	it("should produce the same sorted order for any input permutation", () => {
		// The real total-order property: sort output must not depend on input
		// order. Hardcoded shuffles (no randomness) covering the failure modes
		// from the intransitive string fallback.
		const evens = mixedPool.filter((_, i) => i % 2 === 0);
		const odds = mixedPool.filter((_, i) => i % 2 === 1);
		const permutations = [
			[...mixedPool],
			[...mixedPool].reverse(),
			[...mixedPool.slice(11), ...mixedPool.slice(0, 11)],
			[...evens, ...odds].reverse(),
		];

		const baseline = [...mixedPool].sort(sqlCompare);
		for (const permutation of permutations) {
			assert.deepEqual([...permutation].sort(sqlCompare), baseline);
		}
	});

	it("should satisfy the comparator contract on all mixed-pool pairs", () => {
		// Antisymmetry: sign(cmp(a, b)) === -sign(cmp(b, a)) for every pair.
		for (const a of mixedPool) {
			for (const b of mixedPool) {
				// Compare with === rather than assert.equal: -Math.sign(0) is -0,
				// which strict deep equality distinguishes from 0.
				assert.ok(
					Math.sign(sqlCompare(a, b)) === -Math.sign(sqlCompare(b, a)),
					`antisymmetry failed for ${String(a)} vs ${String(b)}`,
				);
			}
		}

		// Consistency of the sorted order: every earlier element compares <= 0
		// against every later element (a transitivity spot-check). undefined is
		// excluded because Array.prototype.sort always moves undefined elements
		// to the end without consulting the comparator, so its position does not
		// reflect sqlCompare's ordering (the antisymmetry loop above still
		// exercises undefined against every other value).
		const sorted = mixedPool.filter((value) => value !== undefined).sort(sqlCompare);
		for (let i = 0; i < sorted.length; i++) {
			for (let j = i + 1; j < sorted.length; j++) {
				assert.ok(
					sqlCompare(sorted[i], sorted[j]) <= 0,
					`sorted[${i}] (${String(sorted[i])}) should be <= sorted[${j}] (${String(sorted[j])})`,
				);
			}
		}
	});
});

describe("sortBy", () => {
	interface TestRow {
		id: number;
		name: string;
		age: number | null;
		active: boolean;
	}

	it("should sort by single column ascending", () => {
		const rows: TestRow[] = [
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: 35, active: true },
		];

		const sorted = sortBy(rows, [{ key: "id", direction: "asc", nulls: "first" }]);

		assert.equal(sorted[0]!.id, 1);
		assert.equal(sorted[1]!.id, 2);
		assert.equal(sorted[2]!.id, 3);
	});

	it("should sort by single column descending", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 2, name: "Bob", age: 35, active: true },
		];

		const sorted = sortBy(rows, [{ key: "age", direction: "desc", nulls: "first" }]);

		assert.equal(sorted[0]!.age, 35);
		assert.equal(sorted[1]!.age, 30);
		assert.equal(sorted[2]!.age, 25);
	});

	it("should handle nulls first", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: null, active: false },
		];

		const sorted = sortBy(rows, [{ key: "age", direction: "asc", nulls: "first" }]);

		assert.equal(sorted[0]!.age, null);
		assert.equal(sorted[1]!.age, null);
		assert.equal(sorted[2]!.age, 25);
		assert.equal(sorted[3]!.age, 30);
	});

	it("should handle nulls last", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: null, active: false },
		];

		const sorted = sortBy(rows, [{ key: "age", direction: "asc", nulls: "last" }]);

		assert.equal(sorted[0]!.age, 25);
		assert.equal(sorted[1]!.age, 30);
		assert.equal(sorted[2]!.age, null);
		assert.equal(sorted[3]!.age, null);
	});

	it("should handle nulls last with descending order", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
		];

		const sorted = sortBy(rows, [{ key: "age", direction: "desc", nulls: "last" }]);

		assert.equal(sorted[0]!.age, 30);
		assert.equal(sorted[1]!.age, 25);
		assert.equal(sorted[2]!.age, null);
	});

	it("should sort by multiple columns", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: 25, active: true },
		];

		const sorted = sortBy(rows, [
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		]);

		// All age 25 should come first, sorted by name
		assert.equal(sorted[0]!.name, "Alice");
		assert.equal(sorted[1]!.name, "Bob");
		assert.equal(sorted[2]!.name, "David");
		// Then age 30
		assert.equal(sorted[3]!.name, "Charlie");
	});

	it("should sort by multiple columns with mixed directions", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: 25, active: true },
		];

		const sorted = sortBy(rows, [
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "desc", nulls: "first" },
		]);

		// All age 25 should come first, sorted by name descending
		assert.equal(sorted[0]!.name, "David");
		assert.equal(sorted[1]!.name, "Bob");
		assert.equal(sorted[2]!.name, "Alice");
		// Then age 30
		assert.equal(sorted[3]!.name, "Charlie");
	});

	it("should handle all nulls in both values", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: null, active: true },
			{ id: 2, name: "Bob", age: null, active: false },
			{ id: 3, name: "Charlie", age: null, active: true },
		];

		const sorted = sortBy(rows, [
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		]);

		// When age is null for all, should fall back to name sorting
		assert.equal(sorted[0]!.name, "Alice");
		assert.equal(sorted[1]!.name, "Bob");
		assert.equal(sorted[2]!.name, "Charlie");
	});

	it("should handle boolean sorting", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 30, active: false },
			{ id: 3, name: "Charlie", age: 35, active: true },
			{ id: 4, name: "David", age: 40, active: false },
		];

		const sorted = sortBy(rows, [{ key: "active", direction: "asc", nulls: "first" }]);

		// false < true
		assert.equal(sorted[0]!.active, false);
		assert.equal(sorted[1]!.active, false);
		assert.equal(sorted[2]!.active, true);
		assert.equal(sorted[3]!.active, true);
	});

	it("should keep identical rows in input order", () => {
		const row1: TestRow = { id: 1, name: "Alice", age: 25, active: true };
		const row2: TestRow = { id: 1, name: "Alice", age: 25, active: true };
		const orderings: OrderBy<TestRow>[] = [
			{ key: "id", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		];

		assert.deepEqual(sortBy([row1, row2], orderings), [row1, row2]);
		assert.deepEqual(sortBy([row2, row1], orderings), [row2, row1]);
	});

	it("should handle empty orderings array", () => {
		const rows: TestRow[] = [
			{ id: 2, name: "Bob", age: 30, active: false },
			{ id: 1, name: "Alice", age: 25, active: true },
		];

		const sorted = sortBy(rows, []);

		// Should return 0 for all comparisons, maintaining original order (stable sort)
		assert.equal(sorted[0]!.id, 2);
		assert.equal(sorted[1]!.id, 1);
	});

	it("should support ordering by computed values using functions", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "bob", age: 30, active: false },
			{ id: 3, name: "Charlie", age: 35, active: true },
		];

		// Sort by lowercase name for case-insensitive ordering
		const sorted = sortBy(rows, [
			{ key: (row) => row.name.toLowerCase(), direction: "asc", nulls: "first" },
		]);

		// Should be sorted case-insensitively: Alice, bob, Charlie
		assert.equal(sorted[0]!.name, "Alice");
		assert.equal(sorted[1]!.name, "bob");
		assert.equal(sorted[2]!.name, "Charlie");
	});

	it("should support mixing field keys and functions in orderings", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "alice", age: 30, active: false },
			{ id: 3, name: "Bob", age: 25, active: true },
		];

		const sorted = sortBy(rows, [
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: (row) => row.name.toLowerCase(), direction: "asc", nulls: "first" },
		]);

		// Age 25: Alice, Bob (sorted by lowercase name)
		assert.equal(sorted[0]!.age, 25);
		assert.equal(sorted[0]!.name, "Alice");
		assert.equal(sorted[1]!.age, 25);
		assert.equal(sorted[1]!.name, "Bob");
		// Age 30: alice
		assert.equal(sorted[2]!.age, 30);
		assert.equal(sorted[2]!.name, "alice");
	});
});

describe("sortBy key extraction", () => {
	interface Row {
		readonly id: number;
		readonly group: string | null;
		readonly score: number;
	}

	const rows: Row[] = [
		{ id: 1, group: "b", score: 2 },
		{ id: 2, group: "a", score: 1 },
		{ id: 3, group: null, score: 3 },
		{ id: 4, group: "a", score: 2 },
		{ id: 5, group: "b", score: 1 },
		{ id: 6, group: null, score: 1 },
	];

	it("should apply direction and null placement for every ordering shape", () => {
		const cases: [OrderBy<Row>[], number[]][] = [
			[[{ key: "score", direction: "asc" }], [2, 5, 6, 1, 4, 3]],
			[[{ key: "score", direction: "desc" }], [3, 1, 4, 2, 5, 6]],
			// ASC defaults to NULLS LAST, DESC to NULLS FIRST.
			[[{ key: "group", direction: "asc" }], [2, 4, 1, 5, 3, 6]],
			[[{ key: "group", direction: "asc", nulls: "first" }], [3, 6, 2, 4, 1, 5]],
			[[{ key: "group", direction: "asc", nulls: "last" }], [2, 4, 1, 5, 3, 6]],
			[[{ key: "group", direction: "desc" }], [3, 6, 1, 5, 2, 4]],
			[[{ key: "group", direction: "desc", nulls: "first" }], [3, 6, 1, 5, 2, 4]],
			[[{ key: "group", direction: "desc", nulls: "last" }], [1, 5, 2, 4, 3, 6]],
			[
				[
					{ key: "group", direction: "asc" },
					{ key: "score", direction: "desc" },
				],
				[4, 2, 1, 5, 3, 6],
			],
			[[{ key: (row: Row) => row.score * -1, direction: "asc" }], [3, 1, 4, 2, 5, 6]],
		];

		for (const [orderings, expected] of cases) {
			assert.deepEqual(
				sortBy(rows, orderings).map((row) => row.id),
				expected,
				JSON.stringify(orderings.map((o) => ({ ...o, key: String(o.key) }))),
			);
		}
	});

	it("should be stable for rows that compare equal", () => {
		// Sorting an index array rather than the rows themselves loses
		// Array.sort's stability guarantee unless it is restored explicitly.
		const ties = Array.from({ length: 50 }, (_, i) => ({ id: i, group: "same", score: 0 }));
		const sorted = sortBy(ties, [{ key: "group", direction: "asc" }]);
		assert.deepEqual(
			sorted.map((row) => row.id),
			ties.map((row) => row.id),
		);

		// Stability must also hold when an earlier column breaks some ties but
		// not others.
		const partial = [
			{ id: 1, group: "b", score: 0 },
			{ id: 2, group: "a", score: 0 },
			{ id: 3, group: "b", score: 0 },
			{ id: 4, group: "a", score: 0 },
		];
		assert.deepEqual(
			sortBy(partial, [{ key: "group", direction: "asc" }]).map((row) => row.id),
			[2, 4, 1, 3],
		);
	});

	it("should not mutate the input array", () => {
		const original = [...rows];
		const sorted = sortBy(rows, [{ key: "score", direction: "desc" }]);
		assert.deepEqual(rows, original);
		assert.notEqual(sorted, rows);
	});

	it("should handle empty orderings and trivial inputs", () => {
		assert.deepEqual(sortBy(rows, []), rows);
		assert.notEqual(sortBy(rows, []), rows);
		assert.deepEqual(sortBy([], [{ key: "score", direction: "asc" }]), []);
		assert.deepEqual(sortBy([rows[0]!], [{ key: "score", direction: "asc" }]), [rows[0]]);
	});

	it("should extract each row's key exactly once per ordering", () => {
		// The reason sortBy exists: the hydrator's getValue allocates a Proxy
		// per extraction, so O(n log n) extractions is the dominant cost.
		let extractions = 0;
		const many = Array.from({ length: 500 }, (_, i) => ({
			id: i,
			group: "g",
			score: (i * 7) % 500,
		}));
		sortBy(many, [{ key: "score", direction: "asc" }], (row, key) => {
			extractions++;
			return (row as any)[key as string];
		});
		assert.equal(extractions, many.length);
	});

	it("should apply a custom getValue to function keys", () => {
		const sorted = sortBy(rows, [{ key: (row: Row) => row.score, direction: "asc" }], (row, key) =>
			typeof key === "function" ? key({ ...row, score: -row.score }) : (row as any)[key],
		);
		assert.deepEqual(
			sorted.map((row) => row.score),
			[3, 2, 2, 1, 1, 1],
		);
	});
});
