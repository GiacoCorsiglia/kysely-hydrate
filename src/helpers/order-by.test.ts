import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { sqlCompare, makeOrderByComparator } from "./order-by.ts";

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
		// Check sign rather than exact value due to floating point precision
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
		// Cross-type pairs resolve by type rank (boolean < numeric < Date <
		// string < other), never by stringification — comparing 10 vs "10" as
		// equal while 2 vs "10" compares lexicographically breaks transitivity
		// (2 < 10 numerically but "10" < "2" lexicographically), making sort
		// output depend on input order.
		assert.ok(sqlCompare(1, "1") < 0);
		assert.ok(sqlCompare("1", 1) > 0);
		assert.ok(sqlCompare(1, "2") < 0);
		assert.ok(sqlCompare("2", 1) > 0);
		assert.ok(sqlCompare(true, 0) < 0);
		assert.ok(sqlCompare(new Date(0), "a") < 0);
		assert.ok(sqlCompare(new Date(0), 1) > 0);
		assert.ok(sqlCompare("a", {}) < 0);

		// Objects fall back to comparing String() forms.
		assert.ok(sqlCompare({}, []) !== 0);

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
		"10",
		"2",
		"apple",
		{},
		[1, 2],
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

describe("makeOrderByComparator", () => {
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

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "id", direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		assert.equal(rows[0]!.id, 1);
		assert.equal(rows[1]!.id, 2);
		assert.equal(rows[2]!.id, 3);
	});

	it("should sort by single column descending", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 2, name: "Bob", age: 35, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "desc", nulls: "first" },
		]);

		rows.sort(comparator);

		assert.equal(rows[0]!.age, 35);
		assert.equal(rows[1]!.age, 30);
		assert.equal(rows[2]!.age, 25);
	});

	it("should handle nulls first", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: null, active: false },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		assert.equal(rows[0]!.age, null);
		assert.equal(rows[1]!.age, null);
		assert.equal(rows[2]!.age, 25);
		assert.equal(rows[3]!.age, 30);
	});

	it("should handle nulls last", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: null, active: false },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "last" },
		]);

		rows.sort(comparator);

		assert.equal(rows[0]!.age, 25);
		assert.equal(rows[1]!.age, 30);
		assert.equal(rows[2]!.age, null);
		assert.equal(rows[3]!.age, null);
	});

	it("should handle nulls last with descending order", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: false },
			{ id: 2, name: "Bob", age: null, active: true },
			{ id: 3, name: "Charlie", age: 30, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "desc", nulls: "last" },
		]);

		rows.sort(comparator);

		assert.equal(rows[0]!.age, 30);
		assert.equal(rows[1]!.age, 25);
		assert.equal(rows[2]!.age, null);
	});

	it("should sort by multiple columns", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: 25, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		// All age 25 should come first, sorted by name
		assert.equal(rows[0]!.name, "Alice");
		assert.equal(rows[1]!.name, "Bob");
		assert.equal(rows[2]!.name, "David");
		// Then age 30
		assert.equal(rows[3]!.name, "Charlie");
	});

	it("should sort by multiple columns with mixed directions", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 25, active: false },
			{ id: 3, name: "Charlie", age: 30, active: true },
			{ id: 4, name: "David", age: 25, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "desc", nulls: "first" },
		]);

		rows.sort(comparator);

		// All age 25 should come first, sorted by name descending
		assert.equal(rows[0]!.name, "David");
		assert.equal(rows[1]!.name, "Bob");
		assert.equal(rows[2]!.name, "Alice");
		// Then age 30
		assert.equal(rows[3]!.name, "Charlie");
	});

	it("should handle all nulls in both values", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: null, active: true },
			{ id: 2, name: "Bob", age: null, active: false },
			{ id: 3, name: "Charlie", age: null, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		// When age is null for all, should fall back to name sorting
		assert.equal(rows[0]!.name, "Alice");
		assert.equal(rows[1]!.name, "Bob");
		assert.equal(rows[2]!.name, "Charlie");
	});

	it("should handle boolean sorting", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "Bob", age: 30, active: false },
			{ id: 3, name: "Charlie", age: 35, active: true },
			{ id: 4, name: "David", age: 40, active: false },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "active", direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		// false < true
		assert.equal(rows[0]!.active, false);
		assert.equal(rows[1]!.active, false);
		assert.equal(rows[2]!.active, true);
		assert.equal(rows[3]!.active, true);
	});

	it("should return 0 for identical rows", () => {
		const row1: TestRow = { id: 1, name: "Alice", age: 25, active: true };
		const row2: TestRow = { id: 1, name: "Alice", age: 25, active: true };

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "id", direction: "asc", nulls: "first" },
			{ key: "name", direction: "asc", nulls: "first" },
		]);

		assert.equal(comparator(row1, row2), 0);
	});

	it("should handle empty orderings array", () => {
		const rows: TestRow[] = [
			{ id: 2, name: "Bob", age: 30, active: false },
			{ id: 1, name: "Alice", age: 25, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([]);

		rows.sort(comparator);

		// Should return 0 for all comparisons, maintaining original order (stable sort)
		assert.equal(rows[0]!.id, 2);
		assert.equal(rows[1]!.id, 1);
	});

	it("should support ordering by computed values using functions", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "bob", age: 30, active: false },
			{ id: 3, name: "Charlie", age: 35, active: true },
		];

		// Sort by lowercase name for case-insensitive ordering
		const comparator = makeOrderByComparator<TestRow>([
			{ key: (row) => row.name.toLowerCase(), direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		// Should be sorted case-insensitively: Alice, bob, Charlie
		assert.equal(rows[0]!.name, "Alice");
		assert.equal(rows[1]!.name, "bob");
		assert.equal(rows[2]!.name, "Charlie");
	});

	it("should support mixing field keys and functions in orderings", () => {
		const rows: TestRow[] = [
			{ id: 1, name: "Alice", age: 25, active: true },
			{ id: 2, name: "alice", age: 30, active: false },
			{ id: 3, name: "Bob", age: 25, active: true },
		];

		const comparator = makeOrderByComparator<TestRow>([
			{ key: "age", direction: "asc", nulls: "first" },
			{ key: (row) => row.name.toLowerCase(), direction: "asc", nulls: "first" },
		]);

		rows.sort(comparator);

		// Age 25: Alice, Bob (sorted by lowercase name)
		assert.equal(rows[0]!.age, 25);
		assert.equal(rows[0]!.name, "Alice");
		assert.equal(rows[1]!.age, 25);
		assert.equal(rows[1]!.name, "Bob");
		// Age 30: alice
		assert.equal(rows[2]!.age, 30);
		assert.equal(rows[2]!.name, "alice");
	});
});
