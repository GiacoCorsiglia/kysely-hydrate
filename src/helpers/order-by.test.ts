import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
	ComparedToDecimalStub,
	DecimalStub,
	DurationStub,
	PlainDateStub,
	PlainMonthDayStub,
	PlainTimeStub,
} from "./order-by.stubs.ts";
import { makeOrderByComparator, type OrderBy, sortBy, sqlCompare } from "./order-by.ts";

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

	// Values that are all mutually distinguishable: no two compare equal. The
	// strict permutation check below relies on that, since tied elements may
	// legitimately land in either order.
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
		[1, 2, 3],
		[],
		Buffer.from([1, 2]),
		Buffer.from([1, 2, 3]),
		new Uint8Array([0]),
		new Int8Array([-1]),
		new Float64Array([1.5]),
		new Float64Array([Number.NaN]),
		new DecimalStub(2.5),
		new DecimalStub(11),
		new PlainDateStub("2024-01-01"),
		new PlainDateStub("2020-06-15"),
		new PlainTimeStub("10:00:00"),
		new PlainMonthDayStub("01-01"),
		new DurationStub("PT2H"),
		new DurationStub("P1M"),
		// P10D/P9D order natively while P5Y cannot be ordered at all, and the
		// string forms contradict the numeric ones -- the shape that exposes a
		// type mixing two orderings.
		new DurationStub("P10D"),
		new DurationStub("P9D"),
		new DurationStub("P5Y"),
	];

	// Values that deliberately tie with a member of mixedPool. Each tie is
	// correct behavior, asserted individually elsewhere: a decimal equals the
	// plain number of the same value, two unorderable values compare equal, and
	// distinct objects sharing a String() form must compare equal.
	const tiedPool = [
		new DecimalStub(2),
		new DecimalStub(Number.NaN),
		new ComparedToDecimalStub(2.5),
		// A decimal that throws on every comparison, as one library's instance
		// does when handed another's.
		new DecimalStub(7, undefined, true),
		Object.create(null),
	];

	const fullPool = [...mixedPool, ...tiedPool];

	// String() throws on pool members that cannot be coerced (null-prototype
	// objects), so failure messages go through a safe label.
	const label = (value: unknown): string => {
		try {
			return String(value);
		} catch {
			return Object.prototype.toString.call(value);
		}
	};

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

	it("should be transitive across every triple in the pool", () => {
		// Antisymmetry and a sorted-order spot-check both pass on comparators
		// that are still intransitive, which is the failure mode that makes
		// sort output depend on input order. This checks it directly.
		const pool = fullPool.filter((value) => value !== undefined);
		const failures: string[] = [];

		for (const a of pool) {
			for (const b of pool) {
				if (sqlCompare(a, b) > 0) {
					continue;
				}
				for (const c of pool) {
					if (sqlCompare(b, c) > 0) {
						continue;
					}
					// a <= b and b <= c, so a <= c must hold.
					if (sqlCompare(a, c) > 0) {
						failures.push(
							`${label(a)} <= ${label(b)} <= ${label(c)}, but ${label(a)} > ${label(c)}`,
						);
					}
				}
			}
		}

		assert.deepEqual(failures, []);
	});

	it("should sort consistently from any permutation, including tied values", () => {
		// The same property over a pool that contains ties. Identity-level
		// equality cannot be asserted here -- tied elements may appear in either
		// order -- but every result must still be non-decreasing, which is what
		// "the order does not depend on the input" means once ties exist.
		const withoutUndefined = fullPool.filter((value) => value !== undefined);
		const evens = withoutUndefined.filter((_, i) => i % 2 === 0);
		const odds = withoutUndefined.filter((_, i) => i % 2 === 1);
		const permutations = [
			[...withoutUndefined],
			[...withoutUndefined].reverse(),
			[...withoutUndefined.slice(7), ...withoutUndefined.slice(0, 7)],
			[...evens, ...odds],
			[...odds, ...evens].reverse(),
		];

		for (const permutation of permutations) {
			const sorted = [...permutation].sort(sqlCompare);
			for (let i = 1; i < sorted.length; i++) {
				assert.ok(
					sqlCompare(sorted[i - 1], sorted[i]) <= 0,
					`out of order at ${i}: ${label(sorted[i - 1])} then ${label(sorted[i])}`,
				);
			}
		}
	});

	it("should satisfy the comparator contract on all mixed-pool pairs", () => {
		// Antisymmetry: sign(cmp(a, b)) === -sign(cmp(b, a)) for every pair.
		for (const a of fullPool) {
			for (const b of fullPool) {
				// Compare with === rather than assert.equal: -Math.sign(0) is -0,
				// which strict deep equality distinguishes from 0.
				assert.ok(
					Math.sign(sqlCompare(a, b)) === -Math.sign(sqlCompare(b, a)),
					`antisymmetry failed for ${label(a)} vs ${label(b)}`,
				);
			}
		}

		// Consistency of the sorted order: every earlier element compares <= 0
		// against every later element (a transitivity spot-check). undefined is
		// excluded because Array.prototype.sort always moves undefined elements
		// to the end without consulting the comparator, so its position does not
		// reflect sqlCompare's ordering (the antisymmetry loop above still
		// exercises undefined against every other value).
		const sorted = fullPool.filter((value) => value !== undefined).sort(sqlCompare);
		for (let i = 0; i < sorted.length; i++) {
			for (let j = i + 1; j < sorted.length; j++) {
				assert.ok(
					sqlCompare(sorted[i], sorted[j]) <= 0,
					`sorted[${i}] (${label(sorted[i])}) should be <= sorted[${j}] (${label(sorted[j])})`,
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

describe("sqlCompare duck-typed types", () => {
	it("should order binary data byte-wise, shorter prefix first", () => {
		assert.ok(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 3])) < 0);
		assert.ok(sqlCompare(Buffer.from([1, 3]), Buffer.from([1, 2])) > 0);
		assert.equal(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 2])), 0);
		// A prefix sorts before the longer value it prefixes.
		assert.ok(sqlCompare(Buffer.from([1, 2]), Buffer.from([1, 2, 0])) < 0);
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
		// Elements of any supported type, including nulls.
		assert.ok(sqlCompare([null, 1], [1, 1]) < 0);
	});

	it("should order decimal instances within the numeric rank", () => {
		assert.ok(sqlCompare(new DecimalStub(2), new DecimalStub(10)) < 0);
		assert.ok(sqlCompare(new DecimalStub(10), new DecimalStub(2)) > 0);
		assert.equal(sqlCompare(new DecimalStub(5), new DecimalStub(5)), 0);

		// Mixed with plain numbers: numeric, not rank-separated. This is the
		// case a driver hits when some rows arrive as decimals and some as
		// numbers.
		assert.ok(sqlCompare(new DecimalStub(2), 10) < 0);
		assert.ok(sqlCompare(10, new DecimalStub(2)) > 0);
		assert.equal(sqlCompare(new DecimalStub(5), 5), 0);
		assert.ok(sqlCompare(new DecimalStub(2), 3n) < 0);

		// Decimals sort before strings, like other numerics.
		assert.ok(sqlCompare(new DecimalStub(2), "1") < 0);
	});

	it("should support decimal libraries exposing only comparedTo", () => {
		// bignumber.js exposes comparedTo but not cmp.
		const a = new ComparedToDecimalStub(2);
		const b = new ComparedToDecimalStub(10);
		assert.ok(sqlCompare(a, b) < 0);
		assert.ok(sqlCompare(b, a) > 0);
		assert.ok(sqlCompare(a, 10) < 0);
	});

	it("should pin decimal NaN after real numerics, however it is reported", () => {
		// decimal.js returns NaN from cmp; bignumber.js returns null. Both are
		// unorderable and neither may be treated as "equal".
		for (const unorderable of [Number.NaN, null] as const) {
			const nan = new DecimalStub(Number.NaN, unorderable);
			assert.ok(sqlCompare(nan, new DecimalStub(3)) > 0, `${String(unorderable)} vs decimal`);
			assert.ok(sqlCompare(new DecimalStub(3), nan) < 0, `decimal vs ${String(unorderable)}`);
			assert.ok(sqlCompare(nan, 3) > 0, `${String(unorderable)} vs number`);
			assert.ok(sqlCompare(3, nan) < 0, `number vs ${String(unorderable)}`);
			assert.equal(sqlCompare(nan, nan), 0);
		}
	});

	it("should fall back rather than throw when a decimal rejects an operand", () => {
		const throwing = new DecimalStub(1, undefined, true);
		assert.doesNotThrow(() => sqlCompare(throwing, new DecimalStub(2)));
		assert.ok(
			Math.sign(sqlCompare(throwing, new DecimalStub(2))) ===
				-Math.sign(sqlCompare(new DecimalStub(2), throwing)),
		);
	});

	it("should order Temporal values via their type's static compare", () => {
		const early = new PlainDateStub("2020-06-15");
		const late = new PlainDateStub("2024-01-01");
		assert.ok(sqlCompare(early, late) < 0);
		assert.ok(sqlCompare(late, early) > 0);
		assert.equal(sqlCompare(new PlainDateStub("2024-01-01"), new PlainDateStub("2024-01-01")), 0);
	});

	it("should never coerce a Temporal value with valueOf", () => {
		// Temporal deliberately throws from valueOf to block `a < b`. Any code
		// path that coerced instead of using compare would surface here.
		const a = new PlainDateStub("2020-06-15");
		const b = new PlainDateStub("2024-01-01");
		assert.throws(() => a.valueOf());
		assert.doesNotThrow(() => sqlCompare(a, b));
		assert.doesNotThrow(() => sqlCompare(a, 1));
		assert.doesNotThrow(() => sqlCompare(a, "x"));
		assert.doesNotThrow(() => [b, a].sort(sqlCompare));
	});

	it("should separate distinct Temporal types by a stable ordering", () => {
		// PlainDate.compare throws when handed a PlainTime, so unlike Temporal
		// types must never reach it. They are ordered by tag instead.
		const date = new PlainDateStub("2024-01-01");
		const time = new PlainTimeStub("10:00:00");
		assert.doesNotThrow(() => sqlCompare(date, time));
		assert.notEqual(sqlCompare(date, time), 0);
		assert.ok(Math.sign(sqlCompare(date, time)) === -Math.sign(sqlCompare(time, date)));
	});

	it("should handle Temporal types without a usable compare", () => {
		// PlainMonthDay has no static compare at all.
		const a = new PlainMonthDayStub("01-01");
		const b = new PlainMonthDayStub("06-15");
		assert.doesNotThrow(() => sqlCompare(a, b));
		assert.ok(sqlCompare(a, b) < 0);
		assert.equal(sqlCompare(a, a), 0);

		// Duration.compare throws for calendar-ambiguous units, but works for
		// plain time units.
		assert.ok(sqlCompare(new DurationStub("PT90M"), new DurationStub("PT2H")) < 0);
		assert.doesNotThrow(() => sqlCompare(new DurationStub("P1M"), new DurationStub("P30D")));
		assert.ok(
			Math.sign(sqlCompare(new DurationStub("P1M"), new DurationStub("P30D"))) ===
				-Math.sign(sqlCompare(new DurationStub("P30D"), new DurationStub("P1M"))),
		);
	});

	it("should handle objects with no prototype", () => {
		const a = Object.create(null);
		const b = Object.create(null);
		assert.doesNotThrow(() => sqlCompare(a, b));
		assert.equal(sqlCompare(a, a), 0);
		assert.ok(Math.sign(sqlCompare(a, 1)) === -Math.sign(sqlCompare(1, a)));
	});

	it("should keep unlike types apart rather than stringifying them together", () => {
		// Buffer, array, and Temporal each get their own rank, so a column
		// mixing them groups by type instead of by String() form.
		assert.notEqual(sqlCompare(Buffer.from([1, 2]), [1, 2]), 0);
		assert.notEqual(sqlCompare([1, 2], new PlainDateStub("2024-01-01")), 0);
		assert.ok(sqlCompare("z", Buffer.from([1])) < 0);
	});
});

describe("sortBy", () => {
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

	it("should match makeOrderByComparator for every ordering shape", () => {
		const orderingSets: OrderBy<Row>[][] = [
			[{ key: "score", direction: "asc" }],
			[{ key: "score", direction: "desc" }],
			[{ key: "group", direction: "asc" }],
			[{ key: "group", direction: "asc", nulls: "first" }],
			[{ key: "group", direction: "asc", nulls: "last" }],
			[{ key: "group", direction: "desc", nulls: "first" }],
			[{ key: "group", direction: "desc", nulls: "last" }],
			[
				{ key: "group", direction: "asc" },
				{ key: "score", direction: "desc" },
			],
			[{ key: (row: Row) => row.score * -1, direction: "asc" }],
		];

		for (const orderings of orderingSets) {
			assert.deepEqual(
				sortBy(rows, orderings),
				[...rows].sort(makeOrderByComparator(orderings)),
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

describe("sqlCompare total-order edge cases", () => {
	it("should order float typed arrays without letting NaN escape", () => {
		// Raw < / > on elements would silently break antisymmetry here, since
		// every comparison against NaN is false.
		const nan = new Float64Array([Number.NaN]);
		const one = new Float64Array([1]);
		assert.ok(sqlCompare(one, nan) < 0);
		assert.ok(sqlCompare(nan, one) > 0);
		assert.equal(sqlCompare(nan, new Float64Array([Number.NaN])), 0);
		// A NaN in a shared position must not stop the comparison there.
		assert.ok(sqlCompare(new Float64Array([Number.NaN, 1]), new Float64Array([Number.NaN, 2])) < 0);
		assert.ok(sqlCompare(new Float32Array([1]), new Float32Array([2])) < 0);
	});

	it("should read typed array elements with their own signedness", () => {
		assert.ok(sqlCompare(new Int8Array([-1]), new Int8Array([1])) < 0);
		assert.ok(sqlCompare(new Uint8Array([255]), new Uint8Array([1])) > 0);
		assert.ok(sqlCompare(new BigInt64Array([1n]), new BigInt64Array([2n])) < 0);
	});

	it("should stay consistent when a decimal cannot compare its operand", () => {
		// One library's instance handed another's throws. The throwing and
		// non-throwing directions must still agree, and must not contradict
		// plain numbers on the same rank.
		const throwing = new DecimalStub(7, undefined, true);
		const nan = new DecimalStub(Number.NaN);

		assert.ok(Math.sign(sqlCompare(throwing, nan)) === -Math.sign(sqlCompare(nan, throwing)));
		assert.ok(sqlCompare(throwing, nan) < 0, "a real value sorts before an unorderable one");

		// Transitivity against a plain number: 2 < 7 < NaN.
		assert.ok(sqlCompare(2, throwing) < 0);
		assert.ok(sqlCompare(throwing, nan) < 0);
		assert.ok(sqlCompare(2, nan) < 0);
	});

	it("should keep a partly-orderable Temporal type transitive", () => {
		// Duration.compare works for exact time units and throws once years,
		// months, or weeks appear. Ordering the throwing ones by string form
		// alongside natively-ordered ones would be intransitive.
		const tenDays = new DurationStub("P10D");
		const nineDays = new DurationStub("P9D");
		const fiveYears = new DurationStub("P5Y");

		assert.ok(sqlCompare(tenDays, nineDays) > 0, "natively ordered");
		// The unorderable value sorts after both, rather than landing between
		// them by string form ("P10D" < "P5Y" < "P9D").
		assert.ok(sqlCompare(tenDays, fiveYears) < 0);
		assert.ok(sqlCompare(nineDays, fiveYears) < 0);

		// Sort output must not depend on input order.
		const expected = ["P9D", "P10D", "P5Y"];
		for (const permutation of [
			[tenDays, nineDays, fiveYears],
			[fiveYears, tenDays, nineDays],
			[nineDays, fiveYears, tenDays],
		]) {
			assert.deepEqual([...permutation].sort(sqlCompare).map(String), expected);
		}
	});

	it("should distinguish two implementations of the same logical type", () => {
		// A native Temporal value and a polyfilled one produce separate
		// handlers sharing a tag. Calling every cross-implementation pair equal
		// would be intransitive against each implementation's own ordering.
		class OtherPlainDateStub extends PlainDateStub {}
		const nativeEarly = new PlainDateStub("2020-06-15");
		const polyfillLate = new OtherPlainDateStub("2024-01-01");

		assert.ok(sqlCompare(nativeEarly, polyfillLate) < 0);
		assert.ok(sqlCompare(polyfillLate, nativeEarly) > 0);
	});

	it("should not recurse without bound on self-referential arrays", () => {
		const a: unknown[] = [];
		a.push(a);
		const b: unknown[] = [];
		b.push(b);
		assert.doesNotThrow(() => sqlCompare(a, b));
		assert.ok(Math.sign(sqlCompare(a, b)) === -Math.sign(sqlCompare(b, a)));

		const deep = (depth: number): unknown[] => {
			let node: unknown[] = [];
			for (let i = 0; i < depth; i++) {
				node = [node];
			}
			return node;
		};
		assert.doesNotThrow(() => sqlCompare(deep(20_000), deep(20_000)));
	});

	it("should classify by prototype, not by whichever instance arrives first", () => {
		// Handlers are memoized per prototype, so reading instance-level shape
		// would let one value decide the handler for every value sharing its
		// prototype. A plain object with an own cmp is not a decimal.
		const ownCmp = { cmp: () => -1, toString: () => "own" };
		assert.ok(Math.sign(sqlCompare(ownCmp, 1)) === -Math.sign(sqlCompare(1, ownCmp)));
		assert.ok(sqlCompare(ownCmp, 1) > 0, "not treated as numeric");
	});

	it("should handle primitives with no natural ordering", () => {
		const a = Symbol("a");
		const b = Symbol("b");
		assert.doesNotThrow(() => sqlCompare(a, b));
		assert.equal(sqlCompare(a, a), 0);
		assert.ok(Math.sign(sqlCompare(a, b)) === -Math.sign(sqlCompare(b, a)));

		const fn = () => 1;
		assert.equal(sqlCompare(fn, fn), 0);
		assert.ok(Math.sign(sqlCompare(fn, a)) === -Math.sign(sqlCompare(a, fn)));
	});

	it("should tolerate a value whose constructor access throws", () => {
		const hostile = {
			get constructor() {
				throw new Error("nope");
			},
			toString: () => "hostile",
		};
		assert.doesNotThrow(() => sqlCompare(hostile, 1));
	});
});
