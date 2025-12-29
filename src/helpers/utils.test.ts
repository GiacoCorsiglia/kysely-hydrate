import assert from "node:assert/strict";
import { test } from "node:test";

import { UnexpectedCaseError } from "./errors.ts";
import { addObjectToMap, assertNever, isIterable } from "./utils.ts";

// assertNever tests
test("assertNever: throws UnexpectedCaseError", () => {
	assert.throws(
		() => assertNever("unexpected" as never),
		UnexpectedCaseError,
		"Should throw UnexpectedCaseError for unexpected cases",
	);
});

test("assertNever: includes value in error message", () => {
	assert.throws(
		() => assertNever({ type: "unknown" } as never),
		(error: Error) => {
			assert.ok(error instanceof UnexpectedCaseError);
			assert.ok(error.message.includes("unknown"));
			return true;
		},
	);
});

// isIterable tests
test("isIterable: returns true for arrays", () => {
	assert.strictEqual(isIterable([1, 2, 3]), true);
});

test("isIterable: returns false for strings", () => {
	assert.strictEqual(isIterable("test"), false);
});

test("isIterable: returns true for Map", () => {
	assert.strictEqual(isIterable(new Map()), true);
});

test("isIterable: returns true for Set", () => {
	assert.strictEqual(isIterable(new Set()), true);
});

test("isIterable: returns false for null", () => {
	assert.strictEqual(isIterable(null), false);
});

test("isIterable: returns false for undefined", () => {
	assert.strictEqual(isIterable(undefined), false);
});

test("isIterable: returns false for number", () => {
	assert.strictEqual(isIterable(42), false);
});

test("isIterable: returns false for plain object", () => {
	assert.strictEqual(isIterable({ key: "value" }), false);
});

// addObjectToMap tests
test("addObjectToMap: creates new Map from undefined", () => {
	const result = addObjectToMap(undefined, { a: 1, b: 2 });
	assert.deepStrictEqual(result, new Map([["a", 1], ["b", 2]]));
});

test("addObjectToMap: clones existing Map", () => {
	const original = new Map([["x", 10]]);
	const result = addObjectToMap(original, { a: 1 });

	assert.deepStrictEqual(result, new Map([["x", 10], ["a", 1]]));
	assert.notStrictEqual(result, original);
	assert.strictEqual(original.size, 1);
});

test("addObjectToMap: skips undefined values", () => {
	const result = addObjectToMap(undefined, { a: 1, b: undefined, c: 3 });
	assert.deepStrictEqual(result, new Map([["a", 1], ["c", 3]]));
});

test("addObjectToMap: overwrites existing keys", () => {
	const original = new Map([["a", 1], ["b", 2]]);
	const result = addObjectToMap(original, { a: 999 });

	assert.deepStrictEqual(result, new Map([["a", 999], ["b", 2]]));
	assert.strictEqual(original.get("a"), 1);
});

test("addObjectToMap: handles empty object", () => {
	const original = new Map([["a", 1]]);
	const result = addObjectToMap(original, {});

	assert.deepStrictEqual(result, new Map([["a", 1]]));
	assert.notStrictEqual(result, original);
});
