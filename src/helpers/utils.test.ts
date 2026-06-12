import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { UnexpectedCaseError } from "./errors.ts";
import {
	addObjectToMap,
	assertNever,
	isIterable,
	isSelectQueryBuilder,
	mapWithDeleted,
} from "./utils.ts";

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
	// Intentional: strings ARE iterable in JS, but treating a string as an
	// iterable of characters is never what hydrate() callers mean — a single
	// string input must be handled as one value, not exploded. Don't "fix"
	// this to true.
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
	assert.deepStrictEqual(
		result,
		new Map([
			["a", 1],
			["b", 2],
		]),
	);
});

test("addObjectToMap: clones existing Map", () => {
	const original = new Map([["x", 10]]);
	const result = addObjectToMap(original, { a: 1 });

	assert.deepStrictEqual(
		result,
		new Map([
			["x", 10],
			["a", 1],
		]),
	);
	assert.notStrictEqual(result, original);
	assert.strictEqual(original.size, 1);
});

test("addObjectToMap: skips undefined values", () => {
	const result = addObjectToMap(undefined, { a: 1, b: undefined, c: 3 });
	assert.deepStrictEqual(
		result,
		new Map([
			["a", 1],
			["c", 3],
		]),
	);
});

test("addObjectToMap: overwrites existing keys", () => {
	const original = new Map([
		["a", 1],
		["b", 2],
	]);
	const result = addObjectToMap(original, { a: 999 });

	assert.deepStrictEqual(
		result,
		new Map([
			["a", 999],
			["b", 2],
		]),
	);
	assert.strictEqual(original.get("a"), 1);
});

test("addObjectToMap: handles empty object", () => {
	const original = new Map([["a", 1]]);
	const result = addObjectToMap(original, {});

	assert.deepStrictEqual(result, new Map([["a", 1]]));
	assert.notStrictEqual(result, original);
});

// mapWithDeleted tests
test("mapWithDeleted: returns a clone with the key removed", () => {
	const original = new Map([
		["a", 1],
		["b", 2],
	]);
	const result = mapWithDeleted(original, "a");

	assert.deepStrictEqual(result, new Map([["b", 2]]));
	// The original is untouched.
	assert.deepStrictEqual(
		original,
		new Map([
			["a", 1],
			["b", 2],
		]),
	);
});

test("mapWithDeleted: returns the original map instance when the key is absent", () => {
	// Documented contract: no needless clone. Callers rely on cheap no-ops
	// when overriding collections that don't exist yet.
	const original = new Map([["a", 1]]);
	const result = mapWithDeleted(original, "missing");

	assert.strictEqual(result, original);
});

// isSelectQueryBuilder tests
test("isSelectQueryBuilder: true for select query builders only", () => {
	const db = new k.Kysely<{ users: { id: number } }>({
		dialect: {
			createAdapter: () => new k.SqliteAdapter(),
			createDriver: () => new k.DummyDriver(),
			createIntrospector: (innerDb) => new k.SqliteIntrospector(innerDb),
			createQueryCompiler: () => new k.SqliteQueryCompiler(),
		},
	});

	assert.strictEqual(isSelectQueryBuilder(db.selectFrom("users").select("id")), true);
	assert.strictEqual(isSelectQueryBuilder(db.insertInto("users").values({ id: 1 })), false);
	assert.strictEqual(isSelectQueryBuilder(db.updateTable("users").set({ id: 1 })), false);
	assert.strictEqual(isSelectQueryBuilder(db.deleteFrom("users")), false);
});

test("isSelectQueryBuilder: false for non-builders", () => {
	assert.strictEqual(isSelectQueryBuilder(null), false);
	assert.strictEqual(isSelectQueryBuilder(undefined), false);
	assert.strictEqual(isSelectQueryBuilder("select"), false);
	assert.strictEqual(isSelectQueryBuilder({}), false);
});

test("isSelectQueryBuilder: duck-types on the isSelectQueryBuilder property", () => {
	// Intentional: the check is `"isSelectQueryBuilder" in o` (Kysely's own
	// marker property), not an instanceof — any object carrying the marker
	// passes. Pinned so a future "fix" doesn't silently change the contract.
	assert.strictEqual(isSelectQueryBuilder({ isSelectQueryBuilder: false }), true);
});
