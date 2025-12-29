import assert from "node:assert/strict";
import { test } from "node:test";

import { InvalidInstanceError, UnexpectedCaseError } from "./errors.ts";
import { addObjectToMap, assertNever, createPrivateAccessor, isIterable } from "./utils.ts";

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

// createPrivateAccessor tests
test("createPrivateAccessor: basic register and call", () => {
	class TestClass {
		value = 42;
		#privateMethod() {
			return this.value;
		}
		getAccessor() {
			return this.#privateMethod.bind(this);
		}
	}

	const accessor = createPrivateAccessor<TestClass, [], number>();
	const instance = new TestClass();

	accessor.register(instance, instance.getAccessor());
	const result = accessor.call(instance);

	assert.strictEqual(result, 42);
});

test("createPrivateAccessor: throws when calling unregistered instance", () => {
	class TestClass {}

	const accessor = createPrivateAccessor<TestClass, [], void>();
	const instance = new TestClass();

	assert.throws(() => accessor.call(instance), InvalidInstanceError);
});

test("createPrivateAccessor: passes arguments correctly", () => {
	class TestClass {
		#privateMethod(a: string, b: number) {
			return `${a}-${b}`;
		}
		getAccessor() {
			return this.#privateMethod.bind(this);
		}
	}

	const accessor = createPrivateAccessor<TestClass, [a: string, b: number], string>();
	const instance = new TestClass();

	accessor.register(instance, instance.getAccessor());
	const result = accessor.call(instance, "test", 123);

	assert.strictEqual(result, "test-123");
});

test("createPrivateAccessor: isolates instances", () => {
	class TestClass {
		value: number;
		constructor(value: number) {
			this.value = value;
		}
		#privateMethod() {
			return this.value;
		}
		getAccessor() {
			return this.#privateMethod.bind(this);
		}
	}

	const accessor = createPrivateAccessor<TestClass, [], number>();
	const instance1 = new TestClass(100);
	const instance2 = new TestClass(200);

	accessor.register(instance1, instance1.getAccessor());
	accessor.register(instance2, instance2.getAccessor());

	assert.strictEqual(accessor.call(instance1), 100);
	assert.strictEqual(accessor.call(instance2), 200);
});

test("createPrivateAccessor: overwrites previous registration", () => {
	class TestClass {
		value = 42;
		#privateMethod() {
			return this.value;
		}
		getAccessor() {
			return this.#privateMethod.bind(this);
		}
	}

	const accessor = createPrivateAccessor<TestClass, [], number>();
	const instance = new TestClass();

	// First registration
	accessor.register(instance, instance.getAccessor());
	assert.strictEqual(accessor.call(instance), 42);

	// Update value and re-register
	instance.value = 100;
	accessor.register(instance, instance.getAccessor());
	assert.strictEqual(accessor.call(instance), 100);
});

test("createPrivateAccessor: multiple accessors are independent", () => {
	class TestClass {
		#method1() {
			return "method1";
		}
		#method2() {
			return "method2";
		}
		getAccessor1() {
			return this.#method1.bind(this);
		}
		getAccessor2() {
			return this.#method2.bind(this);
		}
	}

	const accessor1 = createPrivateAccessor<TestClass, [], string>();
	const accessor2 = createPrivateAccessor<TestClass, [], string>();
	const instance = new TestClass();

	accessor1.register(instance, instance.getAccessor1());
	accessor2.register(instance, instance.getAccessor2());

	assert.strictEqual(accessor1.call(instance), "method1");
	assert.strictEqual(accessor2.call(instance), "method2");
});
