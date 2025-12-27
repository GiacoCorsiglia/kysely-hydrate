import assert from "node:assert/strict";
import { test } from "node:test";

import { sql } from "kysely";

import { getMappedNodes, map } from "./mapped-expression.ts";

test("map returns MappedExpression instance", () => {
	const mapFn = (x: number) => x.toString();
	const expr = sql<number>`1`;
	const mapped = map(expr, mapFn);

	assert.ok(mapped);
	assert.strictEqual(typeof mapped.as, "function");
	assert.strictEqual(typeof mapped.toOperationNode, "function");
});

test("map().as() returns AliasedExpression", () => {
	const mapFn = (x: number) => x.toString();
	const expr = sql<number>`1`;
	const aliased = map(expr, mapFn).as("test_alias");

	assert.ok(aliased);
	assert.strictEqual(typeof aliased.toOperationNode, "function");
});

test("toOperationNode registers map function in WeakMap", () => {
	const mapFn = (x: number) => x.toString();
	const expr = sql<number>`1`;
	const mapped = map(expr, mapFn);

	const node = mapped.toOperationNode();
	const mappedNodes = getMappedNodes();

	const registeredFn = mappedNodes.get(node);
	assert.ok(registeredFn);
	assert.strictEqual(registeredFn, mapFn);
});

test("nested maps compose into single function", () => {
	const innerFn = (x: number) => x * 2;
	const outerFn = (x: number) => x + 10;

	const expr = sql<number>`5`;
	const mapped = map(map(expr, innerFn), outerFn);

	const node = mapped.toOperationNode();
	const mappedNodes = getMappedNodes();

	const registeredFn = mappedNodes.get(node);
	assert.ok(registeredFn);

	// Should be composed: outerFn(innerFn(5)) = (5 * 2) + 10 = 20
	const result = registeredFn(5);
	assert.strictEqual(result, 20);
});

test("getMappedNodes returns WeakMap", () => {
	const mappedNodes = getMappedNodes();
	assert.ok(mappedNodes instanceof WeakMap);
});

test("multiple map calls create independent entries", () => {
	const fn1 = (x: number) => x * 2;
	const fn2 = (x: number) => x * 3;

	const expr1 = sql<number>`1`;
	const expr2 = sql<number>`2`;

	const mapped1 = map(expr1, fn1);
	const mapped2 = map(expr2, fn2);

	const node1 = mapped1.toOperationNode();
	const node2 = mapped2.toOperationNode();

	const mappedNodes = getMappedNodes();

	assert.strictEqual(mappedNodes.get(node1), fn1);
	assert.strictEqual(mappedNodes.get(node2), fn2);
});
