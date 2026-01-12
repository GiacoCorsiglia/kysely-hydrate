import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { getDbForTest } from "../__tests__/db.ts";
import { type AnySelectArg, hoistAndPrefixSelections, prefixSelectArg } from "./select-renamer.ts";

const db = getDbForTest();

/**
 * Validates that prefixSelectArg correctly prefixes aliases while preserving
 * the underlying expression nodes unchanged. Compares against Kysely's own output.
 */
function assertCorrectPrefixing(
	prefix: string,
	selection: k.SelectArg<any, any, k.SelectExpression<any, any>>,
	expectedAliases: string[],
	expectedOriginalNames: string[],
): void {
	// Get the prefixed version
	const prefixed = prefixSelectArg(prefix, selection as AnySelectArg);

	// Get Kysely's unprefixed version as ground truth
	const kyselyQuery = db.selectFrom("users" as any).select(selection as any);
	const kyselyNode = kyselyQuery.toOperationNode();
	const kyselySelections = kyselyNode.selections || [];

	// Validate counts match
	assert.strictEqual(prefixed.length, kyselySelections.length);
	assert.strictEqual(prefixed.length, expectedAliases.length);
	assert.strictEqual(prefixed.length, expectedOriginalNames.length);

	// Validate each selection
	for (let i = 0; i < prefixed.length; i++) {
		const prefixedItem = prefixed[i]!;
		const kyselyItem = kyselySelections[i]!;

		// Check aliases are correctly prefixed
		assert.strictEqual(prefixedItem.alias, expectedAliases[i]);
		assert.strictEqual(prefixedItem.originalName, expectedOriginalNames[i]);

		// Check that the underlying expression node is identical to Kysely's
		// prefixedItem wraps the node in an ExpressionWrapper, we need to extract it
		const prefixedNode = prefixedItem.toOperationNode() as any;
		const prefixedSelection = prefixedNode.node?.selection || prefixedNode.node;

		// Kysely's node might be wrapped in an AliasNode
		const kyselyItemNode = k.AliasNode.is(kyselyItem.selection)
			? kyselyItem.selection.node
			: kyselyItem.selection;

		assert.deepStrictEqual(prefixedSelection, kyselyItemNode);
	}
}

test("prefixSelectArg: single column selection", () => {
	assertCorrectPrefixing("user$$", "id", ["user$$id"], ["id"]);
});

test("prefixSelectArg: multiple columns", () => {
	assertCorrectPrefixing(
		"post$$",
		["id", "title", "email"],
		["post$$id", "post$$title", "post$$email"],
		["id", "title", "email"],
	);
});

test("prefixSelectArg: aliased column", () => {
	assertCorrectPrefixing("user$$", "username as name", ["user$$name"], ["name"]);
});

test("prefixSelectArg: qualified column reference", () => {
	assertCorrectPrefixing("u$$", "users.id", ["u$$id"], ["id"]);
});

test("prefixSelectArg: qualified column with alias", () => {
	assertCorrectPrefixing("u$$", "users.username as name", ["u$$name"], ["name"]);
});

test("prefixSelectArg: mixed columns and aliases", () => {
	assertCorrectPrefixing(
		"p$$",
		["id", "username as name", "users.email"],
		["p$$id", "p$$name", "p$$email"],
		["id", "name", "email"],
	);
});

test("prefixSelectArg: empty prefix", () => {
	assertCorrectPrefixing("", "id", ["id"], ["id"]);
});

test("prefixSelectArg: expression builder callback", () => {
	assertCorrectPrefixing(
		"u$$",
		(eb) => [eb.ref("id").as("id"), eb.ref("username").as("name")],
		["u$$id", "u$$name"],
		["id", "name"],
	);
});

test("prefixSelectArg: returns empty array for empty selection", () => {
	assertCorrectPrefixing("p$$", [], [], []);
});

test("prefixSelectArg: schema-qualified column reference", () => {
	assertCorrectPrefixing("u$$", "public.users.id", ["u$$id"], ["id"]);
});

test("prefixSelectArg: schema-qualified column with alias", () => {
	assertCorrectPrefixing("u$$", "public.users.username as name", ["u$$name"], ["name"]);
});

test("hoistAndPrefixSelections: basic subquery with simple selections", () => {
	const subquery = db.selectFrom("users").select(["id", "username", "email"]);

	const hoisted = hoistAndPrefixSelections("user$$", subquery, "u");

	assert.strictEqual(hoisted.length, 3);
	assert.strictEqual(hoisted[0]!.alias, "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(hoisted[1]!.alias, "user$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
	assert.strictEqual(hoisted[2]!.alias, "user$$email");
	assert.strictEqual(hoisted[2]!.originalName, "email");

	// Verify the expressions reference the correct table.column
	const node0 = hoisted[0]!.expression.toOperationNode() as k.ReferenceNode;
	assert.strictEqual(node0.kind, "ReferenceNode");
	assert.strictEqual((node0.column as k.ColumnNode).column.name, "id");

	const node1 = hoisted[1]!.expression.toOperationNode() as k.ReferenceNode;
	assert.strictEqual(node1.kind, "ReferenceNode");
	assert.strictEqual((node1.column as k.ColumnNode).column.name, "username");
});

test("hoistAndPrefixSelections: subquery with aliased selections", () => {
	const subquery = db.selectFrom("users").select(["id", "username as name"]);

	const hoisted = hoistAndPrefixSelections("user$$", subquery, "u");

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(hoisted[0]!.alias, "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(hoisted[1]!.alias, "user$$name");
	assert.strictEqual(hoisted[1]!.originalName, "name");
});

test("hoistAndPrefixSelections: subquery with expression builder", () => {
	const subquery = db
		.selectFrom("users")
		.select((eb) => [eb.ref("id").as("user_id"), eb.ref("username").as("username")]);

	const hoisted = hoistAndPrefixSelections("u$$", subquery, "u");

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(hoisted[0]!.alias, "u$$user_id");
	assert.strictEqual(hoisted[0]!.originalName, "user_id");
	assert.strictEqual(hoisted[1]!.alias, "u$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
});

test("hoistAndPrefixSelections: empty prefix", () => {
	const subquery = db.selectFrom("users").select(["id", "username"]);

	const hoisted = hoistAndPrefixSelections("", subquery, "u");

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(hoisted[0]!.alias, "id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(hoisted[1]!.alias, "username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
});

test("hoistAndPrefixSelections: returns empty array for subquery with no selections", () => {
	// Create a subquery node with no selections
	const subquery = db.selectFrom("users");

	const hoisted = hoistAndPrefixSelections("u$$", subquery, "u");

	assert.strictEqual(hoisted.length, 0);
});

test("hoistAndPrefixSelections: subquery with schema-qualified selections", () => {
	const subquery = db.selectFrom("users").select([
		"public.users.id as id",
		"public.users.username as username",
		"public.users.email as email",
		// I'm not actually sure how to configure Kysely to understand
		// schema-qualified columns at the type-level, but this works well enough
		// for the test.
	] as any);

	const hoisted = hoistAndPrefixSelections("user$$", subquery, "u");

	assert.strictEqual(hoisted.length, 3);
	assert.strictEqual(hoisted[0]!.alias, "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(hoisted[1]!.alias, "user$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
	assert.strictEqual(hoisted[2]!.alias, "user$$email");
	assert.strictEqual(hoisted[2]!.originalName, "email");

	// Verify the expressions reference the correct table.column from the subquery alias
	const node0 = hoisted[0]!.expression.toOperationNode() as k.ReferenceNode;
	assert.strictEqual(node0.kind, "ReferenceNode");
	assert.strictEqual((node0.column as k.ColumnNode).column.name, "id");
});
