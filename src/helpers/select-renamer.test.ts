import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { type SeedDB } from "../__tests__/fixture.ts";
import { hoistAndPrefixSelections } from "./select-renamer.ts";

// These tests only build and inspect query ASTs — they never execute SQL — so
// use a no-op driver instead of spinning up a real database (which, under
// HYDRATE_TEST_DB=postgres, would create and seed a whole schema for nothing).
const db = new k.Kysely<SeedDB>({
	dialect: {
		createAdapter: () => new k.SqliteAdapter(),
		createDriver: () => new k.DummyDriver(),
		createIntrospector: (innerDb) => new k.SqliteIntrospector(innerDb),
		createQueryCompiler: () => new k.SqliteQueryCompiler(),
	},
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
