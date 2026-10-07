import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { type SeedDB } from "../__tests__/fixture.ts";
import { aliasQuery, hoistAndPrefixSelections } from "./select-renamer.ts";

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

/** The output name a hoisted selection selects its column as. */
const aliasOf = (hoisted: k.OperationNodeSource) =>
	((hoisted.toOperationNode() as k.AliasNode).alias as k.IdentifierNode).name;

test("hoistAndPrefixSelections: basic subquery with simple selections", () => {
	const subquery = db.selectFrom("users").select(["id", "username", "email"]);

	const hoisted = hoistAndPrefixSelections("user$$", aliasQuery(subquery, "u"));

	assert.strictEqual(hoisted.length, 3);
	assert.strictEqual(aliasOf(hoisted[0]!), "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(aliasOf(hoisted[1]!), "user$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
	assert.strictEqual(aliasOf(hoisted[2]!), "user$$email");
	assert.strictEqual(hoisted[2]!.originalName, "email");

	// Verify the expressions reference the correct table.column
	const node0 = hoisted[0]!.toOperationNode().node as k.ReferenceNode;
	assert.strictEqual(node0.kind, "ReferenceNode");
	assert.strictEqual((node0.column as k.ColumnNode).column.name, "id");

	const node1 = hoisted[1]!.toOperationNode().node as k.ReferenceNode;
	assert.strictEqual(node1.kind, "ReferenceNode");
	assert.strictEqual((node1.column as k.ColumnNode).column.name, "username");
});

test("hoistAndPrefixSelections: subquery with aliased selections", () => {
	const subquery = db.selectFrom("users").select(["id", "username as name"]);

	const hoisted = hoistAndPrefixSelections("user$$", aliasQuery(subquery, "u"));

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(aliasOf(hoisted[0]!), "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(aliasOf(hoisted[1]!), "user$$name");
	assert.strictEqual(hoisted[1]!.originalName, "name");
});

test("hoistAndPrefixSelections: subquery with expression builder", () => {
	const subquery = db
		.selectFrom("users")
		.select((eb) => [eb.ref("id").as("user_id"), eb.ref("username").as("username")]);

	const hoisted = hoistAndPrefixSelections("u$$", aliasQuery(subquery, "u"));

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(aliasOf(hoisted[0]!), "u$$user_id");
	assert.strictEqual(hoisted[0]!.originalName, "user_id");
	assert.strictEqual(aliasOf(hoisted[1]!), "u$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
});

test("hoistAndPrefixSelections: empty prefix", () => {
	const subquery = db.selectFrom("users").select(["id", "username"]);

	const hoisted = hoistAndPrefixSelections("", aliasQuery(subquery, "u"));

	assert.strictEqual(hoisted.length, 2);
	assert.strictEqual(aliasOf(hoisted[0]!), "id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(aliasOf(hoisted[1]!), "username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
});

test("hoistAndPrefixSelections: returns empty array for subquery with no selections", () => {
	// Create a subquery node with no selections
	const subquery = db.selectFrom("users");

	const hoisted = hoistAndPrefixSelections("u$$", aliasQuery(subquery, "u"));

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

	const hoisted = hoistAndPrefixSelections("user$$", aliasQuery(subquery, "u"));

	assert.strictEqual(hoisted.length, 3);
	assert.strictEqual(aliasOf(hoisted[0]!), "user$$id");
	assert.strictEqual(hoisted[0]!.originalName, "id");
	assert.strictEqual(aliasOf(hoisted[1]!), "user$$username");
	assert.strictEqual(hoisted[1]!.originalName, "username");
	assert.strictEqual(aliasOf(hoisted[2]!), "user$$email");
	assert.strictEqual(hoisted[2]!.originalName, "email");

	// Verify the expressions reference the correct table.column from the subquery alias
	const node0 = hoisted[0]!.toOperationNode().node as k.ReferenceNode;
	assert.strictEqual(node0.kind, "ReferenceNode");
	assert.strictEqual((node0.column as k.ColumnNode).column.name, "id");
	// The table part must be the subquery alias ("u") — the whole point of
	// hoisting — not the original (possibly schema-qualified) table
	assert.strictEqual(node0.table?.table.identifier.name, "u");
	assert.strictEqual(node0.table?.table.schema, undefined);
});

test("hoistAndPrefixSelections: an owner's hoisted selections are built once and shared", () => {
	const subquery = db.selectFrom("users").select(["id", "username"]);
	const owner = {};

	const first = hoistAndPrefixSelections("user$$", aliasQuery(subquery, "u"), owner);
	// A rebuild converts the subquery to a new node, with the same names.
	const again = hoistAndPrefixSelections("user$$", aliasQuery(subquery, "u"), owner);
	assert.deepStrictEqual(
		again.map((h, i) => h === first[i]),
		[true, true],
	);

	// A column the owner hasn't hoisted before is built alongside the shared ones.
	const wider = hoistAndPrefixSelections(
		"user$$",
		aliasQuery(db.selectFrom("users").select(["email", "id"]), "u"),
		owner,
	);
	assert.deepStrictEqual(wider.map(aliasOf), ["user$$email", "user$$id"]);
	assert.strictEqual(wider[1], first[0]);
});

test("hoistAndPrefixSelections: an owner's selections are kept apart by alias and prefix", () => {
	const subquery = db.selectFrom("users").select(["id"]);
	const owner = {};

	const shown = (prefix: string, alias: string) => {
		const [hoisted] = hoistAndPrefixSelections(prefix, aliasQuery(subquery, alias), owner);
		const reference = hoisted!.toOperationNode().node as k.ReferenceNode;
		return [aliasOf(hoisted!), reference.table!.table.identifier.name];
	};

	assert.deepStrictEqual(shown("a$$", "u"), ["a$$id", "u"]);
	assert.deepStrictEqual(shown("b$$", "u"), ["b$$id", "u"]);
	assert.deepStrictEqual(shown("a$$", "v"), ["a$$id", "v"]);
	assert.deepStrictEqual(shown("a$$", "u"), ["a$$id", "u"]);
	// Names that would collide if alias and prefix were joined into one key.
	assert.deepStrictEqual(shown("\0b$$", "a"), ["\0b$$id", "a"]);
	assert.deepStrictEqual(shown("b$$", "a\0"), ["b$$id", "a\0"]);
});

test("hoistAndPrefixSelections: without an owner, selections are built per call", () => {
	const subquery = db.selectFrom("users").select(["id"]);

	const [first] = hoistAndPrefixSelections("u$$", aliasQuery(subquery, "u"));
	const [again] = hoistAndPrefixSelections("u$$", aliasQuery(subquery, "u"));
	assert.notStrictEqual(first, again);
	assert.strictEqual(aliasOf(again!), "u$$id");
});
