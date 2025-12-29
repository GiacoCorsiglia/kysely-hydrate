import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { db } from "../__tests__/sqlite.ts";
import { prefixSelectArg } from "./select-renamer.ts";

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
	const prefixed = prefixSelectArg(prefix, selection);

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
