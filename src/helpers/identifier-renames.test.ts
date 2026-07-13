import assert from "node:assert/strict";
import { test } from "node:test";

import {
	computeIdentifierRenames,
	mayExceedIdentifierLimit,
	restoreRowLogicalNames,
} from "./identifier-renames.ts";

//
// mayExceedIdentifierLimit
//

test("mayExceedIdentifierLimit: short names are safe", () => {
	assert.strictEqual(mayExceedIdentifierLimit("id"), false);
	assert.strictEqual(mayExceedIdentifierLimit("posts$$comments$$commentText"), false);
});

test("mayExceedIdentifierLimit: 63 bytes without growth is safe", () => {
	const name = "a".repeat(63);
	assert.strictEqual(mayExceedIdentifierLimit(name), false);
});

test("mayExceedIdentifierLimit: 64 bytes is over the limit", () => {
	const name = "a".repeat(64);
	assert.strictEqual(mayExceedIdentifierLimit(name), true);
});

test("mayExceedIdentifierLimit: camelCase humps count toward the worst case", () => {
	// 58 characters, but 6 upper-case characters mean the CamelCasePlugin can
	// snake_case this to 64 bytes.
	const name = "employeeDirectoryEntries$$employeePreferredFullDisplayName";
	assert.strictEqual(name.length, 58);
	assert.strictEqual(mayExceedIdentifierLimit(name), true);

	// 63 characters of lower-case is fine ...
	assert.strictEqual(mayExceedIdentifierLimit("a".repeat(63)), false);
	// ... but a single hump pushes the worst case to 64 bytes.
	assert.strictEqual(mayExceedIdentifierLimit(`${"a".repeat(62)}B`), true);
});

test("mayExceedIdentifierLimit: digit runs count toward the worst case", () => {
	// `underscoreBeforeDigits` inserts one underscore per digit run.
	assert.strictEqual(mayExceedIdentifierLimit(`${"a".repeat(61)}12`), true);
	// A digit run only grows by one byte, no matter its length.
	assert.strictEqual(mayExceedIdentifierLimit(`${"a".repeat(50)}123456789012`), false);
});

test("mayExceedIdentifierLimit: multi-byte characters are measured in bytes", () => {
	// 32 euro signs = 96 UTF-8 bytes even though it's only 32 characters.
	assert.strictEqual(mayExceedIdentifierLimit("€".repeat(32)), true);
	assert.strictEqual(mayExceedIdentifierLimit("€".repeat(21)), false); // 63 bytes
});

//
// computeIdentifierRenames
//

test("computeIdentifierRenames: no candidates produces empty maps", () => {
	const renames = computeIdentifierRenames(["id", "username", "posts$$id"]);
	assert.strictEqual(renames.toShort.size, 0);
	assert.strictEqual(renames.toLogical.size, 0);
});

test("computeIdentifierRenames: renames over-long prefixed names sequentially", () => {
	const long1 = `posts$$${"a".repeat(70)}`;
	const long2 = `posts$$${"b".repeat(70)}`;
	const renames = computeIdentifierRenames(["id", long1, "posts$$id", long2]);

	assert.deepStrictEqual(
		[...renames.toShort],
		[
			[long1, "$c0"],
			[long2, "$c1"],
		],
	);
	assert.deepStrictEqual(
		[...renames.toLogical],
		[
			["$c0", long1],
			["$c1", long2],
		],
	);
});

test("computeIdentifierRenames: does not rename over-long plain (unprefixed) names", () => {
	// A plain column name is the user's own; it would be equally truncated in a
	// plain Kysely query, so it is left alone.
	const renames = computeIdentifierRenames(["a".repeat(70)]);
	assert.strictEqual(renames.toShort.size, 0);
});

test("computeIdentifierRenames: skips short aliases taken by real columns", () => {
	const long = `posts$$${"a".repeat(70)}`;
	const renames = computeIdentifierRenames(["$c0", long]);
	assert.strictEqual(renames.toShort.get(long), "$c1");
});

//
// restoreRowLogicalNames
//

test("restoreRowLogicalNames: translates short keys and passes others through", () => {
	const long = `posts$$${"a".repeat(70)}`;
	const { toLogical } = computeIdentifierRenames(["id", long]);

	const restored = restoreRowLogicalNames({ id: 1, $c0: "value" }, toLogical);

	assert.deepStrictEqual(restored, { id: 1, [long]: "value" });
});

test("restoreRowLogicalNames: passes non-object rows through", () => {
	const { toLogical } = computeIdentifierRenames([`posts$$${"a".repeat(70)}`]);
	assert.strictEqual(restoreRowLogicalNames(null, toLogical), null);
	assert.strictEqual(restoreRowLogicalNames(42, toLogical), 42);
});
