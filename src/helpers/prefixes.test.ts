import assert from "node:assert/strict";
import { test } from "node:test";

import {
	applyPrefix,
	createdPrefixedAccessor,
	getPrefixedValue,
	hasPrefix,
	makePrefix,
	removePrefix,
} from "./prefixes.ts";

//
// String helpers
//

test("makePrefix: creates an initial prefix from an empty parent", () => {
	assert.strictEqual(makePrefix("", "posts"), "posts$$");
});

test("makePrefix: chains a sub-prefix onto a parent prefix", () => {
	assert.strictEqual(makePrefix("posts$$", "comments"), "posts$$comments$$");
});

test("applyPrefix: prepends the prefix to a key", () => {
	assert.strictEqual(applyPrefix("posts$$", "id"), "posts$$id");
});

test("applyPrefix: returns the key unchanged for the empty prefix", () => {
	assert.strictEqual(applyPrefix("", "id"), "id");
});

test("removePrefix: strips the prefix length from the key", () => {
	assert.strictEqual(removePrefix("posts$$", "posts$$id"), "id");
});

test("removePrefix: strips only one level of a nested prefix", () => {
	assert.strictEqual(removePrefix("posts$$", "posts$$comments$$id"), "comments$$id");
});

test("hasPrefix: true only when the key starts with the prefix", () => {
	assert.strictEqual(hasPrefix("posts$$", "posts$$id"), true);
	assert.strictEqual(hasPrefix("posts$$", "id"), false);
	assert.strictEqual(hasPrefix("posts$$", "post$$id"), false);
	// Every key matches the empty prefix.
	assert.strictEqual(hasPrefix("", "id"), true);
});

test("getPrefixedValue: reads the prefixed key from the input", () => {
	const row = { id: 1, posts$$id: 10 };

	assert.strictEqual(getPrefixedValue("posts$$", row, "id"), 10);
	assert.strictEqual(getPrefixedValue("", row, "id"), 1);
	assert.strictEqual(getPrefixedValue("posts$$", row, "missing"), undefined);
});

//
// createdPrefixedAccessor (the Proxy used for extras/extenders/comparators
// at nested levels)
//

test("createdPrefixedAccessor: empty prefix returns the input object itself", () => {
	const row = { id: 1, name: "alice" };

	assert.strictEqual(createdPrefixedAccessor("", row), row);
});

test("createdPrefixedAccessor: get reads through the prefix", () => {
	const row = { id: 1, posts$$id: 10, posts$$title: "Post 10" };
	const accessor = createdPrefixedAccessor("posts$$", row);

	assert.strictEqual(accessor.id, 10);
	assert.strictEqual(accessor.title, "Post 10");
});

test("createdPrefixedAccessor: get returns undefined for keys outside the prefix", () => {
	const row = { id: 1, posts$$id: 10 };
	const accessor = createdPrefixedAccessor("posts$$", row) as Record<string, unknown>;

	// "id" resolves to "posts$$id"; the parent's bare "id" is not reachable.
	assert.strictEqual(accessor.missing, undefined);
	assert.strictEqual(accessor.posts$$id, undefined);
});

test("createdPrefixedAccessor: has trap checks the prefixed key", () => {
	const row = { id: 1, posts$$id: 10 };
	const accessor = createdPrefixedAccessor("posts$$", row);

	assert.strictEqual("id" in accessor, true);
	assert.strictEqual("title" in accessor, false);
	// The parent's keys are invisible.
	assert.strictEqual("posts$$id" in accessor, false);
});

test("createdPrefixedAccessor: ownKeys lists only this prefix level, stripped", () => {
	const row = {
		id: 1,
		name: "alice",
		posts$$id: 10,
		posts$$title: "Post 10",
		posts$$comments$$id: 100,
	};
	const accessor = createdPrefixedAccessor("posts$$", row);

	// Nested-collection keys keep their remaining prefix; hydration filters
	// them out separately (see #getAutoFields).
	assert.deepStrictEqual(Object.keys(accessor), ["id", "title", "comments$$id"]);
});

test("createdPrefixedAccessor: spread materializes the stripped view", () => {
	const row = { id: 1, posts$$id: 10, posts$$title: "Post 10" };
	const accessor = createdPrefixedAccessor("posts$$", row);

	assert.deepStrictEqual({ ...accessor }, { id: 10, title: "Post 10" });
});

test("createdPrefixedAccessor: works for a doubly-nested prefix", () => {
	const row = { id: 1, posts$$id: 10, posts$$comments$$id: 100, posts$$comments$$body: "hi" };
	const accessor = createdPrefixedAccessor("posts$$comments$$", row);

	assert.strictEqual(accessor.id, 100);
	assert.deepStrictEqual({ ...accessor }, { id: 100, body: "hi" });
});

test("createdPrefixedAccessor: null and undefined values are preserved", () => {
	const row = { posts$$id: null, posts$$title: undefined };
	const accessor = createdPrefixedAccessor("posts$$", row);

	assert.strictEqual(accessor.id, null);
	assert.strictEqual(accessor.title, undefined);
	assert.strictEqual("id" in accessor, true);
	assert.strictEqual("title" in accessor, true);
});
