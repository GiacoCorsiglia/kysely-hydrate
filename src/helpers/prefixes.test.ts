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

//
// Non-extensible inputs
//
// Proxy invariants are enforced against the Proxy's target, and `ownKeys` must
// report a non-extensible target's own keys exactly.  The accessor hides the
// keys of other prefixes, so it cannot use the row as its target.
//

test("createdPrefixedAccessor: enumerates a frozen input row", () => {
	const input = Object.freeze({ id: 1, posts$$id: 7, posts$$title: "t" });

	const accessor = createdPrefixedAccessor("posts$$", input);

	assert.deepStrictEqual(Object.keys(accessor), ["id", "title"]);
	assert.deepStrictEqual({ ...accessor }, { id: 7, title: "t" });
	assert.deepStrictEqual(Object.entries(accessor), [
		["id", 7],
		["title", "t"],
	]);
	assert.strictEqual(JSON.stringify(accessor), '{"id":7,"title":"t"}');
});

test("createdPrefixedAccessor: reports properties of a frozen row as configurable", () => {
	// A descriptor for a property the target does not have may not report
	// non-configurability, which a frozen row's own descriptors do.
	const input = Object.freeze({ posts$$id: 7 });

	const descriptor = Object.getOwnPropertyDescriptor(
		createdPrefixedAccessor("posts$$", input),
		"id",
	);

	assert.deepStrictEqual(descriptor, {
		value: 7,
		writable: false,
		enumerable: true,
		configurable: true,
	});
});

//
// Writes
//
// Reads apply the prefix, so writes must too, or they land on the input row
// unprefixed - at the wrong nesting level, on an object the caller owns.
//

test("createdPrefixedAccessor: set writes through the prefix", () => {
	const input: Record<string, unknown> = { id: 1, posts$$id: 7 };

	const accessor = createdPrefixedAccessor("posts$$", input) as Record<string, unknown>;
	accessor["title"] = "written";
	accessor["id"] = 8;

	assert.strictEqual(input["posts$$title"], "written");
	assert.strictEqual(input["posts$$id"], 8);
	// Nothing lands unprefixed, and the parent level's own keys are untouched.
	assert.deepStrictEqual(Object.keys(input), ["id", "posts$$id", "posts$$title"]);
	assert.strictEqual(input["id"], 1);
	// The write reads back through the accessor.
	assert.strictEqual(accessor["title"], "written");
});

test("createdPrefixedAccessor: defineProperty and delete apply the prefix", () => {
	const input: Record<string, unknown> = { posts$$id: 7, posts$$title: "t" };

	const accessor = createdPrefixedAccessor("posts$$", input) as Record<string, unknown>;
	Object.defineProperty(accessor, "extra", { value: 1, enumerable: true, configurable: true });
	delete accessor["title"];

	assert.deepStrictEqual(Object.keys(input), ["posts$$id", "posts$$extra"]);
	assert.strictEqual(input["posts$$extra"], 1);
});

test("createdPrefixedAccessor: writing to a frozen input row throws", () => {
	const accessor = createdPrefixedAccessor("posts$$", Object.freeze({ posts$$id: 7 })) as Record<
		string,
		unknown
	>;

	assert.throws(() => {
		accessor["title"] = "nope";
	}, TypeError);
});

test("createdPrefixedAccessor: the accessor's own machinery is not observable", () => {
	// The Proxy target holds the prefix and the input row, so columns that
	// happen to share those names must still resolve to the row's values.
	const input = { posts$$prefix: "col-prefix", posts$$input: "col-input" };

	const accessor = createdPrefixedAccessor("posts$$", input) as Record<string, unknown>;

	assert.strictEqual(accessor["prefix"], "col-prefix");
	assert.strictEqual(accessor["input"], "col-input");
	assert.deepStrictEqual(Object.keys(accessor), ["prefix", "input"]);
	assert.deepStrictEqual({ ...accessor }, { prefix: "col-prefix", input: "col-input" });
});
