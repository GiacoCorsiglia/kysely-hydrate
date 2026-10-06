import assert from "node:assert";
import { test } from "node:test";

import { CardinalityViolationError } from "./helpers/errors.ts";
import { createHydrator, hydrate } from "./hydrator.ts";

// Test data types
interface User {
	id: number;
	name: string;
}

//
// Grouping and deduplication
//

test("grouping: deduplicates by keyBy even without nested collections", async () => {
	// Regression test: hydrators without nested collections skipped grouping
	// entirely, so duplicate rows produced duplicate entities (inconsistent
	// with hydrators that have collections, which always group by keyBy).
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>("id").fields(["id", "name"]);

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("grouping: hasOne tolerates duplicated parent rows", async () => {
	// Regression test: when the parent's keyBy groups several identical rows
	// (e.g. a base query with duplicate keys), a collection-less child hydrator
	// skipped grouping and saw one child per row, throwing a spurious
	// CardinalityViolationError for "one" collections.
	interface UserWithProfile extends User {
		profile$$id: number | null;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: 5 },
		{ id: 1, name: "Alice", profile$$id: 5 },
	];

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields(["id", "name"])
		.hasOne("profile", "profile$$", (h) => h("id").fields(["id"]));

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice", profile: { id: 5 } }]);
});

test("grouping: hasMany deduplicates children under duplicated parent rows", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10 },
		{ id: 1, name: "Alice", posts$$id: 10 },
		{ id: 1, name: "Alice", posts$$id: 11 },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields(["id", "name"])
		.hasMany("posts", "posts$$", (h) => h("id").fields(["id"]));

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice", posts: [{ id: 10 }, { id: 11 }] }]);
});

test("grouping: hasOne throws CardinalityViolationError for multiple distinct children", async () => {
	interface UserWithProfile extends User {
		profile$$id: number | null;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: 5 },
		{ id: 1, name: "Alice", profile$$id: 6 },
	];

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields(["id", "name"])
		.hasOne("profile", "profile$$", (h) => h("id").fields(["id"]));

	await assert.rejects(hydrate(rows, hydrator), CardinalityViolationError);
});

test("hydrate: rejects instead of throwing synchronously", async () => {
	// Regression test: when there were no attach fetches, hydrate() ran
	// synchronously and threw instead of rejecting, so callers using
	// `hydrate(...).catch(...)` without an immediate await missed the error.
	interface UserWithProfile extends User {
		profile$$id: number | null;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: 5 },
		{ id: 1, name: "Alice", profile$$id: 6 },
	];

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields(["id", "name"])
		.hasOne("profile", "profile$$", (h) => h("id").fields(["id"]));

	let promise: Promise<unknown> | undefined;
	assert.doesNotThrow(() => {
		promise = hydrator.hydrate(rows);
	});
	await assert.rejects(promise!, CardinalityViolationError);
});

test("hydrate: rejects when the hydrator factory throws", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	let promise: Promise<unknown> | undefined;
	assert.doesNotThrow(() => {
		promise = hydrate(users, () => {
			throw new Error("factory failed");
		});
	});
	await assert.rejects(promise!, /factory failed/);
});

test("hydrate: rejects when the hydrator factory returns a non-hydrator", async () => {
	// A factory returning something without a .hydrate() method must reject
	// (like every other failure mode) rather than throw synchronously.
	let promise: Promise<unknown> | undefined;
	assert.doesNotThrow(() => {
		promise = hydrate([], () => ({}) as any);
	});
	await assert.rejects(promise!, TypeError);
});

test("grouping: attachOne throws CardinalityViolationError for multiple matching children", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields(["id", "name"])
		.attachOne(
			"profile",
			() => [
				{ userId: 1, bio: "first" },
				{ userId: 1, bio: "second" },
			],
			{ matchChild: "userId", toParent: "id" },
		);

	await assert.rejects(async () => hydrate(users, hydrator), CardinalityViolationError);
});

//
// Composite keys
//

test("composite keys: groups by multiple fields", async () => {
	interface CompositeRow {
		key1: string;
		key2: number;
		value: string;
		nested$$id: number | null;
	}

	const rows: CompositeRow[] = [
		{ key1: "a", key2: 1, value: "first", nested$$id: 1 },
		{ key1: "a", key2: 1, value: "first", nested$$id: 2 },
		{ key1: "a", key2: 2, value: "second", nested$$id: 3 },
	];

	const hydrator = createHydrator<CompositeRow>(["key1", "key2"])
		.fields({
			key1: true,
			key2: true,
			value: true,
		})
		.hasMany("items", "nested$$", (h) => h("id").fields({ id: true }));

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], {
		key1: "a",
		key2: 1,
		value: "first",
		items: [{ id: 1 }, { id: 2 }],
	});
	assert.deepStrictEqual(result[1], {
		key1: "a",
		key2: 2,
		value: "second",
		items: [{ id: 3 }],
	});
});

test("composite keys: work at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$key1: string | null;
		posts$$key2: number | null;
		posts$$title: string | null;
		posts$$comments$$id: number | null;
		posts$$comments$$text: string | null;
	}

	// The three posts pairwise share key1 OR key2, so a composite keyBy that
	// collapsed to either single column would merge posts and change the
	// grouping below
	const rows: UserWithPosts[] = [
		{
			id: 1,
			name: "Alice",
			posts$$key1: "a",
			posts$$key2: 1,
			posts$$title: "Post a1",
			posts$$comments$$id: 100,
			posts$$comments$$text: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$key1: "a",
			posts$$key2: 1,
			posts$$title: "Post a1",
			posts$$comments$$id: 101,
			posts$$comments$$text: "Comment 2",
		},
		{
			id: 1,
			name: "Alice",
			posts$$key1: "a",
			posts$$key2: 2,
			posts$$title: "Post a2",
			posts$$comments$$id: 102,
			posts$$comments$$text: "Comment 3",
		},
		{
			id: 1,
			name: "Alice",
			posts$$key1: "b",
			posts$$key2: 1,
			posts$$title: "Post b1",
			posts$$comments$$id: 103,
			posts$$comments$$text: "Comment 4",
		},
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h(["key1", "key2"])
				.fields({ key1: true, key2: true, title: true })
				.hasMany("comments", "comments$$", (h) => h("id").fields({ id: true, text: true })),
		);

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [
				{
					key1: "a",
					key2: 1,
					title: "Post a1",
					comments: [
						{ id: 100, text: "Comment 1" },
						{ id: 101, text: "Comment 2" },
					],
				},
				{
					key1: "a",
					key2: 2,
					title: "Post a2",
					comments: [{ id: 102, text: "Comment 3" }],
				},
				{
					key1: "b",
					key2: 1,
					title: "Post b1",
					comments: [{ id: 103, text: "Comment 4" }],
				},
			],
		},
	]);
});

test("composite keys: skips rows where any key part is null", async () => {
	interface CompositeRow {
		key1: string | null;
		key2: number | null;
		value: string;
	}

	const rows: CompositeRow[] = [
		{ key1: "a", key2: 1, value: "valid" },
		{ key1: "a", key2: null, value: "invalid" },
		{ key1: null, key2: 1, value: "invalid" },
	];

	const hydrator = createHydrator<CompositeRow>(["key1", "key2"]).fields({
		key1: true,
		key2: true,
		value: true,
	});

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0], { key1: "a", key2: 1, value: "valid" });
});

test("composite keys: a nil part after the first does not strand its branch", async () => {
	interface CompositeRow {
		key1: string;
		key2: number | null;
		value: string;
	}

	// A row whose second part is nil is abandoned after its first part was
	// already matched; later rows sharing that first part must still group.
	const rows: CompositeRow[] = [
		{ key1: "a", key2: null, value: "invalid" },
		{ key1: "a", key2: 1, value: "a1" },
		{ key1: "a", key2: null, value: "invalid" },
		{ key1: "a", key2: 2, value: "a2" },
		{ key1: "a", key2: 1, value: "a1 again" },
	];

	const hydrator = createHydrator<CompositeRow>(["key1", "key2"]).fields({
		key1: true,
		key2: true,
		value: true,
	});

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [
		{ key1: "a", key2: 1, value: "a1" },
		{ key1: "a", key2: 2, value: "a2" },
	]);
});

interface KeyPartRow {
	key1: unknown;
	key2: unknown;
	nested$$id: number;
}

/**
 * Hydrates one row per `[key1, key2]` pair (each carrying a distinct child)
 * and asserts how the rows grouped: `groups[i]` names the group row `i`
 * belongs to, and groups are expected in the order their keys first appear.
 */
async function assertKeyGrouping(
	keyBy: "key1" | ["key1", "key2"],
	rows: readonly (readonly [key1: unknown, key2: unknown])[],
	groups: readonly number[],
) {
	const hydrator = createHydrator<KeyPartRow>(keyBy)
		.fields({ key1: true, key2: true })
		.hasMany("items", "nested$$", (h) => h("id").fields({ id: true }));

	const expected: { key1: unknown; key2: unknown; items: { id: number }[] }[] = [];
	const byGroup = new Map<number, (typeof expected)[number]>();
	rows.forEach(([key1, key2], index) => {
		const item = { id: index + 1 };
		const existing = byGroup.get(groups[index]!);
		if (existing) {
			existing.items.push(item);
			return;
		}
		const entity = { key1, key2, items: [item] };
		byGroup.set(groups[index]!, entity);
		expected.push(entity);
	});

	const result = await hydrate(
		rows.map(([key1, key2], index) => ({ key1, key2, nested$$id: index + 1 })),
		hydrator,
	);

	assert.deepStrictEqual(result, expected);
}

const date = new Date("2026-01-02T03:04:05.678Z");

const nullPrototypeValue = Object.create(null);

const stringLikeObject = { toString: () => "true" };

/** A grouping case: the rows' key parts, and the group each row belongs to. */
type GroupingCase<Row> = [name: string, rows: Row[], groups: number[]];

// Values for a key's first part, each paired with the group it belongs to:
// values in the same group are one entity, values in different groups must not
// collide.  Every case must hold for both `keyBy` shapes, since a single key
// and a one-part composite key canonicalize their parts the same way.
const keyPartCases: GroupingCase<unknown>[] = [
	// Regression: bigints were encoded as `${value}n`, colliding with "123n".
	["bigints do not collide with their string form", [123n, "123n"], [0, 1]],
	// Regression: NaN and Infinity both JSON-serialize to null.
	[
		"NaN groups with NaN and does not collide with Infinity",
		[Number.NaN, Number.NaN, Number.POSITIVE_INFINITY],
		[0, 0, 1],
	],
	// Regression: Date#toJSON made a Date and its ISO string produce one key.
	// The millisecond apart pins the time value: a Date's String() form is only
	// second-resolution, so canonicalizing by it would merge these two.
	[
		"Dates group by time value and do not collide with their ISO string",
		[date, new Date(date.getTime()), date.toISOString(), new Date(date.getTime() + 1)],
		[0, 0, 1, 2],
	],
	// Buffer#toString() decodes as UTF-8, which is lossy: distinct invalid
	// sequences both decode to U+FFFD, so bytes must be compared as bytes.
	[
		"binary values group by content, keeping byte boundaries",
		[
			new Uint8Array([1, 2]),
			new Uint8Array([1, 2]),
			new Uint8Array([12]),
			Buffer.from([0xc0]),
			Buffer.from([0xc1]),
		],
		[0, 0, 1, 2, 3],
	],
	// SQL types a column, so a part holds one type across rows and equal string
	// forms across types are accepted as one key rather than defended against.
	// Primitives of different types still never collide.
	[
		"values of different types with equal string forms share a key",
		[true, "true", stringLikeObject],
		[0, 1, 1],
	],
	// String() throws for null-prototype objects; the fallback must still key.
	[
		"values without a primitive conversion group rather than reject",
		[nullPrototypeValue, nullPrototypeValue],
		[0, 0],
	],
];

for (const [name, values, groups] of keyPartCases) {
	test(`keys: ${name}`, async () => {
		const rows = values.map((key1) => [key1, "x"] as const);
		await assertKeyGrouping("key1", rows, groups);
		await assertKeyGrouping(["key1", "key2"], rows, groups);
	});
}

// Whole composite keys, as `[key1, key2]` pairs, with the group each belongs
// to.  These cases vary both parts, so they have no single-key equivalent.
const compositeKeyCases: GroupingCase<readonly [unknown, unknown]>[] = [
	// Regression: parts were joined with "::", so both of these keyed "x::y::z".
	[
		"values containing a separator do not collide",
		[
			["x::y", "z"],
			["x", "y::z"],
		],
		[0, 1],
	],
	// Quotes and backslashes are what a text encoding would have to escape.
	[
		"values containing quotes and backslashes do not collide",
		[
			['a"s"b', "c"],
			["a", 'b"s"c'],
			['a\\"s"b', "c"],
			["a\\", '"s"b\\"c'],
		],
		[0, 1, 2, 3],
	],
	[
		"part boundaries cannot shift",
		[
			["a", "b"],
			["ab", ""],
			["", "ab"],
		],
		[0, 1, 2],
	],
	// Entities sharing a first part are adjacent in the lookup structure, but
	// output order must follow first appearance in the rows.
	[
		"entities keep first-appearance order, not key order",
		[
			["a", 1],
			["b", 1],
			["a", 2],
			["b", 1],
		],
		[0, 1, 2, 1],
	],
];

for (const [name, rows, groups] of compositeKeyCases) {
	test(`composite keys: ${name}`, async () => {
		await assertKeyGrouping(["key1", "key2"], rows, groups);
	});
}

//
// Hydration modes and edge cases
//

test("hydrate: handles single input", async () => {
	const user: User = { id: 1, name: "Alice" };

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(user, hydrator);

	assert.deepStrictEqual(result, { id: 1, name: "Alice" });
});

test("hydrate: handles empty array", async () => {
	const users: User[] = [];

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, []);
});

test("hydrate: skips entities with null keys", async () => {
	interface NullableUser {
		id: number | null;
		name: string;
	}

	const users: NullableUser[] = [
		{ id: 1, name: "Alice" },
		{ id: null, name: "Invalid" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<NullableUser>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], { id: 1, name: "Alice" });
	assert.deepStrictEqual(result[1], { id: 2, name: "Bob" });
});

test("hydrate function: accepts inline hydrator creation", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const result = await hydrate(users, (keyBy) => keyBy("id").fields({ id: true, name: true }));

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

//
// Input ownership
//

test("hydrate: does not mutate the caller's input array", async () => {
	const users: User[] = [
		{ id: 2, name: "Bob" },
		{ id: 1, name: "Alice" },
		{ id: 3, name: "Carol" },
	];
	const snapshot = users.map((user) => ({ ...user }));

	const hydrator = createHydrator<User>("id").fields({ id: true, name: true }).orderBy("id");

	const result = await hydrate(users, hydrator);

	// The output is sorted...
	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
		{ id: 3, name: "Carol" },
	]);
	// ...but the caller's array keeps its original order.
	assert.deepStrictEqual(users, snapshot);
});

test("hydrate: sorting one collection does not reorder its sibling's input rows", async () => {
	// Sibling collections at the same level hydrate from the same underlying
	// row array, so sorting must never reorder it in place. Observable here
	// because `b` has no orderBy and so preserves input order.
	interface Row extends User {
		a$$id: number;
		b$$id: number;
	}

	const rows: Row[] = [
		{ id: 1, name: "Alice", a$$id: 1, b$$id: 10 },
		{ id: 1, name: "Alice", a$$id: 2, b$$id: 30 },
		{ id: 1, name: "Alice", a$$id: 3, b$$id: 20 },
	];

	const hydrator = createHydrator<Row>("id")
		.fields({ id: true, name: true })
		.hasMany("a", "a$$", (h) => h("id").fields({ id: true }).orderBy("id", "desc"))
		.hasMany("b", "b$$", (h) => h("id").fields({ id: true }));

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(
		result[0]?.a.map((item) => item.id),
		[3, 2, 1],
	);
	// `b` must see the rows in their original input order, not in the order its
	// sibling sorted them.
	assert.deepStrictEqual(
		result[0]?.b.map((item) => item.id),
		[10, 30, 20],
	);
});

//
// Default keyBy
//

test("createHydrator: keyBy defaults to 'id' when input has id", async () => {
	// keyBy omitted - should default to "id". The repeated id with a differing
	// name proves the key is id: keying by name would yield 3 entities.
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 1, name: "Alice (duplicate row)" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>().fields({ id: true, name: true });

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("hasMany: keyBy defaults to 'id' when nested input has id", async () => {
	type UserWithPosts = User & {
		posts$$id: number;
		posts$$title: string;
	};

	// The repeated posts$$id 1 with a differing title proves the nested
	// default key is id: keying posts by title would yield 3 posts for Alice
	const data: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 1, posts$$title: "Post 1" },
		{ id: 1, name: "Alice", posts$$id: 1, posts$$title: "Post 1 (duplicate row)" },
		{ id: 1, name: "Alice", posts$$id: 2, posts$$title: "Post 2" },
		{ id: 2, name: "Bob", posts$$id: 3, posts$$title: "Post 3" },
	];

	// Both createHydrator and hasMany keyBy omitted
	const hydrator = createHydrator<UserWithPosts>()
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (create) => create().fields({ id: true, title: true }));

	const result = await hydrate(data, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0]?.posts, [
		{ id: 1, title: "Post 1" },
		{ id: 2, title: "Post 2" },
	]);
	assert.deepStrictEqual(result[1]?.posts, [{ id: 3, title: "Post 3" }]);
});

test("hasOne: keyBy defaults to 'id' when nested input has id", async () => {
	interface Post {
		id: number;
		title: string;
	}

	type PostWithAuthor = Post & {
		author$$id: number;
		author$$name: string;
	};

	// The repeated author$$id with a differing name proves the nested default
	// key is id: keying the author by name would yield two distinct authors
	// (a cardinality violation for hasOne)
	const data: PostWithAuthor[] = [
		{ id: 1, title: "Post 1", author$$id: 1, author$$name: "Alice" },
		{ id: 1, title: "Post 1", author$$id: 1, author$$name: "Alice (duplicate row)" },
	];

	// Both createHydrator and hasOne keyBy omitted
	const hydrator = createHydrator<PostWithAuthor>()
		.fields({ id: true, title: true })
		.hasOne("author", "author$$", (create) => create().fields({ id: true, name: true }));

	const result = await hydrate(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0]?.author, { id: 1, name: "Alice" });
});

test("hasOneOrThrow: keyBy defaults to 'id' when nested input has id", async () => {
	type UserWithProfile = User & {
		profile$$id: number;
		profile$$bio: string;
	};

	// The repeated profile$$id with a differing bio proves the nested default
	// key is id: keying the profile by bio would yield two distinct profiles
	// (a cardinality violation for hasOneOrThrow)
	const data: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: 1, profile$$bio: "Bio for Alice" },
		{ id: 1, name: "Alice", profile$$id: 1, profile$$bio: "Bio for Alice (duplicate row)" },
	];

	// Both createHydrator and hasOneOrThrow keyBy omitted
	const hydrator = createHydrator<UserWithProfile>()
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (create) => create().fields({ id: true, bio: true }));

	const result = await hydrate(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0]?.profile, { id: 1, bio: "Bio for Alice" });
});

test("multiple nested levels: keyBy defaults to 'id' at all levels", async () => {
	type UserWithPostsAndComments = User & {
		posts$$id: number;
		posts$$title: string;
		posts$$comments$$id: number;
		posts$$comments$$content: string;
	};

	const data: UserWithPostsAndComments[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 1,
			posts$$comments$$content: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 2,
			posts$$comments$$content: "Comment 2",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 2,
			posts$$title: "Post 2",
			posts$$comments$$id: 3,
			posts$$comments$$content: "Comment 3",
		},
		{
			// Duplicate ids at EVERY level with differing non-key fields: if any
			// level defaulted to a non-id key, the entity counts below would grow
			id: 1,
			name: "Alice (duplicate row)",
			posts$$id: 1,
			posts$$title: "Post 1 (duplicate row)",
			posts$$comments$$id: 1,
			posts$$comments$$content: "Comment 1 (duplicate row)",
		},
	];

	// All keyBy parameters omitted
	const hydrator = createHydrator<UserWithPostsAndComments>()
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (create) =>
			create()
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (create) =>
					create().fields({ id: true, content: true }),
				),
		);

	const result = await hydrate(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[0], { id: 1, content: "Comment 1" });
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[1], { id: 2, content: "Comment 2" });
	assert.strictEqual(result[0]?.posts[1]?.comments.length, 1);
	assert.deepStrictEqual(result[0]?.posts[1]?.comments[0], { id: 3, content: "Comment 3" });
});
