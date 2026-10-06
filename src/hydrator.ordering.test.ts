import assert from "node:assert";
import { test } from "node:test";

import { type OrderBy, sortBy } from "./helpers/order-by.ts";
import { createHydrator } from "./hydrator.ts";

interface Post {
	id: number;
	title: string;
	user_id: number;
}

interface User {
	id: number;
	username: string;
	posts$$id: number;
	posts$$title: string;
	posts$$user_id: number;
}

test("ordering: sorts nested collections in nested mode", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Post 3", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("id", "asc"),
		);

	const result = await hydrator.hydrate(rows, { sort: "nested" });

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[1]!.id, 2);
	assert.strictEqual(result[0]!.posts[2]!.id, 3);
});

test("ordering: does not sort top-level in nested mode", async () => {
	const rows: User[] = [
		{ id: 3, username: "charlie", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		{ id: 1, username: "alice", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		{ id: 2, username: "bob", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
	];

	const hydrator = createHydrator<User>().fields(["id", "username"]).orderBy("id", "asc");

	const result = await hydrator.hydrate(rows, { sort: "nested" });

	assert.strictEqual(result.length, 3);
	// Should maintain original order (3, 1, 2) not sorted order
	assert.strictEqual(result[0]!.id, 3);
	assert.strictEqual(result[1]!.id, 1);
	assert.strictEqual(result[2]!.id, 2);
});

test('ordering: sorts top-level when sort mode is "all"', async () => {
	const rows: User[] = [
		{ id: 3, username: "charlie", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		{ id: 1, username: "alice", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		{ id: 2, username: "bob", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
	];

	const hydrator = createHydrator<User>().fields(["id", "username"]).orderBy("id", "asc");

	const result = await hydrator.hydrate(rows, { sort: "all" });

	assert.strictEqual(result.length, 3);
	// Should be sorted by id
	assert.strictEqual(result[0]!.id, 1);
	assert.strictEqual(result[1]!.id, 2);
	assert.strictEqual(result[2]!.id, 3);
});

test('ordering: does not sort when sort mode is "none"', async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Post 3", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("id", "asc"),
		);

	const result = await hydrator.hydrate(rows, { sort: "none" });

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should maintain original order (3, 1, 2) not sorted order
	assert.strictEqual(result[0]!.posts[0]!.id, 3);
	assert.strictEqual(result[0]!.posts[1]!.id, 1);
	assert.strictEqual(result[0]!.posts[2]!.id, 2);
});

test("ordering: sorts by multiple columns", async () => {
	interface UserWithPriority {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string;
		posts$$user_id: number;
		posts$$priority: number;
	}

	const rows: UserWithPriority[] = [
		{
			id: 1,
			username: "alice",
			posts$$id: 3,
			posts$$title: "Post 3",
			posts$$user_id: 1,
			posts$$priority: 1,
		},
		{
			id: 1,
			username: "alice",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$user_id: 1,
			posts$$priority: 2,
		},
		{
			id: 1,
			username: "alice",
			posts$$id: 2,
			posts$$title: "Post 2",
			posts$$user_id: 1,
			posts$$priority: 1,
		},
	];

	const hydrator = createHydrator<UserWithPriority>()
		.fields(["id", "username"])
		.hasMany(
			"posts",
			"posts$$",
			(create) =>
				create()
					.fields(["id", "title", "user_id", "priority"])
					.orderBy("priority", "asc") // First by priority
					.orderBy("id", "asc"), // Then by id
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Priority 1: posts 2, 3 (sorted by id)
	assert.strictEqual(result[0]!.posts[0]!.id, 2);
	assert.strictEqual(result[0]!.posts[0]!.priority, 1);
	assert.strictEqual(result[0]!.posts[1]!.id, 3);
	assert.strictEqual(result[0]!.posts[1]!.priority, 1);
	// Priority 2: post 1
	assert.strictEqual(result[0]!.posts[2]!.id, 1);
	assert.strictEqual(result[0]!.posts[2]!.priority, 2);
});

test("ordering: uses orderByKeys as final tie-breaker", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Same", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany(
			"posts",
			"posts$$",
			(create) =>
				create()
					.fields(["id", "title", "user_id"])
					.orderBy("title", "asc") // All titles are the same
					.orderByKeys(), // Use id as tie-breaker
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should be sorted by id (the keyBy) as tie-breaker
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[1]!.id, 2);
	assert.strictEqual(result[0]!.posts[2]!.id, 3);
});

test("ordering: orderByKeys is always last even when called before orderBy", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "C", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "A", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "A", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany(
			"posts",
			"posts$$",
			(create) =>
				create()
					.fields(["id", "title", "user_id"])
					.orderByKeys() // Called first
					.orderBy("title", "asc"), // But this should take priority
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should be sorted by title first, then by id as tie-breaker
	// Two posts with title "A": should be sorted by id (1, 2)
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[0]!.title, "A");
	assert.strictEqual(result[0]!.posts[1]!.id, 2);
	assert.strictEqual(result[0]!.posts[1]!.title, "A");
	assert.strictEqual(result[0]!.posts[2]!.id, 3);
	assert.strictEqual(result[0]!.posts[2]!.title, "C");
});

test("ordering: orderByKeys works after combining hydrators with .with()", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Same", posts$$user_id: 1 },
	];

	const baseHydrator = createHydrator<Post>().fields(["id", "title", "user_id"]).orderByKeys();

	const extendedHydrator = createHydrator<Post>().orderBy("title", "asc").with(baseHydrator);

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", extendedHydrator);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should be sorted by title first, then by id as tie-breaker
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[1]!.id, 2);
	assert.strictEqual(result[0]!.posts[2]!.id, 3);
});

test("ordering: orderByKeys is preserved through .with() when the other hydrator never set it", async () => {
	// Regression test: .with() always took the other hydrator's orderByKeys,
	// even when the other hydrator never called .orderByKeys() and just had
	// the default.  An unset value on the other side must not override an
	// explicit setting on this side.
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Same", posts$$user_id: 1 },
	];

	const baseHydrator = createHydrator<Post>().fields(["id", "title", "user_id"]).orderByKeys();
	const otherHydrator = createHydrator<Post>().fields(["title"]); // No orderByKeys call.
	const combined = baseHydrator.with(otherHydrator);

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", combined);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should be sorted by id (the keyBy) from the base hydrator's orderByKeys
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[1]!.id, 2);
	assert.strictEqual(result[0]!.posts[2]!.id, 3);
});

test("ordering: with() — the other hydrator's explicit orderByKeys(false) takes precedence", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Same", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Same", posts$$user_id: 1 },
	];

	const baseHydrator = createHydrator<Post>().fields(["id", "title", "user_id"]).orderByKeys();
	const otherHydrator = createHydrator<Post>().orderByKeys(false); // Explicitly disabled.
	const combined = baseHydrator.with(otherHydrator);

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", combined);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// No ordering: posts keep their input order
	assert.strictEqual(result[0]!.posts[0]!.id, 3);
	assert.strictEqual(result[0]!.posts[1]!.id, 1);
	assert.strictEqual(result[0]!.posts[2]!.id, 2);
});

test("ordering: handles nulls correctly with nulls first", async () => {
	interface UserWithNullablePosts {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string | null;
		posts$$user_id: number;
	}

	const rows: UserWithNullablePosts[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "C", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: null, posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "A", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<UserWithNullablePosts>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("title", "asc", "first"),
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Nulls should come first
	assert.strictEqual(result[0]!.posts[0]!.id, 1);
	assert.strictEqual(result[0]!.posts[0]!.title, null);
	assert.strictEqual(result[0]!.posts[1]!.title, "A");
	assert.strictEqual(result[0]!.posts[2]!.title, "C");
});

test("ordering: handles nulls correctly with nulls last", async () => {
	interface UserWithNullablePosts {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string | null;
		posts$$user_id: number;
	}

	const rows: UserWithNullablePosts[] = [
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "C", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: null, posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "A", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<UserWithNullablePosts>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("title", "asc", "last"),
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Nulls should come last
	assert.strictEqual(result[0]!.posts[0]!.title, "A");
	assert.strictEqual(result[0]!.posts[1]!.title, "C");
	assert.strictEqual(result[0]!.posts[2]!.id, 1);
	assert.strictEqual(result[0]!.posts[2]!.title, null);
});

test("ordering: descending order places nulls first by default", async () => {
	// nullsDefault(desc) === "first": with no explicit nulls argument, a desc
	// ordering must put null values ahead of the non-null ones.
	interface UserWithNullablePosts {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string | null;
		posts$$user_id: number;
	}

	const rows: UserWithNullablePosts[] = [
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "A", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: null, posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "C", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<UserWithNullablePosts>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("title", "desc"),
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result[0]!.posts.length, 3);
	assert.deepStrictEqual(
		result[0]!.posts.map((p) => p.title),
		[null, "C", "A"],
	);
});

test("ordering: sorts a nested collection descending", async () => {
	const rows: User[] = [
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Post 3", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("id", "desc"),
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(
		result[0]!.posts.map((p) => p.id),
		[3, 2, 1],
	);
});

test("ordering: sorts each parent's nested collection independently across multiple parents", async () => {
	// Multiple parents (listed out of order), each with its own unsorted posts:
	// the top-level array sorts by id and every parent's posts sort independently.
	const rows: User[] = [
		{ id: 2, username: "bob", posts$$id: 5, posts$$title: "Post 5", posts$$user_id: 2 },
		{ id: 2, username: "bob", posts$$id: 4, posts$$title: "Post 4", posts$$user_id: 2 },
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Post 3", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<User>()
		.fields(["id", "username"])
		.orderBy("id", "asc")
		.hasMany("posts", "posts$$", (create) =>
			create().fields(["id", "title", "user_id"]).orderBy("id", "asc"),
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(
		result.map((u) => u.id),
		[1, 2],
	);
	assert.deepStrictEqual(
		result[0]!.posts.map((p) => p.id),
		[1, 2, 3],
	);
	assert.deepStrictEqual(
		result[1]!.posts.map((p) => p.id),
		[4, 5],
	);
});

test("ordering: sorts a doubly-nested collection by its own column", async () => {
	// The ordering applies at the deepest (a$$b$$col) level: comments sort
	// independently of how posts or users are ordered.
	interface DeepRow {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string;
		posts$$comments$$id: number;
		posts$$comments$$body: string;
	}

	const rows: DeepRow[] = [
		{
			id: 1,
			username: "alice",
			posts$$id: 10,
			posts$$title: "Post",
			posts$$comments$$id: 3,
			posts$$comments$$body: "c3",
		},
		{
			id: 1,
			username: "alice",
			posts$$id: 10,
			posts$$title: "Post",
			posts$$comments$$id: 1,
			posts$$comments$$body: "c1",
		},
		{
			id: 1,
			username: "alice",
			posts$$id: 10,
			posts$$title: "Post",
			posts$$comments$$id: 2,
			posts$$comments$$body: "c2",
		},
	];

	const hydrator = createHydrator<DeepRow>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create()
				.fields(["id", "title"])
				.hasMany("comments", "comments$$", (c) => c().fields(["id", "body"]).orderBy("id", "desc")),
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result[0]!.posts.length, 1);
	assert.deepStrictEqual(
		result[0]!.posts[0]!.comments.map((c) => c.id),
		[3, 2, 1],
	);
});

test("ordering: supports ordering by computed values using functions", async () => {
	interface UserWithPosts {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string;
		posts$$user_id: number;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, username: "alice", posts$$id: 1, posts$$title: "zebra", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Apple", posts$$user_id: 1 },
		{ id: 1, username: "alice", posts$$id: 3, posts$$title: "banana", posts$$user_id: 1 },
	];

	const hydrator = createHydrator<UserWithPosts>()
		.fields(["id", "username"])
		.hasMany("posts", "posts$$", (create) =>
			create()
				.fields(["id", "title", "user_id"])
				// Sort by lowercase title to get case-insensitive ordering
				.orderBy((post) => post.title.toLowerCase(), "asc"),
		);

	const result = await hydrator.hydrate(rows);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.posts.length, 3);
	// Should be sorted case-insensitively: Apple, banana, zebra
	assert.strictEqual(result[0]!.posts[0]!.title, "Apple");
	assert.strictEqual(result[0]!.posts[1]!.title, "banana");
	assert.strictEqual(result[0]!.posts[2]!.title, "zebra");
});

// Hydrators group rows before sorting them, sorting one row per entity, and
// sort every row only when an entity's rows disagree on an ordering.  Either
// way the result must be what sorting the rows, then grouping them, gives.
interface Row {
	id: number;
	rank: number | null;
	name: string;
	child$$id: number;
}

/** A seeded generator (mulberry32), so failures reproduce. */
const random = (seed: number) => () => {
	seed = (seed + 0x6d2b79f5) >>> 0;
	let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
	t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
	return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

/**
 * Rows for 1 to 6 entities, repeated across children.  With `consistent`,
 * an entity's rows agree on `rank` and `name`; otherwise they may not.
 */
const makeRows = (seed: number, consistent: boolean): Row[] => {
	const next = random(seed);
	const pick = (n: number) => Math.floor(next() * n);
	const rankOf = () => (pick(4) === 0 ? null : pick(3));
	const ranks = [0, 1, 2, 3, 4, 5, 6].map(rankOf);
	// A lone entity too: its rows still get sorted when they disagree.
	const entities = 1 + pick(6);
	return Array.from({ length: 4 + pick(16) }, () => {
		const id = 1 + pick(entities);
		return {
			id,
			rank: consistent ? ranks[id]! : rankOf(),
			name: consistent ? `n${id % 3}` : `n${pick(3)}`,
			child$$id: 1 + pick(4),
		};
	});
};

const keys: OrderBy<Row> = { key: "id", direction: "asc", nulls: "last" };
const orderingsCases: [string, OrderBy<Row>[]][] = [
	["rank asc", [{ key: "rank", direction: "asc" }]],
	["rank desc, nulls first", [{ key: "rank", direction: "desc", nulls: "first" }]],
	[
		"name, then rank desc",
		[
			{ key: "name", direction: "asc" },
			{ key: "rank", direction: "desc" },
		],
	],
	["a function key", [{ key: (row) => (row.rank ?? 0) % 2, direction: "asc" }]],
	// What orderByKeys() appends.
	["rank, then keys", [{ key: "rank", direction: "asc" }, keys]],
];

for (const [name, orderings] of orderingsCases) {
	for (const consistent of [true, false]) {
		test(`ordering: ${name}, ${consistent ? "consistent" : "inconsistent"} rows`, async () => {
			const hydrator = orderings.reduce(
				(h, { key, direction, nulls }) => h.orderBy(key, direction, nulls),
				createHydrator<Row>("id")
					.fields({ id: true, rank: true, name: true })
					// Children keep their rows' order, so they show which rows each
					// entity got, in what order, and which row stood for it.
					.hasMany("children", "child$$", (h) => h("id").fields({ id: true })),
			);
			let sortedEveryRow = false;
			for (let seed = 1; seed <= 200; seed++) {
				const rows = makeRows(seed, consistent);
				const sorted = sortBy(rows, orderings);
				assert.deepStrictEqual(
					await hydrator.hydrate(rows, { sort: "all" }),
					await hydrator.hydrate(sorted, { sort: "none" }),
					`seed ${seed}`,
				);
				// Whether some entity's rows disagree, so it takes the other path.
				const valueOf = (row: Row, key: OrderBy<Row>["key"]) =>
					typeof key === "function" ? key(row) : row[key];
				sortedEveryRow ||= rows.some((row) =>
					rows.some(
						(other) =>
							other.id === row.id &&
							orderings.some(({ key }) => valueOf(row, key) !== valueOf(other, key)),
					),
				);
			}
			assert.strictEqual(sortedEveryRow, !consistent);
		});
	}
}

test("ordering: an entity whose rows disagree orders by its first sorted row", async () => {
	const rows = [
		{ category: "A", price: 5, item$$id: 1 },
		{ category: "B", price: 3, item$$id: 2 },
		{ category: "A", price: 1, item$$id: 3 },
	];
	const hydrator = createHydrator<(typeof rows)[number]>("category")
		.fields({ category: true, price: true })
		.hasMany("items", "item$$", (h) => h("id").fields({ id: true }))
		.orderBy("price");

	assert.deepStrictEqual(await hydrator.hydrate(rows, { sort: "all" }), [
		{ category: "A", price: 1, items: [{ id: 3 }, { id: 1 }] },
		{ category: "B", price: 3, items: [{ id: 2 }] },
	]);
});

test("ordering: a lone entity whose rows disagree is built from its first sorted row", async () => {
	const rows = [
		{ category: "A", price: 5, item$$id: 1 },
		{ category: "A", price: 1, item$$id: 2 },
	];
	const hydrator = createHydrator<(typeof rows)[number]>("category")
		.fields({ category: true, price: true })
		.hasMany("items", "item$$", (h) => h("id").fields({ id: true }))
		.orderBy("price");

	assert.deepStrictEqual(await hydrator.hydrate(rows, { sort: "all" }), [
		{ category: "A", price: 1, items: [{ id: 2 }, { id: 1 }] },
	]);
});
