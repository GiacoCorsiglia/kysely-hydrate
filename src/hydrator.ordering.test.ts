import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { type OrderBy, sortBy } from "./helpers/order-by.ts";
import { createHydrator } from "./hydrator.ts";

describe("Hydrator ordering", () => {
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

	it("should sort nested collections in nested mode", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[1]!.id, 2);
		assert.equal(result[0]!.posts[2]!.id, 3);
	});

	it("should not sort top-level in nested mode", async () => {
		const rows: User[] = [
			{ id: 3, username: "charlie", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
			{ id: 1, username: "alice", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
			{ id: 2, username: "bob", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		];

		const hydrator = createHydrator<User>().fields(["id", "username"]).orderBy("id", "asc");

		const result = await hydrator.hydrate(rows, { sort: "nested" });

		assert.equal(result.length, 3);
		// Should maintain original order (3, 1, 2) not sorted order
		assert.equal(result[0]!.id, 3);
		assert.equal(result[1]!.id, 1);
		assert.equal(result[2]!.id, 2);
	});

	it('should sort top-level when sort mode is "all"', async () => {
		const rows: User[] = [
			{ id: 3, username: "charlie", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
			{ id: 1, username: "alice", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
			{ id: 2, username: "bob", posts$$id: 0, posts$$title: "", posts$$user_id: 0 },
		];

		const hydrator = createHydrator<User>().fields(["id", "username"]).orderBy("id", "asc");

		const result = await hydrator.hydrate(rows, { sort: "all" });

		assert.equal(result.length, 3);
		// Should be sorted by id
		assert.equal(result[0]!.id, 1);
		assert.equal(result[1]!.id, 2);
		assert.equal(result[2]!.id, 3);
	});

	it('should not sort when sort mode is "none"', async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should maintain original order (3, 1, 2) not sorted order
		assert.equal(result[0]!.posts[0]!.id, 3);
		assert.equal(result[0]!.posts[1]!.id, 1);
		assert.equal(result[0]!.posts[2]!.id, 2);
	});

	it("should sort by multiple columns", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Priority 1: posts 2, 3 (sorted by id)
		assert.equal(result[0]!.posts[0]!.id, 2);
		assert.equal(result[0]!.posts[0]!.priority, 1);
		assert.equal(result[0]!.posts[1]!.id, 3);
		assert.equal(result[0]!.posts[1]!.priority, 1);
		// Priority 2: post 1
		assert.equal(result[0]!.posts[2]!.id, 1);
		assert.equal(result[0]!.posts[2]!.priority, 2);
	});

	it("should use orderByKeys as final tie-breaker", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should be sorted by id (the keyBy) as tie-breaker
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[1]!.id, 2);
		assert.equal(result[0]!.posts[2]!.id, 3);
	});

	it("orderByKeys should always be last even when called before orderBy", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should be sorted by title first, then by id as tie-breaker
		// Two posts with title "A": should be sorted by id (1, 2)
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[0]!.title, "A");
		assert.equal(result[0]!.posts[1]!.id, 2);
		assert.equal(result[0]!.posts[1]!.title, "A");
		assert.equal(result[0]!.posts[2]!.id, 3);
		assert.equal(result[0]!.posts[2]!.title, "C");
	});

	it("orderByKeys should work after combining hydrators with .with()", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should be sorted by title first, then by id as tie-breaker
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[1]!.id, 2);
		assert.equal(result[0]!.posts[2]!.id, 3);
	});

	it("orderByKeys should be preserved through .with() when the other hydrator never set it", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should be sorted by id (the keyBy) from the base hydrator's orderByKeys
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[1]!.id, 2);
		assert.equal(result[0]!.posts[2]!.id, 3);
	});

	it("with(): the other hydrator's explicit orderByKeys(false) takes precedence", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// No ordering: posts keep their input order
		assert.equal(result[0]!.posts[0]!.id, 3);
		assert.equal(result[0]!.posts[1]!.id, 1);
		assert.equal(result[0]!.posts[2]!.id, 2);
	});

	it("should handle nulls correctly with nulls first", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Nulls should come first
		assert.equal(result[0]!.posts[0]!.id, 1);
		assert.equal(result[0]!.posts[0]!.title, null);
		assert.equal(result[0]!.posts[1]!.title, "A");
		assert.equal(result[0]!.posts[2]!.title, "C");
	});

	it("should handle nulls correctly with nulls last", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Nulls should come last
		assert.equal(result[0]!.posts[0]!.title, "A");
		assert.equal(result[0]!.posts[1]!.title, "C");
		assert.equal(result[0]!.posts[2]!.id, 1);
		assert.equal(result[0]!.posts[2]!.title, null);
	});

	it("should support ordering by computed values using functions", async () => {
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

		assert.equal(result.length, 1);
		assert.equal(result[0]!.posts.length, 3);
		// Should be sorted case-insensitively: Apple, banana, zebra
		assert.equal(result[0]!.posts[0]!.title, "Apple");
		assert.equal(result[0]!.posts[1]!.title, "banana");
		assert.equal(result[0]!.posts[2]!.title, "zebra");
	});
});

// Hydrators group rows before sorting them, sorting one row per entity, and
// sort every row only when an entity's rows disagree on an ordering.  Either
// way the result must be what sorting the rows, then grouping them, gives.
describe("Hydrator ordering: entities ordered as their sorted rows would be", () => {
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
	 * Rows for up to 6 entities, repeated across children.  With `consistent`,
	 * an entity's rows agree on `rank` and `name`; otherwise they may not.
	 */
	const makeRows = (seed: number, consistent: boolean): Row[] => {
		const next = random(seed);
		const pick = (n: number) => Math.floor(next() * n);
		const rankOf = () => (pick(4) === 0 ? null : pick(3));
		const ranks = [0, 1, 2, 3, 4, 5, 6].map(rankOf);
		return Array.from({ length: 4 + pick(16) }, () => {
			const id = 1 + pick(6);
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
			it(`${name}, ${consistent ? "consistent" : "inconsistent"} rows`, async () => {
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
					assert.deepEqual(
						await hydrator.hydrate(rows, { sort: "all" }),
						await hydrator.hydrate(sorted, { sort: "none" }),
						`seed ${seed}`,
					);
					// Whether some entity's rows disagree, so it takes the other path.
					sortedEveryRow ||= rows.some((row) =>
						rows.some(
							(other) =>
								other.id === row.id &&
								orderings.some(({ key }) =>
									typeof key === "function" ? key(row) !== key(other) : row[key] !== other[key],
								),
						),
					);
				}
				assert.equal(sortedEveryRow, !consistent);
			});
		}
	}

	it("an entity whose rows disagree orders by its first sorted row", async () => {
		const rows = [
			{ category: "A", price: 5, item$$id: 1 },
			{ category: "B", price: 3, item$$id: 2 },
			{ category: "A", price: 1, item$$id: 3 },
		];
		const hydrator = createHydrator<(typeof rows)[number]>("category")
			.fields({ category: true, price: true })
			.hasMany("items", "item$$", (h) => h("id").fields({ id: true }))
			.orderBy("price");

		assert.deepEqual(await hydrator.hydrate(rows, { sort: "all" }), [
			{ category: "A", price: 1, items: [{ id: 3 }, { id: 1 }] },
			{ category: "B", price: 3, items: [{ id: 2 }] },
		]);
	});
});
