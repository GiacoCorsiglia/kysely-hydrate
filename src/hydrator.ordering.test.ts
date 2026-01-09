import assert from "node:assert/strict";
import { describe, it } from "node:test";

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

	it("orderByKeys should work after extend", async () => {
		const rows: User[] = [
			{ id: 1, username: "alice", posts$$id: 3, posts$$title: "Same", posts$$user_id: 1 },
			{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Same", posts$$user_id: 1 },
			{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Same", posts$$user_id: 1 },
		];

		const baseHydrator = createHydrator<Post>().fields(["id", "title", "user_id"]).orderByKeys();

		const extendedHydrator = createHydrator<Post>().orderBy("title", "asc").extend(baseHydrator);

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
