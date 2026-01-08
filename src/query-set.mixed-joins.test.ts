import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 6: Mixed Joins Tests
//

// Multiple cardinality-one joins

test("mixed: multiple innerJoinOne on same QuerySet", async () => {
	// Use specific post IDs to ensure exactly one post per user
	// User 2 (bob) -> post 1, User 3 (carol) -> post 3, User 4 (dave) -> post 4
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinOne(
			"primaryPost",
			(init) =>
				init((eb) =>
					eb
						.selectFrom("posts")
						.select(["id", "title", "user_id"])
						.where("id", "in", [1, 3, 4]), // Exactly one post per user
				),
			"primaryPost.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
			primaryPost: { id: 1, title: "Post 1", user_id: 2 },
		},
		{
			id: 3,
			username: "carol",
			profile: { id: 3, bio: "Bio for user 3", user_id: 3 },
			primaryPost: { id: 3, title: "Post 3", user_id: 3 },
		},
		{
			id: 4,
			username: "dave",
			profile: { id: 4, bio: "Bio for user 4", user_id: 4 },
			primaryPost: { id: 4, title: "Post 4", user_id: 4 },
		},
	]);
});

test("mixed: toJoinedQuery with multiple innerJoinOne shows all prefixed columns", async () => {
	const rows = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinOne(
			"primaryPost",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 4),
				),
			"primaryPost.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.toJoinedQuery()
		.execute();

	// Bob appears twice because he has 2 posts with id <= 4 (cardinality violation for innerJoinOne)
	assert.strictEqual(rows.length, 4);
	assert.deepStrictEqual(rows, [
		{
			id: 2,
			username: "bob",
			profile$$id: 2,
			profile$$bio: "Bio for user 2",
			profile$$user_id: 2,
			primaryPost$$id: 1,
			primaryPost$$title: "Post 1",
			primaryPost$$user_id: 2,
		},
		{
			id: 2,
			username: "bob",
			profile$$id: 2,
			profile$$bio: "Bio for user 2",
			profile$$user_id: 2,
			primaryPost$$id: 2,
			primaryPost$$title: "Post 2",
			primaryPost$$user_id: 2,
		},
		{
			id: 3,
			username: "carol",
			profile$$id: 3,
			profile$$bio: "Bio for user 3",
			profile$$user_id: 3,
			primaryPost$$id: 3,
			primaryPost$$title: "Post 3",
			primaryPost$$user_id: 3,
		},
		{
			id: 4,
			username: "dave",
			profile$$id: 4,
			profile$$bio: "Bio for user 4",
			profile$$user_id: 4,
			primaryPost$$id: 4,
			primaryPost$$title: "Post 4",
			primaryPost$$user_id: 4,
		},
	]);
});

test("mixed: leftJoinOne and innerJoinOne together", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) =>
				init(
					(eb) =>
						eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No match
				),
			"profile.user_id",
			"user.id",
		)
		.innerJoinOne(
			"primaryPost",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 4),
				),
			"primaryPost.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			profile: null, // Left join with no match
			primaryPost: { id: 1, title: "Post 1", user_id: 2 },
		},
		{
			id: 3,
			username: "carol",
			profile: null, // Left join with no match
			primaryPost: { id: 3, title: "Post 3", user_id: 3 },
		},
		{
			id: 4,
			username: "dave",
			profile: null, // Left join with no match
			primaryPost: { id: 4, title: "Post 4", user_id: 4 },
		},
	]);
});

// Multiple cardinality-many joins

test("mixed: multiple innerJoinMany on same QuerySet with cartesian product", async () => {
	// When multiple innerJoinMany are used, cartesian products cause row multiplication in SQL
	// Post 1: 2 comments × 1 user = 2 rows, but hydrator deduplicates sibling collections
	// Post 2: 1 comment × 1 user = 1 row
	const posts = await querySet(db)
		.init("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
		.innerJoinMany(
			"comments",
			(init) => init((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
			"comments.post_id",
			"post.id",
		)
		.innerJoinMany(
			"users",
			(init) => init((eb) => eb.selectFrom("users").select(["id", "username"])),
			"users.id",
			"post.user_id",
		)
		.where("posts.id", "<=", 2)
		.execute();

	const raw = [
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments$$id: 1,
			comments$$content: "Comment 1 on post 1",
			comments$$post_id: 1,
			users$$id: 2,
			users$$username: "bob",
		},
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments$$id: 2,
			comments$$content: "Comment 2 on post 1",
			comments$$post_id: 1,
			users$$id: 2,
			users$$username: "bob",
		},
		{
			id: 2,
			title: "Post 2",
			user_id: 2,
			comments$$id: 3,
			comments$$content: "Comment 3 on post 2",
			comments$$post_id: 2,
			users$$id: 2,
			users$$username: "bob",
		},
	];

	// Verify basic structure
	assert.strictEqual(posts.length, 2);
	assert.strictEqual(posts[0]?.comments.length, 2);
	assert.strictEqual(posts[1]?.comments.length, 1);

	// IMPORTANT: Sibling hasMany collections are deduplicated based on keyBy
	// Even though SQL produces 2 rows for Post 1 (cartesian product), the hydrator
	// deduplicates the users array to only contain 1 bob entry
	assert.strictEqual(posts[0]?.users.length, 1);
	assert.strictEqual(posts[1]?.users.length, 1);

	// Full result validation
	assert.deepStrictEqual(posts, [
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments: [
				{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
				{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
			],
			users: [
				{ id: 2, username: "bob" }, // Only appears once (deduplicated)
			],
		},
		{
			id: 2,
			title: "Post 2",
			user_id: 2,
			comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
			users: [{ id: 2, username: "bob" }],
		},
	]);
});

test("mixed: toJoinedQuery with multiple innerJoinMany shows row explosion", async () => {
	const rows = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 3),
				),
			"posts.user_id",
			"user.id",
		)
		.innerJoinMany(
			"profiles",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profiles.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.toJoinedQuery()
		.execute();

	// Row explosion: bob (2 posts × 1 profile = 2 rows) + carol (1 post × 1 profile = 1 row) = 3 rows
	assert.strictEqual(rows.length, 3);
	assert.deepStrictEqual(rows, [
		{
			id: 2,
			username: "bob",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$user_id: 2,
			profiles$$id: 2,
			profiles$$bio: "Bio for user 2",
			profiles$$user_id: 2,
		},
		{
			id: 2,
			username: "bob",
			posts$$id: 2,
			posts$$title: "Post 2",
			posts$$user_id: 2,
			profiles$$id: 2,
			profiles$$bio: "Bio for user 2",
			profiles$$user_id: 2,
		},
		{
			id: 3,
			username: "carol",
			posts$$id: 3,
			posts$$title: "Post 3",
			posts$$user_id: 3,
			profiles$$id: 3,
			profiles$$bio: "Bio for user 3",
			profiles$$user_id: 3,
		},
	]);
});

// Mix of cardinality-one and cardinality-many joins

test("mixed: innerJoinOne and innerJoinMany together", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[1]?.posts.length, 2);
	assert.strictEqual(users[2]?.posts.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
		{
			id: 3,
			username: "carol",
			profile: { id: 3, bio: "Bio for user 3", user_id: 3 },
			posts: [
				{ id: 3, title: "Post 3", user_id: 3 },
				{ id: 15, title: "Post 15", user_id: 3 },
			],
		},
		{
			id: 4,
			username: "dave",
			profile: { id: 4, bio: "Bio for user 4", user_id: 4 },
			posts: [
				{ id: 4, title: "Post 4", user_id: 4 },
				{ id: 13, title: "Post 13", user_id: 4 },
			],
		},
	]);
});

test("mixed: leftJoinOne and leftJoinMany together", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.leftJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 2)
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
			posts: [], // Alice has no posts
		},
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("mixed: executeCount with multiple joins counts unique base records", async () => {
	const count = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.executeCount(Number);

	// Users with profiles AND posts: bob (2), carol (3), dave (4)
	assert.strictEqual(count, 3);
});

test("mixed: pagination with multiple joins uses nested subquery", async () => {
	const query = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.limit(2);

	const users = await query.execute();
	const allUsers = await query.clearLimit().execute();

	// Should return first 2 users with ALL their data
	assert.strictEqual(users.length, 2);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(0, 2));
});

// toQuery() vs toJoinedQuery() - should be same without pagination

test("mixed: toQuery without pagination equals toJoinedQuery for cardinality-one", async () => {
	const base = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinOne(
			"primaryPost",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 4),
				),
			"primaryPost.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4);

	const queryRows = await base.toQuery().execute();
	const joinedRows = await base.toJoinedQuery().execute();

	// Without pagination, toQuery() should equal toJoinedQuery()
	assert.deepStrictEqual(queryRows, joinedRows);
});

test("mixed: toQuery without pagination equals toJoinedQuery for cardinality-many", async () => {
	const base = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 3),
				),
			"posts.user_id",
			"user.id",
		)
		.innerJoinMany(
			"profiles",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profiles.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const queryRows = await base.toQuery().execute();
	const joinedRows = await base.toJoinedQuery().execute();

	// Without pagination, toQuery() should equal toJoinedQuery()
	assert.deepStrictEqual(queryRows, joinedRows);
});

test("mixed: toQuery without pagination equals toJoinedQuery for mixed joins", async () => {
	const base = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const queryRows = await base.toQuery().execute();
	const joinedRows = await base.toJoinedQuery().execute();

	// Without pagination, toQuery() should equal toJoinedQuery()
	assert.deepStrictEqual(queryRows, joinedRows);
});
