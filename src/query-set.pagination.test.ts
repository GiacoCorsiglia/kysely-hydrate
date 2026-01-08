import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 5: Pagination Tests
//

// Basic pagination without joins

test("pagination: limit without joins", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
	]);
});

test("pagination: offset without joins", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.limit(1000) // SQLite requires LIMIT when using OFFSET
		.offset(7)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.deepStrictEqual(users, [
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});

test("pagination: limit and offset without joins", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.offset(2)
		.execute();

	assert.strictEqual(users.length, 3);
	assert.deepStrictEqual(users, [
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
		{ id: 5, username: "eve" },
	]);
});

// Pagination with cardinality-one joins

test("pagination: limit with innerJoinOne", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.limit(2)
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
		},
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
		},
	]);
});

test("pagination: limit and offset with leftJoinOne", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.limit(2)
		.offset(1)
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
		},
		{
			id: 3,
			username: "carol",
			profile: { id: 3, bio: "Bio for user 3", user_id: 3 },
		},
	]);
});

// Pagination with cardinality-many joins (should use nested subquery)

test("pagination: limit with innerJoinMany returns limited users with ALL their posts", async () => {
	// User 2 has 4 posts, User 3 has 2 posts
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(1)
		.execute();

	// Should return only user 2, but with ALL 4 of their posts
	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("pagination: limit with leftJoinMany returns limited users with ALL their posts", async () => {
	// User 1 has no posts, User 2 has 4 posts, User 3 has 2 posts
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2)
		.execute();

	// Should return users 1 and 2, with user 2 having all 4 posts
	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			posts: [],
		},
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("pagination: offset with innerJoinMany skips base records correctly", async () => {
	// User 2 has 4 posts, User 3 has 2 posts
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(1000) // SQLite requires LIMIT when using OFFSET
		.offset(1)
		.execute();

	// Should skip user 2, return only user 3 with their 2 posts
	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 3,
			username: "carol",
			posts: [
				{ id: 3, title: "Post 3", user_id: 3 },
				{ id: 15, title: "Post 15", user_id: 3 },
			],
		},
	]);
});

test("pagination: limit and offset with innerJoinMany", async () => {
	// Get users with posts, starting from the 2nd user
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.limit(2)
		.offset(1)
		.execute();

	// Should return users 3 and 4 with all their posts
	assert.strictEqual(users.length, 2);
	assert.ok(users[0]?.id === 3);
	assert.ok(users[1]?.id === 4);
});

// executeCount and executeExists should ignore pagination

test("pagination: executeCount ignores limit/offset", async () => {
	const count = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.offset(2)
		.executeCount(Number);

	// Should count all 10 users, not just the 3 in the page
	assert.strictEqual(count, 10);
});

test("pagination: executeExists ignores limit/offset", async () => {
	const exists = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", ">", 100) // No users match
		.limit(1)
		.executeExists();

	// Should return false because no users match, even with limit
	assert.strictEqual(exists, false);
});

test("pagination: executeCount with joins ignores limit/offset", async () => {
	const count = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(1)
		.executeCount(Number);

	// Should count 2 users (users 2 and 3 have posts), not just 1
	assert.strictEqual(count, 2);
});

// clearLimit and clearOffset

test("pagination: clearLimit removes limit", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.clearLimit()
		.execute();

	// Should return all 10 users
	assert.strictEqual(users.length, 10);
});

test("pagination: clearOffset removes offset", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.offset(7)
		.clearOffset()
		.execute();

	// Should return all 10 users starting from id 1
	assert.strictEqual(users.length, 10);
	assert.strictEqual(users[0]?.id, 1);
});
