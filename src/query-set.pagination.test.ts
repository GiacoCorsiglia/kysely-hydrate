import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 5: Pagination Tests
//

// Basic pagination without joins

test("pagination: limit without joins", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3);

	const users = await query.execute();
	const allUsers = await query.clearLimit().execute();

	// Paginated results should be subset of full results
	assert.strictEqual(users.length, 3);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(0, 3));
});

test("pagination: offset without joins", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(1000) // SQLite requires LIMIT when using OFFSET
		.offset(7);

	const users = await query.execute();
	const allUsers = await query.clearLimit().clearOffset().execute();

	// Paginated results should be subset of full results
	assert.strictEqual(users.length, 3);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(7));
});

test("pagination: limit and offset without joins", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.offset(2);

	const users = await query.execute();
	const allUsers = await query.clearLimit().clearOffset().execute();

	// Paginated results should be subset of full results
	assert.strictEqual(users.length, 3);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(2, 5));
});

// Pagination with cardinality-one joins

test("pagination: limit with innerJoinOne", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.limit(2);

	const users = await query.execute();
	const allUsers = await query.clearLimit().execute();

	// Paginated results should be subset of full results
	assert.strictEqual(users.length, 2);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(0, 2));
});

test("pagination: limit and offset with leftJoinOne", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.limit(2)
		.offset(1);

	const users = await query.execute();
	const allUsers = await query.clearLimit().clearOffset().execute();

	// Paginated results should be subset of full results
	assert.strictEqual(users.length, 2);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(1, 3));
});

// Pagination with cardinality-many joins (should use nested subquery)

test("pagination: limit with innerJoinMany returns limited users with ALL their posts", async () => {
	// User 2 has 4 posts, User 3 has 2 posts
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(1);

	const users = await query.execute();
	const allUsers = await query.clearLimit().execute();

	// Should return only user 2, but with ALL 4 of their posts
	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.ok(users.length < allUsers.length);
	// First user in paginated results should match first user in full results
	assert.deepStrictEqual(users[0], allUsers[0]);
});

test("pagination: limit with leftJoinMany returns limited users with ALL their posts", async () => {
	// User 1 has no posts, User 2 has 4 posts, User 3 has 2 posts
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2);

	const users = await query.execute();
	const allUsers = await query.clearLimit().execute();

	// Should return users 1 and 2, with user 2 having all 4 posts
	assert.strictEqual(users.length, 2);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(0, 2));
});

test("pagination: offset with innerJoinMany skips base records correctly", async () => {
	// User 2 has 4 posts, User 3 has 2 posts
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(1000) // SQLite requires LIMIT when using OFFSET
		.offset(1);

	const users = await query.execute();
	const allUsers = await query.clearLimit().clearOffset().execute();

	// Should skip user 2, return only user 3 with their 2 posts
	assert.strictEqual(users.length, 1);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(1));
});

test("pagination: limit and offset with innerJoinMany", async () => {
	// Get users with posts, starting from the 2nd user
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 4)
		.limit(2)
		.offset(1);

	const users = await query.execute();
	const allUsers = await query.clearLimit().clearOffset().execute();

	// Should return users 3 and 4 with all their posts
	assert.strictEqual(users.length, 2);
	assert.ok(users.length < allUsers.length);
	assert.deepStrictEqual(users, allUsers.slice(1, 3));
});

// executeCount and executeExists should ignore pagination

test("pagination: executeCount ignores limit/offset", async () => {
	const count = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.offset(2)
		.executeCount(Number);

	// Should count all 10 users, not just the 3 in the page
	assert.strictEqual(count, 10);
});

test("pagination: executeExists ignores limit/offset", async () => {
	const exists = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", ">", 100) // No users match
		.limit(1)
		.executeExists();

	// Should return false because no users match, even with limit
	assert.strictEqual(exists, false);
});

test("pagination: executeCount with joins ignores limit/offset", async () => {
	const count = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(3)
		.clearLimit()
		.execute();

	// Should return all 10 users
	assert.strictEqual(users.length, 10);
});

test("pagination: clearOffset removes offset", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.offset(7)
		.clearOffset()
		.execute();

	// Should return all 10 users starting from id 1
	assert.strictEqual(users.length, 10);
	assert.strictEqual(users[0]?.id, 1);
});
