import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 3: leftJoinOne Tests
//

test("leftJoinOne: toJoinedQuery shows nullable columns", async () => {
	// Need a user without a profile to test nullability
	// All users 1-10 have profiles, so we'll need to add where clause that won't match
	const rows = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) =>
				init(
					(eb) =>
						eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No profile with this user_id
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 2)
		.toJoinedQuery()
		.execute();

	assert.strictEqual(rows.length, 2);
	// All profile columns should be null since no profiles match
	assert.deepStrictEqual(rows, [
		{
			id: 1,
			username: "alice",
			profile$$id: null,
			profile$$bio: null,
			profile$$user_id: null,
		},
		{
			id: 2,
			username: "bob",
			profile$$id: null,
			profile$$bio: null,
			profile$$user_id: null,
		},
	]);
});

test("leftJoinOne: execute returns null when no match", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) =>
				init(
					(eb) =>
						eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No profile with this user_id
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 2)
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", profile: null },
		{ id: 2, username: "bob", profile: null },
	]);
});

test("leftJoinOne: execute returns object when match exists", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.execute();

	assert.strictEqual(users.length, 3);
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
		{
			id: 3,
			username: "carol",
			profile: { id: 3, bio: "Bio for user 3", user_id: 3 },
		},
	]);
});

test("leftJoinOne: executeCount counts all base records", async () => {
	const count = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) =>
				init(
					(eb) =>
						eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No profiles match
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeCount(Number);

	// Should count all 5 users even though none have matching profiles
	assert.strictEqual(count, 5);
});

test("leftJoinOne: executeExists checks existence", async () => {
	const exists = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) =>
				init(
					(eb) =>
						eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No profiles match
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeExists();

	// Should return true because users exist (even without profiles)
	assert.strictEqual(exists, true);
});

test("leftJoinOne: executeTakeFirst with join", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "=", 2)
		.executeTakeFirst();

	assert.deepStrictEqual(user, {
		id: 2,
		username: "bob",
		profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
	});
});

test("leftJoinOne: toBaseQuery returns base query without joins", async () => {
	const baseQuery = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", "<=", 3)
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.toBaseQuery();

	const rows = await baseQuery.execute();
	assert.strictEqual(rows.length, 3);
	assert.deepStrictEqual(rows, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
	]);
});
