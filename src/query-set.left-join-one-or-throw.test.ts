import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 3: leftJoinOneOrThrow Tests
//

test("leftJoinOneOrThrow: toJoinedQuery shows non-nullable columns", async () => {
	const rows = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 2)
		.toJoinedQuery()
		.execute();

	assert.strictEqual(rows.length, 2);
	assert.deepStrictEqual(rows, [
		{
			id: 1,
			username: "alice",
			profile$$id: 1,
			profile$$bio: "Bio for user 1",
			profile$$user_id: 1,
		},
		{
			id: 2,
			username: "bob",
			profile$$id: 2,
			profile$$bio: "Bio for user 2",
			profile$$user_id: 2,
		},
	]);
});

test("leftJoinOneOrThrow: execute returns object when match exists", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
			"profile.user_id",
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
		},
		{
			id: 2,
			username: "bob",
			profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
		},
	]);
});

test("leftJoinOneOrThrow: execute throws when no match", async () => {
	await assert.rejects(async () => {
		await querySet(db)
			.init("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				(init) =>
					init((eb) =>
						eb
							.selectFrom("profiles")
							.select(["id", "bio", "user_id"])
							.where("user_id", "=", 999), // No profile with this user_id
					),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "=", 1)
			.execute();
	});
});

test("leftJoinOneOrThrow: executeCount counts all base records", async () => {
	const count = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeCount(Number);

	assert.strictEqual(count, 5);
});

test("leftJoinOneOrThrow: executeExists checks existence", async () => {
	const exists = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeExists();

	assert.strictEqual(exists, true);
});

test("leftJoinOneOrThrow: executeTakeFirst with join", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
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

test("leftJoinOneOrThrow: toBaseQuery returns base query without joins", async () => {
	const baseQuery = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", "<=", 3)
		.leftJoinOneOrThrow(
			"profile",
			(init) =>
				init((eb) =>
					eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
				),
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
