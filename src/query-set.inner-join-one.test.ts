import assert from "node:assert";
import { describe, test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 3: innerJoinOne Tests
//

describe("query-set: inner-join-one", () => {
	test("innerJoinOne: toJoinedQuery returns flat rows with $$ prefixes", async () => {
	const rows = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.toJoinedQuery()
		.execute();

	assert.strictEqual(rows.length, 3);
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
		{
			id: 3,
			username: "carol",
			profile$$id: 3,
			profile$$bio: "Bio for user 3",
			profile$$user_id: 3,
		},
	]);
});

	test("innerJoinOne: execute returns hydrated nested objects", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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

	test("innerJoinOne: executeTakeFirst with join", async () => {
	const user = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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

	test("innerJoinOne: executeCount counts base records", async () => {
	const count = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeCount(Number);

	assert.strictEqual(count, 5);
});

	test("innerJoinOne: executeExists checks existence", async () => {
	const exists = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 5)
		.executeExists();

	assert.strictEqual(exists, true);
});

	test("innerJoinOne: toBaseQuery returns base query without joins", async () => {
	const baseQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", "<=", 4)
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.toBaseQuery();

	const rows = await baseQuery.execute();
	assert.strictEqual(rows.length, 4);
	assert.deepStrictEqual(rows, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
	]);
});

	test("innerJoinOne: callback join condition with onRef", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			(join) => join.onRef("profile.user_id", "=", "user.id"),
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

	test("innerJoinOne: pre-built QuerySet variant", async () => {
	const profileQuery = querySet(db).selectAs("profile", (eb) =>
		eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
	);

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne("profile", profileQuery, "profile.user_id", "user.id")
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
});
