import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Phase 3: leftJoinOneOrThrow Tests
//

describe("query-set: left-join-one-or-throw", () => {
	test("leftJoinOneOrThrow: toJoinedQuery shows non-nullable columns", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.leftJoinOneOrThrow(
					"profile",
					({ eb, qs }) =>
						qs(
							eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999), // No profile with this user_id
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 5)
			.executeCount(Number);

		assert.strictEqual(count, 5);
	});

	test("leftJoinOneOrThrow: executeExists checks existence", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 5)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("leftJoinOneOrThrow: executeTakeFirst with join", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
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

	test("leftJoinOneOrThrow: toBaseQuery returns base query without joins", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("id", "<=", 3)
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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

	test("leftJoinOneOrThrow: pagination with a sibling many-join", async () => {
		// Regression test: "oneOrThrow" joins were misclassified as
		// cardinality-many, so they were excluded from the paginated inner
		// subquery and re-joined outside the limit.  They are cardinality-one and
		// belong inside it (like leftJoinOne).
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(2);

		// The oneOrThrow join must live inside the paginated subquery, so its
		// columns are hoisted through the subquery alias (not selected from a
		// join applied outside the limit).
		const { sql } = query.toQuery().compile();
		assert.ok(sql.includes('"user"."profile$$bio"'), sql);

		const users = await query.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
				posts: [],
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
});
