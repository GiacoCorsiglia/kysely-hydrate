import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Phase 4: leftJoinMany Tests
//

describe("query-set: left-join-many", () => {
	test("leftJoinMany: toJoinedQuery shows nullable columns for no matches", async () => {
		// User 1 (alice) has no posts
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 1)
			.toJoinedQuery()
			.execute();

		// Should have 1 row with null post columns
		assert.strictEqual(rows.length, 1);
		assert.deepStrictEqual(rows, [
			{
				id: 1,
				username: "alice",
				posts$$id: null,
				posts$$title: null,
				posts$$user_id: null,
			},
		]);
	});

	test("leftJoinMany: toJoinedQuery shows row explosion when matches exist", async () => {
		// User 2 (bob) has 4 posts
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 2)
			.toJoinedQuery()
			.execute();

		// Should have 4 rows (one per post)
		assert.strictEqual(rows.length, 4);
		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$user_id: 2,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$title: "Post 2",
				posts$$user_id: 2,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 5,
				posts$$title: "Post 5",
				posts$$user_id: 2,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 12,
				posts$$title: "Post 12",
				posts$$user_id: 2,
			},
		]);
	});

	test("leftJoinMany: execute returns empty array when no matches", async () => {
		// User 1 (alice) has no posts
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 1)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				posts: [],
			},
		]);
	});

	test("leftJoinMany: execute returns hydrated arrays when matches exist", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 2)
			.execute();

		assert.strictEqual(users.length, 1);
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

	test("leftJoinMany: execute includes all base records (with and without matches)", async () => {
		// User 1 (alice) has no posts, User 2 (bob) has 4 posts
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
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

	test("leftJoinMany: executeTakeFirst returns first base record with all children", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 2)
			.executeTakeFirst();

		assert.deepStrictEqual(user, {
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		});
	});

	test("leftJoinMany: executeCount counts all base records", async () => {
		// Should count all 3 users (alice, bob, carol) even though alice has no posts
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.executeCount(Number);

		assert.strictEqual(count, 3);
	});

	test("leftJoinMany: executeExists checks existence", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 5)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("leftJoinMany: toBaseQuery returns base query without joins", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("id", "<=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.toBaseQuery();

		const rows = await baseQuery.execute();
		assert.strictEqual(rows.length, 2);
		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("leftJoinMany: callback join condition with onRef", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				(join) => join.onRef("posts.user_id", "=", "user.id"),
			)
			.where("users.id", "<=", 2)
			.execute();

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

	test("leftJoinMany: pre-built QuerySet variant", async () => {
		const postsQuery = querySet(db).selectAs("post", (eb) =>
			eb.selectFrom("posts").select(["id", "title", "user_id"]),
		);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany("posts", postsQuery, "posts.user_id", "user.id")
			.where("users.id", "<=", 2)
			.execute();

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
});
