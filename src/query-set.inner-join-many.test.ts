import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Phase 4: innerJoinMany Tests
//

describe("query-set: inner-join-many", () => {
	test("innerJoinMany: toJoinedQuery shows row explosion with $$ prefixes", async () => {
		// User 2 (bob) has 4 posts (ids: 1, 2, 5, 12)
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
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

	test("innerJoinMany: execute returns hydrated arrays", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 2)
			.execute();

		// Should have 1 user with 4 posts in array
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

	test("innerJoinMany: execute returns multiple users with their posts", async () => {
		// User 2 (bob) has 4 posts, User 3 (carol) has 2 posts
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.execute();

		// Should have 2 users, each with their own posts
		assert.strictEqual(users.length, 2);
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

	test("innerJoinMany: filters out base records without matches", async () => {
		// User 1 (alice) has no posts, so should be filtered out by inner join
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 2)
			.execute();

		// Only bob should be returned (alice filtered out)
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

	test("innerJoinMany: executeTakeFirst returns first base record with all children", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
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

	test("innerJoinMany: executeCount counts unique base records (not exploded rows)", async () => {
		// User 2 and 3 have posts
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.executeCount(Number);

		// Should count 2 users (bob and carol), not 6 exploded rows
		assert.strictEqual(count, 2);
	});

	test("innerJoinMany: executeExists checks if any base records exist", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 5)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("innerJoinMany: toBaseQuery returns base query without joins", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("id", "<=", 2)
			.innerJoinMany(
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

	test("innerJoinMany: callback join condition with onRef", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				(join) => join.onRef("posts.user_id", "=", "user.id"),
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

	test("innerJoinMany: pre-built QuerySet variant", async () => {
		const postsQuery = querySet(db).selectAs("post", (eb) =>
			eb.selectFrom("posts").select(["id", "title", "user_id"]),
		);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany("posts", postsQuery, "posts.user_id", "user.id")
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
});
