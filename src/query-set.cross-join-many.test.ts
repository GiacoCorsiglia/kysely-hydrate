import assert from "node:assert";
import { describe, test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 4: crossJoinMany Tests
//

describe("query-set: cross-join-many", () => {
	test("crossJoinMany: toJoinedQuery shows cartesian product with $$ prefixes", async () => {
		// Cross join user 1 with all posts (15 posts total)
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			)
			.where("users.id", "=", 1)
			.toJoinedQuery()
			.execute();

		// Should have 15 rows (user 1 × 15 posts)
		assert.strictEqual(rows.length, 15);
		// Verify first few rows have the cartesian product structure
		assert.ok(rows.every((row) => row.id === 1 && row.username === "alice"));
		assert.ok(rows.some((row) => row.posts$$id === 1));
		assert.ok(rows.some((row) => row.posts$$id === 15));
	});

	test("crossJoinMany: execute returns all posts for each user (cartesian product)", async () => {
		// User 1 crossed with a subset of posts
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 3)),
			)
			.where("users.id", "=", 1)
			.execute();

		assert.strictEqual(users.length, 1);
		// User 1 should have all 3 posts (ids 1, 2, 3) regardless of user_id
		assert.strictEqual(users[0]?.posts.length, 3);
		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
					{ id: 3, title: "Post 3", user_id: 3 },
				],
			},
		]);
	});

	test("crossJoinMany: multiple users get same posts (full cartesian product)", async () => {
		// Two users crossed with same posts
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.strictEqual(users.length, 2);
		// Both users should have the same 2 posts
		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
				],
			},
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
				],
			},
		]);
	});

	test("crossJoinMany: executeTakeFirst returns first user with all crossed posts", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
			)
			.where("users.id", "=", 1)
			.executeTakeFirst();

		assert.deepStrictEqual(user, {
			id: 1,
			username: "alice",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
			],
		});
	});

	test("crossJoinMany: executeCount counts unique base records", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			)
			.where("users.id", "<=", 3)
			.executeCount(Number);

		// Should count 3 users, not 3 * 15 = 45 exploded rows
		assert.strictEqual(count, 3);
	});

	test("crossJoinMany: executeExists checks existence of base records", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			)
			.where("users.id", "<=", 5)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("crossJoinMany: toBaseQuery returns base query without joins", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("id", "<=", 2)
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			)
			.toBaseQuery();

		const rows = await baseQuery.execute();
		assert.strictEqual(rows.length, 2);
		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("crossJoinMany: empty posts collection filters out base records", async () => {
		// Cross join with no posts (WHERE clause filters all)
		// CROSS JOIN with empty set returns no rows (this is correct SQL behavior)
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 999), // No post with this ID
				),
			)
			.where("users.id", "=", 1)
			.execute();

		// CROSS JOIN with empty set = no results
		assert.strictEqual(users.length, 0);
		assert.deepStrictEqual(users, []);
	});
});
