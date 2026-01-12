/**
 * CamelCasePlugin compatibility tests for QuerySet API.
 *
 * These tests verify that QuerySet works correctly with Kysely's CamelCasePlugin,
 * which transforms snake_case column names to camelCase in the JavaScript layer.
 *
 * These tests require a PostgreSQL database to run since the fixture uses snake_case
 * column names (user_id, post_id, etc.) which the plugin transforms.
 *
 * To run these tests:
 *   RUN_POSTGRES_TESTS=true npm test -- src/query-set.camel-case.test.ts
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin } from "kysely";

import { getDbForTest } from "./__tests__/postgres.ts";
import { querySet } from "./query-set.ts";

// Skip all tests if not explicitly enabled
const shouldRun = process.env.POSTGRES_URL || process.env.RUN_POSTGRES_TESTS;

describe("CamelCasePlugin compatibility", { skip: !shouldRun }, () => {
	const db = getDbForTest();

	//
	// Basic queries
	//

	test("basic query with camelCase column selection", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			posts: { id: number; userId: number; title: string; content: string };
		}>();

		const posts = await querySet(camelDb)
			.selectAs("post", camelDb.selectFrom("posts").select(["id", "userId", "title"]))
			.where("posts.id", "in", [1, 2])
			.execute();

		// With CamelCasePlugin, user_id should be converted to userId
		assert.deepStrictEqual(posts, [
			{ id: 1, userId: 2, title: "Post 1" },
			{ id: 2, userId: 2, title: "Post 2" },
		]);
	});

	//
	// innerJoinMany
	//

	test("innerJoinMany with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		const users = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "userId"]).orderBy("posts.id").limit(2)),
				"posts.userId",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", userId: 2 },
					{ id: 2, title: "Post 2", userId: 2 },
				],
			},
		]);
	});

	//
	// leftJoinMany
	//

	test("leftJoinMany with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		const users = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "userId"]).orderBy("posts.id").limit(2)),
				"posts.userId",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				posts: [], // Alice has no posts
			},
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", userId: 2 },
					{ id: 2, title: "Post 2", userId: 2 },
				],
			},
		]);
	});

	//
	// innerJoinOne
	//

	test("innerJoinOne with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			profiles: { id: number; userId: number; bio: string | null };
		}>();

		const users = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "userId", "bio"])),
				"profile.userId",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				profile: { id: 1, userId: 1, bio: "Bio for user 1" },
			},
		]);
	});

	//
	// leftJoinOne
	//

	test("leftJoinOne with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			posts: { id: number; title: string; userId: number };
			users: { id: number; username: string };
		}>();

		const posts = await querySet(camelDb)
			.selectAs("post", camelDb.selectFrom("posts").select(["id", "title", "userId"]))
			.where("posts.id", "=", 1)
			.leftJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"author.id",
				"post.userId",
			)
			.execute();

		assert.deepStrictEqual(posts, [
			{
				id: 1,
				title: "Post 1",
				userId: 2,
				author: { id: 2, username: "bob" },
			},
		]);
	});

	//
	// Nested joins
	//

	test("nested joins with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
			comments: { id: number; content: string; postId: number; userId: number };
		}>();

		const users = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "userId"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "content", "postId", "userId"])),
						"comments.postId",
						"posts.id",
					),
				"posts.userId",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						title: "Post 1",
						userId: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", postId: 1, userId: 2 },
							{ id: 2, content: "Comment 2 on post 1", postId: 1, userId: 3 },
						],
					},
					{
						id: 2,
						title: "Post 2",
						userId: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", postId: 2, userId: 1 }],
					},
				],
			},
		]);
	});

	//
	// toJoinedQuery
	//

	test("toJoinedQuery with camelCase columns shows prefixed camelCase", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		const rows = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "userId"]).orderBy("posts.id").limit(2)),
				"posts.userId",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		assert.strictEqual(rows.length, 2);
		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$userId: 2,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$title: "Post 2",
				posts$$userId: 2,
			},
		]);
	});

	//
	// executeCount
	//

	test("executeCount with camelCase columns", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		const count = await querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "userId"])),
				"posts.userId",
				"user.id",
			)
			.executeCount(Number);

		assert.strictEqual(count, 2);
	});
});
