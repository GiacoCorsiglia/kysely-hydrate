/**
 * CamelCasePlugin compatibility tests for QuerySet API.
 *
 * These tests verify that QuerySet works correctly with Kysely's CamelCasePlugin,
 * which transforms snake_case column names to camelCase in the JavaScript layer.
 *
 * The fixture uses snake_case column names (user_id, post_id, etc.) which the
 * plugin transforms. The suite runs against both sqlite and postgres, like the
 * rest of the shared suite.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

describe("query-set: camel-case", () => {
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

	test("nested left joins with camelCase keys, emitted as a flat chain", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
			comments: { id: number; postId: number };
		}>();

		const query = querySet(camelDb)
			.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 2)
			.leftJoinMany(
				"userPosts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "userId"]).where("id", "<=", 2),
					).leftJoinMany(
						"postComments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "postId"])),
						(join) =>
							join.onRef("postComments.postId", "=", "userPosts.id").on("postComments.id", "<", 3),
					),
				"userPosts.userId",
				"user.id",
			);

		// The plugin snake_cases the nested join's alias along with every reference to it.
		const { sql } = query.toQuery().compile();
		assert.match(
			sql,
			/\) as "user_posts\$\$post_comments" on "user_posts\$\$post_comments"\."post_id" = "user_posts"\."id"/,
		);

		assert.deepStrictEqual(await query.execute(), [
			{ id: 1, username: "alice", userPosts: [] },
			{
				id: 2,
				username: "bob",
				userPosts: [
					{
						id: 1,
						title: "Post 1",
						userId: 2,
						postComments: [
							{ id: 1, postId: 1 },
							{ id: 2, postId: 1 },
						],
					},
					{ id: 2, title: "Post 2", userId: 2, postComments: [] },
				],
			},
		]);
	});

	test("a nested query set with other plugins than its parent keeps its derived table", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			posts: { id: number; userId: number };
			comments: { id: number; postId: number };
		}>();

		// Only the nested query set snake_cases, so its own join's ON must stay in its query.
		const posts = querySet(camelDb)
			.selectAs("post", camelDb.selectFrom("posts").select(["id", "userId"]))
			.leftJoinMany(
				"comments",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "postId"])),
				"comments.postId",
				"post.id",
			);
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany("posts", posts, "posts.user_id" as any, "user.id");

		assert.doesNotMatch(query.toQuery().compile().sql, /\) as "posts\$\$comments"/);
		assert.deepStrictEqual(await query.execute(), [
			{ id: 1, posts: [] },
			{
				id: 2,
				posts: [
					{
						id: 1,
						user_id: 2,
						comments: [
							{ id: 1, post_id: 1 },
							{ id: 2, post_id: 1 },
						],
					},
					{ id: 2, user_id: 2, comments: [{ id: 3, post_id: 2 }] },
					{ id: 5, user_id: 2, comments: [{ id: 5, post_id: 5 }] },
					{ id: 12, user_id: 2, comments: [] },
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
