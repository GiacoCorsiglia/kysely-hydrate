/**
 * PostgreSQL-specific tests (namely, lateral joins).
 *
 * These tests require a PostgreSQL database to run.
 *
 * To run these tests:
 *   RUN_POSTGRES_TESTS=true npm test -- src/query-builder.postgres.test.ts
 */

import assert from "node:assert";
import { after, before, describe, test } from "node:test";

import { CamelCasePlugin } from "kysely";

import { db, setupDatabase, teardownDatabase } from "./__tests__/postgres.ts";
import { hydrate } from "./query-builder.ts";

// Skip all tests if not explicitly enabled
const shouldRun = process.env.POSTGRES_URL || process.env.RUN_POSTGRES_TESTS;

describe("PostgreSQL tests", { skip: !shouldRun }, () => {
	before(async () => {
		await setupDatabase();
	});

	after(async () => {
		await teardownDatabase();
	});

	test("innerJoinLateral: adds inner join lateral", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.innerJoinLateral(
				(db) =>
					db
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.limit(1)
						.as("latest_post"),
				(join) => join.onTrue(),
			)
			.select(["latest_post.title"])
			.execute();

		assert.deepStrictEqual(users, [{ id: 2, username: "bob", title: "Post 1" }]);
	});

	test("leftJoinLateral: adds left join lateral", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "in", [1, 2]))
			.leftJoinLateral(
				(db) =>
					db
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.limit(1)
						.as("latest_post"),
				(join) => join.onTrue(),
			)
			.select(["latest_post.title"])
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", title: null }, // Alice has no posts
			{ id: 2, username: "bob", title: "Post 1" },
		]);
	});

	test("crossJoinLateral: adds cross join lateral", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.crossJoinLateral((db) =>
				db
					.selectFrom("posts")
					.select(["posts.id", "posts.title"])
					.whereRef("posts.user_id", "=", "users.id")
					.limit(1)
					.as("latest_post"),
			)
			.select(["latest_post.title"])
			.execute();

		assert.deepStrictEqual(users, [{ id: 2, username: "bob", title: "Post 1" }]);
	});

	test("hasMany with innerJoinLateral and hydrated subquery", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						).as("p"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("hasMany with innerJoinLateral and hydrated subquery with transformations", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						)
							.mapFields({
								title: (title) => title.toUpperCase(),
							})
							.extras({
								titleLength: (post) => post.title.length,
							})
							.as("p"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "POST 1", titleLength: 6 },
					{ id: 2, title: "POST 2", titleLength: 6 },
				],
			},
		]);
	});

	// NOTE: This limit is super counterintutive and wouldn't be the
	// right way to do this in a real-world scenario.  We happen to
	// know that the first post has 2 comments and the second post
	// has 1 comments, meaning 3 total rows.  This gets the test to
	// pass, but really you should chain your lateral joins with
	// limits instead of nesting them.
	const MAGIC_POST_LIMIT = 3;

	test("nested hydrated subqueries: posts with comments using lateral joins at both levels", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(MAGIC_POST_LIMIT),
							"id",
						)
							.hasMany("comments", ({ leftJoinLateral }) =>
								leftJoinLateral(
									(eb2) =>
										hydrate(
											eb2
												.selectFrom("comments")
												.select(["comments.id", "comments.content"])
												.whereRef("comments.post_id", "=", "posts.id")
												.orderBy("comments.id")
												.limit(2),
											"id",
										).as("c"),
									(join) => join.onTrue(),
								),
							)
							.as("p"),
					(join) => join.onTrue(),
				),
			);

		assert.deepStrictEqual(await users.execute(), [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						title: "Post 1",
						comments: [
							{ id: 1, content: "Comment 1 on post 1" },
							{ id: 2, content: "Comment 2 on post 1" },
						],
					},
					{
						id: 2,
						title: "Post 2",
						comments: [{ id: 3, content: "Comment 3 on post 2" }],
					},
				],
			},
		]);
	});

	test("nested hydrated subqueries with transformations at multiple levels", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.mapFields({
				username: (username) => username.toUpperCase(),
			})
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						)
							.mapFields({
								title: (title) => title.toUpperCase(),
							})
							.extras({
								titleLength: (post) => post.title.length,
							})
							.hasMany("comments", ({ innerJoinLateral }) =>
								innerJoinLateral(
									(eb2) =>
										hydrate(
											eb2
												.selectFrom("comments")
												.select(["comments.id", "comments.content"])
												.whereRef("comments.post_id", "=", "posts.id")
												.orderBy("comments.id")
												.limit(1),
											"id",
										)
											.mapFields({
												content: (content) => `[${content}]`,
											})
											.extras({
												contentLength: (comment) => comment.content.length,
											})
											.as("c"),
									(join) => join.onTrue(),
								),
							)
							.as("p"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "BOB",
				posts: [
					{
						id: 1,
						title: "POST 1",
						titleLength: 6,
						comments: [
							{
								id: 1,
								content: "[Comment 1 on post 1]",
								contentLength: "Comment 1 on post 1".length,
							},
						],
					},
					{
						id: 2,
						title: "POST 2",
						titleLength: 6,
						comments: [
							{
								id: 3,
								content: "[Comment 3 on post 2]",
								contentLength: "Comment 3 on post 2".length,
							},
						],
					},
				],
			},
		]);
	});

	test("hasOne with leftJoinLateral and hydrated subquery", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "in", [1, 2]).orderBy("users.id"))
			.hasOne("latestPost", ({ leftJoinLateral }) =>
				leftJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id", "desc")
								.limit(1),
							"id",
						)
							.mapFields({
								title: (title) => title.toUpperCase(),
							})
							.as("latest"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				latestPost: null, // Alice has no posts
			},
			{
				id: 2,
				username: "bob",
				latestPost: { id: 12, title: "POST 12" },
			},
		]);
	});

	test("multiple lateral joins in one query", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 2))
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						).as("p"),
					(join) => join.onTrue(),
				),
			)
			.hasOne("latestComment", ({ leftJoinLateral }) =>
				leftJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("comments")
								.select(["comments.id", "comments.content"])
								.whereRef("comments.user_id", "=", "users.id")
								.orderBy("comments.id", "desc")
								.limit(1),
							"id",
						).as("lc"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
				latestComment: { id: 11, content: "Comment 11 on post 11" },
			},
		]);
	});

	test("leftJoinLateral with hasMany handles empty results", async () => {
		const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "in", [1, 2]).orderBy("users.id"))
			.hasMany("posts", ({ leftJoinLateral }) =>
				leftJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						).as("p"),
					(join) => join.onTrue(),
				),
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
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("CamelCasePlugin: basic hydration with camelCase conversion", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		// Use $narrowType to properly type camelCase column names
		const users = await hydrate(
			camelDb.selectFrom("users").select(["users.id", "users.username"]),
			"id",
		)
			.modify((qb) => qb.where("users.id", "=", 2))
			.innerJoinLateral(
				(eb) =>
					eb
						.selectFrom("posts")
						.select(["posts.id", "posts.title", "posts.userId"])
						.whereRef("posts.user_id", "=", "users.id")
						.limit(1)
						.as("latestPost"),
				(join) => join.onTrue(),
			)
			.select(["latestPost.title", "latestPost.userId"])
			.execute();

		// With CamelCasePlugin, user_id should be converted to userId
		assert.deepStrictEqual(users, [{ id: 2, username: "bob", title: "Post 1", userId: 2 }]);
	});

	test("CamelCasePlugin: hasMany with nested hydrated subquery", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();

		const users = await hydrate(
			camelDb.selectFrom("users").select(["users.id", "users.username"]),
			"id",
		)
			.modify((qb) => qb.where("users.id", "=", 2))
			.hasMany("posts", ({ innerJoinLateral }) =>
				innerJoinLateral(
					(eb) =>
						hydrate(
							eb
								.selectFrom("posts")
								.select(["posts.id", "posts.title", "posts.userId"])
								.whereRef("posts.userId", "=", "users.id")
								.orderBy("posts.id")
								.limit(2),
							"id",
						).as("p"),
					(join) => join.onTrue(),
				),
			)
			.execute();

		// With CamelCasePlugin, user_id should be converted to userId
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

	test("CamelCasePlugin: top-level selection with camelCase selections", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			posts: { id: number; userId: number; title: string; content: string };
		}>();

		const posts = await hydrate(
			camelDb.selectFrom("posts").select(["posts.id", "posts.userId", "posts.title"]),
			"id",
		)
			.modify((qb) => qb.where("posts.id", "in", [1, 2]).orderBy("posts.id"))
			.execute();

		// With CamelCasePlugin, user_id should be converted to userId
		assert.deepStrictEqual(posts, [
			{ id: 1, userId: 2, title: "Post 1" },
			{ id: 2, userId: 2, title: "Post 2" },
		]);
	});
});
