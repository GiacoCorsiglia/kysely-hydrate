/**
 * Column alias tests for QuerySet API.
 *
 * These tests verify that QuerySet correctly handles column aliases in SQL
 * generation and hydration. When using aliases like `select(["username as name"])`,
 * the aliased name should be used in the output and correctly prefixed for nested joins.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

describe("query-set: column-aliases", () => {
	// (Compiled-SQL alias tests live in query-set.sql.test.ts; this file covers
	// the hydration behavior.)

	//
	// Execution Tests
	//

	test("execute: base query with column alias returns aliased field name", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "<=", 3),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, name: "alice" },
			{ id: 2, name: "bob" },
			{ id: 3, name: "carol" },
		]);
	});

	test("execute: innerJoinMany with column aliases hydrates correctly", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2),
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title as postTitle", "user_id"])
							.orderBy("id")
							.limit(2),
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				name: "bob",
				posts: [
					{ id: 1, postTitle: "Post 1", user_id: 2 },
					{ id: 2, postTitle: "Post 2", user_id: 2 },
				],
			},
		]);
	});

	test("execute: leftJoinOne with column aliases hydrates correctly", async () => {
		const posts = await querySet(db)
			.selectAs(
				"post",
				db.selectFrom("posts").select(["id", "title as postTitle", "user_id"]).where("id", "=", 1),
			)
			.leftJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username as authorName"])),
				"author.id",
				"post.user_id",
			)
			.execute();

		assert.deepStrictEqual(posts, [
			{
				id: 1,
				postTitle: "Post 1",
				user_id: 2,
				author: { id: 2, authorName: "bob" },
			},
		]);
	});

	test("execute: innerJoinOne with column aliases hydrates correctly", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 1),
			)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio as biography", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				name: "alice",
				profile: { id: 1, biography: "Bio for user 1", user_id: 1 },
			},
		]);
	});

	test("execute: nested joins with column aliases at multiple levels", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2),
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title as postTitle", "user_id"])
							.where("id", "<=", 2)
							.orderBy("id"),
					).innerJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "content as commentText", "post_id", "user_id"])
									.orderBy("id"),
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				name: "bob",
				posts: [
					{
						id: 1,
						postTitle: "Post 1",
						user_id: 2,
						comments: [
							{ id: 1, commentText: "Comment 1 on post 1", post_id: 1, user_id: 2 },
							{ id: 2, commentText: "Comment 2 on post 1", post_id: 1, user_id: 3 },
						],
					},
					{
						id: 2,
						postTitle: "Post 2",
						user_id: 2,
						comments: [{ id: 3, commentText: "Comment 3 on post 2", post_id: 2, user_id: 1 }],
					},
				],
			},
		]);
	});

	test("execute: mixed aliased and non-aliased columns in same query", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name", "email"]).where("id", "=", 1),
			)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio as biography", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				name: "alice",
				email: "alice@example.com",
				profile: { id: 1, biography: "Bio for user 1", user_id: 1 },
			},
		]);
	});

	test("execute: leftJoinMany with column aliases and empty results", async () => {
		const users = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 1),
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title as postTitle", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// Alice (id=1) has no posts
		assert.deepStrictEqual(users, [
			{
				id: 1,
				name: "alice",
				posts: [],
			},
		]);
	});

	test("execute: toJoinedQuery with column aliases shows prefixed aliases", async () => {
		const rows = await querySet(db)
			.selectAs(
				"user",
				db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2),
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title as postTitle", "user_id"])
							.orderBy("id")
							.limit(2),
					),
				"posts.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		assert.deepStrictEqual(rows, [
			{
				id: 2,
				name: "bob",
				posts$$id: 1,
				posts$$postTitle: "Post 1",
				posts$$user_id: 2,
			},
			{
				id: 2,
				name: "bob",
				posts$$id: 2,
				posts$$postTitle: "Post 2",
				posts$$user_id: 2,
			},
		]);
	});
});
