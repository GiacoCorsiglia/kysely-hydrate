/**
 * Column alias tests for QuerySet API.
 *
 * These tests verify that QuerySet correctly handles column aliases in SQL
 * generation and hydration. When using aliases like `select(["username as name"])`,
 * the aliased name should be used in the output and correctly prefixed for nested joins.
 */

import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// SQL Generation Tests
//

test("SQL: base query with column alias", () => {
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username as name"]),
	);

	const sql = qs.toQuery().compile().sql;

	// Should preserve the alias in the output
	assert.ok(sql.includes('"username" as "name"'), `Expected alias in SQL: ${sql}`);
});

test("SQL: innerJoinMany with column aliases are correctly prefixed", () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title as postTitle", "user_id"])),
			"posts.user_id",
			"user.id",
		);

	const sql = qs.toQuery().compile().sql;

	// Base query alias should be preserved in the subquery
	assert.ok(sql.includes('"username" as "name"'), `Expected base alias in subquery: ${sql}`);

	// In the outer query, the alias is used as the column name and prefixed
	// Inner subquery: "title" as "postTitle"
	// Outer query references: "posts"."postTitle" as "posts$$postTitle"
	assert.ok(
		sql.includes('"posts"."postTitle" as "posts$$postTitle"'),
		`Expected prefixed alias reference in outer query: ${sql}`,
	);
});

test("SQL: doubly-prefixed aliases for deeply nested collections", () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) =>
					eb.selectFrom("posts").select(["id", "title as postTitle", "user_id"]),
				).innerJoinMany(
					"comments",
					(init2) =>
						init2((eb) =>
							eb
								.selectFrom("comments")
								.select(["id", "content as commentText", "post_id", "user_id"]),
						),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		);

	const sql = qs.toQuery().compile().sql;

	// Deeply nested alias gets double prefix in outer query
	// Inner: "content" as "commentText" -> "comments"."commentText" as "comments$$commentText"
	// Outer: "posts"."comments$$commentText" as "posts$$comments$$commentText"
	assert.ok(
		sql.includes('"posts"."comments$$commentText" as "posts$$comments$$commentText"'),
		`Expected doubly-prefixed alias reference: ${sql}`,
	);
});

test("SQL: mixed aliased and non-aliased columns", () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username as name", "email"]))
		.innerJoinOne(
			"profile",
			(nest) =>
				nest((eb) => eb.selectFrom("profiles").select(["id", "bio as biography", "user_id"])),
			"profile.user_id",
			"user.id",
		);

	const sql = qs.toQuery().compile().sql;

	// Base query: aliased and non-aliased in subquery
	assert.ok(sql.includes('"username" as "name"'), `Expected base alias: ${sql}`);
	assert.ok(sql.includes('"email"'), `Expected non-aliased column: ${sql}`);

	// Nested: alias is used as column name in outer query with prefix
	// Inner subquery: "bio" as "biography"
	// Outer query: "profile"."biography" as "profile$$biography"
	assert.ok(
		sql.includes('"profile"."biography" as "profile$$biography"'),
		`Expected prefixed alias reference: ${sql}`,
	);
	assert.ok(
		sql.includes('"profile"."user_id" as "profile$$user_id"'),
		`Expected prefixed non-aliased column: ${sql}`,
	);
});

test("SQL: leftJoinOne with column alias", () => {
	const qs = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "title as postTitle", "user_id"]))
		.leftJoinOne(
			"author",
			(nest) => nest((eb) => eb.selectFrom("users").select(["id", "username as authorName"])),
			"author.id",
			"post.user_id",
		);

	const sql = qs.toQuery().compile().sql;

	// Base query alias preserved in subquery
	assert.ok(sql.includes('"title" as "postTitle"'), `Expected base alias in subquery: ${sql}`);

	// Nested: alias is used as column name with prefix in outer query
	// Inner subquery: "username" as "authorName"
	// Outer query: "author"."authorName" as "author$$authorName"
	assert.ok(
		sql.includes('"author"."authorName" as "author$$authorName"'),
		`Expected prefixed alias reference: ${sql}`,
	);
});

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
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) =>
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
			(nest) => nest((eb) => eb.selectFrom("users").select(["id", "username as authorName"])),
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
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 1))
		.innerJoinOne(
			"profile",
			(nest) =>
				nest((eb) => eb.selectFrom("profiles").select(["id", "bio as biography", "user_id"])),
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
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) =>
					eb
						.selectFrom("posts")
						.select(["id", "title as postTitle", "user_id"])
						.where("id", "<=", 2)
						.orderBy("id"),
				).innerJoinMany(
					"comments",
					(init2) =>
						init2((eb) =>
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
			(nest) =>
				nest((eb) => eb.selectFrom("profiles").select(["id", "bio as biography", "user_id"])),
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
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 1))
		.leftJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title as postTitle", "user_id"])),
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
		.selectAs("user", db.selectFrom("users").select(["id", "username as name"]).where("id", "=", 2))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) =>
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
