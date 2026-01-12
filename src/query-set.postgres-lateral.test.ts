/**
 * PostgreSQL-specific tests for QuerySet API (namely, lateral joins).
 *
 * These tests require a PostgreSQL database to run.
 *
 * To run these tests:
 *   HYDRATE_TEST_DB=postgres npm test -- src/query-set.postgres-lateral.test.ts
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

// Skip all tests if not running against postgres
const shouldSkip = dialect !== "postgres";

describe("query-set: postgres-lateral", { skip: shouldSkip }, () => {
	const db = getDbForTest();

	//
	// Basic lateral join methods - innerJoinLateralMany, leftJoinLateralMany, crossJoinLateralMany
	//

	test("innerJoinLateralMany: basic usage with limit", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
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

	test("leftJoinLateralMany: handles users without posts", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
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

	test("crossJoinLateralMany: creates cartesian product with limit", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.crossJoinLateralMany("allPosts", ({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						.select(["id", "title"])
						.whereRef("posts.user_id", "=", "user.id")
						.orderBy("posts.id")
						.limit(2),
				),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				allPosts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	//
	// Cardinality-one lateral joins
	//

	test("innerJoinLateralOne: fetches single related entity", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralOne(
				"latestPost",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				latestPost: { id: 12, title: "Post 12" },
			},
		]);
	});

	test("leftJoinLateralOne: returns null when no match", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinLateralOne(
				"latestPost",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
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
				latestPost: { id: 12, title: "Post 12" },
			},
		]);
	});

	test("leftJoinLateralOneOrThrow: returns entity when exists", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.leftJoinLateralOneOrThrow(
				"latestPost",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				latestPost: { id: 12, title: "Post 12" },
			},
		]);
	});

	test("leftJoinLateralOneOrThrow: throws when no match", async () => {
		await assert.rejects(async () => {
			await querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "=", 1)
				.leftJoinLateralOneOrThrow(
					"latestPost",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "title"])
								.whereRef("posts.user_id", "=", "user.id")
								.orderBy("posts.id", "desc")
								.limit(1),
						),
					(join) => join.onTrue(),
				)
				.execute();
		});
	});

	//
	// Nested lateral joins
	//

	test("nested lateral joins: posts with comments at both levels", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).leftJoinLateralMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "content"])
									.whereRef("comments.post_id", "=", "posts.id")
									.orderBy("comments.id")
									.limit(2),
							),
						(join) => join.onTrue(),
					),
				(join) => join.onTrue(),
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

	//
	// Hydration features with lateral joins
	//

	test("lateral joins with mapFields transformation", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).mapFields({
						title: (title) => title.toUpperCase(),
					}),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "POST 1" },
					{ id: 2, title: "POST 2" },
				],
			},
		]);
	});

	test("lateral joins with extras", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).extras({
						titleLength: (row) => row.title.length,
					}),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", titleLength: 6 },
					{ id: 2, title: "Post 2", titleLength: 6 },
				],
			},
		]);
	});

	test("lateral joins with omit", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title", "user_id"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).omit(["user_id"]),
				(join) => join.onTrue(),
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

	test("nested lateral joins with transformations at multiple levels", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.mapFields({
				username: (username) => username.toUpperCase(),
			})
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					)
						.mapFields({
							title: (title) => title.toUpperCase(),
						})
						.extras({
							titleLength: (post) => post.title.length,
						})
						.innerJoinLateralMany(
							"comments",
							({ eb, qs }) =>
								qs(
									eb
										.selectFrom("comments")
										.select(["id", "content"])
										.whereRef("comments.post_id", "=", "posts.id")
										.orderBy("comments.id")
										.limit(1),
								)
									.mapFields({
										content: (content) => `[${content}]`,
									})
									.extras({
										contentLength: (comment) => comment.content.length,
									}),
							(join) => join.onTrue(),
						),
				(join) => join.onTrue(),
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

	//
	// Multiple lateral joins on same QuerySet
	//

	test("multiple lateral joins: posts and comments as siblings", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.leftJoinLateralOne(
				"latestComment",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("comments")
							.select(["id", "content"])
							.whereRef("comments.user_id", "=", "user.id")
							.orderBy("comments.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
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

	//
	// Pagination with lateral joins
	//

	test("limit: limits base records with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		const users = await qs.limit(3).execute();
		const allUsers = await qs.execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(allUsers.length, 10);
		assert.deepStrictEqual(users, allUsers.slice(0, 3));
	});

	test("offset: skips base records with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		const users = await qs.offset(2).limit(3).execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(users[0]?.id, 3);
		assert.strictEqual(users[1]?.id, 4);
		assert.strictEqual(users[2]?.id, 5);
	});

	test("pagination: limit + offset with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		// Page 1: users 1-2
		const page1 = await qs.limit(2).execute();
		assert.strictEqual(page1.length, 2);
		assert.strictEqual(page1[0]?.id, 1);
		assert.strictEqual(page1[1]?.id, 2);

		// Page 2: users 3-4
		const page2 = await qs.offset(2).limit(2).execute();
		assert.strictEqual(page2.length, 2);
		assert.strictEqual(page2[0]?.id, 3);
		assert.strictEqual(page2[1]?.id, 4);
	});

	//
	// executeCount with lateral joins
	//

	test("executeCount: counts base records with innerJoinLateralMany", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2, 3])
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.limit(1),
					),
				(join) => join.onTrue(),
			);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();

		// Should count users with posts (alice has none, so 2)
		assert.strictEqual(count, 2);
		assert.strictEqual(users.length, 2);
	});

	test("executeCount: counts base records with leftJoinLateralMany", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();

		// Should count all 2 users (left join doesn't filter)
		assert.strictEqual(count, 2);
		assert.strictEqual(users.length, 2);
	});

	test("executeCount: ignores limit/offset with lateral joins", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.limit(1)
			.offset(1)
			.executeCount(Number);

		assert.strictEqual(count, 3); // Counts all matching users, not just paginated
	});

	//
	// toJoinedQuery with lateral joins
	//

	test("toJoinedQuery: shows flattened rows with lateral joins", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.toJoinedQuery()
			.execute();

		// Should have 2 flattened rows (one per post)
		assert.strictEqual(rows.length, 2);
		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$title: "Post 2",
			},
		]);
	});

	test("toJoinedQuery: nested lateral joins show double prefixes", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(1),
					).innerJoinLateralMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "content"])
									.whereRef("comments.post_id", "=", "posts.id")
									.orderBy("comments.id")
									.limit(2),
							),
						(join) => join.onTrue(),
					),
				(join) => join.onTrue(),
			)
			.toJoinedQuery()
			.execute();

		// Post 1 has 2 comments = 2 rows
		assert.strictEqual(rows.length, 2);
		assert.ok("posts$$id" in rows[0]!);
		assert.ok("posts$$title" in rows[0]!);
		assert.ok("posts$$comments$$id" in rows[0]!);
		assert.ok("posts$$comments$$content" in rows[0]!);
	});

	//
	// Mixed lateral and regular joins
	//

	test("mixed: innerJoinOne and innerJoinLateralMany", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("mixed: innerJoinMany and innerJoinLateralOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
				"posts.user_id",
				"user.id",
			)
			.innerJoinLateralOne(
				"latestComment",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("comments")
							.select(["id", "content"])
							.whereRef("comments.user_id", "=", "user.id")
							.orderBy("comments.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
				],
				latestComment: { id: 11, content: "Comment 11 on post 11" },
			},
		]);
	});

	//
	// Collection modification with lateral joins
	//

	test("modify: add where clause to lateral join collection", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(5),
					),
				(join) => join.onTrue(),
			)
			.modify("posts", (qs) => qs.where("posts.id", "<=", 2))
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

	test("modify: add extras to lateral join collection", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.modify("posts", (qs) =>
				qs.extras({
					titleLength: (row) => row.title.length,
				}),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", titleLength: 6 },
					{ id: 2, title: "Post 2", titleLength: 6 },
				],
			},
		]);
	});
});
