import assert from "node:assert";
import { describe, test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 7: Complex Scenarios - Multi-level nesting and real-world patterns
//

describe("query-set: complex", () => {
	test("complex: 3-level nesting users → posts → comments", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
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
				username: "bob",
				posts: [
					{
						id: 1,
						title: "Post 1",
						user_id: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
							{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
						],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
					},
				],
			},
		]);
	});

	test("complex: 4-level nesting with mixed cardinality", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
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
				username: "bob",
				profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
				posts: [
					{
						id: 1,
						title: "Post 1",
						user_id: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
							{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
						],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
					},
				],
			},
		]);
	});

	test("complex: nested joins with attach at multiple levels", async () => {
		const fetchTags = async () => {
			return [
				{ id: 1, name: "typescript", post_id: 1 },
				{ id: 2, name: "kysely", post_id: 2 },
			];
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2))
						.innerJoinMany(
							"comments",
							({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
							"comments.post_id",
							"posts.id",
						)
						.attachMany("tags", fetchTags, { matchChild: "post_id", toParent: "id" }),
				"posts.user_id",
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
						user_id: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
							{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
						],
						tags: [{ id: 1, name: "typescript", post_id: 1 }],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
						tags: [{ id: 2, name: "kysely", post_id: 2 }],
					},
				],
			},
		]);
	});

	test("complex: multiple modifications chained with hydration", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.modify("posts", (qs) => qs.where("posts.id", "<=", 2))
			.modify("posts", (qs) =>
				qs.extras({
					titleLength: (row) => row.title.length,
				}),
			)
			.modify("posts", (qs) => qs.omit(["user_id"]))
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

	test("complex: mixed nullability with deep nesting", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).leftJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
				posts: [], // Alice has no posts
			},
			{
				id: 2,
				username: "bob",
				profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
				posts: [
					{
						id: 1,
						title: "Post 1",
						user_id: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
							{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
						],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
					},
				],
			},
		]);
	});

	test("complex: pagination with deep nesting", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "in", [2, 3]);

		const users = await qs.limit(1).execute();
		const allUsers = await qs.execute();

		// Should return only first user (bob) with ALL their posts and comments
		// Note: Post 12 is filtered out because it has no comments (innerJoinMany)
		assert.strictEqual(users.length, 1);
		assert.strictEqual(allUsers.length, 2);
		assert.deepStrictEqual(users, allUsers.slice(0, 1));
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						title: "Post 1",
						user_id: 2,
						comments: [
							{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
							{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
						],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
					},
					{
						id: 5,
						title: "Post 5",
						user_id: 2,
						comments: [{ id: 5, content: "Comment 5 on post 5", post_id: 5 }],
					},
				],
			},
		]);
	});

	test("complex: sibling collections with transformation", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])).extras({
						bioLength: (row) => row.bio?.length ?? 0,
					}),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).mapFields({
						title: (value) => value.toUpperCase(),
					}),
				"posts.user_id",
				"user.id",
			)
			.map((user) => ({
				userId: user.id,
				name: user.username,
				bio: user.profile.bio,
				bioLength: user.profile.bioLength,
				postTitles: user.posts.map((p) => p.title),
			}))
			.execute();

		assert.deepStrictEqual(users, [
			{
				userId: 2,
				name: "bob",
				bio: "Bio for user 2",
				bioLength: 14,
				postTitles: ["POST 1", "POST 2"],
			},
		]);
	});

	test("complex: executeCount with deep nesting", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 4);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();
		const joinedRows = await qs.toJoinedQuery().execute();

		// Users with posts that have comments
		assert.strictEqual(count, 3); // bob (2), carol (3), and one more
		assert.strictEqual(users.length, 3); // Verify count matches execute
		assert.ok(joinedRows.length > users.length); // Row explosion in joined query
	});

	test("complex: toJoinedQuery with deep nesting shows full row explosion", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		// Flattened rows with prefixes showing cartesian product:
		// Post 1 has 2 comments = 2 rows
		// Post 2 has 1 comment = 1 row
		// Total = 3 rows
		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$user_id: 2,
				posts$$comments$$id: 1,
				posts$$comments$$content: "Comment 1 on post 1",
				posts$$comments$$post_id: 1,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$user_id: 2,
				posts$$comments$$id: 2,
				posts$$comments$$content: "Comment 2 on post 1",
				posts$$comments$$post_id: 1,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$title: "Post 2",
				posts$$user_id: 2,
				posts$$comments$$id: 3,
				posts$$comments$$content: "Comment 3 on post 2",
				posts$$comments$$post_id: 2,
			},
		]);
	});
});
