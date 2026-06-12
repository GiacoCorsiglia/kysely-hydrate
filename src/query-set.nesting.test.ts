import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Deep Nesting Tests
//
// Multi-level joined collections (user → posts → comments → replies),
// attaches inside joined collections, null/empty branches at every level,
// and pagination/flat-row behavior through deep nesting.
//

describe("query-set: nesting", () => {
	test("nesting: 4 levels with mixed cardinality", async () => {
		// user → posts → comments → replies, with a sibling cardinality-one
		// profile at the second level
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
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "content", "post_id"])).leftJoinMany(
								"replies",
								({ eb, qs }) =>
									qs(eb.selectFrom("replies").select(["id", "content", "comment_id"])),
								"replies.comment_id",
								"comments.id",
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
				username: "bob",
				profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
				posts: [
					{
						id: 1,
						title: "Post 1",
						user_id: 2,
						comments: [
							{
								id: 1,
								content: "Comment 1 on post 1",
								post_id: 1,
								replies: [
									{ id: 2, content: "Reply 2 to comment 1", comment_id: 1 },
									{ id: 4, content: "Reply 4 to comment 1", comment_id: 1 },
								],
							},
							{ id: 2, content: "Comment 2 on post 1", post_id: 1, replies: [] },
						],
					},
					{
						id: 2,
						title: "Post 2",
						user_id: 2,
						comments: [
							{
								id: 3,
								content: "Comment 3 on post 2",
								post_id: 2,
								replies: [
									{ id: 1, content: "Reply 1 to comment 3", comment_id: 3 },
									{ id: 5, content: "Reply 5 to comment 3", comment_id: 3 },
								],
							},
						],
					},
				],
			},
		]);
	});

	test("nesting: attach collections at multiple levels inside joins", async () => {
		const fetchTags = async () => {
			return [
				{ id: 1, name: "typescript", post_id: 1 },
				{ id: 2, name: "kysely", post_id: 2 },
			];
		};

		const fetchBadges = async () => {
			return [{ owner_id: 2, badge: "author" }];
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
			.attachMany("badges", fetchBadges, { matchChild: "owner_id" })
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				badges: [{ owner_id: 2, badge: "author" }],
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

	test("nesting: mixed nullability with deep nesting", async () => {
		// Every null/empty branch is exercised: alice gets a NULL profile (the
		// subquery excludes her) and an empty posts array; bob's post 12 has an
		// empty comments array
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "!=", 1)),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 12]),
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
				profile: null, // Filtered out of the profile subquery
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
						id: 12,
						title: "Post 12",
						user_id: 2,
						comments: [], // Post 12 has no comments
					},
				],
			},
		]);
	});

	test("nesting: pagination with deep nesting", async () => {
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

	test("nesting: toJoinedQuery with deep nesting shows full row explosion", async () => {
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
			// The compiled SQL orders only by the base key; child-row order is
			// engine-dependent, so pin it for the comparison below
			.orderBy("posts$$id")
			.orderBy("posts$$comments$$id")
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
