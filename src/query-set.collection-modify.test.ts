import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 6: Collection Modification - Modifying nested collections via .modify()
//

test("modify collection: add WHERE clause to joined collection", async () => {
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
		.execute();

	// User 2 (bob) has 4 posts total (1, 2, 5, 12), but we filter to only posts <= 2
	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
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

test("modify collection: add extras to joined collection", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) =>
			qs.where("posts.id", "<=", 2).extras({
				titleLength: (row) => row.title.length,
			}),
		)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2, titleLength: 6 },
				{ id: 2, title: "Post 2", user_id: 2, titleLength: 6 },
			],
		},
	]);
});

test("modify collection: add nested join within collection", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) =>
			qs
				.where("posts.id", "<=", 2)
				.leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
		)
		.execute();

	// User 2's post 1 has 2 comments, post 2 has 1 comment
	assert.strictEqual(users.length, 1);
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

test("modify collection: multiple modifications on same collection", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) => qs.where("posts.id", "<=", 5))
		.modify("posts", (qs) =>
			qs.extras({
				isEarly: (row) => row.id <= 2,
			}),
		)
		.execute();

	// User 2 has posts 1, 2, 5 after filtering
	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2, isEarly: true },
				{ id: 2, title: "Post 2", user_id: 2, isEarly: true },
				{ id: 5, title: "Post 5", user_id: 2, isEarly: false },
			],
		},
	]);
});

test("modify collection: modify nested collection within collection", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) =>
			qs
				.where("posts.id", "<=", 2)
				.modify("comments", (commentsQs) => commentsQs.where("comments.id", ">=", 2)),
		)
		.execute();

	// Post 1 has comments 1 and 2, after filtering only comment 2 remains
	// Post 2 has comment 3
	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{
					id: 1,
					title: "Post 1",
					user_id: 2,
					comments: [{ id: 2, content: "Comment 2 on post 1", post_id: 1 }],
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

test("modify collection: omit fields from joined collection", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) => qs.where("posts.id", "<=", 2).omit(["user_id"]))
		.execute();

	assert.strictEqual(users.length, 1);
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

test("modify collection: map joined collection items", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (qs) =>
			qs.where("posts.id", "<=", 2).map((post) => ({
				postId: post.id,
				postTitle: post.title,
			})),
		)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ postId: 1, postTitle: "Post 1" },
				{ postId: 2, postTitle: "Post 2" },
			],
		},
	]);
});
