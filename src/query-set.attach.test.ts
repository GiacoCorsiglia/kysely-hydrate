import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

//
// Phase 6: Attach Methods - attachMany, attachOne, attachOneOrThrow
//

test("attachMany: fetches and matches related entities", async () => {
	const fetchPosts = async () => {
		return await db
			.selectFrom("posts")
			.select(["id", "title", "user_id"])
			.where("user_id", "in", [2, 3])
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "in", [2, 3])
		.attachMany("posts", fetchPosts, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
		{
			id: 3,
			username: "carol",
			posts: [
				{ id: 3, title: "Post 3", user_id: 3 },
				{ id: 15, title: "Post 15", user_id: 3 },
			],
		},
	]);
});

test("attachMany: returns empty array when no matches", async () => {
	const fetchPosts = async () => {
		return await db
			.selectFrom("posts")
			.select(["id", "title", "user_id"])
			.where("user_id", "=", 999)
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.attachMany("posts", fetchPosts, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			posts: [],
		},
	]);
});

test("attachMany: uses toParent for custom matching keys", async () => {
	const fetchPosts = async () => {
		return await db
			.selectFrom("posts")
			.select(["id", "title", "user_id"])
			.where("user_id", "=", 2)
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.attachMany("posts", fetchPosts, { matchChild: "user_id", toParent: "id" })
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("attachMany: accepts QuerySet return from fetchFn", async () => {
	const fetchPosts = () => {
		return querySet(db).init(
			"post",
			db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
		);
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.attachMany("posts", fetchPosts, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("attachMany: accepts SelectQueryBuilder return from fetchFn", async () => {
	const fetchPosts = () => {
		return db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2);
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.attachMany("posts", fetchPosts, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2 },
				{ id: 2, title: "Post 2", user_id: 2 },
				{ id: 5, title: "Post 5", user_id: 2 },
				{ id: 12, title: "Post 12", user_id: 2 },
			],
		},
	]);
});

test("attachMany: works at nested level", async () => {
	const fetchComments = async () => {
		return await db
			.selectFrom("comments")
			.select(["id", "content", "post_id"])
			.where("post_id", "in", [1, 2])
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]),
				).attachMany("comments", fetchComments, { matchChild: "post_id", toParent: "id" }),
			"posts.user_id",
			"user.id",
		)
		.execute();

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

test("attachOne: returns single match or null", async () => {
	const fetchProfile = async () => {
		return await db.selectFrom("profiles").select(["id", "bio", "user_id"]).execute();
	};

	const usersWithProfile = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.attachOne("profile", fetchProfile, { matchChild: "user_id" })
		.execute();

	const usersWithoutProfile = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 999)
		.attachOne("profile", fetchProfile, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(usersWithProfile.length, 1);
	assert.deepStrictEqual(usersWithProfile, [
		{
			id: 1,
			username: "alice",
			profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
		},
	]);

	assert.strictEqual(usersWithoutProfile.length, 0);
});

test("attachOne: throws on cardinality violation", async () => {
	const fetchPosts = async () => {
		return await db.selectFrom("posts").select(["id", "title", "user_id"]).execute();
	};

	const qs = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.attachOne("post", fetchPosts, { matchChild: "user_id" });

	await assert.rejects(async () => {
		await qs.execute();
	});
});

test("attachOne: works at nested level", async () => {
	const fetchLatestComment = async () => {
		// Return only 1 comment per post to avoid cardinality violation
		return await db
			.selectFrom("comments")
			.select(["id", "content", "post_id"])
			.where("post_id", "in", [1, 2])
			.where("id", "in", [1, 3]) // Only comment 1 for post 1, comment 3 for post 2
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]),
				).attachOne("latestComment", fetchLatestComment, { matchChild: "post_id", toParent: "id" }),
			"posts.user_id",
			"user.id",
		)
		.execute();

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
					latestComment: { id: 1, content: "Comment 1 on post 1", post_id: 1 },
				},
				{
					id: 2,
					title: "Post 2",
					user_id: 2,
					latestComment: { id: 3, content: "Comment 3 on post 2", post_id: 2 },
				},
			],
		},
	]);
});

test("attachOneOrThrow: returns entity when exists", async () => {
	const fetchProfile = async () => {
		return await db
			.selectFrom("profiles")
			.select(["id", "bio", "user_id"])
			.where("user_id", "=", 1)
			.execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.attachOneOrThrow("requiredProfile", fetchProfile, { matchChild: "user_id" })
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			requiredProfile: { id: 1, bio: "Bio for user 1", user_id: 1 },
		},
	]);
});

test("attachOneOrThrow: throws when no match exists", async () => {
	const fetchProfile = async () => {
		return await db
			.selectFrom("profiles")
			.select(["id", "bio", "user_id"])
			.where("user_id", "=", 999)
			.execute();
	};

	const qs = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.attachOneOrThrow("requiredProfile", fetchProfile, { matchChild: "user_id" });

	await assert.rejects(async () => {
		await qs.execute();
	}, ExpectedOneItemError);
});

test("attachOneOrThrow: works at nested level", async () => {
	const fetchAuthor = async () => {
		return await db.selectFrom("users").select(["id", "username"]).execute();
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]),
				).attachOneOrThrow("author", fetchAuthor, { matchChild: "id", toParent: "user_id" }),
			"posts.user_id",
			"user.id",
		)
		.execute();

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
					author: { id: 2, username: "bob" },
				},
				{
					id: 2,
					title: "Post 2",
					user_id: 2,
					author: { id: 2, username: "bob" },
				},
			],
		},
	]);
});

test("attachOneOrThrow: throws at nested level when missing", async () => {
	const fetchAuthor = async () => {
		// Return no matching authors
		return await db.selectFrom("users").select(["id", "username"]).where("id", "=", 999).execute();
	};

	const qs = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1),
				).attachOneOrThrow("requiredAuthor", fetchAuthor, {
					matchChild: "id",
					toParent: "user_id",
				}),
			"posts.user_id",
			"user.id",
		);

	await assert.rejects(async () => {
		await qs.execute();
	}, ExpectedOneItemError);
});

test("attachMany: modify attached QuerySet via init callback", async () => {
	const fetchPosts = () => {
		// Modify the QuerySet before returning it
		return querySet(db)
			.init(
				"post",
				db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
			)
			.where("posts.id", "<=", 2)
			.extras({
				titleLength: (row) => row.title.length,
			});
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.attachMany("posts", fetchPosts, { matchChild: "user_id" })
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

test("attachMany: with nested join and attach combination", async () => {
	const fetchComments = async () => {
		return await db.selectFrom("comments").select(["id", "content", "post_id"]).execute();
	};

	const fetchTags = async () => {
		return [
			{ id: 1, name: "typescript", post_id: 1 },
			{ id: 2, name: "kysely", post_id: 1 },
			{ id: 3, name: "nodejs", post_id: 2 },
		];
	};

	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			(init) =>
				init((eb) =>
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]),
				)
					.attachMany("comments", fetchComments, { matchChild: "post_id", toParent: "id" })
					.attachMany("tags", fetchTags, { matchChild: "post_id", toParent: "id" }),
			"posts.user_id",
			"user.id",
		)
		.execute();

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
					tags: [
						{ id: 1, name: "typescript", post_id: 1 },
						{ id: 2, name: "kysely", post_id: 1 },
					],
				},
				{
					id: 2,
					title: "Post 2",
					user_id: 2,
					comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
					tags: [{ id: 3, name: "nodejs", post_id: 2 }],
				},
			],
		},
	]);
});
