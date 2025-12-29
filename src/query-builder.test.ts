import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { ExpectedOneItemError } from "./helpers/errors.ts";
import { hydrateQuery } from "./query-builder.ts";

//
// Basic Query Execution
//

test("execute: returns underlying query results", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	).execute();

	assert.ok(Array.isArray(users));
	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users[0], { id: 1, username: "alice" });
});

test("executeTakeFirst: returns first result or undefined", async () => {
	const user = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 1),
		"id",
	).executeTakeFirst();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });

	const noUser = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 999),
		"id",
	).executeTakeFirst();

	assert.strictEqual(noUser, undefined);
});

test("executeTakeFirstOrThrow: returns first result or throws", async () => {
	const user = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 1),
		"id",
	).executeTakeFirstOrThrow();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });

	await assert.rejects(async () => {
		await hydrateQuery(
			db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 999),
			"id",
		).executeTakeFirstOrThrow();
	});
});

//
// Query Modification
//

test("modify: allows modifying underlying query", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "<", 3))
		.execute();

	assert.strictEqual(users.length, 2);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[1]?.id, 2);
});

//
// hasMany
//

test("hasMany: creates nested array via left join", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.username, "bob");
	assert.strictEqual(users[0]?.posts.length, 4); // Bob has 4 posts
	assert.deepStrictEqual(users[0]?.posts[0], { id: 1, title: "Post 1" });
});

test("hasMany: produces correctly prefixed columns in raw query", async () => {
	const builder = hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		);

	const rows = await builder.toQuery().execute();

	// Should have prefixed columns
	assert.strictEqual(rows.length, 4); // Bob has 4 posts
	assert.strictEqual(rows[0]?.id, 2);
	assert.strictEqual(rows[0]?.username, "bob");
	assert.strictEqual(rows[0]?.["posts$$id"], 1);
	assert.strictEqual(rows[0]?.["posts$$title"], "Post 1");
});

test("hasMany: returns empty array when no matches", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1)) // Alice has no posts
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 0);
});

test("hasMany: handles multiple nesting levels", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.hasMany(
						"comments",
						({ leftJoin }) =>
							leftJoin("comments", "comments.post_id", "posts.id").select([
								"comments.id",
								"comments.content",
							]),
						"id",
					),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 4);
	// Post 1 has 2 comments, Post 2 has 1 comment, Post 5 has 1 comment, Post 12 has 0 comments
	assert.strictEqual(users[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(users[0]?.posts[0]?.comments[0], {
		id: 1,
		content: "Comment 1 on post 1",
	});
});

test("hasMany: multiple nesting produces doubly-prefixed columns", async () => {
	const builder = hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.hasMany(
						"comments",
						({ leftJoin }) =>
							leftJoin("comments", "comments.post_id", "posts.id").select([
								"comments.id",
								"comments.content",
							]),
						"id",
					),
			"id",
		);

	const rows = await builder.toQuery().execute();

	// Find a row with a comment (post 1 has comments)
	const rowWithComment = rows.find((r) => r["posts$$comments$$id"] !== null);

	assert.ok(rowWithComment, "Should have at least one row with a comment");
	assert.strictEqual(rowWithComment.id, 2);
	assert.strictEqual(rowWithComment.username, "bob");
	assert.strictEqual(rowWithComment["posts$$id"], 1);
	assert.strictEqual(rowWithComment["posts$$title"], "Post 1");
	assert.strictEqual(rowWithComment["posts$$comments$$id"], 1);
	assert.strictEqual(rowWithComment["posts$$comments$$content"], "Comment 1 on post 1");
});

//
// hasOne
//

test("hasOne: creates nullable nested object via left join", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "<=", 2))
		.hasOne(
			"profile",
			({ leftJoin }) =>
				leftJoin("profiles", "profiles.user_id", "users.id").select([
					"profiles.id",
					"profiles.bio",
				]),
			"id",
		)
		.execute();

	assert.strictEqual(users.length, 2);
	assert.ok(users[0]?.profile);
	assert.strictEqual(users[0]?.profile.bio, "Bio for user 1");
});

test("hasOne: creates non-nullable nested object via inner join", async () => {
	const posts = await hydrateQuery(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
		.modify((qb) => qb.where("posts.id", "=", 1))
		.hasOne(
			"author",
			({ innerJoin }) =>
				innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
			"id",
		)
		.execute();

	assert.strictEqual(posts[0]?.author.username, "bob");
});

test("hasOne: produces correctly prefixed columns in raw query", async () => {
	const builder = hydrateQuery(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
		.modify((qb) => qb.where("posts.id", "=", 1))
		.hasOne(
			"author",
			({ innerJoin }) =>
				innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
			"id",
		);

	const rows = await builder.toQuery().execute();

	assert.strictEqual(rows.length, 1);
	assert.strictEqual(rows[0]?.id, 1);
	assert.strictEqual(rows[0]?.title, "Post 1");
	assert.strictEqual(rows[0]?.["author$$id"], 2);
	assert.strictEqual(rows[0]?.["author$$username"], "bob");
});

test("hasOne: returns null when no match with left join", async () => {
	// Create a temporary user with no posts to test null case
	const result = await db
		.insertInto("users")
		.values({ username: "temp", email: "temp@example.com" })
		.returningAll()
		.executeTakeFirstOrThrow();

	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", result.id))
		.hasOne(
			"latestPost",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.latestPost, null);

	// Cleanup
	await db.deleteFrom("users").where("users.id", "=", result.id).execute();
});

//
// hasOneOrThrow
//

test("hasOneOrThrow: returns nested object when exists", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.hasOneOrThrow(
			"profile",
			({ leftJoin }) =>
				leftJoin("profiles", "profiles.user_id", "users.id").select([
					"profiles.id",
					"profiles.bio",
				]),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.profile.bio, "Bio for user 1");
});

test("hasOneOrThrow: throws when nested object missing", async () => {
	const result = await db
		.insertInto("users")
		.values({ username: "temp", email: "temp@example.com" })
		.returningAll()
		.executeTakeFirst();

	await assert.rejects(async () => {
		await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", result!.id))
			.hasOneOrThrow(
				"profile",
				({ leftJoin }) =>
					leftJoin("profiles", "profiles.user_id", "users.id").select([
						"profiles.id",
						"profiles.bio",
					]),
				"id",
			)
			.execute();
	}, ExpectedOneItemError);

	// Cleanup
	await db.deleteFrom("users").where("users.id", "=", result!.id).execute();
});

//
// attachMany
//

test("attachMany: fetches and attaches related entities", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.attachMany(
			"posts",
			async (userRows) => {
				const userIds = userRows.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["posts.id", "posts.user_id", "posts.title"])
					.where("posts.user_id", "in", userIds)
					.execute();
			},
			{ matchChild: "user_id" },
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.title, "Post 1");
});

test("attachMany: calls fetchFn exactly once", async () => {
	let callCount = 0;

	await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "<=", 3))
		.attachMany(
			"posts",
			async (userRows) => {
				callCount++;
				const userIds = userRows.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["posts.id", "posts.user_id", "posts.title"])
					.where("posts.user_id", "in", userIds)
					.execute();
			},
			{ matchChild: "user_id" },
		)
		.execute();

	assert.strictEqual(callCount, 1);
});

test("attachMany: returns empty array when no matches", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1)) // Alice has no posts
		.attachMany(
			"posts",
			async (userRows) => {
				const userIds = userRows.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["posts.id", "posts.user_id", "posts.title"])
					.where("posts.user_id", "in", userIds)
					.execute();
			},
			{ matchChild: "user_id" },
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 0);
});

//
// attachOne
//

test("attachOne: returns first match or null", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "in", [1, 2]))
		.attachOne(
			"latestPost",
			async (userRows) => {
				const userIds = userRows.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["posts.id", "posts.user_id", "posts.title"])
					.where("posts.user_id", "in", userIds)
					.execute();
			},
			{ matchChild: "user_id" },
		)
		.execute();

	assert.strictEqual(users[0]?.latestPost, null); // Alice has no posts
	assert.strictEqual(users[1]?.latestPost?.title, "Post 1"); // Bob's first post
});

//
// attachOneOrThrow
//

test("attachOneOrThrow: returns entity when exists", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.attachOneOrThrow(
			"requiredPost",
			async (userRows) => {
				const userIds = userRows.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["posts.id", "posts.user_id", "posts.title"])
					.where("posts.user_id", "in", userIds)
					.execute();
			},
			{ matchChild: "user_id" },
		)
		.execute();

	assert.strictEqual(users[0]?.requiredPost.title, "Post 1");
});

test("attachOneOrThrow: throws when no match exists", async () => {
	await assert.rejects(async () => {
		await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id")
			.modify((qb) => qb.where("users.id", "=", 1)) // Alice has no posts
			.attachOneOrThrow(
				"requiredPost",
				async (userRows) => {
					const userIds = userRows.map((u) => u.id);
					return db
						.selectFrom("posts")
						.select(["posts.id", "posts.user_id", "posts.title"])
						.where("posts.user_id", "in", userIds)
						.execute();
				},
				{ matchChild: "user_id" },
			)
			.execute();
	}, ExpectedOneItemError);
});

//
// Complex Nesting
//

test("complex nesting: hasMany with nested hasMany and attach", async () => {
	let fetchCount = 0;

	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.hasMany(
						"comments",
						({ leftJoin }) =>
							leftJoin("comments", "comments.post_id", "posts.id").select([
								"comments.id",
								"comments.content",
							]),
						"id",
					)
					.attachMany(
						"tags",
						async (posts) => {
							fetchCount++;
							const [firstPost] = posts;

							// Ensure prefixing is working correctly.
							assert.ok(firstPost !== undefined);
							assert.ok(firstPost.id !== undefined);
							assert.ok(firstPost.title !== undefined);
							assert.ok(firstPost.comments$$content !== undefined);
							assert.ok(firstPost.comments$$id !== undefined);

							return []; // No tags in this fixture
						},
						{ matchChild: "post_id", toParent: "id" },
					),
			"id",
		)
		.execute();

	// Verify structure
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.comments.length, 2);
	assert.strictEqual(users[0]?.posts[0]?.tags.length, 0);

	// Verify fetch was called once
	assert.strictEqual(fetchCount, 1);
});

test("mixing hasOne and hasMany", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasOne(
			"profile",
			({ leftJoin }) =>
				leftJoin("profiles", "profiles.user_id", "users.id").select([
					"profiles.id",
					"profiles.bio",
				]),
			"id",
		)
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.profile?.bio, "Bio for user 2");
	assert.strictEqual(users[0]?.posts.length, 4);
});

//
// extras()
//

test("extras: computes additional fields at root level", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.extras({
			displayName: (row) => `User: ${row.username}`,
			idSquared: (row) => row.id * row.id,
		})
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.displayName, "User: bob");
	assert.strictEqual(users[0]?.idSquared, 4);
});

test("extras: work with nested collections", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.extras({
						titleUpper: (row) => row.title.toUpperCase(),
						postNumber: (row) => `Post #${row.id}`,
					}),
			"id",
		)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.titleUpper, "POST 1");
	assert.strictEqual(users[0]?.posts[0]?.postNumber, "Post #1");
});

//
// Join Types
//

test("innerJoin: adds inner join to query", async () => {
	const posts = await hydrateQuery(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
		.modify((qb) => qb.where("posts.id", "=", 1))
		.innerJoin("users", "users.id", "posts.user_id")
		.select(["users.username"])
		.execute();

	assert.strictEqual(posts[0]?.username, "bob");
});

test("innerJoin with select: columns are not prefixed at top level", async () => {
	const builder = hydrateQuery(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
		.modify((qb) => qb.where("posts.id", "=", 1))
		.innerJoin("users", "users.id", "posts.user_id")
		.select(["users.username"]);

	const rows = await builder.toQuery().execute();

	// Top-level select should not be prefixed
	assert.strictEqual(rows.length, 1);
	assert.strictEqual(rows[0]?.id, 1);
	assert.strictEqual(rows[0]?.title, "Post 1");
	assert.strictEqual(rows[0]?.username, "bob");
	assert.ok(!("users$$username" in rows[0]));
});

test("leftJoin: adds left join to query", async () => {
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.leftJoin("posts", "posts.user_id", "users.id")
		.select(["posts.title"])
		.execute();

	// Alice has no posts, so title should be null
	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.title, null);
});

test("crossJoin: adds cross join to query", async () => {
	const result = await hydrateQuery(
		db.selectFrom("users").select(["users.id"]).where("users.id", "=", 1),
		"id",
	)
		.crossJoin("profiles")
		.select(["profiles.id as profile_id"])
		.modify((qb) => qb.where("profiles.id", "=", 1))
		.execute();

	// Cross join filtered to single profile
	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.profile_id, 1);
});

//
// Lateral Joins (skip for SQLite)
//

test.skip("innerJoinLateral: adds inner join lateral", async () => {
	// SQLite does not support lateral joins
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
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

	assert.ok(users.length > 0);
});

test.skip("leftJoinLateral: adds left join lateral", async () => {
	// SQLite does not support lateral joins
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
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

	assert.ok(users.length > 0);
});

test.skip("crossJoinLateral: adds cross join lateral", async () => {
	// SQLite does not support lateral joins
	const users = await hydrateQuery(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	)
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

	assert.ok(users.length > 0);
});

//
// Edge Cases
//

test("composite keys: works with composite keyBy", async () => {
	// Create a view that uses composite keys
	const result = await hydrateQuery(
		db
			.selectFrom("posts")
			.select(["posts.id", "posts.user_id", "posts.title"])
			.where("posts.user_id", "=", 2),
		["id", "user_id"],
	)
		.hasMany(
			"comments",
			({ leftJoin }) =>
				leftJoin("comments", "comments.post_id", "posts.id").select([
					"comments.id",
					"comments.content",
				]),
			"id",
		)
		.execute();

	assert.strictEqual(result.length, 4); // Bob has 4 posts
	assert.strictEqual(result[0]?.comments.length, 2); // Post 1 has 2 comments
});

test("toQuery: returns underlying kysely query builder", async () => {
	const builder = hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]), "id");

	const query = builder.toQuery();

	// Should be able to use kysely methods directly
	const result = await query.where("users.id", "=", 1).execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.id, 1);
});

//
// Optional keyBy (defaults to "id")
//

test("hydrateQuery: keyBy defaults to 'id' when row has id", async () => {
	// keyBy omitted - should default to "id"
	const users = await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "<=", 2))
		.execute();

	assert.strictEqual(users.length, 2);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[1]?.id, 2);
});

test("hasMany: keyBy defaults to 'id' when nested row has id", async () => {
	// Both hydrateQuery and hasMany keyBy omitted
	const users = await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany("posts", ({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
		)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.posts.length, 4);
	assert.deepStrictEqual(users[0]?.posts[0], { id: 1, title: "Post 1" });
});

test("hasOne: keyBy defaults to 'id' when nested row has id", async () => {
	// Both hydrateQuery and hasOne keyBy omitted
	const posts = await hydrateQuery(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.modify((qb) => qb.where("posts.id", "=", 1))
		.hasOne("author", ({ innerJoin }) =>
			innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
		)
		.execute();

	assert.strictEqual(posts[0]?.author.username, "bob");
});

test("hasOneOrThrow: keyBy defaults to 'id' when nested row has id", async () => {
	// Both hydrateQuery and hasOneOrThrow keyBy omitted
	const users = await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "=", 1))
		.hasOneOrThrow("profile", ({ leftJoin }) =>
			leftJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id", "profiles.bio"]),
		)
		.execute();

	assert.strictEqual(users[0]?.profile.bio, "Bio for user 1");
});

test("multiple nested levels: keyBy defaults to 'id' at all levels", async () => {
	// All keyBy parameters omitted
	const users = await hydrateQuery(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany("posts", ({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.hasMany("comments", ({ leftJoin }) =>
					leftJoin("comments", "comments.post_id", "posts.id").select([
						"comments.id",
						"comments.content",
					]),
				),
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(users[0]?.posts[0]?.comments[0], {
		id: 1,
		content: "Comment 1 on post 1",
	});
});
