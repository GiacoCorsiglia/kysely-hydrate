import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { ExpectedOneItemError } from "./helpers/errors.ts";
import { hydrate } from "./query-builder.ts";

//
// Basic Query Execution
//

test("execute: returns underlying query results", async () => {
	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	).execute();

	assert.ok(Array.isArray(users));
	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users[0], { id: 1, username: "alice" });
});

test("executeTakeFirst: returns first result or undefined", async () => {
	const user = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 1),
		"id",
	).executeTakeFirst();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });

	const noUser = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 999),
		"id",
	).executeTakeFirst();

	assert.strictEqual(noUser, undefined);
});

test("executeTakeFirst: works with hasMany (user with 2+ posts)", async () => {
	// User 2 (bob) has 4 posts in the fixture
	const user = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 2),
		"id",
	)
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.executeTakeFirst();

	assert.strictEqual(user?.id, 2);
	assert.strictEqual(user?.username, "bob");
	assert.ok(Array.isArray(user?.posts));
	assert.ok(user!.posts.length >= 2, "User should have at least 2 posts");
});

test("executeTakeFirstOrThrow: returns first result or throws", async () => {
	const user = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 1),
		"id",
	).executeTakeFirstOrThrow();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });

	await assert.rejects(async () => {
		await hydrate(
			db.selectFrom("users").select(["users.id", "users.username"]).where("users.id", "=", 999),
			"id",
		).executeTakeFirstOrThrow();
	});
});

//
// Query Modification
//

test("modify: allows modifying underlying query", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "<", 3))
		.execute();

	assert.strictEqual(users.length, 2);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[1]?.id, 2);
});

//
// mapFields
//

test("mapFields: transforms field values", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.mapFields({
			username: (username) => username.toUpperCase(),
		})
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "ALICE");
});

test("mapFields: leaves unmapped fields unchanged", async () => {
	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.mapFields({
			username: (username) => username.toUpperCase(),
		})
		.execute();

	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "ALICE");
	assert.strictEqual(users[0]?.email, "alice@example.com"); // unchanged
});

test("mapFields: works in nested collections", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.mapFields({
						title: (title) => title.toUpperCase(),
					}),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.title, "POST 1");
	assert.strictEqual(users[0]?.posts[0]?.id, 1); // unchanged
});

//
// omit
//

test("omit: removes fields from output", async () => {
	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.omit(["email"])
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual("email" in users[0]!, false);
});

test("omit: works with extras to hide implementation details", async () => {
	// Select username and email, then use them to compute displayName, but hide email
	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.extras({
			displayName: (row) => `${row.username} <${row.email}>`,
		})
		.omit(["email"])
		.execute();

	assert.strictEqual(users[0]?.displayName, "alice <alice@example.com>");
	assert.strictEqual("email" in users[0]!, false);
	// Other fields still present
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
});

test("omit: works in nested collections", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title", "posts.content"])
					.omit(["content"]),
			"id",
		)
		.execute();

	assert.strictEqual(users[0]?.posts.length, 4);
	assert.strictEqual(users[0]?.posts[0]?.id, 1);
	assert.strictEqual(users[0]?.posts[0]?.title, "Post 1");
	assert.strictEqual("content" in users[0]!.posts[0]!, false);
});

//
// with
//

test("with: merges fields from hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
		email: string;
	}

	const extraFields = createHydrator<User>("id").fields({ email: true });

	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.with(extraFields)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual(users[0]?.email, "alice@example.com");
});

test("with: merges extras from hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
		email: string;
	}

	const extraFields = createHydrator<User>("id").extras({
		displayName: (user) => `${user.username} <${user.email}>`,
	});

	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.with(extraFields)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.displayName, "alice <alice@example.com>");
});

test("with: other hydrator's configuration takes precedence", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
	}

	const override = createHydrator<User>("id").fields({
		username: (username) => username.toUpperCase(),
	});

	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.mapFields({
			username: (username) => username.toLowerCase(),
		})
		.with(override)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.username, "ALICE"); // override wins
});

test("with: works with omit in hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
		email: string;
	}

	const withExtras = createHydrator<User>("id")
		.extras({
			displayName: (user) => `${user.username} <${user.email}>`,
		})
		.omit(["email"]);

	const users = await hydrate(
		db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
		"id",
	)
		.modify((qb) => qb.where("users.id", "=", 1))
		.with(withExtras)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual(users[0]?.displayName, "alice <alice@example.com>");
	assert.strictEqual("email" in users[0]!, false);
});

//
// hasMany
//

test("hasMany: creates nested array via left join", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const builder = hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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

test("hasMany: generates SQL with correctly prefixed column aliases", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	).hasMany(
		"posts",
		({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id").select([
				"posts.id",
				"posts.title as postTitle",
				"posts.content",
			]),
		"id",
	);

	const compiled = builder.toQuery().compile();

	// Verify parent columns are not prefixed and nested columns are prefixed with collection name
	// Both regular columns and aliased columns should be prefixed
	assert.ok(
		compiled.sql.startsWith(
			'select "users"."id", "users"."username", "posts"."id" as "posts$$id", "posts"."title" as "posts$$postTitle", "posts"."content" as "posts$$content"',
		),
	);
});

test("hasMany: returns empty array when no matches", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const builder = hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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

test("hasMany: generates SQL with doubly-prefixed aliases for nested collections", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username as name"]),
		"id",
	).hasMany(
		"posts",
		({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title as postTitle"])
				.hasMany(
					"comments",
					({ leftJoin }) =>
						leftJoin("comments", "comments.post_id", "posts.id").select([
							"comments.id as commentId",
							"comments.content",
						]),
					"commentId",
				),
		"id",
	);

	const compiled = builder.toQuery().compile();

	// Verify parent aliases not prefixed, first-level uses single prefix, second-level uses double prefix
	// All user-defined aliases and regular columns should be prefixed appropriately
	assert.ok(
		compiled.sql.startsWith(
			'select "users"."id", "users"."username" as "name", "posts"."id" as "posts$$id", "posts"."title" as "posts$$postTitle", "comments"."id" as "posts$$comments$$commentId", "comments"."content" as "posts$$comments$$content"',
		),
	);
});

//
// hasOne
//

test("hasOne: creates nullable nested object via left join", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const posts = await hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
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
	const builder = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
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

test("hasOne: generates SQL with correctly prefixed column aliases", async () => {
	const builder = hydrate(
		db.selectFrom("posts").select(["posts.id", "posts.title as postTitle"]),
		"id",
	).hasOne(
		"author",
		({ innerJoin }) =>
			innerJoin("users", "users.id", "posts.user_id").select([
				"users.id as userId",
				"users.username",
				"users.email as authorEmail",
			]),
		"userId",
	);

	const compiled = builder.toQuery().compile();

	// Verify parent aliases not prefixed and nested columns/aliases prefixed with relation name
	assert.ok(
		compiled.sql.startsWith(
			'select "posts"."id", "posts"."title" as "postTitle", "users"."id" as "author$$userId", "users"."username" as "author$$username", "users"."email" as "author$$authorEmail"',
		),
	);
});

test("hasOne: returns null when no match with left join", async () => {
	// Create a temporary user with no posts to test null case
	const result = await db
		.insertInto("users")
		.values({ username: "temp", email: "temp@example.com" })
		.returningAll()
		.executeTakeFirstOrThrow();

	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
		await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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

	await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
		await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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

	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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

test("mixing hasOne and hasMany: generates SQL with correctly prefixed aliases", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username as userName"]),
		"id",
	)
		.hasOne(
			"profile",
			({ leftJoin }) =>
				leftJoin("profiles", "profiles.user_id", "users.id").select([
					"profiles.id as profileId",
					"profiles.bio",
				]),
			"profileId",
		)
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id").select([
					"posts.id",
					"posts.title as postTitle",
					"posts.content",
				]),
			"id",
		);

	const compiled = builder.toQuery().compile();

	// Verify parent aliases not prefixed, hasOne and hasMany each use their own prefixes
	// Both user-defined aliases and regular columns should be handled correctly
	assert.ok(
		compiled.sql.startsWith(
			'select "users"."id", "users"."username" as "userName", "profiles"."id" as "profile$$profileId", "profiles"."bio" as "profile$$bio", "posts"."id" as "posts$$id", "posts"."title" as "posts$$postTitle", "posts"."content" as "posts$$content"',
		),
	);
});

//
// extras()
//

test("extras: computes additional fields at root level", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
// map()
//

test("map: transforms hydrated output", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.map((user) => ({ userId: user.id, userName: user.username }))
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.userId, 1);
	assert.strictEqual(users[0]?.userName, "alice");
	assert.strictEqual("id" in users[0]!, false);
	assert.strictEqual("username" in users[0]!, false);
});

test("map: allows chaining multiple transformations", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.map((user) => ({ ...user, upper: user.username.toUpperCase() }))
		.map((user) => ({ final: user.upper }))
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.final, "ALICE");
	assert.strictEqual("id" in users[0]!, false);
	assert.strictEqual("username" in users[0]!, false);
	assert.strictEqual("upper" in users[0]!, false);
});

test("map: transforms into class instances", async () => {
	class UserModel {
		id: number;
		name: string;

		constructor(id: number, name: string) {
			this.id = id;
			this.name = name;
		}

		getDisplayName() {
			return `User: ${this.name}`;
		}
	}

	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.map((user) => new UserModel(user.id, user.username))
		.execute();

	assert.strictEqual(users.length, 1);
	assert.ok(users[0] instanceof UserModel);
	assert.strictEqual(users[0]?.getDisplayName(), "User: alice");
});

test("map: works with nested collections", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 2))
		.hasMany(
			"posts",
			({ leftJoin }) =>
				leftJoin("posts", "posts.user_id", "users.id")
					.select(["posts.id", "posts.title"])
					.map((post) => ({ postId: post.id, postTitle: post.title })),
			"id",
		)
		.map((user) => {
			const [firstPost] = user.posts;
			assert.ok(firstPost !== undefined);
			assert.deepStrictEqual(firstPost, {
				postId: 1,
				postTitle: "Post 1",
			});

			return { userName: user.username, postCount: user.posts.length };
		})
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.userName, "bob");
	assert.strictEqual(users[0]?.postCount, 4);
	assert.strictEqual("id" in users[0]!, false);
	assert.strictEqual("posts" in users[0]!, false);
});

test("map: works with attached collections", async () => {
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
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
		.map((user) => ({
			userName: user.username,
			postTitles: user.posts.map((p) => p.title),
		}))
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.userName, "bob");
	assert.strictEqual(users[0]?.postTitles.length, 4);
	assert.strictEqual(users[0]?.postTitles[0], "Post 1");
});

//
// Join Types
//

test("innerJoin: adds inner join to query", async () => {
	const posts = await hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
		.modify((qb) => qb.where("posts.id", "=", 1))
		.innerJoin("users", "users.id", "posts.user_id")
		.select(["users.username"])
		.execute();

	assert.strictEqual(posts[0]?.username, "bob");
});

test("innerJoin with select: columns are not prefixed at top level", async () => {
	const builder = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]), "id")
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
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.modify((qb) => qb.where("users.id", "=", 1))
		.leftJoin("posts", "posts.user_id", "users.id")
		.select(["posts.title"])
		.execute();

	// Alice has no posts, so title should be null
	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.title, null);
});

test("crossJoin: adds cross join to query", async () => {
	const result = await hydrate(
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
// innerJoinLateral with HydratedQueryBuilder
//

test("innerJoinLateral with hydrated subquery: generates correct SQL with prefixed columns", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	).hasOne("latestPost", ({ innerJoinLateral }) =>
		innerJoinLateral(
			(db) =>
				hydrate(
					db
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.limit(1),
					"id",
				).as("latest_post"),
			// Omitting `(join) => join.onTrue()` as it should be the default.
		),
	);

	const compiled = builder.toQuery().compile();

	assert.strictEqual(
		compiled.sql,
		[
			"select ",
			'"users"."id", ',
			'"users"."username", ',
			'"latest_post"."id" as "latestPost$$id", ',
			'"latest_post"."title" as "latestPost$$title" ',
			'from "users" ',
			"inner join lateral (",
			'select "posts"."id", "posts"."title" ',
			'from "posts" ',
			'where "posts"."user_id" = "users"."id" ',
			"limit ?",
			') as "latest_post" on true',
		].join(""),
	);
});

test("innerJoinLateral with hydrated subquery: handles column aliases", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username as userName"]),
		"id",
	).hasOne(
		"latestPost",
		({ innerJoinLateral }) =>
			innerJoinLateral(
				(db) =>
					hydrate(
						db
							.selectFrom("posts")
							.select(["posts.id as postId", "posts.title", "posts.content as postContent"])
							.whereRef("posts.user_id", "=", "users.id")
							.orderBy("posts.id", "desc")
							.limit(1),
						"postId",
					).as("latest_post"),
				(join) => join.onTrue(),
			),
		"postId",
	);

	const compiled = builder.toQuery().compile();

	assert.strictEqual(
		compiled.sql,
		[
			"select ",
			'"users"."id", ',
			'"users"."username" as "userName", ',
			'"latest_post"."postId" as "latestPost$$postId", ',
			'"latest_post"."title" as "latestPost$$title", ',
			'"latest_post"."postContent" as "latestPost$$postContent" ',
			'from "users" ',
			"inner join lateral (",
			"select ",
			'"posts"."id" as "postId", ',
			'"posts"."title", ',
			'"posts"."content" as "postContent" ',
			'from "posts" ',
			'where "posts"."user_id" = "users"."id" ',
			'order by "posts"."id" desc ',
			"limit ?",
			') as "latest_post" on true',
		].join(""),
	);
});

test("innerJoinLateral with nested hydrated subquery: generates doubly-prefixed columns", async () => {
	const builder = hydrate(
		db.selectFrom("users").select(["users.id", "users.username"]),
		"id",
	).hasOne("latestPost", ({ innerJoinLateral }) =>
		innerJoinLateral(
			(db) =>
				hydrate(
					db
						.selectFrom("posts")
						.select(["posts.id", "posts.title"])
						.whereRef("posts.user_id", "=", "users.id")
						.limit(1),
					"id",
				)
					.hasMany(
						"comments",
						({ leftJoin }) =>
							leftJoin("comments", "comments.post_id", "posts.id").select([
								"comments.id",
								"comments.content as commentText",
							]),
						"id",
					)
					.as("latest_post"),
			(join) => join.onTrue(),
		),
	);

	const compiled = builder.toQuery().compile();

	assert.strictEqual(
		compiled.sql,
		[
			"select ",
			'"users"."id", ',
			'"users"."username", ',
			'"latest_post"."id" as "latestPost$$id", ',
			'"latest_post"."title" as "latestPost$$title", ',
			'"latest_post"."comments$$id" as "latestPost$$comments$$id", ',
			'"latest_post"."comments$$commentText" as "latestPost$$comments$$commentText" ',
			'from "users" ',
			"inner join lateral (",
			"select ",
			'"posts"."id", ',
			'"posts"."title", ',
			'"comments"."id" as "comments$$id", ',
			'"comments"."content" as "comments$$commentText" ',
			'from "posts" ',
			'left join "comments" on "comments"."post_id" = "posts"."id" ',
			'where "posts"."user_id" = "users"."id" ',
			"limit ?",
			') as "latest_post" on true',
		].join(""),
	);
});

test("innerJoinLateral with multiple hydrated subqueries: prefixes each independently", async () => {
	const builder = hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.hasOne(
			"firstPost",
			({ innerJoinLateral }) =>
				innerJoinLateral(
					(db) =>
						hydrate(
							db
								.selectFrom("posts")
								.select(["posts.id as firstId", "posts.title as firstTitle"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id", "asc")
								.limit(1),
							"firstId",
						).as("first_post"),
					(join) => join.onTrue(),
				),
			"firstId",
		)
		.hasOne(
			"lastPost",
			({ innerJoinLateral }) =>
				innerJoinLateral(
					(db) =>
						hydrate(
							db
								.selectFrom("posts")
								.select(["posts.id as lastId", "posts.title as lastTitle"])
								.whereRef("posts.user_id", "=", "users.id")
								.orderBy("posts.id", "desc")
								.limit(1),
							"lastId",
						).as("last_post"),
					(join) => join.onTrue(),
				),
			"lastId",
		);

	const compiled = builder.toQuery().compile();

	assert.strictEqual(
		compiled.sql,
		[
			"select ",
			'"users"."id", ',
			'"users"."username", ',
			'"first_post"."firstId" as "firstPost$$firstId", ',
			'"first_post"."firstTitle" as "firstPost$$firstTitle", ',
			'"last_post"."lastId" as "lastPost$$lastId", ',
			'"last_post"."lastTitle" as "lastPost$$lastTitle" ',
			'from "users" ',
			"inner join lateral (",
			'select "posts"."id" as "firstId", ',
			'"posts"."title" as "firstTitle" ',
			'from "posts" ',
			'where "posts"."user_id" = "users"."id" ',
			'order by "posts"."id" asc ',
			"limit ?",
			') as "first_post" on true ',
			"inner join lateral (",
			'select "posts"."id" as "lastId", ',
			'"posts"."title" as "lastTitle" ',
			'from "posts" ',
			'where "posts"."user_id" = "users"."id" ',
			'order by "posts"."id" desc ',
			"limit ?",
			') as "last_post" on true',
		].join(""),
	);
});

//
// Edge Cases
//

test("composite keys: works with composite keyBy", async () => {
	// Create a view that uses composite keys
	const result = await hydrate(
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
	const builder = hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id");

	const query = builder.toQuery();

	// Should be able to use kysely methods directly
	const result = await query.where("users.id", "=", 1).execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.id, 1);
});

//
// Optional keyBy (defaults to "id")
//

test("hydrate: keyBy defaults to 'id' when row has id", async () => {
	// keyBy omitted - should default to "id"
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "<=", 2))
		.execute();

	assert.strictEqual(users.length, 2);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[1]?.id, 2);
});

test("hasMany: keyBy defaults to 'id' when nested row has id", async () => {
	// Both hydrate and hasMany keyBy omitted
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
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
	// Both hydrate and hasOne keyBy omitted
	const posts = await hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.modify((qb) => qb.where("posts.id", "=", 1))
		.hasOne("author", ({ innerJoin }) =>
			innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
		)
		.execute();

	assert.strictEqual(posts[0]?.author.username, "bob");
});

test("hasOneOrThrow: keyBy defaults to 'id' when nested row has id", async () => {
	// Both hydrate and hasOneOrThrow keyBy omitted
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.modify((qb) => qb.where("users.id", "=", 1))
		.hasOneOrThrow("profile", ({ leftJoin }) =>
			leftJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id", "profiles.bio"]),
		)
		.execute();

	assert.strictEqual(users[0]?.profile.bio, "Bio for user 1");
});

test("multiple nested levels: keyBy defaults to 'id' at all levels", async () => {
	// All keyBy parameters omitted
	const users = await hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
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

//
// innerJoinLateral with AliasedHydratedExpression
//

// Note: Tests for innerJoinLateral with AliasedHydratedExpression are skipped
// because SQLite does not support lateral joins. The feature can be tested
// manually with PostgreSQL or similar databases that support lateral joins.
