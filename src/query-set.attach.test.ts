import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";

const db = getDbForTest();
import { CardinalityViolationError, ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

describe("query-set: attach", () => {
	//
	// Attach Methods - attachMany, attachOne, attachOneOrThrow
	//

	test("attachMany: fetches and matches related entities", async () => {
		const fetchPosts = async () => {
			// Attached arrays preserve fetch order, so the fetch needs an ORDER BY
			// for the deterministic comparison below
			return await db
				.selectFrom("posts")
				.select(["id", "title", "user_id"])
				.where("user_id", "in", [2, 3])
				.orderBy("id")
				.execute();
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
		// toParent names a NON-key parent column: if toParent were ignored, the
		// default (the keyBy, id = 2) would match nothing and badges would be []
		const fetchBadges = async () => {
			return [
				{ awardedTo: "bob", badge: "early-adopter" },
				{ awardedTo: "bob", badge: "contributor" },
			];
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachMany("badges", fetchBadges, { matchChild: "awardedTo", toParent: "username" })
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				badges: [
					{ awardedTo: "bob", badge: "early-adopter" },
					{ awardedTo: "bob", badge: "contributor" },
				],
			},
		]);
	});

	test("attachMany: accepts QuerySet return from fetchFn", async () => {
		const fetchPosts = () => {
			return querySet(db).selectAs(
				"post",
				db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
			);
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			return db
				.selectFrom("posts")
				.select(["id", "title", "user_id"])
				.where("user_id", "=", 2)
				.orderBy("id"); // Attached arrays preserve fetch order
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
				.orderBy("id") // Attached arrays preserve fetch order
				.execute();
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
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
		// Only alice's profile is fetched, so bob — a parent that exists but has
		// no matching child — gets null
		const fetchProfile = async () => {
			return await db
				.selectFrom("profiles")
				.select(["id", "bio", "user_id"])
				.where("user_id", "=", 1)
				.execute();
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.attachOne("profile", fetchProfile, { matchChild: "user_id" })
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
			},
			{
				id: 2,
				username: "bob",
				profile: null,
			},
		]);
	});

	test("attachOne: throws on cardinality violation", async () => {
		const fetchPosts = async () => {
			return await db.selectFrom("posts").select(["id", "title", "user_id"]).execute();
		};

		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachOne("post", fetchPosts, { matchChild: "user_id" });

		await assert.rejects(async () => {
			await qs.execute();
		}, CardinalityViolationError);
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]),
					).attachOne("latestComment", fetchLatestComment, {
						matchChild: "post_id",
						toParent: "id",
					}),
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
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
			return await db
				.selectFrom("users")
				.select(["id", "username"])
				.where("id", "=", 999)
				.execute();
		};

		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
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

	test("attachMany: configures the attached QuerySet inside the fetchFn", async () => {
		const fetchPosts = () => {
			// Configure the QuerySet before returning it
			return querySet(db)
				.selectAs(
					"post",
					db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
				)
				.where("posts.id", "<=", 2)
				.extras({
					titleLength: (row) => row.title.length,
				});
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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

	//
	// .modify("<attachKey>", …) — the three documented forms: the modifier
	// receives whatever the fetchFn returned (QuerySet, query builder, or the
	// value/promise itself).
	//

	test("attachMany: modify attached QuerySet via .modify(key, fn)", async () => {
		const fetchPosts = () =>
			querySet(db).selectAs(
				"post",
				db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
			);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachMany("posts", fetchPosts, { matchChild: "user_id" })
			.modify("posts", (posts) =>
				posts.where("posts.id", "<=", 2).extras({ titleLength: (row) => row.title.length }),
			)
			.execute();

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

	test("attachMany: modify attached query builder via .modify(key, fn)", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachMany(
				"posts",
				() => db.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 2),
				{ matchChild: "user_id" },
			)
			.modify("posts", (qb) => qb.where("posts.id", "<=", 2))
			.execute();

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

	test("attachMany: replaces a join collection previously defined under the same key", async () => {
		// Regression test: attach* used to register only on the hydrator,
		// leaving the same-key join in the compiled SQL (row explosion would
		// inflate executeCount; the join's prefixed columns went unconsumed)
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2) // bob has 4 posts via the join
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.attachMany("posts", () => [{ ownerId: 2, title: "attached" }], { matchChild: "ownerId" });

		assert.ok(!qs.toQuery().compile().sql.includes("posts$$"));
		assert.strictEqual(await qs.executeCount(Number), 1);
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 2, username: "bob", posts: [{ ownerId: 2, title: "attached" }] },
		]);
	});

	test("leftJoinMany: replaces an attach collection previously defined under the same key", async () => {
		// Regression test: joins registered only their own collection on the
		// hydrator, so the same-key stale attach fetchFn still ran (a spurious
		// query) and its output clobbered the join's data during hydration
		let fetchCount = 0;
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachMany(
				"posts",
				() => {
					fetchCount++;
					return [{ ownerId: 2, title: "FROM STALE ATTACH" }];
				},
				{ matchChild: "ownerId" },
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1)),
				"posts.user_id",
				"user.id",
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 2, username: "bob", posts: [{ id: 1, title: "Post 1", user_id: 2 }] },
		]);
		assert.strictEqual(fetchCount, 0);
	});

	test("attachMany: replaces a oneOrThrow join collection previously defined under the same key", async () => {
		// Regression test: the overridden join's SQL was correctly removed, but
		// its stale oneOrThrow hydrator spec remained, read the now-nonexistent
		// prefixed columns, and threw ExpectedOneItemError during hydration
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.leftJoinOneOrThrow(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1)),
				"posts.user_id",
				"user.id",
			)
			.attachMany("posts", () => [{ ownerId: 2, title: "attached" }], { matchChild: "ownerId" })
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob", posts: [{ ownerId: 2, title: "attached" }] },
		]);
	});

	test("attachMany: overriding a join also drops the join's nested attach fetches", async () => {
		// Regression test: an attach nested inside an overridden join's query set
		// still ran its fetchFn (a stale side effect) even though the join itself
		// was replaced
		let nestedFetchCount = 0;
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).attachMany(
						"comments",
						() => {
							nestedFetchCount++;
							return [];
						},
						{ matchChild: "post_id", toParent: "id" },
					),
				"posts.user_id",
				"user.id",
			)
			.attachMany("posts", () => [{ ownerId: 2, title: "attached" }], { matchChild: "ownerId" })
			.execute();

		assert.strictEqual(nestedFetchCount, 0);
		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob", posts: [{ ownerId: 2, title: "attached" }] },
		]);
	});

	test("attachMany: modify attached external values via .modify(key, fn)", async () => {
		const fetchBadges = async () => [
			{ ownerId: 2, badge: "founder" },
			{ ownerId: 2, badge: "contributor" },
		];

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.attachMany("badges", fetchBadges, { matchChild: "ownerId" })
			.modify("badges", async (badgesPromise) =>
				(await badgesPromise).map((b) => ({ ...b, badgeUpper: b.badge.toUpperCase() })),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				badges: [
					{ ownerId: 2, badge: "founder", badgeUpper: "FOUNDER" },
					{ ownerId: 2, badge: "contributor", badgeUpper: "CONTRIBUTOR" },
				],
			},
		]);
	});

	test("attachMany: with nested join and attach combination", async () => {
		const fetchComments = async () => {
			// Attached arrays preserve fetch order
			return await db
				.selectFrom("comments")
				.select(["id", "content", "post_id"])
				.orderBy("id")
				.execute();
		};

		const fetchTags = async () => {
			return [
				{ id: 1, name: "typescript", post_id: 1 },
				{ id: 2, name: "kysely", post_id: 1 },
				{ id: 3, name: "nodejs", post_id: 2 },
			];
		};

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 2]))
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

	test("attachMany: toParent defaults to the query set's explicit keyBy", async () => {
		// Regression test: when keys.toParent is omitted, matching must fall back
		// to the keyBy passed to selectAs.  Previously it fell back to the
		// hydrator's default ("id") regardless of the explicit keyBy.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]), "username")
			.where("users.id", "in", [1, 2])
			.attachMany(
				"badges",
				() => [
					{ awardedTo: "alice", badge: "founder" },
					{ awardedTo: "bob", badge: "early-adopter" },
					{ awardedTo: "bob", badge: "contributor" },
				],
				{ matchChild: "awardedTo" },
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				badges: [{ awardedTo: "alice", badge: "founder" }],
			},
			{
				id: 2,
				username: "bob",
				badges: [
					{ awardedTo: "bob", badge: "early-adopter" },
					{ awardedTo: "bob", badge: "contributor" },
				],
			},
		]);
	});

	//
	// The fetchFn input contract: one input per parent entity.  Raw joined rows
	// repeat parents (row explosion from sibling many-joins) and contain
	// all-null phantom rows (left joins with no match); fetchFns must see
	// neither.
	//

	test("attach: fetchFn receives each parent entity once despite many-join row explosion", async () => {
		const received: { id: number; username: string }[][] = [];

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2, 3])
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.attachMany(
				"badges",
				(parents) => {
					received.push(parents.map((p) => ({ id: p.id, username: p.username })));
					return [];
				},
				{ matchChild: "ownerId" },
			)
			.execute();

		assert.strictEqual(users.length, 3);
		// Called exactly once, with one input per user — even though bob has 4
		// posts and carol has 2, so the raw joined rows repeat them.
		assert.deepStrictEqual(received, [
			[
				{ id: 1, username: "alice" },
				{ id: 2, username: "bob" },
				{ id: 3, username: "carol" },
			],
		]);
	});

	test("attach: nested fetchFn receives deduplicated parents without phantom null rows", async () => {
		const received: { id: number | null; title: string | null }[][] = [];

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			// User 1 (alice) has no posts: her joined row has all-null post columns.
			.where("users.id", "in", [1, 2, 3])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
						"comments",
						({ eb: eb2, qs: qs2 }) =>
							qs2(eb2.selectFrom("comments").select(["id", "post_id", "content"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.modify("posts", (posts) =>
				posts.attachMany(
					"tags",
					(parents) => {
						received.push(parents.map((p) => ({ id: p.id, title: p.title })));
						return [];
					},
					{ matchChild: "postId" },
				),
			)
			.execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(received.length, 1); // Called exactly once.

		const parents = received[0]!;
		// No phantom row from alice's matchless left join.
		assert.ok(
			parents.every((p) => p.id !== null),
			`expected no null-key parents, got ${JSON.stringify(parents)}`,
		);
		// No duplicates from the comments sibling join exploding post rows.
		const ids = parents.map((p) => p.id);
		assert.deepStrictEqual(ids, [...new Set(ids)]);
		// And every real post is present exactly once.
		assert.deepStrictEqual(
			ids.toSorted((a, b) => a! - b!),
			[1, 2, 3, 5, 12, 15],
		);
	});
});
