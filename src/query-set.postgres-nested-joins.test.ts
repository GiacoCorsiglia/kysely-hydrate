import assert from "node:assert";
import { test } from "node:test";

import { sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { fixLongAliases } from "./fix-long-aliases.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Nested Joins: PostgreSQL-only shapes
//
// Nested joins combined with lateral joins (at the parent and the nested
// level), write queries (UPDATE/INSERT RETURNING and data-modifying CTEs),
// and keys long enough that the generated aliases pass the 63-byte limit.
//

const comment = (id: number, postId: number) => ({ id, post_id: postId });

describePg("query-set: postgres nested joins", () => {
	//
	// Lateral joins.
	//

	test("lateral parent with a nested non-lateral join", async () => {
		const build = (limit: number | null) =>
			querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "in", [1, 2])
				.leftJoinLateralMany(
					"posts",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "user_id"])
								.whereRef("posts.user_id", "=", "user.id"),
						)
							.orderBy("id", "desc")
							.limit(limit)
							.leftJoinMany(
								"comments",
								({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "post_id"])),
								"comments.post_id",
								"posts.id",
							),
					(join) => join.onTrue(),
				);

		// Unpaginated: the nested orderBy sorts the hydrated posts.
		assert.deepStrictEqual(await build(null).execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 12, user_id: 2, comments: [] },
					{ id: 5, user_id: 2, comments: [comment(5, 5)] },
					{ id: 2, user_id: 2, comments: [comment(3, 2)] },
					{ id: 1, user_id: 2, comments: [comment(1, 1), comment(2, 1)] },
				],
			},
		]);
		// Top 2 posts per user.
		assert.deepStrictEqual(await build(2).execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 12, user_id: 2, comments: [] },
					{ id: 5, user_id: 2, comments: [comment(5, 5)] },
				],
			},
		]);
		assert.deepStrictEqual(await build(2).limit(1).offset(1).execute(), [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 12, user_id: 2, comments: [] },
					{ id: 5, user_id: 2, comments: [comment(5, 5)] },
				],
			},
		]);
	});

	for (const method of ["leftJoinLateralOne", "innerJoinLateralOne"] as const) {
		test(`nested ${method} correlated to the nested base alias`, async () => {
			// The posts query set is aliased "p" but joined as "posts"; its lateral
			// subquery references "p".
			const posts = (
				querySet(db).selectAs("p", db.selectFrom("posts").select(["id", "user_id"])) as any
			)[method](
				"latestComment",
				({ eb, qs }: any) =>
					qs(
						eb
							.selectFrom("comments")
							.select(["id", "post_id"])
							.whereRef("comments.post_id", "=", "p.id" as any),
					)
						.orderBy("id", "desc")
						.limit(1),
				(join: any) => join.onTrue(),
			);

			const qs = querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "in", [1, 2])
				.leftJoinMany("posts", posts, "posts.user_id", "user.id");

			const bobPosts = [
				{ id: 1, user_id: 2, latestComment: comment(2, 1) },
				{ id: 2, user_id: 2, latestComment: comment(3, 2) },
				{ id: 5, user_id: 2, latestComment: comment(5, 5) },
				...(method === "leftJoinLateralOne" ? [{ id: 12, user_id: 2, latestComment: null }] : []),
			];
			const expected = [
				{ id: 1, username: "alice", posts: [] },
				{ id: 2, username: "bob", posts: bobPosts },
			];
			assert.deepStrictEqual(await qs.execute(), expected);
			assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
			assert.strictEqual(await qs.executeCount(Number), 2);
		});
	}

	test("nested crossJoinLateralMany inside leftJoinMany", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				querySet(db)
					.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
					.crossJoinLateralMany("comments", ({ eb, qs }) =>
						qs(
							eb
								.selectFrom("comments")
								.select(["id", "post_id"])
								.whereRef("comments.post_id", "=", "p.id" as any),
						),
					),
				"posts.user_id",
				"user.id",
			);

		// Post 12 has no comments, so the cross lateral join drops it (but not bob).
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, user_id: 2, comments: [comment(1, 1), comment(2, 1)] },
					{ id: 2, user_id: 2, comments: [comment(3, 2)] },
					{ id: 5, user_id: 2, comments: [comment(5, 5)] },
				],
			},
		]);
	});

	//
	// Writes.
	//

	const postsWithCommentsAndReplies = (trx: typeof db) =>
		querySet(trx)
			.selectAs("p", trx.selectFrom("posts").select(["id", "user_id"]))
			.leftJoinMany(
				"comments",
				({ eb, qs }) =>
					qs(eb.selectFrom("comments").select(["id", "post_id"])).innerJoinMany(
						"replies",
						({ eb, qs }) => qs(eb.selectFrom("replies").select(["id", "comment_id"])),
						"replies.comment_id",
						"comments.id",
					),
				"comments.post_id",
				"p.id",
			);

	const bobPostsWithReplies = [
		{
			id: 1,
			user_id: 2,
			comments: [
				{
					...comment(1, 1),
					replies: [
						{ id: 2, comment_id: 1 },
						{ id: 4, comment_id: 1 },
					],
				},
			],
		},
		{
			id: 2,
			user_id: 2,
			comments: [
				{
					...comment(3, 2),
					replies: [
						{ id: 1, comment_id: 3 },
						{ id: 5, comment_id: 3 },
					],
				},
			],
		},
		{ id: 5, user_id: 2, comments: [{ ...comment(5, 5), replies: [{ id: 3, comment_id: 5 }] }] },
		{ id: 12, user_id: 2, comments: [] },
	];

	test("update() with nested joins, with and without pagination", async () => {
		await testInTransaction(db, async (trx) => {
			const qs = querySet(trx)
				.selectAs("user", trx.selectFrom("users").select(["id", "username"]))
				.leftJoinMany("posts", postsWithCommentsAndReplies(trx), "posts.user_id", "user.id")
				.update((db) =>
					db
						.updateTable("users")
						.set({ username: sql`upper(username)` })
						.where("id", "in", [1, 2])
						.returning(["id", "username"]),
				);

			assert.deepStrictEqual(await qs.execute(), [
				{ id: 1, username: "ALICE", posts: [] },
				{ id: 2, username: "BOB", posts: bobPostsWithReplies },
			]);
		});

		await testInTransaction(db, async (trx) => {
			const qs = querySet(trx)
				.selectAs("user", trx.selectFrom("users").select(["id", "username"]))
				.leftJoinMany("posts", postsWithCommentsAndReplies(trx), "posts.user_id", "user.id")
				.update((db) =>
					db
						.updateTable("users")
						.set({ username: sql`upper(username)` })
						.where("id", "in", [1, 2])
						.returning(["id", "username"]),
				)
				.limit(1)
				.offset(1);

			assert.deepStrictEqual(await qs.execute(), [
				{ id: 2, username: "BOB", posts: bobPostsWithReplies },
			]);
		});
	});

	test("insertAs() with a nested one-join and nested many-joins", async () => {
		await testInTransaction(db, async (trx) => {
			const [result, ...rest] = await querySet(trx)
				.insertAs("post", (db) =>
					db
						.insertInto("posts")
						.values({ user_id: 2, title: "New", content: "New content" })
						.returning(["id", "user_id", "title"]),
				)
				.innerJoinOne(
					"author",
					({ eb, qs }) =>
						qs(eb.selectFrom("users").select(["id", "username"])).leftJoinMany(
							"authorPosts",
							postsWithCommentsAndReplies(trx),
							"authorPosts.user_id",
							"author.id",
						),
					"author.id",
					"post.user_id",
				)
				.execute();

			assert.deepStrictEqual(rest, []);
			assert.ok(result);
			// The CTE's own insert is not visible to the joined posts (statement snapshot).
			assert.deepStrictEqual(result.author, {
				id: 2,
				username: "bob",
				authorPosts: bobPostsWithReplies,
			});
			assert.strictEqual(result.title, "New");
		});
	});

	test("writeAs() with nested joins", async () => {
		await testInTransaction(db, async (trx) => {
			const qs = querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "changed@example.com" })
								.where("id", "in", [1, 2])
								.returning(["id", "username", "email"]),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.leftJoinMany("posts", postsWithCommentsAndReplies(trx), "posts.user_id", "updated.id");

			const expected = [
				{ id: 1, username: "alice", email: "changed@example.com", posts: [] },
				{ id: 2, username: "bob", email: "changed@example.com", posts: bobPostsWithReplies },
			];
			assert.deepStrictEqual(await qs.execute(), expected);
		});
	});

	//
	// Keys whose aliases pass the 63-byte limit at every level.
	//

	test("fixLongAliases: three levels of long keys with aliases different from the keys", async () => {
		const longDb = db.withPlugin(fixLongAliases());
		const POSTS = "postsAuthoredByThisParticularUserAccount";
		const COMMENTS = "commentsLeftOnThatParticularPostByAnybody";
		const REPLIES = "repliesWrittenInResponseToThatVeryComment";

		const qs = querySet(longDb)
			.selectAs("user", longDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				POSTS,
				querySet(longDb)
					.selectAs("p", longDb.selectFrom("posts").select(["id", "user_id"]))
					.innerJoinOne(
						"author",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
						"author.id",
						"p.user_id",
					)
					.leftJoinMany(
						COMMENTS,
						querySet(longDb)
							.selectAs("c", longDb.selectFrom("comments").select(["id", "post_id"]))
							.leftJoinMany(
								REPLIES,
								({ eb, qs }) => qs(eb.selectFrom("replies").select(["id", "comment_id"])),
								`${REPLIES}.comment_id`,
								"c.id",
							),
						`${COMMENTS}.post_id`,
						"p.id",
					),
				`${POSTS}.user_id`,
				"user.id",
			);

		const bob = { id: 2, username: "bob" };
		const bobPosts = bobPostsWithReplies.map((post) => ({
			id: post.id,
			user_id: post.user_id,
			author: bob,
			[COMMENTS]: (
				{
					1: [
						{ ...comment(1, 1), [REPLIES]: [2, 4].map((id) => ({ id, comment_id: 1 })) },
						{ ...comment(2, 1), [REPLIES]: [] },
					],
					2: [{ ...comment(3, 2), [REPLIES]: [1, 5].map((id) => ({ id, comment_id: 3 })) }],
					5: [{ ...comment(5, 5), [REPLIES]: [{ id: 3, comment_id: 5 }] }],
					12: [],
				} as Record<number, unknown[]>
			)[post.id],
		}));
		const expected = [
			{ id: 1, username: "alice", [POSTS]: [] },
			{ ...bob, [POSTS]: bobPosts },
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
		assert.strictEqual(await qs.executeCount(Number), 2);
	});
});
