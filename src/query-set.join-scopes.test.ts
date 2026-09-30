import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin, sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Name Scoping in Nested Joins
//
// Each nested query set is its own scope: its ON clauses name its base
// alias and its own join keys, and those names never collide with names
// used elsewhere in the tree (sibling keys, the parent's alias, the same
// key at another depth, real table names). Also covers code that names
// joined tables from outside the query set (later sibling ON clauses,
// consumers of `toJoinedQuery()`), orderings and pagination of nested sets,
// modifiers, CTEs, attaches, plugins, and reuse of one query set in
// several places.
//

const users = () =>
	querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

const commentsQs = () =>
	querySet(db).selectAs("comments", db.selectFrom("comments").select(["id", "post_id"]));

/** Bob's posts (base alias `alias`) with comments joined under `commentsKey`. */
const bobPostsWithComments = (commentsKey = "comments") => [
	{ id: 1, user_id: 2, [commentsKey]: [comment(1), comment(2)] },
	{ id: 2, user_id: 2, [commentsKey]: [comment(3)] },
	{ id: 5, user_id: 2, [commentsKey]: [comment(5)] },
	{ id: 12, user_id: 2, [commentsKey]: [] },
];

const COMMENT_POST: Record<number, number> = { 1: 1, 2: 1, 3: 2, 5: 5, 10: 10, 11: 11 };
const comment = (id: number) => ({ id, post_id: COMMENT_POST[id]! });

describe("query-set: join scopes", () => {
	//
	// Aliases.
	//

	test("nested base alias shadows a top-level sibling key", async () => {
		// Inside the posts query set, "profile" is the posts base alias; at the
		// top level it is a sibling join.
		const posts = querySet(db)
			.selectAs("profile", db.selectFrom("posts").select(["id", "user_id"]))
			.leftJoinMany("comments", commentsQs(), "comments.post_id", "profile.id");

		for (const order of ["profile first", "posts first"] as const) {
			const withProfile = (qs: any) =>
				qs.innerJoinOne(
					"profile",
					querySet(db).selectAs("profile", db.selectFrom("profiles").select(["id", "user_id"])),
					"profile.user_id",
					"user.id",
				);
			const withPosts = (qs: any) => qs.leftJoinMany("posts", posts, "posts.user_id", "user.id");

			const base = users().where("users.id", "in", [1, 2]);
			const qs =
				order === "profile first" ? withPosts(withProfile(base)) : withProfile(withPosts(base));

			assert.deepStrictEqual(
				await qs.execute(),
				[
					{ id: 1, username: "alice", profile: { id: 1, user_id: 1 }, posts: [] },
					{ id: 2, username: "bob", profile: { id: 2, user_id: 2 }, posts: bobPostsWithComments() },
				],
				order,
			);
		}
	});

	test("nested join key equals a top-level sibling key", async () => {
		// "comments" is both a top-level one-join and the key of the posts' own join.
		const posts = querySet(db)
			.selectAs("posts", db.selectFrom("posts").select(["id", "user_id"]))
			.leftJoinMany("comments", commentsQs(), "comments.post_id", "posts.id");

		for (const order of ["comments first", "posts first"] as const) {
			const withComment = (qs: any) =>
				qs.leftJoinOne(
					"comments",
					querySet(db).selectAs(
						"comments",
						db.selectFrom("comments").select(["id", "user_id"]).where("id", "in", [3, 11]),
					),
					"comments.user_id",
					"user.id",
				);
			const withPosts = (qs: any) => qs.leftJoinMany("posts", posts, "posts.user_id", "user.id");

			const base = users().where("users.id", "in", [1, 2]);
			const qs =
				order === "comments first" ? withPosts(withComment(base)) : withComment(withPosts(base));

			const expected = [
				{ id: 1, username: "alice", comments: { id: 3, user_id: 1 }, posts: [] },
				{ id: 2, username: "bob", comments: { id: 11, user_id: 2 }, posts: bobPostsWithComments() },
			];
			assert.deepStrictEqual(await qs.execute(), expected, order);
			assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1), order);
		}
	});

	test("the same key at different depths", async () => {
		// user → posts → comments → posts (the comment's post)
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "user_id"]).where("id", "in", [1, 2]),
					).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "post_id"])).innerJoinOne(
								"posts",
								({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title"])),
								"posts.id",
								"comments.post_id",
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);

		assert.deepStrictEqual(await qs.execute(), [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						user_id: 2,
						comments: [
							{ ...comment(1), posts: { id: 1, title: "Post 1" } },
							{ ...comment(2), posts: { id: 1, title: "Post 1" } },
						],
					},
					{
						id: 2,
						user_id: 2,
						comments: [{ ...comment(3), posts: { id: 2, title: "Post 2" } }],
					},
				],
			},
		]);
	});

	test("parent base alias equals the nested base alias", async () => {
		// Both the top-level users and the nested posts are aliased "posts".
		const qs = querySet(db)
			.selectAs("posts", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"userPosts",
				querySet(db)
					.selectAs("posts", db.selectFrom("posts").select(["id", "user_id"]))
					.leftJoinMany("comments", commentsQs(), "comments.post_id", "posts.id"),
				"userPosts.user_id",
				"posts.id",
			);

		const expected = [
			{ id: 1, username: "alice", userPosts: [] },
			{ id: 2, username: "bob", userPosts: bobPostsWithComments() },
		];
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).execute(), expected.slice(0, 1));
	});

	test("nested base alias equals a real table name used below it", async () => {
		// The posts query set is aliased "comments"; its "notes" join selects
		// from the real comments table.
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				querySet(db)
					.selectAs("comments", db.selectFrom("posts").select(["id", "user_id"]))
					.leftJoinMany(
						"notes",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "post_id"])),
						"notes.post_id",
						"comments.id",
					),
				"posts.user_id",
				"user.id",
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: bobPostsWithComments("notes") },
		]);
	});

	test("nested ON references a sibling join of the nested set", async () => {
		const posts = querySet(db)
			.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
			.innerJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"author.id",
				"p.user_id",
			)
			.leftJoinMany(
				"authorComments",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "user_id"])),
				"authorComments.user_id",
				// Not a name the typed API offers (a join's ON clause sees only the
				// base alias and its own key), but valid SQL in the nested scope.
				"author.id" as any,
			);

		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.leftJoinMany("posts", posts, "posts.user_id", "user.id");

		const post = (id: number, userId: number, username: string, commentIds: number[]) => ({
			id,
			user_id: userId,
			author: { id: userId, username },
			authorComments: commentIds.map((c) => ({ id: c, user_id: userId })),
		});
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: [1, 2, 5, 12].map((id) => post(id, 2, "bob", [1, 11])) },
			{ id: 3, username: "carol", posts: [3, 15].map((id) => post(id, 3, "carol", [2])) },
		]);
	});

	test("nested ON references a hoisted column of a nested one-join", async () => {
		// "author.profile$$user_id" is a column of the author join's own nested join.
		const posts = querySet(db)
			.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
			.innerJoinOne(
				"author",
				({ eb, qs }) =>
					qs(eb.selectFrom("users").select(["id", "username"])).innerJoinOne(
						"profile",
						({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "user_id"])),
						"profile.user_id",
						"author.id",
					),
				"author.id",
				"p.user_id",
			)
			.leftJoinMany(
				"authorComments",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "user_id"])),
				"authorComments.user_id",
				// Not a name the typed API offers, but valid SQL in the nested scope.
				"author.profile$$user_id" as any,
			);

		const qs = users()
			.where("users.id", "in", [1, 3])
			.leftJoinMany("posts", posts, "posts.user_id", "user.id");

		const carolPost = (id: number) => ({
			id,
			user_id: 3,
			author: { id: 3, username: "carol", profile: { id: 3, user_id: 3 } },
			authorComments: [{ id: 2, user_id: 3 }],
		});
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{ id: 3, username: "carol", posts: [carolPost(3), carolPost(15)] },
		]);
	});

	test("top-level ON references columns of an earlier nesting join", async () => {
		// "profile.user_id" is a base column of the profile join, and
		// "profile.owner$$id" a hoisted column of its nested join.
		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"])).innerJoinOne(
						"owner",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
						"owner.id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"ownerComments",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "user_id"])),
				"ownerComments.user_id",
				// Not names the typed API offers (a join's ON clause sees only the
				// base alias and its own key), but valid SQL at the top level.
				"profile.owner$$id" as any,
			)
			.leftJoinMany(
				"profilePosts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "user_id"])),
				"profilePosts.user_id",
				"profile.user_id" as any,
			);

		const user = (id: number, username: string, commentIds: number[], postIds: number[]) => ({
			id,
			username,
			profile: { id, user_id: id, owner: { id, username } },
			ownerComments: commentIds.map((c) => ({ id: c, user_id: id })),
			profilePosts: postIds.map((p) => ({ id: p, user_id: id })),
		});
		assert.deepStrictEqual(await qs.execute(), [
			user(1, "alice", [3, 10], []),
			user(2, "bob", [1, 11], [1, 2, 5, 12]),
			user(3, "carol", [2], [3, 15]),
		]);
		assert.strictEqual(await qs.executeCount(Number), 3);
	});

	test("a later sibling's ON clause with an unqualified column", async () => {
		// "user_id" is unambiguous here: the only table in scope with a column
		// of that name is the posts join (its comments' user_id is hoisted as
		// "comments$$user_id").
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "user_id"]).where("id", "in", [1, 2]),
					).leftJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "user_id"])),
						// Joined by id only to keep the rows few.
						"comments.id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.leftJoinOne(
				"postAuthor",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				// Not a name the typed API offers, but valid SQL at the top level.
				(join) => join.onRef("postAuthor.id", "=", "user_id" as any),
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [], postAuthor: null },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, user_id: 2, comments: [{ id: 1, user_id: 2 }] },
					{ id: 2, user_id: 2, comments: [{ id: 2, user_id: 3 }] },
				],
				postAuthor: { id: 2, username: "bob" },
			},
		]);
	});

	test("toJoinedQuery consumers can filter on a nested join's hoisted column", async () => {
		const qs = users().leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "user_id"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "post_id", "content"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		);

		const rows = await qs
			.toJoinedQuery()
			// Hoisted columns of the "posts" derived table are not in toJoinedQuery()'s
			// types, but are valid SQL.
			.where("posts.comments$$content" as any, "=", "Comment 3 on post 2")
			.execute();

		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$user_id: 2,
				posts$$comments$$id: 3,
				posts$$comments$$post_id: 2,
				posts$$comments$$content: "Comment 3 on post 2",
			},
		]);
		assert.deepStrictEqual(await qs.hydrate(rows), [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 2,
						user_id: 2,
						comments: [{ id: 3, post_id: 2, content: "Comment 3 on post 2" }],
					},
				],
			},
		]);
	});

	test("column aliases with dots and dollar signs at the grandchild level", async () => {
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]).where("id", "=", 1)).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "post_id", "content as note.text", "user_id as author$id"]),
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);

		const expected = [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						user_id: 2,
						comments: [
							{ id: 1, post_id: 1, "note.text": "Comment 1 on post 1", author$id: 2 },
							{ id: 2, post_id: 1, "note.text": "Comment 2 on post 1", author$id: 3 },
						],
					},
				],
			},
		];
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).execute(), expected);
	});

	//
	// Reuse.
	//

	test("one query set reused at two places in a tree", async () => {
		const commentsWithReplies = querySet(db)
			.selectAs("c", db.selectFrom("comments").select(["id", "post_id", "user_id"]))
			.leftJoinMany(
				"replies",
				({ eb, qs }) => qs(eb.selectFrom("replies").select(["id", "comment_id"])),
				"replies.comment_id",
				"c.id",
			);

		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				querySet(db)
					.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
					.leftJoinMany("comments", commentsWithReplies, "comments.post_id", "p.id"),
				"posts.user_id",
				"user.id",
			)
			.leftJoinMany("comments", commentsWithReplies, "comments.user_id", "user.id");

		const c = (id: number, postId: number, userId: number, replyIds: number[]) => ({
			id,
			post_id: postId,
			user_id: userId,
			replies: replyIds.map((r) => ({ id: r, comment_id: id })),
		});
		const expected = [
			{
				id: 1,
				username: "alice",
				posts: [],
				comments: [c(3, 2, 1, [1, 5]), c(10, 10, 1, [])],
			},
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, user_id: 2, comments: [c(1, 1, 2, [2, 4]), c(2, 1, 3, [])] },
					{ id: 2, user_id: 2, comments: [c(3, 2, 1, [1, 5])] },
					{ id: 5, user_id: 2, comments: [c(5, 5, 6, [3])] },
					{ id: 12, user_id: 2, comments: [] },
				],
				comments: [c(1, 1, 2, [2, 4]), c(11, 11, 2, [])],
			},
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
		// Building and compiling the tree leaves the shared query set untouched.
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await commentsWithReplies.where("comments.id", "in", [1, 3]).execute(), [
			c(1, 1, 2, [2, 4]),
			c(3, 2, 1, [1, 5]),
		]);
		assert.strictEqual(qs.toQuery().compile().sql, qs.toQuery().compile().sql);
	});

	//
	// Ordering and pagination of nested sets.
	//

	test("nested orderBy at three levels sorts the hydrated collections", async () => {
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]).where("id", "in", [1, 2]))
						.orderBy("id", "desc")
						.leftJoinMany(
							"comments",
							({ eb, qs }) =>
								qs(eb.selectFrom("comments").select(["id", "post_id"]))
									.orderBy("id", "desc")
									.leftJoinMany(
										"replies",
										({ eb, qs }) =>
											qs(eb.selectFrom("replies").select(["id", "comment_id"])).orderBy(
												"id",
												"desc",
											),
										"replies.comment_id",
										"comments.id",
									),
							"comments.post_id",
							"posts.id",
						),
				"posts.user_id",
				"user.id",
			);

		const r = (id: number, commentId: number) => ({ id, comment_id: commentId });
		assert.deepStrictEqual(await qs.execute(), [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 2, user_id: 2, comments: [{ ...comment(3), replies: [r(5, 3), r(1, 3)] }] },
					{
						id: 1,
						user_id: 2,
						comments: [
							{ ...comment(2), replies: [] },
							{ ...comment(1), replies: [r(4, 1), r(2, 1)] },
						],
					},
				],
			},
		]);
	});

	test("nested orderBy on a nested one-join's column sorts a many-join", async () => {
		// post 1's comments: 1 by bob, 2 by carol → sorted by author desc: carol, bob
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]).where("id", "=", 1)).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "post_id", "user_id"]))
								.innerJoinOne(
									"author",
									({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
									"author.id",
									"comments.user_id",
								)
								.orderBy("author$$username", "desc"),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);

		const [bob] = await qs.execute();
		assert.deepStrictEqual(
			bob!.posts[0]!.comments.map((c) => [c.id, c.author.username]),
			[
				[2, "carol"],
				[1, "bob"],
			],
		);
	});

	for (const how of ["query set limit", "base query limit"] as const) {
		test(`nested set with a ${how} and its own nested join`, async () => {
			// The nested limit applies to the posts set as a whole (posts 1–3),
			// before it is joined to users.
			const posts = querySet(db).selectAs("p", db.selectFrom("posts").select(["id", "user_id"]));
			const limited =
				how === "query set limit"
					? posts.orderBy("id").limit(3)
					: posts.modify((qb) => qb.orderBy("id").limit(3));

			const qs = users()
				.where("users.id", "in", [1, 2, 3])
				.leftJoinMany(
					"posts",
					limited.leftJoinMany("comments", commentsQs(), "comments.post_id", "p.id"),
					"posts.user_id",
					"user.id",
				);

			const expected = [
				{ id: 1, username: "alice", posts: [] },
				{
					id: 2,
					username: "bob",
					posts: [
						{ id: 1, user_id: 2, comments: [comment(1), comment(2)] },
						{ id: 2, user_id: 2, comments: [comment(3)] },
					],
				},
				{ id: 3, username: "carol", posts: [{ id: 3, user_id: 3, comments: [] }] },
			];
			assert.deepStrictEqual(await qs.execute(), expected);
			assert.deepStrictEqual(await qs.limit(2).offset(1).execute(), expected.slice(1));
			assert.strictEqual(await qs.executeCount(Number), 3);
		});
	}

	test("nested set with a limit and offset under an inner join", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.innerJoinMany(
				"posts",
				querySet(db)
					.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
					.innerJoinMany("comments", commentsQs(), "comments.post_id", "p.id")
					.orderBy("id")
					.limit(2)
					.offset(1),
				"posts.user_id",
				"user.id",
			);

		// Posts with comments: 1, 2, 4, 5, ... → the window keeps posts 2 and 4.
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 2, username: "bob", posts: [{ id: 2, user_id: 2, comments: [comment(3)] }] },
		]);
		assert.strictEqual(await qs.executeCount(Number), 1);
	});

	test("top-level orderBy on a column two one-joins deep", async () => {
		const qs = users()
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"])).innerJoinOne(
						"owner",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
						"owner.id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			)
			.orderBy("profile$$owner$$username", "desc");

		const ids = (rows: { id: number }[]) => rows.map((row) => row.id);
		assert.deepStrictEqual(ids(await qs.execute()), [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
		assert.deepStrictEqual(ids(await qs.limit(3).execute()), [10, 9, 8]);
		assert.deepStrictEqual(ids(await qs.limit(3).offset(2).execute()), [8, 7, 6]);

		const withPosts = qs.leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "user_id"])).leftJoinMany(
					"comments",
					commentsQs(),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		);
		assert.deepStrictEqual(
			(await withPosts.limit(3).offset(6).execute()).map((user) => [
				user.id,
				user.posts.map((post) => [post.id, post.comments.map((c) => c.id)]),
			]),
			[
				[
					4,
					[
						[4, [4]],
						[13, []],
					],
				],
				[
					3,
					[
						[3, []],
						[15, [13]],
					],
				],
				[
					2,
					[
						[1, [1, 2]],
						[2, [3]],
						[5, [5]],
						[12, []],
					],
				],
			],
		);
	});

	test("paginated orderBy on a one-join that nests many-joins two deep", async () => {
		const qs = users()
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id", "bio"])).leftJoinMany(
						"posts",
						({ eb, qs }) =>
							qs(eb.selectFrom("posts").select(["id", "user_id"])).innerJoinMany(
								"comments",
								commentsQs(),
								"comments.post_id",
								"posts.id",
							),
						"posts.user_id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			)
			.orderBy("profile$$bio", "desc")
			.limit(3)
			.offset(1);

		// bios desc: 9, 8, 7, 6, ... → offset 1 keeps 8, 7, 6
		assert.deepStrictEqual(
			(await qs.execute()).map((user) => [
				user.id,
				user.profile?.posts.map((post) => [post.id, post.comments.map((c) => c.id)]),
			]),
			[
				[8, [[9, [9]]]],
				[7, [[8, [8]]]],
				[6, [[7, [7]]]],
			],
		);
		assert.strictEqual(await qs.executeCount(Number), 10);
	});

	test("filtering many-joins nested in many-joins: count, exists and pagination", async () => {
		const withReplies = (repliesWhere: number) =>
			users().innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"])).innerJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "post_id"])).innerJoinMany(
								"replies",
								({ eb, qs }) =>
									qs(
										eb
											.selectFrom("replies")
											.select(["id", "comment_id"])
											.where("id", ">", repliesWhere),
									),
								"replies.comment_id",
								"comments.id",
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);

		// Comments with replies: 1, 3 (post 1, 2: bob), 5 (post 5: bob), 6 (post 6: eve)
		assert.strictEqual(await withReplies(0).executeCount(Number), 2);
		assert.strictEqual(await withReplies(0).executeExists(), true);
		assert.deepStrictEqual(
			(await withReplies(0).limit(1).offset(1).execute()).map((user) => user.id),
			[5],
		);
		// Only reply 6 (comment 6, post 6, eve) survives.
		assert.strictEqual(await withReplies(5).executeCount(Number), 1);
		assert.deepStrictEqual(
			(await withReplies(5).execute()).map((user) => user.id),
			[5],
		);
		assert.strictEqual(await withReplies(6).executeCount(Number), 0);
		assert.strictEqual(await withReplies(6).executeExists(), false);
		assert.deepStrictEqual(await withReplies(6).limit(1).execute(), []);
	});

	//
	// Modifiers.
	//

	test("a nested set's end modifier applies to the nested set as a whole", async () => {
		// Post 1 has two comments, but the nested set is cut to one row.
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]).where("id", "=", 1))
						.leftJoinMany("comments", commentsQs(), "comments.post_id", "posts.id")
						.modifyEnd(sql`limit 1`),
				"posts.user_id",
				"user.id",
			);

		const [bob] = await qs.execute();
		assert.strictEqual(bob!.posts.length, 1);
		assert.strictEqual(bob!.posts[0]!.comments.length, 1);
	});

	test("modify() on nested collections after they are joined", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				querySet(db).selectAs("p", db.selectFrom("posts").select(["id", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.modify("posts", (posts) =>
				posts
					.where("posts.id", "<", 5)
					.leftJoinMany("comments", commentsQs(), "comments.post_id", "p.id")
					.modify("comments", (comments) => comments.where("comments.id", "!=", 2)),
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, user_id: 2, comments: [comment(1)] },
					{ id: 2, user_id: 2, comments: [comment(3)] },
				],
			},
		]);
	});

	//
	// CTEs.
	//

	test("top-level base query with a CTE and nested joins", async () => {
		const qs = querySet(db)
			.selectAs(
				"user",
				db
					.with("few", (qb) =>
						qb.selectFrom("users").select(["id", "username"]).where("id", "in", [1, 2]),
					)
					.selectFrom("few")
					.select(["id", "username"]),
			)
			.leftJoinMany(
				"posts",
				querySet(db)
					.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
					.leftJoinMany("comments", commentsQs(), "comments.post_id", "p.id"),
				"posts.user_id",
				"user.id",
			);

		const expected = [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: bobPostsWithComments() },
		];
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
		assert.strictEqual(await qs.executeCount(Number), 2);
	});

	test("nested base query with a CTE and its own nested join", async () => {
		const posts = querySet(db)
			.selectAs(
				"p",
				db
					.with("bobPosts", (qb) =>
						qb.selectFrom("posts").select(["id", "user_id"]).where("user_id", "=", 2),
					)
					.selectFrom("bobPosts")
					.select(["id", "user_id"]),
			)
			.leftJoinMany("comments", commentsQs(), "comments.post_id", "p.id");

		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.leftJoinMany("posts", posts, "posts.user_id", "user.id");

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: bobPostsWithComments() },
			{ id: 3, username: "carol", posts: [] },
		]);
	});

	//
	// Attaches under nested joins.
	//

	test("attach under a grandchild join receives each grandchild once", async () => {
		const inputs: number[][] = [];
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				querySet(db)
					.selectAs("p", db.selectFrom("posts").select(["id", "user_id"]))
					.leftJoinMany(
						"comments",
						commentsQs().attachMany(
							"replies",
							async (comments) => {
								inputs.push(comments.map((c) => c.id));
								return db
									.selectFrom("replies")
									.select(["id", "comment_id"])
									.where(
										"comment_id",
										"in",
										comments.map((c) => c.id),
									)
									.orderBy("id")
									.execute();
							},
							{ matchChild: "comment_id" },
						),
						"comments.post_id",
						"p.id",
					),
				"posts.user_id",
				"user.id",
			);

		const r = (id: number, commentId: number) => ({ id, comment_id: commentId });
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						user_id: 2,
						comments: [
							{ ...comment(1), replies: [r(2, 1), r(4, 1)] },
							{ ...comment(2), replies: [] },
						],
					},
					{ id: 2, user_id: 2, comments: [{ ...comment(3), replies: [r(1, 3), r(5, 3)] }] },
					{ id: 5, user_id: 2, comments: [{ ...comment(5), replies: [r(3, 5)] }] },
					{ id: 12, user_id: 2, comments: [] },
				],
			},
		]);
		assert.deepStrictEqual(inputs, [[1, 2, 3, 5]]);
	});

	//
	// Plugins.
	//

	test("CamelCasePlugin with camelCase keys at three levels", async () => {
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; userId: number };
			comments: { id: number; postId: number; userId: number };
			replies: { id: number; commentId: number };
		}>();

		const qs = querySet(camelDb)
			.selectAs("theUser", camelDb.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"userPosts",
				querySet(camelDb)
					.selectAs("somePost", camelDb.selectFrom("posts").select(["id", "userId"]))
					.leftJoinMany(
						"postComments",
						querySet(camelDb)
							.selectAs("someComment", camelDb.selectFrom("comments").select(["id", "postId"]))
							.innerJoinMany(
								"commentReplies",
								({ eb, qs }) => qs(eb.selectFrom("replies").select(["id", "commentId"])),
								"commentReplies.commentId",
								"someComment.id",
							),
						"postComments.postId",
						"somePost.id",
					),
				"userPosts.userId",
				"theUser.id",
			);

		const expected = [
			{ id: 1, username: "alice", userPosts: [] },
			{
				id: 2,
				username: "bob",
				userPosts: [
					{
						id: 1,
						userId: 2,
						postComments: [
							{
								id: 1,
								postId: 1,
								commentReplies: [
									{ id: 2, commentId: 1 },
									{ id: 4, commentId: 1 },
								],
							},
						],
					},
					{
						id: 2,
						userId: 2,
						postComments: [
							{
								id: 3,
								postId: 2,
								commentReplies: [
									{ id: 1, commentId: 3 },
									{ id: 5, commentId: 3 },
								],
							},
						],
					},
					{
						id: 5,
						userId: 2,
						postComments: [{ id: 5, postId: 5, commentReplies: [{ id: 3, commentId: 5 }] }],
					},
					{ id: 12, userId: 2, postComments: [] },
				],
			},
		];
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
		assert.strictEqual(await qs.executeCount(Number), 2);
	});
});
