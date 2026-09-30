import assert from "node:assert";
import { describe, test } from "node:test";

import { sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Join Conditions on Nested Joins
//
// A nested query set is an isolated unit: its own joins (and their ON
// clauses) only ever see the nested set's rows, so a parent row without a
// matching child is never affected by the child's joins. These tests pin
// that down for every kind of ON clause a nested join can have — including
// ones that are NOT null-rejecting on the parent (`OR ... IS NULL`, `IS NOT
// DISTINCT FROM`, constant-only conditions, `ON TRUE`), where
// `A ⟕ (B ⟕ C)` and `(A ⟕ B) ⟕ C` differ — and check hydrated output,
// counts, pagination and the raw rows of `toJoinedQuery()`.
//

const users = () =>
	querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

// comment id → [post_id, user_id]
const COMMENTS: Record<number, [postId: number, userId: number]> = {
	1: [1, 2],
	2: [1, 3],
	3: [2, 1],
	4: [4, 5],
	5: [5, 6],
	6: [6, 7],
	7: [7, 8],
	8: [8, 9],
	9: [9, 10],
	10: [10, 1],
	11: [11, 2],
	12: [14, 6],
	13: [15, 7],
};
const comment = (id: number) => ({ id, post_id: COMMENTS[id]![0] });
const commentWithUser = (id: number) => ({ ...comment(id), user_id: COMMENTS[id]![1] });

const BOB_POST_IDS = [1, 2, 5, 12];

describe("query-set: join conditions", () => {
	//
	// ON clause variants, table-driven over inner/left at both levels.
	//
	// user (alice, bob) → posts (base alias "p") → comments; `matches` maps
	// each of bob's posts to the comment ids its ON clause matches.
	//

	interface OnVariant {
		name: string;
		on: (join: any) => any;
		matches: Record<number, number[]>;
	}

	const ON_VARIANTS: OnVariant[] = [
		{
			name: "onRef against the nested base alias",
			on: (join) => join.onRef("comments.post_id", "=", "p.id"),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "onRef with operands reversed",
			on: (join) => join.onRef("p.id", "=", "comments.post_id"),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "onRef plus a constant conjunct on the child",
			on: (join) => join.onRef("comments.post_id", "=", "p.id").on("comments.user_id", "!=", 3),
			matches: { 1: [1], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "onRef plus a constant conjunct on the parent",
			on: (join) => join.onRef("comments.post_id", "=", "p.id").on("p.id", "<=", 2),
			matches: { 1: [1, 2], 2: [3], 5: [], 12: [] },
		},
		{
			name: "strict inequality with a constant bound",
			on: (join) => join.onRef("comments.post_id", ">", "p.id").on("comments.post_id", "<", 6),
			matches: { 1: [3, 4, 5], 2: [4, 5], 5: [], 12: [] },
		},
		{
			name: "and() of comparisons",
			on: (join) =>
				join.on((eb: any) =>
					eb.and([eb("comments.post_id", "=", eb.ref("p.id")), eb("comments.id", "<", 3)]),
				),
			matches: { 1: [1, 2], 2: [], 5: [], 12: [] },
		},
		{
			name: "or() with an arm that only references the child",
			on: (join) =>
				join.on((eb: any) =>
					eb.or([eb("comments.post_id", "=", eb.ref("p.id")), eb("comments.id", "=", 13)]),
				),
			matches: { 1: [1, 2, 13], 2: [3, 13], 5: [5, 13], 12: [13] },
		},
		{
			name: "raw sql",
			on: (join) => join.on(sql<boolean>`${sql.ref("comments.post_id")} = ${sql.ref("p.id")}`),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "unqualified child column",
			on: (join) => join.onRef("post_id", "=", "p.id"),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "function call",
			on: (join) =>
				join.on((eb: any) =>
					eb(eb.fn.coalesce("comments.post_id", eb.val(0)), "=", eb.ref("p.id")),
				),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "is not distinct from",
			on: (join) =>
				join.on((eb: any) => eb("comments.post_id", "is not distinct from", eb.ref("p.id"))),
			matches: { 1: [1, 2], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "uncorrelated IN subquery",
			on: (join) =>
				join
					.onRef("comments.post_id", "=", "p.id")
					.on((eb: any) =>
						eb(
							"comments.id",
							"in",
							eb.selectFrom("comments as c2").select("c2.id").where("c2.user_id", "in", [2, 3]),
						),
					),
			matches: { 1: [1, 2], 2: [], 5: [], 12: [] },
		},
		{
			name: "correlated EXISTS subquery",
			on: (join) =>
				join
					.onRef("comments.post_id", "=", "p.id")
					.on((eb: any) =>
						eb.exists(
							eb
								.selectFrom("replies")
								.select("replies.id")
								.whereRef("replies.comment_id", "=", "comments.id"),
						),
					),
			matches: { 1: [1], 2: [3], 5: [5], 12: [] },
		},
		{
			name: "onTrue plus a constant condition on the child",
			on: (join) => join.onTrue().on("comments.id", "in", [1, 13]),
			matches: { 1: [1, 13], 2: [1, 13], 5: [1, 13], 12: [1, 13] },
		},
		{
			name: "constant condition on the child only",
			on: (join) => join.on("comments.post_id", "=", 2),
			matches: { 1: [3], 2: [3], 5: [3], 12: [3] },
		},
	];

	const JOIN_TYPE_PAIRS = [
		["left", "left"],
		["left", "inner"],
		["inner", "left"],
		["inner", "inner"],
	] as const;

	for (const variant of ON_VARIANTS) {
		for (const [outer, inner] of JOIN_TYPE_PAIRS) {
			const build = () => {
				const posts = (
					querySet(db).selectAs("p", db.selectFrom("posts").select(["id", "user_id"])) as any
				)[`${inner}JoinMany`](
					"comments",
					({ eb, qs }: any) => qs(eb.selectFrom("comments").select(["id", "post_id", "user_id"])),
					variant.on,
				);
				return (users().where("users.id", "in", [1, 2]) as any)[`${outer}JoinMany`](
					"posts",
					posts,
					"posts.user_id",
					"user.id",
				);
			};

			const bobPosts = BOB_POST_IDS.map((id) => ({
				id,
				user_id: 2,
				comments: variant.matches[id]!.map(commentWithUser),
			})).filter((post) => inner === "left" || post.comments.length > 0);
			const expected = [
				...(outer === "left" ? [{ id: 1, username: "alice", posts: [] }] : []),
				...(outer === "left" || bobPosts.length > 0
					? [{ id: 2, username: "bob", posts: bobPosts }]
					: []),
			];
			const bobRows = BOB_POST_IDS.reduce((sum, id) => {
				const matches = variant.matches[id]!.length;
				return sum + (inner === "left" ? Math.max(1, matches) : matches);
			}, 0);
			const expectedRowCount =
				(outer === "left" ? 1 : 0) + (bobRows === 0 && outer === "left" ? 1 : bobRows);

			const name = `ON ${variant.name} (${outer} → ${inner})`;

			test(`${name}: execute, pagination, count and exists`, async () => {
				const qs = build();
				assert.deepStrictEqual(await qs.execute(), expected);
				assert.deepStrictEqual(await qs.limit(1).execute(), expected.slice(0, 1));
				assert.deepStrictEqual(await qs.limit(10).offset(1).execute(), expected.slice(1));
				assert.strictEqual(await qs.executeCount(Number), expected.length);
				assert.strictEqual(await qs.executeExists(), expected.length > 0);
			});

			test(`${name}: toJoinedQuery row count`, async () => {
				const rows = await build().toJoinedQuery().execute();
				assert.strictEqual(rows.length, expectedRowCount);
			});
		}
	}

	//
	// ON clauses that are not null-rejecting on the parent.
	//
	// For a user without a profile, a flat `user ⟕ profile ⟕ post` would let
	// these conditions match posts even though there is no profile, multiplying
	// the user's rows (inflating counts, shrinking pages) or filling the
	// grandchild's columns next to a NULL parent.
	//

	test("or() with IS NULL on the parent: one-joins keep one row per user", async () => {
		const qs = users().leftJoinOne(
			"profile",
			({ eb, qs }) =>
				qs(
					eb.selectFrom("profiles").select(["id", "user_id"]).where("user_id", "<=", 3),
				).leftJoinOne(
					"post",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title"])),
					(join) =>
						join.on((eb) =>
							eb.or([eb("post.id", "=", eb.ref("profile.id")), eb("profile.id", "is", null)]),
						),
				),
			"profile.user_id",
			"user.id",
		);

		const expected = [
			{
				id: 1,
				username: "alice",
				profile: { id: 1, user_id: 1, post: { id: 1, title: "Post 1" } },
			},
			{ id: 2, username: "bob", profile: { id: 2, user_id: 2, post: { id: 2, title: "Post 2" } } },
			{
				id: 3,
				username: "carol",
				profile: { id: 3, user_id: 3, post: { id: 3, title: "Post 3" } },
			},
			{ id: 4, username: "dave", profile: null },
			{ id: 5, username: "eve", profile: null },
			{ id: 6, username: "frank", profile: null },
			{ id: 7, username: "grace", profile: null },
			{ id: 8, username: "heidi", profile: null },
			{ id: 9, username: "ivan", profile: null },
			{ id: 10, username: "judy", profile: null },
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(5).execute(), expected.slice(0, 5));
		assert.deepStrictEqual(await qs.limit(3).offset(4).execute(), expected.slice(4, 7));
		assert.strictEqual(await qs.executeCount(Number), 10);
		assert.strictEqual((await qs.toJoinedQuery().execute()).length, 10);
		assert.strictEqual((await qs.toQuery().execute()).length, 10);
	});

	test("is not distinct from on nullable keys: one-joins keep one row per user", async () => {
		// Every profile has a non-null tag matching exactly one post, but most
		// posts have a NULL tag: only a missing profile would "match" them.
		const qs = users().leftJoinOne(
			"profile",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("profiles")
						.select(["id", sql<number>`user_id + 2`.as("tag")])
						.where("user_id", "<=", 3),
				).leftJoinOne(
					"post",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select([
									"id",
									sql<number | null>`case when id between 3 and 5 then id else null end`.as("tag"),
								]),
						),
					(join) => join.on((eb) => eb("post.tag", "is not distinct from", eb.ref("profile.tag"))),
				),
			"profile.id",
			"user.id",
		);

		const profile = (id: number) => ({ id, tag: id + 2, post: { id: id + 2, tag: id + 2 } });
		const expected = [
			{ id: 1, username: "alice", profile: profile(1) },
			{ id: 2, username: "bob", profile: profile(2) },
			{ id: 3, username: "carol", profile: profile(3) },
			{ id: 4, username: "dave", profile: null },
			{ id: 5, username: "eve", profile: null },
			{ id: 6, username: "frank", profile: null },
			{ id: 7, username: "grace", profile: null },
			{ id: 8, username: "heidi", profile: null },
			{ id: 9, username: "ivan", profile: null },
			{ id: 10, username: "judy", profile: null },
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(5).execute(), expected.slice(0, 5));
		assert.strictEqual(await qs.executeCount(Number), 10);
		assert.strictEqual((await qs.toJoinedQuery().execute()).length, 10);
	});

	test("constant-only ON: grandchild columns stay NULL next to a missing parent", async () => {
		const qs = users()
			.where("users.id", "<=", 5)
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("profiles").select(["id", "user_id"]).where("user_id", "<=", 3),
					).leftJoinOne(
						"post",
						({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title"])),
						(join) => join.on("post.id", "=", 1),
					),
				"profile.user_id",
				"user.id",
			);

		const post = { id: 1, title: "Post 1" };
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", profile: { id: 1, user_id: 1, post } },
			{ id: 2, username: "bob", profile: { id: 2, user_id: 2, post } },
			{ id: 3, username: "carol", profile: { id: 3, user_id: 3, post } },
			{ id: 4, username: "dave", profile: null },
			{ id: 5, username: "eve", profile: null },
		]);

		const rows = await qs.toJoinedQuery().execute();
		assert.deepStrictEqual(
			rows.map((row) => [row.id, row.profile$$id, row.profile$$post$$id]),
			[
				[1, 1, 1],
				[2, 2, 1],
				[3, 3, 1],
				[4, null, null],
				[5, null, null],
			],
		);
	});

	test("onTrue between many-joins: no phantom grandchildren under a missing parent", async () => {
		const attachInputs: number[][] = [];
		const qs = users()
			.where("users.id", "in", [1, 3])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"])).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb.selectFrom("comments").select(["id", "post_id"]).where("id", "<=", 2),
							).attachMany(
								"replies",
								async (comments) => {
									attachInputs.push(comments.map((c) => c.id));
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
						(join) => join.onTrue(),
					),
				"posts.user_id",
				"user.id",
			);

		const comments = [
			{ ...comment(1), replies: [2, 4].map((id) => ({ id, comment_id: 1 })) },
			{ ...comment(2), replies: [] },
		];
		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 3,
				username: "carol",
				posts: [
					{ id: 3, user_id: 3, comments },
					{ id: 15, user_id: 3, comments },
				],
			},
		]);
		assert.deepStrictEqual(attachInputs, [[1, 2]]);

		const rows = await qs.toJoinedQuery().execute();
		// alice: 1 row, carol: 2 posts x 2 comments
		assert.strictEqual(rows.length, 5);
		assert.deepStrictEqual(
			rows.filter((row) => row.id === 1),
			[
				{
					id: 1,
					username: "alice",
					posts$$id: null,
					posts$$user_id: null,
					posts$$comments$$id: null,
					posts$$comments$$post_id: null,
				},
			],
		);
	});

	//
	// Cross joins inside left joins.
	//

	test("crossJoinMany with an empty set inside leftJoinMany keeps every parent", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"])).crossJoinMany("none", ({ eb, qs }) =>
						qs(eb.selectFrom("comments").select(["id"]).where("id", "<", 0)),
					),
				"posts.user_id",
				"user.id",
			);

		const expected = [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: [] },
		];
		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(1).execute(), expected.slice(1));
		assert.strictEqual(await qs.executeCount(Number), 2);
		assert.strictEqual((await qs.toJoinedQuery().execute()).length, 2);
	});

	test("crossJoinMany with a non-empty set inside leftJoinMany", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"])).crossJoinMany(
						"firstComments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id"]).where("id", "in", [1, 2])),
					),
				"posts.user_id",
				"user.id",
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: BOB_POST_IDS.map((id) => ({
					id,
					user_id: 2,
					firstComments: [{ id: 1 }, { id: 2 }],
				})),
			},
		]);
		assert.strictEqual((await qs.toJoinedQuery().execute()).length, 1 + 4 * 2);
	});

	//
	// Inner one-join inside a left one-join (counted and paginated directly).
	//

	test("innerJoinOne inside leftJoinOne: a missing grandchild nulls the child, not the parent", async () => {
		const qs = users().leftJoinOne(
			"profile",
			({ eb, qs }) =>
				qs(eb.selectFrom("profiles").select(["id", "user_id"])).innerJoinOne(
					"owner",
					({ eb, qs }) =>
						qs(eb.selectFrom("users").select(["id", "username"]).where("id", "not in", [2, 3])),
					"owner.id",
					"profile.user_id",
				),
			"profile.user_id",
			"user.id",
		);

		const all = await users().execute();
		const expected = all.map((user) => ({
			...user,
			profile:
				user.id === 2 || user.id === 3
					? null
					: { id: user.id, user_id: user.id, owner: { id: user.id, username: user.username } },
		}));

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(3).execute(), expected.slice(0, 3));
		assert.deepStrictEqual(await qs.limit(2).offset(1).execute(), expected.slice(1, 3));
		assert.strictEqual(await qs.executeCount(Number), 10);
		assert.deepStrictEqual(
			await qs
				.orderBy("profile$$owner$$username", (ob) => ob.desc().nullsLast())
				.limit(3)
				.execute(),
			[expected[9], expected[8], expected[7]],
		);
	});

	//
	// Bind parameters at every level.
	//

	test("parameters at every level bind to the right placeholders", async () => {
		// Mixed string and number parameters: misplaced bindings would either
		// fail (Postgres) or silently match nothing (SQLite).
		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.where("users.username", "!=", "zed")
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"]).where("bio", "!=", "nope")),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]).where("id", "!=", 5))
						.leftJoinMany(
							"comments",
							({ eb, qs }) =>
								qs(
									eb
										.selectFrom("comments")
										.select(["id", "post_id", "user_id"])
										.where("content", "!=", "Comment 2 on post 1"),
								),
							(join) =>
								join.onRef("comments.post_id", "=", "posts.id").on("comments.user_id", "!=", 99),
						)
						.innerJoinOne(
							"author",
							({ eb, qs }) =>
								qs(eb.selectFrom("users").select(["id", "username"]).where("email", "like", "%@%")),
							(join) =>
								join.onRef("author.id", "=", "posts.user_id").on("author.username", "!=", "x"),
						),
				(join) => join.onRef("posts.user_id", "=", "user.id").on("posts.id", "<", 15),
			);

		const bob = { id: 2, username: "bob" };
		const carol = { id: 3, username: "carol" };
		const expected = [
			{ id: 1, username: "alice", profile: { id: 1, user_id: 1 }, posts: [] },
			{
				...bob,
				profile: { id: 2, user_id: 2 },
				posts: [
					{ id: 1, user_id: 2, comments: [commentWithUser(1)], author: bob },
					{ id: 2, user_id: 2, comments: [commentWithUser(3)], author: bob },
					{ id: 12, user_id: 2, comments: [], author: bob },
				],
			},
			{
				...carol,
				profile: { id: 3, user_id: 3 },
				posts: [{ id: 3, user_id: 3, comments: [], author: carol }],
			},
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(2).offset(1).execute(), expected.slice(1));
		assert.strictEqual(await qs.executeCount(Number), 3);
	});

	test("ON callbacks are evaluated on every execution", async () => {
		let maxCommentId = 1;
		const qs = users()
			.where("users.id", "=", 2)
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "user_id"]).where("id", "in", [1, 2]),
					).leftJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "post_id"])),
						(join) =>
							join.onRef("comments.post_id", "=", "posts.id").on("comments.id", "<=", maxCommentId),
					),
				"posts.user_id",
				"user.id",
			);

		const run = async () =>
			(await qs.execute()).flatMap((user) =>
				user.posts.map((post) => [post.id, post.comments.map((c) => c.id)]),
			);

		assert.deepStrictEqual(await run(), [
			[1, [1]],
			[2, []],
		]);
		maxCommentId = 3;
		assert.deepStrictEqual(await run(), [
			[1, [1, 2]],
			[2, [3]],
		]);
		maxCommentId = 0;
		assert.deepStrictEqual(await run(), [
			[1, []],
			[2, []],
		]);
	});

	//
	// Mixed trees.
	//

	test("mixed ON kinds and join types across siblings and depths", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2, 3])
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"]))
						.leftJoinMany(
							"comments",
							({ eb, qs }) =>
								qs(eb.selectFrom("comments").select(["id", "post_id"])).leftJoinMany(
									"replies",
									({ eb, qs }) => qs(eb.selectFrom("replies").select(["id", "comment_id"])),
									(join) =>
										join.on(
											sql<boolean>`${sql.ref("replies.comment_id")} = ${sql.ref("comments.id")}`,
										),
								),
							"comments.post_id",
							"posts.id",
						)
						.leftJoinOne(
							"firstComment",
							({ eb, qs }) =>
								qs(eb.selectFrom("comments").select(["id", "post_id"]).where("id", "in", [1, 3])),
							(join) =>
								join.on((eb) =>
									eb.or([
										eb("firstComment.post_id", "=", eb.ref("posts.id")),
										eb("posts.id", "is", null),
									]),
								),
						),
				"posts.user_id",
				"user.id",
			)
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"])).innerJoinOne(
						"owner",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id"]).where("id", "!=", 3)),
						"owner.id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			);

		const r = (id: number, commentId: number) => ({ id, comment_id: commentId });
		const expected = [
			{ id: 1, username: "alice", posts: [], profile: { id: 1, user_id: 1, owner: { id: 1 } } },
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
						firstComment: comment(1),
					},
					{
						id: 2,
						user_id: 2,
						comments: [{ ...comment(3), replies: [r(1, 3), r(5, 3)] }],
						firstComment: comment(3),
					},
					{
						id: 5,
						user_id: 2,
						comments: [{ ...comment(5), replies: [r(3, 5)] }],
						firstComment: null,
					},
					{ id: 12, user_id: 2, comments: [], firstComment: null },
				],
				profile: { id: 2, user_id: 2, owner: { id: 2 } },
			},
			{
				id: 3,
				username: "carol",
				posts: [
					{ id: 3, user_id: 3, comments: [], firstComment: null },
					{ id: 15, user_id: 3, comments: [{ ...comment(13), replies: [] }], firstComment: null },
				],
				profile: null,
			},
		];

		assert.deepStrictEqual(await qs.execute(), expected);
		assert.deepStrictEqual(await qs.limit(1).offset(2).execute(), expected.slice(2));
		assert.strictEqual(await qs.executeCount(Number), 3);
		// alice 1 + bob (3 + 2 + 1 + 1) + carol (1 + 1)
		assert.strictEqual((await qs.toJoinedQuery().execute()).length, 10);
	});

	//
	// leftJoinOneOrThrow around nested joins.
	//

	test("nested leftJoinOneOrThrow only throws for an existing parent", async () => {
		const build = (postIds: number[]) =>
			users()
				.where("users.id", "in", [1, 2])
				.leftJoinMany(
					"posts",
					({ eb, qs }) =>
						qs(
							eb.selectFrom("posts").select(["id", "user_id"]).where("id", "in", postIds),
						).leftJoinOneOrThrow(
							"firstComment",
							({ eb, qs }) =>
								qs(eb.selectFrom("comments").select(["id", "post_id"]).where("id", "in", [1, 3])),
							"firstComment.post_id",
							"posts.id",
						),
					"posts.user_id",
					"user.id",
				);

		assert.deepStrictEqual(await build([1, 2]).execute(), [
			{ id: 1, username: "alice", posts: [] },
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, user_id: 2, firstComment: comment(1) },
					{ id: 2, user_id: 2, firstComment: comment(3) },
				],
			},
		]);
		await assert.rejects(build([1, 12]).execute(), ExpectedOneItemError);
	});

	test("leftJoinOneOrThrow around a nested leftJoinOne with gaps", async () => {
		const qs = users()
			.where("users.id", "in", [1, 2])
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"])).leftJoinOne(
						"owner",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id"]).where("id", "!=", 2)),
						"owner.id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			);

		assert.deepStrictEqual(await qs.execute(), [
			{ id: 1, username: "alice", profile: { id: 1, user_id: 1, owner: { id: 1 } } },
			{ id: 2, username: "bob", profile: { id: 2, user_id: 2, owner: null } },
		]);
		assert.strictEqual(await qs.executeCount(Number), 2);
	});
});
