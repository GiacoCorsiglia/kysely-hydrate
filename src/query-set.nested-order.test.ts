import assert from "node:assert";
import { describe, test } from "node:test";

import { sql } from "kysely";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { describePg } from "./__tests__/helpers.ts";
import { createHydrator } from "./hydrator.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Nested collections are ordered by SQL, exactly like the top level.
//
// Every expectation here is what the database itself returns for the nested
// collection's ORDER BY, which an in-memory comparator cannot reproduce: NULL
// placement differs by dialect, collations are not code-unit order, and pg
// returns int8 as strings.  User 2 owns posts 1, 2, 5 and 12.
//

const user2 = () =>
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2);

const nullScore = sql<number | null>`nullif(${sql.ref("id")}, 5)`.as("score");
const mixedCaseTitle = sql<string>`case when ${sql.ref("id")} = 2 then lower(${sql.ref("title")}) else ${sql.ref("title")} end`;

describe("query-set: nested collections ordered by SQL", () => {
	test("NULL placement follows the dialect", async () => {
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id", nullScore])).orderBy("score", "asc"),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// SQLite sorts NULLs first for ASC; Postgres sorts them last.
		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.id),
			dialect === "sqlite" ? [5, 1, 2, 12] : [1, 2, 12, 5],
		);
	});

	test("orderBy collate() modifiers are honored", async () => {
		const collation = dialect === "sqlite" ? "NOCASE" : "en-US-x-icu";
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id", mixedCaseTitle.as("title")])).orderBy(
						"title",
						(ob) => ob.collate(collation),
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// Case-insensitively: Post 1, Post 12, post 2, Post 5 (code units would put "post 2" last).
		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.title),
			["Post 1", "Post 12", "post 2", "Post 5"],
		);
	});

	test("int8 keys (strings in pg) order numerically, including the keyBy tiebreak", async () => {
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select([sql<string>`cast(${sql.ref("id")} as bigint)`.as("id"), "user_id"]),
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => String(post.id)),
			["1", "2", "5", "12"],
		);
	});

	test("nested pagination keeps the order the limit was applied in", async () => {
		// The global nested limit keeps the first two posts by score; on SQLite
		// those are the NULL score (post 5) and then post 1, in that order.
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id", nullScore]))
						.orderBy("score", "asc")
						.limit(2),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.id),
			dialect === "sqlite" ? [5, 1] : [1, 2],
		);
	});

	test("nested many-joins are ordered within each parent, depth-first, with siblings", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 4])
			.orderBy("username", "desc")
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id", "title"]))
						.orderBy("title", "desc")
						.leftJoinMany(
							"comments",
							({ eb, qs }) =>
								qs(eb.selectFrom("comments").select(["id", "post_id", "user_id"])).orderBy(
									"user_id",
									"desc",
								),
							"comments.post_id",
							"posts.id",
						),
				"posts.user_id",
				"user.id",
			)
			.leftJoinMany(
				"profiles",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "user_id"])).orderBy("id", "desc"),
				"profiles.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(
			users.map((user) => ({
				id: user.id,
				posts: user.posts.map((post) => ({
					id: post.id,
					commenters: post.comments.map((comment) => comment.user_id),
				})),
				profiles: user.profiles.map((profile) => profile.id),
			})),
			[
				{
					id: 4,
					posts: [
						{ id: 4, commenters: [5] },
						{ id: 13, commenters: [] },
					],
					profiles: [4],
				},
				{
					id: 2,
					posts: [
						{ id: 5, commenters: [6] },
						{ id: 2, commenters: [1] },
						{ id: 12, commenters: [] },
						{ id: 1, commenters: [3, 2] },
					],
					profiles: [2],
				},
			],
		);
	});
	test("toJoinedQuery() rows hydrate in the same order", async () => {
		const qs = user2().leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "user_id", "title"])).orderBy("title", "desc"),
			"posts.user_id",
			"user.id",
		);

		const users = await qs.hydrate(qs.toJoinedQuery().execute());

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.id),
			[5, 2, 12, 1],
		);
	});

	test("orderBy() of a hydrator merged with with() does not sort", async () => {
		// Hydration never sorts, so only the query set's own (SQL) ordering counts.
		const byTitleDesc = createHydrator<{ id: number; title: string }>().orderBy(
			(post) => post.title,
			"desc",
		);
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id", "title"])).with(byTitleDesc),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.id),
			[1, 2, 5, 12],
		);
	});
});

describePg("query-set: nested collections ordered by SQL (postgres)", () => {
	test("column collations are honored", async () => {
		const users = await user2()
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select([
								"id",
								"user_id",
								sql<string>`${mixedCaseTitle} collate "en-US-x-icu"`.as("title"),
							]),
					).orderBy("title", "asc"),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.title),
			["Post 1", "Post 12", "post 2", "Post 5"],
		);
	});

	test("lateral top-N keeps the order the limit was applied in", async () => {
		const users = await user2()
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select([sql<string>`cast(${sql.ref("posts.id")} as bigint)`.as("id"), "user_id"])
							.whereRef("posts.user_id", "=", "user.id"),
					)
						.orderBy("id", "desc")
						.limit(3),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(
			users[0]!.posts.map((post) => post.id),
			["12", "5", "2"],
		);
	});
});
