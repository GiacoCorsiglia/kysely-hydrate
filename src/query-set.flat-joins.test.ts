import assert from "node:assert";
import { describe, test } from "node:test";

import { sql } from "kysely";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Flat join chains
//
// A query set nested in a join is emitted as a flat join chain where it can be: its base joined on
// its own as `key`, and its joins hoisted after it as `key$$child` (see QuerySetImpl#joinAs).  The
// decision is made per join, so a tweak to one join may only keep THAT join in a derived table.
//
// - The cliff tests pin which relations each query joins, in order, so that CI fails if a fallback
//   widens (or a flattening that's not an identity sneaks in).
// - The differential tests execute each configuration flat and forced into the nested form (a
//   nested query set with a modifier always keeps its derived table), and compare the hydrated
//   results, counts and existence, on SQLite and Postgres.
//

const qs = querySet(db);
const users = () => qs.selectAs("user", db.selectFrom("users").select(["id", "username"]));
const posts = () => qs.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]));
const comments = () =>
	qs.selectAs("comment", db.selectFrom("comments").select(["id", "content", "post_id", "user_id"]));
const profiles = () =>
	qs.selectAs("profile", db.selectFrom("profiles").select(["id", "bio", "user_id"]));
const replies = () =>
	qs.selectAs("reply", db.selectFrom("replies").select(["id", "comment_id", "content"]));

/** Posts with their comments, joined by `on` (defaults to `comments.post_id = post.id`). */
const postsWithComments = (on?: (join: any) => any) =>
	on
		? posts().leftJoinMany("comments", comments(), on)
		: posts().leftJoinMany("comments", comments(), "comments.post_id", "post.id");

/**
 * The aliases of the derived tables (and CTEs) a query set's query selects from, in order: a flat
 * chain shows up as `posts posts$$comments`, a derived table containing a join as
 * `post comments posts` (the inner base and join, then the derived table itself).
 */
function relations(query: { toQuery(): { compile(): { sql: string } } }): string[] {
	return [
		...query
			.toQuery()
			.compile()
			.sql.matchAll(/\) as "([^"]+)"/g),
	].map((match) => match[1]!);
}

/** The column aliases a query set's query selects, in order. */
function columns(query: { toQuery(): { compile(): { sql: string } } }): string[] {
	const { sql } = query.toQuery().compile();
	return [...sql.matchAll(/(?<!\)) as "([^"]+)"/g)].map((match) => match[1]!);
}

describe("query-set: flat join chains", () => {
	describe("cliffs: which joins are flat", () => {
		test("left join of a left join is flat", () => {
			assert.deepStrictEqual(
				relations(users().leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id")),
				["user", "posts", "posts$$comments"],
			);
		});

		test("every level of a deep chain is flat", () => {
			const query = users().leftJoinMany(
				"posts",
				posts().leftJoinMany(
					"comments",
					comments().leftJoinMany("replies", replies(), "replies.comment_id", "comment.id"),
					"comments.post_id",
					"post.id",
				),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), [
				"user",
				"posts",
				"posts$$comments",
				"posts$$comments$$replies",
			]);
		});

		test("column order is the nested form's, whatever is hoisted", () => {
			const build = (nested: (qs: any) => any) =>
				users().leftJoinMany(
					"posts",
					nested(
						posts()
							.leftJoinOne("author", users(), "author.id", "post.user_id")
							.leftJoinMany("comments", comments(), (join) => join.on(sql`true`)),
					),
					"posts.user_id",
					"user.id",
				);
			const flat = build((qs) => qs);
			// The author is hoisted after the derived table holding the comments.
			assert.deepStrictEqual(relations(flat), [
				"user",
				"post",
				"comments",
				"posts",
				"posts$$author",
			]);
			const topLevel = (query: typeof flat) =>
				columns(query).filter((name) => name.startsWith("posts$$"));
			assert.deepStrictEqual(topLevel(flat), topLevel(build((qs) => qs.modifyEnd(sql``))));
		});

		test("raw SQL in a nested join's ON keeps only that join in the derived table", () => {
			const query = users().leftJoinMany(
				"posts",
				posts()
					.leftJoinMany("comments", comments(), (join) =>
						join.on(sql`${sql.ref("comments.post_id")} = ${sql.ref("post.id")}`),
					)
					.leftJoinOne("author", users(), "author.id", "post.user_id"),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"comments",
				"posts",
				"posts$$author",
			]);
		});

		test("raw SQL in the ON of the join itself doesn't stop its joins from being hoisted", () => {
			const query = users().leftJoinMany("posts", postsWithComments(), (join) =>
				join.on(sql`${sql.ref("posts.user_id")} = ${sql.ref("user.id")}`),
			);
			assert.deepStrictEqual(relations(query), ["user", "posts", "posts$$comments"]);
		});

		test("an ON referencing a hoisted column keeps that column's join in the derived table", () => {
			const query = users().leftJoinMany(
				"posts",
				posts()
					.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
					.leftJoinOne("author", users(), "author.id", "post.user_id"),
				(join) =>
					join
						.onRef("posts.user_id", "=", "user.id")
						.on(sql.ref("posts.comments$$id"), "is not", null),
			);
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"comments",
				"posts",
				"posts$$author",
			]);
		});

		test("OR and IS in a nested left join's ON: flat under an inner join only", () => {
			const or = (join: any) =>
				join.on((eb: any) =>
					eb.or([eb("comments.post_id", "=", eb.ref("post.id")), eb("comments.id", "=", 5)]),
				);
			const is = (join: any) => join.on("comments.post_id", "is", sql.ref("post.id"));
			for (const on of [or, is]) {
				assert.deepStrictEqual(
					relations(
						users().innerJoinMany("posts", postsWithComments(on), "posts.user_id", "user.id"),
					),
					["user", "posts", "posts$$comments"],
				);
				assert.deepStrictEqual(
					relations(
						users().leftJoinMany("posts", postsWithComments(on), "posts.user_id", "user.id"),
					),
					["user", "post", "comments", "posts"],
				);
			}
		});

		test("an OR of null-rejecting comparisons is null-rejecting", () => {
			const on = (join: any) =>
				join.on((eb: any) =>
					eb.or([
						eb("comments.post_id", "=", eb.ref("post.id")),
						eb("comments.user_id", "=", eb.ref("post.user_id")),
					]),
				);
			assert.deepStrictEqual(
				relations(users().leftJoinMany("posts", postsWithComments(on), "posts.user_id", "user.id")),
				["user", "posts", "posts$$comments"],
			);
		});

		test("a non-null-rejecting ON under a left join keeps its join in the derived table", () => {
			const query = users().leftJoinMany(
				"posts",
				postsWithComments((join) => join.on("comments.user_id", "=", 3)),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "post", "comments", "posts"]);
		});

		test("innerJoin inside leftJoinMany keeps the derived table; inside innerJoinMany it's flat", () => {
			const nested = () =>
				posts().innerJoinMany("comments", comments(), "comments.post_id", "post.id");
			assert.deepStrictEqual(
				relations(users().leftJoinMany("posts", nested(), "posts.user_id", "user.id")),
				["user", "post", "comments", "posts"],
			);
			assert.deepStrictEqual(
				relations(users().innerJoinMany("posts", nested(), "posts.user_id", "user.id")),
				["user", "posts", "posts$$comments"],
			);
		});

		test("a cross join is hoisted under an inner join", () => {
			const query = users().innerJoinMany(
				"posts",
				posts().where("posts.id", "<", 3).crossJoinMany("everyone", users()),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "posts", "posts$$everyone"]);
		});

		test("limit/offset on a nested set keeps its derived table, with its insides flat", () => {
			const query = users().leftJoinMany(
				"posts",
				posts()
					.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
					.leftJoinOne(
						"author",
						users().leftJoinOne("profile", profiles(), "profile.user_id", "user.id"),
						"author.id",
						"post.user_id",
					)
					.limit(2),
				"posts.user_id",
				"user.id",
			);
			// The paginated posts (with the one-join), then its many-join, all in the derived table.
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"author",
				"author$$profile",
				"post",
				"comments",
				"posts",
			]);
		});

		test("modify() on the parent keeps the chain flat", () => {
			const query = users()
				.modify((qb) => qb.where("users.id", ">", 1))
				.leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id");
			assert.deepStrictEqual(relations(query), ["user", "posts", "posts$$comments"]);
		});

		test("modifyFront/modifyEnd on the parent keep the chain flat unless they name a hoisted column", () => {
			const base = () =>
				users().leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id");
			assert.deepStrictEqual(relations(base().modifyEnd(sql`limit 100`)), [
				"user",
				"posts",
				"posts$$comments",
			]);
			assert.deepStrictEqual(relations(base().modifyFront(sql`distinct`)), [
				"user",
				"posts",
				"posts$$comments",
			]);
			assert.deepStrictEqual(
				relations(base().modifyEnd(sql`/* ${sql.ref("posts.comments$$id")} */`)),
				["user", "post", "comments", "posts"],
			);
			assert.deepStrictEqual(relations(base().modifyEnd(sql`/* "posts"."comments$$id" */`)), [
				"user",
				"post",
				"comments",
				"posts",
			]);
		});

		test("a qualified hoisted reference in a modifier only keeps that join in the derived table", () => {
			const query = users()
				.leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id")
				.leftJoinOne(
					"profile",
					profiles().leftJoinOne("owner", users(), "owner.id", "profile.user_id"),
					"profile.user_id",
					"user.id",
				)
				.modifyEnd(sql`/* ${sql.ref("posts.comments$$id")} */`);
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"comments",
				"posts",
				"profile",
				"profile$$owner",
			]);
		});

		test("an unqualified column in any ON at a level stops hoisting into that level only", () => {
			// Hoisted tables would make `post_id` ambiguous in the top-level scope.
			const query = users()
				.leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id")
				.leftJoinMany("userComments", comments(), (join) => join.onRef("post_id", "=", "user.id"));
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"comments",
				"posts",
				"userComments",
			]);

			// Inside the posts' derived table (kept by the author's raw ON), the same goes for its own
			// scope, and only it: the comments' replies stay in their derived table.
			const deep = users().leftJoinMany(
				"posts",
				posts()
					.leftJoinMany(
						"comments",
						comments().leftJoinMany("replies", replies(), "replies.comment_id", "comment.id"),
						(join) => join.on(sql`${sql.ref("comments.post_id")} = ${sql.ref("post.id")}`),
					)
					.leftJoinMany("mine", comments(), (join) =>
						join.onRef("mine.post_id", "=", "post.id").on("content", "is not", null),
					),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(deep), [
				"user",
				"post",
				"comment",
				"replies",
				"comments",
				"mine",
				"posts",
			]);
		});

		test("an unqualified column in an EXISTS subquery's ON stops hoisting into the count query", () => {
			const profileWithOwner = () =>
				profiles().innerJoinOne("owner", users(), "owner.id", "profile.user_id");
			const count = (on: (join: any) => any) =>
				users()
					.innerJoinOne("profile", profileWithOwner(), "profile.user_id", "user.id")
					.innerJoinMany("posts", posts(), on)
					.toCountQuery()
					.compile().sql;
			const flat = count((join) => join.onRef("posts.user_id", "=", "user.id"));
			assert.match(flat, /\) as "profile\$\$owner"/);
			const nested = count((join) => join.onRef("title", "=", "user.username"));
			assert.doesNotMatch(nested, /\) as "profile\$\$owner"/);
			assert.match(nested, /\) as "owner"/);
		});

		test("raw SQL is taken to name an unqualified column unless every name in it is qualified", () => {
			const withOn = (on: (join: any) => any) =>
				users().leftJoinMany("posts", postsWithComments(), on);
			assert.deepStrictEqual(relations(withOn((join) => join.on(sql`posts.user_id = "user".id`))), [
				"user",
				"posts",
				"posts$$comments",
			]);
			assert.deepStrictEqual(relations(withOn((join) => join.on(sql`user_id = "user".id`))), [
				"user",
				"post",
				"comments",
				"posts",
			]);
			const base = () =>
				users().leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id");
			assert.deepStrictEqual(relations(base().modifyEnd(sql`for no key update skip locked`)), [
				"user",
				"posts",
				"posts$$comments",
			]);
			assert.deepStrictEqual(relations(base().modifyEnd(sql`order by username`)), [
				"user",
				"post",
				"comments",
				"posts",
			]);
		});

		test("modifiers on a nested set keep its derived table", () => {
			const query = users().leftJoinMany(
				"posts",
				postsWithComments().modifyEnd(sql``),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "post", "comments", "posts"]);
		});

		test("ordering by a nested join's hoisted column keeps only that join in the derived table", () => {
			const profileWithOwner = () =>
				profiles()
					.innerJoinOne("owner", users(), "owner.id", "profile.user_id")
					.leftJoinOne("friend", users(), "friend.id", "profile.id");
			const query = users().innerJoinOne(
				"profile",
				profileWithOwner(),
				"profile.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), [
				"user",
				"profile",
				"profile$$owner",
				"profile$$friend",
			]);
			assert.deepStrictEqual(relations(query.orderBy("profile$$bio")), [
				"user",
				"profile",
				"profile$$owner",
				"profile$$friend",
			]);
			assert.deepStrictEqual(relations(query.orderBy("profile$$owner$$username")), [
				"user",
				"profile",
				"owner",
				"profile",
				"profile$$friend",
			]);
		});

		test("ordering in a paginated wrapper doesn't keep the outer many-joins nested", () => {
			const query = users()
				.innerJoinOne(
					"profile",
					profiles().innerJoinOne("owner", users(), "owner.id", "profile.user_id"),
					"profile.user_id",
					"user.id",
				)
				.leftJoinMany("posts", postsWithComments(), "posts.user_id", "user.id")
				.orderBy("profile$$owner$$username")
				.limit(3);
			assert.deepStrictEqual(relations(query), [
				"user",
				"profile",
				"owner",
				"profile",
				"user",
				"posts",
				"posts$$comments",
			]);
		});

		test("sibling joins: only the one that can't be hoisted stays in the derived table", () => {
			const query = users().leftJoinMany(
				"posts",
				posts()
					.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
					.innerJoinOne("author", users(), "author.id", "post.user_id")
					.leftJoinMany("mine", comments(), (join) => join.on("mine.user_id", "=", 2)),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), [
				"user",
				"post",
				"author",
				"mine",
				"posts",
				"posts$$comments",
			]);
		});

		test("a join that stays in the derived table keeps the joins it may name there too", () => {
			const build = (last: (qs: any) => any) =>
				users().leftJoinMany(
					"posts",
					last(
						posts()
							.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
							.leftJoinOne("author", users(), "author.id", "post.user_id"),
					),
					"posts.user_id",
					"user.id",
				);
			// Its ON names the author (a name the typed API doesn't offer, but valid SQL).
			const naming = build((qs) =>
				qs.leftJoinMany("authorComments", comments(), (join: any) =>
					join.onRef("authorComments.user_id", "=", "author.id").on(sql`true`),
				),
			);
			assert.deepStrictEqual(relations(naming), [
				"user",
				"post",
				"author",
				"authorComments",
				"posts",
				"posts$$comments",
			]);
			// Raw SQL may name anything before it.
			const raw = build((qs) =>
				qs.leftJoinMany("authorComments", comments(), (join: any) =>
					join.on(sql`"authorComments".user_id = author.id`),
				),
			);
			assert.deepStrictEqual(relations(raw), [
				"user",
				"post",
				"comments",
				"author",
				"authorComments",
				"posts",
			]);
			// A join that names neither doesn't keep them.
			const other = build((qs) =>
				qs.leftJoinMany("mine", comments(), (join: any) => join.on("mine.user_id", "=", 2)),
			);
			assert.deepStrictEqual(relations(other), [
				"user",
				"post",
				"mine",
				"posts",
				"posts$$comments",
				"posts$$author",
			]);
		});

		test("a left join without an ON is kept under a left join", () => {
			const query = users().leftJoinMany(
				"posts",
				posts().leftJoinMany("everyone", users(), (join: any) => join),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "post", "everyone", "posts"]);
		});

		test("count and exists queries are flat", () => {
			const query = users().innerJoinMany("posts", postsWithComments(), "posts.user_id", "user.id");
			for (const compiled of [query.toCountQuery().compile(), query.toExistsQuery().compile()]) {
				assert.match(
					compiled.sql,
					/\) as "posts\$\$comments" on "posts\$\$comments"\."post_id" = "posts"\."id"/,
				);
			}
		});

		test("a reduced join keeps its derived table", () => {
			const query = users()
				.leftJoinOne(
					"profile",
					profiles().leftJoinMany("posts", posts(), "posts.user_id", "profile.user_id"),
					"profile.user_id",
					"user.id",
				)
				.orderBy("profile$$bio")
				.limit(3);
			// The reduced profile (its many-join dropped, in a derived table of its own) orders the page;
			// the full one is joined, flat, outside.
			assert.deepStrictEqual(relations(query), [
				"user",
				"profile",
				"profile",
				"user",
				"profile",
				"profile$$posts",
			]);
		});

		test("a nested set whose alias differs from its key", () => {
			const query = users().leftJoinMany(
				"writings",
				postsWithComments(),
				"writings.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "writings", "writings$$comments"]);
			assert.match(
				query.toQuery().compile().sql,
				/"writings\$\$comments"\."post_id" = "writings"\."id"/,
			);
		});

		test("a nested set built by a factory callback", () => {
			const query = users().leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "user_id"])).leftJoinMany(
						"comments",
						comments(),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(relations(query), ["user", "posts", "posts$$comments"]);
		});

		test("an ON callback runs once per compile, and again on the next one", () => {
			let calls = 0;
			const query = users().leftJoinMany(
				"posts",
				postsWithComments((join) => {
					calls++;
					return join.onRef("comments.post_id", "=", "post.id");
				}),
				(join) => {
					calls++;
					return join.onRef("posts.user_id", "=", "user.id");
				},
			);
			query.toQuery().compile();
			assert.strictEqual(calls, 2);
			query.toQuery().compile();
			assert.strictEqual(calls, 4);
		});

		test("an ON callback runs once per compile in paginated, count and exists queries", () => {
			const calls = { profile: 0, posts: 0, comments: 0 };
			const counted = (key: keyof typeof calls, k1: string, k2: string) => (join: any) => {
				calls[key]++;
				return join.onRef(k1, "=", k2);
			};
			const query = users()
				.innerJoinOne(
					"profile",
					profiles().leftJoinOne("owner", users(), "owner.id", "profile.user_id"),
					counted("profile", "profile.user_id", "user.id"),
				)
				.innerJoinMany(
					"posts",
					postsWithComments(counted("comments", "comments.post_id", "post.id")),
					counted("posts", "posts.user_id", "user.id"),
				)
				.limit(2);
			// As many times as the join is in the SQL: the paginated query has the posts both in the
			// EXISTS that filters the page and in the outer query.
			for (const [compile, times] of [
				[() => query.toQuery().compile(), 2],
				[() => query.toCountQuery().compile(), 1],
				[() => query.toExistsQuery().compile(), 1],
			] as const) {
				Object.assign(calls, { profile: 0, posts: 0, comments: 0 });
				compile();
				assert.deepStrictEqual(calls, { profile: 1, posts: times, comments: times });
			}
		});

		test("an ON callback's current condition is used on every compile", () => {
			let threshold = 1;
			const query = users().leftJoinMany(
				"posts",
				postsWithComments((join) =>
					join.onRef("comments.post_id", "=", "post.id").on("comments.id", ">", threshold),
				),
				"posts.user_id",
				"user.id",
			);
			assert.deepStrictEqual(query.toQuery().compile().parameters, [1]);
			threshold = 2;
			assert.deepStrictEqual(query.toQuery().compile().parameters, [2]);
		});
	});

	describe("results", () => {
		test("left join of a left join keeps parents without children", async () => {
			const result = await users()
				.where("users.id", "<=", 3)
				.leftJoinMany(
					"posts",
					postsWithComments().where("posts.id", "in", [1, 3, 15]),
					"posts.user_id",
					"user.id",
				)
				.execute();
			assert.deepStrictEqual(result, [
				{ id: 1, username: "alice", posts: [] },
				{
					id: 2,
					username: "bob",
					posts: [
						{
							id: 1,
							title: "Post 1",
							user_id: 2,
							comments: [
								{ id: 1, content: "Comment 1 on post 1", post_id: 1, user_id: 2 },
								{ id: 2, content: "Comment 2 on post 1", post_id: 1, user_id: 3 },
							],
						},
					],
				},
				{
					id: 3,
					username: "carol",
					posts: [
						{ id: 3, title: "Post 3", user_id: 3, comments: [] },
						{
							id: 15,
							title: "Post 15",
							user_id: 3,
							comments: [{ id: 13, content: "Comment 15 on post 15", post_id: 15, user_id: 7 }],
						},
					],
				},
			]);
		});

		test("a non-null-rejecting nested ON doesn't invent children for missing parents", async () => {
			const result = await users()
				.where("users.id", "<=", 2)
				.leftJoinMany(
					"posts",
					postsWithComments((join) => join.on("comments.user_id", "=", 3)).where(
						"posts.id",
						"=",
						2,
					),
					"posts.user_id",
					"user.id",
				)
				.execute();
			assert.deepStrictEqual(result, [
				{ id: 1, username: "alice", posts: [] },
				{
					id: 2,
					username: "bob",
					posts: [
						{
							id: 2,
							title: "Post 2",
							user_id: 2,
							comments: [{ id: 2, content: "Comment 2 on post 1", post_id: 1, user_id: 3 }],
						},
					],
				},
			]);
		});
	});

	describe("differential: flat vs nested", () => {
		/**
		 * Each configuration builds its query, calling `nested` on every nested query set that has
		 * joins.  Built with `nested = (qs) => qs.modifyEnd(sql``)`, every one of them keeps its
		 * derived table: the nested form, which these results must match.
		 */
		type Nest = <Q extends { modifyEnd(modifier: any): Q }>(qs: Q) => Q;
		interface Config {
			build: (nested: Nest) => {
				execute(): Promise<unknown[]>;
				executeCount(cast: (count: string | number | bigint) => number): Promise<number>;
				executeExists(): Promise<boolean>;
				toQuery(): { compile(): { sql: string } };
				toJoinedQuery(): { execute(): Promise<unknown[]> };
			};
			/** Whether the query is expected to contain a flat join chain. */
			flat: boolean;
			pgOnly?: boolean;
			sqliteOnly?: boolean;
		}

		const pc = (nested: Nest, on?: (join: any) => any) => nested(postsWithComments(on));
		const top = (join: "leftJoinMany" | "innerJoinMany", nestedQs: any) =>
			(users().where("users.id", "<=", 6) as any)[join](
				"posts",
				nestedQs,
				"posts.user_id",
				"user.id",
			);

		const configs: Record<string, Config> = {
			"L(L)": { flat: true, build: (n) => top("leftJoinMany", pc(n)) },
			"L(L) onRef callback": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) => j.onRef("comments.post_id", "=", "post.id")),
					),
			},
			"L(L) and value": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) =>
							j.onRef("comments.post_id", "=", "post.id").on("comments.user_id", "=", 3),
						),
					),
			},
			"L(L) not null-rejecting": {
				flat: false,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) => j.on("comments.user_id", "=", 3)),
					),
			},
			"I(L) not null-rejecting": {
				flat: true,
				build: (n) =>
					top(
						"innerJoinMany",
						pc(n, (j) => j.on("comments.user_id", "=", 3)),
					),
			},
			"L(L) or": {
				flat: false,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) =>
							j.on((eb: any) =>
								eb.or([eb("comments.post_id", "=", eb.ref("post.id")), eb("comments.id", "=", 5)]),
							),
						),
					),
			},
			"I(L) or": {
				flat: true,
				build: (n) =>
					top(
						"innerJoinMany",
						pc(n, (j) =>
							j.on((eb: any) =>
								eb.or([eb("comments.post_id", "=", eb.ref("post.id")), eb("comments.id", "=", 5)]),
							),
						),
					),
			},
			"L(L) raw": {
				flat: false,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) => j.on(sql`${sql.ref("comments.post_id")} = ${sql.ref("post.id")}`)),
					),
			},
			"L(L) function": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						pc(n, (j) =>
							j.on((eb: any) => eb(eb.fn("abs", ["comments.post_id"]), "=", eb.ref("post.id"))),
						),
					),
			},
			"L(I)": {
				flat: false,
				build: (n) =>
					top(
						"leftJoinMany",
						n(posts().innerJoinMany("comments", comments(), "comments.post_id", "post.id")),
					),
			},
			"I(I)": {
				flat: true,
				build: (n) =>
					top(
						"innerJoinMany",
						n(posts().innerJoinMany("comments", comments(), "comments.post_id", "post.id")),
					),
			},
			"I(L)": { flat: true, build: (n) => top("innerJoinMany", pc(n)) },
			"L(L(I one))": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts().leftJoinMany(
								"comments",
								n(comments().innerJoinOne("author", users(), "author.id", "comment.user_id")),
								"comments.post_id",
								"post.id",
							),
						),
					),
			},
			"L(L(L many))": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts().leftJoinMany(
								"comments",
								n(
									comments().leftJoinMany("replies", replies(), "replies.comment_id", "comment.id"),
								),
								"comments.post_id",
								"post.id",
							),
						),
					),
			},
			"L(L, I one, L raw) siblings": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts()
								.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
								.innerJoinOne("author", users(), "author.id", "post.user_id")
								.leftJoinMany("mine", comments(), (j) =>
									j.on(sql`${sql.ref("mine.post_id")} = ${sql.ref("post.id")}`),
								),
						),
					),
			},
			"L(L, L one, L naming it) siblings": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts()
								.leftJoinMany("comments", comments(), "comments.post_id", "post.id")
								.leftJoinOne("author", users(), "author.id", "post.user_id")
								.leftJoinMany("authorComments", comments(), (j: any) =>
									j.onRef("authorComments.user_id", "=", "author.id").on(sql`true`),
								),
						),
					),
			},
			"L(L one, lateral naming it) siblings": {
				flat: false,
				pgOnly: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts()
								.leftJoinOne("author", users(), "author.id", "post.user_id")
								.leftJoinLateralMany(
									"authorComments",
									({ eb, qs }: any) =>
										qs(
											eb
												.selectFrom("comments")
												.select(["id", "user_id"])
												.whereRef("comments.user_id", "=", "author.id"),
										),
									(j: any) => j.onTrue(),
								),
						),
					),
			},
			"one(one) + a later lateral join naming its hoisted column": {
				flat: false,
				pgOnly: true,
				build: (n) =>
					users()
						.leftJoinOne(
							"profile",
							n(profiles().leftJoinOne("owner", users(), "owner.id", "profile.user_id")),
							"profile.user_id",
							"user.id",
						)
						.leftJoinLateralMany(
							"theirPosts",
							({ eb, qs }: any) =>
								qs(
									eb
										.selectFrom("posts")
										.select(["id", "user_id"])
										.whereRef("posts.user_id", "=", "profile.owner$$id"),
								),
							(j: any) => j.onTrue(),
						),
			},
			"L(L without ON)": {
				flat: false,
				sqliteOnly: true,
				build: (n) => top("leftJoinMany", n(posts().leftJoinMany("everyone", users(), (j) => j))),
			},
			"L(L) + sibling top-level join": {
				flat: true,
				build: (n) =>
					top("leftJoinMany", pc(n)).leftJoinOne(
						"profile",
						profiles(),
						"profile.user_id",
						"user.id",
					),
			},
			"L(L) nested where": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							posts()
								.where("posts.id", ">", 2)
								.leftJoinMany(
									"comments",
									comments().where("comments.id", ">", 3),
									"comments.post_id",
									"post.id",
								),
						),
					),
			},
			"L(L) paginated": {
				flat: true,
				build: (n) => top("leftJoinMany", pc(n)).limit(3).offset(1),
			},
			"L(L) ordered desc, paginated": {
				flat: true,
				build: (n) => top("leftJoinMany", pc(n)).orderBy("username", "desc").limit(4),
			},
			"L(L paginated)": {
				flat: false,
				build: (n) => top("leftJoinMany", pc(n).orderBy("id", "desc").limit(3)),
			},
			"one(L) reduced, ordered, paginated": {
				flat: true,
				build: (n) =>
					users()
						.leftJoinOne(
							"profile",
							n(profiles().leftJoinMany("posts", posts(), "posts.user_id", "profile.user_id")),
							"profile.user_id",
							"user.id",
						)
						.orderBy("profile$$bio", "desc")
						.limit(4),
			},
			"one(one) ordered by the nested join": {
				flat: false,
				build: (n) =>
					users()
						.innerJoinOne(
							"profile",
							n(profiles().innerJoinOne("owner", users(), "owner.id", "profile.user_id")),
							"profile.user_id",
							"user.id",
						)
						.orderBy("profile$$owner$$username", "desc"),
			},
			"one(one, one) ordered by one nested join, with many-join, paginated": {
				flat: true,
				build: (n) =>
					users()
						.innerJoinOne(
							"profile",
							n(
								profiles()
									.innerJoinOne("owner", users(), "owner.id", "profile.user_id")
									.leftJoinOne("friend", users(), "friend.id", "profile.id"),
							),
							"profile.user_id",
							"user.id",
						)
						.leftJoinMany("posts", pc(n), "posts.user_id", "user.id")
						.orderBy("profile$$owner$$username", "desc")
						.limit(4),
			},
			"cross(L)": {
				flat: true,
				build: (n) =>
					users()
						.where("users.id", "<", 3)
						.crossJoinMany(
							"posts",
							n(
								posts()
									.where("posts.id", "<", 4)
									.leftJoinMany("comments", comments(), "comments.post_id", "post.id"),
							),
						),
			},
			"I(cross)": {
				flat: true,
				build: (n) =>
					top(
						"innerJoinMany",
						n(posts().crossJoinMany("everyone", users().where("users.id", "<", 3))),
					),
			},
			"L(cross)": {
				flat: false,
				build: (n) =>
					top(
						"leftJoinMany",
						n(posts().crossJoinMany("everyone", users().where("users.id", "<", 3))),
					),
			},
			"L(L) with an attach on the nested set": {
				flat: true,
				build: (n) =>
					top(
						"leftJoinMany",
						n(
							postsWithComments().attachMany(
								"replies",
								() => db.selectFrom("replies").select(["id", "comment_id"]).execute(),
								{ matchChild: "comment_id", toParent: "id" },
							),
						),
					),
			},
			"L(L) modifyEnd on the parent": {
				flat: true,
				build: (n) => top("leftJoinMany", pc(n)).modifyEnd(sql`limit 1000`),
			},
			"lateral(L)": {
				flat: true,
				pgOnly: true,
				build: (n) =>
					users()
						.where("users.id", "<=", 6)
						.leftJoinLateralMany(
							"posts",
							({ eb, qs }) =>
								n(
									qs(
										eb
											.selectFrom("posts")
											.select(["id", "title", "user_id"])
											.whereRef("posts.user_id", "=", "user.id"),
									).leftJoinMany("comments", comments(), "comments.post_id", "posts.id"),
								),
							(join) => join.onTrue(),
						),
			},
		};

		for (const [name, { build, flat, pgOnly, sqliteOnly }] of Object.entries(configs)) {
			const skip = (pgOnly && dialect !== "postgres") || (sqliteOnly && dialect !== "sqlite");
			test(name, { skip }, async () => {
				const flatQuery = build((qs) => qs);
				const nestedQuery = build((qs) => qs.modifyEnd(sql``));

				const isFlat = (query: typeof flatQuery) =>
					relations(query).some((alias) => alias.includes("$$"));
				assert.strictEqual(isFlat(flatQuery), flat, "flat");
				assert.strictEqual(isFlat(nestedQuery), false, "nested");

				const expected = await nestedQuery.execute();
				assert.ok(expected.length > 0);
				assert.deepStrictEqual(await flatQuery.execute(), expected);
				// The unhydrated rows too, in any order.
				const rows = async (query: typeof flatQuery) =>
					(await query.toJoinedQuery().execute()).map((row: unknown) => JSON.stringify(row)).sort();
				assert.deepStrictEqual(await rows(flatQuery), await rows(nestedQuery));
				assert.strictEqual(
					await flatQuery.executeCount(Number),
					await nestedQuery.executeCount(Number),
				);
				assert.strictEqual(await flatQuery.executeExists(), await nestedQuery.executeExists());
			});
		}
	});
});
