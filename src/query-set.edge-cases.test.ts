import assert from "node:assert";
import { describe, test } from "node:test";

import { NoResultError } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Edge Cases & Error Handling
//

describe("query-set: edge-cases", () => {
	test("edge case: empty result set - execute returns empty array", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	test("edge case: empty result set - executeTakeFirst returns undefined", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeTakeFirst();

		assert.strictEqual(user, undefined);
	});

	test("edge case: empty result set - executeTakeFirstOrThrow throws", async () => {
		await assert.rejects(async () => {
			await querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "=", 999)
				.executeTakeFirstOrThrow();
		}, NoResultError);
	});

	test("edge case: empty result set - executeCount returns 0", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeCount(Number);

		assert.strictEqual(count, 0);
	});

	test("edge case: empty result set - executeExists returns false", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	test("edge case: empty result set with joins - execute returns empty array", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	test("edge case: empty result set with many joins - execute returns empty array", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	test("edge case: leftJoinOne with no match returns null", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.leftJoinOne(
				"nonExistentProfile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"nonExistentProfile.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				nonExistentProfile: null,
			},
		]);
	});

	test("edge case: leftJoinOneOrThrow with no match throws", async () => {
		await assert.rejects(async () => {
			await querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "=", 1)
				.leftJoinOneOrThrow(
					"nonExistentProfile",
					({ eb, qs }) =>
						qs(
							eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999),
						),
					"nonExistentProfile.user_id",
					"user.id",
				)
				.execute();
		}, ExpectedOneItemError);
	});

	test("edge case: leftJoinMany with no matches returns empty array", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				posts: [],
			},
		]);
	});

	test("edge case: toBaseQuery ignores all joins and hydration", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.toBaseQuery();

		// toBaseQuery() has no ORDER BY at all, so pin one for the comparison
		const rows = await baseQuery.orderBy("id").execute();

		// Should only have base columns, no joins applied
		assert.strictEqual(rows.length, 2);
		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("edge case: toJoinedQuery vs toQuery without pagination are equivalent", async () => {
		// Illustrative rather than load-bearing: without pagination toQuery()
		// returns the joined query unchanged, so both sides execute the same
		// compiled SQL (pinned as compiled-SQL equality in
		// query-set.joins.test.ts). It documents the equivalence for
		// readers, especially next to the with-pagination contrast test below.
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
				"posts.user_id",
				"user.id",
			);

		const joinedRows = await qs.toJoinedQuery().execute();
		const queryRows = await qs.toQuery().execute();

		// Without pagination, both should be identical (flat rows with prefixes)
		assert.deepStrictEqual(joinedRows, queryRows);
		assert.strictEqual(joinedRows.length, 2); // 2 posts
	});

	test("edge case: toJoinedQuery vs toQuery with pagination differ for many-joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(1);

		const joinedRows = await qs.toJoinedQuery().execute();
		const queryRows = await qs.toQuery().execute();

		// toJoinedQuery does not apply pagination at all: it returns every
		// exploded row for both users (user 2 has 4 posts, user 3 has 2).
		assert.strictEqual(joinedRows.length, 6);

		// toQuery applies the limit to unique base records via a nested
		// subquery: 1 user (user 2, the lowest id), with all 4 of their
		// exploded post rows.
		assert.strictEqual(queryRows.length, 4);
		assert.ok(queryRows.every((row) => row.id === 2));
	});

	test("edge case: executeCount ignores limit and offset", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 5)
			.limit(2)
			.offset(1)
			.executeCount(Number);

		// Should count all matching records, ignoring pagination
		assert.strictEqual(count, 5);
	});

	test("edge case: executeExists ignores limit and offset", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 5)
			.limit(0) // Would normally return no results
			.executeExists();

		// Should check existence regardless of limit
		assert.strictEqual(exists, true);
	});

	test("edge case: collection override - second join with same key wins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1)),
				"posts.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 2)),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// Second posts join should override first
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [{ id: 2, title: "Post 2", user_id: 2 }],
			},
		]);
	});

	test("edge case: composite keyBy with array of keys", async () => {
		// Comments 1 and 3 each have 2 replies, so the left join duplicates
		// their (post_id, user_id) pairs: the base produces 6 raw rows for 4
		// distinct composite keys. The keys share post_id (comments 1, 2 are
		// both on post 1) AND user_id (comments 3, 10 are both by user 1), so
		// a composite keyBy that collapsed to either single column would
		// produce 3 entities instead of 4.
		const qs = querySet(db).selectAs(
			"comment",
			db
				.selectFrom("comments")
				.leftJoin("replies", "replies.comment_id", "comments.id")
				.select(["comments.post_id", "comments.user_id"])
				.where("comments.id", "in", [1, 2, 3, 10]),
			["post_id", "user_id"],
		);

		const rawRows = await qs.toBaseQuery().execute();
		assert.strictEqual(rawRows.length, 6); // Proves the dedup below is real

		const comments = await qs.execute();

		// Deduplicated by the (post_id, user_id) composite key, ordered by it
		assert.deepStrictEqual(comments, [
			{ post_id: 1, user_id: 2 },
			{ post_id: 1, user_id: 3 },
			{ post_id: 2, user_id: 1 },
			{ post_id: 10, user_id: 1 },
		]);
	});

	test("edge case: toJoinedQuery shows raw prefixed columns", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		// Should have prefixed columns
		assert.strictEqual(rows.length, 1);
		assert.ok("profile$$id" in rows[0]!);
		assert.ok("profile$$bio" in rows[0]!);
		assert.strictEqual(rows[0]!.id, 2);
		assert.strictEqual(rows[0]!.username, "bob");
		assert.strictEqual(rows[0]!["profile$$id"], 2);
		assert.strictEqual(rows[0]!["profile$$bio"], "Bio for user 2");
	});

	test("edge case: deeply nested toJoinedQuery shows double prefixes", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1),
					).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		// Should have double-prefixed columns for nested collections
		assert.strictEqual(rows.length, 2); // Post 1 has 2 comments
		assert.ok("posts$$id" in rows[0]!);
		assert.ok("posts$$title" in rows[0]!);
		assert.ok("posts$$comments$$id" in rows[0]!);
		assert.ok("posts$$comments$$content" in rows[0]!);
	});

	test("edge case: executeCount with many-joins counts unique base records", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();
		const joinedRows = await qs.toJoinedQuery().execute();

		// Should count unique users (2), not exploded rows
		assert.strictEqual(count, 2);
		assert.strictEqual(users.length, 2); // Verify count matches execute
		assert.ok(joinedRows.length > users.length); // Row explosion in joined query
	});

	// ("map prevents further joins" is a type-level claim, pinned with
	// ts-expect-error directives in query-set.test-d.ts ("Terminal .map() -
	// limitations"); runtime map behavior is covered in
	// query-set.hydration.test.ts.)

	test("edge case: extras do not cascade - each receives original row", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.extras({
				first: (row) => row.id,
			})
			.extras({
				// Reads the first extra's key: if extras cascaded, this would be 1
				second: (row) => (row as { first?: number }).first,
			})
			.execute();

		// The second extra received the ORIGINAL row, not the first extra's
		// output, so `second` is undefined (the key is still set)
		assert.deepStrictEqual(users, [
			{
				id: 1,
				username: "alice",
				first: 1,
				second: undefined,
			},
		]);
	});

	test("edge case: omit removes original fields not extras", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.extras({
				displayName: (row) => row.username.toUpperCase(),
			})
			.omit(["username"])
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 1,
				displayName: "ALICE",
			},
		]);
	});

	test("edge case: crossJoinMany creates cartesian product", async () => {
		// Create a small dataset to verify cartesian product
		const result = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]).where("id", "<=", 2))
			.crossJoinMany("allPosts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title"]).where("user_id", "=", 3)),
			)
			.execute();

		// User 1 (alice) and User 2 (bob) crossed with carol's 2 posts = 4 combinations
		assert.strictEqual(result.length, 2);
		assert.strictEqual(result[0]?.allPosts.length, 2);
		assert.strictEqual(result[1]?.allPosts.length, 2);

		// Alice gets all of carol's posts
		assert.deepStrictEqual(result, [
			{
				id: 1,
				username: "alice",
				allPosts: [
					{ id: 3, title: "Post 3" },
					{ id: 15, title: "Post 15" },
				],
			},
			{
				id: 2,
				username: "bob",
				allPosts: [
					{ id: 3, title: "Post 3" },
					{ id: 15, title: "Post 15" },
				],
			},
		]);
	});
});
