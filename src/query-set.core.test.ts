import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Core QuerySet Tests
//
// Construction (selectAs / writeAs / write), keyBy semantics, base-query
// modification (.modify() / .where()), and the toBaseQuery / toQuery escape
// hatches — all without nested collections. Execution-method semantics
// (empty results, count/exists) live in query-set.execution.test.ts; joins
// live in query-set.joins.test.ts.
//

// Expected hydrated shapes shared across tests.
const ALL_USERS = [
	{ id: 1, username: "alice" },
	{ id: 2, username: "bob" },
	{ id: 3, username: "carol" },
	{ id: 4, username: "dave" },
	{ id: 5, username: "eve" },
	{ id: 6, username: "frank" },
	{ id: 7, username: "grace" },
	{ id: 8, username: "heidi" },
	{ id: 9, username: "ivan" },
	{ id: 10, username: "judy" },
];

const ALL_USERS_WITH_EMAIL = ALL_USERS.map((user) => ({
	...user,
	email: `${user.username}@example.com`,
}));

describe("query-set: core", () => {
	//
	// Basic query execution
	//

	test("execute: returns hydrated rows (keyBy defaults to 'id')", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.execute();

		assert.deepStrictEqual(users, ALL_USERS);
	});

	test("executeTakeFirst: returns the first row", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.executeTakeFirst();

		assert.deepStrictEqual(user, { id: 1, username: "alice" });
	});

	test("executeTakeFirstOrThrow: returns the first row", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.executeTakeFirstOrThrow();

		assert.deepStrictEqual(user, { id: 1, username: "alice" });
	});

	//
	// Construction and keyBy
	//

	test("init: accepts explicit keyBy", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]), "username")
			.execute();

		assert.deepStrictEqual(users, ALL_USERS_WITH_EMAIL);
	});

	test("init: explicit keyBy works without selecting 'id'", async () => {
		// Regression test: the keyBy passed to selectAs must be used by the
		// hydrator.  Previously the hydrator always used "id", so rows without an
		// "id" column were silently dropped.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["username", "email"]), "username")
			.execute();

		assert.deepStrictEqual(
			users,
			ALL_USERS_WITH_EMAIL.map(({ username, email }) => ({ username, email })),
		);
	});

	test("init: groups by explicit keyBy, not by 'id'", async () => {
		// Distinct post authors keyed by user_id.  The join forces the hydrator to
		// group rows, which must happen by the explicit keyBy ("id" is not even
		// selected here).
		const postAuthors = await querySet(db)
			.selectAs("post", db.selectFrom("posts").select(["user_id"]).distinct(), "user_id")
			.innerJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"author.id",
				"post.user_id",
			)
			.execute();

		assert.deepStrictEqual(postAuthors, [
			{ user_id: 2, author: { id: 2, username: "bob" } },
			{ user_id: 3, author: { id: 3, username: "carol" } },
			{ user_id: 4, author: { id: 4, username: "dave" } },
			{ user_id: 5, author: { id: 5, username: "eve" } },
			{ user_id: 6, author: { id: 6, username: "frank" } },
			{ user_id: 7, author: { id: 7, username: "grace" } },
			{ user_id: 8, author: { id: 8, username: "heidi" } },
			{ user_id: 9, author: { id: 9, username: "ivan" } },
			{ user_id: 10, author: { id: 10, username: "judy" } },
		]);
	});

	test("init: keyBy collapsing duplicate base rows hydrates cardinality-one joins", async () => {
		// Regression test: same as above but WITHOUT distinct, so the base query
		// returns several rows per user_id.  The hydrator must group them into
		// one entity and deduplicate the joined author rather than throwing a
		// CardinalityViolationError.
		const postAuthors = await querySet(db)
			.selectAs("post", db.selectFrom("posts").select(["user_id"]), "user_id")
			.innerJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"author.id",
				"post.user_id",
			)
			.execute();

		assert.deepStrictEqual(postAuthors, [
			{ user_id: 2, author: { id: 2, username: "bob" } },
			{ user_id: 3, author: { id: 3, username: "carol" } },
			{ user_id: 4, author: { id: 4, username: "dave" } },
			{ user_id: 5, author: { id: 5, username: "eve" } },
			{ user_id: 6, author: { id: 6, username: "frank" } },
			{ user_id: 7, author: { id: 7, username: "grace" } },
			{ user_id: 8, author: { id: 8, username: "heidi" } },
			{ user_id: 9, author: { id: 9, username: "ivan" } },
			{ user_id: 10, author: { id: 10, username: "judy" } },
		]);
	});

	test("init: composite keyBy with array of keys", async () => {
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

	test("init: accepts factory function", async () => {
		const users = await querySet(db)
			.selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username", "email"]))
			.execute();

		assert.deepStrictEqual(users, ALL_USERS_WITH_EMAIL);
	});

	//
	// Write-based query sets (writeAs / write creation paths)
	//

	test("init: writeAs passes explicit keyBy to the hydrator", async () => {
		// Same keyBy regression as above, via the writeAs() creation path.
		const users = await querySet(db)
			.writeAs(
				"u",
				(db) =>
					db.with("named_users", (qb) => qb.selectFrom("users").select(["username", "email"])),
				(qc) => qc.selectFrom("named_users").select(["username", "email"]),
				"username",
			)
			.execute();

		assert.strictEqual(users.length, 10);
		assert.deepStrictEqual(users[0], { username: "alice", email: "alice@example.com" });
	});

	test("init: writeAs preserves CTEs with orderByKeys(false)", async () => {
		// Regression test: with no joins, no ordering, and no pagination, the
		// executed query took a fast path that returned the base query directly.
		// For write query sets the CTEs live on the query creator, not the base
		// query, so they were silently dropped ("no such table" errors).
		const query = querySet(db)
			.writeAs(
				"u",
				(db) => db.with("named_users", (qb) => qb.selectFrom("users").select(["id", "username"])),
				(qc) => qc.selectFrom("named_users").select(["id", "username"]),
			)
			.orderByKeys(false);

		const { sql } = query.toQuery().compile();
		assert.ok(sql.startsWith('with "named_users"'), sql);

		const users = await query.execute();
		assert.strictEqual(users.length, 10);
	});

	test("init: writeAs preserves CTEs with orderByKeys(false) and a limit", async () => {
		const users = await querySet(db)
			.writeAs(
				"u",
				(db) => db.with("named_users", (qb) => qb.selectFrom("users").select(["id", "username"])),
				(qc) => qc.selectFrom("named_users").select(["id", "username"]),
			)
			.orderByKeys(false)
			.limit(3)
			.execute();

		assert.strictEqual(users.length, 3);
	});

	test("init: write() preserves CTEs with orderByKeys(false)", async () => {
		// Same regression via the .write() method on an existing query set.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.write(
				(db) => db.with("named_users", (qb) => qb.selectFrom("users").select(["id", "username"])),
				(qc) => qc.selectFrom("named_users").select(["id", "username"]),
			)
			.orderByKeys(false)
			.execute();

		assert.strictEqual(users.length, 10);
	});

	//
	// Base-query modification
	//

	test("modify: add WHERE clause", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.modify((qb) => qb.where("id", ">", 5))
			.execute();

		assert.deepStrictEqual(users, ALL_USERS.slice(5));
	});

	test("modify: add additional SELECT", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.modify((qb) => qb.select("email"))
			.execute();

		assert.deepStrictEqual(users, ALL_USERS_WITH_EMAIL);
	});

	test("modify: multiple calls chained", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.modify((qb) => qb.where("id", "<=", 5))
			.modify((qb) => qb.select("email"))
			.execute();

		assert.deepStrictEqual(users, ALL_USERS_WITH_EMAIL.slice(0, 5));
	});

	test("where: simple reference", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("id", "=", 1)
			.execute();

		assert.deepStrictEqual(users, [{ id: 1, username: "alice" }]);
	});

	test("where: with expression factory", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where((eb) => eb.or([eb("id", "=", 1), eb("id", "=", 2)]))
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("where: multiple chained where calls combine with AND", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", ">=", 2)
			.where("users.id", "<=", 3)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob" },
			{ id: 3, username: "carol" },
		]);
	});

	//
	// Escape hatches: toBaseQuery / toQuery
	//

	test("toBaseQuery: returns underlying base query", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.toBaseQuery();

		// toBaseQuery() has no ORDER BY at all, so pin one for the comparison
		const rows = await baseQuery.orderBy("id").execute();
		assert.deepStrictEqual(rows, ALL_USERS);
	});

	test("toBaseQuery: returns modified base query", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.modify((qb) => qb.where("id", "<", 3))
			.toBaseQuery();

		const rows = await baseQuery.execute();
		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("toQuery: returns opaque query builder", async () => {
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.toQuery();

		const rows = await query.execute();
		assert.deepStrictEqual(rows, ALL_USERS);
	});

	test("toQuery: returns opaque query builder with modifications", async () => {
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.modify((qb) => qb.where("id", "=", 1))
			.toQuery();

		const rows = await query.execute();
		assert.deepStrictEqual(rows, [{ id: 1, username: "alice" }]);
	});
});
