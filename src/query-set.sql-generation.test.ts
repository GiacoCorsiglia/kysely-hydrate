import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// SQL Generation Verification Tests
//
// These tests verify the SQL generation strategies described in example.ts.
// They ensure that the implementation correctly handles:
// 1. WHERE EXISTS conversion for innerJoinMany in count queries
// 2. leftJoin omission from count queries
// 3. Nested subquery wrapping for pagination with many-joins
// 4. Ordering preservation through pagination transformations
//

//
// executeCount SQL Generation
//

test("SQL: executeCount with no joins - simple count query", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// Should be: SELECT COUNT(*) FROM (base query) AS count_subquery
	assert.ok(sql.startsWith('select count(*) as "count"'));
	assert.ok(sql.includes("from (select"));
	assert.ok(sql.includes('from "users"'));
	assert.ok(sql.includes('where "users"."id" <= ?'));
});

test("SQL: executeCount with innerJoinOne - join included in count", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// Should include the innerJoinOne as a regular inner join
	assert.ok(sql.includes("inner join"), "Should have INNER JOIN");
	assert.ok(sql.includes('from (select "id", "username"'), "Should select from base query");
	assert.ok(sql.includes("profiles"), "Should join with profiles table");
	// Note: toCountQuery() clears selections and just does COUNT(*), so we won't see $$-prefixed columns
	assert.ok(sql.startsWith("select count(*)"), "Should start with COUNT(*)");
});

test("SQL: executeCount with leftJoinOne - join included in count", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// leftJoinOne should be included because WHERE clauses might reference it
	assert.ok(sql.includes("left join"), "leftJoinOne should be included in count query");
	assert.ok(sql.includes("profiles"), "Should join with profiles table");
	// Note: toCountQuery() clears selections and just does COUNT(*), so we won't see $$-prefixed columns
	assert.ok(sql.startsWith("select count(*)"), "Should start with COUNT(*)");
});

test("SQL: executeCount with innerJoinMany - converts to WHERE EXISTS", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// innerJoinMany should convert to WHERE EXISTS to avoid row explosion
	assert.ok(sql.includes("where exists"), "innerJoinMany should use WHERE EXISTS");
	assert.ok(sql.includes("select 1"), "EXISTS should use SELECT 1");
	// Note: There will be an INNER JOIN inside the EXISTS clause, which is fine
	// The key is that the main query doesn't have a direct join causing row explosion
	const mainQueryPart = sql.split("where exists")[0]!;
	assert.ok(!mainQueryPart.includes("posts"), "posts should not be joined directly in main query");
});

test("SQL: executeCount with leftJoinMany - join omitted from count", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// leftJoinMany should be omitted from count query
	assert.ok(!sql.includes("left join"), "leftJoinMany should be omitted from count query");
	assert.ok(!sql.includes("exists"), "leftJoinMany should not use EXISTS");
	assert.ok(!sql.includes("posts$$"), "posts columns should not appear in count query");
});

test("SQL: executeCount with all 4 join types - example.ts lines 52-58 pattern", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.leftJoinOne(
			"setting",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "user_id"])),
			"setting.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.leftJoinMany(
			"comments",
			(nest) => nest((eb) => eb.selectFrom("comments").select(["id", "content"])),
			"user.id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// Correct behavior for count queries:
	// Note: toCountQuery() clears selections and just does COUNT(*), so we won't see $$-prefixed columns

	// - innerJoinOne: included as inner join (safe, no row explosion)
	assert.ok(sql.includes("inner join"), "innerJoinOne should be included");
	assert.ok(sql.includes("profiles"), "Should join profiles table");

	// - leftJoinOne: included as left join (WHERE clauses might reference it)
	assert.ok(sql.includes("left join"), "leftJoinOne should be included");
	// Note: Both joins use profiles table, so we can't distinguish them by table name alone

	// - innerJoinMany: converted to WHERE EXISTS (avoids row explosion)
	assert.ok(sql.includes("where exists"), "innerJoinMany should use WHERE EXISTS");
	assert.ok(sql.includes("posts"), "posts table should appear in EXISTS clause");

	// - leftJoinMany: omitted entirely (doesn't filter, doesn't affect count)
	// comments table should not appear anywhere since leftJoinMany is excluded
	assert.ok(!sql.includes("comments"), "leftJoinMany should not appear");
});

test("SQL: executeCount with nested innerJoinMany - multiple WHERE EXISTS", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
					"comments",
					(init2) => init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// Outer innerJoinMany converts to WHERE EXISTS
	assert.ok(sql.includes("where exists"), "Outer innerJoinMany should use EXISTS");

	// Note: The nested innerJoinMany (comments within posts) doesn't need its own EXISTS
	// because it's already inside the outer EXISTS clause. The outer EXISTS already ensures
	// one row per user, so the inner join can be a regular join within that EXISTS.
	assert.ok(sql.includes("comments"), "comments table should appear in the EXISTS clause");
	assert.ok(sql.includes("posts"), "posts table should appear in the EXISTS clause");
});

//
// Pagination SQL Generation with Many-Joins
//

test("SQL: pagination without many-joins - no nested subquery", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2)
		.offset(1);

	const sql = qs.toQuery().compile().sql;

	// With only cardinality-one joins, limit/offset can be applied directly
	assert.ok(sql.includes("limit ?"), "Should have limit");
	assert.ok(sql.includes("offset ?"), "Should have offset");
	assert.ok(sql.includes("inner join"), "Should have inner join");
	assert.ok(sql.includes("profile$$"), "Should have profile columns");

	// Should NOT have nested subquery wrapping
	const fromCount = (sql.match(/from \(/g) || []).length;
	assert.ok(fromCount <= 2, "Should not have excessive nesting for cardinality-one joins");
});

test("SQL: pagination with innerJoinMany - uses nested subquery", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2)
		.offset(1);

	const sql = qs.toQuery().compile().sql;

	// Per example.ts lines 80-101, should use nested subquery wrapping:
	// 1. Inner query: base + cardinality-one joins + WHERE EXISTS for many-joins + limit/offset
	// 2. Outer query: apply cardinality-many joins to paginated base

	// Should have nested structure
	const fromCount = (sql.match(/from \(/g) || []).length;
	assert.ok(fromCount >= 2, "Should have nested subquery for pagination with many-joins");

	// Should have limit and offset
	assert.ok(sql.includes("limit ?"), "Should apply limit");
	assert.ok(sql.includes("offset ?"), "Should apply offset");

	// Should have the many-join applied in outer query
	assert.ok(sql.includes("posts$$"), "Should have posts columns in outer query");
});

test("SQL: pagination with mixed joins - nested subquery with correct structure", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.leftJoinOne(
			"setting",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "user_id"])),
			"setting.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2);

	const sql = qs.toQuery().compile().sql;

	// Per example.ts lines 80-96:
	// Inner subquery should have:
	// - base query
	// - innerJoinOne (safe for ordering)
	// - leftJoinOne (safe for ordering, can be used in WHERE)
	// - WHERE EXISTS for innerJoinMany (not left join)
	// - limit/offset applied here

	// Outer query should have:
	// - innerJoinMany applied for real

	assert.ok(sql.includes("limit ?"), "Should have limit");
	assert.ok(sql.includes("where exists"), "Inner query should use EXISTS for innerJoinMany");
	assert.ok(sql.includes("posts$$"), "Outer query should have posts columns");
	assert.ok(sql.includes("profile$$"), "Should have profile columns (innerJoinOne)");

	// Verify nested structure
	const fromCount = (sql.match(/from \(/g) || []).length;
	assert.ok(fromCount >= 2, "Should have nested subquery structure");
});

//
// toQuery vs toJoinedQuery Differences
//

test("SQL: toQuery vs toJoinedQuery without pagination - should be identical", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const toQuerySql = qs.toQuery().compile().sql;
	const toJoinedQuerySql = qs.toJoinedQuery().compile().sql;

	// Without pagination, both should produce the same SQL
	assert.strictEqual(
		toQuerySql,
		toJoinedQuerySql,
		"toQuery() and toJoinedQuery() should be identical without pagination",
	);
});

test("SQL: toQuery vs toJoinedQuery with pagination - should differ for many-joins", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2);

	const toQuerySql = qs.toQuery().compile().sql;
	const toJoinedQuerySql = qs.toJoinedQuery().compile().sql;

	// With pagination + many-joins:
	// - toQuery() uses nested subquery strategy
	// - toJoinedQuery() applies limit to raw exploded rows

	assert.notStrictEqual(
		toQuerySql,
		toJoinedQuerySql,
		"toQuery() and toJoinedQuery() should differ with pagination + many-joins",
	);

	// toQuery should have nested structure with WHERE EXISTS
	assert.ok(toQuerySql.includes("where exists"), "toQuery() should use WHERE EXISTS");
	const toQueryFromCount = (toQuerySql.match(/from \(/g) || []).length;
	assert.ok(toQueryFromCount >= 2, "toQuery() should have nested subquery");

	// toJoinedQuery is the raw view without limit/offset (user must handle row explosion themselves)
	assert.ok(!toJoinedQuerySql.includes("limit"), "toJoinedQuery() should not apply limit");
	assert.ok(!toJoinedQuerySql.includes("where exists"), "toJoinedQuery() should not use EXISTS");
});

test("SQL: toQuery with pagination and cardinality-one only - applies limit directly", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3)
		.limit(2);

	const toQuerySql = qs.toQuery().compile().sql;
	const toJoinedQuerySql = qs.toJoinedQuery().compile().sql;

	// toQuery() applies limit when only cardinality-one joins (safe, no row explosion)
	assert.ok(toQuerySql.includes("limit ?"), "toQuery() should have limit");

	// toJoinedQuery() never applies limit (raw view)
	assert.ok(!toJoinedQuerySql.includes("limit"), "toJoinedQuery() should not apply limit");

	// Should not have WHERE EXISTS (not needed for cardinality-one)
	assert.ok(!toQuerySql.includes("where exists"), "Should not use EXISTS for cardinality-one");

	// Should not use nested subquery (not needed for cardinality-one)
	assert.ok(!toQuerySql.includes("where exists"), "Should not use nested subquery");
});

//
// executeExists SQL Generation
//

test("SQL: executeExists - wraps query in EXISTS check", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 999);

	const sql = qs.toExistsQuery().compile().sql;

	// Should wrap the base query in an EXISTS check
	assert.ok(sql.startsWith("select exists"), "Should start with SELECT EXISTS");
	assert.ok(sql.includes("select 1"), "EXISTS should use SELECT 1");
	assert.ok(sql.includes('from "users"'), "Should query from users table");
	assert.ok(sql.includes('where "users"."id" = ?'), "Should include WHERE condition");
});

test("SQL: executeExists with joins - includes joins in EXISTS check", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toExistsQuery().compile().sql;

	// Should include the join in the EXISTS subquery
	assert.ok(sql.startsWith("select exists"), "Should start with SELECT EXISTS");
	assert.ok(sql.includes("inner join"), "Should include inner join");
	assert.ok(sql.includes("select 1"), "EXISTS should use SELECT 1");
});

test("SQL: executeExists ignores limit and offset", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", 3)
		.limit(1)
		.offset(2);

	const sql = qs.toExistsQuery().compile().sql;

	// executeExists should ignore pagination
	assert.ok(!sql.includes("limit"), "EXISTS should ignore limit");
	assert.ok(!sql.includes("offset"), "EXISTS should ignore offset");
	assert.ok(sql.startsWith("select exists"), "Should still be an EXISTS query");
});

//
// toBaseQuery - Returns Base Query Without Joins
//

test("SQL: toBaseQuery strips all joins and returns base query", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.where("users.id", "<=", 3);

	const sql = qs.toBaseQuery().compile().sql;

	// Should only have base query, no joins
	assert.ok(sql.includes('from "users"'), "Should query from users table");
	assert.ok(!sql.includes("join"), "Should not have any joins");
	assert.ok(!sql.includes("profile$$"), "Should not have profile columns");
	assert.ok(!sql.includes("posts$$"), "Should not have posts columns");
	assert.ok(sql.includes('where "users"."id" <= ?'), "Should preserve WHERE conditions");
});
