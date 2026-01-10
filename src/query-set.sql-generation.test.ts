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

function snapshot(template: TemplateStringsArray) {
	return template
		.join(" ")
		.replace(/--.*/g, "")
		.replace(/\s+/g, " ")
		.replaceAll("( ", "(")
		.replaceAll(" )", ")")
		.trim();
}

test("snapshot", () => {
	const unindented = "foo bar baz (bing)";
	assert.strictEqual(
		unindented,
		snapshot`
			foo
				-- comment ignored
			  bar
		baz (
			bing
		)
		`,
	);
});

//
// executeCount SQL Generation
//

test("SQL: executeCount with no joins - simple count query", async () => {
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", 3);

	const sql = qs.toCountQuery().compile().sql;

	// Should be: SELECT COUNT(*) FROM (base query) AS count_subquery
	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select "id", "username" from "users" where "users"."id" <= ?
			) as "user"
		`,
	);
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
	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select "id", "username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			inner join (
				select
					"profile"."id" as "id",
					"profile"."bio" as "bio",
					"profile"."user_id" as "user_id"
				from (
					select
						"id",
						"bio",
						"user_id"
					from "profiles"
				) as "profile"
			) as "profile" on "profile"."user_id" = "user"."id"
		`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select
					"id",
					"username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			-- leftJoinOne should be included because WHERE clauses might reference it
			left join (
				select
					"profile"."id" as "id",
					"profile"."bio" as "bio",
					"profile"."user_id" as "user_id"
				from (
					select
						"id",
						"bio",
						"user_id"
					from "profiles"
				) as "profile"
			) as "profile" on "profile"."user_id" = "user"."id"
		`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select
					"id",
					"username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			-- innerJoinMany should convert to WHERE EXISTS to avoid row explosion
			where exists (
				select
					1 as "_",
					"posts"."id" as "posts$$id",
					"posts"."title" as "posts$$title",
					"posts"."user_id" as "posts$$user_id"
				from
					(SELECT 1) as "__"
					-- Note: There will be an INNER JOIN inside the EXISTS clause, which is fine
					-- The key is that the main query doesn't have a direct join causing row explosion
					inner join (
						select
							"posts"."id" as "id",
							"posts"."title"   as "title",
							"posts"."user_id" as "user_id"
						from (
							select
								"id",
								"title",
								"user_id"
							from "posts"
						) as "posts"
					) as "posts" on "posts"."user_id" = "user"."id"
			)
		`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select
					"id",
					"username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			-- leftJoinMany should be omitted from count query
		`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count"
			from (
				select "id", "username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			-- innerJoinOne: included as inner join (safe, no row explosion)
			inner join (
				select "profile"."id" as "id", "profile"."bio" as "bio", "profile"."user_id" as "user_id"
				from (
					select "id", "bio", "user_id"
					from "profiles"
				) as "profile"
			) as "profile" on "profile"."user_id" = "user"."id"
			-- leftJoinOne: included as left join (WHERE clauses might reference it)
			left join (
				select "setting"."id" as "id", "setting"."user_id" as "user_id"
				from (
					select "id", "user_id"
					from "profiles"
				) as "setting"
			) as "setting" on "setting"."user_id" = "user"."id"
			-- innerJoinMany: converted to WHERE EXISTS (avoids row explosion)
			where exists (
				select 1 as "_", "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
				from
					(SELECT 1) as "__"
					inner join (
						select "posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id"
						from (
							select "id", "title", "user_id"
							from "posts"
						) as "posts"
					) as "posts" on "posts"."user_id" = "user"."id"
			)
			-- leftJoinMany: omitted entirely (doesn't filter, doesn't affect count)
			`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select count(*) as "count" from (
				select "id", "username" from "users" where "users"."id" <= ?
			) as "user"
			-- Outer innerJoinMany converts to WHERE EXISTS
			where exists (
				select 1 as "_", "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id", "posts"."comments$$id" as "posts$$comments$$id", "posts"."comments$$content" as "posts$$comments$$content", "posts"."comments$$post_id" as "posts$$comments$$post_id"
				from (
					SELECT 1
				) as "__"
				-- Note: The nested innerJoinMany (comments within posts) doesn't need its own EXISTS
				-- because it's already inside the outer EXISTS clause. The outer EXISTS already ensures
				-- one row per user, so the inner join can be a regular join within that EXISTS.
				-- TODO: Although this is unneeded, we should probably do the optimization anyway.
				inner join (
					select "posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id", "comments"."id" as "comments$$id", "comments"."content" as "comments$$content", "comments"."post_id" as "comments$$post_id"
					from (
						select "id", "title", "user_id" from "posts"
					) as "posts"
					inner join (
						select "comments"."id" as "id", "comments"."content" as "content", "comments"."post_id" as "post_id"
						from (
							select "id", "content", "post_id" from "comments"
						) as "comments"
					) as "comments" on "comments"."post_id" = "posts"."id"
				) as "posts" on "posts"."user_id" = "user"."id"
			)
		`,
	);
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

	assert.strictEqual(
		sql,
		snapshot`
			select
				"user"."id" as "id",
				"user"."username" as "username",
				"profile"."id" as "profile$$id",
				"profile"."bio" as "profile$$bio",
				"profile"."user_id" as "profile$$user_id"
			from (
				select
					"id", "username"
				from "users"
				where "users"."id" <= ?
			) as "user"
			inner join (
				select
					"profile"."id" as "id", "profile"."bio" as "bio", "profile"."user_id" as "user_id"
				from (
					select "id", "bio", "user_id" from "profiles"
				) as "profile"
			) as "profile" on "profile"."user_id" = "user"."id"
			order by "user"."id" asc
			-- With only cardinality-one joins, limit/offset can be applied directly
			limit ?
			offset ?
		`,
	);
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

	// 1. Inner query: base + cardinality-one joins + WHERE EXISTS for many-joins + limit/offset
	// 2. Outer query: apply cardinality-many joins to paginated base
	assert.strictEqual(
		sql,
		snapshot`
			select
				"user"."id" as "id", "user"."username" as "username", "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
			from (
				select
					"user"."id" as "id", "user"."username" as "username"
				from (
					select
						"id", "username"
					from "users" where "users"."id" <= ?
				) as "user"
				where exists (
					select 1 as "_", "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
				from (
					SELECT 1
				) as "__"
				inner join (
					select
						"posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id"
				from (
					select
						"id", "title", "user_id"
					from "posts") as "posts") as "posts" on "posts"."user_id" = "user"."id"
				)
				order by "user"."id" asc
				limit ?
				offset ?
			) as "user"
			inner join (
				select
					"posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id"
				from (
					select
						"id", "title", "user_id"
					from "posts"
				) as "posts"
			) as "posts" on "posts"."user_id" = "user"."id"
			order by "user"."id" asc
		`,
	);
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

	// Inner subquery should have:
	// - base query
	// - innerJoinOne (safe for ordering)
	// - leftJoinOne (safe for ordering, can be used in WHERE)
	// - WHERE EXISTS for innerJoinMany (not left join)
	// - limit/offset applied here

	// Outer query should have:
	// - innerJoinMany applied for real
	assert.strictEqual(
		sql,
		snapshot`
			select
				"user"."id" as "id",
				"user"."username" as "username",
				"user"."profile$$id" as "profile$$id",
				"user"."profile$$bio" as "profile$$bio",
				"user"."profile$$user_id" as "profile$$user_id",
				"user"."setting$$id" as "setting$$id",
				"user"."setting$$user_id" as "setting$$user_id",
				"posts"."id" as "posts$$id",
				"posts"."title" as "posts$$title",
				"posts"."user_id" as "posts$$user_id"
			from (
				select
					"user"."id" as "id",
					"user"."username" as "username",
					"profile"."id" as "profile$$id",
					"profile"."bio" as "profile$$bio",
					"profile"."user_id" as "profile$$user_id",
					"setting"."id" as "setting$$id",
					"setting"."user_id" as "setting$$user_id"
				from (
					select
						"id",
						"username"
					from "users" where "users"."id" <= ?
				) as "user"
				inner join (
					select
						"profile"."id" as "id",
						"profile"."bio" as "bio",
						"profile"."user_id" as "user_id"
					from (
						select
							"id",
							"bio",
							"user_id"
						from "profiles"
					) as "profile"
				) as "profile" on "profile"."user_id" = "user"."id"
				left join (
					select
						"setting"."id" as "id",
						"setting"."user_id" as "user_id"
					from (
						select
							"id",
							"user_id"
						from "profiles"
					) as "setting"
				) as "setting" on "setting"."user_id" = "user"."id"
				where exists (
					select
						1 as "_",
						"posts"."id" as "posts$$id",
						"posts"."title" as "posts$$title",
						"posts"."user_id" as "posts$$user_id"
					from (
						SELECT 1
					) as "__"
					inner join (
						select
						"posts"."id" as "id",
						"posts"."title" as "title",
						"posts"."user_id" as "user_id"
						from (
							select
								"id",
								"title",
								"user_id"
							from "posts"
						) as "posts"
					) as "posts" on "posts"."user_id" = "user"."id"
				)
				order by "user"."id" asc
				limit ?
			) as "user"
			inner join (
				select
					"posts"."id" as "id",
					"posts"."title" as "title",
					"posts"."user_id" as "user_id"
				from (
					select
						"id",
						"title",
						"user_id"
					from "posts"
				) as "posts"
			) as "posts" on "posts"."user_id" = "user"."id"
			order by "user"."id" asc
		`,
	);
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

	assert.strictEqual(
		toQuerySql,
		snapshot`
			select
				"user"."id" as "id",
				"user"."username" as "username",
				"posts"."id" as "posts$$id",
				"posts"."title" as "posts$$title",
				"posts"."user_id" as "posts$$user_id"
			from (
				select
					"id",
					"username" from "users" where "users"."id" <= ?
			) as "user"
			inner join (
				select
					"posts"."id" as "id",
					"posts"."title" as "title",
					"posts"."user_id" as "user_id"
				from (
					select
						"id",
						"title",
						"user_id" from "posts"
				) as "posts"
			) as "posts" on "posts"."user_id" = "user"."id"
			order by "user"."id" asc
		`,
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

	assert.strictEqual(
		sql,
		snapshot`
			select "id", "username" from "users" where "users"."id" <= ?
		`,
	);
});
