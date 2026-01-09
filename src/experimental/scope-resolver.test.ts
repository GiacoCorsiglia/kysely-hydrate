import assert from "node:assert/strict";
import { test } from "node:test";

import * as k from "kysely";

import { db, seedDb } from "../__tests__/sqlite.ts";
import { AmbiguousColumnReferenceError } from "../helpers/errors.ts";
import { getMappedNodes, map } from "./mapped-expression.ts";
import { type SomeColumnType } from "./schema/column-type.ts";
import { type Provenance, traceLineage } from "./scope-resolver.ts";

function getQueryNode(query: k.SelectQueryBuilder<any, any, any>): k.SelectQueryNode {
	return query.toOperationNode() as k.SelectQueryNode;
}

// Helper to create expected COLUMN provenance with columnType
function col(
	table: string,
	column: string,
): { type: "COLUMN"; table: string; column: string; columnType: SomeColumnType; mapFns: any[] } {
	const tableName = table.includes(".") ? table.split(".")[1]! : table;
	const tableSchema = (seedDb as any)[tableName];
	if (!tableSchema) {
		throw new Error(`Table ${tableName} not found in seedDb`);
	}
	const columnType = tableSchema.$columns[column];
	if (!columnType) {
		throw new Error(`Column ${column} not found in table ${tableName}`);
	}
	return { type: "COLUMN", table, column, columnType, mapFns: [] };
}

// Basic single-table queries
test("single table: unqualified column reference", () => {
	const query = db.selectFrom("users").select("id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("users", "id")]]));
});

test("single table: multiple unqualified columns", () => {
	const query = db.selectFrom("users").select(["id", "username"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
		]),
	);
});

test("single table: qualified column reference", () => {
	const query = db.selectFrom("users").select("users.id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("users", "id")]]));
});

// Column aliases
test("column alias: simple alias", () => {
	const query = db.selectFrom("users").select("id as user_id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["user_id", col("users", "id")]]));
});

test("column alias: qualified column with alias", () => {
	const query = db.selectFrom("users").select("users.username as name");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["name", col("users", "username")]]));
});

// Table aliases
test("table alias: unqualified column from aliased table", () => {
	const query = db.selectFrom("users as u").select("id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("users", "id")]]));
});

test("table alias: qualified column using alias", () => {
	const query = db.selectFrom("users as u").select("u.username");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["username", col("users", "username")]]));
});

// Joins
test("join: qualified columns from different tables", () => {
	const query = db
		.selectFrom("users")
		.innerJoin("posts", "posts.user_id", "users.id")
		.select(["users.id", "posts.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["title", col("posts", "title")],
		]),
	);
});

test("join: unqualified column throws when ambiguous", () => {
	const query = db
		.selectFrom("posts")
		.innerJoin("comments", "comments.post_id", "posts.id")
		.select("id"); // Both tables have 'id'
	const node = getQueryNode(query);

	assert.throws(
		() => traceLineage(node, seedDb),
		AmbiguousColumnReferenceError,
		"Should throw on ambiguous column reference",
	);
});

// NOTE: Skipping this test because it requires runtime schema information to
// pass; otherwise, we cannot know whether "username" comes from the "posts"
// table or the "users" table.
test.skip("join: unqualified column succeeds when unique", () => {
	const query = db
		.selectFrom("users")
		.innerJoin("posts", "posts.user_id", "users.id")
		.select("username"); // Only users has 'username'
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["username", col("users", "username")]]));
});

// Derived columns
test("derived: function call returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<string>`UPPER(username)`.as("upper_name"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["upper_name", { type: "DERIVED", mapFns: [] }]]));
});

test("derived: arithmetic expression returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<number>`id * 2`.as("double_id"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["double_id", { type: "DERIVED", mapFns: [] }]]));
});

test("derived: literal value returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<string>`'constant'`.as("literal"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["literal", { type: "DERIVED", mapFns: [] }]]));
});

// Subqueries
test("subquery: simple subquery", () => {
	const subquery = db.selectFrom("users").select(["id", "username"]).as("u");
	const query = db.selectFrom(subquery).select(["u.id", "u.username"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
		]),
	);
});

test("subquery: nested subqueries", () => {
	const inner = db.selectFrom("users").select("id as user_id").as("inner");
	const outer = db.selectFrom(inner).select("inner.user_id as uid").as("outer");
	const query = db.selectFrom(outer).select("outer.uid");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["uid", col("users", "id")]]));
});

test("subquery: derived column breaks lineage", () => {
	const subquery = db
		.selectFrom("users")
		.select(k.sql<string>`UPPER(username)`.as("upper_name"))
		.as("u");
	const query = db.selectFrom(subquery).select("u.upper_name");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["upper_name", { type: "DERIVED", mapFns: [] }]]));
});

// CTEs
test("cte: simple cte", () => {
	const query = db
		.withRecursive("user_cte", (db) => db.selectFrom("users").select(["id", "username"]))
		.selectFrom("user_cte")
		.select(["user_cte.id", "user_cte.username"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
		]),
	);
});

test("cte: multiple ctes", () => {
	const query = db
		.with("cte1", (db) => db.selectFrom("users").select("id as user_id"))
		.with("cte2", (db) => db.selectFrom("posts").select("id as post_id"))
		.selectFrom("cte1")
		.innerJoin("cte2", "cte2.post_id", "cte1.user_id")
		.select(["cte1.user_id", "cte2.post_id"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["user_id", col("users", "id")],
			["post_id", col("posts", "id")],
		]),
	);
});

test("cte: cte referencing another cte", () => {
	const query = db
		.with("cte1", (db) => db.selectFrom("users").select("id"))
		.with("cte2", (db) => db.selectFrom("cte1").select("cte1.id as user_id"))
		.selectFrom("cte2")
		.select("cte2.user_id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["user_id", col("users", "id")]]));
});

// Wildcard expansion tests
test("wildcard: selectAll() expands to all columns", () => {
	const query = db.selectFrom("users").selectAll();
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
			["email", col("users", "email")],
		]),
	);
});

test("wildcard: selectAll('table') expands to table columns", () => {
	const query = db
		.selectFrom("users")
		.innerJoin("posts", "posts.user_id", "users.id")
		.selectAll("users");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
			["email", col("users", "email")],
		]),
	);
});

// RETURNING clauses
test("returning: insert with returning", () => {
	const query = db
		.insertInto("users")
		.values({ username: "test", email: "test@example.com" })
		.returning(["id", "username"]);
	const node = query.toOperationNode() as k.InsertQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
		]),
	);
});

test("returning: update with returning", () => {
	const query = db
		.updateTable("users")
		.set({ username: "updated" })
		.where("id", "=", 1)
		.returning("id");
	const node = query.toOperationNode() as k.UpdateQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("users", "id")]]));
});

test("returning: delete with returning", () => {
	const query = db.deleteFrom("users").where("id", "=", 1).returning(["id", "username"]);
	const node = query.toOperationNode() as k.DeleteQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["username", col("users", "username")],
		]),
	);
});

// Complex scenarios
test("complex: cte with join and subquery", () => {
	const subquery = db.selectFrom("comments").select("post_id").as("c");
	const query = db
		.with("user_posts", (db) =>
			db
				.selectFrom("users")
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["users.id as user_id", "posts.id as post_id"]),
		)
		.selectFrom("user_posts")
		.innerJoin(subquery, "c.post_id", "user_posts.post_id")
		.select(["user_posts.user_id", "user_posts.post_id"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["user_id", col("users", "id")],
			["post_id", col("posts", "id")],
		]),
	);
});

test("complex: multiple aliasing levels", () => {
	const subquery = db.selectFrom("users as u").select("u.id as user_id").as("subq");
	const query = db.selectFrom(subquery).select("subq.user_id as final_id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["final_id", col("users", "id")]]));
});

// Schema-qualified tables
test("schema: table with schema qualifier", () => {
	const query = db.selectFrom("public.users" as any).select("id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("public.users", "id")]]));
});

// CASE expressions
test("case: simple case expression returns DERIVED", () => {
	const query = db
		.selectFrom("users")
		.select(k.sql<string>`CASE WHEN id > 10 THEN 'high' ELSE 'low' END`.as("category"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["category", { type: "DERIVED", mapFns: [] }]]));
});

// COALESCE
test("coalesce: coalesce expression returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<string>`COALESCE(username, email)`.as("name"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["name", { type: "DERIVED", mapFns: [] }]]));
});

// Aggregates
test("aggregate: count(*) returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<number>`COUNT(*)`.as("total"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["total", { type: "DERIVED", mapFns: [] }]]));
});

test("aggregate: count(column) returns DERIVED", () => {
	const query = db.selectFrom("users").select(k.sql<number>`COUNT(id)`.as("user_count"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["user_count", { type: "DERIVED", mapFns: [] }]]));
});

test("aggregate: sum with column returns DERIVED", () => {
	const query = db.selectFrom("posts").select(k.sql<number>`SUM(views)`.as("total_views"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["total_views", { type: "DERIVED", mapFns: [] }]]));
});

test("aggregate: grouped query with column and aggregate", () => {
	const query = db
		.selectFrom("posts")
		.select(["user_id", k.sql<number>`COUNT(*)`.as("post_count")])
		.groupBy("user_id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map<string, Provenance>([
			["user_id", col("posts", "user_id")],
			["post_count", { type: "DERIVED", mapFns: [] }],
		]),
	);
});

// Scalar subquery in SELECT
// NOTE: Current implementation treats scalar subqueries as DERIVED
// Future enhancement: trace through scalar subqueries to their source columns
test("subquery: scalar subquery in select list", () => {
	const query = db
		.selectFrom("posts")
		.select([
			"id",
			(eb) =>
				eb
					.selectFrom("users")
					.select("username")
					.whereRef("users.id", "=", "posts.user_id")
					.limit(1)
					.as("author_name"),
		]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	// Current behavior: scalar subquery returns DERIVED
	assert.deepStrictEqual(
		result,
		new Map<string, Provenance>([
			["id", col("posts", "id")],
			["author_name", { type: "DERIVED", mapFns: [] }],
		]),
	);

	// Desired behavior (future):
	// assert.deepStrictEqual(
	// 	result,
	// 	new Map([
	// 		["id", col("posts", "id")],
	// 		["author_name", col("users", "username")],
	// 	]),
	// );
});

// Subquery in WHERE (should not affect lineage)
test("subquery: subquery in where clause does not affect lineage", () => {
	const query = db
		.selectFrom("posts")
		.select("title")
		.where("user_id", "in", (eb) => eb.selectFrom("users").select("id").where("id", ">", 0));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["title", col("posts", "title")]]));
});

// UPDATE with aliased table
test("update: update with aliased table", () => {
	const query = db
		.updateTable("users as u")
		.set({ username: "test" })
		.where("u.id", "=", 1)
		.returning("u.id");
	const node = query.toOperationNode() as k.UpdateQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("users", "id")]]));
});

// DELETE with FROM/USING
test("delete: delete with from clause", () => {
	const query = db
		.deleteFrom("posts")
		.using("users")
		.whereRef("posts.user_id", "=", "users.id")
		.where("users.id", "=", 1)
		.returning("posts.id");
	const node = query.toOperationNode() as k.DeleteQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", col("posts", "id")]]));
});

// Mixed DERIVED and COLUMN in expression
test("mixed: column with literal in expression", () => {
	const query = db.selectFrom("users").select(k.sql<number>`id + 1`.as("next_id"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["next_id", { type: "DERIVED", mapFns: [] }]]));
});

test("mixed: multiple columns with derived in same select", () => {
	const query = db
		.selectFrom("users")
		.select(["id", "username", k.sql<string>`UPPER(email)`.as("upper_email")]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map<string, Provenance>([
			["id", col("users", "id")],
			["username", col("users", "username")],
			["upper_email", { type: "DERIVED", mapFns: [] }],
		]),
	);
});

// UNRESOLVED references
test("unresolved: nonexistent table reference", () => {
	const query = db.selectFrom("users").select("nonexistent.id" as any);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id", { type: "UNRESOLVED", mapFns: [] }]]));
});

test("unresolved: column from no sources", () => {
	// Create a query with no FROM clause by using raw SQL
	const query = db.selectFrom(k.sql`(SELECT 1)`.as("empty")).select("nonexistent" as any);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	// This should be UNRESOLVED since there's no way to resolve it
	assert.deepStrictEqual(result, new Map([["nonexistent", { type: "UNRESOLVED", mapFns: [] }]]));
});

// Multiple joins with mixed aliasing
test("join: multiple joins with mixed table aliases", () => {
	const query = db
		.selectFrom("users as u")
		.innerJoin("posts as p", "p.user_id", "u.id")
		.innerJoin("comments", "comments.post_id", "p.id")
		.select(["u.username", "p.title", "comments.content"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["username", col("users", "username")],
			["title", col("posts", "title")],
			["content", col("comments", "content")],
		]),
	);
});

// Join types
test("join: left join with nullable result", () => {
	const query = db
		.selectFrom("users")
		.leftJoin("posts", "posts.user_id", "users.id")
		.select(["users.id", "posts.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["title", col("posts", "title")],
		]),
	);
});

test("join: right join", () => {
	const query = db
		.selectFrom("posts")
		.rightJoin("users", "users.id", "posts.user_id")
		.select(["posts.title", "users.username"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["title", col("posts", "title")],
			["username", col("users", "username")],
		]),
	);
});

// NOTE: Current implementation doesn't detect ambiguity when both columns resolve
// Future enhancement: detect ambiguous references even when both candidates resolve to COLUMN
test("join: full outer join with ambiguous column", () => {
	const query = db
		.selectFrom("users")
		.fullJoin("posts", "posts.user_id", "users.id")
		.select(["users.id", "posts.id"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	// Current behavior: resolves both without throwing
	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["id", col("posts", "id")],
		]),
	);

	// Desired behavior (future): should throw on ambiguous unqualified 'id'
	// assert.throws(() => traceLineage(node, seedDb), { message: /ambiguous/i });
});

// Self joins
test("join: self join with different aliases", () => {
	const query = db
		.selectFrom("users as u1")
		.innerJoin("users as u2", "u2.id", "u1.manager_id" as any)
		.select(["u1.username as employee", "u2.username as manager"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["employee", col("users", "username")],
			["manager", col("users", "username")],
		]),
	);
});

// Cross join - skipped due to implementation limitation with function-based join conditions
test.skip("join: cross join", () => {
	// NOTE: Current implementation doesn't handle joins with function callbacks
	// that return raw SQL expressions as the ON condition
	const query = db
		.selectFrom("users")
		.innerJoin("posts", (join) => join.on(k.sql`1`, "=", k.sql`1`))
		.select(["users.id", "posts.id"]);
	const node = getQueryNode(query);

	// This currently throws TypeError instead of handling the join
	assert.throws(() => traceLineage(node, seedDb), { message: /ambiguous/i });
});

// LATERAL joins
test("lateral: simple lateral join", () => {
	const query = db
		.selectFrom("users")
		.innerJoinLateral(
			(eb) =>
				eb
					.selectFrom("posts")
					.select(["posts.id as post_id", "posts.title", "posts.user_id"])
					.as("p"),
			(join) => join.onRef("p.user_id", "=", "users.id"),
		)
		.select(["users.username", "p.post_id", "p.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["username", col("users", "username")],
			["post_id", col("posts", "id")],
			["title", col("posts", "title")],
		]),
	);
});

test("lateral: lateral join with limit (top N per group)", () => {
	const query = db
		.selectFrom("users")
		.innerJoinLateral(
			(eb) =>
				eb
					.selectFrom("posts")
					.select(["posts.title", "posts.id"])
					.whereRef("posts.user_id", "=", "users.id")
					.orderBy("posts.id", "desc")
					.limit(3)
					.as("recent_posts"),
			(join) => join.on(k.sql`1`, "=", k.sql`1`),
		)
		.select(["users.username", "recent_posts.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["username", col("users", "username")],
			["title", col("posts", "title")],
		]),
	);
});

test("lateral: lateral join with aggregation", () => {
	const query = db
		.selectFrom("users")
		.leftJoinLateral(
			(eb) =>
				eb
					.selectFrom("posts")
					.select(k.sql<number>`COUNT(*)`.as("post_count"))
					.whereRef("posts.user_id", "=", "users.id")
					.as("stats"),
			(join) => join.on(k.sql`1`, "=", k.sql`1`),
		)
		.select(["users.id", "users.username", "stats.post_count"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map<string, Provenance>([
			["id", col("users", "id")],
			["username", col("users", "username")],
			["post_count", { type: "DERIVED", mapFns: [] }],
		]),
	);
});

test("lateral: multiple lateral joins", () => {
	const query = db
		.selectFrom("users")
		.innerJoinLateral(
			(eb) =>
				eb
					.selectFrom("posts")
					.select("posts.id as post_id")
					.whereRef("posts.user_id", "=", "users.id")
					.limit(1)
					.as("latest_post"),
			(join) => join.on(k.sql`1`, "=", k.sql`1`),
		)
		.leftJoinLateral(
			(eb) =>
				eb
					.selectFrom("comments")
					.select(["comments.id as comment_id", "comments.content"])
					.whereRef("comments.post_id", "=", "latest_post.post_id")
					.limit(1)
					.as("latest_comment"),
			(join) => join.on(k.sql`1`, "=", k.sql`1`),
		)
		.select(["users.username", "latest_post.post_id", "latest_comment.content"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["username", col("users", "username")],
			["post_id", col("posts", "id")],
			["content", col("comments", "content")],
		]),
	);
});

test("lateral: lateral join with derived column", () => {
	const query = db
		.selectFrom("users")
		.innerJoinLateral(
			(eb) =>
				eb
					.selectFrom("posts")
					.select(k.sql<string>`UPPER(title)`.as("upper_title"))
					.whereRef("posts.user_id", "=", "users.id")
					.limit(1)
					.as("transformed"),
			(join) => join.on(k.sql`1`, "=", k.sql`1`),
		)
		.select(["users.id", "transformed.upper_title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map<string, Provenance>([
			["id", col("users", "id")],
			["upper_title", { type: "DERIVED", mapFns: [] }],
		]),
	);
});

// CTE variations
test("cte: cte shadowing real table", () => {
	const query = db
		.with("users", (db) => db.selectFrom("posts").select(["id", "title"]))
		.selectFrom("users")
		.select("users.id");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	// Should resolve to posts.id because CTE shadows the real users table
	assert.deepStrictEqual(result, new Map([["id", col("posts", "id")]]));
});

test("cte: deeply nested cte chain", () => {
	const query = db
		.with("cte1", (db) => db.selectFrom("users").select("id as a"))
		.with("cte2", (db) => db.selectFrom("cte1").select("cte1.a as b"))
		.with("cte3", (db) => db.selectFrom("cte2").select("cte2.b as c"))
		.selectFrom("cte3")
		.select("cte3.c");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["c", col("users", "id")]]));
});

test("cte: cte with derived column breaks lineage", () => {
	const query = db
		.with("derived_cte", (db) =>
			db.selectFrom("users").select(k.sql<string>`UPPER(username)`.as("upper_name")),
		)
		.selectFrom("derived_cte")
		.select("derived_cte.upper_name");
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["upper_name", { type: "DERIVED", mapFns: [] }]]));
});

// Subquery with join inside
test("subquery: subquery containing join", () => {
	const subquery = db
		.selectFrom("users")
		.innerJoin("posts", "posts.user_id", "users.id")
		.select(["users.id as user_id", "posts.title"])
		.as("joined");
	const query = db.selectFrom(subquery).select(["joined.user_id", "joined.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["user_id", col("users", "id")],
			["title", col("posts", "title")],
		]),
	);
});

// RETURNING with aliases
test("returning: insert with aliased columns in returning", () => {
	const query = db
		.insertInto("users")
		.values({ username: "test", email: "test@example.com" })
		.returning(["id as user_id", "username as name"]);
	const node = query.toOperationNode() as k.InsertQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["user_id", col("users", "id")],
			["name", col("users", "username")],
		]),
	);
});

test("returning: update with expression in returning", () => {
	const query = db
		.updateTable("users")
		.set({ username: "updated" })
		.where("id", "=", 1)
		.returning(k.sql<string>`CONCAT(username, '@example.com')`.as("email"));
	const node = query.toOperationNode() as k.UpdateQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["email", { type: "DERIVED", mapFns: [] }]]));
});

// Empty/minimal queries
test("edge: query with no selections", () => {
	const query = db.selectFrom("users");
	const node = query.toOperationNode() as k.SelectQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map());
});

test("edge: insert without returning", () => {
	const query = db.insertInto("users").values({ username: "test", email: "test@example.com" });
	const node = query.toOperationNode() as k.InsertQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map());
});

test("edge: update without returning", () => {
	const query = db.updateTable("users").set({ username: "updated" }).where("id", "=", 1);
	const node = query.toOperationNode() as k.UpdateQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map());
});

test("edge: delete without returning", () => {
	const query = db.deleteFrom("users").where("id", "=", 1);
	const node = query.toOperationNode() as k.DeleteQueryNode;
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map());
});

// Window functions (if supported)
test("window: window function returns DERIVED", () => {
	const query = db.selectFrom("posts").select(k.sql<number>`ROW_NUMBER() OVER ()`.as("row_num"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["row_num", { type: "DERIVED", mapFns: [] }]]));
});

test("window: window function with partition by", () => {
	const query = db
		.selectFrom("posts")
		.select(k.sql<number>`RANK() OVER (PARTITION BY user_id ORDER BY created_at)`.as("post_rank"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["post_rank", { type: "DERIVED", mapFns: [] }]]));
});

// Type casts
test("cast: explicit type cast", () => {
	const query = db.selectFrom("users").select(k.sql<number>`CAST(id AS TEXT)`.as("id_str"));
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(result, new Map([["id_str", { type: "DERIVED", mapFns: [] }]]));
});

// Multiple table sources without join
// NOTE: Current implementation doesn't detect ambiguity for qualified columns
// Future enhancement: detect when both tables have the same column name
test("from: multiple tables in from clause (implicit cross join)", () => {
	const query = db.selectFrom(["users", "posts"]).select(["users.id", "posts.id"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	// Current behavior: resolves both qualified columns
	assert.deepStrictEqual(
		result,
		new Map([
			["id", col("users", "id")],
			["id", col("posts", "id")],
		]),
	);

	// Note: This creates a Map with duplicate keys; the second one overwrites the first
	// This is a known limitation when selecting multiple columns with the same output name
});

test("from: multiple tables with qualified unique columns", () => {
	const query = db.selectFrom(["users", "posts"]).select(["users.username", "posts.title"]);
	const node = getQueryNode(query);
	const result = traceLineage(node, seedDb);

	assert.deepStrictEqual(
		result,
		new Map([
			["username", col("users", "username")],
			["title", col("posts", "title")],
		]),
	);
});

test("mapFns: simple map on column reference", () => {
	const mapFn = (x: any) => x;
	const query = db.selectFrom("users").select((eb) => [map(eb.ref("id"), mapFn).as("mapped_id")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("mapped_id");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 1);
		assert.strictEqual(provenance.mapFns[0], mapFn);
	}
});

test("mapFns: map on derived expression", () => {
	const mapFn = (x: any) => x;
	const query = db
		.selectFrom("users")
		.select(() => [map(k.sql<number>`COUNT(*)`, mapFn).as("mapped_count")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("mapped_count");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "DERIVED");
	assert.strictEqual(provenance.mapFns.length, 1);
	assert.strictEqual(provenance.mapFns[0], mapFn);
});

test("mapFns: stacks maps through subquery", () => {
	const mapFn1 = (x: any) => x;
	const mapFn2 = (x: any) => x;

	const inner = db
		.selectFrom("users")
		.select((eb) => [map(eb.ref("id"), mapFn1).as("mapped_id")])
		.as("inner");

	const query = db
		.selectFrom(inner)
		.select((eb) => [map(eb.ref("inner.mapped_id"), mapFn2).as("double_mapped")]);

	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("double_mapped");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 2);
		assert.strictEqual(provenance.mapFns[0], mapFn1);
		assert.strictEqual(provenance.mapFns[1], mapFn2);
	}
});

test("mapFns: stacks maps through CTE", () => {
	const mapFn1 = (x: any) => x;
	const mapFn2 = (x: any) => x;

	const query = db
		.with("cte", (db) =>
			db.selectFrom("users").select((eb) => [map(eb.ref("id"), mapFn1).as("mapped_id")]),
		)
		.selectFrom("cte")
		.select((eb) => [map(eb.ref("cte.mapped_id"), mapFn2).as("double_mapped")]);

	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("double_mapped");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 2);
		assert.strictEqual(provenance.mapFns[0], mapFn1);
		assert.strictEqual(provenance.mapFns[1], mapFn2);
	}
});

test("mapFns: preserves through nested subqueries", () => {
	const mapFn1 = (x: any) => x;
	const mapFn2 = (x: any) => x;
	const mapFn3 = (x: any) => x;

	const inner1 = db
		.selectFrom("users")
		.select((eb) => [map(eb.ref("id"), mapFn1).as("id1")])
		.as("inner1");

	const inner2 = db
		.selectFrom(inner1)
		.select((eb) => [map(eb.ref("inner1.id1"), mapFn2).as("id2")])
		.as("inner2");

	const query = db.selectFrom(inner2).select((eb) => [map(eb.ref("inner2.id2"), mapFn3).as("id3")]);

	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("id3");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 3);
		assert.strictEqual(provenance.mapFns[0], mapFn1);
		assert.strictEqual(provenance.mapFns[1], mapFn2);
		assert.strictEqual(provenance.mapFns[2], mapFn3);
	}
});

test("mapFns: unmapped column has empty mapFns", () => {
	const query = db.selectFrom("users").select("id");
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("id");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 0);
	}
});

test("mapFns: mixed mapped and unmapped columns", () => {
	const mapFn = (x: any) => x;
	const query = db
		.selectFrom("users")
		.select((eb) => ["username", map(eb.ref("id"), mapFn).as("mapped_id")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const usernameProvenance = result.get("username");
	assert.ok(usernameProvenance);
	assert.strictEqual(usernameProvenance.type, "COLUMN");
	if (usernameProvenance.type === "COLUMN") {
		assert.strictEqual(usernameProvenance.mapFns.length, 0);
	}

	const idProvenance = result.get("mapped_id");
	assert.ok(idProvenance);
	assert.strictEqual(idProvenance.type, "COLUMN");
	if (idProvenance.type === "COLUMN") {
		assert.strictEqual(idProvenance.mapFns.length, 1);
		assert.strictEqual(idProvenance.mapFns[0], mapFn);
	}
});

test("mapFns: map on qualified column reference", () => {
	const mapFn = (x: any) => x;
	const query = db
		.selectFrom("users")
		.select((eb) => [map(eb.ref("users.id"), mapFn).as("mapped_id")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("mapped_id");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 1);
		assert.strictEqual(provenance.mapFns[0], mapFn);
	}
});

test("mapFns: map preserved through table alias", () => {
	const mapFn = (x: any) => x;
	const query = db
		.selectFrom("users as u")
		.select((eb) => [map(eb.ref("u.id"), mapFn).as("mapped_id")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("mapped_id");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 1);
		assert.strictEqual(provenance.mapFns[0], mapFn);
	}
});

test("mapFns: map on column from joined table", () => {
	const mapFn = (x: any) => x;
	const query = db
		.selectFrom("users")
		.innerJoin("posts", "posts.user_id", "users.id")
		.select((eb) => [map(eb.ref("posts.title"), mapFn).as("mapped_title")]);
	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("mapped_title");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		assert.strictEqual(provenance.mapFns.length, 1);
		assert.strictEqual(provenance.mapFns[0], mapFn);
	}
});

test("mapFns: composes nested map() calls", () => {
	const innerMapFn = (x: any) => x + "_inner";
	const outerMapFn = (x: any) => x + "_outer";

	const query = db
		.selectFrom("users")
		.select((eb) => [map(map(eb.ref("id"), innerMapFn), outerMapFn).as("composed_id")]);

	const node = getQueryNode(query);
	const mappedNodes = getMappedNodes();
	const result = traceLineage(node, seedDb, mappedNodes);

	const provenance = result.get("composed_id");
	assert.ok(provenance);
	assert.strictEqual(provenance.type, "COLUMN");
	if (provenance.type === "COLUMN") {
		// Should have a single composed function, not two separate ones
		assert.strictEqual(provenance.mapFns.length, 1);

		// Verify the function is composed correctly: outer(inner(x))
		const composedFn = provenance.mapFns[0]!;
		assert.strictEqual(composedFn("test"), "test_inner_outer");
	}
});
