/**
 * PostgreSQL kitchen sink tests for QuerySet API.
 *
 * These tests exercise complex SQL generation scenarios to ensure PostgreSQL
 * doesn't choke on any SQL we generate. Focus is on execution success rather
 * than hydration correctness (which is tested elsewhere).
 *
 * To run these tests:
 *   RUN_POSTGRES_TESTS=true npm test -- src/query-set.postgres.test.ts
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/postgres.ts";
import { querySet } from "./query-set.ts";

// Skip all tests if not explicitly enabled
const shouldRun = process.env.POSTGRES_URL || process.env.RUN_POSTGRES_TESTS;

describe("PostgreSQL QuerySet kitchen sink tests", { skip: !shouldRun }, () => {
	const db = getDbForTest();

	//
	// Basic Execution - Verify all query types execute without errors
	//

	test("kitchen sink: execute() with no joins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.execute();

		assert.ok(Array.isArray(users));
		assert.strictEqual(users.length, 3);
	});

	test("kitchen sink: executeTakeFirst() with no joins", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.executeTakeFirst();

		assert.ok(user);
		assert.strictEqual(user.id, 1);
	});

	test("kitchen sink: executeCount() with no joins", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 5)
			.executeCount(Number);

		assert.strictEqual(count, 5);
	});

	test("kitchen sink: executeExists() with no joins", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	//
	// Cardinality-One Joins
	//

	test("kitchen sink: innerJoinOne execute", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 3);
		assert.ok(users[0]?.profile);
	});

	test("kitchen sink: innerJoinOne executeCount", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.executeCount(Number);

		assert.strictEqual(count, 3);
	});

	test("kitchen sink: leftJoinOne execute", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.leftJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 3);
	});

	test("kitchen sink: leftJoinOne executeCount", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.executeCount(Number);

		assert.strictEqual(count, 10);
	});

	//
	// Cardinality-Many Joins
	//

	test("kitchen sink: innerJoinMany execute", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 2);
		assert.ok(Array.isArray(users[0]?.posts));
	});

	test("kitchen sink: innerJoinMany executeCount", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.executeCount(Number);

		// Should count unique users, not exploded rows
		assert.strictEqual(count, 2);
	});

	test("kitchen sink: innerJoinMany toJoinedQuery execute", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		// Should return flat rows with row explosion
		assert.ok(rows.length > 0);
		assert.ok("posts$$id" in rows[0]!);
	});

	test("kitchen sink: leftJoinMany execute", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 2);
		// User 1 (alice) has no posts
		assert.deepStrictEqual(users[0]?.posts, []);
		// User 2 (bob) has posts
		assert.ok(users[1]!.posts.length > 0);
	});

	test("kitchen sink: crossJoinMany execute", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 2)
			.crossJoinMany("allPosts", (nest) =>
				nest((eb) =>
					eb.selectFrom("posts").select(["id", "title"]).where("user_id", "=", 3).limit(2),
				),
			)
			.execute();

		assert.strictEqual(users.length, 2);
		// Both users should have the same posts (cartesian product)
		assert.strictEqual(users[0]?.allPosts.length, 2);
		assert.strictEqual(users[1]?.allPosts.length, 2);
	});

	//
	// Pagination
	//

	test("kitchen sink: pagination with no joins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.limit(3)
			.offset(2)
			.execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(users[0]?.id, 3);
	});

	test("kitchen sink: pagination with innerJoinOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.limit(2)
			.offset(1)
			.execute();

		assert.strictEqual(users.length, 2);
		assert.ok(users[0]?.profile);
	});

	test("kitchen sink: pagination with innerJoinMany", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(2)
			.execute();

		// Should limit base users, each with ALL their posts
		assert.strictEqual(users.length, 2);
		assert.ok(users[0]!.posts.length > 0);
	});

	test("kitchen sink: pagination with mixed joins", async () => {
		const users = await querySet(db)
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
			.limit(1)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.ok(users[0]?.profile);
		assert.ok(users[0]?.posts.length > 0);
	});

	//
	// Nested Joins (Multi-level)
	//

	test("kitchen sink: 3-level nesting users → posts → comments", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) =>
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						(init2) =>
							init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.ok(users[0]!.posts.length > 0);
		assert.ok(users[0]!.posts[0]!.comments.length > 0);
	});

	test("kitchen sink: 3-level nesting with pagination", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						(init2) =>
							init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.limit(1)
			.execute();

		// Should return first user with ALL their posts and comments
		assert.strictEqual(users.length, 1);
		assert.ok(users[0]!.posts.length > 0);
	});

	test("kitchen sink: 4-level nesting with mixed cardinality", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) =>
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).innerJoinMany(
						"comments",
						(init2) =>
							init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.ok(users[0]!.profile);
		assert.ok(users[0]!.posts.length > 0);
		assert.ok(users[0]!.posts[0]!.comments.length > 0);
	});

	//
	// Complex Counting Scenarios
	//

	test("kitchen sink: executeCount with nested innerJoinMany", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 4)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						(init2) =>
							init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.executeCount(Number);

		// Should count unique users with posts that have comments
		assert.ok(count >= 2);
	});

	test("kitchen sink: executeCount with mixed join types", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 5)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinOne(
				"profile2",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile2.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.leftJoinMany(
				"allPosts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"allPosts.user_id",
				"user.id",
			)
			.crossJoinMany("crossPosts", (nest) =>
				nest((eb) =>
					eb.selectFrom("posts").select(["id", "title"]).where("user_id", "=", 3).limit(1),
				),
			)
			.executeCount(Number);

		// Correct behavior:
		// - innerJoinOne: included as inner join
		// - leftJoinOne: included as left join
		// - innerJoinMany: converted to WHERE EXISTS
		// - leftJoinMany: omitted entirely
		// - crossJoinMany: converted to WHERE EXISTS
		assert.ok(count >= 2);
	});

	test("kitchen sink: executeCount ignores limit and offset", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 5)
			.limit(2)
			.offset(1)
			.executeCount(Number);

		// Should count all matching records, ignoring pagination
		assert.strictEqual(count, 5);
	});

	//
	// Exists Queries
	//

	test("kitchen sink: executeExists with simple query", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("kitchen sink: executeExists with joins", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
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
			.executeExists();

		assert.strictEqual(exists, true);
	});

	test("kitchen sink: executeExists ignores limit", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 5)
			.limit(0) // Would normally return no results
			.executeExists();

		// Should check existence regardless of limit
		assert.strictEqual(exists, true);
	});

	//
	// Query Compilation Methods
	//

	test("kitchen sink: toBaseQuery strips all joins", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
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
			.toBaseQuery()
			.execute();

		// Should only have base columns, no joins
		assert.strictEqual(rows.length, 3);
		assert.ok("id" in rows[0]!);
		assert.ok(!("profile" in rows[0]!));
		assert.ok(!("posts" in rows[0]!));
	});

	test("kitchen sink: toJoinedQuery returns flat rows with prefixes", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) =>
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					),
				"posts.user_id",
				"user.id",
			)
			.toJoinedQuery()
			.execute();

		// Should have prefixed columns with row explosion
		assert.ok(rows.length > 0);
		assert.ok("profile$$id" in rows[0]!);
		assert.ok("posts$$id" in rows[0]!);
	});

	test("kitchen sink: toQuery vs toJoinedQuery without pagination", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) =>
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					),
				"posts.user_id",
				"user.id",
			);

		const toQueryRows = await qs.toQuery().execute();
		const toJoinedQueryRows = await qs.toJoinedQuery().execute();

		// Without pagination, both should be identical (flat rows)
		assert.strictEqual(toQueryRows.length, toJoinedQueryRows.length);
	});

	test("kitchen sink: toQuery vs toJoinedQuery with pagination differ", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(1);

		const toQueryRows = await qs.toQuery().execute();
		const toJoinedQueryRows = await qs.toJoinedQuery().execute();

		// toQuery applies limit to base records (with nested subquery)
		// toJoinedQuery is raw view without limit/offset
		// So they should differ (or be same if first user has exactly limit rows)
		assert.ok(Array.isArray(toQueryRows));
		assert.ok(Array.isArray(toJoinedQueryRows));
	});

	//
	// Edge Cases
	//

	test("kitchen sink: empty result set with complex joins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						(init2) =>
							init2((eb) => eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	test("kitchen sink: empty result set executeCount", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeCount(Number);

		assert.strictEqual(count, 0);
	});

	test("kitchen sink: empty result set executeExists", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	//
	// Multiple Sibling Collections
	//

	test("kitchen sink: multiple sibling many-joins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.leftJoinMany(
				"allPosts",
				(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"allPosts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.ok(users[0]!.posts.length > 0);
		assert.ok(users[0]!.allPosts.length > 0);
	});

	test("kitchen sink: multiple sibling one-joins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinOne(
				"profile2",
				(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile2.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(users.length, 1);
		assert.ok(users[0]?.profile);
		assert.ok(users[0]?.profile2);
	});

	//
	// Complex WHERE conditions with joins
	//

	test("kitchen sink: complex nested WHERE conditions", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", ">=", 1)
			.where("users.id", "<=", 5)
			.innerJoinMany(
				"posts",
				(nest) =>
					nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).where(
						"posts.id",
						"<=",
						10,
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// Should execute without errors
		assert.ok(Array.isArray(users));
	});
});
