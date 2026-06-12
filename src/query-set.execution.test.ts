import assert from "node:assert";
import { describe, test } from "node:test";

import { NoResultError } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Execution-Method Semantics
//
// What each execution method returns for empty results, and how
// executeCount / executeExists relate to pagination, joins, and the keyBy
// contract. Happy-path execution lives in query-set.core.test.ts and (per
// join type) in query-set.joins.test.ts.
//

describe("query-set: execution", () => {
	//
	// Empty result sets
	//

	test("empty result: execute returns empty array", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	test("empty result: executeTakeFirst returns undefined", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeTakeFirst();

		assert.strictEqual(user, undefined);
	});

	test("empty result: executeTakeFirstOrThrow throws NoResultError", async () => {
		await assert.rejects(async () => {
			await querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "=", 999)
				.executeTakeFirstOrThrow();
		}, NoResultError);
	});

	test("empty result: executeCount returns 0", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeCount(Number);

		assert.strictEqual(count, 0);
	});

	test("empty result: executeExists returns false", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 999)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	test("empty result: execute returns empty array with a cardinality-one join", async () => {
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

	test("empty result: execute returns empty array with a many-join", async () => {
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

	//
	// executeCount / executeExists ignore pagination
	//

	test("executeCount: ignores limit/offset", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.limit(3)
			.offset(2)
			.executeCount(Number);

		// Should count all 10 users, not just the 3 in the page
		assert.strictEqual(count, 10);
	});

	test("executeCount: with joins ignores limit/offset", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.limit(1)
			.executeCount(Number);

		// Should count 2 users (users 2 and 3 have posts), not just 1
		assert.strictEqual(count, 2);
	});

	test("executeExists: ignores limit/offset", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.limit(1)
			.offset(100) // Past all 10 users: the page itself is empty
			.executeExists();

		// Should return true: matching records exist even though the
		// offset(100) page contains none of them
		assert.strictEqual(exists, true);
	});

	test("executeExists: ignores limit(0)", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 5)
			.limit(0) // execute() would return no rows (limit(0) is honored)
			.executeExists();

		// Should check existence regardless of limit
		assert.strictEqual(exists, true);
	});

	//
	// executeCount semantics
	//

	test("executeCount: counts base rows, not entities, when keyBy is not unique", async () => {
		// keyBy is required to be a unique key of the base query.  Hydration
		// defensively collapses duplicate keys (see query-set.core.test.ts), but
		// counting is a plain COUNT(*) with no DISTINCT, so a non-unique keyBy
		// overcounts relative to execute().  This is intentional: adding DISTINCT
		// would slow down every well-formed count to accommodate a contract
		// violation.
		const qs = () =>
			querySet(db).selectAs("post", db.selectFrom("posts").select(["user_id"]), "user_id");

		const count = await qs().executeCount(Number);
		const entities = await qs().execute();

		assert.strictEqual(entities.length, 9); // Distinct post authors.
		assert.strictEqual(count, 15); // Total post rows.
	});

	test("executeCount: counts base records through deeply nested many-joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 4);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();
		const joinedRows = await qs.toJoinedQuery().execute();

		// Users whose posts have comments: bob (2), carol (3), dave (4)
		assert.strictEqual(count, 3);
		assert.strictEqual(users.length, 3); // Verify count matches execute
		assert.ok(joinedRows.length > users.length); // Row explosion in joined query
	});
});
