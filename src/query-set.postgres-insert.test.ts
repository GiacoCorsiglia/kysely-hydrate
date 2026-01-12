import assert from "node:assert";
import { describe, test } from "node:test";

import * as k from "kysely";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();
const shouldSkip = dialect !== "postgres";

//
// Tests
//

describe("query-set: postgres-insert", { skip: shouldSkip }, () => {
	//
	// Test 1: Simple insert with returningAll()
	//

	test("insertAs() - simple insert with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({
						username: "newUserName",
						email: "new@example.com",
					})
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				username: "newUserName",
				email: "new@example.com",
			});
		});
	});

	//
	// Test 2: Insert with partial returning
	//

	test("insertAs() - insert with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.insertAs("newUser", (db) =>
					db
						.insertInto("users")
						.values({
							username: "partialUser",
							email: "partial@example.com",
						})
						.returningAll(),
				)
				.omit(["id"]); // Omit id from the result

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				username: "partialUser",
				email: "partial@example.com",
			});
		});
	});

	//
	// Test 3: Insert with custom keyBy
	//

	test("insertAs() - insert with custom keyBy", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).insertAs(
				"newUser",
				(db) =>
					db
						.insertInto("users")
						.values({
							username: "customKeyUser",
							email: "customkey@example.com",
						})
						.returningAll(),
				"username",
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				username: "customKeyUser",
				email: "customkey@example.com",
			});
		});
	});

	//
	// Test 4: Insert multiple rows with ordering
	//

	test("insertAs() - insert multiple rows with ordering", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.insertAs("newUsers", (db) =>
					db
						.insertInto("users")
						.values([
							{ username: "user1", email: "user1@example.com" },
							{ username: "user2", email: "user2@example.com" },
							{ username: "user3", email: "user3@example.com" },
						])
						.returningAll(),
				)
				.orderBy("username");

			const results = await query.execute();

			assert.strictEqual(results.length, 3);

			for (const result of results) {
				assert.ok(typeof result.id === "number");
				delete (result as any).id;
			}

			assert.deepStrictEqual(results, [
				{ username: "user1", email: "user1@example.com" },
				{ username: "user2", email: "user2@example.com" },
				{ username: "user3", email: "user3@example.com" },
			]);
		});
	});

	//
	// Test 5: Insert on QuerySet without joins
	//

	test("QuerySet.insert() - without joins", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username", "email"]))
				.insert(
					trx
						.insertInto("users")
						.values({
							username: "noJoinUser",
							email: "nojoin@example.com",
						})
						.returningAll(),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				username: "noJoinUser",
				email: "nojoin@example.com",
			});
		});
	});

	//
	// Test 6: Insert with has-one join (leftJoinOne)
	//

	test("QuerySet.insert() - with has-one join (leftJoinOne)", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinOne(
					"user",
					({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
					"user.id",
					"posts.user_id",
				)
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 1,
							title: "Join Test Post",
							content: "Content for join test",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "Join Test Post",
				user: {
					id: 1,
					username: "alice",
				},
			});
		});
	});

	//
	// Test 7: Insert with has-many join (leftJoinMany)
	//

	test("QuerySet.insert() - with has-many join (leftJoinMany)", async () => {
		await testInTransaction(db, async (trx) => {
			// Insert a user with existing posts
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username"]))
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"users.id",
				)
				.where("users.id", "=", 2) // Bob has 4 existing posts
				.insert(
					trx
						.insertInto("users")
						.values({
							username: "userWithPosts",
							email: "withposts@example.com",
						})
						.returningAll(),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			// New user has no posts, so array should be empty
			assert.deepStrictEqual(result, {
				username: "userWithPosts",
				email: "withposts@example.com",
				posts: [],
			});
		});
	});

	//
	// Test 8: Insert with nested joins
	//

	test("QuerySet.insert() - with nested joins", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinOne(
					"user",
					({ eb, qs }) =>
						qs(eb.selectFrom("users").select(["id", "username"])).leftJoinOne(
							"profile",
							({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
							"profile.user_id",
							"user.id",
						),
					"user.id",
					"posts.user_id",
				)
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 1, // Alice has a profile
							title: "Nested Join Post",
							content: "Content with nested joins",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "Nested Join Post",
				user: {
					id: 1,
					username: "alice",
					profile: {
						id: 1,
						bio: "Bio for user 1",
						user_id: 1,
					},
				},
			});
		});
	});

	//
	// Test 9: Insert with .extras()
	//

	test("QuerySet.insert() - with .extras() at root level", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.extras({
					upperTitle: (row) => row.title.toUpperCase(),
					titleLength: (row) => row.title.length,
				})
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 1,
							title: "extras test",
							content: "content for extras",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "extras test",
				upperTitle: "EXTRAS TEST",
				titleLength: 11,
			});
		});
	});

	//
	// Test 10: Insert with nested extras in joins
	//

	test("QuerySet.insert() - with nested extras in joins", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinOne(
					"user",
					({ eb, qs }) =>
						qs(eb.selectFrom("users").select(["id", "username"])).extras({
							usernameUpper: (row) => row.username.toUpperCase(),
						}),
					"user.id",
					"posts.user_id",
				)
				.extras({
					titleLower: (row) => row.title.toLowerCase(),
				})
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 1,
							title: "NESTED EXTRAS TEST",
							content: "content",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "NESTED EXTRAS TEST",
				titleLower: "nested extras test",
				user: {
					id: 1,
					username: "alice",
					usernameUpper: "ALICE",
				},
			});
		});
	});

	//
	// Test 11: Insert with no results (should return undefined)
	//

	test("insertAs() - with no results returns undefined", async () => {
		await testInTransaction(db, async (trx) => {
			// Use INSERT...SELECT with a WHERE clause that matches nothing
			const query = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.columns(["username", "email"])
					.expression(
						(eb) =>
							eb
								.selectFrom("users")
								.select([
									k.sql<string>`'conditionalUser'`.as("username"),
									k.sql<string>`'conditional@example.com'`.as("email"),
								])
								.where("id", "=", 9999), // Matches nothing, so no rows inserted
					)
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.strictEqual(result, undefined);
		});
	});

	//
	// Test 12: Insert with factory function form
	//

	test("insertAs() - with factory function form", async () => {
		await testInTransaction(db, async (trx) => {
			// Test both the factory function form and direct query form
			const query1 = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({
						username: "factoryUser1",
						email: "factory1@example.com",
					})
					.returningAll(),
			);

			const query2 = querySet(trx).insertAs(
				"newUser",
				trx
					.insertInto("users")
					.values({
						username: "factoryUser2",
						email: "factory2@example.com",
					})
					.returningAll(),
			);

			const result1 = await query1.executeTakeFirst();
			const result2 = await query2.executeTakeFirst();

			assert.ok(result1 && result2);
			assert.ok(typeof result1.id === "number");
			assert.ok(typeof result2.id === "number");

			delete (result1 as any).id;
			delete (result2 as any).id;

			// Both should have the same shape
			assert.deepStrictEqual(result1, {
				username: "factoryUser1",
				email: "factory1@example.com",
			});

			assert.deepStrictEqual(result2, {
				username: "factoryUser2",
				email: "factory2@example.com",
			});
		});
	});
});
