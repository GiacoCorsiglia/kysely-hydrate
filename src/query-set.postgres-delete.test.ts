import assert from "node:assert";
import { describe, test } from "node:test";

import { testInTransaction } from "./__tests__/helpers.ts";
import { getDbForTest } from "./__tests__/postgres.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();
const shouldSkip = !process.env.RUN_POSTGRES_TESTS;

//
// Tests
//

describe("Postgres: DELETE operations", { skip: shouldSkip }, () => {
	//
	// Test 26: Simple delete with returningAll()
	//

	test("deleteAs() - simple delete with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 1).returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 1,
				username: "alice",
				email: "alice@example.com",
			});
		});
	});

	//
	// Test 27: Delete with partial returning
	//

	test("deleteAs() - delete with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.deleteAs("deletedUser", (db) => db.deleteFrom("users").where("id", "=", 2).returningAll())
				.omit(["id"]); // Omit id from the result

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				username: "bob",
				email: "bob@example.com",
			});
		});
	});

	//
	// Test 28: Delete with custom keyBy
	//

	test("deleteAs() - delete with custom keyBy", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs(
				"deletedUser",
				(db) => db.deleteFrom("users").where("id", "=", 3).returningAll(),
				"username",
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.username, "carol");
			assert.deepStrictEqual(result, {
				id: 3,
				username: "carol",
				email: "carol@example.com",
			});
		});
	});

	//
	// Test 29: Delete multiple rows with ordering
	//

	test("deleteAs() - delete multiple rows with ordering", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.deleteAs("deletedUsers", (db) =>
					db.deleteFrom("users").where("id", "in", [4, 5, 6]).returningAll(),
				)
				.orderBy("username");

			const results = await query.execute();

			assert.strictEqual(results.length, 3);
			assert.deepStrictEqual(results, [
				{ id: 4, username: "dave", email: "dave@example.com" },
				{ id: 5, username: "eve", email: "eve@example.com" },
				{ id: 6, username: "frank", email: "frank@example.com" },
			]);
		});
	});

	//
	// Test 30: Delete on QuerySet without joins
	//

	test("QuerySet.delete() - without joins", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username", "email"]))
				.delete(trx.deleteFrom("users").where("id", "=", 7).returningAll());

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 7,
				username: "grace",
				email: "grace@example.com",
			});
		});
	});

	//
	// Test 31: Delete with has-one join (leftJoinOne)
	//

	test("QuerySet.delete() - with has-one join (leftJoinOne)", async () => {
		await testInTransaction(db, async (trx) => {
			// Delete a post and hydrate its user
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinOne(
					"user",
					({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
					"user.id",
					"posts.user_id",
				)
				.delete(trx.deleteFrom("posts").where("id", "=", 1).returning(["id", "user_id", "title"]));

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 1,
				user_id: 2,
				title: "Post 1",
				user: {
					id: 2,
					username: "bob",
				},
			});
		});
	});

	//
	// Test 32: Delete with has-many join (leftJoinMany)
	//

	test("QuerySet.delete() - with has-many join (leftJoinMany)", async () => {
		await testInTransaction(db, async (trx) => {
			// Delete a user and hydrate their posts
			// Note: Since we have ON DELETE CASCADE, we need to be careful
			// Let's delete a user who has posts
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username", "email"]))
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"users.id",
				)
				.delete(trx.deleteFrom("users").where("id", "=", 2).returningAll()); // Bob has 4 posts

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 2);
			assert.strictEqual(result.username, "bob");
			assert.strictEqual(result.email, "bob@example.com");
			// Bob should have 4 posts in the result (before cascade delete)
			assert.strictEqual(result.posts.length, 4);
			assert.ok(result.posts.every((p: any) => p.user_id === 2));
		});
	});

	//
	// Test 33: Delete with nested joins
	//

	test("QuerySet.delete() - with nested joins", async () => {
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
				.delete(trx.deleteFrom("posts").where("id", "=", 2).returning(["id", "user_id", "title"]));

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 2,
				user_id: 2,
				title: "Post 2",
				user: {
					id: 2,
					username: "bob",
					profile: {
						id: 2,
						bio: "Bio for user 2",
						user_id: 2,
					},
				},
			});
		});
	});

	//
	// Test 34: Delete with .extras()
	//

	test("QuerySet.delete() - with .extras() at root level", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.extras({
					upperTitle: (row) => row.title.toUpperCase(),
					titleLength: (row) => row.title.length,
				})
				.delete(trx.deleteFrom("posts").where("id", "=", 3).returning(["id", "user_id", "title"]));

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 3,
				user_id: 3,
				title: "Post 3",
				upperTitle: "POST 3",
				titleLength: 6,
			});
		});
	});

	//
	// Test 35: Delete with nested extras in joins
	//

	test("QuerySet.delete() - with nested extras in joins", async () => {
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
				.delete(trx.deleteFrom("posts").where("id", "=", 4).returning(["id", "user_id", "title"]));

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 4,
				user_id: 4,
				title: "Post 4",
				titleLower: "post 4",
				user: {
					id: 4,
					username: "dave",
					usernameUpper: "DAVE",
				},
			});
		});
	});

	//
	// Test 36: Delete returning no rows
	//

	test("deleteAs() - returning no rows returns undefined", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 9999).returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.strictEqual(result, undefined);
		});
	});

	//
	// Test 37: Delete with factory function
	//

	test("deleteAs() - with factory function form", async () => {
		await testInTransaction(db, async (trx) => {
			// Test both the factory function form and direct query form
			const query1 = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 8).returningAll(),
			);

			const query2 = querySet(trx).deleteAs(
				"deletedUser",
				trx.deleteFrom("users").where("id", "=", 9).returningAll(),
			);

			const result1 = await query1.executeTakeFirst();
			const result2 = await query2.executeTakeFirst();

			assert.ok(result1 && result2);

			// Both should have the same shape
			assert.deepStrictEqual(result1, {
				id: 8,
				username: "heidi",
				email: "heidi@example.com",
			});

			assert.deepStrictEqual(result2, {
				id: 9,
				username: "ivan",
				email: "ivan@example.com",
			});
		});
	});
});
