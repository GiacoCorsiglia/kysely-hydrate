import assert from "node:assert";
import { test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Tests
//

describePg("query-set: postgres-update", () => {
	//
	// Test 13: Simple update with returningAll()
	//

	test("updateAs() - simple update with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ username: "updatedName", email: "updated@example.com" })
					.where("id", "=", 1)
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 1);
			assert.deepStrictEqual(result, {
				id: 1,
				username: "updatedName",
				email: "updated@example.com",
			});
		});
	});

	//
	// Test 14: Update with partial returning
	//

	test("updateAs() - update with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			// A genuine partial RETURNING list (email exists but is not returned)
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ username: "partialUpdate" })
					.where("id", "=", 2)
					.returning(["id", "username"]),
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 2,
				username: "partialUpdate",
			});
		});
	});

	//
	// Test 15: Update with custom keyBy
	//

	test("updateAs() - update with custom keyBy orders results by that key", async () => {
		await testInTransaction(db, async (trx) => {
			// Keyed by title; "Post 12" < "Post 2" < "Post 3" alphabetically, so
			// key-ordering by title is distinguishable from the default id order
			// ([2, 3, 12]). A no-op keyBy would produce a different order.
			const query = querySet(trx).updateAs(
				"updatedPosts",
				(db) =>
					db
						.updateTable("posts")
						.set({ content: "Updated content" })
						.where("id", "in", [2, 3, 12])
						.returningAll(),
				"title",
			);

			const results = await query.execute();

			assert.deepStrictEqual(results, [
				{ id: 12, user_id: 2, title: "Post 12", content: "Updated content" },
				{ id: 2, user_id: 2, title: "Post 2", content: "Updated content" },
				{ id: 3, user_id: 3, title: "Post 3", content: "Updated content" },
			]);
		});
	});

	//
	// Test 16: Update multiple rows with ordering
	//

	test("updateAs() - update multiple rows with ordering", async () => {
		await testInTransaction(db, async (trx) => {
			// "Post 1" < "Post 10" < "Post 2" alphabetically, so ordering by title
			// is distinguishable from id order ([1, 2, 10])
			const query = querySet(trx)
				.updateAs("updatedPosts", (db) =>
					db
						.updateTable("posts")
						.set({ content: "Bulk updated" })
						.where("id", "in", [1, 2, 10])
						.returningAll(),
				)
				.orderBy("title");

			const results = await query.execute();

			assert.deepStrictEqual(results, [
				{ id: 1, user_id: 2, title: "Post 1", content: "Bulk updated" },
				{ id: 10, user_id: 9, title: "Post 10", content: "Bulk updated" },
				{ id: 2, user_id: 2, title: "Post 2", content: "Bulk updated" },
			]);
		});
	});

	//
	// Test 17: Update on QuerySet without joins
	//

	test("QuerySet.update() - without joins", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username", "email"]))
				.update(
					trx
						.updateTable("users")
						.set({ email: "nojoin@example.com" })
						.where("id", "=", 7)
						.returningAll(),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 7,
				username: "grace",
				email: "nojoin@example.com",
			});
		});
	});

	//
	// Test 18: Update with has-one join (leftJoinOne)
	//

	test("QuerySet.update() - with has-one join (leftJoinOne)", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinOne(
					"user",
					({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
					"user.id",
					"posts.user_id",
				)
				.update(
					trx
						.updateTable("posts")
						.set({ title: "Updated Join Title" })
						.where("id", "=", 1)
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 1,
				user_id: 2,
				title: "Updated Join Title",
				user: {
					id: 2,
					username: "bob",
				},
			});
		});
	});

	//
	// Test 19: Update with has-many join (leftJoinMany)
	//

	test("QuerySet.update() - with has-many join (leftJoinMany)", async () => {
		await testInTransaction(db, async (trx) => {
			// Update a user and hydrate their posts
			const query = querySet(trx)
				.selectAs("users", trx.selectFrom("users").select(["id", "username", "email"]))
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"users.id",
				)
				.update(
					trx
						.updateTable("users")
						.set({ email: "manyjoin@example.com" })
						.where("id", "=", 2) // Bob has 4 posts
						.returningAll(),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 2);
			assert.strictEqual(result.username, "bob");
			assert.strictEqual(result.email, "manyjoin@example.com");
			assert.strictEqual(result.posts.length, 4);
			// Verify posts are hydrated correctly
			assert.ok(result.posts.every((p: any) => p.user_id === 2));
		});
	});

	//
	// Test 20: Update with nested joins
	//

	test("QuerySet.update() - with nested joins", async () => {
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
				.update(
					trx
						.updateTable("posts")
						.set({ title: "Nested Join Update" })
						.where("id", "=", 2) // Post 2 is by Bob (user 2) who has a profile
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 2,
				user_id: 2,
				title: "Nested Join Update",
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
	// Test 21: Update with .extras()
	//

	test("QuerySet.update() - with .extras() at root level", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.extras({
					upperTitle: (row) => row.title.toUpperCase(),
					titleLength: (row) => row.title.length,
				})
				.update(
					trx
						.updateTable("posts")
						.set({ title: "extras test" })
						.where("id", "=", 3)
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 3,
				user_id: 3,
				title: "extras test",
				upperTitle: "EXTRAS TEST",
				titleLength: 11,
			});
		});
	});

	//
	// Test 22: Update with nested extras in joins
	//

	test("QuerySet.update() - with nested extras in joins", async () => {
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
				.update(
					trx
						.updateTable("posts")
						.set({ title: "NESTED EXTRAS UPDATE" })
						.where("id", "=", 4)
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 4,
				user_id: 4,
				title: "NESTED EXTRAS UPDATE",
				titleLower: "nested extras update",
				user: {
					id: 4,
					username: "dave",
					usernameUpper: "DAVE",
				},
			});
		});
	});

	//
	// Test 23: Update returning no rows
	//

	test("updateAs() - returning no rows returns undefined", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ email: "nomatch@example.com" })
					.where("id", "=", 9999) // Non-existent ID
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.strictEqual(result, undefined);
		});
	});

	//
	// Test 24: Update with factory function
	//

	test("updateAs() - with factory function form", async () => {
		await testInTransaction(db, async (trx) => {
			// Test both the factory function form and direct query form
			const query1 = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ email: "factory1@example.com" })
					.where("id", "=", 8)
					.returningAll(),
			);

			const query2 = querySet(trx).updateAs(
				"updatedUser",
				trx
					.updateTable("users")
					.set({ email: "factory2@example.com" })
					.where("id", "=", 9)
					.returningAll(),
			);

			const result1 = await query1.executeTakeFirst();
			const result2 = await query2.executeTakeFirst();

			assert.ok(result1 && result2);

			// Both should have the same shape
			assert.deepStrictEqual(result1, {
				id: 8,
				username: "heidi",
				email: "factory1@example.com",
			});

			assert.deepStrictEqual(result2, {
				id: 9,
				username: "ivan",
				email: "factory2@example.com",
			});
		});
	});

	//
	// Test 25: Update with RETURNING clause referencing updated values
	//

	test("updateAs() - RETURNING references updated values", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ username: "newUsername", email: "newEmail@example.com" })
					.where("id", "=", 10)
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			// Should return NEW values, not old ones
			assert.deepStrictEqual(result, {
				id: 10,
				username: "newUsername",
				email: "newEmail@example.com",
			});
		});
	});
});
