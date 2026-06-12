import assert from "node:assert";
import { test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Tests
//
// Every test verifies post-delete DB state with a re-select inside the same
// transaction: a DELETE's RETURNING output is identical to the pre-existing
// seeded rows, so without the re-select these tests would pass even if the
// library generated a plain SELECT instead of a DELETE.
//

describePg("query-set: postgres-delete", () => {
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

			const remaining = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "=", 1)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
		});
	});

	//
	// Test 27: Delete with partial returning
	//

	test("deleteAs() - delete with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 2).returning(["id", "username"]),
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 2,
				username: "bob",
			});

			const remaining = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "=", 2)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
		});
	});

	//
	// Test 28: Delete with custom keyBy
	//

	test("deleteAs() - delete with custom keyBy orders results by that key", async () => {
		await testInTransaction(db, async (trx) => {
			// Keyed by title; "Post 12" < "Post 2" < "Post 3" alphabetically, so
			// key-ordering by title is distinguishable from the default id order
			// ([2, 3, 12]). A no-op keyBy would produce a different order.
			const query = querySet(trx).deleteAs(
				"deletedPosts",
				(db) => db.deleteFrom("posts").where("id", "in", [2, 3, 12]).returningAll(),
				"title",
			);

			const results = await query.execute();

			assert.deepStrictEqual(results, [
				{ id: 12, user_id: 2, title: "Post 12", content: "Content for post 12" },
				{ id: 2, user_id: 2, title: "Post 2", content: "Content for post 2" },
				{ id: 3, user_id: 3, title: "Post 3", content: "Content for post 3" },
			]);

			const remaining = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "in", [2, 3, 12])
				.execute();
			assert.deepStrictEqual(remaining, []);
		});
	});

	//
	// Test 29: Delete multiple rows with ordering
	//

	test("deleteAs() - delete multiple rows with ordering", async () => {
		await testInTransaction(db, async (trx) => {
			// "Post 1" < "Post 10" < "Post 2" alphabetically, so ordering by title
			// is distinguishable from id order ([1, 2, 10]).
			const query = querySet(trx)
				.deleteAs("deletedPosts", (db) =>
					db.deleteFrom("posts").where("id", "in", [1, 2, 10]).returningAll(),
				)
				.orderBy("title");

			const results = await query.execute();

			assert.deepStrictEqual(results, [
				{ id: 1, user_id: 2, title: "Post 1", content: "Content for post 1" },
				{ id: 10, user_id: 9, title: "Post 10", content: "Content for post 10" },
				{ id: 2, user_id: 2, title: "Post 2", content: "Content for post 2" },
			]);

			const remaining = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "in", [1, 2, 10])
				.execute();
			assert.deepStrictEqual(remaining, []);
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

			const remaining = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "=", 7)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
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

			const remainingPost = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "=", 1)
				.executeTakeFirst();
			assert.strictEqual(remainingPost, undefined);

			// The joined user is only hydrated, not deleted
			const joinedUser = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "=", 2)
				.executeTakeFirst();
			assert.ok(joinedUser);
		});
	});

	//
	// Test 32: Delete with has-many join (leftJoinMany)
	//

	test("QuerySet.delete() - with has-many join (leftJoinMany)", async () => {
		await testInTransaction(db, async (trx) => {
			// Delete a user and hydrate their posts (joined against the snapshot
			// before the ON DELETE CASCADE removes them)
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

			const remainingUser = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "=", 2)
				.executeTakeFirst();
			assert.strictEqual(remainingUser, undefined);

			// ON DELETE CASCADE removed the user's posts
			const remainingPosts = await trx
				.selectFrom("posts")
				.select("id")
				.where("user_id", "=", 2)
				.execute();
			assert.deepStrictEqual(remainingPosts, []);
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

			const remainingPost = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "=", 2)
				.executeTakeFirst();
			assert.strictEqual(remainingPost, undefined);
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

			const remaining = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "=", 3)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
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

			const remaining = await trx
				.selectFrom("posts")
				.select("id")
				.where("id", "=", 4)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
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

			// Nothing matched, so nothing was deleted
			const users = await trx.selectFrom("users").select("id").execute();
			assert.strictEqual(users.length, 10);
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

			const remaining = await trx
				.selectFrom("users")
				.select("id")
				.where("id", "in", [8, 9])
				.execute();
			assert.deepStrictEqual(remaining, []);
		});
	});
});
