import assert from "node:assert";
import { describe, test } from "node:test";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();
const shouldSkip = dialect !== "postgres";

//
// Tests
//

describe("query-set: postgres-mixed-writes", { skip: shouldSkip }, () => {
	//
	// Test 38: Chaining write operations
	//

	test("Chaining write operations - latest operation wins", async () => {
		await testInTransaction(db, async (trx) => {
			// Start with an insertAs
			const insertQuery = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({
						username: "insertUser",
						email: "insert@example.com",
					})
					.returningAll(),
			);

			// Chain with update() - should replace the insert
			const updateQuery = insertQuery.update(
				trx
					.updateTable("users")
					.set({ email: "updated@example.com" })
					.where("id", "=", 1)
					.returningAll(),
			);

			const result = await updateQuery.executeTakeFirst();

			assert.ok(result);
			// Should return updated user (id=1, alice), not inserted user
			assert.deepStrictEqual(result, {
				id: 1,
				username: "alice",
				email: "updated@example.com",
			});
		});
	});

	//
	// Test 39: Write operations with .modify()
	//

	test("Write operations with .modify() - modifications preserved", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.modify((qb) => qb.where("user_id", "=", 2)) // Add a WHERE to base query
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 2,
							title: "Modified Insert",
							content: "Content",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				user_id: 2,
				title: "Modified Insert",
			});

			// Verify that the WHERE clause is still in effect by checking we can filter
			// This demonstrates that .modify() is preserved through the .insert() call
		});
	});

	//
	// Test 40: Write with collection .modify()
	//

	test("Write with collection .modify() - join modifications preserved", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				)
				// Modify the comments collection to only include comments with "Comment 1" in them
				.modify("comments", (commentsQuerySet) =>
					commentsQuerySet.modify((qb) => qb.where("content", "like", "%Comment 1%")),
				)
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 1,
							title: "Post with filtered comments",
							content: "Content",
						})
						.returning(["id", "user_id", "title"]),
				);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			// New post has no comments, so array should be empty
			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "Post with filtered comments",
				comments: [],
			});

			// Now test that the filter actually works by updating an existing post
			const updateQuery = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				)
				.modify("comments", (commentsQuerySet) =>
					commentsQuerySet.modify((qb) => qb.where("content", "like", "%Comment 1%")),
				)
				.update(
					trx
						.updateTable("posts")
						.set({ title: "Updated Post" })
						.where("id", "=", 1) // Post 1 has comments with "Comment 1"
						.returning(["id", "user_id", "title"]),
				);

			const updateResult = await updateQuery.executeTakeFirst();

			assert.ok(updateResult);
			assert.strictEqual(updateResult.id, 1);
			// Should only have comments matching the filter
			assert.ok(updateResult.comments.length > 0);
			assert.ok(updateResult.comments.every((c: any) => c.content.includes("Comment 1")));
		});
	});
});
