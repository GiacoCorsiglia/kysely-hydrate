import assert from "node:assert";
import { test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Tests
//

describePg("query-set: postgres-mixed-writes", () => {
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

	test("insert() replaces the base query: prior .modify() filters are discarded", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				// A read filter on the base query is intentionally discarded when
				// switching to a write (see the insert() docs): the write query is
				// used as-is; only joins/attaches and hydration config carry over.
				.modify((qb) => qb.where("user_id", "=", 2))
				.insert(
					trx
						.insertInto("posts")
						.values({
							user_id: 3, // Deliberately does NOT match the discarded filter.
							title: "Not by user 2",
							content: "Content",
						})
						.returning(["id", "user_id", "title"]),
				);

			// No trace of the discarded filter in the compiled SQL.
			const { sql } = query.compile();
			assert.ok(!sql.toLowerCase().includes("where"), sql);

			// The inserted row is returned even though it fails the discarded filter.
			const result = await query.executeTakeFirst();
			assert.ok(result);
			assert.ok(typeof result.id === "number");
			assert.strictEqual(result.user_id, 3);
			assert.strictEqual(result.title, "Not by user 2");
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
