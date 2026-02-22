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

describe("query-set: postgres-write", { skip: shouldSkip }, () => {
	//
	// writeAs() - basic data-modifying CTE
	//

	test("writeAs() - single data-modifying CTE (UPDATE)", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("updated", (db) =>
					db
						.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "write-test@example.com" })
								.where("id", "=", 1)
								.returningAll(),
						)
						.selectFrom("updated")
						.select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 1);
			assert.strictEqual(result.username, "alice");
			assert.strictEqual(result.email, "write-test@example.com");
		});
	});

	test("writeAs() - single data-modifying CTE (INSERT)", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("inserted", (db) =>
					db
						.with("inserted", (qb) =>
							qb
								.insertInto("users")
								.values({ username: "newuser", email: "new@example.com" })
								.returningAll(),
						)
						.selectFrom("inserted")
						.select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			assert.strictEqual(result.username, "newuser");
			assert.strictEqual(result.email, "new@example.com");
		});
	});

	//
	// writeAs() - multiple data-modifying CTEs
	//

	test("writeAs() - multiple data-modifying CTEs", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("updated", (db) =>
					db
						.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "multi-cte@example.com" })
								.where("id", "=", 1)
								.returningAll(),
						)
						.with("newPost", (qb) =>
							qb
								.insertInto("posts")
								.values({
									user_id: 1,
									title: "Audit post",
									content: "User updated email",
								})
								.returning(["id", "user_id", "title"]),
						)
						.selectFrom("updated")
						.select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 1);
			assert.strictEqual(result.email, "multi-cte@example.com");

			// Verify the second CTE also executed
			const post = await trx
				.selectFrom("posts")
				.select(["title"])
				.where("title", "=", "Audit post")
				.executeTakeFirst();
			assert.ok(post);
			assert.strictEqual(post.title, "Audit post");
		});
	});

	//
	// writeAs() with joins
	//

	test("writeAs() with leftJoinMany - hydrates joined data", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("updated", (db) =>
					db
						.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "joined@example.com" })
								.where("id", "=", 2)
								.returningAll(),
						)
						.selectFrom("updated")
						.select(["id", "username", "email"]),
				)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"updated.id",
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 2);
			assert.strictEqual(result.email, "joined@example.com");
			assert.ok(Array.isArray(result.posts));
			assert.ok(result.posts.length > 0);
			assert.ok(result.posts.every((p: any) => p.user_id === 2));
		});
	});

	//
	// .write() on existing QuerySet
	//

	test(".write() on existing QuerySet preserves joins and hydration", async () => {
		await testInTransaction(db, async (trx) => {
			const usersQs = querySet(trx)
				.selectAs("user", trx.selectFrom("users").select(["id", "username", "email"]))
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"user.id",
				);

			const result = await usersQs
				.write((db) =>
					db
						.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "write-method@example.com" })
								.where("id", "=", 2)
								.returningAll(),
						)
						.selectFrom("updated")
						.select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 2);
			assert.strictEqual(result.email, "write-method@example.com");
			assert.ok(Array.isArray(result.posts));
		});
	});

	//
	// writeAs() with extras
	//

	test("writeAs() with extras - computed fields work", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("updated", (db) =>
					db
						.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "extras@example.com" })
								.where("id", "=", 1)
								.returningAll(),
						)
						.selectFrom("updated")
						.select(["id", "username", "email"]),
				)
				.extras({
					displayName: (row) => `${row.username} <${row.email}>`,
				})
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.displayName, "alice <extras@example.com>");
		});
	});

	//
	// writeAs() with DELETE
	//

	test("writeAs() with DELETE CTE", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs("deleted", (db) =>
					db
						.with("deleted", (qb) => qb.deleteFrom("users").where("id", "=", 1).returningAll())
						.selectFrom("deleted")
						.select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.strictEqual(result.id, 1);
			assert.strictEqual(result.username, "alice");

			// Verify deletion happened
			const remaining = await trx
				.selectFrom("users")
				.select(["id"])
				.where("id", "=", 1)
				.executeTakeFirst();
			assert.strictEqual(remaining, undefined);
		});
	});

	//
	// writeAs() with no CTEs works like selectAs()
	//

	test("writeAs() with no CTEs - works like selectAs()", async () => {
		const result = await querySet(db)
			.writeAs("user", db.selectFrom("users").select(["id", "username"]).where("id", "=", 1))
			.executeTakeFirst();

		assert.ok(result);
		assert.strictEqual(result.id, 1);
		assert.strictEqual(result.username, "alice");
	});
});
