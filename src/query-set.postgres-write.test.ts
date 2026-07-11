import assert from "node:assert";
import { test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Tests
//

describePg("query-set: postgres-write", () => {
	//
	// writeAs() - basic data-modifying CTE
	//

	test("writeAs() - single data-modifying CTE (UPDATE)", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "write-test@example.com" })
								.where("id", "=", 1)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
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
				.writeAs(
					"inserted",
					(db) =>
						db.with("inserted", (qb) =>
							qb
								.insertInto("users")
								.values({ username: "newuser", email: "new@example.com" })
								.returningAll(),
						),
					(qc) => qc.selectFrom("inserted").select(["id", "username", "email"]),
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
				.writeAs(
					"updated",
					(db) =>
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
							),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
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
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "joined@example.com" })
								.where("id", "=", 2)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"updated.id",
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 2,
				username: "bob",
				email: "joined@example.com",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
					{ id: 5, title: "Post 5", user_id: 2 },
					{ id: 12, title: "Post 12", user_id: 2 },
				],
			});
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
				.write(
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "write-method@example.com" })
								.where("id", "=", 2)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.deepStrictEqual(result, {
				id: 2,
				username: "bob",
				email: "write-method@example.com",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
					{ id: 5, title: "Post 5", user_id: 2 },
					{ id: 12, title: "Post 12", user_id: 2 },
				],
			});
		});
	});

	//
	// CTE hoisting when the base select gets wrapped (pagination / exists / count)
	//

	test("writeAs() with leftJoinMany and limit - CTE hoisted above the pagination wrap", async () => {
		await testInTransaction(db, async (trx) => {
			// Pagination with a many-join wraps the base select in a derived
			// table; the data-modifying CTE must be hoisted to the top level or
			// Postgres rejects the query (SQLSTATE 0A000).
			const result = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "paginated@example.com" })
								.where("id", "in", [2, 3])
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"updated.id",
				)
				.limit(1)
				.execute();

			// Only the first updated user is returned, with all their posts.
			assert.deepStrictEqual(result, [
				{
					id: 2,
					username: "bob",
					email: "paginated@example.com",
					posts: [
						{ id: 1, title: "Post 1", user_id: 2 },
						{ id: 2, title: "Post 2", user_id: 2 },
						{ id: 5, title: "Post 5", user_id: 2 },
						{ id: 12, title: "Post 12", user_id: 2 },
					],
				},
			]);

			// The write itself is not limited: both rows were updated.
			const emails = await trx
				.selectFrom("users")
				.select(["id", "email"])
				.where("id", "in", [2, 3])
				.orderBy("id")
				.execute();
			assert.deepStrictEqual(emails, [
				{ id: 2, email: "paginated@example.com" },
				{ id: 3, email: "paginated@example.com" },
			]);
		});
	});

	test("writeAs() executeExists - CTE hoisted above the EXISTS wrap", async () => {
		await testInTransaction(db, async (trx) => {
			const exists = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "exists@example.com" })
								.where("id", "=", 2)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"updated.id",
				)
				.executeExists();

			assert.strictEqual(exists, true);

			// The data-modifying CTE still executed.
			const user = await trx
				.selectFrom("users")
				.select(["email"])
				.where("id", "=", 2)
				.executeTakeFirstOrThrow();
			assert.strictEqual(user.email, "exists@example.com");
		});
	});

	test("writeAs() executeExists - returns false when the write matches no rows", async () => {
		await testInTransaction(db, async (trx) => {
			const exists = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "nobody@example.com" })
								.where("id", "=", -1)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.executeExists();

			assert.strictEqual(exists, false);
		});
	});

	test("writeAs() executeCount - CTE stays at top level", async () => {
		await testInTransaction(db, async (trx) => {
			const count = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "counted@example.com" })
								.where("id", "in", [2, 3])
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
				)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"updated.id",
				)
				.executeCount(Number);

			assert.strictEqual(count, 2);

			// The data-modifying CTE still executed.
			const user = await trx
				.selectFrom("users")
				.select(["email"])
				.where("id", "=", 3)
				.executeTakeFirstOrThrow();
			assert.strictEqual(user.email, "counted@example.com");
		});
	});

	//
	// writeAs() with extras
	//

	test("writeAs() with extras - computed fields work", async () => {
		await testInTransaction(db, async (trx) => {
			const result = await querySet(trx)
				.writeAs(
					"updated",
					(db) =>
						db.with("updated", (qb) =>
							qb
								.updateTable("users")
								.set({ email: "extras@example.com" })
								.where("id", "=", 1)
								.returningAll(),
						),
					(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
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
				.writeAs(
					"deleted",
					(db) =>
						db.with("deleted", (qb) => qb.deleteFrom("users").where("id", "=", 1).returningAll()),
					(qc) => qc.selectFrom("deleted").select(["id", "username", "email"]),
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
});
