import assert from "node:assert";
import { test } from "node:test";

import * as k from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { type SeedDB } from "./__tests__/fixture.ts";
import { describePg, testInTransaction } from "./__tests__/helpers.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Write Tests (insert / update / delete)
//
// Consolidates the per-verb suites (postgres-insert, postgres-update,
// postgres-delete) and postgres-mixed-writes.
//
// - The verb-INDEPENDENT hydration contract (how a write's RETURNING rows
//   flow through joins and extras) runs table-driven over all three verbs.
//   Each cell pins its own literals and post-write DB state, so no verb's
//   behavior is inferred from another's.
// - The verbAs() creation path (returning shapes, keyBy, ordering, factory
//   forms) is hand-written per verb.
// - writeAs() data-modifying CTEs live in query-set.postgres-write-cte.test.ts.
//
// Every test runs in a rolled-back transaction; deletes (and the other
// verbs' "simple" rows) re-select inside the transaction to verify the
// write actually happened — RETURNING output alone can't distinguish a
// write from a SELECT.
//

const BOB_POSTS = [
	{ id: 1, title: "Post 1", user_id: 2 },
	{ id: 2, title: "Post 2", user_id: 2 },
	{ id: 5, title: "Post 5", user_id: 2 },
	{ id: 12, title: "Post 12", user_id: 2 },
];

const CAROL_POSTS = [
	{ id: 3, title: "Post 3", user_id: 3 },
	{ id: 15, title: "Post 15", user_id: 3 },
];

type Trx = k.Kysely<SeedDB>;

interface ExecutableWrite {
	executeTakeFirst(): Promise<unknown>;
}

interface VerbWrite<QS> {
	/** Applies this verb's write query to the scenario's query set. */
	apply: (qs: QS, trx: Trx) => ExecutableWrite;
	/** Inserted rows have generated ids; strip before comparing. */
	stripGeneratedId?: boolean;
	/** Expected hydrated output of executeTakeFirst(). */
	expected: unknown;
	/** Post-write DB state assertions (inside the same transaction). */
	verify: (trx: Trx) => Promise<void>;
}

interface WriteScenario<QS> {
	title: string;
	/** Builds the SELECT-side query set whose hydration config the write inherits. */
	build: (trx: Trx) => QS;
	verbs: { insert: VerbWrite<QS>; update: VerbWrite<QS>; delete: VerbWrite<QS> };
}

/** Identity helper so each scenario's cells get a typed `qs` parameter. */
function writeScenario<QS>(scenario: WriteScenario<QS>): WriteScenario<any> {
	return scenario;
}

function postsBase(trx: Trx) {
	return querySet(trx).selectAs(
		"posts",
		trx.selectFrom("posts").select(["id", "user_id", "title"]),
	);
}

function usersBase(trx: Trx) {
	return querySet(trx).selectAs(
		"users",
		trx.selectFrom("users").select(["id", "username", "email"]),
	);
}

/** Re-selects inside the transaction to assert a row no longer exists. */
async function expectRowGone(trx: Trx, table: "users" | "posts", id: number): Promise<void> {
	const row = await trx.selectFrom(table).select("id").where("id", "=", id).executeTakeFirst();
	assert.strictEqual(row, undefined);
}

const WRITE_SCENARIOS: WriteScenario<any>[] = [
	writeScenario({
		title: "without joins",
		build: usersBase,
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("users")
							.values({ username: "noJoinUser", email: "nojoin@example.com" })
							.returningAll(),
					),
				stripGeneratedId: true,
				expected: { username: "noJoinUser", email: "nojoin@example.com" },
				verify: async (trx) => {
					const row = await trx
						.selectFrom("users")
						.select("email")
						.where("username", "=", "noJoinUser")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { email: "nojoin@example.com" });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("users")
							.set({ email: "nojoin@example.com" })
							.where("id", "=", 7)
							.returningAll(),
					),
				expected: { id: 7, username: "grace", email: "nojoin@example.com" },
				verify: async (trx) => {
					const row = await trx
						.selectFrom("users")
						.select("email")
						.where("id", "=", 7)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { email: "nojoin@example.com" });
				},
			},
			delete: {
				apply: (qs, trx) => qs.delete(trx.deleteFrom("users").where("id", "=", 7).returningAll()),
				expected: { id: 7, username: "grace", email: "grace@example.com" },
				verify: (trx) => expectRowGone(trx, "users", 7),
			},
		},
	}),

	writeScenario({
		title: "with a has-one join (leftJoinOne)",
		build: (trx) =>
			postsBase(trx).leftJoinOne(
				"user",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"user.id",
				"posts.user_id",
			),
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("posts")
							.values({ user_id: 1, title: "Join Test Post", content: "Content for join test" })
							.returning(["id", "user_id", "title"]),
					),
				stripGeneratedId: true,
				expected: {
					user_id: 1,
					title: "Join Test Post",
					user: { id: 1, username: "alice" },
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("user_id")
						.where("title", "=", "Join Test Post")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { user_id: 1 });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("posts")
							.set({ title: "Updated Join Title" })
							.where("id", "=", 1)
							.returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 1,
					user_id: 2,
					title: "Updated Join Title",
					user: { id: 2, username: "bob" },
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("title")
						.where("id", "=", 1)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { title: "Updated Join Title" });
				},
			},
			delete: {
				apply: (qs, trx) =>
					qs.delete(
						trx.deleteFrom("posts").where("id", "=", 1).returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 1,
					user_id: 2,
					title: "Post 1",
					user: { id: 2, username: "bob" },
				},
				verify: async (trx) => {
					await expectRowGone(trx, "posts", 1);

					// The joined user is only hydrated, not deleted
					const user = await trx
						.selectFrom("users")
						.select("id")
						.where("id", "=", 2)
						.executeTakeFirst();
					assert.ok(user);
				},
			},
		},
	}),

	writeScenario({
		title: "with a has-many join (leftJoinMany)",
		build: (trx) =>
			usersBase(trx).leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"users.id",
			),
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("users")
							.values({ username: "userWithPosts", email: "withposts@example.com" })
							.returningAll(),
					),
				stripGeneratedId: true,
				// A new user has no posts: empty collection, not null
				expected: { username: "userWithPosts", email: "withposts@example.com", posts: [] },
				verify: async (trx) => {
					const row = await trx
						.selectFrom("users")
						.select("email")
						.where("username", "=", "userWithPosts")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { email: "withposts@example.com" });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("users")
							.set({ email: "manyjoin@example.com" })
							.where("id", "=", 2) // Bob has 4 posts
							.returningAll(),
					),
				expected: { id: 2, username: "bob", email: "manyjoin@example.com", posts: BOB_POSTS },
				verify: async (trx) => {
					const row = await trx
						.selectFrom("users")
						.select("email")
						.where("id", "=", 2)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { email: "manyjoin@example.com" });
				},
			},
			delete: {
				apply: (qs, trx) => qs.delete(trx.deleteFrom("users").where("id", "=", 2).returningAll()),
				// Joined against the snapshot before ON DELETE CASCADE removes them
				expected: { id: 2, username: "bob", email: "bob@example.com", posts: BOB_POSTS },
				verify: async (trx) => {
					await expectRowGone(trx, "users", 2);

					// ON DELETE CASCADE removed the user's posts
					const posts = await trx
						.selectFrom("posts")
						.select("id")
						.where("user_id", "=", 2)
						.execute();
					assert.deepStrictEqual(posts, []);
				},
			},
		},
	}),

	writeScenario({
		title: "with nested joins",
		build: (trx) =>
			postsBase(trx).leftJoinOne(
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
			),
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("posts")
							.values({
								user_id: 1,
								title: "Nested Join Post",
								content: "Content with nested joins",
							})
							.returning(["id", "user_id", "title"]),
					),
				stripGeneratedId: true,
				expected: {
					user_id: 1,
					title: "Nested Join Post",
					user: {
						id: 1,
						username: "alice",
						profile: { id: 1, bio: "Bio for user 1", user_id: 1 },
					},
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("user_id")
						.where("title", "=", "Nested Join Post")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { user_id: 1 });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("posts")
							.set({ title: "Nested Join Update" })
							.where("id", "=", 2)
							.returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 2,
					user_id: 2,
					title: "Nested Join Update",
					user: {
						id: 2,
						username: "bob",
						profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
					},
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("title")
						.where("id", "=", 2)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { title: "Nested Join Update" });
				},
			},
			delete: {
				apply: (qs, trx) =>
					qs.delete(
						trx.deleteFrom("posts").where("id", "=", 2).returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 2,
					user_id: 2,
					title: "Post 2",
					user: {
						id: 2,
						username: "bob",
						profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
					},
				},
				verify: (trx) => expectRowGone(trx, "posts", 2),
			},
		},
	}),

	writeScenario({
		title: "with extras at the root level",
		build: (trx) =>
			postsBase(trx).extras({
				upperTitle: (row) => row.title.toUpperCase(),
				titleLength: (row) => row.title.length,
			}),
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("posts")
							.values({ user_id: 1, title: "extras test", content: "content for extras" })
							.returning(["id", "user_id", "title"]),
					),
				stripGeneratedId: true,
				expected: {
					user_id: 1,
					title: "extras test",
					upperTitle: "EXTRAS TEST",
					titleLength: 11,
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("user_id")
						.where("title", "=", "extras test")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { user_id: 1 });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("posts")
							.set({ title: "extras test" })
							.where("id", "=", 3)
							.returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 3,
					user_id: 3,
					title: "extras test",
					upperTitle: "EXTRAS TEST",
					titleLength: 11,
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("title")
						.where("id", "=", 3)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { title: "extras test" });
				},
			},
			delete: {
				apply: (qs, trx) =>
					qs.delete(
						trx.deleteFrom("posts").where("id", "=", 3).returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 3,
					user_id: 3,
					title: "Post 3",
					upperTitle: "POST 3",
					titleLength: 6,
				},
				verify: (trx) => expectRowGone(trx, "posts", 3),
			},
		},
	}),

	writeScenario({
		title: "with nested extras in joins",
		build: (trx) =>
			postsBase(trx)
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
				}),
		verbs: {
			insert: {
				apply: (qs, trx) =>
					qs.insert(
						trx
							.insertInto("posts")
							.values({ user_id: 1, title: "NESTED EXTRAS TEST", content: "content" })
							.returning(["id", "user_id", "title"]),
					),
				stripGeneratedId: true,
				expected: {
					user_id: 1,
					title: "NESTED EXTRAS TEST",
					titleLower: "nested extras test",
					user: { id: 1, username: "alice", usernameUpper: "ALICE" },
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("user_id")
						.where("title", "=", "NESTED EXTRAS TEST")
						.executeTakeFirst();
					assert.deepStrictEqual(row, { user_id: 1 });
				},
			},
			update: {
				apply: (qs, trx) =>
					qs.update(
						trx
							.updateTable("posts")
							.set({ title: "NESTED EXTRAS UPDATE" })
							.where("id", "=", 4)
							.returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 4,
					user_id: 4,
					title: "NESTED EXTRAS UPDATE",
					titleLower: "nested extras update",
					user: { id: 4, username: "dave", usernameUpper: "DAVE" },
				},
				verify: async (trx) => {
					const row = await trx
						.selectFrom("posts")
						.select("title")
						.where("id", "=", 4)
						.executeTakeFirst();
					assert.deepStrictEqual(row, { title: "NESTED EXTRAS UPDATE" });
				},
			},
			delete: {
				apply: (qs, trx) =>
					qs.delete(
						trx.deleteFrom("posts").where("id", "=", 4).returning(["id", "user_id", "title"]),
					),
				expected: {
					id: 4,
					user_id: 4,
					title: "Post 4",
					titleLower: "post 4",
					user: { id: 4, username: "dave", usernameUpper: "DAVE" },
				},
				verify: (trx) => expectRowGone(trx, "posts", 4),
			},
		},
	}),
];

describePg("query-set: postgres-writes", () => {
	//
	// Shared hydration contract, table-driven over all three verbs
	//

	for (const scenario of WRITE_SCENARIOS) {
		for (const verb of ["insert", "update", "delete"] as const) {
			const verbWrite = scenario.verbs[verb];

			test(`${verb}: ${scenario.title}`, async () => {
				await testInTransaction(db, async (trx) => {
					const result = await verbWrite.apply(scenario.build(trx), trx).executeTakeFirst();

					assert.ok(result);
					if (verbWrite.stripGeneratedId) {
						assert.ok(typeof (result as { id?: unknown }).id === "number");
						delete (result as { id?: unknown }).id;
					}
					assert.deepStrictEqual(result, verbWrite.expected);

					await verbWrite.verify(trx);
				});
			});
		}
	}

	//
	// insertAs() creation path
	//

	test("insertAs: simple insert with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({ username: "newUserName", email: "new@example.com" })
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

			const row = await trx
				.selectFrom("users")
				.select("email")
				.where("username", "=", "newUserName")
				.executeTakeFirst();
			assert.deepStrictEqual(row, { email: "new@example.com" });
		});
	});

	test("insertAs: insert with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			// A genuine partial RETURNING list (email is inserted but not returned)
			const query = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({ username: "partialUser", email: "partial@example.com" })
					.returning(["id", "username"]),
			);

			const result = await query.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			delete (result as any).id;
			assert.deepStrictEqual(result, {
				username: "partialUser",
			});
		});
	});

	test("insertAs: insert with custom keyBy orders results by that key", async () => {
		await testInTransaction(db, async (trx) => {
			// Inserted in non-alphabetical order: key-ordering by username is
			// distinguishable from the default id (= insertion) order
			const query = querySet(trx).insertAs(
				"newUsers",
				(db) =>
					db
						.insertInto("users")
						.values([
							{ username: "zeta", email: "zeta@example.com" },
							{ username: "alpha", email: "alpha@example.com" },
							{ username: "mike", email: "mike@example.com" },
						])
						.returningAll(),
				"username",
			);

			const results = await query.execute();

			for (const result of results) {
				assert.ok(typeof result.id === "number");
				delete (result as any).id;
			}

			assert.deepStrictEqual(results, [
				{ username: "alpha", email: "alpha@example.com" },
				{ username: "mike", email: "mike@example.com" },
				{ username: "zeta", email: "zeta@example.com" },
			]);
		});
	});

	test("insertAs: insert multiple rows with ordering", async () => {
		await testInTransaction(db, async (trx) => {
			// Inserted in non-alphabetical order: orderBy("username") is
			// distinguishable from the default id (= insertion) order
			const query = querySet(trx)
				.insertAs("newUsers", (db) =>
					db
						.insertInto("users")
						.values([
							{ username: "userB", email: "userB@example.com" },
							{ username: "userA", email: "userA@example.com" },
							{ username: "userC", email: "userC@example.com" },
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
				{ username: "userA", email: "userA@example.com" },
				{ username: "userB", email: "userB@example.com" },
				{ username: "userC", email: "userC@example.com" },
			]);
		});
	});

	test("insertAs: with no results returns undefined", async () => {
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

	test("insertAs: with factory function form", async () => {
		await testInTransaction(db, async (trx) => {
			// Test both the factory function form and direct query form
			const query1 = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({ username: "factoryUser1", email: "factory1@example.com" })
					.returningAll(),
			);

			const query2 = querySet(trx).insertAs(
				"newUser",
				trx
					.insertInto("users")
					.values({ username: "factoryUser2", email: "factory2@example.com" })
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

	test("insertAs: executeExists hoists the __base CTE above the EXISTS wrap", async () => {
		await testInTransaction(db, async (trx) => {
			// The implicit __base CTE wrapping the INSERT is data-modifying, so it must be hoisted to
			// the top level of the EXISTS statement or Postgres rejects the query (SQLSTATE 0A000).
			const exists = await querySet(trx)
				.insertAs("newUser", (db) =>
					db
						.insertInto("users")
						.values({ username: "existsUser", email: "exists-insert@example.com" })
						.returning(["id", "username", "email"]),
				)
				.executeExists();

			assert.strictEqual(exists, true);

			// The insert itself still executed.
			const user = await trx
				.selectFrom("users")
				.select(["username"])
				.where("email", "=", "exists-insert@example.com")
				.executeTakeFirstOrThrow();
			assert.strictEqual(user.username, "existsUser");
		});
	});

	//
	// updateAs() creation path
	//

	test("updateAs: simple update with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ username: "updatedName", email: "updated@example.com" })
					.where("id", "=", 1)
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 1,
				username: "updatedName",
				email: "updated@example.com",
			});

			const row = await trx
				.selectFrom("users")
				.select(["username", "email"])
				.where("id", "=", 1)
				.executeTakeFirst();
			assert.deepStrictEqual(row, { username: "updatedName", email: "updated@example.com" });
		});
	});

	test("updateAs: update with partial returning", async () => {
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

	test("updateAs: update with custom keyBy orders results by that key", async () => {
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

	test("updateAs: update multiple rows with ordering", async () => {
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

	test("updateAs: returning no rows returns undefined", async () => {
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

	test("updateAs: with factory function form", async () => {
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

	test("updateAs: RETURNING references updated values", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).updateAs("updatedUser", (db) =>
				db
					.updateTable("users")
					.set({ username: "newUsername", email: "newEmail@example.com" })
					.where("id", "=", 10)
					.returningAll(),
			);

			const result = await query.executeTakeFirst();

			// Should return NEW values, not old ones
			assert.deepStrictEqual(result, {
				id: 10,
				username: "newUsername",
				email: "newEmail@example.com",
			});
		});
	});

	//
	// deleteAs() creation path
	//

	test("deleteAs: simple delete with returningAll()", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 1).returningAll(),
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 1,
				username: "alice",
				email: "alice@example.com",
			});

			await expectRowGone(trx, "users", 1);
		});
	});

	test("deleteAs: delete with partial returning", async () => {
		await testInTransaction(db, async (trx) => {
			const query = querySet(trx).deleteAs("deletedUser", (db) =>
				db.deleteFrom("users").where("id", "=", 2).returning(["id", "username"]),
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 2,
				username: "bob",
			});

			await expectRowGone(trx, "users", 2);
		});
	});

	test("deleteAs: delete with custom keyBy orders results by that key", async () => {
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

	test("deleteAs: delete multiple rows with ordering", async () => {
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

	test("deleteAs: returning no rows returns undefined", async () => {
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

	test("deleteAs: with factory function form", async () => {
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

	//
	// Mixed write operations
	//

	test("mixed: chaining write operations - latest operation wins", async () => {
		await testInTransaction(db, async (trx) => {
			// Start with an insertAs
			const insertQuery = querySet(trx).insertAs("newUser", (db) =>
				db
					.insertInto("users")
					.values({ username: "insertUser", email: "insert@example.com" })
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

			// Should return updated user (id=1, alice), not inserted user
			assert.deepStrictEqual(result, {
				id: 1,
				username: "alice",
				email: "updated@example.com",
			});
		});
	});

	test("mixed: insert() replaces the base query - prior .modify() filters are discarded", async () => {
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

	test("mixed: write with collection .modify() - join modifications preserved", async () => {
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
						.values({ user_id: 1, title: "Post with filtered comments", content: "Content" })
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

			assert.deepStrictEqual(updateResult, {
				id: 1,
				user_id: 2,
				title: "Updated Post",
				comments: [{ id: 1, content: "Comment 1 on post 1", post_id: 1 }],
			});
		});
	});

	//
	// Multi-row writes with joins (row explosion over the RETURNING CTE)
	//

	test("update: multi-row write with a many-join hydrates each entity's children", async () => {
		await testInTransaction(db, async (trx) => {
			// The RETURNING CTE (2 users) joined against posts explodes to 6 raw
			// rows; hydration must group them back into 2 entities with their own
			// children — the core hydration risk for writes.
			const results = await usersBase(trx)
				.leftJoinMany(
					"posts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"posts.user_id",
					"users.id",
				)
				.update(
					trx
						.updateTable("users")
						.set({ email: "bulk@example.com" })
						.where("id", "in", [2, 3])
						.returningAll(),
				)
				.execute();

			assert.deepStrictEqual(results, [
				{ id: 2, username: "bob", email: "bulk@example.com", posts: BOB_POSTS },
				{ id: 3, username: "carol", email: "bulk@example.com", posts: CAROL_POSTS },
			]);

			const rows = await trx
				.selectFrom("users")
				.select("email")
				.where("id", "in", [2, 3])
				.execute();
			assert.deepStrictEqual(rows, [{ email: "bulk@example.com" }, { email: "bulk@example.com" }]);
		});
	});

	//
	// ON CONFLICT (upsert)
	//

	test("insert: has-many join with pagination hoists RETURNING columns", async () => {
		await testInTransaction(db, async (trx) => {
			// Pagination past row explosion wraps the base in a derived table, so the insert's
			// RETURNING columns are hoisted by name.  The many-join sees only the pre-existing posts
			// (the outer SELECT does not see rows inserted by the data-modifying CTE).
			const query = querySet(trx)
				.selectAs("posts", trx.selectFrom("posts").select(["id", "user_id", "title"]))
				.leftJoinMany(
					"existingPosts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"existingPosts.user_id",
					"posts.user_id",
				)
				.insert(
					trx
						.insertInto("posts")
						.values([
							{ user_id: 2, title: "Paginated Post A", content: "content a" },
							{ user_id: 2, title: "Paginated Post B", content: "content b" },
						])
						.returning(["id", "user_id", "title"]),
				)
				.limit(1);

			const results = await query.execute();

			assert.strictEqual(results.length, 1);
			const result = results[0]!;
			assert.ok(typeof result.id === "number");
			assert.strictEqual(result.user_id, 2);
			assert.strictEqual(result.title, "Paginated Post A");

			// Bob (user 2) has 4 pre-existing posts.
			assert.deepStrictEqual(result.existingPosts.map((post) => post.title).sort(), [
				"Post 1",
				"Post 12",
				"Post 2",
				"Post 5",
			]);
		});
	});

	test("insertAs: ON CONFLICT DO UPDATE (upsert) hydrates the updated row", async () => {
		await testInTransaction(db, async (trx) => {
			// profiles.user_id is UNIQUE; user 1 already has a profile
			const query = querySet(trx).insertAs(
				"profile",
				(db) =>
					db
						.insertInto("profiles")
						.values({ user_id: 1, bio: "Upserted bio" })
						.onConflict((oc) => oc.column("user_id").doUpdateSet({ bio: "Upserted bio" }))
						.returning(["user_id", "bio"]),
				"user_id",
			);

			const result = await query.executeTakeFirst();

			assert.deepStrictEqual(result, { user_id: 1, bio: "Upserted bio" });

			// The conflict path updated in place: still exactly one profile
			const profiles = await trx
				.selectFrom("profiles")
				.select("bio")
				.where("user_id", "=", 1)
				.execute();
			assert.deepStrictEqual(profiles, [{ bio: "Upserted bio" }]);
		});
	});

	//
	// Update touching a join key
	//

	test("update: changing a join key hydrates the join against the NEW value", async () => {
		await testInTransaction(db, async (trx) => {
			// Post 1 moves from bob (2) to carol (3). The join runs against the
			// RETURNING rows, whose user_id is the new value — so the hydrated
			// user is carol, not bob.
			const result = await postsBase(trx)
				.leftJoinOne(
					"user",
					({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
					"user.id",
					"posts.user_id",
				)
				.update(
					trx
						.updateTable("posts")
						.set({ user_id: 3 })
						.where("id", "=", 1)
						.returning(["id", "user_id", "title"]),
				)
				.executeTakeFirst();

			assert.deepStrictEqual(result, {
				id: 1,
				user_id: 3,
				title: "Post 1",
				user: { id: 3, username: "carol" },
			});

			const row = await trx
				.selectFrom("posts")
				.select("user_id")
				.where("id", "=", 1)
				.executeTakeFirst();
			assert.deepStrictEqual(row, { user_id: 3 });
		});
	});

	//
	// Attach-after-write visibility (vs join snapshot semantics)
	//

	test("insert: attached collections see the written row; joins in the same statement do not", async () => {
		await testInTransaction(db, async (trx) => {
			// alice (user 1) has no posts. Insert one for her with BOTH:
			// - a leftJoinMany over posts: runs inside the same SQL statement as
			//   the INSERT, so it reads the pre-write snapshot (PostgreSQL
			//   statement visibility) and does NOT see the new post;
			// - an attachMany whose fetchFn queries posts afterwards: runs as a
			//   separate statement in the same transaction, so it DOES see it.
			const result = await querySet(trx)
				.insertAs("newPost", (db) =>
					db
						.insertInto("posts")
						.values({ user_id: 1, title: "Attach Test Post", content: "Content" })
						.returning(["id", "user_id", "title"]),
				)
				.leftJoinMany(
					"joinedPosts",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"joinedPosts.user_id",
					"newPost.user_id",
				)
				.attachMany(
					"attachedPosts",
					() => trx.selectFrom("posts").select(["id", "title", "user_id"]).where("user_id", "=", 1),
					{ matchChild: "user_id", toParent: "user_id" },
				)
				.executeTakeFirst();

			assert.ok(result);
			assert.ok(typeof result.id === "number");
			const newPostId = result.id;
			delete (result as any).id;

			assert.deepStrictEqual(result, {
				user_id: 1,
				title: "Attach Test Post",
				// Pre-write snapshot: alice's posts as of statement start
				joinedPosts: [],
				// Post-write fetch: includes the row this very statement inserted
				attachedPosts: [{ id: newPostId, title: "Attach Test Post", user_id: 1 }],
			});
		});
	});
});
