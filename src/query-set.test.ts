import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 1: Basic Query Execution
//

test("execute: returns array of hydrated rows", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	assert.ok(Array.isArray(users));
	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users[0], { id: 1, username: "alice" });
});

test("executeTakeFirst: returns first row or undefined", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirst();

	assert.strictEqual(user?.id, 1);
	assert.strictEqual(user?.username, "alice");
});

test("executeTakeFirst: returns undefined when no rows", async () => {
	const user = await querySet(db)
		.init(
			"user",
			db.selectFrom("users").select(["id", "username"]).where("id", "=", 999),
		)
		.executeTakeFirst();

	assert.strictEqual(user, undefined);
});

test("executeTakeFirstOrThrow: returns first row", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirstOrThrow();

	assert.strictEqual(user.id, 1);
	assert.strictEqual(user.username, "alice");
});

test("executeTakeFirstOrThrow: throws when no rows", async () => {
	await assert.rejects(async () => {
		await querySet(db)
			.init(
				"user",
				db
					.selectFrom("users")
					.select(["id", "username"])
					.where("id", "=", 999),
			)
			.executeTakeFirstOrThrow();
	});
});

test("init: defaults keyBy to 'id'", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	// Verify hydration works correctly with default keyBy
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[1]?.id, 2);
});

test("init: accepts explicit keyBy", async () => {
	const users = await querySet(db)
		.init(
			"user",
			db.selectFrom("users").select(["id", "username", "email"]),
			"username",
		)
		.execute();

	// Should still work correctly with different keyBy
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual(users[0]?.email, "alice@example.com");
});

test("init: accepts factory function", async () => {
	const users = await querySet(db)
		.init("user", (eb) =>
			eb.selectFrom("users").select(["id", "username", "email"]),
		)
		.execute();

	assert.strictEqual(users.length, 10);
	assert.strictEqual(users[0]?.username, "alice");
});

test("toBaseQuery: returns underlying base query", async () => {
	const baseQuery = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.toBaseQuery();

	const rows = await baseQuery.execute();
	assert.strictEqual(rows.length, 10);
	assert.deepStrictEqual(rows[0], { id: 1, username: "alice" });
});

test("toQuery: returns opaque query builder", async () => {
	const query = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.toQuery();

	const rows = await query.execute();
	assert.strictEqual(rows.length, 10);
});
