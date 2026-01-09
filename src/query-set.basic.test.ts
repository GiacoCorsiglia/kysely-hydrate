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
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
		{ id: 5, username: "eve" },
		{ id: 6, username: "frank" },
		{ id: 7, username: "grace" },
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});

test("executeTakeFirst: returns first row or undefined", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirst();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });
});

test("executeTakeFirst: returns undefined when no rows", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]).where("id", "=", 999))
		.executeTakeFirst();

	assert.strictEqual(user, undefined);
});

test("executeTakeFirstOrThrow: returns first row", async () => {
	const user = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirstOrThrow();

	assert.deepStrictEqual(user, { id: 1, username: "alice" });
});

test("executeTakeFirstOrThrow: throws when no rows", async () => {
	await assert.rejects(async () => {
		await querySet(db)
			.init("user", db.selectFrom("users").select(["id", "username"]).where("id", "=", 999))
			.executeTakeFirstOrThrow();
	});
});

test("init: defaults keyBy to 'id'", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
		{ id: 5, username: "eve" },
		{ id: 6, username: "frank" },
		{ id: 7, username: "grace" },
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});

test("init: accepts explicit keyBy", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username", "email"]), "username")
		.execute();

	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", email: "alice@example.com" },
		{ id: 2, username: "bob", email: "bob@example.com" },
		{ id: 3, username: "carol", email: "carol@example.com" },
		{ id: 4, username: "dave", email: "dave@example.com" },
		{ id: 5, username: "eve", email: "eve@example.com" },
		{ id: 6, username: "frank", email: "frank@example.com" },
		{ id: 7, username: "grace", email: "grace@example.com" },
		{ id: 8, username: "heidi", email: "heidi@example.com" },
		{ id: 9, username: "ivan", email: "ivan@example.com" },
		{ id: 10, username: "judy", email: "judy@example.com" },
	]);
});

test("init: accepts factory function", async () => {
	const users = await querySet(db)
		.init("user", (eb) => eb.selectFrom("users").select(["id", "username", "email"]))
		.execute();

	assert.strictEqual(users.length, 10);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", email: "alice@example.com" },
		{ id: 2, username: "bob", email: "bob@example.com" },
		{ id: 3, username: "carol", email: "carol@example.com" },
		{ id: 4, username: "dave", email: "dave@example.com" },
		{ id: 5, username: "eve", email: "eve@example.com" },
		{ id: 6, username: "frank", email: "frank@example.com" },
		{ id: 7, username: "grace", email: "grace@example.com" },
		{ id: 8, username: "heidi", email: "heidi@example.com" },
		{ id: 9, username: "ivan", email: "ivan@example.com" },
		{ id: 10, username: "judy", email: "judy@example.com" },
	]);
});

test("toBaseQuery: returns underlying base query", async () => {
	const baseQuery = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.toBaseQuery();

	const rows = await baseQuery.execute();
	assert.strictEqual(rows.length, 10);
	assert.deepStrictEqual(rows, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
		{ id: 5, username: "eve" },
		{ id: 6, username: "frank" },
		{ id: 7, username: "grace" },
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});

test("toQuery: returns opaque query builder", async () => {
	const query = querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.toQuery();

	const rows = await query.execute();
	assert.strictEqual(rows.length, 10);
	assert.deepStrictEqual(rows, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
		{ id: 3, username: "carol" },
		{ id: 4, username: "dave" },
		{ id: 5, username: "eve" },
		{ id: 6, username: "frank" },
		{ id: 7, username: "grace" },
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});
