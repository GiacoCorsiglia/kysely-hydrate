import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 2: Simple Base Query Modifications
//

test("modify: add WHERE clause", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", ">", 5))
		.execute();

	assert.strictEqual(users.length, 5); // Users 6-10
	assert.deepStrictEqual(users, [
		{ id: 6, username: "frank" },
		{ id: 7, username: "grace" },
		{ id: 8, username: "heidi" },
		{ id: 9, username: "ivan" },
		{ id: 10, username: "judy" },
	]);
});

test("modify: add additional SELECT", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.select("email"))
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

test("modify: multiple calls chained", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", "<=", 5))
		.modify((qb) => qb.select("email"))
		.execute();

	assert.strictEqual(users.length, 5); // Users 1-5
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", email: "alice@example.com" },
		{ id: 2, username: "bob", email: "bob@example.com" },
		{ id: 3, username: "carol", email: "carol@example.com" },
		{ id: 4, username: "dave", email: "dave@example.com" },
		{ id: 5, username: "eve", email: "eve@example.com" },
	]);
});

test("where: simple reference", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", "=", 1)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.deepStrictEqual(users, [{ id: 1, username: "alice" }]);
});

test("where: with expression factory", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where((eb) => eb.or([eb("id", "=", 1), eb("id", "=", 2)]))
		.execute();

	assert.strictEqual(users.length, 2);
	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
	]);
});

test("toBaseQuery: returns modified base query", async () => {
	const baseQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", "<", 3))
		.toBaseQuery();

	const rows = await baseQuery.execute();
	assert.strictEqual(rows.length, 2);
	assert.deepStrictEqual(rows, [
		{ id: 1, username: "alice" },
		{ id: 2, username: "bob" },
	]);
});

test("toQuery: returns opaque query builder with modifications", async () => {
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", "=", 1))
		.toQuery();

	const rows = await query.execute();
	assert.strictEqual(rows.length, 1);
	assert.deepStrictEqual(rows, [{ id: 1, username: "alice" }]);
});
