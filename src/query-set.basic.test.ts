import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

describe("query-set: basic", () => {
	//
	// Phase 1: Basic Query Execution
	//

	test("execute: returns array of hydrated rows", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.executeTakeFirst();

		assert.deepStrictEqual(user, { id: 1, username: "alice" });
	});

	test("executeTakeFirst: returns undefined when no rows", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]).where("id", "=", 999))
			.executeTakeFirst();

		assert.strictEqual(user, undefined);
	});

	test("executeTakeFirstOrThrow: returns first row", async () => {
		const user = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.executeTakeFirstOrThrow();

		assert.deepStrictEqual(user, { id: 1, username: "alice" });
	});

	test("executeTakeFirstOrThrow: throws when no rows", async () => {
		await assert.rejects(async () => {
			await querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]).where("id", "=", 999))
				.executeTakeFirstOrThrow();
		});
	});

	test("init: defaults keyBy to 'id'", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]), "username")
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

	test("init: explicit keyBy works without selecting 'id'", async () => {
		// Regression test: the keyBy passed to selectAs must be used by the
		// hydrator.  Previously the hydrator always used "id", so rows without an
		// "id" column were silently dropped.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["username", "email"]), "username")
			.execute();

		assert.deepStrictEqual(users, [
			{ username: "alice", email: "alice@example.com" },
			{ username: "bob", email: "bob@example.com" },
			{ username: "carol", email: "carol@example.com" },
			{ username: "dave", email: "dave@example.com" },
			{ username: "eve", email: "eve@example.com" },
			{ username: "frank", email: "frank@example.com" },
			{ username: "grace", email: "grace@example.com" },
			{ username: "heidi", email: "heidi@example.com" },
			{ username: "ivan", email: "ivan@example.com" },
			{ username: "judy", email: "judy@example.com" },
		]);
	});

	test("init: groups by explicit keyBy, not by 'id'", async () => {
		// Distinct post authors keyed by user_id.  The join forces the hydrator to
		// group rows, which must happen by the explicit keyBy ("id" is not even
		// selected here).
		const postAuthors = await querySet(db)
			.selectAs("post", db.selectFrom("posts").select(["user_id"]).distinct(), "user_id")
			.innerJoinOne(
				"author",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"author.id",
				"post.user_id",
			)
			.execute();

		assert.deepStrictEqual(postAuthors, [
			{ user_id: 2, author: { id: 2, username: "bob" } },
			{ user_id: 3, author: { id: 3, username: "carol" } },
			{ user_id: 4, author: { id: 4, username: "dave" } },
			{ user_id: 5, author: { id: 5, username: "eve" } },
			{ user_id: 6, author: { id: 6, username: "frank" } },
			{ user_id: 7, author: { id: 7, username: "grace" } },
			{ user_id: 8, author: { id: 8, username: "heidi" } },
			{ user_id: 9, author: { id: 9, username: "ivan" } },
			{ user_id: 10, author: { id: 10, username: "judy" } },
		]);
	});

	test("init: writeAs passes explicit keyBy to the hydrator", async () => {
		// Same regression as above, via the writeAs() creation path.
		const users = await querySet(db)
			.writeAs(
				"u",
				(db) =>
					db.with("named_users", (qb) => qb.selectFrom("users").select(["username", "email"])),
				(qc) => qc.selectFrom("named_users").select(["username", "email"]),
				"username",
			)
			.execute();

		assert.strictEqual(users.length, 10);
		assert.deepStrictEqual(users[0], { username: "alice", email: "alice@example.com" });
	});

	test("init: accepts factory function", async () => {
		const users = await querySet(db)
			.selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username", "email"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
});
