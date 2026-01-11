import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/postgres.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();
const shouldSkip = !process.env.RUN_POSTGRES_TESTS;

describe("Postgres: inserts", { skip: shouldSkip }, () => {
	test("querySet.insertAs() - no joins", async () => {
		const query = querySet(db).insertAs("newUser", (db) =>
			db
				.insertInto("users")
				.values({
					username: "newUserName",
					email: "new@example.com",
				})
				.returningAll(),
		);

		const result = await query.executeTakeFirst();

		assert.deepStrictEqual(result, {
			id: 11,
			username: "newUserName",
			email: "new@example.com",
		});

		await db.deleteFrom("users").where("id", "=", 11).executeTakeFirstOrThrow();
	});
});
