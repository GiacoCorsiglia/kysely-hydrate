import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/postgres.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();
const shouldSkip = !process.env.RUN_POSTGRES_TESTS;

describe("Postgres: inserts", { skip: shouldSkip }, () => {
	test("querySet().insertAs() - no joins", async () => {
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

	test("QuerySet.insert()", async () => {
		const postsQuerySet = querySet(db)
			.selectAs("posts", db.selectFrom("posts").select(["id", "user_id", "title"]))
			.innerJoinOne(
				"user",
				(nest) => nest((eb) => eb.selectFrom("users").select(["id"])),
				"user.id",
				"posts.user_id",
			)
			.extras({
				upperTitle: (row) => row.title.toUpperCase(),
			})
			.omit(["id"]); // So we don't have to assert it

		const result = await postsQuerySet
			.insert(
				db
					.insertInto("posts")
					.values({
						user_id: 1,
						title: "new title",
						content: "new content",
					})
					.returning(["id", "title", "user_id"]),
			)
			.executeTakeFirst();

		assert.deepStrictEqual(result, {
			user_id: 1,
			title: "new title",
			upperTitle: "NEW TITLE",
			user: {
				id: 1,
			},
		});
	});
});
