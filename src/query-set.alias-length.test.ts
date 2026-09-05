/**
 * Tests for the query set's guard against over-long generated aliases (the
 * `maxAliasBytes` option). The guard runs when the query is built, so these
 * tests need no database round trip and run on every dialect.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin, sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { fixLongAliases } from "./fix-long-aliases.ts";
import { AliasTooLongError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

const utf8Bytes = (s: string) => Buffer.byteLength(s, "utf8");

// The longest generated alias is "<key>$$user_id": 63 bytes for KEY_54 and
// 64 bytes for KEY_55.
const KEY_54 = "postsWrittenByThisUserWithVerboseNamingConventionsForT";
const KEY_55 = "postsWrittenByThisUserWithVerboseNamingConventionsForTe";

function selectUserWithPostsUnder(
	key: string,
	dbToUse = db,
	options?: { maxAliasBytes?: number | null },
) {
	return querySet(dbToUse, options)
		.selectAs("user", dbToUse.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.leftJoinMany(
			key,
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			`${key}.user_id`,
			"user.id",
		);
}

describe("query-set: alias length guard", () => {
	assert.strictEqual(utf8Bytes(`${KEY_54}$$user_id`), 63);
	assert.strictEqual(utf8Bytes(`${KEY_55}$$user_id`), 64);

	test("allows generated aliases of exactly 63 bytes", () => {
		assert.doesNotThrow(() => selectUserWithPostsUnder(KEY_54).toQuery());
	});

	test("throws AliasTooLongError at toQuery() for a 64-byte generated alias", () => {
		assert.throws(
			() => selectUserWithPostsUnder(KEY_55).toQuery(),
			(error: unknown) =>
				error instanceof AliasTooLongError &&
				error.message.includes(`"${KEY_55}$$user_id" is 64 bytes`) &&
				error.message.includes("fixLongAliases()"),
		);
	});

	test("throws at toJoinedQuery() and execute() too", async () => {
		assert.throws(() => selectUserWithPostsUnder(KEY_55).toJoinedQuery(), AliasTooLongError);
		await assert.rejects(selectUserWithPostsUnder(KEY_55).execute(), AliasTooLongError);
	});

	test("measures the alias as the database will see it, after plugins", () => {
		// 58 bytes as written, 64 bytes once CamelCasePlugin snake_cases it.
		const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
			users: { id: number; username: string };
			posts: { id: number; title: string; userId: number };
		}>();
		const key = "employeeDirectoryEntries";
		assert.strictEqual(utf8Bytes(`${key}$$employeePreferredFullDisplayName`), 58);

		assert.throws(
			() =>
				querySet(camelDb)
					.selectAs("user", camelDb.selectFrom("users").select(["id", "username"]))
					.leftJoinMany(
						key,
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("posts")
									.select(["id", "userId", "title as employeePreferredFullDisplayName"]),
							),
						`${key}.userId`,
						"user.id",
					)
					.toQuery(),
			AliasTooLongError,
		);
	});

	test("passes once the fixLongAliases() plugin is installed", () => {
		const fixedDb = db.withPlugin(fixLongAliases());

		assert.doesNotThrow(() => selectUserWithPostsUnder(KEY_55, fixedDb).toQuery());
	});

	test("maxAliasBytes: null disables the guard", () => {
		assert.doesNotThrow(() =>
			selectUserWithPostsUnder(KEY_55, db, { maxAliasBytes: null }).toQuery(),
		);
	});

	test("maxAliasBytes can be lowered", () => {
		assert.throws(
			() => selectUserWithPostsUnder("posts", db, { maxAliasBytes: 10 }).toQuery(),
			AliasTooLongError,
		);
	});

	test("skips a raw alias, which cannot be measured, instead of rejecting the query", () => {
		// With no joins, ordering, or pagination the base query is returned as
		// is, so a raw alias reaches the guard; it must not throw where the
		// query previously built.
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", sql.lit(1).as(sql`"one"`)]) as any)
			.orderByKeys(false);

		assert.doesNotThrow(() => qs.toQuery());
	});

	test("ignores returningAll() on write bases, which output real columns", async () => {
		// Write query sets cannot be built without a database round trip, so
		// only assert that building the query does not throw.
		const write = querySet(db).insertAs("user", (qc) =>
			qc.insertInto("users").values({ username: "x", email: "x@example.com" }).returningAll(),
		);

		assert.doesNotThrow(() => write.toQuery());
	});
});
