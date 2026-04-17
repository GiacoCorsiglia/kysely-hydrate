/**
 * Tests for the lateral join optimizer mode.
 *
 * SQL generation tests run on both SQLite and Postgres (but use .compile()
 * so they never actually execute against the DB).
 *
 * Execution tests are Postgres-only since LATERAL is a Postgres feature.
 *
 * To run:
 *   npm test -- src/query-set.lateral-optimizer.test.ts
 *   HYDRATE_TEST_DB=postgres npm test -- src/query-set.lateral-optimizer.test.ts
 */

import assert from "node:assert";
import { afterEach, describe, test } from "node:test";

import { dialect, getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

describe("query-set: lateral-optimizer", () => {
	afterEach(() => {
		// Restore env-based default (if any) rather than clobbering it
		const envOptimizer = process.env.HYDRATE_OPTIMIZER_MODE;
		querySet.setDefaultOptions(envOptimizer ? { optimizer: envOptimizer as any } : {});
	});

	//
	// SQL generation — verify the optimizer rewrites joins correctly
	//

	test("SQL: innerJoinMany is rewritten to inner join lateral", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		assert.ok(sql.includes("join lateral"), `Expected JOIN LATERAL in: ${sql}`);
		assert.ok(sql.includes("on true"), `Expected ON true in: ${sql}`);
		assert.ok(
			sql.includes('"posts"."user_id" = "user"."id"'),
			`Expected WHERE predicate inside subquery in: ${sql}`,
		);
	});

	test("SQL: leftJoinMany is rewritten to left join lateral", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		assert.ok(sql.includes("left join lateral"), `Expected LEFT JOIN LATERAL in: ${sql}`);
		assert.ok(sql.includes("on true"), `Expected ON true in: ${sql}`);
	});

	test("SQL: leftJoinOne is rewritten to left join lateral", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		assert.ok(sql.includes("left join lateral"), `Expected LEFT JOIN LATERAL in: ${sql}`);
		assert.ok(sql.includes("on true"), `Expected ON true in: ${sql}`);
	});

	test("SQL: innerJoinOne is rewritten to inner join lateral", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		assert.ok(sql.includes("join lateral"), `Expected JOIN LATERAL in: ${sql}`);
		assert.ok(sql.includes("on true"), `Expected ON true in: ${sql}`);
	});

	test("SQL: already-lateral joins are not double-converted", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id"),
					),
				(join) => join.onTrue(),
			);

		const sql = qs.compile().sql;

		// Should still work — just one JOIN LATERAL, not nested
		assert.ok(sql.includes("join lateral"), `Expected JOIN LATERAL in: ${sql}`);
	});

	test("SQL: callback-based ON clause is not converted (only 2-arg refs)", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				(join) => join.onRef("posts.user_id", "=", "user.id"),
			);

		const sql = qs.compile().sql;

		// Callback form can't be auto-converted, so stays as regular join
		assert.ok(!sql.includes("lateral"), `Should NOT contain LATERAL in: ${sql}`);
	});

	test("SQL: default optimizer does not convert to lateral", () => {
		const qs = querySet(db, { optimizer: "default" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		assert.ok(!sql.includes("lateral"), `Should NOT contain LATERAL in: ${sql}`);
	});

	//
	// setDefaultOptions
	//

	test("setDefaultOptions: applies lateralJoin globally", () => {
		querySet.setDefaultOptions({ optimizer: "lateralJoin" });

		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;
		assert.ok(sql.includes("join lateral"), `Expected JOIN LATERAL in: ${sql}`);
	});

	test("setDefaultOptions: per-instance overrides global default", () => {
		querySet.setDefaultOptions({ optimizer: "lateralJoin" });

		const qs = querySet(db, { optimizer: "default" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;
		assert.ok(!sql.includes("lateral"), `Should NOT contain LATERAL in: ${sql}`);
	});

	//
	// Options propagation through nested qs factory
	//

	test("SQL: options propagate to nested joins via qs factory", () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			);

		const sql = qs.compile().sql;

		// Both the outer and inner joins should be lateral
		const lateralCount = (sql.match(/join lateral/g) || []).length;
		assert.ok(
			lateralCount >= 2,
			`Expected at least 2 JOIN LATERAL, got ${lateralCount} in: ${sql}`,
		);
	});
});

//
// Postgres execution tests — verify the optimizer produces correct results
//

const shouldSkipPg = dialect !== "postgres";

describe("query-set: lateral-optimizer (postgres execution)", { skip: shouldSkipPg }, () => {
	const db = getDbForTest();

	afterEach(() => {
		// Restore env-based default (if any) rather than clobbering it
		const envOptimizer = process.env.HYDRATE_OPTIMIZER_MODE;
		querySet.setDefaultOptions(envOptimizer ? { optimizer: envOptimizer as any } : {});
	});

	test("innerJoinMany with lateralJoin: same results as default", async () => {
		const base = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2);

		const defaultResult = await base
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		const lateralResult = await querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(lateralResult, defaultResult);
	});

	test("leftJoinMany with lateralJoin: same results as default", async () => {
		const defaultResult = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		const lateralResult = await querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2])
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(lateralResult, defaultResult);
	});

	test("innerJoinOne with lateralJoin: same results as default", async () => {
		const defaultResult = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		const lateralResult = await querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(lateralResult, defaultResult);
	});

	test("mixed cardinality: one + many with lateralJoin", async () => {
		const lateralResult = await querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(lateralResult.length, 1);
		assert.strictEqual(lateralResult[0]!.username, "bob");
		assert.strictEqual(lateralResult[0]!.profile.bio, "Bio for user 2");
		assert.ok(lateralResult[0]!.posts.length > 0);
	});

	test("nested joins with lateralJoin: posts with comments", async () => {
		const lateralResult = await querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2),
					).leftJoinMany(
						"comments",
						({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.execute();

		assert.strictEqual(lateralResult.length, 1);
		const user = lateralResult[0]!;
		assert.strictEqual(user.posts.length, 2);
		// Post 1 should have comments
		assert.ok(user.posts[0]!.comments.length > 0);
	});

	test("pagination + lateralJoin", async () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const page1 = await qs.limit(2).execute();
		const page2 = await qs.offset(2).limit(2).execute();
		const all = await qs.execute();

		assert.strictEqual(page1.length, 2);
		assert.strictEqual(page2.length, 2);
		assert.strictEqual(all.length, 10);
		assert.deepStrictEqual(page1, all.slice(0, 2));
		assert.deepStrictEqual(page2, all.slice(2, 4));
	});

	test("executeCount + lateralJoin", async () => {
		const qs = querySet(db, { optimizer: "lateralJoin" })
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [1, 2, 3])
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();

		assert.strictEqual(count, users.length);
	});
});
