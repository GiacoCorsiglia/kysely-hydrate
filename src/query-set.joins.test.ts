import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg } from "./__tests__/helpers.ts";
import { CardinalityViolationError, ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Join Tests
//
// Consolidates the per-join-type suites (innerJoinMany, leftJoinMany,
// innerJoinOne, leftJoinOne, leftJoinOneOrThrow, crossJoinMany), the
// mixed-join suite, and the lateral join types (in a PostgreSQL-only
// describePg suite at the bottom — SQLite has no LATERAL support).
//
// - The join-type-INDEPENDENT contract (execution methods, toBaseQuery,
//   flat-row $$ hoisting) runs table-driven over every join type. Each
//   case pins its own hydrated AND flat-row literals, so no join type's
//   behavior is inferred from another's.
// - Join-type-DISTINGUISHING semantics (match/no-match shapes, base
//   filtering, cardinality violations, exists-false through a filtering
//   join) are hand-written.
// - Argument forms (onRef callback, pre-built QuerySet) are covered per
//   join type where the original suite covered them (innerJoinMany,
//   leftJoinMany, innerJoinOne).
//
// Fixture facts used throughout: alice (1) has NO posts; bob (2) has posts
// 1, 2, 5, 12; carol (3) has posts 3, 15; every user has a profile with
// bio "Bio for user N" and profile id = user id.
//

// Expected hydrated shapes shared across tests.
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

// Posts 1 and 2 (both bob's), used as a small cross-join child set.
const CROSS_POSTS = [
	{ id: 1, title: "Post 1", user_id: 2 },
	{ id: 2, title: "Post 2", user_id: 2 },
];

function profileOf(userId: number) {
	return { id: userId, bio: `Bio for user ${userId}`, user_id: userId };
}

// Flat (toJoinedQuery) row shapes for users.id <= 2, shared across the table
// cases below.

// bob's four posts, exploded one row per post.
const FLAT_BOB_POST_ROWS = [
	{ id: 2, username: "bob", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 2 },
	{ id: 2, username: "bob", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 2 },
	{ id: 2, username: "bob", posts$$id: 5, posts$$title: "Post 5", posts$$user_id: 2 },
	{ id: 2, username: "bob", posts$$id: 12, posts$$title: "Post 12", posts$$user_id: 2 },
];

// alice's matchless left join: one row with null child columns.
const FLAT_ALICE_NULL_POST_ROW = {
	id: 1,
	username: "alice",
	posts$$id: null,
	posts$$title: null,
	posts$$user_id: null,
};

// alice and bob with their profiles (used by all three one-join cases).
const FLAT_PROFILE_ROWS = [
	{ id: 1, username: "alice", profile$$id: 1, profile$$bio: "Bio for user 1", profile$$user_id: 1 },
	{ id: 2, username: "bob", profile$$id: 2, profile$$bio: "Bio for user 2", profile$$user_id: 2 },
];

/** The standard base: users keyed by id, selecting id + username. */
function userBase(maxUserId: number) {
	return querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", maxUserId);
}

/** The slice of the QuerySet interface the table-driven loops consume. */
interface ExecutableQuerySet {
	execute(): Promise<unknown[]>;
	executeTakeFirst(): Promise<unknown>;
	executeCount(toNumber: (count: string | number | bigint) => number): Promise<number>;
	executeExists(): Promise<boolean>;
	toBaseQuery(): { execute(): Promise<unknown[]> };
	toJoinedQuery(): { orderBy(ref: any): { execute(): Promise<unknown[]> } };
}

interface JoinTypeCase {
	/** The join method under test (used as the test-name prefix). */
	name: string;
	/** Builds this join type's standard joined query over users.id <= maxUserId. */
	build: (maxUserId: number) => ExecutableQuerySet;
	/** Expected hydrated output for users.id <= 3. */
	hydrated: unknown[];
	/** Expected executeCount for users.id <= 3. */
	count: number;
	/**
	 * Child column used to pin a deterministic flat-row order (the compiled
	 * SQL orders only by the base key; child-row order is engine-dependent).
	 */
	joinedOrderBy: string;
	/** Expected toJoinedQuery rows for users.id <= 2, ordered by joinedOrderBy. */
	joinedRows: unknown[];
}

const JOIN_TYPES: JoinTypeCase[] = [
	{
		name: "innerJoinMany",
		build: (maxUserId) =>
			userBase(maxUserId).innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			),
		// alice has no posts and is filtered out by the inner join.
		hydrated: [
			{ id: 2, username: "bob", posts: BOB_POSTS },
			{ id: 3, username: "carol", posts: CAROL_POSTS },
		],
		count: 2,
		joinedOrderBy: "posts$$id",
		// alice contributes no rows; bob explodes to one row per post.
		joinedRows: FLAT_BOB_POST_ROWS,
	},
	{
		name: "leftJoinMany",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			),
		// alice is kept, with an empty collection.
		hydrated: [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: BOB_POSTS },
			{ id: 3, username: "carol", posts: CAROL_POSTS },
		],
		count: 3,
		joinedOrderBy: "posts$$id",
		// alice is kept as a single row with null child columns.
		joinedRows: [FLAT_ALICE_NULL_POST_ROW, ...FLAT_BOB_POST_ROWS],
	},
	{
		name: "innerJoinOne",
		build: (maxUserId) =>
			userBase(maxUserId).innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			),
		hydrated: [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
			{ id: 3, username: "carol", profile: profileOf(3) },
		],
		count: 3,
		joinedOrderBy: "profile$$id",
		joinedRows: FLAT_PROFILE_ROWS,
	},
	{
		name: "leftJoinOne",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			),
		hydrated: [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
			{ id: 3, username: "carol", profile: profileOf(3) },
		],
		count: 3,
		joinedOrderBy: "profile$$id",
		joinedRows: FLAT_PROFILE_ROWS,
	},
	{
		name: "leftJoinOneOrThrow",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			),
		hydrated: [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
			{ id: 3, username: "carol", profile: profileOf(3) },
		],
		count: 3,
		joinedOrderBy: "profile$$id",
		joinedRows: FLAT_PROFILE_ROWS,
	},
	{
		name: "crossJoinMany",
		build: (maxUserId) =>
			userBase(maxUserId).crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
			),
		// Cartesian product: every user gets the same full child set,
		// regardless of user_id.
		hydrated: [
			{ id: 1, username: "alice", posts: CROSS_POSTS },
			{ id: 2, username: "bob", posts: CROSS_POSTS },
			{ id: 3, username: "carol", posts: CROSS_POSTS },
		],
		count: 3,
		joinedOrderBy: "posts$$id",
		// 2 users × 2 posts = 4 rows.
		joinedRows: [
			{ id: 1, username: "alice", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 2 },
			{ id: 1, username: "alice", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 2 },
			{ id: 2, username: "bob", posts$$id: 1, posts$$title: "Post 1", posts$$user_id: 2 },
			{ id: 2, username: "bob", posts$$id: 2, posts$$title: "Post 2", posts$$user_id: 2 },
		],
	},
];

/**
 * Defines the six join-type-independent contract tests for one join type.
 * Used by both the plain-join table and the (pg-only) lateral-join table.
 */
function defineContractTests(joinCase: JoinTypeCase) {
	const { name, build, hydrated } = joinCase;

	test(`${name}: execute hydrates matched and unmatched base records`, async () => {
		const users = await build(3).execute();

		assert.deepStrictEqual(users, hydrated);
	});

	test(`${name}: executeTakeFirst returns the first hydrated entity`, async () => {
		const user = await build(3).executeTakeFirst();

		assert.deepStrictEqual(user, hydrated[0]);
	});

	test(`${name}: executeCount counts base records, not exploded rows`, async () => {
		const count = await build(3).executeCount(Number);

		assert.strictEqual(count, joinCase.count);
	});

	test(`${name}: toJoinedQuery returns flat rows with $$-prefixed child columns`, async () => {
		const rows = await build(2)
			.toJoinedQuery()
			// The compiled SQL orders only by the base key; child-row order is
			// engine-dependent, so pin it for the comparison.
			.orderBy(joinCase.joinedOrderBy)
			.execute();

		assert.deepStrictEqual(rows, joinCase.joinedRows);
	});

	test(`${name}: executeExists is true when matching base records exist`, async () => {
		const exists = await build(5).executeExists();

		assert.strictEqual(exists, true);
	});

	test(`${name}: toBaseQuery returns the base query without joins`, async () => {
		const rows = await build(2).toBaseQuery().execute();

		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});
}

describe("query-set: joins", () => {
	//
	// Shared contract, table-driven over every join type
	//

	for (const joinCase of JOIN_TYPES) {
		defineContractTests(joinCase);
	}

	//
	// Argument forms: the on-condition as an onRef callback, and the joined
	// collection as a pre-built QuerySet
	//

	test("innerJoinMany: join condition as a callback with onRef", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				(join) => join.onRef("posts.user_id", "=", "user.id"),
			)
			.where("users.id", "=", 2)
			.execute();

		assert.deepStrictEqual(users, [{ id: 2, username: "bob", posts: BOB_POSTS }]);
	});

	test("leftJoinMany: join condition as a callback with onRef", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				(join) => join.onRef("posts.user_id", "=", "user.id"),
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: BOB_POSTS },
		]);
	});

	test("innerJoinOne: join condition as a callback with onRef", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				(join) => join.onRef("profile.user_id", "=", "user.id"),
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
		]);
	});

	test("innerJoinMany: joined collection as a pre-built QuerySet", async () => {
		const postsQuery = querySet(db).selectAs("post", (eb) =>
			eb.selectFrom("posts").select(["id", "title", "user_id"]),
		);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany("posts", postsQuery, "posts.user_id", "user.id")
			.where("users.id", "=", 2)
			.execute();

		assert.deepStrictEqual(users, [{ id: 2, username: "bob", posts: BOB_POSTS }]);
	});

	test("leftJoinMany: joined collection as a pre-built QuerySet", async () => {
		const postsQuery = querySet(db).selectAs("post", (eb) =>
			eb.selectFrom("posts").select(["id", "title", "user_id"]),
		);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany("posts", postsQuery, "posts.user_id", "user.id")
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: BOB_POSTS },
		]);
	});

	test("innerJoinOne: joined collection as a pre-built QuerySet", async () => {
		const profileQuery = querySet(db).selectAs("profile", (eb) =>
			eb.selectFrom("profiles").select(["id", "bio", "user_id"]),
		);

		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne("profile", profileQuery, "profile.user_id", "user.id")
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
		]);
	});

	//
	// Flat rows: scenarios beyond the per-type table coverage
	//

	test("toJoinedQuery: matchless leftJoinOne shows null child columns", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					// No profile has this user_id
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 2)
			.toJoinedQuery()
			.execute();

		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice", profile$$id: null, profile$$bio: null, profile$$user_id: null },
			{ id: 2, username: "bob", profile$$id: null, profile$$bio: null, profile$$user_id: null },
		]);
	});

	test("toJoinedQuery: multiple one-joins show all prefixed columns side by side", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinOne(
				"primaryPost",
				({ eb, qs }) =>
					// Exactly one post per user
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 3])),
				"primaryPost.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.toJoinedQuery()
			.execute();

		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				profile$$id: 2,
				profile$$bio: "Bio for user 2",
				profile$$user_id: 2,
				primaryPost$$id: 1,
				primaryPost$$title: "Post 1",
				primaryPost$$user_id: 2,
			},
			{
				id: 3,
				username: "carol",
				profile$$id: 3,
				profile$$bio: "Bio for user 3",
				profile$$user_id: 3,
				primaryPost$$id: 3,
				primaryPost$$title: "Post 3",
				primaryPost$$user_id: 3,
			},
		]);
	});

	test("toJoinedQuery: sibling many-joins multiply rows (cartesian product)", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 3)),
				"posts.user_id",
				"user.id",
			)
			.innerJoinMany(
				"profiles",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profiles.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.toJoinedQuery()
			// The compiled SQL orders only by the base key; child-row order is
			// engine-dependent, so pin it for the comparison below
			.orderBy("posts$$id")
			.execute();

		// Row explosion: bob (2 posts × 1 profile = 2 rows) + carol (1 post × 1
		// profile = 1 row) = 3 rows
		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$user_id: 2,
				profiles$$id: 2,
				profiles$$bio: "Bio for user 2",
				profiles$$user_id: 2,
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 2,
				posts$$title: "Post 2",
				posts$$user_id: 2,
				profiles$$id: 2,
				profiles$$bio: "Bio for user 2",
				profiles$$user_id: 2,
			},
			{
				id: 3,
				username: "carol",
				posts$$id: 3,
				posts$$title: "Post 3",
				posts$$user_id: 3,
				profiles$$id: 3,
				profiles$$bio: "Bio for user 3",
				profiles$$user_id: 3,
			},
		]);
	});

	//
	// Cardinality-one semantics: no match
	//

	test("leftJoinOne: hydrates null when no row matches", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					// No profile has this user_id
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: null },
			{ id: 2, username: "bob", profile: null },
		]);
	});

	test("leftJoinOneOrThrow: rejects with ExpectedOneItemError when no row matches", async () => {
		await assert.rejects(
			querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.leftJoinOneOrThrow(
					"profile",
					({ eb, qs }) =>
						// No profile has this user_id
						qs(
							eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999),
						),
					"profile.user_id",
					"user.id",
				)
				.where("users.id", "=", 1)
				.execute(),
			ExpectedOneItemError,
		);
	});

	test("leftJoinOne: executeCount counts all base records when nothing matches", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					// No profile has this user_id
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 5)
			.executeCount(Number);

		// A left join never filters the base, even when it matches nothing
		assert.strictEqual(count, 5);
	});

	//
	// Cardinality-one semantics: multiple matches. Every "one" join enforces
	// at-most-one — multiple matching rows are a violation, not a
	// pick-the-first.
	//

	interface OneJoinCase {
		name: string;
		/** Joins bob's 4 posts as a (violated) cardinality-one collection. */
		build: () => ExecutableQuerySet;
	}

	// Base for the violation cases: bob only (id 2, who has 4 posts). The base
	// must not include alice — her zero posts would surface as a different
	// failure (ExpectedOneItemError / filtered out) before bob's violation.
	const bobOnly = () =>
		querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2);

	const ONE_JOINS: OneJoinCase[] = [
		{
			name: "innerJoinOne",
			build: () =>
				bobOnly().innerJoinOne(
					"post",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"post.user_id",
					"user.id",
				),
		},
		{
			name: "leftJoinOne",
			build: () =>
				bobOnly().leftJoinOne(
					"post",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"post.user_id",
					"user.id",
				),
		},
		{
			name: "leftJoinOneOrThrow",
			build: () =>
				bobOnly().leftJoinOneOrThrow(
					"post",
					({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
					"post.user_id",
					"user.id",
				),
		},
	];

	for (const oneJoinCase of ONE_JOINS) {
		test(`${oneJoinCase.name}: rejects with CardinalityViolationError when multiple rows match`, async () => {
			// bob (id 2) has 4 posts
			await assert.rejects(oneJoinCase.build().execute(), CardinalityViolationError);
		});
	}

	//
	// Inner-join filtering and exists-false through a filtering join
	//

	test("innerJoinOne: filters out base records without a match", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					// Only alice's and bob's profiles — carol must be dropped
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "<=", 2)),
				"profile.user_id",
				"user.id",
			)
			.where("users.id", "<=", 3)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
		]);
	});

	test("executeExists: false when an inner many-join filters out every base record", async () => {
		// alice exists but has no posts, so the joined query matches nothing
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "=", 1)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	test("executeExists: false when an inner one-join matches nothing", async () => {
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					// No profile has this user_id
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"profile.user_id",
				"user.id",
			)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	//
	// Cross-join specifics
	//

	test("crossJoinMany: parents sharing the same children receive independent arrays", async () => {
		// A cross join gives every parent the same child set.  Each parent must
		// still receive its own array (and its own child objects), so mutating one
		// parent's collection cannot corrupt a sibling's.  Pins the ownership
		// contract regardless of how hydration is implemented internally (e.g. a
		// future optimization caching identical child groups).
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.strictEqual(users.length, 2);
		assert.deepStrictEqual(users[0]!.posts, users[1]!.posts);
		assert.notStrictEqual(users[0]!.posts, users[1]!.posts);

		users[0]!.posts.pop();
		assert.strictEqual(users[0]!.posts.length, 1);
		assert.strictEqual(users[1]!.posts.length, 2);
	});

	test("crossJoinMany: an empty child set filters out every base record", async () => {
		// CROSS JOIN with an empty set returns no rows (correct SQL behavior) —
		// unlike leftJoinMany, there is no empty-collection fallback.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.crossJoinMany("posts", ({ eb, qs }) =>
				qs(
					// No post with this ID
					eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 999),
				),
			)
			.where("users.id", "=", 1)
			.execute();

		assert.deepStrictEqual(users, []);
	});

	//
	// Combinations of joins on one QuerySet
	//

	test("mixed: multiple innerJoinOne on the same QuerySet", async () => {
		// Posts 1, 3, 4 belong to bob, carol, and dave respectively — exactly one
		// post per user.  alice (no posts) is filtered out by the inner join.
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinOne(
				"primaryPost",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 3, 4]),
					),
				"primaryPost.user_id",
				"user.id",
			)
			.where("users.id", "<=", 4)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				profile: profileOf(2),
				primaryPost: { id: 1, title: "Post 1", user_id: 2 },
			},
			{
				id: 3,
				username: "carol",
				profile: profileOf(3),
				primaryPost: { id: 3, title: "Post 3", user_id: 3 },
			},
			{
				id: 4,
				username: "dave",
				profile: profileOf(4),
				primaryPost: { id: 4, title: "Post 4", user_id: 4 },
			},
		]);
	});

	test("mixed: a matchless leftJoinOne hydrates null beside a matching innerJoinOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) =>
					// No match
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]).where("user_id", "=", 999)),
				"profile.user_id",
				"user.id",
			)
			.innerJoinOne(
				"primaryPost",
				({ eb, qs }) =>
					// Exactly one post per user
					qs(
						eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "in", [1, 3, 4]),
					),
				"primaryPost.user_id",
				"user.id",
			)
			.where("users.id", "<=", 4)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				profile: null,
				primaryPost: { id: 1, title: "Post 1", user_id: 2 },
			},
			{
				id: 3,
				username: "carol",
				profile: null,
				primaryPost: { id: 3, title: "Post 3", user_id: 3 },
			},
			{
				id: 4,
				username: "dave",
				profile: null,
				primaryPost: { id: 4, title: "Post 4", user_id: 4 },
			},
		]);
	});

	test("mixed: sibling many-joins are deduplicated despite the cartesian product", async () => {
		// Post 1 has 2 comments × 1 user = 2 raw rows; the hydrator must
		// deduplicate the sibling "users" collection back to a single bob.
		const posts = await querySet(db)
			.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
			.innerJoinMany(
				"comments",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
				"comments.post_id",
				"post.id",
			)
			.innerJoinMany(
				"users",
				({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
				"users.id",
				"post.user_id",
			)
			.where("posts.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(posts, [
			{
				id: 1,
				title: "Post 1",
				user_id: 2,
				comments: [
					{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
					{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
				],
				// Appears once, not once per comment row
				users: [{ id: 2, username: "bob" }],
			},
			{
				id: 2,
				title: "Post 2",
				user_id: 2,
				comments: [{ id: 3, content: "Comment 3 on post 2", post_id: 2 }],
				users: [{ id: 2, username: "bob" }],
			},
		]);
	});

	test("mixed: innerJoinOne and innerJoinMany together", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.where("users.id", "<=", 4)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob", profile: profileOf(2), posts: BOB_POSTS },
			{ id: 3, username: "carol", profile: profileOf(3), posts: CAROL_POSTS },
			{
				id: 4,
				username: "dave",
				profile: profileOf(4),
				posts: [
					{ id: 4, title: "Post 4", user_id: 4 },
					{ id: 13, title: "Post 13", user_id: 4 },
				],
			},
		]);
	});

	test("mixed: leftJoinOne and leftJoinMany together", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.where("users.id", "<=", 2)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: profileOf(1), posts: [] },
			{ id: 2, username: "bob", profile: profileOf(2), posts: BOB_POSTS },
		]);
	});

	test("mixed: executeCount with multiple joins counts unique base records", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.where("users.id", "<=", 4);

		const count = await qs.executeCount(Number);
		const users = await qs.execute();
		const joinedRows = await qs.toJoinedQuery().execute();

		// Users with profiles AND posts: bob (2), carol (3), dave (4)
		assert.strictEqual(count, 3);
		assert.strictEqual(users.length, 3); // Verify count matches execute
		assert.ok(joinedRows.length > users.length); // Row explosion from many-join
	});

	test("mixed: pagination with multiple joins uses a nested subquery", async () => {
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.where("users.id", "<=", 4)
			.limit(2);

		const users = await query.execute();
		const allUsers = await query.clearLimit().execute();

		// Should return first 2 users with ALL their data
		assert.strictEqual(users.length, 2);
		assert.ok(users.length < allUsers.length);
		assert.deepStrictEqual(users, allUsers.slice(0, 2));
	});

	test("leftJoinOneOrThrow: pagination with a sibling many-join", async () => {
		// Regression test: "oneOrThrow" joins were misclassified as
		// cardinality-many, so they were excluded from the paginated inner
		// subquery and re-joined outside the limit.  They are cardinality-one and
		// belong inside it (like leftJoinOne).
		const query = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinOneOrThrow(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(2);

		// The oneOrThrow join must live inside the paginated subquery, so its
		// columns are hoisted through the subquery alias (not selected from a
		// join applied outside the limit).
		const { sql } = query.toQuery().compile();
		assert.ok(sql.includes('"user"."profile$$bio"'), sql);

		const users = await query.execute();

		assert.deepStrictEqual(users, [
			{ id: 1, username: "alice", profile: profileOf(1), posts: [] },
			{ id: 2, username: "bob", profile: profileOf(2), posts: BOB_POSTS },
		]);
	});

	test("mixed: toQuery without pagination compiles to the same SQL as toJoinedQuery", async () => {
		// The contract is that toQuery() adds no wrapping (subquery, limit,
		// dedup) when there is no pagination — it returns the joined query
		// unchanged. Compiled-SQL equality pins exactly that; executing both
		// and comparing rows would pass trivially even if they diverged into
		// two different-but-equivalent queries.
		const base = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			.where("users.id", "<=", 3);

		const queryCompiled = base.toQuery().compile();
		const joinedCompiled = base.toJoinedQuery().compile();

		assert.strictEqual(queryCompiled.sql, joinedCompiled.sql);
		assert.deepStrictEqual(queryCompiled.parameters, joinedCompiled.parameters);
	});

	test("mixed: toJoinedQuery vs toQuery without pagination are equivalent when executed", async () => {
		// Illustrative rather than load-bearing: without pagination toQuery()
		// returns the joined query unchanged, so both sides execute the same
		// compiled SQL (pinned as compiled-SQL equality in the test above). It
		// documents the equivalence for readers, especially next to the
		// with-pagination contrast test below.
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
				"posts.user_id",
				"user.id",
			);

		const joinedRows = await qs.toJoinedQuery().execute();
		const queryRows = await qs.toQuery().execute();

		// Without pagination, both should be identical (flat rows with prefixes)
		assert.deepStrictEqual(joinedRows, queryRows);
		assert.strictEqual(joinedRows.length, 2); // 2 posts
	});

	test("mixed: toJoinedQuery vs toQuery with pagination differ for many-joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.limit(1);

		const joinedRows = await qs.toJoinedQuery().execute();
		const queryRows = await qs.toQuery().execute();

		// toJoinedQuery does not apply pagination at all: it returns every
		// exploded row for both users (user 2 has 4 posts, user 3 has 2).
		assert.strictEqual(joinedRows.length, 6);

		// toQuery applies the limit to unique base records via a nested
		// subquery: 1 user (user 2, the lowest id), with all 4 of their
		// exploded post rows.
		assert.strictEqual(queryRows.length, 4);
		assert.ok(queryRows.every((row) => row.id === 2));
	});

	test("mixed: toBaseQuery strips multiple joins and hydration", async () => {
		const baseQuery = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 2)
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
			.toBaseQuery();

		// toBaseQuery() has no ORDER BY at all, so pin one for the comparison
		const rows = await baseQuery.orderBy("id").execute();

		// Should only have base columns, no joins applied
		assert.deepStrictEqual(rows, [
			{ id: 1, username: "alice" },
			{ id: 2, username: "bob" },
		]);
	});

	test("mixed: sibling many-joins over the same table under different keys", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.leftJoinMany(
				"allPosts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"allPosts.user_id",
				"user.id",
			)
			.execute();

		// Both collections read from "posts" but hydrate independently under
		// their own keys/prefixes.
		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob", posts: BOB_POSTS, allPosts: BOB_POSTS },
		]);
	});

	test("mixed: sibling one-joins over the same table under different keys", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.leftJoinOne(
				"profile2",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile2.user_id",
				"user.id",
			)
			.execute();

		assert.deepStrictEqual(users, [
			{ id: 2, username: "bob", profile: profileOf(2), profile2: profileOf(2) },
		]);
	});

	test("collection override: second join with the same key wins", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 1)),
				"posts.user_id",
				"user.id",
			)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "=", 2)),
				"posts.user_id",
				"user.id",
			)
			.execute();

		// Second posts join should override first
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [{ id: 2, title: "Post 2", user_id: 2 }],
			},
		]);
	});
});

//
// Lateral join types (PostgreSQL only)
//
// Laterals are the same join family with correlated subqueries, so they run
// through the same table-driven contract as the plain joins above, plus
// hand-written tests for the lateral-specific semantics (correlated
// references, ordering + limit inside the subquery, nesting). SQLite has no
// LATERAL support, so this whole suite runs only under
// HYDRATE_TEST_DB=postgres.
//

// The lateral subqueries below select only ["id", "title"] and keep each
// user's top-2 posts by id.
const BOB_TOP2_POSTS = [
	{ id: 1, title: "Post 1" },
	{ id: 2, title: "Post 2" },
];

const CAROL_TOP2_POSTS = [
	{ id: 3, title: "Post 3" },
	{ id: 15, title: "Post 15" },
];

const FLAT_LATERAL_BOB_POST_ROWS = [
	{ id: 2, username: "bob", posts$$id: 1, posts$$title: "Post 1" },
	{ id: 2, username: "bob", posts$$id: 2, posts$$title: "Post 2" },
];

const FLAT_LATERAL_ALICE_NULL_POST_ROW = {
	id: 1,
	username: "alice",
	posts$$id: null,
	posts$$title: null,
};

const LATERAL_JOIN_TYPES: JoinTypeCase[] = [
	{
		name: "innerJoinLateralMany",
		build: (maxUserId) =>
			userBase(maxUserId).innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			),
		// alice's correlated subquery yields no rows; the inner join drops her.
		hydrated: [
			{ id: 2, username: "bob", posts: BOB_TOP2_POSTS },
			{ id: 3, username: "carol", posts: CAROL_TOP2_POSTS },
		],
		count: 2,
		joinedOrderBy: "posts$$id",
		joinedRows: FLAT_LATERAL_BOB_POST_ROWS,
	},
	{
		name: "leftJoinLateralMany",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			),
		// alice is kept, with an empty collection.
		hydrated: [
			{ id: 1, username: "alice", posts: [] },
			{ id: 2, username: "bob", posts: BOB_TOP2_POSTS },
			{ id: 3, username: "carol", posts: CAROL_TOP2_POSTS },
		],
		count: 3,
		joinedOrderBy: "posts$$id",
		joinedRows: [FLAT_LATERAL_ALICE_NULL_POST_ROW, ...FLAT_LATERAL_BOB_POST_ROWS],
	},
	{
		name: "crossJoinLateralMany",
		build: (maxUserId) =>
			userBase(maxUserId).crossJoinLateralMany("posts", ({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						.select(["id", "title"])
						.whereRef("posts.user_id", "=", "user.id")
						.orderBy("posts.id")
						.limit(2),
				),
			),
		// Not a cartesian product: the subquery is correlated via whereRef, so a
		// cross lateral join behaves like an inner lateral join (alice's empty
		// subquery result removes her).
		hydrated: [
			{ id: 2, username: "bob", posts: BOB_TOP2_POSTS },
			{ id: 3, username: "carol", posts: CAROL_TOP2_POSTS },
		],
		count: 2,
		joinedOrderBy: "posts$$id",
		joinedRows: FLAT_LATERAL_BOB_POST_ROWS,
	},
	{
		name: "innerJoinLateralOne",
		build: (maxUserId) =>
			userBase(maxUserId).innerJoinLateralOne(
				"latestPost",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			),
		// alice has no posts and is dropped by the inner join.
		hydrated: [
			{ id: 2, username: "bob", latestPost: { id: 12, title: "Post 12" } },
			{ id: 3, username: "carol", latestPost: { id: 15, title: "Post 15" } },
		],
		count: 2,
		joinedOrderBy: "latestPost$$id",
		joinedRows: [{ id: 2, username: "bob", latestPost$$id: 12, latestPost$$title: "Post 12" }],
	},
	{
		name: "leftJoinLateralOne",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinLateralOne(
				"latestPost",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			),
		// alice is kept, with a null object.
		hydrated: [
			{ id: 1, username: "alice", latestPost: null },
			{ id: 2, username: "bob", latestPost: { id: 12, title: "Post 12" } },
			{ id: 3, username: "carol", latestPost: { id: 15, title: "Post 15" } },
		],
		count: 3,
		joinedOrderBy: "latestPost$$id",
		joinedRows: [
			{ id: 1, username: "alice", latestPost$$id: null, latestPost$$title: null },
			{ id: 2, username: "bob", latestPost$$id: 12, latestPost$$title: "Post 12" },
		],
	},
	{
		name: "leftJoinLateralOneOrThrow",
		build: (maxUserId) =>
			userBase(maxUserId).leftJoinLateralOneOrThrow(
				"profile",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("profiles")
							.select(["id", "bio", "user_id"])
							.whereRef("profiles.user_id", "=", "user.id")
							.limit(1),
					),
				(join) => join.onTrue(),
			),
		// Uses profiles (every user has one): oneOrThrow over posts would throw
		// for alice — that semantics is pinned in a hand-written test below.
		hydrated: [
			{ id: 1, username: "alice", profile: profileOf(1) },
			{ id: 2, username: "bob", profile: profileOf(2) },
			{ id: 3, username: "carol", profile: profileOf(3) },
		],
		count: 3,
		joinedOrderBy: "profile$$id",
		joinedRows: FLAT_PROFILE_ROWS,
	},
];

describePg("query-set: joins (lateral)", () => {
	//
	// Shared contract, table-driven over every lateral join type
	//

	for (const joinCase of LATERAL_JOIN_TYPES) {
		defineContractTests(joinCase);
	}

	//
	// Cardinality-one semantics
	//

	test("leftJoinLateralOneOrThrow: rejects with ExpectedOneItemError when no row matches", async () => {
		await assert.rejects(
			querySet(db)
				.selectAs("user", db.selectFrom("users").select(["id", "username"]))
				.where("users.id", "=", 1)
				.leftJoinLateralOneOrThrow(
					"latestPost",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "title"])
								.whereRef("posts.user_id", "=", "user.id")
								.orderBy("posts.id", "desc")
								.limit(1),
						),
					(join) => join.onTrue(),
				)
				.execute(),
			ExpectedOneItemError,
		);
	});

	interface LateralOneJoinCase {
		name: string;
		/** Joins bob's 4 posts (no limit) as a (violated) cardinality-one collection. */
		build: () => ExecutableQuerySet;
	}

	// Base for the violation cases: bob only (id 2, who has 4 posts) — the
	// correlated subqueries have NO limit, so they return all 4.
	const bobOnlyLateral = () =>
		querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2);

	const LATERAL_ONE_JOINS: LateralOneJoinCase[] = [
		{
			name: "innerJoinLateralOne",
			build: () =>
				bobOnlyLateral().innerJoinLateralOne(
					"post",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "title"])
								.whereRef("posts.user_id", "=", "user.id"),
						),
					(join) => join.onTrue(),
				),
		},
		{
			name: "leftJoinLateralOne",
			build: () =>
				bobOnlyLateral().leftJoinLateralOne(
					"post",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "title"])
								.whereRef("posts.user_id", "=", "user.id"),
						),
					(join) => join.onTrue(),
				),
		},
		{
			name: "leftJoinLateralOneOrThrow",
			build: () =>
				bobOnlyLateral().leftJoinLateralOneOrThrow(
					"post",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("posts")
								.select(["id", "title"])
								.whereRef("posts.user_id", "=", "user.id"),
						),
					(join) => join.onTrue(),
				),
		},
	];

	for (const oneJoinCase of LATERAL_ONE_JOINS) {
		test(`${oneJoinCase.name}: rejects with CardinalityViolationError when multiple rows match`, async () => {
			// bob (id 2) has 4 posts
			await assert.rejects(oneJoinCase.build().execute(), CardinalityViolationError);
		});
	}

	test("executeExists: false when an inner lateral join filters out every base record", async () => {
		// alice exists but has no posts, so the correlated subquery matches nothing
		const exists = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 1)
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
			)
			.executeExists();

		assert.strictEqual(exists, false);
	});

	//
	// Nested lateral joins
	//

	test("nested lateral joins: posts with comments at both levels", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).leftJoinLateralMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "content"])
									.whereRef("comments.post_id", "=", "posts.id")
									.orderBy("comments.id")
									.limit(2),
							),
						(join) => join.onTrue(),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{
						id: 1,
						title: "Post 1",
						comments: [
							{ id: 1, content: "Comment 1 on post 1" },
							{ id: 2, content: "Comment 2 on post 1" },
						],
					},
					{
						id: 2,
						title: "Post 2",
						comments: [{ id: 3, content: "Comment 3 on post 2" }],
					},
				],
			},
		]);
	});

	//
	// Hydration features with lateral joins
	//

	test("lateral joins with mapFields transformation", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).mapFields({
						title: (title) => title.toUpperCase(),
					}),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "POST 1" },
					{ id: 2, title: "POST 2" },
				],
			},
		]);
	});

	test("lateral joins with extras", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).extras({
						titleLength: (row) => row.title.length,
					}),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", titleLength: 6 },
					{ id: 2, title: "Post 2", titleLength: 6 },
				],
			},
		]);
	});

	test("lateral joins with omit", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title", "user_id"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					).omit(["user_id"]),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("nested lateral joins with transformations at multiple levels", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.mapFields({
				username: (username) => username.toUpperCase(),
			})
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					)
						.mapFields({
							title: (title) => title.toUpperCase(),
						})
						.extras({
							titleLength: (post) => post.title.length,
						})
						.innerJoinLateralMany(
							"comments",
							({ eb, qs }) =>
								qs(
									eb
										.selectFrom("comments")
										.select(["id", "content"])
										.whereRef("comments.post_id", "=", "posts.id")
										.orderBy("comments.id")
										.limit(1),
								)
									.mapFields({
										content: (content) => `[${content}]`,
									})
									.extras({
										contentLength: (comment) => comment.content.length,
									}),
							(join) => join.onTrue(),
						),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "BOB",
				posts: [
					{
						id: 1,
						title: "POST 1",
						titleLength: 6,
						comments: [
							{
								id: 1,
								content: "[Comment 1 on post 1]",
								contentLength: "Comment 1 on post 1".length,
							},
						],
					},
					{
						id: 2,
						title: "POST 2",
						titleLength: 6,
						comments: [
							{
								id: 3,
								content: "[Comment 3 on post 2]",
								contentLength: "Comment 3 on post 2".length,
							},
						],
					},
				],
			},
		]);
	});

	//
	// Multiple lateral joins on same QuerySet
	//

	test("multiple lateral joins: posts and comments as siblings", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.leftJoinLateralOne(
				"latestComment",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("comments")
							.select(["id", "content"])
							.whereRef("comments.user_id", "=", "user.id")
							.orderBy("comments.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
				latestComment: { id: 11, content: "Comment 11 on post 11" },
			},
		]);
	});

	//
	// Pagination with lateral joins
	//

	test("limit: limits base records with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		const users = await qs.limit(3).execute();
		const allUsers = await qs.execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(allUsers.length, 10);
		assert.deepStrictEqual(users, allUsers.slice(0, 3));
	});

	test("offset: skips base records with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		const users = await qs.offset(2).limit(3).execute();

		assert.strictEqual(users.length, 3);
		assert.strictEqual(users[0]?.id, 3);
		assert.strictEqual(users[1]?.id, 4);
		assert.strictEqual(users[2]?.id, 5);
	});

	test("pagination: limit + offset with lateral joins", async () => {
		const qs = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			);

		// Page 1: users 1-2
		const page1 = await qs.limit(2).execute();
		assert.strictEqual(page1.length, 2);
		assert.strictEqual(page1[0]?.id, 1);
		assert.strictEqual(page1[1]?.id, 2);

		// Page 2: users 3-4
		const page2 = await qs.offset(2).limit(2).execute();
		assert.strictEqual(page2.length, 2);
		assert.strictEqual(page2[0]?.id, 3);
		assert.strictEqual(page2[1]?.id, 4);
	});

	test("executeCount: ignores limit/offset with lateral joins", async () => {
		const count = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "<=", 3)
			.leftJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.limit(1)
			.offset(1)
			.executeCount(Number);

		assert.strictEqual(count, 3); // Counts all matching users, not just paginated
	});

	//
	// Flat rows beyond the per-type table coverage
	//

	test("toJoinedQuery: nested lateral joins show double prefixes", async () => {
		const rows = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(1),
					).innerJoinLateralMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("comments")
									.select(["id", "content"])
									.whereRef("comments.post_id", "=", "posts.id")
									.orderBy("comments.id")
									.limit(2),
							),
						(join) => join.onTrue(),
					),
				(join) => join.onTrue(),
			)
			.toJoinedQuery()
			// Post 1's two comments; order pinned for the comparison
			.orderBy("posts$$comments$$id")
			.execute();

		assert.deepStrictEqual(rows, [
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$comments$$id: 1,
				posts$$comments$$content: "Comment 1 on post 1",
			},
			{
				id: 2,
				username: "bob",
				posts$$id: 1,
				posts$$title: "Post 1",
				posts$$comments$$id: 2,
				posts$$comments$$content: "Comment 2 on post 1",
			},
		]);
	});

	//
	// Mixed lateral and regular joins
	//

	test("mixed: innerJoinOne and innerJoinLateralMany", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				profile: { id: 2, bio: "Bio for user 2", user_id: 2 },
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("mixed: innerJoinMany and innerJoinLateralOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"]).where("id", "<=", 2)),
				"posts.user_id",
				"user.id",
			)
			.innerJoinLateralOne(
				"latestComment",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("comments")
							.select(["id", "content"])
							.whereRef("comments.user_id", "=", "user.id")
							.orderBy("comments.id", "desc")
							.limit(1),
					),
				(join) => join.onTrue(),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", user_id: 2 },
					{ id: 2, title: "Post 2", user_id: 2 },
				],
				latestComment: { id: 11, content: "Comment 11 on post 11" },
			},
		]);
	});

	//
	// Collection modification with lateral joins
	//

	test("modify: add where clause to lateral join collection", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(5),
					),
				(join) => join.onTrue(),
			)
			.modify("posts", (qs) => qs.where("posts.id", "<=", 2))
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1" },
					{ id: 2, title: "Post 2" },
				],
			},
		]);
	});

	test("modify: add extras to lateral join collection", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"posts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("posts.id")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.modify("posts", (qs) =>
				qs.extras({
					titleLength: (row) => row.title.length,
				}),
			)
			.execute();

		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "bob",
				posts: [
					{ id: 1, title: "Post 1", titleLength: 6 },
					{ id: 2, title: "Post 2", titleLength: 6 },
				],
			},
		]);
	});

	//
	// Ordering + limit ("top N per group").  The query set's orderBy must be
	// applied INSIDE the lateral subquery (it determines which rows the limit
	// keeps) AND to the hydrated output order.
	//

	test("orderBy + limit on the nested query set: correct rows in correct order", async () => {
		const qs0 = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "in", [2, 3])
			.innerJoinLateralMany(
				"topPosts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title", "user_id"])
							.whereRef("posts.user_id", "=", "user.id"),
					)
						.orderBy("id", "desc")
						.limit(2),
				(join) => join.onTrue(),
			);

		// The ordering must appear inside the lateral subquery, before the limit.
		const { sql } = qs0.compile();
		assert.match(sql, /order by "topPosts"\."id" desc limit/);

		const users = await qs0.execute();

		// Bob's posts are 1, 2, 5, 12; Carol's are 3, 15.  Top-2 descending —
		// both the selected rows and their order.
		assert.deepStrictEqual(
			users.map((u) => ({ id: u.id, topPosts: u.topPosts.map((p) => p.id) })),
			[
				{ id: 2, topPosts: [12, 5] },
				{ id: 3, topPosts: [15, 3] },
			],
		);
	});

	test("orderBy on the raw inner subquery: selects the right rows but hydrates in key order", async () => {
		// Documented behavior (see the lateral JSDoc): an ORDER BY written
		// directly on the inner Kysely query controls which rows the LIMIT keeps,
		// but the hydrator cannot see it, so the hydrated array is re-sorted by
		// the nested query set's own orderings (by default, the keys, ascending).
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.where("users.id", "=", 2)
			.innerJoinLateralMany(
				"topPosts",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("posts")
							.select(["id", "title", "user_id"])
							.whereRef("posts.user_id", "=", "user.id")
							.orderBy("id", "desc")
							.limit(2),
					),
				(join) => join.onTrue(),
			)
			.execute();

		// The top-2 posts by id desc (5, 12) were selected, but hydrate ascending.
		assert.deepStrictEqual(
			users.map((u) => ({ id: u.id, topPosts: u.topPosts.map((p) => p.id) })),
			[{ id: 2, topPosts: [5, 12] }],
		);
	});
});
