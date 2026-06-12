import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { CardinalityViolationError, ExpectedOneItemError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Join Tests
//
// Consolidates the per-join-type suites (innerJoinMany, leftJoinMany,
// innerJoinOne, leftJoinOne, leftJoinOneOrThrow, crossJoinMany) and the
// mixed-join suite.
//
// - The join-type-INDEPENDENT contract (execution methods, toBaseQuery,
//   flat-row $$ hoisting) runs table-driven over every join type. Each
//   case pins its own hydrated AND flat-row literals, so no join type's
//   behavior is inferred from another's.
// - Join-type-DISTINGUISHING semantics (match/no-match shapes, base
//   filtering, cardinality violations, exists-false through a filtering
//   join) are hand-written.
// - Argument-form plumbing (onRef callback, pre-built QuerySet) is shared
//   machinery, tested once each.
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

describe("query-set: joins", () => {
	//
	// Shared contract, table-driven over every join type
	//

	for (const joinCase of JOIN_TYPES) {
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

	//
	// Argument forms (shared plumbing; tested once each)
	//

	test("join condition as a callback with onRef", async () => {
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

	test("joined collection as a pre-built QuerySet", async () => {
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
});
