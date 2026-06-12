import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest({ fixture: "order-by-fixture" });

//
// ORDER BY Tests
//
// These tests verify that the orderBy, clearOrderBy, and orderByKeys methods
// work correctly in different scenarios.
//
// NOTE: This test file uses a dedicated fixture with RANDOMIZED data to ensure
// ordering is truly tested. In the fixture:
// - Users inserted as: grace, alice, ivan, eve, carol, bob, judy, frank, dave, heidi
// - So ID order ≠ alphabetical order
//

//
// No joins - basic ordering
//

describe("query-set: order-by", () => {
	test("orderBy: orders by single column ascending", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.orderBy("username", "asc")
			.execute();

		// Alphabetically: alice(2), bob(6), carol(5), dave(9), eve(4), frank(8), grace(1), heidi(10), ivan(3), judy(7)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice" },
			{ id: 6, username: "bob" },
			{ id: 5, username: "carol" },
			{ id: 9, username: "dave" },
			{ id: 4, username: "eve" },
			{ id: 8, username: "frank" },
			{ id: 1, username: "grace" },
			{ id: 10, username: "heidi" },
			{ id: 3, username: "ivan" },
			{ id: 7, username: "judy" },
		]);
	});

	test("orderBy: orders by single column descending", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.orderBy("username", "desc")
			.execute();

		// Reverse alphabetically: judy(7), ivan(3), heidi(10), grace(1), frank(8), eve(4), dave(9), carol(5), bob(6), alice(2)
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy" },
			{ id: 3, username: "ivan" },
			{ id: 10, username: "heidi" },
			{ id: 1, username: "grace" },
			{ id: 8, username: "frank" },
			{ id: 4, username: "eve" },
			{ id: 9, username: "dave" },
			{ id: 5, username: "carol" },
			{ id: 6, username: "bob" },
			{ id: 2, username: "alice" },
		]);
	});

	test("orderBy: orders by multiple columns", async () => {
		// Each user commented twice, so user_id has real ties and the secondary
		// column decides. With the secondary ignored, the id-asc tiebreaker
		// would instead produce [3, 7, 1, 5, 4, 8, 2, 6].
		const comments = await querySet(db)
			.selectAs("comment", db.selectFrom("comments").select(["id", "user_id", "content"]))
			.orderBy("user_id", "asc")
			.orderBy("content", "desc")
			.execute();

		assert.deepStrictEqual(comments, [
			{ id: 3, user_id: 4, content: "Comment on zeta by eve" },
			{ id: 7, user_id: 4, content: "Comment on kappa by eve" },
			{ id: 5, user_id: 5, content: "Comment on theta by carol" },
			{ id: 1, user_id: 5, content: "Comment on gamma by carol" },
			{ id: 8, user_id: 6, content: "Comment on epsilon by bob" },
			{ id: 4, user_id: 6, content: "Comment on beta by bob" },
			{ id: 6, user_id: 9, content: "Comment on delta by dave" },
			{ id: 2, user_id: 9, content: "Comment on alpha by dave" },
		]);
	});

	test("orderByKeys disabled: output preserves the base query's own order", async () => {
		// The base query carries a TOTAL order of its own (user_id asc, id desc
		// within ties). With orderByKeys(false) and no .orderBy(), the QuerySet
		// adds no ordering anywhere — SQL or JS — so the output preserves the
		// base query's order exactly. If keyBy ordering leaked in, the result
		// would be re-sorted to id asc ([1..8]).
		const comments = await querySet(db)
			.selectAs(
				"comment",
				db
					.selectFrom("comments")
					.select(["id", "user_id", "content"])
					.orderBy("user_id", "asc")
					.orderBy("id", "desc"),
			)
			.orderByKeys(false)
			.execute();

		assert.deepStrictEqual(
			comments.map((c) => c.id),
			[7, 3, 5, 1, 8, 4, 6, 2],
		);
	});

	test("orderBy: with orderByKeys disabled", async () => {
		// With an explicit orderBy, disabling key ordering drops the keyBy
		// tiebreaker — pinned in the compiled SQL. Tie order in the output is
		// genuinely unspecified here (having no tiebreaker is the point), so
		// only the user_id grouping is asserted; the deterministic-output case
		// is the test above.
		const qs = querySet(db)
			.selectAs("comment", db.selectFrom("comments").select(["id", "user_id", "content"]))
			.orderByKeys(false)
			.orderBy("user_id", "asc");

		const sql = qs.toQuery().compile().sql;
		assert.ok(sql.endsWith('order by "comment"."user_id" asc'), sql);

		// The explicit ordering still applies; within ties, order is unspecified
		const comments = await qs.execute();
		assert.deepStrictEqual(
			comments.map((c) => c.user_id),
			[4, 4, 5, 5, 6, 6, 9, 9],
		);
	});

	test("orderBy: with default keyBy ordering", async () => {
		// The base query feeds rows in id-DESC order, so the default keyBy
		// ordering must actively re-sort the output to id asc — it can't pass
		// by riding on engine scan order (which this base query inverts)
		const qs = querySet(db)
			// No explicit orderBy, should default to ordering by id (keyBy)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]).orderBy("id", "desc"));

		// The keyBy ordering is also emitted on the outer query (for toQuery
		// consumers streaming the flat rows)
		const sql = qs.toQuery().compile().sql;
		assert.ok(sql.endsWith('order by "user"."id" asc'), sql);

		const users = await qs.execute();

		// Should be ordered by id ascending (default keyBy)
		// grace(1), alice(2), ivan(3), eve(4), carol(5), bob(6), judy(7), frank(8), dave(9), heidi(10)
		assert.deepStrictEqual(users, [
			{ id: 1, username: "grace" },
			{ id: 2, username: "alice" },
			{ id: 3, username: "ivan" },
			{ id: 4, username: "eve" },
			{ id: 5, username: "carol" },
			{ id: 6, username: "bob" },
			{ id: 7, username: "judy" },
			{ id: 8, username: "frank" },
			{ id: 9, username: "dave" },
			{ id: 10, username: "heidi" },
		]);
	});

	test("clearOrderBy: removes custom ordering but keeps keyBy ordering", async () => {
		// Keyed by username, so the keyBy ordering (alphabetical) is
		// distinguishable in the output from both the cleared ordering (id
		// desc, [10..1]) and engine scan order (id asc, [1..10])
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]), "username")
			.orderBy("id", "desc")
			.clearOrderBy()
			.execute();

		// Should revert to keyBy ordering (username asc)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice" },
			{ id: 6, username: "bob" },
			{ id: 5, username: "carol" },
			{ id: 9, username: "dave" },
			{ id: 4, username: "eve" },
			{ id: 8, username: "frank" },
			{ id: 1, username: "grace" },
			{ id: 10, username: "heidi" },
			{ id: 3, username: "ivan" },
			{ id: 7, username: "judy" },
		]);
	});

	test("orderByKeys: can be re-enabled after being disabled", async () => {
		// Keyed by username, so the re-enabled keyBy ordering (alphabetical) is
		// distinguishable in the output from engine scan order (id asc)
		const disabled = querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]), "username")
			.orderByKeys(false);
		const reEnabled = disabled.orderByKeys(true);

		// Disabled: no ordering at all
		assert.ok(!disabled.toQuery().compile().sql.includes("order by"));

		const users = await reEnabled.execute();

		// Should be ordered by username (keyBy re-enabled)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice" },
			{ id: 6, username: "bob" },
			{ id: 5, username: "carol" },
			{ id: 9, username: "dave" },
			{ id: 4, username: "eve" },
			{ id: 8, username: "frank" },
			{ id: 1, username: "grace" },
			{ id: 10, username: "heidi" },
			{ id: 3, username: "ivan" },
			{ id: 7, username: "judy" },
		]);
	});

	//
	// Cardinality-one joins
	//

	test("orderBy: orders by base column with innerJoinOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.orderBy("username", "desc")
			.execute();

		// Alphabetically descending
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy", profile: { id: 1, bio: "Bio for judy", user_id: 7 } },
			{ id: 3, username: "ivan", profile: { id: 8, bio: "Bio for ivan", user_id: 3 } },
			{ id: 10, username: "heidi", profile: { id: 7, bio: "Bio for heidi", user_id: 10 } },
			{ id: 1, username: "grace", profile: { id: 5, bio: "Bio for grace", user_id: 1 } },
			{ id: 8, username: "frank", profile: { id: 10, bio: "Bio for frank", user_id: 8 } },
			{ id: 4, username: "eve", profile: { id: 4, bio: "Bio for eve", user_id: 4 } },
			{ id: 9, username: "dave", profile: { id: 3, bio: "Bio for dave", user_id: 9 } },
			{ id: 5, username: "carol", profile: { id: 9, bio: "Bio for carol", user_id: 5 } },
			{ id: 6, username: "bob", profile: { id: 6, bio: "Bio for bob", user_id: 6 } },
			{ id: 2, username: "alice", profile: { id: 2, bio: "Bio for alice", user_id: 2 } },
		]);
	});

	test("orderBy: orders by joined column with innerJoinOne", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
				"profile.user_id",
				"user.id",
			)
			.orderBy("profile$$bio", "asc")
			.execute();

		// Ordered by bio alphabetically (alice, bob, carol, dave, eve, frank, grace, heidi, ivan, judy)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice", profile: { id: 2, bio: "Bio for alice", user_id: 2 } },
			{ id: 6, username: "bob", profile: { id: 6, bio: "Bio for bob", user_id: 6 } },
			{ id: 5, username: "carol", profile: { id: 9, bio: "Bio for carol", user_id: 5 } },
			{ id: 9, username: "dave", profile: { id: 3, bio: "Bio for dave", user_id: 9 } },
			{ id: 4, username: "eve", profile: { id: 4, bio: "Bio for eve", user_id: 4 } },
			{ id: 8, username: "frank", profile: { id: 10, bio: "Bio for frank", user_id: 8 } },
			{ id: 1, username: "grace", profile: { id: 5, bio: "Bio for grace", user_id: 1 } },
			{ id: 10, username: "heidi", profile: { id: 7, bio: "Bio for heidi", user_id: 10 } },
			{ id: 3, username: "ivan", profile: { id: 8, bio: "Bio for ivan", user_id: 3 } },
			{ id: 7, username: "judy", profile: { id: 1, bio: "Bio for judy", user_id: 7 } },
		]);
	});

	test("orderBy: orders by multiple columns including joined columns with leftJoinOne", async () => {
		// Comments 1 and 3 each have two replies, so the joined primary column
		// (the comment's content) has real ties and the secondary column
		// decides. With the secondary ignored, the id-asc tiebreaker would
		// instead produce [6, 2, 4, 3, 1, 5].
		const replies = await querySet(db)
			.selectAs("reply", db.selectFrom("replies").select(["id", "comment_id", "user_id"]))
			.leftJoinOne(
				"comment",
				({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content"])),
				"comment.id",
				"reply.comment_id",
			)
			.orderBy("comment$$content", "asc")
			.orderBy("user_id", "asc")
			.execute();

		assert.deepStrictEqual(replies, [
			{ id: 6, comment_id: 6, user_id: 6, comment: { id: 6, content: "Comment on delta by dave" } },
			{
				id: 4,
				comment_id: 1,
				user_id: 4,
				comment: { id: 1, content: "Comment on gamma by carol" },
			},
			{
				id: 2,
				comment_id: 1,
				user_id: 9,
				comment: { id: 1, content: "Comment on gamma by carol" },
			},
			{
				id: 3,
				comment_id: 5,
				user_id: 4,
				comment: { id: 5, content: "Comment on theta by carol" },
			},
			{ id: 5, comment_id: 3, user_id: 5, comment: { id: 3, content: "Comment on zeta by eve" } },
			{ id: 1, comment_id: 3, user_id: 6, comment: { id: 3, content: "Comment on zeta by eve" } },
		]);
	});

	//
	// Cardinality-many joins
	//

	test("orderBy: orders base records with leftJoinMany", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.orderBy("username", "desc")
			.execute();

		// Base records should be ordered by username desc
		// Posts within each user are ordered by their id (default keyBy)
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy", posts: [] },
			{ id: 3, username: "ivan", posts: [] },
			{ id: 10, username: "heidi", posts: [] },
			{ id: 1, username: "grace", posts: [] },
			{ id: 8, username: "frank", posts: [] },
			{
				id: 4,
				username: "eve",
				posts: [
					{ id: 7, title: "Post Eta", user_id: 4 },
					{ id: 9, title: "Post Iota", user_id: 4 },
				],
			},
			{
				id: 9,
				username: "dave",
				posts: [
					{ id: 2, title: "Post Beta", user_id: 9 },
					{ id: 5, title: "Post Epsilon", user_id: 9 },
				],
			},
			{
				id: 5,
				username: "carol",
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
			{
				id: 6,
				username: "bob",
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{ id: 2, username: "alice", posts: [] },
		]);
	});

	test("orderBy: orders base records with innerJoinMany", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.orderBy("username", "asc")
			.execute();

		// Only users with posts: bob(6), carol(5), dave(9), eve(4)
		// Ordered alphabetically: bob, carol, dave, eve
		assert.deepStrictEqual(users, [
			{
				id: 6,
				username: "bob",
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{
				id: 5,
				username: "carol",
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
			{
				id: 9,
				username: "dave",
				posts: [
					{ id: 2, title: "Post Beta", user_id: 9 },
					{ id: 5, title: "Post Epsilon", user_id: 9 },
				],
			},
			{
				id: 4,
				username: "eve",
				posts: [
					{ id: 7, title: "Post Eta", user_id: 4 },
					{ id: 9, title: "Post Iota", user_id: 4 },
				],
			},
		]);
	});

	//
	// Mixed joins (cardinality-one + cardinality-many)
	//

	test("orderBy: orders by base and cardinality-one joined columns with cardinality-many join", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
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
			.orderBy("profile$$bio", "desc")
			.execute();

		// Should be ordered by profile.bio descending
		// judy, ivan, heidi, grace, frank, eve, dave, carol, bob, alice
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy", profile: { id: 1, bio: "Bio for judy", user_id: 7 }, posts: [] },
			{ id: 3, username: "ivan", profile: { id: 8, bio: "Bio for ivan", user_id: 3 }, posts: [] },
			{
				id: 10,
				username: "heidi",
				profile: { id: 7, bio: "Bio for heidi", user_id: 10 },
				posts: [],
			},
			{
				id: 1,
				username: "grace",
				profile: { id: 5, bio: "Bio for grace", user_id: 1 },
				posts: [],
			},
			{
				id: 8,
				username: "frank",
				profile: { id: 10, bio: "Bio for frank", user_id: 8 },
				posts: [],
			},
			{
				id: 4,
				username: "eve",
				profile: { id: 4, bio: "Bio for eve", user_id: 4 },
				posts: [
					{ id: 7, title: "Post Eta", user_id: 4 },
					{ id: 9, title: "Post Iota", user_id: 4 },
				],
			},
			{
				id: 9,
				username: "dave",
				profile: { id: 3, bio: "Bio for dave", user_id: 9 },
				posts: [
					{ id: 2, title: "Post Beta", user_id: 9 },
					{ id: 5, title: "Post Epsilon", user_id: 9 },
				],
			},
			{
				id: 5,
				username: "carol",
				profile: { id: 9, bio: "Bio for carol", user_id: 5 },
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
			{
				id: 6,
				username: "bob",
				profile: { id: 6, bio: "Bio for bob", user_id: 6 },
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{
				id: 2,
				username: "alice",
				profile: { id: 2, bio: "Bio for alice", user_id: 2 },
				posts: [],
			},
		]);
	});

	//
	// Ordering with pagination
	//

	test("orderBy: works correctly with limit", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.orderBy("username", "desc")
			.limit(3)
			.execute();

		// First 3 when ordered by username desc: judy, ivan, heidi
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy" },
			{ id: 3, username: "ivan" },
			{ id: 10, username: "heidi" },
		]);
	});

	test("orderBy: works correctly with limit and offset", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.orderBy("username", "asc")
			.limit(3)
			.offset(2)
			.execute();

		// Alphabetically: alice, bob, carol, dave, eve, frank, grace, heidi, ivan, judy
		// Skip 2 (alice, bob), take 3: carol, dave, eve
		assert.deepStrictEqual(users, [
			{ id: 5, username: "carol" },
			{ id: 9, username: "dave" },
			{ id: 4, username: "eve" },
		]);
	});

	test("orderBy: works correctly with leftJoinMany and pagination", async () => {
		// bob and carol have 4 and 2 posts (6 exploded rows), so a limit
		// applied to rows instead of entities would truncate the result
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
				"posts.user_id",
				"user.id",
			)
			.orderBy("username", "asc")
			.offset(1)
			.limit(2)
			.execute();

		// Ordered by username asc, skip alice, take 2: bob, carol
		assert.deepStrictEqual(users, [
			{
				id: 6,
				username: "bob",
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{
				id: 5,
				username: "carol",
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
		]);
	});

	//
	// Nested joins
	//

	test("orderBy: with nested one-many (user -> profile -> posts)", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
				"profile",
				({ eb, qs }) =>
					qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])).leftJoinMany(
						"posts",
						({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
						"posts.user_id",
						"profile.user_id",
					),
				"profile.user_id",
				"user.id",
			)
			.orderBy("username", "asc")
			.execute();

		// Users ordered alphabetically: alice, bob, carol, dave, eve, frank, grace, heidi, ivan, judy
		// Profile posts should be ordered by post id (their keyBy)
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "alice",
				profile: { id: 2, bio: "Bio for alice", user_id: 2, posts: [] },
			},
			{
				id: 6,
				username: "bob",
				profile: {
					id: 6,
					bio: "Bio for bob",
					user_id: 6,
					posts: [
						{ id: 3, title: "Post Gamma", user_id: 6 },
						{ id: 6, title: "Post Zeta", user_id: 6 },
						{ id: 8, title: "Post Theta", user_id: 6 },
						{ id: 10, title: "Post Kappa", user_id: 6 },
					],
				},
			},
			{
				id: 5,
				username: "carol",
				profile: {
					id: 9,
					bio: "Bio for carol",
					user_id: 5,
					posts: [
						{ id: 1, title: "Post Alpha", user_id: 5 },
						{ id: 4, title: "Post Delta", user_id: 5 },
					],
				},
			},
			{
				id: 9,
				username: "dave",
				profile: {
					id: 3,
					bio: "Bio for dave",
					user_id: 9,
					posts: [
						{ id: 2, title: "Post Beta", user_id: 9 },
						{ id: 5, title: "Post Epsilon", user_id: 9 },
					],
				},
			},
			{
				id: 4,
				username: "eve",
				profile: {
					id: 4,
					bio: "Bio for eve",
					user_id: 4,
					posts: [
						{ id: 7, title: "Post Eta", user_id: 4 },
						{ id: 9, title: "Post Iota", user_id: 4 },
					],
				},
			},
			{
				id: 8,
				username: "frank",
				profile: { id: 10, bio: "Bio for frank", user_id: 8, posts: [] },
			},
			{
				id: 1,
				username: "grace",
				profile: { id: 5, bio: "Bio for grace", user_id: 1, posts: [] },
			},
			{
				id: 10,
				username: "heidi",
				profile: { id: 7, bio: "Bio for heidi", user_id: 10, posts: [] },
			},
			{
				id: 3,
				username: "ivan",
				profile: { id: 8, bio: "Bio for ivan", user_id: 3, posts: [] },
			},
			{
				id: 7,
				username: "judy",
				profile: { id: 1, bio: "Bio for judy", user_id: 7, posts: [] },
			},
		]);
	});

	test("orderBy: with nested many-many (user -> posts -> comments)", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"])),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.orderBy("username", "desc")
			.execute();

		// Users ordered reverse alphabetically
		// Posts within each user ordered by their id (keyBy)
		// Comments within each post ordered by their id (keyBy)
		assert.deepStrictEqual(users, [
			{ id: 7, username: "judy", posts: [] },
			{ id: 3, username: "ivan", posts: [] },
			{ id: 10, username: "heidi", posts: [] },
			{ id: 1, username: "grace", posts: [] },
			{ id: 8, username: "frank", posts: [] },
			{
				id: 4,
				username: "eve",
				posts: [
					{
						id: 7,
						title: "Post Eta",
						user_id: 4,
						comments: [],
					},
					{
						id: 9,
						title: "Post Iota",
						user_id: 4,
						comments: [],
					},
				],
			},
			{
				id: 9,
				username: "dave",
				posts: [
					{
						id: 2,
						title: "Post Beta",
						user_id: 9,
						comments: [{ id: 4, content: "Comment on beta by bob", post_id: 2, user_id: 6 }],
					},
					{
						id: 5,
						title: "Post Epsilon",
						user_id: 9,
						comments: [{ id: 8, content: "Comment on epsilon by bob", post_id: 5, user_id: 6 }],
					},
				],
			},
			{
				id: 5,
				username: "carol",
				posts: [
					{
						id: 1,
						title: "Post Alpha",
						user_id: 5,
						comments: [{ id: 2, content: "Comment on alpha by dave", post_id: 1, user_id: 9 }],
					},
					{
						id: 4,
						title: "Post Delta",
						user_id: 5,
						comments: [{ id: 6, content: "Comment on delta by dave", post_id: 4, user_id: 9 }],
					},
				],
			},
			{
				id: 6,
				username: "bob",
				posts: [
					{
						id: 3,
						title: "Post Gamma",
						user_id: 6,
						comments: [{ id: 1, content: "Comment on gamma by carol", post_id: 3, user_id: 5 }],
					},
					{
						id: 6,
						title: "Post Zeta",
						user_id: 6,
						comments: [{ id: 3, content: "Comment on zeta by eve", post_id: 6, user_id: 4 }],
					},
					{
						id: 8,
						title: "Post Theta",
						user_id: 6,
						comments: [{ id: 5, content: "Comment on theta by carol", post_id: 8, user_id: 5 }],
					},
					{
						id: 10,
						title: "Post Kappa",
						user_id: 6,
						comments: [{ id: 7, content: "Comment on kappa by eve", post_id: 10, user_id: 4 }],
					},
				],
			},
			{ id: 2, username: "alice", posts: [] },
		]);
	});

	test("orderBy: with nested many-many-many (user -> posts -> comments -> replies)", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.leftJoinMany(
				"posts",
				({ eb, qs }) =>
					qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
						"comments",
						({ eb, qs }) =>
							qs(
								eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"]),
							).leftJoinMany(
								"replies",
								({ eb, qs }) =>
									qs(eb.selectFrom("replies").select(["id", "content", "comment_id", "user_id"])),
								"replies.comment_id",
								"comments.id",
							),
						"comments.post_id",
						"posts.id",
					),
				"posts.user_id",
				"user.id",
			)
			.orderBy("username", "asc")
			.execute();

		// Users ordered alphabetically: alice, bob, carol, dave, eve, frank, grace, heidi, ivan, judy
		// Posts within each user ordered by their id (keyBy)
		// Comments within each post ordered by their id (keyBy)
		// Replies within each comment ordered by their id (keyBy)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice", posts: [] },
			{
				id: 6,
				username: "bob",
				posts: [
					{
						id: 3,
						title: "Post Gamma",
						user_id: 6,
						comments: [
							{
								id: 1,
								content: "Comment on gamma by carol",
								post_id: 3,
								user_id: 5,
								replies: [
									{ id: 2, content: "Reply to gamma comment by dave", comment_id: 1, user_id: 9 },
									{ id: 4, content: "Another reply to gamma by eve", comment_id: 1, user_id: 4 },
								],
							},
						],
					},
					{
						id: 6,
						title: "Post Zeta",
						user_id: 6,
						comments: [
							{
								id: 3,
								content: "Comment on zeta by eve",
								post_id: 6,
								user_id: 4,
								replies: [
									{ id: 1, content: "Reply to zeta comment by bob", comment_id: 3, user_id: 6 },
									{ id: 5, content: "Reply to zeta comment by carol", comment_id: 3, user_id: 5 },
								],
							},
						],
					},
					{
						id: 8,
						title: "Post Theta",
						user_id: 6,
						comments: [
							{
								id: 5,
								content: "Comment on theta by carol",
								post_id: 8,
								user_id: 5,
								replies: [
									{ id: 3, content: "Reply to theta comment by eve", comment_id: 5, user_id: 4 },
								],
							},
						],
					},
					{
						id: 10,
						title: "Post Kappa",
						user_id: 6,
						comments: [
							{
								id: 7,
								content: "Comment on kappa by eve",
								post_id: 10,
								user_id: 4,
								replies: [],
							},
						],
					},
				],
			},
			{
				id: 5,
				username: "carol",
				posts: [
					{
						id: 1,
						title: "Post Alpha",
						user_id: 5,
						comments: [
							{
								id: 2,
								content: "Comment on alpha by dave",
								post_id: 1,
								user_id: 9,
								replies: [],
							},
						],
					},
					{
						id: 4,
						title: "Post Delta",
						user_id: 5,
						comments: [
							{
								id: 6,
								content: "Comment on delta by dave",
								post_id: 4,
								user_id: 9,
								replies: [
									{ id: 6, content: "Reply to delta comment by bob", comment_id: 6, user_id: 6 },
								],
							},
						],
					},
				],
			},
			{
				id: 9,
				username: "dave",
				posts: [
					{
						id: 2,
						title: "Post Beta",
						user_id: 9,
						comments: [
							{
								id: 4,
								content: "Comment on beta by bob",
								post_id: 2,
								user_id: 6,
								replies: [],
							},
						],
					},
					{
						id: 5,
						title: "Post Epsilon",
						user_id: 9,
						comments: [
							{
								id: 8,
								content: "Comment on epsilon by bob",
								post_id: 5,
								user_id: 6,
								replies: [],
							},
						],
					},
				],
			},
			{
				id: 4,
				username: "eve",
				posts: [
					{
						id: 7,
						title: "Post Eta",
						user_id: 4,
						comments: [],
					},
					{
						id: 9,
						title: "Post Iota",
						user_id: 4,
						comments: [],
					},
				],
			},
			{ id: 8, username: "frank", posts: [] },
			{ id: 1, username: "grace", posts: [] },
			{ id: 10, username: "heidi", posts: [] },
			{ id: 3, username: "ivan", posts: [] },
			{ id: 7, username: "judy", posts: [] },
		]);
	});

	//
	// Edge cases
	//

	test("orderBy: keyBy as tiebreaker when custom ordering has duplicates", async () => {
		// Each user commented twice, so user_id has real ties for the keyBy
		// tiebreaker to break. The base query feeds rows in id-DESC order, so
		// the tiebreaker must actively re-sort each tie to id asc — it can't
		// pass by riding on engine scan order. The compiled SQL pins the
		// trailing keyBy term too.
		const qs = querySet(db)
			.selectAs(
				"comment",
				db.selectFrom("comments").select(["id", "user_id", "content"]).orderBy("id", "desc"),
			)
			.orderBy("user_id", "asc");

		const sql = qs.toQuery().compile().sql;
		assert.ok(sql.endsWith('order by "comment"."user_id" asc, "comment"."id" asc'), sql);

		const comments = await qs.execute();

		// Ordered by user_id asc, ties broken by id asc
		assert.deepStrictEqual(comments, [
			{ id: 3, user_id: 4, content: "Comment on zeta by eve" },
			{ id: 7, user_id: 4, content: "Comment on kappa by eve" },
			{ id: 1, user_id: 5, content: "Comment on gamma by carol" },
			{ id: 5, user_id: 5, content: "Comment on theta by carol" },
			{ id: 4, user_id: 6, content: "Comment on beta by bob" },
			{ id: 8, user_id: 6, content: "Comment on epsilon by bob" },
			{ id: 2, user_id: 9, content: "Comment on alpha by dave" },
			{ id: 6, user_id: 9, content: "Comment on delta by dave" },
		]);
	});

	test("orderBy: custom keyBy column", async () => {
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]), "username")
			// No explicit orderBy, should order by keyBy (username)
			.execute();

		// Should be ordered by username (the keyBy)
		assert.deepStrictEqual(users, [
			{ id: 2, username: "alice", email: "alice@example.com" },
			{ id: 6, username: "bob", email: "bob@example.com" },
			{ id: 5, username: "carol", email: "carol@example.com" },
			{ id: 9, username: "dave", email: "dave@example.com" },
			{ id: 4, username: "eve", email: "eve@example.com" },
			{ id: 8, username: "frank", email: "frank@example.com" },
			{ id: 1, username: "grace", email: "grace@example.com" },
			{ id: 10, username: "heidi", email: "heidi@example.com" },
			{ id: 3, username: "ivan", email: "ivan@example.com" },
			{ id: 7, username: "judy", email: "judy@example.com" },
		]);
	});

	test("orderBy: composite keyBy", async () => {
		const posts = await querySet(db)
			.selectAs("post", db.selectFrom("posts").select(["id", "user_id", "title"]), [
				"user_id",
				"id",
			])
			// No explicit orderBy, should order by keyBy (user_id, id)
			.execute();

		// Should be ordered first by user_id, then by id
		// user_id 4: posts 7, 9
		// user_id 5: posts 1, 4
		// user_id 6: posts 3, 6, 8, 10
		// user_id 9: posts 2, 5
		assert.deepStrictEqual(posts, [
			{ id: 7, user_id: 4, title: "Post Eta" },
			{ id: 9, user_id: 4, title: "Post Iota" },
			{ id: 1, user_id: 5, title: "Post Alpha" },
			{ id: 4, user_id: 5, title: "Post Delta" },
			{ id: 3, user_id: 6, title: "Post Gamma" },
			{ id: 6, user_id: 6, title: "Post Zeta" },
			{ id: 8, user_id: 6, title: "Post Theta" },
			{ id: 10, user_id: 6, title: "Post Kappa" },
			{ id: 2, user_id: 9, title: "Post Beta" },
			{ id: 5, user_id: 9, title: "Post Epsilon" },
		]);
	});

	//
	// Pagination with many-joins and nested orderBy
	//

	test("orderBy: orders by joined column with pagination and leftJoinMany", async () => {
		// This test verifies the fix for the bug where orderBy on nested columns
		// with pagination + many-joins would fail with "missing FROM-clause entry"
		const users = await querySet(db)
			.selectAs("user", db.selectFrom("users").select(["id", "username"]))
			.innerJoinOne(
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
			.orderBy("profile$$bio", "asc")
			.limit(5)
			.offset(0)
			.execute();

		// Should be ordered by profile.bio asc, then by id asc
		// Users with profiles: alice(2), bob(6), carol(5), dave(9), eve(4), frank(8), grace(1), heidi(10), ivan(3), judy(7)
		// First 5: alice, bob, carol, dave, eve
		assert.strictEqual(users.length, 5);
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "alice",
				profile: { id: 2, bio: "Bio for alice", user_id: 2 },
				posts: [], // alice has no posts
			},
			{
				id: 6,
				username: "bob",
				profile: { id: 6, bio: "Bio for bob", user_id: 6 },
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{
				id: 5,
				username: "carol",
				profile: { id: 9, bio: "Bio for carol", user_id: 5 },
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
			{
				id: 9,
				username: "dave",
				profile: { id: 3, bio: "Bio for dave", user_id: 9 },
				posts: [
					{ id: 2, title: "Post Beta", user_id: 9 },
					{ id: 5, title: "Post Epsilon", user_id: 9 },
				],
			},
			{
				id: 4,
				username: "eve",
				profile: { id: 4, bio: "Bio for eve", user_id: 4 },
				posts: [
					{ id: 7, title: "Post Eta", user_id: 4 },
					{ id: 9, title: "Post Iota", user_id: 4 },
				],
			},
		]);
	});

	test("orderBy: orders by leftJoinOneOrThrow column with pagination and leftJoinMany", async () => {
		// Regression test: "oneOrThrow" joins were misclassified as
		// cardinality-many and excluded from the inner cardinality-one subquery,
		// so ordering by their columns in a paginated query produced invalid SQL
		// ("no such column: profile.bio").
		const users = await querySet(db)
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
			.orderBy("profile$$bio", "asc")
			.limit(5)
			.execute();

		// Every user has a profile, so the result matches the innerJoinOne
		// variant above: ordered by profile.bio asc, first 5 users.
		assert.deepStrictEqual(users, [
			{
				id: 2,
				username: "alice",
				profile: { id: 2, bio: "Bio for alice", user_id: 2 },
				posts: [], // alice has no posts
			},
			{
				id: 6,
				username: "bob",
				profile: { id: 6, bio: "Bio for bob", user_id: 6 },
				posts: [
					{ id: 3, title: "Post Gamma", user_id: 6 },
					{ id: 6, title: "Post Zeta", user_id: 6 },
					{ id: 8, title: "Post Theta", user_id: 6 },
					{ id: 10, title: "Post Kappa", user_id: 6 },
				],
			},
			{
				id: 5,
				username: "carol",
				profile: { id: 9, bio: "Bio for carol", user_id: 5 },
				posts: [
					{ id: 1, title: "Post Alpha", user_id: 5 },
					{ id: 4, title: "Post Delta", user_id: 5 },
				],
			},
			{
				id: 9,
				username: "dave",
				profile: { id: 3, bio: "Bio for dave", user_id: 9 },
				posts: [
					{ id: 2, title: "Post Beta", user_id: 9 },
					{ id: 5, title: "Post Epsilon", user_id: 9 },
				],
			},
			{
				id: 4,
				username: "eve",
				profile: { id: 4, bio: "Bio for eve", user_id: 4 },
				posts: [
					{ id: 7, title: "Post Eta", user_id: 4 },
					{ id: 9, title: "Post Iota", user_id: 4 },
				],
			},
		]);
	});
});
