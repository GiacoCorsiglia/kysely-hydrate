import assert from "node:assert";
import { test } from "node:test";

import { orderByDb as db } from "./__tests__/order-by-fixture.ts";
import { querySet } from "./query-set.ts";

//
// ORDER BY Tests
//
// These tests verify that the orderBy, clearOrderBy, and orderByKeys methods
// work correctly in different scenarios. The tests should initially fail due
// to incorrect ordering in results (not type errors).
//
// NOTE: This test file uses a dedicated fixture with RANDOMIZED data to ensure
// ordering is truly tested. In the fixture:
// - Users inserted as: grace, alice, ivan, eve, carol, bob, judy, frank, dave, heidi
// - So ID order ≠ alphabetical order
//

//
// No joins - basic ordering
//

test("orderBy: orders by single column ascending", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
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
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username", "email"]))
		.orderBy("username", "desc")
		.orderBy("id", "asc")
		.execute();

	// Primary: username desc, Secondary: id asc (though each username is unique)
	assert.deepStrictEqual(users, [
		{ id: 7, username: "judy", email: "judy@example.com" },
		{ id: 3, username: "ivan", email: "ivan@example.com" },
		{ id: 10, username: "heidi", email: "heidi@example.com" },
		{ id: 1, username: "grace", email: "grace@example.com" },
		{ id: 8, username: "frank", email: "frank@example.com" },
		{ id: 4, username: "eve", email: "eve@example.com" },
		{ id: 9, username: "dave", email: "dave@example.com" },
		{ id: 5, username: "carol", email: "carol@example.com" },
		{ id: 6, username: "bob", email: "bob@example.com" },
		{ id: 2, username: "alice", email: "alice@example.com" },
	]);
});

test("orderBy: with orderByKeys disabled", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.orderByKeys(false)
		.orderBy("username", "desc")
		.execute();

	// Should only order by username, NOT by id as a tiebreaker
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

test("orderBy: with default keyBy ordering", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		// No explicit orderBy, should default to ordering by id (keyBy)
		.execute();

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
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.orderBy("username", "desc")
		.clearOrderBy()
		.execute();

	// Should revert to default keyBy ordering (id asc)
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

test("orderByKeys: can be re-enabled after being disabled", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.orderByKeys(false)
		.orderByKeys(true)
		.execute();

	// Should be ordered by id (keyBy re-enabled)
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

//
// Cardinality-one joins
//

test("orderBy: orders by base column with innerJoinOne", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.orderBy("profile$$bio", "asc")
		.orderBy("username", "desc")
		.execute();

	// Primary ordering by profile.bio asc, secondary by username desc, tertiary by id asc
	// (All users have profiles in this fixture)
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

//
// Cardinality-many joins
//

test("orderBy: orders base records with leftJoinMany", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.leftJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
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
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(init) => init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.orderBy("username", "desc")
		.limit(2)
		.execute();

	// First 2 when ordered by username desc: judy, ivan
	assert.deepStrictEqual(users, [
		{ id: 7, username: "judy", posts: [] },
		{ id: 3, username: "ivan", posts: [] },
	]);
});

//
// Nested joins
//

test("orderBy: with nested one-many (user -> profile -> posts)", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(init) =>
				init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])).leftJoinMany(
					"posts",
					(init2) => init2((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(init) =>
				init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					(init2) =>
						init2((eb) =>
							eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"]),
						),
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
		.init("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(init) =>
				init((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					(init2) =>
						init2((eb) =>
							eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"]),
						).leftJoinMany(
							"replies",
							(init3) =>
								init3((eb) =>
									eb.selectFrom("replies").select(["id", "content", "comment_id", "user_id"]),
								),
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
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username", "email"]))
		.orderBy("email", "asc")
		.execute();

	// Should be ordered by email (all unique), with id as tiebreaker
	// Alphabetically by email: alice, bob, carol, dave, eve, frank, grace, heidi, ivan, judy
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

test("orderBy: custom keyBy column", async () => {
	const users = await querySet(db)
		.init("user", db.selectFrom("users").select(["id", "username", "email"]), "username")
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
		.init("post", db.selectFrom("posts").select(["id", "user_id", "title"]), ["user_id", "id"])
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
