import assert from "node:assert";
import { test } from "node:test";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

//
// Phase 6: Hydration Features - extras, mapFields, omit, with, map
//

// extras: Add computed fields

test("extras: add computed field at root level", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "<=", 3)
		.extras({
			displayName: (row) => `User: ${row.username}`,
		})
		.execute();

	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", email: "alice@example.com", displayName: "User: alice" },
		{ id: 2, username: "bob", email: "bob@example.com", displayName: "User: bob" },
		{ id: 3, username: "carol", email: "carol@example.com", displayName: "User: carol" },
	]);
});

test("extras: add computed field with full row access", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "<=", 2)
		.extras({
			emailDomain: (row) => row.email.split("@")[1],
			idPlusOne: (row) => row.id + 1,
		})
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			emailDomain: "example.com",
			idPlusOne: 2,
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			emailDomain: "example.com",
			idPlusOne: 3,
		},
	]);
});

test("extras: add computed field in nested join", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).extras({
					titleUpper: (row) => row.title.toUpperCase(),
				}),
			"posts.user_id",
			"user.id",
		)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", user_id: 2, titleUpper: "POST 1" },
				{ id: 2, title: "Post 2", user_id: 2, titleUpper: "POST 2" },
				{ id: 5, title: "Post 5", user_id: 2, titleUpper: "POST 5" },
				{ id: 12, title: "Post 12", user_id: 2, titleUpper: "POST 12" },
			],
		},
	]);
});

test("extras: multiple extras on same QuerySet", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.extras({
			first: (_) => "first",
		})
		.extras({
			second: (_) => "second",
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: 1, username: "alice", first: "first", second: "second" }]);
});

// mapFields: Transform field values

test("mapFields: transform single field value", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", 2)
		.mapFields({
			username: (value) => value.toUpperCase(),
		})
		.execute();

	assert.deepStrictEqual(users, [
		{ id: 1, username: "ALICE" },
		{ id: 2, username: "BOB" },
	]);
});

test("mapFields: transform multiple fields", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.mapFields({
			id: (value) => `ID-${value}`,
			username: (value) => value.toUpperCase(),
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: "ID-1", username: "ALICE" }]);
});

test("mapFields: unmapped fields remain unchanged", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.mapFields({
			username: (value) => value.toUpperCase(),
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: 1, email: "alice@example.com", username: "ALICE" }]);
});

test("mapFields: type transformation (number to string)", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.mapFields({
			id: (value) => String(value),
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: "1", username: "alice" }]);
});

test("mapFields: in nested join", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).mapFields({
					title: (value) => value.toUpperCase(),
				}),
			"posts.user_id",
			"user.id",
		)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "POST 1", user_id: 2 },
				{ id: 2, title: "POST 2", user_id: 2 },
				{ id: 5, title: "POST 5", user_id: 2 },
				{ id: 12, title: "POST 12", user_id: 2 },
			],
		},
	]);
});

test("mapFields: multiple mapFields merge configurations", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.mapFields({
			id: (value) => value * 10,
		})
		.mapFields({
			username: (value) => value.toUpperCase(),
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: 10, username: "ALICE" }]);
});

// omit: Remove fields from output

test("omit: remove single field from root", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.omit(["email"])
		.execute();

	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual("email" in users[0]!, false);
});

test("omit: remove multiple fields", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.omit(["username", "email"])
		.execute();

	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual("username" in users[0]!, false);
	assert.strictEqual("email" in users[0]!, false);
});

test("omit: used with extras to hide intermediate fields", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.extras({
			displayName: (row) => `${row.username} (${row.email})`,
		})
		.omit(["username", "email"])
		.execute();

	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.displayName, "alice (alice@example.com)");
	assert.strictEqual("username" in users[0]!, false);
	assert.strictEqual("email" in users[0]!, false);
});

test("omit: in nested join", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).omit(["user_id"]),
			"posts.user_id",
			"user.id",
		)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1" },
				{ id: 2, title: "Post 2" },
				{ id: 5, title: "Post 5" },
				{ id: 12, title: "Post 12" },
			],
		},
	]);
});

// with: Merge hydrator configuration using createHydrator

test("with: merges extras from hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
		email: string;
	}

	const extraFields = createHydrator<User>("id").extras({
		displayName: (user) => `${user.username} <${user.email}>`,
	});

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.with(extraFields)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			displayName: "alice <alice@example.com>",
		},
	]);
});

test("with: merges mapFields from hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
	}

	const upperCaseHydrator = createHydrator<User>("id").fields({
		username: (username) => username.toUpperCase(),
	});

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.with(upperCaseHydrator)
		.execute();

	assert.deepStrictEqual(users, [{ id: 1, username: "ALICE" }]);
});

test("with: hydrator configuration takes precedence over existing", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
	}

	const override = createHydrator<User>("id").fields({
		username: (username) => username.toUpperCase(),
	});

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.mapFields({
			username: (username) => username.toLowerCase(),
		})
		.with(override)
		.execute();

	// The hydrator passed to with() takes precedence
	assert.deepStrictEqual(users, [{ id: 1, username: "ALICE" }]);
});

test("with: merges omit from hydrator", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface User {
		id: number;
		username: string;
		email: string;
	}

	const withOmit = createHydrator<User>("id")
		.extras({
			displayName: (user) => `${user.username} <${user.email}>`,
		})
		.omit(["email"]);

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.where("users.id", "=", 1)
		.with(withOmit)
		.execute();

	assert.strictEqual(users.length, 1);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual(users[0]?.displayName, "alice <alice@example.com>");
	assert.strictEqual("email" in users[0]!, false);
});

test("with: works in nested QuerySet", async () => {
	const { createHydrator } = await import("./hydrator.ts");

	interface Post {
		id: number;
		title: string;
		user_id: number;
	}

	const postHydrator = createHydrator<Post>("id")
		.extras({
			titleUpper: (post) => post.title.toUpperCase(),
		})
		.omit(["user_id"]);

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).with(postHydrator),
			"posts.user_id",
			"user.id",
		)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 2,
			username: "bob",
			posts: [
				{ id: 1, title: "Post 1", titleUpper: "POST 1" },
				{ id: 2, title: "Post 2", titleUpper: "POST 2" },
				{ id: 5, title: "Post 5", titleUpper: "POST 5" },
				{ id: 12, title: "Post 12", titleUpper: "POST 12" },
			],
		},
	]);
});

// Miscellaneous hydration configuration tests

test("nested QuerySet with extras and omit", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.innerJoinOne(
			"profile",
			({ eb, qs }) =>
				qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"]))
					.omit(["user_id"])
					.extras({
						bioLength: (row) => row.bio?.length ?? 0,
					}),
			"profile.user_id",
			"user.id",
		)
		.execute();

	assert.deepStrictEqual(users, [
		{
			id: 1,
			username: "alice",
			profile: { id: 1, bio: "Bio for user 1", bioLength: 14 },
		},
	]);
});

test("multiple mapFields calls: later takes precedence for same field", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.mapFields({
			username: (value) => value.toUpperCase(),
		})
		.mapFields({
			username: (value) => value.toLowerCase(), // This should win
		})
		.execute();

	assert.deepStrictEqual(users, [{ id: 1, username: "alice" }]);
});

// map: Transform entire output

test("map: transform entire output object", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "<=", 2)
		.map((user) => ({
			...user,
			transformed: true,
		}))
		.execute();

	assert.deepStrictEqual(users, [
		{ id: 1, username: "alice", transformed: true },
		{ id: 2, username: "bob", transformed: true },
	]);
});

test("map: transform into different shape", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.map((user) => `User #${user.id}: ${user.username}`)
		.execute();

	assert.deepStrictEqual(users, ["User #1: alice"]);
});

test("map: chain multiple maps", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.map((user) => ({ ...user, step1: true }))
		.map((user) => ({ ...user, step2: true }))
		.execute();

	assert.deepStrictEqual(users, [{ id: 1, username: "alice", step1: true, step2: true }]);
});

test("map: transform into class instances", async () => {
	class UserModel {
		id: number;
		username: string;

		constructor(id: number, username: string) {
			this.id = id;
			this.username = username;
		}

		greet() {
			return `Hello, I'm ${this.username}`;
		}
	}

	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 1)
		.map((user) => new UserModel(user.id, user.username))
		.execute();

	assert.strictEqual(users.length, 1);
	assert.ok(users[0] instanceof UserModel);
	assert.strictEqual(users[0]?.id, 1);
	assert.strictEqual(users[0]?.username, "alice");
	assert.strictEqual(users[0]?.greet(), "Hello, I'm alice");
});

test("map: with nested joins", async () => {
	const users = await querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("users.id", "=", 2)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.map((user) => ({
			userId: user.id,
			name: user.username,
			postCount: user.posts.length,
		}))
		.execute();

	assert.deepStrictEqual(users, [{ userId: 2, name: "bob", postCount: 4 }]);
});
