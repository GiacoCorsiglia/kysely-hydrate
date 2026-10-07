import assert from "node:assert";
import { test } from "node:test";

import { KeyByMismatchError } from "./helpers/errors.ts";
import { createHydrator, hydrate } from "./hydrator.ts";

// Test data types
interface User {
	id: number;
	name: string;
}

//
// Basic configuration: fields, extras, extend, omit
//

test("fields: includes specified fields as-is", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("fields: accepts array shorthand for including fields", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>("id").fields(["id", "name"]);

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("fields: transforms field values with functions", async () => {
	const users: User[] = [{ id: 1, name: "alice" }];

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: (name) => name.toUpperCase(),
	});

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, name: "ALICE" }]);
});

test("fields: transformations work at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "hello world" },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id").fields({
				id: true,
				title: (title) => title?.toUpperCase(),
			}),
		);

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.title, "HELLO WORLD");
});

test("extras: computes additional fields from input", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true })
		.extras({
			displayName: (input) => `User ${input.name}`,
		});

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, displayName: "User Alice" }]);
});

test("extras: work at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post" }];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.extras({
					fullTitle: (input) => `Post #${input.id}: ${input.title}`,
				}),
		);

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.fullTitle, "Post #10: Post");
});

test("extend: spreads returned object onto hydrated output", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true })
		.extend((input) => ({
			displayName: `User ${input.name}`,
			nameLength: input.name.length,
		}));

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, displayName: "User Alice", nameLength: 5 }]);
});

test("extend: can be chained and interleaved with extras", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true })
		.extras({ upper: (input) => input.name.toUpperCase() })
		.extend((input) => ({ lower: input.name.toLowerCase() }))
		.extras({ length: (input) => input.name.length });

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, upper: "ALICE", lower: "alice", length: 5 }]);
});

test("extend: preserved through .with() composition", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true })
		.extend((input) => ({ displayName: `User ${input.name}` }));

	const otherHydrator = createHydrator<User>("id").extras({
		upper: (input) => input.name.toUpperCase(),
	});

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [{ id: 1, displayName: "User Alice", upper: "ALICE" }]);
});

test("extend: normal keys still merge with later keys winning", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.extend((input) => ({ name: input.name.toUpperCase(), greeting: `Hi ${input.name}` }))
		.extend(() => ({ greeting: "Hello" }));

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, name: "ALICE", greeting: "Hello" }]);
});

test("extend: a nullish extension is a no-op, matching Object.assign", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	// The Extender type forbids nullish returns, but untyped callers rely on
	// Object.assign tolerating them.
	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.extend(() => undefined as any)
		.extend(() => null as any);

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

test("omit: removes specified fields from output", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id").fields({ id: true, name: true }).omit(["name"]);

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1 }]);
	assert.strictEqual("name" in result[0]!, false);
});

test("omit: works with extras to hide implementation details", async () => {
	interface UserWithNames extends User {
		firstName: string;
		lastName: string;
	}

	const users: UserWithNames[] = [
		{ id: 1, name: "Alice Smith", firstName: "Alice", lastName: "Smith" },
	];

	const hydrator = createHydrator<UserWithNames>("id")
		.fields({ id: true, firstName: true, lastName: true })
		.extras({
			fullName: (input) => `${input.firstName} ${input.lastName}`,
		})
		.omit(["firstName", "lastName"]);

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, fullName: "Alice Smith" }]);
	assert.strictEqual("firstName" in result[0]!, false);
	assert.strictEqual("lastName" in result[0]!, false);
});

test("omit: works at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$content: string | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post", posts$$content: "Content here" },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id").fields({ id: true, title: true, content: true }).omit(["content"]),
		);

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result[0]?.posts[0], { id: 10, title: "Post" });
	assert.strictEqual("content" in result[0]!.posts[0]!, false);
});

//
// Nested callback arguments
//
//
// Nested levels receive a prefixed accessor over the raw input row rather than
// the row itself.
//

test("extend: a nested callback can enumerate a frozen input row", async () => {
	// Callers hydrating pre-fetched rows may hand over frozen objects.
	const rows = [Object.freeze({ id: 1, posts$$id: 7, posts$$title: "t" })] as any[];

	const hydrator = createHydrator<any>("id")
		.fields({ id: true })
		.hasMany("posts", "posts$$", (h: any) =>
			h("id").extend((post: any) => ({ ...post, seen: true })),
		);

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, posts: [{ id: 7, title: "t", seen: true }] }]);
});

test("extras: a nested callback writing to its argument stays at its own level", async () => {
	const rows = [{ id: 1, name: "Alice", posts$$id: 7 }] as any[];

	const hydrator = createHydrator<any>("id")
		.fields({ id: true })
		.hasMany("posts", "posts$$", (h: any) =>
			h("id").extras({
				// Memoizing onto the row is the shape that used to write an
				// unprefixed key onto the caller's row.
				slug: (post: any) => (post.slug ??= `post-${post.id}`),
			}),
		);

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [{ id: 1, posts: [{ slug: "post-7" }] }]);
	// The write landed at the nested level, so no key appears unprefixed where
	// the parent level (or a later hydration of these rows) would pick it up.
	assert.deepStrictEqual(Object.keys(rows[0]), ["id", "name", "posts$$id", "posts$$slug"]);
});

//
// Configuration is immutable
//

test("chaining methods: creates immutable configurations", async () => {
	const base = createHydrator<User>("id").fields({ id: true });

	const withName = base.fields({ name: true });
	const withExtra = base.extras({ displayName: (u) => `User ${u.id}` });

	const users: User[] = [{ id: 1, name: "Alice" }];

	const resultBase = await hydrate(users, base);
	const resultWithName = await hydrate(users, withName);
	const resultWithExtra = await hydrate(users, withExtra);

	// Each configuration should be independent
	assert.deepStrictEqual(resultBase, [{ id: 1 }]);
	assert.deepStrictEqual(resultWithName, [{ id: 1, name: "Alice" }]);
	assert.deepStrictEqual(resultWithExtra, [{ id: 1, displayName: "User 1" }]);
});

//
// Composition with .with()
//

test("with: merges fields from two hydrators", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id").fields({ id: true });

	const nameHydrator = createHydrator<User>("id").fields({ name: true });

	const combined = baseHydrator.with(nameHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

test("with: other hydrator's fields take precedence", async () => {
	const users: User[] = [{ id: 1, name: "alice" }];

	const baseHydrator = createHydrator<User>("id").fields({
		id: true,
		name: (name) => name.toUpperCase(),
	});

	const otherHydrator = createHydrator<User>("id").fields({
		name: (name) => name.toLowerCase() + "-other",
	});

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [{ id: 1, name: "alice-other" }]);
});

test("with: merges extras from two hydrators", async () => {
	interface UserWithEmail extends User {
		email: string;
	}

	const users: UserWithEmail[] = [{ id: 1, name: "Alice", email: "alice@example.com" }];

	const baseHydrator = createHydrator<UserWithEmail>("id")
		.fields({ id: true, name: true, email: true })
		.extras({
			displayName: (user) => `${user.name}`,
		});

	const otherHydrator = createHydrator<UserWithEmail>("id").extras({
		emailUpper: (user) => user.email.toUpperCase(),
	});

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			email: "alice@example.com",
			displayName: "Alice",
			emailUpper: "ALICE@EXAMPLE.COM",
		},
	]);
});

test("with: other hydrator's extras take precedence", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.extras({
			greeting: () => "Hello",
		});

	const otherHydrator = createHydrator<User>("id").extras({
		greeting: () => "Hi",
	});

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.strictEqual(result[0]?.greeting, "Hi");
});

test("with: merges collections from two hydrators", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
		comments$$id: number | null;
		comments$$content: string | null;
	}

	const rows: UserWithPosts[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "Post 1",
			comments$$id: 100,
			comments$$content: "Comment 1",
		},
	];

	const baseHydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const otherHydrator = createHydrator<UserWithPosts>("id").hasMany("comments", "comments$$", (h) =>
		h("id").fields({ id: true, content: true }),
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, title: "Post 1" }],
			comments: [{ id: 100, content: "Comment 1" }],
		},
	]);
});

test("with: other hydrator's collections take precedence", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "post title",
		},
	];

	const baseHydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true }));

	const otherHydrator = createHydrator<UserWithPosts>("id").hasMany("posts", "posts$$", (h) =>
		h("id").fields({ id: true, title: (title) => title?.toUpperCase() ?? "" }),
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, title: "POST TITLE" }],
		},
	]);
});

test("with: other hydrator's nested collection overrides own attach with the same key", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Joined Post" },
	];

	let fetchCount = 0;
	const fetchPosts = async () => {
		fetchCount++;
		return [{ id: 999, userId: 1, title: "FROM STALE ATTACH" }];
	};

	const baseHydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const otherHydrator = createHydrator<UserWithPosts>("id").hasMany("posts", "posts$$", (h) =>
		h("id").fields({ id: true, title: true }),
	);

	const result = await hydrate(rows, baseHydrator.with(otherHydrator));

	assert.strictEqual(fetchCount, 0);
	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [{ id: 10, title: "Joined Post" }] },
	]);
});

test("with: other hydrator's attach overrides own nested collection with the same key", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: null, posts$$title: null }];

	const fetchPosts = async () => [{ id: 10, userId: 1, title: "Attached Post" }];

	const baseHydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const otherHydrator = createHydrator<UserWithPosts>("id").attachMany("posts", fetchPosts, {
		matchChild: "userId",
	});

	const result = await hydrate(rows, baseHydrator.with(otherHydrator));

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [{ id: 10, userId: 1, title: "Attached Post" }] },
	]);
});

test("with: throws when keyBy doesn't match", () => {
	interface Post {
		id: number;
		userId: number;
	}

	const userHydrator = createHydrator<User>("id").fields({ id: true });

	const postHydrator = createHydrator<Post>("userId").fields({ userId: true });

	assert.throws(() => userHydrator.with(postHydrator as any), KeyByMismatchError);
});

test("with: a key matches its one-part composite form", async () => {
	const rows: User[] = [{ id: 1, name: "Alice" }];

	const idHydrator = createHydrator<User>("id").fields({ id: true });
	const nameHydrator = createHydrator<User>(["id"]).fields({ name: true });

	assert.deepStrictEqual(await hydrate(rows, idHydrator.with(nameHydrator)), [
		{ id: 1, name: "Alice" },
	]);
	assert.deepStrictEqual(await hydrate(rows, nameHydrator.with(idHydrator)), [
		{ name: "Alice", id: 1 },
	]);
});

test("with: works with composite keys", async () => {
	interface UserPost {
		userId: number;
		postId: number;
		content: string;
	}

	const rows: UserPost[] = [{ userId: 1, postId: 10, content: "Hello" }];

	const baseHydrator = createHydrator<UserPost>(["userId", "postId"]).fields({
		userId: true,
		postId: true,
	});

	const otherHydrator = createHydrator<UserPost>(["userId", "postId"]).fields({
		content: true,
	});

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(rows, combined);

	assert.deepStrictEqual(result, [{ userId: 1, postId: 10, content: "Hello" }]);
});

test("with: works bidirectionally (no constraint on OtherInput)", async () => {
	interface AdminUser extends User {
		role: string;
	}

	const users: AdminUser[] = [{ id: 1, name: "Alice", role: "admin" }];

	const userHydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const adminHydrator = createHydrator<AdminUser>("id").fields({ role: true });

	// Direction 1: User composed with AdminUser
	const combined1 = userHydrator.with(adminHydrator);
	const result1 = await hydrate(users, combined1);

	assert.deepStrictEqual(result1, [{ id: 1, name: "Alice", role: "admin" }]);

	// Direction 2: AdminUser composed with User (reverse)
	const combined2 = adminHydrator.with(userHydrator);
	const result2 = await hydrate(users, combined2);

	assert.deepStrictEqual(result2, [{ id: 1, name: "Alice", role: "admin" }]);
});

test("with: merges hasOne collections", async () => {
	interface UserWithProfile extends User {
		profile$$id: number | null;
		profile$$bio: string | null;
		settings$$id: number | null;
		settings$$theme: string | null;
	}

	const rows: UserWithProfile[] = [
		{
			id: 1,
			name: "Alice",
			profile$$id: 100,
			profile$$bio: "Developer",
			settings$$id: 200,
			settings$$theme: "dark",
		},
	];

	const baseHydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOne("profile", "profile$$", (h) => h("id").fields({ id: true, bio: true }));

	const otherHydrator = createHydrator<UserWithProfile>("id").hasOne(
		"settings",
		"settings$$",
		(h) => h("id").fields({ id: true, theme: true }),
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			profile: { id: 100, bio: "Developer" },
			settings: { id: 200, theme: "dark" },
		},
	]);
});

test("with: merges hasOneOrThrow collections", async () => {
	interface UserWithSettings extends User {
		settings$$id: number;
		settings$$theme: string;
		preferences$$id: number;
		preferences$$language: string;
	}

	const rows: UserWithSettings[] = [
		{
			id: 1,
			name: "Alice",
			settings$$id: 100,
			settings$$theme: "dark",
			preferences$$id: 200,
			preferences$$language: "en",
		},
	];

	const baseHydrator = createHydrator<UserWithSettings>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("settings", "settings$$", (h) => h("id").fields({ id: true, theme: true }));

	const otherHydrator = createHydrator<UserWithSettings>("id").hasOneOrThrow(
		"preferences",
		"preferences$$",
		(h) => h("id").fields({ id: true, language: true }),
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			settings: { id: 100, theme: "dark" },
			preferences: { id: 200, language: "en" },
		},
	]);
});

test("with: merges attachMany collections", async () => {
	interface Post {
		id: number;
		userId: number;
		title: string;
	}

	interface Comment {
		id: number;
		userId: number;
		content: string;
	}

	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			async (users) => {
				assert.deepStrictEqual(users, [{ id: 1, name: "Alice" }]);
				return [{ id: 10, userId: 1, title: "Post 1" }] as Post[];
			},
			{ matchChild: "userId" },
		);

	const otherHydrator = createHydrator<User>("id").attachMany(
		"comments",
		async (users) => {
			assert.deepStrictEqual(users, [{ id: 1, name: "Alice" }]);
			return [{ id: 100, userId: 1, content: "Comment 1" }] as Comment[];
		},
		{ matchChild: "userId" },
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, userId: 1, title: "Post 1" }],
			comments: [{ id: 100, userId: 1, content: "Comment 1" }],
		},
	]);
});

test("with: merges attachOne collections", async () => {
	interface Profile {
		userId: number;
		bio: string;
	}

	interface Settings {
		userId: number;
		theme: string;
	}

	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne("profile", async () => [{ userId: 1, bio: "Developer" }] as Profile[], {
			matchChild: "userId",
		});

	const otherHydrator = createHydrator<User>("id").attachOne(
		"settings",
		async () => [{ userId: 1, theme: "dark" }] as Settings[],
		{ matchChild: "userId" },
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			profile: { userId: 1, bio: "Developer" },
			settings: { userId: 1, theme: "dark" },
		},
	]);
});

test("with: merges attachOneOrThrow collections", async () => {
	interface Settings {
		userId: number;
		theme: string;
	}

	interface Preferences {
		userId: number;
		language: string;
	}

	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("settings", async () => [{ userId: 1, theme: "dark" }] as Settings[], {
			matchChild: "userId",
		});

	const otherHydrator = createHydrator<User>("id").attachOneOrThrow(
		"preferences",
		async () => [{ userId: 1, language: "en" }] as Preferences[],
		{ matchChild: "userId" },
	);

	const combined = baseHydrator.with(otherHydrator);
	const result = await hydrate(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			settings: { userId: 1, theme: "dark" },
			preferences: { userId: 1, language: "en" },
		},
	]);
});

//
// map() transformations
//

test("map: transforms hydrated output", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.map((user) => ({ userId: user.id, userName: user.name }));

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], { userId: 1, userName: "Alice" });
	assert.deepStrictEqual(result[1], { userId: 2, userName: "Bob" });
});

test("map: allows chaining multiple transformations", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.map((user) => ({ ...user, uppercaseName: user.name.toUpperCase() }))
		.map((user) => ({ ...user, nameLength: user.uppercaseName.length }))
		.map((user) => ({ final: `${user.uppercaseName} (${user.nameLength})` }));

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [{ final: "ALICE (5)" }]);
});

test("map: transforms into class instances", async () => {
	class UserModel {
		id: number;
		name: string;

		constructor(id: number, name: string) {
			this.id = id;
			this.name = name;
		}

		greet() {
			return `Hello, I'm ${this.name}`;
		}
	}

	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.map((user) => new UserModel(user.id, user.name));

	const result = await hydrate(users, hydrator);

	assert.ok(result[0] instanceof UserModel);
	assert.strictEqual(result[0]?.greet(), "Hello, I'm Alice");
});

test("map: works with nested collections", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post 1" },
		{ id: 1, name: "Alice", posts$$id: 11, posts$$title: "Post 2" },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany(
			"posts",
			"posts$$",
			(h) =>
				h("id")
					.fields({ id: true, title: true })
					.map((post) => ({ postId: post.id, postTitle: post.title?.toUpperCase() })), // Map nested
		)
		.map((user) => ({ userName: user.name, postCount: user.posts.length, posts: user.posts })); // Map parent

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0], {
		userName: "Alice",
		postCount: 2,
		posts: [
			{ postId: 10, postTitle: "POST 1" },
			{ postId: 11, postTitle: "POST 2" },
		],
	});
});

test("map: works with attached collections", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	interface Post {
		id: number;
		userId: number;
		title: string;
	}

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			async () =>
				[
					{ id: 10, userId: 1, title: "Post 1" },
					{ id: 11, userId: 1, title: "Post 2" },
				] as Post[],
			{ matchChild: "userId" },
		)
		.map((user) => ({
			user: user.name,
			postTitles: user.posts.map((p) => p.title),
		}));

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result, [
		{
			user: "Alice",
			postTitles: ["Post 1", "Post 2"],
		},
	]);
});
