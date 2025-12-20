import assert from "node:assert";
import { test } from "node:test";

import { ExpectedOneItemError } from "./helpers/errors.ts";
import { createHydratable, hydrate } from "./hydratable.ts";

// Test data types
interface User {
	id: number;
	name: string;
}

//
// Basic Configuration
//

test("fields: includes specified fields as-is", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydratable = createHydratable<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydratable);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("fields: transforms field values with functions", async () => {
	const users: User[] = [{ id: 1, name: "alice" }];

	const hydratable = createHydratable<User>("id").fields({
		id: true,
		name: (name) => name.toUpperCase(),
	});

	const result = await hydrate(users, hydratable);

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

	const hydratable = createHydratable<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id").fields({
				id: true,
				title: (title) => title?.toUpperCase(),
			}),
		);

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result[0]?.posts[0]?.title, "HELLO WORLD");
});

test("extras: computes additional fields from input", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydratable = createHydratable<User>("id")
		.fields({ id: true })
		.extras({
			displayName: (input) => `User ${input.name}`,
		});

	const result = await hydrate(users, hydratable);

	assert.deepStrictEqual(result, [{ id: 1, displayName: "User Alice" }]);
});

test("extras: work at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post" }];

	const hydratable = createHydratable<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.extras({
					fullTitle: (input) => `Post #${input.id}: ${input.title}`,
				}),
		);

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result[0]?.posts[0]?.fullTitle, "Post #10: Post");
});

test("composite keys: groups by multiple fields", async () => {
	interface CompositeRow {
		key1: string;
		key2: number;
		value: string;
		nested$$id: number | null;
	}

	const rows: CompositeRow[] = [
		{ key1: "a", key2: 1, value: "first", nested$$id: 1 },
		{ key1: "a", key2: 1, value: "first", nested$$id: 2 },
		{ key1: "a", key2: 2, value: "second", nested$$id: 3 },
	];

	const hydratable = createHydratable<CompositeRow>(["key1", "key2"])
		.fields({
			key1: true,
			key2: true,
			value: true,
		})
		.hasMany("items", "nested$$", (h) => h("id").fields({ id: true }));

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], {
		key1: "a",
		key2: 1,
		value: "first",
		items: [{ id: 1 }, { id: 2 }],
	});
	assert.deepStrictEqual(result[1], {
		key1: "a",
		key2: 2,
		value: "second",
		items: [{ id: 3 }],
	});
});

test("composite keys: work at nested level", async () => {
	interface UserWithPosts extends User {
		posts$$key1: string | null;
		posts$$key2: number | null;
		posts$$title: string | null;
		posts$$comments$$id: number | null;
		posts$$comments$$text: string | null;
	}

	const rows: UserWithPosts[] = [
		{
			id: 1,
			name: "Alice",
			posts$$key1: "a",
			posts$$key2: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 100,
			posts$$comments$$text: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$key1: "a",
			posts$$key2: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 101,
			posts$$comments$$text: "Comment 2",
		},
	];

	const hydratable = createHydratable<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h(["key1", "key2"])
				.fields({ key1: true, key2: true, title: true })
				.hasMany("comments", "comments$$", (h) => h("id").fields({ id: true, text: true })),
		);

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result[0]?.posts.length, 1);
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
});

test("composite keys: skips rows where any key part is null", async () => {
	interface CompositeRow {
		key1: string | null;
		key2: number | null;
		value: string;
	}

	const rows: CompositeRow[] = [
		{ key1: "a", key2: 1, value: "valid" },
		{ key1: "a", key2: null, value: "invalid" },
		{ key1: null, key2: 1, value: "invalid" },
	];

	const hydratable = createHydratable<CompositeRow>(["key1", "key2"]).fields({
		key1: true,
		key2: true,
		value: true,
	});

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0], { key1: "a", key2: 1, value: "valid" });
});

//
// Nested Collections (has/hasMany/hasOne/hasOneOrThrow)
//

test("hasMany: creates nested array collections", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post 1" },
		{ id: 1, name: "Alice", posts$$id: 11, posts$$title: "Post 2" },
		{ id: 2, name: "Bob", posts$$id: null, posts$$title: null },
	];

	const hydratable = createHydratable<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], {
		id: 1,
		name: "Alice",
		posts: [
			{ id: 10, title: "Post 1" },
			{ id: 11, title: "Post 2" },
		],
	});
	assert.deepStrictEqual(result[1], {
		id: 2,
		name: "Bob",
		posts: [],
	});
});

test("hasMany: handles multiple nesting levels", async () => {
	interface NestedRow extends User {
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$comments$$id: number | null;
		posts$$comments$$content: string | null;
	}

	const rows: NestedRow[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "Post 1",
			posts$$comments$$id: 100,
			posts$$comments$$content: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "Post 1",
			posts$$comments$$id: 101,
			posts$$comments$$content: "Comment 2",
		},
	];

	const hydratable = createHydratable<NestedRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (h) => h("id").fields({ id: true, content: true })),
		);

	const result = await hydrate(rows, hydratable);

	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[0], {
		id: 100,
		content: "Comment 1",
	});
});

test("hasOne: returns first nested entity or null", async () => {
	interface UserWithProfile extends User {
		profile$$name: string | null;
		profile$$age: number | null;
	}

	const usersWithProfile: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$name: "Alice P.", profile$$age: 30 },
	];

	const usersWithoutProfile: UserWithProfile[] = [
		{ id: 2, name: "Bob", profile$$name: null, profile$$age: null },
	];

	const hydratable = createHydratable<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOne("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	const withProfile = await hydrate(usersWithProfile, hydratable);
	assert.deepStrictEqual(withProfile[0]?.profile, {
		name: "Alice P.",
		age: 30,
	});

	const withoutProfile = await hydrate(usersWithoutProfile, hydratable);
	assert.strictEqual(withoutProfile[0]?.profile, null);
});

test("hasOneOrThrow: returns nested entity when exists", async () => {
	interface UserWithProfile extends User {
		profile$$name: string;
		profile$$age: number;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$name: "Alice P.", profile$$age: 30 },
	];

	const hydratable = createHydratable<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	const result = await hydrate(rows, hydratable);

	assert.deepStrictEqual(result[0]?.profile, { name: "Alice P.", age: 30 });
});

test("hasOneOrThrow: throws when nested entity is missing", async () => {
	interface UserWithProfile extends User {
		profile$$name: string | null;
		profile$$age: number | null;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$name: null, profile$$age: null },
	];

	const hydratable = createHydratable<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	await assert.rejects(async () => {
		await hydrate(rows, hydratable);
	}, ExpectedOneItemError);
});

//
// Attached Collections (attach/attachMany/attachOne/attachOneOrThrow)
//

test("attachMany: fetches and matches related entities", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const fetchPosts = async (inputs: User[]) => {
		const userIds = inputs.map((u) => u.id);
		const posts = [
			{ id: 10, userId: 1, title: "Alice Post 1" },
			{ id: 11, userId: 1, title: "Alice Post 2" },
			{ id: 12, userId: 2, title: "Bob Post 1" },
		].filter((p) => userIds.includes(p.userId));

		return posts.map((p) => ({ id: p.id, userId: p.userId, title: p.title }));
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0], {
		id: 10,
		userId: 1,
		title: "Alice Post 1",
	});
	assert.strictEqual(result[1]?.posts.length, 1);
});

test("attachMany: calls fetchFn once", async () => {
	let userPostsFetchCount = 0;
	let postCommentsFetchCount = 0;

	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	interface PostOutput {
		id: number;
		userId: number;
		title: string;
		comments: Array<{ id: number; content: string }>;
	}

	const fetchPosts = async (inputs: User[]): Promise<PostOutput[]> => {
		userPostsFetchCount++;

		const userIds = inputs.map((u) => u.id);
		const posts = [
			{ id: 10, userId: 1, title: "Post 1" },
			{ id: 11, userId: 1, title: "Post 2" },
			{ id: 12, userId: 2, title: "Post 3" },
		].filter((p) => userIds.includes(p.userId));

		const fetchComments = async (
			postInputs: Array<{ id: number; userId: number; title: string }>,
		) => {
			postCommentsFetchCount++;

			const postIds = postInputs.map((p) => p.id);
			return [
				{ id: 100, postId: 10, content: "Comment 1" },
				{ id: 101, postId: 10, content: "Comment 2" },
				{ id: 102, postId: 11, content: "Comment 3" },
			].filter((c) => postIds.includes(c.postId));
		};

		const postHydratable = createHydratable<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachMany("comments", fetchComments, { childKey: "postId", parentKey: "id" });

		return await hydrate(posts, postHydratable);
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	// Each fetch function should be called exactly once
	assert.strictEqual(userPostsFetchCount, 1);
	assert.strictEqual(postCommentsFetchCount, 1);

	// Verify structure
	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.strictEqual(result[0]?.posts[1]?.comments.length, 1);
	assert.strictEqual(result[1]?.posts[0]?.comments.length, 0);
});

test("attachMany: returns empty array when no matches", async () => {
	const users: User[] = [{ id: 999, name: "NoMatch" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.ok(Array.isArray(result[0]?.posts));
	assert.strictEqual(result[0]?.posts.length, 0);
});

test("attachMany: uses compareTo for custom matching keys", async () => {
	const users: User[] = [{ id: 100, name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, authorId: 100, title: "Post" }];
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "authorId", parentKey: "id" });

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result[0]?.posts.length, 1);
	assert.deepStrictEqual(result[0]?.posts[0], {
		id: 10,
		authorId: 100,
		title: "Post",
	});
});

test("attachMany: works with composite keys", async () => {
	interface Entity {
		key1: string;
		key2: number;
		value: string;
	}

	const entities: Entity[] = [
		{ key1: "a", key2: 1, value: "Entity 1" },
		{ key1: "b", key2: 2, value: "Entity 2" },
	];

	const fetchRelated = async () => {
		return [
			{ relKey1: "a", relKey2: 1, data: "Related 1" },
			{ relKey1: "a", relKey2: 1, data: "Related 2" },
			{ relKey1: "b", relKey2: 2, data: "Related 3" },
		];
	};

	const hydratable = createHydratable<Entity>(["key1", "key2"])
		.fields({ key1: true, key2: true, value: true })
		.attachMany("related", fetchRelated, {
			childKey: ["relKey1", "relKey2"],
			parentKey: ["key1", "key2"],
		});

	const result = await hydrate(entities, hydratable);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.related.length, 2);
	assert.strictEqual(result[1]?.related.length, 1);
});

test("attachOne: returns first match or null", async () => {
	const usersWithMatch: User[] = [{ id: 1, name: "Alice" }];
	const usersWithoutMatch: User[] = [{ id: 999, name: "NoMatch" }];

	const fetchPosts = async () => {
		return [
			{ id: 10, userId: 1, title: "First" },
			{ id: 11, userId: 1, title: "Second" },
		];
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachOne("latestPost", fetchPosts, { childKey: "userId" });

	const withMatch = await hydrate(usersWithMatch, hydratable);
	assert.deepStrictEqual(withMatch[0]?.latestPost, {
		id: 10,
		userId: 1,
		title: "First",
	});

	const withoutMatch = await hydrate(usersWithoutMatch, hydratable);
	assert.strictEqual(withoutMatch[0]?.latestPost, null);
});

test("attachOne: works at nested level", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	interface PostOutput {
		id: number;
		userId: number;
		title: string;
		latestComment: { id: number; content: string } | null;
	}

	const fetchPosts = async (inputs: User[]): Promise<PostOutput[]> => {
		const userIds = inputs.map((u) => u.id);
		const posts = [
			{ id: 10, userId: 1, title: "Post 1" },
			{ id: 11, userId: 2, title: "Post 2" },
		].filter((p) => userIds.includes(p.userId));

		const fetchComments = async () => {
			return [
				{ id: 100, postId: 10, content: "Comment 1" },
				{ id: 101, postId: 10, content: "Comment 2" },
			];
		};

		const postHydratable = createHydratable<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOne("latestComment", fetchComments, {
				childKey: "postId",
				parentKey: "id",
			});

		return await hydrate(posts, postHydratable);
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result[0]?.posts[0]?.latestComment?.content, "Comment 1");
	assert.strictEqual(result[1]?.posts[0]?.latestComment, null);
});

test("attachOneOrThrow: returns entity when exists", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("requiredPost", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.deepStrictEqual(result[0]?.requiredPost, {
		id: 10,
		userId: 1,
		title: "Post",
	});
});

test("attachOneOrThrow: throws when no match exists", async () => {
	const users: User[] = [{ id: 999, name: "NoMatch" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("requiredPost", fetchPosts, { childKey: "userId" });

	await assert.rejects(async () => {
		await hydrate(users, hydratable);
	}, ExpectedOneItemError);
});

test("attachOneOrThrow: works at nested level", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		const posts = [{ id: 10, userId: 1, title: "Post 1" }];

		const fetchAuthor = async () => {
			return [{ id: 100, postId: 10, name: "Author" }];
		};

		const postHydratable = createHydratable<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOneOrThrow("author", fetchAuthor, {
				childKey: "postId",
				parentKey: "id",
			});

		return await hydrate(posts, postHydratable);
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.deepStrictEqual(result[0]?.posts[0]?.author, {
		id: 100,
		postId: 10,
		name: "Author",
	});
});

test("attachOneOrThrow: throws at nested level when missing", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		const posts = [{ id: 10, userId: 1, title: "Post 1" }];

		const fetchAuthor = async () => {
			return [];
		};

		const postHydratable = createHydratable<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOneOrThrow("requiredAuthor", fetchAuthor, {
				childKey: "postId",
				parentKey: "id",
			});

		return await hydrate(posts, postHydratable);
	};

	const hydratable = createHydratable<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	await assert.rejects(async () => {
		await hydrate(users, hydratable);
	}, ExpectedOneItemError);
});

//
// Hydration Modes and Edge Cases
//

test("hydrate: handles single input", async () => {
	const user: User = { id: 1, name: "Alice" };

	const hydratable = createHydratable<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(user, hydratable);

	assert.deepStrictEqual(result, { id: 1, name: "Alice" });
});

test("hydrate: handles empty array", async () => {
	const users: User[] = [];

	const hydratable = createHydratable<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydratable);

	assert.deepStrictEqual(result, []);
});

test("hydrate: skips entities with null keys", async () => {
	interface NullableUser {
		id: number | null;
		name: string;
	}

	const users: NullableUser[] = [
		{ id: 1, name: "Alice" },
		{ id: null, name: "Invalid" },
		{ id: 2, name: "Bob" },
	];

	const hydratable = createHydratable<NullableUser>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], { id: 1, name: "Alice" });
	assert.deepStrictEqual(result[1], { id: 2, name: "Bob" });
});

test("hydrate function: accepts inline hydratable creation", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const result = await hydrate(users, (keyBy) => keyBy("id").fields({ id: true, name: true }));

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

test("chaining methods: creates immutable configurations", async () => {
	const base = createHydratable<User>("id").fields({ id: true });

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

test("mixing has and attach collections", async () => {
	interface UserWithProfile extends User {
		profile$$bio: string | null;
	}

	const users: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$bio: "Developer" },
		{ id: 2, name: "Bob", profile$$bio: "Designer" },
	];

	const fetchPosts = async () => {
		return [
			{ id: 10, userId: 1, title: "Alice Post" },
			{ id: 11, userId: 2, title: "Bob Post" },
		];
	};

	const hydratable = createHydratable<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOne("profile", "profile$$", (h) => h("bio").fields({ bio: true }))
		.attachMany("posts", fetchPosts, { childKey: "userId" });

	const result = await hydrate(users, hydratable);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0]?.profile, { bio: "Developer" });
	assert.strictEqual(result[0]?.posts.length, 1);
	assert.deepStrictEqual(result[1]?.profile, { bio: "Designer" });
	assert.strictEqual(result[1]?.posts.length, 1);
});

test("complex nesting: has and attach at multiple levels", async () => {
	let authorsFetchCount = 0;
	let tagsFetchCount = 0;

	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
		posts$$comments$$id: number | null;
		posts$$comments$$content: string | null;
	}

	const rows: UserWithPosts[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "Post 1",
			posts$$comments$$id: 100,
			posts$$comments$$content: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 10,
			posts$$title: "Post 1",
			posts$$comments$$id: 101,
			posts$$comments$$content: "Comment 2",
		},
	];

	const fetchAuthors = async () => {
		authorsFetchCount++;
		return [{ id: 200, commentId: 100, name: "Author 1" }];
	};

	const fetchTags = async () => {
		tagsFetchCount++;
		return [
			{ id: 300, postId: 10, name: "tag1" },
			{ id: 301, postId: 10, name: "tag2" },
		];
	};

	const hydratable = createHydratable<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (h) =>
					h("id").fields({ id: true, content: true }).attachOne("author", fetchAuthors, {
						childKey: "commentId",
						parentKey: "id",
					}),
				)
				.attachMany("tags", fetchTags, { childKey: "postId", parentKey: "id" }),
		);

	const result = await hydrate(rows, hydratable);

	// Verify fetch counts
	assert.strictEqual(authorsFetchCount, 1);
	assert.strictEqual(tagsFetchCount, 1);

	// Verify structure
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.tags.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[0]?.author, {
		id: 200,
		commentId: 100,
		name: "Author 1",
	});
	assert.strictEqual(result[0]?.posts[0]?.comments[1]?.author, null);
});
