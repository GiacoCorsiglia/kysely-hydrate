import assert from "node:assert";
import { test } from "node:test";

import { ExpectedOneItemError, KeyByMismatchError } from "./helpers/errors.ts";
import { createHydrator, hydrateData } from "./hydrator.ts";

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

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrateData(users, hydrator);

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

	const result = await hydrateData(users, hydrator);

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

	const result = await hydrateData(rows, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.title, "HELLO WORLD");
});

test("extras: computes additional fields from input", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true })
		.extras({
			displayName: (input) => `User ${input.name}`,
		});

	const result = await hydrateData(users, hydrator);

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

	const result = await hydrateData(rows, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.fullTitle, "Post #10: Post");
});

test("omit: removes specified fields from output", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const hydrator = createHydrator<User>("id").fields({ id: true, name: true }).omit(["name"]);

	const result = await hydrateData(users, hydrator);

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

	const result = await hydrateData(users, hydrator);

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

	const result = await hydrateData(rows, hydrator);

	assert.deepStrictEqual(result[0]?.posts[0], { id: 10, title: "Post" });
	assert.strictEqual("content" in result[0]!.posts[0]!, false);
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

	const hydrator = createHydrator<CompositeRow>(["key1", "key2"])
		.fields({
			key1: true,
			key2: true,
			value: true,
		})
		.hasMany("items", "nested$$", (h) => h("id").fields({ id: true }));

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h(["key1", "key2"])
				.fields({ key1: true, key2: true, title: true })
				.hasMany("comments", "comments$$", (h) => h("id").fields({ id: true, text: true })),
		);

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<CompositeRow>(["key1", "key2"]).fields({
		key1: true,
		key2: true,
		value: true,
	});

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<NestedRow>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (h) => h("id").fields({ id: true, content: true })),
		);

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOne("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	const withProfile = await hydrateData(usersWithProfile, hydrator);
	assert.deepStrictEqual(withProfile[0]?.profile, {
		name: "Alice P.",
		age: 30,
	});

	const withoutProfile = await hydrateData(usersWithoutProfile, hydrator);
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

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	const result = await hydrateData(rows, hydrator);

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

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (h) => h("name").fields({ name: true, age: true }));

	await assert.rejects(async () => {
		await hydrateData(rows, hydrator);
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

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

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

		const postHydrator = createHydrator<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachMany("comments", fetchComments, { matchChild: "postId", toParent: "id" });

		return await hydrateData(posts, postHydrator);
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

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

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

	assert.ok(Array.isArray(result[0]?.posts));
	assert.strictEqual(result[0]?.posts.length, 0);
});

test("attachMany: uses compareTo for custom matching keys", async () => {
	const users: User[] = [{ id: 100, name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, authorId: 100, title: "Post" }];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "authorId", toParent: "id" });

	const result = await hydrateData(users, hydrator);

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

	const hydrator = createHydrator<Entity>(["key1", "key2"])
		.fields({ key1: true, key2: true, value: true })
		.attachMany("related", fetchRelated, {
			matchChild: ["relKey1", "relKey2"],
			toParent: ["key1", "key2"],
		});

	const result = await hydrateData(entities, hydrator);

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

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne("latestPost", fetchPosts, { matchChild: "userId" });

	const withMatch = await hydrateData(usersWithMatch, hydrator);
	assert.deepStrictEqual(withMatch[0]?.latestPost, {
		id: 10,
		userId: 1,
		title: "First",
	});

	const withoutMatch = await hydrateData(usersWithoutMatch, hydrator);
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

		const postHydrator = createHydrator<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOne("latestComment", fetchComments, {
				matchChild: "postId",
				toParent: "id",
			});

		return await hydrateData(posts, postHydrator);
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.latestComment?.content, "Comment 1");
	assert.strictEqual(result[1]?.posts[0]?.latestComment, null);
});

test("attachOneOrThrow: returns entity when exists", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("requiredPost", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

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

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("requiredPost", fetchPosts, { matchChild: "userId" });

	await assert.rejects(async () => {
		await hydrateData(users, hydrator);
	}, ExpectedOneItemError);
});

test("attachOneOrThrow: works at nested level", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		const posts = [{ id: 10, userId: 1, title: "Post 1" }];

		const fetchAuthor = async () => {
			return [{ id: 100, postId: 10, name: "Author" }];
		};

		const postHydrator = createHydrator<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOneOrThrow("author", fetchAuthor, {
				matchChild: "postId",
				toParent: "id",
			});

		return await hydrateData(posts, postHydrator);
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

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

		const postHydrator = createHydrator<{
			id: number;
			userId: number;
			title: string;
		}>("id")
			.fields({ id: true, userId: true, title: true })
			.attachOneOrThrow("requiredAuthor", fetchAuthor, {
				matchChild: "postId",
				toParent: "id",
			});

		return await hydrateData(posts, postHydrator);
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	await assert.rejects(async () => {
		await hydrateData(users, hydrator);
	}, ExpectedOneItemError);
});

//
// Hydration Modes and Edge Cases
//

test("hydrate: handles single input", async () => {
	const user: User = { id: 1, name: "Alice" };

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrateData(user, hydrator);

	assert.deepStrictEqual(result, { id: 1, name: "Alice" });
});

test("hydrate: handles empty array", async () => {
	const users: User[] = [];

	const hydrator = createHydrator<User>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrateData(users, hydrator);

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

	const hydrator = createHydrator<NullableUser>("id").fields({
		id: true,
		name: true,
	});

	const result = await hydrateData(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0], { id: 1, name: "Alice" });
	assert.deepStrictEqual(result[1], { id: 2, name: "Bob" });
});

test("hydrate function: accepts inline hydrator creation", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const result = await hydrateData(users, (keyBy) => keyBy("id").fields({ id: true, name: true }));

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

test("chaining methods: creates immutable configurations", async () => {
	const base = createHydrator<User>("id").fields({ id: true });

	const withName = base.fields({ name: true });
	const withExtra = base.extras({ displayName: (u) => `User ${u.id}` });

	const users: User[] = [{ id: 1, name: "Alice" }];

	const resultBase = await hydrateData(users, base);
	const resultWithName = await hydrateData(users, withName);
	const resultWithExtra = await hydrateData(users, withExtra);

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

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.hasOne("profile", "profile$$", (h) => h("bio").fields({ bio: true }))
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrateData(users, hydrator);

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

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (h) =>
					h("id").fields({ id: true, content: true }).attachOne("author", fetchAuthors, {
						matchChild: "commentId",
						toParent: "id",
					}),
				)
				.attachMany("tags", fetchTags, { matchChild: "postId", toParent: "id" }),
		);

	const result = await hydrateData(rows, hydrator);

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

//
// Hydrator composition
//

test("extend: merges fields from two hydrators", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id").fields({ id: true });

	const nameHydrator = createHydrator<User>("id").fields({ name: true });

	const combined = baseHydrator.extend(nameHydrator);
	const result = await hydrateData(users, combined);

	assert.deepStrictEqual(result, [{ id: 1, name: "Alice" }]);
});

test("extend: other hydrator's fields take precedence", async () => {
	const users: User[] = [{ id: 1, name: "alice" }];

	const baseHydrator = createHydrator<User>("id").fields({
		id: true,
		name: (name) => name.toUpperCase(),
	});

	const otherHydrator = createHydrator<User>("id").fields({
		name: (name) => name.toLowerCase() + "-other",
	});

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

	assert.deepStrictEqual(result, [{ id: 1, name: "alice-other" }]);
});

test("extend: merges extras from two hydrators", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

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

test("extend: other hydrator's extras take precedence", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const baseHydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.extras({
			greeting: () => "Hello",
		});

	const otherHydrator = createHydrator<User>("id").extras({
		greeting: () => "Hi",
	});

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

	assert.strictEqual(result[0]?.greeting, "Hi");
});

test("extend: merges collections from two hydrators", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, title: "Post 1" }],
			comments: [{ id: 100, content: "Comment 1" }],
		},
	]);
});

test("extend: other hydrator's collections take precedence", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, title: "POST TITLE" }],
		},
	]);
});

test("extend: throws when keyBy doesn't match", () => {
	interface Post {
		id: number;
		userId: number;
	}

	const userHydrator = createHydrator<User>("id").fields({ id: true });

	const postHydrator = createHydrator<Post>("userId").fields({ userId: true });

	assert.throws(() => userHydrator.extend(postHydrator as any), KeyByMismatchError);
});

test("extend: works with composite keys", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(rows, combined);

	assert.deepStrictEqual(result, [{ userId: 1, postId: 10, content: "Hello" }]);
});

test("extend: works bidirectionally (no constraint on OtherInput)", async () => {
	interface AdminUser extends User {
		role: string;
	}

	const users: AdminUser[] = [{ id: 1, name: "Alice", role: "admin" }];

	const userHydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const adminHydrator = createHydrator<AdminUser>("id").fields({ role: true });

	// Direction 1: User extended with AdminUser
	const combined1 = userHydrator.extend(adminHydrator);
	const result1 = await hydrateData(users, combined1);

	assert.deepStrictEqual(result1, [{ id: 1, name: "Alice", role: "admin" }]);

	// Direction 2: AdminUser extended with User (reverse)
	const combined2 = adminHydrator.extend(userHydrator);
	const result2 = await hydrateData(users, combined2);

	assert.deepStrictEqual(result2, [{ id: 1, name: "Alice", role: "admin" }]);
});

test("extend: merges hasOne collections", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			profile: { id: 100, bio: "Developer" },
			settings: { id: 200, theme: "dark" },
		},
	]);
});

test("extend: merges hasOneOrThrow collections", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(rows, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			settings: { id: 100, theme: "dark" },
			preferences: { id: 200, language: "en" },
		},
	]);
});

test("extend: merges attachMany collections", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, userId: 1, title: "Post 1" }],
			comments: [{ id: 100, userId: 1, content: "Comment 1" }],
		},
	]);
});

test("extend: merges attachOne collections", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			profile: { userId: 1, bio: "Developer" },
			settings: { userId: 1, theme: "dark" },
		},
	]);
});

test("extend: merges attachOneOrThrow collections", async () => {
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

	const combined = baseHydrator.extend(otherHydrator);
	const result = await hydrateData(users, combined);

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
// Default keyBy
//

test("createHydrator: keyBy defaults to 'id' when input has id", async () => {
	// keyBy omitted - should default to "id"
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const hydrator = createHydrator<User>().fields({ id: true, name: true });

	const result = await hydrateData(users, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("hasMany: keyBy defaults to 'id' when nested input has id", async () => {
	type UserWithPosts = User & {
		posts$$id: number;
		posts$$title: string;
	};

	const data: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 1, posts$$title: "Post 1" },
		{ id: 1, name: "Alice", posts$$id: 2, posts$$title: "Post 2" },
		{ id: 2, name: "Bob", posts$$id: 3, posts$$title: "Post 3" },
	];

	// Both createHydrator and hasMany keyBy omitted
	const hydrator = createHydrator<UserWithPosts>()
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (create) => create().fields({ id: true, title: true }));

	const result = await hydrateData(data, hydrator);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0], { id: 1, title: "Post 1" });
	assert.strictEqual(result[1]?.posts.length, 1);
	assert.deepStrictEqual(result[1]?.posts[0], { id: 3, title: "Post 3" });
});

test("hasOne: keyBy defaults to 'id' when nested input has id", async () => {
	interface Post {
		id: number;
		title: string;
	}

	type PostWithAuthor = Post & {
		author$$id: number;
		author$$name: string;
	};

	const data: PostWithAuthor[] = [{ id: 1, title: "Post 1", author$$id: 1, author$$name: "Alice" }];

	// Both createHydrator and hasOne keyBy omitted
	const hydrator = createHydrator<PostWithAuthor>()
		.fields({ id: true, title: true })
		.hasOne("author", "author$$", (create) => create().fields({ id: true, name: true }));

	const result = await hydrateData(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0]?.author, { id: 1, name: "Alice" });
});

test("hasOneOrThrow: keyBy defaults to 'id' when nested input has id", async () => {
	type UserWithProfile = User & {
		profile$$id: number;
		profile$$bio: string;
	};

	const data: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: 1, profile$$bio: "Bio for Alice" },
	];

	// Both createHydrator and hasOneOrThrow keyBy omitted
	const hydrator = createHydrator<UserWithProfile>()
		.fields({ id: true, name: true })
		.hasOneOrThrow("profile", "profile$$", (create) => create().fields({ id: true, bio: true }));

	const result = await hydrateData(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.deepStrictEqual(result[0]?.profile, { id: 1, bio: "Bio for Alice" });
});

test("multiple nested levels: keyBy defaults to 'id' at all levels", async () => {
	type UserWithPostsAndComments = User & {
		posts$$id: number;
		posts$$title: string;
		posts$$comments$$id: number;
		posts$$comments$$content: string;
	};

	const data: UserWithPostsAndComments[] = [
		{
			id: 1,
			name: "Alice",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 1,
			posts$$comments$$content: "Comment 1",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 1,
			posts$$title: "Post 1",
			posts$$comments$$id: 2,
			posts$$comments$$content: "Comment 2",
		},
		{
			id: 1,
			name: "Alice",
			posts$$id: 2,
			posts$$title: "Post 2",
			posts$$comments$$id: 3,
			posts$$comments$$content: "Comment 3",
		},
	];

	// All keyBy parameters omitted
	const hydrator = createHydrator<UserWithPostsAndComments>()
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (create) =>
			create()
				.fields({ id: true, title: true })
				.hasMany("comments", "comments$$", (create) =>
					create().fields({ id: true, content: true }),
				),
		);

	const result = await hydrateData(data, hydrator);

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[0], { id: 1, content: "Comment 1" });
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[1], { id: 2, content: "Comment 2" });
	assert.strictEqual(result[0]?.posts[1]?.comments.length, 1);
	assert.deepStrictEqual(result[0]?.posts[1]?.comments[0], { id: 3, content: "Comment 3" });
});
