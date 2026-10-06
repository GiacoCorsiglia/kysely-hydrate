import assert from "node:assert";
import { test } from "node:test";

import { AttachedKeysArityMismatchError, ExpectedOneItemError } from "./helpers/errors.ts";
import { createHydrator, hydrate } from "./hydrator.ts";

// Test data types
interface User {
	id: number;
	name: string;
}

//
// Attached collections: attachMany / attachOne / attachOneOrThrow
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

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.posts.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0], {
		id: 10,
		userId: 1,
		title: "Alice Post 1",
	});
	assert.strictEqual(result[1]?.posts.length, 1);
});

test("attachMany: accepts a one-shot iterable (generator) input", async () => {
	// Attach-fetching and hydration both consume the input; the hydrator must
	// materialize a one-shot iterable once rather than iterating it twice
	// (which would silently hydrate zero rows).
	function* generateUsers(): Generator<User> {
		yield { id: 1, name: "Alice" };
		yield { id: 2, name: "Bob" };
	}

	const fetchPosts = async (inputs: User[]) =>
		[
			{ id: 10, userId: 1, title: "Alice Post 1" },
			{ id: 12, userId: 2, title: "Bob Post 1" },
		].filter((p) => inputs.some((u) => u.id === p.userId));

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrator.hydrate(generateUsers());

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [{ id: 10, userId: 1, title: "Alice Post 1" }] },
		{ id: 2, name: "Bob", posts: [{ id: 12, userId: 2, title: "Bob Post 1" }] },
	]);
});

test("hydrate: accepts a one-shot iterable (generator) input without attaches", async () => {
	function* generateUsers(): Generator<User> {
		yield { id: 1, name: "Alice" };
		yield { id: 2, name: "Bob" };
	}

	const hydrator = createHydrator<User>("id").fields({ id: true, name: true });

	const result = await hydrator.hydrate(generateUsers());

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	]);
});

test("attach: fetchFn receives deduplicated inputs with null-key rows dropped", async () => {
	// Inputs that look like raw joined rows: Alice appears twice (row explosion)
	// and one row has a null key (a left join phantom).  The fetchFn must
	// receive one input per actual entity — the same rules hydration applies.
	const rows = [
		{ id: 1, name: "Alice" },
		{ id: 1, name: "Alice" },
		{ id: null as unknown as number, name: null as unknown as string },
		{ id: 2, name: "Bob" },
	];

	const received: User[][] = [];

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			(inputs: User[]) => {
				received.push(inputs);
				return [];
			},
			{ matchChild: "userId" },
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [] },
		{ id: 2, name: "Bob", posts: [] },
	]);
	assert.deepStrictEqual(received, [
		[
			{ id: 1, name: "Alice" },
			{ id: 2, name: "Bob" },
		],
	]);
});

test("attach: fetchFn is not called when all parent keys are nil", async () => {
	// Every row is a left-join phantom with a nil key, so no parent entity
	// exists to attach to.  The fetchFn must be skipped entirely: user code
	// building `WHERE x IN (...)` from the inputs would otherwise generate
	// invalid or pointless SQL.
	const rows = [
		{ id: null as unknown as number, name: null as unknown as string },
		{ id: null as unknown as number, name: null as unknown as string },
	];

	let fetchCount = 0;

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			() => {
				fetchCount++;
				return [];
			},
			{ matchChild: "userId" },
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result, []);
	assert.strictEqual(fetchCount, 0);
});

test("attach: fetchFn is not called when there are no input rows", async () => {
	let fetchCount = 0;

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			() => {
				fetchCount++;
				return [];
			},
			{ matchChild: "userId" },
		);

	const result = await hydrator.hydrate([]);

	assert.deepStrictEqual(result, []);
	assert.strictEqual(fetchCount, 0);
});

test("attach: nested attach fetchFn is not called when all nested keys are nil", async () => {
	// The empty-input skip must apply per nesting level: the parents exist (so
	// their fetch runs), but every nested profile is a matchless left join, so
	// the profile-level attach fetch must be skipped.
	interface UserWithProfile extends User {
		profile$$id: number | null;
	}

	const rows: UserWithProfile[] = [
		{ id: 1, name: "Alice", profile$$id: null },
		{ id: 2, name: "Bob", profile$$id: null },
	];

	let parentFetchCount = 0;
	let nestedFetchCount = 0;

	const hydrator = createHydrator<UserWithProfile>("id")
		.fields({ id: true, name: true })
		.attachMany(
			"posts",
			() => {
				parentFetchCount++;
				return [];
			},
			{ matchChild: "userId" },
		)
		.hasOne("profile", "profile$$", (h) =>
			h("id")
				.fields(["id"])
				.attachMany(
					"badges",
					() => {
						nestedFetchCount++;
						return [];
					},
					{ matchChild: "profileId" },
				),
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [], profile: null },
		{ id: 2, name: "Bob", posts: [], profile: null },
	]);
	assert.strictEqual(parentFetchCount, 1);
	assert.strictEqual(nestedFetchCount, 0);
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

		return await hydrate(posts, postHydrator);
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

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

test("attachMany: parents sharing a match value receive independent arrays", async () => {
	// Both posts attach the same two tags (same categoryId).  Each parent must
	// get its own array: the grouped rows are internal storage, and handing the
	// same instance to both parents would let one parent's mutation (e.g. a
	// .sort() in a map function) corrupt its sibling's collection.
	interface CategorizedPost {
		id: number;
		categoryId: number;
		title: string;
	}

	const posts: CategorizedPost[] = [
		{ id: 1, categoryId: 7, title: "A" },
		{ id: 2, categoryId: 7, title: "B" },
	];

	const fetchTags = async () => [
		{ id: 100, categoryId: 7, tag: "x" },
		{ id: 101, categoryId: 7, tag: "y" },
	];

	const hydrator = createHydrator<CategorizedPost>("id")
		.fields({ id: true, title: true })
		.attachMany("tags", fetchTags, { matchChild: "categoryId", toParent: "categoryId" });

	const result = await hydrator.hydrate(posts);

	assert.deepStrictEqual(result[0]?.tags, result[1]?.tags);
	assert.notStrictEqual(result[0]?.tags, result[1]?.tags);

	// Mutating one parent's collection must not affect the sibling's.
	result[0]!.tags.pop();
	assert.strictEqual(result[0]!.tags.length, 1);
	assert.strictEqual(result[1]!.tags.length, 2);
});

test("attachMany: returns empty array when no matches", async () => {
	const users: User[] = [{ id: 999, name: "NoMatch" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

	assert.ok(Array.isArray(result[0]?.posts));
	assert.strictEqual(result[0]?.posts.length, 0);
});

test("attachMany: uses toParent for custom matching keys", async () => {
	// toParent names a NON-key parent column: if toParent were ignored, the
	// default (the keyBy, id = 100) would match nothing and posts would be []
	interface UserWithSlug extends User {
		slug: string;
	}

	const users: UserWithSlug[] = [{ id: 100, slug: "alice", name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, authorSlug: "alice", title: "Post" }];
	};

	const hydrator = createHydrator<UserWithSlug>("id")
		.fields({ id: true, slug: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "authorSlug", toParent: "slug" });

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result[0]?.posts, [{ id: 10, authorSlug: "alice", title: "Post" }]);
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

	const result = await hydrate(entities, hydrator);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.related.length, 2);
	assert.strictEqual(result[1]?.related.length, 1);
});

test("attachMany: composite keys sharing a first part match separately", async () => {
	interface Entity {
		key1: string;
		key2: number;
	}

	// Parents sharing a first part share a branch of the lookup structure: each
	// must still see only its own children, and a nil child part matches none.
	const entities: Entity[] = [
		{ key1: "a", key2: 1 },
		{ key1: "a", key2: 2 },
		{ key1: "b", key2: 1 },
	];

	const fetchRelated = async () => [
		{ relKey1: "a", relKey2: 2, data: "a2" },
		{ relKey1: "a", relKey2: 1, data: "a1" },
		{ relKey1: "b", relKey2: 1, data: "b1" },
		{ relKey1: "a", relKey2: null, data: "orphan" },
	];

	const hydrator = createHydrator<Entity>(["key1", "key2"])
		.fields({ key1: true, key2: true })
		.attachMany("related", fetchRelated, {
			matchChild: ["relKey1", "relKey2"],
			toParent: ["key1", "key2"],
		});

	const result = await hydrate(entities, hydrator);

	assert.deepStrictEqual(
		result.map((entity) => entity.related.map((related) => related.data)),
		[["a1"], ["a2"], ["b1"]],
	);
});

// matchChild and toParent are independent, so they may describe a one-part key
// with either `keyBy` shape.
const matchKeyCases: [
	name: string,
	matchChild: "id" | readonly ["id"],
	toParent: "id" | readonly ["id"],
][] = [
	["a one-part array matchChild matches a string toParent", ["id"], "id"],
	["a string matchChild matches a one-part array toParent", "id", ["id"]],
];

for (const [name, matchChild, toParent] of matchKeyCases) {
	test(`attachMany: ${name}`, async () => {
		const hydrator = createHydrator<{ id: number }>("id")
			.fields({ id: true })
			.attachMany("related", async () => [{ id: 1, other: 1, data: "child" }], {
				matchChild,
				toParent,
			});

		const result = await hydrate([{ id: 1 }], hydrator);

		assert.deepStrictEqual(
			result.map((entity) => entity.related.map((related) => related.data)),
			[["child"]],
		);
	});
}

// Keys of different arity can never match, so registering them is a mistake
// that would otherwise surface as every parent attaching nothing.
const arityMismatchCases: [name: string, keys: any][] = [
	["more matchChild parts than toParent", { matchChild: ["id", "other"], toParent: "id" }],
	["more toParent parts than matchChild", { matchChild: "id", toParent: ["id", "id"] }],
	[
		"a one-part array does not excuse a mismatch",
		{ matchChild: ["id", "other"], toParent: ["id"] },
	],
	// toParent defaults to the parent's keyBy, which is one part here.
	["a mismatch against the default toParent", { matchChild: ["id", "other"] }],
];

for (const [name, keys] of arityMismatchCases) {
	test(`attach: throws on ${name}`, () => {
		const hydrator = createHydrator<{ id: number }>("id").fields({ id: true });

		assert.throws(
			() => hydrator.attachMany("related", async () => [], keys),
			AttachedKeysArityMismatchError,
		);
	});
}

test("attach: the arity mismatch error names the collection and both arities", () => {
	const hydrator = createHydrator<{ id: number }>("id").fields({ id: true });

	assert.throws(
		() =>
			hydrator.attachMany("related", async () => [], {
				matchChild: ["id", "other"],
				toParent: "id",
			} as any),
		(error: Error) => {
			assert.match(error.message, /"related"/);
			assert.match(error.message, /2 key part\(s\) but toParent has 1/);
			return true;
		},
	);
});

test("attachOne: returns single match or null", async () => {
	const usersWithMatch: User[] = [{ id: 1, name: "Alice" }];
	const usersWithoutMatch: User[] = [{ id: 999, name: "NoMatch" }];

	const fetchPosts = async () => {
		return [
			{ id: 10, userId: 1, title: "Only Post" }, // Single post for user 1
		];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne("latestPost", fetchPosts, { matchChild: "userId" });

	const withMatch = await hydrate(usersWithMatch, hydrator);
	assert.deepStrictEqual(withMatch[0]?.latestPost, {
		id: 10,
		userId: 1,
		title: "Only Post",
	});

	const withoutMatch = await hydrate(usersWithoutMatch, hydrator);
	assert.strictEqual(withoutMatch[0]?.latestPost, null);
});

test("attachOne: throws on cardinality violation", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		return [
			{ id: 10, userId: 1, title: "First" },
			{ id: 11, userId: 1, title: "Second" }, // Multiple posts for same user
		];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne("latestPost", fetchPosts, { matchChild: "userId" });

	await assert.rejects(
		async () => await hydrate(users, hydrator),
		(err: Error) => {
			assert.ok(err.message.includes("Expected exactly one item"));
			assert.ok(err.message.includes("latestPost"));
			assert.ok(err.message.includes("but got 2"));
			return true;
		},
	);
});

test("attachOne: works at nested level", async () => {
	// The attach is embedded in a hasMany sub-hydrator built from prefixed
	// rows — the library's real nested-attach machinery — rather than a
	// manual hydrate() call inside a parent fetchFn
	interface UserWithPosts extends User {
		posts$$id: number;
		posts$$title: string;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post 1" },
		{ id: 1, name: "Alice", posts$$id: 11, posts$$title: "Post 2" },
		{ id: 2, name: "Bob", posts$$id: 12, posts$$title: "Post 3" },
	];

	const fetchComments = async () => [
		{ id: 100, postId: 10, content: "Comment 1" },
		{ id: 102, postId: 12, content: "Comment 2" },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.attachOne("latestComment", fetchComments, { matchChild: "postId", toParent: "id" }),
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [
				{
					id: 10,
					title: "Post 1",
					latestComment: { id: 100, postId: 10, content: "Comment 1" },
				},
				{ id: 11, title: "Post 2", latestComment: null },
			],
		},
		{
			id: 2,
			name: "Bob",
			posts: [
				{
					id: 12,
					title: "Post 3",
					latestComment: { id: 102, postId: 12, content: "Comment 2" },
				},
			],
		},
	]);
});

test("attachOneOrThrow: returns entity when exists", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		return [{ id: 10, userId: 1, title: "Post" }];
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOneOrThrow("requiredPost", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

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
		await hydrate(users, hydrator);
	}, ExpectedOneItemError);
});

test("attachOneOrThrow: works at nested level", async () => {
	// Embedded in a hasMany sub-hydrator: exercises the real nested-attach
	// machinery (see "attachOne: works at nested level")
	interface UserWithPosts extends User {
		posts$$id: number;
		posts$$title: string;
	}

	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post 1" }];

	const fetchAuthor = async () => [{ id: 100, postId: 10, name: "Author" }];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.attachOneOrThrow("author", fetchAuthor, { matchChild: "postId", toParent: "id" }),
		);

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			name: "Alice",
			posts: [{ id: 10, title: "Post 1", author: { id: 100, postId: 10, name: "Author" } }],
		},
	]);
});

test("attachOneOrThrow: throws at nested level when missing", async () => {
	interface UserWithPosts extends User {
		posts$$id: number;
		posts$$title: string;
	}

	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "Post 1" }];

	const fetchAuthor = async (): Promise<{ id: number; postId: number }[]> => [];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				.attachOneOrThrow("requiredAuthor", fetchAuthor, { matchChild: "postId", toParent: "id" }),
		);

	await assert.rejects(async () => {
		await hydrator.hydrate(rows);
	}, ExpectedOneItemError);
});

//
// Executable support in attach methods
//

test("attachMany: accepts Executable return from fetchFn", async () => {
	const users: User[] = [
		{ id: 1, name: "Alice" },
		{ id: 2, name: "Bob" },
	];

	const fetchPosts = async () => {
		return {
			execute: async () => [
				{ id: 10, userId: 1, title: "Alice Post" },
				{ id: 11, userId: 2, title: "Bob Post" },
			],
		};
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.posts.length, 1);
	assert.deepStrictEqual(result[0]?.posts[0], { id: 10, userId: 1, title: "Alice Post" });
	assert.strictEqual(result[1]?.posts.length, 1);
	assert.deepStrictEqual(result[1]?.posts[0], { id: 11, userId: 2, title: "Bob Post" });
});

test("attachOne: accepts Executable return from fetchFn", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = () => ({
		execute: async () => [{ id: 10, userId: 1, title: "Post" }],
	});

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachOne("latestPost", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

	assert.deepStrictEqual(result[0]?.latestPost, { id: 10, userId: 1, title: "Post" });
});

test("attachMany: accepts Promise<Executable> return from fetchFn", async () => {
	const users: User[] = [{ id: 1, name: "Alice" }];

	const fetchPosts = async () => {
		await Promise.resolve(); // Simulate async work
		return {
			execute: async () => [{ id: 10, userId: 1, title: "Post" }],
		};
	};

	const hydrator = createHydrator<User>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result[0]?.posts.length, 1);
});
