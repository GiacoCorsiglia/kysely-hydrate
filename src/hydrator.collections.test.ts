import assert from "node:assert";
import { test } from "node:test";

import { ExpectedOneItemError } from "./helpers/errors.ts";
import { createHydrator, hydrate } from "./hydrator.ts";

// Test data types
interface User {
	id: number;
	name: string;
}

//
// Nested collections: hasMany / hasOne / hasOneOrThrow
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

	const result = await hydrate(rows, hydrator);

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

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(result[0]?.posts[0]?.comments.length, 2);
	assert.deepStrictEqual(result[0]?.posts[0]?.comments[0], {
		id: 100,
		content: "Comment 1",
	});
});

test("hasOne: returns single nested entity or null", async () => {
	// (hasOne never returns a "first" entity: more than one distinct nested
	// entity is a cardinality violation and throws — see the grouping test
	// "hasOne throws CardinalityViolationError for multiple distinct children")
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

	const withProfile = await hydrate(usersWithProfile, hydrator);
	assert.deepStrictEqual(withProfile[0]?.profile, {
		name: "Alice P.",
		age: 30,
	});

	const withoutProfile = await hydrate(usersWithoutProfile, hydrator);
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

	const result = await hydrate(rows, hydrator);

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
		await hydrate(rows, hydrator);
	}, ExpectedOneItemError);
});

// A parent hydrated from one row (in an array, or as a top-level object)
// hydrates its nested collections from that row alone, without grouping it;
// from two copies of the row, it groups them.  All must agree on every mode, on
// missing children, and on map functions that return undefined (which "one"
// modes treat as no entity).
test("nested collections hydrate the same from one row as from duplicated rows", async () => {
	type Row = { id: number; child$$a: number | null; child$$b: number | null };
	const present: Row = { id: 1, child$$a: 1, child$$b: 2 };
	const missing: Row = { id: 1, child$$a: null, child$$b: null };
	const partlyMissing: Row = { id: 1, child$$a: 1, child$$b: null };

	const results = async (
		mode: "many" | "one" | "oneOrThrow",
		keyBy: "a" | ["a", "b"],
		row: Row,
		map: (child: object) => unknown = (child) => child,
	) => {
		const hydrator = createHydrator<Row>("id").has(mode, "child", "child$$", (h) =>
			h(keyBy).fields({ a: true }).map(map),
		);
		const settle = (output: Promise<unknown>) =>
			output.then(
				(value) => ({ value }),
				(error: unknown) => ({ error: (error as Error).constructor }),
			);
		const single = await settle(hydrate([row], hydrator));
		// The top-level object path must agree with the one-row array path.
		const object = await settle(hydrate(row, hydrator).then((value) => [value]));
		assert.deepStrictEqual(object, single, "hydrate(row) vs. hydrate([row])");
		return [single, await settle(hydrate([row, { ...row }], hydrator))] as const;
	};

	for (const mode of ["many", "one", "oneOrThrow"] as const) {
		for (const keyBy of ["a", ["a", "b"]] satisfies ("a" | ["a", "b"])[]) {
			for (const row of [present, missing, partlyMissing]) {
				const [single, grouped] = await results(mode, keyBy, row);
				assert.deepStrictEqual(single, grouped, `${mode} ${String(keyBy)} ${JSON.stringify(row)}`);
			}
			const [single, grouped] = await results(mode, keyBy, present, () => undefined);
			assert.deepStrictEqual(single, grouped, `${mode} ${String(keyBy)} mapped to undefined`);
		}
	}

	// Spot-check that the cases above cover what they mean to.
	assert.deepStrictEqual(await results("many", "a", present, () => undefined), [
		{ value: [{ child: [undefined] }] },
		{ value: [{ child: [undefined] }] },
	]);
	assert.deepStrictEqual((await results("one", ["a", "b"], partlyMissing))[0], {
		value: [{ child: null }],
	});
	assert.deepStrictEqual((await results("oneOrThrow", "a", missing))[0], {
		error: ExpectedOneItemError,
	});
});

//
// Array independence (hasMany)
//

test("hasMany: parents with identical child content receive independent arrays", async () => {
	// Same ownership contract as the attachMany independent-arrays test (in
	// hydrator.attach.test.ts), but for joined
	// collections: even when two parents' hydrated children are identical in
	// content, each parent must get its own array, so mutation cannot leak
	// between siblings (pins the contract against future implementation changes
	// such as caching identical child groups).
	interface UserWithPosts extends User {
		posts$$id: number;
		posts$$title: string;
	}

	const rows: UserWithPosts[] = [
		{ id: 1, name: "Alice", posts$$id: 10, posts$$title: "shared" },
		{ id: 1, name: "Alice", posts$$id: 11, posts$$title: "shared too" },
		{ id: 2, name: "Bob", posts$$id: 10, posts$$title: "shared" },
		{ id: 2, name: "Bob", posts$$id: 11, posts$$title: "shared too" },
	];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const result = await hydrator.hydrate(rows);

	assert.deepStrictEqual(result[0]?.posts, result[1]?.posts);
	assert.notStrictEqual(result[0]!.posts, result[1]!.posts);

	result[0]!.posts.pop();
	assert.strictEqual(result[0]!.posts.length, 1);
	assert.strictEqual(result[1]!.posts.length, 2);
});

//
// Mixing nested and attached collections
//

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

	const result = await hydrate(users, hydrator);

	assert.strictEqual(result.length, 2);
	assert.deepStrictEqual(result[0]?.profile, { bio: "Developer" });
	assert.strictEqual(result[0]?.posts.length, 1);
	assert.deepStrictEqual(result[1]?.profile, { bio: "Designer" });
	assert.strictEqual(result[1]?.posts.length, 1);
});

test("collection override: has after attach with the same key drops the attach", async () => {
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

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.attachMany("posts", fetchPosts, { matchChild: "userId" })
		.hasMany("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }));

	const result = await hydrate(rows, hydrator);

	assert.strictEqual(fetchCount, 0);
	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [{ id: 10, title: "Joined Post" }] },
	]);
});

test("collection override: attach after has with the same key drops the nested spec", async () => {
	interface UserWithPosts extends User {
		posts$$id: number | null;
		posts$$title: string | null;
	}

	// No prefixed child columns exist in the data — as when a query set removes
	// an overridden join's SQL.  A stale oneOrThrow spec would throw
	// ExpectedOneItemError here.
	const rows: UserWithPosts[] = [{ id: 1, name: "Alice", posts$$id: null, posts$$title: null }];

	const fetchPosts = async () => [{ id: 10, userId: 1, title: "Attached Post" }];

	const hydrator = createHydrator<UserWithPosts>("id")
		.fields({ id: true, name: true })
		.hasOneOrThrow("posts", "posts$$", (h) => h("id").fields({ id: true, title: true }))
		.attachMany("posts", fetchPosts, { matchChild: "userId" });

	const result = await hydrate(rows, hydrator);

	assert.deepStrictEqual(result, [
		{ id: 1, name: "Alice", posts: [{ id: 10, userId: 1, title: "Attached Post" }] },
	]);
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

	const result = await hydrate(rows, hydrator);

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
// Sibling collections at the same level
//

test("hasMany: multiple sibling collections at same level", async () => {
	interface PostWithCommentsAndUsers {
		id: number;
		title: string;
		user_id: number;
		comments$$id: number | null;
		comments$$content: string | null;
		comments$$post_id: number | null;
		users$$id: number | null;
		users$$username: string | null;
	}

	const raw: PostWithCommentsAndUsers[] = [
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments$$id: 1,
			comments$$content: "Comment 1 on post 1",
			comments$$post_id: 1,
			users$$id: 2,
			users$$username: "bob",
		},
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments$$id: 2,
			comments$$content: "Comment 2 on post 1",
			comments$$post_id: 1,
			users$$id: 2,
			users$$username: "bob",
		},
		{
			id: 2,
			title: "Post 2",
			user_id: 2,
			comments$$id: 3,
			comments$$content: "Comment 3 on post 2",
			comments$$post_id: 2,
			users$$id: 2,
			users$$username: "bob",
		},
	];

	const hydrator = createHydrator<PostWithCommentsAndUsers>("id")
		.fields({ id: true, title: true, user_id: true })
		.hasMany("comments", "comments$$", (h) =>
			h("id").fields({ id: true, content: true, post_id: true }),
		)
		.hasMany("users", "users$$", (h) => h("id").fields({ id: true, username: true }));

	const result = await hydrate(raw, hydrator);

	// Expected: 2 posts
	// Post 1: 2 comments, 1 user (deduplicated by keyBy)
	// Post 2: 1 comment, 1 user
	assert.strictEqual(result.length, 2);
	assert.strictEqual(result[0]?.comments.length, 2);
	assert.strictEqual(result[1]?.comments.length, 1);

	// IMPORTANT: Sibling hasMany collections should deduplicate based on keyBy
	// The raw data has cartesian product (2 comments × 1 user = 2 rows with same user)
	// But the hydrator should deduplicate the users array to only contain 1 bob entry
	assert.strictEqual(result[0]?.users.length, 1); // Should be 1, not 2!
	assert.strictEqual(result[1]?.users.length, 1);

	assert.deepStrictEqual(result, [
		{
			id: 1,
			title: "Post 1",
			user_id: 2,
			comments: [
				{ id: 1, content: "Comment 1 on post 1", post_id: 1 },
				{ id: 2, content: "Comment 2 on post 1", post_id: 1 },
			],
			users: [
				{ id: 2, username: "bob" }, // Should only appear once (deduplicated)
			],
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
