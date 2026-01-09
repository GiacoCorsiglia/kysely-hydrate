import assert from "node:assert";
import { test } from "node:test";

import { createHydrator, hydrate } from "./hydrator.ts";

//
// Kitchen Sink Test: Mega Hydration with Everything
//
// One massive test with:
// - Multiple levels of nesting (4+ deep)
// - Row explosion from cartesian products
// - Mixed cardinality (hasOne + hasMany at various levels)
// - Sibling collections causing cartesian products
// - Multiple users, posts, comments, authors, tags, profiles
//

test("kitchen sink: mega hydration with deep nesting, cartesian products, and mixed cardinality", async () => {
	// Raw data with massive row explosion due to nested cartesian products
	// Multiple users with different patterns of nested data
	const data = [
		// Alice - User 1, Post 10 with 2 comments × 2 tags = 4 rows
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile$$id: 1,
			profile$$bio: "Alice bio",
			posts$$id: 10,
			posts$$title: "Alice Post 1",
			posts$$comments$$id: 100,
			posts$$comments$$content: "Comment 100",
			posts$$comments$$author$$id: 2,
			posts$$comments$$author$$username: "bob",
			posts$$tags$$id: 1000,
			posts$$tags$$name: "typescript",
		},
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile$$id: 1,
			profile$$bio: "Alice bio",
			posts$$id: 10,
			posts$$title: "Alice Post 1",
			posts$$comments$$id: 100,
			posts$$comments$$content: "Comment 100",
			posts$$comments$$author$$id: 2,
			posts$$comments$$author$$username: "bob",
			posts$$tags$$id: 1001,
			posts$$tags$$name: "nodejs",
		},
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile$$id: 1,
			profile$$bio: "Alice bio",
			posts$$id: 10,
			posts$$title: "Alice Post 1",
			posts$$comments$$id: 101,
			posts$$comments$$content: "Comment 101",
			posts$$comments$$author$$id: 3,
			posts$$comments$$author$$username: "carol",
			posts$$tags$$id: 1000,
			posts$$tags$$name: "typescript",
		},
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile$$id: 1,
			profile$$bio: "Alice bio",
			posts$$id: 10,
			posts$$title: "Alice Post 1",
			posts$$comments$$id: 101,
			posts$$comments$$content: "Comment 101",
			posts$$comments$$author$$id: 3,
			posts$$comments$$author$$username: "carol",
			posts$$tags$$id: 1001,
			posts$$tags$$name: "nodejs",
		},
		// Alice, Post 11 with 1 comment × 1 tag = 1 row
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile$$id: 1,
			profile$$bio: "Alice bio",
			posts$$id: 11,
			posts$$title: "Alice Post 2",
			posts$$comments$$id: 102,
			posts$$comments$$content: "Comment 102",
			posts$$comments$$author$$id: 2,
			posts$$comments$$author$$username: "bob",
			posts$$tags$$id: 1002,
			posts$$tags$$name: "testing",
		},

		// Bob - User 2, Post 20 with 2 comments × 2 tags = 4 rows
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 20,
			posts$$title: "Bob Post 1",
			posts$$comments$$id: 200,
			posts$$comments$$content: "Comment 200",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 2000,
			posts$$tags$$name: "sql",
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 20,
			posts$$title: "Bob Post 1",
			posts$$comments$$id: 200,
			posts$$comments$$content: "Comment 200",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 2001,
			posts$$tags$$name: "kysely",
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 20,
			posts$$title: "Bob Post 1",
			posts$$comments$$id: 201,
			posts$$comments$$content: "Comment 201",
			posts$$comments$$author$$id: 3,
			posts$$comments$$author$$username: "carol",
			posts$$tags$$id: 2000,
			posts$$tags$$name: "sql",
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 20,
			posts$$title: "Bob Post 1",
			posts$$comments$$id: 201,
			posts$$comments$$content: "Comment 201",
			posts$$comments$$author$$id: 3,
			posts$$comments$$author$$username: "carol",
			posts$$tags$$id: 2001,
			posts$$tags$$name: "kysely",
		},
		// Bob, Post 21 with 1 comment × 3 tags = 3 rows
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 21,
			posts$$title: "Bob Post 2",
			posts$$comments$$id: 202,
			posts$$comments$$content: "Comment 202",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 2002,
			posts$$tags$$name: "react",
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 21,
			posts$$title: "Bob Post 2",
			posts$$comments$$id: 202,
			posts$$comments$$content: "Comment 202",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 2003,
			posts$$tags$$name: "vue",
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile$$id: 2,
			profile$$bio: "Bob bio",
			posts$$id: 21,
			posts$$title: "Bob Post 2",
			posts$$comments$$id: 202,
			posts$$comments$$content: "Comment 202",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 2004,
			posts$$tags$$name: "svelte",
		},

		// Carol - User 3, Post 30 with 3 comments × 1 tag = 3 rows
		{
			id: 3,
			username: "carol",
			email: "carol@example.com",
			profile$$id: 3,
			profile$$bio: "Carol bio",
			posts$$id: 30,
			posts$$title: "Carol Post 1",
			posts$$comments$$id: 300,
			posts$$comments$$content: "Comment 300",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 3000,
			posts$$tags$$name: "rust",
		},
		{
			id: 3,
			username: "carol",
			email: "carol@example.com",
			profile$$id: 3,
			profile$$bio: "Carol bio",
			posts$$id: 30,
			posts$$title: "Carol Post 1",
			posts$$comments$$id: 301,
			posts$$comments$$content: "Comment 301",
			posts$$comments$$author$$id: 2,
			posts$$comments$$author$$username: "bob",
			posts$$tags$$id: 3000,
			posts$$tags$$name: "rust",
		},
		{
			id: 3,
			username: "carol",
			email: "carol@example.com",
			profile$$id: 3,
			profile$$bio: "Carol bio",
			posts$$id: 30,
			posts$$title: "Carol Post 1",
			posts$$comments$$id: 302,
			posts$$comments$$content: "Comment 302",
			posts$$comments$$author$$id: 1,
			posts$$comments$$author$$username: "alice",
			posts$$tags$$id: 3000,
			posts$$tags$$name: "rust",
		},
	];

	// Build the deeply nested hydrator
	const hydrator = createHydrator<(typeof data)[number]>("id")
		.fields({ id: true, username: true, email: true })
		// hasOne: profile
		.hasOne("profile", "profile$$", (h) => h("id").fields({ id: true, bio: true }))
		// hasMany: posts with nested comments (hasMany) and tags (hasMany)
		.hasMany("posts", "posts$$", (h) =>
			h("id")
				.fields({ id: true, title: true })
				// hasMany nested: comments with nested author (hasOne)
				.hasMany("comments", "comments$$", (h) =>
					h("id")
						.fields({ id: true, content: true })
						// hasOne nested 3 levels deep: author
						.hasOne("author", "author$$", (h) => h("id").fields({ id: true, username: true })),
				)
				// hasMany sibling to comments: tags (causes cartesian product)
				.hasMany("tags", "tags$$", (h) => h("id").fields({ id: true, name: true })),
		);

	const result = await hydrate(data, hydrator);

	// One massive assertion with the entire expected result
	assert.deepStrictEqual(result, [
		{
			id: 1,
			username: "alice",
			email: "alice@example.com",
			profile: {
				id: 1,
				bio: "Alice bio",
			},
			posts: [
				{
					id: 10,
					title: "Alice Post 1",
					comments: [
						{
							id: 100,
							content: "Comment 100",
							author: {
								id: 2,
								username: "bob",
							},
						},
						{
							id: 101,
							content: "Comment 101",
							author: {
								id: 3,
								username: "carol",
							},
						},
					],
					tags: [
						{
							id: 1000,
							name: "typescript",
						},
						{
							id: 1001,
							name: "nodejs",
						},
					],
				},
				{
					id: 11,
					title: "Alice Post 2",
					comments: [
						{
							id: 102,
							content: "Comment 102",
							author: {
								id: 2,
								username: "bob",
							},
						},
					],
					tags: [
						{
							id: 1002,
							name: "testing",
						},
					],
				},
			],
		},
		{
			id: 2,
			username: "bob",
			email: "bob@example.com",
			profile: {
				id: 2,
				bio: "Bob bio",
			},
			posts: [
				{
					id: 20,
					title: "Bob Post 1",
					comments: [
						{
							id: 200,
							content: "Comment 200",
							author: {
								id: 1,
								username: "alice",
							},
						},
						{
							id: 201,
							content: "Comment 201",
							author: {
								id: 3,
								username: "carol",
							},
						},
					],
					tags: [
						{
							id: 2000,
							name: "sql",
						},
						{
							id: 2001,
							name: "kysely",
						},
					],
				},
				{
					id: 21,
					title: "Bob Post 2",
					comments: [
						{
							id: 202,
							content: "Comment 202",
							author: {
								id: 1,
								username: "alice",
							},
						},
					],
					tags: [
						{
							id: 2002,
							name: "react",
						},
						{
							id: 2003,
							name: "vue",
						},
						{
							id: 2004,
							name: "svelte",
						},
					],
				},
			],
		},
		{
			id: 3,
			username: "carol",
			email: "carol@example.com",
			profile: {
				id: 3,
				bio: "Carol bio",
			},
			posts: [
				{
					id: 30,
					title: "Carol Post 1",
					comments: [
						{
							id: 300,
							content: "Comment 300",
							author: {
								id: 1,
								username: "alice",
							},
						},
						{
							id: 301,
							content: "Comment 301",
							author: {
								id: 2,
								username: "bob",
							},
						},
						{
							id: 302,
							content: "Comment 302",
							author: {
								id: 1,
								username: "alice",
							},
						},
					],
					tags: [
						{
							id: 3000,
							name: "rust",
						},
					],
				},
			],
		},
	]);

	console.log("✅ Kitchen sink mega hydration passed!");
});
