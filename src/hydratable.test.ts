import assert from "node:assert";
import { test } from "node:test";

import { createHydratable, hydrate } from "./hydratable.ts";

test("hydratable", async () => {
	type Selection = {
		id: number;
		name: string;
		posts__id?: number | null;
		posts__title?: string | null;
		posts__comments__id?: number | null;
		posts__comments__content?: string | null;
	};

	const rows: Selection[] = [
		{
			id: 1,
			name: "Alice",
			posts__id: 10,
			posts__title: "Hello World",
			posts__comments__id: 100,
			posts__comments__content: "Great post!",
		},
		{
			id: 1,
			name: "Alice",
			posts__id: 10,
			posts__title: "Hello World",
			posts__comments__id: 200,
			posts__comments__content: "Greater post!",
		},
		{ id: 1, name: "Alice", posts__id: 11, posts__title: "Another Post" },
		{ id: 2, name: "Bob", posts__id: null, posts__title: null },
	];

	const hydratable = createHydratable<Selection>("id")
		.fields({
			id: true,
			name: true,
		})
		.extras({
			fullName: (input) => `User: ${input.name} (ID: ${input.id})`,
		})
		.hasMany("posts", "posts__", (keyBy) =>
			keyBy("id")
				.fields({
					id: true,
					title: true,
				})
				.extras({
					foo: (input) => `Post: ${input.title} (ID: ${input.id})`,
				})
				.hasMany("comments", "comments__", (keyBy) =>
					keyBy("id").fields({
						id: true,
						content: true,
					}),
				),
		);

	assert.deepEqual(await hydrate(rows, hydratable), [
		{
			id: 1,
			name: "Alice",
			fullName: "User: Alice (ID: 1)",
			posts: [
				{
					id: 10,
					title: "Hello World",
					foo: "Post: Hello World (ID: 10)",
					comments: [
						{ id: 100, content: "Great post!" },
						{ id: 200, content: "Greater post!" },
					],
				},
				{
					id: 11,
					title: "Another Post",
					foo: "Post: Another Post (ID: 11)",
					comments: [],
				},
			],
		},
		{ id: 2, name: "Bob", fullName: "User: Bob (ID: 2)", posts: [] },
	]);
});
