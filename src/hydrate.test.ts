import { test } from "node:test";
import util from "util";
import { hydrate } from "./hydrate.ts";

test("denormalize", () => {
	const rows = [
		{
			id: 1,
			name: "Alice",
			post_id: 10,
			post_title: "Hello World",
			comment_id: 100,
			comment_content: "Great post!",
		},
		{
			id: 1,
			name: "Alice",
			post_id: 10,
			post_title: "Hello World",
			comment_id: 200,
			comment_content: "Greater post!",
		},
		{ id: 1, name: "Alice", post_id: 11, post_title: "Another Post" },
		{ id: 2, name: "Bob", post_id: null, post_title: null },
	];

	const ting = hydrate(rows, {
		keyBy: "id",

		fields: {
			id: true,
			name: true,
		},

		extras: {
			fullName: (row) => `User: ${row.name} (ID: ${row.id})`,
		},

		collections: {
			posts: {
				keyBy: "post_id",
				fields: {
					post_id: true,
					post_title: true,
				},

				collections: {
					comments: {
						fields: {
							comment_id: true,
							comment_content: true,
						},
					},
				},
			},
		},
	});

	console.log(
		// Deep inspection of the denormalized result
		util.inspect(
			hydrate(rows, {
				keyBy: "id",

				fields: {
					id: true,
					name: true,
				},

				extras: {
					fullName: (row) => `User: ${row.name} (ID: ${row.id})`,
				},

				collections: {
					posts: {
						keyBy: "post_id",
						fields: {
							post_id: true,
							post_title: true,
						},

						collections: {
							comments: {
								fields: {
									comment_id: true,
									comment_content: true,
								},
							},
						},
					},
				},
			}),
			{ depth: null, colors: true },
		),
	);
});
