// Attached collections fed by query sets, query builders and plain promises.
import { expectTypeOf } from "expect-type";

import { querySet } from "../../src/index.ts";
import { db } from "./schema.ts";

declare function fetchAvatars(userIds: number[]): Promise<{ user_id: number; url: string }[]>;

const posts = querySet(db)
	.selectAs("post", db.selectFrom("posts").select(["id", "user_id", "title"]))
	.attachOneOrThrow(
		"author",
		(posts) =>
			querySet(db)
				.selectAs("user", (eb) =>
					eb
						.selectFrom("users")
						.select(["id", "username"])
						.where(
							"id",
							"in",
							posts.map((p) => p.user_id),
						),
				)
				.attachOne("avatar", (users) => fetchAvatars(users.map((u) => u.id)), {
					matchChild: "user_id",
				}),
		{ matchChild: "id", toParent: "user_id" },
	)
	.attachMany(
		"comments",
		(posts) =>
			db
				.selectFrom("comments")
				.select(["id", "post_id", "content"])
				.where(
					"post_id",
					"in",
					posts.map((p) => p.id),
				),
		{ matchChild: "post_id" },
	)
	.attachMany(
		"tags",
		(posts) =>
			querySet(db).selectAs("tag", (eb) =>
				eb
					.selectFrom("post_tags")
					.innerJoin("tags", "tags.id", "post_tags.tag_id")
					.select(["tags.id", "tags.name", "post_tags.post_id"])
					.where(
						"post_tags.post_id",
						"in",
						posts.map((p) => p.id),
					),
			),
		{ matchChild: "post_id" },
	);

expectTypeOf(posts.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		user_id: number;
		title: string;
		author: {
			id: number;
			username: string;
			avatar: { user_id: number; url: string } | null;
		};
		comments: { id: number; post_id: number; content: string }[];
		tags: { id: number; name: string; post_id: number }[];
	}[]
>();
