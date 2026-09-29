// insertAs and updateAs query sets that join the rows they write to other tables.
import { expectTypeOf } from "expect-type";

import { querySet } from "../../src/index.ts";
import { db } from "./schema.ts";

const author = querySet(db)
	.selectAs("author", db.selectFrom("users").select(["id", "username"]))
	.leftJoinOne(
		"profile",
		({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "user_id", "avatar_url"])),
		"profile.user_id",
		"author.id",
	);

const inserted = querySet(db)
	.insertAs("post", (db) =>
		db
			.insertInto("posts")
			.values({ user_id: 1, title: "Hello", slug: "hello", content: "", status: "draft" })
			.returning(["id", "user_id", "title", "status"]),
	)
	.innerJoinOne("author", author, "author.id", "post.user_id")
	.leftJoinMany(
		"tags",
		({ eb, qs }) =>
			qs(
				eb
					.selectFrom("post_tags")
					.innerJoin("tags", "tags.id", "post_tags.tag_id")
					.select(["tags.id", "tags.name", "post_tags.post_id"]),
			),
		"tags.post_id",
		"post.id",
	);

const updated = querySet(db)
	.updateAs("user", (db) =>
		db
			.updateTable("users")
			.set({ display_name: "New name" })
			.where("id", "=", 1)
			.returning(["id", "username", "display_name"]),
	)
	.leftJoinOne(
		"settings",
		({ eb, qs }) => qs(eb.selectFrom("settings").select(["id", "user_id", "theme"])),
		"settings.user_id",
		"user.id",
	)
	.leftJoinMany(
		"posts",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "user_id", "title"])),
		"posts.user_id",
		"user.id",
	);

type Author = {
	id: number;
	username: string;
	profile: { id: number; user_id: number; avatar_url: string | null } | null;
};

expectTypeOf(inserted.executeTakeFirstOrThrow()).resolves.toEqualTypeOf<{
	id: number;
	user_id: number;
	title: string;
	status: "draft" | "published";
	author: Author;
	tags: { id: number; name: string; post_id: number }[];
}>();

expectTypeOf(updated.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		username: string;
		display_name: string | null;
		settings: { id: number; user_id: number; theme: "light" | "dark" } | null;
		posts: { id: number; user_id: number; title: string }[];
	}[]
>();
