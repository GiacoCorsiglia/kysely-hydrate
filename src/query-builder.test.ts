import { test } from "node:test";
import util from "node:util";

import SQLite from "better-sqlite3";
import * as k from "kysely";

import { hydrated } from "./query-builder.ts";
import { seed, type SeedDB } from "./seed.ts";

const sqlite = new SQLite(":memory:");
await seed(sqlite);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

const db = new k.Kysely<SeedDB>({
	dialect,
});

test("queryBuilder", async () => {
	const foo = db
		.selectFrom("users")
		.select(["users.id", "users.email"])
		.leftJoinLateral(
			(qb) =>
				qb
					.selectFrom("posts")
					.select(["posts.id", "posts.title"])
					.orderBy("posts.id", "desc")
					.limit(1)
					.as("latestPost"),
			(join) => join.onTrue(),
		)
		.select(["latestPost.id as latestPostId", "latestPost.title as latestPostTitle"])
		.leftJoin("posts", "posts.user_id", "users.id")
		.select(["posts.id as postId", "posts.title as postTitle"]);

	const _bar = foo.execute();

	const query = hydrated(db.selectFrom("users").select(["users.id", "users.email"]), "id")
		// .joinMany(
		// 	"posts",

		// 	({ leftJoin }) =>
		// 		leftJoin("posts", "posts.user_id", "users.id")
		// 			.select(["posts.id", "posts.title"])

		// 			.joinMany(
		// 				"comments",

		// 				({ leftJoin }) =>
		// 					leftJoin("comments", "comments.id", "posts.id").select([
		// 						"comments.id",
		// 					]),

		// 				"id",
		// 			),

		// 	"id",
		// )
		.joinOne(
			"latestPost",
			({ leftJoinLateral }) =>
				leftJoinLateral(
					(qb) =>
						qb
							.selectFrom("posts")
							.select(["posts.id", "posts.title"])
							.orderBy("posts.id", "desc")
							.limit(1)
							.as("latestPosts"),
					(join) => join.onTrue(),
				).select(["latestPosts.id", "latestPosts.title"]),
			"id",
		);

	const result = await query.execute();

	console.log(util.inspect(result, { depth: null, colors: true }));
});
