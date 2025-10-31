import { test } from "node:test";

import SQLite from "better-sqlite3";

import * as k from "kysely";
import { orm } from "./index.ts";
import { seed, type SeedDB } from "./seed.ts";

const sqlite = new SQLite(":memory:");
await seed(sqlite);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

const db = new k.Kysely<SeedDB>({
	dialect,
});

test("orm", async () => {
	const example = orm(db)
		.query(
			(db) =>
				db
					.selectFrom("users")
					.select(["users.id", "username as foo", "users.id as bar"]),
			"id",
		)
		.withMany(
			"posts",
			(qb) =>
				qb.innerJoin("posts", "posts.user_id", "users.id").select(["posts.id"]),
			"id",
		)
		.withMany(
			"profiles",
			(qb) =>
				qb
					.leftJoin("profiles", "profiles.user_id", "users.id")
					.select(["profiles.id", "profiles.bio"]),
			"id",
		)
		.withMany(
			"comments",
			(qb) =>
				qb
					.innerJoin("comments", "comments.user_id", "users.id")
					.select(["comments.id", "comments.content as commentContent"]),
			"id",
		);

	const result = await example.execute();

	// console.log(result[0]);
});
