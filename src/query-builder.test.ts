import { test } from "node:test";
import util from "node:util";
import SQLite from "better-sqlite3";
import * as k from "kysely";
import { hydrated } from "./query-builder.ts";
import { type SeedDB, seed } from "./seed.ts";

const sqlite = new SQLite(":memory:");
await seed(sqlite);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

const db = new k.Kysely<SeedDB>({
	dialect,
});

test("queryBuilder", async () => {
	const query = hydrated(
		db.selectFrom("users").select(["users.id", "users.email"]),
		"id",
	).joinMany(
		"posts",

		({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])

				.joinMany(
					"comments",

					({ leftJoin }) =>
						leftJoin("comments", "comments.id", "posts.id").select([
							"comments.id",
						]),

					"id",
				),

		"id",
	);

	console.log(
		util.inspect(await query.execute(), { depth: null, colors: true }),
	);
});
