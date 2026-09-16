/**
 * Database handles for the suites that build or execute real queries.
 *
 * `emptyDb()` is for query building, which never runs anything, so it costs
 * nothing to create.  `seededSqlite()` pays to insert 10,000 rows and is only
 * called by the suite that executes queries.
 */
import SQLite from "better-sqlite3";
import * as k from "kysely";

import { makeRows } from "./rows.ts";

export interface DB {
	users: { id: number; username: string; email: string };
	posts: { id: number; user_id: number; title: string; content: string };
	comments: { id: number; post_id: number; user_id: number; content: string };
}

const schema = `
	CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL);
	CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
	CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, content TEXT NOT NULL);
`;

function kysely(sqlite: SQLite.Database, plugins: k.KyselyPlugin[] = []): k.Kysely<DB> {
	return new k.Kysely<DB>({ dialect: new k.SqliteDialect({ database: sqlite }), plugins });
}

/**
 * An empty database with the schema in place.  Enough to build and compile
 * queries against, which is all the query-building suite needs.
 */
export function emptyDb(plugins: k.KyselyPlugin[] = []): k.Kysely<DB> {
	const sqlite = new SQLite(":memory:");
	sqlite.exec(schema);
	return kysely(sqlite, plugins);
}

/** The rows the seeded database holds; exported so hydration can be compared against it. */
export const seedRows = () => makeRows(500, 5, 4);

/**
 * A database holding exactly the rows `makeRows(500, 5, 4)` describes, so the
 * users -> posts -> comments join returns those same 10,000 rows.  Seeded from
 * the row builder itself rather than by replaying its loop, so the two can't
 * drift apart while still claiming to describe the same work.  The rows repeat
 * each ancestor once per leaf, hence OR IGNORE.
 */
export function seededSqlite(plugins: k.KyselyPlugin[] = []): k.Kysely<DB> {
	const sqlite = new SQLite(":memory:");
	sqlite.exec(schema);

	const insUser = sqlite.prepare("INSERT OR IGNORE INTO users VALUES (?, ?, ?)");
	const insPost = sqlite.prepare("INSERT OR IGNORE INTO posts VALUES (?, ?, ?, ?)");
	const insComment = sqlite.prepare("INSERT OR IGNORE INTO comments VALUES (?, ?, ?, ?)");

	sqlite.transaction(() => {
		for (const row of seedRows()) {
			insUser.run(row.id, row.username, row.email);
			insPost.run(row.posts$$id, row.posts$$user_id, row.posts$$title, "content");
			insComment.run(
				row.posts$$comments$$id,
				row.posts$$comments$$post_id,
				row.id,
				row.posts$$comments$$content,
			);
		}
	})();

	return kysely(sqlite, plugins);
}
