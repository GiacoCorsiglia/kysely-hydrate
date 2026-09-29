/**
 * The query sets the suites share, over the `lib/db.ts` schema.  Each table is a
 * reusable query set, so a join is one line and every suite builds the same SQL.
 * `QuerySet` is immutable, so deriving from these is safe.
 */
import * as k from "kysely";

import { querySet } from "../../src/query-set.ts";
import { type DB } from "./db.ts";

export function queries(db: k.Kysely<DB>) {
	const users = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);
	const posts = querySet(db).selectAs(
		"post",
		db.selectFrom("posts").select(["id", "title", "user_id"]),
	);
	const comments = querySet(db).selectAs(
		"comment",
		db.selectFrom("comments").select(["id", "content", "post_id"]),
	);

	/** users -> posts -> comments: 10,000 rows into 500 users over the seeded data. */
	const usersPostsComments = users.leftJoinMany(
		"posts",
		posts.leftJoinMany("comments", comments, "comments.post_id", "post.id"),
		"posts.user_id",
		"user.id",
	);

	return { users, posts, comments, usersPostsComments };
}
