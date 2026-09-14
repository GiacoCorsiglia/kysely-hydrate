/**
 * Micro-benchmarks for the runtime hot paths: hydration of flat rows and
 * query building.  Run with `npm run bench`.
 *
 * Numbers are printed as ops/sec and ms/op over a fixed wall-clock budget per
 * case; use them for relative comparisons on one machine, not as absolutes.
 */
import SQLite from "better-sqlite3";
import * as k from "kysely";

import { createHydrator, EnableAutoInclusion } from "../src/hydrator.ts";
import { querySet } from "../src/query-set.ts";

const only = process.argv[2];

function bench(name: string, fn: () => unknown, ms = 1500): void {
	if (only && !name.includes(only)) return;
	// Warm up.
	for (let i = 0; i < 20; i++) fn();
	const start = performance.now();
	let iterations = 0;
	while (performance.now() - start < ms) {
		fn();
		iterations++;
	}
	const elapsed = performance.now() - start;
	const perOp = elapsed / iterations;
	console.log(
		`${name.padEnd(48)} ${(1000 / perOp).toFixed(0).padStart(9)} ops/s ${perOp.toFixed(3).padStart(9)} ms/op`,
	);
}

async function benchAsync(name: string, fn: () => Promise<unknown>, ms = 1500): Promise<void> {
	if (only && !name.includes(only)) return;
	for (let i = 0; i < 20; i++) await fn();
	const start = performance.now();
	let iterations = 0;
	while (performance.now() - start < ms) {
		await fn();
		iterations++;
	}
	const elapsed = performance.now() - start;
	const perOp = elapsed / iterations;
	console.log(
		`${name.padEnd(48)} ${(1000 / perOp).toFixed(0).padStart(9)} ops/s ${perOp.toFixed(3).padStart(9)} ms/op`,
	);
}

//
// Synthetic flat rows: users x posts x comments (row explosion).
//

interface FlatRow {
	id: number;
	username: string;
	email: string;
	posts$$id: number | null;
	posts$$title: string | null;
	posts$$user_id: number | null;
	posts$$comments$$id: number | null;
	posts$$comments$$content: string | null;
	posts$$comments$$post_id: number | null;
}

function makeRows(users: number, postsPerUser: number, commentsPerPost: number): FlatRow[] {
	const rows: FlatRow[] = [];
	let postId = 1;
	let commentId = 1;
	for (let u = 1; u <= users; u++) {
		for (let p = 0; p < postsPerUser; p++, postId++) {
			for (let c = 0; c < commentsPerPost; c++, commentId++) {
				rows.push({
					id: u,
					username: `user${u}`,
					email: `user${u}@example.com`,
					posts$$id: postId,
					posts$$title: `Post ${postId}`,
					posts$$user_id: u,
					posts$$comments$$id: commentId,
					posts$$comments$$content: `Comment ${commentId}`,
					posts$$comments$$post_id: postId,
				});
			}
		}
	}
	return rows;
}

const rows1k = makeRows(100, 5, 2); // 1,000 rows -> 100 users
const rows10k = makeRows(500, 5, 4); // 10,000 rows -> 500 users

const nested = createHydrator<FlatRow>("id").hasMany("posts", "posts$$", (h) =>
	h("id").hasMany("comments", "comments$$", (h) => h("id")),
);

const nestedWithExtras = createHydrator<FlatRow>("id")
	.extras({ upper: (r) => r.username.toUpperCase() })
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.extras({ slug: (r) => String(r.title).toLowerCase() })
			.hasMany("comments", "comments$$", (h) => h("id")),
	);

const nestedSorted = createHydrator<FlatRow>("id")
	.orderBy("username", "desc")
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.orderBy("title", "desc")
			.hasMany("comments", "comments$$", (h) => h("id").orderBy("content", "desc")),
	);

const flat = createHydrator<FlatRow>("id");

const autoOpts = { [EnableAutoInclusion]: true };

await benchAsync("hydrate flat 10k rows (fields auto)", () => flat.hydrate(rows10k, autoOpts));
await benchAsync("hydrate nested 1k rows", () => nested.hydrate(rows1k, autoOpts));
await benchAsync("hydrate nested 10k rows", () => nested.hydrate(rows10k, autoOpts));
await benchAsync("hydrate nested+extras 10k rows", () =>
	nestedWithExtras.hydrate(rows10k, autoOpts),
);
await benchAsync("hydrate nested+sorted 10k rows", () => nestedSorted.hydrate(rows10k, autoOpts));
await benchAsync("hydrate nested 10k rows, sort none", () =>
	nested.hydrate(rows10k, { ...autoOpts, sort: "none" }),
);

//
// Query building and end-to-end via SQLite.
//

interface DB {
	users: { id: number; username: string; email: string };
	posts: { id: number; user_id: number; title: string; content: string };
	comments: { id: number; post_id: number; user_id: number; content: string };
}

const sqlite = new SQLite(":memory:");
sqlite.exec(`
	CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL);
	CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
	CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, content TEXT NOT NULL);
`);
{
	const insUser = sqlite.prepare("INSERT INTO users VALUES (?, ?, ?)");
	const insPost = sqlite.prepare("INSERT INTO posts VALUES (?, ?, ?, ?)");
	const insComment = sqlite.prepare("INSERT INTO comments VALUES (?, ?, ?, ?)");
	let postId = 1;
	let commentId = 1;
	sqlite.transaction(() => {
		for (let u = 1; u <= 500; u++) {
			insUser.run(u, `user${u}`, `user${u}@example.com`);
			for (let p = 0; p < 5; p++, postId++) {
				insPost.run(postId, u, `Post ${postId}`, "content");
				for (let c = 0; c < 4; c++, commentId++) {
					insComment.run(commentId, postId, u, `Comment ${commentId}`);
				}
			}
		}
	})();
}
const db = new k.Kysely<DB>({ dialect: new k.SqliteDialect({ database: sqlite }) });

const usersWithPosts = () =>
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.orderBy("id")
		.limit(500);

const built = usersWithPosts();

bench("querySet build (chain only)", () => usersWithPosts());
bench("querySet toQuery()", () => built.toQuery());
bench("querySet compile()", () => built.compile());
bench("querySet toCountQuery().compile()", () => built.toCountQuery().compile());
await benchAsync("querySet execute() 10k rows (sqlite)", () => built.execute());
await benchAsync("raw kysely execute 10k rows (sqlite, no hydrate)", () =>
	built.toQuery().execute(),
);
