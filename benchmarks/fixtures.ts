/**
 * Shared workloads for the benchmark suite.
 *
 * The synthetic rows mirror the shape a `querySet` produces for a
 * users -> posts -> comments join: one flat row per leaf, with ancestor columns
 * repeated (the "row explosion" hydration exists to undo).  The SQLite fixture
 * below is built with matching cardinality so the end-to-end numbers can be
 * compared against the in-memory ones.
 */
import SQLite from "better-sqlite3";
import * as k from "kysely";

import { createHydrator, EnableAutoInclusion, type HydrateOptions } from "../src/hydrator.ts";
import { querySet } from "../src/query-set.ts";

////////////////////////////////////////////////////////////
// Hydration options.
////////////////////////////////////////////////////////////

/**
 * The options `QuerySet#hydrate` passes (see `src/query-set.ts`), which is how
 * essentially all hydration happens in practice.  Benchmarks that aren't
 * specifically measuring a different sort mode should use these, so the numbers
 * describe the path real callers take.
 *
 * `EnableAutoInclusion` is internal to the library; we reach for it here for the
 * same reason `QuerySet` does, namely that the benchmarks don't declare
 * `.fields()` and want every selected column included.
 */
export const querySetOptions: HydrateOptions = {
	[EnableAutoInclusion]: true,
	sort: "nested",
};

/** As {@link querySetOptions}, but with the sort mode overridden. */
export function withSort(sort: NonNullable<HydrateOptions["sort"]>): HydrateOptions {
	return { ...querySetOptions, sort };
}

/**
 * Auto-inclusion fills in fields the hydrator never declared, so a hydrator
 * built without `.fields()` has a static output type narrower than what it
 * actually returns.  `QuerySet` recovers the difference from the query's
 * selection; a standalone hydrator has nothing to recover it from.  Assertions
 * about hydrated rows go through this rather than scattering casts.
 */
export function autoIncluded<T>(value: unknown): T {
	return value as T;
}

/** The runtime shape of the nested hydrators below, under auto-inclusion. */
export interface HydratedUser {
	id: number;
	username: string;
	email: string;
	posts: {
		id: number;
		title: string;
		user_id: number;
		comments: { id: number; content: string; post_id: number }[];
	}[];
}

/** {@link HydratedUser} plus the attached collections. */
export interface HydratedUserWithAttaches extends HydratedUser {
	otherPosts: { id: number; user_id: number; title: string }[];
	profile: { id: number; user_id: number; bio: string } | null;
}

////////////////////////////////////////////////////////////
// Synthetic rows.
////////////////////////////////////////////////////////////

export interface FlatRow {
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

/**
 * Builds `users * postsPerUser * commentsPerPost` rows: the cartesian product a
 * two-level join returns.
 */
export function makeRows(users: number, postsPerUser: number, commentsPerPost: number): FlatRow[] {
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

/**
 * Builds `count` rows that each key to a distinct entity, i.e. what a query
 * without any many-joins returns.  Grouping has nothing to collapse here, which
 * makes this the complement to {@link makeRows}, where 20 rows collapse into one
 * entity.
 */
export function makeDistinctRows(count: number): FlatRow[] {
	const rows: FlatRow[] = [];
	for (let u = 1; u <= count; u++) {
		rows.push({
			id: u,
			username: `user${u}`,
			email: `user${u}@example.com`,
			posts$$id: null,
			posts$$title: null,
			posts$$user_id: null,
			posts$$comments$$id: null,
			posts$$comments$$content: null,
			posts$$comments$$post_id: null,
		});
	}
	return rows;
}

export const rows1 = makeRows(1, 5, 4); // 20 rows -> 1 entity
export const rows10 = makeRows(10, 5, 4); // 200 rows -> 10 entities
export const rows1k = makeRows(50, 5, 4); // 1,000 rows -> 50 entities
export const rows10k = makeRows(500, 5, 4); // 10,000 rows -> 500 entities
export const distinctRows10k = makeDistinctRows(10_000); // 10,000 rows -> 10,000 entities

////////////////////////////////////////////////////////////
// Hydrators.
////////////////////////////////////////////////////////////

/** No orderings, so hydration never sorts regardless of the sort mode. */
export const nested = createHydrator<FlatRow>("id").hasMany("posts", "posts$$", (h) =>
	h("id").hasMany("comments", "comments$$", (h) => h("id")),
);

export const nestedWithExtras = createHydrator<FlatRow>("id")
	.extras({ upper: (r) => r.username.toUpperCase() })
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.extras({ slug: (r) => String(r.title).toLowerCase() })
			.hasMany("comments", "comments$$", (h) => h("id")),
	);

/**
 * Orders by strings at every level, the expensive case: sorting is skipped
 * entirely unless a level declares an ordering, so this is the only hydrator
 * whose sort mode changes what runs.
 */
export const nestedSorted = createHydrator<FlatRow>("id")
	.orderBy("username", "desc")
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.orderBy("title", "desc")
			.hasMany("comments", "comments$$", (h) => h("id").orderBy("content", "desc")),
	);

/** Ordering by the numeric key columns only, the cheaper sorting case. */
export const nestedKeyed = nested.orderByKeys();

export const flat = createHydrator<FlatRow>("id");

////////////////////////////////////////////////////////////
// Attached collections.
////////////////////////////////////////////////////////////

interface AttachedPost {
	id: number;
	user_id: number;
	title: string;
}

interface AttachedProfile {
	id: number;
	user_id: number;
	bio: string;
}

function makeAttachedPosts(users: number, perUser: number): AttachedPost[] {
	const posts: AttachedPost[] = [];
	let id = 1;
	for (let u = 1; u <= users; u++) {
		for (let p = 0; p < perUser; p++, id++) {
			posts.push({ id, user_id: u, title: `Attached post ${id}` });
		}
	}
	return posts;
}

function makeAttachedProfiles(users: number): AttachedProfile[] {
	const profiles: AttachedProfile[] = [];
	for (let u = 1; u <= users; u++) {
		profiles.push({ id: u, user_id: u, bio: `Bio ${u}` });
	}
	return profiles;
}

const attachedPosts1 = makeAttachedPosts(1, 5);
const attachedProfiles1 = makeAttachedProfiles(1);
const attachedPosts500 = makeAttachedPosts(500, 5);
const attachedProfiles500 = makeAttachedProfiles(500);

/**
 * Attach fetching is the library's only async phase, and it groups the fetched
 * rows by match key up front.  The fetch functions here return a pre-built array
 * so the benchmark measures that grouping and stitching, not I/O.
 */
export const withAttaches1 = nested
	.attachMany("otherPosts", () => attachedPosts1, { matchChild: "user_id" })
	.attachOne("profile", () => attachedProfiles1, { matchChild: "user_id" })
	.orderByKeys();

export const withAttaches500 = nested
	.attachMany("otherPosts", () => attachedPosts500, { matchChild: "user_id" })
	.attachOne("profile", () => attachedProfiles500, { matchChild: "user_id" })
	.orderByKeys();

////////////////////////////////////////////////////////////
// SQLite fixture, for query building and end-to-end runs.
////////////////////////////////////////////////////////////

interface DB {
	users: { id: number; username: string; email: string };
	posts: { id: number; user_id: number; title: string; content: string };
	comments: { id: number; post_id: number; user_id: number; content: string };
}

function seedSqlite(): SQLite.Database {
	const sqlite = new SQLite(":memory:");
	sqlite.exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL);
		CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
		CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, content TEXT NOT NULL);
	`);

	const insUser = sqlite.prepare("INSERT INTO users VALUES (?, ?, ?)");
	const insPost = sqlite.prepare("INSERT INTO posts VALUES (?, ?, ?, ?)");
	const insComment = sqlite.prepare("INSERT INTO comments VALUES (?, ?, ?, ?)");
	let postId = 1;
	let commentId = 1;
	sqlite.transaction(() => {
		// Matches makeRows(500, 5, 4), so the join returns the same 10,000 rows as
		// `rows10k`.
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

	return sqlite;
}

export const sqlite = seedSqlite();

export const db = new k.Kysely<DB>({ dialect: new k.SqliteDialect({ database: sqlite }) });

export const buildUsersWithPosts = () =>
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

export const usersWithPosts = buildUsersWithPosts();
