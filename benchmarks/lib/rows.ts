/**
 * Row builders, shared by the suites that need flat rows.
 *
 * These mirror the shape a `querySet` produces for a users -> posts -> comments
 * join: one flat row per leaf, with ancestor columns repeated (the "row
 * explosion" hydration exists to undo).
 *
 * Nothing here is built at import.  A suite constructs only the row sets it
 * benchmarks, so an `order-by` micro-benchmark doesn't pay for 10,000 rows it
 * never touches.
 */
import { EnableAutoInclusion, type HydrateOptions } from "../../src/hydrator.ts";

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

////////////////////////////////////////////////////////////
// Rows.
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

/** The runtime shape of a users -> posts -> comments hydrator, under auto-inclusion. */
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

/** Builds `[1, 2, ... n]` worth of `T`, since every fixture here is 1-based. */
export function times<T>(n: number, build: (i: number) => T): T[] {
	return Array.from({ length: n }, (_, i) => build(i + 1));
}

/**
 * The user columns, shared by both row builders so a change to `FlatRow` can't
 * leave them describing different users.
 */
export const userColumns = (u: number) => ({
	id: u,
	username: `user${u}`,
	email: `user${u}@example.com`,
});

/** The join columns as a left join leaves them when nothing matched. */
export const noJoinColumns = {
	posts$$id: null,
	posts$$title: null,
	posts$$user_id: null,
	posts$$comments$$id: null,
	posts$$comments$$content: null,
	posts$$comments$$post_id: null,
} as const satisfies Omit<FlatRow, "id" | "username" | "email">;

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
					...userColumns(u),
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
	return times(count, (u) => ({ ...userColumns(u), ...noJoinColumns }));
}
