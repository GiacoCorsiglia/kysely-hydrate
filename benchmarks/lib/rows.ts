/**
 * Row builders for the suites that need flat join rows: one row per leaf, with
 * ancestor columns repeated (the "row explosion" hydration exists to undo).
 * Nothing is built at import.
 */
import { EnableAutoInclusion, type HydrateOptions } from "../../src/hydrator.ts";

/**
 * What `QuerySet#hydrate` passes (see `src/query-set.ts`), so the numbers
 * describe the path real callers take.  `EnableAutoInclusion` is internal; the
 * benchmarks reach for it for the same reason `QuerySet` does.
 */
export const querySetOptions: HydrateOptions = { [EnableAutoInclusion]: true, sort: "nested" };

export const withSort = (sort: NonNullable<HydrateOptions["sort"]>): HydrateOptions => ({
	...querySetOptions,
	sort,
});

/**
 * Auto-inclusion returns fields the hydrator never declared, which its static
 * type can't know about; assertions recover them through this, not casts.
 */
export const autoIncluded = <T>(value: unknown): T => value as T;

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

export type Row = Record<string, unknown>;

const userColumns = (u: number): Row => ({
	id: u,
	username: `user${u}`,
	email: `user${u}@example.com`,
});

/** A joined collection: `count` children per parent, each with its own joined collections. */
export interface Join {
	/** Plural table name; also the output key, and `name$$` is the column prefix. */
	name: string;
	count: number;
	/** The text column, e.g. `title`; its value reads `Post 3` for the third post. */
	text?: string;
	joins?: Join[];
}

/**
 * The rows a left join of `users` with `joins` returns.  Sibling joins
 * multiply: each entity contributes the cartesian product of its joins' rows.
 * Ids are numbered per table in row order, and each child carries a
 * `<parent>_id` column pointing at its parent.
 */
export function makeJoinRows(users: number, joins: Join[]): Row[] {
	const ids = new Map<string, number>();

	const expand = (columns: Row, prefix: string, parent: string, id: number, joins: Join[]) => {
		let rows = [columns];
		for (const { name, count, text = "label", joins: nested = [] } of joins) {
			const p = `${prefix}${name}$$`;
			const singular = name.slice(0, -1);
			const children = times(count, () => {
				const childId = (ids.get(p) ?? 0) + 1;
				ids.set(p, childId);
				const label = `${singular[0]!.toUpperCase()}${singular.slice(1)} ${childId}`;
				const child = { [`${p}id`]: childId, [`${p}${text}`]: label, [`${p}${parent}_id`]: id };
				return expand(child, p, singular, childId, nested);
			}).flat();
			rows = rows.flatMap((row) => children.map((child) => ({ ...row, ...child })));
		}
		return rows;
	};

	return times(users, (u) => expand(userColumns(u), "", "user", u, joins)).flat();
}

/** A chain of `fanOut.length` nested hasMany joins, `fanOut[d]` children per parent at depth d. */
export const chainJoins = (fanOut: readonly number[], names = CHAIN): Join[] =>
	fanOut.length === 0
		? []
		: [{ ...names[0]!, count: fanOut[0]!, joins: chainJoins(fanOut.slice(1), names.slice(1)) }];

const CHAIN = [
	{ name: "posts", text: "title" },
	{ name: "comments", text: "content" },
	{ name: "reactions" },
	{ name: "votes" },
];

/** `users * postsPerUser * commentsPerPost` rows of a users -> posts -> comments join. */
export const makeRows = (users: number, postsPerUser: number, commentsPerPost: number) =>
	makeJoinRows(users, chainJoins([postsPerUser, commentsPerPost])) as unknown as FlatRow[];

/**
 * `count` users whose joins all missed, as a left join leaves them: grouping
 * has nothing to collapse, the complement to {@link makeRows}.
 */
export const makeDistinctRows = (count: number): FlatRow[] =>
	times(count, (u) => ({
		...(userColumns(u) as Pick<FlatRow, "id" | "username" | "email">),
		posts$$id: null,
		posts$$title: null,
		posts$$user_id: null,
		posts$$comments$$id: null,
		posts$$comments$$content: null,
		posts$$comments$$post_id: null,
	}));
