/**
 * Fixture builders.  Nothing is built at import, and nothing here imports the
 * library, so a suite that only needs `range` doesn't pay to load it.
 */

/** `[start, start + 1, ... start + n - 1]`. */
export const range = (n: number, start = 0) => Array.from({ length: n }, (_, i) => start + i);

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

export type Row = Record<string, unknown>;

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
 * The flat rows a left join of `users` with `joins` returns: one per leaf, with
 * ancestor columns repeated.  Sibling joins multiply: each entity contributes
 * the cartesian product of its joins' rows.  Ids are 1-based and numbered per
 * table in row order, and each child carries a `<parent>_id` column.
 */
export function makeJoinRows(users: number, joins: Join[]): Row[] {
	const ids = new Map<string, number>();

	const expand = (columns: Row, prefix: string, parent: string, id: number, joins: Join[]) => {
		let rows = [columns];
		for (const { name, count, text = "label", joins: nested = [] } of joins) {
			const p = `${prefix}${name}$$`;
			const singular = name.slice(0, -1);
			const children = range(count).flatMap(() => {
				const childId = (ids.get(p) ?? 0) + 1;
				ids.set(p, childId);
				const label = `${singular[0]!.toUpperCase()}${singular.slice(1)} ${childId}`;
				const child = { [`${p}id`]: childId, [`${p}${text}`]: label, [`${p}${parent}_id`]: id };
				return expand(child, p, singular, childId, nested);
			});
			rows = rows.flatMap((row) => children.map((child) => ({ ...row, ...child })));
		}
		return rows;
	};

	return range(users, 1).flatMap((u) =>
		expand({ id: u, username: `user${u}`, email: `user${u}@example.com` }, "", "user", u, joins),
	);
}

const CHAIN = [
	{ name: "posts", text: "title" },
	{ name: "comments", text: "content" },
	{ name: "reactions" },
	{ name: "votes" },
];

/** A chain of nested joins, `fanOut[d]` children per parent at depth d. */
export const chainJoins = (fanOut: readonly number[], names = CHAIN): Join[] =>
	fanOut.length === 0
		? []
		: [{ ...names[0]!, count: fanOut[0]!, joins: chainJoins(fanOut.slice(1), names.slice(1)) }];

/** `users * postsPerUser * commentsPerPost` rows of a users -> posts -> comments join. */
export const makeRows = (users: number, postsPerUser: number, commentsPerPost: number) =>
	makeJoinRows(users, chainJoins([postsPerUser, commentsPerPost])) as unknown as FlatRow[];

/** `count` users whose joins all missed, as a left join leaves them. */
export const makeDistinctRows = (count: number): FlatRow[] =>
	makeRows(count, 1, 1).map((row) => ({
		...row,
		...Object.fromEntries(
			Object.keys(row).flatMap((key) => (key.includes("$$") ? [[key, null]] : [])),
		),
	}));
