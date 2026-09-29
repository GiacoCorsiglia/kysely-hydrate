/**
 * Building and compiling query sets; nothing executes.  `compile()` is
 * `toQuery().compile()`, so each compile benchmark covers building the Kysely
 * query and rendering its SQL.  The terminals group separates the two.
 */
import assert from "node:assert/strict";

import { querySet } from "../src/query-set.ts";
import { emptyDb } from "./lib/db.ts";
import { assertDeepEqual, group, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";
import { range } from "./lib/rows.ts";

const db = emptyDb();
const { users, posts, comments, usersPostsComments } = queries(db);

interface Compilable {
	compile(): { sql: string };
}

/** What an entry's compiled SQL must look like.  Every entry counts its joins, so none can drop one. */
interface Shape {
	joins: number;
	has?: RegExp | RegExp[];
	lacks?: RegExp;
}

type Entry = [qs: Compilable, shape: Shape];

const joinCount = (sql: string) => (sql.match(/ join /g) ?? []).length;

const assertShape = (sql: string, { joins, has = [], lacks }: Shape) => {
	assert.equal(joinCount(sql), joins);
	for (const pattern of [has].flat()) assert.match(sql, pattern);
	if (lacks) assert.doesNotMatch(sql, lacks);
};

/** A group timing `compile()` on each entry. */
const compileGroup = (entries: Record<string, Entry>) =>
	group(
		Object.fromEntries(
			Object.entries(entries).map(([name, [qs, shape]]) => [
				name,
				{ run: () => qs.compile(), check: ({ sql }: { sql: string }) => assertShape(sql, shape) },
			]),
		),
		"compile ",
	);

/** The pagination landed inside a derived table aliased as the base: the wrapped shape. */
const wrapped = (alias: string) => new RegExp(`(limit|offset) \\?\\) as "${alias}"`);

/** The write sits in the data-modifying CTE at the top of the statement. */
const writeCte = (statement: string) => new RegExp(`^with "__base" as \\(${statement} `);

/** The join a method should render: `innerJoinLateralMany` -> `inner join lateral (`. */
const joinSql = (method: string) =>
	new RegExp(
		`${method.startsWith("left") ? "left" : "inner"} join ${method.includes("Lateral") ? "lateral " : ""}\\(`,
	);

/** One entry per join method, each built by `join`. */
const byMethod = (methods: string[], join: (method: string) => Compilable) =>
	Object.fromEntries(
		methods.map((method): [string, Entry] => [
			method,
			[join(method), { joins: 1, has: joinSql(method) }],
		]),
	);

// Every join changes a query set's type, which TypeScript can't follow through a
// loop or recursion.  The fixtures built that way are only compiled, so they
// drop the types.
const untyped = (qs: unknown) => qs as any;

const siblings = (n: number) =>
	range(n, 1).reduce(
		(qs, i) => qs.leftJoinMany(`p${i}`, posts, `p${i}.user_id`, "user.id"),
		untyped(users),
	);

/** Each level many-joins the next.  With three tables, levels 3 and 4 revisit posts and users. */
const chain = [
	["posts", posts, "posts.user_id", "user.id"],
	["comments", comments, "comments.post_id", "post.id"],
	["post", posts, "post.id", "comment.post_id"],
	["author", users, "author.id", "post.user_id"],
] as const;

const depth = (n: number, parent: unknown = users, level = 0): Compilable => {
	if (level === n) return untyped(parent);
	const [key, qs, ...refs] = chain[level]!;
	return untyped(parent).leftJoinMany(key, depth(n, qs, level + 1), ...refs);
};

const manyJoin = users.leftJoinMany("posts", posts, "posts.user_id", "user.id");
const oneJoin = users.leftJoinOne("posts", posts, "posts.user_id", "user.id");

// A lateral body is a correlated subquery with its own limit, so the lateral
// group has its own baseline rather than comparing against a plain join.
const lateral = (method: string) =>
	untyped(users)[method](
		"posts",
		({ eb, qs }: any) =>
			qs(
				eb
					.selectFrom("posts")
					.select(["id", "title"])
					.whereRef("posts.user_id", "=", "user.id")
					.orderBy("posts.id", "desc")
					.limit(method.endsWith("One") ? 1 : 2),
			),
		(join: any) => join.onTrue(),
	);

// The schema's columns aren't `Generated`, so an insert supplies every one.
const insertRow = { id: 1, user_id: 1, title: "Title", content: "Content" };
const returned = ["id", "user_id", "title"] as const;
const insert = querySet(db).insertAs("post", (d) =>
	d.insertInto("posts").values(insertRow).returning(returned),
);
const writes = {
	insertAs: [insert, 'insert into "posts"'],
	updateAs: [
		querySet(db).updateAs("post", (d) =>
			d.updateTable("posts").set({ title: "Title" }).where("id", "=", 1).returning(returned),
		),
		'update "posts"',
	],
	deleteAs: [
		querySet(db).deleteAs("post", (d) =>
			d.deleteFrom("posts").where("id", "=", 1).returning(returned),
		),
		'delete from "posts"',
	],
} as const;
const withComments = (qs: unknown) =>
	untyped(qs).leftJoinMany("comments", comments, "comments.post_id", "post.id");

compileGroup(
	Object.fromEntries(
		[1, 3, 5].map((n): [string, Entry] => [`sibling joins: ${n}`, [siblings(n), { joins: n }]]),
	),
);

compileGroup(
	Object.fromEntries(
		[1, 2, 3, 4].map((n): [string, Entry] => {
			// The deepest level's columns carry a prefix per level above them.
			const prefix = chain.slice(0, n).map(([key]) => key);
			return [
				`depth: ${n}`,
				[depth(n), { joins: n, has: new RegExp(`"${prefix.join("\\$\\$")}\\$\\$id"`) }],
			];
		}),
	),
);

compileGroup(
	byMethod(["leftJoinMany", "leftJoinOne", "innerJoinOne", "innerJoinMany"], (method) =>
		untyped(users)[method]("posts", posts, "posts.user_id", "user.id"),
	),
);
compileGroup(
	byMethod(["leftJoinLateralMany", "innerJoinLateralMany", "leftJoinLateralOne"], lateral),
);

// Pagination over a many-join can't limit the exploded rows, so it wraps a
// paginated cardinality-one query in a derived table; over a one-join the limit
// lands on the joined query.  The unpaginated rows are the reference points.
compileGroup({
	"many join, no pagination": [manyJoin, { joins: 1, lacks: /limit|offset/ }],
	"many join, limit": [manyJoin.limit(50), { joins: 1, has: wrapped("user") }],
	"many join, offset": [manyJoin.offset(50), { joins: 1, has: wrapped("user") }],
	"one join, no pagination": [oneJoin, { joins: 1, lacks: /limit|offset/ }],
	"one join, limit": [oneJoin.limit(50), { joins: 1, has: /limit \?$/, lacks: wrapped("user") }],
	"one join, offset": [oneJoin.offset(50), { joins: 1, has: /offset \?$/, lacks: wrapped("user") }],
});

// With a many-join and pagination, the write CTE is hoisted above the derived table.
compileGroup({
	...Object.fromEntries(
		Object.entries(writes).flatMap(([name, [qs, statement]]): [string, Entry][] => [
			[name, [qs, { joins: 0, has: writeCte(statement) }]],
			[`${name} with many join`, [withComments(qs), { joins: 1, has: writeCte(statement) }]],
		]),
	),
	"insertAs, returningAll": [
		querySet(db).insertAs("post", (d) => d.insertInto("posts").values(insertRow).returningAll()),
		{ joins: 0, has: /returning \*\)/ },
	],
	"insertAs with many join, limit": [
		withComments(insert).limit(10),
		{ joins: 1, has: [writeCte('insert into "posts"'), wrapped("post")] },
	],
	"insert on a select base": [
		manyJoin.insert(
			db.insertInto("users").values({ id: 1, username: "name", email: "e" }).returningAll(),
		),
		{ joins: 1, has: writeCte('insert into "users"') },
	],
});

compileGroup({
	"no joins": [users, { joins: 0, has: /from "users"/ }],
	"no joins, where": [
		users.where("username", "=", "name"),
		{ joins: 0, has: /where "username" = \?/ },
	],
	"no joins, CTE base": [
		querySet(db).selectAs(
			"user",
			db
				.with("recent", (d) => d.selectFrom("posts").select(["id", "user_id"]))
				.selectFrom("users")
				.select(["id", "username", "email"]),
		),
		{ joins: 0, has: /with "recent" as \(/ },
	],
	"many join, where": [
		manyJoin.where("username", "=", "name"),
		{ joins: 1, has: /where "username" = \?/ },
	],
	"many join, orderBy": [
		manyJoin.orderBy("username"),
		{ joins: 1, has: /order by "user"."username" asc/ },
	],
});

// Terminals over one query set: depth 2, ordered, and limited, so the wrap is in play.
const representative = usersPostsComments.orderBy("id").limit(500);
const compiled = (check: (sql: string) => unknown) => (q: Compilable) => check(q.compile().sql);
/** The representative's full query: both joins, around the wrapped pagination. */
const full: Shape = { joins: 2, has: wrapped("user") };

group({
	// Builds `representative` from scratch inside the measured call; the
	// terminals run on it prebuilt, so none of them includes this cost.
	"querySet build": {
		run: () => queries(db).usersPostsComments.orderBy("id").limit(500),
		check: compiled((sql) => assert.equal(sql, representative.compile().sql)),
	},
	"querySet toQuery": {
		run: () => representative.toQuery(),
		check: compiled((sql) => assertShape(sql, full)),
	},
	"querySet compile": {
		run: () => representative.compile(),
		check: ({ sql }) => assertShape(sql, full),
	},
	"querySet toOperationNode": {
		run: () => representative.toOperationNode(),
		check: (node) => assertDeepEqual(node, representative.toQuery().toOperationNode()),
	},
	// Returns the stored base query untouched: the floor, not a ratio to read.
	"querySet toBaseQuery": {
		run: () => representative.toBaseQuery(),
		check: compiled((sql) => assert.equal(joinCount(sql), 0)),
	},
	"querySet toJoinedQuery": {
		run: () => representative.toJoinedQuery(),
		check: compiled((sql) => assertShape(sql, { joins: 2, lacks: /limit/ })),
	},
	"querySet toCountQuery then compile": {
		run: () => representative.toCountQuery().compile(),
		check: ({ sql }) => assert.match(sql, /count\(\*\)/),
	},
	"querySet toExistsQuery then compile": {
		run: () => representative.toExistsQuery().compile(),
		check: ({ sql }) => assert.match(sql, /^select exists /),
	},
});

// The depth generator must build the same SQL as the shared users -> posts -> comments.
await runSuite({
	verify: () => assert.equal(depth(2).compile().sql, usersPostsComments.compile().sql),
});
