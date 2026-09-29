/**
 * Building and compiling query sets; nothing executes.  `compile()` is
 * `toQuery().compile()`, so each compile benchmark covers building the Kysely
 * query and rendering its SQL.  The terminals group separates the two.
 */
import assert from "node:assert/strict";

import { summary } from "mitata";

import { querySet } from "../src/query-set.ts";
import { emptyDb } from "./lib/db.ts";
import { benchSync, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";

const db = emptyDb();
const { users, posts, comments, usersPostsComments } = queries(db);

interface Compilable {
	compile(): { sql: string };
}

/** What `verifyWorkloads` asserts about an entry's compiled SQL. */
interface Shape {
	joins?: number;
	has?: RegExp | RegExp[];
	lacks?: RegExp;
}

type Entry = [qs: Compilable, shape: Shape];

const compileEntries: (readonly [name: string, Entry])[] = [];

/** A `summary()` group timing `compile()` on each entry; the first is the baseline. */
function compileGroup(entries: Record<string, Entry>) {
	const named = Object.entries(entries).map(([name, entry]) => [`compile ${name}`, entry] as const);
	compileEntries.push(...named);
	summary(() => {
		named.forEach(([name, [qs]], i) => benchSync(name, () => qs.compile(), { baseline: i === 0 }));
	});
}

const joinCount = (sql: string) => (sql.match(/ join /g) ?? []).length;

/** The pagination landed inside a derived table aliased as the base: the wrapped shape. */
const wrapped = (alias: string) => new RegExp(`(limit|offset) \\?\\) as "${alias}"`);

/** The write sits in the data-modifying CTE at the top of the statement. */
const writeCte = (statement: string) => new RegExp(`^with "__base" as \\(${statement} `);

/** The join a method should render: `innerJoinLateralMany` -> `inner join lateral (`. */
const joinSql = (method: string) =>
	new RegExp(
		`${method.startsWith("left") ? "left" : "inner"} join ${method.includes("Lateral") ? "lateral " : ""}\\(`,
	);

const byMethod = (qsByMethod: Record<string, Compilable>) =>
	Object.fromEntries(
		Object.entries(qsByMethod).map(([method, qs]): [string, Entry] => [
			method,
			[qs, { joins: 1, has: joinSql(method) }],
		]),
	);

// Every join changes a query set's type, which TypeScript can't follow through a
// loop or recursion.  The fixtures built that way are only compiled, so they
// drop the types.
const untyped = (qs: unknown) => qs as any;

const siblings = (n: number) =>
	Array.from({ length: n }, (_, i) => `p${i + 1}`).reduce(
		(qs, key) => qs.leftJoinMany(key, posts, `${key}.user_id`, "user.id"),
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
	byMethod({
		leftJoinMany: manyJoin,
		leftJoinOne: oneJoin,
		innerJoinOne: users.innerJoinOne("posts", posts, "posts.user_id", "user.id"),
		innerJoinMany: users.innerJoinMany("posts", posts, "posts.user_id", "user.id"),
	}),
);

compileGroup(
	byMethod(
		Object.fromEntries(
			["leftJoinLateralMany", "innerJoinLateralMany", "leftJoinLateralOne"].map((m) => [
				m,
				lateral(m),
			]),
		),
	),
);

// Pagination over a many-join can't limit the exploded rows, so it wraps a
// paginated cardinality-one query in a derived table; over a one-join the limit
// lands on the joined query.  The unpaginated rows are the reference points.
compileGroup({
	"many join, no pagination": [manyJoin, { lacks: /limit|offset/ }],
	"many join, limit": [manyJoin.limit(50), { has: wrapped("user") }],
	"many join, offset": [manyJoin.offset(50), { has: wrapped("user") }],
	"one join, no pagination": [oneJoin, { lacks: /limit|offset/ }],
	"one join, limit": [oneJoin.limit(50), { has: /limit \?$/, lacks: wrapped("user") }],
	"one join, offset": [oneJoin.offset(50), { has: /offset \?$/, lacks: wrapped("user") }],
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
		{ has: /returning \*\)/ },
	],
	"insertAs with many join, limit": [
		withComments(insert).limit(10),
		{ has: [writeCte('insert into "posts"'), wrapped("post")] },
	],
	"insert on a select base": [
		manyJoin.insert(
			db.insertInto("users").values({ id: 1, username: "name", email: "e" }).returningAll(),
		),
		{ joins: 1, has: writeCte('insert into "users"') },
	],
});

compileGroup({
	"no joins": [users, { joins: 0 }],
	"no joins, where": [users.where("username", "=", "name"), { has: /where "username" = \?/ }],
	"no joins, CTE base": [
		querySet(db).selectAs(
			"user",
			db
				.with("recent", (d) => d.selectFrom("posts").select(["id", "user_id"]))
				.selectFrom("users")
				.select(["id", "username", "email"]),
		),
		{ has: /with "recent" as \(/ },
	],
	"many join, where": [
		manyJoin.where("username", "=", "name"),
		{ joins: 1, has: /where "username" = \?/ },
	],
	"many join, orderBy": [manyJoin.orderBy("username"), { has: /order by "user"."username" asc/ }],
});

// Terminals over one query set: depth 2, ordered, and limited, so the wrap is in play.
const representative = usersPostsComments.orderBy("id").limit(500);

summary(() => {
	// The one benchmark that constructs inside the measured call: it builds
	// `representative` from scratch, so each terminal reads as what it adds.
	benchSync("querySet build", () => queries(db).usersPostsComments.orderBy("id").limit(500), {
		baseline: true,
	});
	benchSync("querySet toQuery", () => representative.toQuery());
	benchSync("querySet compile", () => representative.compile());
	benchSync("querySet toOperationNode", () => representative.toOperationNode());
	// Returns the stored base query untouched: the floor, not a ratio to read.
	benchSync("querySet toBaseQuery", () => representative.toBaseQuery());
	benchSync("querySet toJoinedQuery", () => representative.toJoinedQuery());
	benchSync("querySet toCountQuery then compile", () => representative.toCountQuery().compile());
	benchSync("querySet toExistsQuery then compile", () => representative.toExistsQuery().compile());
});

function verifyWorkloads(): void {
	for (const [name, [qs, { joins, has = [], lacks }]] of compileEntries) {
		const { sql } = qs.compile();
		if (joins !== undefined) assert.equal(joinCount(sql), joins, name);
		for (const pattern of [has].flat()) assert.match(sql, pattern, name);
		if (lacks) assert.doesNotMatch(sql, lacks, name);
	}

	// The depth generator must build the same SQL as the shared users -> posts -> comments.
	assert.equal(depth(2).compile().sql, usersPostsComments.compile().sql);

	assert.match(representative.compile().sql, wrapped("user"));
	assert.match(representative.toQuery().compile().sql, /^select /);
	assert.match(representative.toCountQuery().compile().sql, /count\(\*\)/);
	assert.match(representative.toExistsQuery().compile().sql, /^select exists /);
	assert.equal(representative.toOperationNode().kind, "SelectQueryNode");
	assert.equal(joinCount(representative.toBaseQuery().compile().sql), 0);
	const joined = representative.toJoinedQuery().compile().sql;
	assert.equal(joinCount(joined), 2);
	assert.doesNotMatch(joined, /limit/);
}

await runSuite({ verify: verifyWorkloads });
