/**
 * Benchmarks for query construction: `querySet()`, the join builders, and the
 * `to*Query()` / `compile()` terminals in `src/query-set.ts`.
 *
 *   npm run bench query-build                        # print a report
 *   npm run bench query-build -- --filter "sibling"  # only matching benchmarks
 *
 * Nothing here executes.  The database is `emptyDb()`: query building needs a
 * `Kysely` instance for its dialect and its types, never any rows, so seeding
 * one would only slow the suite's startup down.
 *
 * Read the numbers as relative comparisons within a `summary()` group.  Each
 * group varies exactly one thing — join count, nesting depth, join flavour,
 * pagination, the terminal, the write type — and holds the rest of the shape
 * fixed, so the ratio inside a group is the answer and the absolute microseconds
 * are just context.
 *
 * `QuerySet#compile()` is `toQuery().compile()`, so every "compile" benchmark
 * below covers building the Kysely query *and* rendering the SQL.  That is
 * deliberate: it is what a caller pays per query, and the terminals group
 * separates the two halves.
 */
import assert from "node:assert/strict";

import { summary } from "mitata";

import { querySet } from "../src/query-set.ts";
import { emptyDb } from "./lib/db.ts";
import { benchSync, runSuite } from "./lib/harness.ts";

const db = emptyDb();

////////////////////////////////////////////////////////////
// Workloads.
//
// Every query set is built once, here, because all but one benchmark measures a
// terminal against an already-built query set.  The single benchmark that
// measures construction ("querySet build") calls a builder function instead, and
// says so.
//
// The shapes are derived from each other wherever possible — `siblings3` is
// `siblings1` plus two joins, `representative` is `usersPostsComments` plus a
// limit — so a group can only differ in the way its comment claims.  `QuerySet`
// is immutable, so deriving is safe.
////////////////////////////////////////////////////////////

/** The base shared by every select query set below. */
const users = () =>
	querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username", "email"]));

//
// Sibling joins: 1, 3 and 5 many-joins hanging off the same base.
//
// All five join the same table on the same refs and select the same columns, so
// the only variable is how many of them there are.
//

const siblings1 = users().leftJoinMany(
	"p1",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"p1.user_id",
	"user.id",
);

const siblings3 = siblings1
	.leftJoinMany(
		"p2",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
		"p2.user_id",
		"user.id",
	)
	.leftJoinMany(
		"p3",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
		"p3.user_id",
		"user.id",
	);

const siblings5 = siblings3
	.leftJoinMany(
		"p4",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
		"p4.user_id",
		"user.id",
	)
	.leftJoinMany(
		"p5",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
		"p5.user_id",
		"user.id",
	);

//
// Nesting depth: 1 to 4 levels of many-join.
//
// Each level is a many-join inside the level above, so the SQL gains a derived
// table per level and the column prefixes grow with it ("posts$$comments$$...").
// The chain revisits `users` and `posts` at levels 3 and 4 because the benchmark
// schema only has three tables; the join kind is what the code path turns on, not
// which table it points at.  These cannot be derived from one another the way the
// siblings are: nesting happens inside the join callback.
//

const depth1 = users().leftJoinMany(
	"posts",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"posts.user_id",
	"user.id",
);

/** users -> posts -> comments, the shape the terminals group also uses. */
function buildUsersPostsComments() {
	return users().leftJoinMany(
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
	);
}

const depth2 = buildUsersPostsComments();

const depth3 = users().leftJoinMany(
	"posts",
	({ eb, qs }) =>
		qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
			"comments",
			({ eb, qs }) =>
				qs(eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"])).leftJoinMany(
					"authors",
					({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
					"authors.id",
					"comments.user_id",
				),
			"comments.post_id",
			"posts.id",
		),
	"posts.user_id",
	"user.id",
);

const depth4 = users().leftJoinMany(
	"posts",
	({ eb, qs }) =>
		qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
			"comments",
			({ eb, qs }) =>
				qs(eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"])).leftJoinMany(
					"authors",
					({ eb, qs }) =>
						qs(eb.selectFrom("users").select(["id", "username"])).leftJoinMany(
							"authorPosts",
							({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
							"authorPosts.user_id",
							"authors.id",
						),
					"authors.id",
					"comments.user_id",
				),
			"comments.post_id",
			"posts.id",
		),
	"posts.user_id",
	"user.id",
);

//
// Join flavours.  Same table, same refs, same selection: only the join method
// differs, so the gap is the code path and nothing else.  `depth1` is the
// `leftJoinMany` member of this set.
//

const leftOne = users().leftJoinOne(
	"post",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"post.user_id",
	"user.id",
);

const leftOneOrThrow = users().leftJoinOneOrThrow(
	"post",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"post.user_id",
	"user.id",
);

const innerOne = users().innerJoinOne(
	"post",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"post.user_id",
	"user.id",
);

const innerMany = users().innerJoinMany(
	"posts",
	({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
	"posts.user_id",
	"user.id",
);

//
// Lateral joins.  A lateral join's body is a correlated subquery — that is the
// point of it — so these carry a `whereRef` and their own `limit` that the plain
// joins above do not.  The group's baseline is `leftJoinLateralMany`, not a plain
// join, because comparing against one would attribute that extra subquery to
// "lateral".
//

const lateralMany = users().leftJoinLateralMany(
	"posts",
	({ eb, qs }) =>
		qs(
			eb
				.selectFrom("posts")
				.select(["id", "title"])
				.whereRef("posts.user_id", "=", "user.id")
				.orderBy("posts.id")
				.limit(2),
		),
	(join) => join.onTrue(),
);

const lateralInnerMany = users().innerJoinLateralMany(
	"posts",
	({ eb, qs }) =>
		qs(
			eb
				.selectFrom("posts")
				.select(["id", "title"])
				.whereRef("posts.user_id", "=", "user.id")
				.orderBy("posts.id")
				.limit(2),
		),
	(join) => join.onTrue(),
);

const lateralOne = users().leftJoinLateralOne(
	"latestPost",
	({ eb, qs }) =>
		qs(
			eb
				.selectFrom("posts")
				.select(["id", "title"])
				.whereRef("posts.user_id", "=", "user.id")
				.orderBy("posts.id", "desc")
				.limit(1),
		),
	(join) => join.onTrue(),
);

//
// Pagination.  A limit or offset over a many-join cannot be applied to the joined
// query — row explosion would make it count the wrong rows — so `#toQuery` builds
// a paginated cardinality-one query and wraps it in a derived table, re-hoisting
// every selection (see `#toCardinalityOneQuery`).  Over a one-join there is no
// explosion and the limit lands on the joined query directly.  Both sides of that
// branch are here, with their unpaginated shapes as the reference points.
//

const manyLimit = depth1.limit(50);
const manyOffset = depth1.offset(50);
const oneLimit = leftOne.limit(50);
const oneOffset = leftOne.offset(50);

//
// Terminals, all against one query set: depth 2, ordered, and limited, so the
// wrap above is in play.  This is the shape the hydrate suite's query-building
// group used, which those four benchmarks move here from.
//

const representative = depth2.orderBy("id").limit(500);

//
// Writes.  A write query set becomes a data-modifying CTE with a SELECT over it,
// and when the query wraps (a many-join plus pagination) that CTE and the
// RETURNING columns are hoisted to the top level — see commit b8de50e.  The
// insert pairs isolate: the RETURNING list, adding a join, and adding the wrap.
//

// The benchmark schema declares plain column types rather than `Generated`, so
// every column is required on insert, keys included.
const insertRow = { id: 1, user_id: 1, title: "Title", content: "Content" };

const insertAll = querySet(db).insertAs("post", (d) =>
	d.insertInto("posts").values(insertRow).returningAll(),
);

const insertColumns = querySet(db).insertAs("post", (d) =>
	d.insertInto("posts").values(insertRow).returning(["id", "user_id", "title"]),
);

const insertJoined = insertColumns.leftJoinMany(
	"comments",
	({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
	"comments.post_id",
	"post.id",
);

/** The wrapping case: the write CTE has to be hoisted past the derived table. */
const insertJoinedLimit = insertJoined.limit(10);

/** `insert()` rather than `insertAs()`: the base stays a SELECT and the write is attached to it. */
const insertOnSelect = users()
	.leftJoinMany(
		"posts",
		({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
		"posts.user_id",
		"user.id",
	)
	.insert(
		db
			.insertInto("users")
			.values({ id: 1, username: "name", email: "name@example.com" })
			.returningAll(),
	);

const updateAll = querySet(db).updateAs("post", (d) =>
	d.updateTable("posts").set({ title: "Title" }).where("id", "=", 1).returningAll(),
);

const updateJoined = querySet(db)
	.updateAs("post", (d) =>
		d
			.updateTable("posts")
			.set({ title: "Title" })
			.where("id", "=", 1)
			.returning(["id", "user_id", "title"]),
	)
	.leftJoinMany(
		"comments",
		({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
		"comments.post_id",
		"post.id",
	);

const deleteAll = querySet(db).deleteAs("post", (d) =>
	d.deleteFrom("posts").where("id", "=", 1).returningAll(),
);

const deleteJoined = querySet(db)
	.deleteAs("post", (d) =>
		d.deleteFrom("posts").where("id", "=", 1).returning(["id", "user_id", "title"]),
	)
	.leftJoinMany(
		"comments",
		({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
		"comments.post_id",
		"post.id",
	);

//
// Clauses over a join-free base, where the fixed per-query cost is all there is.
//

const plain = users();
const plainWhere = plain.where("username", "=", "name");
const plainCte = querySet(db).selectAs(
	"user",
	db
		.with("recent", (d) => d.selectFrom("posts").select(["id", "user_id"]))
		.selectFrom("users")
		.select(["id", "username", "email"]),
);

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/** How many joins of any flavour the compiled SQL contains. */
function joinCount(sql: string): number {
	return (sql.match(/ join /g) ?? []).length;
}

/**
 * Whether the pagination was wrapped: the LIMIT (or OFFSET) landed inside a
 * derived table aliased as the base rather than on the outer query, which is the
 * one thing that distinguishes the wrapped shape from the unwrapped one.
 */
function isWrapped(sql: string, alias: string): boolean {
	return new RegExp(`(limit|offset) \\?\\) as "${alias}"`).test(sql);
}

/**
 * Every query set runs its terminal once here and has its SQL asserted before
 * anything is timed.  A change that silently dropped a join, stopped wrapping a
 * paginated query, or turned a write into a bare select would otherwise read as a
 * large speedup rather than a failure.
 */
function verifyWorkloads(): void {
	// Join count: the whole point of the first group is that only the count
	// differs, so assert the count and nothing else about the shape.
	assert.equal(joinCount(siblings1.compile().sql), 1);
	assert.equal(joinCount(siblings3.compile().sql), 3);
	assert.equal(joinCount(siblings5.compile().sql), 5);

	// Depth: one join per level, and the deepest level's columns carry a prefix
	// per level above them.
	assert.equal(joinCount(depth1.compile().sql), 1);
	assert.equal(joinCount(depth2.compile().sql), 2);
	assert.equal(joinCount(depth3.compile().sql), 3);
	assert.equal(joinCount(depth4.compile().sql), 4);
	assert.match(depth4.compile().sql, /"posts\$\$comments\$\$authors\$\$authorPosts\$\$id"/);

	// Join flavours: each produces the join it names.
	assert.match(depth1.compile().sql, /left join \(/);
	assert.match(leftOne.compile().sql, /left join \(/);
	assert.match(leftOneOrThrow.compile().sql, /left join \(/);
	assert.match(innerOne.compile().sql, /inner join \(/);
	assert.match(innerMany.compile().sql, /inner join \(/);
	assert.match(lateralMany.compile().sql, /left join lateral \(/);
	assert.match(lateralInnerMany.compile().sql, /inner join lateral \(/);
	assert.match(lateralOne.compile().sql, /left join lateral \(/);

	// Pagination: a many-join wraps, a one-join does not.  Both halves matter —
	// the group is meaningless if the "wrapped" side stops wrapping, and equally
	// so if the unwrapped side starts.
	assert.equal(isWrapped(manyLimit.compile().sql, "user"), true);
	assert.equal(isWrapped(manyOffset.compile().sql, "user"), true);
	assert.equal(isWrapped(oneLimit.compile().sql, "post"), false);
	assert.equal(isWrapped(oneOffset.compile().sql, "post"), false);
	assert.equal(depth1.compile().sql.includes("limit"), false);
	assert.match(oneLimit.compile().sql, /limit \?$/);

	// The build benchmark constructs `representative` from scratch; if it drifted
	// from the query set the terminals run against, the two halves of the
	// build-versus-terminal comparison would stop describing the same query.
	assert.equal(
		buildUsersPostsComments().orderBy("id").limit(500).compile().sql,
		representative.compile().sql,
	);

	// Terminals: each returns its own kind of query rather than the plain select.
	assert.match(representative.toQuery().compile().sql, /^select /);
	assert.match(representative.toCountQuery().compile().sql, /count\(\*\)/);
	assert.match(representative.toExistsQuery().compile().sql, /^select exists /);
	assert.equal(representative.toOperationNode().kind, "SelectQueryNode");
	// The base query is the un-joined select the query set was created from; the
	// joined query is that plus the joins but without the pagination wrap.
	assert.equal(joinCount(representative.toBaseQuery().compile().sql), 0);
	assert.equal(joinCount(representative.toJoinedQuery().compile().sql), 2);
	assert.equal(representative.toJoinedQuery().compile().sql.includes("limit"), false);

	// Writes: each produces its own statement, inside the data-modifying CTE.
	assert.match(insertAll.compile().sql, /^with "__base" as \(insert into "posts" /);
	assert.match(insertColumns.compile().sql, /returning "id", "user_id", "title"\)/);
	assert.match(insertOnSelect.compile().sql, /^with "__base" as \(insert into "users" /);
	assert.match(updateAll.compile().sql, /^with "__base" as \(update "posts" /);
	assert.match(updateJoined.compile().sql, /^with "__base" as \(update "posts" /);
	assert.match(deleteAll.compile().sql, /^with "__base" as \(delete from "posts" /);
	assert.match(deleteJoined.compile().sql, /^with "__base" as \(delete from "posts" /);
	assert.equal(joinCount(insertJoined.compile().sql), 1);

	// The hoisting case: the wrap pushes the write CTE up to the top level while
	// the paginated select becomes the derived table.
	const hoisted = insertJoinedLimit.compile().sql;
	assert.match(hoisted, /^with "__base" as \(insert into "posts" /);
	assert.equal(isWrapped(hoisted, "post"), true);

	// Clauses.
	assert.equal(joinCount(plain.compile().sql), 0);
	assert.match(plainWhere.compile().sql, /where "username" = \?/);
	assert.match(plainCte.compile().sql, /with "recent" as \(/);
}

verifyWorkloads();

////////////////////////////////////////////////////////////
// Declaring benchmarks.
////////////////////////////////////////////////////////////

/**
 * Benchmark names double as `--filter` patterns, which are regexes, so a name
 * containing a metacharacter would either fail to compile or match something
 * else.  Restricting them to words, digits, spaces and commas keeps every name a
 * regex that matches itself, and the assertion keeps it that way.
 */
function benchQuery(name: string, fn: () => unknown) {
	assert.match(name, /^[A-Za-z0-9 ,]+$/, `benchmark name must be a self-matching regex: ${name}`);
	assert.equal(new RegExp(name).test(name), true);
	return benchSync(name, fn);
}

////////////////////////////////////////////////////////////
// How join count scales.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile 1 sibling join", () => siblings1.compile()).baseline(true);
	benchQuery("compile 3 sibling joins", () => siblings3.compile());
	benchQuery("compile 5 sibling joins", () => siblings5.compile());
});

////////////////////////////////////////////////////////////
// How nesting depth scales.
//
// Depth costs more than the same number of sibling joins would: each level is a
// derived table inside the one above, and every column below it gains another
// prefix, so both the node tree and the identifiers grow.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile depth 1", () => depth1.compile()).baseline(true);
	benchQuery("compile depth 2", () => depth2.compile());
	benchQuery("compile depth 3", () => depth3.compile());
	benchQuery("compile depth 4", () => depth4.compile());
});

////////////////////////////////////////////////////////////
// One join flavour against another.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile leftJoinMany", () => depth1.compile()).baseline(true);
	benchQuery("compile leftJoinOne", () => leftOne.compile());
	benchQuery("compile leftJoinOneOrThrow", () => leftOneOrThrow.compile());
	benchQuery("compile innerJoinOne", () => innerOne.compile());
	benchQuery("compile innerJoinMany", () => innerMany.compile());
});

////////////////////////////////////////////////////////////
// Lateral joins.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile leftJoinLateralMany", () => lateralMany.compile()).baseline(true);
	benchQuery("compile innerJoinLateralMany", () => lateralInnerMany.compile());
	benchQuery("compile leftJoinLateralOne", () => lateralOne.compile());
});

////////////////////////////////////////////////////////////
// What the pagination wrap costs.
//
// The two unpaginated rows are the reference points: the difference between each
// of them and its paginated partner is the wrap, and only the many-join side pays
// for it.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile many join, no pagination", () => depth1.compile()).baseline(true);
	benchQuery("compile many join, limit", () => manyLimit.compile());
	benchQuery("compile many join, offset", () => manyOffset.compile());
	benchQuery("compile one join, no pagination", () => leftOne.compile());
	benchQuery("compile one join, limit", () => oneLimit.compile());
	benchQuery("compile one join, offset", () => oneOffset.compile());
});

////////////////////////////////////////////////////////////
// Building the query set against the terminals that consume it.
//
// "querySet build" is the one benchmark here that constructs inside the measured
// call: it builds exactly `representative`, which every other row in this group
// starts from already built.  So the baseline is the chain of `leftJoinMany` /
// `orderBy` / `limit` calls, and each row above it is what a terminal adds on
// top.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("querySet build", () => buildUsersPostsComments().orderBy("id").limit(500)).baseline(
		true,
	);
	benchQuery("querySet toQuery", () => representative.toQuery());
	benchQuery("querySet compile", () => representative.compile());
	benchQuery("querySet toOperationNode", () => representative.toOperationNode());
	// `toBaseQuery` hands back the stored base query builder untouched, so it reads
	// as roughly zero and the summary's ratio against it is meaningless.  It stays
	// in the group as the floor: it is what a terminal costs when it does no work,
	// which is the other end of the range `compile` sits at.
	benchQuery("querySet toBaseQuery", () => representative.toBaseQuery());
	benchQuery("querySet toJoinedQuery", () => representative.toJoinedQuery());
	benchQuery("querySet toCountQuery then compile", () => representative.toCountQuery().compile());
	benchQuery("querySet toExistsQuery then compile", () => representative.toExistsQuery().compile());
});

////////////////////////////////////////////////////////////
// Write query sets.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile insertAs, returningAll", () => insertAll.compile()).baseline(true);
	benchQuery("compile insertAs, returning 3 columns", () => insertColumns.compile());
	benchQuery("compile insertAs with many join", () => insertJoined.compile());
	benchQuery("compile insertAs with many join, limit", () => insertJoinedLimit.compile());
	benchQuery("compile insert on a select base", () => insertOnSelect.compile());
	benchQuery("compile updateAs", () => updateAll.compile());
	benchQuery("compile updateAs with many join", () => updateJoined.compile());
	benchQuery("compile deleteAs", () => deleteAll.compile());
	benchQuery("compile deleteAs with many join", () => deleteJoined.compile());
});

////////////////////////////////////////////////////////////
// The floor: a query set with no joins at all.
////////////////////////////////////////////////////////////

summary(() => {
	benchQuery("compile no joins", () => plain.compile()).baseline(true);
	benchQuery("compile no joins, where", () => plainWhere.compile());
	benchQuery("compile no joins, CTE base", () => plainCte.compile());
});

////////////////////////////////////////////////////////////
// Run.
////////////////////////////////////////////////////////////

await runSuite("query-build");
