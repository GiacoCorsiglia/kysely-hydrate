/** Hydration: turning flat join rows into nested objects, the hottest runtime path. */
import assert from "node:assert/strict";

import {
	type CollectionMode,
	createHydrator,
	EnableAutoInclusion,
	type FullHydrator,
	type HydrateOptions,
	type MappedHydrator,
} from "../src/hydrator.ts";
import { assertDeepEqual, group, runSuite, type Workload } from "./lib/harness.ts";
import {
	chainJoins,
	type Join,
	makeDistinctRows,
	makeJoinRows,
	makeRows,
	range,
	shuffle,
} from "./lib/rows.ts";

/**
 * What `QuerySet#hydrate` passes (see `src/query-set.ts`), so the numbers
 * describe the path real callers take.  `EnableAutoInclusion` is internal; the
 * benchmarks reach for it for the same reason `QuerySet` does.
 */
const querySetOptions: HydrateOptions = { [EnableAutoInclusion]: true, sort: "nested" };

type Level = FullHydrator<any, any>;

/**
 * Hydrates `input` as `QuerySet` would.  Auto-inclusion returns fields the
 * hydrator's static type doesn't know about, so `check` sees `any`.
 */
const hydrating = (
	hydrator: MappedHydrator<any, unknown>,
	input: unknown,
	check: (out: any) => unknown,
	options = querySetOptions,
): Workload<Promise<any>> => ({ run: () => hydrator.hydrate(input as never, options), check });

/** Every entity at the end of `path`. */
const descend = (out: any[], ...path: string[]): any[] =>
	path.reduce((level, key) => level.flatMap((e) => e[key]), out);

/** Checks how many entities each level along `path` holds, top level first. */
const sizes =
	(expected: number[], ...path: string[]) =>
	(out: any[]) =>
		assert.deepEqual(
			[out, ...path].map((_, d) => descend(out, ...path.slice(0, d)).length),
			expected,
		);

/** The columns at `prefix`'s own level of `row`, unprefixed. */
const ownColumns = (row: object, prefix: string) =>
	Object.keys(row)
		.filter((k) => k.startsWith(prefix) && !k.slice(prefix.length).includes("$$"))
		.map((k) => k.slice(prefix.length));

/**
 * A hydrator for `joins`, nesting each as `mode`, with `modify` applied at
 * every level (given that level's columns, read from `sample`).
 */
interface Nesting {
	modify?: (h: Level, columns: string[], prefix: string) => Level;
	mode?: CollectionMode;
	sample?: object | undefined;
}

function nest(
	joins: readonly Join[],
	{ modify = (h) => h, mode = "many", sample = {} }: Nesting = {},
	prefix = "",
	h: Level = createHydrator<any>("id"),
): Level {
	return joins.reduce(
		(parent, { name, joins = [] }) =>
			parent.has(mode, name, `${name}$$`, (c: any) =>
				nest(joins, { modify, mode, sample }, `${prefix}${name}$$`, c("id")),
			),
		modify(h, ownColumns(sample, prefix), prefix),
	);
}

////////////////////////////////////////////////////////////
// Fixtures.
////////////////////////////////////////////////////////////

const chain = chainJoins([5, 4]);
const rows10k = makeRows(500, 5, 4);
/** One child per parent: any wider and `hasOne` throws instead of measuring. */
const oneToOneRows10k = makeRows(10_000, 1, 1);

const nested = nest(chain);
const flat = createHydrator<any>("id");
const every = (modify: (h: Level, columns: string[]) => Level) =>
	nest(chain, { modify, sample: rows10k[0] });
const key = (r: { id: number }) => r.id * 2;

const base = hydrating(nested, rows10k, sizes([500, 2500, 10_000], "posts", "comments"));
const equalTo = (other: Workload<Promise<any>>) => async (out: any) =>
	assertDeepEqual(out, await other.run());

const attached = (parents: number, perParent: number, parentKey: string) =>
	range(parents * perParent, 1).map((id) => ({
		id,
		[parentKey]: Math.ceil(id / perParent),
		label: `A${id}`,
	}));
const withAttaches = (users: number) => {
	const [posts, profiles] = [attached(users, 5, "user_id"), attached(users, 1, "user_id")];
	return nested
		.attachMany("otherPosts", () => posts, { matchChild: "user_id" })
		.attachOne("profile", () => profiles, { matchChild: "user_id" });
};
/** The last user has 5 attached posts and a profile of its own. */
const attachedTo = (users: number) => (out: any[]) => {
	assert.equal(out[users - 1].otherPosts.length, 5);
	assert.equal(out[users - 1].profile.user_id, users);
};

////////////////////////////////////////////////////////////
// Benchmarks.
////////////////////////////////////////////////////////////

// Every modifier applies at all three levels, so the checks read the deepest.
const extras = hydrating(
	every((h) => h.extras({ key })),
	rows10k,
	(out) => assert.equal(descend(out, "posts", "comments")[0].key, 2),
);
group({
	"10k rows to 500 entities": base,
	"10k rows, fields declared": hydrating(
		every((h, columns) => h.fields(columns)),
		rows10k,
		equalTo(base),
	),
	"10k rows, fields transformed": hydrating(
		every((h, columns) => h.fields(Object.fromEntries(columns.map((c) => [c, String])))),
		rows10k,
		(out) =>
			assert.deepEqual(descend(out, "posts", "comments")[0], {
				id: "1",
				content: "Comment 1",
				post_id: "1",
			}),
	),
	"10k rows, omit": hydrating(
		every((h) => h.omit(["id"])),
		rows10k,
		(out) =>
			assert.deepEqual(descend(out, "posts", "comments")[0], { content: "Comment 1", post_id: 1 }),
	),
	"10k rows, extras": extras,
	"10k rows, extend": hydrating(
		every((h) => h.extend((r) => ({ key: key(r) }))),
		rows10k,
		equalTo(extras),
	),
	"10k rows, with": hydrating(
		every((h) => h.with(createHydrator<any>("id").extras({ key }))),
		rows10k,
		equalTo(extras),
	),
	// `rows10k` is in key order at every level, which would make ordering it
	// TimSort's best case and grouping it unordered match `base` already.  Each
	// user's rows are shuffled instead (`sort: "nested"` leaves the top level to
	// SQL), so only ordering every nested level by key rebuilds `base`'s output.
	"10k rows, orderByKeys": hydrating(
		every((h) => h.orderByKeys()),
		range(500).flatMap((u) => shuffle(rows10k.slice(u * 20, u * 20 + 20), u)),
		equalTo(base),
	),
	"10k rows, mapped": hydrating(
		nested.map((u: any) => ({ ...u, label: `${u.username} has ${u.posts.length} posts` })),
		rows10k,
		(out) => assert.equal(out[0].label, "user1 has 5 posts"),
	),
});

const hasOne = hydrating(nest(chain, { mode: "one" }), oneToOneRows10k, (out) => {
	assert.equal(out.length, 10_000);
	assert.equal(out[9999].posts.comments.id, 10_000, "hasOne should nest an object, not an array");
});
group({
	"10k one-to-one rows, hasMany": hydrating(
		nested,
		oneToOneRows10k,
		sizes([10_000, 10_000, 10_000], "posts", "comments"),
	),
	"10k one-to-one rows, hasOne": hasOne,
	"10k one-to-one rows, hasOneOrThrow": hydrating(
		nest(chain, { mode: "oneOrThrow" }),
		oneToOneRows10k,
		equalTo(hasOne),
	),
	// Rows are repeated by reference, so the heap figure is the hydrator's alone.
	"10k rows, every join missed": hydrating(
		nested,
		makeDistinctRows(500).flatMap((row) => range(20).map(() => row)),
		sizes([500, 0], "posts"),
	),
});

// The sort modes only differ against a hydrator that orders, so show they do.
const ordered = every((h, columns) => h.orderBy(columns[1]!, "desc"));
const sorted = (
	sort: NonNullable<HydrateOptions["sort"]>,
	[username, title]: [username: string, title: string],
) =>
	hydrating(
		ordered,
		rows10k,
		(out) => assert.deepEqual([out[0].username, out[0].posts[0].title], [username, title]),
		{ ...querySetOptions, sort },
	);
group({
	"sorted 10k rows, sort:none": sorted("none", ["user1", "Post 1"]),
	"sorted 10k rows, sort:nested": sorted("nested", ["user1", "Post 5"]),
	"sorted 10k rows, sort:all": sorted("all", ["user99", "Post 495"]),
});

group({
	"10k distinct rows to 10k entities": hydrating(flat, makeDistinctRows(10_000), sizes([10_000])),
	"10k duplicated rows to 500 entities": hydrating(flat, rows10k, sizes([500])),
});

/**
 * Entity `n` is keyed by `uid: n` alone, or by `tenant_id` and `id`, each of
 * which repeats: grouping by either column alone merges entities.  The two keys
 * group identically, so arity is the only variable.
 */
const tenantKeys = (n: number, prefix = "") => ({
	[`${prefix}uid`]: n,
	[`${prefix}tenant_id`]: 1 + ((n - 1) % 4),
	[`${prefix}id`]: Math.ceil(n / 4),
});
const compositeRows = rows10k.map((row) => ({
	...tenantKeys(row.id),
	...tenantKeys(row.posts$$id!, "items$$"),
	items$$label: row.posts$$title,
}));
const keyed = (k: string | [string, string], check: (out: any) => unknown) =>
	hydrating(
		createHydrator<any>(k).hasMany("items", "items$$", (h: any) => h(k)),
		compositeRows,
		check,
	);
const singleKey = keyed("uid", sizes([500, 2500], "items"));
group({
	"10k rows, single column key": singleKey,
	"10k rows, two column key": keyed(["tenant_id", "id"], equalTo(singleKey)),
});

// Same fetched row count at both levels; nested attaches key 2,500 parents, not 500.
const likesPerUser = attached(500, 5, "user_id");
const likesPerPost = attached(2500, 1, "post_id");
group({
	"10k rows, no attaches": base,
	"10k rows, attachOne and attachMany": hydrating(withAttaches(500), rows10k, attachedTo(500)),
	"10k rows, attachMany on users": hydrating(
		nested.attachMany("likes", () => likesPerUser, { matchChild: "user_id" }),
		rows10k,
		sizes([500, 2500], "likes"),
	),
	"10k rows, attachMany on posts": hydrating(
		nest(chain, {
			modify: (h, _, prefix) =>
				prefix === "posts$$"
					? h.attachMany("likes", () => likesPerPost, { matchChild: "post_id" })
					: h,
			sample: rows10k[0],
		}),
		rows10k,
		sizes([500, 2500, 2500], "posts", "likes"),
	),
});

// Same 2,100 entities; the cartesian product of sibling joins is 5x the rows.
const siblingJoins: Join[] = [
	{ name: "posts", count: 10, text: "title" },
	{ name: "tags", count: 10 },
];
const singleJoin: Join[] = [{ name: "posts", count: 20, text: "title" }];
const siblingRows = makeJoinRows(100, siblingJoins);
assert.equal(siblingRows.length, 10_000, "sibling joins should multiply");
group({
	"100 users, 20 posts": hydrating(
		nest(singleJoin),
		makeJoinRows(100, singleJoin),
		sizes([100, 2000], "posts"),
	),
	"100 users, 10 posts x 10 tags": hydrating(nest(siblingJoins), siblingRows, (out) => {
		sizes([100, 1000], "posts")(out);
		sizes([100, 1000], "tags")(out);
	}),
	// Each user's rows shuffled, so both levels have 100 rows to put in order.
	"100 users, 10 posts x 10 tags, ordered": hydrating(
		nest(siblingJoins, { modify: (h, _, prefix) => (prefix ? h.orderBy("id", "desc") : h) }),
		range(100).flatMap((u) => shuffle(siblingRows.slice(u * 100, u * 100 + 100), u)),
		(out) => {
			sizes([100, 1000], "posts")(out);
			assert.deepEqual(
				[out[0].posts, out[0].tags].map((level) => level.map((e: any) => e.id)),
				[range(10, 1).toReversed(), range(10, 1).toReversed()],
			);
		},
	),
});

// 10k rows at every depth, so the per-level cost is what grows.
const chainNames = (joins: Join[]): string[] =>
	joins.flatMap((j) => [j.name, ...chainNames(j.joins ?? [])]);
group(
	Object.fromEntries(
		[[16], [4, 4], [4, 2, 2], [2, 2, 2, 2]].map((fanOut) => {
			const joins = chainJoins(fanOut);
			const counts = fanOut.reduce((acc, n) => [...acc, acc.at(-1)! * n], [625]);
			return [
				`10k rows, depth ${fanOut.length}`,
				hydrating(nest(joins), makeJoinRows(625, joins), sizes(counts, ...chainNames(joins))),
			];
		}),
	),
);

// Output objects built by keyed stores leave V8's fast mode between 16 and 20 columns.
group(
	Object.fromEntries(
		[9, 16, 20, 30, 60].map((columns) => [
			`2k rows of ${columns} columns`,
			hydrating(
				flat,
				range(2000, 1).map((id) => {
					const row: Record<string, unknown> = { id };
					for (let c = 1; c < columns; c++) row[`col${c}`] = `value ${id}-${c}`;
					return row;
				}),
				(out) => {
					assert.equal(out.length, 2000);
					assert.equal(Object.keys(out[0]).length, columns);
				},
			),
		]),
	),
);

const rows1 = makeRows(1, 5, 4);
group({
	"1 row to 1 entity, no collections": hydrating(flat, rows1[0], (out) => assert.equal(out.id, 1)),
	"20 rows to 1 entity": hydrating(nested, rows1, sizes([1, 5, 20], "posts", "comments")),
	"20 rows to 1 entity, 2 attaches": hydrating(withAttaches(1), rows1, attachedTo(1)),
	"200 rows to 10 entities": hydrating(
		nested,
		makeRows(10, 5, 4),
		sizes([10, 50, 200], "posts", "comments"),
	),
	"1k rows to 50 entities": hydrating(
		nested,
		makeRows(50, 5, 4),
		sizes([50, 250, 1000], "posts", "comments"),
	),
});

await runSuite();
