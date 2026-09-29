/** Hydration: turning flat join rows into nested objects, the hottest runtime path. */
import assert from "node:assert/strict";

import { summary } from "mitata";

import {
	type CollectionMode,
	createHydrator,
	type FullHydrator,
	type HydrateOptions,
	type MappedHydrator,
} from "../src/hydrator.ts";
import { benchAsync, runSuite } from "./lib/harness.ts";
import {
	autoIncluded,
	chainJoins,
	type HydratedUser,
	type Join,
	makeDistinctRows,
	makeJoinRows,
	makeRows,
	querySetOptions,
	type Row,
	times,
	withSort,
} from "./lib/rows.ts";

type Level = FullHydrator<any, any>;
type Workload = readonly [MappedHydrator<any, unknown>, input: unknown, options?: HydrateOptions];

const hydrate = ([hydrator, input, options = querySetOptions]: Workload) =>
	hydrator.hydrate(input as never, options);
const hydrated = async <T = any[]>(w: Workload) => autoIncluded<T>(await hydrate(w));

/** A `summary()` group, the first entry its baseline. */
function group(workloads: Record<string, Workload>) {
	summary(() => {
		Object.entries(workloads).forEach(([name, w], i) =>
			benchAsync(name, () => hydrate(w), { baseline: i === 0 }),
		);
	});
}

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

/** Flattens `entities` along `path`, so a dropped level shows as a short count. */
const descend = (entities: any[], path: string[]): any[] =>
	path.reduce((level, key) => level.flatMap((e) => e[key]), entities);

////////////////////////////////////////////////////////////
// Fixtures.
////////////////////////////////////////////////////////////

const chain = chainJoins([5, 4]);
const rows1 = makeRows(1, 5, 4);
const rows10 = makeRows(10, 5, 4);
const rows1k = makeRows(50, 5, 4);
const rows10k = makeRows(500, 5, 4);
const distinctRows10k = makeDistinctRows(10_000);
/** Rows are repeated by reference, so the heap figure is the hydrator's alone. */
const nullJoinRows10k = makeDistinctRows(500).flatMap((row) => times(20, () => row));
/** One child per parent: any wider and `hasOne` throws instead of measuring. */
const oneToOneRows10k = makeRows(10_000, 1, 1);

const nested = nest(chain);
const flat = createHydrator<any>("id");
const every = (modify: (h: Level, columns: string[]) => Level) =>
	nest(chain, { modify, sample: rows10k[0] });
const key = (r: { id: number }) => r.id * 2;

const attached = (parents: number, perParent: number, parentKey: string) =>
	times(parents * perParent, (id) => ({
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
const likesPerUser = attached(500, 5, "user_id");
const likesPerPost = attached(2500, 1, "post_id");

/** Output objects built by keyed stores leave V8's fast mode between 16 and 20 columns. */
const WIDTHS = [9, 16, 20, 30, 60];
const wideRows = (columns: number) =>
	times(2000, (id) => {
		const row: Row = { id };
		for (let c = 1; c < columns; c++) row[`col${c}`] = `value ${id}-${c}`;
		return row;
	});

const DEPTH_FAN_OUT = [[16], [4, 4], [4, 2, 2], [2, 2, 2, 2]];
const depthJoins = DEPTH_FAN_OUT.map((fanOut) => chainJoins(fanOut));
const chainNames = (joins: Join[]): string[] =>
	joins.length ? [joins[0]!.name, ...chainNames(joins[0]!.joins ?? [])] : [];

const siblingJoins: Join[] = [
	{ name: "posts", count: 10, text: "title" },
	{ name: "tags", count: 10 },
];
const singleJoin: Join[] = [{ name: "posts", count: 20, text: "title" }];

/** `tenant_id` derives from `id`, so both keys group identically: arity is the only variable. */
const compositeRows = rows10k.map((row, i) => ({
	tenant_id: 1 + (row.id % 4),
	id: row.id,
	items$$tenant_id: 1 + (row.id % 4),
	items$$id: row.posts$$id ?? i,
	items$$label: row.posts$$title,
}));
const keyed = (k: string | [string, string]) =>
	createHydrator<any>(k).hasMany("items", "items$$", (h: any) => h(k));

////////////////////////////////////////////////////////////
// Workloads.
////////////////////////////////////////////////////////////

const W = {
	base: [nested, rows10k],
	fieldsDeclared: [every((h, columns) => h.fields(columns)), rows10k],
	fieldsTransformed: [
		every((h, columns) => h.fields(Object.fromEntries(columns.map((c) => [c, String])))),
		rows10k,
	],
	omit: [every((h) => h.omit(["id"])), rows10k],
	extras: [every((h) => h.extras({ key })), rows10k],
	extend: [every((h) => h.extend((r) => ({ key: key(r) }))), rows10k],
	with: [every((h) => h.with(createHydrator<any>("id").extras({ key }))), rows10k],
	orderByKeys: [every((h) => h.orderByKeys()), rows10k],
	mapped: [
		nested.map((u: any) => ({ ...u, label: `${u.username} has ${u.posts.length} posts` })),
		rows10k,
	],

	oneToOneMany: [nested, oneToOneRows10k],
	oneToOneOne: [nest(chain, { mode: "one" }), oneToOneRows10k],
	oneToOneOrThrow: [nest(chain, { mode: "oneOrThrow" }), oneToOneRows10k],
	nullJoin: [nested, nullJoinRows10k],

	sortNone: [every((h, columns) => h.orderBy(columns[1]!, "desc")), rows10k, withSort("none")],

	distinct: [flat, distinctRows10k],
	duplicated: [flat, rows10k],

	singleKey: [keyed("id"), compositeRows],
	compositeKey: [keyed(["tenant_id", "id"]), compositeRows],

	attaches: [withAttaches(500), rows10k],
	attachUsers: [nested.attachMany("likes", () => likesPerUser, { matchChild: "user_id" }), rows10k],
	attachPosts: [
		nest(chain, {
			modify: (h, _, prefix) =>
				prefix === "posts$$"
					? h.attachMany("likes", () => likesPerPost, { matchChild: "post_id" })
					: h,
			sample: rows10k[0],
		}),
		rows10k,
	],

	siblings: [nest(siblingJoins), makeJoinRows(100, siblingJoins)],
	single: [nest(singleJoin), makeJoinRows(100, singleJoin)],

	one: [flat, rows1[0]],
	rows20: [nested, rows1],
	rows20Attaches: [withAttaches(1), rows1],
	rows200: [nested, rows10],
	rows1k: [nested, rows1k],
} satisfies Record<string, Workload>;

const sorted = (sort: "nested" | "all"): Workload => [W.sortNone[0], rows10k, withSort(sort)];
const widths = WIDTHS.map((columns): Workload => [flat, wideRows(columns)]);
const depths = depthJoins.map((joins): Workload => [nest(joins), makeJoinRows(625, joins)]);

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

async function verifyWorkloads(): Promise<void> {
	const users = await hydrated<HydratedUser[]>(W.base);
	assert.equal(users.length, 500);
	assert.equal(descend(users, ["posts"]).length, 2500);
	assert.equal(descend(users, ["posts", "comments"]).length, 10_000);
	for (const [w, n] of [
		[W.rows20, 1],
		[W.rows200, 10],
		[W.rows1k, 50],
	] as const) {
		assert.equal((await hydrated(w)).length, n);
	}
	assert.equal((await hydrated<Row>(W.one)).id, 1);

	// Every modifier applies at all three levels, so check the deepest.
	const comment = async (w: Workload) => descend(await hydrated(w), ["posts", "comments"])[0];
	assert.deepEqual(await hydrated(W.fieldsDeclared), users);
	assert.deepEqual(await comment(W.fieldsTransformed), {
		id: "1",
		content: "Comment 1",
		post_id: "1",
	});
	assert.deepEqual(await comment(W.omit), { content: "Comment 1", post_id: 1 });
	const extras = await hydrated(W.extras);
	assert.equal(descend(extras, ["posts", "comments"])[0].key, 2);
	assert.deepEqual(await hydrated(W.extend), extras);
	assert.deepEqual(await hydrated(W.with), extras);
	assert.equal(descend(await hydrated(W.orderByKeys), ["posts", "comments"]).length, 10_000);
	assert.equal((await hydrated(W.mapped))[0].label, "user1 has 5 posts");

	const [many, one, orThrow] = await Promise.all([
		hydrated(W.oneToOneMany),
		hydrated(W.oneToOneOne),
		hydrated(W.oneToOneOrThrow),
	]);
	assert.equal(many.length, 10_000);
	assert.equal(one[9999].posts.comments.id, many[9999].posts[0].comments[0].id);
	assert.equal(Array.isArray(one[9999].posts), false, "hasOne should not produce an array");
	assert.deepEqual(orThrow, one);
	const nullJoined = await hydrated<HydratedUser[]>(W.nullJoin);
	assert.equal(nullJoined.length, 500);
	assert.equal(nullJoined[0]!.posts.length, 0, "a missed join should produce no children");

	// The sort modes only differ against a hydrator that orders, so show they do.
	const [none, byNested, all] = await Promise.all([
		hydrated<HydratedUser[]>(W.sortNone),
		hydrated<HydratedUser[]>(sorted("nested")),
		hydrated<HydratedUser[]>(sorted("all")),
	]);
	assert.equal(all[0]!.username, "user99", '"all" sorts the top level descending');
	assert.equal(byNested[0]!.username, "user1", '"nested" leaves the top level alone');
	assert.equal(byNested[0]!.posts[0]!.title, "Post 5", '"nested" still sorts collections');
	assert.equal(none[0]!.posts[0]!.title, "Post 1", '"none" sorts nothing');

	assert.equal((await hydrated(W.distinct)).length, 10_000);
	assert.equal((await hydrated(W.duplicated)).length, 500);
	for (const w of [W.singleKey, W.compositeKey]) {
		assert.equal(descend(await hydrated(w), ["items"]).length, 2500);
	}

	const attaches = await hydrated(W.attaches);
	assert.equal(attaches[499].otherPosts.length, 5);
	assert.equal(attaches[499].profile.user_id, 500);
	assert.equal((await hydrated(W.attachUsers))[499].likes.length, 5);
	const postLikes = descend(await hydrated(W.attachPosts), ["posts", "likes"]);
	assert.equal(postLikes.length, 2500);
	assert.equal(postLikes[2499].post_id, 2500);
	const small = await hydrated(W.rows20Attaches);
	assert.equal(small[0].otherPosts.length, 5);
	assert.equal(small[0].profile.user_id, 1);

	const siblings = await hydrated(W.siblings);
	const single = await hydrated(W.single);
	assert.equal(W.siblings[1].length, 10_000, "sibling joins should multiply");
	assert.equal(descend(siblings, ["posts"]).length + descend(siblings, ["tags"]).length, 2000);
	assert.equal(descend(single, ["posts"]).length, 2000);

	for (const [i, w] of widths.entries()) {
		const out = await hydrated(w);
		assert.equal(out.length, 2000);
		assert.equal(Object.keys(out[0]).length, WIDTHS[i]);
	}
	for (const [i, w] of depths.entries()) {
		assert.equal(
			descend(await hydrated(w), chainNames(depthJoins[i]!)).length,
			10_000,
			`depth ${i + 1}`,
		);
	}
}

////////////////////////////////////////////////////////////
// Benchmarks.
////////////////////////////////////////////////////////////

group({
	"10k rows to 500 entities": W.base,
	"10k rows, fields declared": W.fieldsDeclared,
	"10k rows, fields transformed": W.fieldsTransformed,
	"10k rows, omit": W.omit,
	"10k rows, extras": W.extras,
	"10k rows, extend": W.extend,
	"10k rows, with": W.with,
	"10k rows, orderByKeys": W.orderByKeys,
	"10k rows, mapped": W.mapped,
});

group({
	"10k one-to-one rows, hasMany": W.oneToOneMany,
	"10k one-to-one rows, hasOne": W.oneToOneOne,
	"10k one-to-one rows, hasOneOrThrow": W.oneToOneOrThrow,
	"10k rows, every join missed": W.nullJoin,
});

group({
	"sorted 10k rows, sort:none": W.sortNone,
	"sorted 10k rows, sort:nested": sorted("nested"),
	"sorted 10k rows, sort:all": sorted("all"),
});

group({
	"10k distinct rows to 10k entities": W.distinct,
	"10k duplicated rows to 500 entities": W.duplicated,
});

group({ "10k rows, single column key": W.singleKey, "10k rows, two column key": W.compositeKey });

// Same fetched row count at both levels; nested attaches key 2,500 parents, not 500.
group({
	"10k rows, no attaches": W.base,
	"10k rows, attachOne and attachMany": W.attaches,
	"10k rows, attachMany on users": W.attachUsers,
	"10k rows, attachMany on posts": W.attachPosts,
});

// Same 2,100 entities; the cartesian product of sibling joins is 5x the rows.
group({ "100 users, 20 posts": W.single, "100 users, 10 posts x 10 tags": W.siblings });

// 10k rows at every depth, so the per-level cost is what grows.
group(Object.fromEntries(depths.map((w, i) => [`10k rows, depth ${i + 1}`, w])));
group(Object.fromEntries(widths.map((w, i) => [`2k rows of ${WIDTHS[i]} columns`, w])));

group({
	"1 row to 1 entity, no collections": W.one,
	"20 rows to 1 entity": W.rows20,
	"20 rows to 1 entity, 2 attaches": W.rows20Attaches,
	"200 rows to 10 entities": W.rows200,
	"1k rows to 50 entities": W.rows1k,
});

await runSuite({ verify: verifyWorkloads });
