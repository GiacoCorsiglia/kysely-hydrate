/**
 * Hydration: turning flat join rows into nested objects.  This is the library's
 * hottest runtime path and the one most worth watching.
 *
 * Run through `npm run bench` (see `run.ts` for the flags).  The `summary()`
 * groupings below compare benchmarks against each other within a single
 * process, which is the trustworthy comparison: every variant sees the same
 * heap, the same JIT state and the same machine.
 *
 * Hydration is object-graph construction, so how much it allocates is as much
 * the story as how long it takes; the suite runs under `--expose-gc` so mitata
 * reports both.
 */
import assert from "node:assert/strict";

import { summary } from "mitata";

import { createHydrator } from "../src/hydrator.ts";
import { benchAsync, runSuite } from "./lib/harness.ts";
import {
	autoIncluded,
	type FlatRow,
	type HydratedUser,
	makeDistinctRows,
	makeRows,
	querySetOptions,
	times,
	withSort,
} from "./lib/rows.ts";

////////////////////////////////////////////////////////////
// Rows.
////////////////////////////////////////////////////////////

const rows1 = makeRows(1, 5, 4); // 20 rows -> 1 entity
const rows10 = makeRows(10, 5, 4); // 200 rows -> 10 entities
const rows1k = makeRows(50, 5, 4); // 1,000 rows -> 50 entities
const rows10k = makeRows(500, 5, 4); // 10,000 rows -> 500 entities
const distinctRows10k = makeDistinctRows(10_000); // 10,000 rows -> 10,000 entities

/**
 * Rows whose joins all missed, as a left join leaves them.  `groupByKey` skips
 * null keys, so nested collections stay empty and the per-entity work is only
 * the base level.  Each of the 500 rows is repeated by reference rather than
 * copied, so this allocates less up front than a driver's result would; it
 * changes nothing the hydrator does, but the heap figure is the hydrator's
 * alone.
 */
const nullJoinRows10k = makeDistinctRows(500).flatMap((row) => times(20, () => row));

/**
 * One post per user and one comment per post, so `hasOne` and `hasMany` are both
 * valid over exactly these rows.  Any wider fixture makes `hasOne` throw a
 * cardinality violation rather than measure anything, so this is the only shape
 * that compares the two fairly.
 */
const oneToOneRows10k = makeRows(10_000, 1, 1);

////////////////////////////////////////////////////////////
// Hydrators.
////////////////////////////////////////////////////////////

/** No orderings, so hydration never sorts regardless of the sort mode. */
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

/**
 * Orders by strings at every level, the expensive case: sorting is skipped
 * entirely unless a level declares an ordering, so this is the only hydrator
 * whose sort mode changes what runs.
 */
const nestedSorted = createHydrator<FlatRow>("id")
	.orderBy("username", "desc")
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.orderBy("title", "desc")
			.hasMany("comments", "comments$$", (h) => h("id").orderBy("content", "desc")),
	);

/** Ordering by the numeric key columns only, the cheaper sorting case. */
const nestedKeyed = nested.orderByKeys();

const flat = createHydrator<FlatRow>("id");

/**
 * The same rows through `hasOne` rather than `hasMany`.  A one-collection keeps
 * a single child instead of building an array, so this is the shape a query
 * without row explosion produces.
 */
const nestedOne = createHydrator<FlatRow>("id").hasOne("post", "posts$$", (h) =>
	h("id").hasOne("comment", "comments$$", (h) => h("id")),
);

/**
 * Auto-inclusion has to work out the field list per level from the rows; naming
 * fields explicitly skips that.  Both produce the same output here, so the gap
 * is the cost of inferring it.
 */
const nestedExplicitFields = createHydrator<FlatRow>("id")
	.fields(["id", "username", "email"])
	.hasMany("posts", "posts$$", (h) =>
		h("id")
			.fields(["id", "title", "user_id"])
			.hasMany("comments", "comments$$", (h) => h("id").fields(["id", "content", "post_id"])),
	);

/** `.map()` is terminal, and runs once per top-level entity. */
const nestedMapped = nested.map((entity) => {
	const user = autoIncluded<HydratedUser>(entity);
	return { ...user, label: `${user.username} has ${user.posts.length} posts` };
});

////////////////////////////////////////////////////////////
// Wide rows.
//
// Auto-inclusion caches the field list per level (`autoFieldsCache`), so the
// column count is what that cache is sized against.  A 60-column row is not
// unusual for a `select *` across a few joined tables.
////////////////////////////////////////////////////////////

const WIDE_COLUMNS = 60;

type WideRow = { id: number } & Record<string, unknown>;

const wideRows = times(2000, (i) => {
	const row: WideRow = { id: i };
	for (let c = 0; c < WIDE_COLUMNS; c++) row[`col${c}`] = `value ${i}-${c}`;
	return row;
});

const wide = createHydrator<WideRow>("id");

/** The same row count at the usual width, hoisted so the slice isn't timed. */
const narrowRows2k = distinctRows10k.slice(0, 2000);

////////////////////////////////////////////////////////////
// Composite keys.
//
// `groupByKey` compares keys by value, so a two-column key builds and compares a
// composite per row rather than reading one.
////////////////////////////////////////////////////////////

interface CompositeRow {
	tenant_id: number;
	id: number;
	name: string;
	items$$tenant_id: number;
	items$$id: number;
	items$$label: string;
}

const compositeRows = makeRows(500, 5, 4).map(
	(row, i): CompositeRow => ({
		tenant_id: 1 + (row.id % 4),
		id: row.id,
		name: row.username,
		items$$tenant_id: 1 + (row.id % 4),
		items$$id: row.posts$$id ?? i,
		items$$label: row.posts$$title ?? "",
	}),
);

const singleKey = createHydrator<CompositeRow>("id").hasMany("items", "items$$", (h) => h("id"));

const compositeKey = createHydrator<CompositeRow>(["tenant_id", "id"]).hasMany(
	"items",
	"items$$",
	(h) => h(["tenant_id", "id"]),
);

////////////////////////////////////////////////////////////
// Attached collections.
////////////////////////////////////////////////////////////

const attachedPosts = (users: number, perUser: number) =>
	times(users, (u) =>
		times(perUser, (p) => ({
			id: (u - 1) * perUser + p,
			user_id: u,
			title: `Attached post ${(u - 1) * perUser + p}`,
		})),
	).flat();

const attachedProfiles = (users: number) =>
	times(users, (u) => ({ id: u, user_id: u, bio: `Bio ${u}` }));

const attachedPosts1 = attachedPosts(1, 5);
const attachedProfiles1 = attachedProfiles(1);
const attachedPosts500 = attachedPosts(500, 5);
const attachedProfiles500 = attachedProfiles(500);

/**
 * Attach fetching is the library's only async phase, and it groups the fetched
 * rows by match key up front.  The fetch functions return a pre-built array so
 * the benchmark measures that grouping and stitching, not I/O.
 */
const withAttaches1 = nested
	.attachMany("otherPosts", () => attachedPosts1, { matchChild: "user_id" })
	.attachOne("profile", () => attachedProfiles1, { matchChild: "user_id" })
	.orderByKeys();

const withAttaches500 = nested
	.attachMany("otherPosts", () => attachedPosts500, { matchChild: "user_id" })
	.attachOne("profile", () => attachedProfiles500, { matchChild: "user_id" })
	.orderByKeys();

interface HydratedUserWithAttaches extends HydratedUser {
	otherPosts: { id: number; user_id: number; title: string }[];
	profile: { id: number; user_id: number; bio: string } | null;
}

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/**
 * Every workload runs once here and has its output asserted before anything is
 * timed.  Without this a change that makes hydration bail out early — returning
 * nothing, or skipping the sort — reads as a large speedup instead of a failure.
 */
async function verifyWorkloads(): Promise<void> {
	const users = (rows: readonly FlatRow[]) =>
		nested.hydrate(rows, querySetOptions).then((r) => autoIncluded<HydratedUser[]>(r));

	const entities = await users(rows10k);
	assert.equal(entities.length, 500, "10k rows should collapse into 500 entities");
	assert.equal(entities[0]!.posts.length, 5);
	assert.equal(entities[0]!.posts[0]!.comments.length, 4);

	assert.equal((await users(rows1)).length, 1);
	assert.equal((await users(rows10)).length, 10);
	assert.equal((await users(rows1k)).length, 50);
	assert.equal(autoIncluded<FlatRow>(await flat.hydrate(rows1[0]!, querySetOptions)).id, 1);
	assert.equal((await nestedWithExtras.hydrate(rows10k, querySetOptions))[0]!.upper, "USER1");
	assert.equal((await nestedKeyed.hydrate(rows10k, querySetOptions)).length, 500);

	// Distinct rows key to one entity each; the same hydrator over `rows10k` has
	// 20 rows per entity to collapse.  The pair is only meaningful if they really
	// do differ this way.
	assert.equal((await flat.hydrate(distinctRows10k, querySetOptions)).length, 10_000);
	assert.equal((await flat.hydrate(rows10k, querySetOptions)).length, 500);

	// Joins that missed leave null keys, which `groupByKey` skips, so the nested
	// collections come back empty rather than holding a phantom child.
	const nullJoined = autoIncluded<HydratedUser[]>(
		await nested.hydrate(nullJoinRows10k, querySetOptions),
	);
	assert.equal(nullJoined.length, 500);
	assert.equal(nullJoined[0]!.posts.length, 0, "a missed join should produce no children");

	// One-collections keep a single child rather than an array.  These rows hold
	// exactly one child per parent, so the hasMany hydrator must agree.
	const one = autoIncluded<{ post: { comment: unknown } | null }[]>(
		await nestedOne.hydrate(oneToOneRows10k, querySetOptions),
	);
	const many = autoIncluded<HydratedUser[]>(await nested.hydrate(oneToOneRows10k, querySetOptions));
	assert.equal(one.length, 10_000);
	assert.equal(many.length, 10_000);
	assert.equal(many[0]!.posts.length, 1);
	assert.equal(Array.isArray(one[0]!.post), false, "hasOne should not produce an array");
	assert.notEqual(one[0]!.post, null);

	// Explicit fields and auto-inclusion must agree, or the pair compares two
	// different amounts of work rather than two ways of deciding the same fields.
	const explicit = autoIncluded<HydratedUser[]>(
		await nestedExplicitFields.hydrate(rows10k, querySetOptions),
	);
	assert.deepEqual(explicit[0], entities[0], "explicit fields should match auto-inclusion");

	assert.equal(
		(await nestedMapped.hydrate(rows10k, querySetOptions))[0]!.label,
		"user1 has 5 posts",
	);

	assert.equal((await wide.hydrate(wideRows, querySetOptions)).length, 2000);
	assert.equal(
		Object.keys(autoIncluded<object>((await wide.hydrate(wideRows, querySetOptions))[0])).length,
		WIDE_COLUMNS + 1,
	);

	// `tenant_id` is derived from `id`, so the two-column key groups exactly as
	// the single-column one does.  That is deliberate: identical grouping leaves
	// key arity as the only difference between the two benchmarks.
	const single = await singleKey.hydrate(compositeRows, querySetOptions);
	const composite = await compositeKey.hydrate(compositeRows, querySetOptions);
	assert.equal(single.length, 500);
	assert.equal(composite.length, 500);

	// Sorting is skipped entirely at levels that declare no ordering, so the sort
	// modes are only worth benchmarking against a hydrator that orders — and only
	// if the modes actually produce different output.  Assert that they do.
	const sorted = (mode: "all" | "nested" | "none") =>
		nestedSorted.hydrate(rows10k, withSort(mode)).then((r) => autoIncluded<HydratedUser[]>(r));

	const sortedAll = await sorted("all");
	const sortedNested = await sorted("nested");
	const unsorted = await sorted("none");

	assert.equal(sortedAll[0]!.username, "user99", '"all" sorts the top level descending');
	assert.equal(sortedNested[0]!.username, "user1", '"nested" leaves the top level alone');
	assert.equal(unsorted[0]!.username, "user1");
	assert.equal(sortedNested[0]!.posts[0]!.title, "Post 5", '"nested" still sorts collections');
	assert.equal(unsorted[0]!.posts[0]!.title, "Post 1", '"none" sorts nothing');

	// Attached collections are fetched once for all parents and grouped by match
	// key; check the stitching landed on the right parent.
	const attached1 = autoIncluded<HydratedUserWithAttaches[]>(
		await withAttaches1.hydrate(rows1, querySetOptions),
	);
	assert.equal(attached1[0]!.otherPosts.length, 5);
	assert.equal(attached1[0]!.profile?.bio, "Bio 1");

	const attached500 = autoIncluded<HydratedUserWithAttaches[]>(
		await withAttaches500.hydrate(rows10k, querySetOptions),
	);
	assert.equal(attached500.length, 500);
	assert.equal(attached500[499]!.otherPosts.length, 5);
	assert.equal(attached500[499]!.profile?.bio, "Bio 500");
}

await verifyWorkloads();

////////////////////////////////////////////////////////////
// 10,000 rows into 500 entities, feature by feature.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k rows to 500 entities", () => nested.hydrate(rows10k, querySetOptions)).baseline(
		true,
	);
	benchAsync("10k rows to 500 entities, extras", () =>
		nestedWithExtras.hydrate(rows10k, querySetOptions),
	);
	benchAsync("10k rows to 500 entities, orderByKeys", () =>
		nestedKeyed.hydrate(rows10k, querySetOptions),
	);
	benchAsync("10k rows to 500 entities, mapped", () =>
		nestedMapped.hydrate(rows10k, querySetOptions),
	);
	// `withAttaches500` orders by keys, so the orderByKeys row above is its
	// like-for-like partner rather than the bare baseline.
	benchAsync("10k rows to 500 entities, 2 attaches", () =>
		withAttaches500.hydrate(rows10k, querySetOptions),
	);
});

////////////////////////////////////////////////////////////
// Deciding which fields to include.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k rows, fields inferred", () => nested.hydrate(rows10k, querySetOptions)).baseline(
		true,
	);
	benchAsync("10k rows, fields declared", () =>
		nestedExplicitFields.hydrate(rows10k, querySetOptions),
	);
});

////////////////////////////////////////////////////////////
// Collection shape.
////////////////////////////////////////////////////////////

summary(() => {
	// The first two run over rows holding exactly one child per parent, which is
	// the only shape `hasOne` accepts, so the gap between them is building an
	// array against keeping a single child.
	benchAsync("10k one-to-one rows, hasMany", () =>
		nested.hydrate(oneToOneRows10k, querySetOptions),
	).baseline(true);
	benchAsync("10k one-to-one rows, hasOne", () =>
		nestedOne.hydrate(oneToOneRows10k, querySetOptions),
	);
	benchAsync("10k rows, every join missed", () => nested.hydrate(nullJoinRows10k, querySetOptions));
});

////////////////////////////////////////////////////////////
// What sorting costs.
//
// All three use the same hydrator, which orders by a string at every level, so
// the sort mode is the only variable.  The options are hoisted because building
// them inside the measured call would time an object literal too.
////////////////////////////////////////////////////////////

const sortNone = withSort("none");
const sortNested = withSort("nested");
const sortAll = withSort("all");

summary(() => {
	benchAsync("sorted 10k rows, sort:none", () => nestedSorted.hydrate(rows10k, sortNone)).baseline(
		true,
	);
	benchAsync("sorted 10k rows, sort:nested", () => nestedSorted.hydrate(rows10k, sortNested));
	benchAsync("sorted 10k rows, sort:all", () => nestedSorted.hydrate(rows10k, sortAll));
});

////////////////////////////////////////////////////////////
// Per-entity cost against per-row cost.
//
// Both hydrate 10,000 rows, but one produces 10,000 entities and the other 500,
// so the gap is per-entity overhead and the allocation that comes with it, not
// grouping alone.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k distinct rows to 10k entities", () =>
		flat.hydrate(distinctRows10k, querySetOptions),
	).baseline(true);
	benchAsync("10k duplicated rows to 500 entities", () => flat.hydrate(rows10k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Key arity.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k rows, single column key", () =>
		singleKey.hydrate(compositeRows, querySetOptions),
	).baseline(true);
	benchAsync("10k rows, two column key", () =>
		compositeKey.hydrate(compositeRows, querySetOptions),
	);
});

////////////////////////////////////////////////////////////
// Row width.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("2k rows of 60 columns", () => wide.hydrate(wideRows, querySetOptions)).baseline(true);
	benchAsync("2k rows of 9 columns", () => flat.hydrate(narrowRows2k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Small results, where fixed per-call costs dominate.
//
// The scaling entries all use one hydrator, so they read as a curve; the
// attaches entry sits here because its result is the same size, not because it
// belongs to that curve.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("1 row to 1 entity, no collections", () =>
		flat.hydrate(rows1[0]!, querySetOptions),
	).baseline(true);
	benchAsync("20 rows to 1 entity", () => nestedKeyed.hydrate(rows1, querySetOptions));
	benchAsync("20 rows to 1 entity, 2 attaches", () =>
		withAttaches1.hydrate(rows1, querySetOptions),
	);
	benchAsync("200 rows to 10 entities", () => nestedKeyed.hydrate(rows10, querySetOptions));
	benchAsync("1k rows to 50 entities", () => nestedKeyed.hydrate(rows1k, querySetOptions));
});

await runSuite("hydrate");
