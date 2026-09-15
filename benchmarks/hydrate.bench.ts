/**
 * Benchmarks for the runtime hot paths: hydrating flat rows into nested objects,
 * building and compiling queries, and running one end to end against SQLite.
 *
 *   npm run bench                 # print a report
 *   npm run bench -- --filter X   # only benchmarks whose name matches /X/
 *   npm run bench:save            # record benchmarks/baseline.json
 *   npm run bench:compare         # diff this run against that baseline
 *
 * Read the numbers as relative comparisons.  The `summary()` groupings below
 * compare benchmarks against each other within a single process, which is the
 * trustworthy comparison: every variant sees the same heap, the same JIT state
 * and the same machine.  Across runs, wall-clock timings on shared hardware
 * drift by more than 10% on their own; `--compare` accounts for that with a wide
 * threshold, and allocation per iteration is the steadier signal.
 *
 * Run under `--expose-gc` (the npm scripts do) so mitata can collect between
 * samples and report heap usage.  Hydration is object-graph construction, so how
 * much it allocates is as much the story as how long it takes.
 */
import assert from "node:assert/strict";

import { bench, do_not_optimize, run, summary } from "mitata";

import { type HydrateOptions } from "../src/hydrator.ts";
import { compareBaseline, saveBaseline } from "./baseline.ts";
import {
	autoIncluded,
	distinctRows10k,
	flat,
	type FlatRow,
	type HydratedUser,
	type HydratedUserWithAttaches,
	nested,
	nestedKeyed,
	nestedSorted,
	nestedWithExtras,
	querySetOptions,
	rows1,
	rows10,
	rows1k,
	rows10k,
	usersWithPosts,
	withAttaches1,
	withAttaches500,
	withSort,
} from "./fixtures.ts";

////////////////////////////////////////////////////////////
// Arguments.
////////////////////////////////////////////////////////////

function flagValue(flag: string): string | undefined {
	const index = process.argv.indexOf(flag);
	if (index === -1) return undefined;

	const value = process.argv[index + 1];
	if (value === undefined || value.startsWith("--")) {
		throw new Error(`${flag} requires a value`);
	}
	return value;
}

const savePath = flagValue("--save");
const comparePath = flagValue("--compare");
const filter = flagValue("--filter");

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/**
 * Every workload runs once here and has its output asserted before anything is
 * timed.  Without this a change that makes hydration bail out early — returning
 * nothing, or skipping the sort — reads as a large speedup instead of a failure.
 */
async function verifyWorkloads(): Promise<void> {
	const users = (rows: readonly FlatRow[], options = querySetOptions) =>
		nested.hydrate(rows, options).then((r) => autoIncluded<HydratedUser[]>(r));

	const entities = await users(rows10k);
	assert.equal(entities.length, 500, "10k rows should collapse into 500 entities");
	assert.equal(entities[0]!.posts.length, 5);
	assert.equal(entities[0]!.posts[0]!.comments.length, 4);

	const one = await users(rows1);
	assert.equal(one.length, 1);
	assert.equal(one[0]!.posts.length, 5);

	const single = autoIncluded<FlatRow>(await flat.hydrate(rows1[0]!, querySetOptions));
	assert.equal(single.id, 1);

	assert.equal((await users(rows10)).length, 10);
	assert.equal((await users(rows1k)).length, 50);
	assert.equal((await nestedWithExtras.hydrate(rows10k, querySetOptions))[0]!.upper, "USER1");
	assert.equal((await nestedKeyed.hydrate(rows10k, querySetOptions)).length, 500);

	// Distinct rows key to one entity each; the same hydrator over `rows10k` has
	// 20 rows per entity to collapse.  The pair is only meaningful if they really
	// do differ this way.
	assert.equal((await flat.hydrate(distinctRows10k, querySetOptions)).length, 10_000);
	assert.equal((await flat.hydrate(rows10k, querySetOptions)).length, 500);

	// Sorting is skipped entirely at levels that declare no ordering, so the sort
	// modes are only worth benchmarking against a hydrator that orders — and only
	// if the modes actually produce different output.  Assert that they do.
	const sorted = (options: HydrateOptions) =>
		nestedSorted.hydrate(rows10k, options).then((r) => autoIncluded<HydratedUser[]>(r));

	const sortedAll = await sorted(withSort("all"));
	const sortedNested = await sorted(withSort("nested"));
	const unsorted = await sorted(withSort("none"));

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

	// The SQLite fixture is seeded to match `rows10k`, so the end-to-end and
	// in-memory numbers describe the same amount of work.
	assert.equal((await usersWithPosts.toQuery().execute()).length, 10_000);
	assert.equal((await usersWithPosts.execute()).length, 500);
	assert.equal(usersWithPosts.compile().sql.length > 0, true);
}

await verifyWorkloads();

////////////////////////////////////////////////////////////
// Helpers.
////////////////////////////////////////////////////////////

/**
 * `do_not_optimize` keeps the JIT from discarding work whose result is thrown
 * away, which is every benchmark here.
 */
function benchAsync(name: string, fn: () => Promise<unknown>) {
	return bench(name, async () => {
		do_not_optimize(await fn());
	});
}

function benchSync(name: string, fn: () => unknown) {
	return bench(name, () => {
		do_not_optimize(fn());
	});
}

////////////////////////////////////////////////////////////
// Hydration: 10,000 rows into 500 entities.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("nested 10k rows", () => nested.hydrate(rows10k, querySetOptions)).baseline(true);
	benchAsync("nested 10k rows + extras", () => nestedWithExtras.hydrate(rows10k, querySetOptions));
	benchAsync("nested 10k rows + orderByKeys", () => nestedKeyed.hydrate(rows10k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Hydration: what sorting costs.
//
// All three use the same hydrator, which orders by a string at every level, so
// the sort mode is the only variable.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("sorted 10k rows, sort:none", () =>
		nestedSorted.hydrate(rows10k, withSort("none")),
	).baseline(true);
	benchAsync("sorted 10k rows, sort:nested", () =>
		nestedSorted.hydrate(rows10k, withSort("nested")),
	);
	benchAsync("sorted 10k rows, sort:all", () => nestedSorted.hydrate(rows10k, withSort("all")));
});

////////////////////////////////////////////////////////////
// Hydration: grouping cost, with and without duplicate rows.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k distinct rows -> 10k entities", () =>
		flat.hydrate(distinctRows10k, querySetOptions),
	).baseline(true);
	benchAsync("10k duplicated rows -> 500 entities", () => flat.hydrate(rows10k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Hydration: small results, where fixed per-call costs dominate.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("1 row -> 1 entity, no collections", () =>
		flat.hydrate(rows1[0]!, querySetOptions),
	).baseline(true);
	benchAsync("20 rows -> 1 entity", () => nestedKeyed.hydrate(rows1, querySetOptions));
	benchAsync("200 rows -> 10 entities", () => nestedKeyed.hydrate(rows10, querySetOptions));
	benchAsync("1k rows -> 50 entities", () => nested.hydrate(rows1k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Hydration: attached collections.
//
// Attach fetching is the library's only async phase; these measure grouping the
// fetched rows by match key and stitching them onto parents, not I/O.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("20 rows -> 1 entity, no attaches", () =>
		nestedKeyed.hydrate(rows1, querySetOptions),
	).baseline(true);
	benchAsync("20 rows -> 1 entity, 2 attaches", () =>
		withAttaches1.hydrate(rows1, querySetOptions),
	);
});

summary(() => {
	benchAsync("10k rows -> 500 entities, no attaches", () =>
		nestedKeyed.hydrate(rows10k, querySetOptions),
	).baseline(true);
	benchAsync("10k rows -> 500 entities, 2 attaches", () =>
		withAttaches500.hydrate(rows10k, querySetOptions),
	);
});

////////////////////////////////////////////////////////////
// Query building.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("querySet toQuery()", () => usersWithPosts.toQuery()).baseline(true);
	benchSync("querySet compile()", () => usersWithPosts.compile());
	benchSync("querySet toCountQuery().compile()", () => usersWithPosts.toCountQuery().compile());
});

////////////////////////////////////////////////////////////
// End to end against SQLite.
//
// The gap between these two is what hydration adds to a real round trip.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("kysely execute, 10k rows, no hydration", () =>
		usersWithPosts.toQuery().execute(),
	).baseline(true);
	benchAsync("querySet execute, 10k rows -> 500 entities", () => usersWithPosts.execute());
});

////////////////////////////////////////////////////////////
// Run.
////////////////////////////////////////////////////////////

const trials = await run(filter === undefined ? {} : { filter: new RegExp(filter) });

if (savePath !== undefined) saveBaseline(savePath, trials);

if (
	comparePath !== undefined &&
	!compareBaseline(comparePath, trials, { reportMissing: filter === undefined })
) {
	process.exitCode = 1;
}
