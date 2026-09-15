/**
 * Benchmarks for the runtime hot paths: hydrating flat rows into nested objects,
 * building and compiling queries, and running one end to end against SQLite.
 *
 *   npm run bench                 # print a report
 *   npm run bench -- --filter X   # only benchmarks matching the regex /X/
 *   npm run bench:save            # record benchmarks/baseline.json
 *   npm run bench:compare         # diff this run against that baseline
 *
 * Read the numbers as relative comparisons.  The `summary()` groupings below
 * compare benchmarks against each other within a single process, which is the
 * trustworthy comparison: every variant sees the same heap, the same JIT state
 * and the same machine.  Across runs both time and allocation drift by about
 * 17% on shared hardware, so `--compare` only flags a change beyond that.
 *
 * Run under `--expose-gc` (the npm scripts do) so mitata can collect between
 * samples and report heap usage.  Hydration is object-graph construction, so how
 * much it allocates is as much the story as how long it takes.
 */
import assert from "node:assert/strict";

import { bench, do_not_optimize, run, summary } from "mitata";

import { type HydrateOptions } from "../src/hydrator.ts";
import { compareBaseline, readBaseline, saveBaseline } from "./baseline.ts";
import {
	autoIncluded,
	buildUsersWithPosts,
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

/** Reads `--flag value` or `--flag=value`, and rejects anything ambiguous. */
function flagValue(flag: string): string | undefined {
	const args = process.argv.slice(2);
	const matches = args.filter((a) => a === flag || a.startsWith(`${flag}=`));

	if (matches.length === 0) return undefined;
	if (matches.length > 1) throw new Error(`${flag} was given more than once`);

	const [match] = matches;
	if (match!.startsWith(`${flag}=`)) {
		const value = match!.slice(flag.length + 1);
		if (value === "") throw new Error(`${flag} requires a value`);
		return value;
	}

	// A separate value may legitimately start with "-" (a regex like "-foo"), so
	// only a second flag is rejected.
	const value = args[args.indexOf(match!) + 1];
	if (value === undefined || value.startsWith("--")) throw new Error(`${flag} requires a value`);
	return value;
}

const savePath = flagValue("--save");
const comparePath = flagValue("--compare");
const filter = flagValue("--filter");

if (savePath !== undefined && filter !== undefined) {
	// A filtered save would drop every unmatched benchmark from the baseline, and
	// nothing would later report them as missing.
	throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
}

// Read the baseline up front so a bad path fails now rather than after several
// minutes of benchmarking.
const baseline = comparePath === undefined ? undefined : readBaseline(comparePath);

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
// Hydration: 10,000 rows into 500 entities, feature by feature.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k rows -> 500 entities", () => nested.hydrate(rows10k, querySetOptions)).baseline(
		true,
	);
	benchAsync("10k rows -> 500 entities, extras", () =>
		nestedWithExtras.hydrate(rows10k, querySetOptions),
	);
	benchAsync("10k rows -> 500 entities, orderByKeys", () =>
		nestedKeyed.hydrate(rows10k, querySetOptions),
	);
	// Attach fetching is the library's only async phase.  The fetch functions
	// return a pre-built array, so this measures grouping the fetched rows by
	// match key and stitching them onto 500 parents, not I/O.  `withAttaches500`
	// orders by keys, so the row above is its like-for-like partner.
	benchAsync("10k rows -> 500 entities, 2 attaches", () =>
		withAttaches500.hydrate(rows10k, querySetOptions),
	);
});

////////////////////////////////////////////////////////////
// Hydration: what sorting costs.
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
// Hydration: per-entity cost against per-row cost.
//
// Both hydrate 10,000 rows, but one produces 10,000 entities and the other 500,
// so the gap is per-entity overhead and the allocation that comes with it, not
// grouping alone.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("10k distinct rows -> 10k entities", () =>
		flat.hydrate(distinctRows10k, querySetOptions),
	).baseline(true);
	benchAsync("10k duplicated rows -> 500 entities", () => flat.hydrate(rows10k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Hydration: small results, where fixed per-call costs dominate.
//
// Every entry but the first uses one hydrator, so the series is a scaling curve
// rather than a comparison of different configurations.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("1 row -> 1 entity, no collections", () =>
		flat.hydrate(rows1[0]!, querySetOptions),
	).baseline(true);
	benchAsync("20 rows -> 1 entity", () => nestedKeyed.hydrate(rows1, querySetOptions));
	benchAsync("20 rows -> 1 entity, 2 attaches", () =>
		withAttaches1.hydrate(rows1, querySetOptions),
	);
	benchAsync("200 rows -> 10 entities", () => nestedKeyed.hydrate(rows10, querySetOptions));
	benchAsync("1k rows -> 50 entities", () => nestedKeyed.hydrate(rows1k, querySetOptions));
});

////////////////////////////////////////////////////////////
// Query building.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync("querySet build", buildUsersWithPosts).baseline(true);
	benchSync("querySet toQuery", () => usersWithPosts.toQuery());
	benchSync("querySet compile", () => usersWithPosts.compile());
	benchSync("querySet toCountQuery then compile", () => usersWithPosts.toCountQuery().compile());
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

// `throw: true` makes mitata propagate a failing benchmark instead of recording
// the error on the run and carrying on, which would drop it from the report.
const trials = await run({
	throw: true,
	...(filter !== undefined && { filter: new RegExp(filter) }),
});

if (savePath !== undefined) saveBaseline(savePath, trials);

if (
	baseline !== undefined &&
	!compareBaseline(baseline, trials, { reportMissing: filter === undefined })
) {
	process.exitCode = 1;
}
