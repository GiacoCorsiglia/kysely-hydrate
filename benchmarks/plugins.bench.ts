/**
 * Benchmarks for `fixLongAliases()`, the Kysely plugin in `src/fix-long-aliases.ts`.
 *
 *   npm run bench plugins                 # just this suite
 *   npm run bench plugins -- --filter X   # only benchmarks matching the regex /X/
 *
 * The plugin sits on two hot paths.  `transformQuery` runs once per query and
 * walks (and sometimes deep-clones) the whole operation node tree.
 * `transformResult` runs `restoreRow` over every result row, and `restoreRow`
 * rebuilds each row key by key through `restore()` — 90,000 `restore()` calls
 * and 10,000 fresh objects for a 10,000-row, 9-column result.
 *
 * ## A warning about module-level state
 *
 * `originalByShort`, `restoredByKey` and `queriesToRestore` in the module under
 * test are module-level and never evicted, so every benchmark in this process
 * shares them and nothing here can reset them.  That is a real property of the
 * plugin, not an artifact of benchmarking it, and the last group below exists
 * precisely to measure what it costs.  Two consequences shape this file:
 *
 *  1. `originalByShort` only ever grows, so the scaling group's entries have to
 *     be declared — and therefore run — from the smallest alias count upwards,
 *     and each one registers its own aliases on its first call rather than up
 *     front.  It is declared last so that nothing else is measured against a map
 *     that it inflated.
 *  2. Everything outside that group is O(1) in the map's size — `restoreRow`
 *     hits `restoredByKey`, and `shorten` only does a `Map.get` on
 *     `originalByShort` — so those benchmarks are safe to run in any order.
 */
import assert from "node:assert/strict";

import * as k from "kysely";
import { summary } from "mitata";

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { querySet } from "../src/query-set.ts";
import { emptyDb } from "./lib/db.ts";
import { benchAsync, benchSync, runSuite } from "./lib/harness.ts";
import { makeRows, times } from "./lib/rows.ts";

////////////////////////////////////////////////////////////
// Helpers.
////////////////////////////////////////////////////////////

/**
 * Benchmark names double as `--filter` patterns, which are regexes, so a name
 * holding a metacharacter would not select itself.  Checked rather than eyeballed.
 */
function regexSafe(name: string): string {
	assert.ok(new RegExp(name).test(name), `benchmark name does not match itself: ${name}`);
	return name;
}

const bytes = (identifier: string) => Buffer.byteLength(identifier);

/** Every identifier in a node tree, in the order the transformer would meet them. */
function identifiersIn(node: unknown, found: string[] = []): string[] {
	if (Array.isArray(node)) {
		for (const item of node) {
			identifiersIn(item, found);
		}
	} else if (typeof node === "object" && node !== null && "kind" in node) {
		const n = node as k.OperationNode;
		if (k.IdentifierNode.is(n)) {
			found.push(n.name);
		} else {
			for (const key in n) {
				identifiersIn((node as Record<string, unknown>)[key], found);
			}
		}
	}
	return found;
}

/** The output aliases of a {@link selectAliases} node, in order. */
function outputAliases(node: k.RootOperationNode): string[] {
	assert.ok(k.SelectQueryNode.is(node));
	return (node.selections ?? []).map((selection) => {
		const { alias } = selection.selection as k.AliasNode;
		assert.ok(k.IdentifierNode.is(alias));
		return alias.name;
	});
}

/**
 * `toOperationNode()` is typed as any node, while a plugin only ever sees a root
 * one.  Narrowed by assertion rather than by cast, since everything here is a
 * select.
 */
function rootNode(query: { toOperationNode: () => k.OperationNode }): k.RootOperationNode {
	const node = query.toOperationNode();
	assert.ok(k.SelectQueryNode.is(node));
	return node;
}

const db = emptyDb();

/**
 * `SELECT 1 AS "<alias>", ...`: the cheapest node that carries a chosen set of
 * identifiers, and the only way to hand the plugin aliases it has never seen.
 */
function selectAliases(aliases: readonly string[]): k.RootOperationNode {
	return rootNode(db.selectNoFrom(aliases.map((alias) => k.sql.lit(1).as(alias))));
}

/**
 * Puts `aliases` into the module-level map and marks `queryId` as one whose rows
 * need restoring, then reports what each alias was shortened to.  `transformQuery`
 * is the only entry point, so registering is the same work a real query does.
 */
const registrar = fixLongAliases();
function registerAliases(aliases: readonly string[], queryId = k.createQueryId()): string[] {
	return outputAliases(registrar.transformQuery({ queryId, node: selectAliases(aliases) }));
}

////////////////////////////////////////////////////////////
// Result rows, for the per-row restore path.
////////////////////////////////////////////////////////////

// Two levels of `$$` prefixing already cost 67 bytes, so every column below is
// over the limit — the state a deep join leaves a result set in.
const ROW_PREFIX = "postsAuthoredByEachRegisteredUser$$commentsLeftOnEachOfThosePosts$$";

const rowAliases = Object.keys(makeRows(1, 1, 1)[0]!).map((column) => `${ROW_PREFIX}${column}`);
const restoreQueryId = k.createQueryId();
const rowShorts = registerAliases(rowAliases, restoreQueryId);

/** `makeRows` output re-keyed to the shortened names the database would return. */
function shortKeyedRows(users: number): k.QueryResult<k.UnknownRow> {
	const rows = makeRows(users, 5, 4).map((row) =>
		Object.fromEntries(Object.values(row).map((value, i) => [rowShorts[i]!, value])),
	);
	return { rows };
}

const result100 = shortKeyedRows(5);
const result1k = shortKeyedRows(50);
const result10k = shortKeyedRows(500);

// Not passed through `transformQuery`, so the plugin returns its rows untouched.
// The gap against the row counts above is what restoring costs over doing nothing.
const untouchedQueryId = k.createQueryId();

////////////////////////////////////////////////////////////
// Queries, for the per-query path.
////////////////////////////////////////////////////////////

// The same three-level query set twice over, differing only in what the joined
// collections are called.  The library prefixes a column with every collection
// above it, so the long names push the deepest columns past 63 bytes while the
// short ones leave every identifier comfortably inside it.
const shortJoins = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.leftJoinMany(
		"posts",
		({ eb, qs }) =>
			qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
				"comments",
				({ eb, qs }) =>
					qs(eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"])).leftJoinOne(
						"author",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
						"author.id",
						"comments.user_id",
					),
				"comments.post_id",
				"posts.id",
			),
		"posts.user_id",
		"user.id",
	)
	.orderBy("id")
	.limit(500);

const longJoins = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.leftJoinMany(
		"postsAuthoredByEachRegisteredUser",
		({ eb, qs }) =>
			qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
				"commentsLeftOnEachOfThosePosts",
				({ eb, qs }) =>
					qs(eb.selectFrom("comments").select(["id", "content", "post_id", "user_id"])).leftJoinOne(
						"theAuthorOfEachOfThoseComments",
						({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
						"theAuthorOfEachOfThoseComments.id",
						"commentsLeftOnEachOfThosePosts.user_id",
					),
				"commentsLeftOnEachOfThosePosts.post_id",
				"postsAuthoredByEachRegisteredUser.id",
			),
		"postsAuthoredByEachRegisteredUser.user_id",
		"user.id",
	)
	.orderBy("id")
	.limit(500);

const shortNode = rootNode(shortJoins);
const longNode = rootNode(longJoins);
const queryQueryId = k.createQueryId();

const plugin = fixLongAliases();
const camelPlugin = fixLongAliases(new k.CamelCasePlugin());

////////////////////////////////////////////////////////////
// Alias pool, for shortening and for the scaling group.
////////////////////////////////////////////////////////////

// Distinct aliases of the shape a deep join produces.  They share a prefix, so
// their shortened forms differ only in the trailing hash, which is the case that
// makes `restore()` compare the most characters before rejecting an entry.
const POOL_PREFIX = "organizationalDepartments$$departmentalEmployeeRecords$$employee_";
const pool = times(10_000, (i) => `${POOL_PREFIX}preferred_full_display_name_${i}`);

let registered = 0;
let poolShort = "";

/**
 * Grows `originalByShort` to hold the pool's first `count` aliases.  Cumulative
 * because the map has no way back: a benchmark can only ever be measured against
 * a map at least as large as the one before it left behind.
 */
function ensureRegistered(count: number): void {
	if (count <= registered) {
		return;
	}
	const shorts = registerAliases(pool.slice(registered, count));
	poolShort ||= shorts[0]!;
	registered = count;
}

// Registered up front because the shortening group below measures this same
// slice, and because it fixes the floor the scaling group starts from.
const HASHED_ALIASES = 100;
ensureRegistered(HASHED_ALIASES);
const hashedNode = selectAliases(pool.slice(0, HASHED_ALIASES));

/** A one-row result whose only key has never been restored before. */
let novelKeys = 0;
function novelKeyResult(): k.QueryResult<k.UnknownRow> {
	// An outer query prefixing an alias that was already shortened, which is the
	// shape that makes `restore()` take a second pass over the whole map: the
	// first pass substitutes, so the fixed-point loop has to prove the second
	// changes nothing.
	return { rows: [{ [`enclosingQuery${novelKeys++}$$${poolShort}`]: 1 }] };
}

/** As above, but matching nothing, so one pass over the map settles it. */
function inertKeyResult(): k.QueryResult<k.UnknownRow> {
	return { rows: [{ [`enclosingQuery${novelKeys++}$$column_with_no_shortened_name`]: 1 }] };
}

const poolQueryId = k.createQueryId();
registerAliases([pool[0]!], poolQueryId);

// Restored once below and cached from then on, so this is the control the
// scaling entries are read against.
const cachedKeyResult: k.QueryResult<k.UnknownRow> = {
	rows: [{ [`enclosingQueryCached$$${poolShort}`]: 1 }],
};

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/**
 * Every workload runs once here and has its output asserted before anything is
 * timed.  Without this a change that turned the plugin into a pass-through —
 * shortening nothing, restoring nothing — would read as an enormous speedup
 * rather than as a failure.
 *
 * The exception is the scaling group, which cannot be verified from here: each
 * entry's aliases would be registered by the verification pass, and since the
 * map only grows, the smallest measurement would end up describing the largest
 * map.  Those entries assert their own first result instead; mitata discards a
 * benchmark's first call as warmup, so the check costs nothing that is reported.
 */
async function verifyWorkloads(): Promise<void> {
	assert.equal(rowShorts.length, 9, "the result fixture should have 9 columns");
	for (const [i, short] of rowShorts.entries()) {
		assert.ok(bytes(rowAliases[i]!) > MAX_IDENTIFIER_BYTES, rowAliases[i]);
		assert.ok(bytes(short) <= MAX_IDENTIFIER_BYTES, short);
	}

	// Restoring has to put every original name back, on every row.
	const restored = await plugin.transformResult({
		queryId: restoreQueryId,
		result: result10k,
	});
	assert.equal(restored.rows.length, 10_000);
	assert.deepEqual(Object.keys(restored.rows[0]!), rowAliases);
	assert.deepEqual(Object.keys(restored.rows.at(-1)!), rowAliases);
	assert.equal(
		(await plugin.transformResult({ queryId: restoreQueryId, result: result100 })).rows.length,
		100,
	);
	assert.equal(
		(await plugin.transformResult({ queryId: restoreQueryId, result: result1k })).rows.length,
		1_000,
	);

	// A query that was never shortened must come back as the very same rows, or
	// the pair of 10k benchmarks is measuring the same thing twice.
	const untouched = await plugin.transformResult({
		queryId: untouchedQueryId,
		result: result10k,
	});
	assert.equal(untouched.rows, result10k.rows);

	// The short-named query has nothing to do, and proves it by handing back the
	// node it was given rather than a clone.
	assert.ok(
		identifiersIn(shortNode).every((name) => bytes(name) <= MAX_IDENTIFIER_BYTES),
		"the short-named query should contain no long identifier",
	);
	assert.equal(plugin.transformQuery({ queryId: queryQueryId, node: shortNode }), shortNode);

	// The long-named one does, and every identifier it emits must fit.
	assert.ok(
		identifiersIn(longNode).some((name) => bytes(name) > MAX_IDENTIFIER_BYTES),
		"the long-named query should contain a long identifier",
	);
	for (const node of [
		plugin.transformQuery({ queryId: queryQueryId, node: longNode }),
		camelPlugin.transformQuery({ queryId: queryQueryId, node: longNode }),
	]) {
		assert.notEqual(node, longNode);
		for (const name of identifiersIn(node)) {
			assert.ok(bytes(name) <= MAX_IDENTIFIER_BYTES, name);
		}
	}

	// Hashing is not reachable on its own, so it is measured as the difference
	// between a plugin that has these 100 names cached and one that has to shorten
	// them again.  Both must produce the same, already-registered short names.
	const cached = outputAliases(
		fixLongAliases().transformQuery({ queryId: queryQueryId, node: hashedNode }),
	);
	assert.equal(cached.length, HASHED_ALIASES);
	assert.deepEqual(cached, registerAliases(pool.slice(0, HASHED_ALIASES)));

	// The scaling group's control: restoring a key that is already cached still
	// has to put the original name back.
	const control = await plugin.transformResult({
		queryId: poolQueryId,
		result: cachedKeyResult,
	});
	assert.deepEqual(Object.keys(control.rows[0]!), [`enclosingQueryCached$$${pool[0]!}`]);
}

await verifyWorkloads();

////////////////////////////////////////////////////////////
// transformResult: the per-row path.
//
// One `restore()` call per column of every row, and one fresh object per row.
// The last entry skips all of that, so the gap against the 10k entry is what
// restoring a result set costs over handing it straight back.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync(regexSafe("transformResult 100 rows"), () =>
		plugin.transformResult({ queryId: restoreQueryId, result: result100 }),
	).baseline(true);
	benchAsync(regexSafe("transformResult 1k rows"), () =>
		plugin.transformResult({ queryId: restoreQueryId, result: result1k }),
	);
	benchAsync(regexSafe("transformResult 10k rows"), () =>
		plugin.transformResult({ queryId: restoreQueryId, result: result10k }),
	);
	benchAsync(regexSafe("transformResult 10k rows, nothing to restore"), () =>
		plugin.transformResult({ queryId: untouchedQueryId, result: result10k }),
	);
});

////////////////////////////////////////////////////////////
// transformQuery: the per-query path.
//
// Both entries scan the whole tree.  Only the second finds a long identifier and
// goes on to `transformNode`, which deep-clones it — the cost `scan` exists to
// avoid paying on the queries that do not need it.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync(regexSafe("transformQuery, no long identifiers"), () =>
		plugin.transformQuery({ queryId: queryQueryId, node: shortNode }),
	).baseline(true);
	benchSync(regexSafe("transformQuery, long identifiers"), () =>
		plugin.transformQuery({ queryId: queryQueryId, node: longNode }),
	);
});

////////////////////////////////////////////////////////////
// Shortening, and the hash inside it.
//
// `hash()` is not exported and every caller memoizes it, so it is measured by
// difference: a fresh plugin has an empty name cache and has to hash all 100
// aliases, a warm one has none to hash.  A third entry prices the plugin
// instance itself, since the cold entry allocates one per iteration.
////////////////////////////////////////////////////////////

const warmPlugin = fixLongAliases();
warmPlugin.transformQuery({ queryId: queryQueryId, node: hashedNode });

summary(() => {
	benchSync(regexSafe("transformQuery 100 long aliases, names cached"), () =>
		warmPlugin.transformQuery({ queryId: queryQueryId, node: hashedNode }),
	).baseline(true);
	benchSync(regexSafe("transformQuery 100 long aliases, names hashed"), () =>
		fixLongAliases().transformQuery({ queryId: queryQueryId, node: hashedNode }),
	);
	benchSync(regexSafe("fixLongAliases construction"), () => fixLongAliases());
});

////////////////////////////////////////////////////////////
// Composing with CamelCasePlugin, the documented setup.
//
// The wrapped plugin runs first and deep-clones the tree in its own right, so
// this is two passes over the query rather than one.
////////////////////////////////////////////////////////////

summary(() => {
	benchSync(regexSafe("transformQuery long query, bare"), () =>
		plugin.transformQuery({ queryId: queryQueryId, node: longNode }),
	).baseline(true);
	benchSync(regexSafe("transformQuery long query, wrapping CamelCasePlugin"), () =>
		camelPlugin.transformQuery({ queryId: queryQueryId, node: longNode }),
	);
});

////////////////////////////////////////////////////////////
// What a growing alias map costs — declared last, and in ascending order.
//
// `restore()` answers a cached key from a map, but on a miss it walks the whole
// of `originalByShort` doing `split().join()` per entry, and repeats until a
// pass changes nothing.  So the cost of a key the plugin has never seen is
// proportional to every long alias the process has ever shortened, and neither
// map is ever evicted from.  These entries make that visible by growing the map
// between them; the alias counts are the pool's contribution, on top of the
// handful this file's own fixtures registered.
//
// Every entry builds its one-row result the same way, so the row construction
// inside the measured call is common to all of them and cancels out.
////////////////////////////////////////////////////////////

/** Declares one point on the curve, registering its aliases on its first call. */
function benchNovelKey(name: string, aliases: number, result = novelKeyResult) {
	let checked = false;
	return benchAsync(regexSafe(name), async () => {
		ensureRegistered(aliases);
		const restored = await plugin.transformResult({ queryId: poolQueryId, result: result() });
		if (!checked) {
			checked = true;
			const [key] = Object.keys(restored.rows[0]!);
			assert.ok(key?.includes("$$"), key);
			assert.equal(key?.includes("~"), false, `a novel key was left shortened: ${key}`);
		}
		return restored;
	});
}

summary(() => {
	benchAsync(regexSafe("restore a cached key"), () =>
		plugin.transformResult({ queryId: poolQueryId, result: cachedKeyResult }),
	).baseline(true);
	benchNovelKey("restore a novel key, 100 aliases registered", 100);
	benchNovelKey("restore a novel key, 1k aliases registered", 1_000);
	benchNovelKey("restore a novel key, 10k aliases registered", 10_000);
	// Same map, but nothing to substitute, so the fixed point is reached after one
	// pass instead of two: the difference is what the repeat costs.
	benchNovelKey("restore a novel inert key, 10k aliases registered", 10_000, inertKeyResult);
});

await runSuite("plugins");
