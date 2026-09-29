/**
 * `fixLongAliases()` from `src/fix-long-aliases.ts`.
 *
 * The plugin's `originalByShort`, `restoredByKey` and `queriesToRestore` are
 * module-level and never evicted, so every benchmark here shares them.  Only
 * the scaling group, declared last, depends on the map's size: everything
 * before it is O(1) in it (`restoreRow` hits `restoredByKey`, `shorten` does a
 * `Map.get`), so nothing else is measured against a map it inflated.
 */
import assert from "node:assert/strict";

import * as k from "kysely";
import { summary } from "mitata";

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { emptyDb } from "./lib/db.ts";
import { benchAsync, benchSync, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";
import { makeRows, times } from "./lib/rows.ts";

const fits = (identifier: string) => Buffer.byteLength(identifier) <= MAX_IDENTIFIER_BYTES;

/** Every identifier in a node tree. */
function identifiersIn(node: unknown, found: string[] = []): string[] {
	if (Array.isArray(node)) {
		for (const item of node) identifiersIn(item, found);
	} else if (typeof node === "object" && node !== null && "kind" in node) {
		if (k.IdentifierNode.is(node as k.OperationNode)) found.push((node as k.IdentifierNode).name);
		else for (const value of Object.values(node)) identifiersIn(value, found);
	}
	return found;
}

/** A plugin only ever sees root nodes; everything here is a select. */
function rootNode(query: { toOperationNode: () => k.OperationNode }): k.RootOperationNode {
	const node = query.toOperationNode();
	assert.ok(k.SelectQueryNode.is(node));
	return node;
}

/** The output aliases of a {@link selectAliases} node. */
function outputAliases(node: k.RootOperationNode): string[] {
	assert.ok(k.SelectQueryNode.is(node));
	return (node.selections ?? []).map(({ selection }) => {
		const { alias } = selection as k.AliasNode;
		assert.ok(k.IdentifierNode.is(alias));
		return alias.name;
	});
}

const db = emptyDb();

/** `SELECT 1 AS "<alias>", ...`: the only way to hand the plugin aliases it has never seen. */
const selectAliases = (aliases: readonly string[]) =>
	rootNode(db.selectNoFrom(aliases.map((alias) => k.sql.lit(1).as(alias))));

/**
 * Shortens `aliases` into the module-level map and marks `queryId` for
 * restoring, through `transformQuery`, the only entry point.
 */
const registrar = fixLongAliases();
const registerAliases = (aliases: readonly string[], queryId = k.createQueryId()) =>
	outputAliases(registrar.transformQuery({ queryId, node: selectAliases(aliases) }));

const plugin = fixLongAliases();
const camelPlugin = fixLongAliases(new k.CamelCasePlugin());
const queryId = k.createQueryId();

/**
 * users -> posts -> comments, with the joins named as given.  Every column is
 * prefixed with each collection above it, so long names push the deepest
 * columns past 63 bytes; short ones leave every identifier inside it.  Typed
 * as the short names, since generic join names defeat the ref types.
 */
function joinsNamed(postsName: string, commentsName: string) {
	const [posts, comments] = [postsName as "posts", commentsName as "comments"];
	const q = queries(db);
	const query = q.users
		.leftJoinMany(
			posts,
			q.posts.leftJoinMany(comments, q.comments, `${comments}.post_id`, "post.id"),
			`${posts}.user_id`,
			"user.id",
		)
		.orderBy("id")
		.limit(500);
	return { node: rootNode(query), prefix: `${posts}$$${comments}$$` };
}

const shortJoins = joinsNamed("posts", "comments");
const longJoins = joinsNamed("postsAuthoredByEachRegisteredUser", "commentsLeftOnEachOfThosePosts");

// Two levels of long prefixes put every result column over the limit.
const rowAliases = Object.keys(makeRows(1, 1, 1)[0]!).map((column) => longJoins.prefix + column);
const restoreQueryId = k.createQueryId();
const rowShorts = registerAliases(rowAliases, restoreQueryId);

/** `makeRows(users, 5, 4)`, keyed by `keys` in column order. */
const keyedRows = (users: number, keys: string[]) =>
	makeRows(users, 5, 4).map((row) =>
		Object.fromEntries(Object.values(row).map((value, i) => [keys[i]!, value])),
	);

// A query id never passed through `transformQuery` gets its rows back untouched.
const resultWorkloads = (
	[
		["100 rows", 5, restoreQueryId],
		["1k rows", 50, restoreQueryId],
		["10k rows", 500, restoreQueryId],
		["10k rows, nothing to restore", 500, k.createQueryId()],
	] as const
).map(([label, users, id]) => {
	const result = { rows: keyedRows(users, rowShorts) };
	return {
		name: `transformResult ${label}`,
		run: () => plugin.transformResult({ queryId: id, result }),
		expected: id === restoreQueryId ? keyedRows(users, rowAliases) : result.rows,
	};
});

// `scan` exists so queries without a long identifier skip `transformNode`'s
// deep clone.  CamelCasePlugin, the documented setup, deep-clones in its own right.
const queryWorkloads = [
	["transformQuery, long identifiers", plugin, longJoins.node],
	["transformQuery, no long identifiers", plugin, shortJoins.node],
	["transformQuery, long identifiers, wrapping CamelCasePlugin", camelPlugin, longJoins.node],
] as const;

// Aliases of a deep join's shape.  The shared prefix means shortened forms
// differ only in the trailing hash, making `restore()` compare the most
// characters before rejecting an entry.
const pool = times(
	10_000,
	(i) =>
		`organizationalDepartments$$departmentalEmployeeRecords$$employee_preferred_full_display_name_${i}`,
);
const HASHED_ALIASES = 100;
const hashedNode = selectAliases(pool.slice(0, HASHED_ALIASES));
const poolQueryId = k.createQueryId();
const hashedShorts = registerAliases(pool.slice(0, HASHED_ALIASES), poolQueryId);

/**
 * `hash()` is private and memoized per plugin, so it is measured by
 * difference: a fresh plugin has to hash all 100 aliases, a warm one none.
 */
const warmPlugin = fixLongAliases();
warmPlugin.transformQuery({ queryId, node: hashedNode });

// Restored once by verification and cached from then on: the scaling control.
const cachedKeyResult = { rows: [{ [`enclosingQueryCached$$${hashedShorts[0]}`]: 1 }] };

async function verifyWorkloads(): Promise<void> {
	assert.ok(!rowAliases.some(fits) && rowShorts.every(fits));
	for (const { name, run, expected } of resultWorkloads) {
		assert.deepEqual((await run()).rows, expected, name);
	}

	assert.ok(identifiersIn(shortJoins.node).every(fits));
	assert.ok(!identifiersIn(longJoins.node).every(fits));
	for (const [name, p, node] of queryWorkloads) {
		const out = p.transformQuery({ queryId, node });
		assert.ok(identifiersIn(out).every(fits), name);
		// Nothing to shorten must mean no clone.
		assert.equal(out === node, node === shortJoins.node, name);
	}

	for (const p of [warmPlugin, fixLongAliases()]) {
		assert.deepEqual(outputAliases(p.transformQuery({ queryId, node: hashedNode })), hashedShorts);
	}

	const control = await plugin.transformResult({ queryId: poolQueryId, result: cachedKeyResult });
	assert.deepEqual(Object.keys(control.rows[0]!), [`enclosingQueryCached$$${pool[0]}`]);
}

summary(() => {
	resultWorkloads.forEach(({ name, run }, i) => benchAsync(name, run, { baseline: i === 0 }));
});

summary(() => {
	queryWorkloads.forEach(([name, p, node], i) =>
		benchSync(name, () => p.transformQuery({ queryId, node }), { baseline: i === 0 }),
	);
});

summary(() => {
	benchSync(
		"transformQuery 100 long aliases, names cached",
		() => warmPlugin.transformQuery({ queryId, node: hashedNode }),
		{ baseline: true },
	);
	benchSync("transformQuery 100 long aliases, names hashed", () =>
		fixLongAliases().transformQuery({ queryId, node: hashedNode }),
	);
	benchSync("fixLongAliases construction", () => fixLongAliases());
});

// On a miss, `restore()` walks all of `originalByShort` per pass, repeating
// until a pass changes nothing, so a novel key costs time proportional to every
// alias the process has ever shortened.  The map only grows, so these must run
// in ascending order, each growing it on its first call; verifying them up
// front would register the largest map before the smallest measurement.  So
// each checks its own first result instead, which mitata discards as warmup.
let registered = HASHED_ALIASES;
let novelKeys = 0;

/** `suffix` is the key's already-shortened tail, or one that matches nothing. */
function benchNovelKey(name: string, aliases: number, suffix = hashedShorts[0]!) {
	let checked = false;
	benchAsync(name, async () => {
		if (aliases > registered) {
			registerAliases(pool.slice(registered, aliases), poolQueryId);
			registered = aliases;
		}
		const result = { rows: [{ [`enclosingQuery${novelKeys++}$$${suffix}`]: 1 }] };
		const restored = await plugin.transformResult({ queryId: poolQueryId, result });
		if (!checked) {
			checked = true;
			const [key] = Object.keys(restored.rows[0]!);
			assert.ok(
				key?.includes("$$") && !key.includes("~"),
				`a novel key was left shortened: ${key}`,
			);
		}
		return restored;
	});
}

summary(() => {
	benchAsync(
		"restore a cached key",
		() => plugin.transformResult({ queryId: poolQueryId, result: cachedKeyResult }),
		{ baseline: true },
	);
	for (const [label, aliases] of [
		["100", 100],
		["1k", 1_000],
		["10k", 10_000],
	] as const) {
		benchNovelKey(`restore a novel key, ${label} aliases registered`, aliases);
	}
	// Nothing to substitute, so one pass settles it: the gap is what the repeat costs.
	benchNovelKey("restore a novel inert key, 10k aliases registered", 10_000, "unshortened_column");
});

await runSuite({ verify: verifyWorkloads });
