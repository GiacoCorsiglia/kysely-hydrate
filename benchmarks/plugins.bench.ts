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

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { emptyDb } from "./lib/db.ts";
import { group, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";
import { makeRows, range } from "./lib/rows.ts";

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
function rootNode(query: { toOperationNode: () => k.OperationNode }): k.SelectQueryNode {
	const node = query.toOperationNode();
	assert.ok(k.SelectQueryNode.is(node));
	return node;
}

/** The output aliases of a {@link selectAliases} node. */
const outputAliases = (node: k.RootOperationNode) =>
	(node as k.SelectQueryNode).selections!.map(
		({ selection }) => ((selection as k.AliasNode).alias as k.IdentifierNode).name,
	);

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
 * users -> posts -> comments -> author, with the joins named as given.  Every
 * column is prefixed with each collection above it, so long names push the
 * deepest columns past 63 bytes; short ones leave every identifier inside it.
 * Typed as the short names, since generic join names defeat the ref types.
 */
function joinsNamed(...names: [posts: string, comments: string, author: string]) {
	const [posts, comments, author] = names as ["posts", "comments", "author"];
	const q = queries(db);
	const commentsWithAuthor = q.comments.leftJoinOne(
		author,
		q.users,
		`${author}.id`,
		"comment.user_id",
	);
	return rootNode(
		q.users
			.leftJoinMany(
				posts,
				q.posts.leftJoinMany(comments, commentsWithAuthor, `${comments}.post_id`, "post.id"),
				`${posts}.user_id`,
				"user.id",
			)
			.orderBy("id")
			.limit(500),
	);
}

const shortJoins = joinsNamed("posts", "comments", "author");
const longJoins = joinsNamed(
	"postsAuthoredByEachRegisteredUser",
	"commentsLeftOnEachOfThosePosts",
	"theAuthorOfEachOfThoseComments",
);
assert.ok(identifiersIn(shortJoins).every(fits));
assert.ok(!identifiersIn(longJoins).every(fits));

// Two levels of long prefixes put every result column over the limit.
const rowAliases = Object.keys(makeRows(1, 1, 1)[0]!).map(
	(column) => `postsAuthoredByEachRegisteredUser$$commentsLeftOnEachOfThosePosts$$${column}`,
);
const restoreQueryId = k.createQueryId();
const rowShorts = registerAliases(rowAliases, restoreQueryId);
assert.ok(!rowAliases.some(fits) && rowShorts.every(fits));

/** `makeRows(users, 5, 4)`, keyed by `keys` in column order. */
const keyedRows = (users: number, keys: string[]) =>
	makeRows(users, 5, 4).map((row) =>
		Object.fromEntries(Object.values(row).map((value, i) => [keys[i]!, value])),
	);

const transformResult = (users: number, id: k.QueryId) => {
	const result = { rows: keyedRows(users, rowShorts) };
	return {
		run: () => plugin.transformResult({ queryId: id, result }),
		// A query id never passed through `transformQuery` gets its rows back untouched.
		expected: id === restoreQueryId ? { rows: keyedRows(users, rowAliases) } : result,
	};
};

group({
	"transformResult 100 rows": transformResult(5, restoreQueryId),
	"transformResult 1k rows": transformResult(50, restoreQueryId),
	"transformResult 10k rows": transformResult(500, restoreQueryId),
	"transformResult 10k rows, nothing to restore": transformResult(500, k.createQueryId()),
});

// `scan` exists so queries without a long identifier skip `transformNode`'s
// deep clone.  CamelCasePlugin, the documented setup, deep-clones in its own right.
const transformQuery = (p: k.KyselyPlugin, node: k.RootOperationNode) => ({
	run: () => p.transformQuery({ queryId, node }),
	check: (out: k.RootOperationNode) => {
		const identifiers = identifiersIn(out);
		assert.equal(identifiers.length, identifiersIn(node).length);
		assert.ok(identifiers.every(fits));
		// Nothing to shorten must mean no clone.
		assert.equal(out === node, node === shortJoins);
	},
});

group({
	"transformQuery, long identifiers": transformQuery(plugin, longJoins),
	"transformQuery, no long identifiers": transformQuery(plugin, shortJoins),
	"transformQuery, long identifiers, wrapping CamelCasePlugin": transformQuery(
		camelPlugin,
		longJoins,
	),
});

// Aliases of a deep join's shape.  The shared prefix means shortened forms
// differ only in the trailing hash, making `restore()` compare the most
// characters before rejecting an entry.
const pool = range(10_000, 1).map(
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
const hashes = (p: () => k.KyselyPlugin) => ({
	run: () => p().transformQuery({ queryId, node: hashedNode }),
	check: (out: k.RootOperationNode) => assert.deepEqual(outputAliases(out), hashedShorts),
});

group({
	"transformQuery 100 long aliases, names cached": hashes(() => warmPlugin),
	"transformQuery 100 long aliases, names hashed": hashes(() => fixLongAliases()),
	"fixLongAliases construction": { run: () => fixLongAliases() },
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
function novelKey(aliases: number, suffix = hashedShorts[0]!) {
	let checked = false;
	return {
		run: async () => {
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
		},
	};
}

// Restored once by verification and cached from then on: the scaling control.
const cachedKeyResult = { rows: [{ [`enclosingQueryCached$$${hashedShorts[0]}`]: 1 }] };

group({
	"restore a cached key": {
		run: () => plugin.transformResult({ queryId: poolQueryId, result: cachedKeyResult }),
		check: ({ rows }) => assert.deepEqual(rows, [{ [`enclosingQueryCached$$${pool[0]}`]: 1 }]),
	},
	"restore a novel key, 100 aliases registered": novelKey(100),
	"restore a novel key, 1k aliases registered": novelKey(1_000),
	"restore a novel key, 10k aliases registered": novelKey(10_000),
	// Nothing to substitute, so one pass settles it: the gap is what the repeat costs.
	"restore a novel inert key, 10k aliases registered": novelKey(10_000, "unshortened_column"),
});

await runSuite();
