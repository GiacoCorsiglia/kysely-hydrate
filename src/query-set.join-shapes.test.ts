import assert from "node:assert";
import { describe, test } from "node:test";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

//
// Join Shapes (oracle matrix)
//
// Every combination of inner/left joins over several 3–4 level join trees,
// checked against an in-memory oracle that evaluates each nested query set
// as an isolated unit: a child collection's own joins filter only the
// child's rows (the semantics of `A ⟕ (B ⟕ C)`, not `(A ⟕ B) ⟕ C`).
//
// Each tree is run twice: once with nested base aliases equal to their join
// keys, and once with base aliases that differ from the keys (so any
// rewriting of ON-clause qualifiers must map the alias to the key).
//
// The fixture has natural gaps at every level (alice has no posts, several
// posts have no comments, most comments have no replies); the trees also
// exclude rows at each level to create more.
//

type TableName = "users" | "posts" | "comments" | "replies" | "profiles";
type Row = Record<string, unknown>;
type Tables = Record<TableName, Row[]>;

interface Shape {
	readonly table: TableName;
	readonly alias: string;
	readonly columns: readonly string[];
	/** Rows of the base query whose `column` is in `values` are excluded (in SQL and the oracle). */
	readonly exclude?: { readonly column: string; readonly values: readonly number[] };
	readonly joins?: readonly Edge[];
}

interface Edge {
	readonly key: string;
	readonly type: "inner" | "left";
	readonly mode: "one" | "many";
	readonly child: Shape;
	/** `${key}.${childColumn} = ${parentAlias}.${parentColumn}` */
	readonly on: readonly [childColumn: string, parentColumn: string];
}

//
// Query set construction.
//

function build(shape: Shape): any {
	let base: any = db.selectFrom(shape.table).select(shape.columns as any);
	if (shape.exclude) {
		base = base.where(shape.exclude.column, "not in", shape.exclude.values);
	}
	let qs: any = querySet(db).selectAs(shape.alias, base);
	for (const edge of shape.joins ?? []) {
		const method = `${edge.type}Join${edge.mode === "one" ? "One" : "Many"}`;
		qs = qs[method](
			edge.key,
			build(edge.child),
			`${edge.key}.${edge.on[0]}`,
			`${shape.alias}.${edge.on[1]}`,
		);
	}
	return qs;
}

//
// Oracle.
//

let tablesPromise: Promise<Tables> | undefined;
function loadTables(): Promise<Tables> {
	tablesPromise ??= (async () => {
		const tables = {} as Tables;
		for (const table of ["users", "posts", "comments", "replies", "profiles"] as const) {
			tables[table] = await db.selectFrom(table).selectAll().orderBy("id").execute();
		}
		return tables;
	})();
	return tablesPromise;
}

const byId = (a: Row, b: Row) => (a.id as number) - (b.id as number);

function baseRows(shape: Shape, tables: Tables): Row[] {
	return tables[shape.table]
		.filter((row) => !shape.exclude?.values.includes(row[shape.exclude.column] as number))
		.map((row) => Object.fromEntries(shape.columns.map((column) => [column, row[column]])));
}

/** The hydrated output of `shape`, evaluated with derived-table semantics. */
function expectedHydrated(shape: Shape, tables: Tables): Row[] {
	const out: Row[] = [];
	for (const row of baseRows(shape, tables)) {
		const entity: Row = { ...row };
		let keep = true;
		for (const edge of shape.joins ?? []) {
			const children = expectedHydrated(edge.child, tables).filter(
				(child) => child[edge.on[0]] === row[edge.on[1]],
			);
			if (edge.type === "inner" && children.length === 0) {
				keep = false;
				break;
			}
			if (edge.mode === "one") {
				assert.ok(children.length <= 1, `oracle: ${edge.key} matched more than one row`);
				entity[edge.key] = children[0] ?? null;
			} else {
				entity[edge.key] = children;
			}
		}
		if (keep) {
			out.push(entity);
		}
	}
	return out.sort(byId);
}

/** Every column of `toJoinedQuery()`, prefixed as it appears in the flat rows. */
function flatColumns(shape: Shape, prefix = ""): string[] {
	return [
		...shape.columns.map((column) => prefix + column),
		...(shape.joins ?? []).flatMap((edge) => flatColumns(edge.child, `${prefix}${edge.key}$$`)),
	];
}

function prefixRow(row: Row, prefix: string): Row {
	return Object.fromEntries(Object.entries(row).map(([k, v]) => [prefix + k, v]));
}

/** The flat rows of `toJoinedQuery()`, evaluated with derived-table semantics. */
function expectedFlat(shape: Shape, tables: Tables): Row[] {
	const out: Row[] = [];
	for (const row of baseRows(shape, tables)) {
		let partials: Row[] = [row];
		for (const edge of shape.joins ?? []) {
			const matches = expectedFlat(edge.child, tables).filter(
				(child) => child[edge.on[0]] === row[edge.on[1]],
			);
			let childRows = matches.map((child) => prefixRow(child, `${edge.key}$$`));
			if (childRows.length === 0) {
				if (edge.type === "inner") {
					partials = [];
					break;
				}
				childRows = [
					Object.fromEntries(flatColumns(edge.child, `${edge.key}$$`).map((c) => [c, null])),
				];
			}
			partials = partials.flatMap((partial) =>
				childRows.map((child) => ({ ...partial, ...child })),
			);
		}
		out.push(...partials);
	}
	return out;
}

/** Order-insensitive comparison of row multisets. */
function sortRows(rows: readonly Row[]): string[] {
	return rows
		.map((row) =>
			JSON.stringify(
				Object.fromEntries(Object.entries(row).sort(([a], [b]) => a.localeCompare(b))),
			),
		)
		.sort();
}

//
// Trees.
//

type JoinType = Edge["type"];
const JOIN_TYPES: readonly JoinType[] = ["inner", "left"];

function combinations(n: number): JoinType[][] {
	if (n === 0) {
		return [[]];
	}
	return combinations(n - 1).flatMap((rest) => JOIN_TYPES.map((type) => [type, ...rest]));
}

/** Nested base aliases: equal to the join key, or different from it. */
type AliasStyle = "alias = key" | "alias ≠ key";
const ALIAS_STYLES: readonly AliasStyle[] = ["alias = key", "alias ≠ key"];

interface Tree {
	readonly name: string;
	readonly depth: number;
	readonly shape: (
		types: readonly JoinType[],
		alias: (key: string, other: string) => string,
	) => Shape;
}

const TREES: readonly Tree[] = [
	{
		// users → posts → comments → replies (many-joins at every level)
		name: "user → posts[] → comments[] → replies[]",
		depth: 3,
		shape: ([t1, t2, t3], alias) => ({
			table: "users",
			alias: "user",
			columns: ["id", "username"],
			exclude: { column: "id", values: [4] },
			joins: [
				{
					key: "posts",
					type: t1!,
					mode: "many",
					on: ["user_id", "id"],
					child: {
						table: "posts",
						alias: alias("posts", "p"),
						columns: ["id", "user_id", "title"],
						exclude: { column: "id", values: [5] },
						joins: [
							{
								key: "comments",
								type: t2!,
								mode: "many",
								on: ["post_id", "id"],
								child: {
									table: "comments",
									alias: alias("comments", "c"),
									columns: ["id", "post_id", "content"],
									exclude: { column: "id", values: [4] },
									joins: [
										{
											key: "replies",
											type: t3!,
											mode: "many",
											on: ["comment_id", "id"],
											child: {
												table: "replies",
												alias: alias("replies", "r"),
												columns: ["id", "comment_id", "content"],
											},
										},
									],
								},
							},
						],
					},
				},
			],
		}),
	},
	{
		// posts → comments → author (one) → profile (one)
		name: "post → comments[] → author → profile",
		depth: 3,
		shape: ([t1, t2, t3], alias) => ({
			table: "posts",
			alias: "post",
			columns: ["id", "user_id", "title"],
			joins: [
				{
					key: "comments",
					type: t1!,
					mode: "many",
					on: ["post_id", "id"],
					child: {
						table: "comments",
						alias: alias("comments", "c"),
						columns: ["id", "post_id", "user_id", "content"],
						joins: [
							{
								key: "author",
								type: t2!,
								mode: "one",
								on: ["id", "user_id"],
								child: {
									table: "users",
									alias: alias("author", "u"),
									columns: ["id", "username"],
									exclude: { column: "id", values: [3, 5] },
									joins: [
										{
											key: "profile",
											type: t3!,
											mode: "one",
											on: ["user_id", "id"],
											child: {
												table: "profiles",
												alias: alias("profile", "pr"),
												columns: ["id", "user_id", "bio"],
												exclude: { column: "user_id", values: [2, 7] },
											},
										},
									],
								},
							},
						],
					},
				},
			],
		}),
	},
	{
		// users → profile (one) → posts (many) → comments (many): a one-join that nests many-joins
		name: "user → profile → posts[] → comments[]",
		depth: 3,
		shape: ([t1, t2, t3], alias) => ({
			table: "users",
			alias: "user",
			columns: ["id", "username"],
			joins: [
				{
					key: "profile",
					type: t1!,
					mode: "one",
					on: ["user_id", "id"],
					child: {
						table: "profiles",
						alias: alias("profile", "pr"),
						columns: ["id", "user_id", "bio"],
						exclude: { column: "user_id", values: [3] },
						joins: [
							{
								key: "posts",
								type: t2!,
								mode: "many",
								on: ["user_id", "user_id"],
								child: {
									table: "posts",
									alias: alias("posts", "p"),
									columns: ["id", "user_id", "title"],
									exclude: { column: "id", values: [12] },
									joins: [
										{
											key: "comments",
											type: t3!,
											mode: "many",
											on: ["post_id", "id"],
											child: {
												table: "comments",
												alias: alias("comments", "c"),
												columns: ["id", "post_id", "content"],
											},
										},
									],
								},
							},
						],
					},
				},
			],
		}),
	},
	{
		// Siblings at two levels: user → { profile, posts[] → { comments[], author } }
		name: "user → { profile, posts[] → { comments[], author } }",
		depth: 4,
		shape: ([t1, t2, t3, t4], alias) => ({
			table: "users",
			alias: "user",
			columns: ["id", "username"],
			joins: [
				{
					key: "profile",
					type: t1!,
					mode: "one",
					on: ["user_id", "id"],
					child: {
						table: "profiles",
						alias: alias("profile", "pr"),
						columns: ["id", "user_id", "bio"],
						exclude: { column: "user_id", values: [2] },
					},
				},
				{
					key: "posts",
					type: t2!,
					mode: "many",
					on: ["user_id", "id"],
					child: {
						table: "posts",
						alias: alias("posts", "p"),
						columns: ["id", "user_id", "title"],
						joins: [
							{
								key: "comments",
								type: t3!,
								mode: "many",
								on: ["post_id", "id"],
								child: {
									table: "comments",
									alias: alias("comments", "c"),
									columns: ["id", "post_id", "content"],
									exclude: { column: "id", values: [1, 2] },
								},
							},
							{
								key: "author",
								type: t4!,
								mode: "one",
								on: ["id", "user_id"],
								child: {
									table: "users",
									alias: alias("author", "u"),
									columns: ["id", "username"],
									exclude: { column: "id", values: [3] },
								},
							},
						],
					},
				},
			],
		}),
	},
];

describe("query-set: join shapes", () => {
	for (const tree of TREES) {
		for (const types of combinations(tree.depth)) {
			for (const aliasStyle of ALIAS_STYLES) {
				const alias = (key: string, other: string) => (aliasStyle === "alias = key" ? key : other);
				const shape = tree.shape(types, alias);
				const name = `${tree.name} [${types.join(", ")}] (${aliasStyle})`;

				test(`${name}: execute and pagination match the oracle`, async () => {
					const tables = await loadTables();
					const expected = expectedHydrated(shape, tables);
					const qs = build(shape);

					assert.deepStrictEqual(await qs.execute(), expected);
					assert.deepStrictEqual(await qs.limit(3).offset(1).execute(), expected.slice(1, 4));
					assert.deepStrictEqual(
						await qs.orderBy("id", "desc").limit(2).execute(),
						expected.toReversed().slice(0, 2),
					);
				});

				test(`${name}: count and exists match the oracle`, async () => {
					const tables = await loadTables();
					const expected = expectedHydrated(shape, tables);
					const qs = build(shape);

					assert.strictEqual(await qs.executeCount(Number), expected.length);
					assert.strictEqual(await qs.limit(1).executeCount(Number), expected.length);
					assert.strictEqual(await qs.executeExists(), expected.length > 0);
				});

				test(`${name}: toJoinedQuery rows match the oracle`, async () => {
					const tables = await loadTables();
					const rows = await build(shape).toJoinedQuery().execute();

					assert.deepStrictEqual(sortRows(rows), sortRows(expectedFlat(shape, tables)));
				});
			}
		}
	}
});
