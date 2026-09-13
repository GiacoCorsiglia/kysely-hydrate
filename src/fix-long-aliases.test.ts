import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin, type Compilable, type Kysely, type KyselyPlugin, sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { fixLongAliases } from "./fix-long-aliases.ts";

const rawDb = getDbForTest();
const db = rawDb.withPlugin(fixLongAliases());

const bytes = (s: string) => Buffer.byteLength(s);

/** `SELECT <value> AS "<alias>", ...` with no FROM clause. */
const selectLiterals = (db: Kysely<any>, entries: Record<string, number | string>) =>
	db.selectNoFrom(Object.entries(entries).map(([alias, value]) => sql.lit(value).as(alias)));

/** Output aliases in the compiled SQL, in order. */
const aliasesIn = (query: Compilable) =>
	[...query.compile().sql.matchAll(/ as "([^"]+)"/g)].map((m) => m[1]!);

const ALIAS_63 = "departmentalEmployeeRecords$$employee_preferred_full_display_na";
const ALIAS_64 = "departmentalEmployeeRoster$$employee_preferred_full_display_name";
const ALIAS_93 =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_preferred_full_display_name";
const ALIAS_97 =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_secondary_contact_email_address";

describe("fix-long-aliases", () => {
	assert.strictEqual(bytes(ALIAS_63), 63);
	assert.strictEqual(bytes(ALIAS_64), 64);

	test("leaves a query with no long aliases unchanged", () => {
		const entries = { [ALIAS_63]: 1, short: 2 };

		assert.strictEqual(
			selectLiterals(db, entries).compile().sql,
			selectLiterals(rawDb, entries).compile().sql,
		);
	});

	test("shortens a 64-byte alias, keeping its start", () => {
		const [alias] = aliasesIn(selectLiterals(db, { [ALIAS_64]: 1 }));

		assert.ok(alias);
		assert.ok(bytes(alias) <= 63, alias);
		assert.ok(alias.startsWith("departmentalEmployeeRoster$$employee_"), alias);
	});

	test("restores the original alias in result rows", async () => {
		const row = await selectLiterals(db, { [ALIAS_64]: 1, short: 2 }).executeTakeFirstOrThrow();

		assert.deepStrictEqual(row, { [ALIAS_64]: 1, short: 2 });
	});

	test("is deterministic across plugin instances", () => {
		const other = rawDb.withPlugin(fixLongAliases());

		assert.strictEqual(
			selectLiterals(db, { [ALIAS_64]: 1 }).compile().sql,
			selectLiterals(other, { [ALIAS_64]: 1 }).compile().sql,
		);
	});

	test("keeps aliases that share their first 63 bytes distinct", async () => {
		const query = selectLiterals(db, { [ALIAS_93]: "name", [ALIAS_97]: "email" });

		const [first, second] = aliasesIn(query);
		assert.notStrictEqual(first, second);
		assert.deepStrictEqual(await query.executeTakeFirstOrThrow(), {
			[ALIAS_93]: "name",
			[ALIAS_97]: "email",
		});
	});

	test("never splits a multi-byte character", async () => {
		const alias = "ü".repeat(40) + "$$x"; // 83 bytes
		const query = selectLiterals(db, { [alias]: 1 });

		const [shortAlias] = aliasesIn(query);
		assert.ok(shortAlias);
		assert.match(shortAlias, /^ü+~[a-z]{14}$/);
		assert.ok(bytes(shortAlias) <= 63);
		assert.deepStrictEqual(await query.executeTakeFirstOrThrow(), { [alias]: 1 });
	});

	test("restores rows from db.executeQuery()", async () => {
		const { rows } = await db.executeQuery(selectLiterals(db, { [ALIAS_64]: 1 }).compile());

		assert.deepStrictEqual(rows, [{ [ALIAS_64]: 1 }]);
	});

	test("rewrites references to a shortened alias in an enclosing query", async () => {
		const inner = selectLiterals(db, { [ALIAS_64]: 1, [ALIAS_93]: 2 });
		const outer = db
			.selectFrom(inner.as("sub"))
			.select([sql.ref(`sub.${ALIAS_64}`).as("a"), sql.ref(`sub.${ALIAS_93}`).as("b")])
			.orderBy(sql.ref(`sub.${ALIAS_64}`));

		assert.deepStrictEqual(await outer.executeTakeFirstOrThrow(), { a: 1, b: 2 });
	});

	test("restores an alias built from an already-shortened one", async () => {
		// Kysely compiles an embedded subquery on its own, so an outer query that
		// reads the inner query's aliases (as query sets do) sees shortened names.
		const inner = selectLiterals(db, { [ALIAS_64]: 1 });
		const [innerAlias] = aliasesIn(inner);
		assert.ok(innerAlias?.includes("~"));

		for (const prefix of ["short", "organizationalDepartmentsOfTheOrganization"]) {
			const outer = db
				.selectFrom(inner.as("sub"))
				.select(sql.ref(`sub.${innerAlias}`).as(`${prefix}$$${innerAlias}`));

			assert.ok(bytes(aliasesIn(outer).at(-1)!) <= 63);
			assert.deepStrictEqual(await outer.executeTakeFirstOrThrow(), {
				[`${prefix}$$${ALIAS_64}`]: 1,
			});
		}
	});

	describe("wrapping CamelCasePlugin", () => {
		const camelDb = rawDb.withPlugin(fixLongAliases(new CamelCasePlugin()));

		// 58 bytes as written, 64 once snake_cased.
		const CAMEL_58 = "employeeDirectoryEntries$$employeePreferredFullDisplayName";
		const SNAKE_64 = "employee_directory_entries$$employee_preferred_full_display_name";

		test("measures the snake_cased alias", () => {
			assert.strictEqual(bytes(CAMEL_58), 58);
			assert.strictEqual(bytes(SNAKE_64), 64);

			const [alias] = aliasesIn(selectLiterals(camelDb, { [CAMEL_58]: 1 }));

			assert.ok(alias);
			assert.ok(bytes(alias) <= 63);
			assert.ok(alias.startsWith("employee_directory_entries$$"), alias);
		});

		test("returns camelCase keys", async () => {
			const row = await selectLiterals(camelDb, {
				[CAMEL_58]: 1,
				createdAt: 2,
			}).executeTakeFirstOrThrow();

			assert.deepStrictEqual(row, { [CAMEL_58]: 1, createdAt: 2 });
		});
	});

	test("runs the wrapped plugin first on queries and last on results", async () => {
		const seen: string[] = [];
		const spy: KyselyPlugin = {
			transformQuery({ node }) {
				seen.push("query");
				return node;
			},
			async transformResult({ result }) {
				seen.push("result");
				return result;
			},
		};

		await selectLiterals(rawDb.withPlugin(fixLongAliases(spy)), {
			[ALIAS_64]: 1,
		}).executeTakeFirstOrThrow();

		assert.deepStrictEqual(seen, ["query", "result"]);
	});
});
