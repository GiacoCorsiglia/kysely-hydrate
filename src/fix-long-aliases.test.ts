/**
 * Tests for the `fixLongAliases()` Kysely plugin on its own, without query
 * sets. Runs against both SQLite and Postgres: the shortening happens in the
 * plugin, so the SQL shape and the restored keys are the same on any dialect.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin, type Kysely, type KyselyPlugin, sql } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { fixLongAliases } from "./fix-long-aliases.ts";

const rawDb = getDbForTest();

const utf8Bytes = (s: string) => Buffer.byteLength(s, "utf8");

/** Extracts the output aliases from compiled SQL, in order. */
function outputAliases(sqlText: string): string[] {
	return [...sqlText.matchAll(/ as "([^"]+)"/g)].map((m) => m[1]!);
}

/** `SELECT <value> AS "<alias>"` with no FROM clause, for every alias given. */
function selectLiterals(db: Kysely<any>, entries: Record<string, number | string>) {
	return db.selectNoFrom(Object.entries(entries).map(([alias, value]) => sql.lit(value).as(alias)));
}

const ALIAS_63 = "departmentalEmployeeRecords$$employee_preferred_full_display_na";
const ALIAS_64 = "departmentalEmployeeRoster$$employee_preferred_full_display_name";
const ALIAS_97 =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_secondary_contact_email_address";
const ALIAS_93 =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_preferred_full_display_name";

describe("fix-long-aliases", () => {
	assert.strictEqual(utf8Bytes(ALIAS_63), 63);
	assert.strictEqual(utf8Bytes(ALIAS_64), 64);

	const db = rawDb.withPlugin(fixLongAliases());

	test("leaves aliases of up to 63 bytes untouched, producing identical SQL", () => {
		const entries = { [ALIAS_63]: 1, short: 2 };

		assert.strictEqual(
			selectLiterals(db, entries).compile().sql,
			selectLiterals(rawDb, entries).compile().sql,
		);
	});

	test("shortens a 64-byte alias to at most 63 bytes, keeping a readable head", () => {
		const [alias] = outputAliases(selectLiterals(db, { [ALIAS_64]: 1 }).compile().sql);

		assert.ok(alias);
		assert.notStrictEqual(alias, ALIAS_64);
		assert.ok(utf8Bytes(alias) <= 63, `${alias} is ${utf8Bytes(alias)} bytes`);
		assert.ok(alias.startsWith("departmentalEmployeeRoster$$employee_"), alias);
	});

	test("restores the original alias in result rows", async () => {
		const row = await selectLiterals(db, { [ALIAS_64]: 1, short: 2 }).executeTakeFirstOrThrow();

		assert.deepStrictEqual(row, { [ALIAS_64]: 1, short: 2 });
	});

	test("shortening is deterministic across plugin instances", () => {
		const other = rawDb.withPlugin(fixLongAliases());

		assert.strictEqual(
			selectLiterals(db, { [ALIAS_64]: 1 }).compile().sql,
			selectLiterals(other, { [ALIAS_64]: 1 }).compile().sql,
		);
	});

	test("aliases sharing their first 63 bytes stay distinct", async () => {
		const query = selectLiterals(db, { [ALIAS_93]: "name", [ALIAS_97]: "email" });

		const [first, second] = outputAliases(query.compile().sql);
		assert.ok(first && second);
		assert.notStrictEqual(first, second);

		assert.deepStrictEqual(await query.executeTakeFirstOrThrow(), {
			[ALIAS_93]: "name",
			[ALIAS_97]: "email",
		});
	});

	test("truncates the head on a character boundary, never splitting a multi-byte character", async () => {
		// 40 two-byte characters: 80 bytes, 40 characters.
		const alias = "ü".repeat(40) + "$$x";
		const query = selectLiterals(db, { [alias]: 1 });

		const [shortAlias] = outputAliases(query.compile().sql);
		assert.ok(shortAlias);
		assert.match(shortAlias, /^ü+~[a-z]{14}$/);
		assert.ok(utf8Bytes(shortAlias) <= 63);

		assert.deepStrictEqual(await query.executeTakeFirstOrThrow(), { [alias]: 1 });
	});

	test("restores rows executed from a compiled query via db.executeQuery()", async () => {
		const compiled = selectLiterals(db, { [ALIAS_64]: 1 }).compile();

		const { rows } = await db.executeQuery(compiled);

		assert.deepStrictEqual(rows, [{ [ALIAS_64]: 1 }]);
	});

	test("rewrites references to a shortened alias in an enclosing query to match", async () => {
		const inner = selectLiterals(db, { [ALIAS_64]: 1, [ALIAS_93]: 2 });
		const outer = db
			.selectFrom(inner.as("sub"))
			.select([sql.ref(`sub.${ALIAS_64}`).as("a"), sql.ref(`sub.${ALIAS_93}`).as("b")])
			.orderBy(sql.ref(`sub.${ALIAS_64}`));

		assert.deepStrictEqual(await outer.executeTakeFirstOrThrow(), { a: 1, b: 2 });
	});

	test("an alias that embeds an already-shortened alias is shortened again and fully restored", async () => {
		// The inner query is compiled (and shortened) on its own when it is
		// embedded; the outer alias is then built from the shortened name.
		const inner = selectLiterals(db, { [ALIAS_64]: 1 });
		const [innerAlias] = outputAliases(inner.compile().sql);
		assert.ok(innerAlias && innerAlias.includes("~"));

		const outerAlias = `organizationalDepartments$$${ALIAS_64}`;
		const outer = db.selectFrom(inner.as("sub")).select(sql.ref(`sub.${ALIAS_64}`).as(outerAlias));

		const [compiledOuterAlias] = outputAliases(outer.compile().sql).slice(-1);
		assert.ok(compiledOuterAlias);
		assert.ok(utf8Bytes(compiledOuterAlias) <= 63);

		assert.deepStrictEqual(await outer.executeTakeFirstOrThrow(), { [outerAlias]: 1 });
	});

	test("maxBytes option lowers the limit", () => {
		const tight = rawDb.withPlugin(fixLongAliases({ maxBytes: 30 }));
		const alias = "a".repeat(31);

		const [shortAlias] = outputAliases(selectLiterals(tight, { [alias]: 1 }).compile().sql);

		assert.ok(shortAlias);
		assert.notStrictEqual(shortAlias, alias);
		assert.ok(utf8Bytes(shortAlias) <= 30);
	});

	test("instances with different maxBytes each keep their own limit for the same alias, and both restore", async () => {
		// One process may talk to two databases with different limits. The
		// shortened forms are shared between instances, so the second instance
		// to see an alias must not reuse the first instance's (longer) form.
		const alias = "q".repeat(70);
		const wide = rawDb.withPlugin(fixLongAliases({ maxBytes: 63 }));
		const narrow = rawDb.withPlugin(fixLongAliases({ maxBytes: 40 }));

		const [wideAlias] = outputAliases(selectLiterals(wide, { [alias]: 1 }).compile().sql);
		const [narrowAlias] = outputAliases(selectLiterals(narrow, { [alias]: 1 }).compile().sql);

		assert.ok(wideAlias && narrowAlias);
		assert.strictEqual(utf8Bytes(wideAlias), 63);
		assert.strictEqual(utf8Bytes(narrowAlias), 40);

		assert.deepStrictEqual(await selectLiterals(wide, { [alias]: 1 }).executeTakeFirstOrThrow(), {
			[alias]: 1,
		});
		assert.deepStrictEqual(await selectLiterals(narrow, { [alias]: 2 }).executeTakeFirstOrThrow(), {
			[alias]: 2,
		});
	});

	describe("wrapping CamelCasePlugin", () => {
		const camelDb = rawDb.withPlugin(fixLongAliases(new CamelCasePlugin()));

		// 58 bytes as written; 64 once snake_cased.
		const CAMEL_58 = "employeeDirectoryEntries$$employeePreferredFullDisplayName";
		const SNAKE_64 = "employee_directory_entries$$employee_preferred_full_display_name";

		test("measures the snake_cased alias, not the camelCase one", () => {
			assert.strictEqual(utf8Bytes(CAMEL_58), 58);
			assert.strictEqual(utf8Bytes(SNAKE_64), 64);

			const [alias] = outputAliases(selectLiterals(camelDb, { [CAMEL_58]: 1 }).compile().sql);

			assert.ok(alias);
			assert.ok(utf8Bytes(alias) <= 63);
			assert.ok(alias.startsWith("employee_directory_entries$$"), alias);
		});

		test("returns camelCase keys after restoring", async () => {
			const row = await selectLiterals(camelDb, {
				[CAMEL_58]: 1,
				createdAt: 2,
			}).executeTakeFirstOrThrow();

			assert.deepStrictEqual(row, { [CAMEL_58]: 1, createdAt: 2 });
		});

		test("still camelCases everything when nothing is over-long", async () => {
			const row = await selectLiterals(camelDb, { userId: 1 }).executeTakeFirstOrThrow();

			assert.deepStrictEqual(row, { userId: 1 });
		});

		test("accepts options alongside the wrapped plugin", () => {
			const tight = rawDb.withPlugin(fixLongAliases(new CamelCasePlugin(), { maxBytes: 20 }));

			const [alias] = outputAliases(
				selectLiterals(tight, { someLongerAliasName: 1 }).compile().sql,
			);

			assert.ok(alias);
			assert.ok(utf8Bytes(alias) <= 20);
		});
	});

	test("wraps any plugin, running it first on queries and last on results", async () => {
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

		const spied = rawDb.withPlugin(fixLongAliases(spy));
		await selectLiterals(spied, { [ALIAS_64]: 1 }).executeTakeFirstOrThrow();

		assert.deepStrictEqual(seen, ["query", "result"]);
	});
});
