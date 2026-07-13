/**
 * Unit tests for the identifier-length plugin pair, independent of QuerySet.
 * These run against in-memory SQLite (which has no identifier-length limit),
 * exercising the shorten/restore round trip directly.
 */

import assert from "node:assert";
import { test } from "node:test";

import SQLite from "better-sqlite3";
import * as k from "kysely";

import {
	MAX_IDENTIFIER_BYTES,
	RestoreLongIdentifiersPlugin,
	ShortenLongIdentifiersPlugin,
	withIdentifierLengthGuard,
} from "./identifier-length.ts";

// Generated-style aliases (containing the `$$` separator) that exceed the
// 63-byte limit, including a pair sharing the same first 63 bytes.
const LONG_ALIAS_A =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_preferred_full_display_name";
const LONG_ALIAS_B =
	"departmentalEmployeeRecordsWithVerboseNamingConventions$$employee_secondary_contact_email_address";
// A long identifier WITHOUT the separator: user-authored, must pass through.
const LONG_PLAIN_ALIAS =
	"a_user_authored_alias_that_is_really_quite_long_but_contains_no_separator";

interface TestDB {
	things: { id: number; name: string; email: string };
}

function createDb(...plugins: k.KyselyPlugin[]): k.Kysely<TestDB> {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT, email TEXT);");
	sqlite.exec("INSERT INTO things (id, name, email) VALUES (1, 'Alice', 'alice@example.com');");
	return new k.Kysely<TestDB>({
		dialect: new k.SqliteDialect({ database: sqlite }),
		plugins,
	});
}

function compiledSql(db: k.Kysely<TestDB>): string {
	return db
		.selectFrom("things")
		.select(["id", `name as ${LONG_ALIAS_A}` as "name", `email as ${LONG_ALIAS_B}` as "email"])
		.compile().sql;
}

test("identifier-length: shortens over-long generated aliases in the compiled SQL", () => {
	const db = withIdentifierLengthGuard(createDb());
	const sql = compiledSql(db);

	assert.ok(!sql.includes(LONG_ALIAS_A));
	assert.ok(!sql.includes(LONG_ALIAS_B));

	// Every quoted identifier fits within the limit.
	const identifiers = [...sql.matchAll(/"([^"]+)"/g)].map((match) => match[1]!);
	assert.ok(identifiers.length > 0);
	for (const identifier of identifiers) {
		assert.ok(
			new TextEncoder().encode(identifier).length <= MAX_IDENTIFIER_BYTES,
			`identifier too long: ${identifier}`,
		);
	}

	// Shortening is deterministic and collision-free for aliases sharing the
	// same first 63 bytes.
	const again = compiledSql(withIdentifierLengthGuard(createDb()));
	assert.strictEqual(sql, again);
	const shortened = identifiers.filter((identifier) => identifier.includes("$$"));
	assert.strictEqual(new Set(shortened).size, shortened.length);
});

test("identifier-length: restores full-length keys in result rows", async () => {
	const db = withIdentifierLengthGuard(createDb());

	const rows = await db
		.selectFrom("things")
		.select(["id", `name as ${LONG_ALIAS_A}` as "name", `email as ${LONG_ALIAS_B}` as "email"])
		.execute();

	assert.deepStrictEqual(rows, [
		{
			id: 1,
			[LONG_ALIAS_A]: "Alice",
			[LONG_ALIAS_B]: "alice@example.com",
		},
	]);
});

test("identifier-length: works with CamelCasePlugin (shorten after, restore before)", async () => {
	// The camelCase alias is under the limit, but the snake_cased identifier
	// the CamelCasePlugin sends to the database is over it.
	const camelAlias = "employeeDirectoryEntries$$employeeSecondaryContactEmailAddress";
	const db = withIdentifierLengthGuard(createDb(new k.CamelCasePlugin()));

	const rows = await db
		.selectFrom("things")
		.select(["id", `email as ${camelAlias}` as "email"])
		.execute();

	assert.deepStrictEqual(rows, [{ id: 1, [camelAlias]: "alice@example.com" }]);
});

test("identifier-length: leaves long identifiers without the separator alone", () => {
	const db = withIdentifierLengthGuard(createDb());
	const sql = db
		.selectFrom("things")
		.select([`name as ${LONG_PLAIN_ALIAS}` as "name"])
		.compile().sql;

	assert.ok(sql.includes(LONG_PLAIN_ALIAS));
});

test("identifier-length: guard is idempotent and normalizes manual installs", () => {
	const db = createDb(new ShortenLongIdentifiersPlugin(), new k.CamelCasePlugin());

	const guarded = withIdentifierLengthGuard(db);
	assert.strictEqual(withIdentifierLengthGuard(guarded), guarded);

	const plugins = guarded.getExecutor().plugins;
	// [restore, camel, shorten]: the manually-installed (misplaced) shorten
	// instance was removed and the canonical pair applied around the camel
	// plugin.
	assert.strictEqual(plugins.length, 3);
	assert.ok(plugins[0] instanceof RestoreLongIdentifiersPlugin);
	assert.ok(plugins[1] instanceof k.CamelCasePlugin);
	assert.ok(plugins[2] instanceof ShortenLongIdentifiersPlugin);
});
