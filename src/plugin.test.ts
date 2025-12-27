import assert from "node:assert/strict";
import { test } from "node:test";

import SQLite from "better-sqlite3";
import { Kysely, SqliteDialect } from "kysely";

import { type SeedDB } from "./__tests__/fixture.ts";
import { HydratePlugin } from "./plugin.ts";
import { timestamp } from "./schema/sqlite.ts";
import { createDatabase } from "./schema/table.ts";

const testDb = createDatabase("public", {
	events: {
		id: timestamp(),
		name: timestamp(),
	},
});

test("plugin transforms timestamp columns from driver format", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1734998400000, 1735084800000)");

	// Without plugin: raw numbers from driver
	const dbWithoutPlugin = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
	});

	const rawResult = await dbWithoutPlugin
		.selectFrom("events" as any)
		.selectAll()
		.execute();

	assert.strictEqual(rawResult.length, 1);
	assert.strictEqual(typeof rawResult[0]!.id, "number");
	assert.strictEqual(typeof rawResult[0]!.name, "number");
	assert.strictEqual(rawResult[0]!.id, 1734998400000);
	assert.strictEqual(rawResult[0]!.name, 1735084800000);

	// With plugin: transformed to Date objects
	const dbWithPlugin = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	const transformedResult = await dbWithPlugin
		.selectFrom("events" as any)
		.selectAll()
		.execute();

	assert.strictEqual(transformedResult.length, 1);
	assert.ok(transformedResult[0]!.id instanceof Date);
	assert.ok(transformedResult[0]!.name instanceof Date);
	assert.strictEqual(transformedResult[0]!.id.getTime(), 1734998400000);
	assert.strictEqual(transformedResult[0]!.name.getTime(), 1735084800000);

	sqlite.close();
});

test("plugin passes through derived columns unchanged", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1000, 2000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	const result = await db
		.selectFrom("events" as any)
		.select(({ fn }) => [fn.sum("id").as("total")])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(typeof result[0]!.total, "number");
	assert.strictEqual(result[0]!.total, 1000);

	sqlite.close();
});

test("plugin handles mixed column and derived selections", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1734998400000, 2000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	const result = await db
		.selectFrom("events" as any)
		.select(["id", ({ fn }) => fn.sum("name").as("total")])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.ok(result[0]!.id instanceof Date);
	assert.strictEqual(typeof result[0]!.total, "number");
	assert.strictEqual(result[0]!.total, 2000);

	sqlite.close();
});
