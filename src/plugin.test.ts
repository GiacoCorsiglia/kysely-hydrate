import assert from "node:assert/strict";
import { test } from "node:test";

import SQLite from "better-sqlite3";
import { Kysely, SqliteDialect } from "kysely";

import { type SeedDB } from "./__tests__/fixture.ts";
import { map } from "./mapped-expression.ts";
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

test("plugin applies map function to column", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1734998400000, 1735084800000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	const result = await db
		.selectFrom("events" as any)
		.select((eb) => [map(eb.ref("id"), (d: Date) => d.toISOString()).as("iso_id")])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(typeof result[0]!.iso_id, "string");
	assert.strictEqual(result[0]!.iso_id, new Date(1734998400000).toISOString());

	sqlite.close();
});

test("plugin stacks maps through subqueries", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1734998400000, 1735084800000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	// Inner query: fromDriver converts to Date, then map to ISO string
	const inner = db
		.selectFrom("events" as any)
		.select((eb) => [map(eb.ref("id"), (d: Date) => d.toISOString()).as("iso_id")])
		.as("inner");

	// Outer query: apply another map to uppercase the string
	const result = await db
		.selectFrom(inner)
		.select((eb) => [map(eb.ref("inner.iso_id"), (s: string) => s.toUpperCase()).as("upper_iso")])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(typeof result[0]!.upper_iso, "string");
	assert.strictEqual(result[0]!.upper_iso, new Date(1734998400000).toISOString().toUpperCase());

	sqlite.close();
});

test("plugin applies map to derived expressions", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1000, 2000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	const result = await db
		.selectFrom("events" as any)
		.select((eb) => [map(eb.fn.sum("id"), (n) => (n as number) * 2).as("doubled")])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(result[0]!.doubled, 2000);

	sqlite.close();
});

test("plugin composes nested map functions", async () => {
	const sqlite = new SQLite(":memory:");
	sqlite.exec("CREATE TABLE events (id INTEGER, name INTEGER)");
	sqlite.exec("INSERT INTO events (id, name) VALUES (1734998400000, 1735084800000)");

	const db = new Kysely<SeedDB>({
		dialect: new SqliteDialect({ database: sqlite }),
		plugins: [new HydratePlugin(testDb)],
	});

	// Nested maps: fromDriver converts to Date, then first map to ISO string, then second map to uppercase
	const result = await db
		.selectFrom("events" as any)
		.select((eb) => [
			map(
				map(eb.ref("id"), (d: Date) => d.toISOString()),
				(s: string) => s.toUpperCase(),
			).as("composed"),
		])
		.execute();

	assert.strictEqual(result.length, 1);
	assert.strictEqual(typeof result[0]!.composed, "string");
	assert.strictEqual(result[0]!.composed, new Date(1734998400000).toISOString().toUpperCase());

	sqlite.close();
});
