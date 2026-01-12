import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import SQLite from "better-sqlite3";
import * as k from "kysely";

import { type SeedDB, seedDb } from "./fixture.ts";

export { seedDb };

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export interface DbTestOptions {
	/**
	 * Name of the fixture file to use (without .sql extension).
	 * Defaults to "fixture".
	 * Example: "order-by-fixture"
	 */
	fixture?: string;
}

export function getDbForTest(options: DbTestOptions = {}) {
	const { fixture = "fixture" } = options;

	const sqlite = new SQLite(":memory:");
	const schemaPath = join(__dirname, "fixture-schema.sql");
	const schema = readFileSync(schemaPath, "utf-8");

	sqlite.exec("PRAGMA foreign_keys = ON;");
	sqlite.exec(schema);

	const sqlPath = join(__dirname, `${fixture}.sql`);
	const sql = readFileSync(sqlPath, "utf-8");

	sqlite.exec("PRAGMA foreign_keys = ON;");
	// Execute the SQL file
	// SQLite's exec() method can handle multiple statements separated by semicolons
	sqlite.exec(sql);

	const dialect = new k.SqliteDialect({
		database: sqlite,
	});

	const db: k.Kysely<SeedDB> = new k.Kysely<SeedDB>({
		dialect,
	});

	return db;
}
