import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import SQLite from "better-sqlite3";
import * as k from "kysely";

import { type SeedDB } from "./fixture.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const sqlite = new SQLite(":memory:");
const sqlPath = join(__dirname, "fixture.sql");
const sql = readFileSync(sqlPath, "utf-8");

sqlite.exec("PRAGMA foreign_keys = ON;");
// Execute the SQL file
// SQLite's exec() method can handle multiple statements separated by semicolons
sqlite.exec(sql);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

export const db = new k.Kysely<SeedDB>({
	dialect,
});
