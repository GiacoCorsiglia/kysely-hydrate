import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import SQLite from "better-sqlite3";
import * as k from "kysely";

import { type SeedDB } from "./fixture.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const sqlite = new SQLite(":memory:");

const schemaPath = join(__dirname, "fixture-schema.sql");
const schema = readFileSync(schemaPath, "utf-8");

sqlite.exec("PRAGMA foreign_keys = ON;");
sqlite.exec(schema);

const sqlPath = join(__dirname, "order-by-fixture.sql");
const sql = readFileSync(sqlPath, "utf-8");

sqlite.exec(sql);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

export const orderByDb: k.Kysely<SeedDB> = new k.Kysely<SeedDB>({
	dialect,
});
