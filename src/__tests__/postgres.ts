import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import * as k from "kysely";
import pg from "pg";

import { type SeedDB } from "./fixture.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Default to port 5433 for local docker-compose (avoids conflict with local postgres)
// CI uses POSTGRES_URL or the default port 5432
const connectionString =
	process.env.POSTGRES_URL || "postgres://postgres:postgres@localhost:5433/kysely_hydrate_test";

const pool = new pg.Pool({
	connectionString,
	max: 10,
});

const dialect = new k.PostgresDialect({
	pool,
});

export const db: k.Kysely<SeedDB> = new k.Kysely<SeedDB>({
	dialect,
});

/**
 * Transforms SQLite SQL to PostgreSQL-compatible SQL.
 */
function transformSqlForPostgres(sql: string): string {
	return sql.replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, "SERIAL PRIMARY KEY");
}

/**
 * Sets up the test database schema and seed data.
 * Should be called before running tests.
 */
export async function setupDatabase(): Promise<void> {
	// Drop tables if they exist (in reverse order of dependencies)
	await db.schema.dropTable("comments").ifExists().execute();
	await db.schema.dropTable("profiles").ifExists().execute();
	await db.schema.dropTable("posts").ifExists().execute();
	await db.schema.dropTable("users").ifExists().execute();

	// Read and transform the fixture SQL
	const sqlPath = join(__dirname, "fixture.sql");
	const sqliteSql = readFileSync(sqlPath, "utf-8");
	const postgresSql = transformSqlForPostgres(sqliteSql);

	// Execute the transformed SQL using the raw pg client
	const client = await pool.connect();
	try {
		await client.query(postgresSql);
	} finally {
		client.release();
	}
}

/**
 * Cleans up the database connection.
 * Should be called after all tests complete.
 */
export async function teardownDatabase(): Promise<void> {
	await db.destroy();
}
