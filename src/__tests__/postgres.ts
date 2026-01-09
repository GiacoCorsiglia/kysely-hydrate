import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, before } from "node:test";
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

/**
 * Transforms SQLite SQL to PostgreSQL-compatible SQL.
 */
function transformSqlForPostgres(sql: string): string {
	return sql.replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, "SERIAL PRIMARY KEY");
}

export function getDbForTest() {
	const testSchema = `test_${Math.random().toString(36).substring(2, 15)}`;

	const pool = new pg.Pool({
		connectionString,
		max: 10,
	});

	const dialect = new k.PostgresDialect({
		pool,
	});

	const db: k.Kysely<SeedDB> = new k.Kysely<SeedDB>({
		dialect,
	});

	/**
	 * Sets up the test database schema and seed data.
	 * Should be called before running tests.
	 */
	async function setupDatabase(): Promise<void> {
		await pool.query(`CREATE SCHEMA IF NOT EXISTS ${testSchema};`);
		await pool.query(`SET search_path TO ${testSchema};`);

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
	async function teardownDatabase(): Promise<void> {
		await pool.query(`DROP SCHEMA IF EXISTS ${testSchema} CASCADE;`);

		await db.destroy();
	}

	//
	// Register test hooks.
	//

	before(async () => {
		await setupDatabase();
	});

	after(async () => {
		await teardownDatabase();
	});

	return db;
}
