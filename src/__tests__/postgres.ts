import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, before } from "node:test";
import { fileURLToPath } from "node:url";

import * as k from "kysely";
import pg from "pg";

import { type SeedDB } from "./fixture.ts";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Default to port 5434 for local docker-compose (avoids conflict with local postgres)
// CI uses POSTGRES_URL or the default port 5432
const connectionString =
	process.env.POSTGRES_URL || "postgres://postgres:postgres@localhost:5434/kysely_hydrate_test";

/**
 * Transforms SQLite SQL to PostgreSQL-compatible SQL.
 */
function transformSqlForPostgres(sql: string): string {
	return sql.replace(/INTEGER PRIMARY KEY AUTOINCREMENT/gi, "SERIAL PRIMARY KEY");
}

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
	const testSchema = `test_${Math.random().toString(36).substring(2, 15)}`;

	const pool = new pg.Pool({
		connectionString,
		max: 10,
		// Apply the schema at connection startup so every connection the pool
		// opens uses it. (`SET search_path` would only configure the single
		// connection that happened to run it.)
		options: `-c search_path=${testSchema}`,
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

		// Read and transform the fixture SQL
		const sqlPath = join(__dirname, `${fixture}.sql`);
		const sqliteSql = readFileSync(sqlPath, "utf-8");
		const postgresSql = transformSqlForPostgres(sqliteSql);

		const schemaPath = join(__dirname, "fixture-schema.sql");
		const sqliteSchema = readFileSync(schemaPath, "utf-8");
		const postgresSchema = transformSqlForPostgres(sqliteSchema);

		// Execute the transformed SQL using the raw pg client
		const client = await pool.connect();
		try {
			await client.query(postgresSchema);
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

		// Kysely initializes its driver lazily, so `db.destroy()` only ends the
		// pool if a Kysely query was actually executed. In files that only
		// compile queries (e.g. the SQL-generation tests), the pool would
		// otherwise stay open and stall the process for pg's 10s idle timeout.
		if (!pool.ended) {
			await pool.end();
		}
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
