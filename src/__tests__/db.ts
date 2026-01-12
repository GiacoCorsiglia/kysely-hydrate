/**
 * Database test utilities that switch between PostgreSQL and SQLite
 * based on the HYDRATE_TEST_DB environment variable.
 *
 * Usage:
 *   HYDRATE_TEST_DB=postgres npm test
 *   HYDRATE_TEST_DB=sqlite npm test (default)
 */

import { type Kysely } from "kysely";

import { type SeedDB } from "./fixture.ts";
import { type DbTestOptions, getDbForTest as getPostgresDb } from "./postgres.ts";
import { getDbForTest as getSqliteDb } from "./sqlite.ts";

export type { DbTestOptions };

// Determine which database to use
const testDb = process.env.HYDRATE_TEST_DB?.toLowerCase() || "sqlite";

if (!["postgres", "sqlite"].includes(testDb)) {
	throw new Error(`Invalid HYDRATE_TEST_DB value: ${testDb}. Must be "postgres" or "sqlite"`);
}

export const dialect = testDb as "postgres" | "sqlite";

export function getDbForTest(options?: DbTestOptions): Kysely<SeedDB> {
	if (dialect === "postgres") {
		return getPostgresDb(options);
	}
	return getSqliteDb(options);
}
