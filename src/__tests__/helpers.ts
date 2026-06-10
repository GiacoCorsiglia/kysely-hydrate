import { describe } from "node:test";

import * as k from "kysely";

import { dialect } from "./db.ts";
import { type SeedDB } from "./fixture.ts";

/**
 * Like `describe`, but the suite only runs against Postgres
 * (HYDRATE_TEST_DB=postgres) and is skipped otherwise. Use for tests of
 * Postgres-only features (lateral joins, writes with RETURNING, etc.).
 */
export function describePg(name: string, fn: () => void): void {
	describe(name, { skip: dialect !== "postgres" }, fn);
}

/**
 * RollbackError is thrown to trigger transaction rollback in tests.
 * This ensures all write operations are automatically rolled back.
 */
class RollbackError extends Error {}

/**
 * Helper function to run tests inside a transaction that automatically rolls back.
 * This prevents test pollution by ensuring no data persists after tests complete.
 *
 * @param db - The database instance to create a transaction from
 * @param testFn - The test function to run inside the transaction
 *
 * @example
 * ```ts
 * test("insert user", async () => {
 *   await testInTransaction(db, async (trx) => {
 *     const result = await querySet(trx)
 *       .insertAs("user", ...)
 *       .execute();
 *     assert.ok(result);
 *   });
 * });
 * ```
 */
export async function testInTransaction(
	db: k.Kysely<SeedDB>,
	testFn: (trx: k.Kysely<SeedDB>) => Promise<void>,
): Promise<void> {
	try {
		await db.transaction().execute(async (trx) => {
			await testFn(trx);
			throw new RollbackError();
		});
	} catch (e) {
		if (e instanceof RollbackError) {
			// Suppress - this is expected
			return;
		}
		throw e;
	}
}
