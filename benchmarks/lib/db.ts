/**
 * The schema every suite builds against, and the one seeded dataset the
 * `execute` suite runs on in both SQLite and Postgres.
 */
import SQLite from "better-sqlite3";
import * as k from "kysely";
import pg from "pg";

import { onTeardown } from "./harness.ts";
import { makeRows, range } from "./rows.ts";

export interface DB {
	users: { id: number; username: string; email: string };
	posts: { id: number; user_id: number; title: string; content: string };
	comments: { id: number; post_id: number; user_id: number; content: string };
	/** Verbose on purpose, so nested aliases overflow Postgres's 63-byte identifier limit. */
	organizations: { id: number; organization_name: string };
	organizational_departments: { id: number; organization_id: number; department_name: string };
	departmental_employee_records: {
		id: number;
		organizational_department_id: number;
		employee_preferred_full_display_name: string;
		employee_secondary_contact_email_address: string;
	};
}

/** Valid in both dialects. */
const schema = `
	CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL);
	CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
	CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, content TEXT NOT NULL);
	CREATE TABLE organizations (id INTEGER PRIMARY KEY, organization_name TEXT NOT NULL);
	CREATE TABLE organizational_departments (id INTEGER PRIMARY KEY, organization_id INTEGER NOT NULL, department_name TEXT NOT NULL);
	CREATE TABLE departmental_employee_records (
		id INTEGER PRIMARY KEY,
		organizational_department_id INTEGER NOT NULL,
		employee_preferred_full_display_name TEXT NOT NULL,
		employee_secondary_contact_email_address TEXT NOT NULL
	);
	CREATE INDEX posts_user_id ON posts (user_id);
	CREATE INDEX comments_post_id ON comments (post_id);
	CREATE INDEX comments_user_id ON comments (user_id);
	CREATE INDEX departments_organization_id ON organizational_departments (organization_id);
	CREATE INDEX employees_department_id ON departmental_employee_records (organizational_department_id);
`;

/** An empty database with the schema in place: enough to build and compile queries. */
export function emptyDb(): k.Kysely<DB> {
	const database = new SQLite(":memory:");
	database.exec(schema);
	return new k.Kysely<DB>({ dialect: new k.SqliteDialect({ database }) });
}

const ORGS = 20;
const DEPARTMENTS_PER_ORG = 3;
const EMPLOYEES_PER_DEPARTMENT = 5;

/**
 * Each table's rows, taken from the same join rows the `hydrate` suite
 * hydrates, so the users -> posts -> comments join returns exactly those.
 */
export function tableRows(): { [T in keyof DB]: DB[T][] } {
	const byId = <T extends { id: number }>(rows: T[]) => [
		...new Map(rows.map((r) => [r.id, r])).values(),
	];
	// A full cartesian product, so no join column is null.
	const rows = makeRows(500, 5, 4);
	const departments = ORGS * DEPARTMENTS_PER_ORG;
	return {
		users: byId(rows.map((r) => ({ id: r.id, username: r.username, email: r.email }))),
		posts: byId(
			rows.map((r) => ({
				id: r.posts$$id!,
				user_id: r.id,
				title: r.posts$$title!,
				content: "content",
			})),
		),
		comments: byId(
			rows.map((r) => ({
				id: r.posts$$comments$$id!,
				post_id: r.posts$$id!,
				user_id: r.id,
				content: r.posts$$comments$$content!,
			})),
		),
		organizations: range(ORGS, 1).map((id) => ({ id, organization_name: `Organization ${id}` })),
		organizational_departments: range(departments, 1).map((id) => ({
			id,
			organization_id: Math.ceil(id / DEPARTMENTS_PER_ORG),
			department_name: `Department ${id}`,
		})),
		departmental_employee_records: range(departments * EMPLOYEES_PER_DEPARTMENT, 1).map((id) => ({
			id,
			organizational_department_id: Math.ceil(id / EMPLOYEES_PER_DEPARTMENT),
			employee_preferred_full_display_name: `Employee ${id}`,
			employee_secondary_contact_email_address: `employee${id}@example.com`,
		})),
	};
}

/** 1,000 rows of at most 4 columns stays under both SQLite's and pg's bound-parameter limits. */
async function seed(db: k.Kysely<DB>): Promise<k.Kysely<DB>> {
	for (const [table, rows] of Object.entries(tableRows())) {
		for (let i = 0; i < rows.length; i += 1_000) {
			await db
				.insertInto(table as keyof DB)
				.values(rows.slice(i, i + 1_000) as never)
				.execute();
		}
	}
	return db;
}

export const seededSqlite = () => seed(emptyDb());

/**
 * Seeds a fresh schema of its own, set as every pooled connection's search
 * path.  Dropping it and closing the pool is registered with `onTeardown()`
 * before anything is created, so an interrupted run or a failed seed leaves
 * nothing behind.
 */
export async function seededPostgres(connectionString: string) {
	const name = `bench_${Math.random().toString(36).slice(2, 10)}`;
	// A short timeout: nothing listening is the expected outcome on most machines.
	const pool = new pg.Pool({
		connectionString,
		max: 4,
		connectionTimeoutMillis: 2_000,
		options: `-c search_path=${name}`,
	});
	const db = new k.Kysely<DB>({ dialect: new k.PostgresDialect({ pool }) });
	let created = false;
	const destroy = onTeardown(async () => {
		try {
			if (created) await k.sql.raw(`DROP SCHEMA ${name} CASCADE`).execute(db);
		} finally {
			await db.destroy();
		}
	});
	try {
		// One implicit transaction: the schema exists only if every table does.
		await pool.query(`CREATE SCHEMA ${name}; ${schema}`);
		created = true;
		await seed(db);
		// Fresh statistics, so query plans don't depend on when autovacuum ran.
		await k.sql.raw(`ANALYZE ${Object.keys(tableRows()).join(", ")}`).execute(db);
	} catch (error) {
		await destroy();
		throw error;
	}
	return { db, destroy };
}
