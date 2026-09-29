/**
 * The schema every suite builds against, and the one seeded dataset the
 * `execute` suite runs on in both SQLite and Postgres.
 */
import SQLite from "better-sqlite3";
import * as k from "kysely";
import pg from "pg";

import { makeRows, times } from "./rows.ts";

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
export function emptyDb(plugins: k.KyselyPlugin[] = []): k.Kysely<DB> {
	const database = new SQLite(":memory:");
	database.exec(schema);
	return new k.Kysely<DB>({ dialect: new k.SqliteDialect({ database }), plugins });
}

/**
 * The users -> posts -> comments join over the seeded database returns exactly
 * these rows, so hydrating them can be compared against it.
 */
export const seedRows = () => makeRows(500, 5, 4);

export const ORGS = 20;
export const DEPARTMENTS_PER_ORG = 3;
export const EMPLOYEES_PER_DEPARTMENT = 5;

/** Each table's rows, taken from `seedRows()` so the two can't drift apart. */
function tableRows(): { [T in keyof DB]: DB[T][] } {
	const byId = <T extends { id: number }>(rows: T[]) => [
		...new Map(rows.map((r) => [r.id, r])).values(),
	];
	const rows = seedRows();
	const departments = ORGS * DEPARTMENTS_PER_ORG;
	// `seedRows()` is a full cartesian product, so no join column is null.
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
		organizations: times(ORGS, (id) => ({ id, organization_name: `Organization ${id}` })),
		organizational_departments: times(departments, (id) => ({
			id,
			organization_id: Math.ceil(id / DEPARTMENTS_PER_ORG),
			department_name: `Department ${id}`,
		})),
		departmental_employee_records: times(departments * EMPLOYEES_PER_DEPARTMENT, (id) => ({
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
 * path, so `destroy()` drops it and closes the pool in one go.
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
	try {
		await pool.query(`CREATE SCHEMA ${name}; ${schema}`);
	} catch (error) {
		await pool.end().catch(() => {});
		throw error;
	}
	const db = await seed(new k.Kysely<DB>({ dialect: new k.PostgresDialect({ pool }) }));
	return {
		db,
		destroy: async () => {
			await k.sql.raw(`DROP SCHEMA ${name} CASCADE`).execute(db);
			await db.destroy();
		},
	};
}
