/**
 * Benchmarks for execution end to end: build the query, hand it to a real
 * driver, hydrate the rows that come back.
 *
 *   node --expose-gc benchmarks/execute.bench.ts
 *   npm run bench -- execute
 *   npm run bench -- execute --filter postgres
 *
 * The other suites measure the library with the database taken out of the
 * picture.  This one puts it back, which makes it the slowest suite here:
 * every sample is a real round trip.  The number worth carrying away is the gap
 * between `toQuery().execute()` and `execute()` in the first group — kysely
 * alone against kysely plus hydration, over identical rows.  That gap is what
 * this library costs a caller.
 *
 * Postgres is optional.  If nothing answers on the connection string the
 * Postgres benchmarks are skipped with a message and the suite still exits 0,
 * so `npm run bench` works on a machine that never started one.  Pass
 * `--postgres-url <url>` to point elsewhere, or `--no-postgres` to skip it
 * outright.  A baseline saved with Postgres running will report its benchmarks
 * as missing when compared against a run without it; that is the comparison
 * telling the truth, not a fault.
 *
 * SQLite and Postgres never share a `summary()` group: the ratio between them
 * describes the two drivers, not this library.
 */
import assert from "node:assert/strict";

import * as k from "kysely";
import { summary } from "mitata";
import pg from "pg";

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { querySet } from "../src/query-set.ts";
import { type DB, seededSqlite, seedRows } from "./lib/db.ts";
import { benchAsync, flagValue, hasFlag, runSuite } from "./lib/harness.ts";

////////////////////////////////////////////////////////////
// Workloads: users -> posts -> comments.
////////////////////////////////////////////////////////////

/**
 * The join every suite here is built around: 10,000 flat rows collapsing into
 * 500 users, each with 5 posts of 4 comments.  `limit` counts base entities,
 * not rows, so it is the knob that separates fixed per-call cost from
 * per-entity cost.
 */
function usersWithPosts(db: k.Kysely<DB>, limit: number) {
	return querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.orderBy("id")
		.limit(limit);
}

/**
 * The same tables seen through `CamelCasePlugin`, which is why this exists
 * twice: the plugin moves the seam between the column names the database has
 * and the ones the query declares, so the query has to declare the other set.
 */
type CamelDB = {
	users: { id: number; username: string; email: string };
	posts: { id: number; userId: number; title: string; content: string };
	comments: { id: number; postId: number; userId: number; content: string };
};

function usersWithPostsCamel(db: k.Kysely<DB & CamelDB>, limit: number) {
	return querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.leftJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "userId"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "postId"])),
					"comments.postId",
					"posts.id",
				),
			"posts.userId",
			"user.id",
		)
		.orderBy("id")
		.limit(limit);
}

////////////////////////////////////////////////////////////
// Workloads: identifiers that overflow Postgres's 63-byte limit.
////////////////////////////////////////////////////////////

/**
 * Deliberately verbose tables, so that a two-level query set's generated
 * `key$$nestedKey$$column` aliases overflow where the tables themselves do not.
 * Mirrors `src/__tests__/identifier-length-fixture.sql`.
 */
type OrgDB = {
	organizations: { id: number; organization_name: string };
	organizational_departments: { id: number; organization_id: number; department_name: string };
	departmental_employee_records: {
		id: number;
		organizational_department_id: number;
		employee_preferred_full_display_name: string;
		employee_secondary_contact_email_address: string;
	};
};

const ORGS = 20;
const DEPARTMENTS_PER_ORG = 3;
const EMPLOYEES_PER_DEPARTMENT = 5;

/** The widest column in {@link OrgDB}, and the one that pushes aliases over the limit. */
const EMPLOYEE_EMAIL = "employee_secondary_contact_email_address";

/**
 * The two shapes of the same query, differing only in the keys the joins nest
 * under.  There is no "long aliases without the plugin" variant to compare
 * against, because Postgres would silently truncate those aliases and hydration
 * would key off names that no longer exist.  So the comparison is between the
 * plugin engaging and the plugin standing idle, over identical rows.
 */
function orgHierarchies(db: k.Kysely<DB & OrgDB>) {
	const organizations = querySet(db).selectAs(
		"org",
		db.selectFrom("organizations").select(["id", "organization_name"]),
	);
	const departments = querySet(db).selectAs(
		"department",
		db.selectFrom("organizational_departments").select(["id", "organization_id"]),
	);
	const employees = querySet(db).selectAs(
		"employee",
		db
			.selectFrom("departmental_employee_records")
			.select([
				"id",
				"organizational_department_id",
				"employee_preferred_full_display_name",
				EMPLOYEE_EMAIL,
			]),
	);

	return {
		long: organizations
			.innerJoinMany(
				"organizationalDepartments",
				departments.innerJoinMany(
					"departmentalEmployeeRecords",
					employees,
					"departmentalEmployeeRecords.organizational_department_id",
					"department.id",
				),
				"organizationalDepartments.organization_id",
				"org.id",
			)
			.orderBy("id"),

		short: organizations
			.innerJoinMany(
				"depts",
				departments.innerJoinMany(
					"staff",
					employees,
					"staff.organizational_department_id",
					"department.id",
				),
				"depts.organization_id",
				"org.id",
			)
			.orderBy("id"),
	};
}

////////////////////////////////////////////////////////////
// SQLite.
////////////////////////////////////////////////////////////

const sqlite = seededSqlite();

const sqliteAll = usersWithPosts(sqlite, 500);
const sqlite1 = usersWithPosts(sqlite, 1);
const sqlite10 = usersWithPosts(sqlite, 10);
const sqlite100 = usersWithPosts(sqlite, 100);

////////////////////////////////////////////////////////////
// Postgres: connecting, and skipping when there is nothing to connect to.
////////////////////////////////////////////////////////////

const postgresUrl =
	flagValue("--postgres-url") ??
	process.env.POSTGRES_URL ??
	// The port `docker-compose.yml` publishes, chosen to miss a local install.
	"postgres://postgres:postgres@localhost:5434/kysely_hydrate_test";

const pgSchema = `bench_${Math.random().toString(36).slice(2, 10)}`;

const pgTables = `
	CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT NOT NULL, email TEXT NOT NULL);
	CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
	CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, user_id INTEGER NOT NULL, content TEXT NOT NULL);
	CREATE TABLE organizations (id INTEGER PRIMARY KEY, organization_name TEXT NOT NULL);
	CREATE TABLE organizational_departments (id INTEGER PRIMARY KEY, organization_id INTEGER NOT NULL, department_name TEXT NOT NULL);
	CREATE TABLE departmental_employee_records (
		id INTEGER PRIMARY KEY,
		organizational_department_id INTEGER NOT NULL,
		employee_preferred_full_display_name TEXT NOT NULL,
		${EMPLOYEE_EMAIL} TEXT NOT NULL
	);
`;

/**
 * Seeds the same rows `seededSqlite()` holds, so the two dialects' numbers
 * describe the same amount of work.  Taken from `seedRows()` itself rather than
 * rebuilt here, for the same reason the SQLite fixture is: a second copy of the
 * generator would drift.  The rows repeat each ancestor once per leaf, so they
 * are deduplicated by id on the way in.
 */
async function seedPostgres(pool: pg.Pool): Promise<void> {
	await pool.query(`CREATE SCHEMA ${pgSchema};`);
	await pool.query(pgTables);

	const users = new Map<number, [username: string, email: string]>();
	const posts = new Map<number, [userId: number, title: string]>();
	const comments = new Map<number, [postId: number, userId: number, content: string]>();

	for (const row of seedRows()) {
		users.set(row.id, [row.username, row.email]);
		// `seedRows()` is a full cartesian product, so every join column is
		// present; the nullable types describe a left join that matched nothing,
		// which cannot happen here.
		posts.set(row.posts$$id!, [row.posts$$user_id!, row.posts$$title!]);
		comments.set(row.posts$$comments$$id!, [
			row.posts$$comments$$post_id!,
			row.id,
			row.posts$$comments$$content!,
		]);
	}

	// One statement per table, with each column passed as an array: 13,000
	// single-row inserts would dominate the suite's startup time.
	const column = <T extends readonly unknown[], I extends number>(
		map: ReadonlyMap<number, T>,
		index: I,
	): T[I][] => [...map.values()].map((tuple) => tuple[index]);

	await pool.query(
		`INSERT INTO users (id, username, email)
		 SELECT * FROM UNNEST($1::int[], $2::text[], $3::text[]);`,
		[[...users.keys()], column(users, 0), column(users, 1)],
	);
	await pool.query(
		`INSERT INTO posts (id, user_id, title, content)
		 SELECT *, 'content' FROM UNNEST($1::int[], $2::int[], $3::text[]);`,
		[[...posts.keys()], column(posts, 0), column(posts, 1)],
	);
	await pool.query(
		`INSERT INTO comments (id, post_id, user_id, content)
		 SELECT * FROM UNNEST($1::int[], $2::int[], $3::int[], $4::text[]);`,
		[[...comments.keys()], column(comments, 0), column(comments, 1), column(comments, 2)],
	);

	const orgIds: number[] = [];
	const departmentIds: number[] = [];
	const departmentOrgIds: number[] = [];
	const employeeIds: number[] = [];
	const employeeDepartmentIds: number[] = [];

	for (let org = 1; org <= ORGS; org++) {
		orgIds.push(org);
		for (let d = 0; d < DEPARTMENTS_PER_ORG; d++) {
			const department = (org - 1) * DEPARTMENTS_PER_ORG + d + 1;
			departmentIds.push(department);
			departmentOrgIds.push(org);
			for (let e = 0; e < EMPLOYEES_PER_DEPARTMENT; e++) {
				employeeIds.push((department - 1) * EMPLOYEES_PER_DEPARTMENT + e + 1);
				employeeDepartmentIds.push(department);
			}
		}
	}

	await pool.query(
		`INSERT INTO organizations (id, organization_name)
		 SELECT id, 'Organization ' || id FROM UNNEST($1::int[]) AS id;`,
		[orgIds],
	);
	await pool.query(
		`INSERT INTO organizational_departments (id, organization_id, department_name)
		 SELECT id, organization_id, 'Department ' || id
		 FROM UNNEST($1::int[], $2::int[]) AS t(id, organization_id);`,
		[departmentIds, departmentOrgIds],
	);
	await pool.query(
		`INSERT INTO departmental_employee_records
		   (id, organizational_department_id, employee_preferred_full_display_name, ${EMPLOYEE_EMAIL})
		 SELECT id, department_id, 'Employee ' || id, 'employee' || id || '@example.com'
		 FROM UNNEST($1::int[], $2::int[]) AS t(id, department_id);`,
		[employeeIds, employeeDepartmentIds],
	);
}

interface Postgres {
	db: k.Kysely<DB>;
	teardown: () => Promise<void>;
}

/**
 * Gated on reaching the server, not on an environment variable being set: a
 * variable says what someone intended, and what this suite needs to know is
 * whether there is anything listening.
 */
async function connectPostgres(): Promise<Postgres | undefined> {
	if (hasFlag("--no-postgres")) {
		console.log("Postgres benchmarks skipped: --no-postgres was passed.\n");
		return undefined;
	}

	const pool = new pg.Pool({
		connectionString: postgresUrl,
		max: 4,
		// Short, because "nothing is listening" is the expected outcome on a
		// machine that never started a server, and the suite should say so
		// rather than stall in front of the benchmarks it can still run.
		connectionTimeoutMillis: 2_000,
		// A startup option rather than `SET search_path`, so every connection the
		// pool opens lands in this run's own schema and cleanup is one DROP.
		options: `-c search_path=${pgSchema}`,
	});

	try {
		await pool.query("SELECT 1");
	} catch (error) {
		// `pool.end()` on a pool that never connected can itself reject; nothing
		// about that changes the outcome, which is that we skip.
		await pool.end().catch(() => {});
		console.log(
			`Postgres benchmarks skipped: cannot reach ${postgresUrl}\n` +
				`  ${error instanceof Error ? error.message : String(error)}\n` +
				`  Start one with: docker compose up --detach --wait postgres\n`,
		);
		return undefined;
	}

	await seedPostgres(pool);

	const db = new k.Kysely<DB>({ dialect: new k.PostgresDialect({ pool }) });

	return {
		db,
		teardown: async () => {
			await pool.query(`DROP SCHEMA IF EXISTS ${pgSchema} CASCADE;`);
			await db.destroy();
			// Kysely creates its driver lazily, so `destroy()` only ends the pool
			// if a Kysely query actually ran.  A pool left open holds the process
			// open for pg's idle timeout.
			if (!pool.ended) await pool.end();
		},
	};
}

/**
 * Every Postgres variant shares one pool, through `withPlugin`, so there is a
 * single connection to close and the plugin comparison is not measuring three
 * differently warmed pools against each other.
 */
function postgresWorkloads(db: k.Kysely<DB>) {
	const fixed = db.withPlugin(fixLongAliases());

	return {
		plain: usersWithPosts(db, 500),
		fixed: usersWithPosts(fixed, 500),
		camel: usersWithPostsCamel(
			// The documented recommended setup: `fixLongAliases` wraps
			// `CamelCasePlugin` so lengths are measured on the snake_cased names
			// the database actually sees.
			db.withPlugin(fixLongAliases(new k.CamelCasePlugin())).withTables<CamelDB>(),
			500,
		),
		orgs: orgHierarchies(fixed.withTables<OrgDB>()),
	};
}

const postgres = await connectPostgres();
const pgWork = postgres === undefined ? undefined : postgresWorkloads(postgres.db);

////////////////////////////////////////////////////////////
// Correctness.
////////////////////////////////////////////////////////////

/**
 * Every workload runs once and has its output asserted before anything is
 * timed.  Without this, a change that makes a query return nothing — a broken
 * join, a plugin that mangles an alias — reads as an enormous speedup instead
 * of a failure.
 */
async function verifyWorkloads(): Promise<void> {
	// The raw row count is half of the headline comparison, so assert it rather
	// than assume the fixture still explodes the way it is supposed to.
	assert.equal((await sqliteAll.toQuery().execute()).length, 10_000, "the join returns 10k rows");

	const users = await sqliteAll.execute();
	assert.equal(users.length, 500, "10k rows collapse into 500 entities");
	assert.equal(users[0]!.posts.length, 5);
	assert.equal(users[0]!.posts[0]!.comments.length, 4);

	assert.equal((await sqlite1.execute()).length, 1);
	assert.equal((await sqlite10.execute()).length, 10);
	assert.equal((await sqlite100.execute()).length, 100);

	// `executeTakeFirst` cannot delegate to `qb.executeTakeFirst()`, which would
	// suppress the rows a nested join needs, so it runs the whole query and
	// keeps one entity.  Assert that it really does return that entity, since
	// the benchmark's point is how much that costs.
	assert.equal((await sqliteAll.executeTakeFirst())?.id, 1);
	assert.equal((await sqliteAll.executeTakeFirstOrThrow()).id, 1);
	assert.equal(await sqliteAll.executeCount(Number), 500);
	assert.equal(await sqliteAll.executeExists(), true);

	if (pgWork === undefined) return;

	assert.equal((await pgWork.plain.toQuery().execute()).length, 10_000, "pg returns 10k rows");

	for (const [name, qs] of [
		["plain", pgWork.plain],
		["fixLongAliases", pgWork.fixed],
	] as const) {
		const pgUsers = await qs.execute();
		assert.equal(pgUsers.length, 500, `pg ${name} collapses into 500 entities`);
		assert.equal(pgUsers[0]!.posts[0]!.comments.length, 4, `pg ${name} nests comments`);
	}

	// The camelCase variant selects different column names for the same columns,
	// so checking one of them proves the plugin round-tripped rather than that
	// some query somewhere returned 500 of something.
	const camelUsers = await pgWork.camel.execute();
	assert.equal(camelUsers.length, 500);
	assert.equal(camelUsers[0]!.posts[0]!.userId, 1);
	assert.equal(camelUsers[0]!.posts[0]!.comments[0]!.postId, 1);

	const longAlias = `organizationalDepartments$$departmentalEmployeeRecords$$${EMPLOYEE_EMAIL}`;
	assert.ok(
		Buffer.byteLength(longAlias) > MAX_IDENTIFIER_BYTES,
		`"${longAlias}" must overflow for the shortening path to engage`,
	);
	assert.ok(
		Buffer.byteLength(`depts$$staff$$${EMPLOYEE_EMAIL}`) <= MAX_IDENTIFIER_BYTES,
		"the short-alias variant must not engage it",
	);

	const orgs = await pgWork.orgs.long.execute();
	assert.equal(orgs.length, ORGS);
	const departments = orgs[0]!.organizationalDepartments;
	assert.equal(departments.length, DEPARTMENTS_PER_ORG);
	const employees = departments[0]!.departmentalEmployeeRecords;
	assert.equal(employees.length, EMPLOYEES_PER_DEPARTMENT);
	// The whole point of the plugin: Postgres saw a 63-byte hash of these names,
	// and the rows came back under the names the query asked for.
	assert.deepEqual(employees[0], {
		id: 1,
		organizational_department_id: 1,
		employee_preferred_full_display_name: "Employee 1",
		employee_secondary_contact_email_address: "employee1@example.com",
	});

	const shortOrgs = await pgWork.orgs.short.execute();
	assert.equal(shortOrgs.length, ORGS);
	assert.deepEqual(
		shortOrgs[0]!.depts[0]!.staff[0],
		employees[0],
		"both variants must return the same rows, or the pair is not a comparison",
	);
}

await verifyWorkloads();

////////////////////////////////////////////////////////////
// Declaring benchmarks.
////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////
// SQLite: what hydration adds to a round trip.
//
// The one comparison to read first.  Both run the same SQL against the same
// database and return the same 10,000 rows; the second one then hydrates them.
// The ratio is this library's cost over the driver alone.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("sqlite kysely execute, 10k rows, no hydration", () =>
		sqliteAll.toQuery().execute(),
	).baseline(true);
	benchAsync("sqlite querySet execute, 10k rows -> 500 entities", () => sqliteAll.execute());
});

////////////////////////////////////////////////////////////
// SQLite: how the cost scales with the result.
//
// One query at four limits.  The first entry is nearly all fixed cost — compile,
// round trip, one entity — so the rest of the curve is what each further entity
// and its 20 rows add.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("sqlite execute, limit 1", () => sqlite1.execute()).baseline(true);
	benchAsync("sqlite execute, limit 10", () => sqlite10.execute());
	benchAsync("sqlite execute, limit 100", () => sqlite100.execute());
	benchAsync("sqlite execute, limit 500", () => sqliteAll.execute());
});

////////////////////////////////////////////////////////////
// SQLite: the execution terminals.
//
// `executeTakeFirst` is not `qb.executeTakeFirst()` — that would cut off the
// rows a nested join needs — so it runs the whole query and keeps one entity,
// and costs the same as `execute()`.  `executeCount` and `executeExists` run a
// different, much smaller query and never hydrate, which is the contrast.
////////////////////////////////////////////////////////////

summary(() => {
	benchAsync("sqlite execute, all 500 entities", () => sqliteAll.execute()).baseline(true);
	benchAsync("sqlite executeTakeFirst", () => sqliteAll.executeTakeFirst());
	benchAsync("sqlite executeCount", () => sqliteAll.executeCount(Number));
	benchAsync("sqlite executeExists", () => sqliteAll.executeExists());
});

////////////////////////////////////////////////////////////
// Postgres.
//
// Kept in their own groups: a SQLite-to-Postgres ratio would be a fact about
// an in-process C library against a TCP round trip, not about this library.
////////////////////////////////////////////////////////////

if (pgWork !== undefined) {
	// The same decomposition as the first SQLite group.  Over a socket the
	// driver's share of the work is far larger, so hydration should look cheaper
	// here in relative terms while costing exactly the same in absolute ones.
	summary(() => {
		benchAsync("postgres kysely execute, 10k rows, no hydration", () =>
			pgWork.plain.toQuery().execute(),
		).baseline(true);
		benchAsync("postgres querySet execute, 10k rows -> 500 entities", () => pgWork.plain.execute());
	});

	// What the recommended plugin stack costs on a query whose aliases all fit.
	// `fixLongAliases` still walks every query node looking for one that does
	// not, and still inspects 10,000 result rows, so "nothing to do" is not the
	// same as "free".
	summary(() => {
		benchAsync("postgres execute, no plugins", () => pgWork.plain.execute()).baseline(true);
		benchAsync("postgres execute, fixLongAliases", () => pgWork.fixed.execute());
		benchAsync("postgres execute, fixLongAliases with CamelCasePlugin", () =>
			pgWork.camel.execute(),
		);
	});

	// And what it costs when it does have something to do: the same 300 rows
	// under keys that overflow the 63-byte limit, against keys that do not.
	summary(() => {
		benchAsync("postgres deep join, short aliases, shortening idle", () =>
			pgWork.orgs.short.execute(),
		).baseline(true);
		benchAsync("postgres deep join, long aliases, shortening engaged", () =>
			pgWork.orgs.long.execute(),
		);
	});
}

////////////////////////////////////////////////////////////
// Run.
////////////////////////////////////////////////////////////

try {
	await runSuite("execute");
} finally {
	// Unconditionally, and in a `finally`: a pg pool left open keeps the event
	// loop alive, and the process would hang after a perfectly good report.
	await sqlite.destroy();
	await postgres?.teardown();
}
