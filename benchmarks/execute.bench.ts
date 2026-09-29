/**
 * Execution end to end: build the query, run it on a real driver, hydrate.
 *
 * The number to read first is `kysely execute, no hydration` against
 * `execute` in each dialect's terminals group: identical SQL and rows, so the
 * gap is what hydration costs a caller.  SQLite and Postgres never share a
 * group, since their ratio describes the drivers, not this library.
 */
import assert from "node:assert/strict";

import * as k from "kysely";
import { summary } from "mitata";

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { querySet } from "../src/query-set.ts";
import {
	type DB,
	DEPARTMENTS_PER_ORG,
	EMPLOYEES_PER_DEPARTMENT,
	ORGS,
	seededPostgres,
	seededSqlite,
} from "./lib/db.ts";
import { benchAsync, cli, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";
import { autoIncluded } from "./lib/rows.ts";

const heavy = { gcEachIteration: true };

function workloads(db: k.Kysely<DB>) {
	const { users, posts, comments, usersPostsComments } = queries(db);
	const all = usersPostsComments.orderBy("id");
	return {
		all,
		limits: [1, 10, 100, 500].map((n) => [n, all.limit(n)] as const),
		// Two queries, users then their posts ⋈ comments, instead of one 10k-row join.
		attached: users
			.attachMany(
				"posts",
				(rows) =>
					posts
						.leftJoinMany("comments", comments, "comments.post_id", "post.id")
						.where(
							"user_id",
							"in",
							rows.map((u) => u.id),
						)
						.orderBy("id"),
				{ matchChild: "user_id" },
			)
			.orderBy("id"),
	};
}

/** Nested aliases like `organizationalDepartments$$departmentalEmployeeRecords$$…` overflow 63 bytes. */
function orgHierarchies(db: k.Kysely<DB>) {
	const org = querySet(db).selectAs(
		"org",
		db.selectFrom("organizations").select(["id", "organization_name"]),
	);
	const department = querySet(db).selectAs(
		"department",
		db.selectFrom("organizational_departments").select(["id", "organization_id"]),
	);
	const employee = querySet(db).selectAs(
		"employee",
		db
			.selectFrom("departmental_employee_records")
			.select([
				"id",
				"organizational_department_id",
				"employee_preferred_full_display_name",
				"employee_secondary_contact_email_address",
			]),
	);
	return {
		long: org
			.innerJoinMany(
				"organizationalDepartments",
				department.innerJoinMany(
					"departmentalEmployeeRecords",
					employee,
					"departmentalEmployeeRecords.organizational_department_id",
					"department.id",
				),
				"organizationalDepartments.organization_id",
				"org.id",
			)
			.orderBy("id"),
		short: org
			.innerJoinMany(
				"depts",
				department.innerJoinMany(
					"staff",
					employee,
					"staff.organizational_department_id",
					"department.id",
				),
				"depts.organization_id",
				"org.id",
			)
			.orderBy("id"),
	};
}

const sqliteDb = await seededSqlite();
const sqlite = workloads(sqliteDb);

async function connectPostgres() {
	if (cli()["no-postgres"]) return console.log("Postgres benchmarks skipped: --no-postgres.\n");
	const url =
		cli()["postgres-url"] ??
		process.env.POSTGRES_URL ??
		"postgres://postgres:postgres@localhost:5434/kysely_hydrate_test";
	try {
		return await seededPostgres(url);
	} catch (error) {
		console.log(
			`Postgres benchmarks skipped: cannot reach ${url}\n  ${String(error)}\n` +
				"  Start one with: docker compose up --detach --wait postgres\n",
		);
	}
}

const postgres = await connectPostgres();
const pgDb = postgres?.db;
const pgWork = pgDb && {
	...workloads(pgDb),
	fixed: workloads(pgDb.withPlugin(fixLongAliases())).all,
	// The documented setup: `fixLongAliases` wraps `CamelCasePlugin`, so it
	// measures the snake_case names the database sees.  The shared queries
	// already select snake_case, which the plugin passes through unchanged and
	// then camelCases in the result, so the static types don't describe it.
	camel: workloads(pgDb.withPlugin(fixLongAliases(new k.CamelCasePlugin()))).all,
	orgs: orgHierarchies(pgDb.withPlugin(fixLongAliases())),
};

async function verifyDialect(w: ReturnType<typeof workloads>) {
	assert.equal((await w.all.toQuery().execute()).length, 10_000);
	const users = await w.all.execute();
	assert.equal(users.length, 500);
	assert.equal(users[0]!.posts.length, 5);
	assert.equal(users[0]!.posts[0]!.comments.length, 4);
	for (const [n, qs] of w.limits) assert.equal((await qs.execute()).length, n);
	assert.deepEqual(
		await w.attached.execute(),
		users,
		"attach and join must hydrate the same users",
	);
	assert.equal((await w.all.executeTakeFirst())?.id, 1);
	assert.equal(await w.all.executeCount(Number), 500);
	assert.equal(await w.all.executeExists(), true);
	return users;
}

async function verifyWorkloads() {
	const users = await verifyDialect(sqlite);
	if (!pgWork) return;
	assert.deepEqual(await verifyDialect(pgWork), users, "both dialects must do the same work");
	assert.deepEqual(await pgWork.fixed.execute(), users);

	const camel = autoIncluded<{ posts: { userId: number; comments: { postId: number }[] }[] }[]>(
		await pgWork.camel.execute(),
	);
	assert.equal(camel.length, 500);
	assert.equal(camel[0]!.posts[0]!.userId, 1);
	assert.equal(camel[0]!.posts[0]!.comments[0]!.postId, 1);

	const email = "employee_secondary_contact_email_address";
	assert.ok(
		Buffer.byteLength(`organizationalDepartments$$departmentalEmployeeRecords$$${email}`) >
			MAX_IDENTIFIER_BYTES,
	);
	assert.ok(Buffer.byteLength(`depts$$staff$$${email}`) <= MAX_IDENTIFIER_BYTES);
	const orgs = await pgWork.orgs.long.execute();
	assert.equal(orgs.length, ORGS);
	const employees = orgs[0]!.organizationalDepartments[0]!.departmentalEmployeeRecords;
	assert.equal(orgs[0]!.organizationalDepartments.length, DEPARTMENTS_PER_ORG);
	assert.equal(employees.length, EMPLOYEES_PER_DEPARTMENT);
	// Postgres saw hashed aliases; the rows came back under the names asked for.
	assert.deepEqual(employees[0], {
		id: 1,
		organizational_department_id: 1,
		employee_preferred_full_display_name: "Employee 1",
		[email]: "employee1@example.com",
	});
	assert.deepEqual((await pgWork.orgs.short.execute())[0]!.depts[0]!.staff[0], employees[0]);
}

function declareDialect(dialect: string, w: ReturnType<typeof workloads>) {
	// Every way to fetch the 500 users.  `executeTakeFirst` can't use `qb.executeTakeFirst()`, which would cut off
	// the rows a nested join needs, so it costs a full `execute()`.  Count and
	// exists run a smaller query and never hydrate.
	summary(() => {
		benchAsync(
			`${dialect} kysely execute, 10k rows, no hydration`,
			() => w.all.toQuery().execute(),
			{
				baseline: true,
				...heavy,
			},
		);
		benchAsync(`${dialect} execute, 10k rows -> 500 entities`, () => w.all.execute(), heavy);
		benchAsync(`${dialect} execute via attachMany, 2 queries`, () => w.attached.execute(), heavy);
		benchAsync(`${dialect} executeTakeFirst`, () => w.all.executeTakeFirst(), heavy);
		benchAsync(`${dialect} executeCount`, () => w.all.executeCount(Number));
		benchAsync(`${dialect} executeExists`, () => w.all.executeExists());
	});

	// SQLite can't flatten the nested posts ⋈ comments derived table the query
	// set emits, so it MATERIALIZEs all 10k rows on every call and even limit 1
	// pays for them.  Postgres flattens it, so its limit 1 is the fixed cost.
	summary(() => {
		for (const [n, qs] of w.limits) {
			benchAsync(`${dialect} execute, limit ${n}`, () => qs.execute(), {
				baseline: n === 1,
				...heavy,
			});
		}
	});
}

declareDialect("sqlite", sqlite);

if (pgWork) {
	declareDialect("postgres", pgWork);

	// Aliases that all fit: `fixLongAliases` still walks every query node and
	// inspects 10,000 result rows.
	summary(() => {
		benchAsync("postgres execute, no plugins", () => pgWork.all.execute(), {
			baseline: true,
			...heavy,
		});
		benchAsync("postgres execute, fixLongAliases", () => pgWork.fixed.execute(), heavy);
		benchAsync(
			"postgres execute, fixLongAliases with CamelCasePlugin",
			() => pgWork.camel.execute(),
			heavy,
		);
	});

	// The same 300 rows under keys that overflow the limit, and keys that don't.
	summary(() => {
		benchAsync(
			"postgres deep join, short aliases, shortening idle",
			() => pgWork.orgs.short.execute(),
			{
				baseline: true,
			},
		);
		benchAsync("postgres deep join, long aliases, shortening engaged", () =>
			pgWork.orgs.long.execute(),
		);
	});
}

await runSuite({
	verify: verifyWorkloads,
	teardown: async () => {
		await sqliteDb.destroy();
		await postgres?.destroy();
	},
});
