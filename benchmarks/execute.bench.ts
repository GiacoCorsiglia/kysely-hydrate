/**
 * Execution end to end: build the query, run it on a real driver, hydrate.
 *
 * The number to read first is `kysely execute, 10k rows, no hydration` against
 * the `execute` after it: identical SQL and rows, so the gap is what hydration
 * costs a caller.  SQLite and Postgres never share a
 * group, since their ratio describes the drivers, not this library.
 */
import assert from "node:assert/strict";

import * as k from "kysely";

import { fixLongAliases, MAX_IDENTIFIER_BYTES } from "../src/fix-long-aliases.ts";
import { querySet } from "../src/query-set.ts";
import { type DB, seededPostgres, seededSqlite, tableRows } from "./lib/db.ts";
import { assertDeepEqual, cli, group, onTeardown, runSuite } from "./lib/harness.ts";
import { queries } from "./lib/queries.ts";

const sqliteDb = await seededSqlite();
onTeardown(() => sqliteDb.destroy());

/** The URL with its password hidden, for printing. */
function redact(url: string): string {
	try {
		const parsed = new URL(url);
		if (parsed.password) parsed.password = "***";
		return parsed.href;
	} catch {
		return "(an unparseable URL)";
	}
}

/** Refused, or timed out connecting: pg-pool's messages for the latter carry no code. */
const nothingListening = (error: unknown) =>
	error instanceof Error &&
	((error as NodeJS.ErrnoException).code === "ECONNREFUSED" ||
		/^(Connection terminated due to connection timeout|timeout exceeded when trying to connect)$/.test(
			error.message,
		));

/**
 * Only the default URL may be unreachable: a URL someone gave, as CI does,
 * must work.  Any failure after connecting, such as bad credentials or a
 * failed seed, fails the run.
 */
async function connectPostgres() {
	if (cli()["no-postgres"]) return console.log("Postgres benchmarks skipped: --no-postgres.\n");
	const given = cli()["postgres-url"] ?? process.env.POSTGRES_URL;
	const url = given ?? "postgres://postgres:postgres@localhost:5434/kysely_hydrate_test";
	try {
		return await seededPostgres(url);
	} catch (error) {
		if (given !== undefined || !nothingListening(error)) {
			throw new Error(`Postgres benchmarks failed on ${redact(url)}`, { cause: error });
		}
		console.log(
			`Postgres benchmarks skipped: nothing listening on ${redact(url)}\n  ${String(error)}\n` +
				"  Start one with: docker compose up --detach --wait postgres\n",
		);
	}
}
const postgres = await connectPostgres();

const fetchAll = (db: k.Kysely<DB>) => queries(db).usersPostsComments.orderBy("id");
type Users = Awaited<ReturnType<ReturnType<typeof fetchAll>["execute"]>>;

let reference: Users | undefined;
/**
 * The 500 users every full fetch must hydrate, in either dialect: SQLite's,
 * checked for shape once.
 */
async function users(): Promise<Users> {
	if (!reference) {
		reference = await fetchAll(sqliteDb).execute();
		assert.equal(reference.length, 500);
		assert.equal(reference[0]!.posts.length, 5);
		assert.equal(reference[0]!.posts[0]!.comments.length, 4);
	}
	return reference;
}
const allUsers = async (out: unknown) => assertDeepEqual(out, await users());

/** `value` with every key camelCased, as `CamelCasePlugin` returns it. */
const camelKeys = (value: unknown): unknown =>
	Array.isArray(value)
		? value.map(camelKeys)
		: typeof value === "object" && value !== null
			? Object.fromEntries(
					Object.entries(value).map(([key, v]) => [
						key.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase()),
						camelKeys(v),
					]),
				)
			: value;

function declareDialect(dialect: string, db: k.Kysely<DB>) {
	const { users: userSet, postsComments } = queries(db);
	const all = fetchAll(db);
	// Two queries, users then their posts ⋈ comments, instead of one 10k-row join.
	const attached = userSet
		.attachMany(
			"posts",
			(rows) =>
				postsComments
					.where(
						"user_id",
						"in",
						rows.map((u) => u.id),
					)
					.orderBy("id"),
			{ matchChild: "user_id" },
		)
		.orderBy("id");

	// Every way to fetch the 500 users.  `executeTakeFirst` can't use
	// `qb.executeTakeFirst()`, which would cut off the rows a nested join needs,
	// so it costs a full `execute()`.  Count and exists run a smaller query and
	// never hydrate.
	group(
		{
			"kysely execute, 10k rows, no hydration": {
				run: () => all.toQuery().execute(),
				check: (rows) => assert.equal(rows.length, 10_000),
			},
			"execute, 10k rows -> 500 entities": { run: () => all.execute(), check: allUsers },
			"execute via attachMany, 2 queries": {
				run: () => attached.execute(),
				check: allUsers,
			},
			executeTakeFirst: {
				run: () => all.executeTakeFirst(),
				check: async (user) => assertDeepEqual(user, (await users())[0]),
			},
			executeCount: {
				run: () => all.executeCount(Number),
				check: (n) => assert.equal(n, 500),
			},
			executeExists: {
				run: () => all.executeExists(),
				// A query matching nothing must say so, or a constant `true` would pass.
				check: async (exists) => {
					assert.equal(exists, true);
					assert.equal(await all.where("id", "<", 0).executeExists(), false);
				},
			},
		},
		`${dialect} `,
	);

	// SQLite can't flatten the nested posts ⋈ comments derived table the query
	// set emits, so it MATERIALIZEs all 10k rows on every call and even limit 1
	// pays for them.  Postgres flattens it, so its limit 1 is the fixed cost.
	group(
		Object.fromEntries(
			[1, 10, 100, 500].map((n) => {
				const limited = all.limit(n);
				return [
					`execute, limit ${n}`,
					{
						run: () => limited.execute(),
						check: async (out: unknown) => assertDeepEqual(out, (await users()).slice(0, n)),
					},
				];
			}),
		),
		`${dialect} `,
	);
	return all;
}

declareDialect("sqlite", sqliteDb);

/**
 * org -> departments -> employees, with the joins named as given.  Typed as
 * the short names, since generic join names defeat the ref types.
 */
function orgHierarchy(db: k.Kysely<DB>, ...names: [departments: string, employees: string]) {
	const [departments, employees] = names as ["depts", "staff"];
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
	const query = querySet(db)
		.selectAs("org", db.selectFrom("organizations").select(["id", "organization_name"]))
		.innerJoinMany(
			departments,
			querySet(db)
				.selectAs(
					"department",
					db.selectFrom("organizational_departments").select(["id", "organization_id"]),
				)
				.innerJoinMany(
					employees,
					employee,
					`${employees}.organizational_department_id`,
					"department.id",
				),
			`${departments}.organization_id`,
			"org.id",
		)
		.orderBy("id");

	// Whatever aliases Postgres saw, the rows come back under the names asked for.
	const rows = tableRows();
	const expected = rows.organizations.map((org) => ({
		...org,
		[departments]: rows.organizational_departments
			.filter((d) => d.organization_id === org.id)
			.map(({ id, organization_id }) => ({
				id,
				organization_id,
				[employees]: rows.departmental_employee_records.filter(
					(e) => e.organizational_department_id === id,
				),
			})),
	}));
	const deepest = `${departments}$$${employees}$$employee_secondary_contact_email_address`;
	return {
		run: () => query.execute(),
		check: (orgs: unknown) => assertDeepEqual(orgs, expected),
		overflows: Buffer.byteLength(deepest) > MAX_IDENTIFIER_BYTES,
	};
}

if (postgres) {
	const pgAll = declareDialect("postgres", postgres.db);
	const withPlugin = (plugin: k.KyselyPlugin) => fetchAll(postgres.db.withPlugin(plugin));
	const fixed = withPlugin(fixLongAliases());
	// The documented setup: `fixLongAliases` wraps `CamelCasePlugin`, so it
	// measures the snake_case names the database sees.  The shared queries
	// select snake_case, which the plugin passes through and then camelCases in
	// the result, so the static types don't describe it.
	const camel = withPlugin(fixLongAliases(new k.CamelCasePlugin()));

	// Aliases that all fit: `fixLongAliases` still walks every query node and
	// inspects 10,000 result rows.
	group({
		"postgres execute, no plugins": { run: () => pgAll.execute(), check: allUsers },
		"postgres execute, fixLongAliases": { run: () => fixed.execute(), check: allUsers },
		"postgres execute, fixLongAliases with CamelCasePlugin": {
			run: () => camel.execute(),
			check: async (out) => assertDeepEqual(out, camelKeys(await users())),
		},
	});

	// The same 300 rows under keys that overflow the limit, and keys that don't.
	const pgFixed = postgres.db.withPlugin(fixLongAliases());
	const short = orgHierarchy(pgFixed, "depts", "staff");
	const long = orgHierarchy(pgFixed, "organizationalDepartments", "departmentalEmployeeRecords");
	assert.ok(!short.overflows && long.overflows);
	group({
		"postgres deep join, short aliases, shortening idle": short,
		"postgres deep join, long aliases, shortening engaged": long,
	});
}

await runSuite();
