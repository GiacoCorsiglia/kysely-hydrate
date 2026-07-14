/**
 * PostgreSQL identifier-length (63-byte truncation) tests for QuerySet.
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1), emitting only a NOTICE. QuerySet builds prefixed column
 * aliases when nesting relations (`parent$$child$$column`), so with deep
 * nesting and/or long table/column names the generated alias exceeds 63
 * bytes. Postgres truncates the alias in the result set and hydration then
 * silently produces wrong output (mangled/missing fields, or collisions
 * between two distinct columns that share the same first 63 bytes).
 *
 * The first suite reproduces the bug as it manifested before over-long
 * aliases were encoded (see helpers/alias-encoding.ts); it is Postgres-only
 * because SQLite has no identifier-length limit. The second suite covers the
 * encoding itself (ORDER BY, pagination, keyBy, attaches, collision and
 * restoration failure modes) and runs on both dialects, since the encoding
 * is dialect-independent.
 *
 * Every identifier in the fixture DDL (identifier-length-fixture.sql) is
 * itself under 63 bytes — only the generated alias chains are over-long.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import {
	CamelCasePlugin,
	type KyselyPlugin,
	type PluginTransformQueryArgs,
	type PluginTransformResultArgs,
	type QueryResult,
	type RootOperationNode,
	type UnknownRow,
} from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg } from "./__tests__/helpers.ts";
import { AliasCollisionError, AliasRestorationError } from "./helpers/errors.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest({ fixture: "identifier-length-fixture" });

describePg("query-set: postgres identifier length (63-byte truncation)", () => {
	//
	// Without CamelCasePlugin
	//

	const snakeDb = db.withTables<{
		organizations: { id: number; organization_name: string };
		organizational_departments: {
			id: number;
			organization_id: number;
			department_name: string;
		};
		departmental_employee_records: {
			id: number;
			organizational_department_id: number;
			employee_preferred_full_display_name: string;
			employee_secondary_contact_email_address: string;
		};
	}>();

	test("deeply nested join whose generated alias exceeds 63 bytes", async () => {
		// The generated alias for the grandchild display-name column is
		// "organizationalDepartments$$departmentalEmployeeRecords$$employee_preferred_full_display_name"
		// (92 chars). Postgres truncates it to its first 63 bytes
		// ("...$$employe"), so the hydrator cannot find the column under its
		// full name.
		const organizations = await querySet(snakeDb)
			.selectAs("org", snakeDb.selectFrom("organizations").select(["id", "organization_name"]))
			.where("organizations.id", "=", 1)
			.innerJoinMany(
				"organizationalDepartments",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("organizational_departments")
							.select(["id", "organization_id", "department_name"]),
					).innerJoinMany(
						"departmentalEmployeeRecords",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("departmental_employee_records")
									.select([
										"id",
										"organizational_department_id",
										"employee_preferred_full_display_name",
									]),
							),
						"departmentalEmployeeRecords.organizational_department_id",
						"organizationalDepartments.id",
					),
				"organizationalDepartments.organization_id",
				"org.id",
			)
			.execute();

		assert.deepStrictEqual(organizations, [
			{
				id: 1,
				organization_name: "Acme Corporation",
				organizationalDepartments: [
					{
						id: 1,
						organization_id: 1,
						department_name: "Engineering",
						departmentalEmployeeRecords: [
							{
								id: 1,
								organizational_department_id: 1,
								employee_preferred_full_display_name: "Alice Anderson",
							},
							{
								id: 2,
								organizational_department_id: 1,
								employee_preferred_full_display_name: "Bob Barker",
							},
						],
					},
				],
			},
		]);
	});

	test("two distinct aliases sharing the same first 63 bytes collide", async () => {
		// With this 55-char collection key, the generated aliases
		// "<key>$$employee_preferred_full_display_name"      (93 chars) and
		// "<key>$$employee_secondary_contact_email_address"  (97 chars)
		// share the same first 63 bytes ("<key>$$employ"). Postgres truncates
		// both to the SAME identifier, so the result set contains duplicate
		// column names and one value clobbers the other.
		const departments = await querySet(snakeDb)
			.selectAs(
				"department",
				snakeDb.selectFrom("organizational_departments").select(["id", "department_name"]),
			)
			.where("organizational_departments.id", "=", 1)
			.innerJoinMany(
				"departmentalEmployeeRecordsWithVerboseNamingConventions",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmental_employee_records")
							.select([
								"id",
								"organizational_department_id",
								"employee_preferred_full_display_name",
								"employee_secondary_contact_email_address",
							]),
					),
				"departmentalEmployeeRecordsWithVerboseNamingConventions.organizational_department_id",
				"department.id",
			)
			.execute();

		assert.deepStrictEqual(departments, [
			{
				id: 1,
				department_name: "Engineering",
				departmentalEmployeeRecordsWithVerboseNamingConventions: [
					{
						id: 1,
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Alice Anderson",
						employee_secondary_contact_email_address: "alice.anderson@example.com",
					},
					{
						id: 2,
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Bob Barker",
						employee_secondary_contact_email_address: "bob.barker@example.com",
					},
				],
			},
		]);
	});

	//
	// With CamelCasePlugin
	//

	const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
		organizations: { id: number; organizationName: string };
		organizationalDepartments: {
			id: number;
			organizationId: number;
			departmentName: string;
		};
		departmentalEmployeeRecords: {
			id: number;
			organizationalDepartmentId: number;
			employeePreferredFullDisplayName: string;
			employeeSecondaryContactEmailAddress: string;
		};
	}>();

	test("with CamelCasePlugin: deeply nested join whose generated alias exceeds 63 bytes", async () => {
		// The camelCase alias
		// "organizationalDepartments$$departmentalEmployeeRecords$$employeePreferredFullDisplayName"
		// (88 chars) is snake_cased by the plugin to a 95-char SQL identifier,
		// which Postgres truncates to 63 bytes. The truncated identifier is
		// camelized back, so the row key no longer matches the alias the
		// hydrator expects.
		const organizations = await querySet(camelDb)
			.selectAs("org", camelDb.selectFrom("organizations").select(["id", "organizationName"]))
			.where("organizations.id", "=", 1)
			.innerJoinMany(
				"organizationalDepartments",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("organizationalDepartments")
							.select(["id", "organizationId", "departmentName"]),
					).innerJoinMany(
						"departmentalEmployeeRecords",
						({ eb, qs }) =>
							qs(
								eb
									.selectFrom("departmentalEmployeeRecords")
									.select(["id", "organizationalDepartmentId", "employeePreferredFullDisplayName"]),
							),
						"departmentalEmployeeRecords.organizationalDepartmentId",
						"organizationalDepartments.id",
					),
				"organizationalDepartments.organizationId",
				"org.id",
			)
			.execute();

		assert.deepStrictEqual(organizations, [
			{
				id: 1,
				organizationName: "Acme Corporation",
				organizationalDepartments: [
					{
						id: 1,
						organizationId: 1,
						departmentName: "Engineering",
						departmentalEmployeeRecords: [
							{
								id: 1,
								organizationalDepartmentId: 1,
								employeePreferredFullDisplayName: "Alice Anderson",
							},
							{
								id: 2,
								organizationalDepartmentId: 1,
								employeePreferredFullDisplayName: "Bob Barker",
							},
						],
					},
				],
			},
		]);
	});

	test("with CamelCasePlugin: camelCase alias under 63 chars whose snake_case form exceeds 63 bytes", async () => {
		// This failure mode is SPECIFIC to the CamelCasePlugin: the JS-visible
		// aliases are within Postgres's limit —
		//   "employeeDirectoryEntries$$employeePreferredFullDisplayName"     (58 chars)
		//   "employeeDirectoryEntries$$employeeSecondaryContactEmailAddress" (62 chars)
		// — but the plugin snake_cases them to 64- and 68-byte SQL
		// identifiers, which Postgres truncates. The truncated snake_case
		// identifiers are camelized back into row keys that no longer match.
		const departments = await querySet(camelDb)
			.selectAs(
				"department",
				camelDb.selectFrom("organizationalDepartments").select(["id", "departmentName"]),
			)
			.where("organizationalDepartments.id", "=", 1)
			.innerJoinMany(
				"employeeDirectoryEntries",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmentalEmployeeRecords")
							.select([
								"id",
								"organizationalDepartmentId",
								"employeePreferredFullDisplayName",
								"employeeSecondaryContactEmailAddress",
							]),
					),
				"employeeDirectoryEntries.organizationalDepartmentId",
				"department.id",
			)
			.execute();

		assert.deepStrictEqual(departments, [
			{
				id: 1,
				departmentName: "Engineering",
				employeeDirectoryEntries: [
					{
						id: 1,
						organizationalDepartmentId: 1,
						employeePreferredFullDisplayName: "Alice Anderson",
						employeeSecondaryContactEmailAddress: "alice.anderson@example.com",
					},
					{
						id: 2,
						organizationalDepartmentId: 1,
						employeePreferredFullDisplayName: "Bob Barker",
						employeeSecondaryContactEmailAddress: "bob.barker@example.com",
					},
				],
			},
		]);
	});
});

//
// Over-long alias encoding — cross-dialect behavior.
//
// The over-63-byte alias handling changes the generated SQL on every dialect
// (the encoding cannot depend on the dialect, or query generation and
// hydration could disagree), so unlike the reproduction suite above these
// tests also run against SQLite.
//

describe("query-set: over-long generated alias encoding", () => {
	const snakeDb = db.withTables<{
		organizations: { id: number; organization_name: string };
		organizational_departments: {
			id: number;
			organization_id: number;
			department_name: string;
		};
		departmental_employee_records: {
			id: number;
			organizational_department_id: number;
			employee_preferred_full_display_name: string;
			employee_secondary_contact_email_address: string;
		};
	}>();

	const camelDb = db.withPlugin(new CamelCasePlugin()).withTables<{
		organizations: { id: number; organizationName: string };
		organizationalDepartments: {
			id: number;
			organizationId: number;
			departmentName: string;
		};
		departmentalEmployeeRecords: {
			id: number;
			organizationalDepartmentId: number;
			employeePreferredFullDisplayName: string;
			employeeSecondaryContactEmailAddress: string;
		};
	}>();

	test("orderBy on a deeply nested over-long alias (inner path)", async () => {
		// The nested alias
		// "grandparentOrganizationEntityOfTheDepartment$$organization_name"
		// (63 chars) is over-long at the middle level, and the full alias
		// (95 chars) is over-long again at the top level.  The ORDER BY must
		// reference the middle subquery's encoded column name.
		const employees = await querySet(snakeDb)
			.selectAs(
				"employeeRecord",
				snakeDb
					.selectFrom("departmental_employee_records")
					.select(["id", "organizational_department_id", "employee_preferred_full_display_name"]),
			)
			.innerJoinOne(
				"parentOrganizationalDepartment",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("organizational_departments")
							.select(["id", "organization_id", "department_name"]),
					).innerJoinOne(
						"grandparentOrganizationEntityOfTheDepartment",
						({ eb, qs }) => qs(eb.selectFrom("organizations").select(["id", "organization_name"])),
						"grandparentOrganizationEntityOfTheDepartment.id",
						"parentOrganizationalDepartment.organization_id",
					),
				"parentOrganizationalDepartment.id",
				"employeeRecord.organizational_department_id",
			)
			.orderBy(
				"parentOrganizationalDepartment$$grandparentOrganizationEntityOfTheDepartment$$organization_name",
				"desc",
			)
			.execute();

		const acmeEngineering = {
			id: 1,
			organization_id: 1,
			department_name: "Engineering",
			grandparentOrganizationEntityOfTheDepartment: {
				id: 1,
				organization_name: "Acme Corporation",
			},
		};
		const zenithMarketing = {
			id: 2,
			organization_id: 2,
			department_name: "Marketing",
			grandparentOrganizationEntityOfTheDepartment: {
				id: 2,
				organization_name: "Zenith Industries",
			},
		};

		// Ordered by organization name descending (Zenith first), then id.
		assert.deepStrictEqual(employees, [
			{
				id: 3,
				organizational_department_id: 2,
				employee_preferred_full_display_name: "Carol Chen",
				parentOrganizationalDepartment: zenithMarketing,
			},
			{
				id: 4,
				organizational_department_id: 2,
				employee_preferred_full_display_name: "Dan Diaz",
				parentOrganizationalDepartment: zenithMarketing,
			},
			{
				id: 1,
				organizational_department_id: 1,
				employee_preferred_full_display_name: "Alice Anderson",
				parentOrganizationalDepartment: acmeEngineering,
			},
			{
				id: 2,
				organizational_department_id: 1,
				employee_preferred_full_display_name: "Bob Barker",
				parentOrganizationalDepartment: acmeEngineering,
			},
		]);
	});

	test("pagination + many-join + orderBy on over-long alias (outer path); hydrate() round-trip", async () => {
		// Pagination plus a many-join forces the nested-subquery query plan:
		// the outer ORDER BY must reference the hoisted (encoded) column, and
		// pagination must still count base entities, not exploded rows.
		const departmentsQuerySet = querySet(snakeDb)
			.selectAs(
				"department",
				snakeDb
					.selectFrom("organizational_departments")
					.select(["id", "organization_id", "department_name"]),
			)
			.innerJoinOne(
				"parentOrganizationRecordForOrganizationalDepartment",
				({ eb, qs }) => qs(eb.selectFrom("organizations").select(["id", "organization_name"])),
				"parentOrganizationRecordForOrganizationalDepartment.id",
				"department.organization_id",
			)
			.innerJoinMany(
				"departmentalEmployeeRecords",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmental_employee_records")
							.select([
								"id",
								"organizational_department_id",
								"employee_preferred_full_display_name",
							]),
					),
				"departmentalEmployeeRecords.organizational_department_id",
				"department.id",
			)
			.orderBy("parentOrganizationRecordForOrganizationalDepartment$$organization_name", "desc")
			.limit(1);

		const expected = [
			{
				id: 2,
				organization_id: 2,
				department_name: "Marketing",
				parentOrganizationRecordForOrganizationalDepartment: {
					id: 2,
					organization_name: "Zenith Industries",
				},
				departmentalEmployeeRecords: [
					{
						id: 3,
						organizational_department_id: 2,
						employee_preferred_full_display_name: "Carol Chen",
					},
					{
						id: 4,
						organizational_department_id: 2,
						employee_preferred_full_display_name: "Dan Diaz",
					},
				],
			},
		];

		const departments = await departmentsQuerySet.execute();
		assert.deepStrictEqual(departments, expected);

		// Hydrating externally executed raw rows (the documented
		// toQuery().execute() + hydrate() flow) restores the same output.
		const rows = await departmentsQuerySet.toQuery().execute();
		assert.deepStrictEqual(await departmentsQuerySet.hydrate(rows), expected);

		// Count and exists queries are unaffected by alias encoding.
		assert.strictEqual(Number(await departmentsQuerySet.executeCount()), 2);
		assert.strictEqual(await departmentsQuerySet.executeExists(), true);
	});

	test("with CamelCasePlugin: pagination + offset + orderBy on over-long alias", async () => {
		// The orderBy alias
		// "parentOrganizationRecordForOrganizationalDepartment$$organizationName"
		// (69 chars) snake_cases past 63 bytes.
		const departments = await querySet(camelDb)
			.selectAs(
				"department",
				camelDb
					.selectFrom("organizationalDepartments")
					.select(["id", "organizationId", "departmentName"]),
			)
			.innerJoinOne(
				"parentOrganizationRecordForOrganizationalDepartment",
				({ eb, qs }) => qs(eb.selectFrom("organizations").select(["id", "organizationName"])),
				"parentOrganizationRecordForOrganizationalDepartment.id",
				"department.organizationId",
			)
			.innerJoinMany(
				"departmentalEmployeeRecords",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmentalEmployeeRecords")
							.select(["id", "organizationalDepartmentId", "employeePreferredFullDisplayName"]),
					),
				"departmentalEmployeeRecords.organizationalDepartmentId",
				"department.id",
			)
			.orderBy("parentOrganizationRecordForOrganizationalDepartment$$organizationName", "desc")
			.limit(1)
			.offset(1)
			.execute();

		// Ordered by organization name descending (Zenith, Acme); offset 1
		// picks the Acme department.
		assert.deepStrictEqual(departments, [
			{
				id: 1,
				organizationId: 1,
				departmentName: "Engineering",
				parentOrganizationRecordForOrganizationalDepartment: {
					id: 1,
					organizationName: "Acme Corporation",
				},
				departmentalEmployeeRecords: [
					{
						id: 1,
						organizationalDepartmentId: 1,
						employeePreferredFullDisplayName: "Alice Anderson",
					},
					{
						id: 2,
						organizationalDepartmentId: 1,
						employeePreferredFullDisplayName: "Bob Barker",
					},
				],
			},
		]);
	});

	test("nested keyBy on a column whose generated alias is over-long (and collides at 63 bytes)", async () => {
		// The nested collection is keyed by
		// "employee_secondary_contact_email_address", whose generated alias
		// shares its first 63 bytes with the display-name alias.  Keying (and
		// the keyBy tie-break ordering) must use the restored full-length
		// field.
		const departments = await querySet(snakeDb)
			.selectAs(
				"department",
				snakeDb.selectFrom("organizational_departments").select(["id", "department_name"]),
			)
			.where("organizational_departments.id", "=", 1)
			.innerJoinMany(
				"departmentalEmployeeRecordsWithVerboseNamingConventions",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmental_employee_records")
							.select([
								"organizational_department_id",
								"employee_preferred_full_display_name",
								"employee_secondary_contact_email_address",
							]),
						"employee_secondary_contact_email_address",
					),
				"departmentalEmployeeRecordsWithVerboseNamingConventions.organizational_department_id",
				"department.id",
			)
			.execute();

		assert.deepStrictEqual(departments, [
			{
				id: 1,
				department_name: "Engineering",
				departmentalEmployeeRecordsWithVerboseNamingConventions: [
					{
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Alice Anderson",
						employee_secondary_contact_email_address: "alice.anderson@example.com",
					},
					{
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Bob Barker",
						employee_secondary_contact_email_address: "bob.barker@example.com",
					},
				],
			},
		]);
	});

	test("attaches at the top level and nested under an over-long join", async () => {
		// The nested attach's fetchFn receives parent rows through the
		// prefixed accessor, which must see restored full-length field names.
		const departments = await querySet(snakeDb)
			.selectAs(
				"department",
				snakeDb
					.selectFrom("organizational_departments")
					.select(["id", "organization_id", "department_name"]),
			)
			.where("organizational_departments.id", "=", 1)
			.innerJoinMany(
				"departmentalEmployeeRecordsWithVerboseNamingConventions",
				({ eb, qs }) =>
					qs(
						eb
							.selectFrom("departmental_employee_records")
							.select([
								"id",
								"organizational_department_id",
								"employee_preferred_full_display_name",
							]),
					).attachMany(
						"employeeContactNotes",
						(employees) =>
							employees.map((employee) => ({
								employee_record_id: employee.id,
								note: `note for ${employee.employee_preferred_full_display_name}`,
							})),
						{ matchChild: "employee_record_id" },
					),
				"departmentalEmployeeRecordsWithVerboseNamingConventions.organizational_department_id",
				"department.id",
			)
			.attachOne(
				"attachedOrganization",
				(rows) => rows.map((row) => ({ id: row.organization_id, fetched: true })),
				{ matchChild: "id", toParent: "organization_id" },
			)
			.execute();

		assert.deepStrictEqual(departments, [
			{
				id: 1,
				organization_id: 1,
				department_name: "Engineering",
				departmentalEmployeeRecordsWithVerboseNamingConventions: [
					{
						id: 1,
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Alice Anderson",
						employeeContactNotes: [{ employee_record_id: 1, note: "note for Alice Anderson" }],
					},
					{
						id: 2,
						organizational_department_id: 1,
						employee_preferred_full_display_name: "Bob Barker",
						employeeContactNotes: [{ employee_record_id: 2, note: "note for Bob Barker" }],
					},
				],
				attachedOrganization: { id: 1, fetched: true },
			},
		]);
	});

	test("canonical-form alias collision throws at toQuery() time, before any SQL runs", () => {
		// The alias encoding is case- and underscore-insensitive (so it stays
		// stable under CamelCasePlugin's snake_case/camelize round trip), which
		// means two over-long sibling aliases differing only by underscores
		// encode to the same SQL identifier. That must throw when the query is
		// built — SQL with duplicate column aliases would otherwise reach the
		// database, where one column silently clobbers the other.
		const qs = querySet(snakeDb)
			.selectAs("department", snakeDb.selectFrom("organizational_departments").select(["id"]))
			.innerJoinMany(
				// 53 chars with 8 uppercase letters: worst-case 61 bytes, so both
				// prefixed aliases below are over-long (61 + 2 + 9 or 10 > 63).
				"employeeRecordsUnderAnExtremelyVerboseRelationKeyName",
				({ eb, qs }) =>
					qs(
						eb.selectFrom("departmental_employee_records").select([
							"id",
							"organizational_department_id",
							// Distinct columns whose aliases share a canonical form
							// ("createdat"): they encode to the same SQL identifier.
							"employee_preferred_full_display_name as created_at",
							"employee_secondary_contact_email_address as createdat",
						]),
					),
				"employeeRecordsUnderAnExtremelyVerboseRelationKeyName.organizational_department_id",
				"department.id",
			);

		assert.throws(() => qs.toQuery(), AliasCollisionError);
	});

	test("a plugin that transforms result values indiscriminately fails loudly", async () => {
		// Restoring encoded aliases pushes a synthetic row of numeric markers
		// through the plugins' transformResult. A plugin that converts values
		// without regard to column type or name corrupts those markers; that
		// must throw rather than silently dropping the renames (which would
		// mangle hydration of the affected columns).
		class NumbersToStringsPlugin implements KyselyPlugin {
			transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
				return args.node;
			}

			async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
				return {
					...args.result,
					rows: args.result.rows.map((row) =>
						Object.fromEntries(
							Object.entries(row).map(([key, value]) => [
								key,
								typeof value === "number" ? String(value) : value,
							]),
						),
					),
				};
			}
		}

		const pluginDb = db.withPlugin(new NumbersToStringsPlugin()).withTables<{
			organizational_departments: {
				id: number;
				organization_id: number;
				department_name: string;
			};
			departmental_employee_records: {
				id: number;
				organizational_department_id: number;
				employee_preferred_full_display_name: string;
			};
		}>();

		await assert.rejects(
			querySet(pluginDb)
				.selectAs(
					"department",
					pluginDb
						.selectFrom("organizational_departments")
						.select(["id", "organization_id", "department_name"]),
				)
				.where("organizational_departments.id", "=", 1)
				.innerJoinMany(
					"departmentalEmployeeRecordsWithVerboseNamingConventions",
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("departmental_employee_records")
								.select([
									"id",
									"organizational_department_id",
									"employee_preferred_full_display_name",
								]),
						),
					"departmentalEmployeeRecordsWithVerboseNamingConventions.organizational_department_id",
					"department.id",
				)
				.execute(),
			AliasRestorationError,
		);
	});
});
