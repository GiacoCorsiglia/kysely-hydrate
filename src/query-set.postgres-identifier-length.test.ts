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
 * These tests REPRODUCE the bug: they currently FAIL against Postgres and
 * are written to PASS once the library handles long generated aliases.
 *
 * Every identifier in the fixture DDL (identifier-length-fixture.sql) is
 * itself under 63 bytes — only the generated alias chains are over-long.
 *
 * SQLite has no identifier-length limit, so this suite is Postgres-only.
 */

import assert from "node:assert";
import { test } from "node:test";

import { CamelCasePlugin } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg } from "./__tests__/helpers.ts";
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
