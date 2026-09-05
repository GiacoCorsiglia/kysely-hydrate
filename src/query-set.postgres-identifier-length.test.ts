/**
 * PostgreSQL identifier-length (63-byte truncation) tests for QuerySet.
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1), emitting only a NOTICE. QuerySet builds prefixed column
 * aliases when nesting relations (`parent$$child$$column`), so with deep
 * nesting and/or long table/column names the generated alias exceeds 63
 * bytes. Postgres truncates the alias in the result set and hydration then
 * silently produces wrong output:
 *
 * 1. A truncated alias yields a mangled field name in the hydrated object
 *    (the hydrator strips the known prefix from the truncated row key and
 *    is left with a stub).
 * 2. Two aliases sharing their first 63 bytes truncate to the same
 *    identifier; the later column silently clobbers the earlier one.
 * 3. Same as 1 and 2 under Kysely's CamelCasePlugin, which snake_cases the
 *    alias before Postgres sees it (adding a byte per camel hump) and then
 *    camelizes the truncated key back.
 * 4. Under CamelCasePlugin only: a camelCase alias that is itself under 63
 *    bytes fails because its snake_case form is 64+ bytes.
 *
 * A special case of 1: when the nested KEY column's alias truncates, the
 * hydrator cannot find the key and hydrates matched left-join rows as null
 * (or a nested collection as empty).
 *
 * Every test asserts the CORRECT observable behavior (full field names, no
 * lost data, correct ordering) and says nothing about how the SQL identifiers
 * are kept legal. The fix under test is the `fixLongAliases()` plugin,
 * installed on the Kysely instance (wrapping `CamelCasePlugin` where one is
 * used). Without it, every test whose alias exceeds 63 bytes fails with
 * silently corrupted output.
 *
 * Every identifier in the fixture DDL (identifier-length-fixture.sql) is
 * itself under 63 bytes — only the generated alias chains are over-long.
 *
 * SQLite has no identifier-length limit, so this suite is Postgres-only.
 */

import assert from "node:assert";
import { describe, test } from "node:test";

import { CamelCasePlugin, type CamelCasePluginOptions } from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { describePg } from "./__tests__/helpers.ts";
import { fixLongAliases } from "./fix-long-aliases.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest({ fixture: "identifier-length-fixture" });

/** Asserts the byte length of a would-be SQL identifier, so that each test's premise is checked. */
function assertBytes(identifier: string, bytes: number) {
	assert.strictEqual(
		Buffer.byteLength(identifier, "utf8"),
		bytes,
		`expected "${identifier}" to be ${bytes} bytes`,
	);
}

describePg("query-set: postgres identifier length (63-byte truncation)", () => {
	//
	// Without CamelCasePlugin
	//

	const snakeDb = db.withPlugin(fixLongAliases()).withTables<{
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

	// Fixture rows as they hydrate when selecting (id, department id, display name).
	const alice = {
		id: 1,
		organizational_department_id: 1,
		employee_preferred_full_display_name: "Alice Anderson",
	};
	const bob = {
		id: 2,
		organizational_department_id: 1,
		employee_preferred_full_display_name: "Bob Barker",
	};
	const carol = {
		id: 3,
		organizational_department_id: 2,
		employee_preferred_full_display_name: "Carol Chen",
	};
	const dan = {
		id: 4,
		organizational_department_id: 2,
		employee_preferred_full_display_name: "Dan Diaz",
	};

	/**
	 * Department 1 with its two employees nested under `key`. The longest
	 * generated alias is `${key}$$employee_preferred_full_display_name`, so
	 * `key` controls exactly how long the alias gets.
	 */
	function selectEngineeringEmployeesUnder(key: string) {
		const employees = querySet(snakeDb).selectAs(
			"employee",
			snakeDb
				.selectFrom("departmental_employee_records")
				.select(["id", "organizational_department_id", "employee_preferred_full_display_name"]),
		);

		return querySet(snakeDb)
			.selectAs(
				"department",
				snakeDb.selectFrom("organizational_departments").select(["id", "department_name"]),
			)
			.where("organizational_departments.id", "=", 1)
			.innerJoinMany(key, employees, `${key}.organizational_department_id`, "department.id")
			.execute();
	}

	function engineeringWith(key: string, employees: unknown[]) {
		return [{ id: 1, department_name: "Engineering", [key]: employees }];
	}

	/**
	 * Department 1 with its two employees, including the two columns whose
	 * generated aliases share their first 63 bytes:
	 *   "<key>$$employee_preferred_full_display_name"     (93 bytes)
	 *   "<key>$$employee_secondary_contact_email_address" (97 bytes)
	 * Postgres truncates both to "<key>$$employ".
	 */
	function selectEngineeringEmployeesWithCollidingColumns() {
		return querySet(snakeDb)
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
			);
	}

	const engineeringWithCollidingColumns = [
		{
			id: 1,
			department_name: "Engineering",
			departmentalEmployeeRecordsWithVerboseNamingConventions: [
				{ ...alice, employee_secondary_contact_email_address: "alice.anderson@example.com" },
				{ ...bob, employee_secondary_contact_email_address: "bob.barker@example.com" },
			],
		},
	];

	/**
	 * All departments with their parent organization (a one-join whose alias
	 * "parentOrganizationRecordForOrganizationalDepartment$$organization_name"
	 * is 70 bytes) and their employees (a many-join), ordered by organization
	 * name descending. Pagination on top of a many-join makes QuerySet wrap
	 * the cardinality-one part in a subquery and re-hoist its columns, so the
	 * outer ORDER BY references the hoisted over-long alias — a different
	 * code path from the plain ORDER BY.
	 */
	function selectDepartmentsWithParentOrganization() {
		return querySet(snakeDb)
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
			.orderBy("parentOrganizationRecordForOrganizationalDepartment$$organization_name", "desc");
	}

	const engineeringWithParentOrganization = {
		id: 1,
		organization_id: 1,
		department_name: "Engineering",
		parentOrganizationRecordForOrganizationalDepartment: {
			id: 1,
			organization_name: "Acme Corporation",
		},
		departmentalEmployeeRecords: [alice, bob],
	};
	const marketingWithParentOrganization = {
		id: 2,
		organization_id: 2,
		department_name: "Marketing",
		parentOrganizationRecordForOrganizationalDepartment: {
			id: 2,
			organization_name: "Zenith Industries",
		},
		departmentalEmployeeRecords: [carol, dan],
	};

	describe("without CamelCasePlugin", () => {
		test("alias of exactly 63 bytes hydrates unchanged", async () => {
			assertBytes("departmentEmployeeRecords$$employee_preferred_full_display_name", 63);

			const departments = await selectEngineeringEmployeesUnder("departmentEmployeeRecords");

			assert.deepStrictEqual(
				departments,
				engineeringWith("departmentEmployeeRecords", [alice, bob]),
			);
		});

		test("alias of 64 bytes hydrates with full field names", async () => {
			assertBytes("departmentalEmployeeRoster$$employee_preferred_full_display_name", 64);

			const departments = await selectEngineeringEmployeesUnder("departmentalEmployeeRoster");

			assert.deepStrictEqual(
				departments,
				engineeringWith("departmentalEmployeeRoster", [alice, bob]),
			);
		});

		test("alias under 63 characters but over 63 bytes (multi-byte UTF-8) hydrates with full field names", async () => {
			// Postgres measures identifiers in bytes; "ü" is one character but
			// two bytes.
			const alias = "verknüpfteMitarbeiterAkte$$employee_preferred_full_display_name";
			assert.strictEqual(alias.length, 63);
			assertBytes(alias, 64);

			const departments = await selectEngineeringEmployeesUnder("verknüpfteMitarbeiterAkte");

			assert.deepStrictEqual(
				departments,
				engineeringWith("verknüpfteMitarbeiterAkte", [alice, bob]),
			);
		});

		test("two-level nesting: overflow already at the intermediate level hydrates fully", async () => {
			// The intermediate alias
			// "departmentalEmployeeRecords$$employee_preferred_full_display_name"
			// (65 bytes) is already over-long inside the departments subquery;
			// the top-level alias
			// "organizationalDepartments$$departmentalEmployeeRecords$$employee_preferred_full_display_name"
			// (92 bytes) is over-long again.
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
							departmentalEmployeeRecords: [alice, bob],
						},
					],
				},
			]);
		});

		test("two-level nesting: overflow only at the top level hydrates fully and orderBy on the deep column applies", async () => {
			// The intermediate alias
			// "grandparentOrganizationEntityOfTheDepartment$$organization_name"
			// is exactly 63 bytes (legal); only the top-level alias
			// "parentOrganizationalDepartment$$grandparentOrganizationEntityOfTheDepartment$$organization_name"
			// (95 bytes) is over-long. ORDER BY references that top-level alias.
			assertBytes("grandparentOrganizationEntityOfTheDepartment$$organization_name", 63);

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
							({ eb, qs }) =>
								qs(eb.selectFrom("organizations").select(["id", "organization_name"])),
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

			// Organization name descending (Zenith before Acme), then id.
			assert.deepStrictEqual(employees, [
				{ ...carol, parentOrganizationalDepartment: zenithMarketing },
				{ ...dan, parentOrganizationalDepartment: zenithMarketing },
				{ ...alice, parentOrganizationalDepartment: acmeEngineering },
				{ ...bob, parentOrganizationalDepartment: acmeEngineering },
			]);
		});

		test("three-level nesting: overflow first at an intermediate level hydrates fully", async () => {
			// Alias lengths per level for the great-grandchild column:
			//   "assignedDepartmentRecord$$department_name"                                (41 bytes, legal)
			//   "departmentalEmployeeRecords$$assignedDepartmentRecord$$department_name"   (70 bytes)
			//   "organizationalDepartments$$departmentalEmployeeRecords$$assignedDepartmentRecord$$department_name" (97 bytes)
			assertBytes("assignedDepartmentRecord$$department_name", 41);
			assertBytes("departmentalEmployeeRecords$$assignedDepartmentRecord$$department_name", 70);

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
								).innerJoinOne(
									"assignedDepartmentRecord",
									({ eb, qs }) =>
										qs(
											eb.selectFrom("organizational_departments").select(["id", "department_name"]),
										),
									"assignedDepartmentRecord.id",
									"departmentalEmployeeRecords.organizational_department_id",
								),
							"departmentalEmployeeRecords.organizational_department_id",
							"organizationalDepartments.id",
						),
					"organizationalDepartments.organization_id",
					"org.id",
				)
				.execute();

			const engineering = { id: 1, department_name: "Engineering" };

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
								{ ...alice, assignedDepartmentRecord: engineering },
								{ ...bob, assignedDepartmentRecord: engineering },
							],
						},
					],
				},
			]);
		});

		test("sibling columns whose aliases differ only after byte 63 are both hydrated", async () => {
			const departments = await selectEngineeringEmployeesWithCollidingColumns().execute();

			assert.deepStrictEqual(departments, engineeringWithCollidingColumns);
		});

		test("toJoinedQuery() rows carry every selected column's value", async () => {
			// Two department columns plus four employee columns per row; the
			// two colliding employee columns must both survive as distinct
			// values (the raw row shape is otherwise an implementation detail).
			const rows = await selectEngineeringEmployeesWithCollidingColumns().toJoinedQuery().execute();

			assert.strictEqual(rows.length, 2);
			for (const row of rows) {
				assert.strictEqual(Object.keys(row).length, 6);
			}
			const values = rows.flatMap((row) => Object.values(row));
			assert.ok(values.includes("Alice Anderson"));
			assert.ok(values.includes("alice.anderson@example.com"));
			assert.ok(values.includes("Bob Barker"));
			assert.ok(values.includes("bob.barker@example.com"));
		});

		test("orderBy on a nested one-join column with an over-long alias orders the results and hydrates fully", async () => {
			assertBytes("organizationalDepartmentAssignmentForThisEmployeeRecord$$department_name", 72);

			const employees = await querySet(snakeDb)
				.selectAs(
					"employee",
					snakeDb
						.selectFrom("departmental_employee_records")
						.select(["id", "organizational_department_id", "employee_preferred_full_display_name"]),
				)
				.innerJoinOne(
					"organizationalDepartmentAssignmentForThisEmployeeRecord",
					({ eb, qs }) =>
						qs(eb.selectFrom("organizational_departments").select(["id", "department_name"])),
					"organizationalDepartmentAssignmentForThisEmployeeRecord.id",
					"employee.organizational_department_id",
				)
				.orderBy("organizationalDepartmentAssignmentForThisEmployeeRecord$$department_name", "desc")
				.execute();

			const engineering = { id: 1, department_name: "Engineering" };
			const marketing = { id: 2, department_name: "Marketing" };

			// Department name descending (Marketing before Engineering), then id.
			assert.deepStrictEqual(employees, [
				{ ...carol, organizationalDepartmentAssignmentForThisEmployeeRecord: marketing },
				{ ...dan, organizationalDepartmentAssignmentForThisEmployeeRecord: marketing },
				{ ...alice, organizationalDepartmentAssignmentForThisEmployeeRecord: engineering },
				{ ...bob, organizationalDepartmentAssignmentForThisEmployeeRecord: engineering },
			]);
		});

		test("leftJoinOne whose key column alias is over-long hydrates matches as objects and non-matches as null", async () => {
			// Even the nested key column's alias "<key>$$id" is over-long (64
			// bytes). The hydrator decides between "matched" and "null" by that
			// key column, so truncating it must not turn matched rows into null.
			const key = "organizationalDepartmentAssignmentRecordForThisEmployeeIfAny";
			assertBytes(`${key}$$id`, 64);

			// Only department 1 is joinable, so Marketing's employees have no match.
			const employees = await querySet(snakeDb)
				.selectAs(
					"employee",
					snakeDb
						.selectFrom("departmental_employee_records")
						.select(["id", "organizational_department_id", "employee_preferred_full_display_name"]),
				)
				.leftJoinOne(
					key,
					({ eb, qs }) =>
						qs(
							eb
								.selectFrom("organizational_departments")
								.select(["id", "department_name"])
								.where("organizational_departments.id", "=", 1),
						),
					`${key}.id`,
					"employee.organizational_department_id",
				)
				.execute();

			const engineering = { id: 1, department_name: "Engineering" };

			assert.deepStrictEqual(employees, [
				{ ...alice, [key]: engineering },
				{ ...bob, [key]: engineering },
				{ ...carol, [key]: null },
				{ ...dan, [key]: null },
			]);
		});

		test("pagination with a many-join and orderBy on an over-long alias returns the right page, ordered and hydrated", async () => {
			assertBytes("parentOrganizationRecordForOrganizationalDepartment$$organization_name", 70);

			const firstPage = await selectDepartmentsWithParentOrganization().limit(1).execute();
			assert.deepStrictEqual(firstPage, [marketingWithParentOrganization]);

			const secondPage = await selectDepartmentsWithParentOrganization()
				.limit(1)
				.offset(1)
				.execute();
			assert.deepStrictEqual(secondPage, [engineeringWithParentOrganization]);
		});

		test("hydrate() restores rows executed through toQuery()", async () => {
			const departments = selectDepartmentsWithParentOrganization().limit(1);

			const rows = await departments.toQuery().execute();

			assert.deepStrictEqual(await departments.hydrate(rows), [marketingWithParentOrganization]);
		});

		test("hydrate() accepts rows executed by an identically built query set", async () => {
			// hydrate() is documented for rows that come from elsewhere (another
			// query, a cache), so two equal query sets must agree on the row
			// shape.
			const rows = await selectDepartmentsWithParentOrganization().toQuery().execute();

			assert.deepStrictEqual(await selectDepartmentsWithParentOrganization().hydrate(rows), [
				marketingWithParentOrganization,
				engineeringWithParentOrganization,
			]);
		});

		test("executeCount() and executeExists() are unaffected by over-long aliases", async () => {
			const departments = selectDepartmentsWithParentOrganization().limit(1);

			assert.strictEqual(await departments.executeCount(Number), 2);
			assert.strictEqual(await departments.executeExists(), true);
		});

		test("nested keyBy on a column whose alias collides at 63 bytes keys by the full column", async () => {
			// The nested collection is keyed by
			// "employee_secondary_contact_email_address", whose generated alias
			// shares its first 63 bytes with the display-name alias.
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

		test("attaches at the top level and nested under an over-long join receive full field names", async () => {
			// The nested attach's fetchFn receives the parent rows through the
			// prefixed accessor, which must expose full-length field names.
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
							...alice,
							employeeContactNotes: [{ employee_record_id: 1, note: "note for Alice Anderson" }],
						},
						{
							...bob,
							employeeContactNotes: [{ employee_record_id: 2, note: "note for Bob Barker" }],
						},
					],
					attachedOrganization: { id: 1, fetched: true },
				},
			]);
		});
	});

	//
	// With CamelCasePlugin
	//

	type CamelTables = {
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
	};

	function camelDbWith(options?: CamelCasePluginOptions) {
		return db.withPlugin(fixLongAliases(new CamelCasePlugin(options))).withTables<CamelTables>();
	}

	const camelDb = camelDbWith();

	const camelAlice = {
		id: 1,
		organizationalDepartmentId: 1,
		employeePreferredFullDisplayName: "Alice Anderson",
	};
	const camelBob = {
		id: 2,
		organizationalDepartmentId: 1,
		employeePreferredFullDisplayName: "Bob Barker",
	};
	const camelCarol = {
		id: 3,
		organizationalDepartmentId: 2,
		employeePreferredFullDisplayName: "Carol Chen",
	};
	const camelDan = {
		id: 4,
		organizationalDepartmentId: 2,
		employeePreferredFullDisplayName: "Dan Diaz",
	};

	/**
	 * CamelCasePlugin counterpart of `selectEngineeringEmployeesUnder`: the
	 * longest generated alias is `${key}$$employeePreferredFullDisplayName`,
	 * which the plugin snake_cases before Postgres sees it.
	 */
	function selectCamelEngineeringEmployeesUnder(
		camelDb: ReturnType<typeof camelDbWith>,
		key: string,
	) {
		const employees = querySet(camelDb).selectAs(
			"employee",
			camelDb
				.selectFrom("departmentalEmployeeRecords")
				.select(["id", "organizationalDepartmentId", "employeePreferredFullDisplayName"]),
		);

		return querySet(camelDb)
			.selectAs(
				"department",
				camelDb.selectFrom("organizationalDepartments").select(["id", "departmentName"]),
			)
			.where("organizationalDepartments.id", "=", 1)
			.innerJoinMany(key, employees, `${key}.organizationalDepartmentId`, "department.id")
			.execute();
	}

	function camelEngineeringWith(key: string, employees: unknown[]) {
		return [{ id: 1, departmentName: "Engineering", [key]: employees }];
	}

	describe("with CamelCasePlugin (default options)", () => {
		test("alias whose snake_case form is exactly 63 bytes hydrates unchanged", async () => {
			assertBytes("employee_directory_fy2024$$employee_preferred_full_display_name", 63);

			const departments = await selectCamelEngineeringEmployeesUnder(
				camelDb,
				"employeeDirectoryFy2024",
			);

			assert.deepStrictEqual(
				departments,
				camelEngineeringWith("employeeDirectoryFy2024", [camelAlice, camelBob]),
			);
		});

		test("camelCase alias under 63 bytes whose snake_case form is 64 bytes hydrates with full field names", async () => {
			// The JS-visible alias is legal on its own; only the plugin's
			// snake_case form exceeds the limit.
			assertBytes("employeeDirectoryEntries$$employeePreferredFullDisplayName", 58);
			assertBytes("employee_directory_entries$$employee_preferred_full_display_name", 64);

			const departments = await selectCamelEngineeringEmployeesUnder(
				camelDb,
				"employeeDirectoryEntries",
			);

			assert.deepStrictEqual(
				departments,
				camelEngineeringWith("employeeDirectoryEntries", [camelAlice, camelBob]),
			);
		});

		test("two-level nesting hydrates with full camelCase field names", async () => {
			// "organizationalDepartments$$departmentalEmployeeRecords$$employeePreferredFullDisplayName"
			// (88 bytes) snake_cases to 95 bytes.
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
										.select([
											"id",
											"organizationalDepartmentId",
											"employeePreferredFullDisplayName",
										]),
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
							departmentalEmployeeRecords: [camelAlice, camelBob],
						},
					],
				},
			]);
		});

		test("sibling columns whose snake_cased aliases share their first 63 bytes are all hydrated", async () => {
			// The snake_cased key plus "$$" is exactly 63 bytes, so EVERY nested
			// column alias truncates to the same identifier.
			assertBytes("departmental_employee_records_with_verbose_naming_conventions$$", 63);

			const departments = await querySet(camelDb)
				.selectAs(
					"department",
					camelDb.selectFrom("organizationalDepartments").select(["id", "departmentName"]),
				)
				.where("organizationalDepartments.id", "=", 1)
				.innerJoinMany(
					"departmentalEmployeeRecordsWithVerboseNamingConventions",
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
					"departmentalEmployeeRecordsWithVerboseNamingConventions.organizationalDepartmentId",
					"department.id",
				)
				.execute();

			assert.deepStrictEqual(departments, [
				{
					id: 1,
					departmentName: "Engineering",
					departmentalEmployeeRecordsWithVerboseNamingConventions: [
						{ ...camelAlice, employeeSecondaryContactEmailAddress: "alice.anderson@example.com" },
						{ ...camelBob, employeeSecondaryContactEmailAddress: "bob.barker@example.com" },
					],
				},
			]);
		});

		test("orderBy on a nested one-join column whose snake_cased alias exceeds 63 bytes orders the results and hydrates fully", async () => {
			assertBytes(
				"organizational_department_assignment_for_this_employee_record$$department_name",
				78,
			);

			const employees = await querySet(camelDb)
				.selectAs(
					"employee",
					camelDb
						.selectFrom("departmentalEmployeeRecords")
						.select(["id", "organizationalDepartmentId", "employeePreferredFullDisplayName"]),
				)
				.innerJoinOne(
					"organizationalDepartmentAssignmentForThisEmployeeRecord",
					({ eb, qs }) =>
						qs(eb.selectFrom("organizationalDepartments").select(["id", "departmentName"])),
					"organizationalDepartmentAssignmentForThisEmployeeRecord.id",
					"employee.organizationalDepartmentId",
				)
				.orderBy("organizationalDepartmentAssignmentForThisEmployeeRecord$$departmentName", "desc")
				.execute();

			const engineering = { id: 1, departmentName: "Engineering" };
			const marketing = { id: 2, departmentName: "Marketing" };

			assert.deepStrictEqual(employees, [
				{ ...camelCarol, organizationalDepartmentAssignmentForThisEmployeeRecord: marketing },
				{ ...camelDan, organizationalDepartmentAssignmentForThisEmployeeRecord: marketing },
				{ ...camelAlice, organizationalDepartmentAssignmentForThisEmployeeRecord: engineering },
				{ ...camelBob, organizationalDepartmentAssignmentForThisEmployeeRecord: engineering },
			]);
		});

		test("pagination with a many-join and orderBy on an over-long snake_cased alias returns the right page, ordered and hydrated", async () => {
			assertBytes(
				"parent_organization_record_for_organizational_department$$organization_name",
				75,
			);

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

			// Organization name descending (Zenith, Acme); offset 1 picks Acme.
			assert.deepStrictEqual(departments, [
				{
					id: 1,
					organizationId: 1,
					departmentName: "Engineering",
					parentOrganizationRecordForOrganizationalDepartment: {
						id: 1,
						organizationName: "Acme Corporation",
					},
					departmentalEmployeeRecords: [camelAlice, camelBob],
				},
			]);
		});
	});

	describe("with CamelCasePlugin options", () => {
		// `upperCase: true` is a separate, known Kysely incompatibility and is
		// deliberately not covered here.

		describe("underscoreBeforeDigits", () => {
			const digitsDb = camelDbWith({ underscoreBeforeDigits: true });

			test("alias whose snake_case form is exactly 63 bytes hydrates unchanged", async () => {
				assertBytes("personnel_records_fy_2024$$employee_preferred_full_display_name", 63);

				const departments = await selectCamelEngineeringEmployeesUnder(
					digitsDb,
					"personnelRecordsFy2024",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("personnelRecordsFy2024", [camelAlice, camelBob]),
				);
			});

			test("the extra underscore pushing a 63-byte alias to 64 bytes still hydrates with full field names", async () => {
				// Legal (63 bytes) under the default options, 64 bytes with the
				// underscore before "2024".
				assertBytes("employee_directory_fy_2024$$employee_preferred_full_display_name", 64);

				const departments = await selectCamelEngineeringEmployeesUnder(
					digitsDb,
					"employeeDirectoryFy2024",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("employeeDirectoryFy2024", [camelAlice, camelBob]),
				);
			});
		});

		describe("underscoreBetweenUppercaseLetters", () => {
			const uppercaseDb = camelDbWith({ underscoreBetweenUppercaseLetters: true });

			test("alias whose snake_case form is exactly 63 bytes hydrates unchanged", async () => {
				assertBytes("departmental_employee_h_r$$employee_preferred_full_display_name", 63);

				const departments = await selectCamelEngineeringEmployeesUnder(
					uppercaseDb,
					"departmentalEmployeeHR",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("departmentalEmployeeHR", [camelAlice, camelBob]),
				);
			});

			test("the extra underscore pushing a 63-byte alias to 64 bytes still hydrates with full field names", async () => {
				assertBytes("departmental_employees_h_r$$employee_preferred_full_display_name", 64);

				const departments = await selectCamelEngineeringEmployeesUnder(
					uppercaseDb,
					"departmentalEmployeesHR",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("departmentalEmployeesHR", [camelAlice, camelBob]),
				);
			});
		});

		describe("underscoreBeforeDigits + underscoreBetweenUppercaseLetters", () => {
			const bothDb = camelDbWith({
				underscoreBeforeDigits: true,
				underscoreBetweenUppercaseLetters: true,
			});

			test("alias whose snake_case form is exactly 63 bytes hydrates unchanged", async () => {
				assertBytes("department_staff_h_r_2024$$employee_preferred_full_display_name", 63);

				const departments = await selectCamelEngineeringEmployeesUnder(
					bothDb,
					"departmentStaffHR2024",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("departmentStaffHR2024", [camelAlice, camelBob]),
				);
			});

			test("the extra underscores pushing a 63-byte alias to 65 bytes still hydrate with full field names", async () => {
				// Legal (63 bytes) under the default options.
				assertBytes("departmental_staff_hr2024$$employee_preferred_full_display_name", 63);
				assertBytes("departmental_staff_h_r_2024$$employee_preferred_full_display_name", 65);

				const departments = await selectCamelEngineeringEmployeesUnder(
					bothDb,
					"departmentalStaffHR2024",
				);

				assert.deepStrictEqual(
					departments,
					camelEngineeringWith("departmentalStaffHR2024", [camelAlice, camelBob]),
				);
			});
		});
	});
});
