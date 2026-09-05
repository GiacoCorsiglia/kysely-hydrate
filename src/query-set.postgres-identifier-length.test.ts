/**
 * PostgreSQL identifier-length (63-byte truncation) tests for QuerySet.
 *
 * PostgreSQL silently truncates identifiers longer than 63 bytes
 * (NAMEDATALEN - 1), emitting only a NOTICE. QuerySet builds prefixed column
 * aliases when nesting relations (`parent$$child$$column`), so deep nesting or
 * long names push the generated alias past 63 bytes, and hydration then
 * silently produces wrong output:
 *
 * 1. A truncated alias yields a mangled field name in the hydrated object. If
 *    it is the nested KEY column, the hydrator cannot find the key and treats
 *    matched left-join rows as null (or a nested collection as empty).
 * 2. Two aliases sharing their first 63 bytes truncate to the same identifier;
 *    the later column silently clobbers the earlier one.
 * 3. The same as 1 and 2 under Kysely's CamelCasePlugin, which snake_cases the
 *    alias before Postgres sees it (adding a byte per camel hump).
 * 4. Under CamelCasePlugin only: an alias that is itself under 63 bytes fails
 *    because its snake_case form is 64+ bytes.
 *
 * Every test asserts the CORRECT observable behavior (full field names, no
 * lost data, correct ordering) and says nothing about how the SQL identifiers
 * are kept legal. The fix under test is the `fixLongAliases()` plugin,
 * installed on the Kysely instance (wrapping `CamelCasePlugin` where one is
 * used). Every identifier in the fixture DDL is itself under 63 bytes; only
 * the generated alias chains are over-long.
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

/** Checks a test's premise: the byte length of a would-be SQL identifier. */
function assertBytes(identifier: string, bytes: number) {
	assert.strictEqual(
		Buffer.byteLength(identifier),
		bytes,
		`"${identifier}" should be ${bytes} bytes`,
	);
}

describePg("query-set: postgres identifier length (63-byte truncation)", () => {
	describe("without CamelCasePlugin", () => {
		const snakeDb = db.withPlugin(fixLongAliases()).withTables<{
			organizations: { id: number; organization_name: string };
			organizational_departments: { id: number; organization_id: number; department_name: string };
			departmental_employee_records: {
				id: number;
				organizational_department_id: number;
				employee_preferred_full_display_name: string;
				employee_secondary_contact_email_address: string;
			};
		}>();

		const acme = { id: 1, organization_name: "Acme Corporation" };
		const zenith = { id: 2, organization_name: "Zenith Industries" };
		const engineering = { id: 1, organization_id: 1, department_name: "Engineering" };
		const marketing = { id: 2, organization_id: 2, department_name: "Marketing" };
		const employee = (id: number, departmentId: number, name: string) => ({
			id,
			organizational_department_id: departmentId,
			employee_preferred_full_display_name: name,
		});
		const [alice, bob, carol, dan] = [
			employee(1, 1, "Alice Anderson"),
			employee(2, 1, "Bob Barker"),
			employee(3, 2, "Carol Chen"),
			employee(4, 2, "Dan Diaz"),
		];
		const withEmail = <T extends object>(record: T, email: string) => ({
			...record,
			employee_secondary_contact_email_address: email,
		});

		const organizations = querySet(snakeDb).selectAs(
			"org",
			snakeDb.selectFrom("organizations").select(["id", "organization_name"]),
		);
		const departments = querySet(snakeDb).selectAs(
			"department",
			snakeDb
				.selectFrom("organizational_departments")
				.select(["id", "organization_id", "department_name"]),
		);
		const employees = querySet(snakeDb).selectAs(
			"employee",
			snakeDb
				.selectFrom("departmental_employee_records")
				.select(["id", "organizational_department_id", "employee_preferred_full_display_name"]),
		);
		const employeesWithEmail = querySet(snakeDb).selectAs(
			"employee",
			snakeDb
				.selectFrom("departmental_employee_records")
				.select([
					"id",
					"organizational_department_id",
					"employee_preferred_full_display_name",
					"employee_secondary_contact_email_address",
				]),
		);
		const acmeOnly = organizations.where("organizations.id", "=", 1);
		const engineeringOnly = departments.where("organizational_departments.id", "=", 1);

		/** Department 1 with its employees under `key`, whose longest alias is `${key}$$${NAME}`. */
		const engineeringEmployeesUnder = (key: string) =>
			engineeringOnly
				.innerJoinMany(key, employees, `${key}.organizational_department_id`, "department.id")
				.execute();

		// Postgres measures identifiers in bytes, not characters: "ü" is two bytes.
		const NAME = "employee_preferred_full_display_name";
		for (const [key, bytes, chars] of [
			["departmentEmployeeRecords", 63, 63],
			["departmentalEmployeeRoster", 64, 64],
			["verknüpfteMitarbeiterAkte", 64, 63],
		] as const) {
			test(`alias of ${bytes} bytes (${chars} characters) hydrates with full field names`, async () => {
				assertBytes(`${key}$$${NAME}`, bytes);
				assert.strictEqual(`${key}$$${NAME}`.length, chars);

				assert.deepStrictEqual(await engineeringEmployeesUnder(key), [
					{ ...engineering, [key]: [alice, bob] },
				]);
			});
		}

		test("two-level nesting with overflow already at the intermediate level hydrates fully", async () => {
			assertBytes(`departmentalEmployeeRecords$$${NAME}`, 65);
			assertBytes(`organizationalDepartments$$departmentalEmployeeRecords$$${NAME}`, 92);

			const result = await acmeOnly
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
				.execute();

			assert.deepStrictEqual(result, [
				{
					...acme,
					organizationalDepartments: [
						{ ...engineering, departmentalEmployeeRecords: [alice, bob] },
					],
				},
			]);
		});

		test("two-level nesting with overflow only at the top level hydrates fully and orders by the deep column", async () => {
			const grandparent = "grandparentOrganizationEntityOfTheDepartment";
			const deepAlias = `parentOrganizationalDepartment$$${grandparent}$$organization_name`;
			assertBytes(`${grandparent}$$organization_name`, 63);
			assertBytes(deepAlias, 95);

			const result = await employees
				.innerJoinOne(
					"parentOrganizationalDepartment",
					departments.innerJoinOne(
						grandparent,
						organizations,
						`${grandparent}.id`,
						"department.organization_id",
					),
					"parentOrganizationalDepartment.id",
					"employee.organizational_department_id",
				)
				.orderBy(deepAlias, "desc")
				.execute();

			const zenithMarketing = { ...marketing, [grandparent]: zenith };
			const acmeEngineering = { ...engineering, [grandparent]: acme };
			assert.deepStrictEqual(result, [
				{ ...carol, parentOrganizationalDepartment: zenithMarketing },
				{ ...dan, parentOrganizationalDepartment: zenithMarketing },
				{ ...alice, parentOrganizationalDepartment: acmeEngineering },
				{ ...bob, parentOrganizationalDepartment: acmeEngineering },
			]);
		});

		test("three-level nesting with overflow first at an intermediate level hydrates fully", async () => {
			assertBytes("assignedDepartmentRecord$$department_name", 41);
			assertBytes("departmentalEmployeeRecords$$assignedDepartmentRecord$$department_name", 70);
			assertBytes(
				"organizationalDepartments$$departmentalEmployeeRecords$$assignedDepartmentRecord$$department_name",
				97,
			);

			// Built with the callback form, so that nesting path is covered too.
			const result = await acmeOnly
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
										.select(["id", "organizational_department_id", NAME]),
								).innerJoinOne(
									"assignedDepartmentRecord",
									({ eb, qs }) =>
										qs(
											eb
												.selectFrom("organizational_departments")
												.select(["id", "organization_id", "department_name"]),
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

			assert.deepStrictEqual(result, [
				{
					...acme,
					organizationalDepartments: [
						{
							...engineering,
							departmentalEmployeeRecords: [
								{ ...alice, assignedDepartmentRecord: engineering },
								{ ...bob, assignedDepartmentRecord: engineering },
							],
						},
					],
				},
			]);
		});

		// The two employee aliases under this key share their first 63 bytes.
		const verbose = "departmentalEmployeeRecordsWithVerboseNamingConventions";
		const verboseEmployees = engineeringOnly.innerJoinMany(
			verbose,
			employeesWithEmail,
			`${verbose}.organizational_department_id`,
			"department.id",
		);
		const aliceWithEmail = withEmail(alice, "alice.anderson@example.com");
		const bobWithEmail = withEmail(bob, "bob.barker@example.com");

		test("sibling columns whose aliases differ only after byte 63 are both hydrated", async () => {
			assertBytes(`${verbose}$$${NAME}`, 93);
			assertBytes(`${verbose}$$employee_secondary_contact_email_address`, 97);

			assert.deepStrictEqual(await verboseEmployees.execute(), [
				{ ...engineering, [verbose]: [aliceWithEmail, bobWithEmail] },
			]);
		});

		test("toJoinedQuery() rows carry every selected column's value", async () => {
			const rows = await verboseEmployees.toJoinedQuery().execute();

			// Three department columns plus four employee columns; the raw row
			// shape is otherwise an implementation detail.
			assert.strictEqual(rows.length, 2);
			for (const row of rows) {
				assert.strictEqual(Object.keys(row).length, 7);
			}
			const values = rows.flatMap((row) => Object.values(row));
			for (const value of [
				"Alice Anderson",
				"alice.anderson@example.com",
				"Bob Barker",
				"bob.barker@example.com",
			]) {
				assert.ok(values.includes(value), value);
			}
		});

		test("nested keyBy on a column whose alias collides at 63 bytes keys by the full column", async () => {
			const employeesByEmail = querySet(snakeDb).selectAs(
				"employee",
				employeesWithEmail.toBaseQuery(),
				"employee_secondary_contact_email_address",
			);

			const result = await engineeringOnly
				.innerJoinMany(
					verbose,
					employeesByEmail,
					`${verbose}.organizational_department_id`,
					"department.id",
				)
				.execute();

			assert.deepStrictEqual(result, [
				{ ...engineering, [verbose]: [aliceWithEmail, bobWithEmail] },
			]);
		});

		test("orderBy on a nested one-join column with an over-long alias orders the results and hydrates fully", async () => {
			const key = "organizationalDepartmentAssignmentForThisEmployeeRecord";
			assertBytes(`${key}$$department_name`, 72);

			const result = await employees
				.innerJoinOne(key, departments, `${key}.id`, "employee.organizational_department_id")
				.orderBy(`${key}$$department_name`, "desc")
				.execute();

			assert.deepStrictEqual(result, [
				{ ...carol, [key]: marketing },
				{ ...dan, [key]: marketing },
				{ ...alice, [key]: engineering },
				{ ...bob, [key]: engineering },
			]);
		});

		test("leftJoinOne whose key column alias is over-long hydrates matches as objects and non-matches as null", async () => {
			// The hydrator decides between "matched" and "null" by the nested key
			// column, whose alias "<key>$$id" is itself over-long here.
			const key = "organizationalDepartmentAssignmentRecordForThisEmployeeIfAny";
			assertBytes(`${key}$$id`, 64);

			// Only department 1 is joinable, so Marketing's employees have no match.
			const result = await employees
				.leftJoinOne(key, engineeringOnly, `${key}.id`, "employee.organizational_department_id")
				.execute();

			assert.deepStrictEqual(result, [
				{ ...alice, [key]: engineering },
				{ ...bob, [key]: engineering },
				{ ...carol, [key]: null },
				{ ...dan, [key]: null },
			]);
		});

		/**
		 * All departments with their parent organization (one-join) and
		 * employees (many-join), by organization name descending. Pagination on
		 * top of a many-join makes QuerySet wrap the cardinality-one part in a
		 * subquery and re-hoist its columns, so the outer ORDER BY references the
		 * hoisted over-long alias: a different code path from a plain ORDER BY.
		 */
		const parent = "parentOrganizationRecordForOrganizationalDepartment";
		const departmentsByOrganization = () =>
			departments
				.innerJoinOne(parent, organizations, `${parent}.id`, "department.organization_id")
				.innerJoinMany(
					"departmentalEmployeeRecords",
					employees,
					"departmentalEmployeeRecords.organizational_department_id",
					"department.id",
				)
				.orderBy(`${parent}$$organization_name`, "desc");
		const marketingByZenith = {
			...marketing,
			[parent]: zenith,
			departmentalEmployeeRecords: [carol, dan],
		};
		const engineeringByAcme = {
			...engineering,
			[parent]: acme,
			departmentalEmployeeRecords: [alice, bob],
		};

		test("pagination with a many-join and orderBy on an over-long alias returns the right page, ordered and hydrated", async () => {
			assertBytes(`${parent}$$organization_name`, 70);

			assert.deepStrictEqual(await departmentsByOrganization().limit(1).execute(), [
				marketingByZenith,
			]);
			assert.deepStrictEqual(await departmentsByOrganization().limit(1).offset(1).execute(), [
				engineeringByAcme,
			]);
		});

		test("hydrate() restores rows executed through toQuery()", async () => {
			const firstPage = departmentsByOrganization().limit(1);

			const rows = await firstPage.toQuery().execute();

			assert.deepStrictEqual(await firstPage.hydrate(rows), [marketingByZenith]);
		});

		test("hydrate() accepts rows executed by an identically built query set", async () => {
			// hydrate() is documented for rows that come from elsewhere (another
			// query, a cache), so two equal query sets must agree on the row shape.
			const rows = await departmentsByOrganization().toQuery().execute();

			assert.deepStrictEqual(await departmentsByOrganization().hydrate(rows), [
				marketingByZenith,
				engineeringByAcme,
			]);
		});

		test("executeCount() and executeExists() are unaffected by over-long aliases", async () => {
			const firstPage = departmentsByOrganization().limit(1);

			assert.strictEqual(await firstPage.executeCount(Number), 2);
			assert.strictEqual(await firstPage.executeExists(), true);
		});

		test("attaches at the top level and nested under an over-long join receive full field names", async () => {
			// The nested attach's fetchFn receives the parent rows through the
			// prefixed accessor, which must expose full-length field names.
			const result = await engineeringOnly
				.innerJoinMany(
					verbose,
					employees.attachMany(
						"employeeContactNotes",
						(rows) =>
							rows.map((row) => ({
								employee_record_id: row.id,
								note: `note for ${row.employee_preferred_full_display_name}`,
							})),
						{ matchChild: "employee_record_id" },
					),
					`${verbose}.organizational_department_id`,
					"department.id",
				)
				.attachOne(
					"attachedOrganization",
					(rows) => rows.map((row) => ({ id: row.organization_id, fetched: true })),
					{ matchChild: "id", toParent: "organization_id" },
				)
				.execute();

			assert.deepStrictEqual(result, [
				{
					...engineering,
					[verbose]: [
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

	describe("with CamelCasePlugin", () => {
		const camelDbWith = (options?: CamelCasePluginOptions) =>
			db.withPlugin(fixLongAliases(new CamelCasePlugin(options))).withTables<{
				organizations: { id: number; organizationName: string };
				organizationalDepartments: { id: number; organizationId: number; departmentName: string };
				departmentalEmployeeRecords: {
					id: number;
					organizationalDepartmentId: number;
					employeePreferredFullDisplayName: string;
					employeeSecondaryContactEmailAddress: string;
				};
			}>();
		type CamelDb = ReturnType<typeof camelDbWith>;
		const camelDb = camelDbWith();

		const acme = { id: 1, organizationName: "Acme Corporation" };
		const engineering = { id: 1, organizationId: 1, departmentName: "Engineering" };
		const marketing = { id: 2, organizationId: 2, departmentName: "Marketing" };
		const employee = (id: number, departmentId: number, name: string) => ({
			id,
			organizationalDepartmentId: departmentId,
			employeePreferredFullDisplayName: name,
		});
		const [alice, bob, carol, dan] = [
			employee(1, 1, "Alice Anderson"),
			employee(2, 1, "Bob Barker"),
			employee(3, 2, "Carol Chen"),
			employee(4, 2, "Dan Diaz"),
		];

		// Builders take the db because each CamelCasePlugin option set needs its own.
		const organizations = (db: CamelDb) =>
			querySet(db).selectAs(
				"org",
				db.selectFrom("organizations").select(["id", "organizationName"]),
			);
		const departments = (db: CamelDb) =>
			querySet(db).selectAs(
				"department",
				db
					.selectFrom("organizationalDepartments")
					.select(["id", "organizationId", "departmentName"]),
			);
		const employees = (db: CamelDb, ...extra: "employeeSecondaryContactEmailAddress"[]) =>
			querySet(db).selectAs(
				"employee",
				db
					.selectFrom("departmentalEmployeeRecords")
					.select([
						"id",
						"organizationalDepartmentId",
						"employeePreferredFullDisplayName",
						...extra,
					]),
			);

		/** CamelCasePlugin counterpart of `engineeringEmployeesUnder`. */
		const engineeringEmployeesUnder = (db: CamelDb, key: string) =>
			departments(db)
				.where("organizationalDepartments.id", "=", 1)
				.innerJoinMany(key, employees(db), `${key}.organizationalDepartmentId`, "department.id")
				.execute();

		// [CamelCasePlugin options, join key, the key's snake_case form, bytes of
		// the snake_cased alias, an extra (legal) alias form to measure]. The
		// options each add underscores, so one key can be legal under the
		// default options and over-long under another set.
		// `upperCase: true` is a separate, known Kysely incompatibility.
		const SNAKE_NAME = "$$employee_preferred_full_display_name";
		const digits = { underscoreBeforeDigits: true };
		const uppercase = { underscoreBetweenUppercaseLetters: true };
		const cases: [CamelCasePluginOptions, string, string, number, [string, number]?][] = [
			[{}, "employeeDirectoryFy2024", "employee_directory_fy2024", 63],
			// Legal as written; only the snake_case form is over-long.
			[
				{},
				"employeeDirectoryEntries",
				"employee_directory_entries",
				64,
				["employeeDirectoryEntries$$employeePreferredFullDisplayName", 58],
			],
			[digits, "personnelRecordsFy2024", "personnel_records_fy_2024", 63],
			[digits, "employeeDirectoryFy2024", "employee_directory_fy_2024", 64],
			[uppercase, "departmentalEmployeeHR", "departmental_employee_h_r", 63],
			[uppercase, "departmentalEmployeesHR", "departmental_employees_h_r", 64],
			[{ ...digits, ...uppercase }, "departmentStaffHR2024", "department_staff_h_r_2024", 63],
			[
				{ ...digits, ...uppercase },
				"departmentalStaffHR2024",
				"departmental_staff_h_r_2024",
				65,
				[`departmental_staff_hr2024${SNAKE_NAME}`, 63],
			],
		];
		for (const [options, key, snakeKey, bytes, alsoLegal] of cases) {
			const optionNames = Object.keys(options).join(" + ") || "default options";
			test(`${optionNames}: alias whose snake_case form is ${bytes} bytes hydrates with full camelCase field names (${key})`, async () => {
				assertBytes(`${snakeKey}${SNAKE_NAME}`, bytes);
				if (alsoLegal) {
					assertBytes(...alsoLegal);
				}

				assert.deepStrictEqual(await engineeringEmployeesUnder(camelDbWith(options), key), [
					{ ...engineering, [key]: [alice, bob] },
				]);
			});
		}

		test("two-level nesting hydrates with full camelCase field names", async () => {
			assertBytes(`organizational_departments$$departmental_employee_records${SNAKE_NAME}`, 95);

			// Built with the callback form, so that nesting path is covered too.
			const result = await organizations(camelDb)
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

			assert.deepStrictEqual(result, [
				{
					...acme,
					organizationalDepartments: [
						{ ...engineering, departmentalEmployeeRecords: [alice, bob] },
					],
				},
			]);
		});

		test("sibling columns whose snake_cased aliases share their first 63 bytes are all hydrated", async () => {
			// The snake_cased key plus "$$" is exactly 63 bytes, so EVERY nested
			// column alias truncates to the same identifier.
			const key = "departmentalEmployeeRecordsWithVerboseNamingConventions";
			assertBytes("departmental_employee_records_with_verbose_naming_conventions$$", 63);

			const result = await departments(camelDb)
				.where("organizationalDepartments.id", "=", 1)
				.innerJoinMany(
					key,
					employees(camelDb, "employeeSecondaryContactEmailAddress"),
					`${key}.organizationalDepartmentId`,
					"department.id",
				)
				.execute();

			assert.deepStrictEqual(result, [
				{
					...engineering,
					[key]: [
						{ ...alice, employeeSecondaryContactEmailAddress: "alice.anderson@example.com" },
						{ ...bob, employeeSecondaryContactEmailAddress: "bob.barker@example.com" },
					],
				},
			]);
		});

		test("orderBy on a nested one-join column whose snake_cased alias exceeds 63 bytes orders the results and hydrates fully", async () => {
			const key = "organizationalDepartmentAssignmentForThisEmployeeRecord";
			assertBytes(
				"organizational_department_assignment_for_this_employee_record$$department_name",
				78,
			);

			const result = await employees(camelDb)
				.innerJoinOne(key, departments(camelDb), `${key}.id`, "employee.organizationalDepartmentId")
				.orderBy(`${key}$$departmentName`, "desc")
				.execute();

			assert.deepStrictEqual(result, [
				{ ...carol, [key]: marketing },
				{ ...dan, [key]: marketing },
				{ ...alice, [key]: engineering },
				{ ...bob, [key]: engineering },
			]);
		});

		test("pagination with a many-join and orderBy on an over-long snake_cased alias returns the right page, ordered and hydrated", async () => {
			const parent = "parentOrganizationRecordForOrganizationalDepartment";
			assertBytes(
				"parent_organization_record_for_organizational_department$$organization_name",
				75,
			);

			const result = await departments(camelDb)
				.innerJoinOne(parent, organizations(camelDb), `${parent}.id`, "department.organizationId")
				.innerJoinMany(
					"departmentalEmployeeRecords",
					employees(camelDb),
					"departmentalEmployeeRecords.organizationalDepartmentId",
					"department.id",
				)
				.orderBy(`${parent}$$organizationName`, "desc")
				.limit(1)
				.offset(1)
				.execute();

			// Organization name descending (Zenith, Acme); offset 1 picks Acme.
			assert.deepStrictEqual(result, [
				{ ...engineering, [parent]: acme, departmentalEmployeeRecords: [alice, bob] },
			]);
		});
	});
});
