-- Fixture for src/query-set.postgres-identifier-length.test.ts.
--
-- Tables and columns with long — but individually legal (< 63 byte) —
-- identifiers. The generated join aliases (`key$$nestedKey$$column`) built
-- from these names exceed PostgreSQL's 63-byte identifier limit
-- (NAMEDATALEN - 1), which Postgres silently truncates with only a NOTICE.

CREATE TABLE IF NOT EXISTS organizations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS organizational_departments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organization_id INTEGER NOT NULL,
  department_name TEXT NOT NULL,
  FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS departmental_employee_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  organizational_department_id INTEGER NOT NULL,
  employee_preferred_full_display_name TEXT NOT NULL,
  employee_secondary_contact_email_address TEXT NOT NULL,
  FOREIGN KEY (organizational_department_id) REFERENCES organizational_departments(id) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO organizations (id, organization_name) VALUES
  (1, 'Acme Corporation');

INSERT INTO organizational_departments (id, organization_id, department_name) VALUES
  (1, 1, 'Engineering');

INSERT INTO departmental_employee_records
  (id, organizational_department_id, employee_preferred_full_display_name, employee_secondary_contact_email_address)
VALUES
  (1, 1, 'Alice Anderson', 'alice.anderson@example.com'),
  (2, 1, 'Bob Barker', 'bob.barker@example.com');
