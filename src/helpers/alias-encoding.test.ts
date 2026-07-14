import assert from "node:assert";
import { describe, test } from "node:test";

import { encodeAlias, MAX_IDENTIFIER_BYTES, worstCaseIdentifierBytes } from "./alias-encoding.ts";

describe("helpers: alias-encoding", () => {
	describe("worstCaseIdentifierBytes", () => {
		test("plain lowercase identifiers count their UTF-8 bytes", () => {
			assert.strictEqual(worstCaseIdentifierBytes("posts$$id"), 9);
		});

		test("uppercase letters count double (potential snake_case expansion)", () => {
			// "aB" may become "a_b".
			assert.strictEqual(worstCaseIdentifierBytes("aB"), 3);
		});

		test("digits starting a run count double (underscoreBeforeDigits)", () => {
			// "a97" may become "a_97"; only the first digit of a run expands.
			assert.strictEqual(worstCaseIdentifierBytes("a97"), 4);
		});

		test("the first character never expands", () => {
			assert.strictEqual(worstCaseIdentifierBytes("B"), 1);
			assert.strictEqual(worstCaseIdentifierBytes("9a"), 2);
		});

		test("is invariant between camelCase and snake_case spellings", () => {
			assert.strictEqual(
				worstCaseIdentifierBytes("posts$$createdAt"),
				worstCaseIdentifierBytes("posts$$created_at"),
			);
			assert.strictEqual(
				worstCaseIdentifierBytes("employeeDirectoryEntries$$employeePreferredFullDisplayName"),
				worstCaseIdentifierBytes(
					"employee_directory_entries$$employee_preferred_full_display_name",
				),
			);
			// Digits with underscoreBeforeDigits: "a9" <-> "a_9".
			assert.strictEqual(worstCaseIdentifierBytes("a9"), worstCaseIdentifierBytes("a_9"));
		});

		test("counts multi-byte characters by UTF-8 length", () => {
			// é is 2 bytes in UTF-8.
			assert.strictEqual(worstCaseIdentifierBytes("é"), 2);
		});
	});

	describe("encodeAlias", () => {
		test("returns safely short aliases unchanged", () => {
			assert.strictEqual(encodeAlias("posts$$createdAt"), "posts$$createdAt");
			assert.strictEqual(encodeAlias("id"), "id");
		});

		test("encodes aliases whose worst-case length exceeds the limit, even under 63 raw chars", () => {
			// 58 chars, but snake_cases to 64 bytes.
			const alias = "employeeDirectoryEntries$$employeePreferredFullDisplayName";
			assert.notStrictEqual(encodeAlias(alias), alias);
		});

		test("encoded aliases stay within the limit and never expand", () => {
			const alias = `collection$$${"averyLongColumnName".repeat(8)}`;
			const encoded = encodeAlias(alias);
			assert.ok(worstCaseIdentifierBytes(encoded) <= MAX_IDENTIFIER_BYTES);
			// No uppercase letters or underscores: the encoded alias is a fixed
			// point of every camelCase/snake_case transformation.
			assert.match(encoded, /^[^A-Z_]+\$[a-z]{12}$/);
		});

		test("keeps a readable head of the original alias", () => {
			const alias = `organizationalDepartments$$departmentalEmployeeRecords$$employee_preferred_full_display_name`;
			const encoded = encodeAlias(alias);
			assert.ok(encoded.startsWith("organizationaldepartments$$departmentalemployee"));
		});

		test("is invariant across camelCase and snake_case spellings of the same alias", () => {
			assert.strictEqual(
				encodeAlias("employeeDirectoryEntries$$employeePreferredFullDisplayName"),
				encodeAlias("employee_directory_entries$$employee_preferred_full_display_name"),
			);
		});

		test("distinguishes aliases sharing the same first 63 bytes", () => {
			const prefix = "departmentalEmployeeRecordsWithVerboseNamingConventions$$";
			const a = encodeAlias(`${prefix}employee_preferred_full_display_name`);
			const b = encodeAlias(`${prefix}employee_secondary_contact_email_address`);
			assert.notStrictEqual(a, b);
		});

		test("truncates multi-byte heads without splitting characters", () => {
			const alias = `col$$${"é".repeat(80)}`;
			const encoded = encodeAlias(alias);
			assert.ok(Buffer.byteLength(encoded, "utf8") <= MAX_IDENTIFIER_BYTES);
			// Well-formed UTF-8 round-trips losslessly.
			assert.strictEqual(Buffer.from(encoded, "utf8").toString("utf8"), encoded);
		});

		test("is deterministic", () => {
			const alias = `x$$${"y".repeat(100)}`;
			assert.strictEqual(encodeAlias(alias), encodeAlias(alias));
		});
	});
});
