import { ColumnType } from "./column-type.ts";

export abstract class SQLiteColumnType<
	SelectType,
	InsertType,
	UpdateType,
	DriverType,
	JsonType,
> extends ColumnType<SelectType, InsertType, UpdateType, DriverType, JsonType> {
	readonly dialect = "sqlite";
}

// Integer-based types

export class SQLiteInteger extends SQLiteColumnType<number, number, number, number, number> {
	readonly sqlType = "integer";
}

export function integer(): SQLiteInteger {
	return new SQLiteInteger();
}

export interface SQLiteTimestampConfig {
	mode?: "timestamp" | "timestamp_ms";
}

export class SQLiteTimestamp extends SQLiteColumnType<
	Date,
	Date | number,
	Date | number,
	number,
	number
> {
	readonly sqlType = "integer";
	readonly mode: "timestamp" | "timestamp_ms";

	constructor(config?: SQLiteTimestampConfig) {
		super();
		this.mode = config?.mode ?? "timestamp_ms";
	}

	fromDriver(input: number): Date {
		if (this.mode === "timestamp") {
			return new Date(input * 1000);
		}
		return new Date(input);
	}
}

export function timestamp(config?: SQLiteTimestampConfig): SQLiteTimestamp {
	return new SQLiteTimestamp(config);
}

export class SQLiteBoolean extends SQLiteColumnType<boolean, boolean, boolean, number, number> {
	readonly sqlType = "integer";

	fromDriver(input: number): boolean {
		return Number(input) === 1;
	}
}

export function boolean(): SQLiteBoolean {
	return new SQLiteBoolean();
}

// Real type

export class SQLiteReal extends SQLiteColumnType<number, number, number, number, number> {
	readonly sqlType = "real";
}

export function real(): SQLiteReal {
	return new SQLiteReal();
}

// Text-based types

export class SQLiteText extends SQLiteColumnType<string, string, string, string, string> {
	readonly sqlType = "text";
}

export function text(): SQLiteText {
	return new SQLiteText();
}

export class SQLiteTextJson<T = unknown> extends SQLiteColumnType<T, T, T, string, string> {
	readonly sqlType = "text";

	fromDriver(input: string): T {
		return JSON.parse(input);
	}
}

export function json<T = unknown>(): SQLiteTextJson<T> {
	return new SQLiteTextJson<T>();
}

// Blob-based types

export class SQLiteBlob extends SQLiteColumnType<
	Buffer,
	Buffer | Uint8Array,
	Buffer | Uint8Array,
	Buffer,
	string
> {
	readonly sqlType = "blob";

	fromDriver(input: Buffer): Buffer {
		if (Buffer.isBuffer(input)) {
			return input;
		}
		return Buffer.from(input as Uint8Array);
	}

	fromJson(input: string): Buffer {
		// Base64 encoded
		return Buffer.from(input, "base64");
	}
}

export function blob(): SQLiteBlob {
	return new SQLiteBlob();
}

export class SQLiteBlobBigInt extends SQLiteColumnType<
	bigint,
	bigint | number | string,
	bigint | number | string,
	Buffer,
	string
> {
	readonly sqlType = "blob";

	fromDriver(input: Buffer): bigint {
		if (Buffer.isBuffer(input)) {
			return BigInt(input.toString("utf8"));
		}
		// Handle Uint8Array
		const decoder = new TextDecoder("utf-8");
		return BigInt(decoder.decode(input));
	}

	fromJson(input: string): bigint {
		return BigInt(input);
	}
}

export function blobBigInt(): SQLiteBlobBigInt {
	return new SQLiteBlobBigInt();
}

export class SQLiteBlobJson<T = unknown> extends SQLiteColumnType<T, T, T, Buffer, string> {
	readonly sqlType = "blob";

	fromDriver(input: Buffer): T {
		if (Buffer.isBuffer(input)) {
			return JSON.parse(input.toString("utf8"));
		}
		// Handle Uint8Array
		const decoder = new TextDecoder("utf-8");
		return JSON.parse(decoder.decode(input));
	}

	fromJson(input: string): T {
		return JSON.parse(input);
	}
}

export function blobJson<T = unknown>(): SQLiteBlobJson<T> {
	return new SQLiteBlobJson<T>();
}

// Numeric types

export class SQLiteNumeric extends SQLiteColumnType<
	string,
	string | number,
	string | number,
	string,
	string
> {
	readonly sqlType = "numeric";

	fromDriver(input: string): string {
		if (typeof input === "string") {
			return input;
		}
		return String(input);
	}
}

export function numeric(): SQLiteNumeric {
	return new SQLiteNumeric();
}

export class SQLiteNumericNumber extends SQLiteColumnType<number, number, number, string, number> {
	readonly sqlType = "numeric";

	fromDriver(input: string): number {
		if (typeof input === "number") {
			return input;
		}
		return Number(input);
	}
}

export function numericNumber(): SQLiteNumericNumber {
	return new SQLiteNumericNumber();
}

export class SQLiteNumericBigInt extends SQLiteColumnType<
	bigint,
	bigint | number | string,
	bigint | number | string,
	string,
	string
> {
	readonly sqlType = "numeric";

	fromDriver(input: string): bigint {
		return BigInt(input);
	}
}

export function numericBigInt(): SQLiteNumericBigInt {
	return new SQLiteNumericBigInt();
}
