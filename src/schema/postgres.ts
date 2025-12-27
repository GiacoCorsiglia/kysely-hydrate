import { parse as parsePgArray } from "postgres-array";

import { ColumnType } from "./column-type.ts";

export abstract class PgColumnType<
	SelectType,
	InsertType,
	UpdateType,
	DriverType,
	JsonType,
> extends ColumnType<SelectType, InsertType, UpdateType, DriverType, JsonType> {
	readonly dialect = "postgres";
	abstract readonly sqlType: string;

	array(size?: number): PgArray<SelectType, InsertType, UpdateType, DriverType, JsonType> {
		return new PgArray(this, size);
	}
}

// Numeric types

export class PgBigInt extends PgColumnType<
	bigint,
	bigint | number | string,
	bigint | number | string,
	string,
	string
> {
	readonly sqlType = "bigint";

	fromDriver(input: string): bigint {
		return BigInt(input);
	}
}

export function bigint(): PgBigInt {
	return new PgBigInt();
}

export class PgBigSerial extends PgColumnType<
	bigint,
	bigint | number | string | undefined,
	bigint | number | string,
	string,
	string
> {
	readonly sqlType = "bigserial";

	fromDriver(input: string): bigint {
		return BigInt(input);
	}
}

export function bigserial(): PgBigSerial {
	return new PgBigSerial();
}

export class PgInteger extends PgColumnType<number, number, number, number, number> {
	readonly sqlType = "integer";
}

export function integer(): PgInteger {
	return new PgInteger();
}

export class PgSerial extends PgColumnType<number, number | undefined, number, number, number> {
	readonly sqlType = "serial";
}

export function serial(): PgSerial {
	return new PgSerial();
}

export class PgSmallInt extends PgColumnType<number, number, number, number, number> {
	readonly sqlType = "smallint";
}

export function smallint(): PgSmallInt {
	return new PgSmallInt();
}

export class PgSmallSerial extends PgColumnType<
	number,
	number | undefined,
	number,
	number,
	number
> {
	readonly sqlType = "smallserial";
}

export function smallserial(): PgSmallSerial {
	return new PgSmallSerial();
}

export class PgReal extends PgColumnType<number, number, number, number, number> {
	readonly sqlType = "real";
}

export function real(): PgReal {
	return new PgReal();
}

export class PgDoublePrecision extends PgColumnType<number, number, number, number, number> {
	readonly sqlType = "double precision";
}

export function doublePrecision(): PgDoublePrecision {
	return new PgDoublePrecision();
}

export interface PgNumericConfig {
	precision?: number;
	scale?: number;
}

export class PgNumeric extends PgColumnType<
	string,
	string | number,
	string | number,
	string,
	string
> {
	readonly precision: number | undefined;
	readonly scale: number | undefined;

	constructor(config?: PgNumericConfig) {
		super();
		this.precision = config?.precision;
		this.scale = config?.scale;
	}

	get sqlType(): string {
		if (this.precision !== undefined && this.scale !== undefined) {
			return `numeric(${this.precision},${this.scale})`;
		}
		if (this.precision !== undefined) {
			return `numeric(${this.precision})`;
		}
		return "numeric";
	}
}

export function numeric(config?: PgNumericConfig): PgNumeric {
	return new PgNumeric(config);
}

export const decimal = numeric;

// Boolean type

export class PgBoolean extends PgColumnType<boolean, boolean, boolean, boolean, boolean> {
	readonly sqlType = "boolean";
}

export function boolean(): PgBoolean {
	return new PgBoolean();
}

// String types

export class PgText extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "text";
}

export function text(): PgText {
	return new PgText();
}

export interface PgVarcharConfig {
	length?: number;
}

export class PgVarchar extends PgColumnType<string, string, string, string, string> {
	readonly length: number | undefined;

	constructor(config?: PgVarcharConfig) {
		super();
		this.length = config?.length;
	}

	get sqlType(): string {
		return this.length !== undefined ? `varchar(${this.length})` : "varchar";
	}
}

export function varchar(config?: PgVarcharConfig): PgVarchar {
	return new PgVarchar(config);
}

export interface PgCharConfig {
	length?: number;
}

export class PgChar extends PgColumnType<string, string, string, string, string> {
	readonly length: number | undefined;

	constructor(config?: PgCharConfig) {
		super();
		this.length = config?.length;
	}

	get sqlType(): string {
		return this.length !== undefined ? `char(${this.length})` : "char";
	}
}

export function char(config?: PgCharConfig): PgChar {
	return new PgChar(config);
}

// Date/Time types

export class PgDate extends PgColumnType<
	Date,
	Date | string,
	Date | string,
	string | Date,
	string
> {
	readonly sqlType = "date";

	fromDriver(input: string | Date): Date {
		if (typeof input === "string") {
			return new Date(input);
		}
		return input;
	}

	fromJson(input: string): Date {
		return new Date(input);
	}
}

export function date(): PgDate {
	return new PgDate();
}

export type Precision = 0 | 1 | 2 | 3 | 4 | 5 | 6;

export interface PgTimestampConfig {
	precision?: Precision;
	withTimezone?: boolean;
}

export class PgTimestamp extends PgColumnType<
	Date,
	Date | string,
	Date | string,
	string | Date,
	string
> {
	readonly precision: Precision | undefined;
	readonly withTimezone: boolean;

	constructor(config?: PgTimestampConfig) {
		super();
		this.precision = config?.precision;
		this.withTimezone = config?.withTimezone ?? false;
	}

	get sqlType(): string {
		const precision = this.precision === undefined ? "" : `(${this.precision})`;
		return `timestamp${precision}${this.withTimezone ? " with time zone" : ""}`;
	}

	fromDriver(input: string | Date): Date {
		if (typeof input === "string") {
			return new Date(input);
		}
		return input;
	}

	fromJson(input: string): Date {
		return new Date(input);
	}
}

export function timestamp(config?: PgTimestampConfig): PgTimestamp {
	return new PgTimestamp(config);
}

export interface PgTimeConfig {
	precision?: Precision;
	withTimezone?: boolean;
}

export class PgTime extends PgColumnType<string, string, string, string, string> {
	readonly precision: Precision | undefined;
	readonly withTimezone: boolean;

	constructor(config?: PgTimeConfig) {
		super();
		this.precision = config?.precision;
		this.withTimezone = config?.withTimezone ?? false;
	}

	get sqlType(): string {
		const precision = this.precision === undefined ? "" : `(${this.precision})`;
		return `time${precision}${this.withTimezone ? " with time zone" : ""}`;
	}
}

export function time(config?: PgTimeConfig): PgTime {
	return new PgTime(config);
}

export type IntervalFields =
	| "year"
	| "month"
	| "day"
	| "hour"
	| "minute"
	| "second"
	| "year to month"
	| "day to hour"
	| "day to minute"
	| "day to second"
	| "hour to minute"
	| "hour to second"
	| "minute to second";

export interface PgIntervalConfig {
	fields?: IntervalFields;
	precision?: Precision;
}

export class PgInterval extends PgColumnType<string, string, string, string, string> {
	readonly fields: IntervalFields | undefined;
	readonly precision: Precision | undefined;

	constructor(config?: PgIntervalConfig) {
		super();
		this.fields = config?.fields;
		this.precision = config?.precision;
	}

	get sqlType(): string {
		const fields = this.fields ? ` ${this.fields}` : "";
		const precision = this.precision !== undefined ? `(${this.precision})` : "";
		return `interval${fields}${precision}`;
	}
}

export function interval(config?: PgIntervalConfig): PgInterval {
	return new PgInterval(config);
}

// JSON types

export class PgJson<T = unknown> extends PgColumnType<T, T, T, T | string, T | string> {
	readonly sqlType = "json";

	fromDriver(input: T | string): T {
		if (typeof input === "string") {
			try {
				return JSON.parse(input) as T;
			} catch {
				return input as T;
			}
		}
		return input;
	}
}

export function json<T = unknown>(): PgJson<T> {
	return new PgJson<T>();
}

export class PgJsonb<T = unknown> extends PgColumnType<T, T, T, T | string, T | string> {
	readonly sqlType = "jsonb";

	fromDriver(input: T | string): T {
		if (typeof input === "string") {
			try {
				return JSON.parse(input) as T;
			} catch {
				return input as T;
			}
		}
		return input;
	}
}

export function jsonb<T = unknown>(): PgJsonb<T> {
	return new PgJsonb<T>();
}

// UUID type

export class PgUuid extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "uuid";
}

export function uuid(): PgUuid {
	return new PgUuid();
}

// Network address types

export class PgCidr extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "cidr";
}

export function cidr(): PgCidr {
	return new PgCidr();
}

export class PgInet extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "inet";
}

export function inet(): PgInet {
	return new PgInet();
}

export class PgMacaddr extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "macaddr";
}

export function macaddr(): PgMacaddr {
	return new PgMacaddr();
}

export class PgMacaddr8 extends PgColumnType<string, string, string, string, string> {
	readonly sqlType = "macaddr8";
}

export function macaddr8(): PgMacaddr8 {
	return new PgMacaddr8();
}

// Enum type

interface PgEnum<TValues extends readonly string[]> {
	(): PgEnumColumn<TValues>;
	readonly enumName: string;
	readonly enumValues: TValues;
	readonly schema: string | undefined;
}

export class PgEnumColumn<TValues extends readonly string[]> extends PgColumnType<
	TValues[number],
	TValues[number],
	TValues[number],
	TValues[number],
	TValues[number]
> {
	readonly enumName: string;
	readonly enumValues: TValues;
	readonly schema: string | undefined;

	constructor(enumInstance: PgEnum<TValues>) {
		super();
		this.enumName = enumInstance.enumName;
		this.enumValues = enumInstance.enumValues;
		this.schema = enumInstance.schema;
	}

	get sqlType(): string {
		return this.enumName;
	}
}

export function pgEnum<const TValues extends readonly string[]>(
	enumName: string,
	values: TValues,
	schema?: string,
): PgEnum<TValues> {
	const enumInstance: PgEnum<TValues> = Object.assign(
		(): PgEnumColumn<TValues> => new PgEnumColumn(enumInstance),
		{
			enumName,
			enumValues: values,
			schema,
		},
	);

	return enumInstance;
}

// Array type

export class PgArray<SelectType, InsertType, UpdateType, DriverType, JsonType> extends PgColumnType<
	SelectType[],
	InsertType[],
	UpdateType[],
	DriverType[] | string,
	JsonType[]
> {
	readonly baseColumn: PgColumnType<SelectType, InsertType, UpdateType, DriverType, JsonType>;
	readonly size: number | undefined;

	constructor(
		baseColumn: PgColumnType<SelectType, InsertType, UpdateType, DriverType, JsonType>,
		size?: number,
	) {
		super();
		this.baseColumn = baseColumn;
		this.size = size;
	}

	get sqlType(): string {
		const sizeStr = this.size !== undefined ? this.size : "";
		return `${this.baseColumn.sqlType}[${sizeStr}]`;
	}

	fromDriver(input: DriverType[] | string): SelectType[] {
		// The postgres driver converts known array types to real arrays, but not
		// for arrays of enums.  For those cases, this assumes the DriverType is
		// `string`; but that's probably fine.
		const arrayValues: unknown[] = typeof input === "string" ? parsePgArray(input) : input;

		return arrayValues.map((v) => this.baseColumn.fromDriver(v as DriverType));
	}

	fromJson(input: JsonType[]): SelectType[] {
		return input.map((v) => this.baseColumn.fromJson(v));
	}
}
