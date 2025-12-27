import * as k from "kysely";

export type SomeColumnType = ColumnType<unknown, unknown, unknown, unknown, unknown>;

/**
 * @template SelectType - The type of the column when selected.
 * @template InsertType - The type of the column when inserted.
 * @template UpdateType - The type of the column when updated.
 * @template DriverType - The type of the column when retrieved from the database driver.
 * @template JsonType - The type of the column when serialized to JSON by the database.
 */
export abstract class ColumnType<
	SelectType,
	InsertType = SelectType,
	UpdateType = SelectType,
	DriverType = string,
	JsonType = string,
> implements k.ColumnType<SelectType, InsertType, UpdateType> {
	readonly __select__!: SelectType;
	readonly __insert__!: InsertType;
	readonly __update__!: UpdateType;

	readonly __driverType__!: DriverType;
	readonly __jsonType__!: JsonType;

	abstract readonly dialect: string;
	abstract readonly sqlType: string;

	nullable(): Nullable<this> {
		return new Nullable(this);
	}

	generated(): Generated<this> {
		return new Generated(this);
	}

	generatedAlways(): GeneratedAlways<this> {
		return new GeneratedAlways(this);
	}

	fromDriver(input: DriverType): SelectType {
		return input as unknown as SelectType;
	}

	fromJson(input: JsonType): SelectType {
		return this.fromDriver(input as unknown as DriverType);
	}
}

export class Nullable<T extends SomeColumnType> extends ColumnType<
	T["__select__"] | null,
	T["__insert__"] | null | undefined,
	T["__update__"] | null,
	T["__driverType__"] | null,
	T["__jsonType__"] | null
> {
	readonly dialect: string;
	readonly sqlType: string;

	readonly column: T;

	constructor(column: T) {
		super();
		this.dialect = column.dialect;
		this.sqlType = column.sqlType;
		this.column = column;
	}

	fromDriver(input: T["__driverType__"] | null) {
		if (input === null) {
			return null;
		}

		return this.column.fromDriver(input);
	}

	fromJson(input: T["__jsonType__"] | null) {
		if (input === null) {
			return null;
		}

		return this.column.fromJson(input);
	}
}

export class Generated<T extends SomeColumnType>
	extends ColumnType<
		T["__select__"],
		T["__insert__"] | undefined,
		T["__update__"],
		T["__driverType__"],
		T["__jsonType__"]
	>
	implements k.Generated<T["__select__"]>
{
	readonly dialect: string;
	readonly sqlType: string;

	readonly column: T;

	constructor(column: T) {
		super();
		this.dialect = column.dialect;
		this.sqlType = column.sqlType;
		this.column = column;
	}

	fromDriver(input: T["__driverType__"]) {
		return this.column.fromDriver(input);
	}

	fromJson(input: T["__jsonType__"]) {
		return this.column.fromJson(input);
	}
}

export class GeneratedAlways<T extends SomeColumnType>
	extends ColumnType<T["__select__"], never, never, T["__driverType__"], T["__jsonType__"]>
	implements k.GeneratedAlways<T["__select__"]>
{
	readonly dialect: string;
	readonly sqlType: string;

	readonly column: T;

	constructor(column: T) {
		super();
		this.dialect = column.dialect;
		this.sqlType = column.sqlType;
		this.column = column;
	}

	fromDriver(input: T["__driverType__"]) {
		return this.column.fromDriver(input);
	}

	fromJson(input: T["__jsonType__"]) {
		return this.column.fromJson(input);
	}
}

export class Never extends ColumnType<never, never, never, never, never> {
	readonly dialect: string;
	readonly sqlType: string;

	constructor(dialect: string, sqlType: string) {
		super();
		this.dialect = dialect;
		this.sqlType = sqlType;
	}

	fromDriver(input: never) {
		return input;
	}
}
