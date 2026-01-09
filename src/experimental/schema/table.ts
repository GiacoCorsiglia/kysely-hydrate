import { type SomeColumnType } from "./column-type.ts";

type ColumnsRecord = Record<string, SomeColumnType>;

export type SomeTable = Table<string, string, ColumnsRecord>;

export interface Table<Schema extends string, Name extends string, Columns extends ColumnsRecord> {
	readonly $schema: Schema;
	readonly $name: Name;
	readonly $columns: Columns;
}

export function createTable<
	Schema extends string,
	Name extends string,
	Columns extends ColumnsRecord,
>(schema: Schema, name: Name, columns: Columns): Table<Schema, Name, Columns> {
	return {
		$schema: schema,
		$name: name,
		$columns: columns,
	};
}

export type Database = Record<string, SomeTable>;

type DatabaseFromTables<Schema extends string, Tables extends Record<string, ColumnsRecord>> = {
	[Name in keyof Tables & string]: Table<Schema, Name, Tables[Name]>;
};

export function createDatabase<Schema extends string, Tables extends Record<string, ColumnsRecord>>(
	schema: string,
	tables: Tables,
): DatabaseFromTables<Schema, Tables> {
	return Object.fromEntries(
		Object.entries(tables).map(([name, columns]) => [name, createTable(schema, name, columns)]),
	) as DatabaseFromTables<Schema, Tables>;
}
