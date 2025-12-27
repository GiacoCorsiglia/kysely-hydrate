import { type SomeColumnType } from "./column-type.ts";

export type Database = Record<string, SomeTable>;

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
