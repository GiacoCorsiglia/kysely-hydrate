import { ColumnType } from "./column-type";

/*
    TODO: Classes for each of these.

		bigint,
		bigserial,
		boolean,
		char,
		cidr,
		customType,
		date,
		doublePrecision,
		inet,
		integer,
		interval,
		json,
		jsonb,
		line,
		macaddr,
		macaddr8,
		numeric,
		point,
		geometry,
		real,
		serial,
		smallint,
		smallserial,
		text,
		time,
		timestamp,
		uuid,
		varchar,
		bit,
		halfvec,
		sparsevec,
		vector
*/

abstract class PgColumnType<
	SelectType,
	InsertType,
	UpdateType,
	DriverType,
	JsonType,
> extends ColumnType<SelectType, InsertType, UpdateType, DriverType, JsonType> {
	readonly dialect = "postgres";
}
