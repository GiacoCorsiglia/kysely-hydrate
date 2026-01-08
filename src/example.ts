// oxlint-disable no-unused-vars
import * as k from "kysely";
import pg from "pg";

const db = new k.Kysely<any>({
	dialect: new k.PostgresDialect({
		pool: new pg.Pool(),
	}),
});

const innerOneQuery = db.selectFrom("innerOneTable");
const innerManyQuery = db.selectFrom("innerManyTable");
const leftOneQuery = db.selectFrom("leftOneTable");
const leftManyQuery = db.selectFrom("leftManyTable");

declare function toExistsQuery(
	query: k.SelectQueryBuilder<any, any, any>,
): k.SelectQueryBuilder<any, any, { exists: boolean }>;

const baseAlias = "base";

// Base query, passed to init as `init(baseAlias, baseQuery)` without the alias
const baseQuery = db.selectFrom("someTable").select(["someTable.id"]);

// aliased base query:
const aliasedBaseQuery = baseQuery.as(baseAlias);

// How we execute and expand the base query.
const wrapped = db.selectFrom(aliasedBaseQuery).selectAll(baseAlias);

// With joins (subject to row explosion):
const joined = wrapped
	.innerJoin(innerOneQuery.as("innerOne"), "innerOne.id", "base.id")
	.innerJoin(innerManyQuery.as("innerMany"), "innerMany.id", "base.id")
	.leftJoin(leftOneQuery.as("leftOne"), "leftOne.id", "base.id")
	.leftJoin(leftManyQuery.as("leftMany"), "leftMany.id", "base.id")
	.select(["innerOne.id", "innerMany.id", "leftOne.id", "leftMany.id"]);

// ^ This is safe if we are not paginating (no limit or offset).

// We can even order the joined query safely.  Can't order by nested
// cardinality-many joins since that doesn't really make sense anyway.
// But this will be hydrated into an ordered result no problem.
const joinedOrdered = joined.orderBy(`${baseAlias}.col`, "desc");

// When you want to get a COUNT(*) of the root rows.

// With no joins:
const countNoJoins = baseQuery.clearSelect().select((eb) => eb.fn.countAll().as("count"));

// With joins: we have to be careful about deduplication.
const countWithJoins = wrapped
	// Cardinal-one inner join is safe (and required).
	.innerJoin(innerOneQuery.as("innerOne"), "innerOne.id", "base.id")
	// Cardinal-many inner join must be converted to a WHERE EXISTS
	.where(({ exists }) => exists(toExistsQuery(innerManyQuery)));
// Left joins safely omitted
// No limit or offset, no ordering applied.

// Paginated (with limit or offset), possibly ordered.

// If no joins, we can just apply the limit and offset to the base query.
const paginatedNoJoins = wrapped.limit(10).offset(0);

// If we only have cardinality-one joins only, we can apply the limit and offset
// to the joined query safely.
const paginatedCardinalOneJoins = joined
	.limit(10)
	.offset(0)
	.orderBy("whatever")
	.selectAll(baseAlias)
	.select([
		"innerOne.id as innerOne$$id",
		"innerMany.id as innerMany$$id",
		"leftOne.id as leftOne$$id",
		"leftMany.id as leftMany$$id",
	]);

// If we have cardinality-many joins, we need to do more wrapping.
const paginated = db
	.selectFrom(
		wrapped
			// Cardinal-one inner join is safe (and required).
			.innerJoin(innerOneQuery.as("innerOne"), "innerOne.id", "base.id")
			// Cardinal-many inner join must be converted to a WHERE EXISTS
			.where(({ exists }) => exists(toExistsQuery(innerManyQuery)))
			// Cardinal-one left join included here so it can be used for ordering.
			.leftJoin(leftOneQuery.as("leftOne"), "leftOne.id", "base.id")
			// Apply limit and offset.
			.limit(10)
			.offset(0)
			// Apply ordering.
			.orderBy("whatever")
			// Rewrap with the same alias
			.as(baseAlias),
	)
	// Apply cardinality-many joins for real.
	.innerJoin(innerManyQuery.as("innerMany"), "innerMany.id", "base.id")
	.leftJoin(leftManyQuery.as("leftMany"), "leftMany.id", "base.id")
	// Reapply ordering.
	.orderBy("whatever");
