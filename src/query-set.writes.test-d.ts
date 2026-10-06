import { expectTypeOf } from "expect-type";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

////////////////////////////////////////////////////////////
// Writes
////////////////////////////////////////////////////////////

//
// Write operations on QuerySet vs MappedQuerySet
//

{
	// .insert() on QuerySet returns QuerySet, so .innerJoinMany IS available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.insert(
			db.insertInto("users").values({ username: "test", email: "test@test.com" }).returningAll(),
		);

	// Output type IS expanded to include all columns from returningAll()
	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	// innerJoinMany is available on QuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .insert() on MappedQuerySet (after .map()) - .innerJoinMany is NOT available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((row) => ({ visibleId: row.id }))
		.insert(
			db.insertInto("users").values({ username: "test", email: "test@test.com" }).returningAll(),
		);

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<{ visibleId: number }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .update() on QuerySet returns QuerySet, so .innerJoinMany IS available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.update(
			db.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
		);

	// Output type IS expanded to include all columns from returningAll()
	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	// innerJoinMany is available on QuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .update() on MappedQuerySet (after .map()) - .innerJoinMany is NOT available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((row) => ({ visibleId: row.id }))
		.update(
			db.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
		);

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<{ visibleId: number }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .delete() on QuerySet returns QuerySet, so .innerJoinMany IS available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.delete(db.deleteFrom("users").where("id", "=", 1).returningAll());

	// Output type IS expanded to include all columns from returningAll()
	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	// innerJoinMany is available on QuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .delete() on MappedQuerySet (after .map()) - .innerJoinMany is NOT available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((row) => ({ visibleId: row.id }))
		.delete(db.deleteFrom("users").where("id", "=", 1).returningAll());

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<{ visibleId: number }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

//
// writeAs() type tests
//

{
	// writeAs() with data-modifying CTE (two-callback form)
	const qs = querySet(db).writeAs(
		"updated",
		(db) =>
			db.with("updated", (qb) =>
				qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
			),
		(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
	);

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	// innerJoinMany is available on QuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// writeAs() with custom keyBy (two-callback form)
	const qs = querySet(db).writeAs(
		"user",
		(db) =>
			db.with("updated", (qb) =>
				qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
			),
		(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
		"username",
	);

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();
}

//
// .write() on QuerySet vs MappedQuerySet
//

{
	// .write() on QuerySet returns QuerySet, so .innerJoinMany IS available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.write(
			(db) =>
				db.with("updated", (qb) =>
					qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
				),
			(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
		);

	// Output type IS expanded to include all columns from the write query
	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	// innerJoinMany is available on QuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

{
	// .write() on MappedQuerySet (after .map()) - .innerJoinMany is NOT available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((row) => ({ visibleId: row.id }))
		.write(
			(db) =>
				db.with("updated", (qb) =>
					qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
				),
			(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
		);

	expectTypeOf(qs.execute()).resolves.toEqualTypeOf<{ visibleId: number }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	qs.innerJoinMany;
}

//
// Omit after selectAs().write()
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.write(
			(db) =>
				db.with("updated", (qb) =>
					qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
				),
			(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
		)
		.omit(["email"]);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit before write (omit().write())
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["email"])
		.write(
			(db) =>
				db.with("updated", (qb) =>
					qb.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
				),
			(qc) => qc.selectFrom("updated").select(["id", "username", "email"]),
		);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}
