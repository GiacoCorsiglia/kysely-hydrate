// Not a scenario: checked before each fixture, in the same program, so the
// fixture's count leaves out resolving the library, Kysely and expect-type
// declarations every fixture shares.  Its own schema, so no fixture's
// instantiations are cached here.
import { expectTypeOf } from "expect-type";
import type * as k from "kysely";

import { createHydrator, querySet } from "../../src/index.ts";

interface WarmUpDB {
	parents: { id: k.Generated<number>; name: string };
	children: { id: k.Generated<number>; parent_id: number; label: string | null };
}

declare const db: k.Kysely<WarmUpDB>;

const children = querySet(db).selectAs(
	"child",
	db.selectFrom("children").select(["id", "parent_id", "label"]),
);
const parents = querySet(db)
	.selectAs("parent", db.selectFrom("parents").select(["id", "name"]))
	.leftJoinMany("many", children, "many.parent_id", "parent.id")
	.innerJoinOne(
		"one",
		({ eb, qs }) => qs(eb.selectFrom("children").select(["id", "parent_id"])),
		"one.parent_id",
		"parent.id",
	)
	.attachMany("attached", () => db.selectFrom("children").selectAll().execute(), {
		matchChild: "parent_id",
	})
	.orderBy("name")
	.limit(1);
const inserted = querySet(db)
	.insertAs("parent", (d) => d.insertInto("parents").values({ name: "" }).returningAll())
	.leftJoinOne("child", children, "child.parent_id", "parent.id");

const hydrator = createHydrator<{ id: number; name: string; c$$id: number }>("id")
	.fields({ name: true })
	.extras({ upper: (r) => r.name.toUpperCase() })
	.hasMany("c", "c$$", (h) => h("id").fields({ id: true }));

expectTypeOf(parents.execute()).resolves.toEqualTypeOf<
	{
		id: number;
		name: string;
		many: { id: number; parent_id: number; label: string | null }[];
		one: { id: number; parent_id: number };
		attached: { id: number; parent_id: number; label: string | null }[];
	}[]
>();
expectTypeOf(inserted.executeTakeFirstOrThrow()).resolves.toEqualTypeOf<{
	id: number;
	name: string;
	child: { id: number; parent_id: number; label: string | null } | null;
}>();
expectTypeOf(hydrator.hydrate([])).resolves.toEqualTypeOf<
	{ name: string; upper: string; c: { id: number }[] }[]
>();
