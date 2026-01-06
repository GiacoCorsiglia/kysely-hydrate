import { expectTypeOf } from "expect-type";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

{
	const result = querySet(db)
		.init("users", (eb) => eb.selectFrom("users").select(["id", "username"]))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
		}[]
	>();
}

{
	const result = querySet(db)
		.init("users", (eb) => eb.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "user_id", "title"])),
			"users.id",
			"posts.user_id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.attachOne("foo", () => ({}) as Promise<{ id: number }[]>, { matchChild: "id" }),
		)
		.attachMany(
			"attachedPosts",
			() => {
				return querySet(db).init("posts", (eb) =>
					eb.selectFrom("posts").select(["id", "user_id", "title"]),
				);
			},
			{ matchChild: "user_id" },
		)
		.modify("attachedPosts", (postsQuerySet) =>
			postsQuerySet.map((p) => ({ ...p, upperTitle: p.title.toUpperCase() })),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;

			attachedPosts: {
				id: number;
				title: string;
				user_id: number;

				// Look, the modifier added this field via a map.
				upperTitle: string;
			}[];

			posts: {
				id: number;
				user_id: number;
				title: string;

				// Look, it got attached!
				foo: { id: number } | null;
			}[];
		}[]
	>();
}
