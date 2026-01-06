import { expectTypeOf } from "expect-type";

import { db } from "./__tests__/sqlite.ts";
import { querySet } from "./query-set.ts";

// init: builder overloads
{
	//
	// Direct overloads
	//

	// @ts-expect-error DEFAULT_KEY_BY ("id") is not selected, so keyBy must be provided.
	querySet(db).init("users", db.selectFrom("users").select(["username"]));

	querySet(db).init("users", db.selectFrom("users").select(["username"]), "username");

	// Optional keyBy (defaults to "id" when valid)
	querySet(db).init("users", db.selectFrom("users").select(["id"]));
	// Can always provide keyBy if you want to be explicit
	querySet(db).init("users", db.selectFrom("users").select(["id"]), "id");
	querySet(db).init("users", db.selectFrom("users").select(["id", "username"]), "id");
	querySet(db).init("users", db.selectFrom("users").select(["id", "username"]), "username");

	// @ts-expect-error Invalid keyBy (with default key by)
	querySet(db).init("users", db.selectFrom("users").select(["id"]), "invalid");

	// @ts-expect-error Invalid keyBy (without default key by)
	querySet(db).init("users", db.selectFrom("users").select(["username"]), "invalid");

	//
	// Factory overloads
	//

	// @ts-expect-error DEFAULT_KEY_BY ("id") is not selected, so keyBy must be provided.
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["username"]));

	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["username"]), "username");

	// Optional keyBy (defaults to "id" when valid)
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["id"]));
	// Can always provide keyBy if you want to be explicit
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["id"]), "id");
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["id", "username"]), "id");
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["id", "username"]), "username");

	// @ts-expect-error Invalid keyBy (with default key by)
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["id"]), "invalid");

	// @ts-expect-error Invalid keyBy (without default key by)
	querySet(db).init("users", (eb) => eb.selectFrom("users").select(["username"]), "invalid");
}
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
