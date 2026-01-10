import { expectTypeOf } from "expect-type";
import * as k from "kysely";

import {
	type SeedDB,
	type User as DBUser,
	type Profile as DBProfile,
} from "./__tests__/fixture.ts";
import { db } from "./__tests__/sqlite.ts";
import { createHydrator } from "./hydrator.ts";
import { querySet } from "./query-set.ts";

type InferDB<T> = T extends k.SelectQueryBuilder<infer DB, any, any> ? DB : never;
type InferO<T> = T extends k.SelectQueryBuilder<any, any, infer O> ? O : never;
type InferTB<T> = T extends k.SelectQueryBuilder<any, infer TB, any> ? TB : never;

////////////////////////////////////////////////////////////
// Shared test data (DRY foundation)
////////////////////////////////////////////////////////////

interface User {
	id: number;
	username: string;
	email: string;
}

interface Post {
	id: number;
	user_id: number;
	title: string;
	content: string;
}

interface Comment {
	id: number;
	post_id: number;
	content: string;
}

////////////////////////////////////////////////////////////
// Section 1: Initialization (.selectAs)
////////////////////////////////////////////////////////////

//
// Default keyBy inference
//

{
	// Valid: default keyBy when "id" is selected
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Explicit keyBy required
//

{
	// Valid: explicit keyBy when "id" not selected
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["username", "email"]), "username")
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ username: string; email: string }[]>();
}

{
	const query = db.selectFrom("users").select(["username"]);

	querySet(db)
		// @ts-expect-error - keyBy required when no "id"
		.selectAs("user", query);
}

//
// Factory function variant
//

{
	// Valid: factory with default keyBy
	const result = querySet(db)
		.selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

{
	// Valid: factory with explicit keyBy
	const result = querySet(db)
		.selectAs("user", (eb) => eb.selectFrom("users").select(["username"]), "username")
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ username: string }[]>();
}

{
	const query = db.selectFrom("users").select(["username"]);

	querySet(db)
		// @ts-expect-error - factory keyBy required when no "id"
		.selectAs("user", query);
}

//
// Direct query variant
//

{
	// Valid: pre-built query
	const query = db.selectFrom("users").select(["id", "username"]);
	const result = querySet(db).selectAs("user", query).execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Invalid keyBy
//

{
	const query = db.selectFrom("users").select(["id"]);

	querySet(db)
		// @ts-expect-error - invalid keyBy (with default key by)
		.selectAs("user", query, "nonExistent");
}

{
	const query = db.selectFrom("users").select(["username"]);

	querySet(db)
		// @ts-expect-error - invalid keyBy (without default key by)
		.selectAs("user", query, "invalid");
}

//
// Composite keyBy
//

{
	// Valid: array of keys
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]), ["id", "username"])
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

////////////////////////////////////////////////////////////
// Section 2: Join Methods - innerJoinOne
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable single object
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
		}[]
	>();
}

//
// Pre-built QuerySet variant
//

{
	const profileQuery = querySet(db).selectAs(
		"profile",
		db.selectFrom("profiles").select(["id", "bio", "user_id"]),
	);

	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne("profile", profileQuery, "profile.user_id", "user.id")
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
		}[]
	>();
}

//
// Callback join condition
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			(join) => join.onRef("profile.user_id", "=", "user.id"),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
		}[]
	>();
}

//
// Nested joins (2 levels)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) =>
				nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])).innerJoinOne(
					"comments",
					(init2) =>
						init2((eb2) => eb2.selectFrom("comments").select(["user_id", "content"]), "user_id"),
					"comments.user_id",
					"profile.user_id",
				),
			"profile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: {
				id: number;
				bio: string | null;
				user_id: number;
				comments: { user_id: number; content: string };
			};
		}[]
	>();
}

//
// Invalid join references
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id"])),
			// @ts-expect-error - invalid left column
			"profile.nonExistent",
			"user.id",
		);
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id"])),
			"profile.id",
			// @ts-expect-error - invalid right column
			"user.nonExistent",
		);
}

////////////////////////////////////////////////////////////
// Section 3: Join Methods - innerJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

//
// Callback join condition
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			(join) => join.onRef("posts.user_id", "=", "user.id"),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 4: Join Methods - leftJoinOne
////////////////////////////////////////////////////////////

//
// Basic usage: nullable single object
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number } | null;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 5: Join Methods - leftJoinOneOrThrow
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable (throws if missing)
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
		.leftJoinOneOrThrow(
			"author",
			(nest) => nest((eb) => eb.selectFrom("users").select(["id", "username"])),
			"author.id",
			"post.user_id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			user_id: number;
			author: { id: number; username: string };
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 6: Join Methods - leftJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable array (empty if no matches)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 7: Join Methods - crossJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: cartesian product as array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.crossJoinMany("comments", (nest) =>
			nest((eb) => eb.selectFrom("comments").select(["id", "content"])),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			comments: { id: number; content: string }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 8: Join Methods - innerJoinLateralOne
////////////////////////////////////////////////////////////

//
// Basic usage with lateral reference
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinLateralOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 9: Join Methods - innerJoinLateralMany
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinLateralMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 10: Join Methods - leftJoinLateralOne
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinLateralOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number } | null;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 11: Join Methods - leftJoinLateralOneOrThrow
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
		.leftJoinLateralOneOrThrow(
			"author",
			(nest) => nest((eb) => eb.selectFrom("users").select(["id", "username"])),
			"author.id",
			"post.user_id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			user_id: number;
			author: { id: number; username: string };
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 12: Join Methods - leftJoinLateralMany
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinLateralMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 13: Join Methods - crossJoinLateralMany
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.crossJoinLateralMany("comments", (nest) =>
			nest((eb) => eb.selectFrom("comments").select(["id", "content"])),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			comments: { id: number; content: string }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 14: Attach Methods - attachMany
////////////////////////////////////////////////////////////

//
// Basic usage with Promise<Iterable>
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			async (users) => {
				expectTypeOf(users).toEqualTypeOf<{ id: number; username: string }[]>();
				return [] as Post[];
			},
			{ matchChild: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: Post[];
		}[]
	>();
}

//
// QuerySet variant (auto-execute)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			(users) => {
				expectTypeOf(users).toEqualTypeOf<{ id: number; username: string }[]>();
				const userIds = users.map((u) => u.id);
				return querySet(db).selectAs("post", (eb) =>
					eb.selectFrom("posts").select(["id", "user_id", "title"]).where("user_id", "in", userIds),
				);
			},
			{ matchChild: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string }[];
		}[]
	>();
}

//
// SelectQueryBuilder variant
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			(users) => {
				const userIds = users.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["id", "user_id", "title"])
					.where("user_id", "in", userIds);
			},
			{ matchChild: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string }[];
		}[]
	>();
}

//
// Match with toParent
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "user_id", "title"]))
		.attachMany("comments", async () => [] as Comment[], { matchChild: "post_id", toParent: "id" })
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			user_id: number;
			title: string;
			comments: Comment[];
		}[]
	>();
}

//
// Invalid match keys
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))

		.attachMany("posts", async () => [] as Post[], {
			// @ts-expect-error - matchChild field doesn't exist on attached type
			matchChild: "nonExistent",
		});
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany("posts", async () => [] as Post[], {
			matchChild: "user_id",
			// @ts-expect-error - toParent field doesn't exist on parent type
			toParent: "nonExistent",
		});
}

////////////////////////////////////////////////////////////
// Section 15: Attach Methods - attachOne
////////////////////////////////////////////////////////////

//
// Basic usage: nullable
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "user_id"]))
		.attachOne("author", async () => [] as User[], { matchChild: "id", toParent: "user_id" })
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			user_id: number;
			author: User | null;
		}[]
	>();
}

//
// QuerySet variant
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "user_id"]))
		.attachOne(
			"author",
			(posts) => {
				const userIds = [...new Set(posts.map((p) => p.user_id))];
				return querySet(db).selectAs("user", (eb) =>
					eb.selectFrom("users").select(["id", "username"]).where("id", "in", userIds),
				);
			},
			{ matchChild: "id", toParent: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			user_id: number;
			author: { id: number; username: string } | null;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 16: Attach Methods - attachOneOrThrow
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable (throws if missing)
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "user_id"]))
		.attachOneOrThrow("author", async () => [] as User[], { matchChild: "id", toParent: "user_id" })
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			user_id: number;
			author: User;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 17: Hydration - extras
////////////////////////////////////////////////////////////

//
// Add computed fields
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.extras({
			displayName: (row) => {
				expectTypeOf(row).toEqualTypeOf<{ id: number; username: string; email: string }>();
				return `${row.username} <${row.email}>`;
			},
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			email: string;
			displayName: string;
		}[]
	>();
}

//
// Multiple extras
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({
			upper: (row) => row.username.toUpperCase(),
			length: (row) => row.username.length,
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			upper: string;
			length: number;
		}[]
	>();
}

//
// Invalid field reference
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({
			// @ts-expect-error - accessing non-existent field
			invalid: (row) => row.nonExistent,
		});
}

////////////////////////////////////////////////////////////
// Section 18: Hydration - mapFields
////////////////////////////////////////////////////////////

//
// Transform existing field (same type)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.mapFields({
			username: (name) => {
				expectTypeOf(name).toEqualTypeOf<string>();
				return name.toUpperCase();
			},
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Transform to different type
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.mapFields({
			username: (name) => name.length,
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: number }[]>();
}

//
// Multiple transformations
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.mapFields({
			username: (name) => name.toUpperCase(),
			email: (email) => email.length,
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string; email: number }[]>();
}

//
// Invalid field
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.mapFields({
			// @ts-expect-error - field doesn't exist
			nonExistent: (x: any) => x,
		});
}

////////////////////////////////////////////////////////////
// Section 19: Hydration - omit
////////////////////////////////////////////////////////////

//
// Omit single field
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["email"])
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit multiple fields
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["username", "email"])
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number }[]>();
}

//
// Invalid field
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - cannot omit non-existent field
		.omit(["nonExistent"]);
}

////////////////////////////////////////////////////////////
// Section 20: Hydration - with
////////////////////////////////////////////////////////////

//
// Extend with FullHydrator
//

{
	const extraFields = createHydrator<User>("id").extras({
		displayName: (u) => `${u.username} <${u.email}>`,
	});

	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.with(extraFields)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			email: string;
			displayName: string;
		}[]
	>();
}

//
// Extend with MappedHydrator
//

{
	const mappedHydrator = createHydrator<User>("id")
		.fields({ id: true })
		.map((u) => ({ userId: u.id }));

	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.with(mappedHydrator)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			email: string;
			userId: number;
		}[]
	>();
}

//
// Invalid: hydrator fields not in row
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - extraField not in row type
		.with(createHydrator<{ id: number; username: string; extraField: string }>("id"));
}

////////////////////////////////////////////////////////////
// Section 21: Hydration chaining
////////////////////////////////////////////////////////////

//
// extras → mapFields → omit
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.extras({
			displayName: (row) => `${row.username} <${row.email}>`,
		})
		.mapFields({
			username: (name) => name.toUpperCase(),
		})
		.omit(["email"])
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			displayName: string;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 22: Nested hydration
////////////////////////////////////////////////////////////

//
// extras in nested join
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).extras({
					titleUpper: (post) => {
						expectTypeOf(post).toEqualTypeOf<{ id: number; title: string; user_id: number }>();
						return post.title.toUpperCase();
					},
				}),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; titleUpper: string; user_id: number }[];
		}[]
	>();
}

//
// mapFields in nested join
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).mapFields({
					title: (title) => {
						expectTypeOf(title).toEqualTypeOf<string>();
						return title.toUpperCase();
					},
				}),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

//
// omit in nested join
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id", "content"])).omit([
					"content",
				]),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 23: Modification - base query
////////////////////////////////////////////////////////////

//
// Add WHERE clause
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", ">", 100))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Add additional SELECT
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.select("email"))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string; email: string }[]>();
}

//
// Chaining modifications
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.modify((qb) => qb.where("id", ">", 100))
		.modify((qb) => qb.select("email"))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string; email: string }[]>();
}

//
// Cannot modify base query with incompatible output type
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - incompatible output type (can't select *fewer* columns).
		.modify((qb) => qb.clearSelect())
		.execute();
}

////////////////////////////////////////////////////////////
// Section 24: Modification - join collection
////////////////////////////////////////////////////////////

//
// Modify joined QuerySet with filtering
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.modify((qb) => qb.where("title", "like", "%test%")),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

//
// Modify with extras
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.extras({ titleUpper: (p) => p.title.toUpperCase() }),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; titleUpper: string; user_id: number }[];
		}[]
	>();
}

//
// Modify with nested attach
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.attachMany("comments", async () => [] as Comment[], {
				matchChild: "post_id",
				toParent: "id",
			}),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number; comments: Comment[] }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 25: Modification - attach collection (QuerySet)
////////////////////////////////////////////////////////////

//
// Modify attached QuerySet
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			() =>
				querySet(db).selectAs("post", (eb) =>
					eb.selectFrom("posts").select(["id", "user_id", "title"]),
				),
			{ matchChild: "user_id" },
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.extras({ titleUpper: (p) => p.title.toUpperCase() }),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string; titleUpper: string }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 26: Modification - attach collection (SelectQueryBuilder)
////////////////////////////////////////////////////////////

//
// Modify SelectQueryBuilder attach
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			(users) => {
				const userIds = users.map((u) => u.id);
				return db
					.selectFrom("posts")
					.select(["id", "user_id", "title"])
					.where("user_id", "in", userIds);
			},
			{ matchChild: "user_id" },
		)
		.modify("posts", (qb) => qb.select("content"))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string; content: string }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 27: Modification - attach collection (external)
////////////////////////////////////////////////////////////

//
// Transform via map
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany("posts", async () => [] as Post[], { matchChild: "user_id" })
		.modify("posts", async (postsPromise) =>
			(await postsPromise).map((p) => ({ ...p, upperTitle: p.title.toUpperCase() })),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			posts: { id: number; user_id: number; title: string; content: string; upperTitle: string }[];
			username: string;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 28: Modification - invalid collection key
////////////////////////////////////////////////////////////

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - collection key doesn't exist
		.modify("nonExistent", (qs: any) => qs);
}

////////////////////////////////////////////////////////////
// Section 29: .where() convenience
////////////////////////////////////////////////////////////

//
// Reference expression
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", ">", 100)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Expression factory
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where((eb) => eb("username", "like", "%admin%"))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Chaining
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.where("id", ">", 100)
		.where((eb) => eb("username", "like", "%admin%"))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Invalid column
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - column doesn't exist
		.where("nonExistent", "=", "value");
}

////////////////////////////////////////////////////////////
// Section 30: Pagination
////////////////////////////////////////////////////////////

//
// limit/offset
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(10)
		.offset(5)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// clearLimit/clearOffset
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.limit(10)
		.offset(5)
		.clearLimit()
		.clearOffset()
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// orderBy
//

// orderBy with base column name

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.orderBy("username")
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

// orderBy with string modifier

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.orderBy("username", "desc" as "asc" | "desc")
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

// orderBy with callback modifier

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.orderBy("username", (ob) => ob.desc().nullsFirst())
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

// orderBy with nested join (one).

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.orderBy("username") // Still accepts base query columns.
		.orderBy("profile$$bio") // Accepts prefixed nested join columns.
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
		}[]
	>();
}

// Rejects nonsense key

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - nonsense key
		.orderBy("nonExistent")
		.execute();
}

////////////////////////////////////////////////////////////
// Section 31: Query Compilation - toBaseQuery
////////////////////////////////////////////////////////////

{
	const base = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toBaseQuery();

	expectTypeOf(base).toEqualTypeOf<
		k.SelectQueryBuilder<SeedDB, "users", { id: number; username: string }>
	>();
}

////////////////////////////////////////////////////////////
// Section 32: Query Compilation - toJoinedQuery
////////////////////////////////////////////////////////////

{
	const joined = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.toJoinedQuery();

	type DB = InferDB<typeof joined>;
	type TB = InferTB<typeof joined>;
	type O = InferO<typeof joined>;

	// JoinedQuery includes both base and joined tables
	expectTypeOf<DB["users"]>().toEqualTypeOf<DBUser>();
	expectTypeOf<DB["user"]>().toEqualTypeOf<{ id: number; username: string }>();
	expectTypeOf<DB["profiles"]>().toEqualTypeOf<DBProfile>();
	expectTypeOf<DB["profile"]>().toEqualTypeOf<{
		id: number;
		bio: string | null;
		user_id: number;
	}>();

	// Both are joined.
	expectTypeOf<TB>().toEqualTypeOf<"user" | "profile">();

	// Output type is correct.
	expectTypeOf<O>().toEqualTypeOf<{
		id: number;
		username: string;
		profile$$id: number;
		profile$$bio: string | null;
		profile$$user_id: number;
	}>();
}

{
	const joined = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.toJoinedQuery();

	type DB = InferDB<typeof joined>;
	type TB = InferTB<typeof joined>;
	type O = InferO<typeof joined>;

	// JoinedQuery includes both base and joined tables
	expectTypeOf<DB["users"]>().toEqualTypeOf<DBUser>();
	expectTypeOf<DB["user"]>().toEqualTypeOf<{ id: number; username: string }>();
	expectTypeOf<DB["profiles"]>().toEqualTypeOf<DBProfile>();
	expectTypeOf<DB["profile"]>().toEqualTypeOf<{
		id: number | null;
		bio: string | null;
		user_id: number | null;
	}>();

	// Both are joined.
	expectTypeOf<TB>().toEqualTypeOf<"user" | "profile">();

	// Output type is correct (with nullable columns).
	expectTypeOf<O>().toEqualTypeOf<{
		id: number;
		username: string;
		profile$$id: number | null;
		profile$$bio: string | null;
		profile$$user_id: number | null;
	}>();
}

//
// With nested joins, prefixing applied correctly.
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
					"comments",
					(init2) =>
						init2((eb2) => eb2.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.toJoinedQuery()
		.executeTakeFirstOrThrow();

	expectTypeOf(result).resolves.toEqualTypeOf<{
		id: number;
		username: string;
		profile$$id: number;
		profile$$bio: string | null;
		profile$$user_id: number;
		posts$$id: number;
		posts$$title: string;
		posts$$user_id: number;
		posts$$comments$$id: number;
		posts$$comments$$content: string;
		posts$$comments$$post_id: number;
	}>();
}

////////////////////////////////////////////////////////////
// Section 33: Query Compilation - toQuery
////////////////////////////////////////////////////////////

{
	const query = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toQuery();

	// Opaque type with correct output shape
	expectTypeOf(query).toEqualTypeOf<
		k.SelectQueryBuilder<{}, never, { id: number; username: string }>
	>();
}

////////////////////////////////////////////////////////////
// Section 34: Query Compilation - toCountQuery
////////////////////////////////////////////////////////////

{
	const countQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toCountQuery();

	const result = countQuery.execute();
	expectTypeOf(result).resolves.toEqualTypeOf<{ count: string | number | bigint }[]>();
}

////////////////////////////////////////////////////////////
// Section 35: Query Compilation - toExistsQuery
////////////////////////////////////////////////////////////

{
	const existsQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toExistsQuery();

	const result = existsQuery.execute();
	expectTypeOf(result).resolves.toEqualTypeOf<{ exists: k.SqlBool }[]>();
}

////////////////////////////////////////////////////////////
// Section 36: Execution - execute
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

////////////////////////////////////////////////////////////
// Section 37: Execution - executeTakeFirst
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirst();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string } | undefined>();
}

////////////////////////////////////////////////////////////
// Section 38: Execution - executeTakeFirstOrThrow
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirstOrThrow();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

////////////////////////////////////////////////////////////
// Section 39: Execution - executeCount
////////////////////////////////////////////////////////////

//
// Default: string | number | bigint
//

{
	const count = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.executeCount();

	expectTypeOf(count).resolves.toEqualTypeOf<string | number | bigint>();
}

//
// Cast to number
//

{
	const count = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.executeCount(Number);

	expectTypeOf(count).resolves.toEqualTypeOf<number>();
}

//
// Cast to bigint
//

{
	const count = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.executeCount(BigInt);

	expectTypeOf(count).resolves.toEqualTypeOf<bigint>();
}

//
// Cast to string
//

{
	const count = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.executeCount(String);

	expectTypeOf(count).resolves.toEqualTypeOf<string>();
}

////////////////////////////////////////////////////////////
// Section 40: Execution - executeExists
////////////////////////////////////////////////////////////

{
	const exists = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.modify((qb) => qb.where("username", "=", "alice"))
		.executeExists();

	expectTypeOf(exists).resolves.toEqualTypeOf<boolean>();
}

////////////////////////////////////////////////////////////
// Section 41: Terminal .map() - basic
////////////////////////////////////////////////////////////

//
// Basic transformation
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string }>();
			return { userId: user.id, userName: user.username };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ userId: number; userName: string }[]>();
}

//
// Chaining maps
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string }>();
			return { ...user, upper: user.username.toUpperCase() };
		})
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string; upper: string }>();
			return { final: user.upper };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ final: string }[]>();
}

////////////////////////////////////////////////////////////
// Section 42: Terminal .map() - limitations
////////////////////////////////////////////////////////////

//
// Cannot call join methods after map
//

{
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((u) => u.id);

	// @ts-expect-error - cannot call innerJoinMany after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinMany;

	// @ts-expect-error - cannot call innerJoinOne after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinOne;

	// @ts-expect-error - cannot call leftJoinMany after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.leftJoinMany;

	// @ts-expect-error - cannot call leftJoinOne after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.leftJoinOne;
}

//
// Cannot call hydration methods after map
//

{
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((u) => u.id);

	// @ts-expect-error - cannot call extras after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.extras;

	// @ts-expect-error - cannot call mapFields after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.mapFields;

	// @ts-expect-error - cannot call omit after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.omit;

	// @ts-expect-error - cannot call with after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.with;
}

//
// Cannot call attach methods after map
//

{
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((u) => u.id);

	// @ts-expect-error - cannot call attachMany after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachMany;

	// @ts-expect-error - cannot call attachOne after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOne;

	// @ts-expect-error - cannot call attachOneOrThrow after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOneOrThrow;
}

//
// Can still call map, modify, and execution methods
//

{
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((u) => u.id);

	// These should work
	const stillMapped = mapped.map((id) => ({ transformed: id }));
	expectTypeOf(stillMapped.execute()).resolves.toEqualTypeOf<{ transformed: number }[]>();

	const modified = mapped.modify((qb) => qb);
	expectTypeOf(modified.execute()).resolves.toEqualTypeOf<number[]>();

	expectTypeOf(mapped.execute()).resolves.toEqualTypeOf<number[]>();
	expectTypeOf(mapped.executeTakeFirst()).resolves.toEqualTypeOf<number | undefined>();
}

////////////////////////////////////////////////////////////
// Section 43: Terminal .map() - nested
////////////////////////////////////////////////////////////

//
// Map in nested collection
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).map((post) => {
					expectTypeOf(post).toEqualTypeOf<{ id: number; title: string; user_id: number }>();
					return { postId: post.id, postTitle: post.title };
				}),
			"posts.user_id",
			"user.id",
		)
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{
				id: number;
				username: string;
				posts: { postId: number; postTitle: string }[];
			}>();
			return { userName: user.username, postCount: user.posts.length };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postCount: number }[]>();
}

//
// Map with attached data
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany("posts", async () => [] as Post[], { matchChild: "user_id" })
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string; posts: Post[] }>();
			return { userName: user.username, postTitles: user.posts.map((p) => p.title) };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postTitles: string[] }[]>();
}

////////////////////////////////////////////////////////////
// Section 44: Complex Scenarios - multi-level nesting
////////////////////////////////////////////////////////////

//
// 3 levels: Users → Posts → Comments
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					(init2) =>
						init2((eb2) => eb2.selectFrom("comments").select(["id", "content", "post_id"])),
					"comments.post_id",
					"posts.id",
				),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: {
				id: number;
				title: string;
				user_id: number;
				comments: { id: number; content: string; post_id: number }[];
			}[];
		}[]
	>();
}

//
// Mixed cardinality: one + many
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number; bio: string | null; user_id: number };
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

//
// Mixed nullability: inner + left
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"requiredProfile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"requiredProfile.user_id",
			"user.id",
		)
		.leftJoinOne(
			"optionalProfile",
			(nest) => nest((eb) => eb.selectFrom("profiles").select(["id", "user_id", "bio"])),
			"optionalProfile.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			requiredProfile: { id: number; bio: string | null; user_id: number };
			optionalProfile: { id: number; bio: string | null; user_id: number } | null;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 45: Complex Scenarios - attach inside join
////////////////////////////////////////////////////////////

//
// Join with nested attach
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) =>
				nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])).attachMany(
					"comments",
					async () => [] as Comment[],
					{ matchChild: "post_id", toParent: "id" },
				),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number; comments: Comment[] }[];
		}[]
	>();
}

//
// Attach with nested join (via modify)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany(
			"posts",
			() =>
				querySet(db).selectAs("post", (eb) =>
					eb.selectFrom("posts").select(["id", "user_id", "title"]),
				),
			{ matchChild: "user_id" },
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.attachMany("comments", async () => [] as Comment[], {
				matchChild: "post_id",
				toParent: "id",
			}),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string; comments: Comment[] }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 46: Complex Scenarios - collection modification chains
////////////////////////////////////////////////////////////

//
// Join → modify with attach
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.attachMany("comments", async () => [] as Comment[], {
				matchChild: "post_id",
				toParent: "id",
			}),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number; comments: Comment[] }[];
		}[]
	>();
}

//
// Attach → modify with map
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.attachMany("posts", async () => [] as Post[], { matchChild: "user_id" })
		.modify("posts", async (postsPromise) =>
			(await postsPromise).map((p) => ({ ...p, upperTitle: p.title.toUpperCase() })),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; user_id: number; title: string; content: string; upperTitle: string }[];
		}[]
	>();
}

//
// Multiple modifications on same collection
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			(nest) => nest((eb) => eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.extras({ titleLike: (p) => p.title.toLowerCase().includes("test") }),
		)
		.modify("posts", (postsQuerySet) =>
			postsQuerySet.extras({ titleUpper: (p) => p.title.toUpperCase() }),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: {
				id: number;
				title: string;
				user_id: number;
				titleLike: boolean;
				titleUpper: string;
			}[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Section 47: Error Cases - invalid selections
////////////////////////////////////////////////////////////

{
	querySet(db).selectAs(
		"user",
		db
			.selectFrom("users")
			// @ts-expect-error - invalid table-qualified column
			.select(["users.id", "users.nonExistent"]),
	);
}

{
	querySet(db).selectAs(
		"user",
		// @ts-expect-error - invalid table in selectFrom
		db.selectFrom("nonExistentTable"),
	);
}

////////////////////////////////////////////////////////////
// Section 48: Error Cases - invalid join columns
////////////////////////////////////////////////////////////

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"posts",
			(nest) =>
				nest((eb) =>
					eb
						.selectFrom("posts")
						// @ts-expect-error - invalid nested selection
						.select(["nonExistent"]),
				),
			"post.id",
			"user.id",
		);
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"posts",
			(nest) =>
				nest((eb) =>
					eb
						.selectFrom("posts")
						// @ts-expect-error - selecting from table not in nested query
						.select(["comments.content"]),
				),
			"post.id",
			"user.id",
		);
}
