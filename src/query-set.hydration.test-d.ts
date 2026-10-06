import { expectTypeOf } from "expect-type";
import * as k from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import { createHydrator } from "./hydrator.ts";
import { type InferOutput, querySet } from "./query-set.ts";

const db = getDbForTest();

// Shared test data

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

////////////////////////////////////////////////////////////
// Hydration - extras
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
// Hydration - extend
////////////////////////////////////////////////////////////

//
// Return type is correctly inferred through QuerySet
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.extend((row) => ({
			displayName: `${row.username} <${row.email}>`,
			nameLength: row.username.length,
		}))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			email: string;
			displayName: string;
			nameLength: number;
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Hydration - mapFields
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
// Hydration - omit
////////////////////////////////////////////////////////////

//
// Omit after selectAs().insert()
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.insert(
			db.insertInto("users").values({ username: "test", email: "test@test.com" }).returningAll(),
		)
		.omit(["email"]);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit after selectAs().update()
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.update(
			db.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
		)
		.omit(["email"]);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit after selectAs().delete()
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.delete(db.deleteFrom("users").where("id", "=", 1).returningAll())
		.omit(["email"]);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit before insert (omit().insert())
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["email"])
		.insert(
			db.insertInto("users").values({ username: "test", email: "test@test.com" }).returningAll(),
		);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit before update (omit().update())
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["email"])
		.update(
			db.updateTable("users").set({ email: "new@test.com" }).where("id", "=", 1).returningAll(),
		);

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit before delete (omit().delete())
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.omit(["email"])
		.delete(db.deleteFrom("users").where("id", "=", 1).returningAll());

	const result = qs.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit single field
//

{
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);

	const originalResult = qs.execute();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	const result = qs.omit(["email"]).execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Omit multiple fields
//

{
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);

	const originalResult = qs.execute();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string }[]
	>();

	const result = qs.omit(["username", "email"]).execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number }[]>();
}

//
// Omit single field - executeTakeFirst
//

{
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);

	const originalResult = qs.executeTakeFirst();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string } | undefined
	>();

	const result = qs.omit(["email"]).executeTakeFirst();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string } | undefined>();
}

//
// Omit multiple fields - executeTakeFirst
//

{
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);

	const originalResult = qs.executeTakeFirst();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string } | undefined
	>();

	const result = qs.omit(["username", "email"]).executeTakeFirst();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number } | undefined>();
}

//
// Omit single field - executeTakeFirstOrThrow
//

{
	const qs = querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id", "username", "email"]),
	);

	const originalResult = qs.executeTakeFirstOrThrow();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<{
		id: number;
		username: string;
		email: string;
	}>();

	const result = qs.omit(["email"]).executeTakeFirstOrThrow();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

//
// Omit multiple fields - executeTakeFirstOrThrow
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
		.extras({ upperEmail: (row) => row.email.toUpperCase() });

	const originalResult = qs.executeTakeFirstOrThrow();

	expectTypeOf(originalResult).resolves.toEqualTypeOf<{
		id: number;
		username: string;
		email: string;
		upperEmail: string;
	}>();

	const result = qs.omit(["username", "email"]).executeTakeFirstOrThrow();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; upperEmail: string }>();
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
// Hydration - with
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
// Hydration chaining
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
// Nested hydration
////////////////////////////////////////////////////////////

//
// extras in nested join
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).extras({
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
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).mapFields({
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
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id", "content"])).omit(["content"]),
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
// Terminal .map() - basic
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
// Terminal .map() - limitations
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

	// @ts-expect-error - cannot call extend after map
	// oxlint-disable-next-line no-unused-expressions
	mapped.extend;
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
// Terminal .map() - nested
////////////////////////////////////////////////////////////

//
// Map in nested collection
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).map((post) => {
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
// Complex Scenarios - multi-level nesting
////////////////////////////////////////////////////////////

//
// 3 levels: Users → Posts → Comments
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).leftJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"requiredProfile.user_id",
			"user.id",
		)
		.leftJoinOne(
			"optionalProfile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "user_id", "bio"])),
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
// $castTo
////////////////////////////////////////////////////////////

{
	// Basic cast changes output type
	interface CustomOutput {
		userId: number;
		name: string;
	}

	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.$castTo<CustomOutput>()
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<CustomOutput[]>();
}

{
	// Multiple $castTo calls chain correctly
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.$castTo<{ a: number }>()
		.$castTo<{ b: string }>()
		.$castTo<{ c: boolean }>()
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ c: boolean }[]>();
}

{
	// $castTo returns QuerySet, so join methods are still available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.$castTo<{ id: number; username: string }>();

	const result = qs
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{ id: number; username: string; posts: { id: number; title: string; user_id: number }[] }[]
	>();
}

{
	// $castTo().map() returns MappedQuerySet (no QuerySet methods)
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.$castTo<{ id: number; username: string }>()
		.map((user) => user.id);

	expectTypeOf(mapped.execute()).resolves.toEqualTypeOf<number[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinMany;
}

////////////////////////////////////////////////////////////
// $narrowType
////////////////////////////////////////////////////////////

{
	// $narrowType narrows nullable fields
	const qs = querySet(db).selectAs(
		"profile",
		db.selectFrom("profiles").select(["id", "user_id", "avatar_url"]),
	);

	const result1 = qs.execute();
	expectTypeOf(result1).resolves.toEqualTypeOf<
		{ id: number; user_id: number; avatar_url: string | null }[]
	>();

	const result = qs.$narrowType<{ avatar_url: string }>().execute();
	expectTypeOf(result).resolves.toEqualTypeOf<
		{ id: number; user_id: number; avatar_url: string }[]
	>();

	// Can also use k.NotNull to narrow
	const result2 = qs.$narrowType<{ avatar_url: k.NotNull }>().execute();
	expectTypeOf(result2).resolves.toEqualTypeOf<
		{ id: number; user_id: number; avatar_url: string }[]
	>();
}

{
	// $narrowType returns QuerySet, so join methods are still available
	const qs = querySet(db)
		.selectAs("profile", db.selectFrom("profiles").select(["id", "user_id"]))
		.$narrowType<{ id: number }>();

	const result = qs
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"profile.user_id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{ id: number; user_id: number; posts: { id: number; title: string; user_id: number }[] }[]
	>();
}

{
	// $narrowType works on MappedQuerySet (after .map())
	const mapped = querySet(db)
		.selectAs("profile", db.selectFrom("profiles").select(["id", "avatar_url"]))
		.map((row) => ({ visibleId: row.id, url: row.avatar_url }))
		.$narrowType<{ url: string }>();

	expectTypeOf(mapped.execute()).resolves.toEqualTypeOf<{ visibleId: number; url: string }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinMany;
}

////////////////////////////////////////////////////////////
// $assertType
////////////////////////////////////////////////////////////

{
	// $assertType works when types match (with extras for complexity)
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		.$assertType<{ id: number; username: string; upper: string }>()
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string; upper: string }[]>();
}

{
	// $assertType fails when asserted type is a subset (missing fields)
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		// @ts-expect-error - asserted type missing 'upper' field
		.$assertType<{ id: number; username: string }>();
}

{
	// $assertType fails when asserted type is a superset (extra fields)
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		.$assertType<{ id: number; username: string; upper: string; extra: boolean }>()
		// @ts-expect-error - asserted type has extra 'extra' field
		.execute();
}

{
	// $assertType returns QuerySet, so join methods are still available
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		.$assertType<{ id: number; username: string; upper: string }>();

	const result = qs
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			upper: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

{
	// $assertType works on MappedQuerySet (after .map())
	const mapped = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		.map((row) => ({ visibleId: row.id, name: row.upper }))
		.$assertType<{ visibleId: number; name: string }>();

	expectTypeOf(mapped.execute()).resolves.toEqualTypeOf<{ visibleId: number; name: string }[]>();

	// @ts-expect-error - cannot call innerJoinMany on MappedQuerySet
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinMany;
}

////////////////////////////////////////////////////////////
// InferOutput
////////////////////////////////////////////////////////////

{
	// InferOutput on QuerySet
	const usersQuerySet = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() });

	type User = InferOutput<typeof usersQuerySet>;
	expectTypeOf<User>().toEqualTypeOf<{ id: number; username: string; upper: string }>();
}

{
	// InferOutput on MappedQuerySet (after .map())
	const mappedQuerySet = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((row) => ({ visibleId: row.id }));

	type MappedUser = InferOutput<typeof mappedQuerySet>;
	expectTypeOf<MappedUser>().toEqualTypeOf<{ visibleId: number }>();
}

////////////////////////////////////////////////////////////
// hydrate
////////////////////////////////////////////////////////////

//
// hydrate: single row returns single output
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	const row: { id: number; username: string } = { id: 1, username: "alice" };
	const result = qs.hydrate(row);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

//
// hydrate: iterable of rows returns array of outputs
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	const rows: { id: number; username: string }[] = [];
	const result = qs.hydrate(rows);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// hydrate: accepts a Promise of a single row
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	const rowPromise: Promise<{ id: number; username: string }> = Promise.resolve({
		id: 1,
		username: "alice",
	});
	const result = qs.hydrate(rowPromise);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

//
// hydrate: accepts a Promise of rows
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	const rowsPromise: Promise<{ id: number; username: string }[]> = Promise.resolve([]);
	const result = qs.hydrate(rowsPromise);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// hydrate: works with nested joins (input is flat joined shape)
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		);

	// Input type is the flat joined query shape
	const rows: {
		id: number;
		username: string;
		posts$$id: number;
		posts$$title: string;
		posts$$user_id: number;
	}[] = [];

	const result = qs.hydrate(rows);

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

//
// hydrate: works with extras and mapFields
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.extras({ upper: (row) => row.username.toUpperCase() })
		.mapFields({ id: (v) => String(v) });

	const rows: { id: number; username: string }[] = [];
	const result = qs.hydrate(rows);

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: string; username: string; upper: string }[]>();
}

//
// hydrate: works on MappedQuerySet (after .map())
//

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.map((user) => ({ userId: user.id, name: user.username }));

	const rows: { id: number; username: string }[] = [];
	const result = qs.hydrate(rows);

	expectTypeOf(result).resolves.toEqualTypeOf<{ userId: number; name: string }[]>();
}

//
// hydrate: compatible with toQuery().execute() return type
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	const result = qs.hydrate(qs.toQuery().execute());

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// hydrate: rejects invalid input shape
//

{
	const qs = querySet(db).selectAs("user", db.selectFrom("users").select(["id", "username"]));

	// @ts-expect-error - input shape doesn't match joined query output
	qs.hydrate({ id: 1, nonExistent: "bad" });

	// @ts-expect-error - wrong field types
	qs.hydrate({ id: "not a number", username: 123 });

	// @ts-expect-error - promise of wrong shape
	qs.hydrate(Promise.resolve({ wrong: true }));
}
