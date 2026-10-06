import { expectTypeOf } from "expect-type";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

////////////////////////////////////////////////////////////
// Join Methods - innerJoinOne
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable single object
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
			({ eb, qs }) =>
				qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])).innerJoinOne(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["user_id", "content"]), "user_id"),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id"])),
			"profile.id",
			// @ts-expect-error - invalid right column
			"user.nonExistent",
		);
}

////////////////////////////////////////////////////////////
// Join Methods - innerJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
// Join Methods - leftJoinOne
////////////////////////////////////////////////////////////

//
// Basic usage: nullable single object
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
// Join Methods - leftJoinOneOrThrow
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable (throws if missing)
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
		.leftJoinOneOrThrow(
			"author",
			({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
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
// Join Methods - leftJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: non-nullable array (empty if no matches)
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
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
			posts: { id: number; title: string; user_id: number }[];
		}[]
	>();
}

////////////////////////////////////////////////////////////
// Join Methods - crossJoinMany
////////////////////////////////////////////////////////////

//
// Basic usage: cartesian product as array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.crossJoinMany("comments", ({ eb, qs }) =>
			qs(eb.selectFrom("comments").select(["id", "content"])),
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
// Join Methods - lateral
////////////////////////////////////////////////////////////

// A lateral join's body may reference the base query's columns (here via
// `whereRef`). Result types match the non-lateral counterparts.

//
// innerJoinLateralOne: non-nullable object
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinLateralOne(
			"profile",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("profiles")
						.select(["id", "bio", "user_id"])
						.whereRef("profiles.user_id", "=", "user.id"),
				),
			(join) => join.onTrue(),
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
// innerJoinLateralMany: array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinLateralMany(
			"posts",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						.select(["id", "title", "user_id"])
						.whereRef("posts.user_id", "=", "user.id")
						.limit(2),
				),
			(join) => join.onTrue(),
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
// leftJoinLateralOne: nullable object
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinLateralOne(
			"profile",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("profiles")
						.select(["id", "bio", "user_id"])
						.whereRef("profiles.user_id", "=", "user.id"),
				),
			(join) => join.onTrue(),
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

//
// leftJoinLateralOneOrThrow: non-nullable object
//

{
	const result = querySet(db)
		.selectAs("post", db.selectFrom("posts").select(["id", "title", "user_id"]))
		.leftJoinLateralOneOrThrow(
			"author",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("users")
						.select(["id", "username"])
						.whereRef("users.id", "=", "post.user_id"),
				),
			(join) => join.onTrue(),
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

//
// leftJoinLateralMany: non-nullable array
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinLateralMany(
			"posts",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						.select(["id", "title", "user_id"])
						.whereRef("posts.user_id", "=", "user.id"),
				),
			(join) => join.onTrue(),
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
// crossJoinLateralMany: no join condition
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.crossJoinLateralMany("posts", ({ eb, qs }) =>
			qs(eb.selectFrom("posts").select(["id", "title"]).whereRef("posts.user_id", "=", "user.id")),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string }[];
		}[]
	>();
}

//
// Invalid: the base alias is in scope, but its columns are still checked
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinLateralMany(
			"posts",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						.select(["id", "user_id"])
						// @ts-expect-error - "user" does not select "email"
						.whereRef("posts.user_id", "=", "user.email"),
				),
			(join) => join.onTrue(),
		);
}

////////////////////////////////////////////////////////////
// Join Methods - adjacent joins are independent
////////////////////////////////////////////////////////////

// A join's ON clause (and a lateral join's body) sees only the base query and
// the query set being joined, never an adjacent join: count, exists and
// paginated queries drop or rewrite many-joins, so a reference to one would
// break them.  Nest the join instead.

{
	const users = querySet(db).selectAs("user", db.selectFrom("users").select(["id"]));
	const posts = querySet(db).selectAs("posts", db.selectFrom("posts").select(["id", "user_id"]));
	const profile = querySet(db).selectAs(
		"profile",
		db.selectFrom("profiles").select(["id", "user_id"]),
	);
	const comments = querySet(db).selectAs(
		"comments",
		db.selectFrom("comments").select(["id", "post_id"]),
	);
	const withPosts = users.leftJoinMany("posts", posts, "posts.user_id", "user.id");

	// Invalid: a one-join referencing an adjacent many-join
	// @ts-expect-error - "posts" is not in scope
	withPosts.leftJoinOne("profile", profile, "profile.user_id", "posts.user_id");

	// Invalid: the same with a callback
	withPosts.leftJoinOne("profile", profile, (j) =>
		// @ts-expect-error - "posts" is not in scope
		j.onRef("profile.user_id", "=", "posts.user_id"),
	);

	// Invalid: a filtering many-join referencing an adjacent many-join
	// @ts-expect-error - "posts" is not in scope
	withPosts.innerJoinMany("comments", comments, "comments.post_id", "posts.id");

	// Invalid: a join referencing an adjacent one-join
	users
		.leftJoinOne("profile", profile, "profile.user_id", "user.id")
		// @ts-expect-error - "profile" is not in scope
		.leftJoinMany("posts", posts, "posts.user_id", "profile.user_id");

	// Invalid: a lateral join's body referencing an adjacent many-join
	withPosts.leftJoinLateralMany(
		"comments",
		({ eb, qs }) =>
			qs(
				eb
					.selectFrom("comments")
					.select(["id", "post_id"])
					// @ts-expect-error - "posts" is not in scope
					.whereRef("comments.post_id", "=", "posts.id"),
			),
		(j) => j.onTrue(),
	);

	// Valid: nesting the join instead
	users.leftJoinMany(
		"posts",
		posts.innerJoinMany("comments", comments, "comments.post_id", "posts.id"),
		"posts.user_id",
		"user.id",
	);
}

////////////////////////////////////////////////////////////
// Error Cases - invalid join columns
////////////////////////////////////////////////////////////

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"posts",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						// @ts-expect-error - invalid nested selection
						.select(["nonExistent"]),
				),
			"posts.id",
			"user.id",
		);
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinOne(
			"posts",
			({ eb, qs }) =>
				qs(
					eb
						.selectFrom("posts")
						// @ts-expect-error - selecting from table not in nested query
						.select(["comments.content"]),
				),
			"posts.id",
			"user.id",
		);
}

////////////////////////////////////////////////////////////
// Error Cases - Cannot nest writes
////////////////////////////////////////////////////////////

{
	const write = querySet(db).insertAs(
		"foo",
		db
			.insertInto("users")
			.values({
				email: "test",
				username: "test",
			})
			.returningAll(),
	);

	querySet(db)
		.selectAs("posts", db.selectFrom("posts").select(["id", "user_id"]))
		.innerJoinMany(
			"user",
			// @ts-expect-error Cannot nest a write query set
			write,
			"posts.user_id",
			"user.id",
		);
}
