import { expectTypeOf } from "expect-type";
import { type Kysely } from "kysely";

import { type SeedDB } from "./__tests__/fixture.ts";
import { createHydrator } from "./hydrator.ts";
import { hydrate } from "./query-builder.ts";

// Mock db instance type
type DB = Kysely<SeedDB>;
declare const db: DB;

//
// Basic usage: hydrate()
//

{
	// With explicit keyBy
	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id").execute();
	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();

	// keyBy omitted when row has 'id'
	const result2 = hydrate(db.selectFrom("users").select(["id", "username"])).execute();
	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number; username: string }[]>();

	// @ts-expect-error - keyBy required when row doesn't have 'id'
	hydrate(db.selectFrom("users").select(["username", "email"]));
}

//
// Select: properly types selections
//

{
	const result = hydrate(db.selectFrom("users").select("id"), "id").select("username").execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Modify: underlying query modifications
//

{
	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.modify((qb) => qb.where("id", "=", 1))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

//
// Extras: computed fields
//

{
	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.extras({
			displayName: (row) => {
				expectTypeOf(row).toEqualTypeOf<{ id: number; username: string }>();
				return `User: ${row.username}`;
			},
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{ id: number; username: string; displayName: string }[]
	>();
}

//
// mapFields: transform field values
//

{
	// Basic transformation
	const result1 = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.mapFields({
			username: (username) => {
				expectTypeOf(username).toEqualTypeOf<string>();
				return username.toUpperCase();
			},
		})
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; username: string }[]>();

	// Transform to different type
	const result2 = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.mapFields({
			username: (username) => username.length,
		})
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number; username: number }[]>();

	// Multiple transformations
	const result3 = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id")
		.mapFields({
			username: (username) => username.toUpperCase(),
			email: (email) => email.length,
		})
		.execute();

	expectTypeOf(result3).resolves.toEqualTypeOf<{ id: number; username: string; email: number }[]>();

	// Cannot pass true (only functions allowed)
	hydrate(db.selectFrom("users").select(["id", "username"]), "id").mapFields({
		// @ts-expect-error - Type 'boolean' is not assignable to function
		username: true,
	});

	// Field must exist in LocalRow
	hydrate(db.selectFrom("users").select(["id", "username"]), "id").mapFields({
		// @ts-expect-error - 'nonExistent' does not exist in type
		nonExistent: (x: any) => x,
	});
}

//
// Execution methods: execute, executeTakeFirst, executeTakeFirstOrThrow
//

{
	// execute: returns array
	const result1 = hydrate(db.selectFrom("users").select(["id", "username"]), "id").execute();
	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; username: string }[]>();

	// executeTakeFirst: returns first result or undefined
	const result2 = hydrate(
		db.selectFrom("users").select(["id", "username"]),
		"id",
	).executeTakeFirst();
	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number; username: string } | undefined>();

	// executeTakeFirstOrThrow: returns first result (non-nullable)
	const result3 = hydrate(
		db.selectFrom("users").select(["id", "username"]),
		"id",
	).executeTakeFirstOrThrow();
	expectTypeOf(result3).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

//
// toQuery: returns underlying Kysely query builder
//

{
	const query = hydrate(db.selectFrom("users").select(["id", "username"]), "id").toQuery();
	expectTypeOf(query).toEqualTypeOf<
		import("kysely").SelectQueryBuilder<SeedDB, "users", { id: number; username: string }>
	>();
}

//
// hasMany: nested array collection
//

{
	// With explicit keyBy
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]), "id")
		.hasMany(
			"posts",
			(qb) =>
				qb.innerJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
			"id",
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string }[];
		}[]
	>();

	// With keyBy omitted (nested row has 'id')
	const result2 = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb.innerJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
		)
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string }[];
		}[]
	>();
}

//
// hasOne: nullable or non-nullable based on join type
//

{
	// innerJoin with explicit keyBy: non-nullable
	const result1 = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasOne(
			"profile",
			(qb) => qb.innerJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id"]),
			"id",
		)
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number };
		}[]
	>();

	// innerJoin with keyBy omitted: non-nullable
	const result2 = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasOne("profile", (qb) =>
			qb.innerJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id"]),
		)
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number };
		}[]
	>();

	// leftJoin with explicit keyBy: nullable
	const result3 = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasOne(
			"profile",
			(qb) => qb.leftJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id"]),
			"id",
		)
		.execute();

	expectTypeOf(result3).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number } | null;
		}[]
	>();

	// leftJoin with keyBy omitted: nullable
	const result4 = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasOne("profile", (qb) =>
			qb.leftJoin("profiles", "profiles.user_id", "users.id").select(["profiles.id"]),
		)
		.execute();

	expectTypeOf(result4).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: { id: number } | null;
		}[]
	>();
}

//
// hasOneOrThrow: non-nullable nested object
//

{
	// With explicit keyBy
	const result1 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOneOrThrow(
			"author",
			(qb) =>
				qb.innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
			"id",
		)
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();

	// With keyBy omitted (nested row has 'id')
	const result2 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOneOrThrow("author", (qb) =>
			qb.leftJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();
}

//
// attachMany: attached array collection
//

{
	interface Post {
		id: number;
		user_id: number;
		title: string;
	}

	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
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
// attachOne: nullable attached object
//

{
	interface Profile {
		user_id: number;
		bio: string;
	}

	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.attachOne(
			"profile",
			async (users) => {
				expectTypeOf(users).toEqualTypeOf<{ id: number; username: string }[]>();
				return [] as Profile[];
			},
			{ matchChild: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			profile: Profile | null;
		}[]
	>();
}

//
// attachOneOrThrow: non-nullable attached object
//

{
	interface Settings {
		user_id: number;
		theme: string;
	}

	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.attachOneOrThrow(
			"settings",
			async (users) => {
				expectTypeOf(users).toEqualTypeOf<{ id: number; username: string }[]>();
				return [] as Settings[];
			},
			{ matchChild: "user_id" },
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			settings: Settings;
		}[]
	>();
}

//
// Nested extras
//

{
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.extras({
					titleUpper: (post) => {
						expectTypeOf(post).toEqualTypeOf<{ id: number; title: string }>();
						return post.title.toUpperCase();
					},
				}),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; titleUpper: string }[];
		}[]
	>();
}

//
// Nested mapFields
//

{
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.mapFields({
					title: (title) => {
						expectTypeOf(title).toEqualTypeOf<string>();
						return title.toUpperCase();
					},
				}),
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
// omit: removes fields from output type
//

{
	// Basic omit
	const result1 = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id")
		.omit(["email"])
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; username: string }[]>();

	// Multiple fields omitted
	const result2 = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id")
		.omit(["username", "email"])
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number }[]>();

	// @ts-expect-error - cannot omit non-existent field
	hydrate(db.selectFrom("users").select(["id", "username"]), "id").omit(["nonExistent"]);
}

//
// Nested omit
//

{
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title", "posts.content"])
				.omit(["content"]),
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
// extend: merges hydrator configuration
//

{
	interface User {
		id: number;
		username: string;
		email: string;
	}

	// Basic extend with fields
	const extraFields = createHydrator<User>("id").fields({ email: true });

	const result1 = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id")
		.with(extraFields)
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<{ id: number; username: string; email: string }[]>();

	// Extend with field mappings
	const withMapping = createHydrator<User>("id").fields({
		username: (username) => username.toUpperCase(),
	});

	const result2 = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id")
		.with(withMapping)
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<{ id: number; username: string; email: string }[]>();

	// Other hydrator's field types take precedence
	const override = createHydrator<{ id: number; username: string }>("id").fields({
		username: (username) => {
			expectTypeOf(username).toEqualTypeOf<string>();
			return username.length;
		},
	});

	const result3 = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.mapFields({
			username: (username) => username.toUpperCase(),
		})
		.with(override)
		.execute();

	expectTypeOf(result3).resolves.toEqualTypeOf<{ id: number; username: number }[]>();

	// Cannot extend with hydrator that has fields not in LocalRow
	hydrate(db.selectFrom("users").select(["id", "username"]), "id").with(
		// @ts-expect-error - extraField not in LocalRow
		createHydrator<{ id: number; username: string; extraField: string }>("id"),
	);
}

//
// Multi-level nesting
//

{
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.hasMany("comments", (qb2) =>
					qb2
						.innerJoin("comments", "comments.post_id", "posts.id")
						.select(["comments.id", "comments.content"]),
				),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: {
				id: number;
				title: string;
				comments: { id: number; content: string }[];
			}[];
		}[]
	>();
}

//
// Mixed patterns: attach inside has
//

{
	interface Tag {
		post_id: number;
		name: string;
	}

	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.attachMany(
					"tags",
					async (posts) => {
						expectTypeOf(posts).toEqualTypeOf<{ id: number; title: string }[]>();
						return [] as Tag[];
					},
					{ matchChild: "post_id", toParent: "id" },
				),
		)
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			posts: { id: number; title: string; tags: Tag[] }[];
		}[]
	>();
}

//
// Join methods: different join types affect nullability
//

{
	// innerJoin: non-nullable
	const result1 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) =>
			qb.innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();

	// leftJoin: nullable
	const result2 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) =>
			qb.leftJoin("users", "users.id", "posts.user_id").select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string } | null;
		}[]
	>();

	// innerJoinLateral: non-nullable
	const result3 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) =>
			qb
				.innerJoinLateral(
					() => db.selectFrom("users").select(["users.id", "users.username"]).as("users"),
					(join) => join.onRef("users.id", "=", "posts.user_id"),
				)
				.select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result3).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();

	// leftJoinLateral: nullable
	const result4 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) =>
			qb
				.leftJoinLateral(
					() => db.selectFrom("users").select(["users.id", "users.username"]).as("users"),
					(join) => join.onRef("users.id", "=", "posts.user_id"),
				)
				.select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result4).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string } | null;
		}[]
	>();

	// crossJoin: non-nullable
	const result5 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) => qb.crossJoin("users").select(["users.id", "users.username"]))
		.execute();

	expectTypeOf(result5).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();

	// crossJoinLateral: non-nullable
	const result6 = hydrate(db.selectFrom("posts").select(["posts.id", "posts.title"]))
		.hasOne("author", (qb) =>
			qb
				.crossJoinLateral(() =>
					db.selectFrom("users").select(["users.id", "users.username"]).as("users"),
				)
				.select(["users.id", "users.username"]),
		)
		.execute();

	expectTypeOf(result6).resolves.toEqualTypeOf<
		{
			id: number;
			title: string;
			author: { id: number; username: string };
		}[]
	>();
}

//
// Invalid keys: reject nonsense keys
//

{
	// @ts-expect-error - invalid keyBy in hydrate()
	hydrate(db.selectFrom("users").select(["id", "username"]), "nonExistent");

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasMany(
		"posts",
		// @ts-expect-error - keyBy required when nested row doesn't have 'id' in hasMany
		(qb) => qb.innerJoin("posts", "posts.user_id", "users.id").select(["posts.title"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasOne(
		"profile",
		// @ts-expect-error - keyBy required when nested row doesn't have 'id' in hasOne
		(qb) => qb.innerJoin("profiles", "profiles.user_id", "users.id").select(["profiles.bio"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasOneOrThrow(
		"profile",
		// @ts-expect-error - keyBy required when nested row doesn't have 'id' in hasOneOrThrow
		(qb) => qb.innerJoin("profiles", "profiles.user_id", "users.id").select(["profiles.bio"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasMany(
		"posts",
		(qb) => qb.leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
		// @ts-expect-error - invalid keyBy in hasMany
		"invalid",
	);

	interface Post {
		id: number;
		title: string;
	}

	hydrate(db.selectFrom("users").select(["id", "username"])).attachMany(
		"posts",
		async (_users) => [] as Post[],
		// @ts-expect-error - invalid matchChild
		{ matchChild: "invalid" },
	);

	hydrate(db.selectFrom("users").select(["id", "username"])).attachMany(
		"posts",
		async (_users) => [] as Post[],
		// @ts-expect-error - invalid toParent
		{ matchChild: "id", toParent: "invalid" },
	);
}

//
// Invalid selections and joins: reject invalid column/table references
//

{
	// @ts-expect-error - invalid column in select
	hydrate(db.selectFrom("users").select(["id", "nonExistentColumn"]));

	// @ts-expect-error - invalid table-qualified column
	hydrate(db.selectFrom("users").select(["users.id", "users.nonExistent"]));

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasMany(
		"posts",
		// @ts-expect-error - invalid column in nested select
		(qb) => qb.innerJoin("posts", "posts.user_id", "users.id").select(["posts.nonExistent"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasMany(
		"posts",
		// @ts-expect-error - invalid join condition: non-existent column on left
		(qb) => qb.innerJoin("posts", "posts.nonExistent", "users.id").select(["posts.id"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasMany(
		"posts",
		// @ts-expect-error - invalid join condition: non-existent column on right
		(qb) => qb.innerJoin("posts", "posts.user_id", "users.nonExistent").select(["posts.id"]),
	);

	hydrate(db.selectFrom("users").select(["users.id", "users.username"])).hasOne("profile", (qb) =>
		qb
			.innerJoin("profiles", "profiles.user_id", "users.id")
			// @ts-expect-error - selecting from table not in query
			.select(["comments.content"]),
	);

	// @ts-expect-error - invalid table in selectFrom
	hydrate(db.selectFrom("nonExistentTable").select(["id"]));
}

//
// map(): transformations
//

{
	// Basic map: transforms output type
	const result1 = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string }>();
			return { userId: user.id, userName: user.username };
		})
		.execute();

	expectTypeOf(result1).resolves.toEqualTypeOf<{ userId: number; userName: string }[]>();

	// Chaining maps
	const result2 = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string }>();
			return { ...user, upper: user.username.toUpperCase() };
		})
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string; upper: string }>();
			return { final: user.upper };
		})
		.execute();

	expectTypeOf(result2).resolves.toEqualTypeOf<{ final: string }[]>();

	// After map, only map(), modify(), toQuery() and execute methods are available
	const mapped = hydrate(db.selectFrom("users").select(["id", "username"]), "id").map((u) => u.id);

	// These should still be available
	const stillMapped = mapped.map((id) => ({ transformed: id }));
	expectTypeOf(stillMapped.execute()).resolves.toEqualTypeOf<{ transformed: number }[]>();

	// @ts-expect-error - cannot call mapFields() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.mapFields;

	// @ts-expect-error - cannot call extras() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.extras;

	// @ts-expect-error - cannot call omit() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.omit;

	// @ts-expect-error - cannot call with() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.with;

	// @ts-expect-error - cannot call hasMany() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasMany;

	// @ts-expect-error - cannot call hasOne() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasOne;

	// @ts-expect-error - cannot call hasOneOrThrow() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.hasOneOrThrow;

	// @ts-expect-error - cannot call attachMany() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachMany;

	// @ts-expect-error - cannot call attachOne() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOne;

	// @ts-expect-error - cannot call attachOneOrThrow() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.attachOneOrThrow;

	// @ts-expect-error - cannot call select() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.select;

	// @ts-expect-error - cannot call innerJoin() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoin;

	// @ts-expect-error - cannot call leftJoin() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.leftJoin;

	// @ts-expect-error - cannot call crossJoin() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.crossJoin;

	// @ts-expect-error - cannot call innerJoinLateral() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.innerJoinLateral;

	// @ts-expect-error - cannot call leftJoinLateral() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.leftJoinLateral;

	// @ts-expect-error - cannot call crossJoinLateral() after map()
	// oxlint-disable-next-line no-unused-expressions
	mapped.crossJoinLateral;
}

//
// map() with nested collections
//

{
	// Nested map
	const result = hydrate(db.selectFrom("users").select(["users.id", "users.username"]))
		.hasMany("posts", (qb) =>
			qb
				.innerJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				// Map child
				.map((post) => {
					expectTypeOf(post).toEqualTypeOf<{ id: number; title: string }>();
					return { postId: post.id, postTitle: post.title };
				}),
		)
		// Map parent
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{
				id: number;
				username: string;
				// The mapped post type:
				posts: { postId: number; postTitle: string }[];
			}>();
			return { userName: user.username, postCount: user.posts.length };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postCount: number }[]>();
}

//
// map() with attached collections
//

{
	interface Post {
		id: number;
		user_id: number;
		title: string;
	}

	const result = hydrate(db.selectFrom("users").select(["id", "username"]), "id")
		.attachMany(
			"posts",
			async (users): Promise<Post[]> => {
				expectTypeOf(users).toEqualTypeOf<{ id: number; username: string }[]>();
				return [];
			},
			{ matchChild: "user_id" },
		)
		.map((user) => {
			expectTypeOf(user).toEqualTypeOf<{ id: number; username: string; posts: Post[] }>();
			return { userName: user.username, postTitles: user.posts.map((p) => p.title) };
		})
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ userName: string; postTitles: string[] }[]>();
}

//
// map() with extend/with()
//

{
	interface User {
		id: number;
		username: string;
		email: string;
	}

	// Cannot call with() on a mapped query builder
	const baseQb = hydrate(db.selectFrom("users").select(["id", "username", "email"]), "id");

	const mappedQb = baseQb.map((u) => ({ userId: u.id }));

	// @ts-expect-error - cannot call with() after map()
	// oxlint-disable-next-line no-unused-expressions
	mappedQb.with;

	// Extending with a mapped hydrator returns MappedHydratedQueryBuilder
	const mappedHydrator = createHydrator<User>("id")
		.fields({ id: true })
		.map((u) => ({ userId: u.id }));

	const extendedWithMapped = baseQb.with(mappedHydrator);
	const result1 = extendedWithMapped.execute();
	expectTypeOf(result1).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string; userId: number }[]
	>();

	// @ts-expect-error - cannot call extras() after with() using a mapped hydrator
	// oxlint-disable-next-line no-unused-expressions
	extendedWithMapped.extras;

	// Extending with a full hydrator returns HydratedQueryBuilder
	const fullHydrator = createHydrator<User>("id").extras({ displayName: (u) => u.username });

	const extendedWithFull = baseQb.with(fullHydrator);
	expectTypeOf(extendedWithFull.execute()).resolves.toEqualTypeOf<
		{ id: number; username: string; email: string; displayName: string }[]
	>();

	// This is allowed - can call extras() after with() when using a full hydrator
	const extendedWithExtras = extendedWithFull.extras({ idSquared: (u) => u.id ** 2 });
	expectTypeOf(extendedWithExtras.execute()).resolves.toEqualTypeOf<
		{
			id: number;
			username: string;
			email: string;
			displayName: string;
			idSquared: number;
		}[]
	>();
}
