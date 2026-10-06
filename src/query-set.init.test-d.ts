import { expectTypeOf } from "expect-type";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

////////////////////////////////////////////////////////////
// Initialization (.selectAs)
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

{
	// Valid: override default keyBy when "id" is selected
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]), "username")
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
	querySet(db)
		// @ts-expect-error - factory keyBy required when no "id"
		.selectAs("user", (eb) => eb.selectFrom("users").select(["username"]));
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
	querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["id"]),
		// @ts-expect-error - invalid keyBy (with default key by)
		"invalid",
	);
}

{
	querySet(db).selectAs(
		"user",
		db.selectFrom("users").select(["username"]),
		// @ts-expect-error - invalid keyBy (without default key by)
		"invalid",
	);
}

{
	querySet(db).selectAs(
		"user",
		(db) => db.selectFrom("users").select(["id"]),
		// @ts-expect-error - invalid keyBy (with default key by)
		"nonExistent",
	);
}

{
	querySet(db).selectAs(
		"user",
		(db) => db.selectFrom("users").select(["username"]),
		// @ts-expect-error - invalid keyBy (without default key by)
		"invalid",
	);
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

//////
// Nested initialization
//////

//
// Default keyBy inference
//

{
	// Valid: default keyBy when "id" is selected
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(db.selectFrom("posts").select(["id", "user_id"]));

				expectTypeOf(nested.execute()).resolves.toEqualTypeOf<{ id: number; user_id: number }[]>();

				return nested;
			},
			"posts.user_id",
			"user.id",
		)
		.execute();
}

{
	// Valid: override default keyBy when "id" is selected
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(db.selectFrom("posts").select(["id", "user_id"]), "user_id");

				expectTypeOf(nested.execute()).resolves.toEqualTypeOf<{ id: number; user_id: number }[]>();

				return nested;
			},
			"posts.user_id",
			"user.id",
		)
		.execute();
}

//
// Explicit keyBy required
//

{
	// Valid: explicit keyBy when "id" not selected
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(db.selectFrom("posts").select(["title", "user_id"]), "title");

				expectTypeOf(nested.execute()).resolves.toEqualTypeOf<
					{ title: string; user_id: number }[]
				>();

				return nested;
			},
			"posts.user_id",
			"user.id",
		)
		.execute();
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				// @ts-expect-error - keyBy required when no "id"
				const nested = qs(db.selectFrom("posts").select(["title", "user_id"]));

				return nested;
			},
			// @ts-expect-error - fallout from above
			"posts.user_id",
			"user.id",
		);
}

//
// Invalid keyBy
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(
					db.selectFrom("posts").select(["id", "user_id"]),
					// @ts-expect-error - invalid keyBy (with default key by)
					"invalid",
				);

				return nested;
			},
			"posts.user_id",
			"user.id",
		);
}

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(
					db.selectFrom("posts").select(["user_id"]),
					// @ts-expect-error - invalid keyBy (without default key by)
					"invalid",
				);

				return nested;
			},
			"posts.user_id",
			"user.id",
		);
}

//
// Composite keyBy
//

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.innerJoinMany(
			"posts",
			({ qs }) => {
				const nested = qs(db.selectFrom("posts").select(["id", "user_id"]), ["user_id", "id"]);

				expectTypeOf(nested.execute()).resolves.toEqualTypeOf<{ id: number; user_id: number }[]>();

				return nested;
			},
			"posts.user_id",
			"user.id",
		)
		.execute();
}

////////////////////////////////////////////////////////////
// Error Cases - invalid selections
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
