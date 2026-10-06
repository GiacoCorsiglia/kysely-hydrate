import { expectTypeOf } from "expect-type";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

// Shared test data

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
// Modification - base query
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
// Modification - join collection
////////////////////////////////////////////////////////////

//
// Modify joined QuerySet with filtering
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
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
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
// Multiple modifications on same collection
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
// Modification - attach collection (QuerySet)
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
// Modification - attach collection (SelectQueryBuilder)
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
// Modification - attach collection (external)
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
// Modification - invalid collection key
////////////////////////////////////////////////////////////

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		// @ts-expect-error - collection key doesn't exist
		.modify("nonExistent", (qs: any) => qs);
}

////////////////////////////////////////////////////////////
// .where() convenience
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
