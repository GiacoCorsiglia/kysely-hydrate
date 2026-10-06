import { expectTypeOf } from "expect-type";

import { getDbForTest } from "./__tests__/db.ts";
import { querySet } from "./query-set.ts";

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

interface Comment {
	id: number;
	post_id: number;
	content: string;
}

////////////////////////////////////////////////////////////
// Attach Methods - attachMany
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
// Attach Methods - attachOne
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
// Attach Methods - attachOneOrThrow
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
// Complex Scenarios - attach inside join
////////////////////////////////////////////////////////////

//
// Join with nested attach
//

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).attachMany(
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
