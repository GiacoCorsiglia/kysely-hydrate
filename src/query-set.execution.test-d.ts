import { expectTypeOf } from "expect-type";
import * as k from "kysely";

import { getDbForTest } from "./__tests__/db.ts";
import {
	type SeedDB,
	type User as DBUser,
	type Profile as DBProfile,
} from "./__tests__/fixture.ts";
import { querySet } from "./query-set.ts";

const db = getDbForTest();

type InferDB<T> = T extends k.SelectQueryBuilder<infer DB, any, any> ? DB : never;
type InferO<T> = T extends k.SelectQueryBuilder<any, any, infer O> ? O : never;
type InferTB<T> = T extends k.SelectQueryBuilder<any, infer TB, any> ? TB : never;

////////////////////////////////////////////////////////////
// Pagination
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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

// Rejects many-join columns: a base row has no single value to order by

{
	querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.leftJoinMany(
			"posts",
			({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "user_id"])),
			"posts.user_id",
			"user.id",
		)
		// @ts-expect-error - many-join columns are not orderable
		.orderBy("posts$$title");
}

// clearOrderBy and orderByKeys keep the query set's type

{
	const qs = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.orderBy("username");

	expectTypeOf(qs.clearOrderBy()).toEqualTypeOf<typeof qs>();
	expectTypeOf(qs.orderByKeys()).toEqualTypeOf<typeof qs>();
	expectTypeOf(qs.orderByKeys(false)).toEqualTypeOf<typeof qs>();
}

////////////////////////////////////////////////////////////
// Query Compilation - toBaseQuery
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
// Query Compilation - toJoinedQuery
////////////////////////////////////////////////////////////

{
	const joined = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.innerJoinOne(
			"profile",
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
			({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
			"profile.user_id",
			"user.id",
		)
		.innerJoinMany(
			"posts",
			({ eb, qs }) =>
				qs(eb.selectFrom("posts").select(["id", "title", "user_id"])).innerJoinMany(
					"comments",
					({ eb, qs }) => qs(eb.selectFrom("comments").select(["id", "content", "post_id"])),
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
// Query Compilation - toQuery
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
// Query Compilation - toCountQuery
////////////////////////////////////////////////////////////

{
	const countQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toCountQuery();

	const result = countQuery.execute();
	expectTypeOf(result).resolves.toEqualTypeOf<{ count: string | number | bigint }[]>();
}

////////////////////////////////////////////////////////////
// Query Compilation - toExistsQuery
////////////////////////////////////////////////////////////

{
	const existsQuery = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.toExistsQuery();

	const result = existsQuery.execute();
	expectTypeOf(result).resolves.toEqualTypeOf<{ exists: k.SqlBool }[]>();
}

////////////////////////////////////////////////////////////
// Execution - execute
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.execute();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }[]>();
}

////////////////////////////////////////////////////////////
// Execution - executeTakeFirst
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirst();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string } | undefined>();
}

////////////////////////////////////////////////////////////
// Execution - executeTakeFirstOrThrow
////////////////////////////////////////////////////////////

{
	const result = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id", "username"]))
		.executeTakeFirstOrThrow();

	expectTypeOf(result).resolves.toEqualTypeOf<{ id: number; username: string }>();
}

////////////////////////////////////////////////////////////
// Execution - executeCount
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
// Execution - executeExists
////////////////////////////////////////////////////////////

{
	const exists = querySet(db)
		.selectAs("user", db.selectFrom("users").select(["id"]))
		.modify((qb) => qb.where("username", "=", "alice"))
		.executeExists();

	expectTypeOf(exists).resolves.toEqualTypeOf<boolean>();
}
