[![NPM
Version](https://img.shields.io/npm/v/kysely-hydrate?`style=flat&`label=latest)](https://github.com/GiacoCorsiglia/kysely-hydrate/releases)
[![License](https://img.shields.io/github/license/GiacoCorsiglia/kysely-hydrate?style=flat)](https://github.com/GiacoCorsiglia/kysely-hydrate/blob/master/LICENSE)
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/GiacoCorsiglia/kysely-hydrate/ci.yml)](https://github.com/GiacoCorsiglia/kysely-hydrate/actions/workflows/ci.yml)

> [!WARNING]
> This is an early release. **Expect breaking changes.**

# Kysely Hydrate

A TypeScript library that extends the [Kysely](https://kysely.dev) query builder
with utilities for hydrating SQL output into rich, nested JavaScript objects.

## Introduction

Kysely is a beautiful library. It marries the power of SQL with the type safety
and expressiveness of TypeScript for building queries. You tell Kysely what SQL
to produce—any SQL you want—and it gives you a well-typed result.

However, the result matches whatever your database driver returns. Typically,
this means flat objects even if your query includes joins (`post.authorName`
instead of `post.author.name`) and primitive types (`{ id: 1 }`) that you must
manually hydrate into the rich types your application expects (`{ id: new UserId(1) }`).

Most ORMs, on the other hand, constrain you to a subset of SQL, written with an
idiosyncratic syntax that obscures the underlying SQL. In return, your query
results are hydrated into rich objects, including converting joins into nested
properties (`user.posts`). However, when you need something custom—even a
simple `COALESCE()` statement—you typically must drop into writing raw SQL, and
sacrifice all of the benefits of your ORM.

Kysely Hydrate grants Kysely the ability to produce rich, nested objects without
compromising the power or control of SQL. It offers these features:

- [Nested objects from traditional joins](#joins-and-hydration)
- [Application-level joins](#application-level-joins-with-attach)
- [Mapped fields](#mapped-properties-with-mapfields) in hydrated queries
- [Computed properties](#computed-properties-with-extras) in hydrated queries
- [Hydrated writes](#hydrated-writes) (INSERT/UPDATE/DELETE with RETURNING)
- [Counts, ordering, and limits](#pagination-and-aggregation) accounting for row explosion from nested joins

For example:

```ts
import { querySet } from "kysely-hydrate";

const categoriesQuerySet = querySet(db)
	.selectAs("category", db.selectFrom("categories").select(["id", "name"]))
	// Add computed fields and other application-level transformations.
	.extras({
		upperName: (row) => row.name.toUpperCase(),
	});

const postsQuerySet = querySet(db).selectAs(
	"posts",
	db.selectFrom("posts").select((eb) => [
		"id",
		"title",
		"categoryId",
		// Embed whatever SQL you want:
		eb
			.selectFrom("comments")
			.select(eb.fn.countAll().as("count"))
			.whereRef("comments.postId", "=", "posts.id")
			.as("commentsCount"),
	]),
);

const userQuerySet = await querySet(db)
	// Initialize with a base select query and an alias ("user")
	.selectAs("user", db.selectFrom("users").select(["id", "email"]))
	// Add a database-level LEFT JOIN that hydrates into a "posts" array
	.leftJoinMany(
		"posts",
		// Compose query sets to create a nested collection.
		postsQuerySet,
		// Join conditions (referencing the aliases "post" and "user")
		"posts.user_id",
		"user.id",
	)
	// Modify collections after they've been added to the query set.
	.modify("posts", (posts) =>
		// Application-level join: Attach category to posts
		posts.attachOneOrThrow(
			"category",
			async (posts) =>
				categoriesQuerySet.where(
					"id",
					"in",
					posts.map((p) => p.categoryId),
				),
			{ matchChild: "id", toParent: "categoryId" },
		),
	);

// Count with deduplication.
const count = await userQuerySet.executeCount();

// Execute the query and hydrate the result.
const users = await userQuerySet.execute();
// ⬇ Result:
type Result = Array<{
	id: number;
	email: string;

	posts: Array<{
		id: number;
		title: string;
		commentsCount: number;
		categoryId: number;

		category: {
			id: number;
			name: string;
			// Includes computed field:
			upperName: string;
		};
	}>;
}>;
```

## Table of Contents

- [Installation](#installation)
- [Query sets](#query-sets)
  - [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)
  - [Joins and hydration](#joins-and-hydration)
  - [Modifying queries with `.modify()`](#modifying-queries-with-modify)
  - [Application-level joins with `.attach*()`](#application-level-joins-with-attach)
  - [Sorting with `.orderBy()`](#sorting-with-orderby)
  - [Pagination and aggregation](#pagination-and-aggregation)
  - [Inspect the SQL](#inspecting-the-sql)
  - [Mapped properties with `.mapFields()`](#mapped-properties-with-mapfields)
  - [Computed properties with `.extras()`](#computed-properties-with-extras)
  - [Excluded properties with `.omit()`](#excluded-properties-with-omit)
  - [Output transformations with `.map()`](#output-transformations-with-map)
  - [Composable mappings with `.with()`](#composable-mappings-with-with)
  - [Hydrated writes](#hydrated-writes)
- [Hydrators](#hydrators)
  - [Creating hydrators with `createHydrator()`](#creating-hydrators-with-createhydrator)
  - [Manual hydration with `hydrate()`](#manual-hydration-with-hydrate)
  - [Selecting and mapping fields with `.fields()`](#selecting-and-mapping-fields-with-fields)
  - [Computed properties with `.extras()`](#computed-properties-with-extras-1)
  - [Excluding fields with `.omit()`](#excluding-fields-with-omit)
  - [Output transformations with `.map()`](#output-transformations-with-map-1)
  - [Attached collections with `.attach*()`](#attached-collections-with-attach)
  - [Prefixed collections with `.has*()`](#prefixed-collections-with-has)
  - [Composing hydrators with `.extend()`](#composing-hydrators-with-extend)
- [FAQ](#faq)

## Installation

Kysely is a peer dependency:

```bash
npm install kysely kysely-hydrate
```

## Query sets

The `querySet` helper allows you to build queries that automatically hydrate flat SQL results into nested objects and arrays. Unlike standard ORMs, its API that gives you maximal control over the SQL generated at every level.

It allows you to:

- Compose joins that hydrate into nested properties (`innerJoinOne`, `leftJoinMany`, etc.).
- Batch-fetch related data using application-level joins (`attachMany`, `attachOne`).
- Transform data with mapping and computed fields (`mapFields`, `extras`,).
- Pagination that works correctly even with one-to-many joins.

To start, initialize a query set by providing a database instance, a **base alias**, and a **base query**:

```ts
import { querySet } from "kysely-hydrate";

// Select users and give the base row the alias "user"
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.execute();
// ⬇
type Result = Array<{ id: number; username: string }>;
```

> [!WARNING]
> When using the `querySet()` API, you cannot use `.selectAll()`, because Kysely
> Hydrate must introspect your queries for the names of their selections. Query
> set execution will throw if it encounters a wildcard selection.

### Keying and deduplication with keyBy

Hydration works by grouping the flat rows returned by your query into entities.
The `keyBy` argument tells Kysely Hydrate how to **uniquely identify each entity** in
the result set. This allows it to deduplicate parent rows when joins multiply
them (row explosion) and group nested collections correctly.

`keyBy` can be either:

- A single key, like `"id"` (default) or `"uuid"`.
- A composite key, like `["orderId", "productId"]`.

#### Special `"id"` behavior:

- If the row type has an `"id"` property, `keyBy` is optional and defaults to `"id"`.
- If the row type does not have an `"id"` property, you must provide `keyBy`.

```ts
// Default: only allowed by TypeScript if you have selected "id"
querySet(db).selectAs("user", db.selectFrom("users").select(["id", "name"]));

// Explicit: use a specific unique column
querySet(db).selectAs("product", db.selectFrom("products").select(["sku", "name"]), "sku");

// Composite: use multiple columns
querySet(db).selectAs(
	"item",
	db.selectFrom("order_items").select(["orderId", "productId", "quantity"]),
	["orderId", "productId"],
);
```

### Joins and hydration

Instead of "mapping" joins after they happen, Kysely Hydrate treats joins as structural definitions. When you add a join to a query set, you define both the SQL join and the shape of the output (object or array) simultaneously.

Nested query sets are isolated in subqueries to prevent naming collisions and ensure correct scoping.

#### "One" relations (objects) with `.*JoinOne()`

Use `innerJoinOne` or `leftJoinOne` to hydrate a single nested object.

- `innerJoinOne`: The relationship is required. Base rows without a match are
  excluded (by your database). Result is `T`.
- `leftJoinOne`: The relationship is optional. Result is `T | null`.

To add a join, pass a query set to one of the join methods:

```ts
const profileQuerySet = querySet(db).selectAs(
	"profile",
	db.selectFrom("profiles").select(["id", "bio", "userId"]),
);

const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.innerJoinOne(
		"userProfile", // The key for the nested object on the parent.
		profileQuerySet,
		// Join condition (referenced by alias).
		"userProfile.userId",
		"user.id",
	)
	.execute();
// ⬇
type Result = Array<{
	id: number;
	username: string;
	userProfile: { id: number; bio: string; userId: number };
}>;
```

You can also define a nested query set inline with the following syntax, which
is identical to the above.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.innerJoinOne(
		"profile", // The key for the nested object on the parent.
		(nest) =>
			nest(
				"profile", // Alias for the nested table
				(eb) => eb.selectFrom("profiles").select(["id", "bio", "userId"]),
			),
		// Join condition (referenced by alias)
		"profile.userId",
		"user.id",
	);
```

There is also `leftJoinOneOrThrow`, which performs a SQL Left Join but throws an
error during hydration if the relationship is missing.

> [!NOTE]
> Kysely Hydrate's pagination logic depends on `*One` joins producing zero or
> one rows and no more. The library will throw an error during hydration
> if your query returns multiple rows for a `*One` join (e.g., multiple profiles
> for the same user).

#### "Many" relations (arrays) with `.*JoinMany()`

Use `innerJoinMany` or `leftJoinMany` to hydrate a nested array of objects.

- `leftJoinMany`: Returns an array `T[]` (empty if no matches). Parent rows are preserved.
- `innerJoinMany`: Returns an array `T[]`. Parent rows without matches are
  excluded.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.leftJoinMany(
		"posts",
		(nest) => nest("post", (eb) => eb.selectFrom("posts").select(["id", "title", "authorId"])),
		"post.authorId",
		"user.id",
	)
	.execute();
// ⬇
type Result = Array<{
	id: number;
	username: string;
	posts: Array<{ id: number; title: string; authorId: number }>;
}>;
```

#### Supported join types

All standard join types are supported with the same hydration logic:

- `innerJoinOne` / `innerJoinLateralOne`
- `innerJoinMany` / `innerJoinLateralMany`
- `leftJoinOne` / `leftJoinLateralOne`
- `leftJoinOneOrThrow` / `leftJoinLateralOneOrThrow`
- `leftJoinMany` / `leftJoinLateralMany`
- `crossJoinMany` / `crossJoinLateralMany`

#### How it works (SQL generation)

Kysely Hydrate prioritizes correctness and predictability. It uses subqueries
and column aliasing to ensure that your joins don't interfere with each other
and that features like pagination work intuitively, even with complex nested
data.

##### Isolation and prefixing

To hydrate nested objects from a flat result set, Kysely Hydrate automatically
"hoists" selections from joined subqueries and renames them using a unique
separator (`$$`).

When you define a join, the nested query set is wrapped in a subquery to isolate
its logic (e.g., to shield adjacent joins from the filtering effects of nested
inner joins).

```ts
const query = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.innerJoinMany(
		"posts",
		(nest) => nest("post", (eb) => eb.selectFrom("posts").select(["id", "title"])),
		"post.userId",
		"user.id",
	)
	.execute();
```

**Generated SQL:**

```sql
SELECT
  "user"."id",
  "user"."username",
  "posts"."id" as "posts$$id",  -- Prefixed for hydration
  "posts"."title" as "posts$$title"
FROM (
  SELECT "id", "username" FROM "users"  -- Base query
)  as "user"  -- Base alias
INNER JOIN (
  SELECT "id", "title", "userId" FROM "posts"
) as "posts" ON "posts"."userId" = "user"."id"
```

The hydration layer receives rows like `{ id: 1, posts$$title: "..." }` and
un-flattens them into `{ id: 1, posts: [{ title: "..." }] }`.

##### Solving "row explosion" with pagination

A common pain point in SQL is paginating the "one" in a one-to-many
relationship. If you `LIMIT 10` on a query joining Users to Posts, you might
only get 2 users if they each have 5 posts (because the join "explodes" the row
count to 10).

Kysely Hydrate solves this automatically. When you apply `.limit()` or
`.offset()` to a query set with many-joins, it generates a query structure that
applies the limit to the parent rows first.

```ts
// Get the first 10 users, plus all their posts
const result = await querySet(db)
	.selectAs("user", ...)
	.innerJoinMany("posts", ...) // Has-many join
	.limit(10)
	.execute();
```

###### Generated SQL strategy:

1 **Inner Query**: Selects the parent rows, applying the `LIMIT 10` here. This
inner query will include "cardinality-one" joins (`*One()`), so you can use them
in filtering. "Cardinality-many" filtering joins (`innerJoinMany` or
`crossJoinMany`) will be converted to a WHERE EXISTS to filter without causing
row explosion. 2. **Outer Query**: Joins the "many" relations to the limited set of parents.

For example, a query for "users who have posted" looks something like this:

```sql
SELECT *
FROM (
  -- 1. Apply limit to parents only
  SELECT "user".*
  FROM (
    SELECT * FROM "users"
  ) as "user"
  WHERE EXISTS (SELECT 1 FROM "posts" WHERE ...) -- Ensure join condition matches
  LIMIT 10
) as "user"
-- 2. Join children to the specific page of parents
INNER JOIN (
  SELECT * FROM "posts"
) "posts" ON "posts"."userId" = "user"."id"
```

This guarantees that `limit(10)` returns exactly 10 user objects, fully
hydrated with all their posts.

The outer query is omitted if there are no "many" relations.

##### Lateral joins

Because querySet creates isolated units of logic, it naturally supports lateral
joins. This allows you to perform "top N per group" queries or correlated
subqueries while still getting hydrated output.

```ts
// Get users and their LATEST 3 posts
const query = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id"]))
	.leftJoinLateralMany(
		"latestPosts",
		(nest) =>
			nest("post", (eb) =>
				eb
					.selectFrom("posts")
					.select(["id", "title"])
					.whereRef("posts.userId", "=", "user.id") // Correlated reference
					.orderBy("createdAt", "desc")
					.limit(3),
			),
		(join) => join.onTrue(),
	);
```

This compiles to a standard `LEFT JOIN LATERAL`, hoisting the columns
`latestPosts$$id` and `latestPosts$$title` just like any other join.

### Modifying queries with `.modify()`

You can modify the base query or any nested query set or attached collection
using `.modify()`. This is how you add `WHERE` clauses, extra selections, or
modifiers to specific parts of your relationship tree.

#### Modifying the base query

Provide a callback as the only argument to `.modify()` to modify the base query.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.modify((qb) => qb.where("isActive", "=", true)); // Add a WHERE clause
```

Because adding where clauses is so common, the above is equivalent to:

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.where("isActive", "=", true); // Add a WHERE clause to the base query
```

#### Modifying a nested collection

Pass the key of the collection to modify it.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select("id"))
	.leftJoinMany(
		"posts",
		(nest) => nest("post", db.selectFrom("posts").select(["id", "title", "userId"])),
		"post.userId",
		"user.id",
	)
	// Modify the query set for the "posts" collection
	.modify("posts", (postsQuery) => postsQuery.where("isPublished", "=", true))
	.execute();
```

### Application-level joins with `.attach*()`

These methods fetch related data in a separate query (or any async function) to
avoid complex SQL joins or to fetch data from non-SQL sources.

- `attachOne`: `T | null`
- `attachMany`: `T[]`
- `attachOneOrThrow`: `T`

Kysely Hydrate handles the "N+1" problem by batching the fetch for all parent
rows: The fetch function you provide to `attach*()` will be called exactly once
per execution, no matter how deeply it is nested.

```ts
const posts = await querySet(db)
	.selectAs("post", db.selectFrom("posts").select(["id", "title", "authorId"]))
	.attachOne(
		"author",
		// 1. Receive all parent rows
		async (posts) => {
			const authorIds = posts.map((p) => p.authorId);
			// 2. Return matching rows
			return db.selectFrom("users").selectAll().where("id", "in", authorIds).execute();
		},
		// 3. Define how to match child rows back to parents
		{ matchChild: "id", toParent: "authorId" },
	)
	.execute();
// ⬇
type Result = Array<{
	id: number;
	title: string;
	authorId: number;
	author: { id: number; name: string } | null;
}>;
```

You should return attached entities in the order in which you wish for them to
be attached to their parent. The `fetchFn` for `attachOne` should still return
an **array/iterable** containing _all_ matching records for the whole batch of
parents. Kysely Hydrate groups those child rows per parent and then takes the
**first** match (or `null` if there is none).

> [!TIP]
> If your match function returns a query set—or any object with an `execute`
> method, such as Kysely's `SelectQueryBuilder`—the `execute` method will be
> called during hydration.

#### Matching attached rows back to parents (`{ matchChild, toParent }`)

The third argument to `.attachMany()` (and the other `.attach*()` methods) tells
Kysely Hydrate how to match the attached rows back to their parents:

- `matchChild`: the key (or keys) on the attached rows
- `toParent` (optional): the key (or keys) on the parent rows

If you omit `toParent`, it defaults to the parent collection’s `keyBy` (which
itself defaults to `"id"` when available).

```ts
.attachMany(
  "posts",
  async (users) => {
    // 1. Get all user IDs from the parent rows
    const userIds = users.map(u => u.id);

    // 2. Fetch all posts for these users in one query
    return db.selectFrom("posts")
      .select(["id", "title", "userId"])
      .where("userId", "in", userIds)
      .execute();
  },
  // 3. Define how to match child rows (posts) back to parent rows (users)
  { matchChild: "userId" }
)
```

Here's an example where `toParent` is _not_ `"id"`: attaching an author to posts
by matching `authors.id` to `posts.authorId`:

```ts
const posts = await querySet(db)
	.selectAs("posts", db.selectFrom("posts").select(["posts.id", "posts.title", "posts.authorId"]))
	.attachOne(
		"author",
		async (posts) =>
			db
				.selectFrom("authors")
				.select(["authors.id", "authors.name"])
				.where(
					"authors.id",
					"in",
					posts.map((p) => p.authorId),
				)
				.execute(),
		{ matchChild: "id", toParent: "authorId" },
	)
	.execute();
// ⬇
type Result = Array<{
	id: number;
	title: string;
	authorId: number;
	author: { id: number; name: string } | null;
}>;
```

Because the `fetchFn` can be any async function, `.attachMany()` is also useful
for things that _aren’t_ database rows: HTTP calls, caches, etc.

```ts
// Example: Attach feature flags from a cached HTTP endpoint
const users = await querySet(db)
	.selectAs("users", db.selectFrom("users").select(["users.id", "users.email"]))
	.attachMany(
		"flags",
		async (users) => {
			const userIds = users.map((u) => u.id);

			// This could be backed by a CDN, an in-memory cache, Redis, etc.
			const result = await flagsClient.getFlagsForUsers(userIds);

			// Must return an array/iterable of rows with a key that matches back to the parent
			return result.flags.map((f) => ({ userId: f.userId, name: f.name }));
		},
		{ matchChild: "userId" },
	)
	.execute();
```

#### Modifying attached collections

You can also use the `.modify()` method to map the result of your attach's fetch
function during hydration before any entities are attached to their parent.
This is especially useful if your attach function returns a query set:

```ts
const authorsQuerySet = querySet(db)
  .selectAs("authors", db
    .selectFrom("authors")
    .select(["authors.id", "authors.name"])
  );

const posts = await querySet(db)
  .selectAs("posts", db
    .selectFrom("posts")
    .select(["posts.id", "posts.title", "posts.authorId"])
  ),
  .attachOne(
    "author",
    async (posts) =>
      // This attach function returns a query set.
      authorsQuerySet.where("authors.id", "in", posts.map((p) => p.authorId)),
    { matchChild: "id", toParent: "authorId" },
  )
  .modify("author", (qs) => qs.modify((qb) => qb.select('authors.country')))
  .execute();
// ⬇
type Result = Array<{
  id: number;
  title: string;
  authorId: number;
  author: { id: number; name: string; country: string; } | null;
}>;
```

### Overwriting collections

If you repeat a `.*Join*()` or `.attach*()` call with the same key, it will
overwrite the previous definition of the collection on the query set.

### Sorting with `.orderBy()`

Ordering is critical for consistent hydration, especially when using pagination.

By default, query sets automatically orders results by unique key (your `keyBy`
columns) in ascending order. This ensures stable results and deterministic row
deduplication.

When you add your own `.orderBy()`, Kysely Hydrate applies your sorts first, but
still appends the unique key(s) at the end as a tie-breaker.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	// Sort by username descending
	.orderBy("username", "desc")
	.execute();
// SQL: ... ORDER BY "user"."username" DESC, "user"."id" ASC
```

> [!TIP]
> Because it will be used for ordering, your `keyBy` columns should be indexed.
> This will typically be the case, as `keyBy` will likely be your table's primary
> key column(s).

#### Sorting by joined columns

You can pass any selected column from your base query to `.orderBy()`.

In addition, you can order by columns from cardinality-one joins (e.g.
`innerJoinOne`, `leftJoinOne`) by using the prefixed alias (`relation$$column`),
although you should be wary of the performance implications of doing so,
especially if these columns are not indexed.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	.innerJoinOne(
		"profile",
		(nest) => nest("p", (eb) => eb.selectFrom("profiles").select(["id", "bio", "userId"])),
		"profile.userId",
		"user.id",
	)
	// Sort by the joined profile's bio
	.orderBy("profile$$bio", "asc")
	.execute();
```

> [!NOTE]
> You cannot order by columns from "Many" joins (e.g. `innerJoinMany`) at the
> top level, because this would break the grouping of the result set. If you
> want to order by aggregations of a "many" join, modify your base query with a
> correlated subquery in a `.select()`.

#### Sorting nested many-relations

Consider the following example:

```ts
const usersQuerySet = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username"]))
	// Order users by username.
	.orderBy("username")
	.leftJoinMany(
		"posts",
		// Order users.posts by title, per user
		postQuerySet.orderBy("title"),
		"posts.user_id",
		"user.id",
	)
	.leftJoinMany(
		"visits",
		// Order users.visits by title, per user
		postQuerySet.orderBy("visitDate"),
		"visits.user_id",
		"user.id",
	);
```

In general, SQL does not guarantee ordering of subqueries, and specifically it
cannot maintain the per-user ordering of multiple many-relations simultaneously.

Instead of applying the nested sort in SQL, Kysely Hydrate will apply it during
Hydration, with a best-effort attempt to make the sorting semantics match SQL
semantics. This works reasonably well, but if you depend on your database' more
advanced sorting capabilities for nested collections, you must use the
`.attach()` APIs for application-level joins instead.

#### Removing sorting

- `clearOrderBy()`: Removes your custom sorts, but keeps the automatic unique key
  sort.
- `orderByKeys(false)`: Disables the automatic unique key sort entirely (not
  recommended if using pagination).

### Pagination and aggregation

Kysely Hydrate solves the "pagination with joins" problem. When you use
`.limit()` or `.offset()` on a query set with `*Many` joins, the library
automatically wraps your query as described
[above](#solving-row-explosion-with-pagination) to ensure the limit applies to
the parent entities, not the exploded SQL rows.

```ts
const result = await querySet(db)
  .selectAs("user", db.selectFrom("users").select("id"))
  .leftJoinMany("posts", ...)
  .limit(10) // Returns exactly 10 users, even if they have 1000 posts combined
  .offset(20)
  .execute();
```

> [!NOTE]
> You typically want to use `querySet.limit()` directly, instead of adding a limit to the
> base query via `querySet.modify()`. Adding a limit to the base query fails to
> account for the filtering effect of inner joins on your hydrated query.

### Counting

Use `executeCount()` to get the total number of unique base records, ignoring
pagination. It correctly handles filtering joins by converting them to `WHERE
EXISTS` clauses to avoid row multiplication.

### Existence

Use `executeExists()` to check if any records match the query.

### Inspecting the SQL

You can inspect the generated SQL using `.toQuery()`, `.toJoinedQuery()`, or `.toBaseQuery()`.

- `toQuery()`: Returns the exact query that `execute()` will run.
- `toCountQuery()` Returns the exact query that `executeCount()` will run.
- `toExistsQuery()` Returns the exact query that `executeExists()` will run.
- `toJoinedQuery()`: Returns the query with all joins applied (subject to row explosion).
- `toBaseQuery()`: Returns the base query without any joins (but with modifications).

### Mapped properties with `.mapFields()`

Transform individual fields in the result set. This changes the output type for
those fields but does not change the underlying SQL; the mapping runs in
JavaScript after the query.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "email", "metadata"]))
	.mapFields({
		// email: string -> string
		email: (email) => email.toLowerCase(),
		// metadata: string -> { plan: string }
		metadata: (json) => JSON.parse(json) as { plan: string },
	})
	.execute();
// ⬇
type Result = Array<{
	id: number;
	email: string;
	metadata: { plan: string };
}>;
```

### Computed properties with `.extras()`

Add new properties derived from the entire row. Extras are computed in
JavaScript after the query runs.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "firstName", "lastName"]))
	.extras({
		fullName: (row) => `${row.firstName} ${row.lastName}`,
	})
	.execute();
// ⬇
type Result = Array<{
	id: number;
	firstName: string;
	lastName: string;
	fullName: string;
}>;
```

### Excluded properties with `.omit()`

Remove fields from the final output. This is useful for removing intermediate
fields used for computed properties.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "firstName", "lastName"]))
	.extras({
		fullName: (row) => `${row.firstName} ${row.lastName}`,
	})
	// Hide intermediate fields
	.omit(["firstName", "lastName"])
	.execute();
// ⬇
type Result = Array<{ id: number; fullName: string }>;
```

### Output transformations with `.map()`

The `.map()` method transforms the hydrated output into a different shape. Use
it for complex transformations like:

- Converting plain objects into class instances
- Asserting discriminated union types
- Restructuring or reshaping data

Unlike `.mapFields()` and `.extras()`, which operate on individual fields,
`.map()` receives the complete hydrated result and returns a new entity.

```ts
class UserModel {
	constructor(
		public id: number,
		public name: string,
	) {}
}

const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "name"]))
	.map((user) => new UserModel(user.id, user.name))
	.execute();
// ⬇
type Result = UserModel[];
```

#### Chaining transformations

You can chain multiple `.map()` calls. Each function receives the output of the
previous transformation.

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "name"]))
	.map((user) => ({ ...user, nameUpper: user.name.toUpperCase() }))
	.map((user) => ({ id: user.id, display: user.nameUpper }))
	.execute();
// ⬇
type Result = Array<{ id: number; display: string }>;
```

#### Transforming nested collections

Like all query set methods, `.map()` works with nested collections too. You can
apply transformations to child entities inside their query definition, and then
to parents:

```ts
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id"]))
	.leftJoinMany(
		"posts",
		(nest) =>
			nest("post", (eb) => eb.selectFrom("posts").select(["id", "title"]))
				// Transform child:
				.map((post) => ({ postId: post.id, postTitle: post.title })),
		"post.userId",
		"user.id",
	)
	// Transform parent:
	.map((user) => ({
		userId: user.id,
		postCount: user.posts.length,
		posts: user.posts,
	}))
	.execute();
// ⬇
type Result = Array<{
	userId: number;
	postCount: number;
	posts: { postId: number; postTitle: string };
}>;
```

#### Terminal operation

`.map()` is a terminal operation. After calling `.map()`, you can only chain
further `.map()` calls or execute the query. You cannot call configuration
methods like `.mapFields()`, `.extras()`, or `.leftJoinMany()` afterwards.

This is intentional: those methods would affect the input type expected by your
transformation function, which could break your mapping logic.

```ts
const mapped = querySet(db)
  .selectAs("user", ...)
  .map((user) => ({ userId: user.id }));

// ✅ These work:
mapped.map((data) => ({ transformed: data.userId }));
mapped.execute();

// ❌ These don't compile:
mapped.mapFields({ ... });
mapped.leftJoinMany(...);
```

### Composable mappings with `.with()`

Re-use hydration logic by importing it from another [`Hydrator`](#creating-hydrators-with-createhydrator). This is great for
sharing consistent formatting logic across different queries.

```ts
import { createHydrator, querySet } from "kysely-hydrate";

// Define once:
const userHydrator = createHydrator<{
	id: number;
	username: string;
	email: string;
}>("id")
	.extras({
		displayName: (u) => `${u.username} <${u.email}>`,
	})
	.omit(["email"]);

// Reuse in query #1:
const users = await querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.with(userHydrator)
	.execute();
// ⬇
type Result1 = Array<{ id: number; username: string; displayName: string }>;

// Reuse in query #2 (different root query, same hydration rules):
const author = await querySet(db)
	.selectAs("user", (eb) =>
		eb
			.selectFrom("posts")
			.innerJoin("users", "users.id", "posts.authorId")
			.select(["users.id", "users.username", "users.email"])
			.where("posts.id", "=", 123),
	)
	.with(userHydrator)
	.executeTakeFirst();
// ⬇
type Result2 = { id: number; username: string; displayName: string } | undefined;
```

#### `.map()` vs `.mapFields()` and `.extras()`

When should you use `.map()` vs the more targeted methods?

- **Use `.mapFields()`** when you want to transform individual fields by name
  (e.g., normalizing strings)
- **Use `.extras()`** when you want to add computed fields while keeping the
  existing structure
- **Use `.map()`** when you need to:
  - Convert to class instances
  - Completely reshape the output
  - Apply transformations that depend on the full hydrated result
  - Assert or narrow types on the entire output shape

The targeted methods are more composable, because they can be interleaved with
joins, unlike `.map()`.

### Hydrated writes

Kysely Hydrate can also hydrate the results of `INSERT`, `UPDATE`, and `DELETE`
statements. This allows you to write data and get back a fully hydrated result—
including mapped fields, computed extras, and even nested joins—in a single round
trip.

> [!NOTE]
> This feature relies on data-modifying CTEs and `RETURNING` clauses, which only some database
> dialects support.

#### Initializing with writes (`querySet().*As()`)

You can initialize a query set directly with a write query using `insertAs`,
`updateAs`, or `deleteAs`.

The write query is wrapped in a CTE, so you can join other data to the result just
like a normal `SELECT` query.

```ts
const newUser = await querySet(db)
	.insertAs("user", db.insertInto("users").values(newUserData).returning(["id", "username"]))
	.extras({
		upperName: (u) => u.username.toUpperCase(),
	})
	.executeTakeFirstOrThrow();
// ⬇
type Result = {
	id: number;
	username: string;
	upperName: string;
};
```

#### Reusing query sets for writes

A powerful pattern is to define a "canonical" query set for fetching an entity,
and then reuse that definition for writes. This ensures that your application
always receives consistent objects, whether they come from a `SELECT` or an
`INSERT`.

Use the `.insert()`, `.update()`, or `.delete()` methods to switch the base query
of an existing query set to a write operation.

The write query must return columns compatible with the original base query.

```ts
// 1. Define the canonical way to fetch a user
const usersQuerySet = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.extras({
		gravatarUrl: (u) => getGravatar(u.email),
	});

// 2. Reuse it for an insert
const newUser = await usersQuerySet
	.insert((db) =>
		db
			.insertInto("users")
			.values(newUserData)
			// Must return columns matching the base query
			.returning(["id", "username", "email"]),
	)
	.executeTakeFirstOrThrow();
// ⬇ Result has gravatarUrl computed automatically!
type Result = {
	id: number;
	username: string;
	email: string;
	gravatarUrl: string;
};
```

## Hydrators

The `querySet()` API described above is the happy path when you’re building a
query in Kysely and want nested results.

Hydrators are the lower-level API: they let you take _already-fetched_ rows
(from Kysely, raw SQL, a view, an API, anywhere) and hydrate them into nested
objects using the same core logic. `querySet` uses a `Hydrator` under the hood.

Use hydrators when:

- You already have flat rows and want to hydrate them; or,
- You want to define reusable hydration logic independent of any particular query.

> [!NOTE]
> Hydrators don’t “know” what you selected. Unlike `querySet()`, you need to
> specify what you want in the output using `.fields()` (and/or `.extras()`).

### Creating hydrators with `createHydrator()`

Creates a new, empty hydrator configuration.

Like `querySet()`, hydrators use `keyBy` to group and deduplicate entities. The
same rules apply (see [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)).

```ts
// Group by "id"
const h = createHydrator<User>();

// Group by composite key
const h2 = createHydrator<OrderItem>(["orderId", "productId"]);
```

### Manual hydration with `hydrate()`

Hydrates an array of flat objects using a configured hydrator.

```ts
const flatRows = await db
	.selectFrom("users")
	.leftJoin("posts", "posts.userId", "users.id")
	.select([
		"users.id",
		"users.username",
		// Manual prefixing to match the hydrator's expectation:
		"posts.id as posts$$id",
		"posts.title as posts$$title",
	])
	.execute();

const nestedUsers = await hydrate(flatRows, hydrator);
```

You can create the `hydrator` using the dedicated `createHydrator()` helper (see
below), or you can create it inline by providing a callback.

The inline form is handy for small, one-off hydrations, or for keeping the
hydration logic right next to the query that produces the flat rows:

```ts
type FlatRow = {
	id: number;
	username: string;
	posts$$id: number | null;
	posts$$title: string | null;
};

const nestedUsers = await hydrate(flatRows, (h) =>
	h()
		.fields({ id: true, username: true })
		.hasMany("posts", "posts$$", (h) => h().fields({ id: true, title: true })),
);
// ⬇
type Result = Array<{
	id: number;
	username: string;
	posts: Array<{ id: number | null; title: string | null }>;
}>;
```

> [!NOTE]
> The `h(...)` function is `createHydrator` in callback form. It accepts an optional
> `keyBy` argument with the same semantics described above:
>
> - `h()` defaults to `"id"` only when your row type has an `id` field
> - `h("pk")` for a non-`id` primary key
> - `h(["orderId", "productId"])` for composite keys

`hydrate()` also accepts a single object (not just arrays) and returns the
corresponding single hydrated object.

### Selecting and mapping fields with `.fields()`

Configures which fields to include and optionally how to transform them.

This only affects the hydrated output—it does not change your SQL. With hydrators,
any field you don't explicitly include is omitted from the output.

```ts
type UserRow = { id: number; username: string };

const hydrator = createHydrator<UserRow>().fields({
	id: true,
	username: true,
});
// ⬇
type Result = Array<{ id: number; username: string }>;
```

### Computed properties with `.extras()`

Computes new fields from the input row.

```ts
type UserRow = { id: number; username: string; email: string };

const hydrator = createHydrator<UserRow>().extras({
	displayName: (u) => `${u.username} <${u.email}>`,
});
// ⬇
type Result = Array<{ displayName: string }>;
```

### Excluding fields with `.omit()`

Excludes fields from the output that were already included.

```ts
type UserRow = { id: number; passwordHash: string };

const hydrator = createHydrator<UserRow>()
	.fields({ id: true, passwordHash: true })
	.omit(["passwordHash"]);
// ⬇
type Result = Array<{ id: number }>;
```

This method primarily exists for use by query sets, which include all fields by
default. It's not so useful in standalone Hydrators, in which you must
explicitly name the fields to include. The example above is equivalent to
`createHydrator<UserRow>().fields({ id: true })`.

### Output transformations with `.map()`

The `.map()` method works the same way as described in the [query sets
section](#output-transformations-with-map): it transforms the hydrated output
into a different shape, such as class instances or discriminated union types.

```ts
class UserModel {
	constructor(
		public id: number,
		public name: string,
	) {}
}

const hydrator = createHydrator<{ id: number; name: string }>()
	.fields({ id: true, name: true })
	.map((user) => new UserModel(user.id, user.name));

const users = await hydrate(rows, hydrator);
// ⬇
type Result = UserModel[];
```

As in query sets, `.map()` is a terminal operation—after calling it, you can
only call `.map()` again or `.hydrate()`. You cannot call configuration methods
like `.fields()`, `.extras()`, `.has*()`, or `.extend()`.

```ts
const mapped = createHydrator<User>()
  .fields({ id: true })
  .map((u) => ({ userId: u.id }));

// ✅ These work:
mapped.map((data) => ({ transformed: data.userId }));
mapped.hydrate(rows);

// ❌ These don't compile:
mapped.fields({ ... });   // Error: Property 'fields' does not exist
mapped.extend(...);       // Error: Property 'extend' does not exist
```

### Attached collections with `.attach*()`

These work the same as in the `querySet()` API (see the `.attach*()` section above).
They’re useful when your “rows” come from somewhere other than SQL, but you still
want to batch-fetch and attach related data.

### Prefixed collections with `.has*()`

Configures nested collections from flat, prefixed input data. This is primarily used
when you have a flat join result (possibly written manually) and want to hydrate it.

```ts
type FlatRow = {
	id: number;
	username: string;

	// Left-joined posts:
	posts$$id: number | null;
	posts$$title: string | null;

	// Left-joined comments on posts:
	posts$$comments$$id: number | null;
	posts$$comments$$content: string | null;
};

const hydrator = createHydrator<FlatRow>()
	.fields({ id: true, username: true })
	.hasMany("posts", "posts$$", (h) =>
		h()
			.fields({ id: true, title: true })
			.hasMany("comments", "comments$$", (h) => h().fields({ id: true, content: true })),
	);
// ⬇
type Result = Array<{
	id: number;
	username: string;
	posts: Array<{
		id: number | null;
		title: string | null;
		comments: Array<{ id: number | null; content: string | null }>;
	}>;
}>;
```

`hasOne` and `hasOneOrThrow` are also supported.

Notice that every single field in the nested result types are nullable. This
happens because we cannot know if `posts$$title` is nullable because (a) it is a
non-nullable column that was made nullable by a left join; or, (b) it's actually
nullable in the "posts" table. The query set API, on the other hand, _does_
know the difference, and so does not suffer from this problem.

### Composing hydrators with `.extend()`

Merges two hydrators. The second hydrator's configuration takes precedence.

This is a good way to build small, reusable hydrators (for a “user preview”, a
“user display name”, etc.) and compose them.

> [!NOTE]
> Hydrators must have the same `keyBy`. If they don’t, `.extend()` throws.

```ts
type UserRow = { id: number; username: string; email: string };

const base = createHydrator<UserRow>().fields({ id: true, username: true });

const withDisplayName = createHydrator<UserRow>().extras({
	displayName: (u) => `${u.username} <${u.email}>`,
});

const combined = base.extend(withDisplayName);
// ⬇
type Result = Hydrator<UserRow, { id: number; username: string; displayName: string }>;
```

## FAQ

### What about JSON for relational queries?

Kysely [recommends](https://kysely.dev/docs/recipes/relations) using
database-level JSON-aggregation to nest related rows in your queries (e.g.,
`jsonArrayFrom()`). This works, but at a cost: all values are downcast to JSON
types.

Most noticeably, timestamp columns, which your driver might usually convert to
`Date` or `Temporal` instances, will be returned as strings when nested inside
JSON. More dangerously, Postgres serializes bigints to JSON numbers with more
digits than can fit in JavaScript's native `number`, causing data loss.

To address this problem, your query builder or orm must maintain a runtime
understanding of your database schema, so that it knows how to select and
hydrate JSON from the database into the correct types.

On the other hand, traditional joins do not have this problem, because all data
is returned in a fully normalized tuple, which your database driver understands.

### Which join strategy (traditional, application, or JSON) is best?

It depends, of course, on the specifics of your query and data.

| Join Strategy     | Pros                                                                      | Cons                                                                          |
| ----------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Traditional       | No extra round trips; efficient execution                                 | NxM data repetition (`user.name` repeated for each post)                      |
| JSON aggregation  | No extra round trips; reduced data transfer; works in `RETURNING` clauses | Harder for query planner to optimize; DB must aggregate nested rows in memory |
| Application-level | Simple, cacheable queries; reduced data transfer                          | Extra round trips                                                             |

Mix-and-match as you see fit.

### Should I just use [Drizzle](https://orm.drizzle.team)?

Maybe! This library offers a different set of compromises with its commitment
to a query-builder API even for nested relational queries. Drizzle, on the
other hand, has a dedicated relational query API for this purpose. But Drizzle
is a great project—and it's backed by a whole team. If you find yourself
needing more than Kysely for a production project, you should probably consider Drizzle
over Kysely Hydrate.

### I notice you have a `CLAUDE.md`. Is this whole thing AI slop?

No, it's not slop, but I have used LLMs pretty heavily in this codebase. I'm
not sure how I feel about it either! I suppose you should just treat this
library with the same level of (dis)trust you'd apply to any random npm
dependency.

### Does it work with Bun or Deno?

It should run anywhere Kysely runs, but I haven't tested it on anything but Node.js.

## Acknowledgements

Thank you to:

- The [Kysely team](https://github.com/kysely-org/kysely?tab=readme-ov-file#core-team)
- The [Drizzle project](https://orm.drizzle.team), for their column type definitions
- [My boss](https://github.com/jamesvillarrubia) for pushing us to prefer a
  query builder over an ORM (so I basically built my own, lol)
