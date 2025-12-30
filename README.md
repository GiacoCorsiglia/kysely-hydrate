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

Kysely is a beautiful library.  It marries the power of SQL with the type safety
and expressiveness of TypeScript for building queries.  You tell Kysely what SQL
to produce—any SQL you want—and it gives you a well-typed result.

However, the result matches whatever your database driver returns.  Typically,
this means flat objects even if your query includes joins (`post.authorName`
instead of `post.author.name`) and primitive types (`{ id: 1 }`) that you must
manually hydrate into the rich types your application expects (`{ id: new UserId(1) }`).

Most ORMs, on the other hand, constrain you to a subset of SQL, written with an
idiosyncratic syntax that obscures the underlying SQL.  In return, your query
results are hydrated into rich objects, including converting joins into nested
properties (`user.posts`).  However, when you need something custom—even a
simple `COALESCE()` statement—you typically must drop into writing raw SQL, and
sacrifice all of the benefits of your ORM.

Kysely Hydrate grants Kysely the ability to produce rich, nested objects without
compromising the power or control of SQL.  It offers these features:

- Nested objects from traditional joins
- Application-level joins
- Mapped fields in hydrated queries
- Computed properties in hydrated queries

It also supports these experimental features:
- Ad-hoc mapping of arbitrary Kysely expressions in any query
- Automatic mapping of individual columns anywhere they are selected

Finally, these features are on the roadmap:
- Well-typed subquery joins with JSON aggregation
- Code generation for runtime schema objects

For example:

```ts
import { hydrate } from "kysely-hydrate";

const users = await hydrate(
  db.selectFrom("users").select(["users.id", "users.email"]),
)
  // Database-level join:
  .hasMany(
    "posts",
    ({ leftJoin }) =>
      leftJoin("posts", "posts.userId", "users.id")
        .select((eb) => [
          "posts.id",
          "posts.title",
          "posts.authorId",
          // Embed whatever SQL you want:
          eb.selectFrom("likes")
            .select(eb.fn.countAll().as("count"))
            .whereRef("likes.postId", "=", "posts.id")
            .as("likesCount"),
        ])
        // Application-level join:
        .attachOneOrThrow(
          "author",
          // Fetch authors (one query, batched over all posts):
          async (posts) =>
            db.selectFrom("authors")
              .select(["authors.id", "authors.name"])
              .where("authors.id", "in", posts.map((post) => post.authorId))
              .execute(),
          // How to match authors (child) to posts (parent):
          { matchChild: "id", toParent: "authorId" }
        ),
  )
  .execute();

// Result:
[
  {
    // ID columns automatically mapped to an instance of a nominal ID class:
    id: UserId(1),
    email: "...",
    // Nested array resulting from `hasMany`.
    posts: [
      {
        id: PostId(2),
        title: "..."
        authorId: AuthorId(3)
        likesCount: 42,
        // Nested object resulting from `attachOneOrThrow`.
        author: {
          id: AuthorId(3),
          name: "..."
        }
      }
    ]
  }
]
```

> [!NOTE]
> Some ORMs produce class instances with methods for interacting with the
> database (e.g., `user.save()`) or lazy relation loading (e.g., accessing
> `user.posts` triggers a query).  This is **not** a goal for Kysely Hydrate.

<!--
By design, Kysely has the following constraints:

1. It produces exactly the the SQL you tell it to.
2. It has no runtime understanding of your database schema; only a type-level understanding.
3. It returns query results from the underlying database driver verbatim, even
   if they don't match the expected type. -->

## Table of Contents

- [Installation](#installation)
- [Hydrated queries with `hydrate()`](#hydrated-queries-with-hydrate)
  - [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)
  - [Modifying the underlying query with `.modify()`](#modifying-the-underlying-query-with-modify)
  - [Inspecting the underlying query with `.toQuery()`](#inspecting-the-underlying-query-with-toquery)
  - [Traditional joins with `.has*()`](#traditional-joins-with-has)
    - [`.hasMany()`](#hasmany)
    - [`.hasOne()`](#hasone)
    - [`.hasOneOrThrow()`](#hasoneorthrow)
  - [Application-level joins with `.attach*()`](#application-level-joins-with-attach)
    - [`.attachMany()`](#attachmany)
    - [`.attachOne()`](#attachone)
    - [`.attachOneOrThrow()`](#attachoneorthrow-1)
  - [Mapped properties with `.mapFields()`](#mapped-properties-with-mapfields)
  - [Computed properties with `.extras()`](#computed-properties-with-extras)
  - [Excluded properties with `.omit()`](#excluded-properties-with-omit)
  - [Composable mappings with `.with()`](#composable-mappings-with-with)
  - [Execution](#execution)
- [Hydrators](#hydrators)
  - [Creating hydrators with `createHydrator()`](#creating-hydrators-with-createhydrator)
  - [Manual hydration with `hydrateData()`](#manual-hydration-with-hydratedata)
  - [Selecting and mapping fields with `.fields()`](#selecting-and-mapping-fields-with-fields)
  - [Computed properties with `.extras()`](#computed-properties-with-extras-1)
  - [Excluding fields with `.omit()`](#excluding-fields-with-omit)
  - [Attached collections with `.attach*()`](#attached-collections-with-attach)
  - [Prefixed collections with `.has*()`](#prefixed-collections-with-has)
  - [Composing hydrators with `.extend()`](#composing-hydrators-with-extend)
- [Kysely plugin](#kysely-plugin)
  - [Runtime schema definition](#runtime-schema-definition)
  - [Automatic per-column hydration](#automatic-per-column-hydration)
  - [Ad-hoc mapping](#ad-hoc-mapping)
  - [Subquery joins via JSON aggregation](#subquery-joins-via-json-aggregation)
- [Code generation](#code-generation)
- [FAQ](#faq)
- [Acknowledgements](#acknowledgements)

## Installation

Kysely is a peer dependency:

```bash
npm install kysely kysely-hydrate
```

## Hydrated queries with `hydrate()`

The `hydrate` helper accepts an unexecuted Kysely select query and returns a
`HydratedQueryBuilder`, which lets you

- Add **traditional joins** that hydrate into nested properties (`hasMany`, `hasOne`, `hasOneOrThrow`)
- Add **application-level joins** that batch-fetch related rows (`attachMany`, `attachOne`, `attachOneOrThrow`)
- Add **mappings and computed fields** (`mapFields`, `extras`) and then clean up (`omit`)
- Still use Kysely query builder methods via `.modify(qb => ...)`, and then execute with `execute*()`

It’s still “just Kysely”—you can write whatever SQL you want—but you get back a
result that looks like what your application wants to work with: nested objects,
arrays for relations, and rich types you can map in one place.


```ts
import { hydrate } from "kysely-hydrate";

// Result type: Array<{ id: number }>
const users = await hydrate(db.selectFrom("users").select(["users.id"])).execute();
```

### Keying and deduplication with `keyBy`

Hydration works by grouping the flat rows returned by your query into entities.
The `keyBy` argument tells Kysely Hydrate **how to uniquely identify each entity**
in the result set, so it can:

- Deduplicate parent rows when joins multiply them (NxM output).
- Group nested collections correctly.

`keyBy` can be either:

- A single key, like `"id"` or `"pk"`
- A composite key, like `["orderId", "productId"]`

Single key values will be compared via reference equality (`===`), and composite
key values will be stringified for comparison.

#### Special `"id"` behavior:

- If the row type has an `"id"` property, `keyBy` is **optional** and defaults to `"id"`.
- If the row type does **not** have an `"id"` property, you must provide `keyBy`.

Examples:

```ts
// Most examples in this README omit the `keyBy` argument when it defaults to `"id"`.
// If you don’t have an `"id"` column (or you need a composite key), pass `keyBy` explicitly.

// Default keyBy (only allowed when the row type has an "id" property)
await hydrate(db.selectFrom("users").select(["users.id"])).execute();
// ⬇
type Result = Array<{ id: number }>;

// Explicit keyBy (always allowed)
await hydrate(db.selectFrom("users").select(["users.id"]), "id").execute();
// ⬇
type Result = Array<{ id: number }>;

// keyBy is REQUIRED when your primary key is not named "id"
await hydrate(db.selectFrom("widgets").select(["widgets.pk"]), "pk").execute();
// ⬇
type Result = Array<{ pk: number }>;

// Composite keyBy
await hydrate(
  db.selectFrom("order_items").select(["order_items.orderId", "order_items.productId"]),
  ["orderId", "productId"],
).execute();
// ⬇
type Result = Array<{ orderId: string; productId: string }>;
```

If `keyBy` (or any part of a composite key) is `null` or `undefined` for a row,
that row is treated as “no entity” and is skipped for that entity level. This is
how left-joined nested entities resolve to `Entity[] | null` rather than objects
full of `null` fields.

### Modifying the underlying query with `.modify()`

The `modify` method allows you to apply changes to the underlying Kysely query, such as adding `where` clauses, `orderBy` clauses, or pagination.

```ts
// Get the first 10 active users
const users = await hydrate(
  db.selectFrom("users").select(["id", "name"])
)
  .modify((qb) => qb.where("isActive", "=", true).limit(10))
  .execute();
```

This is useful once you have added some joins, and wish to write a `WHERE`
clause using a joined column.

### Inspecting the underlying query with `.toQuery()`

Sometimes you just want to see what SQL you’ve built.  `HydratedQueryBuilder.toQuery()`
returns the underlying Kysely `SelectQueryBuilder`, so you can call `.compile()`
or use any other Kysely tooling you’re used to.

```ts
const builder = hydrate(db.selectFrom("users").select(["users.id"]))
  .hasMany("posts", ({ leftJoin }) =>
    leftJoin("posts", "posts.userId", "users.id").select(["posts.id"]),
  );

const compiled = builder.toQuery().compile();
console.log(compiled.sql);
console.log(compiled.parameters);
```

### Traditional joins with `.has*()`

These methods add SQL joins to your query and hydrate the results into nested objects.

> [!TIP]
> The `.has*()` method suffixes are reused throughout the library:
> - `*Many(...)` methods produce an array (`T[]`)
> - `*One(...)` methods produce a nullable object (`T | null`)
> - `*OneOrThrow(...)` methods produce a required object (`T`) and throw if it’s missing

#### `.hasMany()`

Hydrates a nested array of objects.  It accepts a callback that receives a
nested `HydratedQueryBuilder` instance, which supports all the standard join methods
(`innerJoin`, `leftJoin`, etc.). The join methods behave **exactly** like their Kysely counterparts and have the
exact same effect on the underlying SQL—they just return a `HydratedQueryBuilder`
so you can keep nesting and so your nested selects are automatically prefixed.

```ts
const users = await hydrate(
  db.selectFrom("users").select("users.id"),
).hasMany(
  "posts",
  // Destructure the join method you want (e.g., leftJoin, innerJoin):
  ({ leftJoin }) =>
    leftJoin("posts", "posts.userId", "users.id")
      // Select columns for the nested object:
      .select(["posts.id", "posts.title"]),
).execute();
// ⬇
type Result = Array<{
  id: number;
  posts: Array<{ id: number; title: string }>;
}>;
```

> [!NOTE]
> Like `hydrate()`, the last argument to `hasMany()` is `keyBy` (see
> [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)).
> It defaults to `"id"` if your nested row type has an `"id"` property; otherwise, it is required.
>
> For example:
>
> - If the nested row has an `id` column, you can omit it:
>   - `.hasMany("posts", (jb) => jb.leftJoin(...).select([...]))`
> - If the nested primary key is named something else, pass it:
>   - `.hasMany("posts", (jb) => jb.leftJoin(...).select([...]), "pk")`
> - If the nested entity has a composite key, pass both keys:
>   - `.hasMany("items", (jb) => jb.leftJoin(...).select([...]), ["orderId", "productId"])`

##### Supported join methods

All of these exist on the nested builder:

- `innerJoin`
- `leftJoin`
- `crossJoin`
- `innerJoinLateral`
- `leftJoinLateral`
- `crossJoinLateral`

`RIGHT JOIN` and `FULL JOIN` are intentionally omitted because they don’t map
cleanly to ORM-style nested results (and are usually a sign you want a different
shape for your root query).

##### How nested join building works

Inside the `.hasMany("posts", (jb) => ...)` callback, you are still operating on
the same underlying query:

- Calling `jb.leftJoin(...)` adds a real `LEFT JOIN` to your SQL.
- Calling `jb.select(...)` adds real selections to your SQL, but aliases them
  under the collection key (e.g., `posts$$title`) so hydration can reconstruct
  nested objects.

Because the nested builder is itself a `HydratedQueryBuilder`, you can nest further
collections, add mappings, or use any other feature.

```ts
const users = await hydrate(db.selectFrom("users").select("users.id"))
  .hasMany(
    "posts",
    ({ leftJoin }) =>
      leftJoin("posts", "posts.userId", "users.id")
        .select(["posts.id", "posts.title"])
        // Nested hasMany:
        .hasMany(
          "comments",
          ({ leftJoin }) =>
            leftJoin("comments", "comments.postId", "posts.id")
              .select(["comments.id", "comments.content"]),
        ),
  )
  .execute();
// ⬇
type Result = Array<{
  id: number;
  posts: Array<{
    id: number;
    title: string;
    comments: Array<{ id: number; content: string }>;
  }>;
}>;
```

> [!WARNING]
> Don't use `.modify(qb => qb.select(...))` inside the callback.  Always use
> the builder's `.select()` method so that your selections are automatically prefixed.

##### Chaining multiple `.hasMany()` calls

You can define multiple sibling collections at the same level by chaining:

```ts
const users = await hydrate(db.selectFrom("users").select(["users.id"]))
  .hasMany("posts", ({ leftJoin }) =>
    leftJoin("posts", "posts.userId", "users.id").select(["posts.id", "posts.title"]),
  )
  .hasMany("comments", ({ leftJoin }) =>
    leftJoin("comments", "comments.userId", "users.id").select(["comments.id", "comments.content"]),
  )
  .execute();
// ⬇
type Result = Array<{
  id: number;
  posts: Array<{ id: number; title: string }>;
  comments: Array<{ id: number; content: string }>;
}>;
```

> [!WARNING]
> Kysely Hydrate doesn’t “hide” SQL complexity—you’re still writing SQL. When you
> chain many `.has*()` calls, you are adding more `JOIN`s to a single query. Be
> mindful of join cardinality and row explosion. When in doubt, inspect what
> you’ve built (see [Inspecting the underlying query with `.toQuery()`](#inspecting-the-underlying-query-with-toquery)).
>
> Also note that a nested `.modify(qb => ...)` is still modifying the *same* underlying
> SQL query. That means adding a `WHERE` clause inside a `.hasMany(...)` callback will
> filter the entire result set, not “just that collection”. If you need to filter a
> collection per-parent (e.g., “latest 3 posts per user”), you may want a lateral join
> (or any other SQL pattern that scopes the filtering to the joined relation).

#### `.hasOne()`

Hydrates a single nested object (or `null`).

- If you use `leftJoin`, the result is nullable (`T | null`).
- If you use `innerJoin` (or `crossJoin`), the result is non-nullable (`T`).

```ts
const posts = await hydrate(db.selectFrom("posts").select("posts.id"))
  .hasOne("author", ({ innerJoin }) =>
    innerJoin("users", "users.id", "posts.authorId").select(["users.name"])
  )
  .execute();
// ⬇
type NonNullableAuthor = { author: { name: string } };

const posts2 = await hydrate(db.selectFrom("posts").select("posts.id"))
  .hasOne("author", ({ leftJoin }) =>
    leftJoin("users", "users.id", "posts.authorId").select(["users.name"])
  )
  .execute();
// ⬇
type NullableAuthor = { author: { name: string } | null };
```

> [!NOTE]
> Like `hydrate()`, `hasOne()` also accepts an optional final `keyBy` argument with the
> same semantics (see [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)).

#### `.hasOneOrThrow()`

Similar to `.hasOne()`, but guarantees a non-nullable result.  If the nested object
is missing (i.e., all selected columns are null), it throws an error.

Use this when you need to perform a `leftJoin` (e.g., for performance or complex logic)
but you know the record must exist.

> [!NOTE]
> Like `hydrate()`, `hasOneOrThrow()` also accepts an optional final `keyBy` argument with the
> same semantics (see [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)).

#### SQL output

Kysely Hydrate produces the SQL you tell it to with exactly one exception: it
aliases nested selections to avoid naming collisions.

For example, `users.id` remains `users.id`, but `posts.id` nested under "posts"
becomes `posts$$id`.  The hydration layer then un-flattens these aliases back
into nested objects.

```ts
const query = hydrate(
  db.selectFrom("users").select(["users.id", "users.name"])
).hasMany(
  "posts",
  ({ leftJoin }) =>
    leftJoin("posts", "posts.userId", "users.id")
      .select(["posts.id", "posts.title"]),
);
```

The above produces the following SQL:

```sql
SELECT
  users.id,
  users.name,
  posts.id as posts$$id,
  posts.title as posts$$title
FROM users
LEFT JOIN posts ON posts.userId = users.id
```

#### Combining joins within one entity

You can chain multiple joins within a single `.has*()` block to combine columns
from multiple tables into one nested object.

```ts
.hasOne(
  "details",
  ({ leftJoin }) =>
    leftJoin("profiles", "profiles.userId", "users.id")
      .leftJoin("settings", "settings.userId", "users.id")
      .select(["profiles.bio", "settings.theme"])
)
```

You can also use `.hasOneOrThrow()` without any join at all, simply to group
top-level columns into a nested object:

```ts
.hasOneOrThrow("meta", (jb) => jb.select(["users.createdAt", "users.updatedAt"]))
```

#### Advanced use cases

Kysely Hydrate works on *any* select query Kysely can build—CTEs, grouping, having
clauses, custom expressions, whatever. The only thing it does differently is alias
nested selections so it can reassemble them into nested objects.

Here’s an example mixes a CTE, `GROUP BY` + `HAVING`, and nested joins:

```ts
// Result type: Array<{
//   id: number,
//   username: string,
//   postCount: number,
//   latestPostAt: Date | null,
//   posts: Array<{
//     id: number,
//     title: string,
//     createdAt: Date,
//     comments: Array<{ id: number, content: string }>
//   }>
// }>
const activeAuthors = db
  .with("active_authors", (db) =>
    db
      .selectFrom("users")
      .leftJoin("posts", "posts.userId", "users.id")
      .select(["users.id", "users.username"])
      .select((eb) => [
        eb.fn.count("posts.id").as("postCount"),
        eb.fn.max("posts.createdAt").as("latestPostAt"),
      ])
      .groupBy(["users.id", "users.username"])
      .having((eb) => eb.fn.count("posts.id"), ">", 0),
  )
  .selectFrom("active_authors")
  .select(["active_authors.id", "active_authors.username", "active_authors.postCount", "active_authors.latestPostAt"]);

const result = await hydrate(activeAuthors)
  .hasMany("posts", ({ leftJoin }) =>
    leftJoin("posts as p", "p.userId", "active_authors.id")
      .select(["p.id", "p.title", "p.createdAt"])
      .hasMany("comments", ({ leftJoin }) =>
        leftJoin("comments", "comments.postId", "p.id").select(["comments.id", "comments.content"]),
      ),
  )
  .execute();
```

### Application-level joins with `.attach*()`

Application-level joins allow you to fetch related data in separate queries
(actually, with any async function) while still receiving a nested result.


Kysely Hydrate handles the "N+1" problem automatically: the `fetchFn` you
provide is called **exactly once** per query execution, receiving all parent
rows at once.  This allows you to batch load the related data efficiently.

### `.attachMany()`

Attaches a nested array of objects.

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

Here's an example where `toParent` is *not* `"id"`: attaching an author to posts
by matching `authors.id` to `posts.authorId`:

```ts
const posts = await hydrate(
  db.selectFrom("posts").select(["posts.id", "posts.title", "posts.authorId"]),
)
  .attachOne(
    "author",
    async (posts) =>
      db
        .selectFrom("authors")
        .select(["authors.id", "authors.name"])
        .where("authors.id", "in", posts.map((p) => p.authorId))
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
for things that *aren’t* database rows: HTTP calls, caches, etc.

```ts
// Example: Attach feature flags from a cached HTTP endpoint
const users = await hydrate(db.selectFrom("users").select(["users.id", "users.email"]))
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

The guarantee that your `fetchFn` runs exactly once holds even when the attach
is nested (for example: attaching tags to posts inside a `hasMany("posts", ...)`):
your function will still run once, with the full batch of parent inputs.

```ts
// Example: Attaching tags to posts, where posts are nested under users
hydrate(usersQuery)
  .hasMany("posts", ({ leftJoin }) =>
    leftJoin("posts", ...)
      .select(...)
      // Attach tags to posts:
      .attachMany(
        "tags",
        // Called once with ALL posts from ALL users
        async (posts) => {
           const postIds = posts.map(p => p.id);
           return db.selectFrom("tags")...execute();
        },
        { matchChild: "postId", toParent: "id" }
      ),
  )
```

### `.attachOne()`

Attaches a single nested object (nullable).

```ts
.attachOne(
  "latestPost",
  // Returns an array/iterable of posts (potentially multiple per user)
  async (users) => { /* ... fetch latest post for each user ... */ },
  { matchChild: "userId", toParent: "id" }
)
```

**Note:** The `fetchFn` for `attachOne` should still return an **array/iterable**
containing *all* matching records for the whole batch of parents. Kysely Hydrate
groups those child rows per parent and then takes the **first** match (or `null`
if there is none).

This means **ordering is your responsibility**. If you need “latest post”, make
sure your SQL orders the results appropriately (or use whatever SQL you want:
window functions, `DISTINCT ON`, lateral joins, etc.).

#### `.attachOneOrThrow()`

Attaches a single nested object and throws if it is missing.

### Mapped properties with `.mapFields()`

Transform individual fields in the result set.  This changes the output type for
those fields, but does **not** change the underlying SQL; the mapping runs in
JavaScript after the query.

```ts
const users = await hydrate(
  db.selectFrom("users").select(["users.id", "users.email", "users.metadata"]),
)
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

Add new properties derived from the entire row.  Extras do **not** change the
underlying SQL; they are computed in JavaScript after the query runs.

```ts
const users = await hydrate(
  db.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]),
)
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

Remove fields from the final output.  This is useful for cleaning up sensitive data
or removing intermediate fields used for computed properties.  `.omit()` does
**not** change the underlying SQL; it only removes properties from the hydrated
result.

```ts
const users = await hydrate(
  db
    .selectFrom("users")
    .select(["users.id", "users.firstName", "users.lastName"]),
)
  .extras({
    fullName: (row) => `${row.firstName} ${row.lastName}`,
  })
  // Hide intermediate fields
  .omit(["firstName", "lastName"])
  .execute();
// ⬇
type Result = Array<{ id: number; fullName: string }>;
```

### Composable mappings with `.with()`

Re-use hydration logic by importing it from another `Hydrator`.  This is great for
sharing consistent formatting logic across different queries.  `.with()` does
**not** change the underlying SQL; it only composes hydration configuration
(see [Hydrators](#hydrators) below).

```ts
import { createHydrator } from "kysely-hydrate";

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
const users = await hydrate(
  db.selectFrom("users").select(["users.id", "users.username", "users.email"]),
)
  .with(userHydrator)
  .execute();
// ⬇
type Result1 = Array<{ id: number; username: string; displayName: string }>;

// Reuse in query #2 (different root query, same hydration rules):
const author = await hydrate(
  db
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

### Execution

To run the query and get the hydrated results, use one of the standard execution methods.
These have the same semantics as Kysely's methods but return the hydrated types.

- `execute()`: Returns `Promise<Result[]>`
- `executeTakeFirst()`: Returns `Promise<Result | undefined>`
- `executeTakeFirstOrThrow()`: Returns `Promise<Result>` (throws if empty)

## Hydrators

The `hydrate()` API described above is the happy path when you’re building a
query in Kysely and want nested results.

Hydrators are the lower-level API: they let you take *already-fetched* rows
(from Kysely, raw SQL, a view, an API, anywhere) and hydrate them into nested
objects using the same core logic.  `hydrate` uses a `Hydrator` under the hood.

Use hydrators when:

- You already have flat rows and want to hydrate them; or,
- You want to define reusable hydration logic independent of any particular query.

> [!NOTE]
> Hydrators don’t “know” what you selected. Unlike `hydrate()`, you need to
> specify what you want in the output using `.fields()` (and/or `.extras()`).

### Creating hydrators with `createHydrator()`

Creates a new, empty hydrator configuration.

Like `hydrate()`, hydrators use `keyBy` to group and deduplicate entities. The
same rules apply (see [Keying and deduplication with `keyBy`](#keying-and-deduplication-with-keyby)).

```ts
// Group by "id"
const h = createHydrator<User>();

// Group by composite key
const h2 = createHydrator<OrderItem>(["orderId", "productId"]);
```

### Manual hydration with `hydrateData()`

Hydrates an array of flat objects using a configured hydrator.

```ts
const flatRows = await db
  .selectFrom("users")
  .leftJoin("posts", "posts.userId", "users.id")
  .select([
    "users.id",
    "users.username",
    // Manual prefixing to match the hydrator:
    "posts.id as posts$$id",
    "posts.title as posts$$title",
  ])
  .execute();

const nestedUsers = await hydrateData(flatRows, hydrator);
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

const nestedUsers = await hydrateData(flatRows, (h) =>
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
> - `h()` defaults to `"id"` only when your row type has an `id` field
> - `h("pk")` for a non-`id` primary key
> - `h(["orderId", "productId"])` for composite keys

`hydrateData()` also accepts a single object (not just arrays) and returns the
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

const hydrator = createHydrator<UserRow>()
  .fields({ id: true, username: true, email: true })
  .extras({
    displayName: (u) => `${u.username} <${u.email}>`,
  })
  .omit(["email"]);
// ⬇
type Result = Array<{ id: number; username: string; displayName: string }>;
```

### Excluding fields with `.omit()`

Excludes fields from the output that were already included.  This method
primarily exists for use by the `HydratedQueryBuilder`, which includes all
fields by default.

```ts
type UserRow = { id: number; passwordHash: string };

const hydrator = createHydrator<UserRow>()
  .fields({ id: true, passwordHash: true })
  .omit(["passwordHash"]);
// ⬇
type Result = Array<{ id: number }>;
```

### Attached collections with `.attach*()`

These work the same as in the `hydrate()` API (see the `.attach*()` section above).
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

Notice that every single field in the nested result types are nullable.  This
happens because we cannot know if `posts$$title` is nullable because (a) it is a
non-nullable column that was made nullable by a left join; or, (b) it's actually
nullable in the "posts" table.  The `HydratedQueryBuilder` API, on the other
hand, _does_ know the difference, and so does not suffer from this problem.

### Composing hydrators with `.extend()`

Merges two hydrators.  The second hydrator's configuration takes precedence.

This is a good way to build small, reusable hydrators (for a “user preview”, a
“user display name”, etc.) and compose them.

> [!NOTE]
> Hydrators must have the same `keyBy`. If they don’t, `.extend()` throws.

```ts
type UserRow = { id: number; username: string; email: string };

const base = createHydrator<UserRow>().fields({ id: true, username: true });

const withDisplayName = createHydrator<UserRow>()
  .fields({ email: true })
  .extras({ displayName: (u) => `${u.username} <${u.email}>` })
  .omit(["email"]);

const combined = base.extend(withDisplayName);
// ⬇
type Result = Hydrator<UserRow, { id: number; username: string; displayName: string }>;
```

## Kysely plugin

> [!CAUTION]
> This feature is experimental and is subject to change.

### Runtime schema definition

To enable automatic hydration, you must define your database schema using the
provided runtime schema helpers (inspired by Drizzle ORM).  These helpers define both
the TypeScript type and the runtime transformations (e.g., parsing dates).

```ts
// tables.ts

import * as p from "kysely-hydrate/schema/postgres";

export const users = p.createTable("public", "users", {
  id: p.serial(),
  name: p.text(),
  createdAt: p.timestamp(), // Automatically parses string -> Date
});
```

### Automatic per-column hydration

When you use the `HydratePlugin` with your runtime schema, the library automatically
detects which columns are being selected and applies their `fromDriver` transformation.

```ts
import { HydratePlugin } from "kysely-hydrate";
import { createDatabase } from "kysely-hydrate/schema/table";
import * as t from "./tables"; // Defined above

const database = createDatabase("public", t);

const db = new Kysely<DB>({
  dialect: new PostgresDialect({ ... }),
  plugins: [new HydratePlugin(database)],
});

// usage:
// createdAt will be a Date instance, not a string
const user = await db.selectFrom("users").select("createdAt").executeTakeFirst();
```

### Ad-hoc mapping

You can map arbitrary expressions using the `map` helper.  This is useful for
computed columns where you don't have a schema definition.

```ts
import { map } from "kysely-hydrate";

const result = await db
  .selectFrom("users")
  .select((eb) => [
    "username",
    // Map the result of this expression:
    map(
      eb.fn.countAll(),
      (count) => Number(count) // Convert string/bigint to number
    ).as("count"),
  ])
  .execute();
```

### Subquery joins via JSON aggregation

> [!CAUTION]
> This feature is not yet implemented.

The goal is to support `jsonArrayFrom` and `jsonObjectFrom` but with automatic hydration
of the nested JSON data back into rich types (Dates, etc.) using the runtime schema.

## Code generation

> [!CAUTION]
> This feature is not yet implemented.

Future versions will include functions to generate the runtime schema definitions from
your database, similar to `kysely-codegen` but with runtime metadata.

## FAQ

### What about JSON for relational queries?

Kysely [recommends](https://kysely.dev/docs/recipes/relations) using
database-level JSON-aggregation to nest related rows in your queries (e.g.,
`jsonArrayFrom()`).  This works, but at a cost: all values are downcast to JSON
types.  Most noticeably, timestamp columns, which your driver might usually convert to
`Date` instances, will be returned as strings when nested inside JSON.

To address this problem, your query builder must maintain a runtime
understanding of your database schema, so that it knows how to hydrate JSON from
the database into the correct types.  This is what Kysely Hydrate's plugin does.

On the other hand, traditional joins do not have this problem, because all data is returned in a
fully normalized tuple, which your database driver understands.

### Which join strategy (traditional, application, or JSON) is best?

It depends, of course, on the specifics of your query and data.

| Join Strategy | Pros | Cons |
|-|-|-|
| Traditional | No extra round trips; efficient execution | NxM data repetition (`user.name` repeated for each post) |
| JSON aggregation | No extra round trips; reduced data transfer; works in `RETURNING` clauses | Harder for query planner to optimize; DB must aggregate nested rows in memory |
| Application-level | Simple, cacheable queries; reduced data transfer | Extra round trips |

This is why Kysely Hydrate supports all three strategies.  Mix-and-match as you see fit.

### Should I just use [Drizzle](https://orm.drizzle.team)?

Maybe!  This library offers a different set of compromises with its commitment
to a query-builder API even for nested relational queries.  Drizzle, on the
other hand, has a dedicated relational query API for this purpose.  But Drizzle
is a great project—and it's backed by a whole team.  If you find yourself
needing more than Kysely for a production project, you should probably consider Drizzle
over Kysely Hydrate.

### I notice you have a `CLAUDE.md`.  Is this whole thing AI slop?

No, it's not slop, but I have used LLMs pretty heavily in this codebase.  I'm
not sure how I feel about it either!  I suppose you should just treat this
library with the same level of (dis)trust you'd apply to any random npm
dependency.

### Does it work with Bun or Deno?

It should run anywhere Kysely runs, but I haven't tested it on anything but Node.js.

### Can you publish this to JSR?

I already had to figure out how to publish things to npm.  But, who am I
kidding—if this project gets a real user, I'd be happy to look into JSR!

## Acknowledgements

Thank you to:

- The [Kysely team](https://github.com/kysely-org/kysely?tab=readme-ov-file#core-team)
- The [Drizzle project](https://orm.drizzle.team), for their column type definitions
- [My boss](https://github.com/jamesvillarrubia) for pushing us to prefer a
  query builder over an ORM (so I basically built my own, lol)
