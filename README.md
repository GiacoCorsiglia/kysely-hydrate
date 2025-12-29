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
          "authors",
          // This async function is called exactly once, with all posts:
          async (posts) =>
            db.selectFrom("author")
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
        // Nested object resulting from `attachOne`.
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

## Installation

Kysely is a peer dependency:

```bash
npm install kysely kysely-hydrate
```

## Hydrated queries with `hydrate()`

The `hydrate` helper accepts an unexecuted Kysely select query

```ts
import { hydrate } from "kysely-hydrate";

const result = await hydrate(
  db.selectFrom("users").select(["id"])
).execute()
```

From there, you can add joins and more.  The return type is a `HydratedQueryBuilder`

### Modifying the underlying query with `.modify()`

Example of adding where clauses, etc.

### Collection modes

When defining nested collections, three modes are supported:

- `"many"`: Returns an array of nested objects
- `"one"`: Returns a single nested object or `null` if not found
- `"oneOrThrow"`: Returns a single nested object or throws an error if not found

These modes are used as suffixes for each of the relational query methods
(`has*()`, `attach*()`, and `select*()`).

### Traditional joins with `.has*()`

#### `.hasMany()`

- `innerJoin()`
- `leftJoin()`
- ...

You can use the destructured form (`({ leftJoin }) => leftJoin(...)`)

This is another `HydratedQueryBuilder` so you can do more levels of nesting, add
mappings, etc.

> [!NOTE]
> Don't do `.modify(qb => qb.select(...))`.  Always use
> `HydratedQueryBuilder.select()` so that your selections are automatically prefixed.


#### `.hasOne()`

Works like hasMany but takes the first | null

Make note of special handling for innerJoin (or crossJoin---these are non-nullable) versus leftJoin

##### `.hasOneOrThrow()`

in case you need to write a leftJoin but can guarantee non-nullability anyway

#### SQL output (how does it owrk)

Kysely Hydrate modifies your SQL in exactly one way: it aliases nested selects.
For example: ...

Some examples of SQL output for combinations of collections

#### Combining joins

You can chain `leftJoin(...).innerJoin()` etc within the same `hasMany` to
combine columns from multiple

You can also just do `hasOneOrThrow('foo', (jb) => jb.select([...]))` without a join
if you want to get a nested object

#### Advanced use cases

Show an example with a CTE and a group by clause that then gets some hydrated joins inside of it.

### Application-level joins with `.attach*()`

Explain that each attach fetch fn is called exactly once per execution with the *input*.

### `.attachMany()`

Explain the AttachedKeyArgs thing with several examples

### `.attachOne()`

#### `.attachOneOrThrow()`

### Mapped properties with `.mapFields()`

### Computed properties with `.extras()`

### Excluded properties with `.omit()`

### Composable mappings with `.with()`

See below for Hydrators.

### Execution

execute
executeTakeFirst
executeTakeFirstOrThrow

## Hydrators

Explain that this is a lower level API

### `.fields()`

### `.extras()`

### `.omit()`

### Attached collections with `.attach*()`

Works the same as with the `hydrate()` API.

### Prefixed collections with `.has*()`

In addition to describing the API, Explain why this is not good enough in
general...the reason is that when you do a left join, each column from the
joined table becomes individually nullable in the flat output type.  But that's
not what you want for nested collections

### Composing hydrators with `.extend()`

### Creating hydrators with `createHydrator()`

### Manual hydration with `hydrateData()`

## Kysely plugin

> [!WARNING]
> This feature is experimental and is subject to change.

### Runtime schema definition

See the schema directory

### Automatic per-column hydration

This is the `fromDriver` thing

### Ad-hoc mapping

This is mapped-expression

### Subquery joins via JSON aggregation

> [!CAUTION]
> This feature is not yet implemented.

## Code generation

> [!CAUTION]
> This feature is not yet implemented.

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


----------------

EVERYTHING BELOW HERE IS DEFUNCT.  IT CAN BE DRAWN UPON FOR INSPIRATION BUT
SHOULD BE LEFT ALONE.  DO NOT CONSIDER ANYTHING BELOW THIS LINE WHEN MATCHING
THE TONE OR STYLE OF THE AUTHOR.


## Overview


When you perform SQL JOINs, you get flat rows with prefixed columns. This
library helps you transform those flat results into nested, denormalized
objects—similar to what you'd get from an ORM.

## Query Builder API

Chainable query builder that wraps Kysely's `SelectQueryBuilder` with
`hasMany()` and `hasOne()` methods:

```typescript
import { hydrate } from "kysely-hydrate";

const result = await hydrate(db.selectFrom("users").select(["users.id", "users.email"]), "id")
  .hasMany(
    "posts",
    ({ leftJoin }) =>
      leftJoin("posts", "posts.user_id", "users.id")
        .select(["posts.id", "posts.title"]),
    "id"
  )
  .hasOne(
    "profile",
    ({ leftJoin }) =>
      leftJoin("profiles", "profiles.user_id", "users.id")
        .select(["profiles.bio"]),
    "id"
  )
  .execute();

// Result: [{ id: 1, email: "...", posts: [...], profile: {...} }]
```

## Hydratable API

Configuration-based approach for transforming already-fetched flat rows:

```typescript
import { createHydrator, hydrateData } from "kysely-hydrate";

interface FlatRow {
  id: number;
  name: string;
  posts__id: number;
  posts__title: string;
}

const hydrator = createHydrator<FlatRow>("id")
  .fields({ id: true, name: true })
  .hasMany("posts", "posts__", (keyBy) =>
    keyBy("id").fields({ id: true, title: true })
  );

const nested = await hydrateData(flatRows, hydrator);
```

## Application-Level Joins

Both APIs support `.attachMany()`, `.attachOne()`, and `.attachOneOrThrow()`
methods for performing application-level joins.

For example:
```ts
const posts = [
  { id: 1, title: "Post 1", userId: 1 },
  { id: 2, title: "Post 2", userId: 1 },
  { id: 3, title: "Post 3", userId: 2 },
  { id: 4, title: "Post 4", userId: 3 },
];

const postsWithUser = hydrateData(posts, (keyBy) =>
  keyBy("id")
    .fields({
      id: true,
      title: true,
    })
    .attachOne(
      "user",
      async (posts) => await getUsersById(posts.map((p) => p.userId)),
      { keyBy: "id", compareTo: "userId" }
    ),
);

// Result: [{ id: 1, title: "Post 1", user: { email: "..." }  }, ...]
```

In the above example, the fetch function (which calls `getUsersById`) is called
exactly once, with the entire set of posts.  This guarantee holds true even for
nested hydratables.
