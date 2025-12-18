# Kysely Hydrate

> [!WARNING]
> This is an early release. **Expect breaking changes.**

A TypeScript library that extends [Kysely](https://kysely.dev) with utilities
for hydrating query results into rich JavaScript objects.

## Installation

Kysely is a peer dependency:

```bash
npm install kysely kysely-hydrate
```

## Overview

When you perform SQL JOINs, you get flat rows with prefixed columns. This
library helps you transform those flat results into nested, denormalized
objects—similar to what you'd get from an ORM.

## Query Builder API

Chainable query builder that wraps Kysely's `SelectQueryBuilder` with
`hasMany()` and `hasOne()` methods:

```typescript
import { hydrated } from "kysely-hydrate";

const result = await hydrated(db.selectFrom("users").select(["users.id", "users.email"]), "id")
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
import { createHydratable, hydrate } from "kysely-hydrate";

interface FlatRow {
  id: number;
  name: string;
  posts__id: number;
  posts__title: string;
}

const hydratable = createHydratable<FlatRow>("id")
  .fields({ id: true, name: true })
  .hasMany("posts", "posts__", (keyBy) =>
    keyBy("id").fields({ id: true, title: true })
  );

const nested = await hydrate(flatRows, hydratable);
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

const postsWithUser = hydrate(posts, (keyBy) =>
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

## Collection Modes

When defining nested collections, three modes are supported:

- `"many"`: Returns an array of nested objects
- `"one"`: Returns a single nested object or `null` if not found
- `"oneOrThrow"`: Returns a single nested object or throws an error if not found
