# CLAUDE.md

## Tooling

Written in TypeScript and uses Node.js's native support for running TypeScript
directly.  OXC is used for formatting (oxfmt) and linting (oxlint).  Tests are
written using Node.js's builtin testing library.  See `package.json` scripts.

Tests use better-sqlite3 for in-memory SQLite databases.

## Project Overview

kysely-hydrate is a TypeScript library that extends Kysely (a type-safe SQL
query builder) with utilities for hydrating flat SQL JOIN results into nested
JavaScript objects. The library provides two main approaches:

1. **Hydratable API** (`src/hydratable.ts`): A configuration-based approach where you define how to transform flat rows into nested objects using a declarative API
2. **Query Builder API** (`src/query-builder.ts`): A chainable query builder (`hydrated()`) that wraps Kysely's SelectQueryBuilder and adds `joinMany()` and `joinOne()` methods for nested joins with automatic hydration provided by `Hydratable`.

### Collection Modes

When defining nested collections, three modes are supported:

- `"many"`: Returns an array of nested objects
- `"one"`: Returns a single nested object or `null` if not found
- `"oneOrThrow"`: Returns a single nested object or throws an error if not found

## Notes

- Existing tests are NOT good examples to follow.
- The `nest.ts` file should be ignored as it is an older approach.
- The `mappable-expression.ts` should be ignored as it is still experimental.
