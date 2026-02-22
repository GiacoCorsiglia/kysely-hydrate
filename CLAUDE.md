kysely-hydrate is a TypeScript library that extends Kysely (a type-safe SQL
query builder) with utilities for hydrating flat SQL JOIN results into nested
JavaScript objects with richer types.

It has a goal of zero-compromise type safety, as well as 100% correctness.

Written in TypeScript and uses Node.js's native support for running TypeScript
directly. OXC is used for formatting (oxfmt) and linting (oxlint). Tests are
written using Node.js's builtin testing library. See `package.json` scripts.

Tests use better-sqlite3 for in-memory SQLite databases and Postgres for pg-specific cases. Use `npm test:all` to run them all.

After making changes, always run

- `npm run test:all`
- `npm run typecheck`
- `npm run lint`
- `npm run format`

Ignore the `src/experimental` directory.
