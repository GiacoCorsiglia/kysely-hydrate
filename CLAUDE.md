# CLAUDE.md

## Tooling

Written in TypeScript and uses Node.js's native support for running TypeScript
directly.  OXC is used for formatting (oxfmt) and linting (oxlint).  Tests are
written using Node.js's builtin testing library.  See `package.json` scripts.

Tests use better-sqlite3 for in-memory SQLite databases.

## Project Overview

kysely-hydrate is a TypeScript library that extends Kysely (a type-safe SQL
query builder) with utilities for hydrating flat SQL JOIN results into nested
JavaScript objects with richer types.

## Experimental

- The `nest.ts` file should be ignored as it is an older approach.
