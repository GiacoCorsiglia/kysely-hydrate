# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

kysely-hydrate is a TypeScript library that extends Kysely (a type-safe SQL query builder) with utilities for hydrating flat SQL JOIN results into nested JavaScript objects. The library provides two main approaches:

1. **Hydratable API** (`src/hydratable.ts`): A configuration-based approach where you define how to transform flat rows into nested objects using a declarative API
2. **Query Builder API** (`src/query-builder.ts`): A chainable query builder (`hydrated()`) that wraps Kysely's SelectQueryBuilder and adds `joinMany()` and `joinOne()` methods for nested joins with automatic hydration

## Development Commands

### Testing
```bash
# Run all tests
bun test

# Run a specific test file
bun test src/hydratable.test.ts
bun test src/query-builder.test.ts
```

### Linting & Formatting
```bash
# Check code style and run linter
bunx @biomejs/biome check .

# Auto-fix linting and formatting issues
bunx @biomejs/biome check --write .

# Format code only
bunx @biomejs/biome format --write .
```

**Important**: This project uses Biome (not Prettier/ESLint). Code style follows:
- Tab indentation (not spaces)
- Double quotes for strings
- Recommended Biome rules with `noExplicitAny` and `noBannedTypes` disabled

### Type Checking
```bash
# Type check the project
bunx tsc --noEmit
```

## Architecture

### Core Concepts

**Prefixing System** (`src/helpers/prefixes.ts`):
- The library uses a prefix-based aliasing system to avoid column name collisions when joining tables
- Prefixes use `$$` as a separator (e.g., `posts$$id`, `posts$$author$$name`)
- The `MakePrefix`, `ApplyPrefix`, and `SelectAndStripPrefix` types handle prefix transformations at the type level
- Runtime functions like `makePrefix()`, `applyPrefix()`, and `getPrefixedValue()` manipulate prefixed keys

**Hydration Process**:
- Flat rows from SQL queries are grouped by entity keys (using `keyBy` configuration)
- The `groupByKey()` function groups rows with the same primary key
- Nested collections are recursively hydrated from the grouped rows
- The `Hydratable` class orchestrates the transformation through `#hydrateOne()` and `#hydrateMany()` methods

### Key Files

- **`src/index.ts`**: Public exports (`createHydratable`, `hydrate`, `hydrated`)
- **`src/hydratable.ts`**: Core `Hydratable` class and configuration API
  - Methods: `fields()`, `extras()`, `has()`/`hasMany()`/`hasOne()`/`hasOneOrThrow()`
  - Handles the actual hydration logic via `#hydrateOne()` and `#hydrateMany()`
- **`src/query-builder.ts`**: Query builder wrapper with nested join support
  - `hydrated()` factory function creates a `NestableQueryBuilder`
  - `NestedJoinBuilder` adds prefixed `select()` and join methods
  - `NestedJoinBuilderImpl` implements both interfaces
- **`src/helpers/prefixes.ts`**: Prefix manipulation utilities
- **`src/helpers/select-renamer.ts`**: Transforms Kysely select expressions to add prefixes
- **`src/helpers/utils.ts`**: Type utilities (`Extend`, `KeyBy`, `isIterable`)
- **`src/seed.ts`**: Test database seeding utilities
- **`src/mappable-expression.ts`**: (Work in progress) Plugin for mapping query results

### Type System Architecture

The library makes extensive use of TypeScript's advanced type features:

- **Generic Chaining**: Both `Hydratable` and `NestableQueryBuilder` use complex generic signatures to track type transformations through method chains
- **Type Inference**: The `Extend<>` utility merges types while preserving field information
- **Prefix Type Manipulation**: Types like `ApplyPrefixes<Prefix, T>` and `SelectAndStripPrefix<P, T>` transform object shapes based on prefixes
- **Kysely Type Integration**: The query builder wraps Kysely's `SelectQueryBuilder<DB, TB, O>` generics and transforms them through joins

### Collection Modes

When defining nested collections, three modes are supported:

- `"many"`: Returns an array of nested objects
- `"one"`: Returns a single nested object or `null` if not found
- `"oneOrThrow"`: Returns a single nested object or throws an error if not found

### Test Infrastructure

Tests use:
- Node.js built-in test runner (`node:test`)
- better-sqlite3 for in-memory SQLite databases
- `src/seed.ts` to create test data (users, posts tables)

## Important Patterns

### Adding Fields to Hydratables

When adding fields to a `Hydratable`, the type system tracks both the input shape and output shape separately:
- Use `fields()` to select and optionally transform fields from the input
- Use `extras()` to compute derived fields from the entire input
- The `Fields<Input>` type allows `true` (pass-through) or transformation functions

### Working with Nested Joins

The query builder approach requires careful type management:
- The `Prefix` generic tracks the current nesting level
- `LocalRow` represents the unprefixed shape at the current level
- `HydratedRow` represents the final nested object structure
- `QueryRow` represents the entire flattened row from the SQL query

### Handling Nullability

- Left joins make nested objects nullable
- The `IsNullable` generic tracks whether a join produces nullable results
- `NestedDB` suppresses nullability within nested join builders to provide correct typing

## Notes

- The project is a Bun module (`"type": "module"` in package.json)
- Peer dependency on Kysely ^0.28
- No build step required - TypeScript sources are exported directly via `"module": "src/index.ts"`
- The `nest.ts` file contains an older/alternative approach using `jsonArrayFrom` - not currently exported in the main API
