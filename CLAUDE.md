kysely-hydrate is a TypeScript library that extends Kysely (a type-safe SQL
query builder) with utilities for hydrating flat SQL JOIN results into nested
JavaScript objects with richer types.

It has a goal of zero-compromise type safety, as well as 100% correctness.

Written in TypeScript and uses Node.js's native support for running TypeScript
directly. OXC is used for formatting (oxfmt) and linting (oxlint). Tests are
written using Node.js's builtin testing library. See `package.json` scripts.

Tests use better-sqlite3 for in-memory SQLite databases and Postgres for
pg-specific cases. Use `npm run test:all` to run them all.

Postgres must be running first. `npm run test:db` provisions it (idempotently,
via Docker or a local cluster, whichever is available) and writes `POSTGRES_URL`
to `.env`, which the test scripts read automatically. In Claude Code on the web
the `.claude/hooks/session-start.sh` hook already did this, so `npm run test:all`
just works. See the Development section of the README.

After making changes, always run

- `npm run test:all`
- `npm run typecheck`
- `npm run lint`
- `npm run format`

At the end of each task, launch a subagent to review touched files holistically, refactor, and reduce complexity and duplication. Do so even if it exceeds the mandate of the user's original request.

Do not commit code in a local session. Ask the user to review and let them
commit manually.

In Claude Code on the web (`CLAUDE_CODE_REMOTE=true`) this is reversed: commit
and push to the session's own branch.

Ignore the `src/experimental` directory.
