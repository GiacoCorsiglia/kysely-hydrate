# Release review: changes since `3fabc72` (keyBy fix) through HEAD

Reviewed 2026-07-11, covering 34 commits (`3fabc72^..HEAD`): every functional
fix and the full test restructuring, ahead of the next release. Baseline at
HEAD: SQLite suite 532 pass / 0 fail, Postgres suite 639 pass / 0 fail,
typecheck clean, lint clean.

Method: the full range diff was cataloged, then each functional commit was
adversarially reviewed for (a) whether it actually fixes what it claims,
(b) regressions/new edge cases, and (c) test coverage; the test restructuring
was audited test-by-test against the pre-consolidation files for coverage
gaps or weakened assertions.

---

## 1. Catalog of changes

### 1.1 Functional changes (hydrator: `src/hydrator.ts`)

| Commit | Claim |
|---|---|
| `8aeaa0d` | Prefer `other.orderByKeys` in `hydrator.with()` (orderByKeys becomes tri-state `boolean \| undefined` so composition can tell "explicitly set" from "default") |
| `90d201c` | Composite keys JSON-encode parts instead of joining with `"::"`, avoiding cross-boundary collisions |
| `00d7a0b` | Always dedupe rows with identical key; removes the no-collections fast path and the `hasMultipleManyCollections` / `hasSiblingManyCollections` propagation machinery |
| `621423f` | Grouping stores single rows directly and only allocates a `RowGroup` array on the second row with the same key |
| `5061543` | `hydrate()` / `hydrator.hydrate()` never throw synchronously; sync errors become rejections |
| `87c8b3e` | Input iterables are materialized once so attach-fetching + hydration don't exhaust one-shot iterables |
| `3f2e8f3` | Overload reorder so `hydrate(one)` types as `Promise<Output>` rather than `Promise<Output \| Output[]>` |
| `9f6dad0` | Attach fetch functions receive inputs deduped by parent `keyBy`, with nil-key (phantom left-join) rows excluded |
| `297e363` | `many`-mode attach/group lookups return referentially distinct arrays per parent (`RowGroup.rows.slice()`) |
| `f9db900` | `__proto__` output keys are assigned via `Object.defineProperty` (own data property) instead of being dropped / polluting the prototype |

### 1.2 Functional changes (query set: `src/query-set.ts`, `src/helpers/`)

| Commit | Claim |
|---|---|
| `3fabc72` | `keyBy` now respected in query sets (hydrator created with the query set's `keyBy`) |
| `31c9498` | `limit(0)` / `offset(0)` respected (strict `!== null` checks instead of falsy checks) |
| `6618f09` | `oneOrThrow` joins now count as cardinality-one (`mode !== "many"`), fixing pagination/nesting decisions |
| `ce044c4` | Write CTEs preserved when a write query set has no joins and no pagination (the "less nesting" fast path is skipped for write query sets) |
| `eec255d` | ORDER BY emitted on subqueries with LIMIT in the cardinality-one path (lateral "top-N per group" correctness) |
| `6902197` | Documented + tested: `.modify()`/`.where()` on the base query are discarded by `.insert()`/`.write()` base replacement |
| `fc8b9b5` | `.attach()` goes through `#addCollection` so an attach collection can override a same-key join collection |
| `025e650` | `sqlCompare` fallback returns 0 for equal string forms — comparator is now a valid total order for mixed-type inputs |
| `38fb3ca` | Dead code removed: `src/helpers/query-wrapper.ts`, parts of `prefixes.ts`, `select-renamer.ts`, `utils.ts` |

Docs-only commits in the range: `42ef7bc`, `b5ce0c7`, `0b766fa`, `f2ec40f`,
`6902197` (+ test), `833b51a`, `c9bde4c`, `75a7ede`.

### 1.3 Test restructuring

- **Join-test consolidation** (`35120f2`, `14ba5fd`, `b71b8e3`, `3c5c17b`):
  deleted `inner-join-many`, `inner-join-one`, `left-join-many`,
  `left-join-one`, `left-join-one-or-throw`, `cross-join-many`, `mixed-joins`,
  and `postgres-lateral` test files → consolidated into
  `query-set.joins.test.ts` (2,216 lines).
- **Core/execution split** (`b71b8e3`, `3c5c17b`): deleted
  `query-set.basic.test.ts`, `query-set.edge-cases.test.ts`,
  `query-set.collection-modify.test.ts`, `query-set.modify.test.ts`,
  `query-set.postgres.test.ts` → content distributed into new
  `query-set.core.test.ts`, `query-set.execution.test.ts`, and expanded
  `hydration`/`attach`/`order-by`/`pagination`/`column-aliases` files.
- **Renames**: `query-set.complex.test.ts` → `query-set.nesting.test.ts`;
  `query-set.sql-generation.test.ts` → `query-set.sql.test.ts`.
- **Setup refactor** (`aa84af4`): `src/__tests__/fixture.ts` deleted,
  `helpers.ts` added, `fixture.sql` / `postgres.ts` / `sqlite.ts` reworked,
  `experimental-seed-db.ts` added.
- **New unit tests**: `src/helpers/prefixes.test.ts`, expanded
  `utils.test.ts`, `select-renamer.test.ts`, `hydrator.test.ts` (+741 lines),
  `hydrator.test-d.ts`.

---

## 2. Correctness findings

_(populated below)_

---

## 3. Test coverage findings

### 3.1 Join-test consolidation (`35120f2`, `14ba5fd` + later refactors)

**Verdict: no major coverage regressions.** All 90 tests across the eight
deleted join test files were traced into the new structure; 89 are fully
covered (most with equal or stronger assertions), 1 is weakened (below).

- The consolidation is table-driven: a `defineContractTests` contract
  (execute / executeTakeFirst / executeCount / toJoinedQuery / executeExists /
  toBaseQuery) runs over all 6 plain join types and all 6 lateral join types,
  with hand-written tests for type-distinguishing semantics. Each table case
  pins exact hydrated literals and exact flat-row literals via
  `deepStrictEqual` — no assertion was downgraded to a length check.
- **All 27 Postgres lateral tests survived**, and the consolidated lateral
  block still runs Postgres-only via `describePg` (skips unless
  `HYDRATE_TEST_DB=postgres`), matching the old file's gating. Lateral-specific
  scenarios (ON TRUE, correlated `whereRef`, limit/orderBy inside the lateral,
  nested laterals, top-N-per-group SQL regexes, pagination, `modify`) are all
  present.
- Regression tests tied to fix commits survived verbatim, including the
  `297e363` referential-distinctness test (deepStrictEqual + notStrictEqual +
  `.pop()` mutation isolation) and the oneOrThrow pagination SQL-text
  assertion.
- **New coverage gained**: CardinalityViolationError tests for multi-row
  matches on all one-joins (plain and lateral — relevant to `6618f09`;
  the old suites never tested multi-row matches), `executeExists === false`
  paths, full contract coverage for laterals (`executeTakeFirst`,
  `toBaseQuery`, `executeExists` were never tested for laterals before), and a
  "collection override: second join with the same key wins" test pinning
  `fc8b9b5`.

**Gap (minor):** the old
`leftJoinOne: executeExists checks existence` asserted `executeExists()`
returns `true` when the left-joined child matches *nothing* (child filtered to
`user_id = 999`) — i.e. a matchless left join must not filter the exists
check. The new table-driven exists test uses a matching child, so this exact
combination is only covered indirectly (via `executeCount` with the same
matchless child, which shares the join-filtering strategy). Low risk;
worth restoring as a one-line table case.

---

## 4. Fix plan

_(populated below)_
