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

Severity scale: **major** = wrong results / unsafe behavior reachable from the
public API; **minor** = wrong results on exotic inputs; **note** = residual
sharp edge or documentation gap. Findings marked _confirmed_ were reproduced
with executed probes against HEAD.

### 2.1 Hydrator (`src/hydrator.ts`)

Per-commit verdict: all ten hydrator commits do what they claim and are
covered by named tests, with two partial verdicts — `f9db900` (finding H1)
and `90d201c` (finding H2).

| # | Severity | Where | Finding |
|---|---|---|---|
| H1 | **major** (confirmed) | `#hydrateOne` extender merge (`Object.assign`), ~`src/hydrator.ts:1224`; commit `f9db900` | **`.extend()` is still a live prototype-replacement vector.** Fields, extras, auto-include, collections, and attaches all route `__proto__` keys through `defineProtoShadowedKey`, but extender results are merged with `Object.assign(entity, extender(...))`, which uses `[[Set]]` and therefore triggers `Object.prototype`'s `__proto__` setter. Reproduced: an extender returning `JSON.parse('{"__proto__": {"polluted": true}}')` silently drops the key from own properties and **replaces the entity's prototype** (`entity.polluted === true`). This is the exact bug class the commit fixed elsewhere. No test covers extend + `__proto__`. Fix: copy extender results with a manual loop that routes `"__proto__"` through `defineProtoShadowedKey`. |
| H2 | minor (confirmed) | `getKey`, `src/hydrator.ts:1587`; commit `90d201c` | **The JSON-encoded composite key introduces new collision classes** while fixing separator collisions: (1) bigint `123n` vs string `"123n"` (replacer maps bigint to `"123n"`); (2) `NaN` vs `Infinity` (both serialize to `null` — and the nil check runs on the raw value, so they aren't skipped); (3) `Date` vs its ISO string (`toJSON`). Each pair silently merges two distinct rows into one entity. The old `"::"` join kept all three distinct. Fix: type-tag parts in the encoder (e.g. serialize bigints as `["bigint","123"]` or prefix parts with a type char) and encode NaN/±Infinity distinctly. |
| H3 | minor (confirmed) | `#hydrateMany` sort, `src/hydrator.ts:1295`; adjacent to `87c8b3e` | **`hydrate()` sorts the caller's input array in place.** Top-level arrays are passed through by reference (deliberate, post-`87c8b3e`), and any `orderBy`/`orderByKeys` hydrator with the default `sort: "all"` then does `inputsArray.sort(...)` on the caller's array. Reproduced. Pre-existing, but `87c8b3e` was specifically about not corrupting caller inputs. Fix: copy before sorting when the array is caller-owned. |
| H4 | note (confirmed) | attach lookups; commit `297e363` | Arrays are now referentially distinct per parent, but **child objects inside attach arrays remain shared across parents** with the same match value (`one`-mode attaches return the shared object itself). Matches the commit's literal claim; worth documenting on `FetchFn`/`attach`. |
| H5 | note (plausible) | `#fetchAllAttachedCollections` vs `#hydrateOne`; commit `9f6dad0` | Attach-input dedupe keeps the first row in *input* order while hydration's representative row is the first *after sorting*. If duplicate-keyed rows disagree on a non-key `toParent` column, fetch and lookup can use different values → silent no-match. Requires internally inconsistent data + `orderBy`. Fix: derive the representative row once for both. |
| H6 | note (confirmed) | `src/hydrator.ts:1066`; commit `9f6dad0` | When all parent rows have nil keys, the attach `fetchFn` is still called with `[]`. User code building `WHERE x IN (...)` from inputs will produce invalid or pointless SQL. Fix: skip the fetch when the deduped input array is empty. |
| H7 | note (confirmed) | `getKey` single- vs composite-key paths; surfaced by `00d7a0b` | Single `keyBy: "k"` uses raw Map-key identity (two equal `Date` PKs → two entities) while `keyBy: ["k"]` JSON-encodes (same Dates merge). Pre-existing inconsistency, now applies to every hydration since dedupe is unconditional. Document or normalize. |
| H8 | note (confirmed) | attach `matchChild`/`toParent`; pre-existing | Arity mismatch (`matchChild: ["userId"]` with `toParent: "id"`) silently never matches — composite side is JSON-encoded, single side raw. Consider a runtime arity check in `attach()`. |
| H9 | note | standalone `hydrate()`, `src/hydrator.ts:1488`; commit `5061543` | The factory call is wrapped in try/catch but the trailing `hydrator.hydrate(input)` is not — a factory returning a non-hydrator still throws synchronously, contradicting the "never throws" contract. Move it inside the try. |

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

### 3.2 Remaining test restructuring (core/execution split, renames, setup refactor)

**Verdict: no coverage regressions found.** Every deleted or renamed file was
accounted test-by-test against its state immediately before deletion:

| Old file | Tests | Accounted | Destination(s) |
|---|---|---|---|
| `query-set.basic.test.ts` | 19 | 19/19 | `core.test.ts`, `execution.test.ts` |
| `query-set.edge-cases.test.ts` | 24 | 24/24 | `execution`, `core`, `hydration`, `joins` |
| `query-set.collection-modify.test.ts` | 7 | 7/7 | `hydration.test.ts` (verbatim) |
| `query-set.modify.test.ts` | 7 | 7/7 | `core.test.ts` (verbatim) |
| `query-set.complex.test.ts` → `nesting.test.ts` | 9 | 9/9 | `nesting`, `hydration`, `execution` (one 3-level test subsumed by the retained 4-level superset) |
| `query-set.sql-generation.test.ts` → `sql.test.ts` | 38 | 38/38 | pure rename + 129 added lines; all SQL-text/snapshot/error-class assertions intact |
| `query-set.column-aliases.test.ts` (shrunk) | 13 | 13/13 | 5 SQL tests moved verbatim to `sql.test.ts` |
| `query-set.postgres.test.ts` | 36 | 36/36 | see below |
| `query-set.pagination.test.ts` (3 moved) | 3 | 3/3 | `execution.test.ts` (verbatim) |

Notable points:

- **The deleted 717-line `query-set.postgres.test.ts` was a smoke suite**
  (length / `Array.isArray` checks) of generic API behavior with *no*
  pg-specific constructs (no jsonb, arrays, RETURNING, casts — verified by
  grep). Every generic test file runs under both dialects via
  `getDbForTest()` + `npm run test:all`, so its 36 scenarios went from
  pg-only smoke checks to exact-equality checks under **both** dialects.
  The genuinely pg-only write suites (`postgres-insert/update/delete/write/
  mixed-writes`) survive with identical test counts.
- **Fixture data is structurally unchanged** (`aa84af4`): only reply content
  strings were renamed; row counts, ids, relationships, nulls, and the tricky
  duplicate-reply structure that composite-keyBy/deep-nesting tests depend on
  are intact. `fixture.ts` was not deleted — only the experimental seeder
  moved out.
- **Postgres wiring was strengthened**: `search_path` is now set via pool
  `options` so every pooled connection uses the per-file random schema
  (previously only one connection was configured — a latent flakiness bug),
  and a `pool.end()` guard fixes hangs in compile-only test files.
- Error-class assertions (`NoResultError`, `ExpectedOneItemError`,
  `CardinalityViolationError`, `UnexpectedSelectAllError`,
  `UnexpectedComplexAliasError`) all survive. The deleted `prefixSelectArg`
  tests covered a function deleted in the same commit with no other callers
  (dead code, not a gap); `helpers/query-wrapper.ts` was confirmed
  reference-free before deletion.
- New unit coverage: `prefixes.test.ts` (16 proxy/prefix tests),
  `hydrator.test.ts` 67→85 (proto-safety, dedupe, referential distinctness,
  one-shot iterables, async rejection), expanded hydration/attach/execution
  suites.

---

## 4. Fix plan

_(populated below)_
