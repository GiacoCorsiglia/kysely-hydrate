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

| Commit    | Claim                                                                                                                                                                |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `8aeaa0d` | Prefer `other.orderByKeys` in `hydrator.with()` (orderByKeys becomes tri-state `boolean \| undefined` so composition can tell "explicitly set" from "default")       |
| `90d201c` | Composite keys JSON-encode parts instead of joining with `"::"`, avoiding cross-boundary collisions                                                                  |
| `00d7a0b` | Always dedupe rows with identical key; removes the no-collections fast path and the `hasMultipleManyCollections` / `hasSiblingManyCollections` propagation machinery |
| `621423f` | Grouping stores single rows directly and only allocates a `RowGroup` array on the second row with the same key                                                       |
| `5061543` | `hydrate()` / `hydrator.hydrate()` never throw synchronously; sync errors become rejections                                                                          |
| `87c8b3e` | Input iterables are materialized once so attach-fetching + hydration don't exhaust one-shot iterables                                                                |
| `3f2e8f3` | Overload reorder so `hydrate(one)` types as `Promise<Output>` rather than `Promise<Output \| Output[]>`                                                              |
| `9f6dad0` | Attach fetch functions receive inputs deduped by parent `keyBy`, with nil-key (phantom left-join) rows excluded                                                      |
| `297e363` | `many`-mode attach/group lookups return referentially distinct arrays per parent (`RowGroup.rows.slice()`)                                                           |
| `f9db900` | `__proto__` output keys are assigned via `Object.defineProperty` (own data property) instead of being dropped / polluting the prototype                              |

### 1.2 Functional changes (query set: `src/query-set.ts`, `src/helpers/`)

| Commit    | Claim                                                                                                                                     |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `3fabc72` | `keyBy` now respected in query sets (hydrator created with the query set's `keyBy`)                                                       |
| `31c9498` | `limit(0)` / `offset(0)` respected (strict `!== null` checks instead of falsy checks)                                                     |
| `6618f09` | `oneOrThrow` joins now count as cardinality-one (`mode !== "many"`), fixing pagination/nesting decisions                                  |
| `ce044c4` | Write CTEs preserved when a write query set has no joins and no pagination (the "less nesting" fast path is skipped for write query sets) |
| `eec255d` | ORDER BY emitted on subqueries with LIMIT in the cardinality-one path (lateral "top-N per group" correctness)                             |
| `6902197` | Documented + tested: `.modify()`/`.where()` on the base query are discarded by `.insert()`/`.write()` base replacement                    |
| `fc8b9b5` | `.attach()` goes through `#addCollection` so an attach collection can override a same-key join collection                                 |
| `025e650` | `sqlCompare` fallback returns 0 for equal string forms — comparator is now a valid total order for mixed-type inputs                      |
| `38fb3ca` | Dead code removed: `src/helpers/query-wrapper.ts`, parts of `prefixes.ts`, `select-renamer.ts`, `utils.ts`                                |

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

| #   | Severity              | Where                                                                                     | Finding                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --- | --------------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| H1  | **major** (confirmed) | `#hydrateOne` extender merge (`Object.assign`), ~`src/hydrator.ts:1224`; commit `f9db900` | **`.extend()` is still a live prototype-replacement vector.** Fields, extras, auto-include, collections, and attaches all route `__proto__` keys through `defineProtoShadowedKey`, but extender results are merged with `Object.assign(entity, extender(...))`, which uses `[[Set]]` and therefore triggers `Object.prototype`'s `__proto__` setter. Reproduced: an extender returning `JSON.parse('{"__proto__": {"polluted": true}}')` silently drops the key from own properties and **replaces the entity's prototype** (`entity.polluted === true`). This is the exact bug class the commit fixed elsewhere. No test covers extend + `__proto__`. Fix: copy extender results with a manual loop that routes `"__proto__"` through `defineProtoShadowedKey`. |
| H2  | minor (confirmed)     | `getKey`, `src/hydrator.ts:1587`; commit `90d201c`                                        | **The JSON-encoded composite key introduces new collision classes** while fixing separator collisions: (1) bigint `123n` vs string `"123n"` (replacer maps bigint to `"123n"`); (2) `NaN` vs `Infinity` (both serialize to `null` — and the nil check runs on the raw value, so they aren't skipped); (3) `Date` vs its ISO string (`toJSON`). Each pair silently merges two distinct rows into one entity. The old `"::"` join kept all three distinct. Fix: type-tag parts in the encoder (e.g. serialize bigints as `["bigint","123"]` or prefix parts with a type char) and encode NaN/±Infinity distinctly.                                                                                                                                                 |
| H3  | minor (confirmed)     | `#hydrateMany` sort, `src/hydrator.ts:1295`; adjacent to `87c8b3e`                        | **`hydrate()` sorts the caller's input array in place.** Top-level arrays are passed through by reference (deliberate, post-`87c8b3e`), and any `orderBy`/`orderByKeys` hydrator with the default `sort: "all"` then does `inputsArray.sort(...)` on the caller's array. Reproduced. Pre-existing, but `87c8b3e` was specifically about not corrupting caller inputs. Fix: copy before sorting when the array is caller-owned.                                                                                                                                                                                                                                                                                                                                   |
| H4  | note (confirmed)      | attach lookups; commit `297e363`                                                          | Arrays are now referentially distinct per parent, but **child objects inside attach arrays remain shared across parents** with the same match value (`one`-mode attaches return the shared object itself). Matches the commit's literal claim; worth documenting on `FetchFn`/`attach`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| H5  | note (plausible)      | `#fetchAllAttachedCollections` vs `#hydrateOne`; commit `9f6dad0`                         | Attach-input dedupe keeps the first row in _input_ order while hydration's representative row is the first _after sorting_. If duplicate-keyed rows disagree on a non-key `toParent` column, fetch and lookup can use different values → silent no-match. Requires internally inconsistent data + `orderBy`. Fix: derive the representative row once for both.                                                                                                                                                                                                                                                                                                                                                                                                   |
| H6  | note (confirmed)      | `src/hydrator.ts:1066`; commit `9f6dad0`                                                  | When all parent rows have nil keys, the attach `fetchFn` is still called with `[]`. User code building `WHERE x IN (...)` from inputs will produce invalid or pointless SQL. Fix: skip the fetch when the deduped input array is empty.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| H7  | note (confirmed)      | `getKey` single- vs composite-key paths; surfaced by `00d7a0b`                            | Single `keyBy: "k"` uses raw Map-key identity (two equal `Date` PKs → two entities) while `keyBy: ["k"]` JSON-encodes (same Dates merge). Pre-existing inconsistency, now applies to every hydration since dedupe is unconditional. Document or normalize.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| H8  | note (confirmed)      | attach `matchChild`/`toParent`; pre-existing                                              | Arity mismatch (`matchChild: ["userId"]` with `toParent: "id"`) silently never matches — composite side is JSON-encoded, single side raw. Consider a runtime arity check in `attach()`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| H9  | note                  | standalone `hydrate()`, `src/hydrator.ts:1488`; commit `5061543`                          | The factory call is wrapped in try/catch but the trailing `hydrator.hydrate(input)` is not — a factory returning a non-hydrator still throws synchronously, contradicting the "never throws" contract. Move it inside the try.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

### 2.2 Query set + helpers (`src/query-set.ts`, `src/helpers/`)

Per-commit verdict: `3fabc72`, `31c9498`, `eec255d`, `6902197`, and `38fb3ca`
fully verified with test coverage. `6618f09` is correct for the direct case
but its recursive analogue is still broken (Q3). `ce044c4` fixed the fast
path but not the pagination combination (Q4). `fc8b9b5` fixed the SQL side
only (Q1/Q2). `025e650` fixed pairwise antisymmetry only (Q7).

| #   | Severity                                              | Where                                                                                                                                            | Finding                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| --- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Q1  | **critical** (confirmed)                              | `#addCollection` (`src/query-set.ts:2685`) + hydrator `has()`/`attach()` (`src/hydrator.ts:962`); commit `fc8b9b5`                               | **A join cannot override an attach — the stale attach fetchFn still runs and its output clobbers the join's data.** `#addCollection` cleans up the query-set-level maps, but the hydrator keeps `collections` and `attachedCollections` as two independent maps, and `#hydrateOne` processes attaches _after_ joins. Reproduced: `.attachMany("posts", fetchFn, …)` followed by `.leftJoinMany("posts", …)` → fetchFn is still called (spurious query) and the result contains the attach data, not the join's. Fix: hydrator `has()` should delete the key from `attachedCollections` and `attach()` from `collections` (mirroring `#addCollection`), or expose `removeCollection(key)`. No override-across-type test exists. |
| Q2  | **major** (confirmed)                                 | same root cause; throw at `applyCollectionMode` (`src/hydrator.ts:1518`); commit `fc8b9b5`                                                       | **Attach overriding a `oneOrThrow` join throws `ExpectedOneItemError` during hydration.** The join's SQL is correctly removed, but the stale hydrator spec (mode `oneOrThrow`) reads the now-nonexistent `posts$$…` columns, finds no children, and throws. For overridden `one`/`many` joins the stale spec is silently masked (but nested attaches of an overridden join still fetch — stale side effects). Same fix as Q1.                                                                                                                                                                                                                                                                                                  |
| Q3  | **major** (confirmed)                                 | `#applyOrderBy`/`#toCardinalityOneQuery` (`src/query-set.ts:2846/2893`); type gap in `TOrderableColumnsWithJoin` (`:2499`); sibling of `6618f09` | **Ordering by a one-join's column emits invalid SQL when that join's nested query set contains a many-join and pagination is set.** `#isCollectionCardinalityOne` is recursive, so the join is excluded from the cardinality-one subquery, but the type of `orderBy` keys is not recursive — `.orderBy("author$$username").limit(3)` compiles with no cast and fails at execution with `no such column: author.username`. Fix: tighten `TOrderableColumnsWithJoin` to require recursive cardinality-one plus a clear runtime error (or include row-limited left one-joins in the subquery). No test.                                                                                                                           |
| Q4  | **major** (SQL verified; pg error by documented rule) | `#getSelectFromBase` (`src/query-set.ts:2744`) + wrap (`:3029`); commit `ce044c4`                                                                | **`.write()` CTEs are emitted inside a derived-table subquery when combined with many-joins + pagination** — PostgreSQL rejects data-modifying CTEs below the top level (SQLSTATE 0A000). Same defect for `toExistsQuery()` on any write query set. `ce044c4` fixed only the no-join/no-pagination fast path. Fix: strip CTEs from the inner query and attach the `writeQueryCreator` to the outermost builder. No pg write test uses `limit`/`offset` at all.                                                                                                                                                                                                                                                                 |
| Q5  | major, fails loudly (confirmed)                       | `#getSelectFromBase` (`src/query-set.ts:2768`) vs `applyHoistedSelections` (`:3034`); pre-existing                                               | `.insert()`/`.update()`/`.delete()` base + many-join + pagination cannot compile: the non-select base path uses `.selectAll(baseAlias)` assuming no further hoisting, but the pagination wrap does hoist → misleading `UnexpectedSelectAllError`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Q6  | major (confirmed)                                     | `#toQuery` (`src/query-set.ts:3051`); pre-existing (introduced outside range)                                                                    | **`modifyFront()`/`modifyEnd()` are silently dropped on every query path except many-join + pagination** — all earlier `return` branches skip them. Reproduced: `.modifyEnd(sql\`/_ HINT _/\`)` absent with no joins, no joins + limit, one-join + limit, many-join without limit. Fix: apply the modifiers on every return path.                                                                                                                                                                                                                                                                                                                                                                                              |
| Q7  | major within claim (confirmed)                        | `sqlCompare` (`src/helpers/order-by.ts:83`); commit `025e650`                                                                                    | **The comparator is still not a total order.** (1) NaN: `a - b` returns `NaN`, and `[3, NaN, 1, NaN, 2].sort(sqlCompare)` comes back _completely unsorted_ — even the plain numbers. (2) Cross-type intransitivity: `2 < 10`, `10 ~ "10"`, but `2 > "10"` lexicographically — sort output depends on input order (`[2,"10",10]` → `["10",2,10]`; `[10,"10",2]` → `[10,"10",2]`). Fix: rank by type first (nil < boolean < numeric with NaN pinned to one end < Date < string < other), compare within rank. Only the equal-string-form case is tested.                                                                                                                                                                         |
| Q8  | minor (confirmed)                                     | `src/query-set.ts:2997`; pre-existing                                                                                                            | Non-select base + pagination fast path double-selects: `select "user".*, * from …` (extra `.selectAll()` on a builder that already select-alls). Duplicate columns only.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Q9  | note                                                  | `#asWrite` (`src/query-set.ts:3434`)                                                                                                             | Calling `.insert()`/`.update()`/`.delete()` after `.write()` keeps a stale `writeQueryCreator`: its CTEs are silently never emitted and the `ce044c4` fast-path exclusion forces unnecessary nesting. Reset `writeQueryCreator: null` in `#asWrite`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

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
returns `true` when the left-joined child matches _nothing_ (child filtered to
`user_id = 999`) — i.e. a matchless left join must not filter the exists
check. The new table-driven exists test uses a matching child, so this exact
combination is only covered indirectly (via `executeCount` with the same
matchless child, which shares the join-filtering strategy). Low risk;
worth restoring as a one-line table case.

### 3.2 Remaining test restructuring (core/execution split, renames, setup refactor)

**Verdict: no coverage regressions found.** Every deleted or renamed file was
accounted test-by-test against its state immediately before deletion:

| Old file                                           | Tests | Accounted | Destination(s)                                                                                   |
| -------------------------------------------------- | ----- | --------- | ------------------------------------------------------------------------------------------------ |
| `query-set.basic.test.ts`                          | 19    | 19/19     | `core.test.ts`, `execution.test.ts`                                                              |
| `query-set.edge-cases.test.ts`                     | 24    | 24/24     | `execution`, `core`, `hydration`, `joins`                                                        |
| `query-set.collection-modify.test.ts`              | 7     | 7/7       | `hydration.test.ts` (verbatim)                                                                   |
| `query-set.modify.test.ts`                         | 7     | 7/7       | `core.test.ts` (verbatim)                                                                        |
| `query-set.complex.test.ts` → `nesting.test.ts`    | 9     | 9/9       | `nesting`, `hydration`, `execution` (one 3-level test subsumed by the retained 4-level superset) |
| `query-set.sql-generation.test.ts` → `sql.test.ts` | 38    | 38/38     | pure rename + 129 added lines; all SQL-text/snapshot/error-class assertions intact               |
| `query-set.column-aliases.test.ts` (shrunk)        | 13    | 13/13     | 5 SQL tests moved verbatim to `sql.test.ts`                                                      |
| `query-set.postgres.test.ts`                       | 36    | 36/36     | see below                                                                                        |
| `query-set.pagination.test.ts` (3 moved)           | 3     | 3/3       | `execution.test.ts` (verbatim)                                                                   |

Notable points:

- **The deleted 717-line `query-set.postgres.test.ts` was a smoke suite**
  (length / `Array.isArray` checks) of generic API behavior with _no_
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

### 4.1 Blockers — fix before release

These are regressions or incomplete fixes _within the commits being
released_, plus one unsafe behavior; all reachable from the public API.

1. **Q1 + Q2 — complete the `fc8b9b5` override fix in the hydrator.**
   Make `HydratorImpl.has()` delete its key from `attachedCollections` and
   `attach()` delete its key from `collections` (mirroring
   `#addCollection.mapWithDeleted`). Add tests: join-over-attach (assert the
   join's data wins and the stale fetchFn is _not_ called),
   attach-over-`oneOrThrow`-join (no `ExpectedOneItemError`), and
   attach-over-join with nested attaches (no stale fetches).
2. **H1 — close the `.extend()` prototype-pollution hole (`f9db900`).**
   Replace `Object.assign(entity, extended)` in `#hydrateOne` with a copy
   loop that routes `"__proto__"` through `defineProtoShadowedKey`. Add an
   extend + `__proto__` test (scalar and object values; assert prototype
   unchanged and key present as own property).
3. **Q7 — make `sqlCompare` a real total order (`025e650`).**
   Type-rank first (nil < boolean < numeric with NaN pinned < Date < string
   < other-by-string), compare within rank. Property-style test: for a pool
   of mixed values (numbers, NaN, ±Infinity, numeric strings, Dates, nulls),
   assert sort output is independent of input permutation.

### 4.2 Should fix — pre-existing bugs surfaced by this review

Not regressions from this range, but live correctness bugs; fix now or track
as issues so the release notes can scope them.

4. **Q6 — apply `modifyFront`/`modifyEnd` on every `#toQuery` return path**
   (guarding the non-select fast path). Test each path for the marker SQL.
5. **Q3 — invalid SQL when ordering by a non-recursively-cardinality-one
   join's column with pagination.** Minimum: clear runtime error; better:
   tighten `TOrderableColumnsWithJoin` recursively.
6. **Q4 + Q9 — write CTE placement.** Attach `writeQueryCreator` CTEs to the
   outermost builder when the query gets wrapped (pagination/exists), and
   reset `writeQueryCreator` in `#asWrite`. Add pg tests for write + join +
   limit and write + `toExistsQuery`.
7. **Q5 — `UnexpectedSelectAllError` for insert/update/delete base +
   many-join + pagination.** Hoist real selections instead of
   `.selectAll(baseAlias)` in the non-select base path, or raise a clear
   unsupported-combination error.
8. **H3 — stop sorting the caller's input array in place** (`slice()` before
   sort when the top-level array is caller-owned).

### 4.3 Nice to have — edge hardening and docs

9. **H2 — type-tagged key encoding** to remove the bigint/`"123n"`,
   NaN/Infinity, and Date/ISO-string collision classes (and consider
   normalizing single-key vs composite-key semantics, H7).
10. **H6 — skip attach fetch when the deduped input array is empty** (avoid
    `fetchFn([])` → `WHERE x IN ()` foot-gun).
11. **H5 — use one representative row** for both attach-input dedupe and
    hydrate-time lookup.
12. **H8 — runtime arity check** for `matchChild`/`toParent` in `attach()`.
13. **H9 — move `hydrator.hydrate(input)` inside the try** in standalone
    `hydrate()`.
14. **H4 — document** that attach _elements_ (and `one`-mode results) are
    shared across parents; only the arrays are copies.
15. **Test gap — restore the matchless-left-join `executeExists` case**
    (§3.1) as a table case in `query-set.joins.test.ts`.
16. **Q8 — drop the redundant `.selectAll()`** in the non-select-base
    pagination fast path.

### 4.4 Release readiness

- The test restructuring is safe to ship: both audits found no lost
  coverage; assertions were generally strengthened and the old pg smoke
  suite now runs with exact-equality under both dialects.
- Of the functional fixes, 15 of 19 commits are verified correct and
  covered. The four partial ones (`fc8b9b5`, `f9db900`, `025e650`,
  `ce044c4`) have concrete completions in §4.1/§4.2 — items 1–3 are the
  release blockers, since each is a wrong-result or unsafe behavior in the
  very feature the commit advertises as fixed.
