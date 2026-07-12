# kysely-hydrate vs. rel8 vs. rust-rel8

A study of two libraries that solve the same problem as kysely-hydrate —
turning relational query results into nested, richly-typed objects — with very
different SQL strategies. The goal is to understand **how each structures and
generates SQL** and **what each can and cannot express**, and to extract
concrete improvement ideas for kysely-hydrate.

Subjects:

- **rel8** ([circuithub/rel8](https://github.com/circuithub/rel8)) — Haskell,
  Postgres-only, built on Opaleye. Nesting is done *inside the database* via
  `many`/`some`, which compile to LATERAL subqueries aggregated with
  per-column `ARRAY_AGG`.
- **rust-rel8** ([simmsb/rust-rel8](https://github.com/simmsb/rust-rel8)) — a
  young Rust port of rel8's ideas, introduced in the blog post "Postgres's
  lateral joins allow for quite the good eDSL". Uses a chain of
  `INNER JOIN LATERAL (...) ON TRUE` as the query-composition primitive
  itself.
- **kysely-hydrate** — this library: flat JOINs with `$$`-prefixed column
  aliases plus client-side hydration, decorating Kysely.

> **Sourcing.** rel8 findings come from a clone of `circuithub/rel8` at
> `186d56f` (docs source in `docs/`, internals in `rel8-internal/src/`), plus
> generated-SQL dumps preserved in issues
> [#153](https://github.com/circuithub/rel8/issues/153) and
> [#168](https://github.com/circuithub/rel8/issues/168). rust-rel8 findings
> come from a clone of `simmsb/rust-rel8` (crates.io v0.2.2); its one captured
> real SQL artifact is the repo's `a.sql` — other rust-rel8 SQL below is
> **reconstructed** from its builder code and labeled as such. The blog post
> and its HN/forum threads were unreachable from this environment, so no
> claims below rest on them. kysely-hydrate SQL is quoted verbatim from this
> repo's snapshot tests (`src/query-set.sql.test.ts`, sqlite dialect;
> postgres differs only in `?` → `$n`).

---

## 1. TL;DR — the strategy triangle

All three must answer the same question: **where does the "nesting" happen?**

| | Nesting happens… | One row per parent on the wire? | Dialects |
|---|---|---|---|
| **kysely-hydrate** | in JS, after a flat JOIN (client-side grouping by key) | No — child rows multiply parent rows; parent columns are duplicated across the wire | SQLite + Postgres (dialect-agnostic in principle) |
| **rel8** | in SQL — `many q` = correlated LATERAL subquery aggregated with per-column `ARRAY_AGG`, LEFT-joined `ON TRUE` with an empty-array fallback | Yes, always | Postgres only, deeply |
| **rust-rel8** | in SQL — same per-column `ARRAY_AGG` idea; additionally *all* composition (even plain joins) is a LATERAL chain | Yes | Postgres only, hard-wired |

Consequences in one paragraph each:

**kysely-hydrate** keeps the SQL boring and portable. The cost is row
explosion, which it pays for with real machinery: entity-correct pagination
requires the *cardinality-one subquery* rewrite (many-joins become
`WHERE EXISTS`, LIMIT/OFFSET apply to the entity subquery, then many-joins are
re-applied outside), counts need the same rewrite, per-parent ordering of
multiple sibling collections has to be finished in JS, and a client-side
grouping/dedup pass is always needed. In exchange it works on SQLite, the
generated SQL is readable and deterministic, native wire types survive
untouched, and there's an application-level escape hatch (`attach`) neither
rel8 has.

**rel8** makes "one row per parent" an invariant: after `many`, the outer
query's LIMIT/OFFSET/ORDER BY are trivially correct and no dedup pass exists
anywhere. Because `many` takes an *arbitrary query*, ordering, limiting
(top-N-per-parent), filtering, and further nesting inside a collection all
compose for free. The costs: Postgres-only; SQL output is a barely-readable
pyramid of renamed subqueries that leans on the planner to flatten (and its
`optional` encoding is planner-hostile — issue
[#72](https://github.com/circuithub/rel8/issues/72), open since 2021); and
the per-column-arrays representation forced a text-cast hack for
lists-inside-lists.

**rust-rel8** demonstrates that LATERAL alone is enough to make queries
compose like ordinary values in a host language without Haskell's monads — a
correlated subquery is spliced in as `INNER JOIN LATERAL (...) ON TRUE` and
everything else is a WHERE clause. It's an instructive proof of concept
(compile-time scope/aggregation safety via lifetime branding) but immature:
lists can't nest inside lists, writes are INSERT-only, and there are visible
decoding/rendering bugs.

---

## 2. Schema and type-safety models

**rel8 — higher-kinded data, one definition, three interpretations.** A table
is a record parameterized by a context `f`, each field `Column f a`:

```haskell
data Author f = Author
  { authorId   :: Column f AuthorId
  , authorName :: Column f Text
  , authorUrl  :: Column f (Maybe Text)   -- nullable column
  } deriving stock (Generic) deriving anyclass (Rel8able)
```

`Column` is a type family that erases itself in the result context: `Author
Expr` is a row of SQL expressions, `Author Name` a row of column names (the
`TableSchema`), and `Author Result` is *literally the plain domain record*
(`AuthorId`, `Text`, `Maybe Text`). The type is never defined twice, and
there's an inverse (`HKD`) that manufactures the table shape from an existing
flat record. Nullability is `Maybe` in the type; `Maybe (Maybe a)` is made
unrepresentable. Custom types plug in via `DBType` (encoder + decoder + SQL
type name), with `DBEq`/`DBOrd`/`DBNum` subclasses gating which operators
typecheck.

**rust-rel8 — GATs instead of HKD, plus lifetime-branded scopes.** A derive
macro (`#[derive(TableStruct)]`) on a struct generic over `Mode: TableMode`
gives the same "one struct, many modes" trick: `NameMode` → column names,
`ExprMode` → query row, `ValueMode` → decoded row, `ValueManyMode` →
`Vec<V>` per column for array decoding. Two things rel8 can't do at compile
time fall out of Rust's lifetimes: an `Expr` cannot escape its `query(|q| …)`
scope (compile error, verified by a compile-fail test), and an unaggregated,
ungrouped column cannot appear in an `aggregate` output (the `'inner`→`'outer`
conversion is only reachable through `group_by` or an aggregate function).
Costs: turbofish type annotations everywhere (inference can't cross the
lifetime plumbing), manual `shorten_lifetime()` calls, and **positional**
row decoding — the select-list order is the contract, with every alias
generated from a global atomic counter (which also makes the SQL text
non-deterministic across runs).

**kysely-hydrate — no schema of its own.** It rides Kysely's `DB` interface
and derives everything from the query builders the user writes, via the
`TQuerySet` type-parameter bag in `src/query-set.ts` (reusing Kysely's own
join type transformers so join nullability semantics match Kysely exactly).
There is no "table definition" concept to keep in sync — but also no
define-once domain type: the hydrated output type is *inferred from* the
query rather than declared, and `InferOutput` extracts it. Runtime decoding
is by column *name* (the `$$` prefix convention), not position, which is what
makes the SQL deterministic and debuggable but also why `SELECT *` must be
rejected (`UnexpectedSelectAllError` — names must be statically extractable
for prefixing).

### Runtime vs. erased schema knowledge

A structural difference that in-database nesting makes load-bearing: **both
rel8s maintain a runtime, value-level model of the schema; Kysely's is erased
at compile time.** The runtime model is still *declared in code* — nobody
introspects the live database — the difference is reification: Haskell
typeclass instances and Rust trait impls survive to runtime as actual values,
TypeScript types don't.

rel8 carries a `TypeInformation` record per column type
(`rel8-internal/src/Rel8/Internal/Type/Information.hs:27-37`):

```haskell
data TypeInformation a = TypeInformation
  { encode    :: Encoder a   -- serialize a Haskell value to PostgreSQL
  , decode    :: Decoder a   -- deserialize a result back to Haskell
  , delimiter :: Char        -- delimiter in PG's text format for arrays of this type
  , typeName  :: TypeName    -- the name of the SQL type
  }
```

plus a value-level `TableSchema` (table/column names) and the generic
`Rel8able`/`HTable` traversal over a table's columns — which is literally how
`listAgg` rewrites each column into its own `ARRAY_AGG`. Every field earns
its keep in the nesting pipeline: `typeName` generates the typed
empty-collection fallback (`CAST(ARRAY[] AS int8[])` — impossible without the
SQL type name at runtime) and the `CAST(… AS int8[])` on aggregation outputs;
`decode` supplies the per-element binary array decoders at read time; and
`delimiter` exists solely for the nested-lists hack, which re-parses
Postgres's array *text* format per element type. rust-rel8 carries a thinner
version (a `SCHEMA` const of column names, per-type sqlx `Decode` impls, and
the `Table::visit` order as the positional decoding contract) — and it gets
away with less metadata precisely because it lacks the features that need it:
no typed empty-array literals (empty ⇒ NULL ⇒ `unwrap_or_default()`), no
nested lists.

Kysely knows only column *names* at runtime (from its operation nodes — the
one sliver of runtime schema understanding kysely-hydrate exploits for `$$`
prefixing); column types exist only in the erased layer. That is sufficient
for the flat-JOIN strategy because the driver returns natively-typed scalar
values per column and hydration just regroups them — no per-type marshaling
step exists anywhere. In-database aggregation is exactly the feature that
breaks this: values start moving through a wrapper representation (typed
arrays or JSON), and *something* must know how to unwrap element types. rel8
answers with runtime type dictionaries; it chose typed arrays over JSON
despite the nested-list pain because binary arrays preserve exact wire types
— but only if you have runtime decoders to receive them. JSON is the
strategy you pick precisely when you don't. See §6.1.

---

## 3. Query composition

**rel8: the `Query` monad.** `Query a` means "a SELECT returning rows of
`a`". `fmap` is projection, `<*>` is a cartesian product, and `>>=` is a
*LATERAL* cartesian product — the right-hand side may reference the left's
row. There is no join operator: an inner join is bind plus `where_`:

```haskell
authorForBlogPost :: BlogPost Expr -> Query (Author Expr)
authorForBlogPost post = do
  author <- each authorSchema
  where_ (blogPostAuthorId post ==. authorId author)
  return author
```

Reusable query fragments are therefore ordinary functions, composed with
`>>=`/`<=<`. LEFT JOIN is `optional :: Query a -> Query (MaybeTable Expr a)`
— and `MaybeTable` carries an explicit tag column (see §4), so "no matching
row" is distinguishable from "matched row that is all NULLs". Semijoins are
`exists`/`present`/`absent`; `limit`/`offset`/`orderBy` are plain
query-to-query functions applicable to *any* subquery, which is exactly what
makes `many $ limit 3 $ orderBy … q` a top-3-per-parent.

**rust-rel8: the LATERAL chain as bind.** `query(|q| …)` opens a scope;
every `q.q(subquery)` splices an independent subquery and returns its row as
expressions. At render time the first becomes the FROM item and every
subsequent one becomes `INNER JOIN LATERAL (…) ON TRUE`; all `q.where_`
predicates land in the outer WHERE. Because LATERAL puts earlier FROM items
in scope, a `Query` value can close over outer expressions and still render
as a standalone subquery — that's the whole trick that replaces the monad:

```rust
fn posts_of_user(user_id: Expr<i32>) -> Query<Post> {
    query::<Post<ExprMode>>(|q| {
        let post = q.q(Query::each(POSTS));
        q.where_(user_id.equals(post.user_id.clone()));  // correlated
        post
    })
}
```

The stated mental model (its README): every `q.q` is a cartesian product and
the *database planner* is trusted to narrow cross-join laterals into real
joins. `optional()` reuses rel8's exact encoding (a one-row `VALUES (TRUE)`
LEFT-joined to the tagged subquery); `aggregate` rewrites output expressions
in place into `ARRAY_AGG`/`SUM`/… with the lifetime brand enforcing
grouped-ness.

**kysely-hydrate: a fluent decorator, not a DSL.** The user writes real
Kysely queries; kysely-hydrate composes them. Relations are declared
per-key (`leftJoinMany("posts", postsQuerySet, "posts.user_id", "user.id")`),
nested QuerySets compose recursively, and `modify(key, fn)` reaches into a
named collection. Composition is *relation-map shaped* rather than
*expression shaped*: each capability (join cardinality, lateral variants,
extras, mapFields, attach) is an explicit method rather than an emergent
property of a monad. What the rel8s lack entirely: `attachOne/Many` —
application-level joins that batch-fetch from anywhere (another DB, an API, a
cache) and stitch during hydration; and the base query stays a plain Kysely
builder the user fully controls, so there's no "raw SQL escape hatch"
problem to solve — the escape hatch is the substrate.

---

## 4. SQL structure and generation — side by side

### 4.1 A one-to-many relation (users → posts)

**kysely-hydrate** emits a flat join. Each level's base select is wrapped as
a derived table and the child's selections are hoisted up re-aliased with a
`key$$` prefix (two levels deep: `posts$$comments$$id`). From the snapshot
test at `src/query-set.sql.test.ts:318-368` (inner join body, sqlite):

```sql
select "user"."id" as "id", "user"."username" as "username",
       "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
from ( select "id", "username" from "users" ) as "user"
inner join (
  select "posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id"
  from ( select "id", "title", "user_id" from "posts" ) as "posts"
) as "posts" on "posts"."user_id" = "user"."id"
```

A user with 10 posts occupies 10 wire rows; hydration groups rows by `keyBy`
(default `id`) and dedups client-side (`src/hydrator.ts`, `groupByKey`).
Sibling many-joins multiply against each other (cartesian explosion), which
is why the library steers multi-collection cases toward laterals or
`attach`.

**rel8**: `many q` lowers to `aggregate listAgg q`, which is: aggregate the
correlated subquery into *one row of parallel arrays* (one `ARRAY_AGG` per
column — no `ROW()`, no JSON), then wrap in `optional` so a parent with zero
children still gets a row, with a `CASE WHEN tag IS NULL THEN ARRAY[] …`
fallback. Real output shape (from `showQuery` dumps in issues #153/#168,
abridged — note the mechanical renames and the `(SELECT 0) LEFT OUTER JOIN …
ON TRUE` encoding of `optional`):

```sql
SELECT CAST("result0_3" AS int8[]) as "projectAuthorId",
       CAST("result1_3" AS text[]) as "projectName"
FROM (SELECT *
      FROM (SELECT 0) as "T1"
      LEFT OUTER JOIN
        (SELECT TRUE as "rebind0_3", *
         FROM (SELECT ARRAY_AGG("inner0_2") as "result0_2", ...
               FROM (... correlated user subquery ...) as "T1"
               GROUP BY COALESCE(0)) as "T1") as "T2"
      ON TRUE) as "T1"
-- zero-children fallback, applied where the list is consumed:
-- CASE WHEN ("rebind0_3") IS NULL THEN CAST(ARRAY[] AS int8[]) ELSE "result0_2" END
```

When the subquery references the parent row, Opaleye marks it LATERAL, so
the effective plan is `LEFT JOIN LATERAL (SELECT ARRAY_AGG(…) FROM … WHERE
child.fk = parent.pk) ON TRUE` — one aggregated row per parent, **no GROUP
BY on parent keys ever emitted**; correlation comes entirely from LATERAL.
Decoding zips the parallel arrays back into `[Comment]` per row via hasql's
binary array decoders.

**rust-rel8**: same per-column `ARRAY_AGG`, and the join itself is already a
lateral. From the repo's captured `a.sql` (real output, abridged; `VALUES`
lists because the test uses inline data):

```sql
SELECT "t207"."values_1_217" AS "values_1_220", "t207"."expr218" AS "expr221"
FROM (
  SELECT "t209"."values_1_208" AS "values_1_217", "t216"."expr215" AS "expr218"
  FROM ( ... users ... ) AS "t209"
  INNER JOIN LATERAL (
    SELECT ARRAY_AGG("t210"."values_2_213") AS "expr215"
    FROM ( ... posts ... WHERE "t209"."values_0_208" = "t212"."values_1_211" ) AS "t210"
  ) AS "t216" ON TRUE
) AS "t207" ORDER BY "t207"."values_1_217" ASC
```

Decode is column-major → row-major: each column arrives as `Vec<V>`, then
rows are re-zipped positionally. Empty child set ⇒ NULL arrays ⇒ `vec![]`.

**Analysis.** The two rel8s never duplicate parent data on the wire and never
need a client-side grouping pass — the SQL result *is* the nested shape,
transposed. The price of the per-column-arrays representation (chosen over
JSON to keep native binary wire types) shows up at depth: Postgres can't
nest anonymous composite arrays, so rel8 supports `many (many …)` only via a
**text-cast hack** — the inner array is `CAST(… AS text)`, the outer becomes
`text[]`, and decoding re-parses Postgres array literal syntax with a
hand-written parser (`rel8-internal/src/Rel8/Internal/Type/Array.hs`; the
earlier `ROW()`-based design crashed, issue #168). rust-rel8 simply cannot
nest lists at all (no `TableLoaderManySqlx` impl for `ListTable<ListTable<…>>`).
kysely-hydrate's flat rows have no depth limit — prefixes just stack.

### 4.2 Pagination with a nested collection

This is where the strategies diverge hardest.

**kysely-hydrate** must not let LIMIT count exploded child rows, so `#toQuery`
builds a *cardinality-one subquery*: base + one-joins stay inside, filtering
many-joins become `WHERE EXISTS`, LIMIT/OFFSET/ORDER BY apply there, and the
many-joins are re-applied outside. Verbatim (`src/query-set.sql.test.ts:419-478`):

```sql
select "user"."id" as "id", "user"."username" as "username",
       "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
from (
  select "user"."id" as "id", "user"."username" as "username"
  from ( select "id", "username" from "users" where "users"."id" <= ? ) as "user"
  where exists (
    select 1 as "_", "posts"."id" as "posts$$id", "posts"."title" as "posts$$title", "posts"."user_id" as "posts$$user_id"
    from ( SELECT 1 ) as "__"
    inner join (
      select "posts"."id" as "id", "posts"."title" as "title", "posts"."user_id" as "user_id"
      from ( select "id", "title", "user_id" from "posts" ) as "posts"
    ) as "posts" on "posts"."user_id" = "user"."id"
  )
  order by "user"."id" asc
  limit ? offset ?
) as "user"
inner join ( ... posts again ... ) as "posts" on "posts"."user_id" = "user"."id"
order by "user"."id" asc
```

Note the many-join subquery is effectively evaluated twice (once as EXISTS,
once for real). `toCountQuery` is the same rewrite ending in `COUNT(*)`
(`src/query-set.sql.test.ts:162-213`), with non-filtering `leftJoinMany`
dropped from the count entirely (`:215-242`).

**rel8 / rust-rel8**: after `many`, the outer query is one-row-per-parent by
construction, so pagination is *just* `limit 10 . offset 20 . orderBy …` on
the outer query. There is no special machinery because the problem never
exists. The same holds for counting parents.

### 4.3 Top-N per parent (e.g. latest 3 comments per post)

**rel8** (cookbook flagship, `docs/cookbook.rst:139-168`):

```haskell
latestComments <- many $ limit 3 $ orderBy (commentCreatedAt >$< desc) do
  comment <- each commentSchema
  where_ (commentPostId comment ==. postId post)
```

Because `many` takes an arbitrary query and lowers to LATERAL, the limit is
per-parent automatically. Ordering *inside* the list is preserved into
`ARRAY_AGG` (or expressible as `ARRAY_AGG(x ORDER BY …)` via
`orderAggregateBy`). rust-rel8: same shape (`posts_of_user(uid).limit(1)
.optional()` in its tests).

**kysely-hydrate** supports exactly this via its lateral join methods —
`innerJoinLateralMany` with `.orderBy().limit()` on the nested set applies
the limit *inside* the lateral subquery (code path
`src/query-set.ts:3009-3019`, which explicitly preserves "which rows the
limit keeps"). Postgres-only, exercised in `src/query-set.joins.test.ts`
(no literal-SQL snapshot exists; assertions are on hydrated results). The
non-lateral joins can't do this, and multiple sibling many-collections can't
be simultaneously ordered in SQL — nested ordering is finished in JS during
hydration.

So the capability parity is closer than it first looks: rel8's headline trick
is *available* in kysely-hydrate, but as a distinct opt-in join flavor rather
than the natural consequence of the one composition primitive.

### 4.4 Generation pipeline, readability, parameters

| | Pipeline | Readability | Parameters | Deterministic SQL |
|---|---|---|---|---|
| kysely-hydrate | rewrites Kysely operation nodes; Kysely compiles | High — aliases are meaningful (`posts$$title`), structure mirrors the declaration | Kysely placeholders throughout | Yes |
| rel8 | `Query` ≈ Opaleye `Select` → `PrimQuery` → Opaleye printer; `showQuery` to inspect | Poor — deep `SELECT * FROM (…) AS "T1"` pyramids, mechanical renames (`inner0_3`, `rebind0_5`); trusts the planner to flatten | Historically all literals **inlined** into SQL (no plan cache); newer `prepared` API adds binary-format placeholders | Yes |
| rust-rel8 | sea-query AST → `PostgresQueryBuilder` | Poor — every combinator adds an onion layer; aliases from a **global atomic counter**, so SQL text differs run to run (hostile to statement caches/log diffing) | sqlx placeholders | **No** |

One planner lesson worth recording: rel8's `optional` encodes LEFT JOIN as
`(SELECT 0) LEFT OUTER JOIN (tagged subquery) ON TRUE` *inside* a lateral,
which Postgres cannot convert to a hash join — it degrades to nested loops
(issue #72, open since 2021, still active 2026). A plain
`LEFT JOIN LATERAL (…) ON TRUE` — which kysely-hydrate emits directly —
plans fine. The abstraction-purity of "LEFT JOIN is just `optional` applied
to any query" has a real query-plan cost.

A second one, from rust-rel8: Postgres memoizes uncorrelated *volatile*
lateral subqueries (e.g. `nextval`), so rust-rel8 has to inject `random() AS
dummy` columns and cross-references to defeat the optimizer when volatility
matters. Anyone leaning harder on laterals should remember volatile
expressions behave surprisingly there.

---

## 5. Ergonomics matrix

| Capability | kysely-hydrate | rel8 | rust-rel8 |
|---|---|---|---|
| Nested collections, arbitrary depth | ✅ prefixes stack (`a$$b$$c`) | ✅ but lists-in-lists ride a text-cast hack | ❌ depth 1 only |
| Parent kept when collection empty vs dropped | `leftJoinMany` vs `innerJoinMany` | `many` (→ `[a]`) vs `some` (→ `NonEmpty a`) — the result *type* encodes it | `many()`; `optional()` for maybe-one |
| Ordering inside a collection (in SQL) | Only via lateral joins; otherwise finished in JS during hydration; multiple siblings can't be simultaneously SQL-ordered | ✅ order the subquery before `many`, or `ARRAY_AGG(x ORDER BY …)` | ✅ order the subquery (but a decode bug reverses struct-row order) |
| Top-N per parent | ✅ lateral join methods (PG only) | ✅ `many $ limit n $ …` | ✅ `.limit(n)` on the spliced query |
| Outer pagination with nested collections | ✅ via cardinality-one subquery + `WHERE EXISTS` rewrite | ✅ trivial (one row per parent) | ✅ trivial |
| Correct `count` alongside | ✅ `executeCount` (same rewrite; `keyBy` must be unique or it overcounts) | `countRows` on the pre-`many` query — nothing special needed | `count()` exists (typed `i32`, PG returns bigint — bug) |
| Aggregates mixed with collections | Correlated subqueries in the base select (e.g. `commentsCount`) | ✅ one `aggregate` block can combine `groupBy` + `countOn` + `listAgg` + `FILTER (WHERE …)` | Basic aggregate fns; lifetime brand prevents ungrouped-column bugs at compile time |
| Writes | ✅ insert/update/delete + RETURNING hydrated, multi-write CTE orchestration (`writeAs`; PG) | ✅ fully typed `Insert`/`Update`/`Delete`, `ON CONFLICT` upsert incl. partial-index inference, `RETURNING` as a composable projection, statement-level `WITH` monad chaining DML as CTEs | INSERT-from-query only; no update/delete/upsert/RETURNING |
| App-level joins (other DBs, APIs, batching) | ✅ `attachOne/Many` (batched, no N+1) | ❌ | ❌ |
| Raw SQL escape hatch | The substrate *is* Kysely — full control, incl. `sql` template tag | Expression-level only (`unsafeLiteral`, custom functions); no raw FROM/whole-query splice | Expression vocabulary is thin (no `is_null`, `in`, casts…); no raw escape |
| `SELECT *` | ❌ rejected (names must be statically prefixable) | ✅ `each` selects the whole table by construction | ✅ `each` |
| Empty-vs-all-NULL left-join ambiguity | Resolved by `keyBy` non-nullness (rows with nil keys dropped) | Resolved by an explicit `TRUE` tag column (`MaybeTable`) — independent of any data column | Same tag trick (`VALUES (TRUE)` + tag), with a fallback mode inferring presence from non-nullable columns |
| Dialect portability | SQLite + Postgres | Postgres only (LATERAL, typed arrays, hasql) | Postgres only (sqlx-PG, `PgFunc::array_agg`) |
| Define-once domain types | Output types *inferred* from queries (`InferOutput`) | ✅ HKD: one record is schema + query row + result row; `HKD` also lifts existing plain records | ✅ GAT modes on one struct |
| Compile-time misuse guards | Type-level (e.g. `.map()` locks the builder, `@ts-expect-error` suites) | Type-level (nullability, `DBEq`-gated operators) | Lifetime-branded scopes: expression escape and ungrouped aggregation are *compile errors* |
| Maturity | Early (0.10.x) but tested on 2 dialects, property of this repo | Mature, production use since 2021; docs partially unfinished | WIP v0.2.2, single contributor, visible bugs (`last_value` renders `first_value`, `ntile` typo, `panic!("Nein danke")` on empty `values`) |

The deepest *conceptual* difference is not any single row of that table: in
the rel8s, **nesting is just aggregation of an arbitrary subquery** —
`many`/`some` accept anything, so filtered/ordered/limited/aggregated/nested
collections all compose with zero additional API. In kysely-hydrate, nesting
is a *relation declaration* (a join method per cardinality × flavor), so each
capability is an explicit, separately-implemented method. The declaration
style buys discoverability, per-relation control (`modify(key, …)`), and the
`attach` escape hatch; the aggregation style buys uniformity.

---

## 6. Ideas for kysely-hydrate

Ordered roughly by expected value. All are ideas only — none implemented
here.

### 6.1 An opt-in in-database aggregation mode (`json_agg`) for collections

The single biggest lesson from rel8 is the **one-row-per-parent invariant**:
when collections are aggregated in SQL, outer LIMIT/OFFSET/ORDER
BY/COUNT are correct *by construction*, nothing is duplicated on the wire,
and no dedup pass runs in JS. kysely-hydrate's cardinality-one subquery +
`WHERE EXISTS` rewrite (which evaluates many-join subqueries twice) and its
JS-side nested re-sorting exist precisely because it lacks this invariant.

A plausible design that fits the existing architecture: a per-collection or
per-query flag (`.aggregated()` / `executeAggregated()`) under which
`#addCollectionAsJoin` emits, instead of a hoisting join,

```sql
left join lateral (
  select coalesce(jsonb_agg(t order by …), '[]'::jsonb) as "posts"
  from ( <nested query> ) as t
  where t.user_id = "user"."id"
) as "posts" on true
```

and the hydrator gets a "pre-nested column" mode (it is already SQL-agnostic,
so this is a decode-path addition, not a rewrite). Notes from the two rel8s:

- Prefer `jsonb_agg(to_jsonb(row))` over rel8's per-column parallel arrays.
  The arrays choice preserves binary wire types but forced rel8's
  text-cast hack for nested lists and capped rust-rel8 at depth 1; JSON nests
  arbitrarily and decodes as one column. More fundamentally (see §2, "Runtime
  vs. erased schema knowledge"): rel8's typed-array route *requires* runtime
  per-column type dictionaries to emit typed empty-array literals and to
  decode array elements, and kysely-hydrate has no runtime type channel —
  Kysely's types are erased and only column names survive to runtime. JSON is
  the representation that needs no runtime type knowledge to decode, which
  makes it the only strategy compatible with kysely-hydrate's architecture
  as-is. The cost is that in-DB-nested values arrive JSON-typed, not
  wire-typed: dates/numerics/bytea come back as strings/numbers, differing
  from what the same query returns today via flat joins. Two ways to pay it:
  (a) accept the lossy defaults and delegate repair to user-supplied
  `mapFields` on the collection; (b) an opt-in per-collection decode spec — a
  small explicit runtime type hint (e.g. `{ createdAt: "timestamptz" }`) —
  which would be kysely-hydrate's first deliberate step away from "types are
  erased, names are enough." Start with (a); (b) can layer on later without
  breaking anything.
- Emit the *plain* `LEFT JOIN LATERAL … ON TRUE` form. rel8's
  `(SELECT 0) LEFT JOIN … ON TRUE` encoding is the documented planner
  pessimization (issue #72); kysely-hydrate controls its SQL directly and can
  simply not do that.
- SQLite has `json_group_array`/`json_object`, so unlike the rel8s this mode
  need not be Postgres-only — though laterals are, so the SQLite variant
  would use a correlated subquery in the select list instead.
- This also collapses the "multiple sibling many-collections can't be
  simultaneously SQL-ordered" limitation: each lateral carries its own
  `ORDER BY` inside its own `jsonb_agg`.

### 6.2 A presence tag for left-joined relations

Both rel8s solve the "no matching row vs matched row that is all NULLs"
problem with an explicit discriminator: select `TRUE AS tag` inside the
subquery; outer NULL tag ⇒ absent. kysely-hydrate instead relies on `keyBy`
being non-null, and silently drops rows with nil keys. That's usually right,
but it couples correctness to a data column and to `keyBy` uniqueness (the
README already documents the overcount caveat). Adding a synthetic
`1 as "posts$$__present"` to every left-joined collection subquery would make
presence detection structural, independent of the child's columns, and would
let hydration distinguish "child row whose keyBy is genuinely NULL" from "no
match" instead of conflating them. Cheap to emit; the hydrator's
`groupByKey` nil-check would consult the tag first.

### 6.3 Borrow `some`/`many` semantics into the type system

rel8's two-word vocabulary (from parser combinators) encodes cardinality ×
filtering: `many` → `[a]` with the parent kept, `some` → `NonEmpty a` with
childless parents dropped. kysely-hydrate already has exactly these
semantics as `leftJoinMany` / `innerJoinMany` — but `innerJoinMany` types its
collection as `T[]` even though it is provably non-empty. Typing it
`[T, ...T[]]` would be a free, zero-runtime-cost type-safety win and makes
the semantic difference between the two join flavors self-documenting.
(Optionally: alias the pair as `some`/`many` in docs to teach the model.)

### 6.4 Lean into laterals as the primary many-relation on Postgres

rust-rel8 is an existence proof that LATERAL alone is a complete composition
primitive, and rel8 lowers to it for every `many`. kysely-hydrate already
has the lateral join methods and already applies nested `orderBy`/`limit`
inside the lateral (`src/query-set.ts:3009-3019`) — but they're a
low-visibility variant with no SQL snapshot tests and results-only coverage.
Concretely: add literal-SQL snapshots for the lateral top-N shape (they pin
the most subtle codepath in `#toQuery`), document top-N-per-parent as a
headline feature next to pagination, and consider making
`leftJoinMany` *automatically* upgrade to a lateral on Postgres when the
nested set carries `orderBy`/`limit` — that's the case where the flat join
silently can't honor the nested clauses today. Also worth a docs note: the
volatile-function memoization gotcha inside laterals (rust-rel8's `random()`
dummy hack) applies to any user putting `random()`/`nextval` in a lateral
nested query.

### 6.5 Smaller observations

- **Joins as functions.** rel8's strongest ergonomic property is that a
  reusable relation is just `BlogPost Expr -> Query (Author Expr)`.
  kysely-hydrate's `({eb, qs}) => qs(…)` factories are close; a documented
  pattern (and maybe a helper type) for "correlated QuerySet factory as the
  unit of reuse" would surface it. rel8's `Tabulation k a` (queries indexed
  by key, with `lookup`/`align`/`leftAlign` combinators) is a richer version
  of the same idea and maps suggestively onto what `attach` does at the
  application level.
- **Composable RETURNING.** rel8's `Returning` is a projection whose result
  is again a `Query`, and its `Statement` monad chains DML as CTEs —
  structurally similar to `writeAs` + `stripWithPlugin`. Its documented
  caveat (sub-statement side effects aren't mutually visible within one
  `WITH`) is worth stating in kysely-hydrate's write docs too.
- **Keep the deterministic aliases.** rust-rel8's global-counter aliasing
  makes SQL text non-reproducible; rel8's mechanical renames make `EXPLAIN`
  output nearly unreadable. kysely-hydrate's meaningful, stable `$$` aliases
  are a genuine differentiator for debugging — worth protecting when adding
  any aggregation mode (name the lateral/agg columns after the relation key,
  not a counter).
- **Count-path TODO.** rel8 needs no count machinery because of §6.1's
  invariant; until/unless that lands, the existing TODO at
  `src/query-set.ts:~2920` (convert ON clauses to WHERE in the
  `WHERE EXISTS` count path) is the incremental version of the same win.

---

## Appendix: primary sources

- kysely-hydrate SQL exhibits: `src/query-set.sql.test.ts` (scenarios at
  lines 162-213, 215-242, 244-316, 318-368, 374-417, 419-478, 480-610,
  703-731, 1416+, 1660-1682); lateral behavior `src/query-set.joins.test.ts`
  (case builders from :1256; nested laterals :1543-1570); code paths
  `src/query-set.ts` (`#toQuery` ~:2961, `#toCardinalityOneQuery` ~:2893,
  `#addCollectionAsJoin` ~:2817, lateral limit placement :3009-3019).
- rel8: clone of [circuithub/rel8](https://github.com/circuithub/rel8) @
  `186d56f` — `docs/tutorial.rst`, `docs/concepts/*.rst`,
  `docs/cookbook.rst:139-168` (top-N example);
  `rel8-internal/src/Rel8/Internal/Query/{List,Aggregate,Maybe,Exists,Limit}.hs`,
  `…/Table/{Aggregate,Maybe}.hs`, `…/Type/Array.hs` (nested-array text
  cast), `…/Statement/{Insert,OnConflict,Prepared}.hs`; generated SQL from
  issues [#153](https://github.com/circuithub/rel8/issues/153) and
  [#168](https://github.com/circuithub/rel8/issues/168); planner issue
  [#72](https://github.com/circuithub/rel8/issues/72).
- rust-rel8: clone of [simmsb/rust-rel8](https://github.com/simmsb/rust-rel8)
  (v0.2.2) — `src/lib.rs` (`query` :2009-2074, lateral render comment
  :2053-2054, `optional` :1505-1552, `aggregate` :1396-1423, `many`
  :1428-1452, volatility hack :2022-2040), `derive/src/lib.rs`,
  `tests/queries.rs`, `tests/fail/*` (compile-fail proofs), `a.sql`
  (captured real SQL). The introducing blog post
  ([bensimms.moe](https://bensimms.moe/postgres-lateral-makes-quite-a-good-dsl/))
  and its [forum](https://users.rust-lang.org/t/postgress-lateral-joins-allow-for-quite-the-good-edsl/139846)/[HN](https://news.ycombinator.com/item?id=47921802)
  threads were unreachable from this environment; no claims here rest on
  them.
