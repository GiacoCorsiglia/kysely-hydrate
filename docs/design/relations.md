# Declared Relations: `relate*` / `include`

**Status:** Design complete, implementation-ready. Every load-bearing type and
runtime mechanism in this document was validated by compile/run spikes
(S1–S11) against real kysely 0.28.8 / TypeScript 5.9.3 / this repo's strict
tsconfig, and the design was hardened by adversarial review whose findings
were verified directly against `src/query-set.ts` and `src/hydrator.ts`. Spike
IDs (S1–S11) appear throughout as evidence labels; see [§5](#5-validation) for
the legend and what each spike proved.

**Amended** by the map × relations design
([map-relations.md](./map-relations.md)), which supersedes exactly one
decision here (D6, `map()` terminality) and strengthens a handful of gates
and error surfaces. Amendments are marked in place; M-labeled spike IDs
(M1–M8) refer to that design's own compile/run validation round (its §5).

---

## 1. Motivation & goals

A query set should become the **canonical representation of an entity**: its
base query plus a menu of **declared relations** — all the relationships you
*might* want. Callers then choose which relations to materialize, and (where
possible) by which strategy: a DB-level join (today's `*Join*` behavior) or an
app-level join (today's `attach*` behavior: a separate batched query, matched
back in JS).

Today the README's flagship example must reach through `modify("posts", …)`
and hand-write attach plumbing (the fetch function, the `IN` list, the match
keys) at every call site that wants the relation. Declared relations collapse
that into a one-time declaration plus two words at the call site — while
keeping the library's founding commitments:

- **Zero-compromise type safety.** The output type exactly reflects what was
  included. Not-included relations simply don't exist on the output type — no
  `| undefined` unions on a mega-type, no `any` leaks, no unsafe casts.
  Unavailable strategies are rejected at compile time.
- **100% correctness.** Pagination, deduplication, and count semantics remain
  exactly right under both strategies, formalized in a strategy-equivalence
  contract ([§3.8](#38-semantics)).

Core principles (fixed for this design):

1. **Lazy always.** Declaring a relation is inert: no SQL, no fetch, no
   output-type change. Relations are never fetched unless included; there is
   no eager declaration mode and no opt-out API.
2. **Chainable inclusion.** `usersQs.include("posts")` returns a new query set
   whose `HydratedOutput` has `posts` — not a Prisma-style options object
   passed to `execute()`.
3. **Dual-declarable strategies with derived defaults.** Declaring a relation
   once with join columns makes *both* strategies available with zero extra
   ceremony: the app-level fetch is derived from the same join-column
   information used for the DB join. A custom fetch function (cache, HTTP,
   another DB) is also declarable; strategies that don't apply (laterals have
   no cheap app-level equivalent, a custom fetch has no DB join) are excluded
   by the type system.
4. **Nested inclusion via callbacks only:**
   `.include("posts", (posts) => posts.include("category"))`. No string-path
   or object-literal syntax.
5. **Relations are structurally non-filtering** (left-join semantics /
   attach). Filtering joins (`innerJoin*`, `crossJoin*`) remain the eager
   SQL-shaping vocabulary — this boundary is what makes the count invariant
   and the equivalence contract provable.

Hydrated writes (`insert()`/`update()`/`writeAs()` interplay) are out of
scope; [§3.8](#38-semantics) item 10 records the expected-but-unverified
interactions without binding the future writes design.

---

## 2. API overview

The canonical pattern: entity modules declare menus; call sites include.

```ts
// db/entities.ts
export const Categories = querySet(db).selectAs("category",
	db.selectFrom("categories").select(["id", "name", "slug"]));

export const UserPreviews = querySet(db)
	.selectAs("author", db.selectFrom("users").select(["id", "username", "avatarUrl"]));

export const Posts = querySet(db)
	.selectAs("posts", db.selectFrom("posts")
		.select(["id", "title", "body", "userId", "categoryId", "createdAt"]))
	.relateOneOrThrow("category", () => Categories, "category.id", "posts.categoryId")
	.relateOne("author", UserPreviews, "author.id", "posts.userId")
	.relateMany("recentComments", () => Comments.orderBy("createdAt", "desc"),
		"recentComments.postId", "posts.id", { defaultStrategy: "attach" });

export const Users = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.extras({ displayName: (u) => `${u.username} <${u.email}>` })
	.relateOne("profile", Profiles, "profile.userId", "user.id")
	.relateMany("posts", () => Posts, "posts.userId", "user.id")
	.relateLateralMany("latestPosts", ({ eb, qs }) =>
		qs(eb.selectFrom("posts").select(["id", "title", "createdAt"])
			.whereRef("posts.userId", "=", "user.id"))
			.orderBy("createdAt", "desc").limit(3))
	.relateMany("flags", {
		fetch: async (users) => (await flagsClient.getFlagsForUsers(users.map((u) => u.id)))
			.flags.map((f) => ({ userId: f.userId, name: f.name })),
		matchChild: "userId",
	});

// call site:
const page = await Users
	.include("posts", (posts) => posts.include("category", "attach"))
	.include("flags")
	.limit(10)
	.execute();
```

(`Profiles` and `Comments` are entity modules of the same shape.)

Walking through it: each `relate*` call declares one relation — a key, a
child (a pre-built query set, a zero-arg thunk like `() => Posts` for
circular-import safety, or an `({ eb, qs })` factory for laterals), and a
pair of order-fixed column refs (`childRef` first, then `parentRef`). Because
`"posts.userId"` / `"user.id"` carries everything both strategies need, the
`posts` relation can be materialized as a DB `LEFT JOIN` *or* as a derived
batched `WHERE userId IN (…)` query — the caller picks per include, with
`"join"` as the default unless the declaration says otherwise
(`recentComments` defaults to `"attach"`). `latestPosts` is a correlated
lateral (per-parent top-3), which is join-only; `flags` is a custom fetch
against an HTTP client, which is attach-only — in both cases the unavailable
strategy is a compile error at the include site. Declaring all of this costs
nothing: `Users.execute()` returns bare `{ id, username, email, displayName }`
rows and `Users.executeCount()` never sees a join. At the call site,
`include("posts", …)` materializes posts as a join, the nested
`posts.include("category", "attach")` materializes each post's category as a
second batched query, and `include("flags")` runs the custom fetch. The
result type is exactly `{ id; username; email; displayName; posts: Array<{ …;
category: { … } }>; flags: Array<{ … }> }` — `profile`, `latestPosts`, and
`recentComments` don't exist on it. `.limit(10)` returns exactly 10 users:
join includes participate in the existing two-layer row-explosion handling,
and attach includes never touch the SQL at all.

---

## 3. Full specification

### 3.1 Prerequisites

All spike-verified; land before or with the feature:

- **P1** — `TQuerySetWithAttach` must thread `T["Collections"]`
  (`src/query-set.ts:2425`, a one-word fix) + the three S8 regression
  fixtures. Without it, `include` is dead after any eager `attach*`
  (S3/S4/S8). See [§7](#7-related-fixes-discovered).
- **P2** — `execute()` routes through
  `this.#props.db.executeQuery(this.toQuery())`, making `#props.db` the
  execution authority on every path including `#toQuery()`'s fast paths
  (S10; behavior-preserving in the same-handle case, 538/538 tests).
- **P3** — cross-type collection overwrite must delete from *both* hydrator
  maps (the pre-existing stale-attach-overwrites-join bug), with a regression
  test. **Extended (map design, M3): the cleanup must strip both key forms —
  real and `$graft$$`-mangled — from both hydrator maps and both props maps
  before re-registering.** The extension also fixes a pre-existing latent bug
  on never-mapped chains: a pre-map cross-kind overwrite
  (`attachMany("x", …).leftJoinMany("x", …)`) left the stale attach fetchFn
  registered and running (spy-asserted on main) — release-note item. See
  [§7](#7-related-fixes-discovered).
- **P4** — count/exists exclusion of non-filtering one-joins, scoped by flag
  to the count/exists path only (S9; the paginated inner layer keeps them).
  See [§7](#7-related-fixes-discovered).
- **P5** — packaging (map design, M2): the internal transform aliases that
  appear in exported entity-module types — `TMapped`, `InitialJoinedQuery`,
  `TWithOutput`, `TWithExtendedOutput`, `TWithOmit`, `TQuerySetWithRelation`,
  plus the `TMappedQuerySetWith*` family — must be exported (or
  d.ts-rolled-up), or every consumer entity module compiled under
  `--declaration` fails TS4023. Exporting user entity *classes* is advisory
  only (nameability; the emitter synthesizes a local `declare class`
  otherwise — M6).

P1 and P3 are additionally **hard prerequisites of the map × relations
design** ([map-relations.md](./map-relations.md)), not merely of relations.

### 3.2 Standing type-authoring rules

Spike-derived, normative for implementation:

1. **Never intersect a concrete bag with a bag-constraint interface whose
   fields include `any`** (`X & any = any` silently poisons outputs). S2/S3/S6.
2. **Printer rule**: any *computed* field stored in a bag entry must be
   forced to its evaluated form via an inline intersection with its supertype
   (`Foo<…> & Supertype`), never left as a named-alias application whose
   arguments embed a query-set bag — the declaration printer emits alias
   applications unevaluated (TS7056 at menu depth 6 otherwise). Checkable in
   CI by grepping a fixture package's emitted d.ts for the alias names. S6.
3. **The `_generics` phantom on `MappedQuerySet` is load-bearing** for every
   inference position (`NestedQuerySetOrFactory`, thunks, `InferRelations`);
   removing it silently degrades all `TNested` inference to the constraint
   with no compile error. Exact-type fixtures must assert through it. S2.
4. **Degenerate-context guards**: any type-level key computation over bag
   fields must handle the bare-index-signature case (`string extends keyof …`)
   explicitly. S3/S6.
5. **`NoInfer` stays in parameter positions only** — it survives into emitted
   declarations and doubles as an emission payload in stored types. S6.
6. **Per-member union checks inside a conditional whose outer check is a
   keyof-indexed `[Cs] extends [keyof …]` must be hoisted into a named
   distributed alias** — the inline form compiles but evaluates
   non-distributively, silently reopening the hole it guards (evaluation
   correctness, not style). M4.
7. **The shared tier's (`ExecutableQuerySet`) method returns must be
   conditional-free in `IsMapped`** — `MaybeMappedQuerySet` is allowed in
   dispatch positions only (stored child bags, callback parameters, keyed
   `modify` params), which resolve against concrete discriminants; the
   single gated exception is `$narrowType`
   ([map-relations.md](./map-relations.md) §3.4). M1.

### 3.3 Vocabulary

| Concept | Name |
|---|---|
| Declare | `relateOne` / `relateOneOrThrow` / `relateMany`; `relateLateralOne` / `relateLateralOneOrThrow` / `relateLateralMany` |
| Materialize | `include(key)`, `include(key, strategy)`, `include(key, cb)`, `include(key, strategy, cb)` |
| Strategy values | `"join" \| "attach"` (positional literal; type `RelationStrategy`) |
| Declaration-site default | `options: { defaultStrategy?: … }` (default `"join"`; must be a literal or a union that collapses to `"attach"`, [§3.4](#34-declaration-api)) |
| Relation entry fields | `Prototype`, `Mode`, `Strategies`, `Default`, `ChildColumns`, `Value` |
| Collection kinds after include | `Join` (existing), `AttachedQuery` (new — query-backed attach), `Attach` (existing — fetch-backed) |
| Errors | `UnknownRelationError`, `StrategyUnavailableError`, `InvalidRelationReferenceError`, `RelationMatchColumnMissingError`, `UnsupportedChildPaginationError`, `RelationAlreadyIncludedError`; from the map design: `GraftTargetError`, `GraftCollisionError`, `SharedMapOutputError`, `RelationKeyConsumedError` (two message branches), `RelationMatchIntegrityError`, `ReservedColumnNameError`, `RowShaperAfterMapError`. Demangling rule: every user-facing key-reporting surface reports real, demangled keys ([map-relations.md](./map-relations.md) §3.5) |
| Debug | `relations()` → declared keys; `includedRelations()` → tree of `{ key, strategy, children }` — both report real (demangled) keys at every nesting level |
| Type helpers | `InferRelations<T>` (= `keyof T["Relations"]`), existing `InferOutput<T>` |

Naming rationale (all confirmed under review): `relate*` over `has*` (the
hydrator already owns `has*` for prefixed collections); a positional strategy
literal over a `{ via }` options object (terser at the use site, and the
return type is minted at the call — see the no-`.via()` decision in
[§4](#4-design-decisions)); `"join"`/`"attach"` because they are the
library's own mechanism words, already documented as such.

### 3.4 Declaration API

Three forms per mode-verb, plus lateral verbs. Declaring is inert: no SQL, no
fetch, no output-type change, no hydrator touch (spike-asserted with `Equal`
on `HydratedOutput`/`Collections`/`OrderableColumns`).

**Reference types** (order-fixed, template-literal, checked against the
*base* outputs; comparability-filtered parent side per S11):

```ts
type RelationStrategy = "join" | "attach";
type RelationMode = "One" | "OneOrThrow" | "Many";

/** JS Map–safe key types for app-level matching (SameValueZero). */
type MapSafeKey = string | number | bigint | boolean | null | undefined;

type RelationRef<Alias extends string, O> = `${Alias}.${keyof O & string}`;

/** Single ref or a composite tuple (composite is core; arity ≤ 5, §3.7). */
type ChildRefArg<Key extends string, ChildO> =
	| RelationRef<Key, ChildO>
	| readonly [RelationRef<Key, ChildO>, ...RelationRef<Key, ChildO>[]];

type ColumnOfRef<R> = R extends `${string}.${infer C}` ? C : never;

type ChildColumnsOf<ChildRefs> = ChildRefs extends readonly (infer R extends string)[]
	? (R extends `${string}.${infer C}` ? C : never)
	: (ChildRefs extends `${string}.${infer C}` ? C : never);

/** S11 (adopted): bidirectional overlap on NonNullable — the naive
 * one-directional check has both a false-negative class (string child vs
 * literal-union parent) and a false-positive class (number|null vs
 * string|null overlapping only on null). Validated in
 * spikes-tmp/decl-types/. */
type Overlaps<A, B> = [Extract<A, B>] extends [never]
	? [Extract<B, A>] extends [never] ? false : true
	: true;

/** Parent refs are filtered to columns comparable with the child column —
 * rejection AND autocomplete narrowing from one construct. */
type ComparableParentRef<BaseAlias extends string, ParentO, ChildValue> = {
	[K in keyof ParentO & string]:
		Overlaps<NonNullable<ParentO[K]>, NonNullable<ChildValue>> extends true
			? `${BaseAlias}.${K}` : never;
}[keyof ParentO & string];

/** Parent refs arity-match the child refs; each position is
 * comparability-filtered against its child column's value type. */
type ParentRefArg<ChildRefs, BaseAlias extends string, ParentO, ChildO> =
	ChildRefs extends readonly unknown[]
		? { readonly [I in keyof ChildRefs]:
				ComparableParentRef<BaseAlias, ParentO, ChildO[ColumnOfRef<ChildRefs[I]> & keyof ChildO]> }
		: ComparableParentRef<BaseAlias, ParentO, ChildO[ColumnOfRef<ChildRefs> & keyof ChildO]>;
```

Order-fixed refs are deliberate (S1-verified: swapped-order refs produce
exactly one clean error on the childRef argument; refs check against the
child's *base output*, not the table). The runtime still classifies by alias
prefix and throws `InvalidRelationReferenceError` for same-side refs,
unclassifiable refs, `key === baseAlias`, or composite arity > 5.

**Attach availability, computed at declaration** (S1-verified, with an
`IsAny` guard so an `any`-typed match column never silently qualifies for
attach; strengthened per the map design (M4) with the hydrated-vs-raw
overlap conjunct — a child whose map re-types a match column must not offer
attach):

```ts
type IsAny<T> = 0 extends 1 & T ? true : false;

/** M4 conjunct: hydrated (post-map) vs raw match-column value overlap. */
type HydratedRawOverlapOk<C extends string, TNested extends TSelectQuerySet> = Overlaps<
	NonNullable<TNested["HydratedOutput"][C & keyof TNested["HydratedOutput"]]>,
	NonNullable<TNested["BaseQuery"]["O"][C & keyof TNested["BaseQuery"]["O"]]>
>;

/** Single-column: presence AND a KNOWN Map-safe value type AND hydrated/raw
 * overlap — in this order: presence → IsAny → MapSafeKey → overlap (M4). */
type SingleAttachOk<C extends string, TNested extends TSelectQuerySet> =
	C extends keyof TNested["HydratedOutput"] & string
		? IsAny<TNested["HydratedOutput"][C]> extends true ? never
		: TNested["HydratedOutput"][C] extends MapSafeKey
			? HydratedRawOverlapOk<C, TNested> extends true ? "attach" : never
			: never
		: never;

/** REQUIRED: per-member distribution hoisted into a NAMED alias (standing
 * rule 6) — the inline form compiles and silently reopens the hole (M4). */
type EachHydratedRawOverlapOk<Cs extends string, TNested extends TSelectQuerySet> =
	Cs extends unknown ? HydratedRawOverlapOk<Cs, TNested> : never;

/** Composite: presence + per-member hydrated/raw overlap (composite keys are
 * JSON-encoded; per-member value compatibility is enforced at declaration by
 * ComparableParentRef and at include time by DeclaredMatchShape, §3.5). */
type CompositeAttachOk<Cs extends string, TNested extends TSelectQuerySet> =
	[Cs] extends [keyof TNested["HydratedOutput"] & string]
		? false extends EachHydratedRawOverlapOk<Cs, TNested> ? never : "attach"
		: never;

type FormAStrategies<ChildRefs, TNested extends TSelectQuerySet> =
	| "join"
	| (ChildRefs extends readonly unknown[]
		? CompositeAttachOk<ChildColumnsOf<ChildRefs>, TNested>
		: SingleAttachOk<ChildColumnsOf<ChildRefs> & string, TNested>);

/** Collapse a widened/union default to "attach" — the capability LOWER
 * bound (a union D is only accepted when "attach" is available, so the
 * collapse never fabricates a strategy; the attach shape under-claims
 * capabilities, never over-claims). Keep literals. */
type NormalizeDefault<D extends RelationStrategy> =
	RelationStrategy extends D ? "attach" : D;

/** Kysely-style branded errors, with generic-context escape hatches. */
type ForbidRelationKey<T extends TQuerySet, Key extends string> =
	string extends T["BaseAlias"]
		? unknown                             // generic context: no precision, stay callable
		: Key extends T["BaseAlias"]
			? TypeErrorMessage<`Relation key "${Key}" collides with the base alias`>
			: string extends keyof T["Collections"]
				? unknown
				: Key extends keyof T["Collections"] & string
					? TypeErrorMessage<`"${Key}" is already a collection on this query set`>
					: unknown;
```

The overlap conjunct closes this design's own pre-existing hole for
*terminally-mapped declared children* (a child whose `.map()` re-types the
match column previously kept attach and produced silently-empty
collections). M4-validated: no false positives — nullable widening, literal
narrowing, non-match-column re-typings, and branded-same-representation
columns all keep attach; unmapped children are byte-unaffected (S1/S11/S7
fixtures re-run identical), so the amendment is safe to land ahead of the
map feature. Failing children compute `Strategies: "join"` — the fixture-(c)
error surface. The honest residual — same-type value transforms (`id + 1`)
— is backstopped at runtime by the zero-match integrity check
([§3.7](#37-runtime-design) step 7b).

**Signatures** (`Many` variant; `One`/`OneOrThrow` identical but for `Mode`).
Form A embeds three spike repairs verbatim: the S1 `defaultStrategy`
constraint shape, the S6 printer-rule intersection on `Strategies`, and the
S11 comparable parent refs.

```ts
// Tier hierarchy (amended per the map design, M1): ExecutableQuerySet<in out T>
// is today's MappedQuerySet body minus the seven MaybeMappedQuerySet-returning
// members ($castTo/$narrowType/$assertType/insert/update/delete/write),
// which are redeclared per tier with tier-exact returns;
// MappedQuerySet<in out T> extends ExecutableQuerySet<T>;
// QuerySet<in out T> extends ExecutableQuerySet<T> — siblings.
interface QuerySet<in out T extends TQuerySet> extends ExecutableQuerySet<T> {
	// ── Form A: positional column pair(s) → dual strategy when derivable ──
	relateMany<
		Key extends string,
		TNested extends TSelectQuerySet,
		const ChildRefs extends ChildRefArg<Key, NoInfer<TNested>["BaseQuery"]["O"]>,
		// S1 repair: constrain D itself; the parameter is bare `D`. Yields
		// `Type '"attach"' is not assignable to type '"join"'` on the failure
		// case instead of `Type 'string' is not assignable to type 'undefined'`.
		const D extends FormAStrategies<ChildRefs, NoInfer<TNested>> = "join",
	>(
		key: Key & ForbidRelationKey<T, Key>,
		child: NestedQuerySetThunkOrFactory<T, Key, TNested>,      // value | thunk | factory (§3.4.1)
		childRef: ChildRefs,                                       // "posts.userId" | ["items.orderId","items.region"]
		parentRef: ParentRefArg<ChildRefs, T["BaseAlias"], T["BaseQuery"]["O"],
			NoInfer<TNested>["BaseQuery"]["O"]>,
		options?: { defaultStrategy?: D },
	): QuerySetWithRelation<T, Key, {
		Prototype: "Query";
		Mode: "Many";
		// S6 printer rule: the inline `& RelationStrategy` forces the printer
		// to emit the EVALUATED union ("join" | "attach"), not the alias
		// application embedding the child bag. Without it, --declaration
		// TS7056s at menu depth 6. Must stay inline (a named alias wrapping
		// the same intersection reintroduces the failure).
		Strategies: FormAStrategies<ChildRefs, NoInfer<TNested>> & RelationStrategy;
		Default: NormalizeDefault<D & RelationStrategy>;
		ChildColumns: ChildColumnsOf<ChildRefs>;
		Value: TNested;
	}>;

	// ── Form B: arbitrary ON clause → join-only (no derivable fetch) ──
	relateMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key & ForbidRelationKey<T, Key>,
		child: NestedQuerySetThunkOrFactory<T, Key, TNested>,
		on: JoinCallbackExpression<T, Key, NoInfer<TNested>>,
	): QuerySetWithRelation<T, Key, {
		Prototype: "Query"; Mode: "Many";
		Strategies: "join"; Default: "join";
		ChildColumns: never; Value: TNested;
	}>;

	// ── Form C: custom fetch (cache, HTTP, other DB) → attach-only ──
	relateMany<Key extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: Key & ForbidRelationKey<T, Key>,
		declaration: {
			fetch: ToFetchFn<T, FetchFnReturn>;
			matchChild: ToAttachedKeysArg<T, NoInfer<FetchFnReturn>>["matchChild"];
			toParent?: ToAttachedKeysArg<T, NoInfer<FetchFnReturn>>["toParent"]; // defaults to keyBy
		},
	): QuerySetWithRelation<T, Key, {
		Prototype: "Fetch"; Mode: "Many";
		Strategies: "attach"; Default: "attach";
		ChildColumns: never; Value: FetchFnReturn;
	}>;

	// ── Laterals: join-only; the factory is the correlation channel ──
	relateLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key & ForbidRelationKey<T, Key>,
		child: JoinBuilderCallback<T, Key, TNested>,                 // ({eb, qs}) => …
		on?: JoinCallbackExpression<T, Key, NoInfer<TNested>>,       // default (join) => join.onTrue()
	): QuerySetWithRelation<T, Key, {
		Prototype: "Query"; Mode: "Many";
		Strategies: "join"; Default: "join";
		ChildColumns: never; Value: TNested;
	}>;
}
```

Notes:

- **Comparability is core** (S11 adopted): `parentRef` positions autocomplete
  only comparable columns; `"posts.userId"` ↔ `"user.email"` is rejected at
  declaration (a naive design compiled it *and offered attach*). Composite
  mismatches error per position. Residual: when no parent column is
  comparable the parameter displays as the `ComparableParentRef<…>` alias — a
  `TypeErrorMessage` fallback arm is the known escalation if this proves
  opaque ([§6](#6-remaining-risks--implementation-phase-tests) risk 11).
- **Non-literal `defaultStrategy`** yields `Default: "attach"` (the safe
  lower bound), never `"join"`. `{}` options fall back to the `= "join"`
  type-param default. All seven S1 default fixtures apply; the flip is
  direction-only.
- **Cardinality/optionality**: the verb (`One` → `T | null`, `OneOrThrow` →
  `T` throw-at-hydration, `Many` → `T[]`). Relations remain structurally
  non-filtering; filtering joins stay the eager SQL-shaping vocabulary.
- **Composite refs must be inline tuples or `as const` variables** — a plain
  `string[]` variable is a compile error with good text (S7; document as the
  #1 composite-form FAQ).
- **Composite arity is capped at 5** (kysely's `refTuple`/`tuple` overloads;
  [§3.7](#37-runtime-design)). Declaration throws
  `InvalidRelationReferenceError` for wider tuples; documented as a
  limitation (wider composite join keys are pathological).
- **Overwrite**: re-declaring a key overwrites the *declaration*; declaring a
  key that is already a materialized collection is a compile error
  (`ForbidRelationKey`) and a runtime throw.
- **`relate*` additionally lives on the mapped tier** (`MappedQuerySet`, map
  design): declaration is order-free relative to `map()` — declaring is
  inert and refs still check `BaseQuery["O"]`, which maps never touch; full
  `ChildRefs` precision is retained (M1).
- **Form C inference caveat** (S2 incidental): an *unannotated*
  context-sensitive `fetch` arrow can fail to drive `FetchFnReturn` inference
  before the `NoInfer`'d `matchChild` is checked. Docs recommend annotating
  the fetch parameter; the implementation should attempt a `matchChild` guard
  rework first ([§6](#6-remaining-risks--implementation-phase-tests) risk 5).

#### 3.4.1 Inline vs composed children, and the thunk form

`child` accepts three shapes **in a single parameter union** — never split
into same-arity overloads (a thunk-vs-factory overload split demonstrably
breaks contextual typing of unannotated destructured factories, TS7031;
overloads distinguished by *call* arity — Form A vs B vs C — remain safe).
S5-validated:

```ts
// All three union members accept the shared tier (amended per the map
// design, M1): ExecutableQuerySet — NestedQuerySetFn's return re-points too.
type NestedQuerySetThunk<TNested extends TSelectQuerySet> = () => ExecutableQuerySet<TNested>;

type NestedQuerySetThunkOrFactory<T extends TQuerySet, Alias extends string,
	TNested extends TSelectQuerySet> =
	| ExecutableQuerySet<TNested>                   // 1. pre-built query set (mapped children declarable)
	| NestedQuerySetThunk<TNested>                  // 2. zero-arg thunk — resolved & cached at first include
	| NestedQuerySetFn<T, Alias, TNested>;          // 3. ({eb, qs}) factory — resolved eagerly at declaration
```

- Form A factories get an `eb` re-typed against the child's own DB only (no
  parent pseudo-table) — the correlation channel is a lateral feature.
  Laterals keep the correlating `eb`.
- **Runtime classification** (S5-validated): non-function → value;
  `fn.length >= 1` → factory, resolved eagerly (parity with `#addJoin`);
  `fn.length === 0` → deferred, resolved and cached at first include —
  **invoked with the factory args object anyway**, so a rest/default-param
  factory degrades to late resolution instead of misbehaving
  (misclassification is impossible; only timing shifts). A deferred function
  returning a non-query-set throws a descriptive `TypeError` at include time.
- **TDZ**: eager values crash module evaluation on circular imports; thunks
  survive, including nested include through the cycle (S5 runtime demos).
- **Docs (normative)**: (a) the thunk form's safety requires `noImplicitAny`
  (with it, an unannotated type-level cycle is a loud TS7022/TS7024 at the
  declaration; without it, silent `any`); (b) thunks must be zero-param —
  `(_) => qs` classifies as a factory and resolves eagerly (loud TDZ at the
  declaration line if cyclic); (c) **"always thunk in entity modules"** is
  the headline idiom; (d) slim snapshots remain the back-reference idiom for
  type-level cycles *and* the emission-depth valve (S6: emitted d.ts grows
  ~2.5×/level; keep full-fat published menu chains ≲ 6 deep); (e) the
  annotated escape hatch (`(): QuerySet<HandWrittenBag> => qs` — or
  `(): MappedQuerySet<HandWrittenBag>` for mapped children (M1) — with
  mutually recursive bag interfaces) works for true cycles, at
  invariance-exactness maintenance cost — one paragraph, not the mainline.

### 3.5 Inclusion API

```ts
qs.include(key)                    // declared default strategy
qs.include(key, strategy)          // explicit strategy, per-key constrained
qs.include(key, callback)          // default strategy + modification / nesting
qs.include(key, strategy, callback)
```

The callback receives the child query set — modification and nesting are one
mechanism (S3-verified: contextual typing with no annotation, 3-level
nesting, exact output types):

```ts
usersQs
	.include("posts", (posts) =>
		posts
			.where("isPublished", "=", true)
			.orderBy("createdAt", "desc")
			.include("category", "attach"),
	)
	.execute();
```

**Key-domain types** (S4 message fix + S3 degenerate guard):

```ts
type RelationKeysOf<T extends TQuerySet> = keyof T["Relations"] & string;

/** One-shot inclusion. The keyof is INLINED (not via RelationKeysOf) so the
 * common typo error prints '"posts" | "flags"', not an unexpanded alias over
 * the whole bag (S4). The degenerate-case guard keeps include callable in
 * generic / degraded contexts — modify parity (S3 §6); provably inert for
 * concrete bags. */
type IncludableKeys<T extends TQuerySet> =
	string extends keyof T["Relations"]
		? string
		: Exclude<keyof T["Relations"] & string, keyof T["Collections"] & string>;

type QueryRelationKeys<T extends TQuerySet> = {
	[K in IncludableKeys<T>]: T["Relations"][K]["Prototype"] extends "Query" ? K : never;
}[IncludableKeys<T>];

/** Extract-plumbing: never index the raw TRelation union (S2-verified in
 * concrete AND generic contexts). */
type QueryRelOf<T extends TQuerySet, K extends string> =
	Extract<T["Relations"][K], { Prototype: "Query" }>;

type JoinDefaultKeys<T extends TQuerySet> = {
	[K in QueryRelationKeys<T>]: QueryRelOf<T, K>["Default"] extends "join" ? K : never;
}[QueryRelationKeys<T>];
type AttachDefaultKeys<T extends TQuerySet> = Exclude<QueryRelationKeys<T>, JoinDefaultKeys<T>>;
```

**The attach match-column constraint, value-typed.** The declared
match-column *types* are recovered from the stored child bag (`Value`) — no
new entry field, no emission cost:

```ts
/** The declared child's hydrated match-column shape, nullable-widened.
 * A modified/replacing child must keep each match column with a value type
 * assignable to the DECLARED type (| null | undefined): `String(id)`,
 * `new Date(…)`, and `BigInt(id)` re-typings are rejected for single AND
 * composite keys (a retyped composite member breaks JSON-encoding equality
 * too). Only same-type value transforms (`userId: p.userId + 1`) remain
 * uncatchable — the honest residual, backstopped by the runtime nil guard
 * only for drops, and documented. */
type DeclaredMatchShape<T extends TQuerySet, Key extends string> = {
	[C in QueryRelOf<T, Key>["ChildColumns"]]:
		| QueryRelOf<T, Key>["Value"]["HydratedOutput"][C]
		| null | undefined;
};

/** Guarded against malformed entries: ChildColumns=never with attach
 * available must fail loudly, not vacuously accept. */
type KeepsMatchColumn<T extends TQuerySet, Key extends string> =
	[QueryRelOf<T, Key>["ChildColumns"]] extends [never]
		? never
		: TSelectQuerySet & {
				BaseQuery: { O: Record<QueryRelOf<T, Key>["ChildColumns"], any> };
				HydratedOutput: DeclaredMatchShape<T, Key>;
			};
```

(`KeepsMatchColumn`'s intersection with `TSelectQuerySet` is safe — it is
used only as a *constraint*, never property-accessed; standing rule 1.)

**Include signatures.** Forms 1/2 distribute over `Key` (fixes the union-key
output over-claim and fetch-key mis-routing found by S3); form 2 admits fetch
keys; form 4's conditional constrains whenever attach is *possible* — a
union strategy must not fall to the permissive arm:

```ts
// Tier hierarchy per the map design (M1): QuerySet<in out T> extends
// ExecutableQuerySet<T>, sibling of MappedQuerySet<T> — see the §3.4 header.
// include() also lives on MappedQuerySet (graft-mode, key branded, returning
// via MappedIncludeReturn — map-relations.md §3.4).
interface QuerySet<in out T extends TQuerySet> extends ExecutableQuerySet<T> {
	/** 1. Default strategy, unmodified child (query- or fetch-backed).
	 * Distributes over Key: literal keys (the 99% case) cost nothing; union
	 * keys yield a union of per-key query sets — the honest lower bound —
	 * and fetch members route through their own Prototype branch. */
	include<Key extends IncludableKeys<T>>(
		key: Key,
	): Key extends unknown
		? IncludeReturn<T, Key, T["Relations"][Key]["Default"], RelationChild<T, Key>>
		: never;

	/** 2. Explicit strategy — constrained to the computed availability.
	 * Admits fetch keys (their Strategies is "attach"), so
	 * include("flags", "attach") is legal and greppable. */
	include<Key extends IncludableKeys<T>, Strategy extends T["Relations"][Key]["Strategies"]>(
		key: Key,
		strategy: Strategy,
	): Key extends unknown
		? IncludeReturn<T, Key, Strategy, RelationChild<T, Key>>
		: never;

	/** 3a. Callback on a join-default relation: unconstrained child modification. */
	include<Key extends JoinDefaultKeys<T>, TNestedNew extends TSelectQuerySet>(
		key: Key,
		modify: (child: QuerySetFor<QueryRelOf<T, Key>["Value"]>) => ExecutableQuerySet<TNestedNew>,
	): IncludeReturn<T, Key, "join", TNestedNew>;

	/** 3b. Callback on an attach-default relation: must keep the match
	 * column(s), value-typed against the declared shape. */
	include<Key extends AttachDefaultKeys<T>, TNestedNew extends KeepsMatchColumn<T, Key>>(
		key: Key,
		modify: (child: QuerySetFor<QueryRelOf<T, Key>["Value"]>) => ExecutableQuerySet<TNestedNew>,
	): IncludeReturn<T, Key, "attach", TNestedNew>;

	/** 4. Strategy + callback. Polarity: constrain whenever attach is
	 * POSSIBLE — a union strategy must not fall to the permissive arm. */
	include<
		Key extends QueryRelationKeys<T>,
		Strategy extends QueryRelOf<T, Key>["Strategies"],
		TNestedNew extends [Strategy] extends ["join"]
			? TSelectQuerySet
			: KeepsMatchColumn<T, Key>,
	>(
		key: Key,
		strategy: Strategy,
		modify: (child: QuerySetFor<QueryRelOf<T, Key>["Value"]>) => ExecutableQuerySet<TNestedNew>,
	): IncludeReturn<T, Key, Strategy, TNestedNew>;
}
```

Decisions embedded:

- **No callback overload for fetch relations** — post-include
  `modify(key, valueCb)` is their modification point. The resulting
  three-dialect `modify` matrix is specified in [§3.8](#38-semantics) item 5,
  not glossed.
- **Re-inclusion is a compile error + runtime throw** (S4-verified with
  excellent error text — the remaining menu is printed). Strategy re-swap =
  branch the chain before the include.
- **Include-time modification runs before attach derivation**: a callback
  `.where()` narrows identically under both strategies.
- **Union strategies**: a non-literal strategy yields the **union of the
  join and attach shapes** — there is no non-distributive bracket in forms
  1/2, and none is needed: method calls on the union require validity for
  both members (the capability lower bound in behavior) and both arms'
  `HydratedOutput` are byte-identical, so execution types stay exact
  (S3-measured). Union-strategy calls *with a callback* fall to the
  `KeepsMatchColumn` arm under form 4's polarity — the constraint applies
  whenever attach is possible. The documented idiom for dynamic strategy is
  statement-level branching, with a worked config-flag example in the docs
  (both branches execute cleanly since outputs are identical).
- **Union keys**: distribution makes the return a union of per-key shapes;
  accessing an included property requires narrowing. Residual (disclosed):
  a union key + explicit strategy can admit a strategy illegal for one
  member (`StrategyUnavailableError` runtime backstop); a union key +
  callback fails overload resolution even when each member is individually
  valid (fixture pins the error).
- **Generic contexts**: with the degenerate guards, `relate*`/`include` are
  callable at keyed-`modify` parity (any string; capability-floor returns).
  Key-generic wrappers (`K extends IncludableKeys<T>`) regain full precision
  at concrete call sites (S2/S6-verified — the `paginate` pattern). The 3b/4
  attach arms remain uncallable in fully generic contexts
  (`Record<string, any>` demand) — honesty-ledger item, fixture (l).

### 3.6 Type-level design

**Bag change** — one new field; every existing transformer gains
`Relations: T["Relations"]`; `InitialQuerySet` sets `Relations: {}`. Each
transform that threads the new field gets an S8-pattern regression fixture
(*declare → transform via an unrelated builder → consume with full typing*) —
the `TQuerySetWithAttach` bug proved a bare index signature satisfies every
constraint while silently destroying key precision (S8 implication 4).

```ts
interface TQuerySet {
	DB: any; IsMapped: boolean; BaseAlias: string; BaseQuery: TQuery;
	Collections: TCollections;
	Relations: TRelations;          // NEW — the declared menu
	JoinedQuery: TQuery; OrderableColumns: string; HydratedOutput: any;
	OmittedKeys: PropertyKey;
}
```

**Relation entries** — non-generic structural base, anonymous literal entries
(S2-verified under `in out`, incl. 4-deep mutual recursion, no TS2589):

```ts
interface TRelationBase {
	Prototype: "Query" | "Fetch";
	Mode: RelationMode;
	Strategies: RelationStrategy;   // union of AVAILABLE strategies
	Default: RelationStrategy;
	ChildColumns: string;           // never for Form B / lateral / fetch
	Value: any;
}
type TRelations = { [k in string]: TRelationBase };
interface TQueryRelation extends TRelationBase { Prototype: "Query"; Value: TSelectQuerySet; }
interface TFetchRelation extends TRelationBase {
	Prototype: "Fetch"; Strategies: "attach"; Default: "attach"; ChildColumns: never;
	Value: SomeFetchFnReturn;
}
```

`Value` stores the child's full bag (needed by the include callback, both
strategy arms, and `DeclaredMatchShape`); nothing precomputed is stored;
computed entry fields obey the printer rule (standing rule 2).

**Declaration transform** — O(1), only `Relations` changes (S1/S2-verified
inert):

```ts
type TQuerySetWithRelation<T extends TQuerySet, Key extends string, Rel extends TRelationBase> =
	Flatten<{
		DB: T["DB"]; IsMapped: T["IsMapped"]; BaseAlias: T["BaseAlias"]; BaseQuery: T["BaseQuery"];
		Collections: T["Collections"];
		Relations: ExtendWith<T["Relations"], Key, Rel>;
		JoinedQuery: T["JoinedQuery"];
		OrderableColumns: T["OrderableColumns"];
		HydratedOutput: T["HydratedOutput"];
		OmittedKeys: T["OmittedKeys"];
	}>;

interface QuerySetWithRelation<
	in out T extends TQuerySet, in out Key extends string, in out Rel extends TRelationBase,
> extends QuerySet<TQuerySetWithRelation<T, Key, Rel>> {}
```

**`RelationChild`** — a naive `Value & TSelectQuerySet` intersection
any-poisons `HydratedOutput`/`DB`/`JoinedQuery` outputs *and* collapses
nested-include `IncludableKeys` to `never` (found independently by S2, S3,
and S6):

```ts
/** NEVER `Value & TSelectQuerySet` (standing rule 1). The infer form
 * re-constrains without minting an intersection identity (S3-validated in
 * concrete and generic contexts). */
type RelationChild<T extends TQuerySet, K extends string> =
	QueryRelOf<T, K>["Value"] extends infer V extends TSelectQuerySet ? V : never;
```

**Include transform** — dispatched on stored discriminants (S3-verified:
exact types, all seven fixtures, no depth incidents). The attach arm
materializes a new **AttachedQuery** kind storing the child **bag** (join
convention) plus the match columns — this is what makes post-include
`modify` both typeable and runtime-implementable (see
[§4](#4-design-decisions), the AttachedQuery decision):

```ts
interface RelationJoinTypeMap { One: "LeftJoinOne"; OneOrThrow: "LeftJoinOneOrThrow"; Many: "LeftJoinMany"; }
interface RelationAttachedQueryTypeMap {
	One: "AttachedQueryOne"; OneOrThrow: "AttachedQueryOneOrThrow"; Many: "AttachedQueryMany";
}
interface RelationAttachTypeMap { One: "AttachOne"; OneOrThrow: "AttachOneOrThrow"; Many: "AttachMany"; }
interface RelationHydratedMap<in out TNested extends TSelectQuerySet> {
	One: TOutput<TNested> | null; OneOrThrow: TOutput<TNested>; Many: TOutput<TNested>[];
}

/** Attach sibling of TQuerySetWithJoin. Element type = the child's own
 * output, so join/attach HydratedOutput entries are byte-identical
 * (S3-verified with Equal). Collections entry: Prototype "AttachedQuery",
 * Value = the child BAG, Columns = the declared match columns — this is
 * what makes post-include `modify` typeable AND runtime-implementable. */
type TQuerySetWithAttachedRelation<
	T extends TQuerySet, Key extends string, Mode extends RelationMode, TNested extends TSelectQuerySet,
> = Flatten<{
	DB: T["DB"]; IsMapped: T["IsMapped"]; BaseAlias: T["BaseAlias"]; BaseQuery: T["BaseQuery"];
	Collections: TCollectionsWith<T["Collections"], Key, {
		Prototype: "AttachedQuery";
		Type: RelationAttachedQueryTypeMap[Mode];
		Value: TNested;                                  // the BAG, not a wrapped interface
		Columns: /* the relation's ChildColumns literal union */ string;
	}>;
	Relations: T["Relations"];
	JoinedQuery: T["JoinedQuery"];             // attach never touches SQL
	OrderableColumns: T["OrderableColumns"];   // no key$$… — the enforced divergence
	HydratedOutput: ExtendWith<T["HydratedOutput"], Key, RelationHydratedMap<TNested>[Mode]>;
	OmittedKeys: T["OmittedKeys"];
}>;
interface QuerySetWithAttachedRelation<
	in out T extends TQuerySet, in out Key extends string,
	in out Mode extends RelationMode, in out TNested extends TSelectQuerySet,
> extends QuerySet<TQuerySetWithAttachedRelation<T, Key, Mode, TNested>> {}

interface IncludeReturnMap<
	in out T extends TQuerySet, in out Key extends string,
	in out Mode extends RelationMode, in out TNested extends TSelectQuerySet,
> {
	join: QuerySetWithJoin<T, Key, RelationJoinTypeMap[Mode], TNested>;
	attach: QuerySetWithAttachedRelation<T, Key, Mode, TNested>;
}

type IncludeReturn<
	T extends TQuerySet, Key extends string, Strategy extends RelationStrategy,
	TNested extends TSelectQuerySet,
> = T["Relations"][Key]["Prototype"] extends "Fetch"
	? QuerySetWithAttach<T, Key,
			RelationAttachTypeMap[T["Relations"][Key]["Mode"]],
			Extract<T["Relations"][Key], TFetchRelation>["Value"]>
	: IncludeReturnMap<T, Key, T["Relations"][Key]["Mode"], TNested>[Strategy];
```

**Mapped twins (map design).** On the mapped tier, `IncludeReturn` gains a
twin family: `MappedIncludeReturnMap`/`MappedIncludeReturn` — the same
dispatch with the wrappers swapped for
`MappedQuerySetWithJoin/WithAttach/WithAttachedRelation/WithRelation`, whose
`TMappedQuerySetWith*` transforms land collections as **inline**
intersections on `HydratedOutput` (printer-rule-safe by construction);
`TQuerySetWithAttachedRelation` gains its `TMapped*` twin. Definitions in
[map-relations.md](./map-relations.md) §3.4.

**Keyed `modify` gains a third arm** — required machinery, and the one part
of the include design that touches existing type machinery beyond threading:

- `CollectionModifier` dispatches on the flat `Prototype` discriminant:
  `"Join"` → query-set callback (existing); **`"AttachedQuery"` → query-set
  callback whose return is constrained by the entry's `Columns` against the
  entry's `Value["HydratedOutput"]` (the `KeepsMatchColumn` equivalent —
  this closes the back door that would otherwise let a post-include `modify`
  drop or re-type the match column)**; `"Attach"` → raw fetch-value callback
  (existing).
- `ModifyCollectionReturnMap` gains three entries
  (`AttachedQueryOne/OneOrThrow/Many` →
  `QuerySetWithAttachedRelation<T, Key, Mode, TNestedNew>`).
- Fetch-backed (`Attach*`) and query-backed (`AttachedQuery*`) attaches no
  longer share `Type` literals, so dispatch is principled.

**Instantiation-depth / emission ledger** (measured, not estimated):

1. Declaration ≈ 490 instantiations + ~36 for the availability gate + ~780
   for comparability (S1) — negligible at any plausible menu width. The
   10-entity × 4-relation cross-module workload checks at 437k
   instantiations, 1.2 s, 120 MB — ~19% of the repo's own suite; no TS2589
   anywhere including depth-6 nested includes and generic helpers (S6).
2. **Emission is the real cliff, and it is handled**: with the printer rule,
   `--declaration` passes with ~7× headroom at menu depth 7; growth is
   geometric (~2.5×/level) with the cliff at depth ~8–9 — the slim-snapshot
   idiom is the documented valve; apps that don't emit declarations have no
   cliff in sight. Call-site d.ts is **20–30× smaller** than eager
   composition — the quantified lazy-menu dividend, claimable in docs.
3. Consumers compiling against emitted d.ts retain full include/key/strategy
   precision (S6).
4. Generic contexts: modify-parity via the degenerate guards
   ([§3.5](#35-inclusion-api)); key-generic wrappers fully precise at
   concrete sites.
5. Hover: parity with today's join chains (named wrappers, expanded bag
   arguments); package boundaries: `InferOutput` + interface annotations.
6. Union-key/-strategy behavior: [§3.5](#35-inclusion-api)'s disclosures.

### 3.7 Runtime design

**Stored state:**

```ts
interface QueryRelationDecl {
	readonly kind: "query";
	readonly mode: CollectionMode;
	readonly child: QuerySetImpl | ((args?: FactoryArgs) => unknown); // deferred fns invoked WITH args (S5)
	readonly resolvedChild?: QuerySetImpl;               // first-include cache
	readonly joinMethod: "leftJoin" | "leftJoinLateral";
	readonly joinArgs: AnyJoinArgsTail;
	readonly columns: readonly { child: string; parent: string }[] | null; // null ⇒ attach unavailable
	readonly defaultStrategy: RelationStrategy;
}
interface FetchRelationDecl {
	readonly kind: "fetch"; readonly mode: CollectionMode;
	readonly fetchFn: SomeFetchFn<any, any>; readonly keys: AttachedKeysArg<any, any>;
}
interface QuerySetProps {
	/* …existing… */
	relations: Map<string, QueryRelationDecl | FetchRelationDecl>;
}
```

`relate*` parses refs by alias prefix (order-agnostic at runtime; throws
`InvalidRelationReferenceError` for same-side refs, unknown aliases,
`key === baseAlias`, composite arity > 5), zips composite tuples, resolves
`({eb, qs})` factories eagerly, stores length-0 functions unresolved, and
clones. It never touches `hydrator`/`joinCollections`/`attachCollections` —
declarations stay invisible to `toQuery()`, counts, `hydrate()`, and output
types.

**New collection kind**: query-backed attach includes materialize as

```ts
interface AttachedQueryCollection {
	readonly type: "attachedQuery";
	readonly mode: CollectionMode;
	readonly child: QuerySetImpl;        // rebound; modified by include-cb / modify(key, …)
	readonly columns: readonly ColumnPair[];
}
```

`#addCollection`'s attachedQuery case derives the fetchFn from
`(child, columns)` at snapshot time and registers it with the hydrator
exactly as an attach; `modify(key, cb)` on it runs `cb(child)`, validates,
and re-runs `#addCollection` (which re-derives and re-snapshots — the
existing immutability model). Honest accounting: this is **one new collection
sub-kind**, not "zero new execution paths"; everything else (SQL hoisting,
`$$` prefixing, two-layer pagination, WHERE-EXISTS classification, attach
batching, dedup, `hydrate()`) is inherited verbatim.

**`include(key, strategyOrCb?, maybeCb?)`:**

```ts
include(key, a?, b?) {
	const decl = this.#props.relations.get(key);
	if (!decl) throw new UnknownRelationError(key);
	// Checks BOTH key forms — real and $graft$$-mangled (map design): post-map
	// grafted relations must not fall through to #addCollection under another
	// name. RelationAlreadyIncludedError stays the include path's error;
	// RelationKeyConsumedError (two message branches) is the sugar/graft
	// path's — a deliberate, documented split.
	const mangled = mangleGraftKey(key);
	if (
		this.#props.joinCollections.has(key) || this.#props.attachCollections.has(key) ||
		this.#props.joinCollections.has(mangled) || this.#props.attachCollections.has(mangled)
	)
		throw new RelationAlreadyIncludedError(key);
	const [strategy, cb] = normalizeIncludeArgs(a, b, decl);

	if (decl.kind === "fetch") {
		return this.#addCollection(key, { type: "attach", mode: decl.mode, fetchFn: decl.fetchFn, keys: decl.keys });
	}

	// Rebind BEFORE the callback, under BOTH strategies, so nested includes
	// inside the callback capture the includer's handle transitively.
	// (Effective because of P2: #props.db is the execution authority.)
	const resolved = resolveChild(decl).#withDb(this.#props.db);
	const child = cb ? cb(resolved) : resolved;     // modification BEFORE derivation

	// The pagination guard reads BOTH layers — props limit/offset (offset
	// included) AND top-level limit/offset nodes of the child's baseQuery
	// operation node.
	const pag = childPagination(child);             // { props: bool, baseNode: bool }
	if (strategy === "join") {
		if (decl.joinMethod !== "leftJoinLateral" && (pag.props || pag.baseNode))
			throw new UnsupportedChildPaginationError(key, /* message branches by caller */);
		return this.#addCollection(key, {
			type: "join", method: decl.joinMethod, mode: decl.mode, querySet: child, args: decl.joinArgs,
		});
	}

	// strategy === "attach"
	if (!decl.columns) throw new StrategyUnavailableError(key, "attach");
	if (pag.baseNode)                               // cannot be stripped → per-chunk would leak
		throw new UnsupportedChildPaginationError(key, /* point at props-level .limit() */);
	return this.#addCollection(key, {
		type: "attachedQuery", mode: decl.mode, child, columns: decl.columns,
	});
}
```

**The derived fetch.** The pipeline below makes chunking result-invisible,
gives props-level child pagination true per-batch semantics, and reuses the
join strategy's ordering semantics by sorting *raw* child rows before
hydration. The hydrator is completely untouched by this design:

```ts
function deriveKeys(columns) {
	return columns.length === 1
		? { matchChild: columns[0].child, toParent: columns[0].parent }
		: { matchChild: columns.map((c) => c.child), toParent: columns.map((c) => c.parent) };
}

function deriveFetchFn(child /* already rebound (P2) */, columns, key) {
	return async (parents) => {
		// 1. Dedup + nil-filter (hydrator dedups parents by keyBy, not toParent;
		//    SQL NULL = NULL never matches).
		const values = columns.length === 1
			? uniqueNonNil(parents.map((p) => p[columns[0].parent]))
			: uniqueTuples(parents.map((p) => columns.map((c) => p[c.parent])).filter(allNonNil));
		// 2. Empty batch: no query at all — `IN ()` is invalid SQL.
		if (values.length === 0) return [];
		// 3. Props pagination is stripped from SQL and re-applied in JS (step 7)
		//    so limit/offset are truly PER BATCH at any chunk count.
		const { limit, offset } = child.props;
		const unpaginated = child.#withoutPagination();
		// 4. Chunking by PARAMETER BUDGET, not value count: a k-column
		//    composite consumes k parameters per value.
		const chunkSize = Math.max(1, Math.floor(PARAM_BUDGET / columns.length));
		const rawRows = (await Promise.all(
			chunk(values, chunkSize).map((vs) =>
				whereIn(unpaginated, columns, vs).toQuery().execute()),
		)).flat();
		// 5. Sort RAW rows with the child's finalOrderings comparator — the
		//    same data shape the join strategy sorts (raw rows BEFORE
		//    hydration), so omitted sort columns, `cat$$name` grandchild keys,
		//    mapFields/map transforms, and function keys all behave exactly as
		//    under join. orderByKeys() guarantees a deterministic tie-break,
		//    keeping entity row-groups contiguous.
		sortRawRows(rawRows, child.finalOrderings);
		// 6. First-batch guard for residues the types can't see. (Minted with
		//    the real, demangled relation key — map design's demangling rule.)
		if (rawRows.length > 0 && rawRows.every((r) => isNilKey(r, columns)))
			throw new RelationMatchColumnMissingError(key, columns);
		// 7. Hydrate ONCE over the whole batch (dedup by child keyBy across
		//    chunks; cardinality checks; nested collections — their derived
		//    fetches run here, on the rebound handle, once, not per chunk),
		//    then window ENTITIES: per-batch limit/offset must count child
		//    entities, not raw rows (children with nested many-joins explode
		//    raw rows; the child's own SQL pagination is entity-scoped too).
		const entities = unpaginated.hydrate(rawRows);
		// 7b. Zero-match integrity backstop (map design, M4) — BEFORE the
		//     window, so a legitimate per-batch offset is never misread as
		//     zero-match. If the batch is non-empty and zero hydrated children
		//     match any parent key, SQL IN-selection and SameValueZero matching
		//     disagree: throw RelationMatchIntegrityError naming the relation
		//     key and ENUMERATING the cause classes — a child map() value
		//     transform; collation-insensitive SQL equality (citext / COLLATE
		//     NOCASE); driver decode divergence. Applies to ALL query-backed
		//     attach relations, mapped or not — a release-noted loudness
		//     upgrade to contract exception 3's documented-silent residual.
		//     Non-firing arms (M4-proven): empty parent sets (the fetch is
		//     never constructed), empty batches, per-batch offset windows.
		throwIfZeroMatch(entities, parents, columns, key);
		return applyWindow(entities, limit, offset);
	};
}
```

- `whereIn` is `.where(col, "in", vs)` for single columns. Composites use the
  S7-validated form
  `.where((eb) => eb(eb.refTuple(c1, …), "in", vs.map((v) => eb.tuple(…))))`
  built through internal casts (the runtime column list is dynamic;
  `refTuple`/`tuple` are 2–5-ary typed overloads, hence the declared arity
  cap of 5). No `.in()` method exists on `ExpressionWrapper`.
- `#withDb(db)` is the S10 clone (identity short-circuit when
  `db === props.db`); nested join collections need no rebinding (compiled as
  subquery nodes of the outer statement); the child's own *user-written*
  fetchFns keep their captured handles — deliberate and documented.
  Cross-handle plugin caveat: compile via the declaration handle's executor,
  execute/transform via the new handle — coherent for transactions of the
  same root Kysely (regression-verified through the camel-case suite);
  rebinding across unrelated Kysely instances is unsupported; `#withDb`
  stays internal partly for this reason.
- Row-value `IN` targets Postgres and SQLite ≥ 3.15 (both test targets);
  other dialects: documented limitation, no dialect sniffing.

### 3.8 Semantics

1. **Pagination / row explosion.** Verified against `#toQuery`:
   declared-not-included relations are invisible everywhere; join includes
   are ordinary `leftJoin*` collections (many-cardinality excluded from the
   limited inner layer); attach includes never touch SQL — `.limit(10)` hits
   a bare parent SELECT and the batch sees exactly the page.
2. **`executeCount()` / `executeExists()` — unconditional invariant (S9,
   D4):** *`include` never changes count/exists values — or SQL.* The count
   query is built from the base plus **filtering** collections only;
   non-filtering one-joins are excluded on the count/exists path (flagged
   parameter — the paginated inner layer of `#toQuery` must keep them for
   hoisting/ordering). Dirty-data cardinality violations can no longer
   inflate counts or fork the strategies (fixture: 1 vs the former 4;
   `execute()` still throws `CardinalityViolationError` under both). Stated
   invariant note: this exclusion is sound because ON-clause type scope is
   {base alias, own alias} (`ToInitialJoinedTB = BaseAlias`,
   `src/query-set.ts:366`); if join callbacks are ever widened to see sibling
   collections, revisit. This is a behavior change to today's eager
   `leftJoinOne` counts (correctness-improving; release-notes item).
3. **`orderBy` on included one-relations.** Join grows `OrderableColumns`
   with `profile$$bio`-style entries; attach does not — the one deliberate,
   compile-enforced capability divergence (S2/S3/S6-verified both
   directions). Chain order matters (include before ordering by relation
   columns) — parity with joins today; documented with one example; not
   worth a design tweak.
4. **Child pagination.** *Non-lateral join strategy*: rejected at include
   time — both props-level and base-node-level, offset included
   (`UnsupportedChildPaginationError`; message branches: sugar callers →
   `leftJoinLateral*`, include callers → `relateLateral*` / `"attach"`).
   *Attach strategy*: props-level `limit`/`offset` are **per batch**,
   implemented in JS after the chunk-flatten + raw-row sort — exact at any
   batch width (chunking can no longer change results); base-node-level
   pagination is rejected (cannot be stripped; per-chunk semantics would
   silently return). *Per-parent top-N*: the lateral verbs, join-only in the
   types. The rejection also applies to the eager `leftJoin*` sugar (D1,
   author sign-off still required).
5. **`modify(key, …)`.** Post-include: three dialects, dispatched by
   collection kind — the normative matrix:

   | Collection kind | Callback receives | Runs | Constraint |
   |---|---|---|---|
   | `Join` (join-strategy include, eager joins) | child query set | once, at build time | none extra |
   | `AttachedQuery` (attach-strategy include of a query relation) | child query set | once, at build time (re-derives the fetch) | `Columns`-based match-column keep, value-typed |
   | `Attach` (fetch relations, `attach*` sugar) | the fetchFn's return value (possibly a promise) | per execution, at hydration time | `SomeFetchFnReturn` |
   | Post-map, `Join`/`AttachedQuery` (map design, M5) | child query set | once, at build time (re-derives on `AttachedQuery`) | `PreservesShape` — shape-preserving, subsuming the `Columns`-keep incl. composites; returns `this` |
   | Post-map, `Attach` (map design, M5) | the declared element value(s) | per execution, at hydration time | bare `FetchReturnOf<Element>` union (never intersected with `SomeFetchFnReturn`); returns `this` |

   *Include* has one callback meaning; `modify`'s dialects follow the
   collection kind, as they do today. Pre-include `modify` on a declared key:
   compile error; runtime message `"posts" is declared but not included —
   call .include("posts") first`. Include callback = reset-from-declaration;
   `modify` = compose-on-current — documented pair.
6. **`.map()` — superseded (D6).** `map()` is a stage boundary, not
   terminal; collections added after a map graft onto its output;
   row-shapers remain pre-map-only. `relate*`/`include` additionally live on
   the mapped tier; mapped *children* are declarable and includable as
   before. See the map design record
   ([map-relations.md](./map-relations.md)).
7. **`keyBy` / app-level matching.** `toParent` is the declared parent
   column; parents deduped by `keyBy`; nil keys never match under either
   strategy; both strategies throw `CardinalityViolationError` on >1 match
   for one-modes (the README's "first match" prose gets fixed, see
   [§7](#7-related-fixes-discovered)). Chunking cannot straddle a key across
   chunks (dedup ⇒ each key value lands in exactly one chunk; grouping is
   post-flatten).
8. **`hydrate(rows)`.** Reflects exactly the included set; the round-trip law
   holds per inclusion state; attach includes make `hydrate()` perform DB I/O
   on the handle captured at include time (rebound, hence the parent's).
9. **`toQuery()` family.** Join includes only; `toCountQuery()`/
   `toExistsQuery()` are include-independent (unconditional, S9). Debug:
   `relations()` (declared keys) and `includedRelations()` — a **tree** of
   `{ key, strategy, children }`, so "which endpoint runs N queries" is
   answerable at any nesting depth.
10. **Writes — expected, unverified; non-normative:** the
    `Relations`/`relations` props survive `.insert()`/`.update()`/`.write()`
    base swaps under the output-compatibility contract, and P2 makes the
    transaction story mechanically compatible — but the writes design is
    deferred (out of the brief's scope) and nothing here binds it.

**The strategy-equivalence contract (normative and scoped):**

> For a query-backed relation included by either strategy, **over a stable
> database snapshot**, swapping the strategy changes only the generated SQL
> and the number of round trips — never the parent set, the elements, the
> nesting, the nested order, the child-pagination window, or the errors
> thrown on cardinality violations.
>
> Scoped exceptions, all enforced or documented:
> 1. *Parent orderability* — only join exposes `rel$$col` ordering;
>    compile-enforced.
> 2. *Execution shape* — join is one statement; attach is 1+N statements on
>    the includer's handle (transitively rebound through nested includes).
>    Without a wrapping transaction, attach reads per-statement snapshots;
>    `hydrate()` performs I/O under attach. On single-connection drivers
>    (better-sqlite3) the rebinding guarantee is load-bearing: an unrebound
>    fetch inside a transaction *deadlocks* (S10-demonstrated), it doesn't
>    just read stale.
> 3. *Key equality* — SQL `=` vs SameValueZero. Declaration gates exclude
>    non-Map-safe and `any`-typed single-column keys; the include/modify
>    layers enforce the declared key types; residual divergences (numeric
>    strings vs numbers under DB coercion, collation-insensitive SQL equality
>    such as citext, same-type value transforms) are documented. Composite
>    caveat (map design): a `Date`-typed composite member matches at driver
>    millisecond precision under attach but full DB precision under join — a
>    match-set divergence (single-column `Date`/`Buffer` are already excluded
>    by `MapSafeKey`; BLOB composite members are byte-deterministic). The
>    zero-match integrity backstop (§3.7 step 7b) upgrades this exception's
>    residual from documented-silent to loud.
> 4. *Child pagination* — rejected under non-lateral join; per-batch (JS,
>    entity-level) under attach; per-parent = lateral (join-only). Base-node
>    pagination rejected under both.
> 5. *Scale* — derived `IN` lists are chunked by parameter budget; chunking
>    is invisible to results (ordering and windowing happen after the
>    flatten).
> 6. *Reference identity & invocation counts* (map design, M4). Child
>    element **reference identity** and child-map **invocation counts** are
>    strategy-dependent: attach hydrates the batch once and shares child
>    instances across matching parents (`applyGroupedCollectionMode` copies
>    the array, shares the elements — `src/hydrator.ts:1546`, `:1555`); join
>    hydrates per parent row-group, minting distinct instances per
>    (parent, child) pair (`:1236`). Guarantees are **value-level** (deep
>    equality), never `===`-level. Attach's per-batch windowing runs the
>    child pipeline on entities the window then discards. [M4 pinned this
>    with exact numbers: 3×Author/5×Award under join vs 2×Author/3×Award
>    under attach on the fixture graph, budget-independent.] This documents
>    **pre-existing** sharing, not new sharing (release note).
>
> **Purity clause (map design).** Map functions must be pure and
> deterministic per input row, must not return shared/memoized references
> within an execution (enforced for graft-receiving stages:
> `SharedMapOutputError`; backstopped by `GraftCollisionError`), and must
> return a fresh extensible object per row when collections graft onto the
> output (enforced: `GraftTargetError`/`GraftCollisionError`).
> Value-equivalence is conditional on this clause. Consumers must not mutate
> child instances (attach shares them) — this sentence is ALSO promoted into
> the README attach docs.
>
> **Mapped-child gating (map design).** Attach availability for a declared
> child additionally requires hydrated-vs-raw match-column value overlap
> (§3.4's `HydratedRawOverlapOk` conjunct, named-alias composite form), with
> the derived-fetch zero-match runtime backstop (§3.7 step 7b); same-type
> value transforms that evade the gate are caught by the backstop when
> total, and remain the documented residual when partial (M4-pinned). Form-C
> fetch relations get no backstop.

### 3.9 Fate of the existing API

| Existing | Fate | Rationale / seams (all decided) |
|---|---|---|
| `leftJoinOne/OneOrThrow/Many`, left laterals | **Kept as sugar**, reimplemented internally as build-decl-record + immediate materialization — **bypassing the relations map** (no `Relations` entry, no debug-output pollution, no `ForbidRelationKey` interference) | Behavior changes, stated honestly: (a) §3.8 item 4's pagination rejection (D1); (b) include-independent counts (S9, release note); (c) **D9 (recommended: yes): refs become order-fixed (childRef first)** — one convention library-wide, loud compile-error migration, and "rename the method" refactors then actually work. There is no "zero behavior change" claim. |
| `attachOne/OneOrThrow/Many` on `QuerySet` | **Kept as sugar** (fetch-backed `Attach` collections, order-preserving as today) | Positional-vs-options-object spelling difference vs Form C is deliberate (the object disambiguates from function-valued children) and documented; migration is not a rename. |
| `innerJoin*`, `crossJoin*` (incl. laterals) | **Kept, unchanged, eager-only** | Filtering joins are SQL-shaping, not relations — this is what keeps the equivalence contract and the unconditional count invariant provable. |
| Keyed `modify`, `where`, `orderBy`, pagination, counts, writes, `hydrate`, `toQuery` family, type helpers | Unchanged, except: `modify` gains the `AttachedQuery` arm (§3.8 item 5) and, post-map, the shape-preserving dialect (`PreservesShape`, §3.8 item 5; [map-relations.md](./map-relations.md) §3.4); `execute()` routes through `db.executeQuery` (P2); count/exists exclude non-filtering one-joins (P4) | |
| Hydrator API | **Unchanged — zero changes** (ordering authority lives in the derived fetch, §3.7). Footnote (map design, M3/M7): the hydration hot loop stays untouched, but two **additive** changes land with the map feature — `FullHydrator.withoutCollection()` and `defineProtoShadowedKey` exported `@internal` | Fetch-backed attaches keep their documented fetch-order contract verbatim. |

Prerequisite fixes P1 (type threading) and P3 (cross-type overwrite hydrator
staleness) land with the feature.

The docs reorient: `relate*`/`include` are the headline relational API; the
eager left/attach methods are one-call shorthands; filtering joins are the
"shape your SQL" tier — teachable in one sentence: *declare on entity
modules, include at call sites; use `leftJoin*`/`attach*` only for one-off
inline eager fetches; `innerJoin*`/`crossJoin*` shape the parent set.*

**README main example, rewritten** (with a pre-existing README bug fixed:
`posts` must select its FK columns for the declarations to typecheck — and
note the honest consequence: attach-capable relations keep their FK in the
child output; `.omit()` the FK and the relation is join-only, by type):

```ts
import { querySet } from "kysely-hydrate";

const categoriesQuerySet = querySet(db)
	.selectAs("category", db.selectFrom("categories").select(["id", "name"]))
	.extras({
		upperName: (row) => row.name.toUpperCase(),
	});

const postsQuerySet = querySet(db)
	.selectAs(
		"posts",
		db.selectFrom("posts").select((eb) => [
			"id",
			"title",
			"userId",
			"categoryId",
			// Embed whatever SQL you want:
			eb
				.selectFrom("comments")
				.select(eb.fn.countAll().as("count"))
				.whereRef("comments.postId", "=", "posts.id")
				.as("commentsCount"),
		]),
	)
	// Declare the relation once, with join columns. Declaring costs nothing:
	// no SQL, no fetch, no output change. Both strategies become available.
	.relateOneOrThrow("category", categoriesQuerySet, "category.id", "posts.categoryId");

// The canonical representation of a user: base query + a menu of relations.
const userQuerySet = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "email"]))
	.relateMany("posts", postsQuerySet, "posts.userId", "user.id");

// Count with deduplication — no relation is fetched, no join is generated.
const count = await userQuerySet.executeCount();

// Opt in per call site. Strategy is per-include: posts as a DB-level LEFT
// JOIN, category as an application-level batch (one extra query).
const users = await userQuerySet
	.include("posts", (posts) => posts.include("category", "attach"))
	.execute();
// ⬇ Result — exactly what was included, nothing optional:
type Result = Array<{
	id: number;
	email: string;
	posts: Array<{
		id: number;
		title: string;
		userId: number;
		categoryId: number;
		commentsCount: number;
		category: {
			id: number;
			name: string;
			upperName: string;
		};
	}>;
}>;

// Elsewhere, the same canonical query set, differently materialized:
const bareUsers = await userQuerySet.execute(); // Array<{ id: number; email: string }>
```

The old example's eight-line `modify("posts", (posts) =>
posts.attachOneOrThrow(…))` reach-through — fetch plumbing, `IN` list, and
match keys — collapses into a declaration plus two words at the call site.
The canonical entity-module example ([§2](#2-api-overview)) is the second
doc example, with the rule *"always thunk in entity modules."*

Coordination with the map × relations design: its spikes passed, so the
README examples may adopt the canonical-map flagship (entity module declares
base + relations + `.map((row) => new User(row))`,
[map-relations.md](./map-relations.md) §2) directly — there is no need to
hold map guidance across releases.

### 3.10 Naming, error classes & type-error fixtures

Naming per [§3.3](#33-vocabulary). Error-text discipline: CI fixtures assert
*actual compiler output* (S3/S4 captured the real multi-overload error text
for all of a–g); no invented compiler prose in docs. The fixture list:

- **(a)** typo'd key; **(b)** typo'd key + callback; **(c)** unavailable
  strategy — prints `'"attach"' is not assignable to … '"join"'`; **(d)**
  same + callback — single clean error; **(e1)** `include` after an
  object-producing `.map()` — a *positive* fixture (grafts; map design);
  **(e2)** `include` after a primitive or non-object map — negative, with
  the two branded message variants (primitive; non-object with the
  `| null`/`| undefined` hint) (M2); **(f)** `modify` on a
  declared-but-not-included key; **(g)** re-inclusion — prints the remaining
  menu. (All verified; (e1)/(e2) under the map design's spikes.)
- **(h)** `include(fetchKey, "attach")` — a *positive* fixture.
- **(i)** non-literal `defaultStrategy` → `Default: "attach"`; non-literal
  include strategy → union-of-shapes return; the statement-level branching
  idiom compiles.
- **(j)** match-column re-typing through the include callback
  (`String(userId)`, `Date`, `BigInt`) rejected by `DeclaredMatchShape`;
  same through post-include `modify` on an `AttachedQuery` collection.
- **(k)** `modify(key, qs => qs.limit(10))` on an attach-included query
  relation — *positive* fixture (the `AttachedQuery` machinery working).
- **(l)** `relate*`/`include` inside a generic helper: callable at
  modify-parity; key-generic wrapper regains precision.
- Union-key + callback overload failure pinned.
- Runtime-message tests: declared-not-included `modify`;
  `UnsupportedChildPaginationError` message branches.
- Per-transform `Relations`-threading regression fixtures (the S8 pattern:
  declare → transform via an unrelated builder → consume with full typing).

`TypeErrorMessage` branding: in use for `ForbidRelationKey` (renders
perfectly, S4 cases 8/9); held in reserve for `KeepsMatchColumn`'s
`HydratedOutput` position, fixture (a)'s alias-display case, and the
no-comparable-parent-column case — adopt any of them only if the raw text
proves too spooky in fixture review.

---

## 4. Design decisions

A record of every significant decision: what was chosen, the alternative
rejected, and the evidence. "Review" below means the adversarial design
review, whose findings were verified directly against the codebase
(file:line citations given where load-bearing); spike IDs are compile/run
evidence (legend in [§5](#5-validation)).

### The decision register (D-numbers)

| # | Decision | Status / evidence |
|---|---|---|
| **D1** | Child pagination rejected under non-lateral join — including the eager `leftJoin*` sugar; scope covers offset and base-node pagination. | Adopted; **author sign-off required** (it constrains existing sugar). Review verified `hasPagination` reads props only and base-node limits survive into the join subquery. |
| **D2** | Keep both eager sugars (`leftJoin*`, `attach*`) alongside `relate*`/`include`. | Confirmed under review; the three seams it creates are all decided (see D9 and the sugar decisions below). |
| **D3** | Attach-strategy nested ordering: JS raw-row sort *inside the derived fetch*, using the child's `finalOrderings` comparator — **no hydrator change, no existing-user behavior change** (every existing attach is fetch-backed, so the "query-backed attach" category was empty). | Adopted; verified against `src/hydrator.ts` (`#hydrateMany` vs `groupByKey`; `AttachedCollection` carries no orderings). Sign-off downgraded to FYI. |
| **D4** | Count/exists exclude non-filtering one-joins — making the count invariant unconditional. | **Adopted, spike-proven** (S9: count SQL strictEqual across include states; dirty-data inflation fixed, 1 vs the former 4). |
| **D5** | No public rebinding API; `#withDb` stays internal. Document the `querySet(trx)` idiom, the derived-fetch handle guarantee, and user-fetchFn handle retention. | **Strengthened** by S10: a public `withDb` would need collection re-derivation machinery the internal path doesn't. |
| **D6** | Deferred mapping / `map()` terminality. | **Superseded** by the map × relations design ([map-relations.md](./map-relations.md)): `map()` is a stage boundary, not terminal; collections added after a map graft onto its output; row-shapers remain pre-map-only. |
| **D7** | Derived `IN` chunking by **parameter budget** (default 500 params) divided by column count; no user knob initially. | Adopted; counting values instead of bind parameters is wrong for composites (SQLite caps at 999, PG at 65535). |
| **D8** | Naming per §3.3; one delta: `includes()` renamed `includedRelations()`, returning a tree. | Adopted (see the debug-surface decision below). |
| **D9** | Align `leftJoin*` sugar to order-fixed refs (childRef first) in the same release. | **New; recommended yes; author sign-off.** The library is pre-1.0 and breaking changes are sanctioned; one ref convention library-wide; the break is loud (compile errors on swapped-order calls); "migrate by renaming the method" then actually works. The alternative — two conventions for the same argument pair, forever — fails the coherence bar. |
| **D10** | Debug surface: `includedRelations()` returns a **tree** `{ key, strategy, children: […] }`. | Resolved (not deferred): a flat list cannot answer "which endpoint runs N queries" once attach fetches nest; `relations()` keeps returning the declared key set. Note (map design): `includedRelations()` demangles graft store keys — it reports real keys at every nesting level. |

### Type-level decisions

- **The `AttachedQuery` collection kind** (the largest design change made
  under review). *Chosen:* a third collection kind — `Prototype:
  "AttachedQuery"`, `Type: "AttachedQueryOne|OneOrThrow|Many"`, `Value` = the
  child bag, plus stored match columns — whose runtime record retains the
  bound child query set and re-derives the fetchFn on every `#addCollection`
  snapshot. *Rejected:* typing attach-strategy includes as existing
  fetch-backed `Attach` collections. *Evidence:* all three review lenses
  independently found that the rejected shape types `modify(key, …)` as a
  query-set callback while the runtime hands the modifier a `Promise<Row[]>`
  — compiles and crashes; verified against `CollectionModifier` and runtime
  `modify` (`src/query-set.ts:2365-2371, 3335-3340`). Sharing `Type` literals
  between fetch- and query-backed attaches makes correct dispatch impossible
  even in principle.
- **`NormalizeDefault` collapses union defaults to `"attach"`**. *Chosen:*
  the capability lower bound. *Rejected:* (a) collapsing to `"join"` — a
  config-driven `"join" | "attach"` default typed as the join shape lets
  `orderBy("profile$$bio")` compile while the runtime materializes attach →
  runtime SQL error, and `toQuery()`'s type lies; (b) rejecting non-literal
  defaults outright — that would put a conditional back into the parameter
  position whose inference behavior S1 specifically repaired away.
  *Evidence:* soundness of the flip is provable — under the S1 constraint
  `const D extends FormAStrategies<…>`, a union `D` is only accepted when
  `"attach"` is genuinely available, so the collapse never fabricates a
  strategy; the attach shape under-claims, never over-claims; and with D4
  count SQL is include-independent, discharging the residual objection.
- **Union include-strategies = union of shapes** (not a promised
  non-distributive bracket, which does not exist in forms 1/2). *Evidence:*
  spike S3 measured the distributed union
  `QuerySetWithJoin<…> | QuerySetWithAttachedRelation<…>` and found it safe:
  method calls require validity for both members and both arms'
  `HydratedOutput` are byte-identical. Chains under dynamic strategy degrade
  into growing unions — the documented cost; the sanctioned idiom is
  statement-level branching (worked example in docs).
- **Overload 4's constraint polarity**: `[Strategy] extends ["join"] ?
  TSelectQuerySet : KeepsMatchColumn<T, Key>` — constrain whenever attach is
  *possible*. *Rejected:* the inverted `[Strategy] extends ["attach"]` form,
  which falls to the permissive arm exactly when attach is possible.
  *Evidence:* review finding, unrebutted; fixture added.
- **Value-typed `KeepsMatchColumn` via `DeclaredMatchShape`**. *Chosen:*
  recover the declared match-column types from the stored child bag
  (`QueryRelOf<T,Key>["Value"]["HydratedOutput"][C]`) and check the modified
  child's values against them, nullable-widened — rejecting `String(id)`,
  `Date`, and `BigInt` re-typings for single *and* composite keys (a retyped
  composite member breaks JSON-encoding equality too). *Rejected:* a new
  stored `ParentColumns` entry field (the reviewer's proposal — unnecessary,
  and it would carry emission cost). *Evidence:* review showed
  presence-only checking lets type-visible re-typings sail through and evade
  the runtime nil guard; only *same-type* value transforms are uncatchable —
  the honest residual. This is the one material formula not yet
  compile-verified ([§6](#6-remaining-risks--implementation-phase-tests)
  risk 1). Sub-decisions: the `[ChildColumns] extends [never]` loud-failure
  guard (vacuous `Record<never, any>` acceptance closed); the 3b/4 attach
  arms' generic-context uncallability disclosed (fixture (l)).
- **Generic-context escape hatches** in `ForbidRelationKey` (`string extends
  T["BaseAlias"]` / `string extends keyof T["Collections"]`) and the
  degenerate-case guard in `IncludableKeys`. *Rejected:* the unguarded forms,
  which hard-error on every call in generic contexts / collapse
  `IncludableKeys` to `never` (uncallable, not "less precision").
  *Evidence:* independently confirmed by S3 §6 and S6 §5 (the S6 benchmark's
  generic helper only compiled because it takes `K extends IncludableKeys<T>`
  as a type parameter); provably inert for concrete bags.
- **Thunk as a third union member — no `lazy()` wrapper, no overload split.**
  *Rejected:* a `lazy()` wrapper import (buys nothing the union doesn't
  already deliver) and a thunk-vs-factory same-arity overload split (breaks
  contextual typing of unannotated destructured factories, TS7031).
  *Evidence:* validated in spike S5 — TS 5.9's union contextual-signature
  selection filters by arity and every fixture (value, thunk, unannotated
  destructured factory, negatives) resolves correctly; the `fn.length`
  fragilities are neutralized by invoking deferred functions *with* the
  factory args object (misclassification becomes a timing shift, not
  misbehavior) plus a descriptive TypeError on non-query-set returns. The
  residual `(_) => qs` case fails loudly at the declaration line; adopted as
  a doc note ("thunks must be zero-param"), not a wrapper.
- **Overload 2 widened to fetch keys** (`Key extends IncludableKeys<T>` with
  per-key `Strategies`), so `include(fetchKey, "attach")` is legal and
  greppable while the equally-redundant `include(joinOnlyKey, "join")`
  already was. *Evidence:* review finding; the return type's `Prototype
  extends "Fetch"` discriminant already handles the fetch arm. Small
  unverified delta vs S3's tested shape →
  [§6](#6-remaining-risks--implementation-phase-tests) risk 2.
- **Forms 1/2 distribute over `Key`**; `IncludableKeys` inlines its `keyof`
  (error-text quality: the typo error prints `'"posts" | "flags"'`, not an
  unexpanded alias over the whole bag). *Evidence:* S3 (union keys
  over-claimed output and mis-routed fetch keys through the join map) and S4
  (alias short-circuited error display).
- **`RelationChild` uses the infer-re-constraint form**, never `Value &
  TSelectQuerySet`. *Evidence:* S2 proved (with `IsAny`) the intersection
  any-poisons `HydratedOutput`; independently re-confirmed by S3 and S6.
  Generalized into standing rule 1.
- **The printer rule** (inline `& RelationStrategy` on stored `Strategies`).
  *Evidence:* S6 — the as-sketched field TS7056s at menu depth 6 under
  `--declaration`; the inline intersection fixes it for free; a named alias
  wrapping the same intersection reintroduces the failure. Generalized into
  standing rule 2 with a CI grep check.
- **Pairwise join-column comparability promoted into Form A** (from
  optional). *Evidence:* spike S11 — rejects `"posts.userId"` ↔
  `"user.email"` with the best error text in the spike set; the naive
  one-directional check has both a false-negative and a false-positive
  class; cost ~+780 instantiations/decl (negligible).
- **`FormAStrategies` drops its unused `Key` parameter; `SingleAttachOk`
  gains an `IsAny` guard** (an `any`-typed match column must not silently
  qualify for attach). *Evidence:* review.

### Runtime & semantics decisions

- **Per-batch child pagination as a JS window, post-flatten, at entity
  level.** *Chosen:* strip props-level pagination from the per-chunk SQL and
  apply it to hydrated *entities* after the flatten + raw-row sort.
  *Rejected:* applying the child's `limit`/`offset` per chunk statement
  (silently converts per-batch into per-chunk semantics). *Evidence:* review
  finding verified against the sketch; the window must be entity-level
  because a child with nested many-joins explodes raw rows. Chunking becomes
  fully result-invisible (contract exception 5).
- **Ordering authority inside the derived fetch** (see D3). *Rejected:* a
  shared-comparator change in the hydrator — the join-strategy comparator
  sorts raw prefixed rows pre-hydration while attach groups are hydrated
  outputs, so a hydrator-level "shared comparator" silently no-ops or
  diverges (omitted sort columns, `category$$name` grandchild keys,
  `mapFields`, `.map()`ped children, function keys). Sorting between raw
  execution and hydration is the only variant that genuinely reuses the join
  comparator's semantics. Consequence: the hydrator is untouched by this
  design.
- **Transitive rebinding**: `include()` rebinds the resolved child to
  `this.#props.db` *before* invoking the include callback, under **both**
  strategies. *Rejected:* rebinding only the immediate attach level — the
  parent-handle guarantee then fails to survive nesting (in
  `usersQs.include("posts", p => p.include("category", "attach"))` the inner
  derived fetch would bind to the handle `postsQuerySet` was *declared* on;
  the flagship example would escape the transaction). *Evidence:* review,
  verified against the recursion; effective only together with P2 (S10
  proved `#toQuery()`'s fast paths otherwise execute on the declaration
  handle — with an in-transaction deadlock demonstrated on better-sqlite3).
  Nested-case test in [§6](#6-remaining-risks--implementation-phase-tests)
  risk 7.
- **The pagination guard reads both layers** — props `limit`/`offset`
  (offset explicitly included) *and* top-level `limit`/`offset` nodes of
  `child.#props.baseQuery.toOperationNode()`. Base-*node* pagination is
  rejected under non-lateral join **and** under attach (it cannot be
  stripped and re-applied in JS, so per-chunk semantics would silently
  return — zero-compromise says reject; the error points at props-level
  `.limit()` for per-batch, laterals for per-parent). *Evidence:* review
  showed the props-only guard is bypassable via `selectAs(qb.limit(5))` or
  `modify((qb) => qb.limit(5))`.
- **Count-exclusion soundness objection rejected on repo evidence.** A
  review claim held that sibling ON clauses can reference the excluded
  one-join's alias, falsifying S9's precondition. Adjudicated against the
  repo: `ToInitialJoinedTB<T> = T["BaseAlias"]` (`src/query-set.ts:366`) and
  both `JoinReferenceExpression`/`JoinCallbackExpression` are built over
  `ToInitialJoinedDB<T>` with that TB (`:2477-2495`) — Kysely scopes join
  references to {base alias, the join's own alias}; the posited
  `join.onRef("x.a", "=", "profile.b")` is a compile error today. The
  residual (casts/untyped JS) fails with a loud unknown-alias SQL error,
  never a silently wrong count. Adopted from the finding: the one-line
  invariant note in §3.8 item 2. The proposed narrowing (exclude only
  relation-derived one-joins) is not adopted — it would re-fork count SQL
  between eager `leftJoinOne` and included relations for no soundness gain.
- **Composite runtime `IN`** uses
  `eb(eb.refTuple(…), "in", vs.map((v) => eb.tuple(…)))` with internal
  casts; composite arity capped at 5. *Evidence:* spike S7 — no `.in()`
  method exists on `ExpressionWrapper` (an earlier draft's snippet did not
  compile); `refTuple`/`tuple` are 2–5-ary and reject dynamic spreads.
- **Sugar seams, all decided now** (deferral would be a docs lie): (1) D9 —
  align `leftJoin*` to order-fixed refs; (2) Form C's options-object
  spelling vs `attach*`'s positional spelling is deliberate (the object
  disambiguates from function-valued children) and documented; (3) sugars
  **bypass the relations map** — `leftJoin*`/`attach*` build the declaration
  record internally and materialize immediately, so
  `relations()`/`includedRelations()` reflect only user declarations and
  `ForbidRelationKey`/`IncludableKeys` stay clean.
- **Error-message obligations**: `UnsupportedChildPaginationError`'s message
  branches by caller (sugar → `leftJoinLateral*`; include → `relateLateral*`
  / `"attach"` / props-level limits); the runtime keyed-`modify` miss on a
  declared-but-not-included key gets the message `"posts" is declared but
  not included — call .include("posts") first`, with a runtime-message test.
- **Writes claims labeled non-normative** (expected, unverified; writes
  design deferred), and the `insert(…).include(…)` example moved outside the
  contract's normative scope.
- **Other confirmed rejections**: no fluent `.via()` (the return type is
  minted at the call; a post-hoc swap would need to retract
  `JoinedQuery`/`OrderableColumns` growth — the same grow-only impossibility
  as re-inclusion); no `defineRelations` registry; no Date-normalizing
  `getKey`; chain-order sensitivity of `orderBy("rel$$col")` accepted as
  parity with joins today; "always thunk in entity modules" adopted as the
  documented default.

---

## 5. Validation

Every mechanism above was validated by eleven compile/run spikes against
real kysely 0.28.8, TypeScript 5.9.3, and this repo's strict tsconfig. No
spike failed outright; every "pass with changes" delta is folded into §3.
**The spike code currently lives untracked under `spikes-tmp/` in the
working tree** (`decl-types/`, `entry-shape/`, `include-dispatch/`, `scale/`,
`thunk/`).

| Spike | What was compiled/executed | What it proved |
|---|---|---|
| **S1** | Declaration-time `Strategies` computation: literal capture, `NoInfer` indexed access, availability gate, autocomplete unions, all seven `defaultStrategy` fixtures | All inference and gates work verbatim (~+36 instantiations/decl). Forced the `const D extends FormAStrategies<…> = "join"` constraint shape — the `defaultStrategy?: D & …` intersection form produced `Type 'string' is not assignable to type 'undefined'` on failure. |
| **S2** | Relation entry/constraint shape: anonymous entries under non-generic `TRelationBase`, `in out` wrappers, `Extract`-plumbing, 4-deep mutual recursion | Compiles at 76k instantiations, no TS2589. Proved (with `IsAny`) that `Value & TSelectQuerySet` any-poisons `HydratedOutput` → the infer-re-constraint `RelationChild`; proved the `_generics` phantom is silently load-bearing (standing rule 3). |
| **S3** | Include dispatch: all 4 overloads, the 3a/3b partition, `IncludeReturnMap`, `KeepsMatchColumn`, 3-level nesting, all seven error fixtures | Exact types throughout. Found union keys over-claim output and fetch keys mis-route → forms 1/2 distribute over `Key`; found generic-context `include` uncallable → degenerate guard; measured union-strategy returns safe as a union of shapes. |
| **S4** | Re-inclusion exclusion: `Exclude`-based one-shot inclusion, post-attach chains, raw error-text capture | Works, with the "remaining menu" printed in the error. Found the `RelationKeysOf` alias short-circuits error display → the `keyof` is inlined in `IncludableKeys`. Confirmed the S8 fix as a hard dependency. |
| **S5** | Thunk-form children: single-parameter-union contextual typing (value/thunk/factory), TDZ runtime demos, circular imports, `fn.length` classification | Union shape resolves every fixture correctly; an overload split breaks factory contextual typing (TS7031) — forbidden. Thunks survive TDZ; eager values crash. Deferred fns invoked with the factory args object neutralize misclassification. |
| **S6** | Scale/emission benchmark: 10 entities × 4 relations cross-module, depth-6 nested includes, generic helpers, `--declaration` emission | 437k instantiations (~19% of the repo suite), no TS2589. Found the TS7056 emission cliff at menu depth 6 → the printer rule; call-site d.ts 20–30× smaller than eager composition; cliff at depth ~8–9 with the fix. |
| **S7** | Composite positional form: tuple arity-matching, `ChildColumnsOf` extraction, Date-in-composite vs Date-single, runtime `IN` construction | Exact types. `ExpressionWrapper` has no `.in()`; `refTuple`/`tuple` are 2–5-ary and reject dynamic spreads → the `eb(eb.refTuple(…), "in", …)` form, arity cap 5, "inline tuple or `as const`" rule. |
| **S8** | The `TQuerySetWithAttach` threading fix (P1) + 3 regression fixtures | One-word fix, zero repo fallout (534/534 relevant tests, typecheck clean); fixtures fail pre-fix. `include` dies after any eager attach without it. Established the per-transform regression-fixture pattern. |
| **S9** | Count/exists one-join exclusion (P4), executed against SQLite | Count SQL provably include-independent (strictEqual across include states); dirty-data count inflation fixed (1 vs 4; join and attach converge); reachability verified path-by-path; the pagination inner layer must *keep* one-joins (4 tests break otherwise). |
| **S10** | Executor rebinding: `#withDb` clone end-to-end, transactions, better-sqlite3 | Nested joins ride the rebound handle; trx sees uncommitted writes; an unrebound child *deadlocks* on better-sqlite3. Effective only with the `execute()`-authority change (P2) — `#toQuery()`'s fast paths otherwise return declaration-handle-bound builders. |
| **S11** | Pairwise join-column comparability | Rejects `"posts.userId"` ↔ `"user.email"` with the best error text in the spike set; the naive one-directional check has false-negative *and* false-positive classes; bidirectional-`NonNullable` `Overlaps` fixes both at ~+780 instantiations/decl. Promoted into Form A. |

---

## 6. Remaining risks & implementation-phase tests

Each risk names the test that settles it.

1. **`DeclaredMatchShape` / value-typed `KeepsMatchColumn`** is the one new
   type formula not compile-verified by any spike. Test: extend
   `spikes-tmp/include-dispatch/` with fixtures (j) — `String`/`Date`/
   `BigInt` re-typings rejected, legitimate `.where`/`.extras`/preserving
   `.map` accepted, nullable-widened child accepted, error text captured.
2. **Widened overload 2** (`IncludableKeys` + per-key `Strategies`, fetch
   keys admitted) deviates from S3's tested `QueryRelationKeys` shape. Test:
   re-run the include-dispatch fixture set + fixture (h); watch for overload-
   resolution regressions on cases (b)/(c).
3. **Distributed forms 1/2 returns** (`Key extends unknown ? … : never`) were
   S3-recommended but not compiled. Test: literal-key fixtures byte-identical
   pre/post; union-key fixtures produce the per-member union; fetch member
   routes through its own branch (the fetch-key mis-typing fixture must
   flip).
4. **The `AttachedQuery` collection kind end-to-end** (new
   `CollectionModifier` arm, three `ModifyCollectionReturnMap` entries,
   runtime re-derivation on `modify`) — specified here but unbuilt. Test:
   fixture (k) positive + a runtime test that
   `modify(key, qs => qs.limit(10))` after an attach include executes with
   per-batch semantics and re-snapshots the hydrator; plus the match-column
   constraint on the modify path.
5. **Form C context-sensitive `fetch` inference** (S2 incidental):
   unannotated fetch arrows can break `matchChild` checking. Test: Form C
   fixtures with annotated and unannotated arrows; either rework the
   `matchChild` guard or make "annotate the fetch parameter" a documented
   requirement with a good error.
6. **The revised derived-fetch pipeline** (chunk → flatten → raw-row sort →
   nil-guard → hydrate-once → entity window): specified from verified parts
   (S10 rebinding, existing comparator, existing hydrate) but never run as a
   whole. Test: a property test asserting result-invariance across
   `PARAM_BUDGET ∈ {3, 50, 500}` for a fixture with child ordering + limit +
   offset + a nested many-join child (entity-level window) + composite keys;
   plus join-vs-attach equivalence assertions on the same data.
7. **Transitive rebinding**: S10 tested one level. Test: parent on `trx`,
   join-include of posts whose callback attach-includes category
   (module-declared) — the category batch must observe uncommitted rows;
   plus the deadlock-avoidance variant on better-sqlite3.
8. **Emission depth cliff** (~8–9 with the printer fix): geometric growth is
   inherent. Test: CI gate compiling a depth-7 fixture package with
   `--declaration` + a grep of emitted d.ts for unevaluated
   `FormAStrategies<`/`NoInfer<` occurrences (the printer-rule regression
   check, S6 §4).
9. **Postgres coverage**: S9 and S10 ran sqlite-only (no docker in the spike
   environment). Test: full `npm run test:all` incl. the pg lateral count
   test and row-value `IN` on PG before merge.
10. **`IncludableKeys` degenerate guard** must not change concrete-bag error
    text. Test: re-run S4's raw-error captures; case 1 must still print
    `'"posts" | "flags"'`.
11. **`ComparableParentRef` opacity** when no parent column is comparable
    (alias display). Test: capture the raw error for a child column type
    comparable to nothing; add the `TypeErrorMessage` fallback arm only if
    unacceptable.
12. **Real-bag absolute costs**: S6's mini-bag under-estimates the real
    `query-set.ts` bag (no `DrainOuterGeneric`, leaner scope). Relative
    margins are wide (19% of the repo suite), but the implementation should
    re-run the S6 workload against the real types as its acceptance gate.

---

## 7. Related fixes discovered

Fixes to *existing* code discovered while validating this design. The first
three are prerequisites (P1, P4, P2 in [§3.1](#31-prerequisites)); all
currently sit as **uncommitted changes in agent worktrees under
`.claude/worktrees/`** and need review before landing:

1. **`TQuerySetWithAttach` threading bug (P1 / S8).** `TQuerySetWithAttach`
   passes the bare `TCollections` constraint instead of `T["Collections"]`,
   so any eager `attach*` call collapses `Collections` to a bare index
   signature — silently destroying key precision (and, under this design,
   killing `include` after any attach). One-word fix at
   `src/query-set.ts:2425` (`TCollections` → `T["Collections"]`), plus three
   regression fixtures in `src/query-set.test-d.ts` that fail pre-fix. Zero
   fallout: 534/534 relevant tests pass, typecheck clean.
2. **Count/exists one-join exclusion (P4 / S9).** `executeCount()`/
   `executeExists()` currently include non-filtering one-joins, so
   dirty-data cardinality violations inflate counts (a fixture returns 4
   where the entity count is 1). The change excludes them via a flag scoped
   to the count/exists path only — the paginated inner layer of `#toQuery`
   must keep them (4 tests break otherwise). Correctness-improving behavior
   change to today's eager `leftJoinOne` counts; release-notes item. Changes
   in `src/query-set.ts` + `src/query-set.{execution,joins,sql}.test.ts`.
3. **`execute()` authority (P2 / S10).** `#toQuery()`'s fast paths return
   builders bound to the declaration handle, so `execute()` can run on the
   wrong executor. Fix: `execute()` routes through
   `this.#props.db.executeQuery(this.toQuery())`. Behavior-preserving in the
   same-handle case (538/538 tests); load-bearing for the rebinding
   guarantee (an unrebound fetch inside a better-sqlite3 transaction
   deadlocks). Changes in `src/query-set.ts` + a new
   `src/query-set.rebind.test.ts`.
4. **Hydrator stale-collection-on-cross-type-overwrite bug (P3).**
   Pre-existing: overwriting a collection key with a different collection
   kind (e.g. an attach over a join) leaves the old entry in one of the
   hydrator's two collection maps. Cross-type delete must reach *both* maps,
   with a regression test. Below the severity bar on its own, but cheap and
   load-bearing for the "everything funnels through `#addCollection`" claim.
   Not yet implemented in any worktree.
5. **README prose fix.** The `attach*` docs say the hydrator "takes the
   first match" for one-modes; the code throws `CardinalityViolationError`
   on >1 match (as does the join strategy — the convergence the equivalence
   contract relies on). Fix the prose when the relations docs land.
