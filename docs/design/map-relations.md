# Map × Declared Relations: `map()` as a Stage Boundary

**Status:** Design complete, implementation-ready. Every load-bearing type and
runtime mechanism in this document was validated by compile/run spikes
(M1–M8) against real kysely 0.28.8 / TypeScript 5.9.3 / this repo's strict
tsconfig — including the full test suite (565/565 sqlite green) run against a
working implementation of the runtime pipeline — and the design was hardened
by adversarial review whose findings were verified directly against
`src/query-set.ts` and `src/hydrator.ts`. Spike IDs (M1–M8) appear throughout
as evidence labels; see [§5](#5-validation) for the legend. S-IDs (S1–S11)
refer to the settled relations design's validation round
([relations.md §5](./relations.md#5-validation)).

This design supersedes exactly one decision of the settled relations design —
D6, `map()` terminality — and amends that document accordingly
([§3.8](#38-amendments-applied-to-relationsmd)). Everything else in
[relations.md](./relations.md) is a fixed constraint that this design
composes with, unmodified.

---

## 1. Motivation & goals

The relations design makes a query set the **canonical representation of an
entity**: a base query plus a menu of `relate*` declarations, materialized
per call site with `include()`. But `map()` is terminal today:
`map()` (`src/query-set.ts:793`) returns `MappedQuerySet<TMapped<T, NewOut>>`
with `IsMapped: true`, and after it only execution, further `map()`, and
`modify()` remain. The relations design (its D6) therefore gated `include()`
behind `IsMapped` at compile time, blessing the **include-then-map** idiom.

That is correct but not ergonomic:

- An entity module cannot ship BOTH a relations menu and a canonical output
  mapping (e.g. `.map((row) => new User(...))`). Every call site must
  re-apply the map after its includes.
- Worse, the map function's **input type depends on which relations were
  included**, so there is no single reusable parent map function — call sites
  hand-roll variants or the author maintains helpers per include-combination.

Why map was terminal (the real constraints, all discharged below):

1. **Attach matching**: attach-strategy inclusion must read match columns off
   the parent's hydrated rows to batch-fetch children and match them back; an
   arbitrary `map((row) => new User(...))` can rename or remove those columns.
2. **Join hydration**: nested join collections are hydrated into the parent
   row shape before the map runs; the hydrator's dedup (`keyBy`) reads parent
   key columns.
3. **Type opacity**: after `TMapped`, `HydratedOutput` is the user's
   arbitrary `Output`; collection landings no longer line up with it.

The resolution: **`map()` becomes a stage boundary, not a terminus.** All
matching, dedup, and stitching continue to run against raw rows — the map
runs strictly after them, in an output-stage pipeline — and collections
included after a map **graft** onto the map's output as a typed intersection.
The founding commitments hold with no compromise:

- **Zero-compromise type safety.** Output types exactly reflect includes and
  maps; no `any` leakage; misuse is a compile error with a decodable message,
  backstopped at runtime for JS callers.
- **100% correctness.** The strategy-equivalence contract holds for mapped
  parents: join vs attach inclusion of the same relation on a mapped entity
  produce identical results (value-level, deep equality — M4 property-tested
  at every `PARAM_BUDGET`). Pagination, count, and execute semantics are
  unchanged.

The model, in one paragraph (normative — the docs must teach exactly this):

> **Row-shapers (`extras`, `extend`, `mapFields`, `omit`, `with`) shape the
> row; `map()` consumes it; the collection vocabulary (`relate*`, `include`,
> joins, attaches, keyed `modify`) is always available and lands on whatever
> the current output is.** Before a map, collections extend the row (today's
> `ExtendWith`); after a map, they **graft** onto the map's output as an
> intersection `Output & { key: V }`, assigned onto the instance at
> hydration. SQL vocabulary (`where`/`orderBy`/pagination/counts) always
> sees the row — the two-plane framing.

Zero new method names, zero new bag fields.

---

## 2. API overview

The flagship: the entity module declares base + relations + canonical
mapping **once**; call sites `include(...)` any combination and get the
mapped output with included relations typed in. (Compiled and type-asserted
in the M2/M6 spike fixtures; executed end-to-end in the M3/M7 worktree
suites.)

```ts
// db/entities/users.ts
export class User {                                // EXPORTED — advisory for d.ts nameability (M6)
	readonly id: number;
	readonly username: string;
	readonly email: string;
	constructor(row: { id: number; username: string; email: string }) { /* … */ }
	get displayName() { return `${this.username} <${this.email}>`; } // extras → getters
}

export const Users = querySet(db)
	.selectAs("user", db.selectFrom("users").select(["id", "username", "email"]))
	.relateOne("profile", () => Profiles, "profile.userId", "user.id")
	.relateMany("posts", () => Posts, "posts.userId", "user.id")     // Posts itself mapped
	.relateMany("flags", { fetch: fetchFlags, matchChild: "userId" })
	.map((row) => new User(row));       // canonical mapping, ONCE; menu stays live;
	                                     // relate* AFTER this line also compiles (order-free)

const page = await Users
	.include("posts", (posts) => posts.include("category", "attach"))
	.include("flags")
	.limit(10)
	.execute();
// Array<User & { posts: Array<Post & { category: Category }> } & { flags: Flag[] }>
const bare = await Users.execute();                     // Array<User>
const n = await Users.include("posts").executeCount();  // include- & map-independent
```

The map's input type is the stable bare base output — it never varies with
includes. Includes graft onto the class instance: `page[0]` is a real `User`
(`instanceof`, getters, methods) that *also* has `posts` and `flags`.

**Harder scenarios** (each is a passed fixture, not aspiration):

```ts
// 1. Composite-key attach on a mapped parent (M4, spikes-tmp/map-m4/m4-equivalence.test.ts):
//    orders mapped to class OrderView; order_items keyed (orderId, region).
const orders = await Orders                              // .map((r) => new OrderView(r)) in the module
	.include("items", "attach")                            // composite derived fetch, chunk-invariant
	.execute();                                            // Array<OrderView & { items: Item[] }>
// join vs attach: deepStrictEqual at PARAM_BUDGET ∈ {3, 50, 500}; instanceof at every level.

// 2. Nested mapped child with its own include (M2/M6): Posts is mapped to class Post and
//    grafts its own child inside the callback; the parent grafts the result.
const rows = await Users.include("posts", (p) => p.include("category")).execute();
// Array<User & { posts: Array<Post & { category: Category }> }>  — nominal at both levels.

// 3. Count on a mapped+included set (M3/M8 runtime + M2 types): byte-identical to the bare count.
const total = await Users.include("posts").include("flags").executeCount();

// 4. The sandwich (stage law): a later map consumes earlier grafts; later includes graft onto it.
const dto = await Users
	.include("posts").map((u) => new UserWithPosts(u, u.posts))   // include-then-map: posts on the class
	.include("flags")                                             // grafts onto UserWithPosts
	.execute();                                                   // Array<UserWithPosts & { flags: Flag[] }>
```

**Include-then-map remains the blessed route for "I want `posts` declared
*on* my class"** — the canonical map can never see relations (grafts are
intersection-typed bolt-ons). The docs present that recipe as *the* answer
to that question, not as an appendix.

---

## 3. Full specification

### 3.1 Prerequisites

- **P1 and P3 of the relations design are hard prerequisites of this design**
  (not merely of relations). P1: the `TQuerySetWithAttach` bare-`TCollections`
  fix (`src/query-set.ts:2424-2425`). P3 is **extended**: cross-kind
  collection overwrite must strip **both key forms** (real and
  `$graft$$`-mangled) from **both** hydrator maps and both props maps before
  re-registering. The extension also fixes a pre-existing latent bug on
  never-mapped chains ([§7](#7-related-fixes-discovered)). (M3)
- **Packaging prerequisite (new, mandatory)**: the internal transform aliases
  that appear in exported entity-module types — `TMapped`,
  `InitialJoinedQuery`, `TWithOutput`, `TWithExtendedOutput`, `TWithOmit`,
  `TQuerySetWithRelation`, plus the new `TMappedQuerySetWith*` family — must
  be exported (or d.ts-rolled-up), or every consumer entity module compiled
  under `--declaration` fails TS4023 (M2). Exporting user entity *classes* is
  **advisory** only (nameability: the emitter synthesizes a local
  `declare class` otherwise; no error — M6).
- Two **additive** hydrator changes (the relations design's "hydrator
  untouched" claim is footnoted accordingly): `FullHydrator.withoutCollection(key)`
  (P3 needs a removal API that did not exist — M3) and `defineProtoShadowedKey`
  exported `@internal` (single home for the `__proto__`-safe define — M7).
  The hydration hot loop is untouched.

### 3.2 Standing type-authoring rules

Rules 1–5 of [relations.md §3.2](./relations.md#32-standing-type-authoring-rules)
apply unchanged. Two new rules, spike-derived and normative (added there as
rules 6 and 7):

6. **Per-member union checks inside a conditional whose outer check is a
   keyof-indexed `[Cs] extends [keyof …]` must be hoisted into a named
   distributed alias.** The inline form compiles and silently evaluates
   non-distributively — it reopened the attach-gate hole under both tsc and
   tsgo while appearing correct. (M4)
7. **The shared tier's (`ExecutableQuerySet`) method returns must be
   conditional-free in `IsMapped`.** Deferred `MaybeMappedQuerySet` returns
   on the shared tier fail TS2589 on *every* relate/join/child-inference
   call — reproduced on demand (`spikes-tmp/map-m1/tsconfig.prerepair.json`).
   `MaybeMappedQuerySet`/`QuerySetFor` keep their exact text in **dispatch**
   positions only (stored child bags, include-callback params, keyed `modify`
   params) — those resolve against concrete discriminants and are safe.
   Single gated exception: `$narrowType` ([§3.4](#34-type-transforms-and-guards),
   risk TG-1). CI grep: no `MaybeMappedQuerySet<` in any `ExecutableQuerySet`
   member signature. (M1)

### 3.3 API surface: the three-tier hierarchy

```
ExecutableQuerySet<T>   ← today's MappedQuerySet body (src/query-set.ts:431-1159),
│                         renamed, EXCEPT the seven members that today return
│                         MaybeMappedQuerySet<…> — $castTo, $narrowType, $assertType
│                         (:1042/:1052/:1070) and insert/update/delete/write
│                         (:1122-1158) — which are redeclared per tier with tier-exact
│                         returns (M1; $narrowType is the single ruled exception,
│                         §3.4). Execution, hydrate, SQL shaping, counts, writes, map.
│                         Hosts the `_generics` phantom (standing rule 3).
├── MappedQuerySet<T>   ← what map() returns. ExecutableQuerySet PLUS:
│                         • relate* (six verbs, three forms — relations.md §3.4, return
│                           wrappers swapped; order-free declaration)
│                         • include (four forms — relations.md §3.5, key branded,
│                           returns via MappedIncludeReturn)
│                         • leftJoin*/innerJoin*/crossJoin* + laterals + attach* sugars,
│                           graft-mode, key branded
│                         • keyed modify, shape-preserving dialect (returns this)
│                         • relations() / includedRelations()
└── QuerySet<T>         ← unchanged from today (:1175+): ExecutableQuerySet + row-shapers
                          + extend-mode collection vocabulary + full-flexibility keyed
                          modify + relate*. Sibling of MappedQuerySet — no internal
                          position requires QuerySet ≼ MappedQuerySet (M1-proven).
```

**The seven-member tier-exact-return repair (M1).** The pre-repair split
(deferred `MaybeMappedQuerySet` returns on `ExecutableQuerySet`) fails TS2589
under real inference load; with the seven members redeclared per tier with
tier-exact returns, the full S5/S2 batteries plus mapped replay plus circular
shapes compile green (14.6M instantiations, zero TS2589). Tier-exact
overrides are byte-identical at concrete sites (the conditional only ever
resolved to the receiver's own tier) and strictly more precise in generic
contexts. Documented residual: TS2589 noise on *eager tier-vs-tier
comparisons* (assigning one tier's type to another in a hand-written
conditional) — pinned, not user-facing (M1).

**Supertype positions** move to `ExecutableQuerySet` mechanically:
`NestedQuerySetOrFactory` (`src/query-set.ts:2454`), `JoinBuilderCallback`
(`:2460`), `CollectionModifier`'s join-arm return (`:2370`), `:3227`, the
relations design's thunk union (`NestedQuerySetThunkOrFactory`, all three
members) and include-callback return positions. None of the tier names is
exported from `src/index.ts` (verified) — the rename is free at the public
boundary.

**Availability matrix (normative):**

| Method | Pre-map | Post-map |
|---|---|---|
| `extras`, `extend`, `mapFields`, `omit`, `with(hydrator)` | ✓ | ✗ — TS2339 (method absent) + `RowShaperAfterMapError` at runtime for JS callers (load-bearing, not cosmetic: the props hydrator now stays full, so the old accidental throw no longer protects this surface — M3) |
| `relate*` | ✓ | ✓ (inert declaration; identical semantics; full `ChildRefs` precision — M1) |
| `include`, joins, laterals, `attach*` | ✓ extend-mode | ✓ graft-mode, key branded fresh |
| keyed `modify(key, cb)` | ✓ full flexibility | ✓ shape-preserving, returns `this` |
| `map` | ✓ | ✓ (next stage; input = current grafted output) |
| `$castTo`, `$assertType`, writes | ✓ | ✓ — **tier-exact returns** (M1 repair) |
| `$narrowType` | ✓ (today's structural form) | ✓ — intersection form for valid narrows, structural for invalid (`HasNarrowError` dispatch) |
| SQL plane (`where`, base `modify`, `orderBy` incl. `rel$$col`, pagination), counts, `hydrate`, `toQuery` family | ✓ | ✓ unchanged |

Post-map `extras` is deliberately not offered; replacements: class getters
(compute pre-map — `extras` before `map` still works) and a second `map()`.

### 3.4 Type transforms and guards

**The graft rule.** No shared conditional (a `CollectionLanding` conditional
is rejected — [§4](#4-design-decisions)). Mapped-tier twins of the landing
transforms with the intersection written **inline** (printer rule 2 by
construction; unmapped transforms stay byte-identical to today). Validated at
scale in `spikes-tmp/map-m6/map/mini.ts`: zero unevaluated guard aliases in
emitted d.ts; the emission cliff is unmoved (TS7056 at depth 11 in both the
with- and without-map variants, against a criterion floor of 8).

```ts
type TMappedQuerySetWithJoin<
	T extends TQuerySet, Key extends string, Type extends TJoinType,
	TNested extends TSelectQuerySet,
> = Flatten<{
	DB: T["DB"]; IsMapped: true; BaseAlias: T["BaseAlias"]; BaseQuery: T["BaseQuery"];
	Collections: TCollectionsWith<T["Collections"], Key, { Prototype: "Join"; Type: Type; Value: TNested }>;
	Relations: T["Relations"];
	JoinedQuery: JoinedQueryMap<T, Key, TNested>[Type];   // post-map joins still shape SQL + hydrate() input
	OrderableColumns: TOrderableColumnsWithJoin<T, Key, Type, TNested>;
	HydratedOutput: T["HydratedOutput"] & { [_ in Key]: JoinHydratedRowMap<TNested>[Type] }; // ← inline
	OmittedKeys: T["OmittedKeys"];
}>;
// TMappedQuerySetWithAttach and TMappedQuerySetWithAttachedRelation likewise
// (bags otherwise per src/query-set.ts:2413-2432 / relations.md §3.6; P1's bare-TCollections
//  fix at :2424-2425 is a hard prerequisite).
```

Wrapper interfaces per the repo's pattern (named, `in out`, bag args
expanded): `MappedQuerySetWithJoin/WithAttach/WithAttachedRelation/WithRelation`,
plus `MappedIncludeReturnMap`/`MappedIncludeReturn` — relations.md's
`IncludeReturn` with the wrappers swapped.

**Intersection order (ruled, M2-confirmed)**: class-name-first —
`T["HydratedOutput"] & { [_ in Key]: V }`. M2 reproduced the hazard where,
with an *overlapping* key, class-first leaks a weak slot type into array
callbacks — and proved **freshness, not order, is the invariant**: with a
fresh key (which the guard guarantees on guarded paths) the full array-method
suite passes under both orders. Graft-first is the pre-validated drop-in
fallback (`spikes-tmp/map-m2/m2-order-fallback.ts`), retained but not
expected to be needed.

**The collision guard** — a parameter-position brand (constraint position
pinned worse in `spikes-tmp/map-m2/m2-constraint-position.ts`), uniformly on
every post-map collection-adding method (all four `include` forms' key
parameters and every sugar):

```ts
type IsAny<X> = 0 extends 1 & X ? true : false;

type GraftableKey<T extends TQuerySet, Key extends string> =
	IsAny<T["HydratedOutput"]> extends true ? unknown                        // degenerate: stay callable
	: [T["HydratedOutput"]] extends [string | number | bigint | boolean | symbol]
		? TypeErrorMessage<"Cannot add collections after mapping to a primitive output (a branded primitive is still a primitive at runtime; grafting a property onto it would throw)">
	: [T["HydratedOutput"]] extends [object]
		? string extends keyof T["Collections"] ? unknown                     // standing rule 4
		: [Extract<Key, keyof T["Collections"] & string>] extends [never]     // non-distributive
			? GraftCollision<T["HydratedOutput"], Key>
			: TypeErrorMessage<`"${Key}" is already a collection on this mapped query set; it cannot be redefined`>
		: TypeErrorMessage<"Cannot add collections after mapping to a non-object output (check for | null / | undefined in your map's return type)">;

/** Distributed over union members: fresh in EVERY member, or poisoned. */
type GraftCollision<Out, Key extends string> =
	(Out extends unknown
		? string extends keyof Out ? never                                    // index-signature member: runtime backstop only
		: Key extends keyof Out & string ? true
		: never
	 : never) extends never
		? unknown
		: TypeErrorMessage<`Cannot graft "${Key}": the mapped output already has a property with that name (in at least one union member). Rename the relation key, or use include-then-map. Optional relation slots (posts?: Post[]) are deliberately rejected.`>;

// usage, everywhere:  key: Key & GraftableKey<T, Key>
```

Formula notes, each spike-forced:

- The **primitive-detect arm** exists because branded primitives evaded a
  bare `[object]` check as synthesized — they compiled and would always throw
  at runtime (M2).
- The **Collections-membership arm is non-distributive**
  (`[Extract<…>] extends [never]`) so a union key with one consumed member
  cannot absorb the poison through sugars (`TypeErrorMessage | unknown =
  unknown`); the runtime `RelationKeyConsumedError` backstops it regardless
  (M3-tested). Gated by fixture TG-3.
- Message texts above are drafts at the raw-capture bar: final wording is
  pinned by fixtures asserting *actual compiler output* before ship
  (remedy-first, ≤ ~200 visible chars before any secondary note — M2's
  captures confirmed mid-sentence truncation with the actionable clause
  surviving; the slot *rationale* lives in the docs).
- Rulings carried: optional slots rejected (verified array-callback
  degradation is the reason; M2 renders the message); getter collisions
  caught (prototype accessors are in `keyof` — M2); union outputs distribute
  (M2); `Record<string, any>`/`any` outputs stay callable and fall to the
  runtime backstop. **Honesty-ledger entries include the type side**: an
  `any`-record output grafts as `any`; a `Record<string, string>` output
  grafts as `string & V` garbage — input-side-degenerate, GIGO-scoped.

**Consumed-key rendering split (pinned, M1 + M2)**: through `include`, the
`IncludableKeys` constraint domain rejects first (remaining-menu text, the
relations fixture-(g) shape) — the brand is dead code on that path; through
sugars, the branded message renders. Both decodable; both fixture-pinned; do
not widen the include key domain.

**Generic-wrapper recipes (M6)**: wrappers over the mapped tier must (a)
thread the brand through their own key parameter
(`key: K & GraftableKey<T, K>`) and (b) pin the inner call's type argument
(`qs.include<K>(key)` — otherwise the brand self-compounds under inference;
failure is loud at the wrapper author's desk). Documented alongside the
relations `paginate` rule; `spikes-tmp/map-m6/map/paginate.ts` is the
fixture. **Known floor gap**: *output-generic* wrappers
(`MappedQuerySet<TMapped<TUsers, U>>`, `U` a bounded type parameter) defer
`[U] extends [object]` and reject hardcoded keys; disposition = the same
brand-threading recipe, with hardcoded-key usage disclosed à la relations
fixture (l). Gated by fixture TG-2.

**Post-map keyed `modify`** — one uniform shape-preserving rule, in
generic-constraint form (M5-proven: not vacuous; subsumes the
`Columns`-based match-column keep including composites). Verbatim from
`spikes-tmp/map-m2/lib.ts`:

```ts
/** Degenerate-guarded element shape; Extract-plumbing, never index the raw union. */
type DeclaredElementShape<T extends TQuerySet, Key extends string> =
	string extends keyof T["Collections"] ? any                              // capability floor
	: [Extract<T["Collections"][Key], { Prototype: "Join" | "AttachedQuery" }>] extends [never]
		? any                                                                  // recover to floor, not never (M5)
	: Extract<T["Collections"][Key], { Prototype: "Join" | "AttachedQuery" }>["Value"] extends
			infer V extends TSelectQuerySet ? V["HydratedOutput"] : any;

/** Constraint-only intersection (standing rule 1 applies to ACCESS, not constraints). */
type PreservesShape<T extends TQuerySet, Key extends string> =
	TSelectQuerySet & { HydratedOutput: DeclaredElementShape<T, Key> };

/** Query-backed collection keys, degenerate-guarded. */
type ModifiableQueryCollectionKeys<T extends TQuerySet> =
	string extends keyof T["Collections"] ? string
	: { [K in keyof T["Collections"] & string]:
			[Extract<T["Collections"][K], { Prototype: "Join" | "AttachedQuery" }>] extends [never] ? never : K
		}[keyof T["Collections"] & string];

type QueryCollectionChild<T extends TQuerySet, Key extends string> =
	Extract<T["Collections"][Key & keyof T["Collections"]], { Prototype: "Join" | "AttachedQuery" }>["Value"];

// On MappedQuerySet — query-backed dialect:
modify<
	Key extends ModifiableQueryCollectionKeys<T>,
	TNestedNew extends PreservesShape<T, NoInfer<Key>>,
>(
	key: Key,
	modifier: (child: QuerySetFor<QueryCollectionChild<T, Key>>) => ExecutableQuerySet<TNestedNew>,
): this;

// Fetch-backed dialect: value callback; return constrained to the BARE union
// FetchReturnOf<DeclaredElement> — never intersected with SomeFetchFnReturn
// (M5: the intersection makes TS explain against the wrong member and the
// error never names the offending field).
```

Because the return is `this`, the bag never changes, no graft is ever
re-landed, and the intersection is never asked to overwrite — the re-landing
corruption class is **structurally eliminated** (independently re-derived and
confirmed under review). Polarity: narrowing (`where`/`orderBy`/`limit`),
element subtypes, and additive changes pass (honest under-claim, pinned);
drops and re-typings fail with bounded elaborations whose final lines name
the offending field (M5 raw captures). A `TypeErrorMessage` resolution arm
for these failures is **deliberately not adopted** (M5 no-go: the raw text
clears the decodability bar; a message arm would mask the field-naming
tail). `PreservesShape` is deliberately stricter than the include callback's
nullable-widened `DeclaredMatchShape` — a disclosed asymmetry. To reshape an
included child on a mapped set: use the include callback, or branch before
the map.

**Post-map re-typing escape hatches (ruled).** `$castTo`: documented-unsafe
on all tiers, tier-exact returns (M2 fixture). `$assertType`: today's
exact-equality semantics on all tiers, tier-exact returns; post-map, the
assertion target is the current mapped/grafted output — ledger line.
`$narrowType` — the single sanctioned exception to standing rule 7, because
the mapped tier's intersection return is TS2430-inexpressible as a tier-exact
override (bags with intersection vs structural outputs are
invariance-incompatible). The M2-validated spelling, verbatim from
`spikes-tmp/map-m2/lib.ts`:

```ts
// ONE declaration on ExecutableQuerySet (per-tier override is TS2430-inexpressible):
$narrowType<TNarrow>(): T["IsMapped"] extends true
	? HasNarrowError<NarrowOutput<T, TNarrow>> extends true
		? MappedQuerySet<TWithOutput<T, NarrowOutput<T, TNarrow>>>   // invalid narrow: structural form —
		                                                             // errors poison use sites (today's loudness)
		: MappedQuerySet<TWithOutput<T, TOutput<T> & NarrowPartial<TOutput<T>, TNarrow>>> // valid: class identity survives
	: QuerySet<TWithOutput<T, NarrowOutput<T, TNarrow>>>;            // unmapped: byte-compatible with today

type HasNarrowError<N> =
	true extends { [K in keyof N]: N[K] extends { readonly __typeError__: string } ? true : false }[keyof N]
		? true : false;
```

M2-proven: valid `NotNull`/literal narrows keep class assignability and
grafts still land; invalid narrows fail on use AND on class assignment (the
naive bare-intersection form silently swallowed them — two
`@ts-expect-error` directives went unused until the `HasNarrowError`
dispatch was added). Doc note: valid narrows now differ in *output form*
between tiers (structural vs intersection) — one teaching line. This
member's behavior under the full inference battery is gated by risk TG-1.

**Mapped-child attach gate** — the amendment to relations.md §3.4, exactly
as it must be written (verbatim from `spikes-tmp/map-m4/support.ts`; the
inline composite form is **forbidden** — it compiles and silently reopens
the hole under both tsc and tsgo — standing rule 6):

```ts
/** The M4 conjunct: hydrated (post-map) vs raw match-column value overlap. */
type HydratedRawOverlapOk<C extends string, TNested extends TSelectQuerySet> = Overlaps<
	NonNullable<TNested["HydratedOutput"][C & keyof TNested["HydratedOutput"]]>,
	NonNullable<TNested["BaseQuery"]["O"][C & keyof TNested["BaseQuery"]["O"]]>
>;

/** Settled form (presence + IsAny + MapSafeKey) + the new conjunct, in this order. */
type SingleAttachOk<C extends string, TNested extends TSelectQuerySet> =
	C extends keyof TNested["HydratedOutput"] & string
		? IsAny<TNested["HydratedOutput"][C]> extends true ? never
		: TNested["HydratedOutput"][C] extends MapSafeKey
			? HydratedRawOverlapOk<C, TNested> extends true ? "attach" : never
			: never
		: never;

/** REQUIRED: per-member distribution hoisted into a NAMED alias (standing rule 6). */
type EachHydratedRawOverlapOk<Cs extends string, TNested extends TSelectQuerySet> =
	Cs extends unknown ? HydratedRawOverlapOk<Cs, TNested> : never;

type CompositeAttachOk<Cs extends string, TNested extends TSelectQuerySet> =
	[Cs] extends [keyof TNested["HydratedOutput"] & string]
		? false extends EachHydratedRawOverlapOk<Cs, TNested> ? never : "attach"
		: never;
```

Failing children compute `Strategies: "join"` — the existing relations
fixture-(c) error surface. No false positives: nullable widening, literal
narrowing, non-match-column re-typings, and branded-same-representation
columns all keep attach (M4 + polarity audit under review). Unmapped
children are byte-unaffected (the relations S1/S11/S7 fixtures re-run
identical), so the gate amendment is **safe to land in the settled design
ahead of this feature** — it closes that design's own pre-existing hole for
terminally-mapped declared children. The honest residual — same-type value
transforms (`id + 1`) — is exactly what the runtime backstop exists for
([§3.6](#36-semantics--edge-cases)).

### 3.5 Runtime design: the output-stage pipeline

`src/hydrator.ts`'s hydration hot loop is untouched; the two additive
hydrator changes are listed in [§3.1](#31-prerequisites). The pipeline is
delivered through the existing `hydrator.map()` seam
(`src/hydrator.ts:954-959`, applied at `:1269-1277`), read at exactly the
two verified call sites: `#addCollection`'s child registration (`:2704` →
`collection.querySet.#effectiveHydrator()`) and the `hydrate()` funnel
(`:3095`). `map()` no longer touches the hydrator; **the props hydrator is
full on every construction path** (M8 inventory fixture).

```ts
interface OutputStage {
	readonly map: (value: unknown) => unknown;
	readonly graftKeys: readonly string[];    // REAL keys of collections added after this map
}
// QuerySetProps gains: outputStages: readonly OutputStage[];   ([] ⇒ today, byte-identical — M3 oracle)

#effectiveHydrator(): Hydrator<any, any> {
	const { hydrator, outputStages, baseAlias } = this.#props;
	if (outputStages.length === 0) return hydrator;
	return hydrator.map(composeStages(baseAlias, outputStages));  // FRESH per call — NOT memoized (M7)
}
```

**No memoization**: the composed closure carries the shared-output guard's
WeakSet, whose lifetime must be per-hydration; composing fresh per
`hydrate()`/`execute()` call is the implementation of "per hydration" at the
parent level (one closure + one lazy WeakSet per execute — negligible cost,
M7-measured). **Disclosed residual**: a *child's* pipeline is baked once at
`#addCollection` registration, so a child-level WeakSet persists across
parent executions; a cross-execution memoized child map with its own grafts
surfaces as `SharedMapOutputError` rather than `GraftCollisionError` — loud
and named either way (M7; regression fixture RG-6).

**`composeStages`** (per-level closure; M3 + M7 shipped semantics):

1. **Pluck**: lift `entity[mangle(key)]` for every graft key into a **`Map`**
   (a plain record would hit `Object.prototype`'s `__proto__` accessor for a
   graft keyed `"__proto__"` — M7); delete the mangled property.
   `mangle(key) = "$graft$$" + key`. Mangling is the correctness lynchpin —
   the overwrite it defends against (`entity[key] = output` at
   `src/hydrator.ts:1242`/`:1264` before mapFns run) was reproduced on main
   and corrupts without it (M3, M7).
2. **Pipeline**: per stage in order — `out = stage.map(out)`; shared-output
   guard (step 4) **only when `stage.graftKeys.length > 0`**; then
   `graftAssign(out, key, plucked.get(key))` for that stage's keys.
3. **`graftAssign` — the graft mutation contract** (M7-shipped ordering):
   - non-object target → `GraftTargetError` (names the collection key, the
     entity alias, and the remedy);
   - frozen/sealed/non-extensible target → `GraftTargetError`;
   - walk own + prototype chain, stopping **before** `Object.prototype`
     (`toString`-named relations graft cleanly; class prototype members
     still hit): **accessor anywhere (own or inherited) → `GraftTargetError`**
     ("claimed by a getter/setter…; grafting through accessors is forbidden"
     — setter invocation count asserted 0); own non-writable data property →
     `GraftTargetError`; **any data property → `GraftCollisionError`**
     (hints: rename, include-then-map, cross-execution memoization);
   - `key === "__proto__"` → `defineProtoShadowedKey` (own shadowed data
     property; the prototype is never polluted);
   - else plain assignment.
4. **Shared-output guard**: per-hydration WeakSet (fresh per compose, see
   above), applied **only to results of graft-receiving stages**; after
   `stage.map`, throw `SharedMapOutputError` iff
   `result !== prev && weakSet.has(result)`; add each new reference once.
   Admits identity/mutating maps, multiple same-stage includes onto one
   instance (the flagship), and single-entity flyweights; catches
   cross-entity flyweights eagerly with a cause-naming error. Graft-less
   stages are unguarded — trailing scalar maps and attach-shared child
   references returned by later maps can never throw, preserving strategy
   symmetry (regression fixture RG-1 pins both must-NOT-throw chains).
5. **Reserved-namespace guard**: when stages-with-grafts exist at a level,
   scan **that level's first hydrated entity's keys** for
   `startsWith("$graft$$")` (excluding the level's own registered graft
   store keys) → `ReservedColumnNameError`. Per-level scanning handles
   nested pipelines (join-included mapped children run their own composed fn
   inside the parent's recursion — a child *with* stages has its own
   composed fn registered at the child seam; a child *without* stages has no
   pluck/graft at its level, so a `$graft$$x` column is inert there) and
   does not false-positive on legitimate nested mangled prefixes (M3).
   Residual (documented): a user column deliberately spelled to
   alias-collide with a mangled *child column group*
   (`$graft$$posts$$stowaway`) is mis-attributed to the child — the same
   class as today's `posts$$stowaway`.

**`#addCollection`: the five-row dispatch** (normative; M3-tested verbatim,
including the both-key-form membership predicates):

```ts
#addCollection(key, collection, caller: "add" | "modify" = "add") {
	if (outputStages.length > 0) {
		const isGrafted = outputStages.some((stage) => stage.graftKeys.includes(key));
		if (caller === "modify") {
			storeKey = isGrafted ? mangled : key;                    // (b) / (c)
		} else if (joinCollections.has(key) || attachCollections.has(key)) {
			throw new RelationKeyConsumedError(key, "consumed");     // (a) pre-map key
		} else if (isGrafted || joinCollections.has(mangled) || attachCollections.has(mangled)) {
			throw new RelationKeyConsumedError(key, "grafted");      // (a) grafted key — JS double-include lands here
		} else {
			storeKey = mangled;                                      // (d) fresh: mangle + append to LAST stage
		}
	}
	// (e) stages == 0: today's path, byte-identical.
	// P3, uniform: strip BOTH key forms from BOTH hydrator maps and both props maps
	// before re-registering (asFullHydrator(h).withoutCollection(key).withoutCollection(mangled)).
}
```

| Case | Condition | Action |
|---|---|---|
| (a) | collection-adding caller, key ∈ props maps under either form | throw `RelationKeyConsumedError` — "consumed" text for pre-map keys, "grafted" text for post-map keys (types already brand the sugar path; `include` rejects via its key domain) |
| (b) | `modify` caller, key ∈ ⋃ graftKeys | re-register under the **mangled** key; no graftKeys append (`AttachedQuery` re-derivation re-snapshots under the same mangled key) |
| (c) | `modify` caller, key ∉ ⋃ graftKeys (pre-map collection) | re-register under the **real** key; the modified child keeps feeding the map's input (M3-tested) |
| (d) | fresh key, stages > 0 | register mangled in props maps + hydrator; append to the **last** stage's graftKeys |
| (e) | stages == 0 | today's path, byte-identical |

Because membership is checked under **both key forms**, a JS-caller re-add
of a grafted key throws `RelationKeyConsumedError("grafted")` — the silent
double-append/`undefined`-graft corruption path cannot arise (M3-tested).

**The mangling boundary (normative — M3's tested reading, which superseded
two proposed alternatives under review):**

> Post-map registrations store the **mangled** key in the props collection
> maps, the hydrator maps, and the hoisted output prefix
> (`$graft$$posts$$id`). **Anywhere a collection key names a SQL table
> alias, the real (demangled) key is used** — `#addCollectionAsJoin` splits
> the two roles (`alias = demangleGraftKey(key)`;
> `prefix = makePrefix("", key)`), because user join conditions and lateral
> callbacks reference the real key. **`orderBy("rel$$col")` is mangle-aware
> in both branches**: the expr is mangled when its first segment is a graft
> key (hoisted-outer branch and hydrator ordering key), and demangled in the
> inner branch *before* the `$$`→`.` conversion (the prefix itself embeds
> the separator). `modify()` looks up real-then-mangled. **Every user-facing
> key-reporting surface demangles**: `CardinalityViolationError` /
> `ExpectedOneItemError` strip a leading `$graft$$` in their constructors
> (`src/hydrator.ts` untouched); `RelationMatchColumnMissingError` is minted
> with the real key; `includedRelations()` and `relations()` report real
> keys.

In one rule: *anywhere a collection key names a SQL alias, use the real key;
anywhere it names a hoisted output column or an internal store key, use the
(possibly mangled) store key; every user-facing reporting surface
demangles.* This boundary passed the full 565-test suite, including a
post-map `orderBy("profile$$bio", "desc")` fixture (M3).

**Cross-kind overwrite (P3, extended)**: cleanup strips both key forms from
both hydrator maps. This also fixes a pre-existing latent bug on never-mapped
chains ([§7](#7-related-fixes-discovered)).

**`with(MappedHydrator)` fold (M8-proven).** The overload at
`src/query-set.ts:1329-1331` folds the incoming hydrator's `mapFns` into
`outputStages` (one composed stage 0) and returns
**`MappedQuerySet<TMapped<T, OtherOutput>>`** — the honest output is the
mapped hydrator's final output, not `Extend<…>` (the old declared
`TWithExtendedOutput` was a lie against the old runtime too; its test-d
fixture flips to pin `{userId: number}[]`). Row-level config
(fields/extras/collections) merges as today and runs before the folded map
consumes the row. Hydrator-level `HydratorImpl.with` unchanged. `with()`
post-map stays absent. The docs must teach `with(mappedHydrator)` as the
**second map-inducing verb** — the "what maps you" list is exactly two
items: `.map()` and `with(mappedHydrator)`.

**Where maps run, both strategies** (M4-confirmed with exact counts): join —
the child's composed pipeline runs per child entity inside the parent's
recursion (`src/hydrator.ts:1236`); attach — the derived fetch calls the
child's public `hydrate(rawRows)` once per batch. Parent grafts happen in
`composeStages` after the parent's own hydration; cardinality errors fire
during stitching, before any user map sees that entity's data. Counts,
pagination, dedup, `keyBy`, chunking, rebinding (P2): untouched by
construction (re-verified under review; counts are provably stage-blind).

**Error classes (final inventory)**: `GraftTargetError`,
`GraftCollisionError`, `SharedMapOutputError`, `RelationKeyConsumedError`
(two message branches), `RelationMatchIntegrityError`,
`ReservedColumnNameError`, `RowShaperAfterMapError`; the demangling rule as
above. `RelationAlreadyIncludedError` (settled) remains the
relations-`include()` error for re-including an already-included relation;
`include()`'s already-included guard checks **both key forms**. The class
split by entry path is deliberate and documented.

### 3.6 Semantics & edge cases

**The stage law** (one law, chain order): collections stitch from raw rows
before any map runs; the output pipeline interleaves
map₁ → graft(includes after map₁) → map₂ → …;
`include(a).map(f).include(b)`: `f` sees `a`, `b` grafts onto `f`'s output;
a later map may consume earlier grafts (M3 stage-arithmetic tests; M6
sandwich types). Include-then-map is unchanged and remains the blessed route
for declared class members.

**Strategy-equivalence contract — updated clause set.** relations.md §3.8's
contract text and scoped exceptions 1–5 carry over verbatim, with exception 3
gaining the composite-timestamp caveat (a `Date`-typed composite member
matches at driver millisecond precision under attach but full DB precision
under join — a match-set divergence; single-column `Date`/`Buffer` are
already excluded by `MapSafeKey`; BLOB composite members are
byte-deterministic). Three additions, normative:

> **Exception 6 — reference identity & invocation counts.** Child element
> **reference identity** and child-map **invocation counts** are
> strategy-dependent: attach hydrates the batch once and shares child
> instances across matching parents (`applyGroupedCollectionMode` copies the
> array, shares the elements — `src/hydrator.ts:1546`, `:1555`); join
> hydrates per parent row-group, minting distinct instances per
> (parent, child) pair (`:1236`). Guarantees are **value-level** (deep
> equality), never `===`-level. Attach's per-batch windowing runs the child
> pipeline on entities the window then discards. [M4 pinned this with exact
> numbers: 3×Author/5×Award under join vs 2×Author/3×Award under attach on
> the fixture graph, budget-independent.] This documents **pre-existing**
> sharing, not new sharing (verified against `src/hydrator.ts:1546`/`:1555`;
> release note).
>
> **Purity clause.** Map functions must be pure and deterministic per input
> row, must not return shared/memoized references within an execution
> (enforced for graft-receiving stages: `SharedMapOutputError`; backstopped
> by `GraftCollisionError`), and must return a fresh extensible object per
> row when collections graft onto the output (enforced:
> `GraftTargetError`/`GraftCollisionError`). Value-equivalence is
> conditional on this clause. Consumers must not mutate child instances
> (attach shares them) — this sentence is ALSO promoted into the README
> attach docs.
>
> **Mapped-child gating.** Attach availability for a declared child
> additionally requires hydrated-vs-raw match-column value overlap
> ([§3.4](#34-type-transforms-and-guards)'s conjunct, named-alias composite
> form), with the derived-fetch zero-match runtime backstop; same-type value
> transforms that evade the gate are caught by the backstop when total, and
> remain the documented residual when partial (M4-pinned). Form-C fetch
> relations get no backstop.

**Zero-match integrity backstop (scope ruled).** In the derived-fetch
pipeline (relations.md §3.7 step 7), after batch hydration and **before**
`applyWindow` (a legitimate per-batch `offset` window must not be misread as
zero-match): if the batch is non-empty and zero hydrated children match any
parent key, throw `RelationMatchIntegrityError` naming the relation key and
**enumerating the cause classes** — a child `map()` value transform;
collation-insensitive SQL equality (citext/`COLLATE NOCASE`); driver decode
divergence. It applies to **all query-backed attach relations, mapped or
not**: the firing condition always proves a real join/attach divergence (the
rows were selected `WHERE childCols IN (parentKeys)`, so zero JS matches
proves SQL-vs-SameValueZero disagreement), so this is a loudness upgrade to
exception 3's documented-silent residual — release-noted as a behavior
change to the settled design. Non-firing arms (M4-proven): legitimately-empty
parent sets (the fetch is never constructed), genuinely-empty batches,
per-batch offset windows (placement guarantees this).

**Re-inclusion/overwrite across the boundary**: post-map redefinition of any
existing key (pre-map or grafted) is a branded compile error (sugar path) or
key-domain rejection (include path) + `RelationKeyConsumedError` at runtime
with the matching message branch. Pre-map sugar overwrite stays legal with
`ExtendWith` reflecting it (documented asymmetry) — and now with correct
cross-kind hydrator cleanup (P3 extension).

**`keyBy`/dedup/counts/`hydrate()`/`orderBy`**: raw-row-driven,
map-invariant. `executeCount` on a mapped+included set is byte-identical to
the bare count (M3/M8 runtime-pinned). `hydrate(rows)` input stays
`THydrationInput<T>`; post-map join includes still grow it; attach includes
perform I/O on the include-time handle. `orderBy("profile$$bio")` after a
post-map include works via the mangle-aware translation (M3-tested); attach
doesn't grow orderability — inherited divergence (contract exception 1).

**Serialization**: grafted properties are own+enumerable on plain objects
and class instances; array/function outputs graft but `JSON.stringify`
drops/ignores the grafts — scoped claim, M7 fixture-pinned. **Proxy
residual**: a swallowing `set` trap loses the graft silently — pinned (M7),
documented, not fixable without breaking the mutation contract's simplicity.

**Degenerate contexts**: every guard carries an explicit floor (`IsAny`,
`string extends keyof`, the `[never]`-recovery arm); generic wrappers stay
callable at the modify-parity capability floor (M2/M5/M6 matrices), with the
two documented recipes (brand-threading; explicit type args) and the one
disclosed gap (output-generic hardcoded keys, TG-2).

### 3.7 Migration & compat

- **No public export changes that break annotations**; the tier rename is
  invisible (verified against `src/index.ts`), but the **transform-alias
  export prerequisite** ([§3.1](#31-prerequisites)) is new mandatory
  packaging work.
- **Every newly reachable path previously threw.** Behavior deltas, all
  release-noted: (1) `with(MappedHydrator)` — typed and working, honest
  output type, one test-d fixture flip; (2) previously-rejected
  `map().include()` chains now compile *and mean graft*; (3) the zero-match
  backstop throws where the settled design was documented-silent; (4) the
  pre-map cross-kind stale-fetchFn fix ([§7](#7-related-fixes-discovered));
  (5) the row-shaper post-map throw text improves to
  `RowShaperAfterMapError`; (6) Exception 6 documents pre-existing sharing.
- **test-d corpus**: acceptance gate = byte-identical types for unmapped
  chains plus the enumerated flip-list of `@ts-expect-error` mapped-set
  fixtures (`query-set.test-d.ts:2853, :2915, :2990, :3028, :3062, :3092,
  :3179`) plus the M8 "Extend with MappedHydrator" fixture flip.
- **Reserved namespace**: row fields beginning `$graft$$` (loud, per-level
  guard).
- **`isFullHydrator`**: meaning unchanged; query sets' internal hydrators
  are now always full — externally unobservable.

### 3.8 Amendments applied to relations.md

The following amendments have been applied to
[relations.md](./relations.md) (exact and exhaustive; the changes live in
that document — this list is the cross-reference):

1. D6 superseded (§3.8 item 6 and the §4 decision-register row): `map()` is
   a stage boundary, not terminal.
2. §3.4/§3.5 interface headers reflect the three-tier hierarchy (M1).
3. §3.4 attach gates gain the `HydratedRawOverlapOk` conjunct; the composite
   form via the named distributed alias `EachHydratedRawOverlapOk` — inline
   forbidden (M4). Safe to land ahead of this feature.
4. §3.2 gains standing rules 6 and 7 (M4, M1).
5. §3.4.1 thunk union members re-point to `ExecutableQuerySet`; the annotated
   circular escape hatch documents `(): MappedQuerySet<HandWrittenBag>` for
   mapped children (M1).
6. §3.5 include-callback returns re-point to `ExecutableQuerySet`;
   `IncludeReturn` gains the mapped twin family
   ([§3.4](#34-type-transforms-and-guards)).
7. `relate*` additionally lives on the mapped tier (order-free declaration).
8. §3.8 item 5's modify matrix and §3.9's fate table gain the post-map
   shape-preserving dialect.
9. §3.8's contract gains Exception 6, the purity clause, and the
   mapped-child gating clause; exception 3 gains the composite-timestamp
   caveat.
10. §3.7 derived fetch gains the zero-match integrity backstop (before
    `applyWindow`); `RelationMatchColumnMissingError` pinned to the real
    (demangled) key.
11. §3.7's `include()` already-included guard checks both key forms; the
    `RelationAlreadyIncludedError`/`RelationKeyConsumedError` split
    documented.
12. §3.3 vocabulary gains the new error classes + the demangling rule; the
    debug row notes demangled reporting.
13. §3.9's hydrator row gains the two-additive-changes footnote (M3/M7).
14. P1 and P3 marked hard prerequisites of this design; P3 extended to both
    key forms (M3).
15. §3.10 fixture (e) splits into (e1) positive (object map) / (e2) negative
    (primitive or non-object map, two branded message variants) (M2).
16. New packaging prerequisite: export the transform aliases (TS4023
    otherwise) (M2).
17. §4 D10 note: `includedRelations()` demangles graft store keys.
18. The cross-release coordination hedge on README map guidance is retired:
    the spikes passed, and the relations README examples may adopt the
    canonical-map flagship directly.

### 3.9 Documentation obligations (M-DOC, outstanding)

The teachability page ships with implementation, carried as an open
deliverable ([§6](#6-remaining-risks--implementation-phase-tests)).
Contents: the two-plane model; the stage law with one sandwich example;
**include-then-map presented as THE answer to "why isn't `posts` on my
class?"**; the extras→class-getters recipe; the slot rejection rationale
(moved out of the error message); the purity clause + the
consumer-non-mutation sentence in the README attach docs; the "what maps
you" list (`.map()`, `with(mappedHydrator)`); the generic-wrapper recipes
(M6) and the output-generic disclosure; the consumed-key two-rendering split
(M1); the "export your entity classes" advisory + the TS7056 whole-file
d.ts footnote (M6); the class-map-as-scale-optimization note (M6); rewritten
"Terminal operation" / "map vs mapFields" / hydrator-`map` sections (the
hydrator's own `map` stays terminal — the documented fork). The discipline
stands: if the page cannot be written cleanly in the README's voice, that is
a design signal.

---

## 4. Design decisions

A record of every significant decision: what was chosen, the alternative
rejected, and the evidence. "Review" below means the adversarial design
review (three independent lenses per candidate, plus a post-spike
re-review), whose findings were verified directly against the codebase;
spike IDs are compile/run evidence (legend in [§5](#5-validation)). Where a
re-review finding contradicted spike evidence, the spike won — two such
rulings are recorded below (the mangling boundary; the shared-output guard).

### The chassis: stage boundary + three tiers

Four candidate architectures were designed independently and adversarially
reviewed. *Chosen:* the **stage-boundary model** — three-tier hierarchy
(`ExecutableQuerySet`/`MappedQuerySet`/`QuerySet`), zero new bag fields,
zero new vocabulary, output stages at the query-set level with `$graft$$`
mangling. Its founding observations survived hostile verification: the
runtime footprint is exactly **two call sites** (an exhaustive grep of every
`#props.hydrator` read confirmed `:2704` and `:3095` are the complete set),
and the naive-design corruption bug that motivates mangling is real
(`entity[key] = output` at `src/hydrator.ts:1242`/`:1264` overwrites row
fields before maps run — mangling is load-bearing, not paranoia). Its
surface is the most predictable (one bit: `IsMapped`), and its uniform
shape-preserving `modify` **structurally eliminates** the worst hole found
in any candidate: a live-key `modify` after map-then-include that re-lands
through a bare intersection, minting `never`-property garbage on a fully
legal path with no error. Because post-map keyed `modify` returns `this`,
the output never re-lands and no intersection is ever asked to overwrite.

What the losing candidates contributed or took to the grave:

- **Non-terminal map via `SealedKeys` + a live/sealed `modify` overload
  partition — rejected.** The re-landing hole has no fix inside its "one new
  bag field" thesis; the machinery (a bag field threaded through ~12
  transforms, an overload partition, two constraint families) serves only
  the off-flagship path and produced the worst error message of the round.
  Its **order-freedom insight is adopted**: `relate*` is callable after
  `map()` — declarations check `BaseQuery["O"]`, which maps never touch.
- **Interface unification via poisoned parameters (deferred-`IsMapped`
  conditionals on one merged interface) — rejected.** Generic-context
  behavior of deferred poisons risks regressing today's unmapped
  `mapFields`/`omit`/`with` inside every generic helper, and discoverability
  regresses (methods present but unavailable). The tier split gives honest
  autocomplete; its inference risk was spiked (M1) rather than assumed — and
  the spike indeed forced the seven-member repair.
- **Structural-compatibility map with a non-enumerable `{keep}` column
  carry — dead.** Condemned three ways: the non-enumerable copy recreates,
  one spread later, the exact silent-empty-collection failure it was
  designed to avoid (spread/`Object.assign`/`structuredClone` drop
  non-enumerables while TS's spread type keeps them; every parent silently
  gets `[]`). A type-system lie with silent wrong results. No replacement
  needed: the parent-side required-column set is provably **empty** (that
  candidate's own correct discovery — attach matching reads raw rows, not
  mapped outputs), and children that transform match columns are gated
  ([§3.4](#34-type-transforms-and-guards)).
- **Output-type-dependent dispatch (map availability computed from the
  user's function's return type) — rejected.** Branded primitives, nullable
  unions, `any`, and unions all misclassify; availability becomes a property
  of the user's function rather than the chain. This design keeps one mapped
  tier and moves the object-ness check into the **branded key parameter**,
  where the failure names its cause at the point of use.
- **`mapBase` (a declaration-side canonical-map verb alongside terminal
  `map()`) — dead.** Verb redundancy is the most expensive coherence
  failure: the README's documented idiom `.map((row) => new User(row))`
  would still compile and silently foreclose includability, and the fix is a
  verb the user has no reason to know exists. For the flagship the two verbs
  coincide — so `map()` itself absorbs the job.
- **Optional relation slots (`class User { posts?: Post[] }`) — rejected by
  decision, with the reason in the error text.** Verified (tsc-checked):
  the slot degrades array-callback typing through intersection
  call-signature resolution, and it reimports the `| undefined` mega-type
  the relations design's founding commitment bans.
- **`mapRelation` landing fragments (per-relation map fragments composed per
  include-combination) — deferred, not adopted.** The one genuinely novel
  rejected mechanism, and separable — but fragments silently erase
  nested-include type precision, are strategy/order-sensitive in what their
  `parent` sees, and normalize child mutation under shared-instance attach
  semantics. Nothing in this design forecloses adding fragments later; a
  future design must solve generic fragments (type-level re-application)
  first.
- **A staged hydrator (moving the pipeline into `src/hydrator.ts`) —
  rejected.** Rewrites the hottest loop, requires dismantling the seven-site
  `asFullHydrator` invariant, and carries an auto-fields prefix-leak class.
  Output stages at the query-set level deliver the same semantics with the
  hydrator file untouched (two additive methods aside).
- **Include-aware map (`map((row, included) => …)`), execute-time deferred
  checking, branded attach-safety markers — rejected** (unsound type
  application / error locality / type lies). **Copying child instances under
  attach** (to erase Exception 6) — rejected on cost; the purity clause plus
  the pinned `===` divergence is the honest contract.

### Type-level decisions

- **Tier-split graft transforms with inline intersections; no shared
  `CollectionLanding` conditional.** A stored conditional violates printer
  rule 2 with no in-place fix (its supertype is `any`, so the rule-2
  intersection trick cannot apply). The twins are printer-rule-safe by
  construction; unmapped transforms stay byte-identical to today. *Evidence:*
  review; M6 (zero unevaluated aliases in emitted d.ts).
- **Seven tier-exact returns; standing rule 7.** *Rejected:* deferred
  `MaybeMappedQuerySet` returns on the shared tier (TS2589 on every
  relate/join/child-inference call — reproduced on demand, M1). A
  pre-approved fallback (a sibling-branch hierarchy) was **retired
  unexercised** — the repaired split passed everything.
- **`$narrowType` as the single rule-7 exception, with `HasNarrowError`
  validity dispatch.** *Rejected:* (a) a per-tier override — TS2430-
  inexpressible (invariance-incompatible bags); (b) the bare intersection
  return — it silently swallowed invalid-narrow errors (two
  `@ts-expect-error` directives went unused, M2). The dispatch keeps today's
  loudness for invalid narrows and class identity for valid ones. Gated
  TG-1, with a pre-authorized fallback (mapped tier keeps today's structural
  form + a documented structural-under-claim ledger entry).
- **Class-name-first intersection order.** M2 proved freshness, not order,
  is the invariant; graft-first is the pre-validated fallback, retained but
  not expected to be needed.
- **Parameter-position brand for `GraftableKey`.** Constraint position
  pinned worse (M2). The primitive-detect arm and the non-distributive
  membership arm are spike/review-forced repairs ([§3.4](#34-type-transforms-and-guards)).
- **`PreservesShape` in generic-constraint form.** *Rejected:* a
  fixed-callback-return target — against the invariant
  `MappedQuerySet<in out T>` it either rejects legal refinements or checks
  vacuously through `any`-collapse. The constraint form is the only
  variance-sound spelling; M5 proved it non-vacuous and field-naming on
  failure. The `TypeErrorMessage` resolution arm for its failures is a
  ruled **no-go** (M5: raw text clears the decodability bar; a message arm
  would mask the field-naming tail).
- **`PreservesFetchValue` as the bare `FetchReturnOf<Element>` union** — the
  `SomeFetchFnReturn` intersection form misdirects error elaboration against
  the wrong member (M5).
- **`EachHydratedRawOverlapOk` as a named distributed alias; inline
  forbidden.** The inline composite form compiles and silently reopens the
  gate hole under both tsc and tsgo (M4) — promoted to standing rule 6.
- **`$assertType` keeps exact-equality semantics on all tiers**, tier-exact
  returns; no intersection form (it re-types nothing on success); post-map
  the assertion target is the current mapped/grafted output.

### Runtime & semantics decisions

- **`#effectiveHydrator` is NOT memoized** — composed fresh per `hydrate()`
  call. *Rejected:* the memoized form — it makes the shared-output guard's
  WeakSet lifetime span executions, so a cross-execution memoized map would
  either false-positive or require hydrator surgery. Fresh composition is
  the working implementation of "per-hydration" at the parent level
  (M7-proven, cost negligible); the child-pipeline residual is disclosed
  ([§3.5](#35-runtime-design-the-output-stage-pipeline)).
- **The shared-output guard ships, scoped to graft-receiving stages.**
  Deletion was argued for under review and **rejected on spike evidence**:
  the 60-test M7 matrix shows the guard firing with the correct,
  cause-naming error at the earliest sound moment and zero false positives
  under the corrected scope; `GraftCollisionError` remains the backstop but
  reports one entity later with a collision framing. The corrected scope
  (guard only stages with `graftKeys.length > 0`) structurally removes the
  two real false-positive chains the review found — trailing scalar maps
  (`weakSet.add(primitive)` would `TypeError`) and attach-shared instances
  returned by graft-less later stages (strategy-asymmetric spurious throw).
  Fixture RG-1.
- **The mangling boundary is M3's tested boundary.** Two review lenses
  independently proposed the opposite reading (real keys in the props maps,
  mangled only at the hydrator or SQL prefix); both are contradicted by the
  working spike, whose mangled-store-key registration + mangle-aware
  `orderBy` + per-level entity scan passed the full suite *including* the
  failure scenarios predicted for it. Spikes win.
- **`graftAssign` ordering: accessor-before-collision.** An accessor
  anywhere below `Object.prototype` → `GraftTargetError` (grafting through
  accessors is forbidden; setter invocation count asserted 0); a data
  property → `GraftCollisionError`. A draft ordering that walked data
  properties first misclassified prototype getters (M7).
- **Pluck table is a `Map`**, not a record — `Object.prototype.__proto__`
  is an accessor, so a record-based pluck breaks on a graft keyed
  `"__proto__"` (M7).
- **Reserved-namespace guard as a per-level hydrated-entity-key scan** — a
  funnel-level scan of the first row's keys misses join-included children
  with their own stages (M3; review-converged). The mis-attribution corner
  (a user column spelled as a mangled child column group) is the documented
  residual.
- **Zero-match backstop: wide scope, cause-enumerating message.** The
  premise "zero matches ⇒ a child map transformed the key" is false under
  collation-insensitive SQL equality — so the message enumerates the cause
  classes instead of blaming one. Scope is kept wide (all query-backed
  attach relations, mapped or not) because the firing condition always
  proves a real join/attach divergence; loud beats silently-empty relations
  — zero-compromise correctness outranks quiet compatibility. Placement
  before `applyWindow` is pinned (a per-batch offset must not be misread as
  zero-match). Fixture RG-2 adds the NOCASE case.
- **`RelationKeyConsumedError` with two message branches; the error-class
  split by entry path is deliberate.** The include path throws
  `RelationAlreadyIncludedError` (checking both key forms); the sugar/graft
  path throws `RelationKeyConsumedError` ("consumed" / "grafted"). A third
  type-level guard branch distinguishing grafted keys was **not adopted** —
  it needs a pre/post-map `Collections` discriminant the zero-bag-field
  thesis forbids, and the reworded single message is honest for both cases.
- **`with(MappedHydrator)` returns `MappedQuerySet<TMapped<T, OtherOutput>>`.**
  The honest output is the mapped hydrator's final output; the old declared
  `TWithExtendedOutput` was a lie against the old runtime too (M8).
- **A `GraftCollision` slot-discriminator refinement**
  (`Out[Key] extends readonly unknown[] | undefined` to tailor the message)
  is **deferred to implementation as optional** — it must not complicate the
  guard, and the final text is pinned by raw-capture fixtures either way.

---

## 5. Validation

Every mechanism above was validated by eight compile/run spikes (M1–M8)
against real kysely 0.28.8, TypeScript 5.9.3, and this repo's strict
tsconfig — M3, M7, and M8 against working implementations in the repo,
running the full test suite. No spike failed; no pre-approved fallback
(sibling-branch hierarchy, graft-first intersection order) was needed; every
"pass with changes" delta is folded into [§3](#3-full-specification).
**Spike code currently lives untracked under `spikes-tmp/`**
(`map-m1/`, `map-m2/` — which also hosts the M5 fixtures, `m5-*.ts` —
`map-m4/`, `map-m6/`) **plus two agent worktrees under `.claude/worktrees/`**
(`agent-a761d09a47f51bd39` — M3; `agent-a8029b7a0df5d5e8c` — M7/M8), all
uncommitted and pending review.

| Spike | What was compiled/executed | What it proved / forced |
|---|---|---|
| **M1** — hierarchy split under inference load | Full S5/S2 batteries + mapped replay + circular shapes over the three-tier lib | Green (14.6M instantiations, 0 TS2589) — but only after the **seven-member tier-exact-return repair**; the deferred-conditional split dies TS2589 on every relate/join/child-inference call (`tsconfig.prerepair.json` reproduces it). Forced standing rule 7; retired the sibling-branch fallback; pinned the consumed-key rendering split and the eager tier-vs-tier TS2589 residual. |
| **M2** — graft intersections, guards, hover/order | Depth-3 class grafts, nominal identity, array methods, DTO sandwich, d.ts hover; 29/29 raw negative captures | All exact. Forced the primitive-detect arm, the `$narrowType` respelling (`HasNarrowError` dispatch), the consumed-key rewording, the collision-message trim, and the transform-alias export prerequisite. Confirmed class-first order; pre-validated the graft-first fallback. |
| **M3** — output-stage runtime, ordering bug, dispatch | Working implementation; 565/565 sqlite green; stage-0 byte-parity oracle | The ordering bug reproduced on main and fixed by mangling; all five dispatch rows + both-key-form membership proven; made the mangling boundary normative; forced `FullHydrator.withoutCollection()`, the per-level reserved-namespace scan, the two-branch `RelationKeyConsumedError`, and the load-bearing row-shaper guard; found and fixed the stale-fetchFn bug ([§7](#7-related-fixes-discovered)). |
| **M4** — mapped-child attach gate + backstop + equivalence | Gate polarity matrix (tsc + tsgo); backstop 8/8 incl. all three non-firing arms; join-vs-attach deep-equal property test | The gate closes the settled design's own hole (`_hole_settled` vs `_hole_closed` pinned); the composite conjunct **must** ship as the named distributed alias — the inline form silently reopens the hole; equivalence deep-equal at every `PARAM_BUDGET`, with `===` divergence and constructor counts pinned exactly; gate amendment safe to land ahead (S1/S11/S7 non-regression). |
| **M5** — `PreservesShape` modify mechanics | Constraint-form positive/negative matrix, composites, degenerate floor (`spikes-tmp/map-m2/m5-*.ts`) | Not vacuous; subsumes the `Columns`-keep incl. composites; drops/re-typings rejected with field-naming tails. Forced the bare-union `PreservesFetchValue` and the `[never]`-guard arm in `DeclaredElementShape`; ruled the `TypeErrorMessage` arm a no-go. |
| **M6** — scale & d.ts emission | The relations S6 workload, with maps, under `--declaration` | **PASS unconditional**: instantiations 0.56× the relations-only baseline on a superset workload (245,379 vs 436,953); TS7056 cliff unmoved (depth 11 both variants, floor 8); zero unevaluated guard aliases in emitted d.ts; +0.14% isolated graft-landing cost; executed-row hover *improves*. Pinned: non-exported classes synthesize a local `declare class` (advisory, not an error); the generic-wrapper brand recipe; TS7056 kills the whole file's d.ts (docs footnote). |
| **M7** — graft mutation contract matrix | 45/45 runtime matrix (targets, accessors, frozen, `__proto__`, Proxy, flyweights, memoization) | Every target-hazard cell loud and named (setter invocation count 0; flagship two-includes no-throw; memoized map → collision error naming memoization; `__proto__` inert; Proxy residual pinned). Forced: no memoization of `#effectiveHydrator`; accessor-before-collision ordering; the `Map` pluck table; no `RelationAlreadyIncludedError` on the graft path. |
| **M8** — `with(MappedHydrator)` fold | 15/15: fold end-to-end; full-hydrator invariant on every constructible path; `executeCount` unaffected | Return type pinned to `MappedQuerySet<TMapped<T, OtherOutput>>` (one test-d fixture flips; the old declared type was a lie against the old runtime); hydrator-level `with` untouched; `RowShaperAfterMapError` minted. |

M-DOC (the teachability draft) produced no report in the validation round;
its obligations are enumerated in [§3.9](#39-documentation-obligations-m-doc-outstanding)
and carried as an open implementation-phase deliverable.

---

## 6. Remaining risks & implementation-phase tests

Each risk names the test that settles it. TG = type gate, RG = runtime gate.

| # | Risk | Test that settles it |
|---|---|---|
| TG-1 | `$narrowType`'s base-tier `IsMapped`-conditional return is the one sanctioned exception to standing rule 7; M2 compiled it in a lean lib but the full inference battery never ran over it (M1 ran tier-exact members) | Re-run the full M1 battery (`spikes-tmp/map-m1/` fixtures) with the M2 spelling installed in the repo-verbatim lib; any TS2589 → pre-authorized fallback: mapped-tier structural form + structural-under-claim ledger entry |
| TG-2 | Output-generic wrappers: `[U] extends [object]` defers; hardcoded keys uncallable | Fixture: `withPosts<U extends {id: number}>(qs: MappedQuerySet<TMapped<TUsers, U>>)` — brand-threading recipe compiles; hardcoded-key rejection pinned + ledger disclosure text |
| TG-3 | The non-distributive consumed-key membership (`[Extract<Key, keyof Collections & string>] extends [never]`) is a formula delta no spike compiled | Union-key sugar fixture: `"posts" \| "extra"` with `"posts"` consumed → compile error; legal union keys unregressed; runtime `RelationKeyConsumedError` backstop retained as a paired runtime test |
| RG-1 | Shared-output guard scope (graft-receiving stages only) — the two false-positive chains were never run | Must-NOT-throw, both strategies: trailing scalar map (`map(User).include(k).map(u => u.displayName)`); attach-shared instance returned by a graft-less later stage (`map(User).include("profile","attach").map(u => u.profile)` with a non-unique match column) |
| RG-2 | Backstop under collation-insensitive equality; placement | SQLite `COLLATE NOCASE` match column, no map anywhere → throws `RelationMatchIntegrityError` whose text names the coercion class; per-batch `offset` ≥ entity count → no throw (pre-window placement) |
| RG-3 | Nested join-child `$graft$$` user column | Child with stages + a base-query column aliased `$graft$$x` hoisted under the parent → per-level scan throws `ReservedColumnNameError`; the mis-attribution corner documented |
| RG-4 | Debug-surface demangling | `includedRelations()` on a mapped parent with post-map includes prints real keys at every nesting level |
| RG-5 | Double post-map include + corrected message texts | `map().include("posts").include("posts")` → include-path key-domain rejection (types) / `RelationKeyConsumedError("grafted")` (JS); raw captures of the reworded consumed-key and trimmed collision brands (fixtures assert actual compiler output before ship) |
| RG-6 | Child-level guard-state persistence (M7 residual) | Memoized *child* map with grafts across two parent executions → `SharedMapOutputError` (loud); pinned + documented |
| RG-7 | `writeAs` full-hydrator invariant (M8 covered sqlite paths only) | pg-suite fixture: `writeAs` → map → collection verb executes; `isFullHydrator(props.hydrator)` after every construction path incl. writes |
| RG-8 | Real-bag scale (M6's mini-bag omits `DrainOuterGeneric` etc.; M1 measured 14.6M instantiations on a 334-file spike) | Re-run the M6 workload against the real `src/query-set.ts` types as the implementation acceptance gate (extends relations risk 12); CI greps: M6's emission list + `MaybeMappedQuerySet<` in `ExecutableQuerySet` signatures |
| RG-9 | Postgres coverage (inherits relations risk 9) | Full `npm run test:all` incl. row-value `IN` and pg laterals before merge |
| — | Documented residuals (no test possible): partial same-type transforms; Form-C fetch mismatches (M4-pinned on today's library); Proxy set-trap swallow (M7-pinned); `$graft$$`-spelled child-group alias collision (M3); TS2589 noise on eager tier-vs-tier comparisons (M1); type-side any-leak on `any`/index-signature outputs | Honesty-ledger entries, each with the pinning fixture already written in a spike |
| — | M-DOC page unwritten | Ships with implementation ([§3.9](#39-documentation-obligations-m-doc-outstanding)); the "can't write it cleanly = design signal" clause stands |

Risk-register carryover from the validation round: the type-feasibility and
emission risks are **closed** (M6 unconditional); the runtime-parity and
fold risks closed (M3/M8); the equivalence, mutation-contract, and guard
risks closed with the disclosed residuals above (M4/M7); the hierarchy risk
closed modulo TG-1 (M1); the error-text risks closed by raw captures
(M2/M5).

---

## 7. Related fixes discovered

Fixes to *existing* code and documents discovered while validating this
design:

1. **Stale attach fetchFn on pre-map cross-kind overwrite (P3 extension /
   M3).** Pre-existing latent bug on never-mapped chains: a pre-map
   cross-kind overwrite (`attachMany("x", …).leftJoinMany("x", …)`) leaves
   the stale attach fetchFn registered and running — spy-asserted on main,
   fixed in the M3 worktree by P3's both-key-forms/both-maps cleanup.
   Release-note item.
2. **The settled attach gate's hole for terminally-mapped declared children
   (M4).** The relations design's own `SingleAttachOk`/`CompositeAttachOk`
   accepted a declared child whose terminal map re-types a match column —
   compile-clean, silently-empty collections at runtime. Closed by the
   `HydratedRawOverlapOk` conjunct (applied to relations.md §3.4); safe to
   land ahead of this feature (non-regression proven).
3. **`with(MappedHydrator)`'s declared type was a lie (M8).** The overload
   at `src/query-set.ts:1329-1331` declared `TWithExtendedOutput` while the
   runtime threw — the declared type was wrong against the *old* runtime
   too. Now typed honestly (`TMapped<T, OtherOutput>`) and working; one
   test-d fixture flips.
4. **The row-shaper post-map protection was accidental (M3).** Today's
   post-map `extras`/`mapFields`/`omit`/`with` throw fell out of the props
   hydrator being emptied by `map()`; with the props hydrator now always
   full, the guard is re-minted deliberately as `RowShaperAfterMapError` —
   load-bearing, not cosmetic.
