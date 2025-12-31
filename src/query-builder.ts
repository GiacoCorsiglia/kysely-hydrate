import * as k from "kysely";

import {
	type ApplyPrefixes,
	hasAnyPrefix,
	type MakePrefix,
	makePrefix,
} from "./helpers/prefixes.ts";
import { prefixSelectArg } from "./helpers/select-renamer.ts";
import { type Extend, type KeyBy, type StrictSubset } from "./helpers/utils.ts";
import {
	type AttachedKeysArg,
	type CollectionMode,
	DEFAULT_KEY_BY,
	ensureFields,
	type Extras,
	type FetchFn,
	type FieldMappings,
	type FullHydrator,
	type InferExtras,
	type InferFields,
	createHydrator,
	type Hydrator,
	type InputWithDefaultKey,
	type MappedHydrator,
	asFullHydrator,
} from "./hydrator.ts";

////////////////////////////////////////////////////////////////////
// Interfaces.
////////////////////////////////////////////////////////////////////

/**
 * Super type to HydratedQueryBuilder, that has already had a .map() added,
 * meaning it's no longer safe to continue to chain methods like .mapFields() or
 * .hasMany(), because those will affect the input type expected by the
 * transformation function applied to .map().
 *
 * The generic parameters are the same as HydratedQueryBuilder.
 */
interface MappedHydratedQueryBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalDB,
	LocalRow,
	HydratedRow,
	IsNullable extends boolean,
	HasJoin extends boolean,
> {
	/**
	 * This property exists for complex type reasons and will never be set.
	 *
	 * @internal
	 */
	// Required so that the type system can infer all the generics the even when
	// nested collections return a MappedHydratedQueryBuilder instead of a
	// HydratedQueryBuilder.
	readonly _generics:
		| {
				Prefix: Prefix;
				QueryDB: QueryDB;
				QueryTB: QueryTB;
				QueryRow: QueryRow;
				LocalRow: LocalRow;
				HydratedRow: HydratedRow;
				IsNullable: IsNullable;
				NestedDB: LocalDB;
				HasJoin: HasJoin;
		  }
		| undefined;

	/**
	 * Allows you to modify the underlying select query.  Useful for adding
	 * `where` clauses.  Adding additional SELECTs here is forbidden.
	 *
	 * For example:
	 *
	 * ```ts
	 * hydrate(...).modify((qb) => qb.where("isActive", "=", "true"))
	 * ```
	 */
	modify(
		modifier: (
			qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
		) => k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
	): MappedHydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ HydratedRow,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Returns the raw underlying Kysely select query builder.
	 * Useful for debugging or when you need direct access to the query.
	 */
	toQuery(): k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>;

	/**
	 * Executes the query and returns an array of rows.
	 *
	 * Also see the {@link executeTakeFirst} and {@link executeTakeFirstOrThrow} methods.
	 */
	execute(): Promise<k.Simplify<HydratedRow>[]>;

	/**
	 * Executes the query and returns the first result or undefined if the query
	 * returned no result.
	 */
	executeTakeFirst(): Promise<k.Simplify<HydratedRow> | undefined>;

	/**
	 * Executes the query and returns the first result or throws if the query
	 * returned no result.
	 *
	 * By default an instance of {@link k.NoResultError} is thrown, but you can
	 * provide a custom error class, or callback to throw a different error.
	 */
	executeTakeFirstOrThrow(
		errorConstructor?: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error),
	): Promise<k.Simplify<HydratedRow>>;

	/**
	 * Applies a transformation function to the hydrated output.
	 *
	 * This is a terminal operation: after calling `.map()`, only `.map()` and
	 * `.execute()` are available; you cannot continue to chain methods that
	 * affect the input type expected by the transformation function.
	 *
	 * Use this for more complex transformations, such as:
	 * - Hydrating into class instances
	 * - Asserting discriminated union types
	 * - Complex data reshaping
	 *
	 * For simple field transformations, prefer `.fields()` or `.extras()`.
	 *
	 * @param transform - A function that transforms the hydrated output
	 * @returns A MappedHydratedQueryBuilder with the transformation added
	 */
	map<NewHydratedRow>(
		transform: (row: k.Simplify<HydratedRow>) => NewHydratedRow,
	): MappedHydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ NewHydratedRow,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;
}

/**
 * A query builder that supports both mapping and nested joins.
 *
 * @template Prefix The prefix applied to select aliases.
 * @template QueryDB The `DB` generic for the *entire* select query, including
 * parent queries.  Passed as `k.SelectQueryBuilder<QueryDB, ...>`
 * @template QueryTB The `TB` generic for the *entire* select query, including
 * parent queries.  Passed as `k.SelectQueryBuilder<..., QueryTB, ...>`
 * @template QueryRow The `O` generic for the *entire* select query, including
 * parent queries.  Passed as `k.SelectQueryBuilder<..., QueryRow>`.  This is
 * the row shape returned by `query.execute()`.
 * @template LocalDB A `DB` generic for the select query that suppresses
 * nullability of left joins so that the type of nested objects is correctly
 * non-nullable.
 * @template LocalRow The unprefixed row shape that `query.execute()` *would*
 * return if this was the topmost query, ignoring parent queries.
 * @template HydratedRow The final, local output shape of each row, after joins
 * have been applied.  Ignores parent queries.
 * @template IsNullable Whether the hydrated row resulting from this join should
 * be nullable in its parent.
 * @template HasJoin Preserves whether this join builder has already had a join
 * added, which affects the nullability of this relation when adding more joins.
 */
interface HydratedQueryBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalDB,
	LocalRow,
	HydratedRow,
	IsNullable extends boolean,
	HasJoin extends boolean,
> extends MappedHydratedQueryBuilder<
	Prefix,
	QueryDB,
	QueryTB,
	QueryRow,
	LocalDB,
	LocalRow,
	HydratedRow,
	IsNullable,
	HasJoin
> {
	// The same as the parent's modify method, but preserves the
	// HydratedQueryBuilder return type.
	modify(
		modifier: (
			qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
		) => k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ HydratedRow,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Configures extra computed fields to add to the hydrated output.
	 * Each extra is a function that receives the full row (with prefixed columns
	 * available as accessors) and returns a computed value.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]),
	 *   "id",
	 * )
	 *   .extras({
	 *     fullName: (row) => `${row.firstName} ${row.lastName}`,
	 *   })
	 *   .execute();
	 * // Result: [{ id: 1, firstName: "Alice", lastName: "Smith", fullName: "Alice Smith" }]
	 * ```
	 *
	 * @param extras - An object mapping field names to functions that compute
	 *   the field value from the entire row.
	 * @returns A new HydratedQueryBuilder with the extras applied.
	 */
	extras<E extends Extras<LocalRow>>(
		extras: E,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, InferExtras<LocalRow, E>>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Transforms already-selected field values in the hydrated output.  Fields
	 * not mentioned in the mappings will still be included as-is.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.name"]),
	 *   "id",
	 * )
	 *   .mapFields({
	 *     name: (name) => name.toUpperCase(),
	 *   })
	 *   .execute();
	 * // Result: [{ id: 1, name: "ALICE" }]
	 * ```
	 *
	 * @param mappings - An object mapping field names to transformation
	 * functions.
	 * @returns A new HydratedQueryBuilder with the field transformations applied.
	 */
	mapFields<M extends FieldMappings<LocalRow>>(
		mappings: M,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, InferFields<LocalRow, M>>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Omits specified fields from the hydrated output.  Useful for excluding
	 * fields that were selected for internal use (e.g., for extras).
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]),
	 *   "id",
	 * )
	 *   .extras({
	 *     fullName: (row) => `${row.firstName} ${row.lastName}`,
	 *   })
	 *   .omit(["firstName", "lastName"])
	 *   .execute();
	 * // Result: [{ id: 1, fullName: "Alice Smith" }]
	 * ```
	 *
	 * @param keys - Field names to omit from the output.
	 * @returns A new HydratedQueryBuilder with the fields omitted.
	 */
	omit<K extends keyof LocalRow>(
		keys: readonly K[],
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Omit<HydratedRow, K>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Extends this query builder's hydration configuration with another Hydrator.
	 * The other Hydrator's configuration takes precedence in case of conflicts.
	 *
	 * Both hydrators must have the same `keyBy`, and the other Hydrator's input
	 * type must be a subset of the query's LocalRow (all fields in OtherInput
	 * must exist in LocalRow with compatible types).
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const extraFields = createHydrator<User>("id")
	 *   .fields({ email: true })
	 *   .extras({ displayName: (u) => `${u.name} <${u.email}>` });
	 *
	 * const users = await hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.name", "users.email"]),
	 *   "id",
	 * )
	 *   .with(extraFields)
	 *   .execute();
	 * // Result: [{ id: 1, name: "Alice", email: "...", displayName: "Alice <...>" }]
	 * ```
	 *
	 * @param hydrator - The Hydrator to extend with.
	 * @returns A new HydratedQueryBuilder with merged hydration configuration.
	 */
	with<OtherInput extends StrictSubset<LocalRow, OtherInput>, OtherOutput>(
		hydrator: FullHydrator<OtherInput, OtherOutput>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, OtherOutput>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;
	// If you pass a Hydrator with a map applied, we must return a
	// MappedHydratedQueryBuilder.
	with<OtherInput extends StrictSubset<LocalRow, OtherInput>, OtherOutput>(
		hydrator: MappedHydrator<OtherInput, OtherOutput>,
	): MappedHydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, OtherOutput>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Adds a join to the query that will hydrate into a nested collection.
	 *
	 * **Example:**
	 * ```ts
	 * const userWithPosts = hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.email"]),
	 *   "id",
	 * ).hasMany(
	 *   "posts",
	 *   ({ leftJoin }) =>
	 *     leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
	 *   "id",
	 * ).execute();
	 *
	 * // Result: [{ id: 1, email: "abc@xyz.co", posts: [{ id: 1, title: "Post 1" }, ...]}]
	 * ```
	 * The output SQL is identical to the following:
	 * ```sql
	 * SELECT users.id, users.email, posts.id as posts$$id, posts.title as posts$$title
	 * FROM users
	 * LEFT JOIN posts ON posts.user_id = users.id
	 * ```
	 * Notably, the selections made within the nested join are prefixed with the
	 * `key`, but otherwise the SQL is exactly the same as it would be if you
	 * wrote a flat query without the `hydrate` helper.
	 *
	 * @param key - The key name for the collection in the output.
	 * @param jb - A function that returns a new HydratedQueryBuilder for the
	 * nested collection.
	 * @param keyBy - The key(s) on the nested collection to uniquely identify
	 * those entities. Defaults to "id" if the nested row type has an "id" property.
	 * @returns A new HydratedQueryBuilder with the nested collection added.
	 */
	// Overload 1: keyBy provided - any nested row type
	hasMany<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow,
		NestedHydratedRow,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			// We don't care about nullability for joinMany().
			/* IsNullable:  */ any,
			/* HasJoin:     */ any
		>,
		keyBy: KeyBy<NestedLocalRow>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: NestedHydratedRow[] }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;
	// Overload 2: keyBy omitted - nested LocalRow must have 'id'
	hasMany<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow extends InputWithDefaultKey,
		NestedHydratedRow,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			// We don't care about nullability for joinMany().
			/* IsNullable:  */ any,
			/* HasJoin:     */ any
		>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: NestedHydratedRow[] }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Adds a join to the query that will hydrate into a single nested object.
	 * The object will be nullable if you use a left join, and non-nullable if you
	 * use an inner join.
	 *
	 * **Example:**
	 * ```ts
	 * const userWithPosts = hydrate(
	 *   db.selectFrom("posts").select(["posts.id", "posts.title"]),
	 *   "id",
	 * ).hasOne(
	 *   "user",
	 *   ({ innerJoin }) =>
	 *     innerJoin("users", "users.id", "posts.user_id").select(["users.id", "users.email"]),
	 *   "id",
	 * ).execute();
	 *
	 * // Result: [{ id: 1, title: "Post 1", user: { id: 1, email: "abc@xyz.co" } }]
	 * ```
	 * The output SQL is identical to the following:
	 * ```sql
	 * SELECT posts.id, posts.title, users.id as users$$id, users.email as users$$email
	 * FROM posts
	 * INNER JOIN users ON users.id = posts.user_id
	 * ```
	 * Notably, the selections made within the nested join are prefixed with the
	 * `key`, but otherwise the SQL is exactly the same as it would be if you
	 * wrote a flat query without the `hydrate` helper.
	 *
	 * @param key - The key name for the collection in the output.
	 * @param jb - A function that returns a new HydratedQueryBuilder for the
	 * nested collection.
	 * @param keyBy - The key(s) on the nested collection to uniquely identify
	 * those entities. Defaults to "id" if the nested row type has an "id" property.
	 * @returns A new HydratedQueryBuilder with the nested collection added.
	 */
	// Overload 1: keyBy provided - any nested row type
	hasOne<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow,
		NestedHydratedRow,
		IsChildNullable extends boolean,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			/* IsNullable:  */ IsChildNullable,
			/* HasJoin:     */ any
		>,
		keyBy: KeyBy<NestedLocalRow>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<
			HydratedRow,
			{ [_ in K]: IsChildNullable extends true ? NestedHydratedRow | null : NestedHydratedRow }
		>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;
	// Overload 2: keyBy omitted - nested LocalRow must have 'id'
	hasOne<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow extends InputWithDefaultKey,
		NestedHydratedRow,
		IsChildNullable extends boolean,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			/* IsNullable:  */ IsChildNullable,
			/* HasJoin:     */ any
		>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<
			HydratedRow,
			{ [_ in K]: IsChildNullable extends true ? NestedHydratedRow | null : NestedHydratedRow }
		>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Exactly like {@link hasOne}, but throws an error if the nested object is not found.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param jb - A function that returns a new HydratedQueryBuilder for the nested object.
	 * @param keyBy - The key(s) on the nested object to uniquely identify it.
	 *   Defaults to "id" if the nested row type has an "id" property.
	 * @returns A new HydratedQueryBuilder with the nested object added.
	 */
	// Overload 1: keyBy provided - any nested row type
	hasOneOrThrow<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow,
		NestedHydratedRow,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			/* IsNullable:  */ any,
			/* HasJoin:     */ any
		>,
		keyBy: KeyBy<NestedLocalRow>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: NestedHydratedRow }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;
	// Overload 2: keyBy omitted - nested LocalRow must have 'id'
	hasOneOrThrow<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalDB,
		NestedLocalRow extends InputWithDefaultKey,
		NestedHydratedRow,
	>(
		key: K,
		jb: (
			nb: HydratedQueryBuilder<
				/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
				/* QueryDB:     */ QueryDB,
				/* QueryTB:     */ QueryTB,
				/* QueryRow:    */ QueryRow,
				/* LocalDB:     */ LocalDB,
				/* LocalRow:    */ {}, // LocalRow is empty within the nesting.
				/* HydratedRow: */ {}, // HydratedRow is empty within the nesting.
				/* IsNullable:  */ false,
				/* HasJoin:     */ false
			>,
		) => MappedHydratedQueryBuilder<
			/* Prefix:      */ MakePrefix<Prefix, NoInfer<K>>,
			/* QueryDB:     */ JoinedQueryDB,
			/* QueryTB:     */ JoinedQueryTB,
			/* QueryRow:    */ JoinedQueryRow,
			/* LocalDB:     */ NestedLocalDB,
			/* LocalRow:    */ NestedLocalRow,
			/* HydratedRow: */ NestedHydratedRow,
			/* IsNullable:  */ any,
			/* HasJoin:     */ any
		>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ JoinedQueryDB,
		/* QueryTB:     */ JoinedQueryTB,
		/* QueryRow:    */ JoinedQueryRow,
		/* LocalDB:     */ NestedLocalDB,
		/* LocalRow:    */ LocalRow & ApplyPrefixes<MakePrefix<"", K>, NestedLocalRow>,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: NestedHydratedRow }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Attaches data from an external source (not via SQL joins) as a nested array.
	 * The `fetchFn` is called exactly once per query execution with all parent rows
	 * to avoid N+1 queries.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await hydrate(
	 *   db.selectFrom("users").select(["users.id", "users.name"]),
	 *   "id",
	 * ).attachMany(
	 *   "posts",
	 *   async (userRows) => {
	 *     const userIds = userRows.map((u) => u.id);
	 *     return db.selectFrom("posts")
	 *       .select(["posts.id", "posts.userId", "posts.title"])
	 *       .where("posts.userId", "in", userIds)
	 *       .execute();
	 *   },
	 *   { keyBy: "userId" },
	 * ).execute();
	 * ```
	 *
	 * @param key - The key name for the array in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new HydratedQueryBuilder with the attached collection added.
	 */
	attachMany<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		keys: AttachedKeysArg<LocalRow, AttachedOutput>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: AttachedOutput[] }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Attaches data from an external source (not via SQL joins) as a single nested object.
	 * The object will be nullable. The `fetchFn` is called exactly once per query execution
	 * with all parent rows to avoid N+1 queries.
	 *
	 * **Example:**
	 * ```ts
	 * const posts = await hydrate(
	 *   db.selectFrom("posts").select(["posts.id", "posts.title"]),
	 *   "id",
	 * ).attachOne(
	 *   "author",
	 *   async (postRows) => {
	 *     const userIds = [...new Set(postRows.map((p) => p.userId))];
	 *     return db.selectFrom("users")
	 *       .select(["users.id", "users.name"])
	 *       .where("users.id", "in", userIds)
	 *       .execute();
	 *   },
	 *   { keyBy: "id", compareTo: "userId" },
	 * ).execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new HydratedQueryBuilder with the attached object added.
	 */
	attachOne<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		keys: AttachedKeysArg<LocalRow, AttachedOutput>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: AttachedOutput | null }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	/**
	 * Exactly like {@link attachOne}, but throws an error if the attached object is not found.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new HydratedQueryBuilder with the attached object added.
	 */
	attachOneOrThrow<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		keys: AttachedKeysArg<LocalRow, AttachedOutput>,
	): HydratedQueryBuilder<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ Extend<HydratedRow, { [_ in K]: AttachedOutput }>,
		/* IsNullable:  */ IsNullable,
		/* HasJoin:     */ HasJoin
	>;

	// We omit RIGHT JOIN and FULL JOIN because these are not appropriate for ORM-style queries.

	/**
	 * Joins another table to the query using an `inner join`.
	 *
	 * Exactly like Kysely's {@link k.SelectQueryBuilder.innerJoin}, except
	 * contextualized to a {@link HydratedQueryBuilder}.  This method will add
	 * an `inner join` to your SQL in exactly the same way as Kysely's version.
	 */
	innerJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): HydratedQueryBuilderWithInnerJoin<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ HydratedRow,
		/* IsNullable:  */ IsNullable,
		TE
	>;
	innerJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): HydratedQueryBuilderWithInnerJoin<
		/* Prefix:      */ Prefix,
		/* QueryDB:     */ QueryDB,
		/* QueryTB:     */ QueryTB,
		/* QueryRow:    */ QueryRow,
		/* LocalDB:     */ LocalDB,
		/* LocalRow:    */ LocalRow,
		/* HydratedRow: */ HydratedRow,
		/* IsNullable:  */ IsNullable,
		TE
	>;

	/**
	 * Like {@link innerJoin}, but adds a `left join` instead of an `inner join`.
	 * Left joins make the joined table's columns nullable in the row type.
	 */
	leftJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): HydratedQueryBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		HasJoin,
		TE
	>;
	leftJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): HydratedQueryBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		HasJoin,
		TE
	>;

	/**
	 * Just like {@link innerJoin}, but adds a `cross join` instead of an `inner
	 * join`.
	 */
	crossJoin<TE extends k.TableExpression<QueryDB, QueryTB>>(
		table: TE,
	): HydratedQueryBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link innerJoin} but adds an `inner join lateral` instead of an
	 * `inner join`.
	 */
	innerJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): HydratedQueryBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;
	innerJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): HydratedQueryBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link leftJoin} but adds a `left join lateral` instead of a
	 * `left join`.
	 */
	leftJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): HydratedQueryBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		HasJoin,
		TE
	>;
	leftJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): HydratedQueryBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		HasJoin,
		TE
	>;

	/**
	 * Just like {@link innerJoin} but adds a `cross join lateral` instead of an
	 * `inner join`.
	 */
	crossJoinLateral<TE extends k.TableExpression<QueryDB, QueryTB>>(
		table: TE,
	): HydratedQueryBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalDB,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Adds a select statement to the query.
	 *
	 * Like Kysely's {@link k.SelectQueryBuilder.select} method, but aliases (or
	 * re-aliases) selected columns by prefixing them with the current prefix.
	 * This prefix is automatically applied when using nested joins via
	 * {@link hasMany} or {@link hasOne}.
	 */
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selections: ReadonlyArray<SE>,
	): HydratedQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		LocalDB,
		LocalRow & k.Selection<LocalDB, QueryTB & keyof LocalDB, SE>,
		HydratedRow & k.Selection<LocalDB, QueryTB & keyof LocalDB, SE>,
		IsNullable,
		HasJoin
	>;
	select<CB extends k.SelectCallback<QueryDB, QueryTB>>(
		callback: CB,
	): HydratedQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.CallbackSelection<QueryDB, QueryTB, CB>>,
		LocalDB,
		LocalRow & k.CallbackSelection<LocalDB, QueryTB & keyof LocalDB, CB>,
		HydratedRow & k.CallbackSelection<LocalDB, QueryTB & keyof LocalDB, CB>,
		IsNullable,
		HasJoin
	>;
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selection: SE,
	): HydratedQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		LocalDB,
		LocalRow & k.Selection<LocalDB, QueryTB & keyof LocalDB, SE>,
		HydratedRow & k.Selection<LocalDB, QueryTB & keyof LocalDB, SE>,
		IsNullable,
		HasJoin
	>;
}

// This is a laziness so that we can reuse the k.SelectQueryBuilderWith*Join types.
type InferDB<SQB extends k.SelectQueryBuilder<any, any, any>> =
	SQB extends k.SelectQueryBuilder<infer DB, any, any> ? DB : never;

type HydratedQueryBuilderWithInnerJoin<
	/* Prefix:      */ Prefix extends string,
	/* QueryDB:     */ QueryDB,
	/* QueryTB:     */ QueryTB extends keyof QueryDB,
	/* QueryRow:    */ QueryRow,
	/* LocalDB:     */ _LocalDB,
	/* LocalRow:    */ LocalRow,
	/* HydratedRow: */ HydratedRow,
	/* IsNullable:  */ IsNullable extends boolean,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> =
	k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryRow, TE> extends k.SelectQueryBuilder<
		infer JoinedDB,
		infer JoinedTB,
		infer JoinedRow
	>
		? HydratedQueryBuilder<
				/* Prefix:      */ Prefix,
				/* QueryDB:     */ JoinedDB,
				/* QueryTB:     */ JoinedTB,
				/* QueryRow:    */ JoinedRow,
				/* LocalDB:     */ JoinedDB,
				/* LocalRow:    */ LocalRow,
				/* HydratedRow: */ HydratedRow,
				/* IsNullable:  */ IsNullable,
				/* HasJoin:     */ true
			>
		: never;

type HydratedQueryBuilderWithLeftJoin<
	/* Prefix:      */ Prefix extends string,
	/* QueryDB:     */ QueryDB,
	/* QueryTB:     */ QueryTB extends keyof QueryDB,
	/* QueryRow:    */ QueryRow,
	/* LocalDB:     */ _LocalDB,
	/* LocalRow:    */ LocalRow,
	/* HydratedRow: */ HydratedRow,
	/* IsNullable:  */ _IsNullable extends boolean,
	/* HasJoin:     */ AlreadyHadJoin extends boolean,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> =
	k.SelectQueryBuilderWithLeftJoin<QueryDB, QueryTB, QueryRow, TE> extends k.SelectQueryBuilder<
		infer JoinedDB,
		infer JoinedTB,
		infer JoinedRow
	>
		? HydratedQueryBuilder<
				/* Prefix:      */ Prefix,
				/* QueryDB:     */ JoinedDB,
				/* QueryTB:     */ JoinedTB,
				/* QueryRow:    */ JoinedRow,
				// If the nested join builder does not have a join yet, we can treat the
				// join as an inner join when considered from inside the nested join.
				/* LocalDB:     */ AlreadyHadJoin extends true
					? JoinedDB
					: InferDB<k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryRow, TE>>,
				/* LocalRow:    */ LocalRow,
				/* HydratedRow: */ HydratedRow,
				/* IsNullable:  */ true, // Left joins always produce nullable rows.
				/* HasJoin:     */ true
			>
		: never;

////////////////////////////////////////////////////////////////////
// Implementation.
////////////////////////////////////////////////////////////////////

type AnySelectQueryBuilder = k.SelectQueryBuilder<any, any, any>;

type AnyHydratedQueryBuilder = HydratedQueryBuilder<any, any, any, any, any, any, any, any, any>;

interface HydratedQueryBuilderProps {
	readonly qb: AnySelectQueryBuilder;
	readonly prefix: string;
	readonly hydrator: Hydrator<any, any>;
}

/**
 * Implementation of the {@link HydratedQueryBuilder} interface as well as the
 * {@link MappedHydratedQueryBuilder} interface; there is no runtime distinction.
 */
class HydratedQueryBuilderImpl implements AnyHydratedQueryBuilder {
	#props: HydratedQueryBuilderProps;

	constructor(props: HydratedQueryBuilderProps) {
		this.#props = props;

		// Support destructuring.
		this.innerJoin = this.innerJoin.bind(this);
		this.leftJoin = this.leftJoin.bind(this);
		this.crossJoin = this.crossJoin.bind(this);
		this.innerJoinLateral = this.innerJoinLateral.bind(this);
		this.leftJoinLateral = this.leftJoinLateral.bind(this);
		this.crossJoinLateral = this.crossJoinLateral.bind(this);
	}

	//
	// k.Compilable methods
	//

	compile() {
		return this.#props.qb.compile();
	}

	//
	// k.OperationNodeSource methods.
	//

	toOperationNode() {
		return this.#props.qb.toOperationNode();
	}

	//
	// MappedHydratedQueryBuilder methods.
	//

	get _generics() {
		return undefined;
	}

	modify(modifier: (qb: AnySelectQueryBuilder) => AnySelectQueryBuilder): any {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			qb: modifier(this.#props.qb),
		});
	}

	toQuery(): AnySelectQueryBuilder {
		return this.#props.qb;
	}

	map(transform: (row: any) => any): any {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: this.#props.hydrator.map(transform),
		});
	}

	#hydrate(rows: object[]): object[];
	#hydrate(rows: object | undefined): object | undefined;
	#hydrate(rows: object | object[] | undefined): object | object[] | undefined {
		const isArray = Array.isArray(rows);
		const firstRow = isArray ? rows[0] : rows;

		if (!firstRow) {
			return isArray ? [] : undefined;
		}

		// This dance is necessary to ensure the hydrated result actually includes
		// the selected columns from the top-level select.
		const fieldNames = Object.keys(firstRow)
			// To determine if the key is from the top-level selection versus a
			// nested selection, we simply check if it includes the prefix separator.
			.filter((key) => !hasAnyPrefix(key));

		// Use ensureFields helper to preserve any explicit field mappings/omissions
		// from `mapFields()` or `omit()`.
		const hydratorWithSelection = ensureFields(this.#props.hydrator, fieldNames);

		return hydratorWithSelection.hydrate(rows);
	}

	async execute(): Promise<any[]> {
		const rows = await this.#props.qb.execute();

		return this.#hydrate(rows);
	}

	async executeTakeFirst(): Promise<any | undefined> {
		const result = await this.#props.qb.executeTakeFirst();

		return result ? this.#hydrate(result) : undefined;
	}

	async executeTakeFirstOrThrow(
		errorConstructor: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error) = k.NoResultError,
	): Promise<any> {
		const result = await this.#props.qb.executeTakeFirstOrThrow(errorConstructor);

		return this.#hydrate(result);
	}

	//
	// HydratedQueryBuilder methods.
	//

	extras(extras: Extras<any>) {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: asFullHydrator(this.#props.hydrator).extras(extras),
		});
	}

	mapFields(mappings: FieldMappings<any>): any {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: asFullHydrator(this.#props.hydrator).fields(mappings),
		});
	}

	omit(keys: readonly PropertyKey[]): any {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: asFullHydrator(this.#props.hydrator).omit(keys),
		});
	}

	with(hydrator: Hydrator<any, any>): any {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: asFullHydrator(this.#props.hydrator).extend(hydrator),
		});
	}

	#addJoin(
		mode: CollectionMode,
		key: string,
		jb: (nb: AnyHydratedQueryBuilder) => HydratedQueryBuilderImpl,
		keyBy: any = DEFAULT_KEY_BY,
	) {
		const inputNb = new HydratedQueryBuilderImpl({
			qb: this.#props.qb,
			prefix: makePrefix(this.#props.prefix, key),

			hydrator: createHydrator<any>(keyBy),
		});
		const outputNb = jb(inputNb);

		return new HydratedQueryBuilderImpl({
			...this.#props,

			qb: outputNb.#props.qb,

			hydrator: asFullHydrator(this.#props.hydrator).has(
				mode,
				key,
				// Hydratables do their own job of handling nested prefixes.
				makePrefix("", key),
				outputNb.#props.hydrator,
			),
		});
	}

	hasMany(key: string, jb: (nb: AnyHydratedQueryBuilder) => HydratedQueryBuilderImpl, keyBy?: any) {
		return this.#addJoin("many", key, jb, keyBy);
	}

	hasOne(
		key: string,
		jb: (nb: AnyHydratedQueryBuilder) => HydratedQueryBuilderImpl,
		keyBy?: any,
	): any {
		return this.#addJoin("one", key, jb, keyBy);
	}

	hasOneOrThrow(
		key: string,
		jb: (nb: AnyHydratedQueryBuilder) => HydratedQueryBuilderImpl,
		keyBy?: any,
	): any {
		return this.#addJoin("oneOrThrow", key, jb, keyBy);
	}

	#addAttach(
		mode: CollectionMode,
		key: string,
		fetchFn: FetchFn<any, any>,
		keys: AttachedKeysArg<any, any>,
	) {
		return new HydratedQueryBuilderImpl({
			...this.#props,
			hydrator: asFullHydrator(this.#props.hydrator).attach(mode, key, fetchFn, keys),
		});
	}

	attachMany(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("many", key, fetchFn, keys);
	}

	attachOne(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("one", key, fetchFn, keys);
	}

	attachOneOrThrow(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("oneOrThrow", key, fetchFn, keys);
	}

	//
	// NestedJoinBuilder methods.
	//

	select(selection: k.SelectArg<any, any, any>) {
		const prefixedSelections = prefixSelectArg(this.#props.prefix, selection);

		return new HydratedQueryBuilderImpl({
			...this.#props,

			// This cast to `any` is needed because TS can't follow the overload.
			qb: this.#props.qb.select(prefixedSelections as any),

			// Ensure all selected fields are included in the hydrated output.
			hydrator: asFullHydrator(this.#props.hydrator).fields(
				Object.fromEntries(
					prefixedSelections.map((selection) => [selection.originalName, true as const]),
				),
			),
		});
	}

	innerJoin(...args: [any, any]) {
		return this.modify((qb) => qb.innerJoin(...args));
	}

	leftJoin(...args: [any, any]) {
		return this.modify((qb) => qb.leftJoin(...args));
	}

	crossJoin(...args: [any]) {
		return this.modify((qb) => qb.crossJoin(...args));
	}

	innerJoinLateral(...args: [any, any]) {
		return this.modify((qb) => qb.innerJoinLateral(...args));
	}

	leftJoinLateral(...args: [any, any]) {
		return this.modify((qb) => qb.leftJoinLateral(...args));
	}

	crossJoinLateral(...args: [any]) {
		return this.modify((qb) => qb.crossJoinLateral(...args));
	}
}

////////////////////////////////////////////////////////////////////
// Constructor.
////////////////////////////////////////////////////////////////////

/**
 * Creates a new {@link HydratedQueryBuilder} from a Kysely select query.
 * This enables nested joins and automatic hydration of flat SQL results into nested objects.
 *
 * **Example:**
 * ```ts
 * const users = await hydrate(
 *   db.selectFrom("users").select(["users.id", "users.email"]),
 *   "id",
 * ).hasMany(
 *   "posts",
 *   ({ leftJoin }) =>
 *     leftJoin("posts", "posts.user_id", "users.id").select(["posts.id", "posts.title"]),
 *   "id",
 * ).execute();
 * ```
 *
 * @param qb - A Kysely select query builder to wrap.
 * @param keyBy - The key(s) to uniquely identify rows in the query result.
 *   Defaults to "id" if the row type has an "id" property.
 * @returns A new HydratedQueryBuilder that supports nested joins and hydration.
 */
// Overload 1: keyBy provided - any row type
export function hydrate<QueryDB, QueryTB extends keyof QueryDB, QueryRow>(
	qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
	keyBy: KeyBy<QueryRow>,
): HydratedQueryBuilder<
	/* Prefix:      */ "",
	/* QueryDB:     */ QueryDB,
	/* QueryTB:     */ QueryTB,
	/* QueryRow:    */ QueryRow,
	/* LocalDB:     */ QueryDB,
	/* LocalRow:    */ QueryRow,
	/* HydratedRow: */ QueryRow,
	/* IsNullable:  */ false,
	/* HasJoin:     */ false
>;
// Overload 2: keyBy omitted - row must have 'id'
export function hydrate<
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow extends InputWithDefaultKey,
>(
	qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
): HydratedQueryBuilder<
	/* Prefix:      */ "",
	/* QueryDB:     */ QueryDB,
	/* QueryTB:     */ QueryTB,
	/* QueryRow:    */ QueryRow,
	/* LocalDB:     */ QueryDB,
	/* LocalRow:    */ QueryRow,
	/* HydratedRow: */ QueryRow,
	/* IsNullable:  */ false,
	/* HasJoin:     */ false
>;
// Implementation
export function hydrate(qb: any, keyBy: any = DEFAULT_KEY_BY): any {
	return new HydratedQueryBuilderImpl({
		qb,
		prefix: "",
		hydrator: createHydrator<any>(keyBy),
	});
}
