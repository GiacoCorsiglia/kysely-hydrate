import * as k from "kysely";

import { db } from "./__tests__/postgres.ts";
import { makePrefix } from "./helpers/prefixes.ts";
import { hoistAndPrefixSelections } from "./helpers/select-renamer.ts";
import {
	type Extend,
	type ExtendWith,
	type Flatten,
	type KeyBy,
	type StrictSubset,
	assertNever,
	mapWithDeleted,
} from "./helpers/utils.ts";
import {
	type AttachedKeysArg,
	type AttachedOutputFromFetchFnReturn,
	type CollectionMode,
	type Extras,
	type FieldMappings,
	type FullHydrator,
	type Hydrator,
	type InferExtras,
	type InferFields,
	type InputWithDefaultKey,
	type MappedHydrator,
	type SomeFetchFn,
	type SomeFetchFnReturn,
	asFullHydrator,
	createHydrator,
	DEFAULT_KEY_BY,
} from "./hydrator.ts";

////////////////////////////////////////////////////////////
// Generics.
////////////////////////////////////////////////////////////

interface TQuery<DB = any, TB extends keyof DB = any> {
	DB: DB;
	TB: TB;
	O: any;
}

type SelectQueryBuilderFor<Q extends TQuery> = k.SelectQueryBuilder<Q["DB"], Q["TB"], Q["O"]>;

interface TJoinCollection {
	Prototype: "Join";
	Type: "InnerJoinOne" | "InnerJoinMany" | "LeftJoinOne" | "LeftJoinOneOrThrow" | "LeftJoinMany";
	Value: TQuerySet;
}

interface TAttachCollection {
	Prototype: "Attach";
	Type: "AttachOne" | "AttachOneOrThrow" | "AttachMany";
	Value: any;
}

type TCollection = TJoinCollection | TAttachCollection;

type TAttachType = TAttachCollection["Type"];
type TJoinType = TJoinCollection["Type"];

/**
 * The shape of all the collections for later modification.
 */
type TCollections = {
	[k in string]: TCollection;
};

type TCollectionsWith<
	Collections extends TCollections,
	K extends string,
	Collection extends TCollection,
> = ExtendWith<Collections, K, Collection>;

/**
 * Bag of generics for a QuerySet.
 */
interface TQuerySet {
	/**
	 * Indicates whether the query set has been mapped.
	 */
	IsMapped: boolean;
	/**
	 * The key(s) used to uniquely identify rows in the query result.
	 */
	Keys: string;
	/**
	 * The alias of the base query.
	 */
	BaseAlias: string;
	/**
	 * The shape of the base query.
	 */
	BaseQuery: TQuery;
	/**
	 * The shape of all the collections for later modification.
	 */
	Collections: TCollections;
	/**
	 * The final shape of the query with all the joins applied.
	 */
	JoinedQuery: TQuery;
	/**
	 * The final shape of the hydrated output row.
	 */
	HydratedOutput: any;
}

type QuerySetFor<T extends TQuerySet> = T["IsMapped"] extends true
	? MappedQuerySet<T>
	: QuerySet<T>;

type TInput<T extends TQuerySet> = T["JoinedQuery"]["O"];
type TOutput<T extends TQuerySet> = k.Simplify<T["HydratedOutput"]>;

interface TMapped<T extends TQuerySet, Output> {
	IsMapped: true;
	Keys: T["Keys"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	HydratedOutput: Output;
}

interface TWithBaseQuery<T extends TQuerySet, BaseQuery extends TQuery> {
	IsMapped: T["IsMapped"];
	Keys: T["Keys"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: BaseQuery;
	Collections: T["Collections"];
	JoinedQuery: {
		DB: T["JoinedQuery"]["DB"] & { [_ in T["BaseAlias"]]: BaseQuery["O"] };
		TB: T["JoinedQuery"]["TB"];
		O: T["JoinedQuery"]["O"];
	};
	HydratedOutput: TOutput<T>;
}

interface TWithExtendedOutput<T extends TQuerySet, Output> {
	IsMapped: T["IsMapped"];
	Keys: T["Keys"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	HydratedOutput: Extend<TOutput<T>, Output>;
}

////////////////////////////////////////////////////////////
// Interfaces.
////////////////////////////////////////////////////////////

/**
 * A select query builder whose internal structure is hidden from the user.
 */
type OpaqueSelectQueryBuilder<O> = k.SelectQueryBuilder<{}, never, O>;

/**
 * An opaque select query builder that returns a count.
 */
type OpaqueCountQueryBuilder = OpaqueSelectQueryBuilder<{ count: string | number | bigint }>;

/**
 * An opaque select query builder that returns existence
 */
type OpaqueExistsQueryBuilder = OpaqueSelectQueryBuilder<{ exists: k.SqlBool }>;

/**
 * A limit or offset value, passable to `.limit()` and `.offset()`.
 */
type LimitOrOffset = number | bigint | null;

interface MappedQuerySet<T extends TQuerySet> extends k.Compilable, k.OperationNodeSource {
	/**
	 * This property exists for complex type reasons and will never be set.
	 *
	 * @internal
	 */
	// Required so that the type system can infer all the generics the even when
	// nested collections return a MappedQuerySet instead of a full QuerySet.
	readonly _generics: T | undefined;

	/**
	 * Returns the base query that this query set was initialized with (plus any
	 * modifications).
	 */
	toBaseQuery(): SelectQueryBuilderFor<T["BaseQuery"]>;

	/**
	 * Returns the base query with joins applied for each collection.
	 *
	 * @warning This query is subject to "row explosion." If a base record has
	 * multiple related child records, the base record will appear multiple times
	 * in the result set.  As a result, LIMIT and OFFSET will not be applied.
	 */
	toJoinedQuery(): SelectQueryBuilderFor<T["JoinedQuery"]>;

	/**
	 * Returns the {@link k.SelectQueryBuilder} that will be run if this query set
	 * is executed.
	 */
	toQuery(): OpaqueSelectQueryBuilder<T["JoinedQuery"]["O"]>;

	/**
	 * Returns a query that counts all the unique records in the base table,
	 * accounting for filtering from inner joins.  This query ignores pagination
	 * (offset and limit are removed).
	 */
	toCountQuery(): OpaqueCountQueryBuilder;

	/**
	 * Returns a query that returns a boolean indicating whether the query will
	 * return any results.  This query ignores pagination (offset and limit are
	 * removed).
	 */
	toExistsQuery(): OpaqueExistsQueryBuilder;

	/**
	 * Executes the query and returns an array of rows.
	 *
	 * Also see the {@link executeTakeFirst} and {@link executeTakeFirstOrThrow}
	 * methods.
	 */
	execute(): Promise<TOutput<T>[]>;

	/**
	 * Executes the query and returns the first result or undefined if the query
	 * returned no result.
	 */
	executeTakeFirst(): Promise<TOutput<T> | undefined>;

	/**
	 * Executes the query and returns the first result or throws if the query
	 * returned no result.
	 *
	 * By default an instance of {@link k.NoResultError} is thrown, but you can
	 * provide a custom error class, or callback to throw a different error.
	 */
	executeTakeFirstOrThrow(
		errorConstructor?: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error),
	): Promise<TOutput<T>>;

	/**
	 * Executes a modified version the query and returns the number of root entity
	 * rows.
	 *
	 * By default, Kysely's count function returns `string | number | bigint`.
	 * You can provide a transformation function to convert the count to a number
	 * or bigint.
	 *
	 * @example
	 * ```ts
	 * query.executeCountAll(); // string | number | bigint
	 * query.executeCountAll(Number); // number
	 * query.executeCountAll(BigInt); // bigint
	 * ```
	 */
	executeCount(toBigInt: (count: string | number | bigint) => bigint): Promise<bigint>;
	executeCount(toNumber: (count: string | number | bigint) => number): Promise<number>;
	executeCount(toString: (count: string | number | bigint) => string): Promise<string>;
	executeCount(): Promise<string | number | bigint>;

	/**
	 * Executes a modified version of the query and returns a boolean indicating
	 * whether the query will return any results.
	 */
	executeExists(): Promise<boolean>;

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
	 * @returns A MappedQuerySet with the transformation added
	 */
	map<NewHydratedOutput>(
		transform: (row: TOutput<T>) => NewHydratedOutput,
	): MappedQuerySet<TMapped<T, NewHydratedOutput>>;

	/**
	 * Allows you to modify the base select query.  Useful for adding `where`
	 * clauses.  Adding additional SELECTs here is forbidden.
	 *
	 * For example:
	 *
	 * ```ts
	 * querySet(db).init(...).modify((qb) => qb.where("isActive", "=", "true"))
	 * ```
	 */
	modify<NewDB, NewTB extends keyof NewDB, NewO extends T["BaseQuery"]["O"]>(
		modifier: (
			qb: SelectQueryBuilderFor<T["BaseQuery"]>,
		) => k.SelectQueryBuilder<NewDB, NewTB, NewO>,
	): MappedQuerySet<TWithBaseQuery<T, { DB: NewDB; TB: NewTB; O: NewO }>>;

	/**
	 * Adds a limit clause to the query in a way that handles row explosion.
	 *
	 * Works similarly to {@link k.SelectQueryBuilder.limit()}.
	 *
	 * NOTE: We don't support {@link k.ValueExpression} here because the limit
	 * might be applied to different queries depending on the types of joins you
	 * have added to this query set.
	 */
	limit(limit: LimitOrOffset): this;

	/**
	 * Clears the limit clause from the query.
	 *
	 * Works similarly to {@link k.SelectQueryBuilder.clearLimit()}.
	 */
	clearLimit(): this;

	/**
	 * Adds a limit clause to the query in a way that handles row explosion.
	 *
	 * Works similarly to {@link k.SelectQueryBuilder.offset()}.
	 *
	 * NOTE: We don't support {@link k.ValueExpression} here because the offset
	 * might be applied to different queries depending on the types of joins you
	 * have added to this query set.
	 */
	offset(offset: LimitOrOffset): this;

	/**
	 * Clears the offset clause from the query.
	 *
	 * Works similarly to {@link k.SelectQueryBuilder.clearOffset()}.
	 */
	clearOffset(): this;
}

interface QuerySet<T extends TQuerySet> extends MappedQuerySet<T> {
	////////////////////////////////////////////////////////////
	// Hydration
	////////////////////////////////////////////////////////////

	/**
	 * Configures extra computed fields to add to the hydrated output.
	 * Each extra is a function that receives the full row (with prefixed columns
	 * available as accessors) and returns a computed value.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("users", (eb) => eb.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]))
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
	extras<E extends Extras<TInput<T>>>(
		extras: E,
	): QuerySet<TWithExtendedOutput<T, InferExtras<TInput<T>, E>>>;

	/**
	 * Transforms already-selected field values in the hydrated output.  Fields
	 * not mentioned in the mappings will still be included as-is.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("users", (eb) => eb.selectFrom("users").select(["users.id", "users.name"]))
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
	mapFields<M extends FieldMappings<TInput<T>>>(
		mappings: M,
	): QuerySet<TWithExtendedOutput<T, InferFields<TInput<T>, M>>>;

	/**
	 * Omits specified fields from the hydrated output.  Useful for excluding
	 * fields that were selected for internal use (e.g., for extras).
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("users", (eb) => eb.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]))
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
	omit<K extends keyof TInput<T>>(
		keys: readonly K[],
	): QuerySet<TWithExtendedOutput<T, Omit<TOutput<T>, K>>>;

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
	 * const users = await querySet(db)
	 *   .init("users", (eb) => eb.selectFrom("users").select(["users.id", "users.name", "users.email"]))
	 *   .with(extraFields)
	 *   .execute();
	 * // Result: [{ id: 1, name: "Alice", email: "...", displayName: "Alice <...>" }]
	 * ```
	 *
	 * @param hydrator - The Hydrator to extend with.
	 * @returns A new HydratedQueryBuilder with merged hydration configuration.
	 */
	with<OtherInput extends StrictSubset<TInput<T>, OtherInput>, OtherOutput>(
		hydrator: FullHydrator<OtherInput, OtherOutput>,
	): QuerySet<TWithExtendedOutput<T, OtherOutput>>;
	// If you pass a Hydrator with a map applied, we must return a
	// MappedHydratedQueryBuilder.
	with<OtherInput extends StrictSubset<TInput<T>, OtherInput>, OtherOutput>(
		hydrator: MappedHydrator<OtherInput, OtherOutput>,
	): QuerySet<TWithExtendedOutput<T, OtherOutput>>;

	////////////////////////////////////////////////////////////
	// Attaches
	////////////////////////////////////////////////////////////

	/**
	 * Attaches data from an external source (not via SQL joins) as a nested
	 * array.  The `fetchFn` is called exactly once per query execution with all
	 * parent rows to avoid N+1 queries.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init((eb) => eb.selectFrom("users").select(["users.id", "users.name"]))
	 *   .attachMany(
	 *     "posts",
	 *     async (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return db.selectFrom("posts")
	 *         .select(["posts.id", "posts.userId", "posts.title"])
	 *         .where("posts.userId", "in", userIds)
	 *         .execute();
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the array in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once
	 * with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new QuerySet with the attached collection added.
	 */
	attachMany<K extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: K,
		fetchFn: SomeFetchFn<TInput<T>, FetchFnReturn>,
		keys: AttachedKeysArg<TInput<T>, AttachedOutputFromFetchFnReturn<FetchFnReturn>>,
	): QuerySetWithAttachMany<T, K, FetchFnReturn>;

	/**
	 * Attaches data from an external source (not via SQL joins) as a single
	 * nested object.  The object will be nullable. The `fetchFn` is called
	 * exactly once per query execution with all parent rows to avoid N+1 queries.
	 *
	 * **Example:**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init((eb) => eb.selectFrom("posts").select(["posts.id", "posts.title"]))
	 *   .attachOne(
	 *     "author",
	 *     async (postRows) => {
	 *       const userIds = [...new Set(postRows.map((p) => p.userId))];
	 *       return db.selectFrom("users")
	 *         .select(["users.id", "users.name"])
	 *         .where("users.id", "in", userIds)
	 *         .execute();
	 *     },
	 *     { matchChild: "id", toParent: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once
	 * with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new QuerySet with the attached object added.
	 */
	attachOne<K extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: K,
		fetchFn: SomeFetchFn<TInput<T>, FetchFnReturn>,
		keys: AttachedKeysArg<TInput<T>, AttachedOutputFromFetchFnReturn<NoInfer<FetchFnReturn>>>,
	): QuerySetWithAttachOne<T, K, FetchFnReturn>;

	/**
	 * Exactly like {@link attachOne}, but throws an error if the attached object
	 * is not found.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once
	 * with all parent rows.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new QuerySet with the attached object added.
	 */
	attachOneOrThrow<K extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: K,
		fetchFn: SomeFetchFn<TInput<T>, FetchFnReturn>,
		keys: AttachedKeysArg<TInput<T>, AttachedOutputFromFetchFnReturn<NoInfer<FetchFnReturn>>>,
	): QuerySetWithAttachOneOrThrow<T, K, FetchFnReturn>;

	////////////////////////////////////////////////////////////
	// Joins
	////////////////////////////////////////////////////////////

	//
	// INNER JOIN
	//

	/**
	 *
	 */
	innerJoinOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinOne<T, Key, TNested>;
	innerJoinOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinOne<T, Key, TNested>;
	// Standard version
	// TODO: These need a key by, which means they need even more generics lol
	// innerJoinOne<Key extends string, NestedRow>(
	// 	key: Key,
	// 	table: JoinTableExpression<T, NestedRow>,
	// 	k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<NestedRow>>,
	// 	k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<NestedRow>>,
	// ): QuerySetWithInnerJoin<T, Key, NestedRow, NestedRow>;
	// innerJoinOne<Key extends string, NestedRow>(
	// 	key: Key,
	// 	table: JoinTableExpression<T, NestedRow>,
	// 	callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<NestedRow>>,
	// ): QuerySetWithInnerJoin<T, Key, NestedRow, NestedRow>;

	/**
	 *
	 */
	innerJoinMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;
	innerJoinMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;

	//
	// LEFT JOIN
	//

	/**
	 *
	 */
	leftJoinOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOne<T, Key, TNested>;
	leftJoinOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOne<T, Key, TNested>;

	/**
	 *
	 */
	leftJoinOneOrThrow<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOneOrThrow<T, Key, TNested>;
	leftJoinOneOrThrow<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOneOrThrow<T, Key, TNested>;

	/**
	 *
	 */
	leftJoinMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinMany<T, Key, TNested>;
	leftJoinMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinMany<T, Key, TNested>;

	//
	// CROSS JOIN
	//

	/**
	 *
	 */
	crossJoinMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;

	//
	// INNER JOIN LATERAL
	//

	/**
	 *
	 */
	innerJoinLateralOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinOne<T, Key, TNested>;
	innerJoinLateralOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinOne<T, Key, TNested>;

	/**
	 *
	 */
	innerJoinLateralMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;
	innerJoinLateralMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;

	//
	// LEFT JOIN LATERAL
	//

	/**
	 *
	 */
	leftJoinLateralOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOne<T, Key, TNested>;
	leftJoinLateralOne<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOne<T, Key, TNested>;

	/**
	 *
	 */
	leftJoinLateralOneOrThrow<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOneOrThrow<T, Key, TNested>;
	leftJoinLateralOneOrThrow<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinOneOrThrow<T, Key, TNested>;

	/**
	 *
	 */
	leftJoinLateralMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinMany<T, Key, TNested>;
	leftJoinLateralMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithLeftJoinMany<T, Key, TNested>;

	//
	// CROSS JOIN LATERAL
	//

	/**
	 *
	 */
	crossJoinLateralMany<Key extends string, TNested extends TQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
	): QuerySetWithInnerJoinMany<T, Key, TNested>;

	////////////////////////////////////////////////////////////
	// Modification
	////////////////////////////////////////////////////////////

	/**
	 * When called with one argument, allows you to modify the base select query.
	 * You can add where clauses, and can also select additional columns.  You
	 * cannot, however, select *fewer* columns (e.g., with `.clearSelect()`)---you
	 * can only add to the selection
	 *
	 * When called with two arguments, allows you to modify a nested collection
	 * (either a join or attach).
	 */
	// Modify base query.
	modify<NewDB, NewTB extends keyof NewDB, NewO extends T["BaseQuery"]["O"]>(
		modifier: (
			qb: SelectQueryBuilderFor<T["BaseQuery"]>,
		) => k.SelectQueryBuilder<NewDB, NewTB, NewO>,
	): QuerySet<TWithBaseQuery<T, { DB: NewDB; TB: NewTB; O: NewO }>>;
	// Modify collection.
	modify<
		Key extends keyof T["Collections"] & string,
		TNestedNew extends TQuerySet = never,
		NewValue extends SomeFetchFnReturn = never,
	>(
		key: Key,
		modifier: CollectionModifier<T["Collections"][NoInfer<Key>], TNestedNew, NewValue>,
	): ModifyCollectionReturnMap<T, Key, TNestedNew, NewValue>[T["Collections"][Key]["Type"]];
}

////////////////////////////////////////////////////////////
// Modify Helpers.
////////////////////////////////////////////////////////////

/**
 * A callback for modifying a collection.
 */
type CollectionModifier<
	Collection extends TCollection,
	TNestedNew extends TQuerySet,
	NewValue extends SomeFetchFnReturn,
> = Collection["Prototype"] extends "Join"
	? (value: QuerySetFor<Collection["Value"]>) => MappedQuerySet<TNestedNew>
	: (value: Collection["Value"]) => NewValue;

/**
 * Map of collection types to their return types for a modified collection.
 */
interface ModifyCollectionReturnMap<
	T extends TQuerySet,
	Key extends string,
	TNestedNew extends TQuerySet,
	NewValue extends SomeFetchFnReturn,
> {
	InnerJoinOne: QuerySetWithInnerJoinOne<T, Key, TNestedNew>;
	InnerJoinMany: QuerySetWithInnerJoinMany<T, Key, TNestedNew>;
	LeftJoinOne: QuerySetWithLeftJoinOne<T, Key, TNestedNew>;
	LeftJoinOneOrThrow: QuerySetWithLeftJoinOneOrThrow<T, Key, TNestedNew>;
	LeftJoinMany: QuerySetWithLeftJoinMany<T, Key, TNestedNew>;

	AttachOne: QuerySetWithAttachOne<T, Key, NewValue>;
	AttachOneOrThrow: QuerySetWithAttachOneOrThrow<T, Key, NewValue>;
	AttachMany: QuerySetWithAttachMany<T, Key, NewValue>;
}

////////////////////////////////////////////////////////////
// Attach Helpers.
////////////////////////////////////////////////////////////

interface TQuerySetWithAttach<
	T extends TQuerySet,
	Type extends TAttachType,
	FetchFnReturn extends SomeFetchFnReturn,
	Key extends string,
	AttachedOutput,
> {
	IsMapped: T["IsMapped"];
	Keys: T["Keys"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	JoinedQuery: T["JoinedQuery"];
	Collections: TCollectionsWith<
		TCollections,
		Key,
		{ Prototype: "Attach"; Type: Type; Value: FetchFnReturn }
	>;
	HydratedOutput: ExtendWith<TOutput<T>, Key, AttachedOutput>;
}

interface QuerySetWithAttachMany<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
	TQuerySetWithAttach<
		T,
		"AttachMany",
		FetchFnReturn,
		Key,
		AttachedOutputFromFetchFnReturn<FetchFnReturn>[]
	>
> {}

interface QuerySetWithAttachOne<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
	TQuerySetWithAttach<
		T,
		"AttachOne",
		FetchFnReturn,
		Key,
		AttachedOutputFromFetchFnReturn<FetchFnReturn> | null
	>
> {}

interface QuerySetWithAttachOneOrThrow<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
	TQuerySetWithAttach<
		T,
		"AttachOneOrThrow",
		FetchFnReturn,
		Key,
		AttachedOutputFromFetchFnReturn<FetchFnReturn>
	>
> {}

////////////////////////////////////////////////////////////
// Join Helpers.
////////////////////////////////////////////////////////////

// Important: for all these nested operations, we use the BaseQuery---not the
// JoinedQuery.  This guarantees at a type level that adjacent joins do not
// depend on each other.
type NestedQuerySetOrFactory<T extends TQuerySet, Alias extends string, TNested extends TQuerySet> =
	| MappedQuerySet<TNested>
	| ((
			nest: InitWithAlias<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"], Alias>,
	  ) => MappedQuerySet<TNested>);

type ToTableExpression<Key extends string, TNested extends TQuerySet> = k.AliasedExpression<
	TNested["BaseQuery"]["O"],
	Key
>;

type JoinReferenceExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> = k.JoinReferenceExpression<
	T["BaseQuery"]["DB"],
	T["BaseQuery"]["TB"],
	ToTableExpression<Key, TNested>
>;

type JoinCallbackExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> = k.JoinCallbackExpression<
	T["BaseQuery"]["DB"],
	T["BaseQuery"]["TB"],
	ToTableExpression<Key, TNested>
>;

type TQuerySetWithJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	TNested extends TQuerySet,
	NestedHydratedRow,
	JoinedQuery extends k.SelectQueryBuilder<any, any, any>,
> = Flatten<
	JoinedQuery extends k.SelectQueryBuilder<infer JoinedDB, infer JoinedTB, infer JoinedRow>
		? {
				IsMapped: T["IsMapped"];
				Keys: T["Keys"];
				BaseAlias: T["BaseAlias"];
				BaseQuery: T["BaseQuery"];
				Collections: TCollectionsWith<
					T["Collections"],
					Key,
					{ Prototype: "Join"; Type: Type; Value: TNested }
				>;
				JoinedQuery: {
					DB: JoinedDB;
					TB: JoinedTB;
					O: JoinedRow;
				};
				HydratedOutput: ExtendWith<TOutput<T>, Key, NestedHydratedRow>;
			}
		: never
>;

type TQuerySetWithInnerJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	TNested extends TQuerySet,
	NestedHydratedRow,
> = TQuerySetWithJoin<
	T,
	Key,
	Type,
	TNested,
	NestedHydratedRow,
	k.SelectQueryBuilderWithInnerJoin<
		T["BaseQuery"]["DB"],
		T["BaseQuery"]["TB"],
		T["BaseQuery"]["O"],
		ToTableExpression<Key, TNested>
	>
>;

type TQuerySetWithLeftJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	TNested extends TQuerySet,
	NestedHydratedRow,
> = TQuerySetWithJoin<
	T,
	Key,
	Type,
	TNested,
	NestedHydratedRow,
	k.SelectQueryBuilderWithLeftJoin<
		T["BaseQuery"]["DB"],
		T["BaseQuery"]["TB"],
		T["BaseQuery"]["O"],
		ToTableExpression<Key, TNested>
	>
>;

interface QuerySetWithInnerJoinOne<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> extends QuerySet<TQuerySetWithInnerJoin<T, Key, "InnerJoinOne", TNested, TOutput<TNested>>> {}

interface QuerySetWithInnerJoinMany<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> extends QuerySet<TQuerySetWithInnerJoin<T, Key, "InnerJoinMany", TNested, TOutput<TNested>[]>> {}

interface QuerySetWithLeftJoinOne<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> extends QuerySet<
	TQuerySetWithLeftJoin<T, Key, "LeftJoinOne", TNested, TOutput<TNested> | null>
> {}

interface QuerySetWithLeftJoinOneOrThrow<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> extends QuerySet<
	TQuerySetWithLeftJoin<T, Key, "LeftJoinOneOrThrow", TNested, TOutput<TNested>>
> {}

interface QuerySetWithLeftJoinMany<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> extends QuerySet<TQuerySetWithLeftJoin<T, Key, "LeftJoinMany", TNested, TOutput<TNested>[]>> {}

////////////////////////////////////////////////////////////
// Implementation
////////////////////////////////////////////////////////////

type AnySelectQueryBuilder = k.SelectQueryBuilder<any, any, any>;

type JoinMethod =
	| "innerJoin"
	| "leftJoin"
	| "crossJoin"
	| "innerJoinLateral"
	| "leftJoinLateral"
	| "crossJoinLateral";

const filteringJoins = new Set<JoinMethod>([
	"innerJoin",
	"innerJoinLateral",
	"crossJoin",
	"crossJoinLateral",
]);

const isFilteringJoin = (collection: JoinCollection): boolean =>
	filteringJoins.has(collection.method);

type AnyJoinArgs = [key: string, from: any, callbackOrk1?: any, k2?: any];
type AnyJoinArgsTail = [callbackOrk1?: any, k2?: any];

interface JoinCollection {
	readonly type: "join";
	readonly method: JoinMethod;
	readonly mode: CollectionMode;
	readonly querySet: QuerySetImpl;
	readonly args: AnyJoinArgsTail;
}

interface AttachCollection {
	readonly type: "attach";
	readonly mode: CollectionMode;
	readonly fetchFn: SomeFetchFn<any, any>;
	readonly keys: AttachedKeysArg<any, any>;
}

type Collection = JoinCollection | AttachCollection;

interface QuerySetProps {
	db: k.Kysely<any>;
	baseAlias: string;
	baseQuery: AnySelectQueryBuilder;
	keyBy: KeyBy<any>;
	hydrator: Hydrator<any, any>;
	joinCollections: Map<string, JoinCollection>;
	attachCollections: Map<string, AttachCollection>;
	limit: LimitOrOffset;
	offset: LimitOrOffset;
	// TODO
	orderBy: unknown;
}

/**
 * Implementation of the {@link QuerySet} interface as well as the
 * {@link MappedQuerySet} interface; there is no runtime distinction.
 */
class QuerySetImpl implements QuerySet<TQuerySet> {
	#props: QuerySetProps;

	constructor(props: QuerySetProps) {
		this.#props = props;
	}

	#clone(props: Partial<QuerySetProps>): QuerySetImpl {
		return new QuerySetImpl({
			...this.#props,
			...props,
		});
	}

	#addCollection(key: string, collection: Collection): QuerySetImpl {
		// Careful to overwrite any previous collection definition with the same
		// key, regardless of type.

		// NOTE: Hydrators maintain a Map of collections, so this will correctly
		// overwrite any previous collection definition with the same key.

		const { joinCollections, attachCollections, hydrator } = this.#props;

		switch (collection.type) {
			case "join": {
				return this.#clone({
					joinCollections: new Map(joinCollections).set(key, collection),
					attachCollections: mapWithDeleted(attachCollections, key),

					hydrator: asFullHydrator(hydrator).has(
						collection.mode,
						key,
						makePrefix("", key),
						collection.querySet.#props.hydrator,
					),
				});
			}
			case "attach": {
				return this.#clone({
					joinCollections: mapWithDeleted(joinCollections, key),
					attachCollections: new Map(attachCollections).set(key, collection),

					hydrator: asFullHydrator(hydrator).attach(
						collection.mode,
						key,
						collection.fetchFn,
						collection.keys,
					),
				});
			}
		}
	}

	get _generics() {
		return undefined;
	}

	////////////////////////////////////////////////////////////
	// Query generation.
	////////////////////////////////////////////////////////////

	get #aliasedBaseQuery() {
		return this.#props.baseQuery.as(this.#props.baseAlias);
	}

	toBaseQuery(): AnySelectQueryBuilder {
		// TODO: This might need to be passed through `db` somehow to become executable.
		return this.#props.baseQuery;
	}

	/**
	 * Checks (recursively) if this query set is subject to row explosion (which also means it
	 * would cause row explosion if nested).
	 */
	#isCardinalityOne(): boolean {
		const { joinCollections } = this.#props;

		for (const collection of joinCollections.values()) {
			if (collection.mode === "many") {
				return false;
			}
			if (!collection.querySet.#isCardinalityOne()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Adds a single join to the query.
	 *
	 * @param isForSelection - If true, selections will be hoisted and prefixed.
	 * @param prefix - The prefix to use when hoisting selections.
	 * @param qb - The query builder to add the join to.
	 * @param key - The key of the join.
	 * @param collection - The collection to add the join to.
	 */
	#addCollectionAsJoin(
		prefix: string,
		qb: AnySelectQueryBuilder,
		key: string,
		collection: JoinCollection,
	): AnySelectQueryBuilder {
		// Add the join to the parent query.
		// This cast to a single method helps TypeScript follow the overloads.
		const from = collection.querySet.#toQuery(makePrefix(prefix, key)).as(key);
		qb = qb[collection.method as "innerJoin"](from, ...collection.args);

		// Add the (prefixed) selections from the subquery to the parent query.
		const hoistedSelections = hoistAndPrefixSelections(prefix, from);
		qb = qb.select(hoistedSelections);

		return qb;
	}

	#applyLimitAndOffset(qb: AnySelectQueryBuilder): AnySelectQueryBuilder {
		const { limit, offset } = this.#props;
		if (limit !== null) {
			qb = qb.limit(limit);
		}
		if (offset !== null) {
			qb = qb.offset(offset);
		}
		return qb;
	}

	#applyOrderBy(qb: AnySelectQueryBuilder): AnySelectQueryBuilder {
		// TODO
		return qb;
	}

	/**
	 * Returns a query guaranteed to return one row per entity in the result set,
	 * accounting for filtering joins (inner joins).  It will include
	 * cardinality-one left joins in addition to cardinality-one inner joins, and
	 * hoist selections for both.  It will convert cardinality-many inner joins,
	 * and all cross-joins to a WHERE EXISTS clause.
	 */
	#toCardinalityOneQuery(prefix: string): AnySelectQueryBuilder {
		const { joinCollections } = this.#props;

		let qb = db.selectFrom(this.#aliasedBaseQuery);

		for (const [key, collection] of joinCollections) {
			if (collection.querySet.#isCardinalityOne()) {
				qb = this.#addCollectionAsJoin(prefix, qb, key, collection);
			} else if (isFilteringJoin(collection)) {
				// Otherwise, build a dummy source and replay the join method onto
				// it so we don't have to figure out how to convert arbitrary joins
				// to select queries.
				qb = qb.where(({ exists, selectFrom, lit }) =>
					exists(
						selectFrom(k.sql`(SELECT 1)`.as("__"))
							.select(lit(1).as("_"))
							.$call((qb) => this.#addCollectionAsJoin(prefix, qb, key, collection)),
					),
				);

				// TODO: Convert the ON clause to WHERE clauses so we can use an
				// optimized version of the subquery and do:
				//
				// qb = qb.where(({ exists }) =>
				//   exists(collection.querySet.#toCardinalityOneQuery(makePrefix(prefix,
				// key))),
				// );
			}
		}

		return qb;
	}

	#toJoinedQuery(prefix: string): AnySelectQueryBuilder {
		const { baseAlias, joinCollections } = this.#props;

		let qb = db.selectFrom(this.#aliasedBaseQuery).selectAll(baseAlias);

		for (const [key, collection] of joinCollections) {
			qb = this.#addCollectionAsJoin(prefix, qb, key, collection);
		}

		// NOTE: Limit and offset cannot be applied here because of row explosion.

		// Apply ordering---but only if we're not prefixed, because ordering in
		// subqueries is ignored (well, "not guaranteed") unless you also have a
		// LIMIT or OFFSET.
		if (!prefix) {
			qb = this.#applyOrderBy(qb);
		}

		return qb;
	}

	toJoinedQuery(): AnySelectQueryBuilder {
		return this.#toJoinedQuery("");
	}

	#toQuery(prefix: string): AnySelectQueryBuilder {
		const { baseAlias, db, limit, offset, orderBy, joinCollections } = this.#props;

		// If we have no joins (no row explosion) and no ordering (nothing referencing
		// the baseAlias), we can just apply the limit and offset to the base query.
		if (!joinCollections.size && !orderBy) {
			return this.#applyLimitAndOffset(this.#props.baseQuery);
		}

		if (!limit && !offset) {
			return this.toJoinedQuery();
		}

		const cardinalityOneQuery = this.#toCardinalityOneQuery(prefix);

		let qb = db.selectFrom(cardinalityOneQuery.as(baseAlias));

		// Add any cardinality-many joins.
		for (const [key, collection] of joinCollections) {
			if (!collection.querySet.#isCardinalityOne()) {
				qb = this.#addCollectionAsJoin(prefix, qb, key, collection);
			}
		}

		// Re-apply ordering since the order from the subquery is not guaranteed to
		// be preserved.
		qb = this.#applyOrderBy(qb);

		return qb;
	}

	toQuery(): AnySelectQueryBuilder {
		return this.#toQuery("");
	}

	toCountQuery(): OpaqueCountQueryBuilder {
		return this.#toCardinalityOneQuery("")
			.clearSelect()
			.select((eb) => eb.fn.countAll().as("count"));
	}

	toExistsQuery(): OpaqueExistsQueryBuilder {
		return this.#props.db.selectNoFrom(({ exists }) =>
			exists(this.#toCardinalityOneQuery("")).as("exists"),
		);
	}

	compile() {
		return this.toQuery().compile();
	}

	toOperationNode() {
		return this.toQuery().toOperationNode();
	}

	////////////////////////////////////////////////////////////
	// Execution.
	////////////////////////////////////////////////////////////

	async execute(): Promise<any[]> {
		const rows = await this.toQuery().execute();

		return this.#props.hydrator.hydrate(
			rows,
			// Auto include fields at all levels, so we don't have to understand the
			// shape of the selection and can allow it to be inferred by the shape of
			// the rows.
			// @ts-expect-error - EnableAutoInclusion is a hidden parameter.
			EnableAutoInclusion,
		);
	}

	async executeTakeFirst(): Promise<any | undefined> {
		// We can't use `this.#props.qb.executeTakeFirst()` because it suppresses
		// rows for nested joins.
		const [result] = await this.execute();
		return result;
	}

	async executeTakeFirstOrThrow(
		errorConstructor: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error) = k.NoResultError,
	): Promise<any> {
		const result = await this.executeTakeFirst();

		// This is exactly what Kysely does.
		if (result === undefined) {
			const error = k.isNoResultErrorConstructor(errorConstructor)
				? new errorConstructor(this.toOperationNode())
				: errorConstructor(this.toOperationNode());

			throw error;
		}

		return result;
	}

	async executeCount(
		cast?: (count: string | number | bigint) => string | number | bigint,
	): Promise<any> {
		const { count } = await this.toCountQuery().executeTakeFirstOrThrow();
		return cast ? cast(count) : count;
	}

	async executeExists(): Promise<boolean> {
		const { exists } = await this.toExistsQuery().executeTakeFirstOrThrow();
		return Boolean(exists);
	}

	/////////////////////////////////////////////////////////////
	// Hydration.
	////////////////////////////////////////////////////////////

	map(transform: (row: any) => any): any {
		return this.#clone({
			hydrator: this.#props.hydrator.map(transform),
		});
	}

	extras(extras: Extras<any>) {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).extras(extras),
		});
	}

	mapFields(mappings: FieldMappings<any>) {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).fields(mappings),
		});
	}

	omit(keys: readonly PropertyKey[]) {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).omit(keys),
		});
	}

	with(hydrator: Hydrator<any, any>) {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).extend(hydrator),
		});
	}

	////////////////////////////////////////////////////////////
	// Attaches.
	////////////////////////////////////////////////////////////

	#addAttach(
		mode: CollectionMode,
		key: string,
		fetchFn: SomeFetchFn<any, any>,
		keys: AttachedKeysArg<any, any>,
	): any {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).attach(mode, key, fetchFn, keys),
		});
	}

	attachMany(key: string, fetchFn: SomeFetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("many", key, fetchFn, keys);
	}

	attachOne(key: string, fetchFn: SomeFetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("one", key, fetchFn, keys);
	}

	attachOneOrThrow(key: string, fetchFn: SomeFetchFn<any, any>, keys: AttachedKeysArg<any, any>) {
		return this.#addAttach("oneOrThrow", key, fetchFn, keys);
	}

	////////////////////////////////////////////////////////////
	// Joins.
	////////////////////////////////////////////////////////////

	#addJoin(
		method: JoinMethod,
		mode: CollectionMode,
		key: string,
		nestedQuerySet: NestedQuerySetOrFactory<any, any, any>,
		...args: AnyJoinArgsTail
	): any {
		const nest = ((
			query: SelectQueryBuilderOrFactory<any, any, any, any, any>,
			keyBy?: KeyBy<any>,
		) => {
			const creator = querySet(this.#props.db);
			return creator.init(key, query as any, keyBy as any);
		}) as any as InitWithAlias<any, any, any>;

		const resolved = typeof nestedQuerySet === "function" ? nestedQuerySet(nest) : nestedQuerySet;

		return this.#addCollection(key, {
			type: "join",
			method: method,
			mode: mode,
			querySet: resolved as QuerySetImpl,
			args: args,
		});
	}

	innerJoinOne(...args: AnyJoinArgs) {
		return this.#addJoin("innerJoin", "one", ...args);
	}

	innerJoinMany(...args: AnyJoinArgs) {
		return this.#addJoin("innerJoin", "many", ...args);
	}

	leftJoinOne(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoin", "one", ...args);
	}

	leftJoinOneOrThrow(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoin", "oneOrThrow", ...args);
	}

	leftJoinMany(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoin", "many", ...args);
	}

	crossJoinMany(...args: AnyJoinArgs) {
		return this.#addJoin("crossJoin", "many", ...args);
	}

	innerJoinLateralOne(...args: AnyJoinArgs) {
		return this.#addJoin("innerJoinLateral", "one", ...args);
	}

	innerJoinLateralMany(...args: AnyJoinArgs) {
		return this.#addJoin("innerJoinLateral", "many", ...args);
	}

	leftJoinLateralOne(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoinLateral", "one", ...args);
	}

	leftJoinLateralOneOrThrow(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoinLateral", "oneOrThrow", ...args);
	}

	leftJoinLateralMany(...args: AnyJoinArgs) {
		return this.#addJoin("leftJoinLateral", "many", ...args);
	}

	crossJoinLateralMany(...args: AnyJoinArgs) {
		return this.#addJoin("crossJoinLateral", "many", ...args);
	}

	////////////////////////////////////////////////////////////
	// Modification.
	////////////////////////////////////////////////////////////

	modify(
		keyOrModifier: string | ((qb: AnySelectQueryBuilder) => AnySelectQueryBuilder),
		modifier?: (value: any) => any,
	): any {
		if (typeof keyOrModifier === "function") {
			// It's safe to immediately apply modifications to the base query because
			// it is scoped within its own subselect.  The types capture this.
			return this.#clone({
				baseQuery: keyOrModifier(this.#props.baseQuery),
			});
		}

		if (!modifier) {
			throw new TypeError(`Modifier not provided for collection ${keyOrModifier}`);
		}

		const collection =
			this.#props.joinCollections.get(keyOrModifier) ||
			this.#props.attachCollections.get(keyOrModifier);

		if (!collection) {
			throw new TypeError(`Collection ${keyOrModifier} not found`);
		}

		switch (collection.type) {
			case "join": {
				return this.#addCollection(keyOrModifier, {
					...collection,
					querySet: modifier(collection.querySet),
				});
			}

			case "attach": {
				return this.#addCollection(keyOrModifier, {
					...collection,
					fetchFn: (...args: [any]) => modifier(collection.fetchFn(...args)),
				});
			}

			default: {
				assertNever(collection);
			}
		}
	}

	limit(limit: LimitOrOffset): any {
		return this.#clone({
			limit,
		});
	}

	offset(offset: LimitOrOffset): any {
		return this.#clone({
			offset,
		});
	}

	clearLimit(): any {
		return this.#clone({
			limit: null,
		});
	}

	clearOffset(): any {
		return this.#clone({
			offset: null,
		});
	}
}

////////////////////////////////////////////////////////////
// QuerySetCreator.
////////////////////////////////////////////////////////////

type KeyByToKeys<KB extends KeyBy<any>> =
	KB extends ReadonlyArray<any> ? KB[number] & string : KB & string;

interface InitialQuerySet<
	DB,
	BaseAlias extends string,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
	Keys extends string,
> extends QuerySet<{
	IsMapped: false;
	Keys: Keys;
	BaseAlias: BaseAlias;
	BaseQuery: {
		DB: BaseDB;
		TB: BaseTB;
		O: BaseO;
	};
	Collections: {};
	// The joined query mostly looks like the base query.
	JoinedQuery: {
		// The base query is wrapped in an alias in `SELECT $alias.* FROM (...) as
		// $alias`, so it's treated as another table.
		DB: DB & { [K in BaseAlias]: BaseO };
		// The base query alias is selected as an active table.
		TB: BaseAlias;
		// The output is the same as the base query output.
		O: BaseO;
	};
	// The hydrated output is the same as the base query output; no mapping yet.
	HydratedOutput: BaseO;
}> {}

type InferDB<Q> = Q extends k.SelectQueryBuilder<infer BaseDB, any, any> ? BaseDB : never;
type InferTB<Q> = Q extends k.SelectQueryBuilder<any, infer BaseTB, any> ? BaseTB : never;
type InferO<Q> = Q extends k.SelectQueryBuilder<any, any, infer BaseO> ? BaseO : never;

type SelectQueryBuilderFactory<
	DB,
	TB extends keyof DB,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
> = (eb: k.ExpressionBuilder<DB, TB>) => k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>;

type SelectQueryBuilderOrFactory<
	DB,
	TB extends keyof DB,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
> =
	| k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>
	| SelectQueryBuilderFactory<DB, TB, BaseDB, BaseTB, BaseO>;

interface InitWithAlias<DB, TB extends keyof DB, Alias extends string> {
	<BaseDB, BaseTB extends keyof BaseDB, BaseO extends InputWithDefaultKey>(
		query: SelectQueryBuilderOrFactory<DB, TB, BaseDB, BaseTB, BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, DEFAULT_KEY_BY>;
	<BaseDB, BaseTB extends keyof BaseDB, BaseO, KB extends KeyBy<NoInfer<BaseO>>>(
		query: k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>,
		keyBy: KB,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, KeyByToKeys<KB>>;
	// Infer output from ReturnType<F> to avoid circular inference.
	<
		F extends SelectQueryBuilderFactory<DB, TB, any, any, any>,
		Q extends k.SelectQueryBuilder<any, any, any> = ReturnType<F>,
		BaseDB = InferDB<Q>,
		BaseTB extends keyof BaseDB = InferTB<Q>,
		BaseO = InferO<Q>,
		KB extends KeyBy<NoInfer<BaseO>> = KeyBy<NoInfer<BaseO>>,
	>(
		query: F,
		keyBy: KB,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, KeyByToKeys<KB>>;
}

class QuerySetCreator<DB> {
	#db: k.Kysely<DB>;

	constructor(db: k.Kysely<DB>) {
		this.#db = db;
	}

	// Builder overloads.
	init<
		Alias extends string,
		BaseDB,
		BaseTB extends keyof BaseDB,
		BaseO extends InputWithDefaultKey,
	>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, never, BaseDB, BaseTB, BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, DEFAULT_KEY_BY>;
	init<
		Alias extends string,
		BaseDB,
		BaseTB extends keyof BaseDB,
		BaseO,
		KB extends KeyBy<NoInfer<BaseO>>,
	>(
		alias: Alias,
		query: k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>,
		keyBy: KB,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, KeyByToKeys<KB>>;
	// Infer output from ReturnType<F> to avoid circular inference.
	init<
		Alias extends string,
		F extends SelectQueryBuilderFactory<DB, never, any, any, any>,
		Q extends k.SelectQueryBuilder<any, any, any> = ReturnType<F>,
		BaseDB = InferDB<Q>,
		BaseTB extends keyof BaseDB = InferTB<Q>,
		BaseO = InferO<Q>,
		KB extends KeyBy<NoInfer<BaseO>> = KeyBy<NoInfer<BaseO>>,
	>(
		alias: Alias,
		query: F,
		keyBy: KB,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO, KeyByToKeys<KB>>;
	init(
		alias: string,
		query: any,
		keyBy: KeyBy<any> = DEFAULT_KEY_BY,
	): InitialQuerySet<DB, string, any, any, any, any> {
		const baseQuery = typeof query === "function" ? query(k.expressionBuilder()) : query;

		return new QuerySetImpl({
			db: this.#db,
			baseAlias: alias,
			baseQuery,
			keyBy: keyBy,
			hydrator: createHydrator(),
			joinCollections: new Map(),
			attachCollections: new Map(),
			limit: null,
			offset: null,
			orderBy: null,
		});
	}
}

export function querySet<DB>(db: k.Kysely<DB>): QuerySetCreator<DB> {
	return new QuerySetCreator(db);
}
