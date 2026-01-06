import * as k from "kysely";

import { makePrefix } from "./helpers/prefixes.ts";
import {
	type Extend,
	type ExtendWith,
	type Flatten,
	type KeyBy,
	type KeysWithValueOfType,
	type StrictSubset,
	assertNever,
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
type TCollectionType = TCollection["Type"];

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

interface TQuerySet {
	/**
	 * Indicates whether the query set has been mapped.
	 */
	IsMapped: boolean;
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
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	HydratedOutput: Output;
}

interface TWithExtendedOutput<T extends TQuerySet, Output> {
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	HydratedOutput: Extend<TOutput<T>, Output>;
}

////////////////////////////////////////////////////////////
// Interfaces.
////////////////////////////////////////////////////////////

interface MappedQuerySet<T extends TQuerySet> extends k.Compilable, k.OperationNodeSource {
	/**
	 * This property exists for complex type reasons and will never be set.
	 *
	 * @internal
	 */
	// Required so that the type system can infer all the generics the even when
	// nested collections return a MappedQuerySet instead of a full QuerySet.
	readonly _generics: T | undefined;

	readonly baseAlias: T["BaseAlias"];

	toBaseQuery(): SelectQueryBuilderFor<T["BaseQuery"]>;

	toJoinedQuery(): SelectQueryBuilderFor<T["JoinedQuery"]>;

	// TODO
	toQuery(): k.SelectQueryBuilder<any, any, any>;

	/**
	 * Executes the query and returns an array of rows.
	 *
	 * Also see the {@link executeTakeFirst} and {@link executeTakeFirstOrThrow} methods.
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
	executeCountAll(toBigInt: (count: string | number | bigint) => bigint): Promise<bigint>;
	executeCountAll(toNumber: (count: string | number | bigint) => number): Promise<number>;
	executeCountAll(toString: (count: string | number | bigint) => string): Promise<string>;
	executeCountAll(): Promise<string | number | bigint>;

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
	modify(
		modifier: (qb: SelectQueryBuilderFor<T["BaseQuery"]>) => SelectQueryBuilderFor<T["BaseQuery"]>,
	): this;
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
	 *   .init(db.selectFrom("users").select(["users.id", "users.name"]))
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
	 *   .init(db.selectFrom("posts").select(["posts.id", "posts.title"]))
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
	 *
	 * When called with two arguments, allows you to modify a nested collection
	 * (either a join or attach).
	 */
	// Modify base query.
	// TODO: Should you be allowed to change the shape here?
	modify(
		modifier: (qb: SelectQueryBuilderFor<T["BaseQuery"]>) => SelectQueryBuilderFor<T["BaseQuery"]>,
	): this;
	// Modify collection.
	modify<
		Key extends keyof T["Collections"] & string,
		TNestedNew extends TQuerySet = never,
		NewValue extends SomeFetchFnReturn = never,
	>(
		key: Key,
		modifier: CollectionModifier<T["Collections"][NoInfer<Key>], TNestedNew, NewValue>,
	): ModifyCollectionReturn<
		T,
		NoInfer<Key> & string,
		T["Collections"][NoInfer<Key>]["Type"],
		TNestedNew,
		NewValue
	>;
}

////////////////////////////////////////////////////////////
// Modify Helpers.
////////////////////////////////////////////////////////////

type CollectionModifier<
	Collection extends TCollection,
	TNestedNew extends TQuerySet,
	NewValue extends SomeFetchFnReturn,
> = Collection["Prototype"] extends "Join"
	? (value: QuerySetFor<Collection["Value"]>) => MappedQuerySet<TNestedNew>
	: (value: Collection["Value"]) => NewValue;

interface ModifyCollectionReturns<
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

type ModifyCollectionReturn<
	T extends TQuerySet,
	Key extends string,
	CollectionType extends TCollectionType,
	TNestedNew extends TQuerySet,
	NewValue extends SomeFetchFnReturn,
> = ModifyCollectionReturns<T, Key, TNestedNew, NewValue>[CollectionType];

////////////////////////////////////////////////////////////
// Attach Helpers.
////////////////////////////////////////////////////////////

type QuerySetWithAttach<
	T extends TQuerySet,
	Collections extends TCollections,
	Key extends string,
	AttachedOutput,
> = QuerySet<{
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	JoinedQuery: T["JoinedQuery"];
	Collections: Collections;
	HydratedOutput: ExtendWith<TOutput<T>, Key, AttachedOutput>;
}>;

type QuerySetWithAttachMany<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> = QuerySetWithAttach<
	T,
	TCollectionsWith<
		TCollections,
		Key,
		{ Prototype: "Attach"; Type: "AttachMany"; Value: FetchFnReturn }
	>,
	Key,
	AttachedOutputFromFetchFnReturn<FetchFnReturn>[]
>;

type QuerySetWithAttachOne<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> = QuerySetWithAttach<
	T,
	TCollectionsWith<
		TCollections,
		Key,
		{ Prototype: "Attach"; Type: "AttachOne"; Value: FetchFnReturn }
	>,
	Key,
	AttachedOutputFromFetchFnReturn<FetchFnReturn> | null
>;

type QuerySetWithAttachOneOrThrow<
	T extends TQuerySet,
	Key extends string,
	FetchFnReturn extends SomeFetchFnReturn,
> = QuerySetWithAttach<
	T,
	TCollectionsWith<
		TCollections,
		Key,
		{ Prototype: "Attach"; Type: "AttachOneOrThrow"; Value: FetchFnReturn }
	>,
	Key,
	AttachedOutputFromFetchFnReturn<FetchFnReturn>
>;

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

// type JoinTableExpression<T extends TQuerySet, NestedRow> =
// 	| k.AliasableExpression<NestedRow>
// 	| ((eb: ExpressionBuilderFor<T["JoinedQuery"]>) => k.AliasableExpression<NestedRow>);

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

type AnyJoinArgs = [key: string, from: any, callbackOrk1?: any, k2?: any];
type AnyJoinArgsTail = [callbackOrk1?: any, k2?: any];

interface ModifyOperation {
	readonly type: "modify";
	readonly modifier: (qb: AnySelectQueryBuilder) => AnySelectQueryBuilder;
}

// interface JoinOperation {
// 	readonly type: "join";
// 	readonly key: string;
// 	readonly method: JoinMethod;
// 	readonly mode: CollectionMode;
// 	readonly querySet: MappedQuerySet<any>;
// 	readonly args: AnyJoinArgsTail;
// }

type Operation = ModifyOperation;

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
	collections: Map<string, Collection>;
	operations: readonly Operation[];
	// limit: number | null;
	// offset: number | null;
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

	#addOperation(operation: Operation): QuerySetImpl {
		return this.#clone({
			operations: [...this.#props.operations, operation],
		});
	}

	#addCollectionToHydrator(key: string, collection: Collection): Hydrator<any, any> {
		switch (collection.type) {
			case "join": {
				return asFullHydrator(this.#props.hydrator).has(
					collection.mode,
					key,
					makePrefix("", key),
					collection.querySet.#props.hydrator,
				);
			}
			case "attach": {
				return asFullHydrator(this.#props.hydrator).attach(
					collection.mode,
					key,
					collection.fetchFn,
					collection.keys,
				);
			}
		}
	}

	#addCollection(key: string, collection: Collection): QuerySetImpl {
		return this.#clone({
			// Hydrators maintain a Map of collections, so this will correctly
			// overwrite any previous collection definition with the same key.
			hydrator: this.#addCollectionToHydrator(key, collection),

			collections: new Map(this.#props.collections).set(key, collection),
		});
	}

	get _generics() {
		return undefined;
	}

	////////////////////////////////////////////////////////////
	// Query generation.
	////////////////////////////////////////////////////////////

	get baseAlias() {
		return this.#props.baseAlias;
	}

	toBaseQuery(): AnySelectQueryBuilder {
		return this.#props.baseQuery;
	}

	toJoinedQuery(): AnySelectQueryBuilder {
		throw new Error("Not implemented");
	}

	toQuery(): AnySelectQueryBuilder {
		// TODO: How is this different from toJoinedQuery?
		throw new Error("Not implemented");
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

	async execute(): Promise<never> {
		throw new Error("Not implemented");
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

	async executeCountAll(
		cast?: (count: string | number | bigint) => string | number | bigint,
	): Promise<any> {
		// TODO
		const result = {} as any;

		return cast ? cast(result.count) : result.count;
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
		const nest: InitWithAlias<any, any, any> = (
			query: SelectQueryBuilderOrFactory<any, any, any, any, any>,
			keyBy?: KeyBy<any>,
		) => querySet(this.#props.db).init(key, query, keyBy);

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
			return this.#addOperation({
				type: "modify",
				modifier: keyOrModifier,
			});
		}

		const collection = this.#props.collections.get(keyOrModifier);

		if (!collection) {
			throw new TypeError(`Collection ${keyOrModifier} not found`);
		}

		if (!modifier) {
			throw new TypeError(`Modifier not provided for collection ${keyOrModifier}`);
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
}

////////////////////////////////////////////////////////////
// QuerySetCreator.
////////////////////////////////////////////////////////////

interface InitialQuerySet<
	DB,
	BaseAlias extends string,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
> extends QuerySet<{
	IsMapped: false;
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

type SelectQueryBuilderOrFactory<
	DB,
	TB extends keyof DB,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
> =
	| k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>
	| ((eb: k.ExpressionBuilder<DB, TB>) => k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>);

interface InitWithAlias<DB, TB extends keyof DB, Alias extends string> {
	<BaseDB, BaseTB extends keyof BaseDB, BaseO extends InputWithDefaultKey>(
		query: SelectQueryBuilderOrFactory<DB, TB, BaseDB, BaseTB, BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	<BaseDB, BaseTB extends keyof BaseDB, BaseO>(
		query: SelectQueryBuilderOrFactory<DB, TB, BaseDB, BaseTB, BaseO>,
		keyBy: KeyBy<BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
}

class QuerySetCreator<DB> {
	#db: k.Kysely<DB>;

	constructor(db: k.Kysely<DB>) {
		this.#db = db;
	}

	init<
		Alias extends string,
		BaseDB,
		BaseTB extends keyof BaseDB,
		BaseO extends InputWithDefaultKey,
	>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, never, BaseDB, BaseTB, BaseO>,
		keyBy?: KeyBy<NoInfer<BaseO>>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	init<Alias extends string, BaseDB, BaseTB extends keyof BaseDB, BaseO>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, never, BaseDB, BaseTB, BaseO>,
		keyBy: KeyBy<NoInfer<BaseO>>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	init(
		alias: string,
		query: SelectQueryBuilderOrFactory<any, any, any, any, any>,
		keyBy: KeyBy<any> = DEFAULT_KEY_BY,
	): InitialQuerySet<DB, string, any, any, any> {
		const baseQuery = typeof query === "function" ? query(k.expressionBuilder()) : query;

		return new QuerySetImpl({
			db: this.#db,
			baseAlias: alias,
			baseQuery,
			keyBy: keyBy,
			hydrator: createHydrator(),
			collections: new Map(),
			operations: [],
		});
	}
}

export function querySet<DB>(db: k.Kysely<DB>): QuerySetCreator<DB> {
	return new QuerySetCreator(db);
}
