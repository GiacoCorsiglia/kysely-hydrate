/**
 * @module query-set
 *
 * This module provides the `querySet` API for building queries with nested joins
 * and automatic hydration of flat SQL results into nested objects.
 *
 * Key features:
 * - Nested subqueries for better SQL isolation
 * - Correct pagination with row explosion from many-joins
 * - Type-safe nested joins with cardinality constraints
 * - Flexible hydration with field transformations and computed values
 * - Support for both SQL joins and non-SQL attaches
 *
 * @see {@link querySet} - Main entry point
 * @see {@link QuerySet} - Query builder interface
 * @see {@link MappedQuerySet} - Mapped query builder interface
 */

import * as k from "kysely";

import { kyselyOrderByToOrderBy } from "./helpers/order-by.ts";
import {
	type ApplyPrefixes,
	type ApplyPrefixWithSep,
	type MakePrefix,
	makePrefix,
	SEP,
} from "./helpers/prefixes.ts";
import { hoistAndPrefixSelections, hoistSelections } from "./helpers/select-renamer.ts";
import {
	type DrainOuterGeneric,
	type Extend,
	type ExtendWith,
	type Flatten,
	type KeyBy,
	type StrictEqual,
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
	EnableAutoInclusion,
} from "./hydrator.ts";

////////////////////////////////////////////////////////////
// Generics.
////////////////////////////////////////////////////////////

interface TQuery<in out DB = any, in out TB extends keyof DB = any> {
	DB: DB;
	TB: TB;
	O: any;
}

type InferTQuery<Q extends AnySelectQueryBuilder> =
	Q extends k.SelectQueryBuilder<infer DB, infer TB, infer O> ? { DB: DB; TB: TB; O: O } : never;

type SelectQueryBuilderFor<Q extends TQuery> = k.SelectQueryBuilder<Q["DB"], Q["TB"], Q["O"]>;

interface TJoinCollection {
	Prototype: "Join";
	Type: "InnerJoinOne" | "InnerJoinMany" | "LeftJoinOne" | "LeftJoinOneOrThrow" | "LeftJoinMany";
	NestedHO: any;
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
 *
 * Note: HydratedOutput is NOT part of this bag - it's a separate type parameter
 * on QuerySet and MappedQuerySet to improve variance behavior.
 */
interface TQuerySet {
	/**
	 * The original shape of the database schema from k.Kysely<DB>.
	 */
	DB: any;
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
	 * Columns that can be used in ORDER BY clauses (i.e., columns from
	 * cardinality-one joins).
	 */
	OrderableColumns: string;
}

type QuerySetFor<HO, T extends TQuerySet> = T["IsMapped"] extends true
	? MappedQuerySet<HO, T>
	: QuerySet<HO, T>;

type TInput<T extends TQuerySet> = T["JoinedQuery"]["O"];
type TOutput<HO> = Flatten<HO>;

interface TMapped<in out T extends TQuerySet> {
	DB: T["DB"];
	IsMapped: true;
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	OrderableColumns: T["OrderableColumns"];
}

interface TJoinedQueryWithBaseQuery<
	BaseAlias extends string,
	in out JoinedQuery extends TQuery,
	in out BaseQuery extends TQuery,
> {
	DB: JoinedQuery["DB"] & { [_ in BaseAlias]: BaseQuery["O"] };
	TB: JoinedQuery["TB"];
	O: JoinedQuery["O"];
}

interface TWithBaseQuery<in out T extends TQuerySet, in out BaseQuery extends TQuery> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: BaseQuery;
	Collections: T["Collections"];
	JoinedQuery: TJoinedQueryWithBaseQuery<T["BaseAlias"], T["JoinedQuery"], BaseQuery>;
	OrderableColumns: T["OrderableColumns"] | (keyof BaseQuery["O"] & string);
}

/**
 * Computes the new hydrated output when the base query is extended.
 */
type TWithBaseQueryOutput<HO, BaseQuery extends TQuery> = Extend<BaseQuery["O"], TOutput<HO>>;

interface InitialJoinedQuery<in out DB, in out BaseAlias extends string, in out BaseO> {
	// The base query is wrapped in an alias in `SELECT $alias.* FROM (...) as
	// $alias`, so it's treated as another table.
	DB: DB & { [K in BaseAlias]: BaseO };
	// The base query alias is selected as an active table.
	TB: BaseAlias;
	// The output is the same as the base query output.
	O: BaseO;
}

type ToInitialJoinedDB<T extends TQuerySet> = DrainOuterGeneric<
	T["DB"] & {
		[K in T["BaseAlias"]]: T["BaseQuery"]["O"];
	}
>;
type ToInitialJoinedTB<T extends TQuerySet> = T["BaseAlias"];

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

/**
 * A query set that has been mapped with a transformation function.
 *
 * After calling `.map()`, only query execution and further mapping are available.
 * You cannot continue to add joins, modify hydration, or otherwise change the
 * shape of the query's input, since that would affect the input expected by the
 * transformation function.
 *
 * @template HO - The hydrated output type.
 * @template T - The query set's type parameters.
 */
// oxlint-disable-next-line namespace I don't know why oxlint can't find these interfaces.
interface MappedQuerySet<out HO, in out T extends TQuerySet>
	extends k.Compilable,
		k.OperationNodeSource {
	/**
	 * These properties exist for complex type reasons and will never be set.
	 *
	 * @internal
	 */
	// Required so that the type system can infer all the generics the even when
	// nested collections return a MappedQuerySet instead of a full QuerySet.
	// readonly $inferType: HO;
	readonly _generics: T | undefined;

	/**
	 * Returns the base query that this query set was initialized with, plus any
	 * modifications made via `.modify()`.
	 *
	 * This does not include any joins or hydration configuration, including
	 * limit, offset, or ordering.
	 *
	 * **Example:**
	 * ```ts
	 * const qs = querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("isActive", "=", true));
	 *
	 * const baseQuery = qs.toBaseQuery();
	 * // SELECT id, username FROM users WHERE isActive = true
	 * ```
	 */
	toBaseQuery(): SelectQueryBuilderFor<T["BaseQuery"]>;

	/**
	 * Returns the base query with all joins applied as nested subqueries.
	 *
	 * **Warning:** This query is subject to "row explosion." If a base record has
	 * multiple related child records via many-joins, the base record will appear
	 * multiple times in the result set. Limit and offset are not applied to this
	 * query.
	 *
	 * **Example SQL structure:**
	 * ```sql
	 * SELECT *
	 * FROM (
	 *   SELECT * FROM users WHERE isActive = true
	 * ) AS user
	 * LEFT JOIN (
	 *   SELECT * FROM posts
	 * ) AS posts ON posts.userId = user.id
	 * ```
	 */
	toJoinedQuery(): SelectQueryBuilderFor<T["JoinedQuery"]>;

	/**
	 * Returns the query that will actually be executed when you call `.execute()`.
	 *
	 * This query handles row explosion by using nested subqueries internally when
	 * necessary for correct pagination. The exact SQL structure is an implementation
	 * detail and may change.
	 *
	 * **Note:** The result is still subject to row explosion (you'll get duplicate
	 * base records if there are many-joins), but pagination (limit/offset) will be
	 * applied correctly to unique base records.
	 *
	 * **Example SQL structure (may vary):**
	 * ```sql
	 * SELECT *
	 * FROM (
	 *   -- Cardinality-one subquery with limit/offset applied
	 *   SELECT * FROM (...) AS user
	 *   LEFT JOIN (...) AS profile ON ...
	 *   LIMIT 10
	 * ) AS user
	 * -- Cardinality-many joins applied afterward
	 * LEFT JOIN (...) AS posts ON posts.userId = user.id
	 * ```
	 */
	toQuery(): OpaqueSelectQueryBuilder<T["JoinedQuery"]["O"]>;

	/**
	 * Returns a query that counts unique base records when executed.
	 *
	 * This correctly handles filtering many-joins (like `innerJoinMany`) by
	 * converting them to `WHERE EXISTS` clauses. Pagination (limit/offset) is
	 * ignored.
	 *
	 * **Example:**
	 * ```ts
	 * const count = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany("posts", ...)
	 *   .limit(10)
	 *   .toCountQuery()
	 *   .executeTakeFirstOrThrow();
	 *
	 * console.log(count.count); // Total count, ignoring the limit
	 * ```
	 *
	 * **Example SQL structure (may vary):**
	 * ```sql
	 * SELECT COUNT(*) AS count
	 * FROM (
	 *   SELECT DISTINCT user.id
	 *   FROM (...) AS user
	 *   WHERE EXISTS (SELECT 1 FROM (...) AS posts WHERE ...)
	 * )
	 * ```
	 */
	toCountQuery(): OpaqueCountQueryBuilder;

	/**
	 * Returns a query that checks whether any base records exist when executed.
	 *
	 * Like `toCountQuery`, this correctly handles filtering many-joins and ignores
	 * pagination.
	 *
	 * **Example:**
	 * ```ts
	 * const existsQuery = querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("username", "=", "alice"))
	 *   .toExistsQuery();
	 *
	 * const { exists } = await existsQuery.executeTakeFirstOrThrow();
	 * console.log(exists); // true or false
	 * ```
	 *
	 * **Example SQL structure:**
	 * ```sql
	 * SELECT EXISTS (
	 *   SELECT * FROM (...) AS user WHERE username = 'alice'
	 * ) AS exists
	 * ```
	 */
	toExistsQuery(): OpaqueExistsQueryBuilder;

	/**
	 * Executes the query and returns an array of hydrated rows.
	 *
	 * Nested collections (from joins and attaches) will be hydrated into nested
	 * objects and arrays according to the configuration.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", (init) =>
	 *     init("post", eb => eb.selectFrom("posts").select(["id", "title"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   posts: Array<{ id: number; title: string }>;
	 * }>;
	 * ```
	 *
	 * Also see {@link executeTakeFirst} and {@link executeTakeFirstOrThrow}.
	 */
	execute(): Promise<TOutput<HO>[]>;

	/**
	 * Executes the query and returns the first hydrated result, or `undefined` if
	 * the query returned no results.
	 *
	 * **Example:**
	 * ```ts
	 * const user = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("id", "=", 1))
	 *   .executeTakeFirst();
	 *
	 * if (user) {
	 *   console.log(user.username);
	 * }
	 * ```
	 */
	executeTakeFirst(): Promise<TOutput<HO> | undefined>;

	/**
	 * Executes the query and returns the first hydrated result, or throws if the
	 * query returned no results.
	 *
	 * By default, throws a {@link k.NoResultError}, but you can provide a custom
	 * error constructor or factory function.
	 *
	 * **Example:**
	 * ```ts
	 * const user = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("id", "=", 1))
	 *   .executeTakeFirstOrThrow();
	 *
	 * console.log(user.username); // Safe - will throw if not found
	 * ```
	 *
	 * **Example with custom error:**
	 * ```ts
	 * class UserNotFoundError extends Error {
	 *   constructor() {
	 *     super("User not found");
	 *   }
	 * }
	 *
	 * const user = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("id", "=", 1))
	 *   .executeTakeFirstOrThrow(UserNotFoundError);
	 * ```
	 */
	executeTakeFirstOrThrow(
		errorConstructor?: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error),
	): Promise<TOutput<HO>>;

	/**
	 * Executes the count query (via {@link toCountQuery}) and returns the count of
	 * unique base records.
	 *
	 * By default, Kysely's count function returns `string | number | bigint`. You
	 * can provide a transformation function to convert the count to your preferred
	 * numeric type.
	 *
	 * **Example:**
	 * ```ts
	 * const count = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .executeCount(); // string | number | bigint
	 * ```
	 *
	 * **Example with type conversion:**
	 * ```ts
	 * const count = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .executeCount(Number); // number
	 *
	 * const bigCount = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .executeCount(BigInt); // bigint
	 * ```
	 */
	executeCount(toBigInt: (count: string | number | bigint) => bigint): Promise<bigint>;
	executeCount(toNumber: (count: string | number | bigint) => number): Promise<number>;
	executeCount(toString: (count: string | number | bigint) => string): Promise<string>;
	executeCount(): Promise<string | number | bigint>;

	/**
	 * Executes the exists query (via {@link toExistsQuery}) and returns a boolean
	 * indicating whether any base records exist.
	 *
	 * **Example:**
	 * ```ts
	 * const hasActiveUsers = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id"]))
	 *   .modify((qb) => qb.where("isActive", "=", true))
	 *   .executeExists();
	 *
	 * if (hasActiveUsers) {
	 *   console.log("We have active users!");
	 * } else {
	 *   console.log("Hmm, better do another fundraising round...")
	 * }
	 * ```
	 */
	executeExists(): Promise<boolean>;

	/**
	 * Applies a transformation function to the hydrated output.
	 *
	 * This is a terminal operation: after calling `.map()`, only `.map()` and
	 * execution methods are available. You cannot continue to add joins, modify
	 * hydration, or otherwise change the query's input shape, since that would
	 * affect the input expected by the transformation function.
	 *
	 * Use this for complex transformations such as:
	 * - Hydrating into class instances
	 * - Asserting discriminated union types
	 * - Complex data reshaping
	 *
	 * For simple field transformations, prefer `.mapFields()` or `.extras()`.
	 *
	 * **Example - Hydrating into class instances:**
	 * ```ts
	 * class User {
	 *   constructor(public id: number, public username: string) {}
	 *   greet() {
	 *     return `Hello, I'm ${this.username}`;
	 *   }
	 * }
	 *
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .map((row) => new User(row.id, row.username))
	 *   .execute();
	 *
	 * console.log(users[0].greet()); // "Hello, I'm alice"
	 * ```
	 *
	 * **Example - Asserting discriminated unions:**
	 * ```ts
	 * type AdminUser = { id: number; role: "admin"; permissions: string[] };
	 * type RegularUser = { id: number; role: "user" };
	 * type User = AdminUser | RegularUser;
	 *
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "role", "permissions"]))
	 *   .map((row): User => {
	 *     if (row.role === "admin") {
	 *       return { id: row.id, role: "admin", permissions: row.permissions ?? [] };
	 *     }
	 *     return { id: row.id, role: "user" };
	 *   })
	 *   .execute();
	 * ```
	 *
	 * @param transform - A function that transforms each hydrated row.
	 * @returns A MappedQuerySet with the transformation applied.
	 */
	map<NewHydratedOutput>(
		transform: (row: TOutput<HO>) => NewHydratedOutput,
	): MappedQuerySet<NewHydratedOutput, TMapped<T>>;

	/**
	 * Allows you to modify the base select query. Useful for adding `WHERE`
	 * clauses, additional selections, or other query modifications.
	 *
	 * **Note:** You cannot use `.clearSelect()` or otherwise remove selections
	 * that were already made, but you can add additional columns.
	 *
	 * **Example - Adding WHERE clauses:**
	 * ```ts
	 * const activeUsers = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username", "isActive"]))
	 *   .modify((qb) => qb.where("isActive", "=", true))
	 *   .execute();
	 * ```
	 *
	 * **Example - Adding additional selections:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) =>
	 *     qb
	 *       .leftJoin("posts", "posts.userId", "users.id")
	 *       .select((eb) => eb.fn.count("posts.id").as("postCount"))
	 *       .groupBy("users.id")
	 *   )
	 *   .execute();
	 * ```
	 */
	// Simple overload for simple case.
	modify<O extends StrictEqual<T["BaseQuery"]["O"], O>>(
		modifier: (
			qb: SelectQueryBuilderFor<T["BaseQuery"]>,
		) => k.SelectQueryBuilder<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"], O>,
	): this;

	/**
	 * Adds a `where` expression to the base query.
	 *
	 * This is a convenience method, exactly equivalent to calling
	 * ```ts
	 * myQuerySet.modify((qb) => qb.where(...));
	 * ```
	 * where `qb.where()` is Kysely's `where` method.
	 *
	 * @see {@link k.WhereInterface.where()} for more information.
	 *
	 * **Example - Simple where clause:**
	 * ```ts
	 * const activeUsers = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .where("users.isActive", "=", true)
	 *   .execute();
	 * ```
	 *
	 * **Example - Expression-based where:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username", "age"]))
	 *   .where((eb) => eb.or([
	 *     eb("users.age", "<", 18),
	 *     eb("users.age", ">", 65)
	 *   ]))
	 *   .execute();
	 * ```
	 */
	where<
		RE extends k.ReferenceExpression<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"]>,
		VE extends k.OperandValueExpressionOrList<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"], RE>,
	>(
		lhs: RE,
		op: k.ComparisonOperatorExpression,
		rhs: VE,
	): this;
	where<E extends k.ExpressionOrFactory<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"], k.SqlBool>>(
		expression: E,
	): this;

	/**
	 * Adds a limit clause to the query, correctly handling row explosion from
	 * many-joins.
	 *
	 * The limit is applied to unique base records, not to the exploded rows. The
	 * query builder will use nested subqueries internally to ensure correct
	 * pagination.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ...)
	 *   .limit(10)
	 *   .execute();
	 * // Returns 10 users, each with their full array of posts
	 * ```
	 *
	 * **Note:** Unlike Kysely's `.limit()`, this does not accept
	 * {@link k.ValueExpression} because the limit may be applied to different
	 * internal queries depending on your join structure.
	 */
	limit(limit: LimitOrOffset): this;

	/**
	 * Clears the limit clause from the query.
	 *
	 * **Example:**
	 * ```ts
	 * const query = querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .limit(10);
	 *
	 * const allUsers = await query.clearLimit().execute();
	 * ```
	 */
	clearLimit(): this;

	/**
	 * Adds an offset clause to the query, correctly handling row explosion from
	 * many-joins.
	 *
	 * The offset is applied to unique base records, not to the exploded rows. The
	 * query builder will use nested subqueries internally to ensure correct
	 * pagination.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ...)
	 *   .offset(20)
	 *   .limit(10)
	 *   .execute();
	 * // Returns users 21-30, each with their full array of posts
	 * ```
	 *
	 * **Note:** Unlike Kysely's `.offset()`, this does not accept
	 * {@link k.ValueExpression} because the offset may be applied to different
	 * internal queries depending on your join structure.
	 */
	offset(offset: LimitOrOffset): this;

	/**
	 * Clears the offset clause from the query.
	 *
	 * **Example:**
	 * ```ts
	 * const query = querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .offset(20)
	 *   .limit(10);
	 *
	 * const firstTen = await query.clearOffset().execute();
	 * ```
	 */
	clearOffset(): this;

	/**
	 * Adds an ORDER BY clause to the query.  Note that query sets are always
	 * ordered by the keyBy columns used to deduplicate rows (default `id`) to
	 * guarantee stable ordering (sorted ascending).  This method allows you to
	 * add additional ordering criteria to the query.  If you do, the keyBy
	 * columns will be used as a tie-breaker (appended to the end of the ORDER BY
	 * clause).
	 *
	 * To customize the keyBy behavior, you may explicitly call `.orderBy()` with
	 * one of the keyBy columns.  If you do, that column will be inserted into the
	 * ORDER BY clause in the position of your call, with any remaining keyBy
	 * columns appended to the end of the query.
	 *
	 * To completely disable the keyBy behavior, you may call `.noKeyOrdering()`.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinOne(
	 *     "profile",
	 *     (init) => init((eb) => eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
	 *     "profile.user_id",
	 *     "user.id"
	 *   )
	 *   // Reference base table column
	 *   .orderBy("username")
	 *   // Reference nested join column from cardinality-one join
	 *   .orderBy("profile$$bio")
	 *   .execute();
	 * // Returns users ordered by "username", then by "profile.bio".
	 * ```
	 */
	orderBy(expr: T["OrderableColumns"], modifiers?: k.OrderByModifiers): this;

	/**
	 * Clears custom ORDER BY clauses from the query (note, the query will still
	 * be ordered by the keyBy columns).
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .orderBy("username")
	 *   .clearOrderBy()
	 *   .execute();
	 * // Returns users ordered by "id".
	 * ```
	 */
	clearOrderBy(): this;

	/**
	 * By default, query sets are ordered by the keyBy columns used to deduplicate
	 * rows (default `id`) to guarantee stable ordering (sorted ascending).  Call
	 * this method with `false` to disable this behavior for this query set.  Call
	 * it with `true` to re-enable it.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .orderByKeys(false)
	 *   .execute();
	 * // Returns users ordered by "id".
	 * ```
	 */
	orderByKeys(enabled?: boolean): this;

	/**
	 * Calls {@link k.SelectQueryBuilder.modifyFront} on the query before it is
	 * executed.
	 */
	modifyFront(modifier: k.Expression<any>): this;

	/**
	 * Calls {@link k.SelectQueryBuilder.modifyEnd} on the query before it is
	 * executed.
	 */
	modifyEnd(modifier: k.Expression<any>): this;

	/**
	 * Calls a callback with the query set and returns the result.  Like {@link k.SelectQueryBuilder.$call}.
	 */
	$call<R>(callback: (qs: this) => R): R;
}

/**
 * A query set that supports nested joins and automatic hydration.
 *
 * QuerySet extends {@link MappedQuerySet} with additional methods for:
 * - Configuring hydration (extras, mapFields, omit, with)
 * - Adding nested collections via joins (innerJoinOne, leftJoinMany, etc.)
 * - Adding nested collections via attaches (attachOne, attachMany, etc.)
 * - Modifying nested collections
 *
 * After calling `.map()`, the query set becomes a {@link MappedQuerySet} and
 * these additional methods are no longer available.
 *
 * @template HO - The hydrated output type.
 * @template T - The query set's type parameters.
 */
interface QuerySet<HO, in out T extends TQuerySet> extends MappedQuerySet<HO, T> {
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
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   firstName: string;
	 *   lastName: string;
	 *   fullName: string;
	 * }>;
	 * ```
	 *
	 * @param extras - An object mapping field names to functions that compute
	 *   the field value from the entire row.
	 * @returns A new HydratedQueryBuilder with the extras applied.
	 */
	extras<E extends Extras<TInput<T>>>(extras: E): QuerySet<Extend<HO, InferExtras<TInput<T>, E>>, T>;

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
	 * // ⬇
	 * type Result = Array<{ id: number; name: string }>;
	 * ```
	 *
	 * @param mappings - An object mapping field names to transformation
	 * functions.
	 * @returns A new HydratedQueryBuilder with the field transformations applied.
	 */
	mapFields<M extends FieldMappings<TInput<T>>>(
		mappings: M,
	): QuerySet<Extend<HO, InferFields<TInput<T>, M>>, T>;

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
	 * // ⬇
	 * type Result = Array<{ id: number; fullName: string }>;
	 * ```
	 *
	 * @param keys - Field names to omit from the output.
	 * @returns A new HydratedQueryBuilder with the fields omitted.
	 */
	omit<K extends keyof TInput<T>>(keys: readonly K[]): QuerySet<Omit<TOutput<HO>, K>, T>;

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
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   name: string;
	 *   email: string;
	 *   displayName: string;
	 * }>;
	 * ```
	 *
	 * @param hydrator - The Hydrator to extend with.
	 * @returns A new HydratedQueryBuilder with merged hydration configuration.
	 */
	with<OtherInput extends StrictSubset<TInput<T>, OtherInput>, OtherOutput>(
		hydrator: FullHydrator<OtherInput, OtherOutput>,
	): QuerySet<Extend<HO, OtherOutput>, T>;
	// If you pass a Hydrator with a map applied, we must return a
	// MappedHydratedQueryBuilder.
	with<OtherInput extends StrictSubset<TInput<T>, OtherInput>, OtherOutput>(
		hydrator: MappedHydrator<OtherInput, OtherOutput>,
	): QuerySet<Extend<HO, OtherOutput>, T>;

	////////////////////////////////////////////////////////////
	// Attaches
	////////////////////////////////////////////////////////////

	/**
	 * Attaches data from an external source (not via SQL joins) as a nested
	 * array. The `fetchFn` is called exactly once per query execution with all
	 * parent rows to avoid N+1 queries.
	 *
	 * The `fetchFn` can return either:
	 * - An `Iterable<T>` (e.g., array, Set)
	 * - An `Executable` (an object with `execute(): Promise<Iterable<T>>`)
	 *
	 * When returning an Executable (like a QuerySet or SelectQueryBuilder), do NOT
	 * call `.execute()` - execution happens automatically when the main query runs.
	 * This allows the query to be modified via `.modify()` before execution.
	 *
	 * **Example with QuerySet (recommended):**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return querySet(db).init(
	 *         "post",
	 *         (eb) => eb.selectFrom("posts")
	 *           .select(["id", "userId", "title"])
	 *           .where("userId", "in", userIds)
	 *       );
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * **Example with SelectQueryBuilder:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return db.selectFrom("posts")
	 *         .select(["id", "userId", "title"])
	 *         .where("userId", "in", userIds);
	 *       // Note: No .execute() call - it's executed automatically
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * **Example with external API:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "socialPosts",
	 *     async (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return fetchSocialPostsFromApi(userIds);
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the array in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once
	 * with all parent rows. Can return an Iterable or an Executable.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new QuerySet with the attached collection added.
	 */
	attachMany<K extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: K,
		fetchFn: SomeFetchFn<TInput<T>, FetchFnReturn>,
		keys: AttachedKeysArg<TInput<T>, AttachedOutputFromFetchFnReturn<FetchFnReturn>>,
	): QuerySetWithAttachMany<HO, T, K, FetchFnReturn>;

	/**
	 * Attaches data from an external source (not via SQL joins) as a single
	 * nested object. The object will be nullable. The `fetchFn` is called
	 * exactly once per query execution with all parent rows to avoid N+1 queries.
	 *
	 * The `fetchFn` can return either:
	 * - An `Iterable<T>` (e.g., array, Set)
	 * - An `Executable` (an object with `execute(): Promise<Iterable<T>>`)
	 *
	 * When returning an Executable (like a QuerySet or SelectQueryBuilder), do NOT
	 * call `.execute()` - execution happens automatically when the main query runs.
	 * This allows the query to be modified via `.modify()` before execution.
	 *
	 * **Example with QuerySet (recommended):**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init("post", (eb) => eb.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .attachOne(
	 *     "author",
	 *     (postRows) => {
	 *       const userIds = [...new Set(postRows.map((p) => p.userId))];
	 *       return querySet(db).init(
	 *         "user",
	 *         (eb) => eb.selectFrom("users")
	 *           .select(["id", "username"])
	 *           .where("id", "in", userIds)
	 *       );
	 *     },
	 *     { matchChild: "id", toParent: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * **Example with SelectQueryBuilder:**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init("post", (eb) => eb.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .attachOne(
	 *     "author",
	 *     (postRows) => {
	 *       const userIds = [...new Set(postRows.map((p) => p.userId))];
	 *       return db.selectFrom("users")
	 *         .select(["id", "username"])
	 *         .where("id", "in", userIds);
	 *       // Note: No .execute() call - it's executed automatically
	 *     },
	 *     { matchChild: "id", toParent: "userId" },
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param fetchFn - A function that fetches the attached data. Called once
	 * with all parent rows. Can return an Iterable or an Executable.
	 * @param keys - Configuration for matching attached data to parents.
	 * @returns A new QuerySet with the attached object added.
	 */
	attachOne<K extends string, FetchFnReturn extends SomeFetchFnReturn>(
		key: K,
		fetchFn: SomeFetchFn<TInput<T>, FetchFnReturn>,
		keys: AttachedKeysArg<TInput<T>, AttachedOutputFromFetchFnReturn<NoInfer<FetchFnReturn>>>,
	): QuerySetWithAttachOne<HO, T, K, FetchFnReturn>;

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
	): QuerySetWithAttachOneOrThrow<HO, T, K, FetchFnReturn>;

	////////////////////////////////////////////////////////////
	// Joins
	////////////////////////////////////////////////////////////

	//
	// INNER JOIN
	//

	/**
	 * Adds an inner join that hydrates into a single nested object.
	 *
	 * Similar to {@link k.SelectQueryBuilder.innerJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression. The remaining arguments (join conditions or callback) work the
	 * same as Kysely's `.innerJoin()`.
	 *
	 * The joined record is required (non-nullable) because it's an inner join.
	 * If no matching record is found, the base record will be filtered out.
	 *
	 * The hydrator will also throw an error if it encounters more than one matching
	 * record for a base record, since this violates the cardinality constraint.
	 *
	 * **Example with explicit join conditions:**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .innerJoinOne(
	 *     "author",  // Key (alias) - extra argument compared to Kysely
	 *     (init) => init("user", eb => eb.selectFrom("users").select(["id", "username"])),
	 *     "user.id",    // Same as Kysely's k1
	 *     "post.userId", // Same as Kysely's k2
	 *   )
	 *   .execute();
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   title: string;
	 *   userId: number;
	 *   author: { id: number; username: string };
	 * }>;
	 * ```
	 *
	 * **Example with callback:**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .innerJoinOne(
	 *     "author",
	 *     (init) => init("user", eb =>eb.selectFrom("users").select(["id", "username"])),
	 *     (join) => join.onRef("user.id", "=", "post.userId"),  // Same as Kysely's callback
	 *   )
	 *   .execute();
	 * ```
	 *
	 * **Example with pre-built query set:**
	 * ```ts
	 * const authorQuery = querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]));
	 *
	 * const posts = await querySet(db)
	 *   .init("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .innerJoinOne("author", authorQuery, "user.id", "post.userId")
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output (alias).
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner join added.
	 */
	innerJoinOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;
	innerJoinOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds an inner join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.innerJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression.
	 *
	 * This is a filtering join: base records without matching child records will be
	 * excluded from the result set. The nested array will never be empty.
	 *
	 * **Note:** This causes row explosion in the SQL result. The query builder
	 * handles this internally for pagination, but be aware of the performance
	 * implications for large result sets.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "posts",
	 *     (init) => init("post", eb => eb.selectFrom("posts").select(["id", "title", "userId"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // Only users with posts are included
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   posts: Array<{ id: number; title: string; userId: number }>;
	 * }>;
	 * ```
	 *
	 * **Example with filtering in nested query:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "publishedPosts",
	 *     (init) =>
	 *       init("post", (eb) =>
	 *         eb
	 *           .selectFrom("posts")
	 *           .select(["id", "title", "userId"])
	 *           .where("status", "=", "published")
	 *       ),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // Only users with published posts are included
	 * ```
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner join added.
	 */
	innerJoinMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;
	innerJoinMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	//
	// LEFT JOIN
	//

	/**
	 * Adds a left join that hydrates into a single nested object or null.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression.
	 *
	 * Unlike {@link innerJoinOne}, base records without matching child records will
	 * be included, with the nested object set to `null`.
	 *
	 * The hydrator will throw an error if it encounters more than one matching
	 * record for a base record, since this violates the cardinality constraint.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinOne(
	 *     "profile",
	 *     (init) => init("profile", eb => eb.selectFrom("profiles").select(["id", "bio", "userId"])),
	 *     "profile.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // All users included, with profile null if no profile exists
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   profile: { id: number; bio: string; userId: number } | null;
	 * }>;
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left join added.
	 */
	leftJoinOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds a left join that hydrates into a single nested object, throwing if not found.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression.
	 *
	 * Like {@link leftJoinOne}, but throws an error during hydration if the nested
	 * object is missing. This is useful when you logically expect the relationship to
	 * exist but want to use a left join in SQL (e.g., to avoid filtering out base
	 * records prematurely).
	 *
	 * The hydrator will also throw an error if it encounters more than one matching
	 * record for a base record, since this violates the cardinality constraint.
	 *
	 * **Example:**
	 * ```ts
	 * const posts = await querySet(db)
	 *   .init("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .leftJoinOneOrThrow(
	 *     "author",
	 *     (init) => init("user", eb => eb.selectFrom("users").select(["id", "username"])),
	 *     "user.id",
	 *     "post.userId",
	 *   )
	 *   .execute();
	 * // Throws if any post is missing an author
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   title: string;
	 *   userId: number;
	 *   author: { id: number; username: string };
	 * }>;
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left join added.
	 */
	leftJoinOneOrThrow<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOneOrThrow<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinOneOrThrow<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOneOrThrow<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds a left join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression.
	 *
	 * All base records are included, even if they have no matching child records. If
	 * there are no matches, the nested array will be empty.
	 *
	 * **Note:** This causes row explosion in the SQL result. The query builder
	 * handles this internally for pagination, but be aware of the performance
	 * implications for large result sets.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     (init) => init("post", eb => eb.selectFrom("posts").select(["id", "title", "userId"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // All users included, with empty array if no posts
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   posts: Array<{ id: number; title: string; userId: number }>;
	 * }>;
	 * ```
	 *
	 * **Example with nested joins:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     (init) =>
	 *       init("post", eb => eb.selectFrom("posts").select(["id", "title", "userId"]))
	 *         .leftJoinMany(
	 *           "comments",
	 *           (init) =>
	 *             init("comment", eb => eb.selectFrom("comments").select(["id", "content", "postId"])),
	 *           "comment.postId",
	 *           "post.id",
	 *         ),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   .execute();
	 * // Users with posts, with comments nested in each post
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   posts: Array<{
	 *     id: number;
	 *     title: string;
	 *     userId: number;
	 *     comments: Array<{ id: number; content: string; postId: number }>;
	 *   }>;
	 * }>;
	 * ```
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left join added.
	 */
	leftJoinMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	//
	// CROSS JOIN
	//

	/**
	 * Adds a cross join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.crossJoin}, but with an additional first
	 * argument (`key`) for the alias/key name, and requiring a QuerySet instead of a
	 * table expression.
	 *
	 * A cross join produces the Cartesian product of the base and nested query sets.
	 * This is a filtering join like {@link innerJoinMany}.
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @returns A new QuerySet with the cross join added.
	 */
	crossJoinMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	//
	// INNER JOIN LATERAL
	//

	/**
	 * Adds an inner lateral join that hydrates into a single nested object.
	 *
	 * Similar to {@link k.SelectQueryBuilder.innerJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link innerJoinOne} but with `INNER JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner lateral join added.
	 */
	innerJoinLateralOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;
	innerJoinLateralOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds an inner lateral join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.innerJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link innerJoinMany} but with `INNER JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner lateral join added.
	 */
	innerJoinLateralMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;
	innerJoinLateralMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	//
	// LEFT JOIN LATERAL
	//

	/**
	 * Adds a left lateral join that hydrates into a single nested object or null.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link leftJoinOne} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinLateralOne<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOne<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds a left lateral join that hydrates into a single nested object, throwing if not found.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link leftJoinOneOrThrow} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralOneOrThrow<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOneOrThrow<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinLateralOneOrThrow<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinOneOrThrow<HO, T, Key, InferHO<QS>, InferT<QS>>;

	/**
	 * Adds a left lateral join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link leftJoinMany} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;
	leftJoinLateralMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, InferT<QS>>,
	): QuerySetWithLeftJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	//
	// CROSS JOIN LATERAL
	//

	/**
	 * Adds a cross lateral join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.crossJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query.
	 * Works like {@link crossJoinMany} but with `CROSS JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @returns A new QuerySet with the cross lateral join added.
	 */
	crossJoinLateralMany<Key extends string, QS extends AnyMappedQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, QS>,
	): QuerySetWithInnerJoinMany<HO, T, Key, InferHO<QS>, InferT<QS>>;

	////////////////////////////////////////////////////////////
	// Modification
	////////////////////////////////////////////////////////////

	/**
	 * Modifies the base query or a nested collection.
	 *
	 * **One-argument form:** Modifies the base query. You can add WHERE clauses,
	 * additional SELECT columns, joins, etc. You cannot remove columns with
	 * `.clearSelect()` - only additions are allowed.
	 *
	 * **Two-argument form:** Modifies a nested collection by key.
	 * - For join collections: receives the nested QuerySet and must return a
	 *   modified QuerySet.
	 * - For attach collections: receives the result of the fetch function and
	 *   must return a modified result.  The modifier composes with the existing
	 *   fetch function.
	 *
	 * **Example - Modifying base query:**
	 * ```ts
	 * const activeUsers = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ...)
	 *   .modify((qb) => qb.where("isActive", "=", true))
	 *   .execute();
	 * ```
	 *
	 * **Example - Modifying a joined QuerySet with filtering:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     (init) =>
	 *       init("post", eb => eb.selectFrom("posts").select(["id", "title", "userId"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   // Add a WHERE clause to the posts subquery
	 *   .modify("posts", (postsQuerySet) =>
	 *     postsQuerySet.modify((qb) => qb.where("status", "=", "published"))
	 *   )
	 *   .execute();
	 * // All users included, with only their published posts
	 * ```
	 *
	 * **Example - Adding nested attaches to a joined QuerySet:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "posts",
	 *     (init) =>
	 *       init("post", eb => eb.selectFrom("posts").select(["id", "title", "userId"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   // Enhance the posts collection by attaching additional data
	 *   .modify("posts", (postsQuerySet) =>
	 *     postsQuerySet.attachOne(
	 *       "metadata",
	 *       (postRows) => {
	 *         const postIds = postRows.map((p) => p.id);
	 *         return querySet(db).init("metadata", (eb) =>
	 *           eb.selectFrom("post_metadata")
	 *             .select(["postId", "viewCount", "likeCount"])
	 *             .where("postId", "in", postIds)
	 *         );
	 *       },
	 *       { matchChild: "postId" },
	 *     )
	 *   )
	 *   .execute();
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   posts: Array<{
	 *     id: number;
	 *     title: string;
	 *     userId: number;
	 *     metadata: { postId: number; viewCount: number; likeCount: number } | null;
	 *   }>;
	 * }>;
	 * ```
	 *
	 * **Example - Modifying an attach collection (QuerySet):**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return querySet(db).init("post", (eb) =>
	 *         eb.selectFrom("posts")
	 *           .select(["id", "title", "userId"])
	 *           .where("userId", "in", userIds)
	 *       );
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   // Add additional filtering to the posts query
	 *   .modify("posts", (postsQuerySet) =>
	 *     postsQuerySet.modify((qb) => qb.where("status", "=", "published"))
	 *   )
	 *   .execute();
	 * // All users included, with only their published posts
	 * ```
	 *
	 * **Example - Modifying an attach collection (SelectQueryBuilder):**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return db.selectFrom("posts")
	 *         .select(["id", "title", "userId"])
	 *         .where("userId", "in", userIds);
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   // Add additional filtering to the query
	 *   .modify("posts", (qb) => qb.where("status", "=", "published"))
	 *   .execute();
	 * // All users included, with only their published posts
	 * ```
	 *
	 * **Example - Transforming an external API attach collection:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .init("user", db.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "socialPosts",
	 *     async (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return fetchPostsFromApi(userIds);
	 *     },
	 *     { matchChild: "userId" },
	 *   )
	 *   // Transform the API response by awaiting and mapping
	 *   .modify("socialPosts", async (fetchedPostsPromise) =>
	 *     (await fetchedPostsPromise).map((p) => ({
	 *       ...p,
	 *       upperTitle: p.title.toUpperCase(),
	 *       titleLength: p.title.length,
	 *     }))
	 *   )
	 *   .execute();
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   username: string;
	 *   socialPosts: Array<{
	 *     id: number;
	 *     title: string;
	 *     userId: number;
	 *     upperTitle: string;
	 *     titleLength: number;
	 *   }>;
	 * }>;
	 * ```
	 *
	 * @param keyOrModifier - Collection key (string) or modifier function.
	 * @param modifier - Modifier function (when first param is a key).
	 * @returns A new QuerySet with the modification applied.
	 */
	// Modify base query.
	modify<O extends StrictEqual<T["BaseQuery"]["O"], O>>(
		modifier: (
			qb: SelectQueryBuilderFor<T["BaseQuery"]>,
		) => k.SelectQueryBuilder<T["BaseQuery"]["DB"], T["BaseQuery"]["TB"], O>,
	): this;
	modify<NewDB, NewTB extends keyof NewDB, NewO extends T["BaseQuery"]["O"]>(
		modifier: (
			qb: SelectQueryBuilderFor<T["BaseQuery"]>,
		) => k.SelectQueryBuilder<NewDB, NewTB, NewO>,
	): QuerySet<TWithBaseQueryOutput<HO, { DB: NewDB; TB: NewTB; O: NewO }>, TWithBaseQuery<T, { DB: NewDB; TB: NewTB; O: NewO }>>;
	// Modify collection.
	modify<
		Key extends keyof T["Collections"] & string,
		NewQS extends AnyMappedQuerySet = never,
		NewValue extends SomeFetchFnReturn = never,
	>(
		key: Key,
		modifier: CollectionModifier<T["Collections"][NoInfer<Key>], NewQS, NewValue>,
	): ModifyCollectionReturnMap<HO, T, Key, NewQS, NewValue>[T["Collections"][Key]["Type"]];
}

////////////////////////////////////////////////////////////
// Modify Helpers.
////////////////////////////////////////////////////////////

/**
 * A callback for modifying a collection.
 */
type CollectionModifier<
	Collection extends TCollection,
	NewQS extends AnyMappedQuerySet,
	NewValue extends SomeFetchFnReturn,
> = Collection extends TJoinCollection
	? (value: QuerySetFor<Collection["NestedHO"], Collection["Value"]>) => NewQS
	: (value: Collection["Value"]) => NewValue;

/**
 * Map of collection types to their return types for a modified collection.
 */
interface ModifyCollectionReturnMap<
	HO,
	T extends TQuerySet,
	Key extends string,
	NewQS extends AnyMappedQuerySet,
	NewValue extends SomeFetchFnReturn,
> {
	InnerJoinOne: QuerySetWithInnerJoinOne<HO, T, Key, InferHO<NewQS>, InferT<NewQS>>;
	InnerJoinMany: QuerySetWithInnerJoinMany<HO, T, Key, InferHO<NewQS>, InferT<NewQS>>;
	LeftJoinOne: QuerySetWithLeftJoinOne<HO, T, Key, InferHO<NewQS>, InferT<NewQS>>;
	LeftJoinOneOrThrow: QuerySetWithLeftJoinOneOrThrow<HO, T, Key, InferHO<NewQS>, InferT<NewQS>>;
	LeftJoinMany: QuerySetWithLeftJoinMany<HO, T, Key, InferHO<NewQS>, InferT<NewQS>>;

	AttachOne: QuerySetWithAttachOne<HO, T, Key, NewValue>;
	AttachOneOrThrow: QuerySetWithAttachOneOrThrow<HO, T, Key, NewValue>;
	AttachMany: QuerySetWithAttachMany<HO, T, Key, NewValue>;
}

////////////////////////////////////////////////////////////
// Attach Helpers.
////////////////////////////////////////////////////////////

interface TQuerySetWithAttach<
	in out T extends TQuerySet,
	in out Type extends TAttachType,
	in out FetchFnReturn extends SomeFetchFnReturn,
	in out Key extends string,
> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	JoinedQuery: T["JoinedQuery"];
	Collections: TCollectionsWith<
		TCollections,
		Key,
		{ Prototype: "Attach"; Type: Type; Value: FetchFnReturn }
	>;
	OrderableColumns: T["OrderableColumns"];
}

interface QuerySetWithAttachMany<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	in out FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
		ExtendWith<HO, Key, AttachedOutputFromFetchFnReturn<FetchFnReturn>[]>,
		TQuerySetWithAttach<T, "AttachMany", FetchFnReturn, Key>
	> {}

interface QuerySetWithAttachOne<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	in out FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
		ExtendWith<HO, Key, AttachedOutputFromFetchFnReturn<FetchFnReturn> | null>,
		TQuerySetWithAttach<T, "AttachOne", FetchFnReturn, Key>
	> {}

interface QuerySetWithAttachOneOrThrow<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	in out FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<
		ExtendWith<HO, Key, AttachedOutputFromFetchFnReturn<FetchFnReturn>>,
		TQuerySetWithAttach<T, "AttachOneOrThrow", FetchFnReturn, Key>
	> {}

////////////////////////////////////////////////////////////
// Join Helpers.
////////////////////////////////////////////////////////////

// Important: for all these nested operations, we use the *initial*
// JoinedQuery---not the actual JoinedQuery.  This guarantees at a type level
// that adjacent joins do not depend on each other.  (We furthermore cannot use
// T["BaseQuery"] because that's not what the JoinedQuery ever looks like.)

/**
 * Helper types to extract HO and T from a MappedQuerySet.
 */
type InferHO<Q extends AnyMappedQuerySet> = NonNullable<Q["$inferType"]>;
type InferT<Q extends AnyMappedQuerySet> = NonNullable<Q["_generics"]>;

// oxlint-disable-next-line explicit-function-return-type
type AnyMappedQuerySet = MappedQuerySet<any, any>;

type NestedQuerySetOrFactory<T extends TQuerySet, Alias extends string, QS extends AnyMappedQuerySet> =
	| QS
	| ((nest: InitWithAlias<ToInitialJoinedDB<T>, ToInitialJoinedTB<T>, Alias>) => QS);

type ToTableExpression<Key extends string, TNested extends TQuerySet> = k.AliasedExpression<
	TNested["BaseQuery"]["O"],
	Key
>;

type JoinReferenceExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> = k.JoinReferenceExpression<
	ToInitialJoinedDB<T>,
	ToInitialJoinedTB<T>,
	ToTableExpression<Key, TNested>
>;

type JoinCallbackExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TQuerySet,
> = k.JoinCallbackExpression<
	ToInitialJoinedDB<T>,
	ToInitialJoinedTB<T>,
	ToTableExpression<Key, TNested>
>;

type CardinalityOneJoinType = "InnerJoinOne" | "LeftJoinOne" | "LeftJoinOneOrThrow";

type TOrderableColumnsWithJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	TNested extends TQuerySet,
> = Type extends CardinalityOneJoinType
	? T["OrderableColumns"] | ApplyPrefixWithSep<Key, TNested["OrderableColumns"]>
	: T["OrderableColumns"];

type TQuerySetWithJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	NestedHO,
	TNested extends TQuerySet,
	JoinedQuery extends AnySelectQueryBuilder,
> = Flatten<{
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: TCollectionsWith<
		T["Collections"],
		Key,
		{ Prototype: "Join"; Type: Type; NestedHO: NestedHO; Value: TNested }
	>;
	JoinedQuery: InferTQuery<JoinedQuery>;
	OrderableColumns: TOrderableColumnsWithJoin<T, Key, Type, TNested>;
}>;

type ToJoinOutputInner<
	T extends TQuerySet,
	TNested extends TQuerySet,
	Key extends string,
> = Flatten<
	// Extend the *JoinedQuery* output, which includes both the base output and also
	// output from other joins.
	T["JoinedQuery"]["O"] & ApplyPrefixes<MakePrefix<"", Key>, TNested["JoinedQuery"]["O"]>
>;

// Compared to the inner join, the left joined output is nullable.
type ToJoinOutputLeft<T extends TQuerySet, TNested extends TQuerySet, Key extends string> = Flatten<
	T["JoinedQuery"]["O"] &
		ApplyPrefixes<MakePrefix<"", Key>, k.Nullable<TNested["JoinedQuery"]["O"]>>
>;

interface TQuerySetWithInnerJoin<
	in out T extends TQuerySet,
	in out Key extends string,
	in out Type extends TJoinType,
	in out NestedHO,
	in out TNested extends TQuerySet,
> extends TQuerySetWithJoin<
		T,
		Key,
		Type,
		NestedHO,
		TNested,
		k.SelectQueryBuilderWithInnerJoin<
			ToInitialJoinedDB<T>,
			ToInitialJoinedTB<T>,
			ToJoinOutputInner<T, TNested, Key>,
			ToTableExpression<Key, TNested>
		>
	> {}

interface TQuerySetWithLeftJoin<
	in out T extends TQuerySet,
	in out Key extends string,
	in out Type extends TJoinType,
	in out NestedHO,
	in out TNested extends TQuerySet,
> extends TQuerySetWithJoin<
		T,
		Key,
		Type,
		NestedHO,
		TNested,
		k.SelectQueryBuilderWithLeftJoin<
			ToInitialJoinedDB<T>,
			ToInitialJoinedTB<T>,
			ToJoinOutputLeft<T, TNested, Key>,
			ToTableExpression<Key, TNested>
		>
	> {}

interface QuerySetWithInnerJoinOne<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	NestedHO,
	in out TNested extends TQuerySet,
> extends QuerySet<
		ExtendWith<HO, Key, TOutput<NestedHO>>,
		TQuerySetWithInnerJoin<T, Key, "InnerJoinOne", NestedHO, TNested>
	> {}

interface QuerySetWithInnerJoinMany<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	NestedHO,
	in out TNested extends TQuerySet,
> extends QuerySet<
		ExtendWith<HO, Key, TOutput<NestedHO>[]>,
		TQuerySetWithInnerJoin<T, Key, "InnerJoinMany", NestedHO, TNested>
	> {}

interface QuerySetWithLeftJoinOne<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	NestedHO,
	in out TNested extends TQuerySet,
> extends QuerySet<
		ExtendWith<HO, Key, TOutput<NestedHO> | null>,
		TQuerySetWithLeftJoin<T, Key, "LeftJoinOne", NestedHO, TNested>
	> {}

interface QuerySetWithLeftJoinOneOrThrow<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	NestedHO,
	in out TNested extends TQuerySet,
> extends QuerySet<
		ExtendWith<HO, Key, TOutput<NestedHO>>,
		TQuerySetWithLeftJoin<T, Key, "LeftJoinOneOrThrow", NestedHO, TNested>
	> {}

interface QuerySetWithLeftJoinMany<
	HO,
	in out T extends TQuerySet,
	in out Key extends string,
	NestedHO,
	in out TNested extends TQuerySet,
> extends QuerySet<
		ExtendWith<HO, Key, TOutput<NestedHO>[]>,
		TQuerySetWithLeftJoin<T, Key, "LeftJoinMany", NestedHO, TNested>
	> {}

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

interface QuerySetOrderBy {
	expr: string;
	modifiers?: k.OrderByModifiers | undefined;
}

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
	orderBy: readonly QuerySetOrderBy[];
	orderByKeys: boolean;
	frontModifiers: readonly k.Expression<any>[];
	endModifiers: readonly k.Expression<any>[];
}

/**
 * Implementation of the {@link QuerySet} interface as well as the
 * {@link MappedQuerySet} interface; there is no runtime distinction.
 */
class QuerySetImpl implements QuerySet<any, TQuerySet> {
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

	get $inferType(): any {
		return undefined as any;
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
	 * Checks if a collection is cardinality-one, including checking its query set recursively.
	 */
	#isCollectionCardinalityOne(collection: JoinCollection): boolean {
		return collection.mode === "one" && collection.querySet.#isCardinalityOne();
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
		qb: AnySelectQueryBuilder,
		key: string,
		collection: JoinCollection,
	): AnySelectQueryBuilder {
		// Add the join to the parent query.
		// This cast to a single method helps TypeScript follow the overloads.
		const from = collection.querySet.#toQuery(true).as(key);
		qb = qb[collection.method as "innerJoin"](from, ...collection.args);

		// Add the (prefixed) selections from the subquery to the parent query.
		const prefix = makePrefix("", key);
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
		const { baseAlias, keyBy, orderBy } = this.#props;

		let keyByArray: readonly string[] = typeof keyBy === "string" ? [keyBy] : keyBy;

		// Apply custom orderBy expressions
		for (const { expr, modifiers } of orderBy) {
			// Convert $$ to . for SQL column references (e.g., "profile$$bio" -> "profile.bio")
			// If there's no $$, it's a base column, so add the baseAlias prefix
			const sqlExpr = expr.includes(SEP)
				? // Intentionally only replace first occurrence.
					expr.replace(SEP, ".")
				: `${baseAlias}.${expr}`;

			qb = qb.orderBy(sqlExpr, modifiers);

			// Remove expr from keyByArray if present
			keyByArray = keyByArray.filter((k) => k !== expr);
		}

		// Always order by the key(s) as tie breakers (unless orderByKeys is disabled)
		if (this.#props.orderByKeys) {
			for (const key of keyByArray) {
				qb = qb.orderBy(`${baseAlias}.${key}`, "asc");
			}
		}

		return qb;
	}

	/**
	 * Returns a query guaranteed to return one row per entity in the result set,
	 * suitable for count and exists queries. It includes:
	 * - ALL cardinality-one joins (inner and left) - included directly because
	 *   WHERE clauses might reference their columns
	 * - Cardinality-many filtering joins (innerJoinMany, crossJoinMany) - converted
	 *   to WHERE EXISTS to avoid row explosion
	 * - Cardinality-many non-filtering joins (leftJoinMany) - excluded entirely
	 */
	#toCardinalityOneQuery(): AnySelectQueryBuilder {
		const { db, joinCollections } = this.#props;

		let qb = db.selectFrom(this.#aliasedBaseQuery);

		const hoistedSelects = hoistSelections(this.#aliasedBaseQuery);
		qb = qb.select(hoistedSelects);

		for (const [key, collection] of joinCollections) {
			// For count/exists queries:
			// - ALL cardinality-one joins (innerJoinOne, leftJoinOne, leftJoinOneOrThrow): included as-is
			//   because WHERE clauses might reference columns from these joins
			// - Cardinality-many filtering joins (innerJoinMany, crossJoinMany): converted to WHERE EXISTS
			//   to avoid row explosion
			// - Cardinality-many non-filtering joins (leftJoinMany): excluded from count/exists

			if (this.#isCollectionCardinalityOne(collection)) {
				// All cardinality-one joins are safe to include directly (no row explosion)
				qb = this.#addCollectionAsJoin(qb, key, collection);
			} else if (isFilteringJoin(collection)) {
				// Cardinality-many filtering joins must be converted to WHERE EXISTS
				// to avoid row explosion in count queries
				qb = qb.where(({ exists, selectFrom, lit }) =>
					exists(
						selectFrom(k.sql`(SELECT 1)`.as("__"))
							.select(lit(1).as("_"))
							.$call((qb) => this.#addCollectionAsJoin(qb, key, collection)),
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
			// Cardinality-many non-filtering joins (leftJoinMany) are intentionally excluded
		}

		return qb;
	}

	#toJoinedQuery(isSubquery: boolean): AnySelectQueryBuilder {
		const { db, joinCollections } = this.#props;

		let qb = db.selectFrom(this.#aliasedBaseQuery);

		const hoistedSelects = hoistSelections(this.#aliasedBaseQuery);
		qb = qb.select(hoistedSelects);

		for (const [key, collection] of joinCollections) {
			qb = this.#addCollectionAsJoin(qb, key, collection);
		}

		// NOTE: Limit and offset cannot be applied here because of row explosion.

		// Apply ordering---but only if we're not prefixed, because ordering in
		// subqueries is ignored (well, "not guaranteed") unless you also have a
		// LIMIT or OFFSET.
		if (!isSubquery) {
			qb = this.#applyOrderBy(qb);
		}

		return qb;
	}

	toJoinedQuery(): AnySelectQueryBuilder {
		return this.#toJoinedQuery(false);
	}

	#toQuery(isSubquery: boolean): AnySelectQueryBuilder {
		const { baseAlias, db, limit, offset, orderBy, joinCollections } = this.#props;

		// If we have no joins (no row explosion) and no ordering (nothing referencing
		// the baseAlias), we can just apply the limit and offset to the base query.
		if (!joinCollections.size && !orderBy) {
			return this.#applyLimitAndOffset(this.#props.baseQuery);
		}

		// If no pagination, just return the joined query.
		if (!limit && !offset) {
			return this.#toJoinedQuery(isSubquery);
		}

		// If only cardinality-one joins, we can safely apply limit/offset to the
		// joined query.
		if (this.#isCardinalityOne()) {
			return this.#applyLimitAndOffset(this.#toJoinedQuery(isSubquery));
		}

		let cardinalityOneQuery = this.#toCardinalityOneQuery();

		cardinalityOneQuery = this.#applyLimitAndOffset(cardinalityOneQuery);
		// Ordering in the subquery only matters if there is a limit or offset.
		if (limit || offset) {
			cardinalityOneQuery = this.#applyOrderBy(cardinalityOneQuery);
		}

		const aliasedCardinalityOneQuery = cardinalityOneQuery.as(baseAlias);
		let qb = db.selectFrom(aliasedCardinalityOneQuery);

		const hoistedSelects = hoistSelections(aliasedCardinalityOneQuery);
		qb = qb.select(hoistedSelects);

		// Add any cardinality-many joins.
		for (const [key, collection] of joinCollections) {
			if (!this.#isCollectionCardinalityOne(collection)) {
				qb = this.#addCollectionAsJoin(qb, key, collection);
			}
		}

		// Re-apply ordering since the order from the subquery is not guaranteed to
		// be preserved.  This doesn't matter if we have a prefix because it means
		// we're in a subquery already.
		if (!isSubquery) {
			qb = this.#applyOrderBy(qb);
		}

		for (const modifier of this.#props.frontModifiers) {
			qb = qb.modifyFront(modifier);
		}

		for (const modifier of this.#props.endModifiers) {
			qb = qb.modifyEnd(modifier);
		}

		return qb;
	}

	toQuery(): AnySelectQueryBuilder {
		return this.#toQuery(false);
	}

	toCountQuery(): OpaqueCountQueryBuilder {
		return this.#toCardinalityOneQuery()
			.clearSelect()
			.select((eb) => eb.fn.countAll().as("count"));
	}

	toExistsQuery(): OpaqueExistsQueryBuilder {
		return this.#props.db.selectNoFrom(({ exists }) =>
			exists(
				this.#toCardinalityOneQuery()
					.clearSelect()
					.select((eb) => eb.lit(1).as("_")),
			).as("exists"),
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

		return this.#props.hydrator.hydrate(rows, {
			// Auto include fields at all levels, so we don't have to understand the
			// shape of the selection and can allow it to be inferred by the shape of
			// the rows.
			[EnableAutoInclusion]: true,
			// Sort nested collections, since their order cannot be guaranteed by SQL.
			sort: "nested",
		});
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
			// it is scoped within its own subquery.  The types capture this.
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

	where(...args: any[]): any {
		return this.modify((qb) => (qb.where as any)(...args));
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

	orderBy(expr: string, modifiers: k.OrderByModifiers = "asc"): any {
		const orderBy = kyselyOrderByToOrderBy(expr, modifiers);

		return this.#clone({
			// Add to the orderBy array for SQL ORDER BY clause
			orderBy: [...this.#props.orderBy, { expr, modifiers }],

			hydrator: this.#props.hydrator.orderBy(orderBy.key, orderBy.direction, orderBy.nulls),
		});
	}

	clearOrderBy(): any {
		// Clear the custom orderBy array. The SQL query will revert to ordering by keyBy columns only.
		// Note: We don't clear the hydrator's orderings here because those affect nested collection
		// sorting during hydration, which is independent of the SQL ORDER BY clause.
		return this.#clone({
			orderBy: [],
			// Also clear the hydrator's orderings.
			hydrator: this.#props.hydrator.clearOrderBy(),
		});
	}

	orderByKeys(enabled: boolean = true): any {
		return this.#clone({
			orderByKeys: enabled,
			// Also apply the setting to the hydrator.
			hydrator: this.#props.hydrator.orderByKeys(enabled),
		});
	}

	modifyFront(modifier: k.Expression<any>): any {
		return this.#clone({
			frontModifiers: [...this.#props.frontModifiers, modifier],
		});
	}

	modifyEnd(modifier: k.Expression<any>): any {
		return this.#clone({
			endModifiers: [...this.#props.endModifiers, modifier],
		});
	}

	$call<R>(callback: (qs: this) => R): R {
		return callback(this);
	}
}

////////////////////////////////////////////////////////////
// QuerySetCreator.
////////////////////////////////////////////////////////////

interface InitialQuerySet<
	in out DB,
	in out BaseAlias extends string,
	in out BaseDB,
	in out BaseTB extends keyof BaseDB,
	in out BaseO,
> extends QuerySet<
		// The hydrated output is the same as the base query output; no mapping yet.
		BaseO,
		{
			DB: DB;
			IsMapped: false;
			BaseAlias: BaseAlias;
			BaseQuery: {
				DB: BaseDB;
				TB: BaseTB;
				O: BaseO;
			};
			Collections: {};
			// The joined query mostly looks like the base query.
			JoinedQuery: InitialJoinedQuery<BaseDB, BaseAlias, BaseO>;
			// No orderable columns other than the base query yet.
			OrderableColumns: keyof BaseO & string;
		}
	> {}

// type InferDB<Q> = Q extends k.SelectQueryBuilder<infer BaseDB, any, any> ? BaseDB : never;
// type InferTB<Q> = Q extends k.SelectQueryBuilder<any, infer BaseTB, any> ? BaseTB : never;
// type InferO<Q> = Q extends k.SelectQueryBuilder<any, any, infer BaseO> ? BaseO : never;

// A minimal subset of k.Kysely<DB>, which doesn't allow doing other things,
// such as with expressions.
interface SelectCreator<in out DB, in out TB extends keyof DB> {
	selectFrom: k.ExpressionBuilder<DB, TB>["selectFrom"];
}

interface SelectQueryBuilderFactory<
	in out DB,
	in out TB extends keyof DB,
	in out BaseDB,
	in out BaseTB extends keyof BaseDB,
	in out BaseO,
> {
	(eb: SelectCreator<DB, TB>): k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>;
}

type SelectQueryBuilderOrFactory<
	DB,
	TB extends keyof DB,
	BaseDB,
	BaseTB extends keyof BaseDB,
	BaseO,
> =
	| k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>
	| SelectQueryBuilderFactory<DB, TB, BaseDB, BaseTB, BaseO>;

interface InitWithAlias<in out DB, in out TB extends keyof DB, in out Alias extends string> {
	<BaseDB, BaseTB extends keyof BaseDB, BaseO extends InputWithDefaultKey>(
		query: SelectQueryBuilderOrFactory<DB, TB, BaseDB, BaseTB, BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	<BaseDB, BaseTB extends keyof BaseDB, BaseO>(
		query: SelectQueryBuilderOrFactory<DB, TB, BaseDB, BaseTB, BaseO>,
		keyBy: KeyBy<NoInfer<BaseO>>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	// biome-ignore lint/style/useShorthandFunctionType: Must be a member because of overloads.
	<
		F extends SelectQueryBuilderFactory<DB, never, any, any, any>,
		Q extends k.SelectQueryBuilder<any, any, any> = ReturnType<F>,
		TQ extends TQuery = InferTQuery<Q>,
	>(
		query: F,
		keyBy: KeyBy<NoInfer<TQ["O"]>>,
	): InitialQuerySet<DB, Alias, TQ["DB"], TQ["TB"], TQ["O"]>;
}

/**
 * Factory for creating query sets. Obtained by calling {@link querySet}.
 *
 * @template DB - The database schema type.
 */
class QuerySetCreator<in out DB> {
	#db: k.Kysely<DB>;

	constructor(db: k.Kysely<DB>) {
		this.#db = db;
	}

	/**
	 * Initializes a new query set with a base query and alias.
	 *
	 * The alias is required and will be used to reference columns from the base
	 * query in the generated SQL, including for any nested joins you add. You
	 * must provide either a Kysely query builder or a factory function that
	 * receives an expression builder and returns a query.
	 *
	 * By default, the query set will use `"id"` as the key to uniquely identify
	 * rows.  You can override this by passing a `keyBy` parameter (and must do so
	 * if your selected row does not have an `id` column).
	 *
	 * **Example with query builder:**
	 * ```ts
	 * querySet(db).init(
	 *   "user",
	 *   db.selectFrom("users").select(["id", "username", "email"])
	 * )
	 * ```
	 *
	 * **Example with factory function:**
	 * ```ts
	 * querySet(db).init(
	 *   "user",
	 *   (eb) => eb.selectFrom("users").select(["id", "username", "email"])
	 * )
	 * ```
	 *
	 * **Example with custom keyBy:**
	 * ```ts
	 * querySet(db).init(
	 *   "session",
	 *   db.selectFrom("sessions").select(["sessionId", "userId"]),
	 *   "sessionId" // Use sessionId instead of id
	 * )
	 * ```
	 *
	 * **Example with composite key:**
	 * ```ts
	 * querySet(db).init(
	 *   "userRole",
	 *   db.selectFrom("user_roles").select(["userId", "roleId"]),
	 *   ["userId", "roleId"]
	 * )
	 * ```
	 *
	 * @param alias - The alias for the base query (used in generated SQL).
	 * @param query - A Kysely query builder or factory function.
	 * @param keyBy - The key(s) to uniquely identify rows. Defaults to `"id"`.
	 * @returns A new QuerySet.
	 */
	init<
		Alias extends string,
		BaseDB,
		BaseTB extends keyof BaseDB,
		BaseO extends InputWithDefaultKey,
	>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, never, BaseDB, BaseTB, BaseO>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	init<Alias extends string, BaseDB, BaseTB extends keyof BaseDB, BaseO>(
		alias: Alias,
		query: k.SelectQueryBuilder<BaseDB, BaseTB, BaseO>,
		keyBy: KeyBy<NoInfer<BaseO>>,
	): InitialQuerySet<DB, Alias, BaseDB, BaseTB, BaseO>;
	// Infer output from ReturnType<F> to avoid circular inference.
	init<
		Alias extends string,
		F extends SelectQueryBuilderFactory<DB, never, any, any, any>,
		Q extends k.SelectQueryBuilder<any, any, any> = ReturnType<F>,
		TQ extends TQuery = InferTQuery<Q>,
	>(
		alias: Alias,
		query: F,
		keyBy: KeyBy<NoInfer<TQ["O"]>>,
	): InitialQuerySet<DB, Alias, TQ["DB"], TQ["TB"], TQ["O"]>;
	init(
		alias: string,
		query: any,
		keyBy: KeyBy<any> = DEFAULT_KEY_BY,
	): InitialQuerySet<DB, string, any, any, any> {
		const baseQuery = typeof query === "function" ? query(this.#db) : query;

		return new QuerySetImpl({
			db: this.#db,
			baseAlias: alias,
			baseQuery,
			keyBy: keyBy,
			hydrator: createHydrator().orderByKeys(),
			joinCollections: new Map(),
			attachCollections: new Map(),
			limit: null,
			offset: null,
			orderBy: [],
			orderByKeys: true,
			frontModifiers: [],
			endModifiers: [],
		});
	}
}

/**
 * Creates a new {@link QuerySetCreator} for building query sets with nested joins
 * and automatic hydration of flat SQL results into nested objects.
 *
 * Query sets use nested subqueries to provide better SQL isolation and enable
 * correct pagination even with joined collections.
 *
 * **Example:**
 * ```ts
 * const users = await querySet(db)
 *   .init("user", (eb) => eb.selectFrom("users").select(["id", "username", "email"]))
 *   .leftJoinMany("posts", (init) =>
 *     init("post", (eb) => eb.selectFrom("posts").select(["id", "userId", "title"])),
 *     "post.userId",
 *     "user.id",
 *   )
 *   .execute();
 * // ⬇
 * type Result = Array<{
 *   id: number;
 *   username: string;
 *   email: string;
 *   posts: Array<{ id: number; userId: number; title: string }>;
 * }>;
 * ```
 *
 * @param db - A Kysely database instance.
 * @returns A QuerySetCreator for building query sets.
 */
export function querySet<DB>(db: k.Kysely<DB>): QuerySetCreator<DB> {
	return new QuerySetCreator(db);
}
