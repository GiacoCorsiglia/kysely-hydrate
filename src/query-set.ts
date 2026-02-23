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
	type MakeInitialPrefix,
	makePrefix,
	SEP,
} from "./helpers/prefixes.ts";
import {
	applyHoistedPrefixedSelections,
	applyHoistedSelections,
} from "./helpers/select-renamer.ts";
import {
	type AnySelectQueryBuilder,
	type AnyDeleteQueryBuilder,
	type AnyQueryBuilder,
	type AnyUpdateQueryBuilder,
	type AnyInsertQueryBuilder,
	type DrainOuterGeneric,
	type Extend,
	type ExtendWith,
	type Flatten,
	type KeyBy,
	type NarrowPartial,
	type RawExtend,
	type StrictEqual,
	type StrictSubset,
	type TypeErrorMessage,
	assertNever,
	isSelectQueryBuilder,
	mapWithDeleted,
} from "./helpers/utils.ts";
import {
	type AttachedKeysArg,
	type AttachedOutputFromFetchFnReturn,
	type CollectionMode,
	type Extender,
	type Extras,
	type FieldMappings,
	type FullHydrator,
	type Hydrator,
	type InferExtender,
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
import { InvalidJoinedQuerySetError } from "./index.ts";

/**
 * A stateless Kysely plugin that strips the WITH clause from a
 * SelectQueryNode.  Used to remove CTEs that the query creator
 * attaches to queries built via `selectFn` in `.write()` / `.writeAs()`.
 * The CTEs are already captured separately in `writeQueryCreator`.
 */
const stripWithPlugin: k.KyselyPlugin = {
	transformQuery(args) {
		const node = args.node;
		if (node.kind === "SelectQueryNode" && node.with) {
			return { ...node, with: undefined } as unknown as k.SelectQueryNode;
		}
		return node;
	},
	async transformResult(args) {
		return args.result;
	},
};

////////////////////////////////////////////////////////////
// Generics.
////////////////////////////////////////////////////////////

type TQueryType = "Select" | "Update" | "Insert" | "Delete";

interface TQuery<in out DB = any, in out TB extends keyof DB = any, UT extends keyof DB = never> {
	Type: TQueryType;

	DB: DB;
	TB: TB;
	O: any;
	/**
	 * Updates have an additional table in scope---the one being updated.
	 */
	UT: UT;
}

interface TSelectQuery extends TQuery {
	Type: "Select";
}

type InferTSelectQuery<Q extends AnySelectQueryBuilder> =
	Q extends k.SelectQueryBuilder<infer DB, infer TB, infer O>
		? { Type: "Select"; DB: DB; TB: TB; O: O; UT: never }
		: never;

type InferTInsertQuery<Q extends AnyInsertQueryBuilder> =
	Q extends k.InsertQueryBuilder<infer DB, infer TB, infer O>
		? { Type: "Insert"; DB: DB; TB: TB; O: O; UT: never }
		: never;

type InferTUpdateQuery<Q extends AnyUpdateQueryBuilder> =
	Q extends k.UpdateQueryBuilder<infer DB, infer TB, infer UT, infer O>
		? { Type: "Update"; DB: DB; TB: TB; O: O; UT: UT }
		: never;

type InferTDeleteQuery<Q extends AnyDeleteQueryBuilder> =
	Q extends k.DeleteQueryBuilder<infer DB, infer TB, infer O>
		? { Type: "Delete"; DB: DB; TB: TB; O: O; UT: never }
		: never;

type SelectQueryBuilderFor<Q extends TQuery> = k.SelectQueryBuilder<Q["DB"], Q["TB"], Q["O"]>;
type UpdateQueryBuilderFor<Q extends TQuery> = k.UpdateQueryBuilder<
	Q["DB"],
	Q["TB"],
	Q["UT"],
	Q["O"]
>;
type InsertQueryBuilderFor<Q extends TQuery> = k.InsertQueryBuilder<Q["DB"], Q["TB"], Q["O"]>;
type DeleteQueryBuilderFor<Q extends TQuery> = k.DeleteQueryBuilder<Q["DB"], Q["TB"], Q["O"]>;

interface QueryBuilderForMap<Q extends TQuery> {
	Select: SelectQueryBuilderFor<Q>;
	Update: UpdateQueryBuilderFor<Q>;
	Insert: InsertQueryBuilderFor<Q>;
	Delete: DeleteQueryBuilderFor<Q>;
}

type QueryBuilderFor<Q extends TQuery> = QueryBuilderForMap<Q>[Q["Type"]];

interface NewQueryBuilderForMap<NewDB, NewTB extends keyof NewDB, NewUT extends keyof NewDB, NewO> {
	Select: k.SelectQueryBuilder<NewDB, NewTB, NewO>;
	Update: k.UpdateQueryBuilder<NewDB, NewTB, NewUT, NewO>;
	Insert: k.InsertQueryBuilder<NewDB, NewTB, NewO>;
	Delete: k.DeleteQueryBuilder<NewDB, NewTB, NewO>;
}

type NewQueryBuilderFor<
	Q extends TQuery,
	NewDB,
	NewTB extends keyof NewDB,
	NewUT extends keyof NewDB,
	NewO,
> = NewQueryBuilderForMap<NewDB, NewTB, NewUT, NewO>[Q["Type"]];

interface QueryBuilderWithOutputForMap<Q extends TQuery, O> {
	Select: k.SelectQueryBuilder<Q["DB"], Q["TB"], O>;
	Update: k.UpdateQueryBuilder<Q["DB"], Q["TB"], Q["UT"], O>;
	Insert: k.InsertQueryBuilder<Q["DB"], Q["TB"], O>;
	Delete: k.DeleteQueryBuilder<Q["DB"], Q["TB"], O>;
}

type QueryBuilderWithOutputFor<Q extends TQuery, O> = QueryBuilderWithOutputForMap<Q, O>[Q["Type"]];

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
	/**
	 * The final shape of the hydrated output row.
	 */
	HydratedOutput: any;
	/**
	 * Keys that have been omitted from the output. Tracked separately so they
	 * can be preserved through base query changes (e.g., .insert(), .update()).
	 */
	OmittedKeys: PropertyKey;
}

interface TSelectQuerySet extends TQuerySet {
	BaseQuery: TSelectQuery;
}

type QuerySetFor<T extends TQuerySet> = T["IsMapped"] extends true
	? MappedQuerySet<T>
	: QuerySet<T>;

type THydrationInput<T extends TQuerySet> = T["JoinedQuery"]["O"];

type TOutput<T extends TQuerySet> = T["HydratedOutput"];

interface TMapped<in out T extends TQuerySet, in out Output> {
	DB: T["DB"];
	IsMapped: true;
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	OrderableColumns: T["OrderableColumns"];
	HydratedOutput: Output;
	OmittedKeys: T["OmittedKeys"];
}

interface TJoinedQueryWithBaseQuery<
	in out BaseAlias extends string,
	in out JoinedQuery extends TQuery,
	in out BaseQuery extends TQuery,
> {
	Type: "Select";
	DB: JoinedQuery["DB"] & { [_ in BaseAlias]: BaseQuery["O"] };
	TB: JoinedQuery["TB"];
	O: JoinedQuery["O"];
	UT: never;
}

interface TWithBaseQuery<in out T extends TQuerySet, in out BaseQuery extends TQuery> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: BaseQuery;
	Collections: T["Collections"];
	JoinedQuery: TJoinedQueryWithBaseQuery<T["BaseAlias"], T["JoinedQuery"], BaseQuery>;
	OrderableColumns: T["OrderableColumns"] | (keyof BaseQuery["O"] & string);
	// Extend in this order because we are expanding the input type, but it still needs to be
	// overwritten by .extras() and whatnot. Then apply any omitted keys to ensure they're excluded
	// even after the base query changes.
	//
	// Also, if the query set is mapped, you're not allowed to change the output type by modifying the
	// input type.
	HydratedOutput: T["IsMapped"] extends true
		? T["HydratedOutput"]
		: Flatten<Omit<RawExtend<BaseQuery["O"], T["HydratedOutput"]>, T["OmittedKeys"]>>;
	OmittedKeys: T["OmittedKeys"];
}

interface TWithOutput<in out T extends TQuerySet, in out Output> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	OrderableColumns: T["OrderableColumns"];
	HydratedOutput: Output;
	OmittedKeys: T["OmittedKeys"];
}

interface TWithExtendedOutput<in out T extends TQuerySet, in out Output> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	OrderableColumns: T["OrderableColumns"];
	HydratedOutput: Extend<T["HydratedOutput"], Output>;
	OmittedKeys: T["OmittedKeys"];
}

interface TWithOmit<in out T extends TQuerySet, in out K extends PropertyKey> {
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: T["Collections"];
	JoinedQuery: T["JoinedQuery"];
	OrderableColumns: T["OrderableColumns"];
	HydratedOutput: Flatten<Omit<T["HydratedOutput"], K>>;
	OmittedKeys: T["OmittedKeys"] | K;
}

type NarrowOutput<T extends TQuerySet, Narrow> = NarrowPartial<T["HydratedOutput"], Narrow>;

interface InitialJoinedQuery<in out DB, in out BaseAlias extends string, in out BaseO> {
	Type: "Select";
	// The base query is wrapped in an alias in `SELECT $alias.* FROM (...) as
	// $alias`, so it's treated as another table.
	DB: DB & { [K in BaseAlias]: BaseO };
	// The base query alias is selected as an active table.
	TB: BaseAlias;
	// The output is the same as the base query output.
	O: BaseO;
	// It's never an update.
	UT: never;
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
 * Infer the output type of a query set.  This is the type of one hydrated row.  It's the same as
 * the type returned by `.executeTakeFirstOrThrow()`.
 *
 * **Example:**
 * ```ts
 * const usersQuerySet = querySet(db)
 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
 *
 * type User = InferOutput<typeof usersQuerySet>;
 * // ⬇
 * type User = { id: number; username: string };
 * ```
 */
export type InferOutput<Q extends { _generics: TQuerySet | undefined }> = Q extends {
	_generics: { HydratedOutput: infer O } | undefined;
}
	? O
	: never;

/**
 * Given a TQuerySet, return a QuerySet or MappedQuerySet depending on whether the query set has
 * been mapped (indicated by T["IsMapped"]).
 */
type MaybeMappedQuerySet<T extends TQuerySet> = T["IsMapped"] extends true
	? MappedQuerySet<T>
	: QuerySet<T>;

/**
 * A query set that has been mapped with a transformation function.
 *
 * After calling `.map()`, only query execution and further mapping are available.
 * You cannot continue to add joins, modify hydration, or otherwise change the
 * shape of the query's input, since that would affect the input expected by the
 * transformation function.
 *
 * @template T - The query set's type parameters.
 */
// oxlint-disable-next-line namespace I don't know why oxlint can't find these interfaces.
interface MappedQuerySet<in out T extends TQuerySet> extends k.Compilable, k.OperationNodeSource {
	/**
	 * This property exists for complex type reasons and will never be set.
	 *
	 * @internal
	 */
	// Required so that the type system can infer all the generics the even when
	// nested collections return a MappedQuerySet instead of a full QuerySet.
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("isActive", "=", true));
	 *
	 * const baseQuery = qs.toBaseQuery();
	 * // SELECT id, username FROM users WHERE isActive = true
	 * ```
	 */
	toBaseQuery(): QueryBuilderFor<T["BaseQuery"]>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 * Hydrates pre-fetched raw joined rows into nested output objects without
	 * executing a query.
	 *
	 * This is useful when you already have the flat SQL result (e.g. from a
	 * separate query, a cache, or a transaction) and want to apply the same
	 * hydration logic that `.execute()` uses.
	 *
	 * **Example - single row:**
	 * ```ts
	 * const qs = querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ...);
	 *
	 * const row = await qs.toQuery().executeTakeFirstOrThrow();
	 * const user = await qs.hydrate(row);
	 * ```
	 *
	 * **Example - multiple rows:**
	 * ```ts
	 * const rows = await qs.toQuery().execute();
	 * const users = await qs.hydrate(rows);
	 * ```
	 *
	 * @param input - A single flat row or an iterable of flat rows matching the
	 *   joined query output shape.
	 * @returns The hydrated output(s).
	 */
	hydrate(input: THydrationInput<T> | Promise<THydrationInput<T>>): Promise<TOutput<T>>;
	hydrate(
		input: Iterable<THydrationInput<T>> | Promise<Iterable<THydrationInput<T>>>,
	): Promise<TOutput<T>[]>;
	hydrate(
		input:
			| THydrationInput<T>
			| Iterable<THydrationInput<T>>
			| Promise<THydrationInput<T>>
			| Promise<Iterable<THydrationInput<T>>>,
	): Promise<TOutput<T> | TOutput<T>[]>;

	/**
	 * Executes the query and returns an array of hydrated rows.
	 *
	 * Nested collections (from joins and attaches) will be hydrated into nested
	 * objects and arrays according to the configuration.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ({ eb, qs }) =>
	 *     qs(eb.selectFrom("posts").select(["id", "title"])),
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
	execute(): Promise<TOutput<T>[]>;

	/**
	 * Executes the query and returns the first hydrated result, or `undefined` if
	 * the query returned no results.
	 *
	 * **Example:**
	 * ```ts
	 * const user = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("id", "=", 1))
	 *   .executeTakeFirst();
	 *
	 * if (user) {
	 *   console.log(user.username);
	 * }
	 * ```
	 */
	executeTakeFirst(): Promise<TOutput<T> | undefined>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) => qb.where("id", "=", 1))
	 *   .executeTakeFirstOrThrow(UserNotFoundError);
	 * ```
	 */
	executeTakeFirstOrThrow(
		errorConstructor?: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error),
	): Promise<TOutput<T>>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .executeCount(); // string | number | bigint
	 * ```
	 *
	 * **Example with type conversion:**
	 * ```ts
	 * const count = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .executeCount(Number); // number
	 *
	 * const bigCount = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "role", "permissions"]))
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
		transform: (row: TOutput<T>) => NewHydratedOutput,
	): MappedQuerySet<TMapped<T, NewHydratedOutput>>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username", "isActive"]))
	 *   .modify((qb) => qb.where("isActive", "=", true))
	 *   .execute();
	 * ```
	 *
	 * **Example - Adding additional selections:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .modify((qb) =>
	 *     qb
	 *       .leftJoin("posts", "posts.userId", "users.id")
	 *       .select((eb) => eb.fn.count("posts.id").as("postCount"))
	 *       .groupBy("users.id")
	 *   )
	 *   .execute();
	 * ```
	 */
	// You can't change the selection here.
	modify<O extends StrictEqual<T["BaseQuery"]["O"], O>>(
		modifier: (qb: QueryBuilderFor<T["BaseQuery"]>) => QueryBuilderWithOutputFor<T["BaseQuery"], O>,
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .where("users.isActive", "=", true)
	 *   .execute();
	 * ```
	 *
	 * **Example - Expression-based where:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username", "age"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinOne(
	 *     "profile",
	 *     ({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "user_id"])),
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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

	/**
	 * Changes the output type of the query.
	 *
	 * This method call doesn't change the SQL in any way. This method simply
	 * returns a copy of this query set with a new output type.
	 */
	$castTo<NewOutput>(): MaybeMappedQuerySet<TWithOutput<T, NewOutput>>;

	/**
	 * Narrows (parts of) the output type of the query.
	 *
	 * This method call doesn't change the SQL in any way. This method simply
	 * returns a copy of this query set with a narrowed output type.
	 *
	 * See {@link k.SelectQueryBuilder.$narrowType} for more information.
	 */
	$narrowType<Narrow>(): MaybeMappedQuerySet<TWithOutput<T, NarrowOutput<T, Narrow>>>;

	/**
	 * Asserts that query's output row type equals the given type `T`.
	 *
	 * This method can be used to simplify excessively complex types to make
	 * TypeScript happy and faster.
	 *
	 * It's also useful as a type guard to ensure a query set matches an expected
	 * shape, similar to annotating a function's return type. For example,
	 * `.$assertType<UserDto>()` will produce a type error if the query's output
	 * doesn't match `UserDto`.
	 *
	 * Using this method doesn't reduce type safety at all. You have to pass in
	 * a type that is structurally equal to the current type.
	 *
	 * See {@link k.SelectQueryBuilder.$assertType} for more information.
	 */
	$assertType<NewOutput extends TOutput<T>>(): TOutput<T> extends NewOutput
		? MaybeMappedQuerySet<TWithOutput<T, NewOutput>>
		: TypeErrorMessage<"$assertType() call failed: The type passed in is not equal to the output type of the query.">;

	//
	// Writes
	//

	/**
	 * Switches the base query to an `INSERT` statement.
	 *
	 * The provided `INSERT` statement is wrapped in a CTE (Common Table Expression).
	 * It MUST include a `RETURNING` clause that returns columns compatible with
	 * the QuerySet's existing base selection to ensure correct hydration.
	 *
	 * **Note:** Data-modifying CTEs and `RETURNING` clauses are only supported by
	 * some dialects (e.g. PostgreSQL).
	 *
	 * **Example:**
	 * ```ts
	 * // Define a reusable query set for fetching users
	 * const usersQuerySet = querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username", "firstName", "lastName"]))
	 *   .extras({
	 *     fullName: (row) => `${row.firstName} ${row.lastName}`
	 *   });
	 *
	 * // Use it to insert a new user and get back the hydrated result
	 * const newUser = await usersQuerySet
	 *   .insert((db) =>
	 *     db.insertInto("users")
	 *       .values(userData)
	 *       // Must return columns matching the base query selection
	 *       .returning(["id", "username", "firstName", "lastName"])
	 *   )
	 *   .executeTakeFirst();
	 * ```
	 *
	 * @param iqb - An insert query builder or factory function.
	 * @returns A new QuerySet with the insert query as the base.
	 */
	insert<IQB extends k.InsertQueryBuilder<any, any, T["BaseQuery"]["O"]>>(
		iqb: InsertQueryBuilderOrFactory<T["DB"], IQB>,
	): MaybeMappedQuerySet<TWithBaseQuery<T, InferTInsertQuery<IQB>>>;

	/**
	 * Like {@link insert}, but switches to an `UPDATE` statement.
	 */
	update<IQB extends k.UpdateQueryBuilder<any, any, any, T["BaseQuery"]["O"]>>(
		uqb: UpdateQueryBuilderOrFactory<T["DB"], IQB>,
	): MaybeMappedQuerySet<TWithBaseQuery<T, InferTUpdateQuery<IQB>>>;

	/**
	 * Like {@link insert}, but switches to a `DELETE` statement.
	 */
	delete<IQB extends k.DeleteQueryBuilder<any, any, T["BaseQuery"]["O"]>>(
		dqb: DeleteQueryBuilderOrFactory<T["DB"], IQB>,
	): MaybeMappedQuerySet<TWithBaseQuery<T, InferTDeleteQuery<IQB>>>;

	/**
	 * Switches the base query to a `SELECT` that may contain data-modifying CTEs.
	 *
	 * Callback 1 receives `db`, builds CTEs, and returns a query creator.
	 * Callback 2 receives a query creator typed with the CTE names and builds
	 * the SELECT.
	 *
	 * @param cteFn - Builds CTEs; returns a query creator.
	 * @param selectFn - Builds the SELECT referencing CTE names.
	 * @returns A new QuerySet with the write query as the base.
	 */
	write<NewDB, SQB extends k.SelectQueryBuilder<any, any, T["BaseQuery"]["O"]>>(
		cteFn: (db: k.Kysely<T["DB"]>) => k.QueryCreator<NewDB>,
		selectFn: (qc: k.QueryCreator<NewDB>) => SQB,
	): MaybeMappedQuerySet<TWithBaseQuery<T, InferTSelectQuery<SQB>>>;
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
 * @template T - The query set's type parameters.
 */
interface QuerySet<in out T extends TQuerySet> extends MappedQuerySet<T> {
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
	 *   .selectAs("users", (eb) => eb.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]))
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
	extras<E extends Extras<THydrationInput<T>>>(
		extras: E,
	): QuerySet<TWithExtendedOutput<T, InferExtras<THydrationInput<T>, E>>>;

	/**
	 * Adds computed fields to the hydrated output by spreading the return value
	 * of a function.  Unlike `.extras()` which defines one field at a time,
	 * `.extend()` calls a single function whose returned object is merged into
	 * the output.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("users", (eb) => eb.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]))
	 *   .extend((row) => ({
	 *     fullName: `${row.firstName} ${row.lastName}`,
	 *     initials: `${row.firstName[0]}${row.lastName[0]}`,
	 *   }))
	 *   .execute();
	 * // ⬇
	 * type Result = Array<{
	 *   id: number;
	 *   firstName: string;
	 *   lastName: string;
	 *   fullName: string;
	 *   initials: string;
	 * }>;
	 * ```
	 *
	 * @param fn - A function that receives the row and returns an object of
	 *   computed properties.
	 * @returns A new HydratedQueryBuilder with the extender applied.
	 */
	extend<F extends Extender<THydrationInput<T>>>(
		fn: F,
	): QuerySet<TWithExtendedOutput<T, InferExtender<THydrationInput<T>, F>>>;

	/**
	 * Transforms already-selected field values in the hydrated output.  Fields
	 * not mentioned in the mappings will still be included as-is.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("users", (eb) => eb.selectFrom("users").select(["users.id", "users.name"]))
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
	mapFields<M extends FieldMappings<THydrationInput<T>>>(
		mappings: M,
	): QuerySet<TWithExtendedOutput<T, InferFields<THydrationInput<T>, M>>>;

	/**
	 * Omits specified fields from the hydrated output.  Useful for excluding
	 * fields that were selected for internal use (e.g., for extras).
	 *
	 * ### Examples
	 *
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("users", (eb) => eb.selectFrom("users").select(["users.id", "users.firstName", "users.lastName"]))
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
	omit<K extends keyof THydrationInput<T>>(keys: readonly K[]): QuerySet<TWithOmit<T, K>>;

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
	 *   .selectAs("users", (eb) => eb.selectFrom("users").select(["users.id", "users.name", "users.email"]))
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
	with<OtherInput extends StrictSubset<THydrationInput<T>, OtherInput>, OtherOutput>(
		hydrator: FullHydrator<OtherInput, OtherOutput>,
	): QuerySet<TWithExtendedOutput<T, OtherOutput>>;
	// If you pass a Hydrator with a map applied, we must return a
	// MappedHydratedQueryBuilder.
	with<OtherInput extends StrictSubset<THydrationInput<T>, OtherInput>, OtherOutput>(
		hydrator: MappedHydrator<OtherInput, OtherOutput>,
	): QuerySet<TWithExtendedOutput<T, OtherOutput>>;

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
	 *   .selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return querySet(db).selectAs(
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
	 *   .selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username"]))
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
		fetchFn: ToFetchFn<T, FetchFnReturn>,
		keys: ToAttachedKeysArg<T, NoInfer<FetchFnReturn>>,
	): QuerySetWithAttach<T, K, "AttachMany", FetchFnReturn>;

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
	 *   .selectAs("post", (eb) => eb.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .attachOne(
	 *     "author",
	 *     (postRows) => {
	 *       const userIds = [...new Set(postRows.map((p) => p.userId))];
	 *       return querySet(db).selectAs(
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
	 *   .selectAs("post", (eb) => eb.selectFrom("posts").select(["id", "title", "userId"]))
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
		fetchFn: ToFetchFn<T, FetchFnReturn>,
		keys: ToAttachedKeysArg<T, NoInfer<FetchFnReturn>>,
	): QuerySetWithAttach<T, K, "AttachOne", FetchFnReturn>;

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
		fetchFn: ToFetchFn<T, FetchFnReturn>,
		keys: ToAttachedKeysArg<T, NoInfer<FetchFnReturn>>,
	): QuerySetWithAttach<T, K, "AttachOneOrThrow", FetchFnReturn>;

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
	 *   .selectAs("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .innerJoinOne(
	 *     "author",  // Key (alias) - extra argument compared to Kysely
	 *     ({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
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
	 *   .selectAs("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .innerJoinOne(
	 *     "author",
	 *     ({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
	 *     (join) => join.onRef("user.id", "=", "post.userId"),  // Same as Kysely's callback
	 *   )
	 *   .execute();
	 * ```
	 *
	 * **Example with pre-built query set:**
	 * ```ts
	 * const authorQuery = querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]));
	 *
	 * const posts = await querySet(db)
	 *   .selectAs("post", db.selectFrom("posts").select(["id", "title", "userId"]))
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
	innerJoinOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinOne", TNested>;
	innerJoinOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinOne", TNested>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "posts",
	 *     ({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "userId"])),
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "publishedPosts",
	 *     ({ select }) =>
	 *       select(qb =>
	 *         qb
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
	innerJoinMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;
	innerJoinMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinOne(
	 *     "profile",
	 *     ({ eb, qs }) => qs(eb.selectFrom("profiles").select(["id", "bio", "userId"])),
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
	leftJoinOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOne", TNested>;
	leftJoinOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOne", TNested>;

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
	 *   .selectAs("post", db.selectFrom("posts").select(["id", "title", "userId"]))
	 *   .leftJoinOneOrThrow(
	 *     "author",
	 *     ({ eb, qs }) => qs(eb.selectFrom("users").select(["id", "username"])),
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
	leftJoinOneOrThrow<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOneOrThrow", TNested>;
	leftJoinOneOrThrow<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOneOrThrow", TNested>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     ({ eb, qs }) => qs(eb.selectFrom("posts").select(["id", "title", "userId"])),
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     ({ eb, qs }) =>
	 *       qs(eb.selectFrom("posts").select(["id", "title", "userId"]))
	 *         .leftJoinMany(
	 *           "comments",
	 *           ({ eb, qs }) =>
	 *             qs(eb.selectFrom("comments").select(["id", "content", "postId"])),
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
	leftJoinMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinMany", TNested>;
	leftJoinMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinMany", TNested>;

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
	 * **Example:**
	 * ```ts
	 * const products = await querySet(db)
	 *   .selectAs("product", db.selectFrom("products").select(["id", "name"]))
	 *   .crossJoinMany(
	 *     "colors",
	 *     ({ eb, qs }) => qs(eb.selectFrom("colors").select(["id", "name"])),
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @returns A new QuerySet with the cross join added.
	 */
	crossJoinMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;

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
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 * Works like {@link innerJoinOne} but with `INNER JOIN LATERAL` in SQL.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinLateralOne(
	 *     "latestPost",
	 *     ({ eb, qs }) =>
	 *       qs(
	 *         eb.selectFrom("posts")
	 *           .select(["id", "title", "createdAt"])
	 *           .where("userId", "=", eb.ref("user.id"))
	 *           .orderBy("createdAt", "desc")
	 *           .limit(1)
	 *       ),
	 *     (join) => join.onTrue(),
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner lateral join added.
	 */
	innerJoinLateralOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinOne", TNested>;
	innerJoinLateralOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinOne", TNested>;

	/**
	 * Adds an inner lateral join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.innerJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 * Works like {@link innerJoinMany} but with `INNER JOIN LATERAL` in SQL.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinLateralMany(
	 *     "topPosts",
	 *     ({ eb, qs }) =>
	 *       qs(
	 *         eb.selectFrom("posts")
	 *           .select(["id", "title", "views"])
	 *           .where("userId", "=", eb.ref("user.id"))
	 *           .orderBy("views", "desc")
	 *           .limit(5)
	 *       ),
	 *     (join) => join.onTrue(),
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the inner lateral join added.
	 */
	innerJoinLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;
	innerJoinLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;

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
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 * Works like {@link leftJoinOne} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinLateralOne(
	 *     "latestPost",
	 *     ({ eb, qs }) =>
	 *       qs(
	 *         eb.selectFrom("posts")
	 *           .select(["id", "title", "createdAt"])
	 *           .where("userId", "=", eb.ref("user.id"))
	 *           .orderBy("createdAt", "desc")
	 *           .limit(1)
	 *       ),
	 *     (join) => join.onTrue(),
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOne", TNested>;
	leftJoinLateralOne<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOne", TNested>;

	/**
	 * Adds a left lateral join that hydrates into a single nested object, throwing if not found.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 *
	 * Works like {@link leftJoinOneOrThrow} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested object in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralOneOrThrow<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOneOrThrow", TNested>;
	leftJoinLateralOneOrThrow<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinOneOrThrow", TNested>;

	/**
	 * Adds a left lateral join that hydrates into a nested array.
	 *
	 * Similar to {@link k.SelectQueryBuilder.leftJoinLateral}, but with an additional
	 * first argument (`key`) for the alias/key name, and requiring a QuerySet instead
	 * of a table expression.
	 *
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 *
	 * Works like {@link leftJoinMany} but with `LEFT JOIN LATERAL` in SQL.
	 *
	 * **Example:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinLateralMany(
	 *     "recentPosts",
	 *     ({ eb, qs }) =>
	 *       qs(
	 *         eb.selectFrom("posts")
	 *           .select(["id", "title", "createdAt"])
	 *           .where("userId", "=", eb.ref("user.id"))
	 *           .orderBy("createdAt", "desc")
	 *           .limit(3)
	 *       ),
	 *     (join) => join.onTrue(),
	 *   )
	 *   .execute();
	 * ```
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @param k1 - First join reference (when using simple syntax).
	 * @param k2 - Second join reference (when using simple syntax).
	 * @param callback - Join callback (when using callback syntax).
	 * @returns A new QuerySet with the left lateral join added.
	 */
	leftJoinLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		k1: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
		k2: JoinReferenceExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinMany", TNested>;
	leftJoinLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
		callback: JoinCallbackExpression<T, NoInfer<Key>, NoInfer<TNested>>,
	): QuerySetWithJoin<T, Key, "LeftJoinMany", TNested>;

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
	 * Lateral joins allow the nested query to reference columns from the base query
	 * using the `eb` (expression builder) parameter.
	 *
	 * Works like {@link crossJoinMany} but with `CROSS JOIN LATERAL` in SQL.
	 *
	 * @param key - The key name for the nested array in the output.
	 * @param querySet - A nested query set or factory function.
	 * @returns A new QuerySet with the cross lateral join added.
	 */
	crossJoinLateralMany<Key extends string, TNested extends TSelectQuerySet>(
		key: Key,
		querySet: NestedQuerySetOrFactory<T, NoInfer<Key>, TNested>,
	): QuerySetWithJoin<T, Key, "InnerJoinMany", TNested>;

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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany("posts", ...)
	 *   .modify((qb) => qb.where("isActive", "=", true))
	 *   .execute();
	 * ```
	 *
	 * **Example - Modifying a joined QuerySet with filtering:**
	 * ```ts
	 * const users = await querySet(db)
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .leftJoinMany(
	 *     "posts",
	 *     ({ eb, qs }) =>
	 *       qs(eb.selectFrom("posts").select(["id", "title", "userId"])),
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .innerJoinMany(
	 *     "posts",
	 *     ({ eb, qs }) =>
	 *       qs(eb.selectFrom("posts").select(["id", "title", "userId"])),
	 *     "post.userId",
	 *     "user.id",
	 *   )
	 *   // Enhance the posts collection by attaching additional data
	 *   .modify("posts", (postsQuerySet) =>
	 *     postsQuerySet.attachOne(
	 *       "metadata",
	 *       (postRows) => {
	 *         const postIds = postRows.map((p) => p.id);
	 *         return querySet(db).selectAs("metadata", (eb) =>
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
	 *   .attachMany(
	 *     "posts",
	 *     (userRows) => {
	 *       const userIds = userRows.map((u) => u.id);
	 *       return querySet(db).selectAs("post", (eb) =>
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
	 *   .selectAs("user", db.selectFrom("users").select(["id", "username"]))
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
		modifier: (qb: QueryBuilderFor<T["BaseQuery"]>) => QueryBuilderWithOutputFor<T["BaseQuery"], O>,
	): this;
	modify<
		NewDB,
		NewTB extends keyof NewDB,
		NewUT extends keyof NewDB,
		NewO extends T["BaseQuery"]["O"],
	>(
		modifier: (
			qb: QueryBuilderFor<T["BaseQuery"]>,
		) => NewQueryBuilderFor<T["BaseQuery"], NewDB, NewTB, NewUT, NewO>,
	): QuerySet<TWithBaseQuery<T, { Type: "Select"; DB: NewDB; TB: NewTB; O: NewO; UT: never }>>;
	// Modify collection.
	modify<
		Key extends keyof T["Collections"] & string,
		TNestedNew extends TSelectQuerySet = never,
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
	TNestedNew extends TSelectQuerySet,
	NewValue extends SomeFetchFnReturn,
> {
	InnerJoinOne: QuerySetWithJoin<T, Key, "InnerJoinOne", TNestedNew>;
	InnerJoinMany: QuerySetWithJoin<T, Key, "InnerJoinMany", TNestedNew>;
	LeftJoinOne: QuerySetWithJoin<T, Key, "LeftJoinOne", TNestedNew>;
	LeftJoinOneOrThrow: QuerySetWithJoin<T, Key, "LeftJoinOneOrThrow", TNestedNew>;
	LeftJoinMany: QuerySetWithJoin<T, Key, "LeftJoinMany", TNestedNew>;

	AttachOne: QuerySetWithAttach<T, Key, "AttachOne", NewValue>;
	AttachOneOrThrow: QuerySetWithAttach<T, Key, "AttachOneOrThrow", NewValue>;
	AttachMany: QuerySetWithAttach<T, Key, "AttachMany", NewValue>;
}

////////////////////////////////////////////////////////////
// Attach Helpers.
////////////////////////////////////////////////////////////

type ToFetchFn<T extends TQuerySet, FetchFnReturn extends SomeFetchFnReturn> = SomeFetchFn<
	THydrationInput<T>,
	FetchFnReturn
>;

type ToAttachedKeysArg<
	T extends TQuerySet,
	FetchFnReturn extends SomeFetchFnReturn,
> = AttachedKeysArg<THydrationInput<T>, AttachedOutputFromFetchFnReturn<NoInfer<FetchFnReturn>>>;

interface AttachedOutputMap<in out FetchFnReturn extends SomeFetchFnReturn> {
	AttachOne: AttachedOutputFromFetchFnReturn<FetchFnReturn> | null;
	AttachOneOrThrow: AttachedOutputFromFetchFnReturn<FetchFnReturn>;
	AttachMany: AttachedOutputFromFetchFnReturn<FetchFnReturn>[];
}

interface TQuerySetWithAttach<
	in out T extends TQuerySet,
	in out Key extends string,
	in out Type extends TAttachType,
	in out FetchFnReturn extends SomeFetchFnReturn,
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
	HydratedOutput: ExtendWith<T["HydratedOutput"], Key, AttachedOutputMap<FetchFnReturn>[Type]>;
	OmittedKeys: T["OmittedKeys"];
}

interface QuerySetWithAttach<
	in out T extends TQuerySet,
	in out Key extends string,
	in out Type extends TAttachType,
	in out FetchFnReturn extends SomeFetchFnReturn,
> extends QuerySet<TQuerySetWithAttach<T, Key, Type, FetchFnReturn>> {}

////////////////////////////////////////////////////////////
// Join Helpers.
////////////////////////////////////////////////////////////

// Important: for all these nested operations, we use the *initial*
// JoinedQuery---not the actual JoinedQuery.  This guarantees at a type level
// that adjacent joins do not depend on each other.  (We furthermore cannot use
// T["BaseQuery"] because that's not what the JoinedQuery ever looks like.)

type NestedQuerySetOrFactory<
	T extends TQuerySet,
	Alias extends string,
	TNested extends TSelectQuerySet,
> = MappedQuerySet<TNested> | JoinBuilderCallback<T, Alias, TNested>;

type JoinBuilderCallback<
	T extends TQuerySet,
	Alias extends string,
	TNested extends TSelectQuerySet,
> = (builder: JoinBuilderCallbackArgs<T, Alias>) => MappedQuerySet<TNested>;

interface JoinBuilderCallbackArgs<T extends TQuerySet, Alias extends string> {
	eb: k.ExpressionBuilder<ToInitialJoinedDB<T>, ToInitialJoinedTB<T>>;
	qs: NestedQuerySetFnFor<T, Alias>;
}

type NestedQuerySetFnFor<T extends TQuerySet, Alias extends string> = NestedQuerySetFn<
	ToInitialJoinedDB<T>,
	Alias
>;

type ToTableExpression<Key extends string, TNested extends TSelectQuerySet> = k.AliasedExpression<
	TNested["BaseQuery"]["O"],
	Key
>;

type JoinReferenceExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TSelectQuerySet,
> = k.JoinReferenceExpression<
	ToInitialJoinedDB<T>,
	ToInitialJoinedTB<T>,
	ToTableExpression<Key, TNested>
>;

type JoinCallbackExpression<
	T extends TQuerySet,
	Key extends string,
	TNested extends TSelectQuerySet,
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
	TNested extends TSelectQuerySet,
> = Type extends CardinalityOneJoinType
	? T["OrderableColumns"] | ApplyPrefixWithSep<Key, TNested["OrderableColumns"]>
	: T["OrderableColumns"];

type InnerJoinOutput<
	T extends TQuerySet,
	TNested extends TSelectQuerySet,
	Key extends string,
> = Flatten<
	// Extend the *JoinedQuery* output, which includes both the base output and also
	// output from other joins.
	T["JoinedQuery"]["O"] & ApplyPrefixes<MakeInitialPrefix<Key>, TNested["JoinedQuery"]["O"]>
>;

type TQueryWithInnerJoin<
	T extends TQuerySet,
	Key extends string,
	TNested extends TSelectQuerySet,
> = InferTSelectQuery<
	k.SelectQueryBuilderWithInnerJoin<
		ToInitialJoinedDB<T>,
		ToInitialJoinedTB<T>,
		InnerJoinOutput<T, TNested, Key>,
		ToTableExpression<Key, TNested>
	>
>;

// Compared to the inner join, the left joined output is nullable.
type LeftJoinOutput<
	T extends TQuerySet,
	TNested extends TSelectQuerySet,
	Key extends string,
> = Flatten<
	T["JoinedQuery"]["O"] &
		ApplyPrefixes<MakeInitialPrefix<Key>, k.Nullable<TNested["JoinedQuery"]["O"]>>
>;

type TQueryWithLeftJoin<
	T extends TQuerySet,
	Key extends string,
	TNested extends TSelectQuerySet,
> = InferTSelectQuery<
	k.SelectQueryBuilderWithLeftJoin<
		ToInitialJoinedDB<T>,
		ToInitialJoinedTB<T>,
		LeftJoinOutput<T, TNested, Key>,
		ToTableExpression<Key, TNested>
	>
>;

interface JoinedQueryMap<
	in out T extends TQuerySet,
	in out Key extends string,
	in out TNested extends TSelectQuerySet,
> {
	InnerJoinOne: TQueryWithInnerJoin<T, Key, TNested>;
	InnerJoinMany: TQueryWithInnerJoin<T, Key, TNested>;

	LeftJoinOne: TQueryWithLeftJoin<T, Key, TNested>;
	LeftJoinOneOrThrow: TQueryWithLeftJoin<T, Key, TNested>;
	LeftJoinMany: TQueryWithLeftJoin<T, Key, TNested>;
}

interface JoinHydratedRowMap<in out TNested extends TSelectQuerySet> {
	InnerJoinOne: TOutput<TNested>;
	InnerJoinMany: TOutput<TNested>[];
	LeftJoinOne: TOutput<TNested> | null;
	LeftJoinOneOrThrow: TOutput<TNested>;
	LeftJoinMany: TOutput<TNested>[];
}

type TQuerySetWithJoin<
	T extends TQuerySet,
	Key extends string,
	Type extends TJoinType,
	TNested extends TSelectQuerySet,
> = Flatten<{
	DB: T["DB"];
	IsMapped: T["IsMapped"];
	BaseAlias: T["BaseAlias"];
	BaseQuery: T["BaseQuery"];
	Collections: TCollectionsWith<
		T["Collections"],
		Key,
		{ Prototype: "Join"; Type: Type; Value: TNested }
	>;
	JoinedQuery: JoinedQueryMap<T, Key, TNested>[Type];
	OrderableColumns: TOrderableColumnsWithJoin<T, Key, Type, TNested>;
	HydratedOutput: ExtendWith<T["HydratedOutput"], Key, JoinHydratedRowMap<TNested>[Type]>;
	OmittedKeys: T["OmittedKeys"];
}>;

interface QuerySetWithJoin<
	in out T extends TQuerySet,
	in out Key extends string,
	in out Type extends TJoinType,
	in out TNested extends TSelectQuerySet,
> extends QuerySet<TQuerySetWithJoin<T, Key, Type, TNested>> {}

////////////////////////////////////////////////////////////
// Implementation
////////////////////////////////////////////////////////////

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
	baseQuery: AnyQueryBuilder;
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
	writeQueryCreator: k.QueryCreator<any> | null;
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

	toBaseQuery(): AnyQueryBuilder {
		return this.#props.baseQuery;
	}

	#getSelectFromBase(isNested: boolean, isLocalSubquery: boolean): AnySelectQueryBuilder {
		const { db, baseQuery, baseAlias, writeQueryCreator } = this.#props;

		// We always inline SELECT queries.
		if (isSelectQueryBuilder(baseQuery)) {
			// When a writeQueryCreator is available, use it to build the outer
			// query so CTEs live at the top level.  The base query (which has no
			// CTEs) becomes a derived table.
			if (writeQueryCreator) {
				if (isNested) {
					throw new InvalidJoinedQuerySetError(baseAlias);
				}
				const qc = writeQueryCreator;
				let qb = qc.selectFrom(baseQuery.as(baseAlias));
				qb = applyHoistedSelections(qb, baseQuery, baseAlias);
				return qb;
			}

			const qb = db.selectFrom(baseQuery.as(baseAlias));
			return applyHoistedSelections(qb, baseQuery, baseAlias);
		}

		// Non-select queries must be converted to a CTE.  Also, they cannot be nested.

		if (isNested) {
			throw new InvalidJoinedQuerySetError(baseAlias);
		}

		const queryCreator = isLocalSubquery ? db : db.with("__base", () => baseQuery);

		let qb = queryCreator.selectFrom(`__base as ${baseAlias}`);

		// If it's truly at the top level, we can safely use a `.selectAll()` here because these can't
		// be nested anyway, so no further hoisting can happen.  These seems like a nice convenience so
		// you can just do updateFrom().returningAll() and not have to redeclare your columns.  This
		// should be 99% of use cases for writes unless you're actually applying a LIMIT or OFFSET to
		// the response.
		if (!isLocalSubquery) {
			return qb.selectAll(baseAlias);
		}

		return applyHoistedSelections(qb, baseQuery, baseAlias);
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
		const nestedQuery = collection.querySet.#toQuery(true, true);
		const from = nestedQuery.as(key);
		// This cast to a single method helps TypeScript follow the overloads.
		qb = qb[collection.method as "innerJoin"](from, ...collection.args);

		// Add the (prefixed) selections from the subquery to the parent query.
		const prefix = makePrefix("", key);
		qb = applyHoistedPrefixedSelections(prefix, qb, nestedQuery, key);

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
	#toCardinalityOneQuery(isNested: boolean, isLocalSubquery: boolean): AnySelectQueryBuilder {
		const { joinCollections } = this.#props;

		let qb = this.#getSelectFromBase(isNested, isLocalSubquery);

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

	#toJoinedQuery(isNested: boolean, isLocalSubquery: boolean): AnySelectQueryBuilder {
		const { joinCollections } = this.#props;

		let qb = this.#getSelectFromBase(isNested, isLocalSubquery);

		for (const [key, collection] of joinCollections) {
			qb = this.#addCollectionAsJoin(qb, key, collection);
		}

		// NOTE: Limit and offset cannot be applied here because of row explosion.

		// Apply ordering---but only if we're not prefixed, because ordering in
		// subqueries is ignored (well, "not guaranteed") unless you also have a
		// LIMIT or OFFSET.
		const isSubquery = isNested || isLocalSubquery;
		if (!isSubquery) {
			qb = this.#applyOrderBy(qb);
		}

		return qb;
	}

	toJoinedQuery(): AnySelectQueryBuilder {
		return this.#toJoinedQuery(false, false);
	}

	// This funny syntax because Node type-stripping doesn't support overloaded private methods?
	#toQuery<IsNested extends boolean>(
		isNested: IsNested,
		isLocalSubquery: boolean,
	): IsNested extends true ? AnySelectQueryBuilder : AnyQueryBuilder {
		const { baseQuery, baseAlias, db, limit, offset, orderBy, orderByKeys, joinCollections } =
			this.#props;

		// If we have no joins (no row explosion) and no ordering (therefore nothing referencing the
		// baseAlias) we can do less nesting.
		if (!joinCollections.size && !orderBy.length && !orderByKeys) {
			// No limit and offset and no joins means we can return as is for any type of query builder.
			// No CTE, no subqueries, no nothing.
			if (!limit && !offset) {
				if (isNested && !isSelectQueryBuilder(baseQuery)) {
					throw new InvalidJoinedQuerySetError(baseAlias);
				}
				return baseQuery as IsNested extends true ? AnySelectQueryBuilder : AnyQueryBuilder;
			}

			// If it's a SELECT, we can just apply the limit and offset to the base query.
			if (isSelectQueryBuilder(baseQuery)) {
				return this.#applyLimitAndOffset(baseQuery);
			}

			// Otherwise, for writes, (unusual use case) we need to make it a CTE.  Just select all
			// instead of hoisting, because these can't be nested anyway (so we will never need to hoist
			// from here).
			return this.#applyLimitAndOffset(
				this.#getSelectFromBase(isNested, isLocalSubquery).selectAll(),
			);
		}

		// If no pagination, just return the joined query, even if it has row explosion.
		if (!limit && !offset) {
			return this.#toJoinedQuery(isNested, isLocalSubquery);
		}

		// If only cardinality-one joins, we can safely apply limit/offset to the
		// joined query.
		if (this.#isCardinalityOne()) {
			return this.#applyLimitAndOffset(this.#toJoinedQuery(isNested, isLocalSubquery));
		}

		let cardinalityOneQuery = this.#toCardinalityOneQuery(isNested, isLocalSubquery);

		cardinalityOneQuery = this.#applyLimitAndOffset(cardinalityOneQuery);
		// Ordering in the subquery only matters if there is a limit or offset.
		if (limit || offset) {
			cardinalityOneQuery = this.#applyOrderBy(cardinalityOneQuery);
		}

		const aliasedCardinalityOneQuery = cardinalityOneQuery.as(baseAlias);
		let qb = db.selectFrom(aliasedCardinalityOneQuery);
		// Re-hoist ALL selections from the cardinality one query.  This will include base query
		// selections, but possibly also others.  We could do `"baseAlias".*` but then this couldn't be
		// hoisted further by parent queries.
		qb = applyHoistedSelections(qb, cardinalityOneQuery, baseAlias);

		// Add any cardinality-many joins.
		for (const [key, collection] of joinCollections) {
			if (!this.#isCollectionCardinalityOne(collection)) {
				qb = this.#addCollectionAsJoin(qb, key, collection);
			}
		}

		// Re-apply ordering since the order from the subquery is not guaranteed to
		// be preserved.  This doesn't matter if we have a prefix because it means
		// we're in a subquery already.
		const isSubquery = isNested || isLocalSubquery;
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

	toQuery(): any {
		return this.#toQuery(false, false);
	}

	toCountQuery(): OpaqueCountQueryBuilder {
		return this.#toCardinalityOneQuery(false, false)
			.clearSelect()
			.select((eb) => eb.fn.countAll().as("count"));
	}

	toExistsQuery(): OpaqueExistsQueryBuilder {
		return this.#props.db.selectNoFrom(({ exists }) =>
			exists(
				this.#toCardinalityOneQuery(false, false)
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

	async hydrate(input: any): Promise<any> {
		return this.#props.hydrator.hydrate(await input, {
			// Auto include fields at all levels, so we don't have to understand the
			// shape of the selection and can allow it to be inferred by the shape of
			// the rows.
			[EnableAutoInclusion]: true,
			// Sort nested collections, since their order cannot be guaranteed by SQL.
			sort: "nested",
		});
	}

	async execute(): Promise<any[]> {
		const rows = await this.toQuery().execute();
		return this.hydrate(rows);
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

	extend(fn: Extender<any>) {
		return this.#clone({
			hydrator: asFullHydrator(this.#props.hydrator).extend(fn),
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
			hydrator: asFullHydrator(this.#props.hydrator).with(hydrator),
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
		let resolved: MappedQuerySet<any>;

		if (typeof nestedQuerySet === "function") {
			const qs = ((query: AnySelectQueryBuilder, keyBy?: KeyBy<any>) => {
				const creator = querySet(this.#props.db);
				return creator.selectAs(key, query, keyBy as any);
			}) as any;

			const callbackArgs = {
				eb: k.expressionBuilder<any, any>(),
				qs,
			};

			resolved = nestedQuerySet(callbackArgs);
		} else {
			resolved = nestedQuerySet;
		}

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

	modify(keyOrModifier: string | ((qb: any) => any), modifier?: (value: any) => any): any {
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

	$castTo(): any {
		return this;
	}

	$narrowType(): any {
		return this;
	}

	$assertType(): any {
		return this;
	}

	#asWrite(query: AnyQueryBuilderOrFactory): any {
		return this.#clone({
			baseQuery: typeof query === "function" ? query(this.#props.db) : query,
		});
	}

	insert(iqb: AnyInsertQueryBuilder | AnyInsertQueryBuilderFactory) {
		return this.#asWrite(iqb);
	}

	update(iqb: AnyUpdateQueryBuilder | AnyUpdateQueryBuilderFactory) {
		return this.#asWrite(iqb);
	}

	delete(iqb: AnyDeleteQueryBuilder | AnyDeleteQueryBuilderFactory) {
		return this.#asWrite(iqb);
	}

	write(cteFn: (db: any) => any, selectFn: (qc: any) => AnySelectQueryBuilder): any {
		const qc = cteFn(this.#props.db);
		const baseQuery = selectFn(qc).withPlugin(stripWithPlugin);
		return this.#clone({
			baseQuery,
			writeQueryCreator: qc,
		});
	}
}

////////////////////////////////////////////////////////////
// QuerySetCreator.
////////////////////////////////////////////////////////////

interface InitialQuerySet<
	in out DB,
	in out BaseAlias extends string,
	in out BaseQuery extends TQuery,
> extends QuerySet<{
	DB: DB;
	IsMapped: false;
	BaseAlias: BaseAlias;
	BaseQuery: BaseQuery;
	Collections: {};
	// The joined query mostly looks like the base query.
	JoinedQuery: InitialJoinedQuery<BaseQuery["DB"], BaseAlias, BaseQuery["O"]>;
	// No orderable columns other than the base query yet.
	OrderableColumns: keyof BaseQuery["O"] & string;
	// The hydrated output is the same as the base query output; no mapping yet.
	HydratedOutput: Flatten<BaseQuery["O"]>;
	// No keys have been omitted yet.
	OmittedKeys: never;
}> {}

// type InferDB<Q> = Q extends k.SelectQueryBuilder<infer BaseDB, any, any> ? BaseDB : never;
// type InferTB<Q> = Q extends k.SelectQueryBuilder<any, infer BaseTB, any> ? BaseTB : never;
// type InferO<Q> = Q extends k.SelectQueryBuilder<any, any, infer BaseO> ? BaseO : never;

// Minimal subsets of k.Kysely<DB>, which doesn't allow doing other things, such as with
// expressions.

interface SelectCreator<DB> {
	selectFrom: k.QueryCreator<DB>["selectFrom"];
}
interface InsertCreator<DB> {
	insertInto: k.QueryCreator<DB>["insertInto"];
}
interface UpdateCreator<DB> {
	updateTable: k.QueryCreator<DB>["updateTable"];
}
interface DeleteCreator<DB> {
	deleteFrom: k.QueryCreator<DB>["deleteFrom"];
}

type SelectQueryBuilderFactory<InitialDB, SQB extends AnySelectQueryBuilder> = (
	db: SelectCreator<InitialDB>,
) => SQB;

type SelectQueryBuilderOrFactory<InitialDB, SQB extends AnySelectQueryBuilder> =
	| SQB
	| SelectQueryBuilderFactory<InitialDB, SQB>;

type InsertQueryBuilderFactory<InitialDB, IQB extends AnyInsertQueryBuilder> = (
	db: InsertCreator<InitialDB>,
) => IQB;

type InsertQueryBuilderOrFactory<InitialDB, IQB extends AnyInsertQueryBuilder> =
	| IQB
	| InsertQueryBuilderFactory<InitialDB, IQB>;

type UpdateQueryBuilderFactory<InitialDB, UQB extends AnyUpdateQueryBuilder> = (
	db: UpdateCreator<InitialDB>,
) => UQB;

type UpdateQueryBuilderOrFactory<InitialDB, UQB extends AnyUpdateQueryBuilder> =
	| UQB
	| UpdateQueryBuilderFactory<InitialDB, UQB>;

type DeleteQueryBuilderFactory<InitialDB, DQB extends AnyDeleteQueryBuilder> = (
	db: DeleteCreator<InitialDB>,
) => DQB;

type DeleteQueryBuilderOrFactory<InitialDB, DQB extends AnyDeleteQueryBuilder> =
	| DQB
	| DeleteQueryBuilderFactory<InitialDB, DQB>;

type AnySelectQueryBuilderFactory = (db: SelectCreator<any>) => AnySelectQueryBuilder;
type AnyInsertQueryBuilderFactory = (db: InsertCreator<any>) => AnyInsertQueryBuilder;
type AnyUpdateQueryBuilderFactory = (db: UpdateCreator<any>) => AnyUpdateQueryBuilder;
type AnyDeleteQueryBuilderFactory = (db: DeleteCreator<any>) => AnyDeleteQueryBuilder;

type AnyQueryBuilderFactory =
	| AnySelectQueryBuilderFactory
	| AnyInsertQueryBuilderFactory
	| AnyUpdateQueryBuilderFactory
	| AnyDeleteQueryBuilderFactory;
type AnyQueryBuilderOrFactory = AnyQueryBuilder | AnyQueryBuilderFactory;

interface NestedQuerySetFn<in out DB, in out Alias extends string> {
	<SQB extends k.SelectQueryBuilder<any, any, InputWithDefaultKey>>(
		query: SQB,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
	<SQB extends AnySelectQueryBuilder>(
		query: SQB,
		keyBy: KeyBy<InferO<NoInfer<SQB>>>,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
}

type InferO<X> = X extends k.SelectQueryBuilder<any, any, infer O> ? O : never;

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

	#createQuerySet(
		alias: string,
		query: AnyQueryBuilderOrFactory,
		keyBy: KeyBy<any> = DEFAULT_KEY_BY,
	) {
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
			writeQueryCreator: null,
		}) as any;
	}

	/**
	 * Initializes a new query set with a base select query and an alias.
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
	 * querySet(db).selectAs(
	 *   "user",
	 *   db.selectFrom("users").select(["id", "username", "email"])
	 * )
	 * ```
	 *
	 * **Example with factory function:**
	 * ```ts
	 * querySet(db).selectAs(
	 *   "user",
	 *   (eb) => eb.selectFrom("users").select(["id", "username", "email"])
	 * )
	 * ```
	 *
	 * **Example with custom keyBy:**
	 * ```ts
	 * querySet(db).selectAs(
	 *   "session",
	 *   db.selectFrom("sessions").select(["sessionId", "userId"]),
	 *   "sessionId" // Use sessionId instead of id
	 * )
	 * ```
	 *
	 * **Example with composite key:**
	 * ```ts
	 * querySet(db).selectAs(
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
	selectAs<Alias extends string, SQB extends k.SelectQueryBuilder<any, any, InputWithDefaultKey>>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, SQB>,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
	selectAs<Alias extends string, SQB extends AnySelectQueryBuilder>(
		alias: Alias,
		query: SelectQueryBuilderOrFactory<DB, SQB>,
		keyBy: KeyBy<InferO<NoInfer<SQB>>>,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
	selectAs(alias: string, query: any, keyBy: KeyBy<any> = DEFAULT_KEY_BY): any {
		return this.#createQuerySet(alias, query, keyBy);
	}

	/**
	 * Initializes a new query set with a base `INSERT` query.
	 *
	 * The provided `INSERT` statement is wrapped in a CTE (Common Table Expression)
	 * to allow joining other data to the result. It MUST include a `RETURNING`
	 * clause.
	 *
	 * **Note:** Data-modifying CTEs and `RETURNING` clauses are only supported by
	 * some dialects (e.g. PostgreSQL).
	 *
	 * **Example:**
	 * ```ts
	 * const newUser = await querySet(db)
	 *   .insertAs(
	 *     "user",
	 *     db.insertInto("users")
	 *       .values(userData)
	 *       .returning(["id", "username", "firstName", "lastName"])
	 *   )
	 *   .extras({
	 *     fullName: (row) => `${row.firstName} ${row.lastName}`,
	 *   })
	 *   .executeTakeFirst();
	 * ```
	 *
	 * @param alias - The alias for the base query.
	 * @param query - A Kysely insert query builder or factory function.
	 * @param keyBy - The key(s) to uniquely identify rows. Defaults to `"id"`.
	 * @returns A new QuerySet.
	 */
	insertAs<Alias extends string, IQB extends k.InsertQueryBuilder<any, any, InputWithDefaultKey>>(
		alias: Alias,
		query: InsertQueryBuilderOrFactory<DB, IQB>,
	): InitialQuerySet<DB, Alias, InferTInsertQuery<IQB>>;
	insertAs<Alias extends string, IQB extends AnyInsertQueryBuilder>(
		alias: Alias,
		query: InsertQueryBuilderOrFactory<DB, IQB>,
		keyBy: KeyBy<InferO<NoInfer<IQB>>>,
	): InitialQuerySet<DB, Alias, InferTInsertQuery<IQB>>;
	insertAs(alias: string, query: any, keyBy: KeyBy<any> = DEFAULT_KEY_BY): any {
		return this.#createQuerySet(alias, query, keyBy);
	}

	/**
	 * Like {@link insertAs}, but for an `UPDATE` statement.
	 */
	updateAs<
		Alias extends string,
		UQB extends k.UpdateQueryBuilder<any, any, any, InputWithDefaultKey>,
	>(
		alias: Alias,
		query: UpdateQueryBuilderOrFactory<DB, UQB>,
	): InitialQuerySet<DB, Alias, InferTUpdateQuery<UQB>>;
	updateAs<Alias extends string, UQB extends AnyUpdateQueryBuilder>(
		alias: Alias,
		query: UpdateQueryBuilderOrFactory<DB, UQB>,
		keyBy: KeyBy<InferO<NoInfer<UQB>>>,
	): InitialQuerySet<DB, Alias, InferTUpdateQuery<UQB>>;
	updateAs(alias: string, query: any, keyBy: KeyBy<any> = DEFAULT_KEY_BY): any {
		return this.#createQuerySet(alias, query, keyBy);
	}

	/**
	 * Like {@link insertAs}, but for a `DELETE` statement.
	 */
	deleteAs<Alias extends string, DQB extends k.DeleteQueryBuilder<any, any, InputWithDefaultKey>>(
		alias: Alias,
		query: DeleteQueryBuilderOrFactory<DB, DQB>,
	): InitialQuerySet<DB, Alias, InferTDeleteQuery<DQB>>;
	deleteAs<Alias extends string, UQB extends AnyDeleteQueryBuilder>(
		alias: Alias,
		query: DeleteQueryBuilderOrFactory<DB, UQB>,
		keyBy: KeyBy<InferO<NoInfer<UQB>>>,
	): InitialQuerySet<DB, Alias, InferTDeleteQuery<UQB>>;
	deleteAs(alias: string, query: any, keyBy: KeyBy<any> = DEFAULT_KEY_BY): any {
		return this.#createQuerySet(alias, query, keyBy);
	}

	/**
	 * Initializes a new query set with a base `SELECT` query that may contain
	 * data-modifying CTEs.
	 *
	 * Any CTEs on the provided query will be hoisted to the top level of the
	 * generated SQL, which is required by Postgres for data-modifying CTEs.
	 *
	 * This enables multi-write CTE orchestration patterns like:
	 * ```ts
	 * const result = await querySet(db)
	 *   .writeAs("updated",
	 *     (db) => db.with("updated", (qb) =>
	 *       qb.updateTable("users")
	 *         .set({ email: "new@example.com" })
	 *         .where("id", "=", 1)
	 *         .returningAll()
	 *     ),
	 *     (qc) => qc.selectFrom("updated").selectAll()
	 *   )
	 *   .executeTakeFirst();
	 * ```
	 *
	 * @param alias - The alias for the base query.
	 * @param cteFn - A callback that receives `db` and builds the CTEs, returning a query creator.
	 * @param selectFn - A callback that receives the query creator and builds the SELECT referencing CTE names.
	 * @param keyBy - The key(s) to uniquely identify rows. Defaults to `"id"`.
	 * @returns A new QuerySet.
	 */
	writeAs<
		Alias extends string,
		NewDB,
		SQB extends k.SelectQueryBuilder<any, any, InputWithDefaultKey>,
	>(
		alias: Alias,
		cteFn: (db: k.Kysely<DB>) => k.QueryCreator<NewDB>,
		selectFn: (qc: k.QueryCreator<NewDB>) => SQB,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
	writeAs<Alias extends string, NewDB, SQB extends AnySelectQueryBuilder>(
		alias: Alias,
		cteFn: (db: k.Kysely<DB>) => k.QueryCreator<NewDB>,
		selectFn: (qc: k.QueryCreator<NewDB>) => SQB,
		keyBy: KeyBy<InferO<NoInfer<SQB>>>,
	): InitialQuerySet<DB, Alias, InferTSelectQuery<SQB>>;
	writeAs(alias: string, cteFn: any, selectFn: any, keyBy?: KeyBy<any>): any {
		const qc = cteFn(this.#db);
		const baseQuery = selectFn(qc).withPlugin(stripWithPlugin);
		return new QuerySetImpl({
			db: this.#db,
			baseAlias: alias,
			baseQuery,
			keyBy: keyBy ?? DEFAULT_KEY_BY,
			hydrator: createHydrator().orderByKeys(),
			joinCollections: new Map(),
			attachCollections: new Map(),
			limit: null,
			offset: null,
			orderBy: [],
			orderByKeys: true,
			frontModifiers: [],
			endModifiers: [],
			writeQueryCreator: qc,
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
 *   .selectAs("user", (eb) => eb.selectFrom("users").select(["id", "username", "email"]))
 *   .leftJoinMany("posts", ({ eb, qs }) =>
 *     qs(eb.selectFrom("posts").select(["id", "userId", "title"])),
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
