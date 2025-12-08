import * as k from "kysely";

import {
	type ApplyPrefixes,
	hasAnyPrefix,
	type MakePrefix,
	makePrefix,
} from "./helpers/prefixes.ts";
import { prefixSelectArg } from "./helpers/select-renamer.ts";
import { type Extend, type KeyBy } from "./helpers/utils.ts";
import {
	type CollectionMode,
	type FetchFn,
	createHydratable,
	type Hydratable,
} from "./hydratable.ts";

////////////////////////////////////////////////////////////////////
// Interfaces.
////////////////////////////////////////////////////////////////////

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
 * @template LocalRow The unprefixed row shape that `query.execute()` *would*
 * return if this was the topmost query, ignoring parent queries.
 * @template HydratedRow The final, local output shape of each row, after joins
 * have been applied.  Ignores parent queries.
 * @template IsNullable Whether the hydrated row resulting from this join should
 * be nullable in its parent.
 */
interface NestableQueryBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
	IsNullable extends boolean,
> {
	/**
	 * @internal This is is a fake method that does nothing and is only for
	 * testing types.  The callback will never actually be called.
	 */
	_generics(
		cb: (args: {
			Prefix: Prefix;
			QueryDB: QueryDB;
			QueryTB: QueryTB;
			QueryRow: QueryRow;
			LocalRow: LocalRow;
			HydratedRow: HydratedRow;
		}) => void,
	): this;

	/**
	 * Allows you to modify the underlying select query.  Useful for adding
	 * `where` clauses.  Adding additional SELECTs here is discouraged.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * nestableQuery.modify((qb) => qb.where("isActive", "=", "true"))
	 * ```
	 */
	modify<
		NewQueryDB,
		NewQueryTB extends keyof NewQueryDB,
		// Enforce that you only expand the output shape.  Otherwise joins will fail!
		NewQueryRow extends QueryRow,
	>(
		modifier: (
			qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
		) => k.SelectQueryBuilder<NewQueryDB, NewQueryTB, NewQueryRow>,
	): NestableQueryBuilder<
		Prefix,
		NewQueryDB,
		NewQueryTB,
		NewQueryRow,
		// Not modifying the local row because that would require un-prefixing.  We
		// generally do not expect modifications to the row shape here anyway.
		LocalRow,
		// TODO: This extension might be wrong!
		Extend<NewQueryRow, HydratedRow>,
		IsNullable
	>;

	/**
	 * Returns the raw underlying select query.
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

	joinMany<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalRow,
		NestedHydratedRow,
	>(
		key: K,
		jb: (
			nb: NestedJoinBuilder<
				MakePrefix<Prefix, NoInfer<K>>,
				QueryDB,
				QueryTB,
				QueryRow,
				{}, // LocalRow is empty within the nesting.
				{}, // HydratedRow is empty within the nesting.
				false,
				QueryDB,
				false
			>,
		) => NestableQueryBuilder<
			MakePrefix<Prefix, NoInfer<K>>,
			JoinedQueryDB,
			JoinedQueryTB,
			JoinedQueryRow,
			NestedLocalRow,
			NestedHydratedRow,
			false
		>,
		keyBy: KeyBy<NestedHydratedRow>,
	): NestableQueryBuilder<
		Prefix,
		JoinedQueryDB,
		JoinedQueryTB,
		JoinedQueryRow,
		LocalRow & ApplyPrefixes<MakePrefix<Prefix, K>, NestedLocalRow>,
		Extend<HydratedRow, { [_ in K]: NestedHydratedRow[] }>,
		IsNullable
	>;

	joinOne<
		K extends string,
		JoinedQueryDB,
		JoinedQueryTB extends keyof JoinedQueryDB,
		JoinedQueryRow,
		NestedLocalRow,
		NestedHydratedRow,
		IsChildNullable extends boolean,
	>(
		key: K,
		jb: (
			nb: NestedJoinBuilder<
				MakePrefix<Prefix, NoInfer<K>>,
				QueryDB,
				QueryTB,
				QueryRow,
				{}, // LocalRow is empty within the nesting.
				{}, // HydratedRow is empty within the nesting.
				false,
				QueryDB,
				false
			>,
		) => NestableQueryBuilder<
			MakePrefix<Prefix, NoInfer<K>>,
			JoinedQueryDB,
			JoinedQueryTB,
			JoinedQueryRow,
			NestedLocalRow,
			NestedHydratedRow,
			IsChildNullable
		>,
		keyBy: KeyBy<NestedHydratedRow>,
	): NestableQueryBuilder<
		Prefix,
		JoinedQueryDB,
		JoinedQueryTB,
		JoinedQueryRow,
		LocalRow & ApplyPrefixes<MakePrefix<Prefix, K>, NestedLocalRow>,
		Extend<
			HydratedRow,
			{
				[_ in K]: IsChildNullable extends true ? NestedHydratedRow | null : NestedHydratedRow;
			}
		>,
		IsNullable
	>;

	attachMany<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		matchKey: KeyBy<AttachedOutput>,
	): NestableQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		Extend<HydratedRow, { [_ in K]: AttachedOutput[] }>,
		IsNullable
	>;

	attachOne<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		matchKey: KeyBy<AttachedOutput>,
	): NestableQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		Extend<HydratedRow, { [_ in K]: AttachedOutput | null }>,
		IsNullable
	>;

	attachOneOrThrow<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<LocalRow, AttachedOutput>,
		matchKey: KeyBy<AttachedOutput>,
	): NestableQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		Extend<HydratedRow, { [_ in K]: AttachedOutput }>,
		IsNullable
	>;
}

/**
 * A builder for nested joins that is itself also nestable.
 *
 * @template Prefix See {@link NestableQueryBuilder}.
 * @template QueryDB See {@link NestableQueryBuilder}.
 * @template QueryTB See {@link NestableQueryBuilder}.
 * @template QueryRow See {@link NestableQueryBuilder}.
 * @template LocalRow See {@link NestableQueryBuilder}.
 * @template HydratedRow See {@link NestableQueryBuilder}.
 * @template IsNullable See {@link NestableQueryBuilder}.
 * @template NestedDB A `DB` generic for the select query that suppresses
 * nullability of left joins so that the type of nested objects is correctly
 * non-nullable.
 * @template HasJoin Preserves whether this join builder has already had a join
 * added, which affects the nullability of this relation when adding more joins.
 */
interface NestedJoinBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
	IsNullable extends boolean,
	NestedDB,
	HasJoin extends boolean,
> extends NestableQueryBuilder<
	Prefix,
	QueryDB,
	QueryTB,
	QueryRow,
	LocalRow,
	HydratedRow,
	IsNullable
> {
	/**
	 * @internal This is is a fake method that does nothing and is only for
	 * testing types.  The callback will never actually be called.
	 */
	_generics(
		cb: (args: {
			Prefix: Prefix;
			QueryDB: QueryDB;
			QueryTB: QueryTB;
			QueryRow: QueryRow;
			LocalRow: LocalRow;
			HydratedRow: HydratedRow;
			NestedDB: NestedDB;
			HasJoin: HasJoin;
		}) => void,
	): this;

	// We omit RIGHT JOIN and FULL JOIN because these are not appropriate for ORM-style queries.

	/**
	 * Joins another table to the query using an `inner join`.
	 *
	 * Exactly like Kysely's {@link k.SelectQueryBuilder.innerJoin}, except contextualized to a
	 * {@link NestableQueryBuilder}.  This method will add an `inner join` to your SQL in exactly the same
	 * way as Kysely's version.
	 */
	innerJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;
	innerJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Like {@link innerJoin}, but adds a `left join` instead of an `inner join`.
	 *
	 * TODO: Document how the types vary from inner join.
	 */
	leftJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		HasJoin,
		IsNullable,
		TE
	>;
	leftJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): NestedJoinBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		HasJoin,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link innerJoin}, but adds a `cross join` instead of an `inner join`.
	 */
	crossJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;
	crossJoin<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link innerJoin} but adds an `inner join lateral` instead of an `inner join`.
	 */
	innerJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
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
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link leftJoin} but adds a `left join lateral` instead of a `left join`.
	 */
	leftJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		HasJoin,
		IsNullable,
		TE
	>;
	leftJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): NestedJoinBuilderWithLeftJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		HasJoin,
		IsNullable,
		TE
	>;

	/**
	 * Just like {@link innerJoin} but adds a `cross join lateral` instead of an `inner join`.
	 */
	crossJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		K1 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
		K2 extends k.JoinReferenceExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		k1: K1,
		k2: K2,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;
	crossJoinLateral<
		TE extends k.TableExpression<QueryDB, QueryTB>,
		FN extends k.JoinCallbackExpression<QueryDB, QueryTB, TE>,
	>(
		table: TE,
		callback: FN,
	): NestedJoinBuilderWithInnerJoin<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow,
		IsNullable,
		TE
	>;

	/**
	 * Adds a select statement to the query.
	 *
	 * Like Kysely's {@link k.SelectQueryBuilder.select} method, but aliases (or
	 * re-aliases) selected columns by prefixing them with the join key specified
	 * when the NestedJoinBuilder was instantiated.
	 */
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selections: ReadonlyArray<SE>,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		LocalRow & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		HydratedRow & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		IsNullable,
		NestedDB,
		HasJoin
	>;
	select<CB extends k.SelectCallback<QueryDB, QueryTB>>(
		callback: CB,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.CallbackSelection<QueryDB, QueryTB, CB>>,
		LocalRow & k.CallbackSelection<NestedDB, QueryTB & keyof NestedDB, CB>,
		HydratedRow & k.CallbackSelection<NestedDB, QueryTB & keyof NestedDB, CB>,
		IsNullable,
		NestedDB,
		HasJoin
	>;
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selection: SE,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow & ApplyPrefixes<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		LocalRow & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		HydratedRow & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		IsNullable,
		NestedDB,
		HasJoin
	>;
}

// This is a laziness so that we can reuse the k.SelectQueryBuilderWith*Join types.
type InferDB<SQB extends k.SelectQueryBuilder<any, any, any>> =
	SQB extends k.SelectQueryBuilder<infer DB, any, any> ? DB : never;

type NestedJoinBuilderWithInnerJoin<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
	IsNullable extends boolean,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> =
	k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryRow, TE> extends k.SelectQueryBuilder<
		infer JoinedDB,
		infer JoinedTB,
		infer JoinedRow
	>
		? NestedJoinBuilder<
				Prefix,
				JoinedDB,
				JoinedTB,
				JoinedRow,
				LocalRow,
				HydratedRow,
				IsNullable,
				JoinedDB,
				true
			>
		: never;

type NestedJoinBuilderWithLeftJoin<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
	AlreadyHadJoin extends boolean,
	_IsNullable extends boolean,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> =
	k.SelectQueryBuilderWithLeftJoin<QueryDB, QueryTB, QueryRow, TE> extends k.SelectQueryBuilder<
		infer JoinedDB,
		infer JoinedTB,
		infer JoinedRow
	>
		? NestedJoinBuilder<
				Prefix,
				JoinedDB,
				JoinedTB,
				JoinedRow,
				LocalRow,
				HydratedRow,
				true, // Left joins always produce nullable rows.
				// If the nested join builder does not have a join yet, we can treat the
				// join as an inner join when considered from inside the nested join.
				AlreadyHadJoin extends true
					? JoinedDB
					: InferDB<k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryRow, TE>>,
				true
			>
		: never;

////////////////////////////////////////////////////////////////////
// Implementation.
////////////////////////////////////////////////////////////////////

type AnySelectQueryBuilder = k.SelectQueryBuilder<any, any, any>;

// oxlint-disable-next-line no-unused-vars
type AnyNestableQueryBuilder = NestableQueryBuilder<any, any, any, any, any, any, any>;
type AnyNestedJoinBuilder = NestedJoinBuilder<any, any, any, any, any, any, any, any, any>;

interface NestedJoinBuilderProps {
	readonly qb: AnySelectQueryBuilder;
	readonly prefix: string;
	readonly hydratable: Hydratable<any, any>;
}

/**
 * This is a shared implementation of the entire inheritance chain of builders:
 *
 * - {@link NestableQueryBuilder}
 * - {@link NestedJoinBuilder}
 *
 * The difference between those interfaces is only about controlling which
 * methods can be called where.
 */
class NestedJoinBuilderImpl implements AnyNestedJoinBuilder {
	#props: NestedJoinBuilderProps;

	constructor(props: NestedJoinBuilderProps) {
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
	// NestableQueryBuilder methods.
	//

	_generics(): this {
		return this;
	}

	modify(modifier: (qb: AnySelectQueryBuilder) => AnySelectQueryBuilder): any {
		return new NestedJoinBuilderImpl({
			...this.#props,
			qb: modifier(this.#props.qb),
		});
	}

	toQuery(): AnySelectQueryBuilder {
		return this.#props.qb;
	}

	#hydrate(rows: object[]): object[];
	#hydrate(rows: object | undefined): object | undefined;
	#hydrate(rows: object | object[] | undefined): object | object[] | undefined {
		const isArray = Array.isArray(rows);
		const firstRow = isArray ? rows[0] : rows;

		if (firstRow === undefined) {
			return isArray ? [] : undefined;
		}

		// This dance is necessary to ensure the hydrated result actually includes
		// the selected columns from the top-level select.
		const fields: Record<string, true> = Object.fromEntries(
			Object.keys(firstRow)
				// To determine if the key is from the top-level selection versus a
				// nested selection, we simply check if it includes the prefix separator.
				.filter((key) => !hasAnyPrefix(key))
				.map((key) => [key, true as const]),
		);

		const hydratableWithSelection = this.#props.hydratable.fields(fields);

		return hydratableWithSelection.hydrate(rows);
	}

	async execute(): Promise<any[]> {
		const rows = await this.#props.qb.execute();

		return this.#hydrate(rows);
	}

	async executeTakeFirst(): Promise<any | undefined> {
		const result = await this.#props.qb.executeTakeFirst();

		return result === undefined ? undefined : this.#hydrate(result);
	}

	async executeTakeFirstOrThrow(
		errorConstructor: k.NoResultErrorConstructor | ((node: k.QueryNode) => Error) = k.NoResultError,
	): Promise<any> {
		const result = await this.#props.qb.executeTakeFirstOrThrow(errorConstructor);

		return this.#hydrate(result);
	}

	#addJoin(
		mode: CollectionMode,
		key: string,
		jb: (nb: AnyNestedJoinBuilder) => NestedJoinBuilderImpl,
		keyBy: any,
	) {
		const inputNb = new NestedJoinBuilderImpl({
			qb: this.#props.qb,
			prefix: makePrefix(this.#props.prefix, key),

			hydratable: createHydratable<any>(keyBy),
		});
		const outputNb = jb(inputNb);

		return new NestedJoinBuilderImpl({
			...this.#props,

			qb: outputNb.#props.qb,

			hydratable: this.#props.hydratable.has(
				mode,
				key,
				// Hydratables do their own job of handling nested prefixes.
				makePrefix("", key),
				outputNb.#props.hydratable,
			),
		});
	}

	joinMany(key: string, jb: (nb: AnyNestedJoinBuilder) => NestedJoinBuilderImpl, keyBy: any) {
		return this.#addJoin("many", key, jb, keyBy);
	}

	joinOne(key: string, jb: (nb: AnyNestedJoinBuilder) => NestedJoinBuilderImpl, keyBy: any): any {
		return this.#addJoin("one", key, jb, keyBy);
	}

	#addAttach(mode: CollectionMode, key: string, fetchFn: FetchFn<any, any>, matchKey: KeyBy<any>) {
		return new NestedJoinBuilderImpl({
			...this.#props,
			hydratable: this.#props.hydratable.attach(mode, key, fetchFn, matchKey),
		});
	}

	attachMany(key: string, fetchFn: FetchFn<any, any>, matchKey: KeyBy<any>) {
		return this.#addAttach("many", key, fetchFn, matchKey);
	}

	attachOne(key: string, fetchFn: FetchFn<any, any>, matchKey: KeyBy<any>) {
		return this.#addAttach("one", key, fetchFn, matchKey);
	}

	attachOneOrThrow(key: string, fetchFn: FetchFn<any, any>, matchKey: KeyBy<any>) {
		return this.#addAttach("oneOrThrow", key, fetchFn, matchKey);
	}

	//
	// NestedJoinBuilder methods.
	//

	select(selection: k.SelectArg<any, any, any>) {
		const prefixedSelections = prefixSelectArg(this.#props.prefix, selection);

		return new NestedJoinBuilderImpl({
			...this.#props,

			// This cast to `any` is needed because TS can't follow the overload.
			qb: this.#props.qb.select(prefixedSelections as any),

			hydratable: this.#props.hydratable.fields(
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

export function hydrated<QueryDB, QueryTB extends keyof QueryDB, QueryRow>(
	qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
	keyBy: KeyBy<QueryRow>,
): NestableQueryBuilder<"", QueryDB, QueryTB, QueryRow, QueryRow, QueryRow, false>;
export function hydrated(qb: k.SelectQueryBuilder<any, any, any>, keyBy?: any): any {
	return new NestedJoinBuilderImpl({
		qb,
		prefix: "",

		hydratable: createHydratable<any>(keyBy),
	});
}
