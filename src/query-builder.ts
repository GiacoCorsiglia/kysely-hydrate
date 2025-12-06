import * as k from "kysely";

import { type MakePrefix, makePrefix } from "./helpers/prefixes.ts";
import { prefixSelectArg } from "./helpers/select-renamer.ts";
import type { ApplyPrefixes, Extend, KeyBy } from "./helpers/utils.ts";
import { createHydratable, type Hydratable } from "./hydratable.ts";

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
 * @template HydratedRow The final, local output shape of each row, after maps
 * have been applied.  Ignores parent queries.
 */
interface NestableQueryBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
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
	 * Allows you to modify the underlying select query.  Useful for adding `where` clauses.
	 *
	 * ### Examples
	 *
	 * ```ts
	 * mappedQuery.modify((qb) => qb.where("is_active", "=", "true"))
	 * ```
	 */
	modify<
		NewQueryDB,
		NewQueryTB extends keyof NewQueryDB,
		// Enforce that you only expand the output shape.  Otherwise mappers will fail!
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
		Extend<NewQueryRow, HydratedRow>
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
		errorConstructor?:
			| k.NoResultErrorConstructor
			| ((node: k.QueryNode) => Error),
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
				QueryDB,
				false
			>,
		) => NestableQueryBuilder<
			MakePrefix<Prefix, NoInfer<K>>,
			JoinedQueryDB,
			JoinedQueryTB,
			JoinedQueryRow,
			NestedLocalRow,
			NestedHydratedRow
		>,
		keyBy: KeyBy<NestedHydratedRow>,
	): NestableQueryBuilder<
		Prefix,
		JoinedQueryDB,
		JoinedQueryTB,
		JoinedQueryRow,
		LocalRow & ApplyPrefixes<MakePrefix<Prefix, K>, NestedLocalRow>,
		Extend<HydratedRow, { [_ in K]: NestedHydratedRow[] }>
	>;

	// TODO: Make { [_ in K]: NestedO | null } for left joins.
	// joinOne() (or joinFirst())
}

/**
 * A builder for nested joins that is itself also nestable.
 *
 * @template Prefix See {@link NestableQueryBuilder}.
 * @template QueryDB See {@link MappableQueryBuilder}.
 * @template QueryTB See {@link MappableQueryBuilder}.
 * @template QueryRow See {@link MappableQueryBuilder}.
 * @template LocalRow See {@link MappableQueryBuilder}.
 * @template HydratedRow See {@link MappableQueryBuilder}.
 */
interface NestedJoinBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryRow,
	LocalRow,
	HydratedRow,
	NestedDB,
	HasJoin extends boolean,
> extends NestableQueryBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryRow,
		LocalRow,
		HydratedRow
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
		TE
	>;

	/**
	 * Adds a select statement to the query.
	 *
	 * Like Kysely's {@link k.SelectQueryBuilder.select} method, but aliases (or re-aliases) selected columns by prefixing them
	 * with the join key specified when the NestedJoinBuilder was instantiated.
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
	TE extends k.TableExpression<QueryDB, QueryTB>,
> = k.SelectQueryBuilderWithInnerJoin<
	QueryDB,
	QueryTB,
	QueryRow,
	TE
> extends k.SelectQueryBuilder<infer JoinedDB, infer JoinedTB, infer JoinedRow>
	? NestedJoinBuilder<
			Prefix,
			JoinedDB,
			JoinedTB,
			JoinedRow,
			LocalRow,
			HydratedRow,
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
	TE extends k.TableExpression<QueryDB, QueryTB>,
> = k.SelectQueryBuilderWithLeftJoin<
	QueryDB,
	QueryTB,
	QueryRow,
	TE
> extends k.SelectQueryBuilder<infer JoinedDB, infer JoinedTB, infer JoinedRow>
	? NestedJoinBuilder<
			Prefix,
			JoinedDB,
			JoinedTB,
			JoinedRow,
			LocalRow,
			HydratedRow,
			// If the nested join builder does not have a join yet, we can treat the join as an inner join
			// when considered from inside the nested join.
			AlreadyHadJoin extends true
				? JoinedDB
				: InferDB<
						k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryRow, TE>
					>,
			true
		>
	: never;

////////////////////////////////////////////////////////////////////
// Implementation.
////////////////////////////////////////////////////////////////////

type AnySelectQueryBuilder = k.SelectQueryBuilder<any, any, any>;
type AnyNestableQueryBuilder = NestableQueryBuilder<
	any,
	any,
	any,
	any,
	any,
	any
>;
type AnyNestedJoinBuilder = NestedJoinBuilder<
	any,
	any,
	any,
	any,
	any,
	any,
	any,
	any
>;

interface NestedJoinBuilderProps {
	readonly qb: AnySelectQueryBuilder;
	readonly prefix: string;
	readonly hydratable: Hydratable<any, any>;
}

/**
 * This is a shared implementation of the entire inheritance chain of builders:
 *
 * - {@link MappableQueryBuilder}
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

	// map(mb: AnyMapper | ((mb: Mapper<any, any, never>) => AnyMapper)): any {
	// 	const theMapper = mb instanceof Function ? mb(mapper()) : mb;
	// 	const mappers = this.#props.mappers?.concat(theMapper) ?? [theMapper];
	// 	return new NestedJoinBuilderImpl({
	// 		...this.#props,
	// 		mappers,
	// 	});
	// }

	modify(modifier: (qb: AnySelectQueryBuilder) => AnySelectQueryBuilder): any {
		return new NestedJoinBuilderImpl({
			...this.#props,
			qb: modifier(this.#props.qb),
		});
	}

	toQuery(): AnySelectQueryBuilder {
		return this.#props.qb;
	}

	async execute(): Promise<any[]> {
		const rows = await this.#props.qb.execute();

		return this.#props.hydratable.hydrate(rows);
	}

	async executeTakeFirst(): Promise<any | undefined> {
		const result = await this.#props.qb.executeTakeFirst();

		return result === undefined
			? undefined
			: this.#props.hydratable.hydrate(result);
	}

	async executeTakeFirstOrThrow(
		errorConstructor:
			| k.NoResultErrorConstructor
			| ((node: k.QueryNode) => Error) = k.NoResultError,
	): Promise<any> {
		const result =
			await this.#props.qb.executeTakeFirstOrThrow(errorConstructor);

		return this.#props.hydratable.hydrate(result);
	}

	joinMany(
		key: string,
		jb: (nb: AnyNestedJoinBuilder) => NestedJoinBuilderImpl,
		keyBy: any,
	): any {
		const inputNb = new NestedJoinBuilderImpl({
			qb: this.#props.qb,
			prefix: makePrefix(this.#props.prefix, key),

			hydratable: createHydratable<any>(keyBy),
		});
		const outputNb = jb(inputNb);

		return new NestedJoinBuilderImpl({
			...this.#props,

			qb: outputNb.#props.qb,

			hydratable: this.#props.hydratable.hasMany(
				key,
				// Hydratables do their own job of handling nested prefixes...I think this will work?
				makePrefix("", key),
				outputNb.#props.hydratable,
			),
		});
	}

	//
	// NestedJoinBuilder methods.
	//

	select(selection: k.SelectArg<any, any, any>): any {
		const prefixedSelections = prefixSelectArg(this.#props.prefix, selection);

		return new NestedJoinBuilderImpl({
			...this.#props,

			// This cast to `any` is needed because TS can't follow the overload.
			qb: this.#props.qb.select(prefixedSelections as any),

			hydratable: this.#props.hydratable.fields(
				Object.fromEntries(
					prefixedSelections.map((selection) => [
						selection.originalName,
						true as const,
					]),
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

// export function hydrated<QueryDB, QueryTB extends keyof QueryDB, QueryRow>(
// 	qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
// ): MappableQueryBuilder<QueryDB, QueryTB, QueryRow, QueryRow, QueryRow>;
export function hydrated<QueryDB, QueryTB extends keyof QueryDB, QueryRow>(
	qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryRow>,
	keyBy: KeyBy<QueryRow>,
): NestableQueryBuilder<"", QueryDB, QueryTB, QueryRow, QueryRow, QueryRow>;
export function hydrated(
	qb: k.SelectQueryBuilder<any, any, any>,
	keyBy?: any,
): any {
	return new NestedJoinBuilderImpl({
		qb,
		prefix: "",

		hydratable: createHydratable<any>(keyBy),
	});
}
