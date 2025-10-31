/** biome-ignore-all lint/complexity/noBannedTypes: Magic afoot. */
/** biome-ignore-all lint/suspicious/noExplicitAny: Magic afoot. */
import type * as k from "kysely";

import type {
	AddOrOverride,
	ApplyPrefix,
	Extend,
	KeyBy,
	Override,
} from "./helpers";

declare function mapper<Input>(): Mapper<Input, Input>;

type MappedOutput<V, Output, Omitted extends PropertyKey> = Omit<
	Extend<V, Output>,
	Omitted
>;

interface Mapper<
	// biome-ignore lint/suspicious/noTsIgnore: TODO: Do we need to cast this variance?
	// @ts-ignore cast variance
	in Input,
	in out Output,
	Omitted extends PropertyKey = never,
> {
	apply<V extends Input>(input: V): MappedOutput<V, Output, Omitted>;

	map<K extends keyof Input, V>(
		key: K,
		transform: (value: Input[K]) => V,
	): Mapper<Input, Override<Output, K & keyof Output, V>>;

	add<K extends PropertyKey, V>(
		key: K,
		generate: (row: Input) => V,
	): Mapper<Input, AddOrOverride<Output, K, V>>;

	omit<K extends keyof Output>(key: K): Mapper<Input, Output, Omitted | K>;
	omit<K extends keyof Output>(keys: K[]): Mapper<Input, Output, Omitted | K>;
}

interface MappedQueryBuilder<
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
	MappedO,
> {
	// TODO: The mapper needs to somehow just receive the QueryO from *this level*.
	map<MapperOutput, MapperOmitted extends PropertyKey>(
		mapper: Mapper<QueryO, MapperOutput, MapperOmitted>,
	): MappedQueryBuilder<
		QueryDB,
		QueryTB,
		QueryO,
		MappedOutput<MappedO, MapperOutput, MapperOmitted>
	>;
	map<MapperOutput, MapperOmitted extends PropertyKey>(
		mapper: (
			mb: Mapper<QueryO, MappedO>,
		) => Mapper<QueryO, MapperOutput, MapperOmitted>,
	): MappedQueryBuilder<
		QueryDB,
		QueryTB,
		QueryO,
		MappedOutput<MappedO, MapperOutput, MapperOmitted>
	>;

	modify<
		NewQueryDB,
		NewQueryTB extends keyof NewQueryDB,
		// Enforce that you only expand the output shape.  Otherwise mappers will fail!
		NewQueryO extends QueryO,
	>(
		modifier: (
			qb: k.SelectQueryBuilder<QueryDB, QueryTB, QueryO>,
		) => k.SelectQueryBuilder<NewQueryDB, NewQueryTB, NewQueryO>,
	): MappedQueryBuilder<
		NewQueryDB,
		NewQueryTB,
		NewQueryO,
		Extend<NewQueryO, MappedO>
	>;

	toQuery(): k.SelectQueryBuilder<QueryDB, QueryTB, QueryO>;

	execute(): Promise<k.Simplify<MappedO>[]>;
	executeTakeFirst(): Promise<k.Simplify<MappedO> | undefined>;
	executeTakeFirstOrThrow(): Promise<k.Simplify<MappedO>>;
}

// This is a laziness so that we can reuse the k.SelectQueryBuilderWith*Join types.
type InferDB<SQB extends k.SelectQueryBuilder<any, any, any>> =
	SQB extends k.SelectQueryBuilder<infer DB, any, any> ? DB : never;

type NestedJoinBuilderWithInnerJoin<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
	NestedO,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> = k.SelectQueryBuilderWithInnerJoin<
	QueryDB,
	QueryTB,
	QueryO,
	TE
> extends k.SelectQueryBuilder<infer JoinedDB, infer JoinedTB, infer JoinedO>
	? NestedJoinBuilder<
			Prefix,
			JoinedDB,
			JoinedTB,
			JoinedO,
			NestedO,
			JoinedDB,
			true
		>
	: never;

type NestedJoinBuilderWithLeftJoin<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
	NestedO,
	AlreadyHadJoin extends boolean,
	TE extends k.TableExpression<QueryDB, QueryTB>,
> = k.SelectQueryBuilderWithLeftJoin<
	QueryDB,
	QueryTB,
	QueryO,
	TE
> extends k.SelectQueryBuilder<infer JoinedDB, infer JoinedTB, infer JoinedO>
	? NestedJoinBuilder<
			Prefix,
			JoinedDB,
			JoinedTB,
			JoinedO,
			NestedO,
			// If the nested join builder does not have a join yet, we can treat the join as an inner join
			// when considered from inside the nested join.
			AlreadyHadJoin extends true
				? JoinedDB
				: InferDB<
						k.SelectQueryBuilderWithInnerJoin<QueryDB, QueryTB, QueryO, TE>
					>,
			true
		>
	: never;

interface NestedJoinBuilder<
	Prefix extends string,
	// These are the *true* query generics, with prefixed column names.
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
	// This is the unprefixed output shape of the nested query.
	NestedO,
	// This is the shape of the DB that can be considered legit for within this nested join
	// Specifically, left joins are treated as inner joins inside here, so that all their
	// properties do not become nullable
	NestedDB,
	// Track whether this builder has at least one join.  This dictates the behavior of the leftJoin method:
	HasJoin extends boolean,
> extends NestedQueryBuilder<Prefix, QueryDB, QueryTB, QueryO, NestedO> {
	// These are the only reasonable join types for nested relations.
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
		QueryO,
		NestedO,
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
		QueryO,
		NestedO,
		TE
	>;

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
		QueryO,
		NestedO,
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
		QueryO,
		NestedO,
		HasJoin,
		TE
	>;

	// TODO:
	// crossJoin(): this;
	// innerJoinLateral(): this;
	// leftJoinLateral(): this;
	// crossJoinLateral(): this;

	/**
	 * Like Kysely's `select` method, but aliases (or re-aliases) selected columns by prefixing them
	 * with join key you have chosen for this nested relation.
	 */
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selections: ReadonlyArray<SE>,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryO & ApplyPrefix<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		NestedO & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		NestedDB,
		HasJoin
	>;
	select<CB extends k.SelectCallback<QueryDB, QueryTB>>(
		callback: CB,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryO & ApplyPrefix<Prefix, k.CallbackSelection<QueryDB, QueryTB, CB>>,
		NestedO & k.CallbackSelection<NestedDB, QueryTB & keyof NestedDB, CB>,
		NestedDB,
		HasJoin
	>;
	select<SE extends k.SelectExpression<QueryDB, QueryTB>>(
		selection: SE,
	): NestedJoinBuilder<
		Prefix,
		QueryDB,
		QueryTB,
		QueryO & ApplyPrefix<Prefix, k.Selection<QueryDB, QueryTB, SE>>,
		NestedO & k.Selection<NestedDB, QueryTB & keyof NestedDB, SE>,
		NestedDB,
		HasJoin
	>;
}

interface NestedQueryBuilder<
	Prefix extends string,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
	MappedO,
> extends MappedQueryBuilder<QueryDB, QueryTB, QueryO, MappedO> {
	joinMany<
		K extends string,
		NestedQueryDB,
		NestedQueryTB extends keyof NestedQueryDB,
		NestedQueryO,
		NestedO,
	>(
		key: K,
		jb: (
			nb: NestedJoinBuilder<
				`${Prefix}${K}$`,
				QueryDB,
				QueryTB,
				QueryO,
				{},
				QueryDB,
				false
			>,
		) => MappedQueryBuilder<
			NestedQueryDB,
			NestedQueryTB,
			NestedQueryO,
			NestedO
		>,
		keyBy: KeyBy<NestedO>,
	): NestedQueryBuilder<
		`${Prefix}${K}$`,
		NestedQueryDB,
		NestedQueryTB,
		NestedQueryO,
		Extend<MappedO, { [_ in K]: NestedO[] }>
	>;

	// TODO: Make { [_ in K]: NestedO |null } for left joins.
	// joinOne() (or joinFirst())
}

type QueryFactory<DBObject, QueryDB, QueryTB extends keyof QueryDB, QueryO> = (
	db: DBObject,
) => k.SelectQueryBuilder<QueryDB, QueryTB, QueryO>;

export declare function nested<
	DB,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
>(
	db: k.Kysely<DB>,
	queryFactory: QueryFactory<k.Kysely<DB>, QueryDB, QueryTB, QueryO>,
): MappedQueryBuilder<QueryDB, QueryTB, QueryO, QueryO>;
export declare function nested<
	DB,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
>(
	db: k.Transaction<DB>,
	queryFactory: QueryFactory<k.Transaction<DB>, QueryDB, QueryTB, QueryO>,
): MappedQueryBuilder<QueryDB, QueryTB, QueryO, QueryO>;
export declare function nested<
	DB,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
>(
	db: k.Kysely<DB>,
	queryFactory: QueryFactory<k.Kysely<DB>, QueryDB, QueryTB, QueryO>,
	keyBy: KeyBy<QueryO>,
): NestedQueryBuilder<"", QueryDB, QueryTB, QueryO, QueryO>;
export declare function nested<
	DB,
	QueryDB,
	QueryTB extends keyof QueryDB,
	QueryO,
>(
	db: k.Transaction<DB>,
	queryFactory: QueryFactory<k.Transaction<DB>, QueryDB, QueryTB, QueryO>,
	keyBy: KeyBy<QueryO>,
): NestedQueryBuilder<"", QueryDB, QueryTB, QueryO, QueryO>;

declare const myDb: k.Kysely<{
	users: {
		id: number;
		name: string;
		field: number;
	};

	posts: {
		id: number;
		user_id: number;
		title: string;
		content: string;
	};

	authors: {
		id: number;
		post_id: number;
		name: string;
		bio: string;
	};
}>;

const query = nested(
	myDb,
	(db) => db.selectFrom("users").select(["users.id", "users.name"]),
	"id",
)
	.joinMany(
		"posts",

		({ leftJoin }) =>
			leftJoin("posts", "posts.user_id", "users.id")
				.select(["posts.id", "posts.title"])
				.leftJoin("authors", "authors.id", "posts.id")
				.select(["authors.name"])

				.joinMany(
					"authors",

					({ leftJoin }) =>
						leftJoin("authors", "authors.id", "posts.id").select([
							"authors.id",
						]),

					"id",
				),

		"id",
	)
	// .query((qb) => qb.select(["users.field"]))
	.map((mb) => mb.add("displayName", (row) => `${row} (#${row.id})`));

const qr = await query.toQuery().execute();
const result = await query.execute();
