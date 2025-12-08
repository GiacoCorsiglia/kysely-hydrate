import type * as k from "kysely";
import { jsonArrayFrom } from "kysely/helpers/postgres";

import { type Prettify } from "./helpers/utils.ts";

/**
 * Like {@link k.SelectExpression} but excludes the possibility of aliasing or a
 * dynamic select.
 */
type SelectorSelectExpression<DB, TB extends keyof DB> =
	| k.AnyColumn<DB, TB>
	| k.AnyColumnWithTable<DB, TB>
	| k.AliasableExpression<any>
	| ((eb: k.ExpressionBuilder<DB, TB>) => k.AliasableExpression<any>);

type Hydrator<T> = (rawValue: string) => T;

interface CompleteSelector<DB, TB extends keyof DB, T> {
	select: SelectorSelectExpression<DB, TB>;
	hydrate: Hydrator<T>;
}

type Selector<DB, TB extends keyof DB, T> = Hydrator<T> | CompleteSelector<DB, TB, T>;

type Selection<DB, TB extends keyof DB> = Record<string, Selector<DB, TB, unknown>>;

type InferSelectorOutput<T> = T extends Selector<any, any, infer U> ? U : never;

type InferSelectionOutput<T extends Selection<any, any>> = {
	[K in keyof T]: InferSelectorOutput<T[K]>;
};

class NestBuilder<DB, TB extends keyof DB> {
	#qb: k.SelectQueryBuilder<DB, TB, {}>;

	constructor(qb: k.SelectQueryBuilder<DB, TB, {}>) {
		// Selections must be declared using the `select` method on this class
		// instead of on the SelectQueryBuilder.
		this.#qb = qb.clearSelect();
	}

	select<S extends Selection<DB, TB>>(selection: S): NestExpression<DB, TB, S> {
		return new NestExpression(this.#qb, selection);
	}
}

////////////////////////////////////////////////////////////////////
// Expression.
////////////////////////////////////////////////////////////////////

export type { NestExpression };
class NestExpression<
	DB,
	TB extends keyof DB,
	S extends Selection<DB, TB>,
> implements k.AliasableExpression<Prettify<InferSelectionOutput<S>>> {
	#expression: k.AliasableExpression<InferSelectionOutput<S>>;
	#selection: S;

	constructor(qb: k.SelectQueryBuilder<DB, TB, {}>, selection: S) {
		this.#selection = selection;

		const subquery = qb.select((eb) =>
			Object.entries(selection).map(([key, selector]) => {
				if (typeof selector === "function") {
					return eb.ref(key as k.StringReference<DB, TB>).as(key);
				} else if (typeof selector.select === "string") {
					return eb.ref(selector.select).as(key);
				} else if (typeof selector.select === "function") {
					return selector.select(eb).as(key);
				}

				return selector.select.as(key);
			}),
		);

		this.#expression = jsonArrayFrom(subquery) as k.AliasableExpression<InferSelectionOutput<S>>;
	}

	get selection() {
		return this.#selection;
	}

	get expressionType(): Prettify<InferSelectionOutput<S>> | undefined {
		return undefined;
	}

	as<A extends string>(alias: A | k.Expression<unknown>): AliasedNestExpression<DB, TB, S, A> {
		return new AliasedNestExpression(this, alias);
	}

	toOperationNode(): k.OperationNode {
		return this.#expression.toOperationNode();
	}

	get underlyingExpression() {
		return this.#expression;
	}
}

class AliasedNestExpression<
	DB,
	TB extends keyof DB,
	S extends Selection<DB, TB>,
	A extends string = never,
> implements k.AliasedExpression<Prettify<InferSelectionOutput<S>>, A> {
	readonly nestExpression: NestExpression<DB, TB, S>;
	#alias: A | k.Expression<unknown>;

	constructor(nestExpression: NestExpression<DB, TB, S>, alias: A | k.Expression<unknown>) {
		this.nestExpression = nestExpression;
		this.#alias = alias;
	}

	get expression(): NestExpression<DB, TB, S> {
		return this.nestExpression;
	}

	get alias(): A | k.Expression<unknown> {
		return this.#alias;
	}

	toOperationNode(): k.AliasNode {
		return this.nestExpression.underlyingExpression.as(this.#alias as any).toOperationNode();
	}
}

////////////////////////////////////////////////////////////////////
// Constructor.
////////////////////////////////////////////////////////////////////

export function nestMany<DB, TB extends keyof DB>(qb: k.SelectQueryBuilder<DB, TB, {}>) {
	return new NestBuilder(qb);
}

export function isNestExpression(obj: unknown): obj is NestExpression<any, any, any> {
	return !!obj && obj instanceof NestExpression;
}

////////////////////////////////////////////////////////
// EXAMPLES
////////////////////////////////////////////////////////

interface MyDb {
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
}

declare const myDb: k.Kysely<MyDb>;

const _nested = myDb.selectFrom("posts").select((eb) => [
	"posts.id",
	nestMany(eb.selectFrom("authors").limit(1))
		.select({
			id: Number,
		})
		.as("author"),
]);
