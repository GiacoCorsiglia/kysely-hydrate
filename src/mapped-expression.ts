import * as k from "kysely";

type MapFn = (input: any) => unknown;

const mappedNodes = new WeakMap<k.OperationNode, MapFn>();

/**
 * Returns the global WeakMap that tracks map functions associated with operation nodes.
 * This is used by the HydratePlugin to apply transformations during query execution.
 */
export function getMappedNodes(): WeakMap<k.OperationNode, MapFn> {
	return mappedNodes;
}

/**
 * Wraps an expression with a map function that will be applied to the result value.
 * The map function is executed after any fromDriver transformations.
 *
 * @example
 * db.selectFrom('events')
 *   .select(eb => [
 *     map(eb.ref('created_at'), (d: Date) => d.toISOString()).as('created_iso')
 *   ])
 */
export function map<Input, Output>(
	expression: k.AliasableExpression<Input>,
	mapFn: (input: NoInfer<Input>) => Output,
) {
	return new MappedExpression(expression, mapFn);
}

class MappedExpression<Input, Output> implements k.AliasableExpression<Output> {
	#inner: k.AliasableExpression<Input>;
	#mapFn: (input: Input) => Output;

	constructor(inner: k.AliasableExpression<Input>, mapFn: (input: Input) => Output) {
		this.#inner = inner;
		this.#mapFn = mapFn;
	}

	get expressionType(): Output | undefined {
		return undefined;
	}

	as<A extends string>(alias: A): k.AliasedExpression<Output, A> {
		return new k.AliasedExpressionWrapper(this, alias);
	}

	toOperationNode(): k.OperationNode {
		const node = this.#inner.toOperationNode();

		// Check if the inner expression already has a map function registered.
		// If so, compose them: outerFn(innerFn(x))
		const existingMapFn = mappedNodes.get(node);

		if (existingMapFn) {
			// Compose: apply inner map first, then outer map
			const composedMapFn = (x: any) => this.#mapFn(existingMapFn(x) as Input);
			mappedNodes.set(node, composedMapFn);
		} else {
			// No existing map, just register this one
			mappedNodes.set(node, this.#mapFn);
		}

		return node;
	}
}
