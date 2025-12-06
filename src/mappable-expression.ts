import * as k from "kysely";
import { UnexpectedComplexAliasError } from "./helpers/errors.ts";

export function map<Input, Output>(
	expression: k.AliasableExpression<Input>,
	mapFn: (input: NoInfer<Input>) => Output,
) {
	return new MappedExpression(expression, mapFn);
}

class MappedExpression<Input, Output> implements k.AliasableExpression<Output> {
	#inner: k.AliasableExpression<Input>;
	#mapFn: (input: Input) => Output;

	constructor(
		inner: k.AliasableExpression<Input>,
		mapFn: (input: Input) => Output,
	) {
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
		// TODO: Compose mappers for nested MappedExpressions.
		const node = this.#inner.toOperationNode();

		console.log(node);

		// This should be safe because we expect `toOperationNode` to return a new
		// node each time it's called.
		mappedNodes.set(node, this.#mapFn);

		return node;
	}
}

type MapFn = (input: any) => unknown;

const mappedNodes = new WeakMap<k.OperationNode, MapFn>();

type RowMappers = Map<string, MapFn>;

const queryConfig = new WeakMap<k.QueryId, RowMappers>();

export class KyselyHydratePlugin implements k.KyselyPlugin {
	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		const rowMappers = new Map<string, MapFn>();
		queryConfig.set(args.queryId, rowMappers);

		const transformer = new KyselyHydrateTransformer(rowMappers);

		return transformer.transformNode(args.node);
	}

	async transformResult({
		queryId,
		result,
	}: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		const rowMappers = queryConfig.get(queryId);

		if (rowMappers === undefined || result.rows?.length === 0) {
			return result;
		}

		const mappedRows: k.UnknownRow[] = [];
		for (const row of result.rows) {
			const mappedRow: k.UnknownRow = {};

			for (const key in row) {
				if (Object.hasOwn(row, key)) {
					const value = row[key];
					const mapFn = rowMappers.get(key);
					mappedRow[key] = mapFn ? mapFn(value) : value;
				}
			}

			mappedRows.push(mappedRow);
		}

		return {
			...result,
			rows: mappedRows,
		};
	}
}

class KyselyHydrateTransformer extends k.OperationNodeTransformer {
	#cteStack: string[] = [];
	#outMappers: Map<string, (input: any) => unknown>;

	constructor(rowMappers: Map<string, (input: any) => unknown>) {
		super();
		this.#outMappers = rowMappers;
	}

	protected override transformCommonTableExpression(
		node: k.CommonTableExpressionNode,
	): k.CommonTableExpressionNode {
		this.#cteStack.push(node.name.table.table.identifier.name);
		return super.transformCommonTableExpression(node);
	}
	// protected override transformAlias(node: k.AliasNode): k.AliasNode {
	// 	// if (k.WithNode.is(node.node)) {
	// 	//   if (k.)
	// 	// 	this.#currentWithAlias = node.alias.name;
	// 	// }

	// 	return super.transformAlias(node);
	// }

	protected override transformSelection(
		node: k.SelectionNode,
	): k.SelectionNode {
		// NOTE: We must not call `super.transformSelection` here, because it will
		// clone the node, thus meaning we won't be able to find it in the weak map.

		const { selection } = node;

		// Only alias nodes are relevant here, because they're the only ones that
		// can contain a mapped expression (whether directly or recursively).
		if (!k.AliasNode.is(selection)) {
			return node;
		}

		// Let's see if we have a mapper for this selection.
		const mapFn = mappedNodes.get(selection.node);

		console.log(mappedNodes);
		// If we don't, no mapping necessary.
		if (mapFn === undefined) {
			console.log(selection.node);
			return node;
		}

		const { alias } = selection;

		// If the name is some complex expression, we can't map it.
		if (!k.IdentifierNode.is(alias)) {
			throw new UnexpectedComplexAliasError();
		}

		const { name } = alias;
		this.#outMappers.set(name, mapFn);

		// TODO: Handle nested shit.

		// Finally call the super method to continue the transformation/visitation.
		return super.transformSelection(node);
	}
}
