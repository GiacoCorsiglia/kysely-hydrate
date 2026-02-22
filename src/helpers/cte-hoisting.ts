import * as k from "kysely";

import { type AnySelectQueryBuilder } from "./utils.ts";

/**
 * Extracts the CTE expressions from a query builder's operation node.
 */
export function extractCTEs(
	qb: AnySelectQueryBuilder,
): readonly k.CommonTableExpressionNode[] | undefined {
	const node = qb.toOperationNode();
	return node.with?.expressions;
}

/**
 * A Kysely plugin that strips the WITH clause from a SelectQueryNode,
 * effectively removing all CTEs from the query.
 */
export class StripWithPlugin implements k.KyselyPlugin {
	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		const node = args.node;
		if (node.kind === "SelectQueryNode" && node.with) {
			const stripped: k.SelectQueryNode = { ...node, with: undefined as never };
			return stripped;
		}
		return node;
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		return args.result;
	}
}

/**
 * A Kysely plugin that prepends CTE expressions to the outer query's WithNode.
 */
export class AddCTEsPlugin implements k.KyselyPlugin {
	readonly #expressions: readonly k.CommonTableExpressionNode[];

	constructor(expressions: readonly k.CommonTableExpressionNode[]) {
		this.#expressions = expressions;
	}

	transformQuery(args: k.PluginTransformQueryArgs): k.RootOperationNode {
		const node = args.node;
		if (node.kind !== "SelectQueryNode") {
			return node;
		}

		const existing = node.with?.expressions ?? [];
		const merged = [...this.#expressions, ...existing];

		// Build the WithNode by creating with the first expression, then adding the rest.
		let withNode = k.WithNode.create(merged[0]!);
		for (let i = 1; i < merged.length; i++) {
			withNode = k.WithNode.cloneWithExpression(withNode, merged[i]!);
		}

		const result: k.SelectQueryNode = { ...node, with: withNode };
		return result;
	}

	async transformResult(args: k.PluginTransformResultArgs): Promise<k.QueryResult<k.UnknownRow>> {
		return args.result;
	}
}
