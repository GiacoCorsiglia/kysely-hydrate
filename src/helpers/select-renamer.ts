import * as k from "kysely";
import { type ApplyPrefix, applyPrefix } from "./prefixes.ts";
import { assertNever } from "./utils.ts";

const fakeQb = k.createSelectQueryBuilder({
	queryId: k.createQueryId(),
	queryNode: k.SelectQueryNode.create(),
	executor: k.NOOP_QUERY_EXECUTOR,
});

/**
 * Applies a prefix to the argument passed to a query builder's `qb.select()` call.
 */
export function prefixSelectArg(
	prefix: string,
	selection: k.SelectArg<any, any, k.SelectExpression<any, any>>,
): PrefixedAliasedExpression<any, any, any>[] {
	// This is pretty hilarious.  We use a separate query builder to parse the
	// selectArg into operation nodes, then we iterate over those nodes to rename
	// them, and then we convert them into expression objects that wrap the nodes,
	// so we can subsequently pass them back to the real query builder.
	const selectQueryNode = fakeQb.select(selection as any).toOperationNode();

	const prefixedSelections = selectQueryNode.selections?.map((selectionNode) =>
		prefixSelectionNode(selectionNode, prefix),
	);

	return (prefixedSelections ?? []) satisfies k.SelectArg<
		any,
		any,
		k.SelectExpression<any, any>
	>;
}

class PrefixedAliasedExpression<
	T,
	Prefix extends string,
	OriginalName extends string,
> extends k.AliasedExpressionWrapper<T, ApplyPrefix<Prefix, OriginalName>> {
	readonly originalName: string;

	constructor(
		node: k.OperationNode,
		prefix: Prefix,
		originalName: OriginalName,
	) {
		const alias = applyPrefix(prefix, originalName);
		// We have to gin up a new expression.
		super(new k.ExpressionWrapper(node), alias);
		this.originalName = originalName;
	}
}

function prefixSelectionNode(
	selectionNode: k.SelectionNode,
	prefix: string,
): PrefixedAliasedExpression<any, any, any> {
	const { selection } = selectionNode;

	if (k.ColumnNode.is(selection)) {
		return new PrefixedAliasedExpression(
			selection,
			prefix,
			selection.column.name,
		);
	}

	if (k.ReferenceNode.is(selection)) {
		const { column } = selection;

		if (k.SelectAllNode.is(column)) {
			throw new Error("selectAll not supported in nested relational queries");
		}

		return new PrefixedAliasedExpression(selection, prefix, column.column.name);
	}

	if (k.AliasNode.is(selection)) {
		const alias = selection.alias;

		if (!k.IdentifierNode.is(alias)) {
			throw new Error(
				"Complex aliases not supported in nested relational queries (expected IdentifierNode)",
			);
		}

		return new PrefixedAliasedExpression(selection.node, prefix, alias.name);
	}

	if (k.SelectAllNode.is(selection)) {
		throw new Error("selectAll not supported in nested relational queries");
	}

	assertNever(selection);
}
