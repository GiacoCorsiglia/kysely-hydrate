import * as k from "kysely";

import {
	UnexpectedCaseError,
	UnexpectedComplexAliasError,
	UnexpectedSelectAllError,
} from "./errors.ts";
import { type ApplyPrefix, applyPrefix } from "./prefixes.ts";
import { type AnyQueryBuilder, type AnySelectQueryBuilder, assertNever } from "./utils.ts";

function getSelections(node: k.OperationNode): readonly k.SelectionNode[] | undefined {
	switch (node.kind) {
		case "SelectQueryNode":
			return (node as k.SelectQueryNode).selections;
		case "InsertQueryNode":
		case "DeleteQueryNode":
		case "UpdateQueryNode":
			return (node as k.InsertQueryNode | k.DeleteQueryNode | k.UpdateQueryNode).returning
				?.selections;
		default:
			throw new UnexpectedCaseError(`Unexpected query node kind: ${node.kind}`);
	}
}

/**
 * A query builder converted to its operation node once, so that a query can
 * be both joined (or selected from) and have its selections hoisted without
 * running the builder's plugin transforms twice.
 */
export interface AliasedQueryNode {
	/** The query's operation node. */
	readonly node: k.OperationNode;
	/** The node aliased, for `selectFrom` and the join methods. */
	readonly aliased: k.AliasedExpression<any, string>;
}

export function aliasQueryNode(qb: AnyQueryBuilder, alias: string): AliasedQueryNode {
	const node = qb.toOperationNode();
	return { node, aliased: new k.AliasedExpressionWrapper(new k.ExpressionWrapper(node), alias) };
}

export function applyHoistedSelections(
	toQb: AnySelectQueryBuilder,
	from: AliasedQueryNode,
): AnySelectQueryBuilder {
	return applyHoistedPrefixedSelections("", toQb, from);
}

export function applyHoistedPrefixedSelections(
	prefix: string,
	toQb: AnySelectQueryBuilder,
	from: AliasedQueryNode,
) {
	const hoistedSelections = hoistAndPrefixSelections(prefix, from);
	return toQb.select(hoistedSelections);
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, but aliased with the given prefix.
 */
export function hoistAndPrefixSelections(prefix: string, from: AliasedQueryNode) {
	const selections = getSelections(from.node);
	if (!selections) {
		return [];
	}

	// Reference nodes are built directly rather than parsed from
	// `"alias.name"`: this runs for every hoisted column of every subquery on
	// every query build, and parsing would also misread a name containing a dot.
	// `alias` is typed as `A | Expression<unknown>` because dynamic column references can be
	// aliased with an expression, but we always construct `AliasedQueryNode.aliased` with a
	// plain string alias.
	const alias = from.aliased.alias as string;
	const table = k.TableNode.create(alias);

	return selections.map((selectionNode) => {
		const name = extractSelectionName(selectionNode);

		const referenceExpression = new k.ExpressionWrapper(
			k.ReferenceNode.create(k.ColumnNode.create(name), table),
		);

		return new PrefixedAliasedExpression(referenceExpression, prefix, name);
	});
}

class PrefixedAliasedExpression<
	T,
	Prefix extends string,
	OriginalName extends string,
> extends k.AliasedExpressionWrapper<T, ApplyPrefix<Prefix, OriginalName>> {
	readonly originalName: string;

	constructor(expression: k.Expression<any>, prefix: Prefix, originalName: OriginalName) {
		const alias = applyPrefix(prefix, originalName);
		super(expression, alias);
		this.originalName = originalName;
	}
}

function extractSelectionName({ selection }: k.SelectionNode): string {
	switch (selection.kind) {
		case "ColumnNode":
			return selection.column.name;
		case "ReferenceNode":
			if (k.SelectAllNode.is(selection.column)) {
				throw new UnexpectedSelectAllError();
			}
			return selection.column.column.name;
		case "AliasNode":
			if (!k.IdentifierNode.is(selection.alias)) {
				throw new UnexpectedComplexAliasError();
			}
			return selection.alias.name;
		case "SelectAllNode":
			throw new UnexpectedSelectAllError();
		default:
			assertNever(selection);
	}
}
