import * as k from "kysely";

import { UnexpectedComplexAliasError, UnexpectedSelectAllError } from "./errors.ts";
import { type ApplyPrefix, applyPrefix } from "./prefixes.ts";
import { type AnyQueryBuilder, type AnySelectQueryBuilder, assertNever } from "./utils.ts";

function getSelections(node: AliasedQuery["node"]): readonly k.SelectionNode[] | undefined {
	switch (node.kind) {
		case "SelectQueryNode":
			return node.selections;
		case "InsertQueryNode":
		case "DeleteQueryNode":
		case "UpdateQueryNode":
			return node.returning?.selections;
		default:
			assertNever(node);
	}
}

export type AliasedQuery = ReturnType<typeof aliasQuery>;

/**
 * Converts a query to its node once, so joining it and hoisting its selections
 * don't each re-run its plugins.
 */
export function aliasQuery(qb: AnyQueryBuilder, alias: string) {
	const node = qb.toOperationNode();
	const aliased = new k.AliasedExpressionWrapper<any, string>(new k.ExpressionWrapper(node), alias);
	return { node, alias, aliased };
}

export function applyHoistedSelections(
	toQb: AnySelectQueryBuilder,
	from: AliasedQuery,
): AnySelectQueryBuilder {
	return applyHoistedPrefixedSelections("", toQb, from);
}

export function applyHoistedPrefixedSelections(
	prefix: string,
	toQb: AnySelectQueryBuilder,
	from: AliasedQuery,
) {
	const hoistedSelections = hoistAndPrefixSelections(prefix, from);
	return toQb.select(hoistedSelections);
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, but aliased with the given prefix.
 */
export function hoistAndPrefixSelections(prefix: string, { node, alias }: AliasedQuery) {
	const selections = getSelections(node);
	if (!selections) {
		return [];
	}

	// Built directly: parsing `"alias.name"` is slow and misreads a dotted name.
	const table = k.TableNode.create(alias);

	return selections.map((selectionNode) => {
		const name = extractSelectionName(selectionNode);

		const referenceExpression = new k.ExpressionWrapper(
			k.ReferenceNode.create(k.ColumnNode.create(name), table),
		);

		return new PrefixedAliasedExpression(referenceExpression, prefix, name);
	});
}

export class PrefixedAliasedExpression<
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
