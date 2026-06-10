import * as k from "kysely";

import { UnexpectedComplexAliasError, UnexpectedSelectAllError } from "./errors.ts";
import { type ApplyPrefix, applyPrefix } from "./prefixes.ts";
import { type AnyQueryBuilder, type AnySelectQueryBuilder, assertNever } from "./utils.ts";

function getSelections(qb: AnyQueryBuilder): readonly k.SelectionNode[] | undefined {
	const node = qb.toOperationNode();

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

export function applyHoistedSelections(
	toQb: AnySelectQueryBuilder,
	fromQb: AnyQueryBuilder,
	alias: string,
): AnySelectQueryBuilder {
	return applyHoistedPrefixedSelections("", toQb, fromQb, alias);
}

export function applyHoistedPrefixedSelections(
	prefix: string,
	toQb: AnySelectQueryBuilder,
	fromQb: AnyQueryBuilder,
	alias: string,
) {
	const hoistedSelections = hoistAndPrefixSelections(prefix, fromQb, alias);
	return toQb.select(hoistedSelections);
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, but aliased with the given prefix.
 */
export function hoistAndPrefixSelections(prefix: string, qb: AnyQueryBuilder, alias: string) {
	const selections = getSelections(qb);
	if (!selections) {
		return [];
	}

	const eb = k.expressionBuilder<any, any>();

	return selections.map((selectionNode) => {
		const name = extractSelectionName(selectionNode);

		const referenceExpression = eb.ref(`${alias}.${name}`);

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

function extractSelectionName(selectionNode: k.SelectionNode): string {
	const { selection } = selectionNode;

	if (k.ColumnNode.is(selection)) {
		return selection.column.name;
	}

	if (k.ReferenceNode.is(selection)) {
		const { column } = selection;

		if (k.SelectAllNode.is(column)) {
			throw new UnexpectedSelectAllError();
		}

		return column.column.name;
	}

	if (k.AliasNode.is(selection)) {
		const alias = selection.alias;

		if (!k.IdentifierNode.is(alias)) {
			throw new UnexpectedComplexAliasError();
		}

		return alias.name;
	}

	if (k.SelectAllNode.is(selection)) {
		throw new UnexpectedSelectAllError();
	}

	assertNever(selection);
}
