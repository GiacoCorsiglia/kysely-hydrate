import * as k from "kysely";

import { UnexpectedComplexAliasError, UnexpectedSelectAllError } from "./errors.ts";
import { type AnyQueryBuilder, type AnySelectQueryBuilder, assertNever } from "./utils.ts";

function getSelections(qb: AnyQueryBuilder): readonly k.SelectionNode[] | undefined {
	// NOTE: `toOperationNode()` runs the builder's plugins (e.g.
	// CamelCasePlugin), so the extracted names are in whatever name space the
	// builder's executor produces.  QuerySet builds all internal subqueries
	// plugin-free (see `#getSubqueryDb`) so this space stays consistent across
	// nesting levels.
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

/**
 * Selects on `toQb` everything selected by the subquery `fromQb` (joined into
 * `toQb` under `alias`), renaming each selection per the optional `rename`
 * function (defaults to keeping the original names).
 */
export function applyHoistedSelections(
	toQb: AnySelectQueryBuilder,
	fromQb: AnyQueryBuilder,
	alias: string,
	rename: (name: string) => string = (name) => name,
): AnySelectQueryBuilder {
	return toQb.select(hoistSelections(fromQb, alias, rename));
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, aliased per the given rename function.
 */
export function hoistSelections(
	qb: AnyQueryBuilder,
	alias: string,
	rename: (name: string) => string,
) {
	const selections = getSelections(qb);
	if (!selections) {
		return [];
	}

	const eb = k.expressionBuilder<any, any>();

	return selections.map((selectionNode) => {
		const name = extractSelectionName(selectionNode);

		const referenceExpression = eb.ref(`${alias}.${name}`);

		return new RenamedAliasedExpression(referenceExpression, rename(name), name);
	});
}

class RenamedAliasedExpression<T, Alias extends string> extends k.AliasedExpressionWrapper<
	T,
	Alias
> {
	readonly originalName: string;

	constructor(expression: k.Expression<any>, alias: Alias, originalName: string) {
		super(expression, alias);
		this.originalName = originalName;
	}
}

/**
 * Best-effort extraction of a query's selection names, in select-list order.
 * Returns undefined if any selection's name cannot be determined (select-all
 * or a complex alias); callers that require the names surface the strict
 * error at hoist time instead.
 */
export function tryGetSelectionNames(qb: AnyQueryBuilder): readonly string[] | undefined {
	const selections = getSelections(qb);
	if (!selections) {
		return undefined;
	}

	const names: string[] = [];
	for (const selectionNode of selections) {
		try {
			names.push(extractSelectionName(selectionNode));
		} catch {
			return undefined;
		}
	}
	return names;
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
