import * as k from "kysely";

import {
	AliasTooLongError,
	UnexpectedComplexAliasError,
	UnexpectedSelectAllError,
} from "./errors.ts";
import { type ApplyPrefix, applyPrefix } from "./prefixes.ts";
import {
	type AnyQueryBuilder,
	type AnySelectQueryBuilder,
	assertNever,
	byteLength,
} from "./utils.ts";

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

/** The output column name, or `undefined` for `*`, `table.*`, and non-identifier aliases. */
function getSelectionName({ selection }: k.SelectionNode): string | undefined {
	switch (selection.kind) {
		case "ColumnNode":
			return selection.column.name;
		case "ReferenceNode":
			return k.SelectAllNode.is(selection.column) ? undefined : selection.column.column.name;
		case "AliasNode":
			return k.IdentifierNode.is(selection.alias) ? selection.alias.name : undefined;
		case "SelectAllNode":
			return undefined;
		default:
			assertNever(selection);
	}
}

/** Like `getSelectionName`, but throws: hoisting a selection needs its name. */
function extractSelectionName(selectionNode: k.SelectionNode): string {
	const name = getSelectionName(selectionNode);
	if (name === undefined) {
		throw k.AliasNode.is(selectionNode.selection)
			? new UnexpectedComplexAliasError()
			: new UnexpectedSelectAllError();
	}
	return name;
}

/**
 * Throws if an output column alias is over `maxBytes` (`null` disables the
 * check). Runs after plugins, so it measures what the database sees. Skips
 * `selectAll()` and raw aliases, whose names are not known here.
 */
export function assertAliasesFit<QB extends AnyQueryBuilder>(qb: QB, maxBytes: number | null): QB {
	if (maxBytes === null) {
		return qb;
	}
	for (const selectionNode of getSelections(qb) ?? []) {
		const name = getSelectionName(selectionNode);
		if (name !== undefined && byteLength(name) > maxBytes) {
			throw new AliasTooLongError(name, byteLength(name), maxBytes);
		}
	}
	return qb;
}
