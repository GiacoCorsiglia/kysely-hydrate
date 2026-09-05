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
	utf8ByteLength,
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

/**
 * The output column name of a selection, or `undefined` when it cannot be known
 * statically: `*`, `table.*`, or an alias that is not a plain identifier.
 */
function getSelectionName({ selection }: k.SelectionNode): string | undefined {
	if (k.ColumnNode.is(selection)) {
		return selection.column.name;
	}

	if (k.ReferenceNode.is(selection)) {
		return k.SelectAllNode.is(selection.column) ? undefined : selection.column.column.name;
	}

	if (k.AliasNode.is(selection)) {
		return k.IdentifierNode.is(selection.alias) ? selection.alias.name : undefined;
	}

	if (k.SelectAllNode.is(selection)) {
		return undefined;
	}

	assertNever(selection);
}

/**
 * Like {@link getSelectionName}, but throws when the name cannot be known,
 * because hoisting a selection into a parent query requires it.
 */
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
 * Throws if any output column of the query (as the database will see it, after
 * plugins) is longer than `maxBytes`. PostgreSQL would silently truncate it.
 *
 * Selections without a statically known name are skipped: `selectAll()` /
 * `returningAll()` output real columns, which are the user's responsibility,
 * and a raw alias cannot be measured.
 */
export function assertAliasesFit(qb: AnyQueryBuilder, maxBytes: number): void {
	for (const selectionNode of getSelections(qb) ?? []) {
		const name = getSelectionName(selectionNode);
		if (name === undefined) {
			continue;
		}
		const bytes = utf8ByteLength(name);
		if (bytes > maxBytes) {
			throw new AliasTooLongError(name, bytes, maxBytes);
		}
	}
}
