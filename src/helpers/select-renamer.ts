import * as k from "kysely";

import { UnexpectedComplexAliasError, UnexpectedSelectAllError } from "./errors.ts";
import { applyPrefix } from "./prefixes.ts";
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

/**
 * Options for renaming over-long aliases while hoisting a subquery's
 * selections into a parent query.  See the `identifier-renames` module:
 * aliases that could exceed the database identifier length limit are replaced
 * with short `$cN` aliases.
 *
 * Logical names are passed positionally (rather than re-parsed out of the
 * subquery's operation node) because `toOperationNode()` applies the query's
 * plugins: with e.g. CamelCasePlugin installed, the extracted names are
 * snake_cased and would no longer match the logical names used elsewhere.
 */
export interface HoistOptions {
	/**
	 * The logical names of the subquery's output columns, by select-list
	 * position, used to recover each column's logical name (its SQL alias may
	 * be a `$cN` rename, and plugins may have transformed the extracted name).
	 */
	sourceColumns?: readonly string[] | undefined;
	/**
	 * The hoisting query's renames (logical name → short SQL alias), applied to
	 * the prefixed logical name to pick the final alias.
	 */
	toShort?: ReadonlyMap<string, string> | undefined;
	/**
	 * If provided, receives the (prefixed) logical name of each hoisted
	 * selection, by select-list position.
	 */
	columnsOut?: string[] | undefined;
}

export function applyHoistedPrefixedSelections(
	prefix: string,
	toQb: AnySelectQueryBuilder,
	fromQb: AnyQueryBuilder,
	alias: string,
	options?: HoistOptions,
) {
	const hoistedSelections = hoistAndPrefixSelections(prefix, fromQb, alias, options);
	return toQb.select(hoistedSelections);
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, but aliased with the given prefix (further shortened per
 * `options.toShort`, when provided).
 */
export function hoistAndPrefixSelections(
	prefix: string,
	qb: AnyQueryBuilder,
	alias: string,
	options?: HoistOptions,
) {
	const selections = getSelections(qb);
	if (!selections) {
		return [];
	}

	const eb = k.expressionBuilder<any, any>();

	return selections.map((selectionNode, index) => {
		const name = extractSelectionName(selectionNode);

		const referenceExpression = eb.ref(`${alias}.${name}`);

		// Recover the column's logical name before prefixing, then apply the
		// hoisting query's own rename (if any) to pick the final SQL alias.
		const logicalName = applyPrefix(prefix, options?.sourceColumns?.[index] ?? name);
		const sqlAlias = options?.toShort?.get(logicalName) ?? logicalName;

		options?.columnsOut?.push(logicalName);

		return new HoistedAliasedExpression(referenceExpression, sqlAlias, name);
	});
}

/**
 * Returns the output column names of a query builder's select (or returning)
 * list.
 */
export function getSelectionNames(qb: AnyQueryBuilder): string[] {
	const selections = getSelections(qb);
	return selections ? selections.map(extractSelectionName) : [];
}

class HoistedAliasedExpression<T> extends k.AliasedExpressionWrapper<T, string> {
	readonly originalName: string;

	constructor(expression: k.Expression<T>, alias: string, originalName: string) {
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
