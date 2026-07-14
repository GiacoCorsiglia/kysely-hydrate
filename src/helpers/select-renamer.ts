import * as k from "kysely";

import { encodeAlias } from "./alias-encoding.ts";
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
		// The runtime alias is encoded so it stays within PostgreSQL's 63-byte
		// identifier limit.  The type-level alias remains the full logical name:
		// hydration restores logical names on result rows before they are
		// consumed (see QuerySetImpl.hydrate), so the encoding is invisible to
		// both the type system and user code.  Prefix-less hoisting re-selects
		// names that already appear in a select list (either user-authored or
		// already encoded), so only prefixed (generated) aliases are encoded.
		const logicalAlias = applyPrefix(prefix, originalName);
		const alias = prefix === "" ? logicalAlias : encodeAlias(logicalAlias);
		super(expression, alias as ApplyPrefix<Prefix, OriginalName>);
		this.originalName = originalName;
	}
}

/**
 * Returns the output column names of a query builder's select (or returning)
 * list.  Selections whose name cannot be determined (`SELECT *`, complex
 * aliases) are skipped rather than throwing: callers use this to mirror the
 * select list for alias bookkeeping, and unresolvable selections can never
 * carry generated (encoded) aliases.
 */
export function getSelectionNames(qb: AnyQueryBuilder): string[] {
	const selections = getSelections(qb) ?? [];
	const names: string[] = [];
	for (const selectionNode of selections) {
		try {
			names.push(extractSelectionName(selectionNode));
		} catch (error) {
			// Skip select-all and complex-alias selections; they map to
			// themselves and never need alias restoration.
			if (
				!(error instanceof UnexpectedSelectAllError) &&
				!(error instanceof UnexpectedComplexAliasError)
			) {
				throw error;
			}
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
