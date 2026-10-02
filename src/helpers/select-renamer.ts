import * as k from "kysely";

import { UnexpectedComplexAliasError, UnexpectedSelectAllError } from "./errors.ts";
import { applyPrefix } from "./prefixes.ts";
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

/**
 * Adds hoisted selections to a query.  `select()` accepts any operation node
 * source at runtime; only its types insist on Kysely's own expression classes.
 */
export function selectHoisted(
	toQb: AnySelectQueryBuilder,
	hoisted: readonly HoistedSelection[],
): AnySelectQueryBuilder {
	return toQb.select(hoisted as unknown as readonly k.AliasedExpression<unknown, string>[]);
}

/**
 * Hoisted selections by owner, then by alias, then by prefix, then by column
 * name.  Nested rather than joined into one key: names may contain anything.
 */
const hoistedByOwner = new WeakMap<
	object,
	Map<string, Map<string, Map<string, HoistedSelection>>>
>();

function getOrAddMap<K, V extends Map<any, any>>(
	map: { get(key: K): V | undefined; set(key: K, value: V): unknown },
	key: K,
): V {
	let value = map.get(key);
	if (value === undefined) {
		map.set(key, (value = new Map() as V));
	}
	return value;
}

/**
 * Produces selections for a parent query to select everything selected in a
 * subquery, but aliased with the given prefix.
 *
 * @param owner - What the subquery is built from, if it outlives this build
 *   (a nested query set, or a base query).  A hoisted column's node depends
 *   only on its names, and nodes are immutable, so each owner's are built once
 *   and shared by every later build, rather than rebuilt per request.
 */
export function hoistAndPrefixSelections(
	prefix: string,
	{ node, alias }: AliasedQuery,
	owner?: object,
): HoistedSelection[] {
	const selections = getSelections(node);
	if (!selections) {
		return [];
	}

	const cache =
		owner === undefined
			? undefined
			: getOrAddMap(getOrAddMap(getOrAddMap(hoistedByOwner, owner), alias), prefix);
	let table: k.TableNode | undefined;

	return selections.map((selectionNode) => {
		const name = extractSelectionName(selectionNode);
		let hoisted = cache?.get(name);
		if (hoisted === undefined) {
			// Built directly: parsing `"alias.name"` is slow and misreads a dotted name.
			table ??= k.TableNode.create(alias);
			hoisted = new HoistedSelection(
				k.AliasNode.create(
					k.ReferenceNode.create(k.ColumnNode.create(name), table),
					k.IdentifierNode.create(applyPrefix(prefix, name)),
				),
				name,
			);
			cache?.set(name, hoisted);
		}
		return hoisted;
	});
}

/**
 * A selection of a subquery's column, re-aliased with a prefix.  Holds its
 * node ready-made: `select()` takes any operation node source, so there is no
 * expression to wrap and no alias for Kysely to parse.
 */
class HoistedSelection implements k.OperationNodeSource {
	readonly #node: k.AliasNode;
	/** The column's name in the subquery, before prefixing. */
	readonly originalName: string;

	constructor(node: k.AliasNode, originalName: string) {
		this.#node = node;
		this.originalName = originalName;
	}

	toOperationNode(): k.AliasNode {
		return this.#node;
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
