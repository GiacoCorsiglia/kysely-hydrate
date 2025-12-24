import * as k from "kysely";

import {
	AmbiguousColumnReferenceError,
	UnsupportedAliasNodeTypeError,
	UnsupportedNodeTypeError,
	UnsupportedTableAliasNodeTypeError,
	UnexpectedSelectionTypeError,
	WildcardSelectionError,
} from "./errors.ts";

type Provenance =
	| { type: "COLUMN"; table: string; column: string }
	| { type: "DERIVED" }
	| { type: "UNRESOLVED" };

type SourceDefinition =
	| { kind: "TABLE"; node: k.TableNode }
	| { kind: "SUBQUERY"; node: k.SelectQueryNode }
	| { kind: "CTE"; name: string };

type GlobalContext = {
	ctes: Map<string, k.CommonTableExpressionNode>;
};

type LocalScope = Map<string, SourceDefinition>;

type Tracer<K extends k.RootOperationNode["kind"]> = (
	node: Extract<k.RootOperationNode, { kind: K }>,
	context: GlobalContext,
) => Map<string, Provenance>;

function createLocalScope(): LocalScope {
	return new Map<string, SourceDefinition>();
}

function extractTableName(tableNode: k.TableNode): string {
	return tableNode.table.identifier.name;
}

function buildLocalScope(node: k.SelectQueryNode, context: GlobalContext): LocalScope {
	const scope = createLocalScope();

	// Process FROM clause
	if (node.from) {
		for (const fromItem of node.from.froms) {
			addFromItemToScope(fromItem, scope, context);
		}
	}

	// Process JOINs
	if (node.joins) {
		for (const join of node.joins) {
			addFromItemToScope(join.table, scope, context);
		}
	}

	return scope;
}

function addFromItemToScope(
	fromItem: k.OperationNode,
	scope: LocalScope,
	context: GlobalContext,
): void {
	if (fromItem.kind === "TableNode") {
		const table = fromItem as k.TableNode;
		const tableName = extractTableName(table);
		// Check if this is a CTE reference
		if (context.ctes.has(tableName)) {
			scope.set(tableName, { kind: "CTE", name: tableName });
		} else {
			scope.set(tableName, { kind: "TABLE", node: table });
		}
	} else if (fromItem.kind === "AliasNode") {
		const aliasNode = fromItem as k.AliasNode;
		const alias = (aliasNode.alias as k.IdentifierNode).name;

		if (aliasNode.node.kind === "TableNode") {
			scope.set(alias, { kind: "TABLE", node: aliasNode.node as k.TableNode });
		} else if (aliasNode.node.kind === "SelectQueryNode") {
			scope.set(alias, { kind: "SUBQUERY", node: aliasNode.node as k.SelectQueryNode });
		}
	} else if (fromItem.kind === "ReferenceNode") {
		const ref = fromItem as k.ReferenceNode;
		const columnNode = ref.column as k.ColumnNode;
		const name = (columnNode.column as k.IdentifierNode).name;
		if (context.ctes.has(name)) {
			scope.set(name, { kind: "CTE", name });
		}
	}
}

function traceSelection(
	selection: k.SelectionNode,
	localScope: LocalScope,
	context: GlobalContext,
): [string, Provenance] {
	// Unwrap SelectionNode to get the actual selection
	const actualSelection = (selection as any).selection as k.OperationNode;

	// Check for wildcard selections
	if (k.SelectAllNode.is(actualSelection)) {
		throw new WildcardSelectionError();
	}

	if (k.AliasNode.is(actualSelection)) {
		const aliasNode = actualSelection as k.AliasNode;
		const outputKey = (aliasNode.alias as k.IdentifierNode).name;
		const provenance = traceNode(aliasNode.node, localScope, context);
		return [outputKey, provenance];
	}

	if (k.ColumnNode.is(actualSelection)) {
		const col = actualSelection;
		const columnName = col.column.name;
		const provenance = traceNode(actualSelection, localScope, context);
		return [columnName, provenance];
	}

	if (k.ReferenceNode.is(actualSelection)) {
		const ref = actualSelection;
		// Check if this is a table.* wildcard
		if (k.SelectAllNode.is(ref.column)) {
			throw new WildcardSelectionError();
		}

		const columnName = ref.column.column.name;
		const provenance = traceNode(actualSelection, localScope, context);
		return [columnName, provenance];
	}

	throw new UnexpectedSelectionTypeError(actualSelection.kind);
}

function traceNode(
	node: k.OperationNode,
	localScope: LocalScope,
	context: GlobalContext,
): Provenance {
	if (k.ColumnNode.is(node)) {
		const col = node;
		const columnName = col.column.name;
		return resolveColumn(undefined, columnName, localScope, context);
	}

	if (k.ReferenceNode.is(node)) {
		const ref = node;
		let tableAlias: string | undefined;
		if (ref.table) {
			tableAlias = extractTableName(ref.table);
		}

		if (k.SelectAllNode.is(ref.column)) {
			throw new WildcardSelectionError();
		}

		const columnName = ref.column.column.name;
		return resolveColumn(tableAlias, columnName, localScope, context);
	}

	// Any other node type (functions, literals, expressions) is DERIVED
	return { type: "DERIVED" };
}

function resolveColumn(
	tableAlias: string | undefined,
	columnName: string,
	localScope: LocalScope,
	context: GlobalContext,
): Provenance {
	if (tableAlias) {
		const source = localScope.get(tableAlias);
		if (!source) {
			return { type: "UNRESOLVED" };
		}
		return traceFromSource(source, columnName, context);
	}

	// No alias: search all sources
	if (localScope.size === 0) {
		return { type: "UNRESOLVED" };
	}

	// Optimize for single source.
	if (localScope.size === 1) {
		const [_, source] = Array.from(localScope.entries())[0]!;
		return traceFromSource(source, columnName, context);
	}

	// Multiple sources: try each source and see if exactly one resolves
	const candidates: [string, Provenance][] = [];
	for (const [tableName, source] of localScope.entries()) {
		const provenance = traceFromSource(source, columnName, context);
		// TODO: This is probably wrong, we have a source regardless of provenance.
		if (provenance.type === "COLUMN") {
			candidates.push([tableName, provenance]);
		}
	}

	if (candidates.length === 1) {
		return candidates[0]![1];
	}

	if (candidates.length > 1) {
		throw new AmbiguousColumnReferenceError(columnName);
	}

	return { type: "UNRESOLVED" };
}

function traceFromSource(
	source: SourceDefinition,
	columnName: string,
	context: GlobalContext,
): Provenance {
	if (source.kind === "TABLE") {
		const table = source.node;
		const schemableId = table.table;
		const schema = schemableId.schema ? schemableId.schema.name : undefined;
		const tableName = extractTableName(table);
		const fullTableName = schema ? `${schema}.${tableName}` : tableName;
		return { type: "COLUMN", table: fullTableName, column: columnName };
	}

	if (source.kind === "SUBQUERY") {
		const subResult = traceSelectQuery(source.node, context);
		return subResult.get(columnName) || { type: "UNRESOLVED" };
	}

	if (source.kind === "CTE") {
		const cte = context.ctes.get(source.name);
		if (!cte || !cte.expression) {
			return { type: "UNRESOLVED" };
		}
		const subResult = traceSelectQuery(cte.expression as k.SelectQueryNode, context);
		return subResult.get(columnName) || { type: "UNRESOLVED" };
	}

	return { type: "UNRESOLVED" };
}

function traceSelections(
	selections: readonly k.SelectionNode[],
	localScope: LocalScope,
	context: GlobalContext,
): Map<string, Provenance> {
	const result = new Map<string, Provenance>();
	for (const selection of selections) {
		if (selection.selection.kind === "SelectAllNode") {
			throw new WildcardSelectionError();
		}
		const [outputKey, provenance] = traceSelection(selection, localScope, context);
		result.set(outputKey, provenance);
	}
	return result;
}

const traceSelectQuery: Tracer<"SelectQueryNode"> = (node, context) => {
	if (!node.selections) {
		return new Map();
	}

	// Build global context with CTEs
	if (node.with) {
		for (const cte of node.with.expressions) {
			// Extract CTE name from CommonTableExpressionNameNode
			const cteName = cte.name.table;
			const name = extractTableName(cteName);
			context.ctes.set(name, cte);
		}
	}

	// Build local scope from FROM and JOINs
	const localScope = buildLocalScope(node, context);

	// Process selections
	return traceSelections(node.selections, localScope, context);
};

const traceInsertQuery: Tracer<"InsertQueryNode"> = (node, context) => {
	if (!node.returning) {
		return new Map();
	}

	const localScope = createLocalScope();

	if (node.into) {
		const table = node.into;
		const tableName = extractTableName(table);
		localScope.set(tableName, { kind: "TABLE", node: table });
	}

	return traceSelections(node.returning.selections, localScope, context);
};

const traceUpdateQuery: Tracer<"UpdateQueryNode"> = (node, context) => {
	if (!node.returning) {
		return new Map();
	}

	const localScope = createLocalScope();

	const { table } = node;
	if (table) {
		if (k.TableNode.is(table)) {
			const tableName = extractTableName(table);
			localScope.set(tableName, { kind: "TABLE", node: table });
		} else if (k.AliasNode.is(table)) {
			if (!k.IdentifierNode.is(table.alias)) {
				throw new UnsupportedAliasNodeTypeError(table.alias.kind);
			}
			const alias = table.alias.name;
			if (!k.TableNode.is(table.node)) {
				throw new UnsupportedTableAliasNodeTypeError(table.node.kind);
			}
			localScope.set(alias, { kind: "TABLE", node: table.node });
		}
	}

	return traceSelections(node.returning.selections, localScope, context);
};

const traceDeleteQuery: Tracer<"DeleteQueryNode"> = (node, context) => {
	if (!node.returning) {
		return new Map();
	}

	const localScope = createLocalScope();

	// Build local scope from FROM clause
	if (node.from) {
		const fromNode = node.from;
		for (const fromItem of fromNode.froms) {
			addFromItemToScope(fromItem, localScope, context);
		}
	}

	return traceSelections(node.returning.selections, localScope, context);
};

const noopTracer = () => new Map();

const tracers: {
	[K in k.RootOperationNode["kind"]]: Tracer<K>;
} = {
	SelectQueryNode: traceSelectQuery,
	InsertQueryNode: traceInsertQuery,
	UpdateQueryNode: traceUpdateQuery,
	DeleteQueryNode: traceDeleteQuery,

	AlterTableNode: noopTracer,
	CreateIndexNode: noopTracer,
	CreateSchemaNode: noopTracer,
	CreateTableNode: noopTracer,
	CreateViewNode: noopTracer,
	DropIndexNode: noopTracer,
	DropSchemaNode: noopTracer,
	DropTableNode: noopTracer,
	DropViewNode: noopTracer,
	CreateTypeNode: noopTracer,
	DropTypeNode: noopTracer,
	MergeQueryNode: noopTracer,
	RawNode: noopTracer,
	RefreshMaterializedViewNode: noopTracer,
};

export function traceLineage(node: k.RootOperationNode): Map<string, Provenance> {
	const tracer = tracers[node.kind];
	if (!tracer) {
		throw new UnsupportedNodeTypeError(node.kind);
	}

	const context: GlobalContext = {
		ctes: new Map(),
	};

	return tracer(node as never, context);
}
