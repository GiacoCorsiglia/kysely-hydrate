/**
 * Analysis of join ON clauses for emitting nested joins as a flat join chain.
 *
 * A query set nested in a join is normally emitted as a derived table that contains its own joins:
 * `A ⋈p (B ⋈q C)`.  SQLite never flattens a derived table that is itself a join when it's the right
 * operand of a LEFT JOIN (`flattenSubquery`, restriction 3a), so it materializes it in full, which
 * is catastrophic for a selective outer query.  The same result can instead be produced by the flat
 * chain `A ⋈p B ⋈q' C`, where q' is q with its table qualifiers renamed.  These helpers decide, per
 * nested join, whether that's an identity, and do the renaming.
 */
import * as k from "kysely";

import { SEP } from "./prefixes.ts";

/**
 * The ON condition a join method would build from the given arguments (the tail after the joined
 * table), or `null` for a join without one (a cross join).
 *
 * Built exactly as Kysely's own `parseJoin` builds it, through the public {@link k.JoinBuilder}:
 * no plugins run, since they run on the final query.  Never cached: a callback may build a
 * different condition each time it's called, and every compile must see its current one.
 */
export function buildJoinOn(args: readonly unknown[]): k.OperationNode | null {
	if (args.length === 0) {
		return null;
	}
	// The joined table is irrelevant to the condition; Kysely's builder never reads it.
	let builder = new k.JoinBuilder<any, any>({
		joinNode: k.JoinNode.create("InnerJoin", k.TableNode.create("_")),
	});
	if (args.length === 1) {
		builder = (args[0] as (join: k.JoinBuilder<any, any>) => k.JoinBuilder<any, any>)(builder);
	} else {
		builder = builder.onRef(args[0] as string, "=", args[1] as string);
	}
	return builder.toOperationNode().on?.on ?? null;
}

/**
 * A join callback that sets `on` as the join's condition, for adding a join whose condition has
 * already been built (so that a callback is never run twice in one compile).
 */
export function joinOn(on: k.OperationNode): (join: k.JoinBuilder<any, any>) => any {
	const expression = new k.ExpressionWrapper<any, any, k.SqlBool>(on);
	return (join) => join.on(expression);
}

/**
 * What the clauses of some SQL scope reference that constrains hoisting joins into it: the
 * hoisted (`$$`) columns of the relations joined in it, and unqualified names.
 *
 * A relation `key` whose column `child$$…` is referenced must keep its join `child` inside its
 * derived table, the only place that column exists.  An unqualified name could become ambiguous
 * with any table hoisted into the scope, so it rules out hoisting anything.
 */
export class HoistedReferences {
	/** For each relation, the joins it must keep, or `true` for all of them. */
	readonly #byTable = new Map<string, Set<string> | true>();
	/** Set when nothing may be hoisted into the scope. */
	#all = false;

	/** Whether `table` must keep its join `child` in its derived table. */
	isReferenced(table: string, child: string): boolean {
		if (this.#all) {
			return true;
		}
		const children = this.#byTable.get(table);
		return children === true || (children?.has(child) ?? false);
	}

	/** Records a reference to the column `column` of `table`, if it's a hoisted one. */
	addColumn(table: string, column: string): void {
		const sep = column.indexOf(SEP);
		if (sep === -1) {
			return;
		}
		let children = this.#byTable.get(table);
		if (children === true) {
			return;
		}
		if (children === undefined) {
			this.#byTable.set(table, (children = new Set()));
		}
		children.add(column.slice(0, sep));
	}

	/**
	 * Records the references in `node`.  A table-qualified column is attributed to its table.  A
	 * `$$` anywhere else (in raw SQL, or in an unqualified name) can't be; it's charged to all the
	 * joins of `unattributed` when given, else to every relation.  An unqualified column, or raw
	 * SQL that may contain one (see {@link hasUnqualifiedName}), rules out hoisting altogether.
	 * String values are data, not names, so they're ignored.
	 */
	scan(node: unknown, unattributed?: string): this {
		if (typeof node === "string") {
			this.#scanName(node, unattributed);
		} else if (Array.isArray(node)) {
			for (const item of node) {
				this.scan(item, unattributed);
			}
		} else if (typeof node === "object" && node !== null) {
			const n = node as k.OperationNode;
			if (k.ValueNode.is(n) || k.PrimitiveValueListNode.is(n)) {
				return this;
			}
			if (k.ColumnNode.is(n) || (k.ReferenceNode.is(n) && !n.table)) {
				this.#all = true;
				return this;
			}
			if (k.ReferenceNode.is(n) && k.ColumnNode.is(n.column) && !n.table!.table.schema) {
				this.#scanName(n.table!.table.identifier.name, unattributed);
				this.addColumn(n.table!.table.identifier.name, n.column.column.name);
				return this;
			}
			if (k.RawNode.is(n)) {
				for (const fragment of n.sqlFragments) {
					this.#scanName(fragment, unattributed);
					this.#all ||= hasUnqualifiedName(fragment);
				}
				return this.scan(n.parameters, unattributed);
			}
			for (const key in n) {
				this.scan((n as unknown as Record<string, unknown>)[key], unattributed);
			}
		}
		return this;
	}

	#scanName(name: string, unattributed: string | undefined): void {
		if (!name.includes(SEP)) {
			return;
		}
		if (unattributed === undefined) {
			this.#all = true;
		} else {
			this.#byTable.set(unattributed, true);
		}
	}
}

/** Words raw SQL may contain besides names; any other bare word is taken for a column. */
const SQL_WORDS = new Set(
	(
		"all and any as asc between by case collate desc distinct else end escape exists false first " +
		"for from glob ilike in is last like limit locked not nowait null nulls of offset on or " +
		"order regexp share similar skip some then to true unknown update when"
	).split(" "),
);

/**
 * Whether raw SQL text may name an unqualified column: it has a bare word (outside string
 * literals, not next to a `.`, not a function name before `(`) that isn't one of a few SQL words.
 * Ers toward yes.
 */
export function hasUnqualifiedName(text: string): boolean {
	const words = /'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`|\[[^\]]*\]|[A-Za-z_][\w$]*|\d[\w.]*/g;
	for (let match; (match = words.exec(text)); ) {
		const word = match[0];
		if (word[0] === "'" || /^\d/.test(word)) {
			continue;
		}
		const before = text.slice(0, match.index).trimEnd().at(-1);
		const after = text.slice(match.index + word.length).trimStart()[0];
		if (before === "." || after === ".") {
			continue;
		}
		const isQuoted = /^["`[]/.test(word);
		if (!isQuoted && (after === "(" || SQL_WORDS.has(word.toLowerCase()))) {
			continue;
		}
		return true;
	}
	return false;
}

/**
 * Node kinds an ON clause may contain to be rewritten: logic, comparisons and function calls over
 * qualified columns and values.  Anything else (raw SQL, subqueries, …) might reference a table in
 * a way the rewrite can't see or must not touch.
 */
const REWRITABLE_KINDS = new Set<string>([
	"AndNode",
	"OrNode",
	"ParensNode",
	"BinaryOperationNode",
	"UnaryOperationNode",
	"OperatorNode",
	"FunctionNode",
	"ValueListNode",
]);

/**
 * Rewrites the table qualifiers of an ON clause through `aliases`, or returns `null` if it can't:
 * the clause contains a node outside {@link REWRITABLE_KINDS} (except `onTrue()`'s raw `true`), an
 * unqualified or schema-qualified column, or a table outside `aliases`.
 */
export function rewriteOn(
	node: k.OperationNode,
	aliases: ReadonlyMap<string, string>,
): k.OperationNode | null {
	switch (node.kind) {
		case "ValueNode":
		case "PrimitiveValueListNode":
			return node;
		case "RawNode": {
			// Raw SQL is opaque, except for `onTrue()`'s `true`, and for text-free raw nodes that only
			// wrap other nodes, like `sql.ref()`'s.
			const { sqlFragments, parameters } = node as k.RawNode;
			if (parameters.length === 0) {
				return sqlFragments.length === 1 && sqlFragments[0]!.trim().toLowerCase() === "true"
					? node
					: null;
			}
			if (sqlFragments.some((fragment) => fragment.trim() !== "")) {
				return null;
			}
			const rewritten = rewriteValue(parameters, aliases) as k.OperationNode[] | null;
			return rewritten && k.RawNode.create(sqlFragments, rewritten);
		}
		case "ReferenceNode": {
			const { table, column } = node as k.ReferenceNode;
			if (!table || table.table.schema || !k.ColumnNode.is(column)) {
				return null;
			}
			const to = aliases.get(table.table.identifier.name);
			return to === undefined ? null : k.ReferenceNode.create(column, k.TableNode.create(to));
		}
	}
	if (!REWRITABLE_KINDS.has(node.kind)) {
		return null;
	}
	const out: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(node)) {
		const rewritten = rewriteValue(value, aliases);
		if (rewritten === null) {
			return null;
		}
		out[key] = rewritten;
	}
	return Object.freeze(out) as unknown as k.OperationNode;
}

function rewriteValue(value: unknown, aliases: ReadonlyMap<string, string>): unknown {
	if (Array.isArray(value)) {
		const out: unknown[] = [];
		for (const item of value) {
			const rewritten = rewriteValue(item, aliases);
			if (rewritten === null) {
				return null;
			}
			out.push(rewritten);
		}
		return Object.freeze(out);
	}
	if (typeof value === "object" && value !== null && "kind" in value) {
		return rewriteOn(value as k.OperationNode, aliases);
	}
	// Strings (a function's name, a node's kind) and other plain data.
	return value;
}

/**
 * Comparisons that are never TRUE when an operand is NULL.  (`not in` is left out: SQLite's
 * `NULL NOT IN ()` is TRUE.)
 */
const STRICT_OPERATORS = new Set([
	"=",
	"==",
	"!=",
	"<>",
	"<",
	">",
	"<=",
	">=",
	"in",
	"like",
	"not like",
	"ilike",
	"not ilike",
]);

/**
 * Whether an ON clause is never TRUE when every column of `table` is NULL: it's a conjunction one
 * of whose terms is (or a disjunction all of whose terms are) a strict comparison with a column of
 * `table` as an operand.
 */
export function isNullRejecting(node: k.OperationNode, table: string): boolean {
	if (k.ParensNode.is(node)) {
		return isNullRejecting(node.node, table);
	}
	if (k.AndNode.is(node)) {
		return isNullRejecting(node.left, table) || isNullRejecting(node.right, table);
	}
	if (k.OrNode.is(node)) {
		return isNullRejecting(node.left, table) && isNullRejecting(node.right, table);
	}
	if (k.BinaryOperationNode.is(node)) {
		return (
			k.OperatorNode.is(node.operator) &&
			STRICT_OPERATORS.has(node.operator.operator) &&
			(isColumnOf(node.leftOperand, table) || isColumnOf(node.rightOperand, table))
		);
	}
	return false;
}

function isColumnOf(node: k.OperationNode, table: string): boolean {
	if (k.RawNode.is(node)) {
		// `sql.ref()`: a reference wrapped in text-free raw SQL.
		return (
			node.parameters.length === 1 &&
			node.sqlFragments.every((fragment) => fragment.trim() === "") &&
			isColumnOf(node.parameters[0]!, table)
		);
	}
	return k.ReferenceNode.is(node) && node.table?.table.identifier.name === table;
}
