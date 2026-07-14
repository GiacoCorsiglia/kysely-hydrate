export class KyselyHydrateError extends Error {}

export class UnexpectedSelectAllError extends KyselyHydrateError {
	constructor() {
		super("Hydrated queries do not support selectAll()");
	}
}

export class UnexpectedComplexAliasError extends KyselyHydrateError {
	constructor() {
		super("Hydrated queries do not support complex aliases");
	}
}

export class UnexpectedCaseError extends KyselyHydrateError {}

/**
 * Error thrown when a collection mode expects one item but none is found.
 */
export class ExpectedOneItemError extends KyselyHydrateError {
	constructor(key: string) {
		super(`Expected one item, but got none for key ${key}`);
	}
}

/**
 * Error thrown when a collection mode expects exactly one item but multiple are found.
 */
export class CardinalityViolationError extends KyselyHydrateError {
	constructor(key: string, count: number) {
		super(`Expected exactly one item for key ${key}, but got ${count}`);
	}
}

/**
 * Error thrown when wildcard selections (SELECT * or table.*) are encountered in lineage tracing.
 */
export class WildcardSelectionError extends KyselyHydrateError {
	constructor() {
		super("Wildcard selections are not supported");
	}
}

/**
 * Error thrown when an unexpected selection type is encountered during lineage tracing.
 */
export class UnexpectedSelectionTypeError extends KyselyHydrateError {
	constructor(kind: string) {
		super(`Unexpected selection type: ${kind}`);
	}
}

/**
 * Error thrown when a column reference is ambiguous (exists in multiple tables).
 */
export class AmbiguousColumnReferenceError extends KyselyHydrateError {
	constructor(columnName: string) {
		super(`Ambiguous column reference: ${columnName}`);
	}
}

/**
 * Error thrown when an unsupported alias node type is encountered.
 */
export class UnsupportedAliasNodeTypeError extends KyselyHydrateError {
	constructor(kind: string) {
		super(`Unsupported alias node type ${kind}`);
	}
}

/**
 * Error thrown when an unsupported table alias node type is encountered.
 */
export class UnsupportedTableAliasNodeTypeError extends KyselyHydrateError {
	constructor(kind: string) {
		super(`Unsupported table alias node type ${kind}`);
	}
}

/**
 * Error thrown when an unsupported operation node type is encountered.
 */
export class UnsupportedNodeTypeError extends KyselyHydrateError {
	constructor(kind: string) {
		super(`Unsupported node type: ${kind}`);
	}
}

/**
 * Error thrown when composing a Hydrator with another Hydrator
 * that has a different keyBy configuration.
 */
export class KeyByMismatchError extends KyselyHydrateError {
	constructor(thisKeyBy: string, otherKeyBy: string) {
		super(`Cannot compose hydrators with different keyBy: ${thisKeyBy} vs ${otherKeyBy}`);
	}
}

/**
 * Error thrown when attempting to nest a `QuerySet` with a write operation as a
 * join inside another `QuerySet`.
 */
export class InvalidJoinedQuerySetError extends KyselyHydrateError {
	constructor(baseAlias: string) {
		super(
			`You cannot join query sets with an UPDATE, INSERT, or CREATE base query (attempting to join query set with alias ${baseAlias})`,
		);
	}
}

/**
 * Error thrown when two distinct generated column aliases encode to the same
 * SQL identifier in the over-63-byte alias encoding.
 *
 * The realistic trigger is a canonical-form collision: the encoding is
 * case- and underscore-insensitive (so it stays stable under
 * CamelCasePlugin's snake_case/camelize round trip), which means two
 * over-long sibling aliases that differ only by case or underscores (e.g.
 * `created_at` vs `createdat` under the same relation path) encode
 * identically. A hash collision between unrelated aliases is also possible
 * but astronomically unlikely. Renaming one of the involved relation keys or
 * columns resolves it.
 */
export class AliasCollisionError extends KyselyHydrateError {
	constructor(encoded: string, logicalA: string, logicalB: string) {
		super(
			`Generated column aliases "${logicalA}" and "${logicalB}" both encode to the SQL identifier "${encoded}". Rename one of the involved relation keys or columns to resolve the collision.`,
		);
	}
}

/**
 * Error thrown when the logical names of encoded (over-63-byte) column
 * aliases cannot be restored on result rows because a plugin's
 * `transformResult` altered the marker values used to track them. This
 * happens when a plugin transforms values indiscriminately (e.g. converts
 * every number) rather than based on column type or name. Without this
 * check, hydration would silently drop the affected columns.
 */
export class AliasRestorationError extends KyselyHydrateError {
	constructor() {
		super(
			"Cannot restore over-long generated column aliases: a plugin's transformResult altered the marker values used to map encoded aliases back to their logical names. Use plugins that transform values based on column type or name rather than indiscriminately, or shorten the involved relation keys or columns.",
		);
	}
}
