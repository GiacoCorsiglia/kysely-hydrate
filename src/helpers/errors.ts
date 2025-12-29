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
 * Error thrown when attempting to call a private method on an instance that
 * was not properly registered with the private accessor.
 */
export class InvalidInstanceError extends KyselyHydrateError {
	constructor() {
		super("Invalid instance - private method not registered");
	}
}

/**
 * Error thrown when attempting to extend a Hydrator with another Hydrator
 * that has a different keyBy configuration.
 */
export class KeyByMismatchError extends KyselyHydrateError {
	constructor(thisKeyBy: string, otherKeyBy: string) {
		super(`Cannot extend hydrators with different keyBy: ${thisKeyBy} vs ${otherKeyBy}`);
	}
}
