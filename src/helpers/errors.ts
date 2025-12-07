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
