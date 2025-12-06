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
