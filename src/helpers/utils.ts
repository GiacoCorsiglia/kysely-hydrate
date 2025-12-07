import { UnexpectedCaseError } from "./errors.ts";

export type Prettify<T> = {
	[K in keyof T]: T[K];
} & {};

export type Identity<T> = T;
export type Flatten<T> = Identity<{ [k in keyof T]: T[k] }>;
export type Extend<A, B> = Flatten<
	// fast path when there is no keys overlap
	keyof A & keyof B extends never
		? A & B
		: {
				[K in keyof A as K extends keyof B ? never : K]: A[K];
			} & {
				[K in keyof B]: B[K];
			}
>;

type _Override<T, K extends keyof T, V> = Omit<T, K> & { [_ in K]: V };

export type Override<T, K extends keyof T, V> = Flatten<_Override<T, K, V>>;

export type AddOrOverride<T, K extends PropertyKey, V> = Flatten<
	K extends keyof T ? _Override<T, K, V> : T & { [_ in K]: V }
>;

export type KeyBy<T> = (keyof T & string) | readonly (keyof T & string)[];

export function assertNever(arg: never): never {
	throw new UnexpectedCaseError(`Unexpected case: ${JSON.stringify(arg)}`);
}

export function isIterable<T>(input: unknown): input is Iterable<T> {
	return (
		input !== null &&
		typeof input === "object" &&
		typeof (input as any)[Symbol.iterator] === "function"
	);
}

/**
 * Adds properties from an object to a Map, cloning the Map first for immutability.
 * If the map is undefined, creates a new Map.
 */
export function addObjectToMap<K extends string, V>(
	map: Map<K, V> | undefined,
	obj: Record<string, V | undefined>,
): Map<K, V> {
	const clone = new Map(map);
	for (const key of Object.keys(obj)) {
		const value = obj[key];
		if (value !== undefined) {
			clone.set(key as K, value);
		}
	}
	return clone;
}
