import { UnexpectedCaseError } from "./errors.ts";

export type Prettify<T> = {
	[K in keyof T]: T[K];
} & {};

export type ObjectOf<K extends PropertyKey, V> = { [_ in K]: V };

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
export type ExtendWith<T, K extends PropertyKey, V> = Flatten<
	// fast path when there is no keys overlap
	K & keyof T extends never ? T & ObjectOf<K, V> : Omit<T, K> & ObjectOf<K, V>
>;

type _Override<T, K extends keyof T, V> = Omit<T, K> & ObjectOf<K, V>;

export type Override<T, K extends keyof T, V> = Flatten<_Override<T, K, V>>;

export type AddOrOverride<T, K extends PropertyKey, V> = Flatten<
	K extends keyof T ? _Override<T, K, V> : T & { [_ in K]: V }
>;

/**
 * Ensures that U is a strict subset of T - all keys in U must exist in T
 * with compatible types, and U cannot have any extra keys.  Can be used in functions like
 *
 * ```ts
 * function <U extends StrictSubset<T, U>>(arg: U) {}
 * ```
 */
export type StrictSubset<T, U> = Partial<T> & {
	[K in Exclude<keyof U, keyof T>]: never;
};

type AtLeastOne<T> = readonly [T, ...T[]];
export type KeyBy<T> = (keyof T & string) | AtLeastOne<keyof T & string>;

export type KeysWithValueOfType<O, T> = {
	[K in keyof O]: O[K] extends T ? K : never;
}[keyof O];

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

export function capitalize<K extends string>(str: K): Capitalize<K> {
	if (!str) return "" as Capitalize<K>;
	return (str.charAt(0).toUpperCase() + str.slice(1)) as Capitalize<K>;
}

/**
 * Creates a new Map with the given key deleted.  If the key does not exist,
 * returns the original Map.
 */
export function mapWithDeleted<K, V>(map: Map<K, V>, key: K): Map<K, V> {
	if (!map.has(key)) {
		return map;
	}

	const clone = new Map(map);
	clone.delete(key);
	return clone;
}
