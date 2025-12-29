import { InvalidInstanceError, UnexpectedCaseError } from "./errors.ts";

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

/**
 * Creates a pair of functions for accessing private instance methods from outside the class.
 * Uses a WeakMap to store bound methods, ensuring proper memory cleanup.
 *
 * @returns An object with `call` and `register` functions
 *   - call: Calls the private method on an instance
 *   - register: Registers a private method for an instance (call in constructor)
 *
 * @example
 * ```ts
 * const privateAccessor = createPrivateAccessor<
 *   MyClass,
 *   [arg1: string, arg2: number],
 *   ReturnType
 * >();
 *
 * class MyClass {
 *   constructor() {
 *     privateAccessor.register(this, this.#privateMethod.bind(this));
 *   }
 *   #privateMethod(arg1: string, arg2: number): ReturnType { ... }
 * }
 *
 * // External usage with proper JSDoc:
 * \/**
 *  * Documentation here
 *  *\/
 * export const callPrivateMethod = privateAccessor.call;
 * ```
 */
export function createPrivateAccessor<Instance extends WeakKey, Args extends any[], Return>() {
	const registry = new WeakMap<Instance, (...args: Args) => Return>();

	return {
		/**
		 * Registers a private method for an instance. Call this in the constructor.
		 */
		register: (instance: Instance, method: (...args: Args) => Return): void => {
			registry.set(instance, method);
		},

		/**
		 * Calls the private method on an instance.
		 */
		call: (instance: Instance, ...args: Args): Return => {
			const method = registry.get(instance);
			if (!method) {
				throw new InvalidInstanceError();
			}
			return method(...args);
		},
	};
}
