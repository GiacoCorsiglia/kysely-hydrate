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

// export type IsNullable<T, K extends KeyBy<T>> = K extends keyof T
// 	? null extends T[K]
// 		? true
// 		: false
// 	: K extends readonly (keyof T)[]
// 		? true extends {
// 				[P in K[number]]: null extends T[P] ? true : false;
// 			}[K[number]]
// 			? true
// 			: false
// 		: false;

// type blah = IsNullable<{ a: number; b: string | null; c: false }, "a">;
