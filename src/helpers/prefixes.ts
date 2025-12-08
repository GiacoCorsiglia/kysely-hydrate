type SEP = typeof SEP;
const SEP = "$$";

export type MakePrefix<
	ParentPrefix extends string,
	Prefix extends string = "",
> = `${ParentPrefix}${Prefix}${SEP}`;

/**
 * Creates a sub-prefix.
 *
 * ```ts
 * makePrefix("", "name") => "name$$"
 * makePrefix("prefix$$", "name") => "prefix$$name$$"
 * ```
 */
export function makePrefix<ParentPrefix extends string, Prefix extends string>(
	prefix: ParentPrefix,
	key: Prefix,
): MakePrefix<ParentPrefix, Prefix> {
	return `${prefix}${key}${SEP}`;
}

/**
 * Indicates whether a string has any prefixes from `makePrefix`applied to it.
 */
export function hasAnyPrefix(string: string): boolean {
	return string.includes(SEP);
}

export type ApplyPrefix<Prefix extends string, Key extends string = ""> = `${Prefix}${Key}`;

/**
 * Applies a prefix to a key.
 *
 * ```ts
 * applyPrefix("prefix_", "key") => "prefix_key"
 * ```
 */
export function applyPrefix<Prefix extends string, Key extends string>(
	prefix: Prefix,
	key: Key,
): ApplyPrefix<Prefix, Key> {
	return prefix === "" ? (key as ApplyPrefix<Prefix, Key>) : `${prefix}${key}`;
}

type RemovePrefix<
	Prefix extends string,
	Key extends string,
> = Key extends `${Prefix}${infer Suffix}` ? Suffix : Key;

export function removePrefix<Prefix extends string, Key extends string>(
	prefix: Prefix,
	key: Key,
): RemovePrefix<Prefix, Key> {
	return key.slice(prefix.length) as RemovePrefix<Prefix, Key>;
}

export function hasPrefix<Prefix extends string, Key extends string>(
	prefix: Prefix,
	key: Key,
	// @ts-expect-error Force allow this return type.
): key is ApplyPrefix<Prefix, string> {
	return key.startsWith(prefix);
}

/**
 * Reads a value from an object, with a prefix applied to the key.
 */
export function getPrefixedValue<P extends string, T, K extends string>(
	prefix: P,
	input: T,
	key: K,
): unknown {
	return input[applyPrefix(prefix, key) as keyof T];
}

/**
 * Applies a prefix to all keys in a type.
 */
export type ApplyPrefixes<Prefix extends string, T> = {
	[K in keyof T & string as `${Prefix}${K}`]: T[K];
};

/**
 * Extracts from type `T` only the properties that are prefixed with `P`, and removes the prefix
 */
export type SelectAndStripPrefix<P extends string, T> = {
	[K in keyof T as K extends `${P}${infer Suffix}` ? Suffix : never]: T[K];
};

export function createdPrefixedAccessor<P extends string, T extends object>(
	prefix: P,
	input: T,
): SelectAndStripPrefix<P, T> {
	// In this case, we don't need to apply any prefixing.
	if (prefix === "") {
		return input as SelectAndStripPrefix<P, T>;
	}

	return new Proxy(input, {
		get(target, key) {
			return getPrefixedValue(prefix, target, key as string);
		},

		has(target, key) {
			return applyPrefix(prefix, key as string) in target;
		},

		getOwnPropertyDescriptor(target, key) {
			return Reflect.getOwnPropertyDescriptor(target, applyPrefix(prefix, key as string));
		},

		ownKeys(target) {
			const ownKeys = Reflect.ownKeys(target);
			const result: string[] = [];
			for (const key of ownKeys) {
				if (typeof key === "string" && hasPrefix(prefix, key)) {
					result.push(removePrefix(prefix, key));
				}
			}
			return result;
		},
	}) as SelectAndStripPrefix<P, T>;
}
