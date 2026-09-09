export type SEP = typeof SEP;
export const SEP = "$$";

export type MakePrefix<
	ParentPrefix extends string,
	Prefix extends string = "",
> = `${ParentPrefix}${Prefix}${SEP}`;

export type MakeInitialPrefix<Prefix extends string = ""> = `${Prefix}${SEP}`;

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

export type ApplyPrefix<Prefix extends string, Key extends string = ""> = `${Prefix}${Key}`;

export type ApplyPrefixWithSep<
	Prefix extends string,
	Key extends string = "",
> = `${Prefix}${SEP}${Key}`;

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

/**
 * State for one accessor, held on the Proxy target so that every accessor can
 * share {@link accessorHandler} instead of allocating a handler per row.
 */
interface AccessorTarget {
	readonly prefix: string;
	readonly input: object;
}

/**
 * Traps for {@link createdPrefixedAccessor}.
 *
 * The Proxy target is {@link AccessorTarget}, not the input row, because Proxy
 * invariants are enforced against the target: `ownKeys` must report a
 * non-extensible target's own keys exactly, which a trap that hides the keys of
 * other prefixes cannot do.  Proxying a frozen input row that way throws a
 * TypeError on any enumeration.  A target we own is always extensible, and its
 * own keys are configurable, so no invariant constrains these traps.
 */
const accessorHandler: ProxyHandler<AccessorTarget> = {
	get({ prefix, input }, key) {
		// Inlined rather than getPrefixedValue: this runs on every property read
		// in a nested callback, and the prefix is never empty here.
		return (input as Record<string, unknown>)[prefix + (key as string)];
	},

	set({ prefix, input }, key, value) {
		// Without this trap the write would bypass the prefix and land on the
		// input row unprefixed, at the wrong nesting level.
		return Reflect.set(input, applyPrefix(prefix, key as string), value);
	},

	defineProperty({ prefix, input }, key, descriptor) {
		return Reflect.defineProperty(input, applyPrefix(prefix, key as string), descriptor);
	},

	deleteProperty({ prefix, input }, key) {
		return Reflect.deleteProperty(input, applyPrefix(prefix, key as string));
	},

	has({ prefix, input }, key) {
		return applyPrefix(prefix, key as string) in input;
	},

	getOwnPropertyDescriptor({ prefix, input }, key) {
		const descriptor = Reflect.getOwnPropertyDescriptor(input, applyPrefix(prefix, key as string));
		// A descriptor for a property the target does not have may not report
		// non-configurability, which a frozen input row's descriptors do.  The
		// accessor's view of the property is configurable regardless of how the
		// row holds it.
		return descriptor === undefined ? undefined : { ...descriptor, configurable: true };
	},

	ownKeys({ prefix, input }) {
		const result: string[] = [];
		for (const key of Reflect.ownKeys(input)) {
			if (typeof key === "string" && hasPrefix(prefix, key)) {
				result.push(removePrefix(prefix, key));
			}
		}
		return result;
	},
};

export function createdPrefixedAccessor<P extends string, T extends object>(
	prefix: P,
	input: T,
): SelectAndStripPrefix<P, T> {
	// In this case, we don't need to apply any prefixing.
	if (prefix === "") {
		return input as SelectAndStripPrefix<P, T>;
	}

	// The Proxy presents the row's view of the target, not the target itself.
	return new Proxy(
		{ prefix, input } satisfies AccessorTarget,
		accessorHandler,
	) as unknown as SelectAndStripPrefix<P, T>;
}
