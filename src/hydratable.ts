/** biome-ignore-all lint/suspicious/noExplicitAny: Many generics afoot. */
import {
	applyPrefix,
	createdPrefixedAccessor,
	getPrefixedValue,
	type SelectAndStripPrefix,
} from "./helpers/prefixes.ts";
import { type Extend, isIterable, type KeyBy } from "./helpers/utils.ts";

type Fields<Input> = {
	[K in keyof Input]?: true | ((value: Input[K]) => unknown);
};

type InferFields<Input, F extends Fields<Input>> = {
	[K in keyof F & keyof Input]: F[K] extends true
		? Input[K]
		: F[K] extends (...args: any) => infer R
			? R
			: never;
};

type Extras<Input> = Record<string, (input: Input) => unknown>;

type InferExtras<Input, E extends Extras<Input>> = {
	[K in keyof E]: ReturnType<E[K]>;
};

export type CollectionMode = "many" | "one" | "oneOrThrow";

interface Collection<ChildInput, ChildOutput> {
	/**
	 * The mode of the nested entity: "one" (or "oneOrThrow") for a single
	 * object, "many" for an array.
	 */
	readonly mode: CollectionMode;
	readonly prefix: string;
	readonly hydratable: Hydratable<ChildInput, ChildOutput>;
}

type Collections = Record<PropertyKey, Collection<any, any>>;

interface HydratableProps<Input> {
	/**
	 * The key(s) to group by for this entity.
	 * Can be a single key or an array of keys for composite keys.
	 */
	readonly keyBy: KeyBy<Input>;

	/**
	 * The fields to include in the final denormalized entity.  You can either specify `true` to
	 * include a field as-is, or provide a transformation function to modify the field's value.
	 */
	readonly fields?: Fields<Input>;

	/**
	 * Extra fields generated from the entire input.
	 */
	readonly extras?: Extras<Input>;

	/**
	 * An optional map of nested collections.
	 */
	readonly collections?: Collections;
}

type HydratableArg<Input, Output> =
	| Hydratable<Input, Output>
	| ((keyBy: typeof createHydratable<Input>) => Hydratable<Input, Output>);

type ChildHydratableArg<P extends string, ParentInput, ChildOutput> =
	| Hydratable<SelectAndStripPrefix<P, ParentInput>, ChildOutput>
	| ((
			keyBy: typeof createHydratable<SelectAndStripPrefix<P, ParentInput>>,
	  ) => Hydratable<SelectAndStripPrefix<P, ParentInput>, ChildOutput>);

export type { Hydratable };
class Hydratable<Input, Output> {
	#props: HydratableProps<Input>;

	constructor(props: HydratableProps<Input>) {
		this.#props = props;
	}

	//
	// Configuration.
	//

	fields<F extends Fields<Input>>(
		fields: F,
	): Hydratable<Input, Extend<Output, InferFields<Input, F>>> {
		return new Hydratable({
			...this.#props,
			fields: {
				...this.#props.fields,
				...fields,
			},
		}) as any;
	}

	extras<E extends Extras<Input>>(
		extras: E,
	): Hydratable<Input, Extend<Output, InferExtras<Input, E>>> {
		return new Hydratable({
			...this.#props,
			extras: {
				...this.#props.extras,
				...extras,
			},
		}) as any;
	}

	has<K extends string, P extends string, ChildOutput>(
		mode: "many",
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput[] }>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: "one",
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput | null }>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: "oneOrThrow",
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput }>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: CollectionMode,
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<
		Input,
		Extend<Output, { [_ in K]: ChildOutput[] | ChildOutput | null }>
	>;
	has<K extends string, ChildOutput>(
		mode: CollectionMode,
		key: K,
		prefix: string,
		hydratable: ChildHydratableArg<any, Input, ChildOutput>,
	): Hydratable<Input, any> {
		const collection: Collection<any, ChildOutput> = {
			prefix,
			mode,
			hydratable:
				typeof hydratable === "function"
					? hydratable(createHydratable)
					: hydratable,
		};

		return new Hydratable({
			...this.#props,

			collections: {
				...this.#props.collections,

				[key]: collection,
			},
		}) as any;
	}

	hasMany<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput[] }>> {
		return this.has("many", key, prefix, hydratable) as any;
	}

	hasOne<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput | null }>> {
		return this.has("one", key, prefix, hydratable) as any;
	}

	hasOneOrThrow<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput }>> {
		return this.has("oneOrThrow", key, prefix, hydratable) as any;
	}

	//
	// Hydration.
	//

	#hydrateOne(
		prefix: string,
		input: Input,
		inputRows: Input[] = [input],
	): Output {
		const { fields, extras, collections } = this.#props;

		const entity: any = {};

		if (fields) {
			for (const key in fields) {
				if (Object.hasOwn(fields, key)) {
					const field = fields[key];

					if (field === undefined) {
						continue;
					}

					const value = getPrefixedValue(prefix, input, key);
					entity[key] = field === true ? value : field(value as any);
				}
			}
		}

		if (extras) {
			const accessor = createdPrefixedAccessor(prefix, input as object);

			for (const key in extras) {
				if (Object.hasOwn(extras, key)) {
					const extra = extras[key];

					if (extra === undefined) {
						continue;
					}

					entity[key] = extra(accessor as Input);
				}
			}
		}

		if (collections) {
			for (const key in collections) {
				if (Object.hasOwn(collections, key)) {
					const collection = collections[key];

					if (collection === undefined) {
						continue;
					}

					const childPrefix = applyPrefix(prefix, collection.prefix);

					const collectionOutputs = collection.hydratable.#hydrateMany(
						childPrefix,
						inputRows,
					);

					if (collection.mode === "many") {
						entity[key] = collectionOutputs;
					} else {
						const [first] = collectionOutputs;
						if (first === undefined) {
							if (collection.mode === "oneOrThrow") {
								throw new Error(
									`Expected one item, but got none for key ${key}`,
								);
							}

							entity[key] = null;
						} else {
							entity[key] = first;
						}
					}
				}
			}
		}

		return entity;
	}

	#hydrateMany(prefix: string, inputs: Iterable<Input>): Output[] {
		const { keyBy, collections } = this.#props;

		const result: Output[] = [];

		// If there are no collections, we can skip grouping.
		if (!collections) {
			for (const input of inputs) {
				// Ensure that the input exists in this row.  This check is necessary
				// here but unnecessary below because the groupByKey function will
				// already skip rows with null keys.
				if (isKeyNil(getKey(prefix, input, keyBy))) {
					continue;
				}

				result.push(this.#hydrateOne(prefix, input));
			}
			return result;
		}

		const grouped = groupByKey(prefix, inputs, keyBy);
		for (const groupRows of grouped) {
			// We assume the first row is representative of the group, at least for
			// the top-level entity (not nested collections).
			//
			// biome-ignore lint/style/noNonNullAssertion: One row exists or the group would not exist.
			result.push(this.#hydrateOne(prefix, groupRows[0]!, groupRows));
		}
		return result;
	}

	hydrate(input: Iterable<Input>): Output[];
	hydrate(input: Input | Iterable<Input>): Output | Output[];
	hydrate(input: Input): Output;
	hydrate(input: Input | Iterable<Input>): Output | Output[] {
		if (isIterable(input)) {
			return this.#hydrateMany("", input);
		}

		return this.#hydrateOne("", input);
	}
}

/**
 * Creates a new Hydratable---a configuration for how to hydrate an entity into
 * a denormalized structure.
 *
 * @param keyBy - The key(s) to group by for this entity.
 */
export const createHydratable = <T = {}>(
	keyBy: KeyBy<NoInfer<T>>,
): Hydratable<T, {}> => new Hydratable({ keyBy });

/**
 * Hydrates an entity or collection of entities into a denormalized structure
 * per the given Hydratable configuration.
 *
 * You may provide a function as the second argument to create a Hydratable on the fly.
 */
export function hydrate<Input, Output>(
	input: readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Output[];
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Output | Output[];
export function hydrate<Input, Output>(
	input: Input,
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Output;
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Output | Output[] {
	hydratable =
		typeof hydratable === "function"
			? hydratable(createHydratable)
			: hydratable;

	return hydratable.hydrate(input);
}

/**
 * Determines if a key is nil, meaning the corresponding object does not exist.
 */
function isKeyNil(key: unknown): key is null | undefined {
	return key === null || key === undefined;
}

const KEY_SEPARATOR = "::";

/**
 * Gets the key for an entity from the input.
 *
 * Expected to return values that are good for use as a key in a Map, but not
 * guaranteed to do so depending on the input object
 */
function getKey(
	prefix: string,
	input: unknown,
	keyBy: string | readonly string[],
): unknown {
	if (typeof keyBy !== "object") {
		return getPrefixedValue(prefix, input, keyBy);
	}

	const values: unknown[] = [];
	for (const partKey of keyBy) {
		const value = getPrefixedValue(prefix, input, partKey);
		if (isKeyNil(value)) {
			return null; // A null part invalidates the whole key for this entity
		}
		values.push(value);
	}
	return values.join(KEY_SEPARATOR);
}

/**
 * Groups rows by the entity's key.
 */
function groupByKey<T>(
	prefix: string,
	inputs: Iterable<T>,
	keyBy: string | readonly string[],
): Iterable<T[]> {
	const map = new Map<unknown, T[]>();

	for (const input of inputs) {
		const key = getKey(prefix, input, keyBy);
		// Skip rows with null keys.
		if (isKeyNil(key)) {
			continue;
		}
		let arr = map.get(key);
		if (arr === undefined) {
			arr = [];
			map.set(key, arr);
		}
		arr.push(input);
	}

	return map.values();
}
