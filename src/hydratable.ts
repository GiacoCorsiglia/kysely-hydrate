import { ExpectedOneItemError } from "./helpers/errors.ts";
import {
	applyPrefix,
	createdPrefixedAccessor,
	getPrefixedValue,
	type SelectAndStripPrefix,
} from "./helpers/prefixes.ts";
import { addObjectToMap, type Extend, isIterable, type KeyBy } from "./helpers/utils.ts";

/**
 * Configuration for fields to include in the hydrated output.
 * Each field can be set to `true` to include as-is, or a function to transform the value.
 */
type Fields<Input> = {
	[K in keyof Input]?: true | ((value: Input[K]) => unknown);
};

/**
 * Infers the output type for fields based on the Fields configuration.
 * Fields set to `true` keep their original type, while functions use their return type.
 */
type InferFields<Input, F extends Fields<Input>> = {
	[K in keyof F & keyof Input]: F[K] extends true
		? Input[K]
		: F[K] extends (...args: any) => infer R
			? R
			: never;
};

/**
 * Configuration for extra fields to compute from the entire input.
 * Each extra is a function that receives the full input and returns a computed value.
 */
type Extras<Input> = Record<string, (input: Input) => unknown>;

/**
 * Infers the output type for extras based on the Extras configuration.
 * Uses the return type of each extra function.
 */
type InferExtras<Input, E extends Extras<Input>> = {
	[K in keyof E]: ReturnType<E[K]>;
};

/**
 * The mode of a collection.
 *
 * - "many": The collection is an array of objects.
 * - "one": The collection is a single nullable object.
 * - "oneOrThrow": The collection is a single non-nullable object, and an error
 *   is thrown if the object is null when hydrating.
 */
export type CollectionMode = "many" | "one" | "oneOrThrow";

/**
 * Configuration for a nested collection.
 */
interface Collection<ChildInput, ChildOutput> {
	/**
	 * The mode of the nested entity: "one" (or "oneOrThrow") for a single
	 * object, "many" for an array.
	 */
	readonly mode: CollectionMode;
	/**
	 * The prefix to use for the nested collection.
	 */
	readonly prefix: string;
	/**
	 * The Hydratable to use when hydrating the objects in the nested collection.
	 */
	readonly hydratable: Hydratable<ChildInput, ChildOutput>;
}

/**
 * Async function that fetches and hydrates data to attach. Called exactly once with
 * all parent inputs to avoid N+1 queries. Should return already-hydrated data.
 */
export type FetchFn<ParentInput, AttachedOutput> = (
	inputs: ParentInput[],
) => Iterable<AttachedOutput> | Promise<Iterable<AttachedOutput>>;

/**
 * Input argument for configuring the keys to use for matching attached data to parents.
 */
export interface AttachedKeysArg<ParentInput, AttachedOutput> {
	/**
	 * The key(s) on the attached child output to use for matching to parents.
	 */
	readonly keyBy: KeyBy<AttachedOutput>;
	/**
	 * The key(s) on the parent input to compare with the attached child output's
	 * key.  If not provided, the parent's keyBy will be used.
	 */
	readonly compareTo?: KeyBy<ParentInput> | undefined;
}

/**
 * Configuration for an attached collection.
 */
interface AttachedCollection<ParentInput, AttachedOutput> {
	/**
	 * The mode of the attached collection: "one" (or "oneOrThrow") for a single
	 * object, "many" for an array.
	 */
	readonly mode: CollectionMode;
	/**
	 * Async function that fetches and hydrates the data to attach. Called exactly once with
	 * all parent inputs to avoid N+1 queries. Should return already-hydrated data.
	 */
	readonly fetchFn: FetchFn<ParentInput, AttachedOutput>;
	/**
	 * The key(s) on the attached child output to use for matching to parents.
	 */
	readonly keyBy: KeyBy<AttachedOutput>;
	/**
	 * The key(s) on the parent input to compare with the attached child output's key.
	 */
	readonly compareTo: KeyBy<ParentInput>;
}

/**
 * Internal map type for fields configuration.
 */
type FieldsMap = Map<string, true | ((value: any) => unknown)>;

/**
 * Internal map type for extras configuration.
 */
type ExtrasMap = Map<string, (input: any) => unknown>;

/**
 * Internal map type for nested collections configuration.
 */
type CollectionsMap = Map<string, Collection<any, any>>;

/**
 * Internal map type for attached collections configuration.
 */
type AttachedCollectionsMap = Map<string, AttachedCollection<any, any>>;

/**
 * Internal configuration for a Hydratable.
 */
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
	readonly fields?: FieldsMap;

	/**
	 * Extra fields generated from the entire input.
	 */
	readonly extras?: ExtrasMap;

	/**
	 * An optional map of nested collections.
	 */
	readonly collections?: CollectionsMap;

	/**
	 * An optional map of attached collections (for application-level joins).
	 */
	readonly attachedCollections?: AttachedCollectionsMap;
}

/**
 * A Hydratable instance or a function that creates one.
 * Used to allow inline Hydratable creation in method calls.
 */
type HydratableArg<Input, Output> =
	| Hydratable<Input, Output>
	| ((keyBy: typeof createHydratable<Input>) => Hydratable<Input, Output>);

/**
 * A Hydratable instance for a child collection or a function that creates one.
 * The input type is automatically prefixed based on the parent's prefix.
 */
type ChildHydratableArg<P extends string, ParentInput, ChildOutput> =
	| Hydratable<SelectAndStripPrefix<P, ParentInput>, ChildOutput>
	| ((
			keyBy: typeof createHydratable<SelectAndStripPrefix<P, ParentInput>>,
	  ) => Hydratable<SelectAndStripPrefix<P, ParentInput>, ChildOutput>);

export type { Hydratable };
/**
 * A configuration for how to hydrate flat database rows into a denormalized structure.
 *
 * The Hydratable class provides a fluent API for configuring:
 * - Fields to include (with optional transformations)
 * - Extra computed fields
 * - Nested collections (using `has()` methods)
 * - Attached collections (using `attach()` methods)
 *
 * Once configured, call `hydrate()` to transform input data into the denormalized output.
 *
 * @template Input - The type of the input data (typically from a database query)
 * @template Output - The type of the hydrated output structure
 */
class Hydratable<Input, Output> {
	#props: HydratableProps<Input>;

	constructor(props: HydratableProps<Input>) {
		this.#props = props;
	}

	//
	// Configuration.
	//

	/**
	 * Configures which fields to include in the hydrated output.
	 *
	 * @param fields - An object mapping field names to either `true` (include as-is)
	 *   or a transformation function
	 * @returns A new Hydratable with the fields configuration merged
	 */
	fields<F extends Fields<Input>>(
		fields: F,
	): Hydratable<Input, Extend<Output, InferFields<Input, F>>> {
		return new Hydratable({
			...this.#props,

			fields: addObjectToMap(this.#props.fields, fields),
		}) as any;
	}

	/**
	 * Configures extra computed fields to add to the hydrated output.
	 *
	 * @param extras - An object mapping field names to functions that compute
	 *   the field value from the entire input
	 * @returns A new Hydratable with the extras configuration merged
	 */
	extras<E extends Extras<Input>>(
		extras: E,
	): Hydratable<Input, Extend<Output, InferExtras<Input, E>>> {
		return new Hydratable({
			...this.#props,

			extras: addObjectToMap(this.#props.extras, extras),
		}) as any;
	}

	/**
	 * Configures a nested collection that exists in the same query result. The
	 * child data is expected to be prefixed in the input (e.g., `posts$$id`,
	 * `posts$$title`) with the given `prefix`.
	 *
	 * You may prefer to use the shorthand methods: {@link hasMany},
	 * {@link hasOne}, or {@link hasOneOrThrow}.
	 *
	 * @param mode - The collection mode: "many" for arrays, "one" for nullable
	 *   single, or "oneOrThrow" for non-nullable single.
	 * @param key - The property name for the collection in the output.
	 * @param prefix - The prefix used in the input data (e.g., "posts$$").
	 * @param hydratable - The Hydratable configuration for the child entities, or
	 *   a function that creates one.
	 * @returns A new Hydratable with the nested collection added.
	 */
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
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput[] | ChildOutput | null }>>;
	has<K extends string, ChildOutput>(
		mode: CollectionMode,
		key: K,
		prefix: string,
		hydratable: ChildHydratableArg<any, Input, ChildOutput>,
	): Hydratable<Input, any> {
		return new Hydratable({
			...this.#props,

			collections: new Map(this.#props.collections).set(key, {
				prefix,
				mode,
				hydratable: typeof hydratable === "function" ? hydratable(createHydratable) : hydratable,
			} satisfies Collection<any, ChildOutput>),
		}) as any;
	}

	/**
	 * Shorthand for `has("many", ...)` - configures a nested array collection.
	 *
	 * @param key - The key name for the collection in the output
	 * @param prefix - The prefix used in the input data (e.g., "posts$$")
	 * @param hydratable - The Hydratable configuration for the child entities
	 * @returns A new Hydratable with the nested collection added
	 */
	hasMany<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput[] }>> {
		return this.has("many", key, prefix, hydratable) as any;
	}

	/**
	 * Shorthand for `has("one", ...)` - configures a nested nullable single entity.
	 *
	 * @param key - The key name for the entity in the output
	 * @param prefix - The prefix used in the input data (e.g., "author$$")
	 * @param hydratable - The Hydratable configuration for the child entity
	 * @returns A new Hydratable with the nested entity added
	 */
	hasOne<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput | null }>> {
		return this.has("one", key, prefix, hydratable) as any;
	}

	/**
	 * Shorthand for `has("oneOrThrow", ...)` - configures a nested non-nullable single entity.
	 * Throws an error if the entity is not found during hydration.
	 *
	 * @param key - The key name for the entity in the output.
	 * @param prefix - The prefix used in the input data (e.g., "author$$").
	 * @param hydratable - The Hydratable configuration for the child entity.
	 * @returns A new Hydratable with the nested entity added.
	 */
	hasOneOrThrow<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydratable: ChildHydratableArg<P, Input, ChildOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: ChildOutput }>> {
		return this.has("oneOrThrow", key, prefix, hydratable) as any;
	}

	/**
	 * Configures an attached collection that is fetched from an external source.
	 * The `fetchFn` is called exactly once per hydration with all parent inputs
	 * to avoid N+1 queries, even when this hydratable is nested within another.
	 *
	 * For convenience, you may prefer to use the shorthand methods:
	 * {@link attachMany}, {@link attachOne}, or {@link attachOneOrThrow}.
	 *
	 * @param mode - The collection mode: "many" for arrays, "one" for nullable
	 *   single, or "oneOrThrow" for non-nullable single.
	 * @param key - The property name for the collection in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 *   Called with all parent inputs and should return already-hydrated data.
	 * @param keyBy - The key(s) on the attached output to use for matching to the
	 *   parent input
	 * @param matchBy - The key(s) on the parent input to compare with the
	 *   attached child's key.
	 * @returns A new Hydratable with the attached collection added.
	 */
	attach<K extends string, AttachedOutput>(
		mode: "many",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput[] }>>;
	attach<K extends string, AttachedOutput>(
		mode: "one",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput | null }>>;
	attach<K extends string, AttachedOutput>(
		mode: "oneOrThrow",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput }>>;
	attach<K extends string, AttachedOutput>(
		mode: CollectionMode,
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput[] | AttachedOutput | null }>>;
	attach<K extends string, AttachedOutput>(
		mode: CollectionMode,
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, any> {
		return new Hydratable({
			...this.#props,

			attachedCollections: new Map(this.#props.attachedCollections).set(key, {
				mode,
				fetchFn,
				keyBy: keys.keyBy,
				compareTo: keys.compareTo ?? this.#props.keyBy,
			} satisfies AttachedCollection<Input, AttachedOutput>),
		}) as any;
	}

	/**
	 * Shorthand for `attach("many", ...)` - configures an attached array collection.
	 *
	 * @param key - The property name for the collection in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 * @param keyBy - The key(s) on the attached output to use for matching to parents.
	 * @param matchBy - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydratable with the attached collection added.
	 */
	attachMany<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput[] }>> {
		return this.attach("many", key, fetchFn, keys) as any;
	}

	/**
	 * Shorthand for `attach("one", ...)` - configures an attached nullable single entity.
	 *
	 * @param key - The property name for the entity in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 * @param keyBy - The key(s) on the attached output to use for matching to parents.
	 * @param matchBy - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydratable with the attached entity added.
	 */
	attachOne<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput | null }>> {
		return this.attach("one", key, fetchFn, keys) as any;
	}

	/**
	 * Shorthand for `attach("oneOrThrow", ...)` - configures an attached non-nullable single entity.
	 * Throws an error if the entity is not found during hydration.
	 *
	 * @param key - The property name for the entity in the output
	 * @param fetchFn - A function that fetches and hydrates the attached data
	 * @param keyBy - The key(s) on the attached output to use for matching to parents
	 * @param matchBy - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydratable with the attached entity added
	 */
	attachOneOrThrow<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): Hydratable<Input, Extend<Output, { [_ in K]: AttachedOutput }>> {
		return this.attach("oneOrThrow", key, fetchFn, keys) as any;
	}

	//
	// Hydration.
	//

	/**
	 * Fetches all attach collections (including nested ones) and groups them by match key.
	 * This is the only async operation needed - everything else can work with the resulting map.
	 * Uses prefixed keys for nested collections (e.g., "posts$$comments" for nested comments).
	 *
	 * Writes directly to the provided attachedDataMap and fetchPromises array.
	 */
	#fetchAllAttachedCollections(
		prefix: string,
		inputs: Iterable<Input>,
		attachedDataMap: Map<string, Map<unknown, any[]>>,
		fetchPromises: Promise<void>[],
	): void {
		const { attachedCollections, collections } = this.#props;

		// Fetch attach collections at this level
		if (attachedCollections) {
			// We have to convert to an array because that's what the fetchFn expects.
			// We also need to map and convert the input to prefixed accessors if we
			// are nested, because the fetchFn expects unprefixed inputs.
			let inputArray: any[];
			if (prefix !== "") {
				inputArray = [];
				for (const input of inputs) {
					inputArray.push(createdPrefixedAccessor(prefix, input as object));
				}
			} else if (Array.isArray(inputs)) {
				inputArray = inputs;
			} else {
				inputArray = Array.from(inputs);
			}

			for (const [key, attachedCollection] of attachedCollections) {
				// Use prefixed key for the map
				const mapKey = prefix ? applyPrefix(prefix, key) : key;

				// Create fetch promise
				fetchPromises.push(
					Promise.resolve(attachedCollection.fetchFn(inputArray)).then((attachedOutputs) => {
						// Group fetched rows by their match key
						const grouped = groupByKey(
							"", // Always unprefixed.
							attachedOutputs,
							attachedCollection.keyBy,
						);

						attachedDataMap.set(mapKey, grouped);
					}),
				);
			}
		}

		// Recursively fetch attach collections from nested collections
		if (collections) {
			for (const collection of collections.values()) {
				const childPrefix = applyPrefix(prefix, collection.prefix);

				// Recursively fetch nested attach collections (write directly to the same map).
				collection.hydratable.#fetchAllAttachedCollections(
					childPrefix,
					inputs,
					attachedDataMap,
					fetchPromises,
				);
			}
		}
	}

	/**
	 * Hydrates a single entity. All attach collections are already fetched and provided in attachedDataMap.
	 */
	#hydrateOne(
		prefix: string,
		attachedDataMap: Map<string, Map<unknown, any[]>>,
		input: Input,
		inputRows: Input[],
	): Output {
		const { fields, extras, collections, attachedCollections } = this.#props;

		const entity: any = {};

		if (fields) {
			for (const [key, field] of fields) {
				const value = getPrefixedValue(prefix, input, key);
				entity[key] = field === true ? value : field(value as any);
			}
		}

		if (extras) {
			const accessor = createdPrefixedAccessor(prefix, input as object);

			for (const [key, extra] of extras) {
				entity[key] = extra(accessor as Input);
			}
		}

		if (collections) {
			for (const [key, collection] of collections) {
				const childPrefix = applyPrefix(prefix, collection.prefix);

				// Hydrate nested collections (all attach collections already fetched)
				const collectionOutputs = collection.hydratable.#hydrateMany(
					childPrefix,
					inputRows,
					attachedDataMap,
				);

				entity[key] = applyCollectionMode(collectionOutputs, collection.mode, key);
			}
		}

		// Attach collections from the provided map
		if (attachedCollections) {
			for (const [key, collection] of attachedCollections) {
				// Get the match value from this input using the matchBy.
				const inputKey = getKey(prefix, input, collection.compareTo);

				// Use prefixed key to look up in the map
				const mapKey = prefix ? applyPrefix(prefix, key) : key;

				// Look up attached rows with matching key (already hydrated)
				const groupedData = attachedDataMap.get(mapKey);
				const attachedRows = groupedData?.get(inputKey);

				entity[key] = applyCollectionMode(attachedRows, collection.mode, key);
			}
		}

		return entity;
	}

	/**
	 * Hydrates many entities. All attach collections are already fetched and provided in attachedDataMap.
	 */
	#hydrateMany(
		prefix: string,
		inputs: Iterable<Input>,
		attachedDataMap: Map<string, Map<unknown, any[]>>,
	): Output[] {
		const { keyBy, collections } = this.#props;

		const result: Output[] = [];

		// If there are no collections, we can skip grouping, because each input
		// must correspond to a different top-level entity.  It's safe to do this
		// even if there are attached collections, because those will be specified
		// in their own data arrays.
		if (!collections) {
			for (const input of inputs) {
				// Ensure that the input exists in this row.  This check is necessary
				// here but unnecessary below because the groupByKey function will
				// already skip rows with null keys.
				const inputKey = getKey(prefix, input, keyBy);
				if (isKeyNil(inputKey)) {
					continue;
				}

				const entity = this.#hydrateOne(prefix, attachedDataMap, input, [input]);
				result.push(entity);
			}

			return result;
		}

		const grouped = groupByKey(prefix, inputs, keyBy);
		for (const groupRows of grouped.values()) {
			// We assume the first row is representative of the group, at least for
			// the top-level entity (not nested collections).
			const firstRow = groupRows[0]!;
			const entity = this.#hydrateOne(prefix, attachedDataMap, firstRow, groupRows);
			result.push(entity);
		}

		return result;
	}

	/**
	 * Hydrates the input data into a denormalized structure according to this configuration.
	 *
	 * If attached collections are configured, this method will fetch them asynchronously
	 * before performing the hydration. The method always returns a Promise for consistency.
	 *
	 * @param input - A single input entity or an iterable of input entities
	 * @returns A Promise that resolves to the hydrated output(s)
	 */
	hydrate(input: Iterable<Input>): Promise<Output[]>;
	hydrate(input: Input | Iterable<Input>): Promise<Output | Output[]>;
	hydrate(input: Input): Promise<Output>;
	hydrate(input: Input | Iterable<Input>): Promise<Output | Output[]> {
		// Fetch all attach collections upfront (this is the only async operation).
		// Start with empty prefix for top-level collections.
		const attachedDataMap = new Map<string, Map<unknown, any[]>>();
		const fetchPromises: Promise<void>[] = [];
		this.#fetchAllAttachedCollections(
			"",
			isIterable(input) ? input : [input],
			attachedDataMap,
			fetchPromises,
		);

		const hydrateWithData = () => {
			if (isIterable(input)) {
				return this.#hydrateMany("", input, attachedDataMap);
			}

			return this.#hydrateOne("", attachedDataMap, input, [input]);
		};

		return fetchPromises.length > 0
			? Promise.all(fetchPromises).then(hydrateWithData)
			: Promise.resolve(hydrateWithData());
	}
}

/**
 * Creates a new Hydratable---a configuration for how to hydrate an entity into
 * a denormalized structure.
 *
 * @param keyBy - The key(s) to group by for this entity.
 */
export const createHydratable = <T = {}>(keyBy: KeyBy<NoInfer<T>>): Hydratable<T, {}> =>
	new Hydratable({ keyBy });

/**
 * Hydrates an entity or collection of entities into a denormalized structure
 * per the given Hydratable configuration.
 *
 * You may provide a function as the second argument to create a Hydratable on the fly.
 *
 * Note: If the Hydratable uses `attachMany` or `attachOne` methods, this function
 * will return a Promise that must be awaited.
 */
export function hydrate<Input, Output>(
	input: readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Promise<Output[]>;
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Promise<Output | Output[]>;
export function hydrate<Input, Output>(
	input: Input,
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Promise<Output>;
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydratable: HydratableArg<NoInfer<Input>, Output>,
): Promise<Output | Output[]> {
	hydratable = typeof hydratable === "function" ? hydratable(createHydratable) : hydratable;

	return hydratable.hydrate(input);
}

/**
 * Applies collection mode logic (many/one/oneOrThrow) to collection outputs.
 */
function applyCollectionMode<T>(
	outputs: T[] | undefined,
	mode: CollectionMode,
	key: string,
): T[] | T | null {
	if (mode === "many") {
		return outputs ?? [];
	}

	const first = outputs?.[0];
	if (first !== undefined) {
		return first;
	}

	if (mode === "oneOrThrow") {
		throw new ExpectedOneItemError(key);
	}

	return null;
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
 * guaranteed to do so depending on the input object.
 */
function getKey(prefix: string, input: unknown, keyBy: string | readonly string[]): unknown {
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
): Map<unknown, T[]> {
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

	return map;
}
