import {
	CardinalityViolationError,
	ExpectedOneItemError,
	KeyByMismatchError,
} from "./helpers/errors.ts";
import { makeOrderByComparator, type OrderBy } from "./helpers/order-by.ts";
import {
	applyPrefix,
	createdPrefixedAccessor,
	getPrefixedValue,
	hasPrefix,
	removePrefix,
	type SelectAndStripPrefix,
} from "./helpers/prefixes.ts";
import {
	addObjectToMap,
	type Extend,
	type ExtendWith,
	isIterable,
	type KeyBy,
} from "./helpers/utils.ts";

////////////////////////////////////////////////////////////////////
// Optional keyBy when "id" is a valid key.
////////////////////////////////////////////////////////////////////

/**
 * The default key used for deduplication when not explicitly specified.
 * Only used when the input type has an "id" property.
 */
export const DEFAULT_KEY_BY = "id";
export type DEFAULT_KEY_BY = typeof DEFAULT_KEY_BY;

/**
 * Interface representing an input that has the default key property.
 * Used to constrain overloads where keyBy can be omitted.
 */
export interface InputWithDefaultKey {
	[DEFAULT_KEY_BY]: any;
}

/**
 * Helper type for a field mapping function.
 */
type MapFn<Input, K extends keyof Input> = (value: Input[K]) => unknown;

/**
 * Configuration for fields to include in the hydrated output.
 * Each field can be set to `true` to include as-is, or a function to transform the value.
 */
type Fields<Input> = {
	[K in keyof Input]?: true | MapFn<Input, K>;
};

/**
 * Configuration for field transformations.
 * Each field must be a transformation function (not `true` or `false`).
 */
export type FieldMappings<Input> = {
	[K in keyof Input]?: MapFn<Input, K>;
};

/**
 * Infers the output type for fields based on the Fields configuration.
 * Fields set to `true` keep their original type, while functions use their return type.
 */
export type InferFields<Input, F extends Fields<Input>> = {
	[K in keyof F & keyof Input]: F[K] extends (...args: any) => infer R ? R : Input[K];
};

/**
 * Configuration for extra fields to compute from the entire input.
 * Each extra is a function that receives the full input and returns a computed value.
 */
export type Extras<Input> = Record<string, (input: Input) => unknown>;

/**
 * Infers the output type for extras based on the Extras configuration.
 * Uses the return type of each extra function.
 */
export type InferExtras<Input, E extends Extras<Input>> = {
	[K in keyof E]: ReturnType<E[K]>;
};

/**
 * An extender function that receives the full input and returns an object
 * of computed properties to merge into the output.
 */
export type Extender<Input> = (input: Input) => Record<string, unknown>;

/**
 * Infers the output type for an extender function.
 */
export type InferExtender<Input, F extends Extender<Input>> = ReturnType<F>;

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
	 * The Hydrator to use when hydrating the objects in the nested collection.
	 */
	readonly hydrator: HydratorImpl<ChildInput, ChildOutput>;
}

/**
 * An executable, like a query builder.
 */
interface Executable<Output> {
	execute(): Promise<Output[]>;
}

/**
 * Tests if a value is executable.
 */
function isExecutable<Output>(value: unknown): value is Executable<Output> {
	return (
		typeof value === "object" && value !== null && typeof (value as any).execute === "function"
	);
}

/**
 * Async function that fetches and hydrates data to attach. Called exactly once
 * with one input per parent entity, to avoid N+1 queries.  Parent inputs are
 * deduplicated by the parent's `keyBy`, and rows with nil keys (e.g. phantom
 * all-null rows produced by matchless left joins) are excluded.  Should return
 * already-hydrated data.
 */
export type FetchFn<ParentInput, AttachedOutput> = (
	inputs: ParentInput[],
) =>
	| Iterable<AttachedOutput>
	| Promise<Iterable<AttachedOutput>>
	| Executable<AttachedOutput>
	| Promise<Executable<AttachedOutput>>;

/**
 * The allowed return types of a fetch function.
 */
export type SomeFetchFnReturn =
	| Iterable<any>
	| Promise<Iterable<any>>
	| Executable<any>
	| Promise<Executable<any>>;

/**
 * A fetch function that returns a value of type `FetchFnReturn`.
 */
export type SomeFetchFn<ParentInput, FetchFnReturn extends SomeFetchFnReturn> = (
	inputs: ParentInput[],
) => FetchFnReturn;

export type AttachedOutputFromFetchFnReturn<FetchFnReturn extends SomeFetchFnReturn> =
	Awaited<FetchFnReturn> extends Iterable<infer AttachedOutput> | Executable<infer AttachedOutput>
		? AttachedOutput
		: never;

/**
 * Input argument for configuring the keys to use for matching attached data to parents.
 */
export interface AttachedKeysArg<ParentInput, AttachedOutput> {
	/**
	 * The key(s) on the attached child output to use for matching to parents.
	 */
	readonly matchChild: KeyBy<AttachedOutput>;
	/**
	 * The key(s) on the parent input to compare with the attached child output's
	 * key.  If not provided, the parent's keyBy will be used.
	 */
	readonly toParent?: KeyBy<ParentInput> | undefined;
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
	readonly matchChild: KeyBy<AttachedOutput>;
	/**
	 * The key(s) on the parent input to compare with the attached child output's key.
	 */
	readonly toParent: KeyBy<ParentInput>;
}

/**
 * Internal map type for fields configuration.
 */
type FieldsMap = Map<string, true | false | ((value: any) => unknown)>;

/**
 * Internal map type for extras configuration.
 */
type ExtrasMap = Map<string, (input: any) => unknown>;

/**
 * Internal array type for extender functions.
 */
type ExtendersArray = Array<(input: any) => Record<string, unknown>>;

/**
 * Internal map type for nested collections configuration.
 */
type CollectionsMap = Map<string, Collection<any, any>>;

/**
 * Internal map type for attached collections configuration.
 */
type AttachedCollectionsMap = Map<string, AttachedCollection<any, any>>;

/**
 * Internal configuration for a Hydrator.
 */
interface HydratorProps<Input> {
	/**
	 * The key(s) to group by for this entity.
	 * Can be a single key or an array of keys for composite keys.
	 */
	readonly keyBy: KeyBy<Input>;

	/**
	 * The fields to include in the final denormalized entity.  You can either specify `true` to
	 * include a field as-is, or provide a transformation function to modify the field's value.
	 */
	readonly fields?: FieldsMap | undefined;

	/**
	 * Extra fields generated from the entire input.
	 */
	readonly extras?: ExtrasMap | undefined;

	/**
	 * Extender functions that return objects to merge into the output.
	 */
	readonly extenders?: ExtendersArray | undefined;

	/**
	 * An optional map of nested collections.
	 */
	readonly collections?: CollectionsMap | undefined;

	/**
	 * An optional map of attached collections (for application-level joins).
	 */
	readonly attachedCollections?: AttachedCollectionsMap | undefined;

	/**
	 * An optional array of map functions to apply to the hydrated output.
	 */
	readonly mapFns?: Array<(value: any) => any> | undefined;

	/**
	 * An optional array of orderings to apply during hydration.
	 */
	readonly orderings?: readonly OrderBy<Input>[] | undefined;

	/**
	 * Whether to append keyBy columns as the final ordering (tie-breaker).
	 * Undefined means it was never explicitly set (treated as false), which
	 * matters when composing hydrators via .with(): an explicit setting on
	 * either side survives composition, with the other hydrator's explicit
	 * setting taking precedence.
	 */
	readonly orderByKeys?: boolean | undefined;
}

/**
 * Type for createHydrator when Input extends InputWithDefaultKey or not.
 * Allows optional keyBy only if Input extends InputWithDefaultKey.
 */
interface CreateHydratorWithoutDefaultKey<Input> {
	(keyBy: KeyBy<Input>): FullHydrator<Input, {}>;
}

interface CreateHydratorWithDefaultKey<Input> extends CreateHydratorWithoutDefaultKey<Input> {
	(): FullHydrator<Input, {}>;
}

/**
 * Type for a createHydrator function scoped to a specific Input type.
 */
type CreateHydratorFn<Input> = Input extends InputWithDefaultKey
	? CreateHydratorWithDefaultKey<Input>
	: CreateHydratorWithoutDefaultKey<Input>;

/**
 * A function that creates a Hydrator.
 */
type HydratorFactory<Input, Output> = (
	create: CreateHydratorFn<Input>,
) => MappedHydrator<Input, Output>;

/**
 * A Hydrator instance or a function that creates one.
 * Used to allow inline Hydrator creation in method calls.
 */
type HydratorArg<Input, Output> = MappedHydrator<Input, Output> | HydratorFactory<Input, Output>;

/**
 * A Hydrator instance for a child collection or a function that creates one.
 * The input type is automatically prefixed based on the parent's prefix.
 */
type ChildHydratorArg<P extends string, ParentInput, ChildOutput> =
	| MappedHydrator<SelectAndStripPrefix<P, ParentInput>, ChildOutput>
	| ((
			create: CreateHydratorFn<SelectAndStripPrefix<P, ParentInput>>,
	  ) => MappedHydrator<SelectAndStripPrefix<P, ParentInput>, ChildOutput>);

const IsFullHydrator = Symbol("HydratorType");

/**
 * A configuration for how to hydrate flat database rows into a denormalized structure.
 *
 * The Hydrator class provides a fluent API for configuring:
 * - Fields to include (with optional transformations)
 * - Extra computed fields
 * - Nested collections (using `has()` methods)
 * - Attached collections (using `attach()` methods)
 * - Map functions to apply to the hydrated output
 *
 * Once configured, call `hydrate()` to transform input data into the denormalized output.
 *
 * @template Input - The type of the input data (typically from a database query)
 * @template Output - The type of the hydrated output structure
 */
export type Hydrator<Input, Output> = MappedHydrator<Input, Output> | FullHydrator<Input, Output>;

/**
 * Determines if a hydrator is a full hydrator, meaning it has not had a .map()
 * applied to it yet.
 *
 * @param hydrator - The hydrator to check
 * @returns True if the hydrator is a full hydrator, false otherwise
 */
export const isFullHydrator = <Input, Output>(
	hydrator: Hydrator<Input, Output>,
): hydrator is FullHydrator<Input, Output> => {
	return hydrator[IsFullHydrator];
};

/**
 * Asserts that a hydrator is a full hydrator and returns it.
 *
 * @internal
 */
export const asFullHydrator = <Input, Output>(
	hydrator: Hydrator<Input, Output>,
): FullHydrator<Input, Output> => {
	if (isFullHydrator(hydrator)) {
		return hydrator;
	}

	throw new Error("Hydrator is not a full hydrator");
};

/**
 * Base interface for a mapped hydrator that only allows hydration and further mapping.
 * This is returned after calling `.map()` to prevent further configuration.
 */
export interface MappedHydrator<Input, Output> {
	[IsFullHydrator]: boolean;

	/**
	 * Applies a transformation function to the hydrated output.
	 *
	 * This is a terminal operation: after calling `.map()`, only `.map()` and
	 * `.hydrate()` are available.
	 *
	 * Use this for more complex transformations, such as:
	 * - Hydrating into class instances
	 * - Asserting discriminated union types
	 * - Complex data reshaping
	 *
	 * For simple field transformations, prefer `.fields()` or `.extras()`.
	 *
	 * @param fn - A function that transforms the hydrated output
	 * @returns A MappedHydrator with the transformation added
	 */
	map<NewOutput>(fn: (output: Output) => NewOutput): MappedHydrator<Input, NewOutput>;

	/**
	 * Adds an ordering to apply during hydration. Can be chained to add multiple orderings.
	 *
	 * By default orderings are applied everywhere: to nested collections
	 * (hasMany, etc.) and to the top-level array (the default `sort` mode is
	 * `"all"`).  Pass `sort: "nested"` or `sort: "none"` to `hydrate()` to
	 * restrict or disable sorting — see {@link HydrateOptions}.
	 *
	 * @param key - The field name to order by, or a function that extracts the value to sort by
	 * @param direction - Sort direction: "asc" or "desc" (default: "asc")
	 * @param nulls - Where to place nulls: "first" or "last" (default: "last" for ASC, "first" for DESC)
	 * @returns A new Hydrator with the ordering added
	 */
	orderBy<K extends keyof Input>(
		key: K | ((input: Input) => unknown),
		direction?: "asc" | "desc",
		nulls?: "first" | "last",
	): this;

	/**
	 * Clears custom ordering from the hydrator.  The hydrator will revert to
	 * either no ordering, or ordering by the keyBy columns only if .orderByKeys()
	 * was called.
	 *
	 * @returns A new Hydrator with the custom ORDER BY clauses cleared
	 */
	clearOrderBy(): this;

	/**
	 * Appends the keyBy column(s) as the final ordering (as a tie-breaker).
	 *
	 * This ensures deterministic ordering when multiple records have the same
	 * values for earlier orderings. The keyBy columns are always sorted ascending
	 * with nulls last.
	 *
	 * @param enabled - Whether to enable keyBy ordering.  If not provided,
	 * defaults to `true`
	 * @returns A new Hydrator with keyBy ordering appended
	 */
	orderByKeys(enabled?: boolean): this;

	/**
	 * Hydrates the input data into a denormalized structure according to this configuration.
	 *
	 * If attached collections are configured, this method will fetch them asynchronously
	 * before performing the hydration. The method always returns a Promise for consistency.
	 *
	 * @param input - A single input entity or an iterable of input entities
	 * @param options - Optional hydration options (sort mode, etc.)
	 * @returns A Promise that resolves to the hydrated output(s)
	 */
	hydrate(input: Iterable<Input>, options?: HydrateOptions): Promise<Output[]>;
	hydrate(input: Input, options?: HydrateOptions): Promise<Output>;
	// The union overload must come last: overloads are tried in order, so if it
	// preceded the single-input overload, `hydrate(one)` would resolve to it and
	// be typed `Promise<Output | Output[]>`.
	hydrate(input: Input | Iterable<Input>, options?: HydrateOptions): Promise<Output | Output[]>;
}

/**
 * Full hydrator interface with all configuration methods.
 * Extends MappedHydrator but `.map()` returns MappedHydrator to make it terminal.
 */
export interface FullHydrator<Input, Output> extends MappedHydrator<Input, Output> {
	[IsFullHydrator]: true;

	/**
	 * Configures which fields to include in the hydrated output.
	 *
	 * @param fields - An object mapping field names to either `true` (include as-is)
	 *   or a transformation function.  Also accepts an array of field names to include.
	 * @returns A new Hydrator with the fields configuration merged
	 */
	fields<F extends readonly (keyof Input)[]>(
		fields: F,
	): FullHydrator<Input, Extend<Output, Pick<Input, F[number]>>>;
	fields<F extends Fields<Input>>(
		fields: F,
	): FullHydrator<Input, Extend<Output, InferFields<Input, F>>>;

	/**
	 * Omits specified fields from the hydrated output.
	 *
	 * @param keys - Field names to omit from the output
	 * @returns A new Hydrator with the fields omitted
	 */
	omit<K extends keyof Input>(keys: readonly K[]): FullHydrator<Input, Omit<Output, K>>;

	/**
	 * Configures extra computed fields to add to the hydrated output.
	 *
	 * @param extras - An object mapping field names to functions that compute
	 *   the field value from the entire input
	 * @returns A new Hydrator with the extras configuration merged
	 */
	extras<E extends Extras<Input>>(
		extras: E,
	): FullHydrator<Input, Extend<Output, InferExtras<Input, E>>>;

	/**
	 * Adds computed fields to the hydrated output by spreading the return value
	 * of a function.  Unlike `.extras()` which defines one field at a time,
	 * `.extend()` calls a single function whose returned object is merged into
	 * the output.
	 *
	 * @param fn - A function that receives the input and returns an object of
	 *   computed properties
	 * @returns A new Hydrator with the extender applied
	 */
	extend<F extends Extender<Input>>(
		fn: F,
	): FullHydrator<Input, Extend<Output, InferExtender<Input, F>>>;

	/**
	 * Composes this Hydrator with the configuration from another Hydrator.  The
	 * other Hydrator's configuration takes precedence in case of conflicts.
	 *
	 * Both hydrators must have the same `keyBy`, and any overlapping fields
	 * between the two input types must have compatible types.
	 *
	 * @param other - The Hydrator to compose with
	 * @returns A new Hydrator with merged configuration
	 * @throws {KeyByMismatchError} If the keyBy configurations don't match
	 */
	with<
		// OtherInput doesn't need to overlap with Input, but any overlapping fields
		// must have compatible types.
		OtherInput extends Partial<Input>,
		OtherOutput,
	>(
		other: FullHydrator<OtherInput, OtherOutput>,
	): FullHydrator<
		// Intersect, don't extend because the input must be compatible with both.
		Input & OtherInput,
		// Extend, don't intersect, because the output gets overridden.
		Extend<Output, OtherOutput>
	>;
	with<
		// OtherInput doesn't need to overlap with Input, but any overlapping fields
		// must have compatible types.
		OtherInput extends Partial<Input>,
		OtherOutput,
	>(
		other: MappedHydrator<OtherInput, OtherOutput>,
	): MappedHydrator<
		// Intersect, don't extend because the input must be compatible with both.
		Input & OtherInput,
		// Extend, don't intersect, because the output gets overridden.
		Extend<Output, OtherOutput>
	>;

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
	 * @param hydrator - The Hydrator configuration for the child entities, or
	 *   a function that creates one.
	 * @returns A new Hydrator with the nested collection added.
	 */
	has<K extends string, P extends string, ChildOutput>(
		mode: "many",
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput[]>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: "one",
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput | null>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: "oneOrThrow",
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput>>;
	has<K extends string, P extends string, ChildOutput>(
		mode: CollectionMode,
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput[] | ChildOutput | null>>;

	/**
	 * Shorthand for `has("many", ...)` - configures a nested array collection.
	 *
	 * @param key - The key name for the collection in the output
	 * @param prefix - The prefix used in the input data (e.g., "posts$$")
	 * @param hydrator - The Hydrator configuration for the child entities
	 * @returns A new Hydrator with the nested collection added
	 */
	hasMany<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput[]>>;

	/**
	 * Shorthand for `has("one", ...)` - configures a nested nullable single entity.
	 *
	 * @param key - The key name for the entity in the output
	 * @param prefix - The prefix used in the input data (e.g., "author$$")
	 * @param hydrator - The Hydrator configuration for the child entity
	 * @returns A new Hydrator with the nested entity added
	 */
	hasOne<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput | null>>;

	/**
	 * Shorthand for `has("oneOrThrow", ...)` - configures a nested non-nullable single entity.
	 * Throws an error if the entity is not found during hydration.
	 *
	 * @param key - The key name for the entity in the output.
	 * @param prefix - The prefix used in the input data (e.g., "author$$").
	 * @param hydrator - The Hydrator configuration for the child entity.
	 * @returns A new Hydrator with the nested entity added.
	 */
	hasOneOrThrow<K extends string, P extends string, ChildOutput>(
		key: K,
		prefix: P,
		hydrator: ChildHydratorArg<P, Input, ChildOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, ChildOutput>>;

	/**
	 * Configures an attached collection that is fetched from an external source.
	 * The `fetchFn` is called exactly once per hydration with all parent inputs
	 * to avoid N+1 queries, even when this hydrator is nested within another.
	 *
	 * For convenience, you may prefer to use the shorthand methods:
	 * {@link attachMany}, {@link attachOne}, or {@link attachOneOrThrow}.
	 *
	 * @param mode - The collection mode: "many" for arrays, "one" for nullable
	 *   single, or "oneOrThrow" for non-nullable single.
	 * @param key - The property name for the collection in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 *   Called with all parent inputs and should return already-hydrated data.
	 * @param keys.matchChild - The key(s) on the attached output to use for matching to the
	 *   parent input
	 * @param keys.toParent - The key(s) on the parent input to compare with the
	 *   attached child's key.
	 * @returns A new Hydrator with the attached collection added.
	 */
	attach<K extends string, AttachedOutput>(
		mode: "many",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput[]>>;
	attach<K extends string, AttachedOutput>(
		mode: "one",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput | null>>;
	attach<K extends string, AttachedOutput>(
		mode: "oneOrThrow",
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput>>;
	attach<K extends string, AttachedOutput>(
		mode: CollectionMode,
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput[] | AttachedOutput | null>>;

	/**
	 * Shorthand for `attach("many", ...)` - configures an attached array collection.
	 *
	 * @param key - The property name for the collection in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 * @param keys.matchChild - The key(s) on the attached output to use for matching to parents.
	 * @param keys.toParent - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydrator with the attached collection added.
	 */
	attachMany<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput[]>>;

	/**
	 * Shorthand for `attach("one", ...)` - configures an attached nullable single entity.
	 *
	 * @param key - The property name for the entity in the output.
	 * @param fetchFn - A function that fetches and hydrates the attached data.
	 * @param keys.matchChild - The key(s) on the attached output to use for matching to parents.
	 * @param keys.toParent - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydrator with the attached entity added.
	 */
	attachOne<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput | null>>;

	/**
	 * Shorthand for `attach("oneOrThrow", ...)` - configures an attached non-nullable single entity.
	 * Throws an error if the entity is not found during hydration.
	 *
	 * @param key - The property name for the entity in the output
	 * @param fetchFn - A function that fetches and hydrates the attached data
	 * @param keys.matchChild - The key(s) on the attached output to use for matching to parents
	 * @param keys.toParent - The key(s) on the parent input to compare with the child's key.
	 * @returns A new Hydrator with the attached entity added
	 */
	attachOneOrThrow<K extends string, AttachedOutput>(
		key: K,
		fetchFn: FetchFn<Input, AttachedOutput>,
		keys: AttachedKeysArg<Input, AttachedOutput>,
	): FullHydrator<Input, ExtendWith<Output, K, AttachedOutput>>;
}

////////////////////////////////////////////////////////////////////
// Implementation
////////////////////////////////////////////////////////////////////

/**
 * Special constant to enable auto-inclusion of fields at each level.
 */
export const EnableAutoInclusion = Symbol();

/**
 * Options for hydration behavior.
 */
export interface HydrateOptions {
	/**
	 * When to apply sorting during hydration:
	 * - "nested": Sort nested collections only (depth > 0), not the top-level array
	 * - "all": Sort everything including the top-level array
	 * - "none": Don't sort at all (rely on SQL ordering or input order)
	 *
	 * @default "all"
	 */
	sort?: "nested" | "all" | "none";

	/**
	 * When true, automatically includes all fields at each level (excluding
	 * parent fields and nested collection fields).
	 *
	 * This is an internal option used by the EnableAutoInclusion symbol.
	 * @internal
	 */
	[EnableAutoInclusion]?: boolean;
}

/**
 * Context passed through hydration operations.
 */
interface HydrationContext {
	/**
	 * When true, automatically includes all fields at each level (excluding
	 * parent fields and nested collection fields).
	 */
	readonly autoIncludeFields: boolean;

	/**
	 * When to apply sorting during hydration.
	 */
	readonly sortMode: "nested" | "all" | "none";

	/**
	 * Map of attached collection data, keyed by prefixed collection key.
	 * Populated during the initial fetch phase and used during hydration.
	 * The inner maps hold grouped rows (see groupByKey).
	 */
	readonly attachedDataMap: Map<string, Grouped<any>>;

	/**
	 * Cache for auto-include field names keyed by prefix.
	 * Maps: prefix -> AutoFields
	 */
	readonly autoFieldsCache: Map<string, AutoFields>;
}

/**
 * Auto-include field names for one prefix level, plus a precomputed flag so
 * the per-row assignment loop never has to scan the names itself.
 */
interface AutoFields {
	readonly names: readonly string[];

	/**
	 * True when `names` contains "__proto__", which must be assigned via
	 * {@link defineProtoShadowedKey}.
	 */
	readonly needsProtoShadow: boolean;
}

/**
 * Sets a "__proto__" output key as a normal own data property. Plain
 * `entity[key] = value` assignment would hit `Object.prototype`'s
 * `__proto__` accessor instead: scalar values are silently dropped, and
 * object values would REPLACE the entity's prototype (prototype pollution).
 * An own data property shadows the accessor, so subsequent reads and plain
 * writes behave normally.
 */
function defineProtoShadowedKey(entity: object, value: unknown): void {
	Object.defineProperty(entity, "__proto__", {
		value,
		writable: true,
		enumerable: true,
		configurable: true,
	});
}

/**
 * Implements the entire inheritance chain of Hydrators.
 */
class HydratorImpl<Input = any, Output = any> implements FullHydrator<Input, Output> {
	#props: HydratorProps<Input>;

	/**
	 * Memo for {@link #getConfigNeedsProtoShadow}; computed on first hydration.
	 */
	#configNeedsProtoShadow: boolean | undefined;

	constructor(props: HydratorProps<Input>) {
		this.#props = props;
	}

	/**
	 * True when any configured output key is "__proto__", which must be
	 * assigned via {@link defineProtoShadowedKey}.
	 */
	#getConfigNeedsProtoShadow(): boolean {
		this.#configNeedsProtoShadow ??= Boolean(
			this.#props.fields?.has("__proto__") ||
			this.#props.extras?.has("__proto__") ||
			this.#props.collections?.has("__proto__") ||
			this.#props.attachedCollections?.has("__proto__"),
		);
		return this.#configNeedsProtoShadow;
	}

	get [IsFullHydrator]() {
		// This cast is weird but it works to force HydratorImpl to implement
		// FullHydrator while behaving correctly as a MappedHydrator as well
		return !this.#props.mapFns?.length as true;
	}

	fields(fields: Fields<any> | readonly string[]): any {
		return new HydratorImpl({
			...this.#props,

			fields: Array.isArray(fields)
				? fields.reduce<FieldsMap>(
						(map, field) => map.set(field, true),
						new Map(this.#props.fields),
					)
				: addObjectToMap(this.#props.fields, fields as Fields<any>),
		}) as any;
	}

	omit(keys: readonly PropertyKey[]): any {
		const omitFields = Object.fromEntries(keys.map((key) => [key, false as const]));

		return new HydratorImpl({
			...this.#props,

			fields: addObjectToMap(this.#props.fields, omitFields),
		}) as any;
	}

	extras(extras: Extras<any>): any {
		return new HydratorImpl({
			...this.#props,

			extras: addObjectToMap(this.#props.extras, extras),
		}) as any;
	}

	extend(fn: Extender<any>): any {
		return new HydratorImpl({
			...this.#props,

			extenders: [...(this.#props.extenders ?? []), fn],
		}) as any;
	}

	with(other: MappedHydrator<any, any>): any {
		const otherImpl = other as any as HydratorImpl;
		const thisKeyBy = JSON.stringify(this.#props.keyBy);
		const otherKeyBy = JSON.stringify(otherImpl.#props.keyBy);
		if (thisKeyBy !== otherKeyBy) {
			throw new KeyByMismatchError(thisKeyBy, otherKeyBy);
		}

		const ownProps = this.#props;
		const otherProps = otherImpl.#props;
		return new HydratorImpl({
			keyBy: otherProps.keyBy as any,
			fields: new Map([...(ownProps.fields ?? []), ...(otherProps.fields ?? [])]),
			extras: new Map([...(ownProps.extras ?? []), ...(otherProps.extras ?? [])]),
			extenders: [...(ownProps.extenders ?? []), ...(otherProps.extenders ?? [])],
			collections: new Map([...(ownProps.collections ?? []), ...(otherProps.collections ?? [])]),
			attachedCollections: new Map([
				...(ownProps.attachedCollections ?? []),
				...(otherProps.attachedCollections ?? []),
			]),
			mapFns: [...(this.#props.mapFns ?? []), ...(otherProps.mapFns ?? [])],
			orderings: [...(ownProps.orderings ?? []), ...(otherProps.orderings ?? [])],
			orderByKeys: otherProps.orderByKeys ?? ownProps.orderByKeys,
		});
	}

	orderBy(key: any, direction: "asc" | "desc" = "asc", nulls?: "first" | "last"): any {
		return new HydratorImpl({
			...this.#props,

			orderings: [...(this.#props.orderings ?? []), { key, direction, nulls }],
		});
	}

	clearOrderBy(): any {
		return new HydratorImpl({
			...this.#props,

			orderings: [],
		});
	}

	orderByKeys(enabled: boolean = true): any {
		return new HydratorImpl({
			...this.#props,

			orderByKeys: enabled,
		});
	}

	map(fn: (output: any) => any): any {
		return new HydratorImpl({
			...this.#props,

			mapFns: [...(this.#props.mapFns ?? []), fn],
		});
	}

	has(mode: CollectionMode, key: string, prefix: string, hydrator: any): any {
		const newCollections = new Map(this.#props.collections).set(key, {
			prefix,
			mode,
			hydrator: typeof hydrator === "function" ? hydrator(createHydrator as any) : hydrator,
		} satisfies Collection<any, any>);

		return new HydratorImpl({
			...this.#props,

			collections: newCollections,
		});
	}

	hasMany(key: string, prefix: string, hydrator: any): any {
		return this.has("many", key, prefix, hydrator);
	}

	hasOne(key: string, prefix: string, hydrator: any): any {
		return this.has("one", key, prefix, hydrator);
	}

	hasOneOrThrow(key: string, prefix: string, hydrator: any): any {
		return this.has("oneOrThrow", key, prefix, hydrator);
	}

	attach(
		mode: CollectionMode,
		key: string,
		fetchFn: FetchFn<any, any>,
		keys: AttachedKeysArg<any, any>,
	): any {
		return new HydratorImpl({
			...this.#props,

			attachedCollections: new Map(this.#props.attachedCollections).set(key, {
				mode,
				fetchFn,
				matchChild: keys.matchChild,
				toParent: keys.toParent ?? this.#props.keyBy,
			} satisfies AttachedCollection<any, any>),
		});
	}

	attachMany(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>): any {
		return this.attach("many", key, fetchFn, keys);
	}

	attachOne(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>): any {
		return this.attach("one", key, fetchFn, keys);
	}

	attachOneOrThrow(key: string, fetchFn: FetchFn<any, any>, keys: AttachedKeysArg<any, any>): any {
		return this.attach("oneOrThrow", key, fetchFn, keys);
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
		ctx: HydrationContext,
		prefix: string,
		// Must be an array (not a lazily-consumed iterable): this method runs once
		// per nesting level, and hydration iterates the same inputs afterward.
		inputs: Input[],
		fetchPromises: Promise<void>[],
	): void {
		const { attachedCollections, collections } = this.#props;

		// Fetch attach collections at this level
		if (attachedCollections) {
			// The fetchFn contract is one input per parent entity, but raw joined
			// rows can repeat a parent (row explosion from sibling many-joins) and
			// can be all-null phantoms (left joins with no match).  Apply the same
			// rules hydration itself uses (see groupByKey): dedupe by this level's
			// keyBy and drop rows with nil keys.  We also need to convert the input
			// to prefixed accessors if we are nested, because the fetchFn expects
			// unprefixed inputs.
			const { keyBy } = this.#props;
			const seen = new Set<unknown>();
			const inputArray: any[] = [];
			for (const input of inputs) {
				const key = getKey(prefix, input, keyBy);
				if (isKeyNil(key) || seen.has(key)) {
					continue;
				}
				seen.add(key);
				inputArray.push(prefix !== "" ? createdPrefixedAccessor(prefix, input as object) : input);
			}

			for (const [key, attachedCollection] of attachedCollections) {
				// Use prefixed key for the map
				const mapKey = prefix ? applyPrefix(prefix, key) : key;

				// Create fetch promise
				fetchPromises.push(
					Promise.resolve(attachedCollection.fetchFn(inputArray))
						.then((result) => {
							if (isExecutable(result)) {
								return result.execute();
							}
							return result as Iterable<any>;
						})
						.then((attachedOutputs) => {
							// Group fetched rows by their match key
							const grouped = groupByKey(
								"", // Always unprefixed.
								attachedOutputs,
								attachedCollection.matchChild,
							);

							ctx.attachedDataMap.set(mapKey, grouped);
						}),
				);
			}
		}

		// Recursively fetch attach collections from nested collections
		if (collections) {
			for (const collection of collections.values()) {
				const childPrefix = applyPrefix(prefix, collection.prefix);

				// Recursively fetch nested attach collections (write directly to the same map).
				collection.hydrator.#fetchAllAttachedCollections(ctx, childPrefix, inputs, fetchPromises);
			}
		}
	}

	/**
	 * Gets all the fields belonging to the current prefix level; not to the
	 * parent, and not to any nested collection.  Does this once per hydration
	 * (assumes all inputs have the same keys).
	 */
	#getAutoFields(ctx: HydrationContext, prefix: string, input: unknown): AutoFields {
		// Have we done this already?
		const cached = ctx.autoFieldsCache.get(prefix);
		if (cached) {
			return cached;
		}

		// If we get a null for some bizarre reason, I guess we should try again
		// on the next row.
		if (typeof input !== "object" || input === null) {
			return { names: [], needsProtoShadow: false };
		}

		const { fields, extras, collections } = this.#props;

		// Get the nested collection prefixes
		const nestedPrefixes: string[] = [];
		if (collections) {
			for (const collection of collections.values()) {
				nestedPrefixes.push(applyPrefix(prefix, collection.prefix));
			}
		}

		const autoFields: string[] = [];
		for (const inputKey of Object.keys(input)) {
			// Exclude if its from a parent (not this prefix).
			if (!hasPrefix(prefix, inputKey)) {
				continue;
			}
			// Exclude if its from a child (this prefix but with an additional prefix).
			if (nestedPrefixes.some((nestedPrefix) => hasPrefix(nestedPrefix, inputKey))) {
				continue;
			}

			const unprefixedKey = removePrefix(prefix, inputKey);

			// Exclude if its explicitly set in the fields or extras.
			if (fields?.has(unprefixedKey) || extras?.has(unprefixedKey)) {
				continue;
			}

			// The autoFields gets the unprefixed key.
			autoFields.push(unprefixedKey);
		}

		const result: AutoFields = {
			names: autoFields,
			needsProtoShadow: autoFields.includes("__proto__"),
		};

		// Cache and return the auto-include fields
		ctx.autoFieldsCache.set(prefix, result);
		return result;
	}

	/**
	 * Hydrates a single entity. All attach collections are already fetched and provided in attachedDataMap.
	 */
	#hydrateOne(
		ctx: HydrationContext,
		prefix: string,
		input: Input,
		// Null means the group consists of just `input`; the array is only
		// materialized when nested collections actually need it.
		inputRows: Input[] | null,
	): Output {
		const { fields, extras, extenders, collections, attachedCollections } = this.#props;

		const entity: any = {};

		// A "__proto__" key must go through defineProtoShadowedKey (see its doc).
		// Both flags are precomputed outside the row loop (per cached auto-field
		// set / memoized per hydrator), so the common case pays only a
		// short-circuited boolean test per assignment.
		const configShadow = this.#getConfigNeedsProtoShadow();

		// Auto-include all fields at this prefix level when enabled
		if (ctx.autoIncludeFields) {
			const autoFields = this.#getAutoFields(ctx, prefix, input);
			for (const key of autoFields.names) {
				const value = getPrefixedValue(prefix, input, key);
				if (autoFields.needsProtoShadow && key === "__proto__") {
					defineProtoShadowedKey(entity, value);
				} else {
					entity[key] = value;
				}
			}
		}

		if (fields) {
			for (const [key, field] of fields) {
				// Skip fields explicitly set to false (omitted)
				if (field === false) {
					continue;
				}
				const value = getPrefixedValue(prefix, input, key);
				const output = field === true ? value : field(value as any);
				if (configShadow && key === "__proto__") {
					defineProtoShadowedKey(entity, output);
				} else {
					entity[key] = output;
				}
			}
		}

		if (extras || extenders) {
			const accessor = createdPrefixedAccessor(prefix, input as object);

			if (extras) {
				for (const [key, extra] of extras) {
					const output = extra(accessor as Input);
					if (configShadow && key === "__proto__") {
						defineProtoShadowedKey(entity, output);
					} else {
						entity[key] = output;
					}
				}
			}

			if (extenders) {
				for (const extender of extenders) {
					Object.assign(entity, extender(accessor as Input));
				}
			}
		}

		if (collections) {
			const rows = inputRows ?? [input];

			for (const [key, collection] of collections) {
				const childPrefix = applyPrefix(prefix, collection.prefix);

				// Hydrate nested collections (all attach collections already fetched)
				const collectionOutputs = collection.hydrator.#hydrateMany(ctx, childPrefix, rows);

				const output = applyCollectionMode(collectionOutputs, collection.mode, key);
				if (configShadow && key === "__proto__") {
					defineProtoShadowedKey(entity, output);
				} else {
					entity[key] = output;
				}
			}
		}

		// Attach collections from the provided map
		if (attachedCollections) {
			for (const [key, collection] of attachedCollections) {
				// Get the match value from this input using the matchBy.
				const inputKey = getKey(prefix, input, collection.toParent);

				// Use prefixed key to look up in the map
				const mapKey = prefix ? applyPrefix(prefix, key) : key;

				// Look up attached rows with matching key (already hydrated)
				const groupedData = ctx.attachedDataMap.get(mapKey);
				const attached = groupedData?.get(inputKey);

				const output = applyGroupedCollectionMode(attached, collection.mode, key);
				if (configShadow && key === "__proto__") {
					defineProtoShadowedKey(entity, output);
				} else {
					entity[key] = output;
				}
			}
		}

		// Apply map functions if present
		const { mapFns } = this.#props;
		if (mapFns) {
			let result: any = entity;
			for (const mapFn of mapFns) {
				result = mapFn(result);
			}
			return result;
		}

		return entity;
	}

	/**
	 * Hydrates many entities. All attach collections are already fetched and provided in attachedDataMap.
	 */
	#hydrateMany(ctx: HydrationContext, prefix: string, inputs: Iterable<Input>): Output[] {
		const { keyBy } = this.#props;

		// Sort inputs before hydration if needed
		const finalOrderings = this.#getFinalOrderings();
		const shouldSort = finalOrderings.length > 0 && this.#shouldSort(ctx.sortMode, prefix);

		let sortedInputs: Iterable<Input> = inputs;
		if (shouldSort) {
			// Sort a copy: when `inputs` is already an array, it is usually not
			// owned by this call.  It may be the caller's own array (hydrate()
			// passes top-level arrays through by reference) or a RowGroup's backing
			// array (shared by every sibling collection at the same level), so
			// sorting in place would leak the reordering as a side effect.  (The
			// one owned case, the single-element `[input]` array built by
			// #hydrateOne, doesn't merit avoiding the copy.)  Array.from already
			// produces a fresh array for non-array iterables.
			const inputsArray = Array.isArray(inputs) ? inputs.slice() : Array.from(inputs);

			const comparator = this.#makePrefixedComparator(prefix, finalOrderings);
			inputsArray.sort(comparator);

			sortedInputs = inputsArray;
		}

		const result: Output[] = [];

		// Always group by keyBy: rows with the same key are the same entity.
		// This holds even without nested collections, because the input rows may
		// contain duplicates (e.g. a base query with repeated keys, or cartesian
		// products inherited from an ancestor's sibling many-collections).
		// groupByKey also skips rows with null keys (non-existent entities).
		const grouped = groupByKey(prefix, sortedInputs, keyBy);
		for (const group of grouped.values()) {
			// We assume the first row is representative of the group, at least for
			// the top-level entity (not nested collections).
			const entity =
				group instanceof RowGroup
					? this.#hydrateOne(ctx, prefix, group.rows[0]!, group.rows)
					: this.#hydrateOne(ctx, prefix, group, null);
			result.push(entity);
		}

		return result;
	}

	#cachedOrderings: readonly OrderBy<Input>[] | undefined;

	/**
	 * Builds the final orderings array, appending keyBy columns if orderByKeys is true.
	 * Result is cached to avoid recreating the array on repeated calls.
	 */
	#getFinalOrderings(): readonly OrderBy<Input>[] {
		if (this.#cachedOrderings) {
			return this.#cachedOrderings;
		}

		const { orderings, orderByKeys, keyBy } = this.#props;

		if (!orderByKeys) {
			this.#cachedOrderings = orderings ?? [];
			return this.#cachedOrderings;
		}

		const keys = typeof keyBy === "string" ? [keyBy] : keyBy;
		const keyOrderings = keys.map((key) => ({
			key,
			direction: "asc" as const,
			nulls: "last" as const, // Follows PostgreSQL/Oracle: NULLS LAST for ASC
		}));

		this.#cachedOrderings = [...(orderings ?? []), ...keyOrderings];
		return this.#cachedOrderings;
	}

	/**
	 * Creates a comparator function that handles prefixed field names.
	 * For function keys, creates a prefixed accessor so the function can access unprefixed fields.
	 */
	#makePrefixedComparator(prefix: string, orderings: readonly OrderBy<Input>[]) {
		return makeOrderByComparator(orderings, (obj, key) => {
			if (typeof key === "function") {
				// Create a prefixed accessor so the function can access fields without the prefix
				const accessor = createdPrefixedAccessor(prefix, obj as object);
				return key(accessor as Input);
			}
			return getPrefixedValue(prefix, obj, key as string);
		});
	}

	/**
	 * Determines if sorting should be applied at the given depth.
	 */
	#shouldSort(sortMode: "nested" | "all" | "none", prefix: string): boolean {
		switch (sortMode) {
			case "nested":
				return prefix !== "";
			case "all":
				return true;
			case "none":
				return false;
		}
	}

	hydrate(
		input: Input | Iterable<Input>,
		options?: HydrateOptions | typeof EnableAutoInclusion,
	): Promise<any> {
		// Handle legacy EnableAutoInclusion symbol for backward compatibility
		const opts: HydrateOptions =
			options === EnableAutoInclusion ? { [EnableAutoInclusion]: true } : (options ?? {});

		// Create hydration context for this operation
		const ctx: HydrationContext = {
			autoIncludeFields: opts[EnableAutoInclusion] ?? false,
			sortMode: opts.sort ?? "all",
			attachedDataMap: new Map(),
			autoFieldsCache: new Map(),
		};

		// Most of the work below runs synchronously; catch synchronous errors and
		// turn them into rejections so this method never throws.
		try {
			// Materialize the input once: attach-fetching and hydration each iterate
			// it, which would silently exhaust a one-shot iterable (e.g. a
			// generator) and hydrate zero rows.
			const inputs: Input[] | null = isIterable(input)
				? Array.isArray(input)
					? input
					: Array.from(input)
				: null;

			const hydrateWithData = () => {
				if (inputs) {
					return this.#hydrateMany(ctx, "", inputs);
				}

				return this.#hydrateOne(ctx, "", input as Input, null);
			};

			// Fetch all attach collections upfront (this is the only async operation).
			// Start with empty prefix for top-level collections.
			const fetchPromises: Promise<void>[] = [];
			this.#fetchAllAttachedCollections(ctx, "", inputs ?? [input as Input], fetchPromises);

			return fetchPromises.length > 0
				? Promise.all(fetchPromises).then(hydrateWithData)
				: Promise.resolve(hydrateWithData());
		} catch (error) {
			return Promise.reject(error);
		}
	}
}

/**
 * Creates a new Hydrator---a configuration for how to hydrate an entity into
 * a denormalized structure.
 *
 * @param keyBy - The key(s) to group by for this entity.
 *   Defaults to "id" if the input type has an "id" property.
 */
// Overload 1: keyBy provided - any input type
export function createHydrator<T>(keyBy: KeyBy<NoInfer<T>>): FullHydrator<T, {}>;
// Overload 2: keyBy omitted - input must have 'id'
export function createHydrator<T extends InputWithDefaultKey>(): FullHydrator<T, {}>;
// Implementation
export function createHydrator<T = {}>(keyBy?: KeyBy<NoInfer<T>>): FullHydrator<T, {}> {
	return new HydratorImpl({
		keyBy: keyBy ?? (DEFAULT_KEY_BY as keyof T & string),
		// orderByKeys is left unset (not false) so .with() can tell whether it
		// was ever explicitly configured.
	});
}

/**
 * Hydrates an entity or collection of entities into a denormalized structure
 * per the given Hydrator configuration.
 *
 * You may provide a function as the second argument to create a Hydrator on the fly.
 *
 * The function will return a Promise that resolves to the hydrated output(s).
 */
export function hydrate<Input, Output>(
	input: readonly Input[],
	hydrator: HydratorArg<NoInfer<Input>, Output>,
): Promise<Output[]>;
// `Input` must be inferred from the hydrator here, NOT from the input argument:
// otherwise this overload would swallow a `User | User[]` argument by inferring
// `Input = User | User[]` and mistype the result as a single output.
export function hydrate<Input, Output>(
	input: NoInfer<Input>,
	hydrator: HydratorArg<Input, Output>,
): Promise<Output>;
// The union overload must come last; see the note on Hydrator["hydrate"].
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydrator: HydratorArg<NoInfer<Input>, Output>,
): Promise<Output | Output[]>;
export function hydrate<Input, Output>(
	input: Input | readonly Input[],
	hydrator: HydratorArg<NoInfer<Input>, Output>,
): Promise<Output | Output[]> {
	// The factory is user code; catch synchronous errors and turn them into
	// rejections so this function never throws.
	try {
		hydrator = typeof hydrator === "function" ? hydrator(createHydrator as any) : hydrator;
	} catch (error) {
		return Promise.reject(error);
	}

	return hydrator.hydrate(input);
}

/**
 * Applies collection mode logic (many/one/oneOrThrow) to collection outputs.
 *
 * In "many" mode, `outputs` is returned as-is, so the caller must transfer
 * ownership: pass an array that nothing else references (or copy first, as the
 * RowGroup path in {@link applyGroupedCollectionMode} does).
 */
function applyCollectionMode<T>(
	outputs: T[] | undefined,
	mode: CollectionMode,
	key: string,
): T[] | T | null {
	if (mode === "many") {
		return outputs ?? [];
	}

	const count = outputs?.length ?? 0;

	// For "one" and "oneOrThrow" modes, validate cardinality after deduplication
	if (count > 1) {
		throw new CardinalityViolationError(key, count);
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
 * Applies collection mode logic to data in the grouped form produced by
 * {@link groupByKey} (a single row, or a RowGroup for 2+ rows).  This only
 * normalizes the representation; the mode/cardinality semantics live in
 * {@link applyCollectionMode}.  The single-row case is handled inline so that
 * "one"/"oneOrThrow" lookups don't allocate a temporary array.
 */
function applyGroupedCollectionMode<T>(
	grouped: T | RowGroup<T> | undefined,
	mode: CollectionMode,
	key: string,
): T[] | T | null {
	if (grouped instanceof RowGroup) {
		// "many" returns the rows to the caller, so copy them: `rows` is the
		// grouping map's internal storage, shared by every parent with the same
		// match value.  Returned by reference, one parent mutating its collection
		// would corrupt its siblings' (and single-match parents get fresh arrays
		// below, so mutation would be safe or corrupting depending on match
		// count).  The other modes only read from the array.
		if (mode === "many") {
			return grouped.rows.slice();
		}
		return applyCollectionMode(grouped.rows, mode, key);
	}

	if (grouped === undefined) {
		return applyCollectionMode(undefined, mode, key);
	}

	return mode === "many" ? [grouped] : grouped;
}

/**
 * Determines if a key is nil, meaning the corresponding object does not exist.
 */
function isKeyNil(key: unknown): key is null | undefined {
	return key === null || key === undefined;
}

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
	// JSON-encode the parts (rather than joining them with a separator) so that
	// values containing the separator cannot collide across part boundaries.
	// Bigints are not JSON-serializable, so stringify them explicitly.
	return JSON.stringify(values, (_key, value) => (typeof value === "bigint" ? `${value}n` : value));
}

/**
 * A group of 2+ rows sharing the same key.  Groups of one row are stored as
 * the row itself (see {@link groupByKey}); this wrapper class disambiguates
 * multi-row groups from rows without restricting what a row can be.
 */
class RowGroup<T> {
	readonly rows: T[];

	constructor(first: T, second: T) {
		this.rows = [first, second];
	}
}

/**
 * The result of grouping rows by key: a single row, or a RowGroup for 2+ rows.
 */
type Grouped<T> = Map<unknown, T | RowGroup<T>>;

/**
 * Groups rows by the entity's key.
 *
 * Most groups contain exactly one row, so the row is stored directly and a
 * RowGroup (with its backing array) is only allocated once a second row with
 * the same key shows up.  This keeps grouping allocation-free for the common
 * duplicate-free case.
 */
function groupByKey<T>(
	prefix: string,
	inputs: Iterable<T>,
	keyBy: string | readonly string[],
): Grouped<T> {
	const map: Grouped<T> = new Map();

	for (const input of inputs) {
		const key = getKey(prefix, input, keyBy);
		// Skip rows with null keys.
		if (isKeyNil(key)) {
			continue;
		}
		// Rows are never undefined (reading a key from an undefined row would
		// have thrown above), so undefined reliably means "absent".
		const existing = map.get(key);
		if (existing === undefined) {
			map.set(key, input);
		} else if (existing instanceof RowGroup) {
			existing.rows.push(input);
		} else {
			map.set(key, new RowGroup(existing, input));
		}
	}

	return map;
}
