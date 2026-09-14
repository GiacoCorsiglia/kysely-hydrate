import {
	AttachedKeysArityMismatchError,
	CardinalityViolationError,
	ExpectedOneItemError,
	KeyByMismatchError,
} from "./helpers/errors.ts";
import { type OrderBy, sortBy } from "./helpers/order-by.ts";
import {
	applyPrefix,
	createdPrefixedAccessor,
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
	mapWithDeleted,
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
 * already-hydrated data.  Never called with zero inputs: when every parent row
 * is dropped, the fetch is skipped entirely.
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
	 * This applies across collection kinds as well: a nested (`has`) or
	 * attached (`attach`) collection on the other Hydrator replaces a
	 * collection of either kind registered under the same key on this one.
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
	 * Replaces any collection — nested or attached — previously registered
	 * under the same key.
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
	 * Replaces any collection — nested or attached — previously registered
	 * under the same key.
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
	 * The values hold the fetched rows grouped by their match key (see
	 * {@link groupByKey}).
	 */
	readonly attachedDataMap: Map<string, KeyedGroups<any>>;

	/**
	 * The entity builder for each prefix, resolved from the first row seen at
	 * that prefix (the auto-included fields depend on the rows' keys).
	 */
	readonly builderCache: Map<string, EntityBuilder>;
}

/**
 * Copies a row's fields at one level into a fresh entity: the auto-included
 * fields, then the explicit ones.  Everything else (extras, collections, map
 * functions) is added to the entity afterward.
 */
type EntityBuilder = (input: any) => any;

/**
 * A field to copy from the input: the output key, the (prefixed) input key,
 * and the mapping to apply, if any.
 */
type PlannedField = readonly [
	key: string,
	inputKey: string,
	field: true | ((value: any) => unknown),
];

/**
 * An auto-included field: the output key and the (prefixed) input key.
 */
type PlannedAutoField = readonly [key: string, inputKey: string];

interface PlannedCollection {
	readonly key: string;
	readonly mode: CollectionMode;
	/** The child hydrator's plan at the child prefix. */
	readonly plan: LevelPlan;
}

interface PlannedAttachedCollection {
	readonly key: string;
	/** The key in {@link HydrationContext.attachedDataMap}: `key` prefixed. */
	readonly mapKey: string;
	readonly mode: CollectionMode;
	readonly fetchFn: FetchFn<any, any>;
	/** Unprefixed: fetched rows are never prefixed. */
	readonly matchChild: readonly string[];
	/** Prefixed, as it is read from the parent's input rows. */
	readonly toParent: readonly string[];
}

/**
 * Everything hydration needs to know about one hydrator at one prefix,
 * resolved ahead of time.  Hydration reads several fields of every row, so
 * resolving each field's prefixed input key here (rather than concatenating
 * it per read) is the difference between a property lookup on an interned
 * string and a string allocation plus hash per read.
 */
interface LevelPlan {
	readonly prefix: string;
	/** The hydrator this plan was built from, for `#builderFor` and `#compiledBuilder`. */
	readonly hydrator: HydratorImpl;
	/** The prefixed parts of `keyBy`. */
	readonly keyParts: readonly string[];
	/** Explicit fields, omitted ones already dropped. */
	readonly fields: readonly PlannedField[];
	/**
	 * Entity builders by the auto-included fields they cover (as a signature
	 * string), so that a builder is compiled once per distinct row shape rather
	 * than once per hydration.
	 */
	readonly builders: Map<string, EntityBuilder>;
	readonly extras: readonly (readonly [key: string, extra: (input: any) => unknown])[] | undefined;
	readonly extenders: ExtendersArray | undefined;
	readonly collections: readonly PlannedCollection[] | undefined;
	readonly attachedCollections: readonly PlannedAttachedCollection[] | undefined;
	readonly mapFns: ReadonlyArray<(value: any) => any> | undefined;
	/**
	 * The final orderings (see `#getFinalOrderings`), with string keys
	 * prefixed; undefined when there is nothing to sort by.
	 */
	readonly orderings: readonly OrderBy<any>[] | undefined;
	/**
	 * The value getter for {@link sortBy}, which only function keys need help
	 * with once string keys are prefixed.  Undefined at the top level, where
	 * the default getter serves.
	 */
	readonly getValue: ((obj: any, key: any) => unknown) | undefined;
}

/**
 * Whether this runtime lets us generate code.  Content Security Policies and
 * some edge runtimes (e.g. Cloudflare Workers) forbid it, in which case entity
 * builders fall back to a loop over the fields.
 */
const canGenerateCode: boolean = (() => {
	try {
		return new Function("return true")() === true;
	} catch {
		return false;
	}
})();

/**
 * Builds an {@link EntityBuilder} that copies the given fields, in order.
 *
 * Where possible the builder is generated source: an object literal with the
 * keys spelled out.  V8 then allocates the entity in one step with its final
 * shape and reads each input property through a monomorphic site, where the
 * equivalent loop stores through a megamorphic keyed site and grows the
 * object one transition at a time -- some 8x slower on a three-field entity.
 * Keys are embedded as JSON string literals, so any key is safe to embed.
 * `__proto__` is left to the loop, as an object literal would treat it as the
 * prototype rather than as a field.
 */
function compileEntityBuilder(
	autoFields: readonly PlannedAutoField[],
	fields: readonly PlannedField[],
): EntityBuilder {
	if (canGenerateCode) {
		const mappers: Array<(value: any) => unknown> = [];
		const entries: string[] = [];
		let safe = true;
		for (const [key, inputKey] of autoFields) {
			safe &&= key !== "__proto__";
			entries.push(`${JSON.stringify(key)}: input[${JSON.stringify(inputKey)}]`);
		}
		for (const [key, inputKey, field] of fields) {
			safe &&= key !== "__proto__";
			const value = `input[${JSON.stringify(inputKey)}]`;
			entries.push(
				field === true
					? `${JSON.stringify(key)}: ${value}`
					: `${JSON.stringify(key)}: mappers[${mappers.push(field) - 1}](${value})`,
			);
		}
		if (safe) {
			const source = `return function buildEntity(input) { return { ${entries.join(", ")} }; };`;
			return new Function("mappers", source)(mappers) as EntityBuilder;
		}
	}

	return (input) => {
		const entity: any = {};
		for (let i = 0; i < autoFields.length; i++) {
			const [key, inputKey] = autoFields[i]!;
			entity[key] = input[inputKey];
		}
		for (let i = 0; i < fields.length; i++) {
			const [key, inputKey, field] = fields[i]!;
			const value = input[inputKey];
			entity[key] = field === true ? value : field(value);
		}
		return entity;
	};
}

/**
 * The parts of `keyBy`, each prefixed.
 */
function prefixKeyParts(prefix: string, keyBy: string | readonly string[]): readonly string[] {
	if (typeof keyBy === "string") {
		return [applyPrefix(prefix, keyBy)];
	}
	const parts: string[] = [];
	for (const key of keyBy) {
		parts.push(applyPrefix(prefix, key));
	}
	return parts;
}

/**
 * Creates a sort-key accessor for a nested level.  String keys arrive already
 * prefixed (see {@link LevelPlan.orderings}); function keys get a prefixed
 * accessor so that they can read fields without the prefix.
 */
function makePrefixedGetValue(prefix: string) {
	return (obj: any, key: string | ((input: any) => unknown)): unknown => {
		if (typeof key === "function") {
			return key(createdPrefixedAccessor(prefix, obj as object));
		}
		return obj[key];
	};
}

/**
 * Determines if sorting should be applied at the given depth.
 */
function shouldSort(sortMode: "nested" | "all" | "none", prefix: string): boolean {
	switch (sortMode) {
		case "nested":
			return prefix !== "";
		case "all":
			return true;
		case "none":
			return false;
	}
}

/**
 * Implements the entire inheritance chain of Hydrators.
 */
class HydratorImpl<Input = any, Output = any> implements FullHydrator<Input, Output> {
	#props: HydratorProps<Input>;

	constructor(props: HydratorProps<Input>) {
		this.#props = props;
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

		// The other hydrator's collection definitions take precedence across
		// kinds, too: its definition of a key replaces an own definition of
		// either kind, exactly as if has()/attach() had been called on this
		// hydrator directly.
		const collections = new Map(ownProps.collections);
		const attachedCollections = new Map(ownProps.attachedCollections);
		for (const [key, collection] of otherProps.collections ?? []) {
			attachedCollections.delete(key);
			collections.set(key, collection);
		}
		for (const [key, collection] of otherProps.attachedCollections ?? []) {
			collections.delete(key);
			attachedCollections.set(key, collection);
		}

		return new HydratorImpl({
			keyBy: otherProps.keyBy as any,
			fields: new Map([...(ownProps.fields ?? []), ...(otherProps.fields ?? [])]),
			extras: new Map([...(ownProps.extras ?? []), ...(otherProps.extras ?? [])]),
			extenders: [...(ownProps.extenders ?? []), ...(otherProps.extenders ?? [])],
			collections,
			attachedCollections,
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
			// A key names a single output property regardless of collection kind,
			// so redefining it as a nested collection must drop any attached
			// collection previously registered under the same key.
			attachedCollections:
				this.#props.attachedCollections && mapWithDeleted(this.#props.attachedCollections, key),
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
		const { matchChild } = keys;
		const toParent = keys.toParent ?? this.#props.keyBy;

		// Keys are matched part by part, so keys of different arity never match
		// and the collection would attach nothing to every parent.  Say so here
		// rather than at hydration time, where it looks like missing rows.  The
		// two `keyBy` shapes describe the same one-part key, so only the number
		// of parts matters, not whether a side is a string or an array.
		const matchChildArity = keyArity(matchChild);
		const toParentArity = keyArity(toParent);
		if (matchChildArity !== toParentArity) {
			throw new AttachedKeysArityMismatchError(key, matchChildArity, toParentArity);
		}

		return new HydratorImpl({
			...this.#props,

			// See the corresponding note in has(): the same key must not remain
			// registered as both a nested and an attached collection.
			collections: this.#props.collections && mapWithDeleted(this.#props.collections, key),
			attachedCollections: new Map(this.#props.attachedCollections).set(key, {
				mode,
				fetchFn,
				matchChild,
				toParent,
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
	 * Plans for each prefix this hydrator has hydrated at (see
	 * {@link LevelPlan}).  Props are immutable, so a plan stays valid for the
	 * life of the hydrator; the cache lives here rather than on the hydration
	 * context so that repeated hydrations pay for planning only once.
	 */
	readonly #plans = new Map<string, LevelPlan>();

	/**
	 * The plan for hydrating this hydrator's entities at `prefix`.
	 */
	#planFor(prefix: string): LevelPlan {
		let plan = this.#plans.get(prefix);
		if (plan === undefined) {
			plan = this.#buildPlan(prefix);
			this.#plans.set(prefix, plan);
		}
		return plan;
	}

	#buildPlan(prefix: string): LevelPlan {
		const { keyBy, fields, extras, extenders, collections, attachedCollections, mapFns } =
			this.#props;

		const plannedFields: PlannedField[] = [];
		if (fields) {
			for (const [key, field] of fields) {
				// Fields explicitly set to false are omitted.
				if (field !== false) {
					plannedFields.push([key, applyPrefix(prefix, key), field]);
				}
			}
		}

		const plannedCollections: PlannedCollection[] = [];
		if (collections) {
			for (const [key, collection] of collections) {
				const childPrefix = applyPrefix(prefix, collection.prefix);
				plannedCollections.push({
					key,
					mode: collection.mode,
					// Plan the whole tree up front, so hydration never looks up a plan.
					plan: collection.hydrator.#planFor(childPrefix),
				});
			}
		}

		const plannedAttached: PlannedAttachedCollection[] = [];
		if (attachedCollections) {
			for (const [key, collection] of attachedCollections) {
				plannedAttached.push({
					key,
					mapKey: applyPrefix(prefix, key),
					mode: collection.mode,
					fetchFn: collection.fetchFn,
					matchChild: prefixKeyParts("", collection.matchChild),
					toParent: prefixKeyParts(prefix, collection.toParent),
				});
			}
		}

		const orderings = this.#getFinalOrderings();

		return {
			prefix,
			hydrator: this,
			keyParts: prefixKeyParts(prefix, keyBy),
			fields: plannedFields,
			builders: new Map(),
			extras: extras && extras.size > 0 ? [...extras] : undefined,
			extenders: extenders && extenders.length > 0 ? extenders : undefined,
			collections: plannedCollections.length > 0 ? plannedCollections : undefined,
			attachedCollections: plannedAttached.length > 0 ? plannedAttached : undefined,
			mapFns: mapFns && mapFns.length > 0 ? mapFns : undefined,
			orderings:
				orderings.length > 0
					? orderings.map((ordering) =>
							typeof ordering.key === "function"
								? ordering
								: { ...ordering, key: applyPrefix(prefix, ordering.key as string) },
						)
					: undefined,
			getValue: prefix === "" ? undefined : makePrefixedGetValue(prefix),
		};
	}

	/**
	 * Fetches all attach collections (including nested ones) and groups them by match key.
	 * This is the only async operation needed - everything else can work with the resulting map.
	 * Uses prefixed keys for nested collections (e.g., "posts$$comments" for nested comments).
	 *
	 * Writes directly to the provided attachedDataMap and fetchPromises array.
	 */
	static #fetchAllAttachedCollections(
		ctx: HydrationContext,
		plan: LevelPlan,
		// Must be an array (not a lazily-consumed iterable): this method runs once
		// per nesting level, and hydration iterates the same inputs afterward.
		inputs: unknown[],
		fetchPromises: Promise<void>[],
	): void {
		const { prefix, attachedCollections, collections } = plan;

		// Fetch attach collections at this level
		if (attachedCollections) {
			// The fetchFn contract is one input per parent entity, but raw joined
			// rows can repeat a parent (row explosion from sibling many-joins) and
			// can be all-null phantoms (left joins with no match).  Apply the same
			// rules hydration itself uses (see groupByKey): dedupe by this level's
			// keyBy and drop rows with nil keys.  We also need to convert the input
			// to prefixed accessors if we are nested, because the fetchFn expects
			// unprefixed inputs.
			const { keyParts } = plan;
			const seen = new KeyedGroups<unknown>(keyParts.length);
			const inputArray: any[] = [];
			for (const input of inputs) {
				if (!seen.addFirst(input, keyParts)) {
					continue;
				}
				inputArray.push(prefix !== "" ? createdPrefixedAccessor(prefix, input as object) : input);
			}

			// When there are no inputs left (no rows at all, or every row dropped
			// by the nil-key filter), skip the fetch entirely: user fetchFns
			// commonly build `WHERE x IN (...)` from the inputs, which is invalid
			// or pointless SQL for zero inputs.  Leaving attachedDataMap without
			// an entry behaves identically to storing an empty group — lookups go
			// through `groupedData?.find(...)`, which yields undefined either way.
			if (inputArray.length > 0) {
				for (const { mapKey, fetchFn, matchChild } of attachedCollections) {
					// Create fetch promise
					fetchPromises.push(
						Promise.resolve(fetchFn(inputArray))
							.then((result) => {
								if (isExecutable(result)) {
									return result.execute();
								}
								return result as Iterable<any>;
							})
							.then((attachedOutputs) => {
								// Group fetched rows by their match key (always unprefixed).
								ctx.attachedDataMap.set(mapKey, groupByKey(attachedOutputs, matchChild));
							}),
					);
				}
			}
		}

		// Recursively fetch attach collections from nested collections
		if (collections) {
			for (const collection of collections) {
				// Recursively fetch nested attach collections (write directly to the same map).
				HydratorImpl.#fetchAllAttachedCollections(ctx, collection.plan, inputs, fetchPromises);
			}
		}
	}

	/**
	 * The entity builder for this level, covering the auto-included fields (when
	 * enabled) and the explicit ones.  Auto-included fields are the input's keys
	 * at this prefix that belong neither to a nested collection nor to an
	 * explicit field or extra; they are read from the first row seen at each
	 * prefix (assuming all inputs have the same keys), once per hydration.
	 */
	static #builderFor(ctx: HydrationContext, plan: LevelPlan, input: unknown): EntityBuilder {
		const { prefix } = plan;

		// Have we done this already?
		const cached = ctx.builderCache.get(prefix);
		if (cached) {
			return cached;
		}

		let autoFields: PlannedAutoField[] = [];
		if (ctx.autoIncludeFields) {
			// If we get a null for some bizarre reason, I guess we should try again
			// on the next row.
			if (typeof input !== "object" || input === null) {
				return plan.hydrator.#compiledBuilder(plan, autoFields);
			}

			const { fields, extras, collections } = plan.hydrator.#props;

			// Get the nested collection prefixes
			const nestedPrefixes: string[] = [];
			if (collections) {
				for (const collection of collections.values()) {
					nestedPrefixes.push(applyPrefix(prefix, collection.prefix));
				}
			}

			autoFields = [];
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

				// The output gets the unprefixed key; the input is read by the full key.
				autoFields.push([unprefixedKey, inputKey]);
			}
		}

		const builder = plan.hydrator.#compiledBuilder(plan, autoFields);
		ctx.builderCache.set(prefix, builder);
		return builder;
	}

	/**
	 * The builder for these auto-included fields plus the plan's explicit
	 * fields, compiled once per distinct set of auto-included fields.
	 */
	#compiledBuilder(plan: LevelPlan, autoFields: readonly PlannedAutoField[]): EntityBuilder {
		// Input keys are unique within a row, so they identify the set.  The
		// separator cannot occur in a prefixed key, which never contains a
		// newline... except that a column alias could, so count too.
		let signature = String(autoFields.length);
		for (let i = 0; i < autoFields.length; i++) {
			signature += "\n" + autoFields[i]![1];
		}
		let builder = plan.builders.get(signature);
		if (builder === undefined) {
			builder = compileEntityBuilder(autoFields, plan.fields);
			plan.builders.set(signature, builder);
		}
		return builder;
	}

	/**
	 * Hydrates a single entity. All attach collections are already fetched and provided in attachedDataMap.
	 */
	static #hydrateOne(
		ctx: HydrationContext,
		plan: LevelPlan,
		input: any,
		// Null means the group consists of just `input`; the array is only
		// materialized when nested collections actually need it.
		inputRows: unknown[] | null,
		// Resolved by the caller (see #builderFor), once per batch of entities.
		builder: EntityBuilder,
	): unknown {
		const { prefix, extras, extenders, collections, attachedCollections, mapFns } = plan;

		// Copy the auto-included (when enabled) and explicit fields.
		const entity: any = builder(input);

		if (extras || extenders) {
			const accessor = createdPrefixedAccessor(prefix, input as object);

			if (extras) {
				for (let i = 0; i < extras.length; i++) {
					const [key, extra] = extras[i]!;
					entity[key] = extra(accessor);
				}
			}

			if (extenders) {
				for (let i = 0; i < extenders.length; i++) {
					Object.assign(entity, extenders[i]!(accessor));
				}
			}
		}

		if (collections) {
			const rows = inputRows ?? [input];

			for (let i = 0; i < collections.length; i++) {
				const { key, mode, plan: childPlan } = collections[i]!;

				// Hydrate nested collections (all attach collections already fetched)
				const collectionOutputs = HydratorImpl.#hydrateMany(ctx, childPlan, rows);

				entity[key] = applyCollectionMode(collectionOutputs, mode, key);
			}
		}

		// Attach collections from the provided map
		if (attachedCollections) {
			for (let i = 0; i < attachedCollections.length; i++) {
				const { key, mapKey, mode, toParent } = attachedCollections[i]!;

				// Look up attached rows whose matchChild key matches this input's
				// toParent key (already hydrated)
				const groupedData = ctx.attachedDataMap.get(mapKey);
				const attached = groupedData?.find(input, toParent);

				entity[key] = applyGroupedCollectionMode(attached, mode, key);
			}
		}

		// Apply map functions if present
		if (mapFns) {
			let result: any = entity;
			for (let i = 0; i < mapFns.length; i++) {
				result = mapFns[i]!(result);
			}
			return result;
		}

		return entity;
	}

	/**
	 * Hydrates many entities. All attach collections are already fetched and provided in attachedDataMap.
	 */
	static #hydrateMany(ctx: HydrationContext, plan: LevelPlan, inputs: unknown[]): unknown[] {
		const { keyParts, orderings } = plan;

		// Sort inputs before hydration if needed
		const sortedInputs =
			orderings && shouldSort(ctx.sortMode, plan.prefix)
				? sortBy(inputs, orderings, plan.getValue)
				: inputs;

		const result: unknown[] = [];

		// Always group by keyBy: rows with the same key are the same entity.
		// This holds even without nested collections, because the input rows may
		// contain duplicates (e.g. a base query with repeated keys, or cartesian
		// products inherited from an ancestor's sibling many-collections).
		// groupByKey also skips rows with null keys (non-existent entities).
		const groups = groupByKey(sortedInputs, keyParts).values();
		if (groups.length === 0) {
			return result;
		}

		// Resolve the builder once per batch, from the first row (whose keys are
		// assumed representative of every row at this prefix).
		const first = groups[0]!;
		const builder = HydratorImpl.#builderFor(
			ctx,
			plan,
			first instanceof RowGroup ? first.rows[0] : first,
		);

		for (let i = 0; i < groups.length; i++) {
			const group = groups[i]!;
			// We assume the first row is representative of the group, at least for
			// the top-level entity (not nested collections).
			const entity =
				group instanceof RowGroup
					? HydratorImpl.#hydrateOne(ctx, plan, group.rows[0]!, group.rows, builder)
					: HydratorImpl.#hydrateOne(ctx, plan, group, null, builder);
			result.push(entity);
		}

		return result;
	}

	/**
	 * Builds the final orderings array, appending keyBy columns if orderByKeys is true.
	 */
	#getFinalOrderings(): readonly OrderBy<Input>[] {
		const { orderings, orderByKeys, keyBy } = this.#props;

		if (!orderByKeys) {
			return orderings ?? [];
		}

		const keys = typeof keyBy === "string" ? [keyBy] : keyBy;
		const keyOrderings = keys.map((key) => ({
			key,
			direction: "asc" as const,
			nulls: "last" as const, // Follows PostgreSQL/Oracle: NULLS LAST for ASC
		}));

		return [...(orderings ?? []), ...keyOrderings];
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
			builderCache: new Map(),
		};

		// Most of the work below runs synchronously; catch synchronous errors and
		// turn them into rejections so this method never throws.
		try {
			const plan = this.#planFor("");

			// Materialize the input once: attach-fetching and hydration each iterate
			// it, which would silently exhaust a one-shot iterable (e.g. a
			// generator) and hydrate zero rows.
			const inputs: unknown[] | null = isIterable(input)
				? Array.isArray(input)
					? input
					: Array.from(input)
				: null;

			const hydrateWithData = () => {
				if (inputs) {
					return HydratorImpl.#hydrateMany(ctx, plan, inputs);
				}

				return HydratorImpl.#hydrateOne(
					ctx,
					plan,
					input,
					null,
					HydratorImpl.#builderFor(ctx, plan, input),
				);
			};

			// Fetch all attach collections upfront (this is the only async operation).
			// Start with empty prefix for top-level collections.
			const fetchPromises: Promise<void>[] = [];
			HydratorImpl.#fetchAllAttachedCollections(ctx, plan, inputs ?? [input], fetchPromises);

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
	// rejections so this function never throws.  The hydrate() call stays
	// inside the try for the same reason: a factory that returns a
	// non-hydrator would otherwise throw a synchronous TypeError.
	try {
		hydrator = typeof hydrator === "function" ? hydrator(createHydrator as any) : hydrator;
		return hydrator.hydrate(input);
	} catch (error) {
		return Promise.reject(error);
	}
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
 * The number of parts in a key, for either `keyBy` shape.
 */
function keyArity(keyBy: string | readonly string[]): number {
	return typeof keyBy === "object" ? keyBy.length : 1;
}

/**
 * Reads one part of an input's key and canonicalizes it into a value that
 * compares correctly as a `Map` key, i.e. under SameValueZero.  Returns
 * undefined if the part is nil, meaning the entity does not exist.
 *
 * Primitives already compare by value, so they pass through.  Objects compare
 * by content instead of identity: `Date`s by time value (so all invalid dates
 * are equal), `Uint8Array`s by bytes -- `String()` would decode them as UTF-8,
 * which is lossy -- and everything else by its `String()` form.
 *
 * SQL types a column, so one key part holds one type across rows, and the
 * canonical forms above only have to be injective within their own type.
 * Values of different types that share a string form -- an object and a string,
 * say -- are deliberately one key rather than two.
 *
 * Deliberate equivalences: `-0` and `0` are the same part (SameValueZero), as
 * SQL does not distinguish negative zero, while `NaN` groups only with `NaN`.
 */
function keyPart(input: unknown, partKey: string): unknown {
	const value = (input as Record<string, unknown>)[partKey];
	if (typeof value !== "object") {
		// Symbols and functions have no content to compare, so they keep
		// identity; undefined passes through as the nil sentinel.
		return value;
	}
	if (value === null) {
		return undefined;
	}
	if (value instanceof Date) {
		return value.getTime();
	}
	if (value instanceof Uint8Array) {
		return value.join(",");
	}
	return stringifyKeyPart(value);
}

/**
 * The `String()` form of a key part that is neither a primitive nor a type
 * {@link keyPart} knows, so exotic values still group deterministically (if
 * not always distinctly — every plain object stringifies to
 * `[object Object]`).
 *
 * Kept out of keyPart because a `try` block would stop that hot function from
 * being inlined.
 */
function stringifyKeyPart(value: object): string {
	try {
		return String(value);
	} catch {
		// String() throws for values with no primitive conversion (e.g.
		// null-prototype objects); fall back to the default toString form rather
		// than rejecting.
		return Object.prototype.toString.call(value);
	}
}

/**
 * The slot of a key that has no group: it is absent from the groups, or it
 * cannot have one because it has a nil part or the wrong arity.
 */
const NO_SLOT = -1;

/**
 * A group of 2+ rows sharing the same key.  Groups of one row are stored as
 * the row itself (see {@link KeyedGroups}); this wrapper class disambiguates
 * multi-row groups from rows without restricting what a row can be.
 */
class RowGroup<T> {
	readonly rows: T[];

	constructor(first: T, second: T) {
		this.rows = [first, second];
	}
}

/**
 * One level of {@link KeyedGroups}'s key trie: each key part maps to the next
 * level, or — at the deepest level — to a slot in the groups array.  Which of
 * the two a level holds is fixed by the groups' arity rather than by the type.
 */
type KeyTrie = Map<unknown, number | KeyTrie>;

/**
 * Rows grouped by their entity's key: for each distinct key, the single row,
 * or a {@link RowGroup} for 2+ rows.  Most groups contain exactly one row, so
 * a RowGroup (with its backing array) is only allocated once a second row with
 * the same key shows up.
 *
 * Keys are matched one part at a time in a trie of `Map`s, so parts are
 * compared as `Map` keys: there is no per-row key to build and hash, and no
 * boundary between parts for values to collide across.
 */
class KeyedGroups<T> {
	readonly #root: KeyTrie = new Map();
	readonly #groups: (T | RowGroup<T>)[] = [];
	readonly #arity: number;

	/**
	 * @param arity - The number of parts in the key these groups are keyed by.
	 *   Lookups with a different arity match nothing, so an attach collection
	 *   whose `toParent` and `matchChild` disagree never matches (rather than
	 *   matching the wrong rows).
	 */
	constructor(arity: number) {
		this.#arity = arity;
	}

	/**
	 * Adds a row to its key's group, or ignores it if the key has a nil part
	 * (the entity does not exist).
	 */
	add(input: T, keyParts: readonly string[]): void {
		const slot = this.#walk(input, keyParts, true);
		if (slot === NO_SLOT) {
			return;
		}
		const groups = this.#groups;
		// Slots are allocated sequentially, and `groups` only grows here and in
		// addFirst, so a slot at the end is one just allocated for a new key.
		if (slot === groups.length) {
			groups.push(input);
			return;
		}
		const existing = groups[slot]!;
		if (existing instanceof RowGroup) {
			existing.rows.push(input);
		} else {
			groups[slot] = new RowGroup(existing, input);
		}
	}

	/**
	 * Adds a row only if its key is new, and returns whether it was added.  For
	 * callers that want one row per key: rows with a duplicate key are dropped
	 * rather than grouped, so no RowGroup is ever allocated.
	 */
	addFirst(input: T, keyParts: readonly string[]): boolean {
		// Any slot but the next one is an already-seen key, or NO_SLOT.
		if (this.#walk(input, keyParts, true) !== this.#groups.length) {
			return false;
		}
		this.#groups.push(input);
		return true;
	}

	/**
	 * Returns the group matching this input's key, or undefined if there is
	 * none.  `keyParts` names the parts on `input` to match with, which need not
	 * be the parts the groups were keyed by — only their arity must agree.
	 */
	find(input: unknown, keyParts: readonly string[]): T | RowGroup<T> | undefined {
		const slot = this.#walk(input, keyParts, false);
		return slot === NO_SLOT ? undefined : this.#groups[slot];
	}

	/**
	 * The groups, in the order their keys first appeared.
	 */
	values(): readonly (T | RowGroup<T>)[] {
		return this.#groups;
	}

	/**
	 * Matches this input's key part by part, returning its slot: an existing
	 * one, the next one if `create` is set and the key is new, or NO_SLOT if the
	 * key has a nil part, has the wrong arity, or is absent and `create` is not
	 * set.
	 *
	 * A key abandoned partway through (a nil part after the first) can leave an
	 * empty map behind, which is harmless: no slot is allocated for it, so
	 * nothing can reach it.
	 */
	#walk(input: unknown, keyParts: readonly string[], create: boolean): number {
		if (keyParts.length !== this.#arity) {
			return NO_SLOT;
		}

		// Descend one level per part except the last, which is looked up in the
		// level it lands on.  A single-part key descends nothing, so the root map
		// holds slots directly.
		const last = this.#arity - 1;
		let node = this.#root;
		for (let i = 0; i < last; i++) {
			const part = keyPart(input, keyParts[i]!);
			if (part === undefined) {
				return NO_SLOT; // A nil part invalidates the whole key.
			}
			let next = node.get(part) as KeyTrie | undefined;
			if (next === undefined) {
				if (!create) {
					return NO_SLOT;
				}
				next = new Map();
				node.set(part, next);
			}
			node = next;
		}

		const part = keyPart(input, keyParts[last]!);
		if (part === undefined) {
			return NO_SLOT;
		}
		const slot = node.get(part) as number | undefined;
		if (slot !== undefined) {
			return slot;
		}
		if (!create) {
			return NO_SLOT;
		}
		const allocated = this.#groups.length;
		node.set(part, allocated);
		return allocated;
	}
}

/**
 * Groups rows by the entity's key, read from the given (prefixed) key parts.
 */
function groupByKey<T>(inputs: Iterable<T>, keyParts: readonly string[]): KeyedGroups<T> {
	const groups = new KeyedGroups<T>(keyParts.length);
	for (const input of inputs) {
		groups.add(input, keyParts);
	}
	return groups;
}
