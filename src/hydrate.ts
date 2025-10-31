/** biome-ignore-all lint/suspicious/noExplicitAny: Many generics afoot. */
import type { KeyBy } from "./helpers.ts";

type Collections<T> = Record<PropertyKey, EntityConfig<T>>;

type Fields<T> = {
	[K in keyof T]?: true | ((value: T[K]) => unknown);
};

type Extras<T> = Record<PropertyKey, (row: T) => unknown>;

interface BaseEntityConfig<T> {
	/**
	 * The fields to include in the final denormalized entity.  You can either specify `true` to
	 * include a field as-is, or provide a transformation function to modify the field's value.
	 */
	fields?: Fields<T>;

	/**
	 * Extra fields generated from the row.
	 */
	extras?: Extras<T>;

	/**
	 * The mode of the nested entity: "one" for a single object, "many" for an array.
	 * Defaults to "many".
	 *
	 * This option is ignored for the top-level entity.
	 */
	mode?: "one" | "many";
}

interface EntityConfigWithoutCollections<T> extends BaseEntityConfig<T> {
	collections?: never;
}

interface EntityConfigWithCollections<T> extends BaseEntityConfig<T> {
	/**
	 * The key(s) to group by for this entity.
	 * Can be a single key or an array of keys for composite keys.
	 */
	keyBy: KeyBy<T>;

	/**
	 * An optional map of nested collections.
	 */
	collections: Collections<T>;
}

type EntityConfig<T> =
	| EntityConfigWithoutCollections<T>
	| EntityConfigWithCollections<T>;

type HydratedFields<T, C extends EntityConfig<T>> = {
	[K in keyof C["fields"] & keyof T]: C["fields"][K] extends true
		? T[K]
		: C["fields"][K] extends (value: never) => infer R
			? R
			: never;
};

type HydratedExtras<T, C extends EntityConfig<T>> = {
	[K in keyof C["extras"]]: C["extras"][K] extends (row: T) => infer R
		? R
		: never;
};

type HydratedCollection<T, C extends EntityConfig<T>> = C["mode"] extends "one"
	? HydratedEntity<T, C> | null
	: HydratedEntity<T, C>[];

// TODO: how to determine from `T` if the entire collection is nullable or not
type HydratedCollections<T, C extends EntityConfig<T>> = {
	[K in keyof C["collections"]]: C["collections"][K] extends EntityConfig<T>
		? HydratedCollection<T, C["collections"][K]>
		: never;
};

/**
 * A recursively-defined type that builds the final nested object shape
 * based on the provided configuration.
 */
type HydratedEntity<T, C extends EntityConfig<T>> = Prettify<
	HydratedFields<T, C> & HydratedExtras<T, C> & HydratedCollections<T, C>
>;

/**
 * Hydrates an entity, but does nothing about collections.
 */
function hydrateEntity<T>(row: T, config: EntityConfig<T>) {
	const entity: any = {};

	if (config.fields) {
		for (const [col, transform] of Object.entries(config.fields)) {
			const value = row[col as keyof T];
			entity[col] = transform === true ? value : transform(value);
		}
	}

	if (config.extras) {
		for (const [col, comboFn] of Object.entries(config.extras)) {
			entity[col] = comboFn(row);
		}
	}

	return entity;
}

function getKey<T>(row: T, keyBy: keyof T | readonly (keyof T)[]): unknown {
	if (typeof keyBy !== "object") {
		// Cast `undefined` to `null`.
		return row[keyBy] ?? null;
	}

	const values: unknown[] = [];
	for (const partKey of keyBy) {
		const value = row[partKey];
		if (value === null || value === undefined) {
			return null; // A null part invalidates the whole key for this entity
		}
		values.push(value);
	}
	return values.join("::");
}

/**
 * Groups rows by the entity's key.
 */
function groupRows<T>(
	rows: readonly T[],
	config: EntityConfigWithCollections<T>,
): Map<unknown, T[]> {
	const map = new Map<unknown, T[]>();

	for (const row of rows) {
		const key = getKey(row, config.keyBy);
		// Skip rows with null keys
		if (key === null) {
			continue;
		}
		let arr = map.get(key);
		if (arr === undefined) {
			arr = [];
			map.set(key, arr);
		}
		arr.push(row);
	}

	return map;
}

/**
 * Recursively denormalizes grouped rows into nested entities.
 */
function hydrateEntities<T, C extends EntityConfig<T>>(
	rows: readonly T[],
	config: C,
): HydratedEntity<T, C>[] {
	// If this entity has no collections, we can skip grouping entirely.
	if (!config.collections) {
		const entities: any[] = [];
		for (const row of rows) {
			entities.push(hydrateEntity(row, config));
		}
		return entities;
	}

	const grouped = groupRows(rows, config);
	const result: any[] = [];

	const collections = config.collections
		? Object.entries(config.collections)
		: [];

	for (const groupRows of grouped.values()) {
		// biome-ignore lint/style/noNonNullAssertion: One row exists or the group would not exist.
		const entity: any = hydrateEntity(groupRows[0]!, config);

		// Process nested collections
		if (config.collections) {
			for (const [name, childConfig] of collections) {
				const children = hydrateEntities(groupRows, childConfig);

				entity[name] =
					childConfig.mode === "one" ? (children[0] ?? null) : children;
			}
		}

		result.push(entity);
	}

	return result;
}

export function hydrate<T, C extends EntityConfig<T>>(
	row: T,
	config: C,
): HydratedEntity<T, C>;
export function hydrate<T, C extends EntityConfig<T>>(
	rows: readonly T[],
	config: C,
): HydratedEntity<T, C>[];
export function hydrate<T, C extends EntityConfig<T>>(
	rows: T | readonly T[],
	config: C,
): HydratedEntity<T, C> | HydratedEntity<T, C>[] {
	if (Array.isArray(rows)) {
		if (rows.length === 0) {
			return [];
		}

		return hydrateEntities(rows, config);
	}

	const hydrated = hydrateEntities([rows as T], config);
	// biome-ignore lint/style/noNonNullAssertion: There must be one row if isSingle is true.
	return hydrated[0]!;
}
