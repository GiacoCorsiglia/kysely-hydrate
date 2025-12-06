/** biome-ignore-all lint/suspicious/noExplicitAny: Magic afoot. */
import type { AddOrOverride, Extend, Override, Prettify } from "./helpers";

////////////////////////////////////////////////////////////////////
// Interfaces.
////////////////////////////////////////////////////////////////////

export type MappedOutput<V, Output, Omitted extends PropertyKey> = Prettify<
	Omit<Extend<V, Output>, Omitted>
>;

export interface Mapper<
	// @ts-expect-error Cast variance.
	in Input,
	// @ts-expect-error Cast variance.
	out Output,
	// @ts-expect-error Cast variance.
	out Omitted extends PropertyKey,
> {
	apply<V extends Input>(input: V): MappedOutput<V, Output, Omitted>;

	map<K extends keyof Input, V>(
		key: K,
		transform: (value: Input[K]) => V,
	): Mapper<Input, Override<Output, K & keyof Output, V>, Omitted>;

	add<K extends PropertyKey, V>(
		key: K,
		generate: (row: Input) => V,
	): Mapper<Input, AddOrOverride<Output, K, V>, Omitted>;

	omit<K extends keyof Output>(key: K): Mapper<Input, Output, Omitted | K>;
	omit<K extends keyof Output>(keys: K[]): Mapper<Input, Output, Omitted | K>;
}

////////////////////////////////////////////////////////////////////
// Implementation.
////////////////////////////////////////////////////////////////////

export type AnyMapper = Mapper<any, any, any>;

interface MapperProps {
	readonly fields?: ReadonlyMap<PropertyKey, (input: unknown) => any>;
	readonly additions?: ReadonlyMap<PropertyKey, (input: unknown) => any>;
	readonly omissions?: ReadonlySet<PropertyKey>;
}

class MapperImpl implements AnyMapper {
	#props: MapperProps;

	constructor(props: MapperProps) {
		this.#props = props;
	}

	apply(input: any) {
		const { fields, additions, omissions } = this.#props;

		const output: any = {};

		const allOmissions = new Set(omissions);
		if (fields) {
			for (const key of fields.keys()) {
				allOmissions.add(key);
			}
		}
		if (additions) {
			for (const key of additions.keys()) {
				allOmissions.add(key);
			}
		}

		for (const key in input) {
			if (!Object.hasOwn(input, key)) {
				continue;
			}
			if (allOmissions?.has(key)) {
				continue;
			}
			output[key] = input;
		}

		if (fields) {
			for (const [key, transform] of fields) {
				output[key] = transform(input[key]);
			}
		}

		if (additions) {
			for (const [key, generate] of additions) {
				output[key] = generate(input);
			}
		}

		return output;
	}

	map(
		key: PropertyKey,
		transform: (input: unknown) => any,
	): Mapper<any, any, any> {
		return new MapperImpl({
			...this.#props,
			fields: new Map(this.#props.fields).set(key, transform),
		});
	}

	add(
		key: PropertyKey,
		generate: (input: unknown) => any,
	): Mapper<any, any, any> {
		return new MapperImpl({
			...this.#props,
			additions: new Map(this.#props.additions).set(key, generate),
		});
	}

	omit(keys: PropertyKey | PropertyKey[]): Mapper<any, any, any> {
		const additionalOmissions = Array.isArray(keys)
			? new Set(keys)
			: new Set<PropertyKey>().add(keys);
		const currentOmissions = this.#props.omissions;
		const newOmissions = currentOmissions
			? currentOmissions.union(additionalOmissions)
			: additionalOmissions;
		return new MapperImpl({
			...this.#props,
			omissions: newOmissions,
		});
	}
}

////////////////////////////////////////////////////////////////////
// Constructor.
////////////////////////////////////////////////////////////////////

export function mapper<Input>(): Mapper<Input, Input, never> {
	return new MapperImpl({}) as unknown as Mapper<Input, Input, never>;
}
