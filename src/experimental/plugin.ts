import {
	type KyselyPlugin,
	type PluginTransformQueryArgs,
	type PluginTransformResultArgs,
	type QueryResult,
	type RootOperationNode,
	type UnknownRow,
} from "kysely";

import { getMappedNodes } from "./mapped-expression.ts";
import { type Database } from "./schema/table.ts";
import { type Provenance, traceLineage } from "./scope-resolver.ts";

export class HydratePlugin implements KyselyPlugin {
	readonly #database: Database;
	readonly #lineageCache = new WeakMap<any, Map<string, Provenance>>();

	constructor(database: Database) {
		this.#database = database;
	}

	transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
		const mappedNodes = getMappedNodes();
		const lineage = traceLineage(args.node, this.#database, mappedNodes);
		this.#lineageCache.set(args.queryId, lineage);
		return args.node;
	}

	async transformResult(args: PluginTransformResultArgs): Promise<QueryResult<UnknownRow>> {
		const lineage = this.#lineageCache.get(args.queryId);

		if (!lineage) {
			return args.result;
		}

		return {
			...args.result,
			rows: args.result.rows.map((row) => {
				const transformed: UnknownRow = {};

				for (const [key, value] of Object.entries(row)) {
					const provenance = lineage.get(key);

					if (!provenance) {
						transformed[key] = value;
						continue;
					}

					let transformedValue = value;

					// Apply fromDriver first if this is a COLUMN
					if (provenance.type === "COLUMN") {
						transformedValue = provenance.columnType.fromDriver(transformedValue);
					}

					// Then apply any map functions in order
					for (const mapFn of provenance.mapFns) {
						transformedValue = mapFn(transformedValue);
					}

					transformed[key] = transformedValue;
				}

				return transformed;
			}),
		};
	}
}
