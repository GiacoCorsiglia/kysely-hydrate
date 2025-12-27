import {
	type KyselyPlugin,
	type PluginTransformQueryArgs,
	type PluginTransformResultArgs,
	type QueryResult,
	type RootOperationNode,
	type UnknownRow,
} from "kysely";

import { type Provenance, traceLineage } from "./helpers/scope-resolver.ts";
import { type Database } from "./schema/table.ts";

export class HydratePlugin implements KyselyPlugin {
	readonly #database: Database;
	readonly #lineageCache = new WeakMap<any, Map<string, Provenance>>();

	constructor(database: Database) {
		this.#database = database;
	}

	transformQuery(args: PluginTransformQueryArgs): RootOperationNode {
		const lineage = traceLineage(args.node, this.#database);
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

					if (provenance?.type === "COLUMN") {
						transformed[key] = provenance.columnType.fromDriver(value);
					} else {
						transformed[key] = value;
					}
				}

				return transformed;
			}),
		};
	}
}
