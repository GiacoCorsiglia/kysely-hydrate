/** biome-ignore-all lint/complexity/noBannedTypes: Lots of intentional `{}` in here. */
/** biome-ignore-all lint/suspicious/noExplicitAny: TypeScript shenanigans afoot. */
import * as k from "kysely";

function renameSelectionNode(
	selectionNode: k.SelectionNode,
	newName: string,
): {
	oldName: string;
	renamedSelection: k.SelectionNode;
} {
	const { selection } = selectionNode;

	if (k.ColumnNode.is(selection)) {
		return {
			oldName: `${selection.column.name}`,

			renamedSelection: k.SelectionNode.create(
				k.AliasNode.create(selection, k.IdentifierNode.create(newName)),
			),
		};
	}

	if (k.ReferenceNode.is(selection)) {
		const { column } = selection;

		if (k.SelectAllNode.is(column)) {
			throw new Error("selectAll not supported in nested relational queries");
		}

		return {
			oldName: `${column.column.name}`,

			renamedSelection: k.SelectionNode.create(
				k.AliasNode.create(selection, k.IdentifierNode.create(newName)),
			),
		};
	}

	if (k.AliasNode.is(selection)) {
		const alias = selection.alias;
		if (!k.IdentifierNode.is(alias)) {
			throw new Error("Expected IdentifierNode");
		}

		return {
			oldName: `${alias.name}`,

			renamedSelection: k.SelectionNode.create(
				k.AliasNode.create(selection.node, k.IdentifierNode.create(newName)),
			),
		};
	}

	if (k.SelectAllNode.is(selection)) {
		throw new Error("selectAll not supported in nested relational queries");
	}

	throw new Error("Unknown selection type");
}

class Orm<DB> {
	#db: k.Kysely<DB>;

	constructor(db: k.Kysely<DB>) {
		this.#db = db;
	}

	query<QueryDB, QueryTB extends keyof QueryDB, O>(
		qb: (db: k.Kysely<DB>) => k.SelectQueryBuilder<QueryDB, QueryTB, O>,
		groupBy: keyof O | (keyof O)[],
	): RelationBuilder<QueryDB, QueryTB, O, {}> {
		return new RelationBuilder<QueryDB, QueryTB, O, {}>(
			this.#db,
			qb(this.#db),
			groupBy,
			k.createQueryId(),
			{},
		);
	}
}

type Relations<DB, TB extends keyof DB, RelationsO> = {
	[K in keyof RelationsO]?: (
		qb: k.SelectQueryBuilder<DB, TB, {}>,
	) => k.SelectQueryBuilder<DB, TB, RelationsO[K]>;
};

export type Identity<T> = T;
export type Flatten<T> = Identity<{ [k in keyof T]: T[k] }>;
type Prettify<T> = { [K in keyof T]: T[K] } & {};
type Extend<A, B> = Flatten<
	// fast path when there is no keys overlap
	keyof A & keyof B extends never
		? A & B
		: {
				[K in keyof A as K extends keyof B ? never : K]: A[K];
			} & {
				[K in keyof B]: B[K];
			}
>;

type RowWithRelations<BaseOutput, RelationsOutput> = Extend<
	BaseOutput,
	RelationsOutput
>;

type Renames = Map<string, string | Map<string, string>>;

function renameRow(row: Record<string, unknown>, renames: Renames) {
	const output: Record<string, unknown> = {};

	for (const [outputKey, selectResult] of renames) {
		if (selectResult instanceof Map) {
			output[outputKey] = renameRow(row, selectResult);
		} else {
			output[outputKey] = row[selectResult];
		}
	}

	return output;
}

class RelationBuilder<DB, TB extends keyof DB, O, RelationsO = {}>
	implements k.Compilable, k.OperationNodeSource
{
	#db: k.Kysely<any>;
	#baseQuery: k.SelectQueryBuilder<DB, TB, O>;
	#groupBy: keyof O | (keyof O)[];
	#queryId: k.QueryId;
	#relations?: Relations<DB, TB, RelationsO>;

	constructor(
		db: k.Kysely<any>,
		baseQuery: k.SelectQueryBuilder<DB, TB, O>,
		groupBy: keyof O | (keyof O)[],
		queryId: k.QueryId,
		relations: Relations<DB, TB, RelationsO>,
	) {
		this.#db = db;
		this.#baseQuery = baseQuery;
		this.#groupBy = groupBy;
		this.#queryId = queryId;
		this.#relations = relations;
	}

	withMany<
		RelationKey extends string,
		RelationDB,
		RelationTB extends keyof RelationDB,
		RelationO,
	>(
		key: RelationKey,
		queryBuilder: (
			qb: k.SelectQueryBuilder<DB, TB, {}>,
		) => k.SelectQueryBuilder<RelationDB, RelationTB, RelationO>,
		keyBy: keyof RelationO | (keyof RelationO)[],
	): RelationBuilder<
		RelationDB,
		RelationTB,
		O,
		Extend<RelationsO, { [K in RelationKey]: RelationO }>
	> {
		return new RelationBuilder(
			this.#db,
			this.#baseQuery,
			this.#groupBy,
			this.#queryId,
			{
				...this.#relations,
				[key]: queryBuilder,
			},
		) as any;
	}

	#rootRenames = new Map<string, string>();
	#relationRenames = new Map<string, Map<string, string>>();

	toOperationNode(): k.SelectQueryNode {
		const qb = this.#baseQuery;

		const renamedSelections: k.SelectionNode[] = [];

		let nameCount = 0;

		const renameSelections = (
			qb: k.SelectQueryBuilder<any, any, any>,
			renameMap: Map<string, string>,
		) => {
			const operationNode = qb.toOperationNode() as k.SelectQueryNode;
			if (!operationNode.selections?.length) {
				return;
			}

			for (const selection of operationNode.selections ?? []) {
				if (!k.SelectionNode.is(selection)) {
					throw new Error("Expected SelectionNode");
				}

				const newName = `__${nameCount++}`;
				const { oldName, renamedSelection } = renameSelectionNode(
					selection,
					newName,
				);

				renameMap.set(oldName, newName);
				renamedSelections.push(renamedSelection);
			}
		};

		let query: k.SelectQueryBuilder<any, any, any> = qb;
		renameSelections(query, this.#rootRenames);
		query = query.clearSelect();

		for (const nestedKey in this.#relations) {
			if (!Object.hasOwn(this.#relations, nestedKey)) {
				continue;
			}

			let nestedRenameMap = this.#relationRenames.get(nestedKey);
			if (!(nestedRenameMap instanceof Map)) {
				nestedRenameMap = new Map<string, string>();
				this.#relationRenames.set(nestedKey, nestedRenameMap);
			}

			const relationFn = this.#relations[nestedKey]!;
			query = relationFn(query);
			renameSelections(query, nestedRenameMap);
			query = query.clearSelect();
		}

		const renamed = k.SelectQueryNode.cloneWithSelections(
			query.toOperationNode(),
			renamedSelections,
		);

		return this.#db.getExecutor().transformQuery(renamed, this.#queryId);
	}

	compile(): k.CompiledQuery<Record<string, unknown>> {
		return this.#db
			.getExecutor()
			.compileQuery(this.toOperationNode(), this.#queryId);
	}

	print() {
		const compiledQuery = this.compile();
		console.log(compiledQuery.sql);
		console.log(this.#rootRenames);
		console.log(this.#relationRenames);
		return this;
	}

	private toNested(
		rows: Record<string, unknown>[],
	): RowWithRelations<O, RelationsO>[] {
		const rootRenames = this.#rootRenames;
		const relationRenames = this.#relationRenames;

		const results = new Map<string, any>();

		for (const row of rows) {
			const groupKey = Array.isArray(this.#groupBy)
				? this.#groupBy
						.map((k) => row[rootRenames.get(k as string)!])
						.join("::")
				: row[rootRenames.get(this.#groupBy as string)!]?.toString() || "";

			let result = results.get(groupKey);
			if (!result) {
				result = {};
				for (const [oldName, newName] of rootRenames) {
					result[oldName] = row[newName];
				}
				results.set(groupKey, result);
			}

			for (const [relationKey, renameMap] of relationRenames) {
				if (!result[relationKey]) {
					result[relationKey] = [];
				}

				const relatedRow: Record<string, unknown> = {};
				for (const [oldName, newName] of renameMap) {
					const value = row[newName];
					relatedRow[oldName] = value;
				}
				result[relationKey].push(relatedRow);
			}
		}
		return Array.from(results.values());
	}

	async execute(): Promise<RowWithRelations<O, RelationsO>[]> {
		const compiledQuery = this.compile();
		const result = await this.#db
			.getExecutor()
			.executeQuery<Record<string, unknown>>(compiledQuery);
		return this.toNested(result.rows);
	}

	async executeTakeFirst(): Promise<
		RowWithRelations<O, RelationsO> | undefined
	> {
		const [result] = await this.execute();
		return result;
	}

	async executeTakeFirstOrThrow(
		errorConstructor:
			| k.NoResultErrorConstructor
			| ((node: k.QueryNode) => Error) = k.NoResultError,
	): Promise<RowWithRelations<O, RelationsO>> {
		const result = await this.executeTakeFirst();
		if (result === undefined) {
			const error = k.isNoResultErrorConstructor(errorConstructor)
				? new errorConstructor(this.toOperationNode())
				: errorConstructor(this.toOperationNode());
			throw error;
		}
		return result;
	}
}

export const orm = <DB>(db: k.Kysely<DB>) => new Orm(db);
