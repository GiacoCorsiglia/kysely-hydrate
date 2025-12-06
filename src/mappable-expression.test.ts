import { test } from "node:test";
import util from "node:util";
import SQLite from "better-sqlite3";
import * as k from "kysely";
import { KyselyHydratePlugin, map } from "./mappable-expression.ts";
import { type SeedDB, seed } from "./seed.ts";

const sqlite = new SQLite(":memory:");
await seed(sqlite);

const dialect = new k.SqliteDialect({
	database: sqlite,
});

const db = new k.Kysely<SeedDB>({
	dialect,
	plugins: [new KyselyHydratePlugin()],
});

test("mappableExpression", async () => {
	// const foo = db
	// 	.withPlugin(new KyselyHydratePlugin())
	// 	.selectFrom("users")
	// 	.select((eb) => [
	// 		map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId"),
	// 	]);

	const update = db
		.updateTable("users")
		.set({
			email: "updated@example.com",
		})
		.where("id", "=", 1)
		.returning((eb) => [
			map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId"),
		]);

	const fb = db
		.with("newUsers", (qb) =>
			qb
				.with()
				.selectFrom("users")
				.select((eb) => [
					map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId"),
				]),
		)
		.selectFrom("newUsers")
		.select("bigId");

	const result = await fb.toOperationNode();

	console.log(util.inspect(result, { depth: null, colors: true }));
});
