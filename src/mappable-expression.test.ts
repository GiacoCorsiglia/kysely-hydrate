import { test } from "node:test";
import util from "node:util";

import { db as testDb } from "./__tests__/sqlite.ts";
import { KyselyHydratePlugin, map } from "./mappable-expression.ts";

const db = testDb.withPlugin(new KyselyHydratePlugin());

test.skip("mappableExpression", async () => {
	// const foo = db
	// 	.withPlugin(new KyselyHydratePlugin())
	// 	.selectFrom("users")
	// 	.select((eb) => [
	// 		map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId"),
	// 	]);

	const _update = db
		.updateTable("users")
		.set({
			email: "updated@example.com",
		})
		.where("id", "=", 1)
		.returning((eb) => [map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId")]);

	const fb = db
		.with("newUsers", (qb) =>
			qb
				.selectFrom("users")
				.select((eb) => [map(eb.ref("users.id"), (id) => (id * 100).toString()).as("bigId")]),
		)
		.selectFrom("newUsers")
		.select("bigId");

	const result = await fb.toOperationNode();

	console.log(util.inspect(result, { depth: null, colors: true }));
});
