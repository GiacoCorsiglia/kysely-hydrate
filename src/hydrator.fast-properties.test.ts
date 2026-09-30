import assert from "node:assert";
import { describe, test } from "node:test";
import { setFlagsFromString } from "node:v8";

import { createHydrator, EnableAutoInclusion, hydrate } from "./hydrator.ts";

// Wide entities rely on V8 behaviour to stay out of dictionary mode (see
// `warmShape` in hydrator.ts); these tests catch a refactor or V8 upgrade that
// silently loses it.  Each test uses its own key names so that no test can
// benefit from another's hidden-class transitions.

setFlagsFromString("--allow-natives-syntax");
const hasFastProperties = new Function("o", "return %HasFastProperties(o)") as (
	o: object,
) => boolean;

type Row = Record<string, number>;

const WIDTH = 30;

function columns(name: string): string[] {
	return Array.from({ length: WIDTH }, (_, i) => (i === 0 ? "id" : `${name}${i}`));
}

/** Rows built by keyed stores, as drivers do, so they leave no transitions. */
function rows(keys: readonly string[], count = 3, prefix = ""): Row[] {
	return Array.from({ length: count }, (_, i) => {
		const row: Row = {};
		for (const key of keys) row[prefix + key] = key === "id" ? i + 1 : i;
		return row;
	});
}

/** The first entity of a plan builds its anchor, so may be slow; the rest may not. */
function assertFastAfterFirst(entities: readonly object[]) {
	assert.ok(entities.length > 1);
	for (const entity of entities.slice(1)) {
		assert.ok(Object.keys(entity).length >= WIDTH);
		assert.ok(hasFastProperties(entity));
	}
}

describe("Hydrator fast properties", () => {
	test("V8 still moves naively built wide objects to dictionary mode", () => {
		// If this fails, the anchor in hydrator.ts may no longer be needed.
		const [row] = rows(columns("control"), 1);
		assert.strictEqual(hasFastProperties(row!), false);
	});

	test("wide entities with fields", async () => {
		const keys = columns("fields");
		const result = await hydrate(rows(keys), createHydrator<Row>("id").fields(keys));
		assertFastAfterFirst(result);
	});

	test("wide nested entities", async () => {
		const keys = columns("nested");
		type Input = { id: number } & Record<`children$$${string}`, number>;
		const input = rows(keys, 3, "children$$").map((row) => ({ ...row, id: 1 }));
		const hydrator = createHydrator<Input>("id")
			.fields(["id"])
			.hasMany("children", "children$$", (h) => h("id").fields(keys));
		const [parent] = await hydrate(input, hydrator);
		assertFastAfterFirst(parent!.children);
	});

	test("wide entities from extend", async () => {
		const keys = columns("extend");
		const hydrator = createHydrator<Row>("id")
			.fields(["id"])
			.extend((input) => {
				const extended: Row = {};
				for (const key of keys) extended[`x${key}`] = input[key]!;
				return extended;
			});
		assertFastAfterFirst(await hydrate(rows(keys), hydrator));
	});

	test("wide auto-included entities, also after the columns change", async () => {
		// Same width, so the change is detected by key rather than by count.
		const hydrator = createHydrator<Row>("id");
		for (const name of ["autoA", "autoB", "autoA"]) {
			assertFastAfterFirst(
				await hydrator.hydrate(rows(columns(name)), {
					[EnableAutoInclusion]: true,
				}),
			);
		}
	});
});
