/**
 * Type-checking cost of the fixtures in `types/`, one program per fixture.
 * The TypeScript API checks just the fixture, so the library's own bodies,
 * which a user of the published `.d.ts` never checks, stay out of the count.
 * See the README for how the numbers are compared.
 */
import { readdirSync } from "node:fs";
import { cpus } from "node:os";
import { join } from "node:path";

import ts from "typescript";

import { formatBytes, formatTime, printTable } from "./lib/baseline.ts";
import { benchmarksDir, cli, runSuite } from "./lib/harness.ts";

const fixturesDir = join(benchmarksDir, "types");
/** Checked first in every program; see the file. */
const warmUp = join(fixturesDir, "warm-up.ts");
const { options } = ts.getParsedCommandLineOfConfigFile(
	join(benchmarksDir, "..", "tsconfig.json"),
	{},
	{
		...ts.sys,
		onUnRecoverableConfigFileDiagnostic: (d) => {
			throw new Error(ts.flattenDiagnosticMessageText(d.messageText, "\n"));
		},
	},
)!;

const { filter } = cli();
const pattern = filter === undefined ? undefined : new RegExp(filter);
const names = readdirSync(fixturesDir)
	.filter((f) => f.endsWith(".ts") && f !== "schema.ts" && f !== "warm-up.ts")
	.map((f) => f.slice(0, -".ts".length))
	.filter((n) => !pattern || pattern.test(n))
	.sort();
if (names.length === 0) throw new Error(`No types fixture matches --filter ${filter}`);

let oldProgram: ts.Program | undefined;

/**
 * Checks one fixture in a fresh program, reusing parsed files from the last
 * one.  The counts are taken around the fixture's own check, after the
 * warm-up's, so they are what the fixture adds on top of resolving the
 * library's shared types; a fresh checker per call keeps them independent of
 * which fixtures ran before.  Collecting garbage around the check slows it
 * down, so a call either times the check or weighs what it retained, never both.
 */
function checkOnce(name: string, weigh = false) {
	const file = join(fixturesDir, `${name}.ts`);
	const program = ts.createProgram({
		rootNames: [warmUp, file],
		options,
		...(oldProgram && { oldProgram }),
	});
	oldProgram = program;
	const source = program.getSourceFile(file)!;
	const warmUpDiagnostics = program.getSemanticDiagnostics(program.getSourceFile(warmUp));
	const counts = () => [program.getInstantiationCount(), program.getTypeCount()] as const;

	const [i0, t0] = counts();
	const gc = weigh ? globalThis.gc : undefined;
	gc?.();
	const heap0 = process.memoryUsage().heapUsed;
	const start = process.hrtime.bigint();
	const diagnostics = [
		...program.getSyntacticDiagnostics(source),
		...program.getSemanticDiagnostics(source),
	];
	const time = Number(process.hrtime.bigint() - start);
	gc?.();
	const heap = gc && process.memoryUsage().heapUsed - heap0;
	const [i1, t1] = counts();
	return {
		diagnostics: [...warmUpDiagnostics, ...diagnostics],
		instantiations: i1 - i0,
		types: t1 - t0,
		time,
		heap,
		program,
		source,
	};
}

/** Does `node` contain a call to expect-type's `toEqualTypeOf<...>()`? */
function assertsType(checker: ts.TypeChecker, node: ts.Node): boolean {
	const isAssertion =
		ts.isCallExpression(node) &&
		node.typeArguments?.length === 1 &&
		ts.isPropertyAccessExpression(node.expression) &&
		node.expression.name.text === "toEqualTypeOf" &&
		!!checker
			.getSymbolAtLocation(node.expression.name)
			?.declarations?.some((d) => d.getSourceFile().fileName.includes("/expect-type/"));
	return (
		isAssertion || !!ts.forEachChild(node, (child) => assertsType(checker, child) || undefined)
	);
}

/**
 * Every fixture must type-check and assert its result with expect-type, which
 * rejects `any`, so one whose types collapse fails rather than reading as a
 * speedup.
 */
function verify(name: string): void {
	const { diagnostics, program, source } = checkOnce(name);
	if (!assertsType(program.getTypeChecker(), source)) {
		throw new Error(`types/${name}.ts asserts nothing with expectTypeOf(...).toEqualTypeOf<...>()`);
	}
	if (diagnostics.length > 0) {
		const host = {
			getCanonicalFileName: (f: string) => f,
			getCurrentDirectory: () => fixturesDir,
			getNewLine: () => "\n",
		};
		throw new Error(
			`types/${name}.ts does not type-check:\n${ts.formatDiagnostics(diagnostics, host)}`,
		);
	}
}

/** The median of 5 checks; instantiations must not vary between them. */
function measure(name: string) {
	const runs = Array.from({ length: 5 }, () => checkOnce(name));
	const [{ instantiations, types }] = runs as [(typeof runs)[number]];
	if (runs.some((r) => r.instantiations !== instantiations)) {
		throw new Error(`types/${name}.ts instantiated a different number of types on each run`);
	}
	const time = runs.map((r) => r.time).sort((a, b) => a - b)[2]!;
	return { instantiations, types, time };
}

await runSuite({
	verify: () => names.forEach(verify),
	measure: () => {
		const results = names.map((name) => ({ name, ...measure(name) }));
		// Weighed after every fixture is timed, so the collections don't slow the timings.
		const heaps = results.map(({ name }) => checkOnce(name, true).heap);

		printTable(
			["fixture", "instantiations", "types", "check", "retained"],
			results.map((m, i) => [
				m.name,
				String(m.instantiations),
				String(m.types),
				formatTime(m.time),
				heaps[i] === undefined ? "-" : formatBytes(heaps[i]),
			]),
		);
		// `p50` is check time and `count` instantiations, which gates instead of time.
		return {
			runtime: `typescript ${ts.version}, node ${process.version}`,
			cpu: cpus()[0]?.model ?? "unknown",
			benchmarks: Object.fromEntries(
				results.map((m, i) => [
					m.name,
					{
						p50: m.time,
						count: m.instantiations,
						...(heaps[i] !== undefined && { heap: heaps[i] }),
					},
				]),
			),
		};
	},
});
