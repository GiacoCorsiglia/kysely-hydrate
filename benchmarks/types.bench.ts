/**
 * Type-checking cost of the fixtures in `types/`, one program per fixture.
 * The TypeScript API checks just the fixture, so the library's own bodies,
 * which a user of the published `.d.ts` never checks, stay out of the count.
 * See the README for how the numbers are compared.
 */
import { readdirSync, readFileSync } from "node:fs";
import { cpus } from "node:os";
import { join } from "node:path";

import ts from "typescript";

import {
	type Baseline,
	delta,
	formatTime,
	printTable,
	readBaseline,
	writeBaseline,
} from "./lib/baseline.ts";
import { benchmarksDir, cli } from "./lib/harness.ts";

/** Instantiation growth beyond this fails a comparison; the count is deterministic. */
const countThreshold = 0.02;
const repeats = 5;

const { save, compare, filter, label, ...flags } = cli();
const verifyOnly = flags["verify-only"] ?? false;
const path = join(flags["baselines-dir"] ?? join(benchmarksDir, "baselines"), "types.json");
if (save && filter !== undefined && flags["baselines-dir"] === undefined) {
	throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
}
const before = compare ? readBaseline(path, "types") : undefined;

const fixturesDir = join(benchmarksDir, "types");
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

interface Measurement {
	instantiations: number;
	types: number;
	/** Median check time, in nanoseconds. */
	time: number;
	/** Bytes the checker retained, when `--expose-gc` allows measuring it. */
	heap?: number;
}

let oldProgram: ts.Program | undefined;

/**
 * Checks one fixture in a fresh program, reusing parsed files from the last
 * one.  Collecting garbage around the check slows it down, so a run either
 * times the check or weighs what it retained, never both.
 */
function checkOnce(file: string, weigh: boolean) {
	const program = ts.createProgram({ rootNames: [file], options, oldProgram });
	oldProgram = program;
	program.getTypeChecker();
	const source = program.getSourceFile(file)!;
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
	return { diagnostics, instantiations: i1 - i0, types: t1 - t0, time, heap };
}

/**
 * Checks a fixture `repeats` times, failing on any error.  Every fixture but
 * `empty` must assert its result with expect-type, which rejects `any`, so a
 * fixture whose types collapse fails rather than reading as a speedup.
 */
function measure(name: string): Measurement {
	const file = join(fixturesDir, `${name}.ts`);
	if (name !== "empty" && !readFileSync(file, "utf8").includes(".toEqualTypeOf<")) {
		throw new Error(`types/${name}.ts asserts nothing with expectTypeOf(...).toEqualTypeOf<...>()`);
	}
	const runs = Array.from({ length: verifyOnly ? 1 : repeats }, () => checkOnce(file, false));
	const [first] = runs;
	if (first!.diagnostics.length > 0) {
		throw new Error(
			`types/${name}.ts does not type-check:\n${ts.formatDiagnostics(first!.diagnostics, {
				getCanonicalFileName: (f) => f,
				getCurrentDirectory: () => fixturesDir,
				getNewLine: () => "\n",
			})}`,
		);
	}
	if (runs.some((r) => r.instantiations !== first!.instantiations)) {
		throw new Error(`types/${name}.ts instantiated a different number of types on each run`);
	}
	const median = (xs: number[]) => xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
	return {
		instantiations: first!.instantiations,
		types: first!.types,
		time: median(runs.map((r) => r.time)),
	};
}

const pattern = filter === undefined ? undefined : new RegExp(filter);
const names = readdirSync(fixturesDir)
	.filter((f) => f.endsWith(".ts") && f !== "schema.ts" && f !== "empty.ts")
	.map((f) => f.slice(0, -".ts".length))
	.filter((n) => !pattern || pattern.test(n))
	.sort();
if (names.length === 0) throw new Error(`No types fixture matches --filter ${filter}`);

// `empty` loads the library and the schema; the rest are reported net of it.
const empty = measure("empty");
const results = new Map<string, Measurement>(
	names.map((name) => {
		const m = measure(name);
		return [
			name,
			{ ...m, instantiations: m.instantiations - empty.instantiations, types: m.types - empty.types },
		];
	}),
);
if (verifyOnly) {
	console.log(`types: ${names.length} fixtures verified.`);
	process.exit();
}
// Weighed after every fixture is timed, so the collections don't slow the timings.
if (globalThis.gc) {
	for (const [name, m] of results) m.heap = checkOnce(join(fixturesDir, `${name}.ts`), true).heap;
}

/** `p50` is check time and `count` net instantiations, so time is never misread. */
const after: Baseline = {
	version: 2,
	suite: "types",
	label: label ?? new Date().toISOString(),
	runtime: `typescript ${ts.version}, node ${process.version}`,
	cpu: cpus()[0]?.model ?? "unknown",
	benchmarks: Object.fromEntries(
		[...results].map(([name, m]) => [
			name,
			{ p50: m.time, count: m.instantiations, ...(m.heap !== undefined && { heap: m.heap }) },
		]),
	),
};

const kb = (bytes: number | undefined) => (bytes === undefined ? "-" : `${(bytes / 1024).toFixed(0)} kb`);
const entryOf = (b: Baseline | undefined, name: string) =>
	b?.benchmarks[name] as { p50: number; count?: number } | undefined;

let regressed = false;
const rows = [...results].map(([name, m]) => {
	const row = [name, String(m.instantiations), String(m.types), formatTime(m.time), kb(m.heap)];
	if (!before) return row;
	const then = entryOf(before, name);
	if (then?.count === undefined) return [...row, "new", "new"];
	const count = delta(then.count, m.instantiations, countThreshold);
	regressed ||= count.regressed;
	return [...row, count.label, delta(then.p50, m.time).label];
});
if (before && filter === undefined) {
	for (const name of Object.keys(before.benchmarks)) {
		if (!results.has(name)) rows.push([name, "-", "-", "-", "-", "missing", "missing"]);
	}
}

if (before) {
	console.log(`\n  before: ${before.label}, ${before.runtime}`);
	console.log(`  after:  ${after.label}, ${after.runtime}`);
}
printTable(
	[
		"fixture",
		"instantiations",
		"types",
		"check",
		"retained",
		...(before ? ["instantiations vs before", "check vs before"] : []),
	],
	rows,
);
console.log(
	`\nNet of the empty fixture (${empty.instantiations} instantiations, ${formatTime(empty.time)}).` +
		(before
			? ` Instantiation growth beyond ${countThreshold * 100}% fails the run; check time is shown only.`
			: ""),
);

if (save) {
	writeBaseline(path, after);
	console.log(`\nSaved baseline to ${path}`);
}
if (regressed) process.exitCode = 1;
