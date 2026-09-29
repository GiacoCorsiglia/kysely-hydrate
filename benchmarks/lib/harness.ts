/**
 * What every suite shares: the command line, the benchmark declarations, and
 * the verify / run / save / compare cycle.  A suite declares its benchmarks
 * inside mitata `summary()` groups and ends with `await runSuite({ verify })`.
 * See the README for the flags and how to read the output.
 */
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs, type ParseArgsOptionsConfig } from "node:util";

import { bench, do_not_optimize, run } from "mitata";

import { compareBaselines, fromTrials, readBaseline, writeBaseline } from "./baseline.ts";

export const benchmarksDir = join(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Every flag any suite or `run.ts` accepts.  One list, parsed strictly on both
 * sides, so a typo fails loudly and `run.ts` knows which flags take a value.
 */
export const cliOptions = {
	save: { type: "boolean" },
	compare: { type: "boolean" },
	/** Run every suite's correctness checks and exit without timing anything. */
	"verify-only": { type: "boolean" },
	filter: { type: "string" },
	/** Where baselines are read and written; `--ref` points this at a scratch directory. */
	"baselines-dir": { type: "string" },
	/** Label for a saved baseline; `--ref` passes the git ref. */
	label: { type: "string" },
	/** `run.ts` only: compare the working tree against this git ref. */
	ref: { type: "string" },
	"postgres-url": { type: "string" },
	"no-postgres": { type: "boolean" },
} as const satisfies ParseArgsOptionsConfig;

let parsed: ReturnType<typeof parseCli>["values"] | undefined;

export function parseCli(args: string[], allowPositionals = false) {
	return parseArgs({ args, options: cliOptions, allowPositionals, strict: true, tokens: true });
}

/** This suite's flags, parsed once. */
export function cli() {
	return (parsed ??= parseCli(process.argv.slice(2)).values);
}

////////////////////////////////////////////////////////////
// Declaring benchmarks.
////////////////////////////////////////////////////////////

interface BenchOptions {
	/** The entry the rest of its `summary()` group is compared against. */
	baseline?: boolean;
}

/**
 * `--filter` takes a regex, so a benchmark name has to match itself as one.
 * `+` and `()` have silently made benchmarks unselectable before.
 */
function declare(name: string, fn: () => unknown, options: BenchOptions) {
	let matches = false;
	try {
		matches = new RegExp(name).test(name);
	} catch {}
	if (!matches) throw new Error(`Benchmark name is not usable as a --filter pattern: ${name}`);

	const b = bench(name, fn);
	if (options.baseline) b.baseline(true);
	return b;
}

/** `do_not_optimize` stops the JIT discarding work whose result is thrown away. */
export function benchSync(name: string, fn: () => unknown, options: BenchOptions = {}) {
	return declare(name, () => do_not_optimize(fn()), options);
}

export function benchAsync(name: string, fn: () => Promise<unknown>, options: BenchOptions = {}) {
	return declare(name, async () => do_not_optimize(await fn()), options);
}

////////////////////////////////////////////////////////////
// Running.
////////////////////////////////////////////////////////////

interface SuiteOptions {
	/**
	 * Runs every workload once and asserts its output before anything is timed.
	 * A change that makes a workload stop doing its work would otherwise read as
	 * a large speedup instead of a failure.
	 */
	verify?: () => unknown;
	/** Releases what the suite opened, whether or not the run succeeded. */
	teardown?: () => unknown;
}

/**
 * Where this run reads and writes `suite`'s baseline.  Refuses a filtered
 * `--save` into the default directory, which would silently drop every
 * unmatched benchmark from the baseline.
 */
export function baselinePath(suite: string): string {
	const { save, filter, "baselines-dir": dir } = cli();
	if (save && filter !== undefined && dir === undefined) {
		throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
	}
	return join(dir ?? join(benchmarksDir, "baselines"), `${suite}.json`);
}

/**
 * Verifies, runs everything the suite declared, then saves or compares a
 * baseline as the flags ask.  A regression sets the exit code instead of
 * throwing, so the report is still printed.
 */
export async function runSuite({ verify, teardown }: SuiteOptions = {}): Promise<void> {
	const suite = basename(process.argv[1] ?? "", ".bench.ts");
	const { compare, filter, label, ...flags } = cli();

	try {
		const path = baselinePath(suite);
		// Read before running, so a missing baseline fails in a second, not minutes.
		const before = compare ? readBaseline(path, suite) : undefined;

		await verify?.();
		if (flags["verify-only"]) return console.log(`${suite}: workloads verified.`);

		// `throw: true` makes a failing benchmark fail the run instead of being
		// dropped from the report and the baseline.
		const trials = await run({ throw: true, ...(filter && { filter: new RegExp(filter) }) });
		const after = fromTrials(suite, trials, label ?? new Date().toISOString());

		if (flags.save) {
			writeBaseline(path, after);
			console.log(`\nSaved baseline to ${path}`);
		}
		if (before && !compareBaselines(before, after, { reportMissing: filter === undefined })) {
			process.exitCode = 1;
		}
	} finally {
		// A pg pool left open would keep the process alive after a good report.
		await teardown?.();
	}
}
