/**
 * What every suite shares: the command line, the benchmark declarations, and
 * the verify / run / save / compare cycle.  A suite declares its benchmarks
 * with `group()` and ends with `await runSuite()`.  See the README.
 */
import assert from "node:assert/strict";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { inspect, isDeepStrictEqual, parseArgs, type ParseArgsOptionsConfig } from "node:util";

import { bench, do_not_optimize, run, summary } from "mitata";

import {
	type Baseline,
	compareBaselines,
	fromTrials,
	type Measured,
	readBaseline,
	writeBaseline,
} from "./baseline.ts";

export const benchmarksDir = join(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Every flag any suite or `run.ts` accepts.  One list, parsed strictly on both
 * sides, so a typo fails loudly and `run.ts` knows which flags take a value.
 */
const cliOptions = {
	save: { type: "boolean" },
	compare: { type: "boolean" },
	/** Run every suite's correctness checks and exit without timing anything. */
	"verify-only": { type: "boolean" },
	filter: { type: "string" },
	/** Where baselines are read and written; `--ref` points this at a scratch directory. */
	"baselines-dir": { type: "string" },
	/** Label for a saved baseline; `--ref` passes the side's name. */
	label: { type: "string" },
	/** `run.ts` only: compare the working tree against this git ref. */
	ref: { type: "string" },
	"postgres-url": { type: "string" },
	"no-postgres": { type: "boolean" },
} as const satisfies ParseArgsOptionsConfig;

export function parseCli(args: string[], allowPositionals = false) {
	return parseArgs({ args, options: cliOptions, allowPositionals, strict: true, tokens: true });
}

let parsed: ReturnType<typeof parseCli>["values"] | undefined;

/** This suite's flags, parsed once. */
export function cli() {
	return (parsed ??= parseCli(process.argv.slice(2)).values);
}

////////////////////////////////////////////////////////////
// Declaring benchmarks.
////////////////////////////////////////////////////////////

/** A measured call, and what verification asserts about one call's awaited result. */
export interface Workload<T = unknown> {
	run: () => T;
	/** Deep-equals the result; shorthand for a `check`. */
	expected?: unknown;
	check?: ((result: Awaited<T>) => unknown) | undefined;
}

/**
 * `assert.deepEqual` without the diff: Node's diff of two large results can
 * exhaust memory instead of failing.
 */
export function assertDeepEqual(actual: unknown, expected: unknown): void {
	if (isDeepStrictEqual(actual, expected)) return;
	const brief = (value: unknown) => inspect(value, { depth: 3, maxArrayLength: 3 });
	assert.fail(`Expected\n${brief(expected)}\nbut got\n${brief(actual)}`);
}

/** Everything declared with a check, in declaration order. */
const verified = new Map<Workload<any>, string>();

/**
 * Declares a mitata `summary()` group, each benchmark named `prefix` + its
 * key, the first the baseline the rest are compared against.  Every entry with
 * `expected` or `check` is run once and asserted before anything is timed; one
 * without is never run early, which a benchmark whose first call changes
 * shared state needs.
 */
export function group<R extends Record<string, unknown>>(
	workloads: { [K in keyof R]: Workload<R[K]> },
	prefix = "",
): void {
	summary(() => {
		Object.entries<Workload<any>>(workloads).forEach(([key, workload], i) => {
			const name = prefix + key;
			// `--filter` takes a regex; `+` and `()` have made names unselectable before.
			let selectable = false;
			try {
				selectable = new RegExp(name).test(name);
			} catch {}
			if (!selectable)
				throw new Error(`Benchmark name is not usable as a --filter pattern: ${name}`);

			const { run } = workload;
			// mitata times a call as async when it returns a promise.
			// `do_not_optimize` stops the JIT discarding work whose result is unused.
			const b = bench(name, () => {
				const result = run();
				return result instanceof Promise ? result.then(do_not_optimize) : do_not_optimize(result);
			});
			if (i === 0) b.baseline(true);
			if (("expected" in workload || workload.check) && !verified.has(workload)) {
				verified.set(workload, name);
			}
		});
	});
}

async function verifyDeclared(): Promise<void> {
	for (const [{ run, check, ...rest }, name] of verified) {
		try {
			const result = await run();
			if ("expected" in rest) assertDeepEqual(result, rest.expected);
			await check?.(result);
		} catch (cause) {
			throw new Error(`Verifying "${name}" failed`, { cause });
		}
	}
}

////////////////////////////////////////////////////////////
// Running.
////////////////////////////////////////////////////////////

interface SuiteOptions {
	/** Assertions beyond the declared workloads' own. */
	verify?: () => unknown;
	/** Times the suite; defaults to running every declared `group()` through mitata. */
	measure?: () => Measured | Promise<Measured>;
	/** Releases what the suite opened, whether or not the run succeeded. */
	teardown?: () => unknown;
}

/**
 * Verifies, measures, then saves or compares a baseline as the flags ask.
 * Verifying comes first because a change that makes a workload skip its work
 * would otherwise read as a large speedup.  A regression sets the exit code
 * instead of throwing, so the report is still printed.
 */
export async function runSuite({
	verify,
	measure = runMitata,
	teardown,
}: SuiteOptions = {}): Promise<void> {
	const suite = basename(process.argv[1] ?? "", ".bench.ts");
	const { save, compare, filter, label, "baselines-dir": dir, "verify-only": verifyOnly } = cli();

	try {
		// A filtered save into the default directory would drop every unmatched benchmark.
		if (save && filter !== undefined && dir === undefined) {
			throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
		}
		const path = join(dir ?? join(benchmarksDir, "baselines"), `${suite}.json`);
		// Read before running, so a missing baseline fails in a second, not minutes.
		const before = compare ? readBaseline(path, suite) : undefined;

		await verifyDeclared();
		await verify?.();
		if (verifyOnly) return console.log(`${suite}: workloads verified.`);

		const after: Baseline = {
			version: 2,
			suite,
			label: label ?? new Date().toISOString(),
			...(await measure()),
		};
		if (save) {
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

async function runMitata(): Promise<Measured> {
	const { filter } = cli();
	// `throw: true` fails the run on a failing benchmark, rather than dropping it
	// from the report and the baseline.
	return fromTrials(await run({ throw: true, ...(filter && { filter: new RegExp(filter) }) }));
}
