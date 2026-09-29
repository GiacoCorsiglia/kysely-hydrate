/**
 * What every suite shares: the command line, the benchmark declarations, and
 * the verify / run / save / compare cycle.  A suite declares its benchmarks
 * with `group()` and ends with `await runSuite()`.  See the README.
 */
import assert from "node:assert/strict";
import { constants } from "node:os";
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
	/** Where baselines are read and written. */
	"baselines-dir": { type: "string" },
	/** Label for a saved baseline; `--ref` passes the side's name. */
	label: { type: "string" },
	/**
	 * Internal to `--ref`: save this run's results into the given directory,
	 * even filtered, since each of its runs is compared only against the others.
	 */
	"ref-run-dir": { type: "string" },
	/** `run.ts` only: compare the working tree against this git ref. */
	ref: { type: "string" },
	"postgres-url": { type: "string" },
	"no-postgres": { type: "boolean" },
} as const satisfies ParseArgsOptionsConfig;

export function parseCli(args: string[], allowPositionals = false) {
	const parsed = parseArgs({
		args,
		options: cliOptions,
		allowPositionals,
		strict: true,
		tokens: true,
	});
	const { save, compare, filter, "verify-only": verifyOnly } = parsed.values;
	// A filtered save would drop every unmatched benchmark from the baseline.
	if (save && filter !== undefined) {
		throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
	}
	if (verifyOnly && (save || compare)) {
		throw new Error("--verify-only times nothing, so there is nothing to --save or --compare");
	}
	return parsed;
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

/** Every declared workload, in declaration order. */
const declared = new Map<Workload<any>, string>();

/**
 * Declares a mitata `summary()` group, each benchmark named `prefix` + its
 * key, the first the baseline the rest are compared against.  Every entry with
 * `expected` or `check` is run once and asserted before anything is timed.
 * One without is never run early when timing, which a benchmark whose first
 * call changes shared state needs; `--verify-only` still runs it once.
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
			// `do_not_optimize` stops the JIT discarding work whose result is unused.
			// A promise is returned as is: mitata awaits it, and a `.then()` here
			// would double the cost of a sub-microsecond async call.
			const b = bench(name, () => {
				const result = run();
				do_not_optimize(result);
				return result;
			});
			if (i === 0) b.baseline(true);
			if (!declared.has(workload)) declared.set(workload, name);
		});
	});
}

/**
 * Runs and asserts every checked workload.  Under `--verify-only`, nothing is
 * timed afterwards, so every unchecked one runs too, to prove it doesn't throw.
 */
async function verifyDeclared(): Promise<void> {
	const all = cli()["verify-only"];
	for (const [{ run, check, ...rest }, name] of declared) {
		if (!all && !check && !("expected" in rest)) continue;
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
// Tearing down.
////////////////////////////////////////////////////////////

const teardowns = new Set<() => unknown>();
let tearingDown: Promise<void> | undefined;

/** Runs every registered teardown, newest first, once; later calls await the first. */
function teardown(): Promise<void> {
	return (tearingDown ??= (async () => {
		for (const fn of [...teardowns].reverse()) {
			teardowns.delete(fn);
			try {
				await fn();
			} catch (error) {
				console.error("Teardown failed:", error);
			}
		}
	})());
}

/** Tears down, then exits as the process would have. */
const exitAfterTeardown = (code: number) => () => void teardown().finally(() => process.exit(code));

let handling = false;

/**
 * Registers `fn` to release a resource the moment it's opened.  It runs once:
 * at the end of `runSuite()`, on SIGINT or SIGTERM, or on an uncaught error,
 * whichever comes first.  The returned function runs it early and forgets it.
 */
export function onTeardown(fn: () => unknown): () => Promise<void> {
	if (!handling) {
		handling = true;
		for (const signal of ["SIGINT", "SIGTERM"] as const) {
			process.on(signal, exitAfterTeardown(128 + constants.signals[signal]));
		}
		process.on("uncaughtException", (error) => {
			console.error(error);
			exitAfterTeardown(1)();
		});
	}
	let done: Promise<void> | undefined;
	const once = () => (done ??= (async () => void (await fn()))());
	teardowns.add(once);
	return async () => {
		teardowns.delete(once);
		await once();
	};
}

////////////////////////////////////////////////////////////
// Running.
////////////////////////////////////////////////////////////

interface SuiteOptions {
	/** Assertions beyond the declared workloads' own. */
	verify?: () => unknown;
	/** Times the suite; defaults to running every declared `group()` through mitata. */
	measure?: () => Measured | Promise<Measured>;
}

/**
 * Verifies, measures, then saves or compares a baseline as the flags ask.
 * Verifying comes first because a change that makes a workload skip its work
 * would otherwise read as a large speedup.  A regression sets the exit code
 * instead of throwing, so the report is still printed.  Every `onTeardown()`
 * runs at the end, whether or not the run succeeded.
 */
export async function runSuite({ verify, measure = runMitata }: SuiteOptions = {}): Promise<void> {
	const suite = basename(process.argv[1] ?? "", ".bench.ts");
	const {
		save,
		compare,
		filter,
		label,
		"baselines-dir": dir = join(benchmarksDir, "baselines"),
		"ref-run-dir": refRunDir,
		"verify-only": verifyOnly,
	} = cli();
	const path = (d: string) => join(d, `${suite}.json`);

	try {
		// Read before running, so a missing baseline fails in a second, not minutes.
		const before = compare ? readBaseline(path(dir), suite) : undefined;

		await verifyDeclared();
		await verify?.();
		if (verifyOnly) return console.log(`${suite}: workloads verified.`);

		const after: Baseline = {
			version: 2,
			suite,
			label: label ?? new Date().toISOString(),
			...(await measure()),
		};
		// Not an error: `--filter` spans every suite, and most match in only some.
		// A `--compare` still fails below, since nothing was compared.
		if (Object.keys(after.benchmarks).length === 0) {
			console.log(`${suite}: no benchmark matches --filter ${filter}`);
		}
		for (const target of [save && dir, refRunDir]) {
			if (!target) continue;
			writeBaseline(path(target), after);
			console.log(`\nSaved baseline to ${path(target)}`);
		}
		if (before && !compareBaselines(before, after, { reportMissing: filter === undefined })) {
			process.exitCode = 1;
		}
	} finally {
		// A pg pool left open would keep the process alive after a good report.
		await teardown();
	}
}

async function runMitata(): Promise<Measured> {
	const { filter } = cli();
	// `throw: true` fails the run on a failing benchmark, rather than dropping it
	// from the report and the baseline.
	return fromTrials(await run({ throw: true, ...(filter && { filter: new RegExp(filter) }) }));
}
