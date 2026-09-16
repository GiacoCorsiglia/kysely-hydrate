/**
 * The bits every benchmark suite shares: argument handling, wrappers that keep
 * the JIT from discarding a discarded result, and the run/save/compare cycle.
 *
 * A suite file declares its benchmarks with {@link benchAsync} / {@link benchSync}
 * inside mitata's `summary()` groups, then ends with a single
 * `await runSuite("name")`.  Each suite runs in its own process (see `run.ts`),
 * so it only pays for the fixtures it imports.
 */
import { mkdirSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { bench, do_not_optimize, run } from "mitata";

import { type Baseline, compareBaseline, readBaseline, saveBaseline } from "./baseline.ts";

const benchmarksDir = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Where a suite's recorded baseline lives.  Machine-specific, so gitignored. */
function baselinePath(suite: string): string {
	return join(benchmarksDir, "baselines", `${suite}.json`);
}

/**
 * Flags that take a following value.  `run.ts` needs this to tell a suite
 * selector from a flag's argument, so the list lives here where both can see it:
 * a flag known to one and not the other silently changes which suites run.
 */
export const VALUE_FLAGS: ReadonlySet<string> = new Set(["--filter", "--postgres-url"]);

////////////////////////////////////////////////////////////
// Arguments.
////////////////////////////////////////////////////////////

/** Reads `--flag value` or `--flag=value`, and rejects anything ambiguous. */
export function flagValue(
	flag: string,
	argv: readonly string[] = process.argv.slice(2),
): string | undefined {
	const matches = argv.filter((a) => a === flag || a.startsWith(`${flag}=`));

	if (matches.length === 0) return undefined;
	if (matches.length > 1) throw new Error(`${flag} was given more than once`);

	const match = matches[0]!;
	if (match.startsWith(`${flag}=`)) {
		const value = match.slice(flag.length + 1);
		if (value === "") throw new Error(`${flag} requires a value`);
		return value;
	}

	// A separate value may legitimately start with "-" (a regex like "-foo"), so
	// only a second flag is rejected.
	const value = argv[argv.indexOf(match) + 1];
	if (value === undefined || value.startsWith("--")) throw new Error(`${flag} requires a value`);
	return value;
}

export function hasFlag(flag: string, argv: readonly string[] = process.argv.slice(2)): boolean {
	return argv.includes(flag);
}

interface Options {
	save: boolean;
	filter: string | undefined;
	/** The baseline to diff against, already read; absent unless `--compare` was given. */
	baseline: Baseline | undefined;
}

/**
 * Reads the suite's arguments and eagerly loads the baseline, so a bad or
 * missing one fails in a second rather than after the suite has run.
 */
function readOptions(suite: string): Options {
	const save = hasFlag("--save");
	const compare = hasFlag("--compare");
	const filter = flagValue("--filter");

	if (save && filter !== undefined) {
		// A filtered save would drop every unmatched benchmark from the baseline,
		// and nothing would later report them as missing.
		throw new Error("--save cannot be combined with --filter: it would truncate the baseline");
	}

	return {
		save,
		filter,
		baseline: compare ? readBaseline(baselinePath(suite), suite) : undefined,
	};
}

////////////////////////////////////////////////////////////
// Declaring benchmarks.
////////////////////////////////////////////////////////////

/**
 * A benchmark's name is its `--filter` pattern, and `--filter` takes a regex, so
 * a name has to match itself as one.  `+` and `()` have both silently made a
 * benchmark unselectable here before, so every name is checked at declaration
 * rather than left to whoever next copies one out of the report.
 */
function assertFilterable(name: string): string {
	let matches: boolean;
	try {
		matches = new RegExp(name).test(name);
	} catch {
		// An unbalanced bracket or paren throws rather than failing to match.
		matches = false;
	}

	if (!matches) {
		throw new Error(`Benchmark name is not usable as a --filter pattern: ${name}`);
	}
	return name;
}

/**
 * `do_not_optimize` keeps the JIT from discarding work whose result is thrown
 * away, which is every benchmark in this directory.
 */
export function benchAsync(name: string, fn: () => Promise<unknown>) {
	return bench(assertFilterable(name), async () => {
		do_not_optimize(await fn());
	});
}

export function benchSync(name: string, fn: () => unknown) {
	return bench(assertFilterable(name), () => {
		do_not_optimize(fn());
	});
}

////////////////////////////////////////////////////////////
// Running.
////////////////////////////////////////////////////////////

/**
 * Runs everything the suite declared, then saves or compares a baseline as the
 * arguments ask.  Sets a non-zero exit code on a regression rather than
 * throwing, so the report is still readable.
 */
export async function runSuite(
	suite = basename(process.argv[1] ?? "", ".bench.ts"),
): Promise<void> {
	const { save, filter, baseline } = readOptions(suite);

	// `throw: true` makes mitata propagate a failing benchmark instead of
	// recording the error on the run and carrying on, which would drop it from
	// the report and from any baseline.
	const trials = await run({
		throw: true,
		...(filter !== undefined && { filter: new RegExp(filter) }),
	});

	if (save) {
		mkdirSync(join(benchmarksDir, "baselines"), { recursive: true });
		saveBaseline(suite, baselinePath(suite), trials);
	}

	if (
		baseline !== undefined &&
		!compareBaseline(suite, baseline, trials, { reportMissing: filter === undefined })
	) {
		process.exitCode = 1;
	}
}
