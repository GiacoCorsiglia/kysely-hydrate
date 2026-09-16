/**
 * Saving and comparing benchmark baselines.
 *
 * mitata compares benchmarks against each other within a run (that's what
 * `summary()` prints), but has no built-in notion of comparing one run against
 * an earlier one.  This adds it, over the stats mitata already returns.
 *
 * Both signals are noisy enough to need a threshold.  Measured over three
 * consecutive runs of unchanged code on one machine, wall-clock medians drifted
 * by up to 17% and allocation per iteration by up to 16%, so
 * {@link noiseThreshold} treats anything smaller as the machine rather than the
 * code.  Only a diff taken on quiet, dedicated hardware should be read finer
 * than that.
 *
 * Allocation is compared as mitata's mean, which measurement says is the right
 * statistic and the only robust one available.  Two things that look like better
 * ideas are not: the within-run spread (`heapMax / heapMin`) is no guide to
 * quality, since benchmarks with a 20,000x internal spread still reproduce their
 * mean to within 1% across runs; and `heapMin` is dramatically less stable than
 * the mean, drifting up to 566x across runs where the mean drifts 1.19x.
 *
 * The mean does have one failure mode, which {@link heapFloor} handles.  A
 * benchmark that allocates very little occasionally reports roughly 20 kb more
 * than it should, when a collection lands inside a sample window and survives
 * into the mean.  For a benchmark allocating megabytes that contamination is
 * lost in the noise; for one allocating a few hundred bytes it is a 2000%
 * "regression".
 */
import { readFileSync, writeFileSync } from "node:fs";

/** Times are nanoseconds, heap is bytes: mitata's own units, kept as-is. */
export interface BaselineEntry {
	avg: number;
	p50: number;
	p75: number;
	p99: number;
	min: number;
	max: number;
	heap?: number;
	gc?: number;
}

export interface Baseline {
	version: 1;
	/** Which suite recorded this, so a baseline can't be diffed against another's. */
	suite: string;
	createdAt: string;
	runtime: string;
	cpu: string;
	benchmarks: Record<string, BaselineEntry>;
}

/** The subset of mitata's result shape this module needs. */
interface Trials {
	context: { runtime: string | null; cpu: { name: string | null } };
	benchmarks: readonly {
		alias: string;
		runs: readonly {
			error?: unknown;
			stats?:
				| {
						avg: number;
						p50: number;
						p75: number;
						p99: number;
						min: number;
						max: number;
						heap?: { avg: number } | undefined;
						gc?: { avg: number } | undefined;
				  }
				| undefined;
		}[];
	}[];
}

/**
 * Relative change below which a delta is treated as machine noise rather than a
 * change in the code.  Applied to both time and allocation; see the note above.
 */
export const noiseThreshold = 0.2;

/**
 * Allocation per iteration below which a heap measurement isn't compared; see
 * the note above.  Set well clear of the ~20 kb contamination quantum, which
 * keeps the heap signal for every benchmark where it survived repeated runs and
 * drops it only for the handful too small to measure this way.
 */
const heapFloor = 64 * 1024;

/** Whether a heap measurement is large enough to diff meaningfully. */
function comparableHeap(heap: number | undefined): heap is number {
	return heap !== undefined && heap >= heapFloor;
}

/**
 * Benchmarks that failed rather than producing stats.  mitata records the error
 * on the run instead of throwing, so without this a benchmark that starts
 * throwing would quietly vanish from the baseline and from every later
 * comparison.
 */
function failedBenchmarks(trials: Trials): string[] {
	return trials.benchmarks.filter((t) => !t.runs[0]?.stats).map((t) => t.alias);
}

function toBaseline(suite: string, trials: Trials): Baseline {
	const benchmarks: Record<string, BaselineEntry> = {};

	for (const trial of trials.benchmarks) {
		// Static benchmarks produce exactly one run; parameterized ones would
		// produce several, which this format doesn't distinguish, so take the first.
		const stats = trial.runs[0]?.stats;
		if (!stats) continue;

		benchmarks[trial.alias] = {
			avg: stats.avg,
			p50: stats.p50,
			p75: stats.p75,
			p99: stats.p99,
			min: stats.min,
			max: stats.max,
			...(stats.heap && { heap: stats.heap.avg }),
			...(stats.gc && { gc: stats.gc.avg }),
		};
	}

	return {
		version: 1,
		suite,
		createdAt: new Date().toISOString(),
		runtime: trials.context.runtime ?? "unknown",
		cpu: trials.context.cpu.name ?? "unknown",
		benchmarks,
	};
}

/**
 * Reads and validates a baseline.  Call this before running the suite, so a bad
 * path fails in a second rather than after several minutes of benchmarking.
 */
export function readBaseline(path: string): Baseline {
	let parsed: unknown;
	try {
		parsed = JSON.parse(readFileSync(path, "utf8"));
	} catch (cause) {
		throw new Error(`Could not read a benchmark baseline from ${path}`, { cause });
	}

	if (
		typeof parsed !== "object" ||
		parsed === null ||
		(parsed as Baseline).version !== 1 ||
		typeof (parsed as Baseline).suite !== "string" ||
		typeof (parsed as Baseline).benchmarks !== "object"
	) {
		throw new Error(`${path} is not a version 1 benchmark baseline`);
	}

	return parsed as Baseline;
}

export function saveBaseline(suite: string, path: string, trials: Trials): void {
	const failed = failedBenchmarks(trials);
	if (failed.length > 0) {
		throw new Error(`Refusing to save a baseline; these benchmarks failed: ${failed.join(", ")}`);
	}

	// Two-space JSON with a trailing newline, so a saved baseline diffs readably.
	// mitata's raw result is ~15MB because it carries every sample and the
	// generated source of its measurement loop; only the summary is kept.
	writeFileSync(path, `${JSON.stringify(toBaseline(suite, trials), null, 2)}\n`);
	console.log(`\nSaved baseline to ${path}`);
}

function formatTime(ns: number): string {
	if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
	if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`;
	return `${ns.toFixed(2)} ns`;
}

function formatBytes(bytes: number): string {
	if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(2)} mb`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} kb`;
	return `${bytes.toFixed(0)} b`;
}

interface Delta {
	label: string;
	/** Whether this counts as a regression, rather than something to read off the label. */
	regressed: boolean;
}

const noDelta = (label: string): Delta => ({ label, regressed: false });

function toHeapDelta(before: BaselineEntry, after: BaselineEntry): Delta {
	if (!comparableHeap(before.heap) || !comparableHeap(after.heap)) return noDelta("too small");
	return toDelta(before.heap, after.heap);
}

function toDelta(before: number | undefined, after: number | undefined): Delta {
	if (before === undefined || after === undefined) return noDelta("-");

	const ratio = after / before - 1;
	// A zero baseline (or a zero reading) makes the ratio meaningless rather than
	// infinitely bad.
	if (!Number.isFinite(ratio)) return noDelta("n/a");

	const pct = `${ratio >= 0 ? "+" : ""}${(ratio * 100).toFixed(1)}%`;

	// The band is deliberately ratio-asymmetric: +20% flags but its exact inverse,
	// -16.7%, does not.  That's the right bias for a regression gate.
	if (Math.abs(ratio) < noiseThreshold) return noDelta(`${pct} (noise)`);
	if (ratio < 0) return noDelta(`${pct} faster`);
	return { label: `${pct} WORSE`, regressed: true };
}

interface Column {
	header: string;
	align: "left" | "right";
	values: string[];
}

function printTable(columns: Column[]): void {
	const widths = columns.map((c) => Math.max(c.header.length, ...c.values.map((v) => v.length)));
	const pad = (value: string, i: number) =>
		columns[i]!.align === "left" ? value.padEnd(widths[i]!) : value.padStart(widths[i]!);
	const gap = "  ";

	console.log(`\n${columns.map((c, i) => pad(c.header, i)).join(gap)}`);
	console.log("-".repeat(widths.reduce((a, b) => a + b, 0) + gap.length * (columns.length - 1)));

	for (let row = 0; row < (columns[0]?.values.length ?? 0); row++) {
		console.log(columns.map((c, i) => pad(c.values[row]!, i)).join(gap));
	}
}

export interface CompareOptions {
	/**
	 * Whether to list benchmarks the baseline has and this run doesn't.  False
	 * when the run was filtered, where almost everything is "missing" by design.
	 */
	reportMissing?: boolean;
}

/**
 * Prints each benchmark's median time and mean allocation against a saved
 * baseline.  Returns true when nothing regressed, so a caller can use it as an
 * exit status.
 */
export function compareBaseline(
	suite: string,
	baseline: Baseline,
	trials: Trials,
	{ reportMissing = true }: CompareOptions = {},
): boolean {
	const current = toBaseline(suite, trials);

	if (baseline.suite !== suite) {
		throw new Error(`Baseline was recorded for suite "${baseline.suite}", not "${suite}"`);
	}
	const failed = failedBenchmarks(trials);

	console.log(`\n  baseline: ${baseline.createdAt}, ${baseline.runtime}, ${baseline.cpu}`);
	console.log(`  current:  ${current.createdAt}, ${current.runtime}, ${current.cpu}`);

	if (baseline.cpu !== current.cpu || baseline.runtime !== current.runtime) {
		console.log("\n  ! Baseline was recorded on a different machine or runtime; deltas are");
		console.log("    not meaningful.  Re-record with `npm run bench:save`.");
	}

	const names: string[] = [];
	const times: string[] = [];
	const timeDeltas: string[] = [];
	const heaps: string[] = [];
	const heapDeltas: string[] = [];
	let regressed = false;

	const push = (name: string, time: Delta, heap: Delta, entry?: BaselineEntry) => {
		names.push(name);
		times.push(entry === undefined ? "-" : formatTime(entry.p50));
		timeDeltas.push(time.label);
		heaps.push(entry?.heap === undefined ? "-" : formatBytes(entry.heap));
		heapDeltas.push(heap.label);
		if (time.regressed || heap.regressed) regressed = true;
	};

	for (const [name, after] of Object.entries(current.benchmarks)) {
		const before = baseline.benchmarks[name];
		if (before) {
			push(name, toDelta(before.p50, after.p50), toHeapDelta(before, after), after);
		} else {
			push(name, noDelta("new"), noDelta("new"), after);
		}
	}

	if (reportMissing) {
		for (const name of Object.keys(baseline.benchmarks)) {
			if (name in current.benchmarks) continue;
			push(name, noDelta("missing"), noDelta("missing"));
		}
	}

	printTable([
		{ header: "benchmark", align: "left", values: names },
		{ header: "p50", align: "right", values: times },
		{ header: "vs base", align: "right", values: timeDeltas },
		{ header: "heap", align: "right", values: heaps },
		{ header: "vs base", align: "right", values: heapDeltas },
	]);

	console.log(
		`\nFlagged beyond ${(noiseThreshold * 100).toFixed(0)}% on time or allocation. "too small" marks a benchmark`,
	);
	console.log(
		`allocating under ${(heapFloor / 1024).toFixed(0)} kb per iteration, where the heap probe isn't reliable.`,
	);

	if (failed.length > 0) {
		console.log(`\n  ! These benchmarks failed to run: ${failed.join(", ")}`);
		return false;
	}

	// An empty table means the filter matched nothing.  Reporting that as a clean
	// comparison would turn a typo into a passing check.
	if (names.length === 0) {
		console.log("\n  ! No benchmarks ran, so nothing was compared.");
		return false;
	}

	return !regressed;
}
