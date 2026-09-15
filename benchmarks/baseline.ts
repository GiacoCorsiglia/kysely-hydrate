/**
 * Saving and comparing benchmark baselines.
 *
 * mitata compares benchmarks against each other within a run (that's what
 * `summary()` prints), but has no built-in notion of comparing one run against
 * an earlier one.  This adds it, over the stats mitata already returns.
 *
 * Two caveats worth reading before trusting a diff.
 *
 * Wall-clock timings drift by well over 10% between runs on shared hardware,
 * purely from the machine.  Time deltas below {@link timeThreshold} are
 * therefore reported as noise rather than as changes, and only a diff taken on
 * quiet, dedicated hardware should be read more finely than that.
 *
 * Allocation per iteration is the steadier signal, but only for benchmarks that
 * allocate enough to measure.  mitata samples heap usage around each iteration
 * and discards samples whose delta is negative (a collection landed inside the
 * sample), so for a benchmark allocating a few hundred bytes the surviving mean
 * is dominated by probe granularity: the same unchanged benchmark has been seen
 * to report 744 b on one run and 17 kb on the next.  {@link comparableHeap}
 * rejects those measurements instead of reporting a 2000% regression.
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
	/** Recorded so {@link comparableHeap} can judge whether `heap` is trustworthy. */
	heapMin?: number;
	heapMax?: number;
	gc?: number;
}

export interface Baseline {
	version: 1;
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
			stats?:
				| {
						avg: number;
						p50: number;
						p75: number;
						p99: number;
						min: number;
						max: number;
						heap?: { avg: number; min: number; max: number } | undefined;
						gc?: { avg: number } | undefined;
				  }
				| undefined;
		}[];
	}[];
}

/** Relative change below which a time delta is treated as machine noise. */
export const timeThreshold = 0.2;

/** Relative change below which an allocation delta is treated as noise. */
export const heapThreshold = 0.1;

/** Allocation per iteration below which the heap probe can't resolve a change. */
const heapFloor = 1024 ** 2;

/** Spread above which a heap measurement is too unsteady to diff. */
const heapSpread = 4;

/**
 * Whether a heap measurement is solid enough to compare: it has to be well clear
 * of the probe's resolution, and its samples have to agree with each other.  A
 * benchmark whose heap ranges from 412 kb to 15.80 mb has told us nothing about
 * its typical allocation, so diffing its mean would manufacture regressions.
 */
function comparableHeap(
	entry: BaselineEntry | undefined,
): entry is BaselineEntry & { heap: number } {
	if (entry?.heap === undefined) return false;
	if (entry.heap < heapFloor) return false;
	if (entry.heapMin === undefined || entry.heapMax === undefined) return false;
	if (entry.heapMin <= 0) return false;
	return entry.heapMax / entry.heapMin <= heapSpread;
}

function toBaseline(trials: Trials): Baseline {
	const benchmarks: Record<string, BaselineEntry> = {};

	for (const trial of trials.benchmarks) {
		// Static benchmarks produce exactly one run; parameterized ones would
		// produce several, which this format doesn't distinguish, so take the
		// first.  Runs that threw have no stats to record.
		const stats = trial.runs[0]?.stats;
		if (!stats) continue;

		benchmarks[trial.alias] = {
			avg: stats.avg,
			p50: stats.p50,
			p75: stats.p75,
			p99: stats.p99,
			min: stats.min,
			max: stats.max,
			...(stats.heap && {
				heap: stats.heap.avg,
				heapMin: stats.heap.min,
				heapMax: stats.heap.max,
			}),
			...(stats.gc && { gc: stats.gc.avg }),
		};
	}

	return {
		version: 1,
		createdAt: new Date().toISOString(),
		runtime: trials.context.runtime ?? "unknown",
		cpu: trials.context.cpu.name ?? "unknown",
		benchmarks,
	};
}

export function saveBaseline(path: string, trials: Trials): void {
	// Two-space JSON with a trailing newline, so a committed baseline diffs
	// readably.  mitata's raw result is ~15MB because it carries every sample and
	// the generated source of its measurement loop; only the summary is kept.
	writeFileSync(path, `${JSON.stringify(toBaseline(trials), null, 2)}\n`);
	console.log(`\nSaved baseline to ${path}`);
}

function readBaseline(path: string): Baseline {
	const baseline: unknown = JSON.parse(readFileSync(path, "utf8"));

	if (
		typeof baseline !== "object" ||
		baseline === null ||
		(baseline as Baseline).version !== 1 ||
		typeof (baseline as Baseline).benchmarks !== "object"
	) {
		throw new Error(`${path} is not a version 1 benchmark baseline`);
	}

	return baseline as Baseline;
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

function formatDelta(before: number, after: number, threshold: number): string {
	const ratio = after / before - 1;
	const sign = ratio >= 0 ? "+" : "";
	const pct = `${sign}${(ratio * 100).toFixed(1)}%`;
	if (Math.abs(ratio) < threshold) return `${pct} (noise)`;
	return ratio > 0 ? `${pct} SLOWER` : `${pct} faster`;
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
 * baseline.  Returns true when every comparable benchmark is within its
 * threshold, so a caller can use it as an exit status.
 */
export function compareBaseline(
	path: string,
	trials: Trials,
	{ reportMissing = true }: CompareOptions = {},
): boolean {
	const baseline = readBaseline(path);
	const current = toBaseline(trials);

	console.log(`\nComparing against ${path}`);
	console.log(`  baseline: ${baseline.createdAt}, ${baseline.runtime}, ${baseline.cpu}`);
	console.log(`  current:  ${current.createdAt}, ${current.runtime}, ${current.cpu}`);

	if (baseline.cpu !== current.cpu || baseline.runtime !== current.runtime) {
		console.log("\n  ! Baseline was recorded on a different machine or runtime; deltas are");
		console.log("    not meaningful.  Re-record with `npm run bench:save`.");
	}

	const rows: {
		name: string;
		p50: string;
		p50Delta: string;
		heap: string;
		heapDelta: string;
	}[] = [];
	let withinThresholds = true;

	for (const [name, after] of Object.entries(current.benchmarks)) {
		const before = baseline.benchmarks[name];

		const heap = after.heap === undefined ? "-" : formatBytes(after.heap);

		if (!before) {
			rows.push({ name, p50: formatTime(after.p50), p50Delta: "new", heap, heapDelta: "new" });
			continue;
		}

		const p50Delta = formatDelta(before.p50, after.p50, timeThreshold);
		// Heap is only reported when the runtime exposes usage metrics, and only
		// worth diffing when both measurements are steady enough to mean something.
		const heapDelta =
			comparableHeap(before) && comparableHeap(after)
				? formatDelta(before.heap, after.heap, heapThreshold)
				: "unstable";

		if (p50Delta.includes("SLOWER") || heapDelta.includes("SLOWER")) withinThresholds = false;

		rows.push({ name, p50: formatTime(after.p50), p50Delta, heap, heapDelta });
	}

	if (reportMissing) {
		for (const name of Object.keys(baseline.benchmarks)) {
			if (name in current.benchmarks) continue;
			rows.push({ name, p50: "-", p50Delta: "missing", heap: "-", heapDelta: "missing" });
		}
	}

	const width = (key: keyof (typeof rows)[number], header: string) =>
		Math.max(header.length, ...rows.map((r) => r[key].length));
	const widths = {
		name: width("name", "benchmark"),
		p50: width("p50", "p50"),
		p50Delta: width("p50Delta", "vs base"),
		heap: width("heap", "heap"),
		heapDelta: width("heapDelta", "vs base"),
	};

	console.log(
		`\n${"benchmark".padEnd(widths.name)}  ${"p50".padStart(widths.p50)}  ${"vs base".padStart(widths.p50Delta)}  ${"heap".padStart(widths.heap)}  ${"vs base".padStart(widths.heapDelta)}`,
	);
	console.log(
		"-".repeat(widths.name + widths.p50 + widths.p50Delta + widths.heap + widths.heapDelta + 8),
	);

	for (const row of rows) {
		console.log(
			`${row.name.padEnd(widths.name)}  ${row.p50.padStart(widths.p50)}  ${row.p50Delta.padStart(widths.p50Delta)}  ${row.heap.padStart(widths.heap)}  ${row.heapDelta.padStart(widths.heapDelta)}`,
		);
	}

	console.log(
		`\nThresholds: ${(timeThreshold * 100).toFixed(0)}% on time, ${(heapThreshold * 100).toFixed(0)}% on allocation.`,
	);
	console.log(
		`"unstable" means the heap probe couldn't resolve this benchmark's allocation (under ${formatBytes(heapFloor)} per iteration, or samples spread over ${heapSpread}x).`,
	);

	return withinThresholds;
}
