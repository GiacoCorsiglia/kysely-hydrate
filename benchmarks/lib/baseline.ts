/**
 * Saving benchmark results and comparing one run against another.  mitata only
 * compares benchmarks within a run; this adds comparison across runs.  The
 * README explains why only time fails a comparison and allocation is shown
 * but never fails one.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

/** Median time in nanoseconds and mean heap bytes per iteration: mitata's units. */
export interface BaselineEntry {
	p50: number;
	heap?: number;
}

export interface Baseline {
	version: 2;
	/** Which suite recorded this, so one suite's baseline can't be diffed against another's. */
	suite: string;
	/** Where the numbers came from: a timestamp, or a git ref in `--ref` mode. */
	label: string;
	runtime: string;
	cpu: string;
	benchmarks: Record<string, BaselineEntry>;
}

/** The subset of mitata's `run()` result this module reads. */
export interface Trials {
	context: { runtime: string | null; cpu: { name: string | null } };
	benchmarks: readonly {
		alias: string;
		runs: readonly { stats?: { p50: number; heap?: { avg: number } | undefined } | undefined }[];
	}[];
}

/** A relative change below this is machine noise, not a change in the code. */
export const noiseThreshold = 0.2;

/**
 * Allocation per iteration below which a heap reading isn't compared.  It sits
 * well above the ~20 kb a garbage collection can add when it lands inside a
 * sample.
 */
const heapFloor = 64 * 1024;

export function fromTrials(suite: string, trials: Trials, label: string): Baseline {
	const benchmarks: Record<string, BaselineEntry> = {};
	for (const { alias, runs } of trials.benchmarks) {
		// `run({ throw: true })` means a failing benchmark never gets this far.
		// Static benchmarks have exactly one run.
		const stats = runs[0]?.stats;
		if (!stats) throw new Error(`Benchmark "${alias}" produced no stats`);
		benchmarks[alias] = { p50: stats.p50, ...(stats.heap && { heap: stats.heap.avg }) };
	}
	return {
		version: 2,
		suite,
		label,
		runtime: trials.context.runtime ?? "unknown",
		cpu: trials.context.cpu.name ?? "unknown",
		benchmarks,
	};
}

/** Reads and checks a baseline; call it before the suite runs, so a bad path fails at once. */
export function readBaseline(path: string, suite: string): Baseline {
	let parsed: Baseline;
	try {
		parsed = JSON.parse(readFileSync(path, "utf8"));
	} catch (cause) {
		throw new Error(`Could not read a benchmark baseline from ${path}`, { cause });
	}
	if (parsed?.version !== 2 || typeof parsed.benchmarks !== "object") {
		throw new Error(`${path} is not a version 2 benchmark baseline; re-record it`);
	}
	if (parsed.suite !== suite) {
		throw new Error(`${path} was recorded for suite "${parsed.suite}", not "${suite}"`);
	}
	return parsed;
}

export function writeBaseline(path: string, baseline: Baseline): void {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, `${JSON.stringify(baseline, null, 2)}\n`);
}

/**
 * Averages several runs of one suite into one.  `--ref` mode records each side
 * twice, in ABBA order, so that drift over the session cancels out rather than
 * favouring whichever side ran second.
 */
export function mergeBaselines(runs: readonly Baseline[]): Baseline {
	const [first] = runs;
	if (!first) throw new Error("No runs to merge");

	const mean = (values: (number | undefined)[]) =>
		values.some((v) => v === undefined)
			? undefined
			: (values as number[]).reduce((a, b) => a + b, 0) / values.length;

	const benchmarks: Record<string, BaselineEntry> = {};
	for (const name of Object.keys(first.benchmarks)) {
		const entries = runs.map((r) => r.benchmarks[name]);
		if (entries.some((e) => e === undefined)) continue;
		const heap = mean(entries.map((e) => e!.heap));
		benchmarks[name] = {
			p50: mean(entries.map((e) => e!.p50))!,
			...(heap !== undefined && { heap }),
		};
	}
	return { ...first, benchmarks };
}

////////////////////////////////////////////////////////////
// Reporting.
////////////////////////////////////////////////////////////

export function formatTime(ns: number): string {
	if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
	if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`;
	return `${ns.toFixed(2)} ns`;
}

function formatBytes(bytes: number): string {
	if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(2)} mb`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} kb`;
	return `${bytes.toFixed(0)} b`;
}

export interface Delta {
	label: string;
	regressed: boolean;
}

/**
 * Labels the change from `before` to `after`.  The band is asymmetric by ratio:
 * +20% flags but its inverse, -16.7%, does not, which is the right bias for a
 * regression gate.
 */
export function delta(before: number, after: number, threshold = noiseThreshold): Delta {
	const ratio = after / before - 1;
	if (!Number.isFinite(ratio)) return { label: "n/a", regressed: false };

	const pct = `${ratio >= 0 ? "+" : ""}${(ratio * 100).toFixed(1)}%`;
	if (Math.abs(ratio) < threshold) return { label: `${pct} (noise)`, regressed: false };
	if (ratio < 0) return { label: `${pct} faster`, regressed: false };
	return { label: `${pct} WORSE`, regressed: true };
}

/** Prints rows of cells, the first column left-aligned and the rest right-aligned. */
export function printTable(header: readonly string[], rows: readonly (readonly string[])[]): void {
	const widths = header.map((h, i) => Math.max(h.length, ...rows.map((r) => r[i]!.length)));
	const line = (cells: readonly string[]) =>
		cells.map((c, i) => (i === 0 ? c.padEnd(widths[i]!) : c.padStart(widths[i]!))).join("  ");

	console.log(`\n${line(header)}`);
	console.log("-".repeat(widths.reduce((a, b) => a + b + 2, -2)));
	for (const row of rows) console.log(line(row));
}

/**
 * Prints each benchmark's median time and mean allocation against `before`.
 * Returns true when no time regressed.  `reportMissing` is false for a
 * filtered run, where almost everything is missing by design.
 */
export function compareBaselines(
	before: Baseline,
	after: Baseline,
	{ reportMissing = true } = {},
): boolean {
	console.log(`\n  before: ${before.label}, ${before.runtime}, ${before.cpu}`);
	console.log(`  after:  ${after.label}, ${after.runtime}, ${after.cpu}`);
	if (before.cpu !== after.cpu || before.runtime !== after.runtime) {
		console.log("\n  ! Recorded on a different machine or runtime; deltas are not meaningful.");
	}

	const rows: string[][] = [];
	let regressed = false;

	for (const [name, now] of Object.entries(after.benchmarks)) {
		const then = before.benchmarks[name];
		const time = then ? delta(then.p50, now.p50) : { label: "new", regressed: false };
		const heap = !then
			? "new"
			: (then.heap ?? 0) < heapFloor || (now.heap ?? 0) < heapFloor
				? "too small"
				: delta(then.heap!, now.heap!).label;
		regressed ||= time.regressed;
		rows.push([
			name,
			formatTime(now.p50),
			time.label,
			now.heap === undefined ? "-" : formatBytes(now.heap),
			heap,
		]);
	}

	if (reportMissing) {
		for (const name of Object.keys(before.benchmarks)) {
			if (!(name in after.benchmarks)) rows.push([name, "-", "missing", "-", "missing"]);
		}
	}

	printTable(["benchmark", "p50", "vs before", "heap", "vs before"], rows);
	console.log(
		`\nTime beyond ±${noiseThreshold * 100}% is flagged and fails the run; allocation is shown only.`,
	);

	// An empty table means the filter matched nothing; passing it would turn a
	// typo into a green check.
	if (rows.length === 0) {
		console.log("\n  ! No benchmarks ran, so nothing was compared.");
		return false;
	}
	return !regressed;
}
