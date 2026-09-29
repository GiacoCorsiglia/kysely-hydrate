/**
 * Saving benchmark results and comparing one run against another.  mitata only
 * compares benchmarks within a run; this adds comparison across runs.  The
 * README explains why only time (or, for the types suite, a deterministic
 * count) fails a comparison and allocation is shown but never fails one.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

/** Median time in nanoseconds and mean heap bytes per iteration: mitata's units. */
export interface BaselineEntry {
	p50: number;
	heap?: number;
	/** A deterministic count (the types suite's instantiations); when present it gates instead of time. */
	count?: number;
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

/** A run's results, before the harness names and labels them. */
export type Measured = Pick<Baseline, "runtime" | "cpu" | "benchmarks">;

/** The subset of mitata's `run()` result this module reads. */
interface Trials {
	context: { runtime: string | null; cpu: { name: string | null } };
	benchmarks: readonly {
		alias: string;
		runs: readonly { stats?: { p50: number; heap?: { avg: number } | undefined } | undefined }[];
	}[];
}

/** A relative change below this is machine noise, not a change in the code. */
const noiseThreshold = 0.2;

/**
 * A deterministic count only moves when the code does, so its band is tight:
 * doubling `Flatten`'s mapped work moved the netted counts by only 1-2.4%.
 */
const countThreshold = 0.005;

/**
 * Allocation per iteration below which, on both sides, heap isn't compared.
 * It sits well above the ~20 kb a garbage collection can add when it lands
 * inside a sample.
 */
const heapFloor = 64 * 1024;

export function fromTrials(trials: Trials): Measured {
	const benchmarks: Record<string, BaselineEntry> = {};
	// Every benchmark here is static, so it has exactly one run.
	for (const { alias, runs } of trials.benchmarks) {
		const stats = runs[0]?.stats;
		if (!stats) throw new Error(`Benchmark "${alias}" produced no stats`);
		benchmarks[alias] = { p50: stats.p50, ...(stats.heap && { heap: stats.heap.avg }) };
	}
	return {
		// With the version, so a Node upgrade shows as a different runtime.
		runtime: `${trials.context.runtime ?? "unknown"} ${process.version}`,
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
	const isRecord = (v: unknown): v is Record<string, unknown> =>
		typeof v === "object" && v !== null && !Array.isArray(v);
	const valid =
		isRecord(parsed) &&
		parsed.version === 2 &&
		(["label", "runtime", "cpu"] as const).every((key) => typeof parsed[key] === "string") &&
		isRecord(parsed.benchmarks) &&
		Object.values(parsed.benchmarks).every(
			(e) =>
				isRecord(e) &&
				typeof e.p50 === "number" &&
				(["p50", "heap", "count"] as const).every(
					(key) => e[key] === undefined || (Number.isFinite(e[key]) && (e[key] as number) >= 0),
				),
		);
	if (!valid) throw new Error(`${path} is not a version 2 benchmark baseline; re-record it`);
	if (parsed.suite !== suite) {
		throw new Error(`${path} was recorded for suite "${parsed.suite}", not "${suite}"`);
	}
	return parsed;
}

export function writeBaseline(path: string, baseline: Baseline): void {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, `${JSON.stringify(baseline, null, 2)}\n`);
}

/** Throws unless every run recorded the same benchmarks, naming any that some lack. */
export function assertSameBenchmarks(runs: readonly Baseline[]): void {
	const names = new Set(runs.flatMap((r) => Object.keys(r.benchmarks)));
	const partial = [...names].filter((name) => runs.some((r) => !(name in r.benchmarks)));
	if (partial.length > 0) {
		throw new Error(`Missing from some runs: ${partial.join(", ")}`);
	}
}

/**
 * Averages several runs of one suite into one.  `--ref` mode records each side
 * twice, in ABBA order, so that drift over the session cancels out rather than
 * favouring whichever side ran second.  Every run must record the same
 * benchmarks, so one a run skipped can't vanish from the comparison.
 */
export function mergeBaselines(runs: readonly [Baseline, ...Baseline[]]): Baseline {
	assertSameBenchmarks(runs);
	const benchmarks: Record<string, BaselineEntry> = {};
	for (const name of Object.keys(runs[0].benchmarks)) {
		const entries = runs.map((r) => r.benchmarks[name]);
		const merged: Partial<BaselineEntry> = {};
		for (const key of ["p50", "heap", "count"] as const) {
			const values = entries.map((e) => e![key]);
			if (values.every((v) => v !== undefined)) {
				merged[key] = values.reduce((a, b) => a + b, 0) / values.length;
			}
		}
		benchmarks[name] = merged as BaselineEntry;
	}
	return { ...runs[0], benchmarks };
}

////////////////////////////////////////////////////////////
// Reporting.
////////////////////////////////////////////////////////////

export function formatTime(ns: number): string {
	if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
	if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`;
	return `${ns.toFixed(2)} ns`;
}

export function formatBytes(bytes: number): string {
	if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(2)} mb`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(2)} kb`;
	return `${bytes.toFixed(0)} b`;
}

/**
 * Labels the change from `before` to `after`.  The band is asymmetric by ratio:
 * +20% flags but its inverse, -16.7%, does not, which is the right bias for a
 * regression gate.
 */
function delta(before: number, after: number, threshold = noiseThreshold) {
	// Inputs are finite and non-negative, so only a zero `before` needs care.
	if (before === 0) {
		return after === 0
			? { label: "+0.0% (noise)", regressed: false }
			: { label: "up from 0 WORSE", regressed: true };
	}
	const ratio = after / before - 1;

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
 * Prints each benchmark's median time and mean allocation against `before`,
 * and its count when it has one.  Returns true when nothing regressed: the
 * count where there is one, the time otherwise.  A benchmark in `before` but
 * not `after` fails too, unless `reportMissing` is false: a filtered run,
 * where almost everything is missing by design.  So does a run where nothing
 * was compared.
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

	const counted = Object.values(after.benchmarks).some((e) => e.count !== undefined);
	const rows: string[][] = [];
	let regressed = false;
	let compared = 0;

	for (const [name, now] of Object.entries(after.benchmarks)) {
		const then = before.benchmarks[name];
		const isNew = { label: "new", regressed: false };
		const count =
			then?.count !== undefined && now.count !== undefined
				? delta(then.count, now.count, countThreshold)
				: isNew;
		// A counted benchmark gates on its count; an infinite band shows its time as noise.
		const time = then ? delta(then.p50, now.p50, counted ? Infinity : noiseThreshold) : isNew;
		const heap = !then
			? "new"
			: then.heap === undefined || now.heap === undefined
				? "-"
				: Math.max(then.heap, now.heap) < heapFloor
					? "too small"
					: delta(then.heap, now.heap).label;
		if (then) compared++;
		regressed ||= counted ? count.regressed : time.regressed;
		rows.push([
			name,
			...(counted ? [String(now.count ?? "-"), count.label] : []),
			formatTime(now.p50),
			time.label,
			now.heap === undefined ? "-" : formatBytes(now.heap),
			heap,
		]);
	}

	if (reportMissing) {
		for (const name of Object.keys(before.benchmarks)) {
			if (!(name in after.benchmarks)) {
				regressed = true;
				rows.push([name, ...(counted ? ["-", "missing"] : []), "-", "missing", "-", "missing"]);
			}
		}
	}

	printTable(
		[
			"benchmark",
			...(counted ? ["count", "vs before"] : []),
			"p50",
			"vs before",
			"heap",
			"vs before",
		],
		rows,
	);
	console.log(
		counted
			? `\nA count beyond ±${countThreshold * 100}% is flagged and fails the run; time and allocation are shown only.`
			: `\nTime beyond ±${noiseThreshold * 100}% is flagged and fails the run; allocation is shown only.`,
	);

	// Nothing compared means a filter matched nothing, or the baseline shares no
	// benchmark with this run; passing it would turn a typo into a green check.
	if (compared === 0) {
		console.log("\n  ! No benchmark was in both runs, so nothing was compared.");
		return false;
	}
	return !regressed;
}
