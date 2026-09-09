/**
 * Benchmark harness for the order-by comparator. Not part of `npm test` --
 * the filename deliberately avoids the test runner's discovery patterns.
 *
 * Run with: node src/helpers/order-by.bench.ts
 *
 * Duck-typed types are represented by structural stubs rather than real
 * dependencies: what we measure is our own dispatch, not decimal.js.
 */
import { makeOrderByComparator, type OrderBy, sortBy } from "./order-by.ts";
import { createdPrefixedAccessor, getPrefixedValue } from "./prefixes.ts";

const N = 10_000;
const RUNS = 7;

// A cheap deterministic PRNG keeps runs comparable across invocations.
function makeRandom(seed: number): () => number {
	let state = seed;
	return () => {
		state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
		return state / 2_147_483_648;
	};
}

/** Structural stand-in for decimal.js/big.js/bignumber.js. */
class DecimalStub {
	readonly n: number;

	constructor(n: number) {
		this.n = n;
	}

	cmp(other: DecimalStub | number): number {
		const o = typeof other === "number" ? other : other.n;
		return this.n < o ? -1 : this.n > o ? 1 : 0;
	}
	toString(): string {
		return String(this.n);
	}
}

/** Structural stand-in for Temporal.PlainDate: static compare, throwing valueOf. */
class PlainDateStub {
	static compare(a: PlainDateStub, b: PlainDateStub): number {
		return a.iso < b.iso ? -1 : a.iso > b.iso ? 1 : 0;
	}
	readonly iso: string;

	constructor(iso: string) {
		this.iso = iso;
	}

	get [Symbol.toStringTag](): string {
		return "Temporal.PlainDate";
	}
	valueOf(): never {
		throw new TypeError("Do not use built-in arithmetic operators with Temporal objects.");
	}
	toString(): string {
		return this.iso;
	}
}

interface Scenario {
	readonly name: string;
	readonly rows: readonly unknown[];
	readonly orderings: readonly OrderBy<any>[];
	readonly getValue?: (obj: any, key: any) => unknown;
}

function buildScenarios(): Scenario[] {
	const rand = makeRandom(20260909);
	const ints = Array.from({ length: N }, () => ({ v: Math.floor(rand() * N) }));
	const floats = Array.from({ length: N }, () => ({ v: rand() * 1000 }));
	const strings = Array.from({ length: N }, () => ({ v: `row-${Math.floor(rand() * N)}` }));
	const bools = Array.from({ length: N }, () => ({ v: rand() < 0.5 }));
	const dates = Array.from({ length: N }, () => ({
		v: new Date(Date.UTC(2024, 0, 1) + Math.floor(rand() * 1e9)),
	}));
	const bigints = Array.from({ length: N }, () => ({ v: BigInt(Math.floor(rand() * N)) }));

	// Sparse nulls: the common real-world shape for a nullable column.
	const nullable = Array.from({ length: N }, () => ({
		v: rand() < 0.15 ? null : Math.floor(rand() * N),
	}));

	// Heterogeneous column: forces cross-rank resolution on most comparisons.
	const mixedPool: unknown[] = [1, "a", true, new Date(0), 2n, null, {}, 3.5, "z", false];
	const mixed = Array.from({ length: N }, () => ({
		v: mixedPool[Math.floor(rand() * mixedPool.length)],
	}));

	const decimals = Array.from({ length: N }, () => ({ v: new DecimalStub(rand() * 1000) }));
	const temporals = Array.from({ length: N }, () => ({
		v: new PlainDateStub(
			new Date(Date.UTC(2024, 0, 1) + Math.floor(rand() * 1e9)).toISOString().slice(0, 10),
		),
	}));
	const buffers = Array.from({ length: N }, () => ({
		v: Buffer.from([Math.floor(rand() * 256), Math.floor(rand() * 256), Math.floor(rand() * 256)]),
	}));
	const arrays = Array.from({ length: N }, () => ({
		v: [Math.floor(rand() * 10), Math.floor(rand() * 10), Math.floor(rand() * 10)],
	}));

	// Multi-column: three orderings, mixed directions, realistic tie density.
	const multi = Array.from({ length: N }, () => ({
		a: Math.floor(rand() * 10),
		b: `s${Math.floor(rand() * 100)}`,
		c: Math.floor(rand() * N),
	}));

	// Function key behind the hydrator's prefixed accessor -- the case where a
	// Proxy is allocated per key extraction.
	const prefix = "user_";
	const prefixed = Array.from({ length: N }, () => ({
		user_first: `first${Math.floor(rand() * 997)}`,
		user_last: `last${Math.floor(rand() * 997)}`,
	}));
	const keyFn = (r: any) => `${r.last}, ${r.first}`;
	const prefixedGetValue = (obj: any, key: any) =>
		typeof key === "function"
			? key(createdPrefixedAccessor(prefix, obj as object))
			: getPrefixedValue(prefix, obj, key as string);

	const asc: OrderBy<any>[] = [{ key: "v", direction: "asc" }];

	return [
		{ name: "number (int)", rows: ints, orderings: asc },
		{ name: "number (float)", rows: floats, orderings: asc },
		{ name: "string", rows: strings, orderings: asc },
		{ name: "boolean", rows: bools, orderings: asc },
		{ name: "Date", rows: dates, orderings: asc },
		{ name: "bigint", rows: bigints, orderings: asc },
		{ name: "number w/ 15% null", rows: nullable, orderings: asc },
		{ name: "mixed types", rows: mixed, orderings: asc },
		{ name: "decimal-like", rows: decimals, orderings: asc },
		{ name: "Temporal-like", rows: temporals, orderings: asc },
		{ name: "Buffer", rows: buffers, orderings: asc },
		{ name: "array", rows: arrays, orderings: asc },
		{
			name: "multi-column (3)",
			rows: multi,
			orderings: [
				{ key: "a", direction: "asc" },
				{ key: "b", direction: "desc" },
				{ key: "c", direction: "asc" },
			],
		},
		{
			name: "function key + Proxy",
			rows: prefixed,
			orderings: [{ key: keyFn, direction: "asc" }],
			getValue: prefixedGetValue,
		},
	];
}

function measure(run: () => unknown): number {
	run(); // warm up: let TurboFan settle before timing.
	run();

	const samples: number[] = [];
	for (let i = 0; i < RUNS; i++) {
		const start = performance.now();
		run();
		samples.push(performance.now() - start);
	}
	// Median is more stable than the mean under GC noise.
	samples.sort((a, b) => a - b);
	return samples[Math.floor(samples.length / 2)]!;
}

function main(): void {
	const scenarios = buildScenarios();
	const width = Math.max(...scenarios.map((s) => s.name.length));

	console.log(`order-by: ${N} rows, median of ${RUNS} runs\n`);
	console.log(
		`  ${"scenario".padEnd(width)}  ${"comparator".padStart(11)}  ${"sortBy".padStart(11)}  speedup`,
	);

	const results: Record<string, { comparator: number; sortBy: number }> = {};
	for (const scenario of scenarios) {
		const source = scenario.rows as unknown[];
		const orderings = scenario.orderings as any;

		const viaComparator = measure(() => {
			const copy = source.slice();
			copy.sort(makeOrderByComparator(orderings, scenario.getValue));
			return copy;
		});
		const viaSortBy = measure(() => sortBy(source, orderings, scenario.getValue));

		results[scenario.name] = { comparator: viaComparator, sortBy: viaSortBy };
		const speedup = viaComparator / viaSortBy;
		console.log(
			`  ${scenario.name.padEnd(width)}  ${viaComparator.toFixed(2).padStart(8)} ms  ${viaSortBy
				.toFixed(2)
				.padStart(8)} ms  ${speedup.toFixed(2)}x`,
		);
	}

	// Machine-readable tail so runs can be diffed across implementations.
	console.log(`\n${JSON.stringify(results)}`);
}

main();
