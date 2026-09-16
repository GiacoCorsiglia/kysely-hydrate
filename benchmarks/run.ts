/**
 * Runs the benchmark suites, each in its own process.
 *
 *   npm run bench                     # every suite
 *   npm run bench -- order-by         # just the suites whose name contains this
 *   npm run bench -- --filter sortBy  # flags pass through to each suite
 *   npm run bench:save                # record a baseline per suite
 *   npm run bench:compare             # diff each suite against its baseline
 *
 * Separate processes are what let a suite import only its own fixtures: the
 * order-by suite never builds 10,000 rows or a SQLite database, so a targeted
 * run starts in milliseconds instead of seconds.  A shared process would also
 * make every suite's hydrate call sites megamorphic, though measurement says
 * that costs only a few percent once mitata collects between benchmarks.
 */
import { spawn } from "node:child_process";
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { VALUE_FLAGS } from "./lib/harness.ts";

const benchmarksDir = dirname(fileURLToPath(import.meta.url));

/** Suite files, in the order they should run: cheap and focused ones first. */
const order = ["order-by", "plugins", "query-build", "hydrate", "execute"];

function suiteNames(): string[] {
	const found = readdirSync(benchmarksDir)
		.filter((f) => f.endsWith(".bench.ts"))
		.map((f) => f.slice(0, -".bench.ts".length));

	// Known suites first in the order above, then anything new, so adding a file
	// doesn't require touching this list.
	return [
		...order.filter((s) => found.includes(s)),
		...found.filter((s) => !order.includes(s)).sort(),
	];
}

/**
 * Splits `npm run bench -- hydrate --filter sortBy` into the suites to run and
 * the flags to hand each one.
 *
 * This walks left to right and consults {@link VALUE_FLAGS}, rather than asking
 * whether a word follows a flag.  The naive version got both npm scripts wrong:
 * `bench:save -- hydrate` became `--save hydrate`, `hydrate` looked like
 * `--save`'s value, and all five suites ran.  A word appearing twice
 * (`hydrate --filter hydrate`) broke it the other way.
 */
function parseArgs(args: readonly string[]): { selectors: string[]; flags: string[] } {
	const selectors: string[] = [];
	const flags: string[] = [];

	for (let i = 0; i < args.length; i++) {
		const arg = args[i]!;
		if (!arg.startsWith("--")) {
			selectors.push(arg);
			continue;
		}

		flags.push(arg);
		// `--flag=value` carries its own value; `--flag value` takes the next word.
		if (VALUE_FLAGS.has(arg) && i + 1 < args.length) flags.push(args[++i]!);
	}

	return { selectors, flags };
}

const { selectors, flags } = parseArgs(process.argv.slice(2));

const suites = suiteNames().filter(
	(s) => selectors.length === 0 || selectors.some((sel) => s.includes(sel)),
);

if (suites.length === 0) {
	console.error(
		selectors.length > 0
			? `No benchmark suite matches ${selectors.join(", ")}. Available: ${suiteNames().join(", ")}`
			: "No *.bench.ts files found in benchmarks/",
	);
	process.exit(1);
}

let failed = 0;

for (const suite of suites) {
	console.log(`\n${"=".repeat(70)}\n${suite}\n${"=".repeat(70)}`);

	const code = await new Promise<number>((resolve, reject) => {
		const child = spawn(
			process.execPath,
			// --expose-gc lets mitata collect between samples and report heap usage.
			["--expose-gc", join(benchmarksDir, `${suite}.bench.ts`), ...flags],
			{ stdio: "inherit" },
		);
		child.on("error", reject);
		child.on("close", (c) => resolve(c ?? 1));
	});

	if (code !== 0) failed++;
}

if (failed > 0) {
	console.error(`\n${failed} of ${suites.length} suites reported a problem.`);
	process.exitCode = 1;
}
