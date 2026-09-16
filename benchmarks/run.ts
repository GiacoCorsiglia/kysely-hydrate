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

const args = process.argv.slice(2);
// A bare word selects suites; everything else is a flag for the suites themselves.
const selectors = args.filter((a) => !a.startsWith("--") && !isFlagValue(a));
const flags = args.filter((a) => !selectors.includes(a));

/** Whether this argument is the value of a preceding `--flag value` pair. */
function isFlagValue(arg: string): boolean {
	const index = args.indexOf(arg);
	return index > 0 && args[index - 1]!.startsWith("--") && !args[index - 1]!.includes("=");
}

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
