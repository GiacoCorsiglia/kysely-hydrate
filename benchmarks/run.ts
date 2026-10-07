/**
 * Runs the benchmark suites, each in its own process, so each pays only for the
 * fixtures it imports.  See the README for usage.
 *
 * `--ref <git-ref>` checks the ref out into a scratch worktree, copies this
 * checkout's `benchmarks/` over it so both sides run identical benchmark code,
 * then runs each suite in ABBA order (ref, working tree, working tree, ref)
 * and compares the averages, so drift over the session cancels out.
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { cpSync, mkdtempSync, readdirSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
	assertSameBenchmarks,
	compareBaselines,
	mergeBaselines,
	readBaseline,
} from "./lib/baseline.ts";
import { benchmarksDir, onTeardown, parseCli } from "./lib/harness.ts";

const args = process.argv.slice(2);
const { values, positionals, tokens } = parseCli(args, true);

/** Cheap, focused suites first; any suite not listed runs after these, alphabetically. */
const order = ["plugins", "query-build", "hydrate", "execute", "types"];
const rank = (s: string) => (order.includes(s) ? order.indexOf(s) : order.length);

const available = readdirSync(benchmarksDir)
	.filter((f) => f.endsWith(".bench.ts"))
	.map((f) => f.slice(0, -".bench.ts".length))
	.sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));

const suites = available.filter(
	(s) => positionals.length === 0 || positionals.some((sel) => s.includes(sel)),
);
if (suites.length === 0) {
	console.error(`No benchmark suite matches ${positionals.join(", ")}. Available: ${available}`);
	process.exit(1);
}

const ownedByRef = [
	"save",
	"compare",
	"baselines-dir",
	"label",
	"verify-only",
	"ref-run-dir",
] as const;
if (values.ref !== undefined && ownedByRef.some((flag) => values[flag] !== undefined)) {
	console.error(`--ref records and compares its own runs; drop --${ownedByRef.join(", --")}`);
	process.exit(1);
}

/**
 * The flags to hand each suite: everything but the selectors and `--ref`.
 * Suites run from the repository root, so a relative directory is resolved
 * against the caller's working directory first.
 */
const passthrough = tokens.flatMap((t) => {
	if (t.kind !== "option" || t.name === "ref") return [];
	if (t.value === undefined) return [`--${t.name}`];
	return [`--${t.name}=${t.name === "baselines-dir" ? resolve(t.value) : t.value}`];
});

/**
 * Runs one suite to completion; true when it exited cleanly.  On an interrupt
 * the suite gets to tear down before anything it runs in is removed.
 */
async function runSuite(suite: string, cwd: string, extra: string[] = []): Promise<boolean> {
	// --expose-gc lets mitata collect between samples and report heap usage.
	const child: ChildProcess = spawn(
		process.execPath,
		["--expose-gc", join(cwd, "benchmarks", `${suite}.bench.ts`), ...passthrough, ...extra],
		{ stdio: "inherit", cwd },
	);
	const exited = once(child, "exit");
	let interrupted = false;
	const stop = onTeardown(async () => {
		interrupted = true;
		if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
		await exited;
	});
	const [code] = await exited;
	// Tearing down ends in an exit; start nothing more meanwhile.
	if (interrupted) await new Promise(() => {});
	await stop();
	return code === 0;
}

const banner = (title: string) => console.log(`\n${"=".repeat(70)}\n${title}\n${"=".repeat(70)}`);
const root = join(benchmarksDir, "..");
let failed = 0;

if (values.ref === undefined) {
	for (const suite of suites) {
		banner(suite);
		if (!(await runSuite(suite, root))) failed++;
	}
} else {
	const ref = values.ref;
	const scratch = mkdtempSync(join(tmpdir(), "kysely-hydrate-bench-"));
	const worktree = join(scratch, "ref");
	const git = (...a: string[]) => execFileSync("git", a, { cwd: root, stdio: "inherit" });
	// Registered before the worktree exists, so a failed `add` is cleaned up too.
	const cleanUp = onTeardown(() => {
		try {
			git("worktree", "remove", "--force", worktree);
		} catch {
			git("worktree", "prune");
		}
		rmSync(scratch, { recursive: true, force: true });
	});
	const sides = [
		[ref, worktree],
		["working tree", root],
		["working tree", root],
		[ref, worktree],
	] as const;

	git("worktree", "add", "--detach", worktree, ref);
	rmSync(join(worktree, "benchmarks"), { recursive: true, force: true });
	cpSync(benchmarksDir, join(worktree, "benchmarks"), {
		recursive: true,
		filter: (src) => !src.includes(join("benchmarks", "baselines")),
	});
	symlinkSync(join(root, "node_modules"), join(worktree, "node_modules"), "dir");

	for (const suite of suites) {
		const runs = [];
		for (const [i, [label, cwd]] of sides.entries()) {
			banner(`${suite}: ${label} (${i + 1} of 4)`);
			const dir = join(scratch, String(i));
			// One side failing makes the comparison moot; don't spend the other runs.
			if (!(await runSuite(suite, cwd, [`--ref-run-dir=${dir}`, `--label=${label}`]))) break;
			runs.push(readBaseline(join(dir, `${suite}.json`), suite));
		}
		banner(`${suite}: ${ref} against the working tree`);
		const [a1, b1, b2, a2] = runs;
		let ok = false;
		if (a1 && b1 && b2 && a2) {
			try {
				assertSameBenchmarks(runs);
				ok = compareBaselines(mergeBaselines([a1, a2]), mergeBaselines([b1, b2]), {
					reportMissing: values.filter === undefined,
				});
			} catch (error) {
				console.error(`  ! ${String(error)}`);
			}
		} else {
			console.error("  ! A run failed, so nothing was compared.");
		}
		if (!ok) failed++;
	}
	await cleanUp();
}

if (failed > 0) {
	console.error(`\n${failed} of ${suites.length} suites reported a problem.`);
	process.exitCode = 1;
}
