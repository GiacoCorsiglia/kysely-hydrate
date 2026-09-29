/**
 * Runs the benchmark suites, each in its own process, so each pays only for the
 * fixtures it imports.  See the README for usage.
 *
 * `--ref <git-ref>` checks the ref out into a scratch worktree, copies this
 * checkout's `benchmarks/` over it so both sides run identical benchmark code,
 * then runs each suite in ABBA order (ref, working tree, working tree, ref)
 * and compares the averages, so drift over the session cancels out.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, readdirSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { compareBaselines, mergeBaselines, readBaseline } from "./lib/baseline.ts";
import { benchmarksDir, parseCli } from "./lib/harness.ts";

const args = process.argv.slice(2);
const { values, positionals, tokens } = parseCli(args, true);

/** Cheap, focused suites first; any suite not listed runs after these, alphabetically. */
const order = ["order-by", "plugins", "query-build", "hydrate", "execute", "types"];
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

const ownedByRef = ["save", "compare", "baselines-dir", "label", "verify-only"] as const;
if (values.ref !== undefined && ownedByRef.some((flag) => values[flag] !== undefined)) {
	console.error(`--ref records and compares its own runs; drop --${ownedByRef.join(", --")}`);
	process.exit(1);
}

/** The flags to hand each suite: everything but the selectors and `--ref`. */
const passthrough = tokens.flatMap((t) => {
	if (t.kind !== "option" || t.name === "ref") return [];
	return t.value === undefined || t.inlineValue ? [args[t.index]!] : [args[t.index]!, t.value];
});

/** Runs one suite to completion; true when it exited cleanly. */
function runSuite(suite: string, cwd: string, extra: string[] = []): boolean {
	// --expose-gc lets mitata collect between samples and report heap usage.
	const { status } = spawnSync(
		process.execPath,
		["--expose-gc", join(cwd, "benchmarks", `${suite}.bench.ts`), ...passthrough, ...extra],
		{ stdio: "inherit", cwd },
	);
	return status === 0;
}

const banner = (title: string) => console.log(`\n${"=".repeat(70)}\n${title}\n${"=".repeat(70)}`);
const root = join(benchmarksDir, "..");
let failed = 0;

if (values.ref === undefined) {
	for (const suite of suites) {
		banner(suite);
		if (!runSuite(suite, root)) failed++;
	}
} else {
	const ref = values.ref;
	const scratch = mkdtempSync(join(tmpdir(), "kysely-hydrate-bench-"));
	const worktree = join(scratch, "ref");
	const git = (...a: string[]) => execFileSync("git", a, { cwd: root, stdio: "inherit" });
	const sides = [
		[ref, worktree],
		["working tree", root],
		["working tree", root],
		[ref, worktree],
	] as const;

	try {
		git("worktree", "add", "--detach", worktree, ref);
		try {
			rmSync(join(worktree, "benchmarks"), { recursive: true, force: true });
			cpSync(benchmarksDir, join(worktree, "benchmarks"), {
				recursive: true,
				filter: (src) => !src.includes(join("benchmarks", "baselines")),
			});
			symlinkSync(join(root, "node_modules"), join(worktree, "node_modules"), "dir");

			for (const suite of suites) {
				const [a1, b1, b2, a2] = sides.map(([label, cwd], i) => {
					banner(`${suite}: ${label} (${i + 1} of 4)`);
					const dir = join(scratch, String(i));
					return runSuite(suite, cwd, ["--save", "--baselines-dir", dir, "--label", label])
						? readBaseline(join(dir, `${suite}.json`), suite)
						: undefined;
				});
				banner(`${suite}: ${ref} against the working tree`);
				const ok =
					a1 && b1 && b2 && a2
						? compareBaselines(mergeBaselines([a1, a2]), mergeBaselines([b1, b2]), {
								reportMissing: values.filter === undefined,
							})
						: false;
				if (!ok) failed++;
			}
		} finally {
			git("worktree", "remove", "--force", worktree);
		}
	} finally {
		rmSync(scratch, { recursive: true, force: true });
	}
}

if (failed > 0) {
	console.error(`\n${failed} of ${suites.length} suites reported a problem.`);
	process.exitCode = 1;
}
