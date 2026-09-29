# Benchmarks

Runtime benchmarks run on [mitata](https://github.com/evanwashere/mitata), and a
type-checking benchmark runs on the TypeScript compiler.

```sh
npm run bench                          # every suite
npm run bench -- hydrate order-by      # suites whose name contains any of these
npm run bench -- hydrate --filter sort # only benchmarks matching the regex /sort/
npm run bench:save                     # record a baseline per suite (gitignored)
npm run bench:compare                  # diff each suite against its baseline
npm run bench -- --ref main            # diff the working tree against a git ref
npm run bench -- --verify-only         # run the correctness checks, time nothing (CI)
```

| Suite         | Measures                                                               |
| ------------- | ---------------------------------------------------------------------- |
| `order-by`    | `sortBy` and `sqlCompare`, on plain arrays                             |
| `plugins`     | `fixLongAliases()`: `transformQuery`, `transformResult`, the alias map |
| `query-build` | building and compiling query sets, with no database involved           |
| `hydrate`     | turning flat rows into nested objects, the hottest runtime path        |
| `execute`     | end to end against SQLite and Postgres                                 |
| `types`       | type-checking cost of representative query sets and hydrators          |

Postgres is optional. The `execute` suite skips its Postgres benchmarks if
nothing answers on `--postgres-url` (default: `$POSTGRES_URL`, then the port
`docker-compose.yml` publishes). `--no-postgres` skips them outright.

## How the suites are built

- **Each suite runs in its own process**, so it only pays for the fixtures it
  imports, and its call sites aren't made megamorphic by other suites.
- **Benchmarks are declared with `group()`** from `lib/harness.ts`: a mitata
  `summary()` of named workloads, the first the baseline the rest are read
  against. Each suite ends with `await runSuite()`.
- **Every workload is verified before anything is timed.** A workload is a
  `run` function plus either the `expected` result or a `check` that asserts
  on it; `runSuite` runs each once and asserts before timing starts. A change
  that makes a workload skip its work (returning nothing, dropping a join,
  skipping a sort) would otherwise read as a large speedup instead of a
  failure. CI runs `--verify-only`, which stops there. The few workloads whose
  first call changes shared state (the `plugins` scaling group) check their own
  first result instead, which mitata discards as warmup.
- **Every result goes through `do_not_optimize`**, so the JIT can't discard work
  whose result is thrown away.
- **Fixtures are built once, outside the measured call**, and are
  deterministic: shuffles are seeded, so every run sorts the same permutation.
  The one exception is `query-build`'s `querySet build`, whose job is building.
- **Benchmark names are `--filter` patterns**, so the harness rejects a name
  that doesn't match itself as a regex.

## Reading the numbers

**Read ratios within a `summary()` group.** Each group varies one thing and
holds the rest fixed, and every entry in it shares one process, heap and JIT
state. Absolute times are context. Across runs the machine drifts more than
most of the differences here.

**Across runs, compare medians, with a ±20% noise band.** Three runs of the same
code on one machine drifted by up to 17% in median time. `--compare` fails only
on a time regression beyond the band. `--ref` is the better check: it runs both
sides back to back in ABBA order (ref, working tree, working tree, ref) and
averages each side, so drift over the session cancels out. It copies the
current `benchmarks/` into a scratch checkout of the ref, sharing this
checkout's `node_modules`, so a benchmark for a feature the ref lacks fails
there.

**Allocation is shown, never gated.** mitata's mean heap per iteration is the
most stable allocation figure it reports (`heapMin` drifted 566x across runs,
where the mean drifted 1.19x), but it fails in two ways:

- A benchmark allocating a few hundred bytes sometimes picks up ~20 kb from a
  garbage collection landing inside a sample. Readings under 64 kb show as "too
  small".
- A benchmark allocating megabytes reports a mean that depends on what else ran
  in the process. One read 5 mb on its own and 11–14 mb inside the full suite.

Over six runs of 130 benchmarks, every false positive came from allocation and
none from time.

**No benchmark forces a collection every iteration.** mitata's `.gc("inner")`
was tried on the 10k-row hydrations: it doubled their median (every iteration
then starts on a heap `gc()` just shrank) and cut the sample count, without
narrowing p99 / p50.

## Type-checking

The `types` suite type-checks each fixture under `benchmarks/types/` with the
TypeScript compiler API, checking only the fixture itself, not the library's
source, much as a user of the published types would. The fixtures query their
own `types/schema.ts`, with the column counts, enums and `Generated` columns of
real tables, rather than the runtime suites' minimal `lib/db.ts` schema.
Verifying checks every fixture once; measuring checks each five times and
reports the median time, and the instantiations and types the fixture's own
check created, which leaves out loading the library. The count is
deterministic, so unlike time it's compared tightly: growth beyond 2% fails.
Check time and retained memory are reported for reading only. Every fixture
must type-check and assert its result with `expectTypeOf(...).toEqualTypeOf<...>()`,
so one whose types collapse to `any` fails instead of reading as a speedup.
