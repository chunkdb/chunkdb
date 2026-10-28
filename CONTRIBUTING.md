# Contributing

This file is developer/CI focused.
For end-user installation and running instructions, use:

- [README.md](README.md)
- [docs/WINDOWS_NATIVE.md](docs/WINDOWS_NATIVE.md)

## Local Test Policy

Before opening a PR:

1. run the local quick gate first;
2. then run the full gate (or rely on CI full/stress follow-up if explicitly agreed for large runs).

Commands:

```bash
CHUNKDB_WERROR=ON scripts/test/quick.sh
scripts/test/full.sh
```

For cached Docker checks, run `scripts/test/docker-check.sh gcc` (or `gcc-tls`,
`tsan`, `asan`); append CTest arguments, for example `tsan -R txn_commit -j6`.
Each checkout and configuration keeps its build in a named volume and mounts source read-only.
The script uses `chunkdb:stand`, optional ccache already in that image, and
`PARALLEL_JOBS` (default 6). Each cache admits one run at a time; explicit CTest
arguments replace the default CI smoke selection.

Run the quick gate with `CHUNKDB_WERROR=ON`. `quick.sh` defaults it to `OFF`,
but every `Build and Test` CI job (Linux, macOS, Windows, and both TLS jobs)
runs the quick gate with `CHUNKDB_WERROR=ON`, so a local run without it does
not reproduce the mandatory gate and lets warning-only failures reach CI.
Compiler diagnostics differ per toolchain, so a clean macOS/clang run can still
fail the Linux/GCC or MinGW job.

`full.sh` mirrors CI and defaults `CHUNKDB_WERROR` to `ON`; pass
`CHUNKDB_WERROR=OFF` explicitly if a local toolchain produces diagnostics you
do not want to gate on.

`quick.sh`:
- configures/builds tests
- runs CTest label `smoke`
- intended to stay fast (target: laptop-friendly pre-push gate)
- accepts `CHUNKDB_WERROR` (default `OFF`), `CHUNKDB_WITH_TLS`, `BUILD_DIR`

`full.sh`:
- configures/builds tests
- runs `smoke` + `stress`
- supports stress repeat via `STRESS_REPEAT=<n>`
- accepts `CHUNKDB_WERROR` (default `ON`), `CHUNKDB_WITH_TLS`, `BUILD_DIR`

## CI Policy

- PR CI path uses the quick gate (`smoke`) as the mandatory signal.
- All `Build and Test` jobs run the quick gate with `CHUNKDB_WERROR=ON`; a new
  compiler warning fails CI.
- Stress is tracked separately in the stress-flake workflow.
- TLS build/smoke validation remains a dedicated CI job.

## Scope Discipline

- Keep the stable line focused on correctness, compatibility, validation, and transparent measurement.
- Avoid scope creep into unrelated major features in hardening iterations.

## Commit Message Policy

Use commit subjects in this format:

- `<type>(<scope>): <what changed>`

Examples:

- `perf(scan): keep a lazy SCAN CHUNKS catalog instead of listing the data dir per page`
- `docs(bench): publish layout A/B snapshot and no-go decision`

Do not use stage/phase tracking labels in commit subjects. Banned patterns include:

- `stage-*`
- `phase-*`
- `p0`, `p1` (or similar priority tags)
- `wip`
- `tmp`

Put stage/phase rollout context in PR descriptions, issues, or release notes, not in commit titles.

## Hot-Path Budgets

Plain reads and writes must not get slower as chunkdb grows. Three workloads of grid worlds measure them through the protocol (`chunkdb_server_bench --tests world,canvas,simulation`), and the `relaxed` gate flags a regression only when the upper95% paired-bootstrap median after/before throughput ratio is below0.95. An interval crossing0.95 is inconclusive, not evidence of equal speed.

| Scenario | Workload |
| --- | --- |
| `world` | Each client is a player walking one chunk at a time. Every 50 requests it moves and loads the chunks within 2 chunks of it (`GET AREA AROUND ... RADIUS 2`) and once saves its chunk whole (`SET CHUNK`); otherwise it writes (76%) and reads (20%) blocks within 2 chunks of it. |
| `canvas` | Clients write random blocks anywhere (95%); one request in 20 reads a 4x4-chunk viewport (`GET AREA ... TO ...`). |
| `simulation` | Each client sweeps the region from its own offset, reading a chunk whole and writing it back whole. |

Each scenario covers `--keyspace` blocks along each axis and fills that region with whole chunks before it is timed.

`chunkdb_budget_compare` is built next to `chunkdb_server_bench` (`cmake --build build --target chunkdb_server_bench chunkdb_budget_compare`):

```bash
build/chunkdb_budget_compare BEFORE_BUILD_DIR BEFORE_BUILD_DIR \
  --noise-floor --window-id WINDOW --markdown noise.md --json noise.json
build/chunkdb_budget_compare BEFORE_BUILD_DIR AFTER_BUILD_DIR \
  --window-id WINDOW --noise-reference noise.json \
  --markdown budget.md --json budget.json
```

Start every measurement window with the same binary on both control sides.
Comparisons require its completed raw JSON, the same window ID, profile/settings,
seeds and run count. Missing or mismatched controls fail before measurement;
the control may use a different build from an individual comparison baseline.
Its actual build source commits and hashes remain printed separately. A noisy
control is retained, including threshold failures; noise is never subtracted.
The tool does not schedule windows or run redundant calibration per comparison.

The native benchmark `--io-timeout-ms` limits each socket send/receive wait
and configures the spawned server; the comparator fixes it at5000ms. It is
not an aggregate deadline for a complete reply or DNS/connect.

Build both benchmarks with the reporting changes before comparing them. The
tool records each binary's build-stamped source commit, clean/modified state,
Release/Debug configuration and benchmark-source fingerprint. The JSON includes
raw samples, exact commands and binary hashes; save it with the Markdown.
Each benchmark process's stdout, stderr, exit status and UTC start/end are
saved before parsing, in `FILE.runs/` next to `--json FILE` (otherwise in a new
directory under the temporary directory, printed at start).

A hot-path PR pastes the Markdown output, including its machine/configuration
header. It shows actual request mixes and blocks/request, req/s and blocks/s,
p50 and p99 as medians [min–max] across runs, with the req/s change last. Blocks
mean positions handled by successful requests, not changed values or bytes:
one for a point request, the actual chunk geometry for a chunk request, and
only the returned chunks for an area request. Short per-client cycles and
clipped areas therefore use their actual counts. Latency summaries compare
each run's percentiles; they are not percentiles of pooled samples. Load ranges
are observations immediately before/after each process, not continuous maxima.

Format excerpt (synthetic values illustrating columns, not a measurement):

| Scenario | Before req/s | After req/s | Before blocks/s | After blocks/s | Before p50 ms | After p50 ms | Before p99 ms | After p99 ms | Change req/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `simulation`, GET/SET CHUNK 50%/50%, 256 blocks/request | 1,000 [990–1,010] | 1,010 [1,000–1,020] | 256,000 [253,440–258,560] | 258,560 [256,000–261,120] | 1.0 [0.9–1.1] | 1.0 [0.9–1.1] | 2.0 [1.9–2.1] | 2.0 [1.9–2.1] | +1.00% |

The tool runs the scenarios from both builds in spawn mode, alternating which build goes first, 15 times each. Paired statistics match run identity and seed, independently of execution order. They resample whole after/before req/s ratios100000 times, with the Mersenne Twister draws of Python's random.Random, seed20261011, reset per row, and report median, sample SD and95% percentile intervals using Type7 linear quantiles. Pairs are treated as exchangeable; cross-pair drift is not modeled. The ordinary ratio-of-medians change is retained as a diagnostic, with absolute rate/latency distributions. `--runs 1` remains a smoke check with unavailable CI/SD and no PASS or regression claim; inference needs at least two pairs and small samples remain limited. Exit0 means no gated criterion met, not proof of equivalence.

The relaxed default uses4 clients and4 server workers. `--relaxed-concurrency 8` or `16` changes both clients and server workers; `4` explicitly selects the default. A reference must use the same setting. The fsync-wal configuration and reported-only exit policy remain unchanged:


| Profile | Durability | Requests | Clients / server workers | Region | Gates |
| --- | --- | --- | --- | --- | --- |
| `relaxed` | `relaxed` | 200 000 | 4 / 4 | 2048x2048 blocks | yes, 5% |
| `fsync-wal` | `fsync-wal` | 5 000 | 16 / 16 | 256x256 blocks | no, reported only |

Calibrate the same binary at the start of each window and retain every raw sample, absolute metric and paired interval. An inconclusive interval is not evidence of equivalent speed. Verify that durable measurements use the intended persistent storage rather than a temporary-memory filesystem.

Baseline (2026-10-07, Apple M1 Pro, macOS 27.0, APFS, Release; [summary](bench/artifacts/manual-runs/budgets-20261007-macos-summary.txt)), medians of 15 runs:

| Profile | `world` req/s (p99 ms) | `canvas` req/s (p99 ms) | `simulation` req/s (p99 ms) |
| --- | --- | --- | --- |
| `relaxed` | 74 119 (0.67) | 43 093 (3.81) | 67 519 (2.90) |
| `fsync-wal` | 677 (79.2) | 543 (79.9) | 982 (70.3) |

### Feed subscribers

Writes must not get slower with the number of watches on their table.
The subscriber budget compares one build with itself and attaches watches only to the after side, so the gate measures what the watches cost.
`--watch TABLE[:N][:slots]` keeps N watches of the table open for the whole run; with `slots`, each watch owns a durable slot and acknowledges what it reads.

```bash
for watch in default default:10 default:100 default:1:slots default:100:slots; do
  scripts/bench/compare_budgets.py BUILD_DIR BUILD_DIR \
    --window-id WINDOW --noise-reference noise.json --watch "$watch" \
    --markdown "watch-${watch//:/-}.md" --json "watch-${watch//:/-}.json"
done
```

The watches run in the benchmark process, so their reading of every push shares the machine with the clients.
Watches beyond 64 raise the spawned server's watch limit; an external server needs `--max-watches`.


## Issue triage

See the [issue intake and triage policy](.github/ISSUE_POLICY.md).

## Parser and decoder fuzzing

`CHUNKDB_FUZZ=ON` builds libFuzzer harnesses with Clang, AddressSanitizer and UndefinedBehaviorSanitizer.
The default build has no fuzzing instrumentation.
Use a compiler with those runtimes (Linux Clang if the macOS toolchain lacks libFuzzer), and keep fuzzing apart from benchmark runs.

| Target | Reaches |
| --- | --- |
| `request` | Request line buffer, parameter frames with their total byte bound, SCRAM messages. |
| `cql` | CQL lexer and parser. |
| `wal` | WAL header and frame replay. |
| `image` | Chunk image parser. |
| `chunk_form` | Chunk form decoder (`SET CHUNK`). |
| `manifest` | Store (table) manifest and data directory manifest. |
| `slots` | Change feed slot records. |
| `migrations` | Migration ledger and journal. |
| `backup` | Backup marker record. |

```bash
scripts/ci/fuzz.sh 60 [TARGET...]
```

The script builds `build/fuzz` and runs each target for the given seconds.
It starts from seeds written by `chunkdb_fuzz_seeds` with the production serializers, the corpus kept in `build/fuzz/fuzz/corpus`, and `fuzz/regression`.
Metadata harnesses test both the raw input and a copy with its outer CRC repaired.
Storage harnesses also repair WAL header and frame checksums or the image directory checksum.
The request harness runs the server's own `ReadParameterFrames`.

CI runs every target for 30 seconds on each pull request (`fuzz.yml`) and for 25 minutes each night (`fuzz-scheduled.yml`).
A failing run uploads the crash inputs as an artifact.

To keep a crash as a regression, fix the bug and copy the input to `fuzz/regression/NAME/`.
Every normal test build replays those files and the generated seeds through the same entry points without libFuzzer (`ctest -R fuzz_replay`).
Reproduce a finding with `build/fuzz/fuzz/bin/chunkdb_fuzz_NAME PATH_TO_INPUT`.

## Checked documentation examples

Build the current server, the checker and chunk-cli, then run from the repository root:

```sh
ctest --test-dir build -R '^docs_check$'
build/chunkdb_docs_check examples --server build/chunkdb_server --cli /path/to/chunk-cli
build/chunkdb_docs_check walkthrough --server build/chunkdb_server --cli /path/to/chunk-cli
build/chunkdb_docs_check links
```

Mark CLI shell input with `cql docs-check`, followed immediately by an `output` fence.
The checker runs each page against a fresh authenticated data directory.
Examples on the same page share its tables and named shell connections.
Use `session=first` and `session=second` on input fences for two-connection scenarios.

~~~markdown
```cql docs-check session=main
PING
```

```output
PONG
```
~~~

Use `sh docs-check-shell` for shell commands and `sh docs-check-watch` for a background CLI watch.
A checked watch must be followed by one checked SET BLOCK, which provides its complete one-block event.
Use `sh docs-check-replay` when a slot already retains one change; the checker consumes it and stops the watcher.
The walkthrough check then reuses one server for Quick start, README, blocks, migrations, and feed examples.
Shell examples receive CHUNKDB_URI, CHUNKDB_PASSWORD, and a writable EXAMPLE_DIR.
The checker strips shell prompts while retaining all reply and error text.

Only named volatile fields are normalized: generated passwords, server versions, epochs, and event or migration timestamps.
Temporary example paths become `<example-dir>`; numeric data, schema versions, chunk versions, and revisions remain exact.
Changes to those values fail the comparison.
To rerecord after inspecting intended behavior, pass `--record` and review every changed output fence.

CI checks the current source build on Linux and macOS.
The Docker quick-start check also runs startup, health, authentication, watching, and the separate password-reset page.
