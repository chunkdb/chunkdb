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

Plain reads and writes must not get slower as chunkdb grows. Three workloads of grid worlds measure them through the protocol (`chunkdb_server_bench --tests world,canvas,simulation`), and a change may not lower the median throughput of any of them by more than 5% in the `relaxed` profile.

| Scenario | Workload |
| --- | --- |
| `world` | Each client is a player walking one chunk at a time. Every 50 requests it moves and loads the chunks within 2 chunks of it (`GET AREA AROUND ... RADIUS 2`) and once saves its chunk whole (`SET CHUNK`); otherwise it writes (76%) and reads (20%) blocks within 2 chunks of it. |
| `canvas` | Clients write random blocks anywhere (95%); one request in 20 reads a 4x4-chunk viewport (`GET AREA ... TO ...`). |
| `simulation` | Each client sweeps the region from its own offset, reading a chunk whole and writing it back whole. |

Each scenario covers `--keyspace` blocks along each axis and fills that region with whole chunks before it is timed.

```bash
scripts/bench/compare_budgets.py BEFORE_BUILD_DIR AFTER_BUILD_DIR
```

The script runs the scenarios from both builds in spawn mode, alternating which build goes first, 15 times each, and compares medians:

| Profile | Durability | Requests | Clients / server workers | Region | Gates |
| --- | --- | --- | --- | --- | --- |
| `relaxed` | `relaxed` | 200 000 | 16 / 16 | 2048x2048 blocks | yes, 5% |
| `fsync-wal` | `fsync-wal` | 5 000 | 16 / 16 | 256x256 blocks | no, reported only |

One run proves nothing, and the medians themselves carry noise: passing the same build twice measures it. On the baseline machine identical builds differed by at most 2.1% in the `relaxed` profile and by up to 7.4% in the `fsync-wal` profile, where every write waits for the disk; that profile is therefore reported but does not gate.

Baseline (2026-10-07, Apple M1 Pro, macOS 27.0, APFS, Release; [summary](bench/artifacts/manual-runs/budgets-20261007-macos-summary.txt)), medians of 15 runs:

| Profile | `world` req/s (p99 ms) | `canvas` req/s (p99 ms) | `simulation` req/s (p99 ms) |
| --- | --- | --- | --- |
| `relaxed` | 74 119 (0.67) | 43 093 (3.81) | 67 519 (2.90) |
| `fsync-wal` | 677 (79.2) | 543 (79.9) | 982 (70.3) |


## Issue triage

See the [issue intake and triage policy](.github/ISSUE_POLICY.md).
