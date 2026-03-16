# Issue Triage Notes Archive

GitHub API write access was not available in this environment (`401 Requires authentication`), so this file contains ready-to-post comments and target state changes.

## Issue #1: Document durability guarantee matrix per mode

Target state: **close as completed**

Ready-to-post comment:

> Closing as completed in alpha docs.
>
> Durability matrix is documented in:
> - `README.md` -> "Durability Guarantees Matrix"
> - `docs/ALPHA.md` (scope/limits context)
>
> This issue’s core acceptance (explicit per-mode guarantees and non-guarantees) is now in public docs.

## Issue #2: Add CI build/test matrix for Linux and macOS

Target state: **close as completed**

Ready-to-post comment:

> Closing as completed.
>
> CI matrix and related coverage are in:
> - `.github/workflows/ci.yml`
>   - Ubuntu + macOS quick gate (`smoke`)
>   - Windows quick gate (`smoke`)
>   - Ubuntu TLS build + test + startup smoke
> - `.github/workflows/stress-flake-gate.yml`
>   - separate stress flake workflow (`--repeat until-fail:20`)

## Issue #4: Publish reproducible benchmark run artifacts

Target state: **close as completed**

Ready-to-post comment:

> Closing as completed.
>
> Reproducible benchmark artifact flow is available:
> - script: `scripts/bench/run_reproducible_benchmarks.sh`
> - artifact format: `bench/artifacts/README.md`
> - CI workflow: `.github/workflows/benchmark-artifacts.yml` (manual + release-triggered upload)
> - docs: `docs/PERFORMANCE.md`

## Issue #3: Expand crash and fault-injection recovery coverage

Target state: **keep open; update scope**

Ready-to-post scope update:

> Keeping open. Recent updates improved kill-recovery and WAL/checkpoint cycle validation, but this issue remains for deeper fault-injection coverage.
>
> Remaining scope:
> - additional deterministic kill points around WAL append/checkpoint boundaries
> - broader interrupted-I/O scenarios
> - explicit mapping of covered vs uncovered crash modes per durability mode

## Issue #5: Add runtime observability counters to INFO

Target state: **keep open; update scope**

Ready-to-post scope update:

> Keeping open with narrowed scope.
>
> Delivered in the latest update:
> - `INFO` now includes runtime counters:
>   - `loaded_chunks`, `evictions`, `checkpoints`, `wal_batch_flushes`, `unique_loaded_chunks`
> - implementation: `src/engine.cpp`
> - protocol/docs updates:
>   - `docs/PROTOCOL.md`
>   - `docs/RUNTIME_FLOW.md`
>
> Remaining scope for this issue:
> - decide/add any extra counters beyond current minimal set
> - long-run semantics/retention policy for counters
> - optional structured export format in future milestones

## Issue #6: Validate and harden native Windows TLS path in CI

Target state: **keep open; post-preview tracking**

Ready-to-post scope update:

> Keeping open as post-preview scope.
>
> Current public support boundary:
> - Windows native core path is supported
> - Windows native TLS is not yet guaranteed as fully supported
>
> This issue should stay open until CI coverage and crash/interop validation for Windows native TLS reaches release-quality confidence.
