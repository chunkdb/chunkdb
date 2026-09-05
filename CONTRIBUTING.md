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
- keeps experimental layout targets OFF
- intended to stay fast (target: laptop-friendly pre-push gate)
- accepts `CHUNKDB_WERROR` (default `OFF`), `CHUNKDB_WITH_TLS`, `BUILD_DIR`

`full.sh`:
- configures/builds tests
- runs `smoke` + `stress`
- keeps experimental layout targets OFF
- supports stress repeat via `STRESS_REPEAT=<n>`
- accepts `CHUNKDB_WERROR` (default `ON`), `CHUNKDB_WITH_TLS`, `BUILD_DIR`

Experimental layout checks are opt-in and run separately:

```bash
cmake -S . -B build-exp \
  -DCHUNKDB_BUILD_TESTS=ON \
  -DCHUNKDB_BUILD_EXPERIMENTAL_LAYOUT=ON \
  -DCHUNKDB_WITH_TLS=OFF
cmake --build build-exp --parallel
ctest --test-dir build-exp -L experimental --output-on-failure
REPEATS=1 OPS_LIST='20000' SCENARIOS='sparse_world_writes' DURABILITIES='relaxed' \
  scripts/bench/layout_ab.sh
```

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

- `feat(storage): add experimental fs_region layout for A/B benchmarking`
- `docs(bench): publish layout A/B snapshot and no-go decision`

Do not use stage/phase tracking labels in commit subjects. Banned patterns include:

- `stage-*`
- `phase-*`
- `p0`, `p1` (or similar priority tags)
- `wip`
- `tmp`

Put stage/phase rollout context in PR descriptions, issues, or release notes, not in commit titles.
