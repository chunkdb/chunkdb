# Release Checklist

Use this as a blocker-only gate for Release Preview.

## 1) Test Gates

- [ ] quick gate passes (`smoke`, TLS off)
- [ ] quick gate passes (`smoke`, TLS on)
- [ ] full gate passes (`smoke` + `stress`)
- [ ] crash durability label passes (`crash`)
- [ ] Windows CI quick gate passes

Suggested local commands:

```bash
CHUNKDB_WITH_TLS=OFF scripts/test/quick.sh
CHUNKDB_WITH_TLS=ON scripts/test/quick.sh
scripts/test/full.sh
cmake -S . -B build-crash -DCHUNKDB_BUILD_TESTS=ON -DCHUNKDB_WITH_TLS=OFF
cmake --build build-crash --parallel
cmake --build build-crash --target chunkdb_durability_crash_hardening_test --parallel
ctest --test-dir build-crash -L crash --output-on-failure
```

## 2) Durability Evidence

- [ ] flush->replace boundary failpoint test passes
- [ ] replace->dir-sync boundary failpoint test passes
- [ ] orphan temp cleanup test passes
- [ ] torn WAL tail safety test passes
- [ ] old-or-new invariant test for replace path passes

Reference:
- `tests/durability_crash_hardening_tests.cpp`
- `docs/DURABILITY_CONTRACT.md`

## 3) Packaging

- [ ] `cpack` archive artifacts are generated
- [ ] SHA256 checksum files are generated for artifacts

Suggested commands:

```bash
cmake -S . -B build-release -DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_WITH_TLS=OFF
cmake --build build-release --parallel
cpack --config build-release/CPackConfig.cmake -B build-release/packages
scripts/release/generate_checksums.sh build-release/packages
```

## 4) Documentation Consistency

- [ ] `README.md` support matrix matches current platform claims
- [ ] `docs/KNOWN_LIMITATIONS.md` includes current caveats
- [ ] `docs/DURABILITY_CONTRACT.md` aligned with code/tests

## 5) Issue / Milestone Mapping

- [ ] Issue `#3` is marked release-blocker and attached to release-preview milestone
- [ ] Issue `#6` remains open as post-preview unless Windows Native TLS is explicitly claimed fully supported
- [ ] release notes and docs explicitly state current Windows Native TLS status

If GitHub write access is unavailable in the current environment, record intended state updates in a docs note and apply via web UI before publishing the release.
