# Release Checklist

Use this as a blocker-only gate before publishing a stable release.

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

- [ ] CLI uses module `github.com/chunkdb/chunk-cli/v2` and tag `v2.0.0` exists before the server 2.0 release is announced

- [ ] `README.md` support matrix matches current platform claims
- [ ] `docs/KNOWN_LIMITATIONS.md` includes current caveats
- [ ] `docs/DURABILITY_CONTRACT.md` aligned with code/tests

## 5) Public Issue State

- [ ] Release-blocking issues are closed or explicitly documented as known limitations
- [ ] `Build and Test TLS (windows-latest)` is green on the release commit
- [ ] release notes and docs state the Windows native TLS boundary (MSYS2 MinGW64 only)

If GitHub write access is unavailable in the current environment, record intended
state updates in a docs note and apply them through GitHub before publishing the
release.

## Binary release assets

`release-binaries.yml` runs only on a pushed `v*` tag and attaches archives and
SHA256 sidecars to that tag's GitHub release after all four builds succeed.
Each archive contains `chunkdb_server`, `chunkdb_verify`, `chunkdb_restore`, `chunkdb_admin`, the
license and server help; Windows files have `.exe` suffixes.
The targets are Linux x86-64/arm64 (Ubuntu 22.04 or later), macOS arm64 (macOS 14
or later) and Windows x86-64 (MinGW64).
OpenSSL is linked statically; Windows also links the compiler runtime statically.
The workflow checks architecture on Unix, OpenSSL discovery and runtime dependencies.
Before publishing, download every archive, check its SHA256 sidecar, extract it
on the corresponding platform and run its server, verifier and restore smoke checks.
Build success alone does not establish archive compatibility.

## Docker release image

- [ ] Source/runtime version and the intended `v*` tag agree before tagging; check the CMake project version before the 2.0 release.
- [ ] The Docker quick-start PR job passes the commands extracted from `docs/QUICK_START.md`.
- [ ] After all binary builds pass, the release workflow publishes `ghcr.io/chunkdb/chunkdb:<version>` (the tag without `v`) and `:latest` using `GITHUB_TOKEN` with job-scoped `packages: write`.
- [ ] Both tags contain `linux/amd64` and `linux/arm64`; check with `docker buildx imagetools inspect ghcr.io/chunkdb/chunkdb:2.0.0`.
- [ ] On the first publication, set the GHCR package visibility to public and verify an anonymous pull of the version tag.
- [ ] Pull the published version and run the quick-start checker with the matching CLI; check health, generated login, reads and offline password reset.

The tag workflow publishes images only for a pushed `v*` tag. Local image builds and PR checks do not publish to GHCR.
