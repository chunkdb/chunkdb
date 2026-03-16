# chunkdb Release Preview

Release Preview for the current `0.1.1` line, focused on shipping the specialized chunk/grid storage engine with explicit durability boundaries and release-grade documentation of what is and is not proven.

## Major Capabilities

- specialized chunk/grid storage model with configurable large-chunk, regular-chunk, and block-bit geometry
- chunk-native TCP protocol with `AUTH`, `PING`, `INFO`, `GET`, `SET`, `CHUNK`, `CHUNKBIN`, and `QUIT`
- worker-pool server runtime with buffered parsing
- bit-packed regular chunk payloads with delta WAL and threshold-driven checkpointing
- durability modes:
  - `relaxed`
  - `fsync-wal`
  - `fsync-checkpoint`
- `CHUNKBIN` binary chunk read path for efficient chunk payload transfer

## Durability and Recovery Proof Included In This Preview

- checkpoint failpoint coverage for temp flush -> before rename
- checkpoint failpoint coverage for replace -> before directory sync
- WAL first-create failpoint coverage for file sync -> before parent directory sync
- orphan temp artifact cleanup on recovery
- torn/corrupted WAL tail safety with valid-prefix replay behavior
- durability contract documentation that separates guarantees from non-guarantees

Primary proof surface:

- `tests/durability_crash_hardening_tests.cpp`
- `docs/DURABILITY_CONTRACT.md`
- `docs/KNOWN_LIMITATIONS.md`
- `docs/RELEASE_CHECKLIST.md`

## Current Limitations

- only `fs_split_v1` is part of the production path
- no cross-chunk atomic transaction model
- no replication or distributed durability
- multi-process mode is single-writer / multi-reader rather than shared multi-writer
- durability remains mode-dependent and below full ACID database semantics
- filesystem/device behavior outside documented sync semantics is not fully proven by the current test matrix
- Windows Native TLS is not yet guaranteed as fully supported

## Release Preview Positioning

This preview is intended to show that the current storage engine and protocol are coherent, test-backed, and explicitly documented.
It is not a claim that the engine is feature-complete or that all post-preview recovery/fault-injection work is finished.

## Release Artifacts

The release-preview artifact set consists of:

- CPack `.tar.gz` archive
- CPack `.zip` archive
- SHA256 checksum file for each archive

Artifacts are produced from the current head with:

```bash
cmake -S . -B build-release -DCHUNKDB_BUILD_TESTS=OFF -DCHUNKDB_WITH_TLS=OFF
cmake --build build-release --parallel
cpack --config build-release/CPackConfig.cmake -B build-release/packages
scripts/release/generate_checksums.sh build-release/packages
```
