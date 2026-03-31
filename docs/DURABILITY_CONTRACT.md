# Durability Contract

This document defines what `chunkdb` currently guarantees for checkpoint/WAL persistence and recovery.

## Scope

Applies to the current alpha storage path (`fs_split_v1`) and durability modes:

- `relaxed`
- `fsync-wal`
- `fsync-checkpoint`

## Write/Replace Sequence

Checkpoint image replacement path:

1. write temp file in the same directory as the target image
2. if strict checkpoint durability is enabled, flush temp file data
3. close temp file and fail loudly on close errors
4. atomically replace target entry with the temp file
5. if strict checkpoint durability is enabled, sync parent directory

WAL append path:

1. append WAL header (on first create) and record batch
2. flush userspace stream buffers
3. in synced modes, flush file durability
4. when WAL file is first created in synced modes, sync parent directory

## Platform Contract

### Linux / POSIX

- file durability uses `fdatasync` where valid, with `fsync` fallback
- directory durability uses `fsync` on directory fd
- strict checkpoint mode requires directory sync after atomic replace

### macOS

- strict file durability attempts `F_FULLFSYNC`
- if `F_FULLFSYNC` is unsupported by the runtime/filesystem, falls back to `fsync`
- strict checkpoint mode requires directory sync after atomic replace

### Windows

- file durability uses `FlushFileBuffers`/`_commit` for file handles
- critical replace path uses `SetFileInformationByHandle(FileRenameInfo)` semantics
- directory sync uses `FlushFileBuffers` on directory handle where supported
- in `fsync-wal` and `fsync-checkpoint`, if required directory-sync capability is unavailable,
  the write fails closed instead of continuing under the same strict durability claim

## Mode Guarantees

| Mode | Acknowledged Write Path | Recovery Behavior | Guaranteed | Not Guaranteed |
| --- | --- | --- | --- | --- |
| `relaxed` | WAL append without required sync | WAL replay applies valid prefix; corrupted/truncated tail is ignored safely | No torn chunk image in namespace replace path; recovery preserves valid WAL prefix | No guarantee that recently acknowledged writes survive power loss |
| `fsync-wal` | WAL append + file sync | WAL replay applies valid prefix; corrupted/truncated tail is ignored safely | Higher confidence that acknowledged WAL records reach durable media, subject to OS/filesystem/device behavior | No cross-chunk atomicity; checkpoint image durability still below strict checkpoint mode |
| `fsync-checkpoint` | `fsync-wal` + strict checkpoint replace path | Old-or-new image visibility across crash points around replace; WAL replay still used for pending state | Strongest current mode for single-chunk durability path in this engine | Still not full ACID semantics; no distributed durability/replication |

## Crash/Failpoint Evidence

Coverage in crash hardening tests:

- temp flush -> before replace boundary fault
- replace -> before directory sync boundary fault
- WAL first-create file-sync -> before directory sync boundary fault
- temp/orphan cleanup on load
- injected temp sync failure and close failure paths
- torn WAL tail ignored safely
- repeated old-or-new invariant checks across replace-boundary faults

Reference:
- `tests/durability_crash_hardening_tests.cpp`

## Non-Guarantees (Explicit)

- No multi-chunk atomic transactions
- No snapshot isolation/MVCC
- No replication quorum guarantees
- No claim of durability equivalence to full transactional DBMSs
- No guarantee for filesystems/devices that violate documented sync semantics
