# Durability Contract

This document defines what `chunkdb` currently guarantees for checkpoint/WAL persistence and recovery.

## Scope

Applies to the stable `fs_split_v1` storage path and durability modes:

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

"Strict checkpoint durability" applies in both `fsync-wal` and
`fsync-checkpoint`: a checkpoint removes the chunk's WAL, so in every mode
whose acknowledgements promise durability the replacement image (and its
directory entry) is synced before the WAL is deleted. Removing a durable WAL
in favor of an unsynced image would silently downgrade the contract.

Empty-chunk garbage collection (see `STORAGE_FORMAT.md`) removes the data
image before the WAL, so a crash between the two steps replays the
empty-state WAL over an absent image and never resurrects deleted data.

Conditional mutations (`CHUNKCAS`, `CHUNKBATCH`) are all-or-nothing across
failure and crash. Each logs the full new chunk state as a single WAL frame,
so replay applies it completely or not at all for every geometry. Before
append, the store persists a rollback intent containing the prior WAL
boundary. Any pre-commit failure restores memory and truncates/removes the
WAL; if local repair fails, the store stops serving durability-changing
operations and startup repeats the repair from the intent before replay.
Clearing and syncing the intent is not the commit boundary: after the WAL
sync, the store first atomically replaces the rollback record with a synced
committed record.
Startup truncates only rollback records and preserves WAL for committed
records. The committed record can then be unlinked safely: whether that unlink
survives a crash, recovery keeps the mutation. Intent-cleanup and inline
checkpoint failures after commit cannot be returned as a failed command; the
WAL remains the committed recovery source until checkpoint retry.

Read-only replay follows the same conditional decision without performing
recovery writes. The durable `chunkdb.snapshot` generation is odd before any
image/region, WAL, intent, checkpoint, GC, or recovery transition and advances
to a new even value only after the on-disk state is coherent. For each chunk a
reader accepts its image/region image, WAL, and intent only when the same
validated even generation brackets the complete collection. A stable `CKRB`
replays at most the recorded prior-WAL boundary, including boundary zero;
bytes after that boundary are ignored. A stable `CKRC` replays the committed
WAL normally.

Rollback, restart, checkpoint, GC, and eviction never restore an earlier
generation. A crash while odd requires writer recovery under a fresh odd
generation before even can be published; exhaustion fails instead of wrapping.
The odd record is fully durable (file and directory) before any bracketed
artifact changes. The even record's file data is durable before its rename,
but its directory entry is not required to be synced: losing that rename to a
crash re-exposes the preceding durable odd record, which is strictly more
conservative — readers fail closed until writer recovery. A crash can
therefore surface an odd generation even for a transition that had completed;
recovery then republishes a fresh odd/even pair as usual.
Thus two rejected transactions may recreate byte-identical WAL and absent
intent observations, but cannot recreate the generation that bracketed the
first observation. After eight unstable attempts, or for
malformed/unreadable/inconsistent state, the chunk load fails closed.
Read-only replay does not alter any artifact.

WAL append path:

1. append WAL header (on first create) and record batch
2. flush userspace stream buffers
3. in synced modes, flush file durability
4. when WAL file is first created in synced modes, sync parent directory

Ordinary writes (`SET`/`UNSET`/`CHUNKSET`/`CHUNKSETBIN`, and each `MSET`
item) reserve their version token first, stage the mutation's WAL frame in
memory, and treat the successful WAL flush as the commit point:

- A failure before or during the flush returns an error with memory,
  counters, and the WAL file fully restored. A torn or unsynced append is
  truncated back to the pre-flush boundary inside the same odd generation,
  so neither a reader nor a later retry of the retained batch can observe
  records that were never acknowledged.
- If that repair itself fails, the store fails closed until restart; in that
  narrow double-failure case recovery may replay the rejected records, and
  the client that received the error must treat the outcome as unknown.
- A failure after the flush (inline checkpoint, generation republication) is
  logged and retried later; it is never returned as a command error.

An error reply for an ordinary or conditional mutation therefore means "not
applied", and a success reply means "applied under the mode's write
acknowledgement contract".

## WAL Frames

Every mutation is appended as exactly one WAL frame (`.wal` format v4; the
byte layout is in `STORAGE_FORMAT.md` §4.1): a 22-byte header carrying the
chunk revision, the record count, the body size and a CRC over those fields;
then the changed spans as records whose CRC covers
`byte_offset || data_size || body`; then a trailing CRC over the frame's whole
record body. A mutation's records are staged together and flushed together, so
a frame is never split across two flushes. Relaxed-mode group commit may put
several frames in one flush.

This makes recovery all-or-nothing per mutation at any chunk size:

- A crash inside a frame's append leaves a torn frame. Replay finds fewer than
  `body_size + 4` bytes after the frame header, stops there, and applies
  nothing from that frame, so a mutation is never recovered as a prefix of its
  records. The single-record atomicity bound of 1.x (65535 bytes), which made
  large geometries reject `CHUNKCAS`/`CHUNKBATCH` and made a multi-record
  `CHUNKSET`/`CHUNKSETBIN` non-atomic, no longer applies.
- A frame whose header CRC, frame CRC or any record CRC fails, or that carries
  a record outside the chunk state or straddling the payload/presence
  boundary, stops replay at that frame. Frames before it stay applied.
- Because the record CRC covers `byte_offset` and `data_size`, corruption of
  those fields can no longer apply a CRC-valid body at the wrong offset.
- The frame carries the chunk revision the mutation reserved. Replay adopts
  the last applied frame's revision, which is what keeps `CHUNKVER` stable
  across eviction and restart.

WAL files a 1.x writer left behind (`.wal` v2/v3) keep replaying under the 1.x
record rules, including their weaker body-only record CRC. A 2.x writer
appends a fresh v4 header before its first frame in such a file, and replay
switches to frames at that header.

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
| `relaxed` | WAL append without required sync | WAL replay applies the valid prefix of complete frames; a torn frame and any corrupted/truncated tail are ignored safely | No torn chunk image in namespace replace path; recovery preserves valid WAL prefix | No guarantee that recently acknowledged writes survive power loss |
| `fsync-wal` | WAL append + file sync; checkpoint images are synced before the WAL they replace is removed | WAL replay applies the valid prefix of complete frames; a torn frame and any corrupted/truncated tail are ignored safely | Higher confidence that acknowledged WAL records reach durable media, subject to OS/filesystem/device behavior | No cross-chunk atomicity |
| `fsync-checkpoint` | `fsync-wal` + strict checkpoint replace path | Old-or-new image visibility across crash points around replace; WAL replay still used for pending state | Strongest current mode for single-chunk durability path in this engine | Still not full ACID semantics; no distributed durability/replication |

## Explicit Durability Barrier (`WALFLUSH`)

`WALFLUSH` is a global barrier available in every durability mode:

- On success, every write acknowledged before the server received the
  command is durable on stable storage. In `relaxed` mode this includes
  flushing per-chunk in-memory WAL batches with a file sync and syncing all
  WAL files, checkpoint images, and directory entries written without a sync
  since the previous barrier.
- Writes acknowledged after the barrier started may or may not be covered;
  they are covered by the next barrier.
- Concurrent `WALFLUSH` calls are serialized so each caller's success covers
  its own start point.
- Once a successful barrier establishes durable state, later relaxed-mode
  checkpoints, empty-chunk GC, and WAL replacement sync their replacement
  before deleting the durable artifact. A later write therefore cannot
  downgrade an earlier barrier promise. Reopened initialized stores preserve
  this floor conservatively even when the earlier process never issued a
  barrier.
- Any sync failure aborts the barrier and is returned to the caller; the
  unsynced-artifact bookkeeping is retained so a retried barrier still covers
  them. Barrier bookkeeping is bounded: past 65536 tracked artifacts, the
  next barrier syncs the entire data directory instead.

## Background Maintenance

With `--background-maintenance`, checkpoint compaction and cache eviction run
on a dedicated thread. This does not change acknowledgement semantics: in
`fsync-wal`/`fsync-checkpoint`, acknowledgements still wait for the WAL sync
on the request thread, and checkpoints remain off the acknowledgement path.
Background checkpoint failures are logged, counted, and retried inline by the
next eligible write to the chunk so the error reaches a caller. The queue is
bounded; overflow falls back to inline checkpoints on the writer.

## Crash/Failpoint Evidence

Coverage in crash hardening tests:

- temp flush -> before replace boundary fault
- replace -> before directory sync boundary fault
- WAL first-create file-sync -> before directory sync boundary fault
- temp/orphan cleanup on load
- injected temp sync failure and close failure paths
- torn WAL tail ignored safely
- a WAL cut in the middle of a multi-record frame recovers the pre-mutation
  state and the pre-mutation revision; a flipped `byte_offset`, frame header
  field, or body byte is rejected by the covering CRCs
- repeated old-or-new invariant checks across replace-boundary faults
- conditional rollback/commit intent temp-write, publication, replacement,
  unlink, and directory-sync failures for both `CHUNKCAS` and `CHUNKBATCH`
- abrupt process exits immediately before and after rollback publication,
  commit publication, and committed-intent clearing, followed by ordinary
  writes, `WALFLUSH`, and another abrupt restart
- abrupt exits after odd snapshot-generation publication and before even
  publication; readers fail closed while odd and ordinary writer restart
  advances the generation and recovers
- an exact two-transaction ABA schedule for both conditional commands, both
  WAL boundary cases, and both storage layouts, coordinated after each WAL and
  intent observation

Reference:
- `tests/durability_crash_hardening_tests.cpp`
- `tests/world_ops_regression_tests.cpp` (WAL frame tearing and corruption)

## Non-Guarantees (Explicit)

- No multi-chunk atomic transactions
- No snapshot isolation/MVCC
- No replication quorum guarantees
- No claim of durability equivalence to full transactional DBMSs
- No guarantee for filesystems/devices that violate documented sync semantics
