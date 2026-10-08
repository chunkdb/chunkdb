# Durability Contract

This document defines what `chunkdb` currently guarantees for checkpoint/WAL persistence and recovery.

## Scope

Applies to the stable `fs_split_v1` storage path and durability modes:

- `relaxed`
- `fsync-wal`
- `fsync-checkpoint`

## Data Directory, Tables and Manifests

Durability modes are table options: each table of a data directory has its own, recorded in its manifest and changed with `ALTER TABLE ... SET`. A change applies to writes acknowledged after its reply. `ALTER TABLE` first writes the table's batched acknowledged writes to their WALs and fails, changing nothing, if it cannot; what the old store wrote without a sync is still synced by the next `FLUSH WAL`. `DROP TABLE` writes them too in case the drop fails and reopens the table, but goes ahead if it cannot.

A new data directory writes `chunkdb.manifest` before any other artifact, and a new table writes `table.manifest` before any other artifact of the table, in every durability mode: the bytes are synced under a temporary name, published only if no manifest exists (never replacing one), and the directory entry is synced. A crash leaves either no manifest, and the next start initializes again, or the complete one. A table manifest is replaced only by `ALTER TABLE`, and the data-directory manifest only by `DROP TABLE` (to raise its version floor), atomically and synced. See `STORAGE_FORMAT.md` Sections 1.1 and 1.2.

`CREATE TABLE` and `DROP TABLE` are atomic across a crash: a table exists completely or not at all (`STORAGE_FORMAT.md` Section 1.4). The reply to either comes after its directory changes are synced.

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

Empty-chunk garbage collection (see `STORAGE_FORMAT.md`) flushes the chunk's pending frames into the WAL, with a last frame that sets the whole chunk state to empty when an image exists, and removes the data
image before the WAL, so a crash between the two steps replays the
empty-state WAL over an absent image and never resurrects deleted data. A WAL that outlives a regular checkpoint is replayed only past the image's revision, so it cannot roll the image back. A store that is fail-closed after an unrecoverable rollback runs no checkpoints: the WAL its rollback intent needs stays until the next start repairs it.

Conditional chunk writes (`SET CHUNK ... IF VERSION`) are all-or-nothing across failure and crash. Each logs the full new chunk state as a single WAL frame, so replay applies it completely or not at all for every geometry. Before append, the store persists a rollback intent containing the prior WAL boundary. Any pre-commit failure restores memory and truncates/removes the WAL; if local repair fails, the store stops serving durability-changing operations and startup repeats the repair from the intent before replay. Clearing and syncing the intent is not the commit boundary: after the WAL sync, the store first atomically replaces the rollback record with a synced committed record. Startup truncates only rollback records and preserves WAL for committed records. The committed record can then be unlinked safely: whether that unlink survives a crash, recovery keeps the mutation. Intent-cleanup and inline checkpoint failures after commit cannot be returned as a failed command; the WAL remains the committed recovery source until checkpoint retry.

Read-only replay follows the same conditional decision without performing
recovery writes. The durable `chunkdb.snapshot` generation is odd before any
image, WAL, intent, checkpoint, GC, or recovery transition and advances to a
new even value only after the on-disk state is coherent. For each chunk a
reader accepts its image, WAL, and intent only when the same validated even
generation brackets the complete collection. A stable `CKRB`
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
first observation. For malformed, unreadable, or persistently inconsistent
state the chunk load fails closed after a bounded retry budget (see
"Read-only retry budget" below). Read-only replay does not alter any artifact.

### Bracket coalescing (linger)

One odd epoch may bracket several consecutive transitions. Concurrent writers
have always shared an epoch: the second and later writers join the epoch the
first one opened, and only the last one out publishes even. A single writer now
gets the same coalescing across time — when the last writer leaves, the even
publication is *deferred* rather than written immediately, so a transition that
starts shortly afterwards re-enters the epoch that is still open and costs no
snapshot I/O at all. This is what makes a cache-eviction pass cost roughly one
bracket instead of one bracket per evicted chunk.

The guarantees are unchanged, and the direction of the change is conservative:

- The odd record is still fully durable before any bracketed artifact changes,
  and the epoch's even record is still published only once every artifact it
  brackets is coherent. Lingering only *delays* the even publication; it never
  advances it.
- While an epoch lingers the generation is odd, so read-only loads fail closed
  exactly as they do mid-transition. Longer odd epochs mean readers latch state
  later, never earlier.
- A crash while lingering costs nothing beyond ordinary recovery. Startup
  raises the generation to a fresh odd value unconditionally, replays, and
  publishes even; the bracketed artifacts are recovered from the WAL the same
  way as after a crash inside a real transition.
- A failure publishing a deferred even record leaves the generation odd. That
  is the same fail-closed state an inline even-publication failure produces:
  readers fail closed and the next writer transition refuses until restart.
- An epoch that fails (a transition abandoned mid-bracket) is not lingered; it
  stays odd until writer restart. Because an epoch can now cover several
  transitions, a failure poisons the whole epoch rather than one transition —
  strictly more conservative, and the same rule concurrent writers already had.

The epoch is bounded so it cannot starve readers: it is closed after a linger window (50 ms) or after a fixed number of transitions (512), whichever comes first, by whichever of the writer or the store's closer thread gets there first. `FLUSH WAL` and a clean store close publish the deferred even record before returning, so a barrier and a closed store both leave a stable generation behind.

### Read-only retry budget

A read-only chunk load retries its bracketed collection with eight sleep-free
attempts followed by exponential backoff, up to a total sleep budget of 250 ms.
The budget comfortably exceeds the writer's linger window, so a deliberately
coalesced epoch delays a reader rather than failing it. When the budget is
exhausted — an active writer inside a long transition, or a crashed writer that
left the generation odd — the chunk load still fails closed.

WAL append path:

1. append WAL header (on first create) and record batch
2. flush userspace stream buffers
3. in synced modes, flush file durability
4. when WAL file is first created in synced modes, sync parent directory

Ordinary writes (`SET BLOCK` and `DELETE BLOCK`, with or without `IF VERSION`, and `SET CHUNK` without `IF VERSION`) reserve their version token first, stage the mutation's WAL frame in memory, and treat the successful WAL flush as the commit point:

- A failure before or during the flush returns an error with memory,
  counters, and the WAL file fully restored. A torn or unsynced append is
  truncated back to the pre-flush boundary inside the same odd generation,
  so neither a reader nor a later retry of the retained batch can observe
  records that were never acknowledged.
- If that repair itself fails, the store fails closed until restart; in that
  narrow double-failure case recovery may replay the rejected records, and
  the client that received the error must treat the outcome as unknown.
  Until then the chunk stays cached and reads serve its state before the
  failed write (the same holds after a conditional write whose rollback
  fails); eviction skips it, so the cache can exceed its bound by such chunks.
- A failure after the flush (inline checkpoint, generation republication) is
  logged and retried later; it is never returned as a command error.

An error reply for an ordinary or conditional mutation therefore means "not
applied", and a success reply means "applied under the mode's write
acknowledgement contract". The exceptions are the fail-closed cases above and a conditional write whose commit record cannot be made durable; their error reply is `-ERR INTERNAL write outcome unknown: ...`, and the write may or may not be applied.

## WAL Frames

Every mutation is appended as exactly one WAL frame (the byte layout is in
`STORAGE_FORMAT.md` §4.1): a header carrying the chunk revision, the commit
time, the record count, the body size and optional fields such as a tag, with
a CRC over all of them; then the changed spans as typed records; then a
trailing CRC over every record byte. A mutation's records are staged together and flushed together, so
a frame is never split across two flushes. Relaxed-mode group commit may put
several frames in one flush.

This makes recovery all-or-nothing per mutation at any chunk size:

- A crash inside a frame's append leaves a torn frame. Replay finds fewer than
  `body_size + 4` bytes after the frame header, stops there, and applies
  nothing from that frame, so a mutation is never recovered as a prefix of its
  records, at any chunk size.
- A frame whose header CRC or frame CRC fails, or that carries an unknown
  field or record type, a record outside the chunk state or one straddling
  the payload/presence boundary, stops replay at that frame. Frames before it
  stay applied. The frame CRC covers every record byte, so a corrupted offset
  or size cannot apply a body at the wrong place.
- A stop a crash can leave (no frame header with a valid CRC starts after it)
  is truncated away before a read-write store appends, so frames acknowledged
  later are never written after bytes replay does not get past; a WAL cut
  while it was being created is replaced. A stop followed by a CRC-valid
  frame header (acknowledged frames may follow) and a damaged WAL header fail
  the chunk load and leave the file as it is. A complete last frame that
  fails its checks is indistinguishable from a torn one and is dropped.
- The frame carries the chunk revision the mutation reserved. Replay adopts the last applied frame's revision, which is what keeps the chunk version stable across eviction and restart.

## Platform Contract

### Linux / POSIX

- file durability uses `fdatasync` where valid, with `fsync` fallback
- directory durability uses `fsync` on directory fd
- strict checkpoint mode requires directory sync after atomic replace

### macOS

- every durability sync uses `F_FULLFSYNC`, which also flushes the drive's cache: WAL acknowledgements in `fsync-wal` and `fsync-checkpoint`, `FLUSH WAL`, checkpoint images, conditional-write boundaries and directory entries. Plain `fsync` on macOS returns before the drive has stored the data, so it would not keep the acknowledgement promise across a power loss
- each synced write therefore waits for the drive, which makes `fsync-wal` much slower on macOS than on Linux; `relaxed` with `FLUSH WAL` pays it once per barrier
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

## Explicit Durability Barrier (`FLUSH WAL`)

`FLUSH WAL` is a global barrier available in every durability mode. It covers every table of the data directory, since a connection may have written to several:

- On success, every write acknowledged before the server received the statement is durable on stable storage, in every table. In `relaxed` mode this includes flushing per-chunk in-memory WAL batches with a file sync and syncing all WAL files, checkpoint images, and directory entries written without a sync since the previous barrier.
- Writes acknowledged after the barrier started may or may not be covered;
  they are covered by the next barrier.
- Across restarts: a clean shutdown syncs what each table wrote without a sync, as a barrier does (a failure is logged), so a `FLUSH WAL` after the restart covers what the previous process acknowledged too. After a crash, a write the crashed process acknowledged is durable only if a `FLUSH WAL` covered it before the crash (or the mode synced it); a `FLUSH WAL` in the new process does not sync it.
- Concurrent `FLUSH WAL` calls are serialized so each caller's success covers its own start point.
- A barrier also publishes the deferred even snapshot generation before it returns, so a successful `FLUSH WAL` leaves read-only readers a stable generation rather than an epoch that only a timer would close.
- Once a successful barrier establishes durable state, later relaxed-mode
  checkpoints, empty-chunk GC, and WAL replacement sync their replacement
  before deleting the durable artifact. A later write therefore cannot
  downgrade an earlier barrier promise. Reopened initialized stores preserve
  this floor conservatively even when the earlier process never issued a
  barrier.
- Any sync failure aborts the barrier and is returned to the caller; the
  unsynced-artifact bookkeeping is retained so a retried barrier still covers
  them. A table that is fail-closed after an earlier durability failure fails
  the barrier too. Barrier bookkeeping is bounded per table: past 65536
  tracked artifacts, the next barrier syncs that table's entire directory
  instead.

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
- torn WAL tail ignored safely, and writes acknowledged after it survive the
  next restart (the tail is truncated before appending)
- a WAL cut inside its header is replaced; a damaged header fails the load
- a WAL cut in the middle of a multi-record frame recovers the pre-mutation
  state and the pre-mutation revision; a flipped `byte_offset`, frame header
  field, or body byte is rejected by the covering CRCs
- repeated old-or-new invariant checks across replace-boundary faults
- conditional rollback/commit intent temp-write, publication, replacement, unlink, and directory-sync failures for conditional chunk replaces (`SET CHUNK ... IF VERSION`)
- abrupt process exits immediately before and after rollback publication, commit publication, and committed-intent clearing, followed by ordinary writes, `FLUSH WAL`, and another abrupt restart
- abrupt exits after odd snapshot-generation publication and before even
  publication; readers fail closed while odd and ordinary writer restart
  advances the generation and recovers
- abrupt exit while a bracket is lingering (transitions complete, even record
  deliberately unpublished): readers fail closed, writer restart recovers the
  bracketed state and republishes a fresh odd/even pair
- an exact two-transaction ABA schedule for conditional chunk replaces and both WAL boundary cases, coordinated after each WAL and intent observation
- abrupt exits just before and just after a table manifest is published:
  the directory then holds only the unpublished or the published manifest,
  restarts initialize it again or open it with the recorded geometry
- abrupt exits just before and just after the data-directory manifest is
  published, and just before and after the rename that creates or drops a
  table: the next start has the complete table or none, and removes staging
  and drop leftovers
- `SIGKILL` of a writer that writes to two tables with different geometry and
  durability through one shared cache budget while a third table is created
  and dropped in a loop

Reference:
- `tests/durability_crash_hardening_tests.cpp`
- `tests/table_catalog_tests.cpp` (create and drop crash boundaries)
- `tests/durability_kill_recovery_test.cpp` (tables under `SIGKILL`)
- `tests/wal_format_tests.cpp` (frame guards byte by byte, torn tail and
  interrupted-creation regressions)
- `tests/snapshot_generation_linger_tests.cpp`
- `tests/world_ops_regression_tests.cpp` (WAL frame tearing and corruption)

## Non-Guarantees (Explicit)

- No multi-chunk atomic transactions
- No snapshot isolation/MVCC
- No replication quorum guarantees
- No claim of durability equivalence to full transactional DBMSs
- No guarantee for filesystems/devices that violate documented sync semantics
