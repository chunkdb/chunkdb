# Online backup: design

`BACKUP TO 'snapshot'` creates a server-filesystem backup (#66) under the resolved `--backup-dir`.
The destination is absent or empty and outside the live source; absolute paths, `..` and symlinks below the resolved backup root are refused.
Root aliases, data paths, staging paths and verify/restore inputs may use symlinked ancestors.
MANAGES USERS is required; auth none uses the same bounded destination policy.
Only a single-process read-write catalog supports backup, since another process could change files outside these holds.
One backup owns a nonblocking catalog-wide backup guard; another receives BUSY.
The reply contains per-table epoch/revision cuts and table/file/byte counts.
A stop token checks server shutdown, independent of client disconnect or half-close; checks make no socket calls.
Ordinary failure leaves an incomplete marker; uncertain durable publication has an explicit unknown-outcome error.

## Cut and pin

Each table has its own backup pin acquired while open.
Exclusive operations wait for these pins before publishing Busy and before taking catalog operations_mutex, so a waiting ALTER or DROP does not prevent unrelated-table DDL or ordinary writes.
Backup takes no catalog-wide DDL hold through the pin phase.
A mutex/CV maintenance gate excludes replacement, collection and archive moves while linking a table's file set; checkpoints take its shared side without waiting under chunk locks and defer if it is held.
Releasing a holder and requesting shutdown directly notify cancellable waits; no timed locks or periodic cancellation acquisition loops are used.
Pinning never loads regular chunks, so it cannot trigger inline eviction or recursively enter maintenance.

The store owns the feed's stable per-thread Producer registry even without subscriptions.
Before reserving a version, a write stores its lower bound on its own aligned producer; the bound remains through postcommit durability work and is cleared at scope completion.
Clock loads, bound publication and version reservation use the watermark's sequentially consistent Dekker pairing.
Backup samples S = next revision minus one, then reads the clock and producer bounds until every in-flight bound <=S clears.
Waiting may poll/yield; writers take no completion mutex and perform no completion notification or syscall.
Feed End clears its capture buffers and detaches its registry reference; ended subscriptions cannot later clear another feed's buffers.
The store keeps producer nodes alive through feed recreation and until its own close.

Pin enumerates names without reading cold images or replaying their WALs.
It coordinates admission/eviction with each large-chunk registry and retains ordinary chunk locks while flushing resident batches and recording file lengths.
Cold image/WAL files are hard-linked directly, without adding regular chunks to the cache.
Maintenance prevents checkpoint replacement; destructive WAL repair or rollback uses inode replacement when a staging hard link exists, preserving the captured bytes.
No frame above S is admitted into the completed cut, including chunks loaded or written while pinning.
Durability poison, failed snapshot generation and resident WAL repair failure abort.
All mutations load and lock their targets before reserving revisions; transactions lock every target before their one revision.
A server transaction belongs to one table, so its cut includes it whole.

Record frozen table manifests with schema history, a clock ceiling above S, initialized markers and an even snapshot generation for the independent copy.
Slot metadata keeps names and loss state, clamping positions to S; archives are excluded because restore resets consumption to S.
Copy catalog metadata and an atomic users snapshot, then release each table's pin and maintenance hold before copying files.
Read staged image/WAL bounds and fully validate replay outside writer and maintenance locks.
A WAL whose accepted frames all lie through S can retain its whole captured crash-consistent bytes; otherwise select the final accepted frame end through S.
Hard links retain old inodes after normal checkpoint/GC resumes.
Staged reads use delete sharing on Windows so they do not prevent live-name replacement.

## Intents and publication

Rollback intents <=S must finish repair before their write bounds clear; failed repair poisons the store.
A retained committed intent has the same replay outcome as no intent; every transaction frame is included or excluded together.
Intents of later operations govern only excluded frames, so the derived cut needs no intent files.
Never copy feed archives, writer locks, temporary table artifacts or foreign entries.
Copy in bounded blocks with stop checks, sync copied files and directories, then write the CRC-protected marker and inventory last.
The incomplete guard is durable before contents become visible, and its removal is the completion point.
Prepare reply allocations and final stop checks before that point.
If completion directory sync fails, reinstate and sync the guard; failed durable reinstatement yields an explicit unknown publication outcome.
A durable CRC-protected staging owner record binds each private directory name to its source data_dir_id.
Staging cleanup failure produces a warning and leaves private cleanup work for startup; it does not reject a completed copy.
Startup removes only recognized copies owned by that catalog, even under an aliased staging root; foreign, malformed or unguarded entries are preserved and reported by verification.
An abandoned target remains explicitly incomplete.
Server/store open refuses even a complete backup directory; verification checks marker, exact safe inventory, CRCs, metadata, cuts and ordinary storage without mutation.
True WAL damage fails; ordinary crash tails can be normalized by restore.
Verification also reports leftover `.chunkdb.backups` staging.

## Restore and crashes

Restore verifies the source and builds a sibling temporary directory under a durable restore guard.
It generates a fresh data_dir_id and a new epoch per table, normalizes ordinary crash-shaped WAL tails and rewrites validated image/WAL identity headers while preserving historical schema, compression, optional sections and feature flags.
Clocks stay above S; retained slots get the new epoch at S, a fresh baseline and no archives.
Old-epoch consumers receive resync, and repeated restores choose different identities.
After syncing the temporary tree, an exclusive atomic directory rename publishes the target; platforms without that primitive refuse publication.
Sync the parent and remove/sync the restore guard last, with the same explicit unknown-outcome rule.
Failed prepublication restore removes only its own temporary copy; a crashed sibling is named in the next restore error.
A crash before completion leaves an absent or guarded target; a complete source backup stays unchanged.

## Checks

Deterministic hooks cover writer-bound pairing, late write/rollback, cold admission and replay, per-table DDL, feed recreation, cancellation, copy and publication.
Restored state is compared with acknowledged changes through each cut in relaxed and fsync-wal modes; later revisions are excluded.
Protocol tests cover rights, BUSY, configured relative targets, disconnect, half-close and shutdown.
Restore tests cover identities, temporary cleanup, root aliases, crash tails and real damage, plus subprocess crash boundaries.
Use WERROR, targeted native tests and default GCC Docker TSan; no hot-path measurements.
