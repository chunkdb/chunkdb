# WAL archive, streamed backup and point-in-time restore: design

A writer started with `--archive-dir <path>` or `--archive-command <cmd>` copies every sealed WAL segment of every table to an archive, with a retention bound (#149).
`chunkdb_restore <backup> <target> --archive <path> --until-time ms | --until-revision n` restores a full backup and rolls it forward with the archive to a point in time.
`BACKUP STREAM [TABLES a, b]` sends a backup over the connection instead of writing it under `--backup-dir`.
The [backup design](BACKUP_DESIGN.md) defines cuts, pins and publication; [Feed slots and archives](../STORAGE_FORMAT.md#feed-slots-and-archives) defines segments and base descriptors.

## Terms

A segment is one chunk's WAL between two checkpoints: `C_<coordinates>.<first>-<last>.wal` with its `.base` descriptor, whose base revision is the image the WAL was written on.
A checkpoint seals the segment: it publishes the new image and renames the WAL into `.chunkdb.feed/`; a sealed segment never changes.
A pass is one run of the archiver over all tables; it ends with one seal record.
A table's seal point S in a record means: every frame of the table at or below S is in a segment listed by this record or an earlier one of its chain.
A table's time bound B in a record means: every frame above S has a commit time at or above B.
A chain is the run of seal records of one data directory written while archiving stayed on; each record names its predecessor.

## Sealing

Archiving makes every checkpoint take the archive path that feed slots use, so frames are never dropped with a WAL; checkpoints then sync their images in every mode.
Writes keep WAL prefix boundaries as they do for an active slot; without the archive flags the write path is unchanged.
A table needs the `feed_slots` feature, which new tables have; a writer refuses the archive flags for a table without it, for `--read-only` and for multi-process opening.

One archiver thread serves the server and runs a pass at start, every `--archive-interval-ms` (60000) and at clean shutdown. Per table, under a shared catalog lease:

1. Issue B from the table's commit-time counter, which never decreases.
2. Sample S as backup does: next revision minus one, then wait until every in-flight producer bound at or below S clears. Every revision above S is reserved after the sample and takes its commit time after its reservation, so its time is at or above B.
3. Flush and sync the staged WAL batches up to S, as a slot synchronization does.
4. Checkpoint every chunk whose live WAL holds a frame at or below S; each such frame is now in a sealed segment.
5. Hard-link every sealed segment and descriptor not staged yet into `.chunkdb.archive/table.<epoch>/` and sync it; only then may feed retention remove the `.chunkdb.feed/` names.
6. Stage the table manifest when its bytes differ from the last staged one.

A checkpoint deferred by a running backup leaves the table without a seal point in this pass; its staged files still ship.
The catalog part of a record (names, epochs, manifests, users file, migrations ledger) is captured under the backup metadata gate, so names, schemas and grants agree; the gate is held only for that capture.
The lease is released before shipping, so a slow archive never delays DDL; staged links survive DROP TABLE.

## Shipping and seal records

The pass ships every staged file, then publishes its seal record, then removes the staged files the record lists.
`--archive-dir` copies to a `.`-prefixed temporary name, syncs it, renames it without replacing and syncs the directory; an existing file of equal length and CRC32 counts as shipped, a different one fails the pass.
`--archive-command` runs through the system shell with `%p` the local path, `%f` the archive-relative name and `%%` a `%`; exit status 0 means shipped, and the command must accept a file it already shipped with equal bytes.
A failed pass is retried every interval; staged files stay in the data directory meanwhile, and SHOW STATUS shows `archive` with the error and the staged bytes.

Layout: `table.<epoch>/` holds segments, descriptors and manifests `table.<schema version>.<CRC32>.manifest`; `directory.<data-directory id>/` holds `seal.<capture Unix ms, 20 digits>`, `users.<CRC32>` and `migrations.<CRC32>`.
A seal record holds the data-directory id, its capture time, its predecessor's capture time (zero at a chain start), the users and ledger file names, and per table: name, epoch, seal point and time bound (absent when unsealed), schema version, manifest name, and the files shipped since the predecessor with length and CRC32.
Every table of the catalog appears in every record, so an idle table gets a new seal point each pass.
A record is published only after every file it names is durable in the archive.
`.chunkdb.archive/chain` holds the last published record's name; a writer start with archiving reads it as the predecessor, and a writer start without archiving, or an offline tool that changes table files, removes it before any table file changes, so the next record starts a new chain.
Two records naming the same predecessor are a fork; restore refuses it.

## Retention

`--archive-retention-ms` (0, keep all) applies to `--archive-dir`; a command's destination keeps its own retention.
A pass takes k, the newest record captured before now minus the bound, and removes older records oldest first; it removes a record before the files only it lists.
A segment listed only by removed records has its last revision at or below k's seal point of its table, so no state at or after k needs it.
Files of tables absent from k, and manifests, users and ledger files that no kept record names, go with the last record naming them; unlisted segments at or below k's seal point, left by a crash, are removed too.
A backup created after k's capture time rolls forward; an older one cannot, and restore says so.

## Point-in-time restore

The restore reads the archive from a local path; with `--archive-command`, fetch the destination first.
It checks the backup as today, then chooses the target per table:

- `--until-revision n`: one table (the backup holds one, or `--table` names it); n is at or above the cut and at or below a seal point.
- `--until-time ms`: the state is a revision prefix. For each table it needs a record with time bound above ms; n is the largest revision at or below that record's seal point such that every frame at or below n committed at or before ms.
- R, the catalog record: the newest record captured at or before ms, or for a revision the first that seals the table at or above n. Tables, users and ledger come from R.

The chain from R back must be unbroken to a record captured before the backup was created, so every segment sealed after the backup was staged.
Each table of R is rebuilt from the backup when its epoch is there, or from empty when its epoch first appears after the backup with a manifest without lineage; a table restored by RESTORE TABLE after the backup cannot be rebuilt, and restore refuses and names it (restore the others with `--table`).
Per chunk, the segments with last above the chunk's backup revision must exist with their listed length and CRC and chain: the first one's base is the backup image (or never written, or retained empty), each later base is the previous last.
Frames above the chunk's backup revision and at or below n are appended to the copied WAL, or to a new WAL for a chunk the backup lacks; the writer's first open replays them and finds new chunks by their files.
The table takes the manifest R names, or a newer one from the archive when an applied frame uses a newer schema; an ALTER with no write between R and n is not restored.
A table dropped before n restores to its last seal point, and the output says so; COMPACT TOMBSTONES after the backup is not replayed.
Rebinding then gives the table a new epoch with lineage (old epoch, n) and a clock above n, as a restore at cut n; the output lists each table's epoch and n.
Once cross-table groups exist, a time cut lowers a table's n below any group member whose other members fall outside their tables' n, until no group is split.

## BACKUP STREAM

`BACKUP STREAM [TABLES a, b]` needs BACKUP, no `--backup-dir`, and the backup guard (BUSY while another backup runs); without TABLES it cuts every table.
It pins and validates into `.chunkdb.backups/` as BACKUP TO does, then replies with a map of `tables` (name, epoch, revision), `files` and `bytes`.
Pushes follow: per file `[backup_file, path, length]`, then `[backup_data, bytes]` of at most 1 MiB each, then `[backup_file_end, crc32]`; last `[backup_end, record]`, the `chunkdb.backup` bytes.
A failure ends the stream with `[backup_failed, error]`; after either push ordinary statements resume.
Any statement sent during the stream receives PROTOCOL and closes the connection, as during WATCH; a disconnect, CANCEL or shutdown stops the backup and removes its staging.
The output deadline applies to each push; a slow receiver holds the pins, whose hard links keep replaced files on disk.
The receiver writes the guard first, each file with its length and CRC checked, syncs them, writes `chunkdb.backup` last and removes the guard; the result verifies and restores like BACKUP TO's, and chunk-cli `chunk backup` receives it.

## Crash points

- During a checkpoint: as without archiving; the segment is in `.chunkdb.feed/` or still live.
- After staging links, before their sync: the feed names are still there, and the next pass stages them again.
- During shipping: a temporary name is left; the next pass ships again, and retention removes `.`-prefixed names older than the bound.
- After shipping, before the record: the files are unlisted; the next pass finds them equal and lists them.
- After the record, before staged removal: the next pass ships them again as equal and lists them again; a record may repeat a file.
- During retention: the record is gone first; its files are removed by a later pass as unlisted segments.
- During restore: only its guarded temporary directory exists, as today.
- During BACKUP STREAM: the server removes its staging at the next start; the receiver's target keeps its guard.

## Checks

Crash tests stop the archiver at each point above and check that a restart ships again and the archive still restores.
A restore test writes, backs up, then writes with ALTER, collection, transactions, CREATE and DROP TABLE, and compares a restore at each seal point and at chosen times with the acknowledged state.
Damage tests remove a listed segment, break a chunk chain, restart without archiving, fork a chain, and start the archive after the backup.
Stream tests cover rights, BUSY, TABLES, disconnect, CANCEL, a statement during the stream and a receiver that restores.
The archive is off by default; a bench scenario with `--archive-dir` measures writes with archiving on.
