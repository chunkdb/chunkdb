# WAL archive and point-in-time restore: design

A server started with `--archive-dir <path>` or `--archive-command <cmd>` copies every sealed WAL segment of every table to an archive (#149).
`chunkdb_restore --archive <path> --until-time ms | --until-revision n` rolls a full backup forward with the archive to a point in time.
`BACKUP STREAM` sends a backup over the connection instead of writing it under `--backup-dir`.

## Terms

A segment is one chunk's WAL between two checkpoints; the checkpoint that replaces it seals it.
A seal point of a table is a completed revision R through which every committed frame is in a sealed segment.
The archive of a table is the directory `<archive>/<epoch hex32>/`; a restore never mixes epochs.

## Local sealing

Archiving turns on the archive path of every checkpoint, the one feed slots use: before the image is published, the WAL is renamed into `.chunkdb.feed/` with its base descriptor.
The segment, descriptor and pinned image formats are those of [Feed slots and archives](../STORAGE_FORMAT.md#feed-slots-and-archives), so a table needs the `feed_slots` feature, which 2.0 tables have from creation; a table without it does not open while archiving is on.
Archiving runs only on a single-process writer; `--read-only` refuses the archive flags.

One archiver thread serves the server. Every `--archive-interval-ms` (60000), and once at start and at clean shutdown, it runs a pass over all tables, each under a shared lease:

1. Read R, the completed revision.
2. Flush and sync every staged WAL batch, as a slot synchronization pass does.
3. List the live WAL prefixes and checkpoint every chunk whose live WAL holds a frame at or below R.
4. Hard-link every segment of `.chunkdb.feed/` not staged yet, and its descriptor, into `.chunkdb.archive/table.<epoch>/`, sync that directory, then let retention remove staged segments that no slot needs.
5. Stage the table manifest when its schema version is new for this process.

After step 3 every frame at or below R is in a sealed segment.
A checkpoint deferred by a running backup's link phase leaves the table without a seal point in this pass; its staged files still ship.
The lease is released before shipping, so a slow archive never delays DDL; staged links survive DROP TABLE, so segments sealed before the drop still ship.

## Shipping and seal records

After staging, the pass ships every staged file, then publishes one seal record for all tables, then removes the staged files the record lists.
`--archive-dir` copies to a temporary name, syncs it, renames it without replacing and syncs the directory; an existing file with equal length and CRC32 counts as shipped, a different one fails the pass.
`--archive-command` runs the command through the system shell with `%p` replaced by the local file path, `%f` by the archive-relative name and `%%` by `%`; exit status 0 means shipped, and the command must accept a file it already shipped with equal bytes.
A failing pass is retried every interval; staged files stay meanwhile and SHOW STATUS shows the error.
After a restart, segments still in `.chunkdb.feed/` are staged and shipped again; manifests are named by schema version and content CRC, so the same bytes ship under the same name.

Archive layout: `table.<epoch>/` holds segments `C_<coordinates>.<first>-<last>.wal`, descriptors `C_<coordinates>.<first>.base` and manifests `table.<schema version>.<crc>.manifest`; `directory.<data-directory id>/seal.<capture ms, 20 digits>` holds the seal records.
A seal record lists, per table, the seal point R (if this pass sealed the table), the schema version and the files shipped since the previous record; its format is in [WAL archive](../STORAGE_FORMAT.md#wal-archive).
Each record is published only after its files are durable, so a table's seal point R means every frame at or below R is in a segment listed by this or an earlier record.
An idle table gets a seal point in every record, which proves that nothing changed up to that record's capture time.

## Retention

`--archive-retention-ms` (0, keep all) applies to `--archive-dir`; a command's destination keeps its own retention.
A pass takes boundary record k, the newest record captured before now minus the bound, and removes older records oldest first while each of their tables is sealed in k above every listed segment's last revision, or is listed in neither k nor the newest record.
It removes the record first, then its segments and descriptors; manifests stay, since a later record can list the same one again.
For a table, every segment whose last revision is above the oldest kept seal point is still listed by a kept record.

## Point-in-time restore

The restore copies and verifies the backup as today, then, before rebinding each table, rolls it forward from the table's backup cut C to the target T:

- The archive must start at or below C: its oldest seal record has R at most C.
- `--until-revision n` needs C ≤ n ≤ the newest seal point; with several tables it needs `--table`.
- `--until-time ms` takes for each table the newest revision at or below the newest seal point whose frames have a commit time at or before ms; ms must be before the newest record's capture time.
- Every segment listed in a record with last above C must exist with its length and CRC.
- Per chunk, segments chain: each segment's base revision is the previous segment's last revision.
- Frames above the chunk's backup revision and at or below T are appended to the copied WAL; the server's recovery replays them.
- The table takes the schema history of the archived manifest with the highest schema version used at or below T.

Tables created after the backup are not in it and are not restored; RESTORE TABLE and SWAP TABLES start other epochs, which need a new backup.

## BACKUP STREAM

`BACKUP STREAM [TABLES a, b]` takes the same cuts as BACKUP TO and sends the files as pushes on the connection instead of writing them under `--backup-dir`.
The receiver writes them and the inventory, and the result verifies and restores like a backup on disk.

## Checks

Crash tests stop the archiver between ship, record and removal, and check that a restart ships again and the archive still restores.
A restore test writes, backs up, writes again with ALTER, collection and transactions, and compares a restore at each seal point and time with the acknowledged state.
Damage tests remove a listed segment, break a chain, and start the archive after the backup cut.
