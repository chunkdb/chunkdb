# SWAP TABLES without an atomic exchange: design

SWAP TABLES uses an atomic directory exchange on Linux and macOS where the file system supports it ([table restore](RESTORE_TABLE.md#swap)).
On Windows, three ordinary renames use one durable guard record to decide the swap.

## Why there is no exchange

Windows moves one directory entry per call: `MoveFileExW` and `SetFileInformationByHandle(FileRenameInfo)` move an entry and can replace a file, never a directory that is not empty.
No Win32 call exchanges two entries; Transactional NTFS (`MoveFileTransacted`) is deprecated, absent on ReFS and network shares, and not used by chunkdb.
On Linux, `renameat2(RENAME_EXCHANGE)` fails with `EINVAL` on file systems that lack it; on macOS, `renamex_np(RENAME_SWAP)` fails with `ENOTSUP` outside APFS.
`ExchangeDirectories` reports all three as `function_not_supported`, changing nothing.
Each single rename is atomic, and the three renames below are not: a crash between two of them leaves one name without its table.

## The renames

Let `a` be the first name in name order and `b` the second.
The swap moves `tables/a` to a parked entry `tables/.chunkdb.swap.<16 hex>`, then `tables/b` to `tables/a`, then the parked entry to `tables/b`.
Every move uses `MoveDirectoryNoReplace`, so no step replaces an entry.
The parked entry stays inside `tables/`, so one parent directory holds every move and one directory sync covers them.
Its name starts with a dot and is not a table name: listing and opening skip it, and RECOVER TABLE's cleanup of a name's `.chunkdb.staging` and `.chunkdb.dropped` entries never matches it.

## The guard record

`chunkdb.swap.pending` in the data directory records the decision before the first rename:

| Order | Bytes |
|---|---|
| 1 | `CKSW`, version `u16` = 1, reserved zero `u16` |
| 2 | directory identity, 16 bytes |
| 3 | first name: length `u8`, name bytes, table identity, 16 bytes |
| 4 | second name: length `u8`, name bytes, table identity, 16 bytes |
| 5 | parked entry suffix, 8 bytes, written as 16 lowercase hex digits in the entry name |
| 6 | CRC32 `u32` over every preceding byte |

Names follow table-name rules and appear in strictly increasing order; identities are nonzero and differ.
Publication writes and syncs a temporary file, moves it without replacing, and syncs the data directory.
The Windows swap requires the exclusive writer lock and holds the catalog's table operations while it closes, moves and reopens the stores. An existing record refuses publication.
Windows, Linux and macOS builds recognize the record. It uses no feature bit.

## Steps and crash points

1. Admission, the WAL barriers, closing both stores and the durable slot marks run as for the exchange; a crash here leaves both names unchanged and costs an extra resync.
2. Publish the record and sync its entry before any rename. If the entry survives a crash, recovery finishes the swap.
3. Move `tables/a` to the parked entry.
4. Move `tables/b` to `tables/a`.
5. Move the parked entry to `tables/b`.
6. Sync `tables/`.
7. Remove the record and sync the data directory.
8. Open both stores under their new names and serve them, as after the exchange.

Recovery reads the three entries and identifies each by the table identity in its manifest, never by which step ran last:

| Crash after | `tables/a` | `tables/b` | parked | Recovery |
|---|---|---|---|---|
| step 1 | A | B | none | no record: nothing to do |
| step 2 | A | B | none | steps 3 to 7 |
| step 3 | none | B | A | steps 4 to 7 |
| step 4 | B | none | A | steps 5 to 7 |
| step 5 or 6 | B | A | none | steps 6 and 7 |

A rename that fails leaves its entry where it was, and a later rename needs the name the earlier one freed, so a crash leaves one of these states.
Any other combination, a missing manifest or an identity the record does not name is damage: the record stays and opening is refused with its path.

## Recovery

Writer startup finishes a recorded swap after migration recovery and before interrupted-operation cleanup and table opening.
It validates the record and the directory identity, reads the three entries, syncs the decision, runs the remaining steps and removes the record. A crash during recovery repeats it from the state it finds.
Read-only opening refuses a directory with a record, as it refuses a pending migration; a catalog opened by several processes refuses it without the exclusive writer lock.
`chunkdb_verify` reports the record as recovery pending and checks its structure.
ENABLE FEATURE refuses while a record exists, because its table inventory would miss a parked table, as it refuses beside a migration decision.

## Failures in the running server

A failure before the record is published, or a publication that leaves no record, refuses the swap. The server restores the previous slot records and reopens both tables under their original names.
A failure of step 3 moves nothing: the swap removes the record and syncs the data directory, and then refuses as above.
If publication leaves a visible record but fails to sync it, if undo fails, or if step 4, 5, 6 or 7 fails, both names are fenced with `a swap could not finish; RECOVER TABLE or a restart finishes it`.
A failure of step 8 fences the name that did not open, as after the exchange.
RECOVER TABLE of either name finishes a record that names it with the recovery steps above, then opens its own name; the other name stays fenced until its own RECOVER TABLE or a restart.
If the guard was unlinked and syncing the unlink failed, RECOVER TABLE syncs the data directory before reopening a fenced name.
The fences keep the names: CREATE, RESTORE, RENAME TO and MIGRATE refuse a fenced name, and BACKUP refuses while a table is fenced, so no other statement changes the entries the record describes.
A rename refused by another process holding a handle inside a table directory, such as a virus scanner, is one of these failures; the swap does not retry it.

## Platforms

Windows takes the guarded renames. Linux and macOS still require a supported atomic directory exchange to start a swap. Using guarded renames as a fallback on those systems is outside the Windows change.
The statement, its rights, its reply, epochs, slots, grants and watch endings are the same on both paths; only the steps between closing and reopening the stores differ.
Version clocks and grants of the reopened stores are decided after step 8 and do not depend on the path.

## Checks

Catalog tests exercise guarded renames on Windows and the atomic exchange on Linux and macOS, including refusal when the exchange is unsupported.
Failure tests inject a failure at steps 2 to 7 and check the refusal or the fences, the record on disk, and RECOVER TABLE of each name.
Windows subprocess crash tests stop before publication, after each move and sync, after guard removal and after reopening. They also interrupt startup recovery after each rename. The restarted catalog serves both names exchanged when the decision survived and both under their original names when publication did not happen.
Record tests refuse a bad magic, version, reserved field, name order, identity, length and checksum, and a record beside damaged entries.
Read-only, multi-process, ENABLE FEATURE and `chunkdb_verify` tests open a directory with a record.
