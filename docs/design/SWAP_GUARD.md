# SWAP TABLES without an atomic exchange: design

SWAP TABLES exchanges `tables/a` and `tables/b` in one atomic directory exchange where the platform has one ([table restore](RESTORE_TABLE.md#swap)).
Windows has none, and some Linux and macOS file systems refuse it, so SWAP TABLES is refused there today.
This design makes the swap work on them with three ordinary renames and a guard record that turns the three into one decision.

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
The record is published as `chunkdb.migration.pending` is: written to a temporary file, synced, moved without replacing, and the data directory synced.
At most one record exists: a swap holds the catalog's table operations from its admission to its end, and an existing record refuses publication.
No feature bit guards the record: every 2.0 build reads it, and no earlier binary opens a 2.0 directory, which records `chunk_model`.

## Steps and crash points

1. Admission, the WAL barriers, closing both stores and the durable slot marks run as for the exchange; a crash here leaves both names unchanged and costs an extra resync.
2. Publish the record. Its durable entry is the decision: from here a crash finishes the swap.
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
It validates the record and the directory identity, reads the three entries, runs the remaining steps and removes the record; a crash during recovery repeats it from the state it finds.
Read-only opening refuses a directory with a record, as it refuses a pending migration; a catalog opened by several processes refuses it without the exclusive writer lock.
`chunkdb_verify` reports the record as recovery pending and checks its structure.
ENABLE FEATURE refuses while a record exists, because its table inventory would miss a parked table, as it refuses beside a migration decision.

## Failures in the running server

A failure before the record is published, or a publication that leaves no record, refuses the swap: the slot records are put back and both tables reopen under their names, as after a refused exchange.
A failure of step 3 moves nothing: the swap removes the record and syncs the data directory, and then refuses as above.
If that removal fails, or step 4, 5, 6 or 7 fails, the swap is decided but unfinished: both names are fenced with `a swap could not finish; RECOVER TABLE or a restart finishes it`.
A failure of step 8 fences the name that did not open, as after the exchange.
RECOVER TABLE of either name finishes a record that names it with the recovery steps above, then opens its own name; the other name stays fenced until its own RECOVER TABLE or a restart.
The fences keep the names: CREATE, RESTORE, RENAME TO and MIGRATE refuse a fenced name, and BACKUP refuses while a table is fenced, so no other statement changes the entries the record describes.
A rename refused by another process holding a handle inside a table directory, such as a virus scanner, is one of these failures; the swap does not retry it.

## Platforms

Windows always takes the guarded renames; Linux and macOS take them only when the exchange reports `function_not_supported`.
The exchange stays the first choice where it exists, because it needs no record and no recovery.
The statement, its rights, its reply, epochs, slots, grants and watch endings are the same on both paths; only the steps between closing and reopening the stores differ.
Version clocks and grants of the reopened stores are decided after step 8 and do not depend on the path.

## Checks

Catalog tests run every swap test on the guarded path, forced on Linux and macOS by `CHUNKDB_FAILPOINT_SWAP_TABLES_EXCHANGE_UNSUPPORTED_ONCE`, and on Windows without it.
Failure tests inject a failure at steps 2 to 7 and check the refusal or the fences, the record on disk, and RECOVER TABLE of each name.
Subprocess crash tests stop after each of steps 2 to 7 and during recovery after each rename; the restarted catalog serves both names exchanged, or both unchanged for a crash before step 2.
Record tests refuse a bad magic, version, reserved field, name order, identity, length and checksum, and a record beside damaged entries.
Read-only, multi-process, ENABLE FEATURE and `chunkdb_verify` tests open a directory with a record.
