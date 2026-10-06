# Tables Design (chunkdb 2.0, issue #41)

Status: **implemented** (decisions in §10). The normative descriptions are
`docs/STORAGE_FORMAT.md` (layout, manifests), `docs/PROTOCOL.md` (commands)
and `docs/SERVER_FLAGS.md` (flags); this document keeps the reasoning.

## 1. Goal

One server process and one data directory serve several named worlds
(**tables**). Each table has its own geometry, fixed when the table is created,
and its own durability and checkpoint options, which can change later.

Out of scope: copying data between tables, per-table access control, protocol 2
(`HELLO`, #42), conversion of older data directories (#43).

## 2. Layout

```text
data_dir/
  chunkdb.manifest        data-directory manifest (Section 3)
  .chunkdb.lock/          writer lock: one writer process per data directory
  .chunkdb.staging/       tables being created; emptied when a writer starts
  .chunkdb.dropped/       tables being dropped; emptied when a writer starts
  tables/<name>/          one table
    table.manifest        geometry, store id, feature flags, options
    chunkdb.version       per-table revision clock
    chunkdb.snapshot      per-table snapshot generation
    .chunkdb.initialized
    .chunkdb.intents/
    L_<lx>_<ly>/C_<cx>_<cy>.{chk,wal}
```

A table directory is exactly the store directory of #38/#40, with the manifest
renamed from `chunkdb.manifest` to `table.manifest`. Each table is a separate
`ChunkStore`: its own revision clock, snapshot generation, intents, WAL and
checkpoints. Revisions are ordered within a table, not across tables.

Table names match `[a-z0-9][a-z0-9_-]{0,63}` and are not a Windows device name
(`con`, `prn`, `aux`, `nul`, `com0`-`com9`, `lpt0`-`lpt9`), so a data directory
can move between platforms. Names are lowercase, so case-insensitive
filesystems cannot alias two tables.

## 3. Data-directory manifest (D1)

`data_dir/chunkdb.manifest`: magic `CKDM`, version 1, three feature-flag sets
with the same rules as a table manifest, a random data-directory id, a TLV
options area (none defined) and a CRC32. It is published (no-replace, synced)
before `tables/` or anything else is created.

Why a second manifest: table flags protect what is inside a table. A later
change to the directory itself (a catalog file, table aliases, per-table access
rules) needs a place where an older binary learns that it must not open the
directory. Without it, the only signal would be a missing or unexpected file.

A directory whose `chunkdb.manifest` is a store manifest (magic `CKMF`, the
single-store layout of a 2.0 development build before tables) is refused with
a message naming that layout; `chunkdb_migrate` (#43) converts it.

## 4. Opening a data directory

Writer:

1. Create `data_dir` if needed and take the writer lock.
2. No `chunkdb.manifest`: the directory must hold no chunkdb entry (the lock
   aside); publish the manifest. Otherwise read it and check its flags.
3. Empty `.chunkdb.staging/` and `.chunkdb.dropped/` (interrupted create and
   drop).
4. Open every table under `tables/`. Opening reads and checks the manifest
   only; chunk loading and WAL recovery stay lazy, so startup cost grows with
   the number of tables, not with their size.
5. No table at all: create `default` from the geometry flags (Section 7).

Read-only processes need an existing manifest, open the tables present at
startup and follow each table's snapshot generation. Tables the writer
creates later are not seen until restart. A table the writer drops fails its
next chunk load: its snapshot-generation record, which a writer never removes,
is gone, and a reader that saw it at open treats its absence as removal
instead of reading an empty table.

## 5. Create and drop are crash-atomic

`TABLECREATE` builds the complete table under a fresh name in
`.chunkdb.staging/`: directory, `table.manifest` (synced) and a directory sync.
A rename to `tables/<name>` (no-replace) publishes it, followed by syncs of both
parents. A table directory that contains only its manifest is a valid empty
table (the store initializes the rest on open, as after a crash right after
manifest publication in #38). A crash before the rename leaves a staging
directory, removed at the next start; after it, the full table.

`TABLEDROP` waits for in-flight commands on the table, closes its store,
renames `tables/<name>` to a fresh name in `.chunkdb.dropped/` (syncing both
parents), then deletes it. The rename is the commit point: before it the table
is intact, after it the table is gone, and a crash during deletion leaves
leftovers that the next start removes. If the rename fails, the table is
reopened and the command fails.

Both run under the writer lock and a catalog mutex, so no other creator can
race for the name.

## 6. Tables at runtime

- A **table handle** stays valid for the life of a connection that selected
  it. Every data command takes a short lease on the table; drop and option
  changes wait for running leases and block new ones. After a drop, a lease
  attempt reports the table as gone (`-ERR NO_TABLE`), even if a new table of
  the same name was created since: the new table may have another geometry.
- **Option changes** (`TABLESET`) write the new manifest atomically
  (`AtomicWrite`, synced), close the store and reopen it with the new options.
  This reuses the tested open and close paths instead of changing durability
  settings under running operations. The table's chunks leave the cache.
  `TABLESET` names only the options it changes; they are merged into the
  current options under the catalog lock, so concurrent changes do not undo
  each other.
- An exclusive operation always ends: a failure before the manifest changes
  serves the old store again; a table that cannot be reopened, or whose drop
  fails in an unknown state, is taken out of service until restart instead of
  leaving commands waiting.
- A store opened directly on `tables/<name>` (embedding, tools) takes the data
  directory's writer lock, so it cannot write beside a running server.
- **One cache budget.** `--max-loaded-chunks` counts chunks of all tables. One
  access clock orders accesses across tables. When the total exceeds the
  budget, eviction repeatedly takes the coldest known candidate among all
  tables, so a busy table uses memory an idle table does not. An eviction
  failure in another table is logged and that table is not a victim for a
  second, so a fail-closed table cannot stop other tables from loading chunks
  or flood the log.
- **One WAL stream budget.** `--max-open-wal-streams` is shared the same way:
  file descriptors belong to the process, and per-table pools would multiply
  the descriptor budget the server fits under `RLIMIT_NOFILE`. When the pool is
  full, the least recently used idle stream of any table is closed.
- **`WALFLUSH`** is a barrier over all tables (D3): a connection may have
  written to several tables, and the command's contract is "every write
  acknowledged before the call began is durable". Every table is attempted;
  it fails if any table cannot complete its barrier.

## 7. Options and flags

| Option (TABLEINFO key) | Manifest TLV type | Encoding |
|---|---|---|
| `durability_mode` | 1 | u8: 0 relaxed, 1 fsync-wal, 2 fsync-checkpoint |
| `checkpoint_updates` | 2 | u64, > 0 |
| `checkpoint_wal_bytes` | 3 | u64, > 0 |
| `wal_group_commit_updates` | 4 | u64, > 0 |
| `checkpoint_compression` | 5 | u8: 0 none, 1 zrle |

Tables record all five. Each type appears at most once; an absent type takes
its built-in default, so options added later need no rewrite of old tables.

Server flags `--durability`, `--checkpoint-updates`, `--checkpoint-wal-bytes`,
`--wal-group-commit-updates` and `--checkpoint-compression` are the defaults
for tables created by this server process. They do not change existing tables:
a flag that differs from a table's stored value is logged at startup, and
`TABLESET` changes the table.

The geometry flags describe `default` when the server creates it. When `default`
exists, a given geometry flag must match it, as in #38.

`--max-loaded-chunks`, `--max-open-wal-streams` and the background-maintenance
flags stay server-wide.

## 8. Commands (D2)

Key/value pairs use the names `TABLEINFO` and `USE` print, so what a client
reads is what it writes.

```text
TABLECREATE <name> block_bits <n> [chunk_width_blocks <n>] [chunk_height_blocks <n>]
            [large_chunk_width_chunks <n>] [large_chunk_height_chunks <n>]
            [<option> <value> ...]
TABLEDROP <name>
TABLES
TABLEINFO <name>
TABLESET <name> <option> <value> [<option> <value> ...]
USE <name>
```

- `block_bits` is required; omitted chunk and large-chunk sizes take the
  built-in defaults (16x16 blocks, 8x8 chunks), independent of how the server
  was started.
- `TABLEINFO` and `USE` reply with the same bulk of `key=value` lines: `table`,
  `store_id`, the five geometry fields (named as in `INFO`) and the five
  options. Keys are case-insensitive and may appear once. `INFO` keeps
  server statistics plus the selected table's geometry and options, so 1.x
  clients keep working.
- A connection starts on `default`. `USE` with an unknown name fails with
  `-ERR NO_TABLE` and keeps the current table.
- New error codes: `NO_TABLE` (unknown or dropped table) and `TABLE_EXISTS`.

## 9. Clients (D4)

All three clients get table support in this issue: a table handle
(`client.table("terrain")` in JS, `Client.Table` in Go), one pool per table,
the URI path (`chunk://host:4242/terrain`) selecting the table at connect, and
`use` / `--table` in `chunk-cli`. Clients that never select a table keep
working on `default`.

## 10. Decisions

- **D1** Data-directory manifest: yes (Section 3).
- **D2** Command syntax: key/value pairs named as in `TABLEINFO` (Section 8).
- **D3** `WALFLUSH` covers all tables (Section 6).
- **D4** Client support ships with this issue (Section 9).
