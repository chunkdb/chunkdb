# Rebuild a lost version clock

Each table keeps its revision clock in `tables/<name>/chunkdb.version` and `tables/<name>/.chunkdb.initialized`.
The server refuses to open a table whose clock is missing, malformed or the wrong size, or whose marker is invalid, so it never gives out a revision twice; the error names `chunkdb_admin rebuild-version-clock`.
The server accepts both files missing on a table that holds no data, and rewrites a lost marker beside a valid clock at startup.
A clock the server cannot read because of its permissions or a disk error is not rebuilt; fix the file's access first.
Restore a consistent [backup](BACKUP.md) when you have one; rebuild the clock when no backup holds the lost files.

## Docker

These commands use the container and volume from [Quick start](QUICK_START.md).

```sh
docker stop chunkdb-quickstart
docker run --rm --entrypoint chunkdb_admin \
  -v chunkdb-quickstart-data:/var/lib/chunkdb ghcr.io/chunkdb/chunkdb:2 \
  --data-dir /var/lib/chunkdb/data rebuild-version-clock
docker start chunkdb-quickstart
```

## Downloaded server

Stop the server, then run:

```sh
./chunkdb_admin --data-dir ./data rebuild-version-clock
```

```output
table world: rebuilt, next revision 4294967813, highest on disk 517
```

The tool prints one line per table, then you start the server again.
The next write of a rebuilt table gets a revision at or above its printed next revision.

## What the rebuild does

- It takes the writer lock, so it refuses while a server has the directory open.
- It runs the `chunkdb_verify` checks on every table and writes their findings to standard error.
- It reads the highest revision of chunk images, WALs, archived WALs and their bases, coordinate files, transaction intents, slot positions and the restore lineage.
- Damage other than a missing or malformed clock or initialized marker refuses the whole directory, and no file changes.
- A pending Windows swap must be finished by a writer start before rebuilding clocks.
- It refuses a directory or table that uses a feature this build can only read, as the server does.
- A table whose clock is readable and above every revision on disk keeps it and prints `kept`.
- Any other table gets a clock 2^32 revisions above its highest revision on disk, or above the directory's version floor when that is higher, and prints `rebuilt`.
- The margin covers revisions given out but never written, such as relaxed-durability writes lost in a crash.
- A table whose marker is missing or invalid also gets a new one, and its line ends with `marker rewritten`.
- A clock is never lowered.
- It writes the clock and then the initialized marker, each synced and replaced atomically.

The tool refuses a backup directory; restore the backup with `chunkdb_restore` first.
It exits with 0 after the rebuild, 1 when it refuses, and 2 for a wrong command line.
The tool must have permission to write the data directory; run it as the server's operating-system account.
