# Converting a 1.x data directory

chunkdb 2.0 stores data in a new format and does not open a data directory
written by chunkdb 1.x. Starting a 2.0 server on one fails, without changing
it, with an error that names `chunkdb_migrate`. `chunkdb_migrate` converts the
directory offline into a new 2.0 data directory whose `default` table holds
the old data.

It also converts directories written by development builds of the 2.0 line
from before the 2.0 storage format (the format of `main` between 1.3.0 and
2.0).

## Steps

1. Stop the 1.x server. `chunkdb_migrate` refuses a directory that a running
   server holds.
2. Convert into a new directory, giving the geometry the 1.x server ran with
   (the same flags; the defaults are the server's):

   ```bash
   chunkdb_migrate --from ./data --to ./data-2.0 \
     --block-bits 16 --chunk-width 16 --chunk-height 16 \
     --large-chunk-width 8 --large-chunk-height 8 \
     --durability fsync-wal
   ```

3. Start the 2.0 server on the new directory. Pass the same option flags
   you gave `chunkdb_migrate`, or none: a flag that differs from the table's
   stored option refuses the start.

   ```bash
   chunkdb_server --data-dir ./data-2.0 --durability fsync-wal ...
   ```

The source directory is never modified, so the 1.x server keeps working on it
until you switch. The result is built in a hidden directory whose name
contains `.migrate-`, next to `--to` (inside `--to` when it already exists,
for example as a mount point), and moved into place only once it is complete
and verified; a refused or failed run leaves nothing at `--to`. If the process is
killed, delete that hidden directory.

## Options

- `--from <path>`: the 1.x data directory.
- `--to <path>`: the new directory; it must not exist or must be empty.
- Geometry: `--block-bits`, `--chunk-width`, `--chunk-height`,
  `--large-chunk-width`, `--large-chunk-height`. A 1.x directory does not
  record its large-chunk size, so give every flag the 1.x server was started
  with. Wrong block or chunk sizes are always detected. A wrong large-chunk
  size is detected unless every chunk lies in the same large chunk under
  both sizes, in which case it only changes how the new table groups chunks
  on disk.
- Options of the new `default` table, as for `chunkdb_server`: `--durability`,
  `--checkpoint-updates`, `--checkpoint-wal-bytes`,
  `--wal-group-commit-updates`, `--checkpoint-compression`.
- `--accept-loss`: see [Damaged data](#damaged-data).

## What is converted

Every chunk gets the state the old server loaded:

- the chunk image and its WAL, including a WAL interrupted by a crash (a torn
  last write is ignored, as the old server ignored it);
- interrupted conditional writes (`CHUNKCAS`, `CHUNKBATCH`) are rolled back
  as the old server did at startup;
- chunks with no present block are absent and are not written.

Payload bits of absent blocks and unused padding bits are stored as zero, as
every 2.0 write stores them. `GET` and the presence of blocks are unchanged;
`CHUNKGET ... STATE` differs from the old `CHUNK ... STATE` only in such bits.
The summary counts the chunks where this changed something
(`canonicalized_chunks`).

Chunk versions (`CHUNKVER`): a chunk written by the 2.0 development format
keeps its persisted version. A chunk written by 1.x had none; it gets a new
version at or above the old store's version clock, so no token the 1.x server
handed out matches it.

## Damaged data

The old server dropped data in some cases: a WAL with a damaged header, the
writes after a damaged record in the middle of a WAL, and a chunk whose image
it could not read. `chunkdb_migrate` refuses to convert such a directory and
lists every affected file. `--accept-loss` converts it anyway and drops the
same data the old server dropped; the summary lists each drop.

It always refuses a directory the old server would not have started on (a
damaged version clock or conditional-write intent), files in the wrong place
for the given geometry, and data of the experimental `fs_region_v1` layout.

## Output

On success it prints a summary: chunks converted, image and WAL versions
read, torn tails, conditional writes rolled back, versions kept and assigned,
notes about files it ignored, and the result of running the `chunkdb_verify`
checks on the new directory.

Exit status: `0` converted, `1` refused or failed, `2` invalid invocation.
