# Related data

chunkdb has no query planner and does not join tables.
Each statement reads or writes one table at the coordinates you name, and a transaction covers one table.
This page shows what to do instead, in the [CLI shell](QUICK_START.md#2-connect-the-shell).
Lookup by value needs secondary indexes, which chunkdb does not have yet; see [Known limitations](KNOWN_LIMITATIONS.md) and keep a derived table keyed by the value instead.

## Keep data read together in one table

A transaction covers one table, so put the columns you read or change together in one table:

```cql docs-check session=main
CREATE TABLE terrain (kind u8, height i16) CHUNK 2 x 2
SET BLOCK 0 0 IN terrain kind = 1, height = 12
GET BLOCK 0 0 FROM terrain
```

```output
OK
1
kind = 1
height = 12
```

Values that belong to the whole chunk rather than to one block go in [chunk columns](reference/CHUNKS.md), which every GET CHUNK returns with the chunk's blocks:

```cql docs-check session=main
CREATE TABLE region (kind u8) CHUNK 2 x 2 CHUNK COLUMNS (biome u8 DEFAULT 0)
SET CHUNK 0 0 IN region META biome = 3
SET BLOCK 0 0 IN region kind = 1
--blocks GET CHUNK 0 0 FROM region
```

```output
OK
1
2
version = 2
schema_version = 1
commit_time_ms = <time-ms>
present = 1 of 4 blocks
chunk_columns:
  biome = 3
block 0 0
  kind = 1
```

## Read related chunks of two tables

What works today:

- Within one table, `BEGIN` reads and writes any of its chunks at one snapshot and commits them together.
- Within one table, `BEGIN READ ONLY` reads, scans and aggregates at one snapshot with no read-chunk limit; see [Transactions](TRANSACTIONS.md#read-only-transactions).
- Across two tables, each read has its own snapshot; chunk versions show whether a write came between them.

One transaction that reads two tables at one snapshot, or commits to both, needs cross-table transactions, which chunkdb does not have yet ([#136](https://github.com/chunkdb/chunkdb/issues/136)).

Use a separate table when the data has its own writers, rights or feed consumers.
Give both tables the same chunk size, read the same block or area from each, and match the results by coordinates in your client:

```cql docs-check session=main
CREATE TABLE light (level u4) CHUNK 2 x 2
SET BLOCK 0 0 IN light level = 9
GET BLOCK 0 0 FROM terrain COLUMNS height
GET BLOCK 0 0 FROM light
```

```output
OK
1
height = 12
level = 9
```

The two reads do not share a snapshot, so a write between them can make the results disagree.
A transaction reads its first table at one snapshot and refuses a statement on another table.
Here a read-only transaction reads two chunks of `terrain`, which writes from other connections cannot change while it is open, and refuses the read of `light`:

```cql docs-check session=main
SET BLOCK 2 0 IN terrain kind = 1, height = 7
BEGIN READ ONLY
GET BLOCK 0 0 FROM terrain COLUMNS height
GET BLOCK 2 0 FROM terrain COLUMNS height
GET BLOCK 0 0 FROM light
ROLLBACK
```

```output
2
OK
height = 12
height = 7
error: INVALID_ARGUMENT reason=invalid_argument a transaction covers one table: this one covers terrain
OK
```

To detect a write between reads of two tables, read the chunk of each table with GET CHUNK and note its `version`:

```cql docs-check session=main
--blocks GET CHUNK 0 0 FROM terrain
--blocks GET CHUNK 0 0 FROM light
```

```output
version = 1
schema_version = 1
commit_time_ms = <time-ms>
present = 1 of 4 blocks
block 0 0
  kind = 1
  height = 12
version = 1
schema_version = 1
commit_time_ms = <time-ms>
present = 1 of 4 blocks
block 0 0
  level = 9
```

Then read both chunks again.
If neither `version` changed, nothing wrote to either chunk between the passes and the first reads match one state of both tables.
Otherwise repeat the reads.
This holds only while each table keeps its epoch: SWAP TABLES, RESTORE TABLE, or DROP and CREATE start a new epoch, so also compare the `epoch` from DESCRIBE before the first pass and after the second.
A chunk that GET CHUNK returns as (null) has no version, so the check holds only for chunks present in both passes.
Rows read together from a transaction on one table are always consistent, so use one table when that matters.

## Keep a derived table up to date

A derived table holds values your client computes from another table, such as a summary or a lookup by value.
Here block `cx cy` of `terrain_top` holds the highest `height` in chunk `cx cy` of `terrain`, with the chunk version it was computed from.
Create the derived table and a [slot](CHANGE_FEED.md), fill it from SCAN CHUNKS and GET CHUNK, then apply each change the slot delivers:

```cql docs-check session=main
CREATE TABLE terrain_top (height i16, source_version u64) CHUNK 2 x 2
CREATE SLOT 'terrain_top' ON terrain
SET BLOCK 0 0 IN terrain_top height = 12, source_version = 1
```

```output
OK
OK
1
```

For each change, skip the chunk if its `source_version` is not lower than the change revision; otherwise read the chunk again, write its summary, and ACK the revision after the write succeeds.
The stored version lets a restarted consumer skip changes it already applied.
If the slot is lost, rebuild the derived table as in [Resume a consumer](FEED_RESUME.md).
