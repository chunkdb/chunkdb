# Block History

A table with history keeps every committed change of every block: when it happened (the mutation's revision and commit time), the block before and after (its bits and extra data), and an optional tag the writer attached. Clients list the changes of a block, a chunk or an area, and read chunks as they were at a past revision or time.

## Turning it on

History is a table option, and tables start without it:

```text
TABLECREATE world block_bits 16 history on
TABLESET terrain history on history_max_age_ms 2592000000
```

| Option | Meaning | Values |
| --- | --- | --- |
| `history` | keep history | `on` or `off` (default); once on it cannot be turned off |
| `history_max_age_ms` | history older than this may be removed | `0` (default) keeps it |
| `history_max_chunk_bytes` | the most history one chunk keeps on disk | `0` (default) keeps it all |
| `history_max_tag_bytes` | the longest tag a write may carry | 1 to 255, default 32 |

History starts when it is enabled, at the table's `history_start` revision and `history_start_time_ms`: earlier changes are not events, and what they left is the `before` value of a block's first event. `TABLEINFO`, `USE` and `HELLO` report all of these lines (`off` and zeros without history). Enabling it marks the table with the `history` format feature, which a chunkdb build without it can open only read-only ([COMPATIBILITY.md](COMPATIBILITY.md)). There is no server flag for it. `HELLO` lists the `history` capability, `max_tag_bytes` (255) and `max_history_limit` (1024).

## Tags

`SET`, `UNSET`, `MSET`, `CHUNKPUT`, `CHUNKBATCH`, `XPUT` and `XDEL` take `TAG <hex>`: 1 to `history_max_tag_bytes` bytes written as pairs of hex digits, kept with every event of the write. `MSET` tags each of its writes; the forms are in [PROTOCOL.md](PROTOCOL.md) section 5. A tag on a table without history, or over the limit, fails with `INVALID_ARGUMENT` before anything changes. A write that changes nothing records nothing, its tag included.

## Listing changes

| Command | Events of |
| --- | --- |
| `HISTORY <x> <y> [options]` | one block |
| `CHUNKHISTORY <cx> <cy> [options]` | one chunk |
| `RANGEHISTORY <cx0> <cy0> <cx1> <cy1> [options]` | the chunks of a rectangle, at most 256 |

Options, each at most once: `LIMIT <n>` (1 to 1024, default 100), `ASC` or `DESC` (newest first by default), `AFTER <cursor>` and `BEFORE <cursor>` (exclusive), `SINCE <ms>` and `UNTIL <ms>` (commit time, inclusive), `TAG <hex>` (only that tag).

The reply is an array: first `END` or `CURSOR <cursor>`, then one item per event:

```text
<revision> <time_ms> <x> <y> <before> <after> <before_extra> <after_extra> <tag>
```

`before` and `after` are the block's bits as `GET` returns them, `-` when absent; extra data is `<bit_length>:<hex>`, `-` when none; the tag is hex, `-` when none. Events are ordered by revision, and the events of one mutation (a `CHUNKPUT` or `CHUNKBATCH` changes many blocks) by block. A cursor is `<revision>` or `<revision>:<block_index>`; pass the returned one as `AFTER` (ascending) or `BEFORE` (descending) for the next page. A page can be shorter than `LIMIT`, even empty, with a cursor: only `END` means the window is done.

```text
CHUNKHISTORY 0 0 ASC AFTER 1041          -> what changed since CHUNKVER said 1041
HISTORY 10 4 LIMIT 20 TAG 6a6f62         -> the last 20 changes job "job" made
```

A read sees exactly the mutations whose revisions were issued before it began, so a client polling with `AFTER` never skips a mutation that commits late in another chunk.

## Reading the past

`GET`, `CHUNKGET`, `CHUNKRANGE` and `CHUNKRADIUS` take `AT <revision>` or `AT TIME <ms>` last and reply as without it, with the chunks as they were: after every mutation at or below the revision, or per chunk after its mutations committed at or before the time. A revision at or above the next one, or a time not in the past, fails with `OUT_OF_RANGE`, so an answer never changes later.

Revisions are ordered across the table, commit times only within a chunk: across chunks a lower revision can carry a later time, so `AT TIME` reads each chunk at its own point.

## Retention

The limits apply when a chunk is checkpointed: its oldest history is removed, never its newest segment, and a chunk that is not written keeps what it has. A window that reaches below what a chunk keeps fails with `NOT_RETAINED start=<revision>`: oldest first at once, newest first after the page that returns what is kept. `AT` a point before history started or before what retention kept fails the same way.

## Costs and guarantees

- History is derived from the WAL when a chunk is checkpointed and is never lost or reordered by a crash: a crash before the history is written leaves the WAL to derive it again, one after leaves the events with the WAL frames they came from. The write path does not change, and events are visible to reads at once.
- A checkpoint of a table with history syncs the chunk's WAL, its history and its image in every durability mode, `relaxed` included: about five syncs per checkpoint. On macOS, where a durable sync is `F_FULLFSYNC`, that is 20 to 25 ms per checkpoint, on Linux about 2 ms; raise `checkpoint_updates` to checkpoint less often ([PERFORMANCE.md](PERFORMANCE.md)).
- On disk, history takes about 7 bytes per event for point writes of 16-bit blocks and about 2 per block for whole-chunk rewrites, plus keyframes (a chunk's state every 8 times its size of history).
- `chunkdb_verify` checks history: its records, keyframes, that every event is a change, and that history ends where the chunk's files continue. The format is in [STORAGE_FORMAT.md](STORAGE_FORMAT.md) section 9.
