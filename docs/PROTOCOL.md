# chunk Protocol Specification (protocol 2)

chunkdb 2.0 speaks protocol 2 only. A connection starts with `HELLO 2`; a
1.x client is refused at its first command (section 2).

## 1. Transport

- TCP stream, optionally TLS (`chunks://`).
- Line-based ASCII commands.
- Command line terminator: `\r\n` or `\n`.
- Arguments are space-delimited.
- Command names and option keywords are case-insensitive.
- A request line, terminator included, is at most `max_line_bytes`
  (`--max-line-bytes`, default 65536); a longer line gets `-ERR BAD_REQUEST`
  and the connection is closed. `CHUNKPUT` payload bytes are not part of the
  line.

## 2. Handshake and Authentication

`HELLO 2 [AUTH <token>] [TABLE <name>]` must be the first command on a
connection.

- Any other command before a successful `HELLO` gets
  `-ERR PROTOCOL expected HELLO 2` and the connection is closed. A 1.x client
  sees this at its first command (`AUTH`, `PING`, `GET`, ...).
- A protocol version other than `2` gets the same error and closes.
- `AUTH <token>` authenticates. When the server requires a token and `AUTH`
  is missing, the reply is `-ERR AUTH_REQUIRED`; a wrong token gets
  `-ERR AUTH_FAILED`. A `HELLO` that carries `AUTH` never gets
  `AUTH_REQUIRED`: a 1.x server that requires a token answers it that way, so
  clients can report such a server as not speaking protocol 2 (a 1.x server
  without a token answers `-ERR UNKNOWN_COMMAND`). When the server does not require a token, `AUTH` is
  accepted with any value.
- `TABLE <name>` selects a table. Without it the connection starts on
  `default`; if `default` does not exist, the connection has no table until
  `USE` selects one, even if a table named `default` is created later. An
  unknown table gets `-ERR NO_TABLE`.
- Options may come in either order, each at most once; anything else gets
  `-ERR INVALID_ARGUMENT`.
- After `AUTH_REQUIRED`, `AUTH_FAILED`, `NO_TABLE` or `INVALID_ARGUMENT` the
  connection stays open and not greeted: the client may send `HELLO` again.
- A second `HELLO` after a successful one gets
  `-ERR PROTOCOL HELLO was already sent on this connection`; the connection
  stays open.
- Success reply: bulk text of `key=value` lines:
  - `protocol` (`2`)
  - `server_version`
  - `capabilities` (comma-separated; currently `zrle`)
  - `max_line_bytes`
  - `max_area_chunks` (`CHUNKRANGE` / `CHUNKRADIUS` chunk limit, 256)
  - `max_response_bytes` (`CHUNKRANGE` / `CHUNKRADIUS` response cap, 67108864)
  - `max_scan_limit` (`CHUNKSCAN` limit, 1024)
  - `max_batch_ops` (`CHUNKBATCH` operation limit, 1024)
  - when the connection has a table: the `TABLEINFO` lines of that table
    (name, store id, geometry, options; command 22)

Failed authentication:

- Failed auth attempts are tracked per connection. After
  `max_auth_failures`, the server closes the connection after sending the
  error.
- Failed auth attempts are also tracked per remote source when the server can
  identify it. IPv6 sources are bucketed by their /64 prefix so a single
  allocation cannot multiply tracked entries; IPv4 sources are tracked per
  address.
- After repeated failures from one source, the server may add a small delay
  and temporarily reject new auth attempts from that source.
- The tracking table is hard-bounded (4096 sources); when it is full, the
  least-recently-updated entry is evicted, so an address spray cannot grow
  server memory.

## 3. Response Framing

1. Simple string:
`+<TEXT>\r\n`

2. Error:
`-ERR <CODE> <MESSAGE>\r\n`

When the plain TCP pending-client queue is full, the server returns
`-ERR BUSY pending client queue full` and closes the connection.

3. Bulk payload:
`$<LEN>\r\n<PAYLOAD>\r\n`

`<PAYLOAD>` can be text or binary bytes.

4. Null:
`$-1\r\n`

No value: an unset block (`GET`, an `MGET` item).

5. Array:
`*<COUNT>\r\n` followed by `<COUNT>` bulk payloads or nulls.

## 4. Chunk Data

A chunk's data is binary:

- payload: the packed block bits, `payload_bytes =
  ceil(chunk_width_blocks * chunk_height_blocks * block_bits / 8)`
- state: the payload followed by the presence bitmap, one bit per block,
  `presence_bytes = ceil(chunk_width_blocks * chunk_height_blocks / 8)`;
  bit `i` tells whether block `i` is explicitly present
- unset blocks are zero in the payload; padding bits past the used range of
  the last payload byte and the last presence byte are zero
- `ZRLE` encodes the bytes with the `zrle` codec (`STORAGE_FORMAT.md`):
  `[0x01][u32le uncompressed_size][tokens...]`. Clients must bound
  decompression by the expected size from the geometry and reject a payload
  that declares or produces a different size. The server's encoding is at
  most 11 bytes larger than the data: data that runs of zeros would expand is
  sent as one literal token

Sizes are those of the selected table's geometry, reported by `HELLO`, `USE`
and `TABLEINFO`.

## 5. Commands

A data directory holds named tables, each with its own geometry
(`block_bits`, chunk and large-chunk sizes) and options. The block and chunk
commands (1-15) work on the connection's selected table. If that table is
dropped, they fail with `-ERR NO_TABLE` until `USE` selects a table, even if
a table of the same name is created again: the new table may have another
geometry. A connection without a table gets `-ERR NO_TABLE` from them too.

1. `GET <x> <y>`
- returns one block as bit text (`LEN == block_bits`)
- an unset block returns null (`$-1`)

2. `SET <x> <y> <bits>`
- writes one block
- `<bits>` must contain only `0/1`
- `<bits>.length` must equal the table's `block_bits`
- reply: `+OK`

3. `UNSET <x> <y>`
- clears explicit block presence; a later `GET` returns null
- reply: `+OK`

4. `MGET <x1> <y1> [<x2> <y2> ...]`
- reads multiple blocks in one command
- reply: array with one item per requested block in request order: bit text,
  or null for an unset block

5. `MSET <x1> <y1> <bits1> [<x2> <y2> <bits2> ...]`
- writes multiple blocks in one command; every item is validated first
  (arity, bit-string length, `0/1` alphabet), then items apply strictly in
  request order
- `MSET` is **not atomic across its items**: items apply as independent
  per-block writes. If item *k* fails, items *1..k-1* remain applied and the
  command returns a single error that does not identify the applied prefix
- each individual item is all-or-nothing: a failed item is fully rolled
  back and never becomes durable
- for an atomic multi-block update within one chunk, use `CHUNKBATCH`
- reply: `+OK`

6. `CHUNKGET <cx> <cy> [STATE] [ZRLE]`
- returns the chunk as bulk bytes: the payload, or with `STATE` the state
  (section 4); with `ZRLE`, zrle-encoded
- an absent chunk returns zero bytes of the full size; use `CHUNKEXISTS` to
  tell it from an explicit all-zero chunk
- options may come in either order

7. `CHUNKPUT <cx> <cy> [STATE] [ZRLE] [IF <version>] <length>`
- replaces the whole chunk. The request line is followed by exactly
  `<length>` bytes and then an empty line (`\r\n` or `\n`)
- without `STATE`, the bytes are the payload and every block becomes
  explicitly present, including an all-zero payload
- with `STATE`, the bytes are the state; payload bits of absent blocks are
  stored as zero
- with `ZRLE`, the bytes are the zrle encoding of the payload or state; they
  must decode to exactly its size. `<length>` may be at most that size plus
  16 bytes, which covers any encoding the server itself produces, so a
  `CHUNKGET ... ZRLE` reply can always be written back. A client encoder that
  expands data more must send that chunk uncompressed
- padding bits are ignored and stored as zero
- `IF <version>` applies the write only when `<version>` equals the chunk's
  current version (`CHUNKVER`); otherwise the reply is
  `-ERR VERSION_MISMATCH current=<version>` and the chunk is unchanged. A
  rejected write never becomes visible later, including after restart
- reply: bulk text with the chunk's version after the write. A write that
  does not change the chunk leaves the version unchanged
- framing:
  - a `<length>` that fits the bound but is not the exact size (without
    `ZRLE`), or bytes that are not valid zrle, are read and discarded; the
    command fails with `INVALID_ARGUMENT` and the connection stays usable
  - the server refuses to read the bytes and closes the connection when the
    request cannot be framed safely: a header that does not parse
    (`INVALID_ARGUMENT`), a `<length>` above the bound (`BAD_REQUEST`), a
    missing empty line after the bytes (`BAD_REQUEST`), `CHUNKPUT` before
    `HELLO` (`PROTOCOL`), or no table to size it by (`NO_TABLE`)
  - bytes for a dropped table are read with that table's sizes and then
    refused with `NO_TABLE`
- an error reply means nothing was applied. The write is crash-atomic for
  every geometry: it is logged as one WAL frame

8. `CHUNKEXISTS <cx> <cy>`
- reply: `+1` when any block in the chunk is explicitly present
- reply: `+0` when the chunk is absent/unset

9. `CHUNKVER <cx> <cy>`
- reply: bulk text with the chunk's current version, an opaque unsigned
  64-bit decimal token
- versions change on every content mutation of the chunk and are persisted
  with it: eviction and restart leave the version unchanged, so a token read
  before either still matches unchanged content
- tokens come from a store-wide monotonic clock whose ceiling is persisted
  (fsynced) before use, so on a read-write store a version obtained before a
  mutation can never match one issued afterwards; this is a deterministic
  guarantee, not a probabilistic one
- if a valid initialized marker proves token exposure, a missing, unreadable,
  uninspectable, or invalid clock makes read-write startup fail closed rather
  than reset it; see `STORAGE_FORMAT.md`
- a mutation that does not change chunk content leaves the version unchanged

10. `CHUNKBATCH <cx> <cy> [IF <version>] <op> ...`
- atomic batch of block operations limited to one chunk; `<op>` is
  `SET <x> <y> <bits>` or `UNSET <x> <y>` repeated up to 1024 times
- all block coordinates must lie inside chunk `(cx, cy)`
- without `IF`, the batch applies unconditionally
- the batch applies completely or not at all: validation failure, version
  mismatch, or a WAL/checkpoint write failure leaves the chunk unchanged, and
  a rejected batch never becomes visible later, including after a crash-style
  restart or a subsequent `WALFLUSH`
- crash atomicity holds for every geometry: the resulting chunk state is
  logged as one WAL frame, which replay applies completely or not at all
- the request is one text line bounded by `max_line_bytes`
- success reply: bulk text with the chunk's version after the batch
- mismatch reply: `-ERR VERSION_MISMATCH current=<version>`
- cross-chunk atomic transactions are not supported

11. `CHUNKSCAN <limit> [<cursor_cx> <cursor_cy>]`
- enumerates populated chunks (chunks with at least one explicitly present
  block); absent and emptied chunks are never listed
- each chunk's populated state is evaluated atomically per chunk at scan
  time; the scan as a whole is not a global snapshot
- deterministic ordering: ascending `cx`, then ascending `cy`
- `limit` must be between 1 and 1024
- reply: array of bulk strings; the first item is either `END` (no more
  results) or `CURSOR <cx> <cy>` (pass these coordinates as the cursor of the
  next `CHUNKSCAN` call); remaining items are `<cx> <cy>` pairs
- scanning does not load absent chunks into the server cache, with one
  bounded exception: after several contended read attempts on one chunk the
  server falls back to its authoritative cache path, which caches that chunk
  to preserve read-your-writes consistency

12. `CHUNKRANGE <cx0> <cy0> <cx1> <cy1> [STATE] [ZRLE]`
- bounded rectangular multi-chunk read for world streaming
- requires `cx0 <= cx1`, `cy0 <= cy1`, and at most 256 chunks per request;
  the corner coordinates may be anywhere in the signed 64-bit domain,
  including `INT64_MIN`/`INT64_MAX`, and are handled without overflow
- reply: array with two items per populated chunk: bulk text `<cx> <cy>`,
  then the chunk's bytes exactly as `CHUNKGET` with the same options returns
  them. Chunks are ordered by ascending `cx` then `cy`; absent chunks are
  omitted
- the response is additionally capped at 64 MiB of chunk state; a request
  whose populated chunks would exceed it fails with `-ERR OUT_OF_RANGE`
  instead of allocating the response, so the chunk-count limit is not the
  only bound
- absent chunks probed by a range read are not inserted into the cache,
  except through the same rare contention fallback documented for
  `CHUNKSCAN`
- each chunk's state is read with per-chunk consistency: a value acknowledged
  by a concurrent writer before the read reached that chunk is always
  observed, even when the chunk was not yet cached

13. `CHUNKRADIUS <cx> <cy> <radius_chunks> [STATE] [ZRLE]`
- bounded radius-oriented world read: returns the populated chunks whose
  chunk coordinate lies within Euclidean distance `radius_chunks` of
  `(cx, cy)` (i.e. `dx*dx + dy*dy <= radius_chunks*radius_chunks`)
- `radius_chunks` is a non-negative integer; the covered disc must contain at
  most 256 chunks, and the response is capped exactly like `CHUNKRANGE`
- reply: same shape as `CHUNKRANGE`
- shares `CHUNKRANGE`'s cache and per-chunk consistency behavior

14. `INFO`
- returns key/value lines in bulk payload for the selected table:
  - `table` (the selected table)
  - `tables` (number of tables in the data directory)
  - `access_mode`
  - `chunk_lock_mode` (`serial-mutex` or `shared-mutex`, depending on build/runtime lock path)
  - `loaded_chunks`
  - `evictions`
  - `checkpoints`
  - `wal_batch_flushes`
  - `unique_loaded_chunks`
  - `open_wal_streams` (current number of open WAL append streams)
  - `eviction_snapshot_builds`
  - `eviction_probes`
  - `eviction_no_progress_cycles`
  - `eviction_forced_wal_flushes`
  - `eviction_forced_wal_flushes_with_data`
  - `eviction_forced_wal_flushes_empty_batch`
  - `eviction_recency_skips`
  - `empty_chunk_gcs`
  - `wal_barriers`, `wal_barrier_full_syncs`
  - `compressed_checkpoint_images`
  - `background_checkpoints`, `background_checkpoint_failures`,
    `background_queue_full_inline`, `background_queue_depth`
- geometry and options are reported by `HELLO`, `USE` and `TABLEINFO`; the
  server version by `HELLO`

15. `WALFLUSH`
- explicit global durability barrier: on `+OK`, every write acknowledged
  before the server received `WALFLUSH` is durable on stable storage, in every
  table, including tables in `relaxed` durability mode
- writes acknowledged after the barrier started may or may not be covered
- failures are returned to the caller as errors; a failed barrier makes no
  durability claim and should be retried

16. `PING`
- reply: `+PONG`

17. `QUIT`
- reply: `+BYE`, then connection closes

18. `METRICS`
- reply: bulk text in the Prometheus text exposition format
- includes per-command-class latency histograms (seconds), command/error
  counters, auth failure counters, cache/WAL/checkpoint/eviction gauges and
  counters, active/pending connection gauges, and server-side failure
  counters for outcomes that never reach command execution
  (`chunkdb_connections_rejected_total` for admission-control rejections and
  `chunkdb_malformed_requests_total` for framing failures); label cardinality
  is fixed and bounded
- cache, WAL, checkpoint and eviction counters are summed over all tables;
  `chunkdb_loaded_chunks` counts the cache all tables share
- there is no native HTTP scrape endpoint; scraping requires a small adapter
  that issues `METRICS` (see `docs/KNOWN_LIMITATIONS.md`)

19. `TABLES`
- reply: array of bulk strings, the table names in ascending order

20. `USE <name>`
- selects the table for this connection
- reply: the same bulk as `TABLEINFO <name>`
- unknown table: `-ERR NO_TABLE`, and the connection keeps its table

21. `TABLECREATE <name> block_bits <n> [<key> <value> ...]`
- creates a table; keys are the `TABLEINFO` geometry and option names
  (case-insensitive), each at most once
- `block_bits` is required; omitted geometry keys take 16x16 blocks per chunk
  and 8x8 chunks per large chunk; omitted options take the server's defaults
  (`docs/SERVER_FLAGS.md`)
- names: 1-64 characters from `a-z`, `0-9`, `_`, `-`, starting with a letter
  or digit, and not `con`, `prn`, `aux`, `nul`, `com0`-`com9`, `lpt0`-`lpt9`
- crash-atomic: after a crash the table exists completely or not at all
- reply: `+OK`; an existing name: `-ERR TABLE_EXISTS`; an invalid name,
  geometry or option: `-ERR INVALID_ARGUMENT`
- example: `TABLECREATE terrain block_bits 4 chunk_width_blocks 32
  chunk_height_blocks 32 durability_mode fsync-wal`

22. `TABLEINFO <name>`
- reply: bulk text of `key=value` lines:
  - `table`
  - `store_id` (32 hex digits; a new table of the same name gets a new id)
  - `block_bits`, `chunk_width_blocks`, `chunk_height_blocks`,
    `large_chunk_width_chunks`, `large_chunk_height_chunks` (geometry)
  - `durability_mode`, `checkpoint_updates`, `checkpoint_wal_bytes`,
    `wal_group_commit_updates`, `checkpoint_compression` (options)
- unknown table: `-ERR NO_TABLE`

23. `TABLESET <name> <option> <value> [<option> <value> ...]`
- changes options: `durability_mode` (`relaxed`, `fsync-wal`,
  `fsync-checkpoint`), `checkpoint_updates`, `checkpoint_wal_bytes`,
  `wal_group_commit_updates` (positive integers), `checkpoint_compression`
  (`none`, `zrle`)
- geometry is fixed when a table is created; a geometry key fails with
  `-ERR INVALID_ARGUMENT`
- only the named options change
- waits for commands running on the table, persists the options atomically and
  reopens the table; the table's chunks leave the cache. New durability
  settings apply to writes acknowledged after the reply. If the table cannot
  be reopened, the command fails and the table is unavailable (`NO_TABLE`)
  until the server restarts
- reply: `+OK`; unknown table: `-ERR NO_TABLE`

24. `TABLEDROP <name>`
- deletes a table and its data; irreversible
- waits for commands running on the table; connections that selected it get
  `-ERR NO_TABLE` from then on
- crash-atomic: after a crash the table exists completely or not at all
- reply: `+OK`; unknown table: `-ERR NO_TABLE`
- one auth token grants every command on every table, including `TABLEDROP`

## 6. Error Codes

- `PROTOCOL` (no `HELLO 2` yet, an unsupported protocol version, or a second
  `HELLO`)
- `AUTH_REQUIRED`
- `AUTH_FAILED`
- `UNKNOWN_COMMAND`
- `INVALID_ARGUMENT`
- `OUT_OF_RANGE`
- `VERSION_MISMATCH`
- `BAD_REQUEST`
- `BUSY`
- `NO_TABLE` (unknown or dropped table)
- `TABLE_EXISTS`
- `INTERNAL`

## 7. URI Format

- Insecure endpoint: `chunk://chunk-token@host:4242/`
- TLS endpoint: `chunks://chunk-token@host:4242/`
- URI tokens are development-only. For deployments, start the server with `--token-file` or `CHUNKDB_TOKEN` and keep tokens out of command lines and logs.

Parsed components:
- secure flag
- token (sent as `HELLO 2 AUTH <token>`)
- host
- port
- path: a table name (`chunk://host:4242/terrain`), sent as
  `HELLO 2 TABLE <name>`. An empty path (`/`) means `default`.

## 8. Example Session

Client request lines (`<state bytes>` stands for the raw chunk state of
`<length>` bytes):

```text
HELLO 2 AUTH chunk-token
SET 0 0 1111000011110000
GET 0 0
GET 1 0
CHUNKPUT 1 1 STATE <length>
<state bytes>

CHUNKGET 1 1 STATE
CHUNKVER 1 1
QUIT
```

Representative framed responses:

```text
$<LEN>
protocol=2
server_version=...
...
+OK
$16
1111000011110000
$-1
$<LEN>
<version>
$<LEN>
<binary state bytes>
$<LEN>
<version>
+BYE
```
