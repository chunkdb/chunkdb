# chunk Protocol Specification (v1)

## 1. Transport

- TCP stream.
- Line-based ASCII commands.
- Command line terminator: `\r\n` or `\n`.
- Arguments are space-delimited.
- Command names are case-insensitive.

## 2. Authentication

- If enabled, client must run `AUTH <token>` before data commands.
- Failed auth attempts are tracked per connection.
- After `max_auth_failures`, server closes the connection after sending error.
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

## 4. Commands

1. `PING`
- reply: `+PONG`

2. `AUTH <token>`
- reply: `+OK` or `-ERR AUTH_FAILED ...`

3. `GET <x> <y>`
- returns one block as bit text (`LEN == block_bits`)
- unset blocks are returned as zero bits
- use `EXISTS` to distinguish unset from an explicit all-zero payload

4. `EXISTS <x> <y>`
- reply: `+1` when the block is explicitly present
- reply: `+0` when the block is unset

5. `SET <x> <y> <bits>`
- writes one block
- `<bits>` must contain only `0/1`
- `<bits>.length` must equal configured `block_bits`
- reply: `+OK`

6. `UNSET <x> <y>`
- clears explicit block presence
- later `GET` still returns zero bits
- reply: `+OK`

7. `CHUNK <cx> <cy>`
- returns full chunk as bit text
- unset blocks are represented as zero bits
- absent chunks are returned as zero bits
- length:
  `chunk_width_blocks * chunk_height_blocks * block_bits`

8. `CHUNK <cx> <cy> STATE`
- returns exact chunk state as bulk text:
  `<payload_bits>|<presence_bits>`
- `payload_bits.length == chunk_width_blocks * chunk_height_blocks * block_bits`
- `presence_bits.length == chunk_width_blocks * chunk_height_blocks`
- `presence_bits[i]` describes whether block `i` is explicitly present
- unset blocks are still zero-filled in `payload_bits`

9. `CHUNKEXISTS <cx> <cy>`
- reply: `+1` when any block in the chunk is explicitly present
- reply: `+0` when the chunk is absent/unset

10. `CHUNKSET <cx> <cy> <bits>`
- replaces the full chunk payload
- marks the whole chunk explicitly present, including an all-zero payload
- `<bits>` must contain only `0/1`
- `<bits>.length` must equal `chunk_width_blocks * chunk_height_blocks * block_bits`
- reply: `+OK`

11. `CHUNKSET <cx> <cy> STATE <payload_bits>|<presence_bits>`
- replaces the full chunk payload and per-block presence bitmap
- payload bits for absent blocks are canonicalized to zero before storage
- both halves must contain only `0/1`
- `payload_bits.length == chunk_width_blocks * chunk_height_blocks * block_bits`
- `presence_bits.length == chunk_width_blocks * chunk_height_blocks`
- reply: `+OK`

12. `CHUNKBIN <cx> <cy>`
- returns full chunk as raw packed bytes
- unset blocks are represented as zero bytes in the packed payload
- absent chunks are returned as zero bytes
- preferred for large transfer volumes
- length:
  `ceil(chunk_width_blocks * chunk_height_blocks * block_bits / 8)`

13. `CHUNKBIN <cx> <cy> STATE`
- returns exact chunk state as raw bytes:
  `[payload_bytes][presence_bytes]`
- first `payload_bytes` are the legacy packed payload bytes
- trailing `presence_bytes` are the packed presence bitmap bytes
- length:
  `ceil(chunk_width_blocks * chunk_height_blocks * block_bits / 8) + ceil(chunk_width_blocks * chunk_height_blocks / 8)`

14. `INFO`
- returns key/value lines in bulk payload
- includes static config and runtime counters:
  - `chunkdb_version`
  - `block_bits`
  - `chunk_width_blocks`
  - `chunk_height_blocks`
  - `large_chunk_width_chunks`
  - `large_chunk_height_chunks`
  - `durability_mode`
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
  - `checkpoint_compression` (`none` or `zrle`)

15. `QUIT`
- reply: `+BYE`, then connection closes

16. `CHUNKSCAN <limit> [<cursor_cx> <cursor_cy>]`
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

17. `CHUNKRANGE <cx0> <cy0> <cx1> <cy1>`
- bounded rectangular multi-chunk read for world streaming
- requires `cx0 <= cx1`, `cy0 <= cy1`, and at most 256 chunks per request;
  the corner coordinates may be anywhere in the signed 64-bit domain,
  including `INT64_MIN`/`INT64_MAX`, and are handled without overflow
- reply: array of bulk strings `<cx> <cy> <payload_bits>|<presence_bits>`,
  one per populated chunk, ordered by ascending `cx` then `cy`; absent
  chunks are omitted
- the response body is additionally capped at 64 MiB; a request whose
  populated chunks would exceed it fails with `-ERR OUT_OF_RANGE` instead of
  allocating the response, so the chunk-count limit is not the only bound
- negative coordinates and exact per-block presence state are preserved
- absent chunks probed by a range read are not inserted into the cache,
  except through the same rare contention fallback documented for
  `CHUNKSCAN`
- each chunk's state is read with per-chunk consistency: a value acknowledged
  by a concurrent writer before the read reached that chunk is always
  observed, even when the chunk was not yet cached

17a. `CHUNKRADIUS <cx> <cy> <radius_chunks>`
- bounded radius-oriented world read: returns the populated chunks whose
  chunk coordinate lies within Euclidean distance `radius_chunks` of
  `(cx, cy)` (i.e. `dx*dx + dy*dy <= radius_chunks*radius_chunks`)
- `radius_chunks` is a non-negative integer; the covered disc must contain at
  most 256 chunks, and the response is capped at 64 MiB exactly like
  `CHUNKRANGE`
- reply: same shape as `CHUNKRANGE` (`<cx> <cy> <payload_bits>|<presence_bits>`
  per populated chunk, ascending `cx` then `cy`); absent chunks are omitted
- shares `CHUNKRANGE`'s cache and per-chunk consistency behavior

18. `CHUNKVER <cx> <cy>`
- reply: bulk text with the chunk's current version, an opaque unsigned
  64-bit decimal token
- versions change on every content mutation of the chunk and are persisted
  with it (server 2.x, storage format v2): eviction and restart leave the
  version unchanged, so a token read before either still matches unchanged
  content. Against a 1.x server, or for a chunk whose data was last written by
  1.x, the version also changes whenever the chunk is (re)loaded
- tokens come from a store-wide monotonic clock whose ceiling is persisted
  (fsynced) before use, so on a read-write store a version obtained before a
  mutation can never match one issued afterwards; this is a deterministic
  guarantee, not a probabilistic one
- stable-v1 stores without version bookkeeping migrate automatically; if a
  valid initialized marker proves token exposure, a missing, unreadable,
  uninspectable, or invalid clock makes read-write startup fail closed rather
  than reset it; see `STORAGE_FORMAT.md`
- a mutation that does not change chunk content leaves the version unchanged

19. `CHUNKCAS <cx> <cy> <version> STATE <payload_bits>|<presence_bits>`
- conditional full-state replace: applies only when `<version>` equals the
  chunk's current version
- like `CHUNKBATCH`, the replace is all-or-nothing and a rejected replace
  never becomes visible later (including after restart). The new state is
  logged as one WAL frame, so crash atomicity holds for every geometry
  (server 2.x, storage format v2); a 1.x server instead rejected geometries
  whose chunk state exceeded one 65535-byte WAL record with
  `-ERR INVALID_ARGUMENT`
- the request is one text line, so the whole line (`<payload_bits>|<presence_bits>`
  included, terminator counted) must fit in `max_line_bytes`
  (`--max-line-bytes`, default 65536); a longer line gets `-ERR BAD_REQUEST`
  and the connection is closed
- success reply: bulk text with the new version
- mismatch reply: `-ERR VERSION_MISMATCH current=<version>`; state unchanged

20. `CHUNKBATCH <cx> <cy> <version|-> <op> ...`
- atomic batch of block operations limited to one chunk; `<op>` is
  `SET <x> <y> <bits>` or `UNSET <x> <y>` repeated up to 1024 times
- all block coordinates must lie inside chunk `(cx, cy)`
- pass `-` instead of a version to apply unconditionally
- the batch applies completely or not at all: validation failure, version
  mismatch, or a WAL/checkpoint write failure leaves the chunk unchanged, and
  a rejected batch never becomes visible later, including after a crash-style
  restart or a subsequent `WALFLUSH`
- crash atomicity holds for every geometry (server 2.x, storage format v2):
  the resulting chunk state is logged as one WAL frame, which replay applies
  completely or not at all. A 1.x server instead rejected geometries whose
  chunk state exceeded one 65535-byte WAL record with `-ERR INVALID_ARGUMENT`
- the request is one text line and the whole line is bounded by
  `max_line_bytes` (`--max-line-bytes`, default 65536, terminator counted)
- success reply: bulk text with the new version
- mismatch reply: `-ERR VERSION_MISMATCH current=<version>`
- cross-chunk atomic transactions are not supported

21. `CHUNKBINC <cx> <cy> [STATE]`
- like `CHUNKBIN`, but the payload is compressed with the `zrle` codec
  (see `STORAGE_FORMAT.md` for the codec definition)
- reply: bulk bytes `[0x01][u32le uncompressed_size][tokens...]`
- clients must bound decompression by the geometry-derived expected size and
  reject any payload that declares or produces a different size

22. `WALFLUSH`
- explicit global durability barrier: on `+OK`, every write acknowledged
  before the server received `WALFLUSH` is durable on stable storage,
  including in `relaxed` durability mode
- writes acknowledged after the barrier started may or may not be covered
- failures are returned to the caller as errors; a failed barrier makes no
  durability claim and should be retried

23. `METRICS`
- reply: bulk text in the Prometheus text exposition format
- includes per-command-class latency histograms (seconds), command/error
  counters, auth failure counters, cache/WAL/checkpoint/eviction gauges and
  counters, active/pending connection gauges, and server-side failure
  counters for outcomes that never reach command execution
  (`chunkdb_connections_rejected_total` for admission-control rejections and
  `chunkdb_malformed_requests_total` for framing failures); label cardinality
  is fixed and bounded
- there is no native HTTP scrape endpoint; scraping requires a small adapter
  that issues `METRICS` (see `docs/KNOWN_LIMITATIONS.md`)
- requires authentication exactly like other data commands

24. `MSET <x1> <y1> <bits1> [<x2> <y2> <bits2> ...]`
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

25. `MGET <x1> <y1> [<x2> <y2> ...]`
- reads multiple blocks in one command
- reply: array of bulk strings, one `<bits>` per requested block in request
  order; unset blocks return zero bits exactly like `GET`

26. `CHUNKSETBIN <cx> <cy> [STATE] <payload_length>`
- binary counterpart of `CHUNKSET` / `CHUNKSET STATE`: the request line is
  followed by exactly `<payload_length>` raw bytes and then an empty line
  (`\r\n` or `\n`); the bytes are not subject to `max_line_bytes`
- without `STATE`, the payload is the packed chunk payload exactly as
  `CHUNKBIN` returns it (`ceil(chunk_width_blocks * chunk_height_blocks *
  block_bits / 8)` bytes) and every block becomes explicitly present
- with `STATE`, the payload is `[payload_bytes][presence_bytes]` exactly as
  `CHUNKBIN ... STATE` returns it; payload bits of absent blocks are
  canonicalized to zero before storage
- padding bits past the used range of the last payload byte and the last
  presence byte are ignored and stored as zero
- `<payload_length>` must equal the exact size for the chosen form. A shorter
  or longer length that still fits the geometry's chunk state size is read
  and discarded, the command fails with `INVALID_ARGUMENT`, and the
  connection stays usable
- the server refuses to buffer the payload and closes the connection when the
  request cannot be framed safely: a malformed header (`INVALID_ARGUMENT`), a
  length above the geometry's chunk state size (`BAD_REQUEST`), a missing
  empty line after the payload (`BAD_REQUEST`), or an unauthenticated session
  when auth is enabled (`AUTH_REQUIRED`)
- durability and atomicity match `CHUNKSET`: an error reply means nothing was
  applied, and the write is crash-atomic for every geometry (server 2.x,
  storage format v2) because the replace is logged as one WAL frame
- reply: `+OK`

## 5. Error Codes

- `AUTH_REQUIRED`
- `AUTH_FAILED`
- `UNKNOWN_COMMAND`
- `INVALID_ARGUMENT`
- `OUT_OF_RANGE`
- `VERSION_MISMATCH`
- `BAD_REQUEST`
- `INTERNAL`

## 6. URI Format

- Insecure endpoint: `chunk://chunk-token@host:4242/`
- TLS endpoint: `chunks://chunk-token@host:4242/`
- URI tokens are development-only. For deployments, start the server with `--token-file` or `CHUNKDB_TOKEN` and keep tokens out of command lines and logs.

Parsed components:
- secure flag
- token
- host
- port
- path

## 7. Example Session

Client request lines:

```text
AUTH chunk-token
EXISTS 0 0
SET 0 0 1111000011110000
GET 0 0
UNSET 0 0
CHUNKEXISTS 0 0
CHUNKSET 0 0 <full_chunk_bits>
CHUNK 0 0
CHUNK 0 0 STATE
CHUNKSET 0 0 STATE <payload_bits>|<presence_bits>
CHUNKBIN 0 0
CHUNKBIN 0 0 STATE
INFO
QUIT
```

Representative framed responses:

```text
+OK
$16
1111000011110000
$<LEN>
<binary payload bytes>
+BYE
```
