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

## 3. Response Framing

1. Simple string:
`+<TEXT>\r\n`

2. Error:
`-ERR <CODE> <MESSAGE>\r\n`

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

15. `QUIT`
- reply: `+BYE`, then connection closes

## 5. Error Codes

- `AUTH_REQUIRED`
- `AUTH_FAILED`
- `UNKNOWN_COMMAND`
- `INVALID_ARGUMENT`
- `OUT_OF_RANGE`
- `BAD_REQUEST`
- `INTERNAL`

## 6. URI Format

- Insecure endpoint: `chunk://chunk-token@host:4242/`
- TLS endpoint: `chunks://chunk-token@host:4242/`

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
