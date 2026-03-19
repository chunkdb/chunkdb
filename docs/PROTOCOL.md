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

4. `SET <x> <y> <bits>`
- writes one block
- `<bits>` must contain only `0/1`
- `<bits>.length` must equal configured `block_bits`
- reply: `+OK`

5. `CHUNK <cx> <cy>`
- returns full chunk as bit text
- length:
  `chunk_width_blocks * chunk_height_blocks * block_bits`

6. `CHUNKBIN <cx> <cy>`
- returns full chunk as raw packed bytes
- preferred for large transfer volumes
- length:
  `ceil(chunk_width_blocks * chunk_height_blocks * block_bits / 8)`

7. `INFO`
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

8. `QUIT`
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

- Insecure endpoint: `chunk://token@host:4242/`
- TLS endpoint: `chunks://token@host:4242/`

Parsed components:
- secure flag
- token
- host
- port
- path

## 7. Example Session

Client request lines:

```text
AUTH mytoken
SET 0 0 1111000011110000
GET 0 0
CHUNK 0 0
CHUNKBIN 0 0
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
