# chunk Protocol Specification (v1)

## 1. Transport

- TCP stream.
- UTF-8/ASCII text commands.
- Each command is one line terminated by `\r\n` or `\n`.
- Command and arguments are separated by spaces.
- Commands are case-insensitive (`set`, `SET`, `SeT` are equivalent).

## 2. Authentication

- If auth is enabled, clients must call `AUTH <token>` before data commands.
- Failed auth increments a per-connection failure counter.
- After `max_auth_failures`, server sends error and closes the connection.

## 3. Response Format (strict)

`chunk` uses a compact RESP-like format:

1. Simple string:
`+<TEXT>\r\n`

2. Error:
`-ERR <CODE> <MESSAGE>\r\n`

3. Bulk payload:
`$<LEN>\r\n<PAYLOAD>\r\n`

Notes:
- `<LEN>` is decimal character count of `<PAYLOAD>`.
- Bit payloads are text `0/1` characters.

## 4. Commands

1. `PING`
- Request: `PING`
- Response: `+PONG`

2. `AUTH <token>`
- Request: `AUTH mytoken`
- Response: `+OK` or `-ERR AUTH_FAILED ...`

3. `GET <x> <y>`
- Reads one block at world block coordinates.
- Response: bulk payload of exactly `block_bits` chars.

4. `SET <x> <y> <bits>`
- Writes one block.
- `<bits>` must contain only `0/1` and length == `block_bits`.
- Response: `+OK`

5. `CHUNK <cx> <cy>`
- Reads one regular chunk by chunk coordinates.
- Response: bulk payload of exactly
  `chunk_width_blocks * chunk_height_blocks * block_bits` chars.

6. `INFO`
- Returns key-value info block in bulk format.

7. `QUIT`
- Response: `+BYE`, then connection closes.

## 5. Error Codes

- `AUTH_REQUIRED`
- `AUTH_FAILED`
- `UNKNOWN_COMMAND`
- `INVALID_ARGUMENT`
- `OUT_OF_RANGE`
- `BAD_REQUEST`
- `INTERNAL`

## 6. URI Format

Client endpoints use:

- Insecure: `chunk://token@host:6752/`
- TLS: `chunks://token@host:6752/`

The URI parser supports both schemes and extracts:
- secure flag
- token
- host
- port
- path
