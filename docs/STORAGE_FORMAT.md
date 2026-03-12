# Storage Format Specification

## 1. Hierarchy

Runtime hierarchy:
1. Large chunk
2. Regular chunk
3. Block bits

File system hierarchy:

- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.chk`
- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.wal`

Where:
- `(cx, cy)` are regular chunk coordinates
- `(lx, ly)` are large chunk coordinates derived from `(cx, cy)` and configured large chunk size

## 2. Packed Payload

For one regular chunk:

- Block count = `chunk_width_blocks * chunk_height_blocks`
- Payload bits = `block_count * block_bits`
- Payload bytes = `ceil(payload_bits / 8)`

Blocks are stored bit-packed without padding between blocks.

## 3. Binary Image Format (`.chk` and `.wal`)

All integer fields are little-endian.

Header (48 bytes):

1. `magic[8]`
- `.chk`: `CHKDATA1`
- `.wal`: `CHKWAL01`

2. `version` (`u16`) = `1`
3. `block_bits` (`u16`)
4. `chunk_width_blocks` (`u32`)
5. `chunk_height_blocks` (`u32`)
6. `chunk_x` (`i64` encoded as raw 64-bit)
7. `chunk_y` (`i64` encoded as raw 64-bit)
8. `payload_size` (`u32`)
9. `payload_crc32` (`u32`)
10. `write_timestamp_ms` (`u64`)

Then exactly `payload_size` raw payload bytes follow.

## 4. Write Path and Recovery

On each `SET`:
1. Update in-memory packed chunk payload.
2. Persist WAL image (`.wal`) atomically.
3. Persist data image (`.chk`) atomically.
4. Remove WAL.

On load:
1. If `.wal` exists and is valid, it is treated as latest committed state.
2. `.chk` is rebuilt from WAL payload.
3. WAL is removed.

## 5. Validation

On read, the loader validates:
- magic
- file version
- geometry fields vs current server configuration
- chunk coordinates
- payload size
- payload CRC32

Any mismatch raises an error and prevents silent corruption acceptance.
