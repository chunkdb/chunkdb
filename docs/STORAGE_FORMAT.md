# Storage Format Specification (fs_split_v1 backend)

## 1. Hierarchy

Runtime hierarchy:
1. large chunk
2. regular chunk
3. block bitfield

Filesystem mapping:
- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.chk`
- `data_dir/L_<lx>_<ly>/C_<cx>_<cy>.wal`

Where:
- `(cx, cy)` = regular chunk coordinates
- `(lx, ly)` = large chunk coordinates derived from configured large-chunk dimensions

## 2. Packed Payload

Per regular chunk:
- block_count = `chunk_width_blocks * chunk_height_blocks`
- payload_bits = `block_count * block_bits`
- payload_bytes = `ceil(payload_bits / 8)`

Payload is tightly bit-packed (no block padding).

## 3. `.chk` Data Image Format

All integers are little-endian.

Header (`48` bytes):
1. `magic[8]` = `CHKDATA1`
2. `version` (`u16`) = `1`
3. `block_bits` (`u16`)
4. `chunk_width_blocks` (`u32`)
5. `chunk_height_blocks` (`u32`)
6. `chunk_x` (`i64` raw 64-bit)
7. `chunk_y` (`i64` raw 64-bit)
8. `payload_size` (`u32`)
9. `payload_crc32` (`u32`)
10. `write_timestamp_ms` (`u64`)

Body:
- exactly `payload_size` bytes of packed payload

## 4. `.wal` Delta Log Format

WAL header (`36` bytes):
1. `magic[8]` = `CHKWAL02`
2. `wal_version` (`u16`) = `2`
3. `block_bits` (`u16`)
4. `chunk_width_blocks` (`u32`)
5. `chunk_height_blocks` (`u32`)
6. `chunk_x` (`i64`)
7. `chunk_y` (`i64`)

Then append-only delta records:

Record header (`14` bytes):
1. `record_magic[4]` = `DLT1`
2. `byte_offset` (`u32`)
3. `data_size` (`u16`)
4. `record_crc32` (`u32`)

Record body:
- `data_size` bytes to overwrite at `payload[byte_offset:byte_offset+data_size)`

## 5. Write Path

For each `SET`:
1. update touched bytes in in-memory payload
2. append delta record to `.wal`
3. optionally fsync WAL (depends on durability mode)
4. checkpoint `.chk` when thresholds hit:
  - `checkpoint_update_interval`
  - `checkpoint_wal_bytes`

Checkpoint writes full `.chk` atomically and removes `.wal`.

## 6. Recovery Path

On load:
1. load `.chk` if exists (or zero payload if absent)
2. if `.wal` exists, validate header and replay records in order
3. write checkpointed `.chk` from recovered payload
4. remove `.wal`

Trailing partial WAL record (e.g. torn append) is ignored.
Invalid interior records fail load.

## 7. Validation and Corruption Handling

`.chk` validation checks:
- magic
- version
- geometry fields
- chunk coordinates
- payload size
- payload CRC32

`.wal` validation checks:
- magic
- version
- geometry fields
- chunk coordinates
- per-record magic
- per-record bounds
- per-record CRC32

## 8. Durability Notes

Durability guarantees depend on configured mode (`relaxed`, `fsync-wal`, `fsync-checkpoint`).
See [docs/CONCURRENCY.md](CONCURRENCY.md) for crash semantics details.
