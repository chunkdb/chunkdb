# Per-block Extra Data

A present block can carry one opaque value of 1 or more bits next to its `block_bits` of payload: an owner, a label, an object's state, the parameters of one cell. Values can differ in length from block to block, blocks without one cost nothing, and chunkdb stores each value with its chunk without interpreting it.

Use something else for a small fixed field that most blocks need (widen `block_bits`) or a dense fixed-size field (a second table with the same chunk geometry, for example `terrain` and `terrain_light`; writes to two tables are not atomic together).

## Turning it on

Extra data is a table option, and tables start without it:

```text
TABLECREATE world block_bits 16 extra_max_block_bits 4096
TABLESET terrain extra_max_block_bits 256 extra_max_chunk_bytes 1048576
```

| Option | Meaning | Values |
| --- | --- | --- |
| `extra_max_block_bits` | the longest value one block can carry | 1 to 134217664; `0` (the default) means off |
| `extra_max_chunk_bytes` | the most extra data one chunk can hold; a value costs 8 bytes plus `ceil(bits / 8)` | 9 to 16777216, default 65536; one value of `extra_max_block_bits` must fit |

Enabling is permanent: extra data cannot be turned off and both limits can only be raised, so stored data never exceeds the current limits. It marks the table with the `extra-data` format feature, which a chunkdb build without the feature can open only read-only ([COMPATIBILITY.md](COMPATIBILITY.md)). There is no server flag for it. `TABLEINFO`, `USE` and `HELLO` report both options, `0` for a table without extra data; `HELLO` lists the `extra-data` capability on every server that supports the commands.

## Commands

| Command | Effect |
| --- | --- |
| `XGET <x> <y>` | the value as `[bit_length u32le][bytes]`, or null when the block has none |
| `XPUT <x> <y> <bit_length> <length>` | sets the value; `<length>` (`ceil(bit_length / 8)`) bytes and an empty line follow, as for `CHUNKPUT`; `+OK` |
| `XDEL <x> <y>` | removes the value; `+OK`, also when there was none |
| `CHUNKBATCH ... XPUT <x> <y> <bits>`, `XDEL <x> <y>` | the same inside an atomic batch, mixed with `SET`/`UNSET`; `<bits>` is `0`/`1` text |
| `CHUNKGET <cx> <cy> STATE EXTRA` | the chunk state followed by its EXTRA section |
| `CHUNKPUT <cx> <cy> STATE EXTRA [ZRLE] [IF <version>] <length>` | replaces the state and all values in one write |

Bit `n` of a value is bit `n % 8` of byte `n / 8`, least significant first; in `CHUNKBATCH`, character `n` of the text is bit `n`. Padding bits in a value's last byte are ignored on input and zero on output. The EXTRA section lists each value as `block_index u32le`, `bit_length u32le` and the bytes, in ascending block index (`local_y * chunk_width_blocks + local_x`); the exact rules are in [PROTOCOL.md](PROTOCOL.md) section 4.

```text
SET 10 4 0000000000000101
XPUT 10 4 12 2
<2 bytes>

XGET 10 4                     -> 6 bytes: 0c 00 00 00, then the 2 bytes
CHUNKBATCH 0 0 SET 3 3 0000000000000001 XPUT 3 3 1011
UNSET 10 4                    -> the value goes with the block
```

## Rules

- A value belongs to a present block: `XPUT` on an unset block fails, `UNSET` deletes the value, `SET` keeps it.
- `CHUNKPUT` without `EXTRA` keeps the values of blocks that stay present and drops the others; with `EXTRA` it replaces them all, and each value must belong to a block the new state has present.
- `CHUNKBATCH` applies operations in order: a block must be present at its `XPUT`.
- Every change advances the chunk version (`CHUNKVER`, `IF`); a write that changes nothing does not.
- Every write is one WAL frame, so a block and its value change together, also across a crash.
- A write over a limit, malformed, or on a table without extra data fails with `INVALID_ARGUMENT` before anything changes, and the connection stays usable. Reads on a table without extra data fail the same way.

## Limits and costs

- `GET`, `MGET`, `MSET`, `CHUNKRANGE`, `CHUNKRADIUS` and `CHUNKSCAN` do not carry extra data.
- `XPUT` and `XDEL` take no `IF`; use `CHUNKBATCH ... IF` or `CHUNKPUT ... IF`. Batch values are text, so they count against `max_line_bytes`.
- A cached chunk holds its extra data in memory: its encoded size plus 4 bytes per value, and up to twice that while one write changes several values. The cache limit (`--max-loaded-chunks`) counts chunks, not bytes, so extra data can add up to `max_loaded_chunks * extra_max_chunk_bytes` (4 GiB at the defaults of 65536 chunks and 64 KiB).
- On disk, a chunk without values is stored exactly as in a table without extra data; the format is in [STORAGE_FORMAT.md](STORAGE_FORMAT.md) sections 3.2 and 4.1.
- `chunkdb_verify` checks the values: order, lengths, padding, and that each belongs to a present block.
