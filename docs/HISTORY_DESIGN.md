# Block History (#45): Design

Status: implemented (#45), with the owner's decisions D2 to D9 as recommended and D1 as "included". The user guide is [HISTORY.md](HISTORY.md), the format [STORAGE_FORMAT.md](STORAGE_FORMAT.md) section 9, the measured budgets [PERFORMANCE.md](PERFORMANCE.md). Where the implementation differs from this proposal: extra data is recorded as per-change deltas (set, removed, unchanged) with keyframes carrying the EXTRA section, so an event line has `before_extra` and `after_extra` fields; keyframes are also written as records inside a segment, so the 8x rule holds on small chunks; segments carry flags for a chunk's first segment and for a trim cut instead of a separate `floor` file; history records `history_start_time_ms` for `AT TIME`; retention runs at checkpoints only; and the checkpoint and write-throughput budgets are not met where syncs are expensive (macOS `F_FULLFSYNC`).

## Model

An **event** is one change of one block: `revision, time_ms, x, y, old, new, tag`. `old`/`new` are the block's bits, or absent.

- The revision is the chunk revision of the mutation (`CHUNKVER`): unique in the table, increasing per chunk, not contiguous. A mutation that changes many blocks (`CHUNKPUT`, `CHUNKBATCH`) gives one event per changed block, all with its revision, time and tag, ordered by block index. A batch records the net change per block.
- Only real changes are events: a write that changes nothing records nothing, tag included.
- Time is the frame's commit time. It never decreases within a chunk; across chunks a lower revision can carry a later time (revision and time come from two counters). History therefore orders by revision; time is a filter, and `AT TIME` resolves per chunk. Making time follow revision table-wide would need a lock on every write (D5).
- History starts at **H0**, the clock ceiling when it was enabled. Earlier state is the starting point, not events.

## Storage

```text
tables/<t>/history/floor                                    trim floor (revision, time), checksummed
tables/<t>/history/L_<lx>_<ly>/C_<cx>_<cy>.<base_rev>.hseg   segments per chunk, oldest to newest
```

A separate tree keeps `CHUNKSCAN`, empty-chunk collection and the `L_` walks unchanged; a chunk removed by collection keeps its history. A segment file has a header (store id, chunk, H0, base revision and time) and an optional **keyframe**: the full chunk state at its base, zrle-compressed. Records follow, one per checkpoint append, each with a header (revision and time range, counts, a 256-bit coarse block mask, CRCs) and a body grouped per mutation: Δrevision, Δtime, tag reference, then the changed blocks as a list or a bitmap with packed values. A new segment starts when the body passes 64 KiB, and it gets a keyframe only when the event bytes since the last keyframe reach 8× the keyframe's size (always in a chunk's first segment and at a trim cut).

History is written at **checkpoint**, so the write path does not change. For a history table the checkpoint, under the chunk lock, runs these steps, each durable before the next:

1. Refuse if the store is fail-closed. Flush and sync the WAL batch.
2. Derive events: the image (or the empty state) is the base, WAL frames above its revision are replayed, and only the blocks each frame touched are diffed. Frames at or below H0 or the history's last revision are skipped. The replay must end at the in-memory state, or the checkpoint fails and the store fails closed.
3. Append to history and sync (a new segment: temp file, sync, rename, directory sync).
4. Publish the image (or, for an empty chunk, remove it) strictly.
5. Remove the WAL. Apply retention: delete the oldest segments, never the newest.

A crash before 3 leaves old history and the full WAL; the next checkpoint derives the same events and skips the ones it has. A crash after 3 leaves history covering every frame the image will hold. This needs the replay rule of #51 (frames at or below the image revision are skipped) and its empty-chunk flush. History tables take this synced path in every durability mode, because in `relaxed` mode the source frames are not durable yet (D2).

**Reads** merge the segments, the chunk's WAL frames and the in-memory batch, so history shows every write `GET` shows. A read sees exactly the mutations whose revision was issued before it started, so polling with `AFTER` never skips a mutation that commits late. Read-only processes read history after their existing snapshot bracket; history read later can only be newer, which the skip rule absorbs. **Recovery** removes temp segments and trims a crash-shaped tail of the newest segment. Damage elsewhere fails history for that chunk closed: its state still loads, its checkpoints fail and its WAL is kept (D8). **`chunkdb_verify`** checks segment structure and CRCs, increasing revisions, contiguity (a segment's replayed end equals the next keyframe), that every event is a real change, and that history ends at or before the stored state.

**Trim and retention:** writing `floor` (synced) is the commit point; segments wholly below it are deleted oldest first, the newest never. Options `history_max_age_ms` and `history_max_chunk_bytes` (0 = unlimited) are applied at checkpoint and by the maintenance thread for idle chunks.

## Surface

| Command | Reply |
| --- | --- |
| `HISTORY <x> <y> [opts]`, `CHUNKHISTORY <cx> <cy> [opts]`, `RANGEHISTORY <cx0> <cy0> <cx1> <cy1> [opts]` | `END` or `CURSOR <c>`, then events `<revision> <time_ms> <x> <y> <old> <new> <tag>` (`-` for absent or no tag) |
| `GET ... AT <rev>`, `GET ... AT TIME <ms>`, `CHUNKGET`/`CHUNKRANGE`/`CHUNKRADIUS ... AT ...` | the same reply as without `AT` |
| `TAG <hex>` on `SET`, `UNSET`, `MSET`, `CHUNKPUT`, `CHUNKBATCH`, `XPUT`, `XDEL` | unchanged |
| `TABLECREATE`/`TABLESET ... history on`, `history_max_age_ms`, `history_max_chunk_bytes` | `TABLEINFO` adds `history`, both limits and `history_start` (H0) |

- `opts`: `LIMIT n` (1–1024), `ASC|DESC` (newest first by default), `BEFORE`/`AFTER <cursor>` (exclusive), `SINCE`/`UNTIL <ms>`, `TAG <hex>` (a filter). `RANGEHISTORY` is limited to 256 chunks.
- The cursor is one opaque token, `<rev>` or `<rev>:<block_index>`. A bare revision is valid too, so `CHUNKHISTORY cx cy ASC AFTER <CHUNKVER>` returns what changed since a version the client holds.
- A page stops early with a cursor at the response cap or a scan budget, so it can be short or empty. Only `END` means done.
- A window that reaches below the retained start fails with `NOT_RETAINED start=<rev>` (D6), so a listing that ends with `END` returned every event of its window.
- `AT` at or above the next revision fails with `OUT_OF_RANGE`; `AT` before a block's first event returns the absent state.
- `HELLO` adds the capability `history`, `max_history_limit` and `max_tag_bytes`. A tag on a table without history is `INVALID_ARGUMENT` (D4).
- Clients: `{events, cursor}` pages and async iterators (JS), the same shapes in Go with tag options that keep `Set` compatible, one line per event in the CLI.

Later, not in the first version: extra data in history (D1), `HISTORYTRIM` and turning history off (D3), a binary export form, a table-wide change feed, a tag index.

## Sizing (measured on prototypes, Apple M1 Pro)

| Quantity | Value |
| --- | --- |
| Bytes per event, point writes on 1–64-bit blocks | 4.3–10.9 (WAL today: 51–59) |
| Bytes per event, whole-chunk rewrites (`CHUNKPUT`) | 1.3–2.6, never above today's WAL |
| Keyframe overhead with the 8× rule | +0.3–0.7 B/event (a keyframe per segment: +27% at defaults, up to +400%) |
| Checkpoint cost added at defaults | about 170 µs (+3–4% of a running checkpoint) with a plain sync |
| `HISTORY x y LIMIT 100` p99 | ≤ 0.62 ms, flat from 1e5 to 1e7 events |
| `GET`/`CHUNKGET ... AT` p99 | ≤ 53 µs |
| Tag | default 32 B, maximum 255 B; each byte costs about 3 ns on the write path |

Budgets for the implementation, measured as in `PERFORMANCE.md`:
- tables without history change nothing;
- history tables keep write throughput within 5% and add at most 0.5 ms to checkpoint p50;
- ≤ 7 B/event at 16-bit blocks;
- `HISTORY LIMIT 100` p99 ≤ 1 ms;
- `AT` p99 ≤ 0.2 ms per chunk.

The bench needs a new hot-chunk scenario that really changes state: `hot_chunk_writes` changes it in 243 of 20000 operations and never checkpoints.

## Decisions for the owner

1. **D1.** Extra data in history: excluded at first (a table with extra data cannot then be synced from history alone, because `XPUT` moves `CHUNKVER` without an event), or included (keyframes and events up to 16 MiB).
2. **D2.** History tables always write history synced, also in `relaxed` mode (recommended), or allow gaps marked in the protocol.
3. **D3.** Enabling is permanent like extra data (recommended at first); `history off` and `HISTORYTRIM` follow later.
4. **D4.** Tag limits (32 B default, 255 B maximum), and whether a tag on a table without history is refused (recommended) or ignored.
5. **D5.** Time ordered per chunk (recommended) or table-wide at the cost of a lock on every write.
6. **D6.** A new error code `NOT_RETAINED` (recommended) or `OUT_OF_RANGE`.
7. **D7.** Retention: per-chunk byte cap and max age (recommended), or a table-wide byte budget as well.
8. **D8.** Damaged history: fail closed for that chunk (recommended), or a repair command that continues after a marked gap.
9. **D9.** Milestone: everything is additive (a new `ro_compat` bit, new commands and options), so it can ship in 2.0.0 or a 2.x minor.
