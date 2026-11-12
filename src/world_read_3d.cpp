#include "chunkdb/chunk_store.hpp"

#include <cstdint>
#include <vector>

namespace chunkdb {

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3D(
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1, bool with_vars) {
    return ReadChunkRange3DImpl(nullptr, nullptr, x0, y0, z0, x1, y1, z1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3DAt(
    const TxnSnapshot& snapshot,
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1,
    bool with_vars, const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRange3DImpl(&snapshot, &txn_read, x0, y0, z0, x1, y1, z1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange3DImpl(
    const TxnSnapshot* snapshot, const TxnAreaRead* txn_read,
    std::int64_t x0, std::int64_t y0, std::int64_t z0,
    std::int64_t x1, std::int64_t y1, std::int64_t z1, bool with_vars) {
    return ReadChunkAreaEntriesImpl({.dims = 3U, .first = {x0, y0, z0}, .last = {x1, y1, z1}},
        with_vars, snapshot, txn_read);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3D(
    std::int64_t cx, std::int64_t cy, std::int64_t cz, std::int64_t radius_chunks, bool with_vars) {
    return ReadChunkRadius3DImpl(nullptr, nullptr, cx, cy, cz, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3DAt(
    const TxnSnapshot& snapshot, std::int64_t cx, std::int64_t cy, std::int64_t cz,
    std::int64_t radius_chunks, bool with_vars, const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRadius3DImpl(&snapshot, &txn_read, cx, cy, cz, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius3DImpl(
    const TxnSnapshot* snapshot, const TxnAreaRead* txn_read,
    std::int64_t cx, std::int64_t cy, std::int64_t cz, std::int64_t radius_chunks, bool with_vars) {
    return ReadChunkAreaEntriesImpl({.dims = 3U, .first = {cx, cy, cz},
        .around = true, .radius = radius_chunks}, with_vars, snapshot, txn_read);
}

}  // namespace chunkdb
