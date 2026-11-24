#include <array>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "statement_control.hpp"

// Expiry by a table's expire_column (docs/reference/TABLES.md#expiry). The
// catalog runs the passes on the shared maintenance pool; each deletion is
// an ordinary commit, so the WAL, the feed and the slots see it.
namespace chunkdb {

namespace {

// The present blocks of a chunk whose time in `column` has passed.
[[nodiscard]] std::vector<std::size_t> ExpiredBlocks(const ChunkLayout::FixedColumn& column,
    std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence, std::size_t blocks,
    std::uint64_t now_ms) {
    std::vector<std::size_t> expired;
    for (std::size_t block = 0; block < blocks; ++block) {
        if (!BlockPresent(presence, block)) continue;
        if (column.validity != ChunkLayout::kNoValidity &&
            ((payload[column.validity + block / 8U] >> (block % 8U)) & 1U) == 0U)
            continue;
        std::array<std::uint8_t, 8> bytes{};
        ReadValueBits(payload.data(), column.values * 8U + block * column.width, bytes.data(), column.width);
        std::uint64_t time = 0U;
        for (std::size_t i = 0; i < bytes.size(); ++i) time |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
        // Zero means no expiry, like NULL.
        if (time != 0U && time <= now_ms) expired.push_back(block);
    }
    return expired;
}

}  // namespace

ExpiryPass ChunkStore::ExpireBlocks(std::uint32_t column_id, bool has_cursor, ChunkCoord cursor,
    std::size_t max_chunks, std::uint64_t now_ms) {
    if (access_mode_ == AccessMode::kReadOnly) throw std::invalid_argument("store is read-only");
    ThrowIfDurabilityPoisoned();
    const auto& layout = geometry_.layout();
    const auto index = layout.IndexOfId(column_id);
    const auto* column = index == std::string_view::npos ? nullptr : layout.FixedColumnAt(index);
    if (column == nullptr || layout.schema().columns[index].type != ColumnType{.kind = ColumnKind::kUnsigned, .size = 64U})
        throw std::invalid_argument("the expire_column is not a u64 column of the table");
    ExpiryPass pass;
    const auto page = ScanPopulatedChunks(has_cursor, cursor, max_chunks);
    for (const auto& coord : page.coords) {
        ++pass.chunks;
        pass.deleted += ExpireChunkBlocks(coord, *column, now_ms);
    }
    pass.more = page.has_more;
    if (!page.coords.empty()) pass.next = page.coords.back();
    return pass;
}

std::size_t ChunkStore::ExpireChunkBlocks(const ChunkCoord& coord, const ChunkLayout::FixedColumn& column,
    std::uint64_t now_ms) {
    const auto blocks = geometry_.layout().block_count();
    {
        // Most chunks hold nothing expired: read them without the cache.
        ChunkRangeEntry state;
        if (!ReadPopulatedChunkStateNoCache(coord, &state, false)) return 0U;
        if (ExpiredBlocks(column, state.payload, state.presence_bitmap, blocks, now_ms).empty()) return 0U;
    }
    const auto chunk = GetOrLoadRegularChunk(coord);
    auto lock = LockStatementChunk(chunk->mutex);
    AwaitChunkBacklogLocked(lock, *chunk);
    // Decided again under the lock: a write since the read may have moved
    // the time or deleted the block.
    const auto expired = ExpiredBlocks(column, chunk->payload, chunk->presence_bitmap, blocks, now_ms);
    if (expired.empty()) return 0U;
    auto presence = chunk->presence_bitmap;
    for (const auto block : expired) SetBlockPresent(&presence, block, false);
    auto payload = chunk->payload;
    CanonicalizeAbsentBlocks(geometry_, presence, &payload);
    auto var_update = VarUpdateForState(chunk->vars, presence);
    (void)ApplyChunkStateLocked(coord, chunk, std::move(payload), std::move(presence), std::move(var_update));
    return expired.size();
}

}  // namespace chunkdb
