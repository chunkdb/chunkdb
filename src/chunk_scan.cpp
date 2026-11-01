#include "chunkdb/chunk_store.hpp"

#include "chunk_coordinate_index.hpp"

namespace chunkdb {

ChunkIdentityScanPage ChunkStore::ScanChunkIdentities(bool has_cursor, ChunkCoord cursor,
    std::size_t limit, std::optional<std::uint64_t> since, bool with_data) {
    if (limit == 0U || limit > kMaxChunkScanLimit)
        throw std::invalid_argument("scan limit must be between 1 and " + std::to_string(kMaxChunkScanLimit));
    if (has_cursor) (void)geometry_.ChunkToLarge(cursor);
    ChunkIdentityScanPage page;
    page.position = CompletedRevision();
    const auto index = CoordinateIndexForScan();
    page.retained_deletion_floor = index->retained_deletion_floor();
    const auto require_history = [&](std::uint64_t floor) {
        if (since && *since < floor) throw TombstoneHistoryLostError(floor, page.position);
    };
    require_history(page.retained_deletion_floor);
    const auto finish = [&]() {
        if (access_mode_ == AccessMode::kReadOnly) {
            RequireStoreStillOnDisk();
            const auto retained = RetainedIndexForSnapshot();
            page.retained_deletion_floor = retained->retained_deletion_floor();
            require_history(page.retained_deletion_floor);
        }
        return std::move(page);
    };
    std::size_t retained_bytes = 256U;
    const auto charge = [&](const ChunkRangeEntry& state) {
        std::size_t bytes = 128U;
        if (with_data && !state.presence_bitmap.empty()) {
            bytes = ChunkRangeEntryCostBytes();
            const auto variables = state.vars.encoded_size();
            const auto metadata = state.chunk_columns ? state.chunk_columns->variable_bytes : 0U;
            const auto indexes = state.vars.size() * sizeof(std::uint32_t);
            for (const auto part : {variables, metadata, indexes}) {
                if (part > kMaxChunkRangeResponseBytes || bytes > kMaxChunkRangeResponseBytes - part)
                    return false;
                bytes += part;
            }
        }
        if (bytes > kMaxChunkRangeResponseBytes - retained_bytes) return false;
        retained_bytes += bytes;
        return true;
    };
    bool pass_has_cursor = has_cursor;
    auto pass_cursor = cursor;
    while (page.entries.size() <= limit) {
        const auto candidates = index->Page(pass_has_cursor, pass_cursor,
            limit + 1U - page.entries.size(), since);
        for (const auto& coord : candidates.coords) {
            ChunkRangeEntry state;
            if (!ReadPopulatedChunkStateNoCache(coord, &state, with_data, nullptr, since.has_value(), !with_data)) continue;
            if (since && state.version <= *since) continue;
            if (page.entries.size() == limit) { page.has_more = true; return finish(); }
            if (!charge(state)) {
                if (page.entries.empty()) throw std::out_of_range("SCAN CHUNKS data exceeds the response byte limit");
                page.has_more = true;
                return finish();
            }
            ChunkIdentityScanEntry entry{.coord = coord, .version = state.version,
                .commit_time_ms = state.commit_time_ms};
            if (with_data && !state.presence_bitmap.empty())
                entry.data = ChunkState{.version = state.version, .payload = std::move(state.payload),
                    .presence_bitmap = std::move(state.presence_bitmap), .vars = std::move(state.vars),
                    .written = true, .commit_time_ms = state.commit_time_ms, .chunk_columns = std::move(state.chunk_columns)};
            page.entries.push_back(std::move(entry));
        }
        if (!candidates.more) break;
        pass_has_cursor = true;
        pass_cursor = candidates.coords.back();
    }
    return finish();
}

}  // namespace chunkdb
