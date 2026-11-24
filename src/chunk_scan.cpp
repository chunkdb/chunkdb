#include "chunkdb/chunk_store.hpp"

#include "chunk_coordinate_index.hpp"
#include "statement_control.hpp"

namespace chunkdb {

ChunkIdentityScanPage ChunkStore::ScanChunkIdentities(bool has_cursor, ChunkCoord cursor,
    std::size_t limit, std::optional<std::uint64_t> since, bool with_data,
    const ChunkScanFilter* filter) {
    return ScanChunkIdentitiesImpl(nullptr, has_cursor, cursor, limit, since, with_data, filter);
}

ChunkIdentityScanPage ChunkStore::ScanChunkIdentitiesAt(const TxnSnapshot& snapshot, bool has_cursor,
    ChunkCoord cursor, std::size_t limit, bool with_data, const ChunkScanFilter* filter) {
    RequireOwnTxnSnapshot(snapshot);
    return ScanChunkIdentitiesImpl(&snapshot, has_cursor, cursor, limit, std::nullopt, with_data, filter);
}

ChunkIdentityScanPage ChunkStore::ScanChunkIdentitiesImpl(const TxnSnapshot* snapshot, bool has_cursor,
    ChunkCoord cursor, std::size_t limit, std::optional<std::uint64_t> since, bool with_data,
    const ChunkScanFilter* filter) {
    if (limit == 0U || limit > kMaxChunkScanLimit)
        throw std::invalid_argument("scan limit must be between 1 and " + std::to_string(kMaxChunkScanLimit));
    if (has_cursor) (void)geometry_.ChunkToLarge(cursor);
    ChunkIdentityScanPage page;
    // As of a snapshot the index must list every chunk its writes populated;
    // the page is then that revision's.
    if (snapshot != nullptr) WaitForTxnSnapshotWrites(*snapshot);
    const auto index = snapshot != nullptr ? CoordinateIndexForArea(snapshot) : CoordinateIndexForScan();
    if (index == nullptr) throw std::invalid_argument("the snapshot includes writes that have not completed");
    page.position = snapshot != nullptr ? std::optional(snapshot->version()) : CompletedRevision();
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
    // Identities visited; without a filter, the entries.
    std::size_t visited = 0U;
    std::optional<ChunkCoord> last;
    const bool with_vars = with_data || (filter != nullptr && filter->needs_vars);
    const bool identity_only = !with_data && filter == nullptr;
    const auto more = [&]() {
        page.has_more = true;
        page.after = last;
        return finish();
    };
    while (visited <= limit) {
        const auto candidates = snapshot != nullptr
            ? index->SnapshotPage(pass_has_cursor, pass_cursor, limit + 1U - visited, snapshot->version())
            : index->Page(pass_has_cursor, pass_cursor, limit + 1U - visited, since);
        for (const auto& coord : candidates.coords) {
            CheckStatementCancellation();
            ChunkRangeEntry state;
            const bool populated = snapshot != nullptr
                ? ReadPopulatedChunkStateNoCache(coord, &state, with_vars, snapshot, false, identity_only)
                : ReadPopulatedChunkStateNoCache(coord, &state, with_vars, nullptr, since.has_value(), identity_only);
            if (!populated) continue;
            if (since && state.version <= *since) continue;
            if (visited == limit) return more();
            if (filter != nullptr && !filter->keep(coord, state)) {
                ++visited;
                last = coord;
                continue;
            }
            if (!charge(state)) {
                if (visited == 0U) throw std::out_of_range("SCAN CHUNKS data exceeds the response byte limit");
                return more();
            }
            ChunkIdentityScanEntry entry{.coord = coord, .version = state.version,
                .commit_time_ms = state.commit_time_ms};
            if (with_data && !state.presence_bitmap.empty())
                entry.data = ChunkState{.version = state.version, .payload = std::move(state.payload),
                    .presence_bitmap = std::move(state.presence_bitmap), .vars = std::move(state.vars),
                    .written = true, .commit_time_ms = state.commit_time_ms, .chunk_columns = std::move(state.chunk_columns)};
            page.entries.push_back(std::move(entry));
            ++visited;
            last = coord;
        }
        if (!candidates.more) break;
        pass_has_cursor = true;
        pass_cursor = candidates.coords.back();
    }
    return finish();
}

}  // namespace chunkdb
