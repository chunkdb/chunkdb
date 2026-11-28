#include "chunkdb/chunk_store.hpp"

#include <limits>

#include "area_cursor.hpp"
#include "chunk_coordinate_index.hpp"
#include "statement_control.hpp"

namespace chunkdb {
namespace {

// The coordinate just before `coord` in ascending x, y, z order; false for
// the lowest one.
bool PreviousCoord(const ChunkCoord& coord, ChunkCoord* previous) noexcept {
    constexpr auto lowest = std::numeric_limits<std::int64_t>::min();
    constexpr auto highest = std::numeric_limits<std::int64_t>::max();
    if (coord.z != lowest) *previous = {coord.x, coord.y, coord.z - 1};
    else if (coord.y != lowest) *previous = {coord.x, coord.y - 1, highest};
    else if (coord.x != lowest) *previous = {coord.x - 1, highest, highest};
    else return false;
    return true;
}

}  // namespace

ChunkIdentityScanPage ChunkStore::ScanChunkIdentities(bool has_cursor, ChunkCoord cursor,
    std::size_t limit, std::optional<std::uint64_t> since, bool with_data,
    const ChunkScanFilter* filter, const ChunkAreaSelector* area) {
    return ScanChunkIdentitiesImpl(nullptr, has_cursor, cursor, limit, since, with_data, filter, area);
}

ChunkIdentityScanPage ChunkStore::ScanChunkIdentitiesAt(const TxnSnapshot& snapshot, bool has_cursor,
    ChunkCoord cursor, std::size_t limit, bool with_data, const ChunkScanFilter* filter,
    const ChunkAreaSelector* area) {
    RequireOwnTxnSnapshot(snapshot);
    return ScanChunkIdentitiesImpl(&snapshot, has_cursor, cursor, limit, std::nullopt, with_data, filter, area);
}

ChunkIdentityScanPage ChunkStore::ScanChunkIdentitiesImpl(const TxnSnapshot* snapshot, bool has_cursor,
    ChunkCoord cursor, std::size_t limit, std::optional<std::uint64_t> since, bool with_data,
    const ChunkScanFilter* filter, const ChunkAreaSelector* area) {
    if (limit == 0U || limit > kMaxChunkScanLimit)
        throw std::invalid_argument("scan limit must be between 1 and " + std::to_string(kMaxChunkScanLimit));
    if (has_cursor) (void)geometry_.ChunkToLarge(cursor);
    if (area != nullptr && area->dims != geometry_.dims())
        throw std::invalid_argument("scan area dimensions do not match the table");
    // Without an area the selector is unused; this one is valid.
    const detail::AreaCursor cells(area != nullptr ? *area : ChunkAreaSelector{});
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
    // The candidates after a coordinate. Inside an area, seek through one
    // column at a time and skip gaps with no indexed coordinate.
    const auto candidates_after = [&](bool has_after, ChunkCoord after, std::size_t count) {
        if (area == nullptr)
            return snapshot != nullptr ? index->SnapshotPage(has_after, after, count, snapshot->version())
                                       : index->Page(has_after, after, count, since);
        CoordinatePage area_page;
        ChunkCoord cell, found, end, before;
        while (area_page.coords.size() < count) {
            CheckStatementCancellation();
            if (!cells.FirstAfter(has_after, after, &cell) || !index->FirstCoordinateFrom(cell, &found))
                return area_page;
            has_after = true;
            if (!cells.Contains(found)) {
                after = found;
                continue;
            }
            end = cells.SegmentEnd(found);
            const bool has_before = PreviousCoord(found, &before);
            const auto wanted = count - area_page.coords.size();
            const auto part = snapshot != nullptr
                ? index->SnapshotPage(has_before, before, wanted, snapshot->version(), &end)
                : index->Page(has_before, before, wanted, since, &end);
            area_page.coords.insert(area_page.coords.end(), part.coords.begin(), part.coords.end());
            if (part.more) {
                area_page.more = true;
                return area_page;
            }
            after = end;
        }
        area_page.more = true;
        return area_page;
    };
    while (visited <= limit) {
        const auto candidates = candidates_after(pass_has_cursor, pass_cursor, limit + 1U - visited);
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
