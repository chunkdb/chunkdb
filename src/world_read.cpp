#include <algorithm>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/zrle.hpp"
#include "wal_replay.hpp"
#include "snapshot_generation.hpp"
#include "txn_history.hpp"
#include "chunk_coordinate_index.hpp"
#include "shared_file_input.hpp"
#include "area_cursor.hpp"

namespace chunkdb {

namespace {

// Bounded retries for the no-cache read protocol before it falls back to
// loading the chunk through the cache.
constexpr int kNoCacheReadAttempts = 3;

[[nodiscard]] bool ChunkCoordLess(const ChunkCoord& lhs, const ChunkCoord& rhs) noexcept {
    if (lhs.x != rhs.x) {
        return lhs.x < rhs.x;
    }
    return lhs.y < rhs.y;
}

struct ChunkCoordOrder {
    bool operator()(const ChunkCoord& lhs, const ChunkCoord& rhs) const noexcept {
        return ChunkCoordLess(lhs, rhs);
    }
};

}  // namespace

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::TryGetLoadedChunk(
    const ChunkCoord& chunk_coord) const {
    const LargeChunkCoord large_coord = geometry_.ChunkToLarge(chunk_coord);
    std::shared_ptr<LargeChunk> large_chunk;
    {
        std::lock_guard global_lock(large_chunks_mutex_);
        const auto it = large_chunks_.find(large_coord);
        if (it == large_chunks_.end()) {
            return nullptr;
        }
        large_chunk = it->second;
    }
    std::lock_guard large_lock(large_chunk->mutex);
    const auto it = large_chunk->chunks.find(chunk_coord);
    if (it == large_chunk->chunks.end()) {
        return nullptr;
    }
    return it->second;
}

bool ChunkStore::IsChunkLoadedForTests(std::int64_t chunk_x, std::int64_t chunk_y) const {
    return TryGetLoadedChunk(ChunkCoord{chunk_x, chunk_y}) != nullptr;
}

bool ChunkStore::ReadPopulatedChunkStateNoCache(
    const ChunkCoord& chunk_coord,
    ChunkRangeEntry* out,
    bool with_vars,
    const TxnSnapshot* snapshot, bool include_empty, bool identity_only) {
    const auto include_cached = [&](const RegularChunk& chunk) {
        return include_empty ? chunk.written : ChunkPopulated(geometry_, chunk.presence_bitmap, chunk.chunk_columns.get());
    };
    const auto copy_cached = [&, with_vars](const RegularChunk& chunk, ChunkRangeEntry* entry) {
        entry->version = chunk.version;
        entry->commit_time_ms = chunk.commit_time_ms;
        if (identity_only || (include_empty && !ChunkPopulated(geometry_, chunk.presence_bitmap, chunk.chunk_columns.get()))) {
            entry->payload.clear(); entry->presence_bitmap.clear(); entry->vars = {}; entry->chunk_columns.reset();
            return;
        }
        entry->payload = chunk.payload;
        entry->presence_bitmap = chunk.presence_bitmap;
        entry->chunk_columns = chunk.chunk_columns;
        if (with_vars) {
            entry->vars = chunk.vars;
        }
    };
    if (access_mode_ == AccessMode::kReadOnly) {
        // A read-only store runs no transactions. A chunk a point read keeps
        // is checked against the files (RefreshReadOnlyChunk); any other is
        // read from them without being kept, as the writer reads chunks it
        // does not hold, so an area or a scan over empty chunks does not fill
        // the cache.
        if (TryGetLoadedChunk(chunk_coord) == nullptr) {
            return ReadPopulatedChunkStateFromDisk(chunk_coord, out, with_vars, nullptr, include_empty, identity_only);
        }
        const auto chunk = GetOrLoadRegularChunk(chunk_coord);
        std::shared_lock lock(chunk->mutex);
        if (!include_cached(*chunk)) {
            return false;
        }
        if (out != nullptr) {
            copy_cached(*chunk, out);
        }
        return true;
    }
    // As of a snapshot the state is read whole, current state first and the
    // history after it, before populated-ness is known.
    ChunkRangeEntry snapshot_entry;
    ChunkRangeEntry* const target = snapshot != nullptr && out == nullptr ? &snapshot_entry : out;
    const auto finish_snapshot_read = [&]() {
        ApplyTxnHistory(*snapshot, chunk_coord, with_vars, target);
        return ChunkPopulated(geometry_, target->presence_bitmap, target->chunk_columns.get());
    };
    // Per-chunk consistency protocol: a cached chunk is authoritative (its
    // in-memory state includes acknowledged mutations whose WAL batch has
    // not reached the file yet). The disk files are only trusted when the
    // chunk was absent from the cache both before and after the disk read
    // and no eviction removed a chunk in between: eviction bumps the removal
    // counter under the chunk's lock and the large-chunk lock just before it
    // removes the chunk, after the last change to its files
    // (TryEvictCandidate), so that condition proves every mutation
    // acknowledged before this read began is in the bytes we read, and that
    // no writer changed them while we read.
    // A write keeps what open transactions need while its chunk is cached,
    // so such bytes and the history read after them agree.
    for (int attempt = 0; attempt < kNoCacheReadAttempts; ++attempt) {
        if (const auto loaded = TryGetLoadedChunk(chunk_coord); loaded != nullptr) {
            if (snapshot != nullptr) {
                {
                    std::shared_lock lock(loaded->mutex);
                    copy_cached(*loaded, target);
                }
                TouchChunk(loaded);
                return finish_snapshot_read();
            }
            std::shared_lock lock(loaded->mutex);
            if (!include_cached(*loaded)) {
                return false;
            }
            if (out != nullptr) {
                copy_cached(*loaded, out);
                TouchChunk(loaded);
            }
            return true;
        }

        PauseCacheForTests(CachePausePoint::kNoCacheReadBeforeDisk, chunk_coord);

        const auto eviction_removals_before = eviction_removals_.load(std::memory_order_acquire);
        // Holds this attempt between the checks that bracket its disk read,
        // so a test can load or evict the chunk inside it; re-arming the
        // failpoint holds the next attempt too.
        if (const auto hold =
                ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_NOCACHE_READ_DISK_HOLD_MS_ONCE");
            hold.count() > 0) {
            std::this_thread::sleep_for(hold);
        }
        std::string vars_problem;
        if (snapshot != nullptr) {
            // `chunk_coord` may be the target's own coordinate: only the state
            // is reset.
            target->payload.clear();
            target->presence_bitmap.clear();
            target->version = 0;
            target->vars = ChunkVars{};
        }
        const bool populated = ReadPopulatedChunkStateFromDisk(chunk_coord, target, with_vars, &vars_problem, include_empty, identity_only);
        if (TryGetLoadedChunk(chunk_coord) == nullptr &&
            eviction_removals_.load(std::memory_order_acquire) == eviction_removals_before) {
            if (!vars_problem.empty()) {
                // No writer raced the read, so the files themselves are
                // damaged; a chunk load refuses them the same way.
                throw std::runtime_error(
                    "chunk (" + std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) +
                    ") has inconsistent text and bytes values: " + vars_problem);
            }
            if (snapshot != nullptr) {
                if (!populated) {
                    // An absent chunk is the empty state.
                    target->payload = EmptyPayload();
                    target->presence_bitmap = EmptyPresenceBitmap();
                    target->vars = ChunkVars{};
                }
                return finish_snapshot_read();
            }
            return populated;
        }
        // A writer loaded (or eviction removed) the chunk while we were on
        // the disk path; the bytes we read may predate acknowledged
        // mutations. Retry against the cache.
    }

    // Sustained per-chunk contention: fall back to the authoritative cache
    // path. This inserts the chunk into the cache, trading the no-insert
    // property for consistency on this rare path.
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    if (snapshot != nullptr) {
        {
            std::shared_lock lock(regular_chunk->mutex);
            copy_cached(*regular_chunk, target);
        }
        return finish_snapshot_read();
    }
    std::shared_lock lock(regular_chunk->mutex);
    if (!include_cached(*regular_chunk)) {
        return false;
    }
    if (out != nullptr) {
        copy_cached(*regular_chunk, out);
    }
    return true;
}

bool ChunkStore::ReadPopulatedChunkStateFromDisk(
    const ChunkCoord& chunk_coord,
    ChunkRangeEntry* out,
    bool with_vars,
    std::string* vars_problem, bool include_empty, bool identity_only) {
    if (access_mode_ == AccessMode::kReadOnly) {
        // A writer in another process changes these files at any time: only
        // the snapshot-bracketed load pairs an image and a WAL that existed
        // together, and it also hides a frame a rollback intent rejects.
        auto loaded = LoadChunkPayload(chunk_coord);
        BringChunkColumnsToSchema(geometry_, loaded.schema_version, geometry_.chunk_schema().version, &loaded.chunk_columns);
        BringToCurrentSchema(geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
        const bool populated = ChunkPopulated(geometry_, loaded.presence_bitmap, loaded.chunk_columns.get());
        if (include_empty ? loaded.revision == 0U : !populated) {
            return false;
        }
        if (out != nullptr) {
            out->version = loaded.revision;
            out->commit_time_ms = loaded.commit_time_ms;
            if (identity_only || (include_empty && !populated)) {
                out->payload.clear(); out->presence_bitmap.clear(); out->vars = {}; out->chunk_columns.reset();
                return true;
            }
            out->payload = std::move(loaded.payload);
            out->presence_bitmap = std::move(loaded.presence_bitmap);
            out->version = loaded.revision;
            out->commit_time_ms = loaded.commit_time_ms;
            out->chunk_columns = std::move(loaded.chunk_columns);
            if (with_vars) {
                out->vars = std::move(loaded.vars);
            }
        }
        return true;
    }
    // Evaluate directly from storage without inserting anything into the
    // cache, so scans over absent chunks do not displace hot chunks.
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    std::vector<std::uint8_t> payload(geometry_.ChunkPayloadBytes(), 0U);
    std::vector<std::uint8_t> presence(ChunkPresenceBitmapBytes(geometry_), 0U);
    // Replay validates value records against it, so it is read even when
    // not returned.
    ChunkVars vars;
    std::uint64_t base_revision = 0;
    // The chunk version a load would give it.
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    std::shared_ptr<const ChunkColumnState> chunk_columns;
    // The schema version of the state; 0 while it is the empty state.
    std::uint64_t schema_version = 0;

    if (std::filesystem::exists(data_path)) {
        try {
            const auto bytes = LoadChunkImageFile(data_path, geometry_, chunk_coord, store_id_, features_);
            auto image = ParseChunkImage(bytes, geometry_, chunk_coord, store_id_, features_);
            payload = std::move(image.payload);
            presence = std::move(image.presence_bitmap);
            vars = std::move(image.vars);
            base_revision = image.revision;
            revision = image.revision;
            commit_time_ms = image.commit_time_ms;
            chunk_columns = std::move(image.chunk_columns);
            schema_version = image.schema_version;
        } catch (...) {
            // The image can be replaced or garbage-collected concurrently by
            // an atomic checkpoint rename; only a still-present file is a
            // real read failure.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            std::fill(payload.begin(), payload.end(), std::uint8_t{0});
            std::fill(presence.begin(), presence.end(), std::uint8_t{0});
            vars = ChunkVars{};
            base_revision = 0;
            revision = 0;
            schema_version = 0;
        }
    }

    const auto retained = coordinate_index_->EmptyBoundary(chunk_coord);
    if (retained) {
        (void)ApplyRetainedEmptyBase(geometry_, *retained, revision, commit_time_ms, schema_version,
            payload, presence, vars, chunk_columns);
        base_revision = revision;
    }
    PauseCacheForTests(CachePausePoint::kNoCacheReadBeforeWal, chunk_coord);
    if (std::filesystem::exists(wal_path)) {
        std::optional<SharedFileInput> input;
        try {
            input.emplace(wal_path);
        } catch (const std::system_error&) {
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
        }
        if (input) {
            input->seekg(0, std::ios::end);
            const auto measured = input->tellg();
            if (measured < 0) throw std::runtime_error("failed to measure WAL " + wal_path.string());
            input->seekg(0);
            if (!*input) throw std::runtime_error("failed to rewind WAL " + wal_path.string());
            const auto replay = ReplayWalStream(
                *input, static_cast<std::uint64_t>(measured), geometry_, chunk_coord, store_id_, features_, base_revision, schema_version, &payload,
                &presence, &vars, nullptr, &chunk_columns, {}, commit_time_ms, retained ?
                    std::optional{WalKnownIdentity{retained->revision, retained->commit_time_ms}} : std::nullopt);
            if (revision != 0U || replay.applied_frames != 0U)
                schema_version = geometry_.layout().schema().version;
            if (replay.applied_frames > 0) {
                revision = replay.revision;
                commit_time_ms = replay.commit_time_ms;
            }
            if ((!replay.replayable && !replay.torn_creation) ||
                (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail)) {
                // As for a chunk load: never present state without the
                // mutations a damaged WAL may hold.
                throw std::runtime_error(
                    "WAL " + wal_path.string() + " cannot be replayed (" + replay.stop_reason + ")");
            }
            // A writer racing this read can pair an image and a WAL that
            // never coexisted; the caller tells that from damage.
            *vars_problem = replay.vars_problem;
        }
    }

    if (retained && retained->kind == ChunkIndexKind::kForget && revision == retained->revision) {
        revision = 0U;
        commit_time_ms = 0U;
    }
    BringChunkColumnsToSchema(geometry_, schema_version, geometry_.chunk_schema().version, &chunk_columns);
    BringToCurrentSchema(geometry_, schema_version, presence, &payload, &vars);
    const bool populated = ChunkPopulated(geometry_, presence, chunk_columns.get());
    if (include_empty ? revision == 0U : !populated) {
        return false;
    }
    if (out != nullptr) {
        out->version = revision;
        out->commit_time_ms = commit_time_ms;
        if (identity_only || (include_empty && !populated)) {
            out->payload.clear(); out->presence_bitmap.clear(); out->vars = {}; out->chunk_columns.reset();
            return true;
        }
        out->payload = std::move(payload);
        out->presence_bitmap = std::move(presence);
        out->version = revision;
        out->commit_time_ms = commit_time_ms;
        out->chunk_columns = std::move(chunk_columns);
        if (with_vars) {
            out->vars = std::move(vars);
        }
    }
    return true;
}

ChunkScanPage ChunkStore::ScanPopulatedChunks(
    bool has_cursor,
    ChunkCoord cursor,
    std::size_t limit) {
    if (limit == 0 || limit > kMaxChunkScanLimit) {
        throw std::invalid_argument(
            "scan limit must be between 1 and " + std::to_string(kMaxChunkScanLimit));
    }
    // A read-only store checks, after listing, that its directory still
    // holds its store: a table dropped and created again lists as empty.
    // Store ids never repeat, so a match then covers the whole listing.
    const auto finish = [&](ChunkScanPage& result) -> ChunkScanPage {
        if (access_mode_ == AccessMode::kReadOnly) {
            RequireStoreStillOnDisk();
        }
        return std::move(result);
    };

    // Taking the frontier acquires every covered producer's index publication.
    // Candidates remain hints; each returned coordinate is checked again.
    (void)CompletedRevision();
    const auto index = CoordinateIndexForScan();
    ChunkScanPage page;
    bool pass_has_cursor = has_cursor;
    ChunkCoord pass_cursor = cursor;
    while (page.coords.size() <= limit) {
        const auto candidates = index->Page(pass_has_cursor, pass_cursor, limit + 1U - page.coords.size());
        const auto& pass_coords = candidates.coords;
        for (const auto& coord : pass_coords) {
            if (!ReadPopulatedChunkStateNoCache(coord, nullptr, false)) {
                continue;
            }
            if (page.coords.size() >= limit) {
                page.has_more = true;
                return finish(page);
            }
            page.coords.push_back(coord);
        }
        if (!candidates.more) {
            break;
        }
        pass_has_cursor = true;
        pass_cursor = pass_coords.back();
    }
    return finish(page);
}

std::size_t ChunkStore::ChunkRangeEntryCostBytes() const noexcept {
    // Three signed coordinates, protocol framing and CKCF's fixed header fit
    // in 128 bytes; six section descriptors and fixed-array spans follow.
    std::size_t cost = 128U + 6U * 12U + 4U + geometry_.layout().fixed_columns().size() * 24U +
        geometry_.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry_);
    if (!geometry_.chunk_schema().columns.empty()) {
        cost += 4U + 16U * geometry_.chunk_schema().columns.size();
        for (const auto& column : geometry_.chunk_schema().columns)
            cost += (FixedWidthBits(column.type) + 7U) / 8U;
    }
    return cost;
}

void ChunkStore::AppendPopulatedChunkRangeEntry(
    const ChunkCoord& coord,
    std::size_t max_entries,
    const char* operation_name,
    bool with_vars,
    std::size_t* vars_bytes,
    std::vector<ChunkRangeEntry>* entries,
    const TxnSnapshot* snapshot,
    const TxnAreaRead* txn_read) {
    const auto budget = query_limits_.max_area_response_bytes;
    const auto too_large = [operation_name, budget] {
        return std::out_of_range(
            std::string(operation_name) + " response exceeds the " + std::to_string(budget) +
            "-byte limit; request fewer chunks");
    };
    const auto charge = [&](const ChunkRangeEntry& entry) {
        auto variable = with_vars ? entry.vars.encoded_size() : 0U;
        const auto metadata = entry.chunk_columns ? entry.chunk_columns->variable_bytes : 0U;
        if (variable > budget || metadata > budget - variable)
            throw too_large();
        variable += metadata;
        const auto fixed = (entries->size() + 1U) * ChunkRangeEntryCostBytes();
        if (*vars_bytes > budget - fixed || variable > budget - fixed - *vars_bytes) throw too_large();
        *vars_bytes += variable;
    };
    if (txn_read != nullptr) {
        if (txn_read->covered != nullptr) {
            txn_read->covered->push_back(coord);
        }
        // The transaction's own copy of the chunk, when it wrote it.
        if (const ChunkState* own = txn_read->overlay ? txn_read->overlay(coord) : nullptr; own != nullptr) {
            if (!ChunkPopulated(geometry_, own->presence_bitmap, own->chunk_columns.get())) {
                return;
            }
            if (entries->size() >= max_entries) {
                throw too_large();
            }
            ChunkRangeEntry entry{
                .coord = coord,
                .payload = own->payload,
                .presence_bitmap = own->presence_bitmap,
                .version = own->version,
                .commit_time_ms = own->commit_time_ms,
                .chunk_columns = own->chunk_columns,
            };
            if (with_vars) {
                entry.vars = own->vars;
            }
            charge(entry);
            entries->push_back(std::move(entry));
            return;
        }
    }
    if (entries->size() < max_entries) {
        ChunkRangeEntry entry;
        entry.coord = coord;
        if (ReadPopulatedChunkStateNoCache(entry.coord, &entry, with_vars, snapshot)) {
            charge(entry);
            entries->push_back(std::move(entry));
        }
        return;
    }
    // Byte budget exhausted: probe populated-ness without extracting state
    // strings so the failure stays bounded.
    if (ReadPopulatedChunkStateNoCache(coord, nullptr, false, snapshot)) {
        throw too_large();
    }
}

ChunkAreaPage ChunkStore::VisitChunkArea(
    const ChunkAreaSelector& selector, std::optional<ChunkCoord> after, std::size_t limit,
    const ChunkAreaVisitor& visitor, bool with_vars, const TxnSnapshot* snapshot, const TxnAreaRead& txn_read) {
    if (selector.dims != geometry_.dims()) throw std::invalid_argument("area dimensions must match the table");
    if (limit == 0U || limit > query_limits_.max_area_chunks)
        throw std::invalid_argument("area page limit is outside the configured chunk bound");
    if (!visitor) throw std::invalid_argument("area page visitor is missing");
    if (snapshot != nullptr) RequireOwnTxnSnapshot(*snapshot);
    const detail::AreaCursor cursor(selector);
    if (after && !cursor.Contains(*after)) throw std::invalid_argument("area cursor is outside its selector");
    auto current = after ? cursor.Next(*after) : std::optional{cursor.First()};
    auto admitted = after;
    const auto index = snapshot == nullptr ? CoordinateIndexForScan() : nullptr;
    for (std::size_t visited = 0U; current && visited < limit; ++visited) {
        const auto next = cursor.Next(*current);
        if (txn_read.covered != nullptr) txn_read.covered->push_back(*current);
        std::optional<ChunkRangeEntry> entry;
        const auto* own = txn_read.overlay ? txn_read.overlay(*current) : nullptr;
        if (own != nullptr) {
            if (ChunkPopulated(geometry_, own->presence_bitmap, own->chunk_columns.get())) {
                entry.emplace(ChunkRangeEntry{.coord = *current, .payload = own->payload,
                    .presence_bitmap = own->presence_bitmap, .version = own->version,
                    .vars = with_vars ? own->vars : ChunkVars{}, .commit_time_ms = own->commit_time_ms,
                    .chunk_columns = own->chunk_columns});
            }
        } else if (!index || index->IsPopulationCandidate(*current)) {
            ChunkRangeEntry candidate; candidate.coord = *current;
            if (ReadPopulatedChunkStateNoCache(*current, &candidate, with_vars, snapshot))
                entry.emplace(std::move(candidate));
        }
        if (!visitor(*current, std::move(entry), next.has_value())) {
            if (admitted == after) throw std::out_of_range("an area entry cannot fit one page");
            return {.has_more = true, .after = admitted};
        }
        admitted = current;
        current = next;
    }
    return {.has_more = current.has_value(), .after = current ? admitted : std::nullopt};
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkAreaEntriesImpl(
    const ChunkAreaSelector& selector, bool with_vars,
    const TxnSnapshot* snapshot, const TxnAreaRead* txn_read) {
    if (selector.dims != geometry_.dims()) throw std::invalid_argument("area dimensions must match the table");
    if (detail::AreaRequestedExceeds(selector, query_limits_.max_area_chunks))
        throw std::invalid_argument("chunk area must cover at most " + std::to_string(query_limits_.max_area_chunks) + " chunks");
    const detail::AreaCursor cursor(selector);
    const auto max_entries = query_limits_.max_area_response_bytes / ChunkRangeEntryCostBytes();
    std::vector<ChunkRangeEntry> entries;
    std::size_t variable_bytes = 0U;
    const auto index = snapshot == nullptr ? CoordinateIndexForScan() : nullptr;
    for (auto current = std::optional{cursor.First()}; current; current = cursor.Next(*current)) {
        if (!index || index->IsPopulationCandidate(*current))
            AppendPopulatedChunkRangeEntry(*current, max_entries, "GET AREA", with_vars,
                &variable_bytes, &entries, snapshot, txn_read);
    }
    return entries;
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange(
    std::int64_t chunk_x0,
    std::int64_t chunk_y0,
    std::int64_t chunk_x1,
    std::int64_t chunk_y1,
    bool with_vars) {
    return ReadChunkRangeImpl(nullptr, nullptr, chunk_x0, chunk_y0, chunk_x1, chunk_y1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRangeAt(
    const TxnSnapshot& snapshot,
    std::int64_t chunk_x0,
    std::int64_t chunk_y0,
    std::int64_t chunk_x1,
    std::int64_t chunk_y1,
    bool with_vars,
    const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRangeImpl(&snapshot, &txn_read, chunk_x0, chunk_y0, chunk_x1, chunk_y1, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRangeImpl(
    const TxnSnapshot* snapshot,
    const TxnAreaRead* txn_read,
    std::int64_t chunk_x0,
    std::int64_t chunk_y0,
    std::int64_t chunk_x1,
    std::int64_t chunk_y1,
    bool with_vars) {
    if (geometry_.dims() != 2U) throw std::invalid_argument("chunk coordinates require 3 axes");
    return ReadChunkAreaEntriesImpl({.dims = 2U, .first = {chunk_x0, chunk_y0},
        .last = {chunk_x1, chunk_y1}}, with_vars, snapshot, txn_read);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius(
    std::int64_t center_x,
    std::int64_t center_y,
    std::int64_t radius_chunks,
    bool with_vars) {
    return ReadChunkRadiusImpl(nullptr, nullptr, center_x, center_y, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadiusAt(
    const TxnSnapshot& snapshot,
    std::int64_t center_x,
    std::int64_t center_y,
    std::int64_t radius_chunks,
    bool with_vars,
    const TxnAreaRead& txn_read) {
    RequireOwnTxnSnapshot(snapshot);
    return ReadChunkRadiusImpl(&snapshot, &txn_read, center_x, center_y, radius_chunks, with_vars);
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadiusImpl(
    const TxnSnapshot* snapshot,
    const TxnAreaRead* txn_read,
    std::int64_t center_x,
    std::int64_t center_y,
    std::int64_t radius_chunks,
    bool with_vars) {
    if (geometry_.dims() != 2U) throw std::invalid_argument("chunk coordinates require 3 axes");
    return ReadChunkAreaEntriesImpl({.dims = 2U, .first = {center_x, center_y},
        .around = true, .radius = radius_chunks}, with_vars, snapshot, txn_read);
}

}  // namespace chunkdb
