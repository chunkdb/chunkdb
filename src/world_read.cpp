#include <algorithm>
#include <filesystem>
#include <limits>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/bit_codec.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_replay.hpp"

namespace chunkdb {

namespace {

// Bounded retries for the no-cache read protocol before it falls back to
// loading the chunk through the cache.
constexpr int kNoCacheReadAttempts = 3;

[[nodiscard]] bool ParseCoordSuffix(
    const std::string& stem,
    const std::string& prefix,
    std::int64_t* out_x,
    std::int64_t* out_y) {
    if (stem.rfind(prefix, 0) != 0) {
        return false;
    }
    const std::string rest = stem.substr(prefix.size());
    const std::size_t separator = rest.find('_');
    if (separator == std::string::npos) {
        return false;
    }
    return TryParseInt64(rest.substr(0, separator), out_x) &&
           TryParseInt64(rest.substr(separator + 1), out_y);
}

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

// Ordered, deduplicated, cursor-filtered scan-candidate accumulator bounded
// to the requested page size. It keeps only the `bound` smallest coordinates
// strictly greater than the cursor, so one scan page uses O(page) memory and
// never fails on world size: duplicate artifacts (`.chk` + `.wal` + cached)
// collapse in the set instead of counting against any global cap.
class ScanCandidateAccumulator {
  public:
    ScanCandidateAccumulator(bool has_cursor, ChunkCoord cursor, std::size_t bound)
        : has_cursor_(has_cursor), cursor_(cursor), bound_(bound) {}

    void Insert(const ChunkCoord& coord) {
        if (has_cursor_ && !ChunkCoordLess(cursor_, coord)) {
            return;
        }
        if (kept_.size() >= bound_) {
            const auto last = std::prev(kept_.end());
            if (!ChunkCoordLess(coord, *last)) {
                // At or beyond the kept window: discarding it means chunks may
                // exist past the window, which the caller must resume into.
                overflowed_ = overflowed_ || kept_.count(coord) == 0U;
                return;
            }
        }
        if (kept_.insert(coord).second && kept_.size() > bound_) {
            kept_.erase(std::prev(kept_.end()));
            overflowed_ = true;
        }
    }

    // True when at least one distinct coordinate beyond the kept window was
    // discarded, so the caller must continue from the window's end to see
    // every chunk.
    [[nodiscard]] bool overflowed() const noexcept { return overflowed_; }

    [[nodiscard]] bool has_cursor() const noexcept { return has_cursor_; }
    [[nodiscard]] const ChunkCoord& cursor() const noexcept { return cursor_; }

    // True when the window is full and every kept coordinate has x below
    // `min_x`: nothing at or after column `min_x` can enter the window, so a
    // caller walking columns in ascending x may stop. The caller must then
    // MarkOverflowed() if it skipped anything.
    [[nodiscard]] bool WindowClosedBefore(std::int64_t min_x) const noexcept {
        return kept_.size() >= bound_ && std::prev(kept_.end())->x < min_x;
    }
    void MarkOverflowed() noexcept { overflowed_ = true; }

    [[nodiscard]] std::vector<ChunkCoord> TakeSorted() {
        return std::vector<ChunkCoord>(kept_.begin(), kept_.end());
    }

  private:
    bool has_cursor_;
    ChunkCoord cursor_;
    std::size_t bound_;
    bool overflowed_ = false;
    std::set<ChunkCoord, ChunkCoordOrder> kept_;
};

std::uint64_t ChunkStore::ScanLargeDirsListedForTests() const noexcept {
    return stats_scan_large_dirs_listed_.load(std::memory_order_relaxed);
}

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
    std::string* payload_bits,
    std::string* presence_bits) {
    // Per-chunk consistency protocol: a cached chunk is authoritative (its
    // in-memory state includes acknowledged mutations whose WAL batch has
    // not reached the file yet). The disk files are only trusted when the
    // chunk was absent from the cache both before and after the disk read
    // and no eviction flushed a chunk in between: eviction flushes the WAL
    // and bumps the flush counter before removing a chunk from the cache,
    // all under the large-chunk lock, so that condition proves every
    // mutation acknowledged before this read began is in the bytes we read.
    for (int attempt = 0; attempt < kNoCacheReadAttempts; ++attempt) {
        if (const auto loaded = TryGetLoadedChunk(chunk_coord); loaded != nullptr) {
            std::shared_lock lock(loaded->mutex);
            if (!ChunkPresent(loaded->presence_bitmap)) {
                return false;
            }
            if (payload_bits != nullptr && presence_bits != nullptr) {
                *payload_bits =
                    BitCodec::ExtractBits(loaded->payload, 0, geometry_.ChunkPayloadBits());
                *presence_bits = PresenceBitsText(geometry_, loaded->presence_bitmap);
                TouchChunk(loaded);
            }
            return true;
        }

        if (attempt == 0) {
            if (const auto hold =
                    ConsumeFailpointDelayMs("CHUNKDB_FAILPOINT_NOCACHE_READ_DISK_HOLD_MS_ONCE");
                hold.count() > 0) {
                std::this_thread::sleep_for(hold);
            }
        }

        const auto eviction_flushes_before =
            stats_eviction_forced_wal_flushes_.load(std::memory_order_acquire);
        const bool populated =
            ReadPopulatedChunkStateFromDisk(chunk_coord, payload_bits, presence_bits);
        if (TryGetLoadedChunk(chunk_coord) == nullptr &&
            stats_eviction_forced_wal_flushes_.load(std::memory_order_acquire) ==
                eviction_flushes_before) {
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
    std::shared_lock lock(regular_chunk->mutex);
    if (!ChunkPresent(regular_chunk->presence_bitmap)) {
        return false;
    }
    if (payload_bits != nullptr && presence_bits != nullptr) {
        *payload_bits =
            BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits());
        *presence_bits = PresenceBitsText(geometry_, regular_chunk->presence_bitmap);
    }
    return true;
}

bool ChunkStore::ReadPopulatedChunkStateFromDisk(
    const ChunkCoord& chunk_coord,
    std::string* payload_bits,
    std::string* presence_bits) {
    // Evaluate directly from storage without inserting anything into the
    // cache, so scans over absent chunks do not displace hot chunks.
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);

    std::vector<std::uint8_t> payload(geometry_.ChunkPayloadBytes(), 0U);
    std::vector<std::uint8_t> presence(ChunkPresenceBitmapBytes(geometry_), 0U);

    if (std::filesystem::exists(data_path)) {
        try {
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                const auto bytes = LoadFile(data_path);
                auto image = ParseChunkImage(bytes, geometry_, chunk_coord);
                payload = std::move(image.payload);
                presence = std::move(image.presence_bitmap);
            } else {
                const auto addr =
                    ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
                std::lock_guard region_lock(RegionIoMutex());
                const auto bytes = LoadFile(data_path);
                const auto region =
                    ParseRegionFileImage(bytes, geometry_, addr, experimental_region_span_chunks_);
                const auto slot_state = ExtractRegionSlotState(region, addr.slot_index);
                if (!slot_state.empty()) {
                    SplitChunkStateBytes(geometry_, slot_state, &payload, &presence);
                }
            }
        } catch (...) {
            // The image can be replaced or garbage-collected concurrently by
            // an atomic checkpoint rename; only a still-present file is a
            // real read failure.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            std::fill(payload.begin(), payload.end(), std::uint8_t{0});
            std::fill(presence.begin(), presence.end(), std::uint8_t{0});
        }
    }

    if (std::filesystem::exists(wal_path)) {
        std::vector<std::uint8_t> wal_bytes;
        bool have_wal = true;
        try {
            wal_bytes = LoadFile(wal_path);
        } catch (...) {
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
            have_wal = false;
        }
        if (have_wal) {
            (void)ReplayWal(wal_bytes, geometry_, chunk_coord, &payload, &presence);
        }
    }

    if (!ChunkPresent(presence)) {
        return false;
    }
    if (payload_bits != nullptr && presence_bits != nullptr) {
        *payload_bits = BitCodec::ExtractBits(payload, 0, geometry_.ChunkPayloadBits());
        *presence_bits = PresenceBitsText(geometry_, presence);
    }
    return true;
}

void ChunkStore::CollectPopulatedCandidatesFromDisk(ScanCandidateAccumulator* candidates) const {
    std::error_code exists_ec;
    if (!std::filesystem::exists(data_dir_, exists_ec) || exists_ec) {
        return;
    }

    if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
        // Scan order is ascending (cx, cy). Every L_<lx>_<ly> directory holds
        // chunks with cx in [lx*W, lx*W + W), so the directories form columns
        // in cx. Listing the (few) top-level entries first and then visiting
        // columns in ascending lx lets a page skip the columns entirely before
        // the cursor and stop as soon as the window cannot change, instead of
        // listing every chunk file in the world on every page.
        struct LargeDirEntry {
            std::int64_t large_x;
            std::int64_t large_y;
            std::filesystem::path path;
        };
        std::vector<LargeDirEntry> large_dirs;
        std::error_code it_ec;
        for (std::filesystem::directory_iterator dir_it(data_dir_, it_ec), end;
             dir_it != end && !it_ec;
             dir_it.increment(it_ec)) {
            if (!dir_it->is_directory()) {
                continue;
            }
            std::int64_t large_x = 0;
            std::int64_t large_y = 0;
            if (!ParseCoordSuffix(dir_it->path().filename().string(), "L_", &large_x, &large_y)) {
                continue;
            }
            large_dirs.push_back({large_x, large_y, dir_it->path()});
        }
        std::sort(large_dirs.begin(), large_dirs.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.large_x != rhs.large_x ? lhs.large_x < rhs.large_x : lhs.large_y < rhs.large_y;
        });

        const auto width = static_cast<std::int64_t>(geometry_.config().large_chunk_width_chunks);
        // First and last cx covered by column lx, saturated at the int64 edges
        // so an extreme directory name cannot overflow the comparison.
        const auto column_min_x = [width](std::int64_t large_x) {
            std::int64_t min_x = 0;
            if (__builtin_mul_overflow(large_x, width, &min_x)) {
                return large_x < 0 ? std::numeric_limits<std::int64_t>::min()
                                   : std::numeric_limits<std::int64_t>::max();
            }
            return min_x;
        };
        const auto column_max_x = [width, &column_min_x](std::int64_t large_x) {
            const std::int64_t min_x = column_min_x(large_x);
            std::int64_t max_x = 0;
            if (__builtin_add_overflow(min_x, width - 1, &max_x)) {
                return std::numeric_limits<std::int64_t>::max();
            }
            return max_x;
        };

        std::size_t index = 0;
        while (index < large_dirs.size()) {
            const std::int64_t column = large_dirs[index].large_x;
            std::size_t column_end = index;
            while (column_end < large_dirs.size() && large_dirs[column_end].large_x == column) {
                ++column_end;
            }

            if (candidates->has_cursor() && column_max_x(column) < candidates->cursor().x) {
                // Every chunk in this column precedes the cursor.
                index = column_end;
                continue;
            }
            if (candidates->WindowClosedBefore(column_min_x(column))) {
                // The page is settled; later columns can only hold chunks
                // beyond the window, which the caller resumes into.
                candidates->MarkOverflowed();
                break;
            }

            for (; index < column_end; ++index) {
                stats_scan_large_dirs_listed_.fetch_add(1, std::memory_order_relaxed);
                std::error_code file_ec;
                for (std::filesystem::directory_iterator file_it(large_dirs[index].path, file_ec), file_end;
                     file_it != file_end && !file_ec;
                     file_it.increment(file_ec)) {
                    if (!file_it->is_regular_file()) {
                        continue;
                    }
                    const auto ext = file_it->path().extension();
                    if (ext != ".chk" && ext != ".wal") {
                        continue;
                    }
                    std::int64_t chunk_x = 0;
                    std::int64_t chunk_y = 0;
                    if (!ParseCoordSuffix(
                            file_it->path().stem().string(), "C_", &chunk_x, &chunk_y)) {
                        continue;
                    }
                    candidates->Insert(ChunkCoord{chunk_x, chunk_y});
                }
            }
        }
        return;
    }

    const auto span = static_cast<std::int64_t>(experimental_region_span_chunks_);
    std::error_code it_ec;
    for (std::filesystem::directory_iterator dir_it(data_dir_, it_ec), end;
         dir_it != end && !it_ec;
         dir_it.increment(it_ec)) {
        if (!dir_it->is_regular_file() || dir_it->path().extension() != ".rgn") {
            continue;
        }
        std::int64_t region_x = 0;
        std::int64_t region_y = 0;
        if (!ParseCoordSuffix(dir_it->path().stem().string(), "R_", &region_x, &region_y)) {
            continue;
        }
        std::lock_guard region_lock(RegionIoMutex());
        std::vector<std::uint8_t> bytes;
        try {
            bytes = LoadFile(dir_it->path());
        } catch (...) {
            std::error_code gone_ec;
            if (std::filesystem::exists(dir_it->path(), gone_ec) && !gone_ec) {
                throw;
            }
            continue;
        }
        const RegionChunkAddress addr{
            .region_x = region_x,
            .region_y = region_y,
            .local_x = 0,
            .local_y = 0,
            .slot_index = 0,
        };
        const auto region =
            ParseRegionFileImage(bytes, geometry_, addr, experimental_region_span_chunks_);
        for (std::uint32_t slot = 0; slot < region.slot_count; ++slot) {
            if (!RegionSlotPresent(region, slot)) {
                continue;
            }
            const auto local_x = static_cast<std::int64_t>(slot % experimental_region_span_chunks_);
            const auto local_y = static_cast<std::int64_t>(slot / experimental_region_span_chunks_);
            candidates->Insert(ChunkCoord{
                region_x * span + local_x,
                region_y * span + local_y,
            });
        }
    }

    // Region-mode WAL files live in split-style L_ directories.
    std::error_code wal_it_ec;
    for (std::filesystem::directory_iterator dir_it(data_dir_, wal_it_ec), end;
         dir_it != end && !wal_it_ec;
         dir_it.increment(wal_it_ec)) {
        if (!dir_it->is_directory()) {
            continue;
        }
        std::int64_t large_x = 0;
        std::int64_t large_y = 0;
        if (!ParseCoordSuffix(dir_it->path().filename().string(), "L_", &large_x, &large_y)) {
            continue;
        }
        std::error_code file_ec;
        for (std::filesystem::directory_iterator file_it(dir_it->path(), file_ec), file_end;
             file_it != file_end && !file_ec;
             file_it.increment(file_ec)) {
            if (!file_it->is_regular_file() || file_it->path().extension() != ".wal") {
                continue;
            }
            std::int64_t chunk_x = 0;
            std::int64_t chunk_y = 0;
            if (!ParseCoordSuffix(file_it->path().stem().string(), "C_", &chunk_x, &chunk_y)) {
                continue;
            }
            candidates->Insert(ChunkCoord{chunk_x, chunk_y});
        }
    }
}

ChunkScanPage ChunkStore::ScanPopulatedChunks(
    bool has_cursor,
    ChunkCoord cursor,
    std::size_t limit) {
    if (limit == 0 || limit > kMaxChunkScanLimit) {
        throw std::invalid_argument(
            "scan limit must be between 1 and " + std::to_string(kMaxChunkScanLimit));
    }

    // Candidate collection is bounded to the page size: each pass keeps only
    // the smallest limit+1 distinct coordinates after the cursor. Candidates
    // that turn out unpopulated (stale artifacts, cached-but-empty chunks)
    // shrink a pass below the page size; when that happens and the pass
    // overflowed its window, resume the walk after the window instead of
    // giving up, so large dirty worlds stay fully enumerable.
    ChunkScanPage page;
    bool pass_has_cursor = has_cursor;
    ChunkCoord pass_cursor = cursor;
    while (page.coords.size() <= limit) {
        ScanCandidateAccumulator candidates(
            pass_has_cursor, pass_cursor, limit + 1U - page.coords.size());
        CollectPopulatedCandidatesFromDisk(&candidates);
        {
            std::lock_guard global_lock(large_chunks_mutex_);
            for (const auto& [_, large_chunk] : large_chunks_) {
                std::lock_guard large_lock(large_chunk->mutex);
                for (const auto& [chunk_coord, chunk] : large_chunk->chunks) {
                    (void)chunk;
                    candidates.Insert(chunk_coord);
                }
            }
        }

        const bool overflowed = candidates.overflowed();
        const auto pass_coords = candidates.TakeSorted();
        for (const auto& coord : pass_coords) {
            if (!ReadPopulatedChunkStateNoCache(coord, nullptr, nullptr)) {
                continue;
            }
            if (page.coords.size() >= limit) {
                page.has_more = true;
                return page;
            }
            page.coords.push_back(coord);
        }
        if (!overflowed) {
            break;
        }
        pass_has_cursor = true;
        pass_cursor = pass_coords.back();
    }
    return page;
}

std::size_t ChunkStore::ChunkRangeEntryCostBytes() const noexcept {
    // Upper bound of one response entry: two signed 64-bit decimal
    // coordinates with separators (< 48 bytes), payload bit text, the '|'
    // separator, and presence bit text.
    return 48U + geometry_.ChunkPayloadBits() + 1U + geometry_.ChunkBlockCount();
}

void ChunkStore::AppendPopulatedChunkRangeEntry(
    const ChunkCoord& coord,
    std::size_t max_entries,
    const char* operation_name,
    std::vector<ChunkRangeEntry>* entries) {
    if (entries->size() < max_entries) {
        ChunkRangeEntry entry;
        entry.coord = coord;
        if (ReadPopulatedChunkStateNoCache(
                entry.coord, &entry.payload_bits, &entry.presence_bits)) {
            entries->push_back(std::move(entry));
        }
        return;
    }
    // Byte budget exhausted: probe populated-ness without extracting state
    // strings so the failure stays bounded.
    if (ReadPopulatedChunkStateNoCache(coord, nullptr, nullptr)) {
        throw std::out_of_range(
            std::string(operation_name) + " response exceeds the " +
            std::to_string(kMaxChunkRangeResponseBytes) +
            "-byte limit; request fewer chunks");
    }
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange(
    std::int64_t chunk_x0,
    std::int64_t chunk_y0,
    std::int64_t chunk_x1,
    std::int64_t chunk_y1) {
    if (chunk_x0 > chunk_x1 || chunk_y0 > chunk_y1) {
        throw std::invalid_argument("chunk range corners must satisfy x0<=x1 and y0<=y1");
    }
    // Spans are computed corner-minus-corner in unsigned arithmetic, which
    // is exact for the full int64 domain; comparing the span (width - 1)
    // against the limit avoids the +1 wrap-around at the extreme corners.
    const std::uint64_t span_x =
        static_cast<std::uint64_t>(chunk_x1) - static_cast<std::uint64_t>(chunk_x0);
    const std::uint64_t span_y =
        static_cast<std::uint64_t>(chunk_y1) - static_cast<std::uint64_t>(chunk_y0);
    if (span_x >= kMaxChunkRangeChunks || span_y >= kMaxChunkRangeChunks ||
        (span_x + 1U) * (span_y + 1U) > kMaxChunkRangeChunks) {
        throw std::invalid_argument(
            "chunk range must cover at most " + std::to_string(kMaxChunkRangeChunks) + " chunks");
    }
    const std::size_t max_entries = kMaxChunkRangeResponseBytes / ChunkRangeEntryCostBytes();

    std::vector<ChunkRangeEntry> entries;
    for (std::int64_t chunk_x = chunk_x0;; ++chunk_x) {
        for (std::int64_t chunk_y = chunk_y0;; ++chunk_y) {
            AppendPopulatedChunkRangeEntry(
                ChunkCoord{chunk_x, chunk_y}, max_entries, "CHUNKRANGE", &entries);
            if (chunk_y == chunk_y1) {
                break;
            }
        }
        if (chunk_x == chunk_x1) {
            break;
        }
    }
    return entries;
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRadius(
    std::int64_t center_x,
    std::int64_t center_y,
    std::int64_t radius_chunks) {
    if (radius_chunks < 0) {
        throw std::invalid_argument("chunk radius must be >= 0");
    }
    if (radius_chunks >= static_cast<std::int64_t>(kMaxChunkRangeChunks)) {
        throw std::invalid_argument(
            "chunk radius too large: the covered disc must contain at most " +
            std::to_string(kMaxChunkRangeChunks) + " chunks");
    }

    // Count the disc cells first so oversized requests fail before any read.
    const std::uint64_t radius_sq =
        static_cast<std::uint64_t>(radius_chunks) * static_cast<std::uint64_t>(radius_chunks);
    std::uint64_t disc_cells = 0;
    std::vector<std::int64_t> half_widths(static_cast<std::size_t>(radius_chunks) + 1U, 0);
    for (std::int64_t dy = 0; dy <= radius_chunks; ++dy) {
        const std::uint64_t dy_sq = static_cast<std::uint64_t>(dy) * static_cast<std::uint64_t>(dy);
        std::int64_t half_width = 0;
        while (static_cast<std::uint64_t>(half_width + 1) *
                       static_cast<std::uint64_t>(half_width + 1) +
                   dy_sq <=
               radius_sq) {
            ++half_width;
        }
        half_widths[static_cast<std::size_t>(dy)] = half_width;
        const std::uint64_t row_cells = 2U * static_cast<std::uint64_t>(half_width) + 1U;
        disc_cells += dy == 0 ? row_cells : 2U * row_cells;
    }
    if (disc_cells > kMaxChunkRangeChunks) {
        throw std::invalid_argument(
            "chunk radius covers " + std::to_string(disc_cells) +
            " chunks; the limit is " + std::to_string(kMaxChunkRangeChunks));
    }
    const std::size_t max_entries = kMaxChunkRangeResponseBytes / ChunkRangeEntryCostBytes();

    // Iterate in ascending (cx, cy) order. Cells whose coordinates would
    // fall outside the int64 domain do not exist and are skipped.
    std::vector<ChunkRangeEntry> entries;
    for (std::int64_t dx = -radius_chunks; dx <= radius_chunks; ++dx) {
        if (dx < 0 && center_x < std::numeric_limits<std::int64_t>::min() - dx) {
            continue;
        }
        if (dx > 0 && center_x > std::numeric_limits<std::int64_t>::max() - dx) {
            break;
        }
        const std::int64_t chunk_x = center_x + dx;
        const std::int64_t abs_dx = dx < 0 ? -dx : dx;
        // Largest |dy| with dx^2 + dy^2 <= r^2 equals the half width of the
        // perpendicular row, by symmetry of the disc.
        const std::int64_t dy_limit = half_widths[static_cast<std::size_t>(abs_dx)];
        for (std::int64_t dy = -dy_limit; dy <= dy_limit; ++dy) {
            if (dy < 0 && center_y < std::numeric_limits<std::int64_t>::min() - dy) {
                continue;
            }
            if (dy > 0 && center_y > std::numeric_limits<std::int64_t>::max() - dy) {
                break;
            }
            AppendPopulatedChunkRangeEntry(
                ChunkCoord{chunk_x, center_y + dy}, max_entries, "CHUNKRADIUS", &entries);
        }
    }
    return entries;
}

}  // namespace chunkdb
