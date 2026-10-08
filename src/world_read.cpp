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

[[nodiscard]] std::int64_t SaturatingMul(std::int64_t value, std::int64_t factor) noexcept {
    std::int64_t product = 0;
    if (__builtin_mul_overflow(value, factor, &product)) {
        return value < 0 ? std::numeric_limits<std::int64_t>::min()
                         : std::numeric_limits<std::int64_t>::max();
    }
    return product;
}

[[nodiscard]] std::int64_t SaturatingAdd(std::int64_t value, std::int64_t delta) noexcept {
    std::int64_t sum = 0;
    if (__builtin_add_overflow(value, delta, &sum)) {
        return delta < 0 ? std::numeric_limits<std::int64_t>::min()
                         : std::numeric_limits<std::int64_t>::max();
    }
    return sum;
}

// Inclusive chunk-coordinate rectangle covered by one large chunk. The corners
// saturate at the int64 edges so an extreme directory name or cache key cannot
// overflow the comparisons below.
struct LargeChunkBox {
    ChunkCoord min_coord;
    ChunkCoord max_coord;
};

[[nodiscard]] LargeChunkBox MakeLargeChunkBox(
    std::int64_t large_x,
    std::int64_t large_y,
    std::int64_t width,
    std::int64_t height) noexcept {
    const std::int64_t min_x = SaturatingMul(large_x, width);
    const std::int64_t min_y = SaturatingMul(large_y, height);
    return LargeChunkBox{
        ChunkCoord{min_x, min_y},
        ChunkCoord{SaturatingAdd(min_x, width - 1), SaturatingAdd(min_y, height - 1)},
    };
}

// Smallest coordinate inside `box` that is strictly after `cursor` in scan
// order, or nullopt when every coordinate of the box is at or before it.
// Scan order is x-major, so a box that starts before the cursor can still hold
// candidates in its later x columns: this is what lets a narrow world (one
// large-chunk column wide) prune by y instead of visiting the whole column.
[[nodiscard]] std::optional<ChunkCoord> LowestBoxCoordAfter(
    const LargeChunkBox& box,
    const ChunkCoord& cursor) noexcept {
    if (cursor.x < box.min_coord.x) {
        return box.min_coord;
    }
    if (cursor.x > box.max_coord.x) {
        return std::nullopt;
    }
    if (cursor.y < box.min_coord.y) {
        return ChunkCoord{cursor.x, box.min_coord.y};
    }
    if (cursor.y < box.max_coord.y) {
        return ChunkCoord{cursor.x, cursor.y + 1};
    }
    if (cursor.x == box.max_coord.x) {
        return std::nullopt;
    }
    return ChunkCoord{cursor.x + 1, box.min_coord.y};
}

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

    // True when the window is full and every kept coordinate is below
    // `coord`: nothing at or after `coord` can still enter the window, so a
    // caller visiting large chunks in scan order may skip (or, where the
    // argument is monotone, stop). The caller must then MarkOverflowed().
    [[nodiscard]] bool WindowClosedAt(const ChunkCoord& coord) const noexcept {
        return kept_.size() >= bound_ && !ChunkCoordLess(coord, *std::prev(kept_.end()));
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

std::uint64_t ChunkStore::ScanCachedLargeChunksMergedForTests() const noexcept {
    return stats_scan_cached_large_chunks_merged_.load(std::memory_order_relaxed);
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
    ChunkRangeEntry* out,
    bool with_vars) {
    const auto copy_cached = [out, with_vars](const RegularChunk& chunk) {
        out->payload = chunk.payload;
        out->presence_bitmap = chunk.presence_bitmap;
        out->version = chunk.version;
        if (with_vars) {
            out->vars = chunk.vars;
        }
    };
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
            if (out != nullptr) {
                copy_cached(*loaded);
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
        std::string vars_problem;
        const bool populated = ReadPopulatedChunkStateFromDisk(chunk_coord, out, with_vars, &vars_problem);
        if (TryGetLoadedChunk(chunk_coord) == nullptr &&
            stats_eviction_forced_wal_flushes_.load(std::memory_order_acquire) ==
                eviction_flushes_before) {
            if (!vars_problem.empty()) {
                // No writer raced the read, so the files themselves are
                // damaged; a chunk load refuses them the same way.
                throw std::runtime_error(
                    "chunk (" + std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) +
                    ") has inconsistent text and bytes values: " + vars_problem);
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
    std::shared_lock lock(regular_chunk->mutex);
    if (!ChunkPresent(regular_chunk->presence_bitmap)) {
        return false;
    }
    if (out != nullptr) {
        copy_cached(*regular_chunk);
    }
    return true;
}

bool ChunkStore::ReadPopulatedChunkStateFromDisk(
    const ChunkCoord& chunk_coord,
    ChunkRangeEntry* out,
    bool with_vars,
    std::string* vars_problem) {
    if (access_mode_ == AccessMode::kReadOnly) {
        // A writer in another process changes these files at any time: only
        // the snapshot-bracketed load pairs an image and a WAL that existed
        // together, and it also hides a frame a rollback intent rejects.
        auto loaded = LoadChunkPayload(chunk_coord);
        if (!ChunkPresent(loaded.presence_bitmap)) {
            return false;
        }
        if (out != nullptr) {
            out->payload = std::move(loaded.payload);
            out->presence_bitmap = std::move(loaded.presence_bitmap);
            out->version = loaded.revision;
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
    // The schema version of the state; 0 while it is the empty state.
    std::uint64_t schema_version = 0;

    if (std::filesystem::exists(data_path)) {
        try {
            const auto bytes = LoadFile(data_path);
            auto image = ParseChunkImage(bytes, geometry_, chunk_coord, store_id_, features_);
            payload = std::move(image.payload);
            presence = std::move(image.presence_bitmap);
            vars = std::move(image.vars);
            base_revision = image.revision;
            revision = image.revision;
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
            const auto replay = ReplayWal(
                wal_bytes, geometry_, chunk_coord, store_id_, features_, base_revision, schema_version, &payload,
                &presence, &vars);
            schema_version = geometry_.layout().schema().version;
            if (replay.applied_frames > 0) {
                revision = replay.revision;
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

    if (!ChunkPresent(presence)) {
        return false;
    }
    BringToCurrentSchema(geometry_, schema_version, presence, &payload, &vars);
    if (out != nullptr) {
        out->payload = std::move(payload);
        out->presence_bitmap = std::move(presence);
        out->version = revision;
        if (with_vars) {
            out->vars = std::move(vars);
        }
    }
    return true;
}

void ChunkStore::MergeCachedCandidates(
    const std::shared_ptr<LargeChunk>& large_chunk,
    ScanCandidateAccumulator* candidates) const {
    stats_scan_cached_large_chunks_merged_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard large_lock(large_chunk->mutex);
    for (const auto& [chunk_coord, chunk] : large_chunk->chunks) {
        (void)chunk;
        candidates->Insert(chunk_coord);
    }
}

void ChunkStore::EnsureScanCatalog() const {
    std::lock_guard lock(large_chunks_mutex_);
    const bool external_writer = access_mode_ == AccessMode::kReadOnly || allow_multiple_processes_;
    const auto generation = external_writer
        ? ReadSnapshotGenerationForScan(
              snapshot_generation_path_, snapshot_generation_record_seen_)
        : 0;
    if (scan_catalog_ready_ && !allow_multiple_processes_ &&
        (!external_writer || (generation != 0 && (generation & 1U) == 0 &&
                              generation == scan_catalog_generation_))) {
        return;
    }

    // Build a replacement before publishing it. A failed directory read must
    // not leave a partially populated catalog that later pages trust.
    decltype(scan_catalog_) catalog;
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        std::int64_t x = 0;
        std::int64_t y = 0;
        if (!ParseCoordSuffix(entry.path().filename().string(), "L_", &x, &y)) {
            continue;
        }
        std::error_code ec;
        if (entry.is_directory(ec)) {
            catalog.try_emplace(std::make_pair(x, y), entry.path());
        } else if (ec && ec != std::errc::no_such_file_or_directory) {
            throw std::filesystem::filesystem_error("scan directory", entry.path(), ec);
        }
    }
    // A chunk can be in a pending WAL batch with no directory yet. Registry
    // creation/retirement is excluded by this same mutex; cache before disk
    // visits below covers eviction between the two sources.
    for (const auto& [coord, chunk] : large_chunks_) {
        (void)chunk;
        catalog.try_emplace(std::make_pair(coord.x, coord.y), LargeChunkDirectory(data_dir_, coord));
    }
    scan_catalog_.swap(catalog);
    scan_catalog_generation_ = generation;
    scan_catalog_ready_ = true;
    stats_scan_catalog_builds_.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t ChunkStore::ScanCatalogBuildsForTests() const noexcept {
    return stats_scan_catalog_builds_.load(std::memory_order_relaxed);
}

void ChunkStore::CollectScanCandidates(ScanCandidateAccumulator* candidates) const {
    // Both candidate sources — the on-disk `.chk`/`.wal` artifacts and the
    // resident cache — are keyed by large chunk, so they are visited together
    // in scan order and pruned by the same cursor/window tests. Merging the
    // cache per large chunk instead of globally is what keeps a warm page from
    // costing O(resident chunks) (docs/FORMAT_V2_DESIGN.md section 7, step 2).

    const auto width = static_cast<std::int64_t>(geometry_.config().large_chunk_width_chunks);
    const auto height = static_cast<std::int64_t>(geometry_.config().large_chunk_height_chunks);

    // Verdict for one large chunk, given the cursor and the current window.
    enum class Visit { kSkip, kVisit, kStop };
    const auto classify = [&](std::int64_t large_x, std::int64_t large_y) {
        const LargeChunkBox box = MakeLargeChunkBox(large_x, large_y, width, height);
        // Boxes are visited in ascending (lx, ly), so `box.min_coord.x` never
        // decreases: once no coordinate in this column or later can enter the
        // window, the whole remaining walk is dead.
        if (candidates->WindowClosedAt(
                ChunkCoord{box.min_coord.x, std::numeric_limits<std::int64_t>::min()})) {
            candidates->MarkOverflowed();
            return Visit::kStop;
        }
        ChunkCoord lowest = box.min_coord;
        if (candidates->has_cursor()) {
            const auto after = LowestBoxCoordAfter(box, candidates->cursor());
            if (!after.has_value()) {
                // Entirely at or before the cursor: nothing is skipped.
                return Visit::kSkip;
            }
            lowest = *after;
        }
        if (candidates->WindowClosedAt(lowest)) {
            // Its candidates all sort beyond the window; the caller resumes
            // into them from the window's end.
            candidates->MarkOverflowed();
            return Visit::kSkip;
        }
        return Visit::kVisit;
    };

    EnsureScanCatalog();
    using Key = std::pair<std::int64_t, std::int64_t>;
    Key next{std::numeric_limits<std::int64_t>::min(),
             std::numeric_limits<std::int64_t>::min()};
    if (candidates->has_cursor()) {
        next.first = geometry_.ChunkToLarge(candidates->cursor()).x;
    }
    bool exclusive = false;
    for (;;) {
        Key key;
        std::filesystem::path path;
        std::shared_ptr<LargeChunk> cached_chunk;
        {
            std::lock_guard lock(large_chunks_mutex_);
            const auto it = exclusive ? scan_catalog_.upper_bound(next)
                                      : scan_catalog_.lower_bound(next);
            if (it == scan_catalog_.end()) {
                break;
            }
            key = it->first;
            path = it->second;
            const auto resident = large_chunks_.find({key.first, key.second});
            if (resident != large_chunks_.end()) {
                cached_chunk = resident->second;
            }
        }
        next = key;
        exclusive = true;
        const Visit verdict = classify(key.first, key.second);
        if (verdict == Visit::kStop) {
            return;
        }
        if (verdict == Visit::kSkip) {
            continue;
        }
        if (cached_chunk != nullptr) {
            MergeCachedCandidates(cached_chunk, candidates);
        }
        std::error_code ec;
        std::filesystem::directory_iterator it(path, ec), end;
        if (ec == std::errc::no_such_file_or_directory) {
            continue;  // cache-only or concurrently garbage-collected
        }
        if (ec) {
            throw std::filesystem::filesystem_error("scan directory", path, ec);
        }
        stats_scan_large_dirs_listed_.fetch_add(1, std::memory_order_relaxed);
        for (; it != end; it.increment(ec)) {
            if (ec) {
                throw std::filesystem::filesystem_error("scan directory", path, ec);
            }
            const auto ext = it->path().extension();
            if (ext != ".chk" && ext != ".wal") {
                continue;
            }
            std::error_code type_ec;
            if (!it->is_regular_file(type_ec)) {
                if (type_ec && type_ec != std::errc::no_such_file_or_directory) {
                    throw std::filesystem::filesystem_error("scan file", it->path(), type_ec);
                }
                continue;
            }
            std::int64_t x = 0;
            std::int64_t y = 0;
            if (ParseCoordSuffix(it->path().stem().string(), "C_", &x, &y)) {
                candidates->Insert({x, y});
            }
        }
        if (ec) {
            throw std::filesystem::filesystem_error("scan directory", path, ec);
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
    // A read-only store checks, after listing, that its directory still
    // holds its store: a table dropped and created again lists as empty.
    // Store ids never repeat, so a match then covers the whole listing.
    const auto finish = [&](ChunkScanPage& result) -> ChunkScanPage {
        if (access_mode_ == AccessMode::kReadOnly) {
            RequireStoreStillOnDisk();
        }
        return std::move(result);
    };

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
        CollectScanCandidates(&candidates);

        const bool overflowed = candidates.overflowed();
        const auto pass_coords = candidates.TakeSorted();
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
        if (!overflowed) {
            break;
        }
        pass_has_cursor = true;
        pass_cursor = pass_coords.back();
    }
    return finish(page);
}

std::size_t ChunkStore::ChunkRangeEntryCostBytes() const noexcept {
    // Upper bound of one response entry: the "<cx> <cy>" item with two signed
    // 64-bit decimal coordinates and its framing (at most 48 bytes), then the
    // chunk bytes with their framing (at most 25 bytes) and, in the ZRLE
    // form, the codec overhead (at most kZrleMaxOverheadBytes = 11).
    static_assert(48U + 25U + kZrleMaxOverheadBytes <= 96U);
    return 96U + geometry_.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry_);
}

void ChunkStore::AppendPopulatedChunkRangeEntry(
    const ChunkCoord& coord,
    std::size_t max_entries,
    const char* operation_name,
    bool with_vars,
    std::size_t* vars_bytes,
    std::vector<ChunkRangeEntry>* entries) {
    const auto too_large = [operation_name] {
        return std::out_of_range(
            std::string(operation_name) + " response exceeds the " + std::to_string(kMaxChunkRangeResponseBytes) +
            "-byte limit; request fewer chunks");
    };
    if (entries->size() < max_entries) {
        ChunkRangeEntry entry;
        entry.coord = coord;
        if (ReadPopulatedChunkStateNoCache(entry.coord, &entry, with_vars)) {
            if (with_vars) {
                *vars_bytes += entry.vars.encoded_size();
                if ((entries->size() + 1U) * ChunkRangeEntryCostBytes() + *vars_bytes > kMaxChunkRangeResponseBytes) {
                    throw too_large();
                }
            }
            entries->push_back(std::move(entry));
        }
        return;
    }
    // Byte budget exhausted: probe populated-ness without extracting state
    // strings so the failure stays bounded.
    if (ReadPopulatedChunkStateNoCache(coord, nullptr, false)) {
        throw too_large();
    }
}

std::vector<ChunkRangeEntry> ChunkStore::ReadChunkRange(
    std::int64_t chunk_x0,
    std::int64_t chunk_y0,
    std::int64_t chunk_x1,
    std::int64_t chunk_y1,
    bool with_vars) {
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
    std::size_t vars_bytes = 0;
    for (std::int64_t chunk_x = chunk_x0;; ++chunk_x) {
        for (std::int64_t chunk_y = chunk_y0;; ++chunk_y) {
            AppendPopulatedChunkRangeEntry(
                ChunkCoord{chunk_x, chunk_y}, max_entries, "CHUNKRANGE", with_vars, &vars_bytes, &entries);
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
    std::int64_t radius_chunks,
    bool with_vars) {
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
    std::size_t vars_bytes = 0;
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
                ChunkCoord{chunk_x, center_y + dy}, max_entries, "CHUNKRADIUS", with_vars, &vars_bytes, &entries);
        }
    }
    return entries;
}

}  // namespace chunkdb
