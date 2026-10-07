#include <algorithm>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "history_read.hpp"
#include "history_store.hpp"

namespace chunkdb {

namespace {

[[nodiscard]] history::Position LowerBound(const std::optional<HistoryCursor>& after) noexcept {
    if (!after.has_value()) {
        return history::Position{.revision = 0, .block = history::Position::kBeforeBlocks};
    }
    return history::Position{
        .revision = after->revision,
        .block = after->block_index.has_value() ? std::int64_t{*after->block_index} : history::Position::kAfterBlocks,
    };
}

[[nodiscard]] history::Position UpperBound(const std::optional<HistoryCursor>& before) noexcept {
    if (!before.has_value()) {
        return history::Position{
            .revision = std::numeric_limits<std::uint64_t>::max(), .block = history::Position::kAfterBlocks};
    }
    return history::Position{
        .revision = before->revision,
        .block = before->block_index.has_value() ? std::int64_t{*before->block_index} : history::Position::kBeforeBlocks,
    };
}

// A bound as the cursor that continues past it: a position between blocks
// becomes a cursor without a block.
[[nodiscard]] HistoryCursor CursorAt(const history::Position& position) noexcept {
    HistoryCursor cursor{.revision = position.revision};
    if (position.block >= 0 && position.block < history::Position::kAfterBlocks) {
        cursor.block_index = static_cast<std::uint32_t>(position.block);
    }
    return cursor;
}

[[nodiscard]] std::optional<HistoryBlockValue> ToPublic(const std::optional<history::BlockValue>& value) {
    if (!value.has_value()) {
        return std::nullopt;
    }
    return HistoryBlockValue{.bits = value->bits, .extra = value->extra};
}

}  // namespace

void ChunkStore::ArmHistoryReadPauseForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    history_read_pause_armed_ = true;
    history_read_pause_reached_ = false;
    history_read_pause_resumed_ = false;
}

bool ChunkStore::WaitForHistoryReadPauseForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    return durability_hook_cv_.wait_for(
        lock, std::chrono::seconds(5), [this] { return history_read_pause_reached_; });
}

void ChunkStore::ResumeHistoryReadForTests() {
    std::lock_guard lock(durability_hook_mutex_);
    history_read_pause_resumed_ = true;
    durability_hook_cv_.notify_all();
}

void ChunkStore::PauseHistoryReadForTests() {
    std::unique_lock lock(durability_hook_mutex_);
    if (!history_read_pause_armed_) {
        return;
    }
    history_read_pause_reached_ = true;
    durability_hook_cv_.notify_all();
    durability_hook_cv_.wait(lock, [this] { return history_read_pause_resumed_; });
    history_read_pause_armed_ = false;
}

void ChunkStore::SetHistoryScanBudgetForTests(std::size_t bytes) noexcept {
    history_scan_budget_bytes_.store(bytes, std::memory_order_relaxed);
}

std::mutex& ChunkStore::HistoryMutexFor(const ChunkCoord& chunk_coord) const noexcept {
    return history_mutexes_[ChunkCoordHash{}(chunk_coord) % history_mutexes_.size()];
}

ChunkStore::ChunkHistorySnapshot ChunkStore::ChunkHistoryForReadLocked(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk) {
    const auto read_if_present = [](const std::filesystem::path& path) -> std::optional<std::vector<std::uint8_t>> {
        std::error_code ec;
        const bool present = std::filesystem::exists(path, ec);
        if (ec) {
            throw std::runtime_error("cannot inspect " + path.string() + ": " + ec.message());
        }
        if (!present) {
            return std::nullopt;
        }
        return LoadFile(path);
    };

    if (access_mode_ == AccessMode::kReadOnly) {
        // One stable snapshot of the image and the WAL first, the segments
        // after: a writer appends to history before it replaces the image
        // and removes the WAL, so the segments hold at least what the
        // snapshot lacks.
        const auto files = ReadOnlyChunkFilesFor(chunk_coord);
        auto segments = std::make_shared<history::ChunkHistory>(history_files_->Load(chunk_coord, false));
        auto pending = std::make_shared<history::PendingHistory>();
        pending->derivation = history::DeriveHistory(
            geometry_, chunk_coord, store_id_, features_, files.image.has_value() ? &*files.image : nullptr,
            files.replay_bytes.empty() ? nullptr : &files.replay_bytes, history_start_, segments->last_revision(),
            /*allow_crash_tail=*/true, files.whole_bytes);
        pending->after = segments->last_revision();
        return ChunkHistorySnapshot{.segments = std::move(segments), .pending = std::move(pending)};
    }

    std::shared_ptr<const history::ChunkHistory> segments;
    {
        std::lock_guard guard(HistoryMutexFor(chunk_coord));
        if (chunk->history == nullptr) {
            chunk->history = std::make_shared<history::ChunkHistory>(history_files_->Load(chunk_coord, true));
        }
        segments = chunk->history;
        const auto& cached = chunk->history_pending;
        if (cached != nullptr && cached->version == chunk->version && cached->after == segments->last_revision()) {
            return ChunkHistorySnapshot{.segments = segments, .pending = cached};
        }
    }
    // The WAL the next checkpoint would read: the file, then the batch not
    // yet appended to it (with the header a first append writes).
    const auto image = read_if_present(ChunkDataPath(data_dir_, geometry_, chunk_coord));
    auto wal = read_if_present(ChunkWalPath(data_dir_, geometry_, chunk_coord));
    if (!chunk->wal_batch.empty()) {
        if (!wal.has_value() || wal->empty()) {
            wal = BuildWalHeader(chunk_coord, store_id_, features_);
        }
        wal->insert(wal->end(), chunk->wal_batch.begin(), chunk->wal_batch.end());
    }
    auto pending = std::make_shared<history::PendingHistory>();
    pending->derivation = history::DeriveHistory(
        geometry_, chunk_coord, store_id_, features_, image.has_value() ? &*image : nullptr,
        wal.has_value() ? &*wal : nullptr, history_start_, segments->last_revision(), /*allow_crash_tail=*/false);
    const history::ChunkState memory{
        .state = BuildChunkStateBytes(geometry_, chunk->payload, chunk->presence_bitmap),
        .extra = chunk->extra,
    };
    if (!(pending->derivation.final_state == memory)) {
        throw history::HistoryDamagedError(
            "the image and WAL of chunk (" + std::to_string(chunk_coord.x) + "," + std::to_string(chunk_coord.y) +
            ") do not replay to the state the store holds");
    }
    pending->version = chunk->version;
    pending->after = segments->last_revision();
    {
        std::lock_guard guard(HistoryMutexFor(chunk_coord));
        chunk->history_pending = pending;
    }
    return ChunkHistorySnapshot{.segments = std::move(segments), .pending = std::move(pending)};
}

PastChunkState ChunkStore::ReadChunkAt(std::int64_t chunk_x, std::int64_t chunk_y, const HistoryPoint& at) {
    if (!history_) {
        throw std::invalid_argument("history is not enabled on this table (set its history option)");
    }
    if (at.revision.has_value() == at.time_ms.has_value()) {
        throw std::invalid_argument("a history point is a revision or a time");
    }
    const auto not_retained = [](std::uint64_t start, const std::string& what) {
        return HistoryNotRetainedError(start, what + " is before the history this table keeps, from revision " +
                                                  std::to_string(start));
    };
    if (at.revision.has_value()) {
        // Every mutation below the next revision is settled once the chunk
        // is locked; a later one could still change the answer.
        if (access_mode_ != AccessMode::kReadOnly) {
            const std::uint64_t next = version_clock_.load(std::memory_order_acquire);
            if (*at.revision >= next) {
                throw std::out_of_range(
                    "AT " + std::to_string(*at.revision) + " is not below the next revision (" + std::to_string(next) +
                    ")");
            }
        }
        if (*at.revision < history_start_) {
            throw not_retained(history_start_, "revision " + std::to_string(*at.revision));
        }
    } else {
        if (*at.time_ms >= UnixMillisNow()) {
            throw std::out_of_range("AT TIME " + std::to_string(*at.time_ms) + " is not in the past");
        }
        if (*at.time_ms < history_start_time_ms_) {
            throw not_retained(history_start_, "time " + std::to_string(*at.time_ms));
        }
    }

    const ChunkCoord coord{chunk_x, chunk_y};
    const auto chunk = GetOrLoadRegularChunk(coord);
    std::shared_lock lock(chunk->mutex);
    const auto snapshot = ChunkHistoryForReadLocked(coord, chunk);
    const auto& segments = *snapshot.segments;
    if (const std::uint64_t trimmed = segments.trimmed_before(); trimmed != 0U) {
        if (at.revision.has_value() ? *at.revision < trimmed : *at.time_ms < segments.segments.front().base_time_ms) {
            throw not_retained(trimmed, at.revision.has_value() ? "revision " + std::to_string(*at.revision)
                                                                 : "time " + std::to_string(*at.time_ms));
        }
    }
    const history::ChunkHistorySource source{
        .files = history_files_.get(),
        .chunk = coord,
        .segments = &segments,
        .pending = &snapshot.pending->derivation.mutations,
        .pending_base = &snapshot.pending->derivation.base,
                .pending_base_revision = snapshot.pending->derivation.base_revision,
                .pending_base_time_ms = snapshot.pending->derivation.base_time_ms,
    };
    const auto& current = snapshot.pending->derivation.final_state;
    const auto state = history::StateAt(geometry_, source, current, at.revision, at.time_ms);
    PastChunkState past;
    SplitChunkStateBytes(geometry_, state.state, &past.payload, &past.presence_bitmap);
    past.extra = state.extra;
    return past;
}

HistoryPage ChunkStore::ReadHistory(const HistoryQuery& query) {
    if (!history_) {
        throw std::invalid_argument("history is not enabled on this table (set its history option)");
    }
    if (query.limit == 0U || query.limit > kMaxHistoryLimit) {
        throw std::invalid_argument("history limit must be between 1 and " + std::to_string(kMaxHistoryLimit));
    }
    const auto& first = query.first_chunk;
    const auto& last = query.last_chunk;
    if (first.x > last.x || first.y > last.y) {
        throw std::invalid_argument("history area corners must satisfy x0<=x1 and y0<=y1");
    }
    const std::uint64_t span_x = static_cast<std::uint64_t>(last.x) - static_cast<std::uint64_t>(first.x);
    const std::uint64_t span_y = static_cast<std::uint64_t>(last.y) - static_cast<std::uint64_t>(first.y);
    if (span_x >= kMaxHistoryChunks || span_y >= kMaxHistoryChunks ||
        (span_x + 1U) * (span_y + 1U) > kMaxHistoryChunks) {
        throw std::invalid_argument(
            "history area must cover at most " + std::to_string(kMaxHistoryChunks) + " chunks");
    }
    if (query.block_index.has_value() &&
        (span_x != 0U || span_y != 0U || *query.block_index >= geometry_.ChunkBlockCount())) {
        throw std::invalid_argument("a history block must lie in the one chunk read");
    }
    if (query.since_ms.has_value() && query.until_ms.has_value() && *query.since_ms > *query.until_ms) {
        throw std::invalid_argument("history SINCE must not be after UNTIL");
    }

    // Only revisions issued before the read began: a mutation holds its
    // chunk's lock from the moment it takes its revision until it is
    // committed or rolled back, so each of them is settled by the time the
    // read locks that chunk, while a later one may still commit below a
    // revision the read returns.
    const std::uint64_t horizon = access_mode_ == AccessMode::kReadOnly
                                      ? std::numeric_limits<std::uint64_t>::max()
                                      : version_clock_.load(std::memory_order_acquire);
    history::EventWindow window{
        .lo = LowerBound(query.after),
        .hi = std::min(
            UpperBound(query.before),
            history::Position{.revision = horizon, .block = history::Position::kBeforeBlocks}),
        .since_ms = query.since_ms,
        .until_ms = query.until_ms,
        .tag = query.tag,
        .block = query.block_index,
        .descending = query.descending,
        .limit = query.limit,
    };
    if (!(window.lo < window.hi)) {
        return HistoryPage{};
    }

    struct ChunkResult {
        ChunkCoord chunk;
        history::ChunkEvents events;
    };
    std::vector<ChunkResult> results;
    history::ScanBudget budget{.remaining = history_scan_budget_bytes_.load(std::memory_order_relaxed)};
    // The newest revision retention removed in a chunk whose removed events
    // could fall in the window.
    std::uint64_t boundary = 0;
    for (std::int64_t chunk_x = first.x;; ++chunk_x) {
        for (std::int64_t chunk_y = first.y;; ++chunk_y) {
            const ChunkCoord coord{chunk_x, chunk_y};
            const auto chunk = GetOrLoadRegularChunk(coord);
            std::shared_lock lock(chunk->mutex);
            const auto snapshot = ChunkHistoryForReadLocked(coord, chunk);
            const auto& segments = *snapshot.segments;
            const std::uint64_t trimmed = segments.trimmed_before();
            if (trimmed != 0U &&
                (!query.since_ms.has_value() || *query.since_ms <= segments.segments.front().base_time_ms)) {
                boundary = std::max(boundary, trimmed);
            }
            const history::ChunkHistorySource source{
                .files = history_files_.get(),
                .chunk = coord,
                .segments = &segments,
                .pending = &snapshot.pending->derivation.mutations,
                .pending_base = &snapshot.pending->derivation.base,
                .pending_base_revision = snapshot.pending->derivation.base_revision,
                .pending_base_time_ms = snapshot.pending->derivation.base_time_ms,
            };
            results.push_back(ChunkResult{
                .chunk = coord,
                .events = history::CollectChunkEvents(geometry_, source, window, &budget),
            });
            lock.unlock();
            if (results.size() == 1U) {
                PauseHistoryReadForTests();
            }
            if (chunk_y == last.y) {
                break;
            }
        }
        if (chunk_x == last.x) {
            break;
        }
    }

    // Retention: events at or below the boundary may be gone, so a page
    // that would need them fails. Oldest first that is any window starting
    // below it; newest first, a page returns what lies above it and the
    // next one fails.
    const history::Position kept_from{.revision = boundary, .block = history::Position::kAfterBlocks};
    const bool below_boundary = boundary != 0U && window.lo < kept_from;
    const auto not_retained = [&] {
        return HistoryNotRetainedError(
            boundary, "history before revision " + std::to_string(boundary + 1U) +
                          " was removed by retention; it is kept from revision " + std::to_string(boundary));
    };
    if (below_boundary && (!query.descending || !(kept_from < window.hi))) {
        throw not_retained();
    }

    struct Candidate {
        history::Position position;
        std::size_t result = 0;
        std::size_t event = 0;
    };
    std::vector<Candidate> candidates;
    bool more = false;
    std::optional<history::Position> frontier;
    for (std::size_t r = 0; r < results.size(); ++r) {
        auto& chunk_events = results[r].events;
        bool reached_boundary = false;
        for (std::size_t e = 0; e < chunk_events.events.size(); ++e) {
            const auto position = chunk_events.events[e].position();
            if (below_boundary && !(kept_from < position)) {
                reached_boundary = true;
                continue;
            }
            candidates.push_back(Candidate{.position = position, .result = r, .event = e});
        }
        if (reached_boundary) {
            continue;
        }
        more = more || chunk_events.more;
        if (chunk_events.frontier.has_value() && !(below_boundary && !(kept_from < *chunk_events.frontier))) {
            const auto& f = *chunk_events.frontier;
            frontier = !frontier.has_value()                    ? f
                       : query.descending ? std::max(*frontier, f)
                                          : std::min(*frontier, f);
        }
    }
    std::sort(candidates.begin(), candidates.end(), [&](const Candidate& lhs, const Candidate& rhs) {
        return query.descending ? rhs.position < lhs.position : lhs.position < rhs.position;
    });
    if (frontier.has_value()) {
        // Past the frontier a chunk was not read: stop there.
        const auto past = std::find_if(candidates.begin(), candidates.end(), [&](const Candidate& c) {
            return query.descending ? c.position < *frontier : *frontier < c.position;
        });
        candidates.erase(past, candidates.end());
    }

    HistoryPage page;
    std::size_t bytes = 0;
    const std::size_t block_bits = geometry_.config().block_bits;
    for (const auto& candidate : candidates) {
        if (page.events.size() == query.limit) {
            more = true;
            break;
        }
        auto& event = results[candidate.result].events.events[candidate.event];
        // What the reply takes as text: bits, hex extra data and tag.
        std::size_t cost = 128U + 2U * event.tag.size();
        for (const auto* value : {&event.before, &event.after}) {
            if (value->has_value()) {
                cost += block_bits + ((*value)->extra.has_value() ? 16U + 2U * (*value)->extra->bytes.size() : 0U);
            }
        }
        if (!page.events.empty() && bytes + cost > kMaxChunkRangeResponseBytes) {
            more = true;
            break;
        }
        bytes += cost;
        page.events.push_back(HistoryEvent{
            .revision = event.revision,
            .time_ms = event.time_ms,
            .chunk = results[candidate.result].chunk,
            .block_index = event.block_index,
            .before = ToPublic(event.before),
            .after = ToPublic(event.after),
            .tag = std::move(event.tag),
        });
    }
    if (!page.events.empty() && (more || frontier.has_value())) {
        const auto& last_event = page.events.back();
        page.next = HistoryCursor{.revision = last_event.revision, .block_index = last_event.block_index};
    } else if (frontier.has_value()) {
        page.next = CursorAt(*frontier);
    } else if (below_boundary) {
        if (page.events.empty()) {
            throw not_retained();
        }
        page.next = HistoryCursor{.revision = boundary};
    }
    return page;
}

}  // namespace chunkdb
