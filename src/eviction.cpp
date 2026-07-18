#include "eviction.hpp"

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>

#include "wal_stream_pool.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

[[nodiscard]] std::size_t EvictionLowerWatermark(std::size_t max_loaded_chunks) {
    const std::size_t hysteresis = std::max<std::size_t>(256, max_loaded_chunks / 16);
    if (hysteresis >= max_loaded_chunks) {
        return 1;
    }
    return max_loaded_chunks - hysteresis;
}
std::uint64_t ChunkStore::EvictionSnapshotBuildCountForTests() const noexcept {
    return stats_eviction_snapshot_builds_.load(std::memory_order_relaxed);
}

std::uint64_t ChunkStore::EvictionRefillLargeChunkScanCountForTests() const noexcept {
    return stats_eviction_refill_large_chunk_scans_.load(std::memory_order_relaxed);
}

std::size_t ChunkStore::EvictionLargeChunkRingSizeForTests() const noexcept {
    std::lock_guard lock(large_chunks_mutex_);
    return eviction_large_chunk_ring_.size();
}

std::uint64_t ChunkStore::EvictionPostPassLargeChunkCheckCountForTests() const noexcept {
    return stats_eviction_post_pass_large_chunk_checks_.load(std::memory_order_relaxed);
}

void ChunkStore::ClearEvictionCandidatesForTests() {
    std::lock_guard lock(eviction_state_mutex_);
    eviction_candidates_.clear();
    eviction_cursor_ = 0;
}

void ChunkStore::RegisterEvictionCandidate(
    const LargeChunkCoord& large_coord,
    const ChunkCoord& chunk_coord,
    std::uint64_t recorded_tick) {
    std::lock_guard lock(eviction_state_mutex_);
    if (eviction_cursor_ > 0 &&
        (eviction_cursor_ > 8192 || eviction_cursor_ * 2 > eviction_candidates_.size())) {
        eviction_candidates_.erase(
            eviction_candidates_.begin(),
            eviction_candidates_.begin() + static_cast<std::ptrdiff_t>(eviction_cursor_));
        eviction_cursor_ = 0;
    }

    eviction_candidates_.push_back(EvictionCandidate{
        .large_coord = large_coord,
        .chunk_coord = chunk_coord,
        .recorded_tick = recorded_tick,
    });
}

void ChunkStore::RemoveLargeChunkFromEvictionRing(const LargeChunkCoord& large_coord) {
    if (eviction_large_chunk_ring_.empty()) {
        eviction_large_chunk_cursor_ = 0;
        return;
    }

    std::size_t write_index = 0;
    std::size_t removed_before_cursor = 0;
    const std::size_t old_size = eviction_large_chunk_ring_.size();
    for (std::size_t read_index = 0; read_index < old_size; ++read_index) {
        if (eviction_large_chunk_ring_[read_index] == large_coord) {
            if (read_index < eviction_large_chunk_cursor_) {
                ++removed_before_cursor;
            }
            continue;
        }
        if (write_index != read_index) {
            eviction_large_chunk_ring_[write_index] = eviction_large_chunk_ring_[read_index];
        }
        ++write_index;
    }

    if (write_index == old_size) {
        return;
    }

    eviction_large_chunk_ring_.resize(write_index);
    if (eviction_large_chunk_ring_.empty()) {
        eviction_large_chunk_cursor_ = 0;
        return;
    }

    if (removed_before_cursor >= eviction_large_chunk_cursor_) {
        eviction_large_chunk_cursor_ = 0;
    } else {
        eviction_large_chunk_cursor_ -= removed_before_cursor;
        if (eviction_large_chunk_cursor_ >= eviction_large_chunk_ring_.size()) {
            eviction_large_chunk_cursor_ %= eviction_large_chunk_ring_.size();
        }
    }
}

bool ChunkStore::RefillEvictionCandidatesBounded() {
    std::vector<EvictionCandidate> refill;
    std::size_t examined_large_chunks = 0;

    {
        std::lock_guard global_lock(large_chunks_mutex_);
        if (eviction_large_chunk_ring_.empty()) {
            return false;
        }

        const std::size_t ring_size = eviction_large_chunk_ring_.size();
        std::size_t visited_slots = 0;
        while (visited_slots < ring_size && examined_large_chunks < kEvictionRefillLargeChunkBudget) {
            const LargeChunkCoord large_coord = eviction_large_chunk_ring_[eviction_large_chunk_cursor_];
            eviction_large_chunk_cursor_ =
                (eviction_large_chunk_cursor_ + 1) % ring_size;
            ++visited_slots;

            auto large_it = large_chunks_.find(large_coord);
            if (large_it == large_chunks_.end()) {
                continue;
            }

            ++examined_large_chunks;
            const auto& large_chunk = large_it->second;
            std::lock_guard chunk_lock(large_chunk->mutex);
            for (const auto& [chunk_coord, regular_chunk] : large_chunk->chunks) {
                if (regular_chunk.use_count() == 1) {
                    refill.push_back(EvictionCandidate{
                        .large_coord = large_coord,
                        .chunk_coord = chunk_coord,
                        .recorded_tick =
                            regular_chunk->last_access_tick.load(std::memory_order_relaxed),
                    });
                }
            }
        }
    }

    stats_eviction_snapshot_builds_.fetch_add(1, std::memory_order_relaxed);
    stats_eviction_refill_large_chunk_scans_.fetch_add(
        examined_large_chunks,
        std::memory_order_relaxed);

    if (refill.empty()) {
        return false;
    }

    // Least-recently-used candidates first so eviction prefers cold chunks.
    std::sort(
        refill.begin(),
        refill.end(),
        [](const EvictionCandidate& lhs, const EvictionCandidate& rhs) {
            return lhs.recorded_tick < rhs.recorded_tick;
        });

    std::lock_guard lock(eviction_state_mutex_);
    if (eviction_cursor_ > 0 && eviction_cursor_ == eviction_candidates_.size()) {
        eviction_candidates_.clear();
        eviction_cursor_ = 0;
    }
    eviction_candidates_.insert(
        eviction_candidates_.end(),
        refill.begin(),
        refill.end());
    return true;
}

bool ChunkStore::TryEvictCandidate(
    const EvictionCandidate& candidate,
    bool respect_recency,
    std::size_t* removed,
    std::vector<LargeChunkCoord>* maybe_empty_large_chunks) {
    std::shared_ptr<LargeChunk> large_chunk;
    {
        std::lock_guard global_lock(large_chunks_mutex_);
        auto large_it = large_chunks_.find(candidate.large_coord);
        if (large_it == large_chunks_.end()) {
            return false;
        }
        large_chunk = large_it->second;
    }

    std::lock_guard large_lock(large_chunk->mutex);
    auto it = large_chunk->chunks.find(candidate.chunk_coord);
    if (it == large_chunk->chunks.end()) {
        return false;
    }

    const auto& regular_chunk = it->second;
    if (regular_chunk.use_count() != 1) {
        return false;
    }

    // Second chance: a chunk accessed after its tick was recorded is hot;
    // skip it in the recency-respecting pass instead of evicting it.
    if (respect_recency &&
        regular_chunk->last_access_tick.load(std::memory_order_relaxed) >
            candidate.recorded_tick) {
        stats_eviction_recency_skips_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    {
        std::unique_lock chunk_lock(regular_chunk->mutex);
        const bool had_pending_wal = !regular_chunk->wal_batch.empty();
        FlushWalBatchForEviction(
            candidate.chunk_coord,
            regular_chunk,
            durability_mode_ != DurabilityMode::kRelaxed);
        if (regular_chunk->deferred_wal_compaction && IsCheckpointDue(regular_chunk)) {
            CheckpointChunk(candidate.chunk_coord, regular_chunk);
        }
        if (had_pending_wal) {
            stats_eviction_forced_wal_flushes_with_data_.fetch_add(1, std::memory_order_relaxed);
        } else {
            stats_eviction_forced_wal_flushes_empty_batch_.fetch_add(1, std::memory_order_relaxed);
        }
        stats_eviction_forced_wal_flushes_.fetch_add(1, std::memory_order_relaxed);
        CloseWalAppendStream(regular_chunk);
    }

    if (regular_chunk.use_count() != 1) {
        return false;
    }

    large_chunk->chunks.erase(it);
    if (removed != nullptr) {
        *removed += 1;
    }
    if (maybe_empty_large_chunks != nullptr) {
        maybe_empty_large_chunks->push_back(candidate.large_coord);
    }
    return true;
}

void ChunkStore::MaybeEvictChunks() {
    const auto loaded_hint = loaded_chunk_count_.load(std::memory_order_relaxed);
    if (loaded_hint <= max_loaded_chunks_) {
        return;
    }

    const std::size_t lower_watermark = EvictionLowerWatermark(max_loaded_chunks_);
    const std::size_t required = loaded_hint > lower_watermark ? loaded_hint - lower_watermark : 0;
    if (required == 0) {
        return;
    }
    const std::size_t probe_budget = std::max<std::size_t>(64, required * 16);
    std::size_t removed = 0;
    std::size_t probes = 0;
    std::vector<LargeChunkCoord> maybe_empty_large_chunks;
    maybe_empty_large_chunks.reserve(required);

    const auto run_pass = [&](bool respect_recency) {
        while (removed < required && probes < probe_budget) {
            EvictionCandidate candidate{};
            bool have_candidate = false;
            {
                std::lock_guard lock(eviction_state_mutex_);
                if (eviction_cursor_ < eviction_candidates_.size()) {
                    candidate = eviction_candidates_[eviction_cursor_++];
                    have_candidate = true;
                }
            }

            if (!have_candidate) {
                if (!RefillEvictionCandidatesBounded()) {
                    break;
                }
                continue;
            }

            ++probes;
            (void)TryEvictCandidate(candidate, respect_recency, &removed, &maybe_empty_large_chunks);
        }
    };

    // First pass prefers cold chunks and gives recently touched chunks a
    // second chance. If that makes no progress while evictable chunks may
    // still exist, a second pass evicts by recorded recency order regardless
    // of later touches so the cache bound is still enforced.
    run_pass(true);
    if (removed == 0) {
        run_pass(false);
    }

    stats_eviction_probes_.fetch_add(probes, std::memory_order_relaxed);

    if (removed == 0) {
        stats_eviction_no_progress_cycles_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    loaded_chunk_count_.fetch_sub(removed, std::memory_order_relaxed);
    stats_evictions_.fetch_add(removed, std::memory_order_relaxed);

    std::lock_guard global_lock(large_chunks_mutex_);
    std::sort(
        maybe_empty_large_chunks.begin(),
        maybe_empty_large_chunks.end(),
        [](const LargeChunkCoord& lhs, const LargeChunkCoord& rhs) {
            if (lhs.x != rhs.x) {
                return lhs.x < rhs.x;
            }
            return lhs.y < rhs.y;
        });
    maybe_empty_large_chunks.erase(
        std::unique(maybe_empty_large_chunks.begin(), maybe_empty_large_chunks.end()),
        maybe_empty_large_chunks.end());
    stats_eviction_post_pass_large_chunk_checks_.fetch_add(
        maybe_empty_large_chunks.size(),
        std::memory_order_relaxed);
    for (const auto& large_coord : maybe_empty_large_chunks) {
        auto it = large_chunks_.find(large_coord);
        if (it == large_chunks_.end()) {
            continue;
        }
        auto large_chunk = it->second;
        bool erase_large_chunk = false;
        {
            std::lock_guard large_lock(large_chunk->mutex);
            erase_large_chunk = large_chunk->chunks.empty();
        }
        if (erase_large_chunk) {
            RemoveLargeChunkFromEvictionRing(it->first);
            large_chunks_.erase(it);
        }
    }
}

void ChunkStore::RequestEviction() {
    if (!background_maintenance_) {
        MaybeEvictChunks();
        return;
    }

    // Backpressure: while the maintenance thread catches up, the cache may
    // exceed the configured bound by at most one hysteresis band. Beyond
    // that, the loading thread evicts inline to preserve the hard bound.
    const std::size_t lower_watermark = EvictionLowerWatermark(max_loaded_chunks_);
    const std::size_t hysteresis = max_loaded_chunks_ - std::min(lower_watermark, max_loaded_chunks_);
    const std::size_t hard_bound = max_loaded_chunks_ + std::max<std::size_t>(hysteresis, 1);
    if (loaded_chunk_count_.load(std::memory_order_relaxed) > hard_bound) {
        MaybeEvictChunks();
        return;
    }

    {
        std::lock_guard lock(maintenance_mutex_);
        maintenance_eviction_requested_ = true;
    }
    maintenance_cv_.notify_one();
}

}  // namespace chunkdb
