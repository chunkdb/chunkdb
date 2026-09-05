#include "wal_stream_pool.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>

#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "wal_writer.hpp"

namespace chunkdb {

void ChunkStore::EnsureWalParentDirectoryCached(
    const std::filesystem::path& wal_parent_path,
    bool force_refresh,
    bool durable_sync) {
    const std::string key = CanonicalPathKey(wal_parent_path);
    if (!force_refresh) {
        std::lock_guard lock(wal_parent_cache_mutex_);
        if (wal_parent_dir_cache_.contains(key)) {
            return;
        }
    }

    EnsureDirectoryPathExists(wal_parent_path, durable_sync);

    stats_wal_parent_prepare_calls_.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard lock(wal_parent_cache_mutex_);
    wal_parent_dir_cache_.insert(std::move(key));
}

void ChunkStore::InvalidateWalParentDirectoryCache(const std::filesystem::path& wal_parent_path) {
    const std::string key = CanonicalPathKey(wal_parent_path);
    std::lock_guard lock(wal_parent_cache_mutex_);
    wal_parent_dir_cache_.erase(key);
}

void ChunkStore::EnsureWalAppendStream(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool* first_create,
    bool* artifact_touched) {
    if (first_create != nullptr) {
        *first_create = false;
    }
    if (artifact_touched != nullptr) {
        *artifact_touched = false;
    }

    if (chunk->wal_stream_initialized.load(std::memory_order_acquire) && chunk->wal_append_stream.is_open()) {
        TouchWalStreamState(chunk);
        return;
    }

    std::lock_guard open_guard(wal_open_mutex_);
    if (chunk->wal_stream_initialized.load(std::memory_order_acquire) && chunk->wal_append_stream.is_open()) {
        TouchWalStreamState(chunk);
        return;
    }

    if (chunk->wal_path.empty()) {
        chunk->wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
    }
    const auto& wal_path = chunk->wal_path;
    const auto wal_parent_path = wal_path.parent_path();
    EnsureWalParentDirectoryCached(
        wal_parent_path,
        false,
        durability_mode_ != DurabilityMode::kRelaxed);

    EnsureWalStreamCapacity(chunk);

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_WAL_OPEN_ONCE")) {
        throw std::runtime_error("injected WAL open failure: " + wal_path.string());
    }

    const bool needs_header = !chunk->wal_header_written || chunk->wal_needs_v4_header;
    // Captured before the open so a stat failure is a clean pre-append error:
    // a rollback past this offset has to restore the "needs v4 header" state.
    const std::uint64_t header_offset =
        chunk->wal_needs_v4_header ? CurrentWalFileSize(chunk) : 0U;

    chunk->wal_append_stream.clear();
    if (artifact_touched != nullptr) {
        *artifact_touched = true;
    }
    chunk->wal_append_stream.open(wal_path, std::ios::binary | std::ios::app);
    if (!chunk->wal_append_stream.is_open()) {
        int open_err = errno;
        InvalidateWalParentDirectoryCache(wal_parent_path);
        EnsureWalParentDirectoryCached(
            wal_parent_path,
            true,
            durability_mode_ != DurabilityMode::kRelaxed);
        chunk->wal_append_stream.clear();
        chunk->wal_append_stream.open(wal_path, std::ios::binary | std::ios::app);
        if (!chunk->wal_append_stream.is_open()) {
            open_err = errno;
            throw BuildWalOpenError(wal_path, open_err);
        }
    }

    if (needs_header) {
        const auto wal_header = BuildWalHeader(geometry_, chunk_coord);
        chunk->wal_append_stream.write(
            reinterpret_cast<const char*>(wal_header.data()),
            static_cast<std::streamsize>(wal_header.size()));
        chunk->wal_append_stream.flush();
        if (!chunk->wal_append_stream.good()) {
            chunk->wal_append_stream.close();
            throw std::runtime_error("failed to append WAL header: " + wal_path.string());
        }
        if (first_create != nullptr) {
            *first_create = true;
        }
        if (chunk->wal_needs_v4_header) {
            chunk->wal_v4_header_offset = header_offset;
        }
        chunk->wal_header_written = true;
        chunk->wal_needs_v4_header = false;
    }

    chunk->wal_stream_initialized.store(true, std::memory_order_release);
    stats_wal_open_count_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lock(wal_stream_cache_mutex_);
        const auto tick = wal_stream_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
        open_wal_streams_[chunk.get()] = WalStreamState{
            .chunk = chunk,
            .last_used_tick = tick,
        };
        stats_open_wal_streams_current_.store(open_wal_streams_.size(), std::memory_order_relaxed);
    }
}

void ChunkStore::CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    if (chunk->wal_append_stream.is_open()) {
        chunk->wal_append_stream.flush();
        chunk->wal_append_stream.close();
    }
    chunk->wal_stream_initialized.store(false, std::memory_order_release);
    {
        std::lock_guard lock(wal_stream_cache_mutex_);
        open_wal_streams_.erase(chunk.get());
        stats_open_wal_streams_current_.store(open_wal_streams_.size(), std::memory_order_relaxed);
    }
    wal_stream_cache_cv_.notify_all();
}

bool ChunkStore::TryCloseLeastRecentlyUsedIdleWalStream(
    const std::shared_ptr<RegularChunk>& opening_chunk) {
    std::shared_ptr<RegularChunk> candidate;
    RegularChunk* candidate_key = nullptr;

    {
        std::lock_guard lock(wal_stream_cache_mutex_);
        std::uint64_t best_tick = std::numeric_limits<std::uint64_t>::max();
        for (auto it = open_wal_streams_.begin(); it != open_wal_streams_.end();) {
            auto current = it->second.chunk.lock();
            if (!current || !current->wal_stream_initialized.load(std::memory_order_acquire)) {
                it = open_wal_streams_.erase(it);
                continue;
            }
            if (opening_chunk != nullptr && it->first == opening_chunk.get()) {
                ++it;
                continue;
            }
            if (it->second.last_used_tick <= best_tick) {
                best_tick = it->second.last_used_tick;
                candidate = std::move(current);
                candidate_key = it->first;
            }
            ++it;
        }
        stats_open_wal_streams_current_.store(open_wal_streams_.size(), std::memory_order_relaxed);
    }

    if (candidate == nullptr || candidate_key == nullptr) {
        return false;
    }
    if (!candidate->mutex.try_lock()) {
        TouchWalStreamState(candidate);
        return false;
    }
    {
        std::unique_lock<RegularChunkMutex> lock(candidate->mutex, std::adopt_lock);
        CloseWalAppendStream(candidate);
    }
    return true;
}

void ChunkStore::EnsureWalStreamCapacity(const std::shared_ptr<RegularChunk>& opening_chunk) {
    if (max_open_wal_streams_ == 0) {
        return;
    }
    const auto deadline = std::chrono::steady_clock::now() + kWalStreamCapacityWaitTimeout;
    while (true) {
        {
            std::lock_guard lock(wal_stream_cache_mutex_);
            for (auto it = open_wal_streams_.begin(); it != open_wal_streams_.end();) {
                auto current = it->second.chunk.lock();
                if (!current || !current->wal_stream_initialized.load(std::memory_order_acquire)) {
                    it = open_wal_streams_.erase(it);
                } else {
                    ++it;
                }
            }
            stats_open_wal_streams_current_.store(open_wal_streams_.size(), std::memory_order_relaxed);
            if (open_wal_streams_.size() < max_open_wal_streams_) {
                return;
            }
            if (opening_chunk != nullptr) {
                auto it = open_wal_streams_.find(opening_chunk.get());
                if (it != open_wal_streams_.end()) {
                    return;
                }
            }
        }

        if (TryCloseLeastRecentlyUsedIdleWalStream(opening_chunk)) {
            continue;
        }

        std::unique_lock lock(wal_stream_cache_mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            break;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        wal_stream_cache_cv_.wait_for(
            lock,
            std::min(kWalStreamCapacityRetryInterval, remaining));
    }

    std::size_t open_count = 0;
    {
        std::lock_guard lock(wal_stream_cache_mutex_);
        for (auto it = open_wal_streams_.begin(); it != open_wal_streams_.end();) {
            auto current = it->second.chunk.lock();
            if (!current || !current->wal_stream_initialized.load(std::memory_order_acquire)) {
                it = open_wal_streams_.erase(it);
            } else {
                ++it;
            }
        }
        stats_open_wal_streams_current_.store(open_wal_streams_.size(), std::memory_order_relaxed);
        if (open_wal_streams_.size() < max_open_wal_streams_) {
            return;
        }
        if (opening_chunk != nullptr) {
            auto it = open_wal_streams_.find(opening_chunk.get());
            if (it != open_wal_streams_.end()) {
                return;
            }
        }
        open_count = open_wal_streams_.size();
    }

    throw std::runtime_error(
        "timed out waiting for WAL stream capacity"
        " (cap=" + std::to_string(max_open_wal_streams_) +
        ", open=" + std::to_string(open_count) + ")");
}

void ChunkStore::TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    std::lock_guard lock(wal_stream_cache_mutex_);
    auto it = open_wal_streams_.find(chunk.get());
    if (it == open_wal_streams_.end()) {
        return;
    }
    it->second.last_used_tick = wal_stream_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
}

}  // namespace chunkdb
