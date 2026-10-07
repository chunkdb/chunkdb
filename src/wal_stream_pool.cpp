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

    if (chunk->wal_stream_initialized.load(std::memory_order_acquire) && WalAppendStreamOpen(*chunk)) {
        TouchWalStreamState(chunk);
        return;
    }

    // Shared by all stores: the capacity check and the registration below
    // must not interleave with another store's, or the shared cap is exceeded.
    std::lock_guard open_guard(resources_->wal_open_mutex_);
    if (chunk->wal_stream_initialized.load(std::memory_order_acquire) && WalAppendStreamOpen(*chunk)) {
        TouchWalStreamState(chunk);
        return;
    }

    if (chunk->wal_path.empty()) {
        chunk->wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
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

    const bool needs_header = !chunk->wal_header_written;

    // The stream object (and the ~4 KiB buffer its filebuf allocates in the
    // constructor) exists only while the chunk holds an open stream. Every
    // exit below either leaves it open or releases it again.
    if (chunk->wal_append_stream == nullptr) {
        chunk->wal_append_stream = std::make_unique<std::ofstream>();
    }
    std::ofstream& stream = *chunk->wal_append_stream;

    stream.clear();
    if (artifact_touched != nullptr) {
        *artifact_touched = true;
    }
    stream.open(wal_path, std::ios::binary | std::ios::app);
    if (!stream.is_open()) {
        int open_err = errno;
        InvalidateWalParentDirectoryCache(wal_parent_path);
        EnsureWalParentDirectoryCached(
            wal_parent_path,
            true,
            durability_mode_ != DurabilityMode::kRelaxed);
        stream.clear();
        stream.open(wal_path, std::ios::binary | std::ios::app);
        if (!stream.is_open()) {
            open_err = errno;
            // Keep wal_stream_initialized a faithful mirror of "this chunk owns
            // an open stream" before releasing the object: the stream cache
            // scans that flag for other chunks holding only its own mutex
            // alone, and must never be told to look at a stream that is gone.
            chunk->wal_stream_initialized.store(false, std::memory_order_release);
            chunk->wal_append_stream.reset();
            throw BuildWalOpenError(wal_path, open_err);
        }
    }

    if (needs_header) {
        const auto wal_header = BuildWalHeader(chunk_coord, store_id_, features_);
        stream.write(
            reinterpret_cast<const char*>(wal_header.data()),
            static_cast<std::streamsize>(wal_header.size()));
        stream.flush();
        if (!stream.good()) {
            stream.close();
            chunk->wal_stream_initialized.store(false, std::memory_order_release);
            chunk->wal_append_stream.reset();
            throw std::runtime_error("failed to append WAL header: " + wal_path.string());
        }
        if (first_create != nullptr) {
            *first_create = true;
        }
        chunk->wal_header_written = true;
    }

    chunk->wal_stream_initialized.store(true, std::memory_order_release);
    stats_wal_open_count_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard lock(resources_->wal_stream_mutex_);
        auto& lru = resources_->wal_stream_lru_;
        const auto existing = resources_->open_wal_streams_.find(chunk.get());
        if (existing != resources_->open_wal_streams_.end()) {
            lru.splice(lru.end(), lru, existing->second.lru_position);
            existing->second.chunk = chunk;
            existing->second.owner = this;
        } else {
            lru.push_back(chunk.get());
            resources_->open_wal_streams_.emplace(
                chunk.get(),
                StoreResources::WalStreamState{
                    .chunk = chunk,
                    .owner = this,
                    .lru_position = std::prev(lru.end()),
                });
        }
    }
}

void ChunkStore::CloseWalAppendStream(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    if (chunk->wal_append_stream != nullptr) {
        if (chunk->wal_append_stream->is_open()) {
            chunk->wal_append_stream->flush();
            chunk->wal_append_stream->close();
        }
        // Release the stream object and its filebuf buffer: an idle resident
        // chunk must not keep several KiB of stdio buffer alive.
        chunk->wal_append_stream.reset();
    }
    chunk->wal_stream_initialized.store(false, std::memory_order_release);
    {
        std::lock_guard lock(resources_->wal_stream_mutex_);
        const auto entry = resources_->open_wal_streams_.find(chunk.get());
        if (entry != resources_->open_wal_streams_.end()) {
            resources_->EraseWalStreamLocked(entry);
        }
    }
    resources_->wal_stream_cv_.notify_all();
}

void ChunkStore::CloseAllWalStreams() noexcept {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }
    for (const auto& large_chunk : large_chunks) {
        std::vector<std::shared_ptr<RegularChunk>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [_, chunk] : large_chunk->chunks) {
                chunks.push_back(chunk);
            }
        }
        for (const auto& chunk : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            CloseWalAppendStream(chunk);
        }
    }
}

bool ChunkStore::TryCloseLeastRecentlyUsedIdleWalStream(
    const std::shared_ptr<RegularChunk>& opening_chunk) {
    std::shared_ptr<RegularChunk> candidate;

    {
        // Any store's idle stream will do: the pool is shared. The least
        // recently used entry is at the front; entries whose chunk is gone
        // are dropped on the way, each once.
        std::lock_guard lock(resources_->wal_stream_mutex_);
        auto& open_wal_streams = resources_->open_wal_streams_;
        auto& lru = resources_->wal_stream_lru_;
        for (auto position = lru.begin(); position != lru.end();) {
            const auto entry = open_wal_streams.find(*position);
            auto current = entry->second.chunk.lock();
            ++position;
            if (!current || !current->wal_stream_initialized.load(std::memory_order_acquire)) {
                resources_->EraseWalStreamLocked(entry);
                continue;
            }
            if (opening_chunk != nullptr && current == opening_chunk) {
                continue;
            }
            candidate = std::move(current);
            break;
        }
    }

    if (candidate == nullptr) {
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
    const std::size_t max_open_wal_streams = resources_->max_open_wal_streams_;
    auto& open_wal_streams = resources_->open_wal_streams_;
    // Under resources_->wal_stream_mutex_: whether `opening_chunk` may open a
    // stream. Entries whose chunk is gone still count until the LRU walk
    // drops them, so the cap is never exceeded, only reached early.
    const auto has_capacity_locked = [&]() {
        if (open_wal_streams.size() < max_open_wal_streams) {
            return true;
        }
        return opening_chunk != nullptr && open_wal_streams.contains(opening_chunk.get());
    };

    const auto deadline = std::chrono::steady_clock::now() + kWalStreamCapacityWaitTimeout;
    while (true) {
        {
            std::lock_guard lock(resources_->wal_stream_mutex_);
            if (has_capacity_locked()) {
                return;
            }
        }

        if (TryCloseLeastRecentlyUsedIdleWalStream(opening_chunk)) {
            continue;
        }

        std::unique_lock lock(resources_->wal_stream_mutex_);
        // The LRU walk may have dropped entries of chunks that are gone.
        if (has_capacity_locked()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            break;
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        resources_->wal_stream_cv_.wait_for(
            lock,
            std::min(kWalStreamCapacityRetryInterval, remaining));
    }

    std::size_t open_count = 0;
    {
        std::lock_guard lock(resources_->wal_stream_mutex_);
        if (has_capacity_locked()) {
            return;
        }
        open_count = open_wal_streams.size();
    }

    throw std::runtime_error(
        "timed out waiting for WAL stream capacity"
        " (cap=" + std::to_string(max_open_wal_streams) +
        ", open=" + std::to_string(open_count) + ")");
}

void ChunkStore::TouchWalStreamState(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    if (chunk == nullptr) {
        return;
    }
    std::lock_guard lock(resources_->wal_stream_mutex_);
    auto it = resources_->open_wal_streams_.find(chunk.get());
    if (it == resources_->open_wal_streams_.end()) {
        return;
    }
    auto& lru = resources_->wal_stream_lru_;
    lru.splice(lru.end(), lru, it->second.lru_position);
}

}  // namespace chunkdb
