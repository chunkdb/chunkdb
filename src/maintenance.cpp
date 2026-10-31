#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"
#include "maintenance_pool.hpp"

namespace chunkdb {

namespace {

// Runs a best-effort logging action, containing any exception (e.g. bad_alloc
// while formatting). Used on the background maintenance thread, where an
// escaping exception — including from a noexcept function — would
// std::terminate the server.
template <typename LogAction>
void LogBestEffort(LogAction&& action) noexcept {
    try {
        std::forward<LogAction>(action)();
    } catch (...) {
    }
}

}  // namespace

void ChunkStore::StartMaintenanceThread() {
    std::lock_guard lock(maintenance_mutex_);
    maintenance_pool_ = resources_->MaintenanceWorkers();
    maintenance_task_ = maintenance_pool_->MakeTask([this] { return MaintenanceStep(); });
    maintenance_stop_ = false;
}

void ChunkStore::StopMaintenanceThread() noexcept {
    std::shared_ptr<MaintenanceTask> task;
    std::shared_ptr<MaintenancePool> pool;
    {
        std::lock_guard lock(maintenance_mutex_);
        maintenance_stop_ = true;
        task = maintenance_task_;
        pool = maintenance_pool_;
    }
    if (!task) return;
    if (auto error = pool->Wait(task)) {
        stats_background_checkpoint_failures_.fetch_add(1, std::memory_order_relaxed);
        LogBestEffort([&] {
            try { std::rethrow_exception(error); }
            catch (const std::exception& failure) {
                LogMessage(LogLevel::kError, LogComponent::kStore,
                    "background maintenance failed", {{"error", failure.what()}});
            }
        });
    }
    std::lock_guard lock(maintenance_mutex_);
    maintenance_task_.reset();
    maintenance_pool_.reset();
}

bool ChunkStore::MaintenanceStep() {
    ChunkCoord chunk_coord{};
    bool have_checkpoint = false;
    bool run_eviction = false;
    {
        std::lock_guard lock(maintenance_mutex_);
        if (!maintenance_checkpoint_queue_.empty()) {
            chunk_coord = maintenance_checkpoint_queue_.front();
            maintenance_checkpoint_queue_.erase(maintenance_checkpoint_queue_.begin());
            maintenance_checkpoint_queued_keys_.erase(chunk_coord);
            have_checkpoint = true;
        } else if (maintenance_eviction_requested_) {
            maintenance_eviction_requested_ = false;
            run_eviction = true;
        } else {
            return false;
        }
    }

    if (have_checkpoint) {
        RunBackgroundCheckpoint(chunk_coord);
    } else if (run_eviction) {
        // MaybeEvictChunks flushes/checkpoints dirty chunks and can throw
        // on a transient I/O error or when the store is already fail-closed
        // (odd snapshot generation). This is the std::thread entry path, so
        // an escaping exception would std::terminate the whole server. A
        // failed background eviction is survivable — the request path
        // re-evicts inline under backpressure — so log and continue. The
        // whole block (including the best-effort logging, which can throw
        // under memory pressure) is contained so nothing escapes the thread.
        try {
            MaybeEvictChunks();
        } catch (const std::exception& e) {
            stats_background_eviction_failures_.fetch_add(1, std::memory_order_relaxed);
            try {
                LogMessage(
                    LogLevel::kError,
                    LogComponent::kStore,
                    "background eviction failed; inline eviction will retry under memory pressure",
                    {{"error", e.what()}});
            } catch (...) {
            }
        } catch (...) {
            stats_background_eviction_failures_.fetch_add(1, std::memory_order_relaxed);
            try {
                LogMessage(
                    LogLevel::kError,
                    LogComponent::kStore,
                    "background eviction failed; inline eviction will retry under memory pressure",
                    {{"error", "unknown"}});
            } catch (...) {
            }
        }
    }
    std::lock_guard lock(maintenance_mutex_);
    return !maintenance_checkpoint_queue_.empty() || maintenance_eviction_requested_;
}

bool ChunkStore::EnqueueBackgroundCheckpoint(const ChunkCoord& chunk_coord) {
    {
        std::lock_guard lock(maintenance_mutex_);
        if (maintenance_stop_ || !maintenance_task_) {
            return false;
        }
        if (maintenance_checkpoint_queued_keys_.contains(chunk_coord)) {
            return true;
        }
        if (maintenance_checkpoint_queue_.size() >= background_checkpoint_queue_limit_) {
            stats_background_queue_full_inline_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        maintenance_checkpoint_queued_keys_.insert(chunk_coord);
        try { maintenance_checkpoint_queue_.push_back(chunk_coord); }
        catch (const std::exception&) {
            maintenance_checkpoint_queued_keys_.erase(chunk_coord);
            throw;
        }
        if (!maintenance_pool_->Notify(maintenance_task_)) {
            maintenance_checkpoint_queue_.pop_back();
            maintenance_checkpoint_queued_keys_.erase(chunk_coord);
            return false;
        }
    }
    return true;
}

void ChunkStore::RunBackgroundCheckpoint(const ChunkCoord& chunk_coord) noexcept {
    std::shared_ptr<RegularChunk> chunk;
    try {
        chunk = TryGetLoadedChunk(chunk_coord);
    } catch (const std::exception& e) {
        // A lookup failure drops this dequeued checkpoint; the next eligible
        // write re-checkpoints inline, but surface the error rather than
        // swallowing it silently.
        stats_background_checkpoint_failures_.fetch_add(1, std::memory_order_relaxed);
        LogBestEffort([&]() {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "background checkpoint lookup failed; dequeued request dropped, next write retries inline",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", e.what()},
                });
        });
        return;
    } catch (...) {
        stats_background_checkpoint_failures_.fetch_add(1, std::memory_order_relaxed);
        LogBestEffort([&]() {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "background checkpoint lookup failed; dequeued request dropped, next write retries inline",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", "unknown"},
                });
        });
        return;
    }
    if (chunk == nullptr) {
        // Evicted meanwhile; eviction already flushed and, when due,
        // checkpointed the chunk.
        return;
    }

    std::unique_lock lock(chunk->mutex);
    const bool eligible = chunk->pending_updates >= checkpoint_update_interval_ ||
                          chunk->wal_bytes >= checkpoint_wal_bytes_ ||
                          chunk->deferred_wal_compaction;
    if (!eligible) {
        return;
    }

    try {
        if (!CheckpointChunk(chunk_coord, chunk)) return;
        chunk->background_checkpoint_failed = false;
        stats_background_checkpoints_.fetch_add(1, std::memory_order_relaxed);
    } catch (const std::exception& e) {
        stats_background_checkpoint_failures_.fetch_add(1, std::memory_order_relaxed);
        // The next eligible write to this chunk retries the checkpoint
        // inline so the failure reaches a caller deterministically.
        chunk->background_checkpoint_failed = true;
        LogBestEffort([&]() {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "background checkpoint failed; next write retries inline",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", e.what()},
                });
        });
    } catch (...) {
        stats_background_checkpoint_failures_.fetch_add(1, std::memory_order_relaxed);
        chunk->background_checkpoint_failed = true;
        LogBestEffort([&]() {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "background checkpoint failed; next write retries inline",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"error", "unknown"},
                });
        });
    }
}

}  // namespace chunkdb
