#include "table_expiry.hpp"

#include <atomic>
#include <chrono>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/change_feed.hpp"
#include "chunkdb/logging.hpp"
#include "maintenance_pool.hpp"

// Expiry by expire_column (docs/reference/TABLES.md#expiry). The catalog's
// ticker requests a sweep of each table every expiry_interval; a sweep is a
// series of bounded passes, one per maintenance pool turn, each under a
// table lease taken without waiting, so schema changes, drops and reopens
// never wait for a sweep and a worker never waits for them.
namespace chunkdb {

struct Table::ExpiryJob {
    ExpiryJob(Table* owner, std::uint64_t now_ms) : table(owner), frontier_ms(now_ms) {}

    // One pool turn: a pass of the current sweep. Whether the sweep goes on.
    bool Step() noexcept;

    Table* const table;
    // Held through a pass; StopExpiry takes it to end the passes.
    std::mutex mutex;
    bool stopped = false;
    std::uint32_t column = 0U;
    bool has_cursor = false;
    ChunkCoord cursor{};
    std::uint64_t sweep_started_ms = 0U;
    // Every block whose time is at or before it was deleted, unless written
    // after its sweep passed its chunk: the start of the last completed
    // sweep, or when expiry started on the table.
    std::atomic<std::uint64_t> frontier_ms;
    std::atomic<std::uint64_t> expired_blocks{0U};
    std::atomic<std::uint64_t> failed_passes{0U};
    // Between RequestExpiry starting a sweep and its last turn; when it ended.
    std::atomic<bool> sweeping{false};
    std::atomic<std::chrono::steady_clock::rep> ended{0};
};

bool Table::ExpiryJob::Step() noexcept {
    std::lock_guard lock(mutex);
    const auto end = [this] {
        ended.store(std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_relaxed);
        sweeping.store(false, std::memory_order_release);
        return false;
    };
    if (stopped) return end();
    const auto lease = table->TryAcquire();
    if (!lease) return end();
    std::uint32_t current = 0U;
    {
        std::lock_guard options(table->mutex_);
        current = table->options_.expire_column;
    }
    const std::uint64_t now = UnixMillisNow();
    if (current != column) {
        column = current;
        has_cursor = false;
        frontier_ms.store(now, std::memory_order_relaxed);
    }
    if (column == 0U || Table::StoreFenceReason(lease->store()) != nullptr) return end();
    if (!has_cursor) sweep_started_ms = now;
    try {
        const ScopedWriteUser user(kExpiryWriteUser);
        const auto pass = lease->store().ExpireBlocks(column, has_cursor, cursor, kExpiryPassChunks, now);
        expired_blocks.fetch_add(pass.deleted, std::memory_order_relaxed);
        if (pass.more) {
            has_cursor = true;
            cursor = pass.next;
            return true;
        }
    } catch (const std::exception& error) {
        failed_passes.fetch_add(1U, std::memory_order_relaxed);
        has_cursor = false;
        try {
            LogMessage(LogLevel::kError, LogComponent::kStore, "expiry pass failed; the next sweep retries",
                {{"table", table->name_}, {"error", error.what()}});
        } catch (const std::exception&) {
            // Counted above; logging under memory pressure is best effort.
        }
        return end();
    }
    has_cursor = false;
    frontier_ms.store(sweep_started_ms, std::memory_order_relaxed);
    return end();
}

std::optional<Table::Lease> Table::TryAcquire() {
    active_leases_.fetch_add(1, std::memory_order_seq_cst);
    if (state_.load(std::memory_order_seq_cst) == State::kOpen &&
        !migration_health_->failed.load(std::memory_order_acquire) &&
        !feed_cleanup_failed_.load(std::memory_order_acquire)) {
        return Lease(this, store_.get());
    }
    ReleaseLease();
    return std::nullopt;
}

bool Table::HasExpireColumn() const {
    std::lock_guard options(mutex_);
    return options_.expire_column != 0U;
}

void Table::RequestExpiry(const std::shared_ptr<MaintenancePool>& pool, std::chrono::milliseconds interval, bool now) {
    if (!HasExpireColumn()) return;
    std::lock_guard lock(expiry_mutex_);
    if (expiry_stopped_) return;
    if (expiry_) {
        // One sweep at a time, the next one interval after the last ended.
        if (expiry_->sweeping.load(std::memory_order_acquire)) return;
        const auto ended = std::chrono::steady_clock::time_point(
            std::chrono::steady_clock::duration(expiry_->ended.load(std::memory_order_relaxed)));
        if (!now && std::chrono::steady_clock::now() - ended < interval) return;
    }
    if (!expiry_) {
        auto job = std::make_shared<ExpiryJob>(this, UnixMillisNow());
        // The job outlives every turn: StopExpiry waits for them.
        expiry_task_ = pool->MakeTask([job = job.get()] { return job->Step(); });
        expiry_pool_ = pool;
        expiry_ = std::move(job);
    }
    expiry_->sweeping.store(true, std::memory_order_release);
    if (!expiry_pool_->Notify(expiry_task_)) expiry_->sweeping.store(false, std::memory_order_release);
}

void Table::StopExpiry() noexcept {
    std::shared_ptr<ExpiryJob> job;
    std::shared_ptr<MaintenancePool> pool;
    std::shared_ptr<MaintenanceTask> task;
    {
        std::lock_guard lock(expiry_mutex_);
        expiry_stopped_ = true;
        job = expiry_;
        pool = expiry_pool_;
        task = expiry_task_;
    }
    if (!job) return;
    {
        std::lock_guard lock(job->mutex);
        job->stopped = true;
    }
    // Step catches what a pass throws, so the task holds no failure.
    (void)pool->Wait(task);
}

std::optional<Table::ExpiryStatus> Table::Expiry(std::uint64_t now_ms) const {
    {
        std::lock_guard options(mutex_);
        if (options_.expire_column == 0U) return std::nullopt;
    }
    std::lock_guard lock(expiry_mutex_);
    if (!expiry_) return ExpiryStatus{};
    const auto frontier = expiry_->frontier_ms.load(std::memory_order_relaxed);
    return ExpiryStatus{
        .lag_ms = now_ms > frontier ? now_ms - frontier : 0U,
        .expired_blocks = expiry_->expired_blocks.load(std::memory_order_relaxed),
        .failed_passes = expiry_->failed_passes.load(std::memory_order_relaxed),
    };
}

void TableCatalog::StartExpiryTicker() {
    if (config_.access_mode != AccessMode::kReadWrite || config_.expiry_interval.count() <= 0) return;
    expiry_ticker_ = std::jthread([this](std::stop_token stop) {
        std::unique_lock lock(expiry_ticker_mutex_);
        while (!expiry_ticker_cv_.wait_for(lock, stop, config_.expiry_interval, [] { return false; }) &&
               !stop.stop_requested()) {
            lock.unlock();
            std::vector<std::shared_ptr<Table>> tables;
            try {
                {
                    std::shared_lock served(tables_mutex_);
                    tables.reserve(tables_.size());
                    for (const auto& [name, table] : tables_) tables.push_back(table);
                }
                std::erase_if(tables, [](const std::shared_ptr<Table>& table) { return !table->HasExpireColumn(); });
                // The pool starts only for a table with an expire_column.
                if (!tables.empty()) {
                    const auto pool = resources_->MaintenanceWorkers();
                    for (const auto& table : tables) table->RequestExpiry(pool, config_.expiry_interval);
                }
            } catch (const std::exception& error) {
                try {
                    LogMessage(LogLevel::kError, LogComponent::kStore, "expiry sweeps could not start; the next tick retries",
                        {{"error", error.what()}});
                } catch (const std::exception&) {
                    // Best effort under memory pressure.
                }
            }
            // A table dropped meanwhile may close here, outside the lock.
            tables.clear();
            lock.lock();
        }
    });
}

void TableCatalog::StopExpiryTicker() noexcept {
    if (!expiry_ticker_.joinable()) return;
    expiry_ticker_.request_stop();
    expiry_ticker_cv_.notify_all();
    expiry_ticker_.join();
}

void ExpiryTestAccess::RunSweep(TableCatalog& catalog, std::string_view name) {
    const auto table = catalog.Find(name);
    if (!table) throw TableNotFoundError("no table " + std::string(name));
    table->RequestExpiry(catalog.resources_->MaintenanceWorkers(), catalog.config_.expiry_interval, /*now=*/true);
    std::shared_ptr<MaintenancePool> pool;
    std::shared_ptr<MaintenanceTask> task;
    {
        std::lock_guard lock(table->expiry_mutex_);
        pool = table->expiry_pool_;
        task = table->expiry_task_;
    }
    if (!task) return;
    if (const auto failure = pool->Wait(task)) std::rethrow_exception(failure);
}

}  // namespace chunkdb
