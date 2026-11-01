#include "chunkdb/table_catalog.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace chunkdb {
namespace {
[[noreturn]] void SnapshotWaitEnded(std::stop_token cancelled) {
    if (cancelled.stop_requested()) throw std::runtime_error("transaction snapshot cancelled");
    throw TransactionConflictError(TxnConflictReason::kDuration, "transaction duration elapsed during snapshot capture");
}
}  // namespace

Table::AdmissionPause::AdmissionPause(std::shared_ptr<Table> table,
    std::shared_ptr<ChunkStore> store) noexcept
    : table_(std::move(table)), store_(std::move(store)) {}

Table::AdmissionPause::AdmissionPause(AdmissionPause&& other) noexcept
    : table_(std::move(other.table_)), store_(std::move(other.store_)) {}

Table::AdmissionPause::~AdmissionPause() {
    if (table_) table_->ResumeAdmission();
}

void Table::ResumeAdmission() noexcept {
    {
        std::lock_guard lock(mutex_);
        // A concurrent feed or directory failure remains authoritative.
        const auto next = feed_cleanup_failed_.load(std::memory_order_acquire) ||
            migration_health_->failed.load(std::memory_order_acquire) ? State::kFailed : State::kOpen;
        state_.store(next, std::memory_order_seq_cst);
    }
    cv_.notify_all();
}

Table::AdmissionPause Table::PauseAdmission(
    std::chrono::steady_clock::time_point deadline, std::stop_token cancelled) {
    auto self = shared_from_this();
    migration_health_->Check();
    {
        std::unique_lock lock(mutex_);
        if (!cv_.wait_until(lock, cancelled, deadline, [this] {
                return state_.load(std::memory_order_seq_cst) != State::kBusy ||
                    feed_cleanup_failed_.load(std::memory_order_acquire);
            }) || cancelled.stop_requested())
            SnapshotWaitEnded(cancelled);
        migration_health_->Check();
        const auto state = state_.load(std::memory_order_seq_cst);
        if (state == State::kGone) throw TableNotFoundError("table was dropped during snapshot capture");
        if (state == State::kFailed || feed_cleanup_failed_.load(std::memory_order_acquire))
            throw FeedRecoveryRequiredError();
        state_.store(State::kBusy, std::memory_order_seq_cst);
    }
    AdmissionPause pause(std::move(self), store_);
    cv_.notify_all();
    {
        std::unique_lock lock(mutex_);
        if (!cv_.wait_until(lock, cancelled, deadline, [this] {
                return active_leases_.load(std::memory_order_seq_cst) == 0U;
            }) || cancelled.stop_requested())
            SnapshotWaitEnded(cancelled);
        migration_health_->Check();
        if (feed_cleanup_failed_.load(std::memory_order_acquire)) throw FeedRecoveryRequiredError();
        store_->ThrowIfDurabilityPoisoned();
    }
    return pause;
}

std::vector<CatalogTxnSnapshot> TableCatalog::CaptureTxnSnapshots(
    std::chrono::milliseconds max_duration, std::stop_token cancelled) {
    if (max_duration.count() <= 0) throw std::invalid_argument("transaction duration must be positive");
    constexpr auto duration_cap = std::chrono::hours(24 * 365);
    const auto deadline = std::chrono::steady_clock::now() +
        std::min(max_duration, std::chrono::duration_cast<std::chrono::milliseconds>(duration_cap));
    // All multi-table lease acquisition and metadata work take shared
    // admission here. Excluding them before pausing a table prevents waiting
    // for a group that holds one table and still needs another paused table.
    if (!feature_metadata_gate_.lock_until(deadline, cancelled)) SnapshotWaitEnded(cancelled);
    std::unique_lock metadata(feature_metadata_gate_, std::adopt_lock);
    migration_health_->Check();
    if (std::chrono::steady_clock::now() >= deadline) SnapshotWaitEnded(cancelled);
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        tables.reserve(tables_.size());
        for (const auto& [_, table] : tables_) tables.push_back(table);
    }
    std::sort(tables.begin(), tables.end(), [](const auto& first, const auto& second) {
        return first->store_id() < second->store_id();
    });
    std::vector<Table::AdmissionPause> paused;
    paused.reserve(tables.size());
    std::vector<CatalogTxnSnapshot> snapshots;
    snapshots.reserve(tables.size());
    for (const auto& table : tables) paused.push_back(table->PauseAdmission(deadline, cancelled));
    for (std::size_t i = 0U; i < tables.size(); ++i) {
        if (cancelled.stop_requested()) SnapshotWaitEnded(cancelled);
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) throw TransactionConflictError(TxnConflictReason::kDuration,
            "transaction duration elapsed during snapshot capture");
        snapshots.push_back({tables[i], paused[i].store().BeginTxnSnapshot(remaining)});
    }
    if (cancelled.stop_requested() || std::chrono::steady_clock::now() >= deadline) SnapshotWaitEnded(cancelled);
    return snapshots;
}

}  // namespace chunkdb
