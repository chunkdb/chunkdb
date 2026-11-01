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
[[noreturn]] void LeaseWaitEnded(std::stop_token cancelled) {
    if (cancelled.stop_requested()) throw std::runtime_error("transaction table admission cancelled");
    throw TransactionConflictError(TxnConflictReason::kDuration, "transaction duration elapsed during table admission");
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
    // Complete multi-table lease cohorts and metadata work take this gate
    // before table admission. Excluding them before pausing prevents waiting
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

std::optional<Table::Lease> Table::AcquireUntil(
    std::chrono::steady_clock::time_point deadline, std::stop_token cancelled) {
    for (;;) {
        if (cancelled.stop_requested() || std::chrono::steady_clock::now() >= deadline) LeaseWaitEnded(cancelled);
        migration_health_->Check();
        active_leases_.fetch_add(1, std::memory_order_seq_cst);
        const auto state = state_.load(std::memory_order_seq_cst);
        if (state == State::kOpen) return Lease(this, store_.get());
        ReleaseLease();
        if (state == State::kGone) return std::nullopt;
        if (state == State::kFailed || feed_cleanup_failed_.load(std::memory_order_acquire))
            throw FeedRecoveryRequiredError();
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
            hook->Run(MigrationTestHook::Point::kBeforeTxnLeaseWait, name_);
        std::unique_lock lock(mutex_);
        if (!cv_.wait_until(lock, cancelled, deadline, [this] {
                return state_.load(std::memory_order_seq_cst) != State::kBusy ||
                    feed_cleanup_failed_.load(std::memory_order_acquire);
            }) || cancelled.stop_requested()) LeaseWaitEnded(cancelled);
    }
}

std::vector<CatalogTxnLease> TableCatalog::AcquireTxnTables(
    std::span<const CatalogTxnParticipant> participants,
    std::chrono::milliseconds max_duration, std::stop_token cancelled) {
    if (participants.empty()) throw std::invalid_argument("transaction table cohort is empty");
    if (max_duration.count() <= 0) throw std::invalid_argument("transaction duration must be positive");
    constexpr auto duration_cap = std::chrono::hours(24 * 365);
    const auto deadline = std::chrono::steady_clock::now() +
        std::min(max_duration, std::chrono::duration_cast<std::chrono::milliseconds>(duration_cap));
    for (const auto& participant : participants)
        if (!participant.table) throw std::invalid_argument("transaction table owner is missing");
    std::vector<CatalogTxnParticipant> ordered(participants.begin(), participants.end());
    std::sort(ordered.begin(), ordered.end(), [](const auto& first, const auto& second) {
        return first.table->store_id() < second.table->store_id();
    });
    for (std::size_t i = 1U; i < ordered.size(); ++i)
        if (ordered[i - 1U].table->store_id() == ordered[i].table->store_id())
            throw std::invalid_argument("transaction table cohort contains a duplicate epoch");
    if (!feature_metadata_gate_.lock_until(deadline, cancelled)) LeaseWaitEnded(cancelled);
    std::unique_lock metadata(feature_metadata_gate_, std::adopt_lock);
    migration_health_->Check();
    const auto changed = [](const Table& table) {
        return TransactionConflictError(TxnConflictReason::kTableChanged,
            "table '" + table.name() + "' was altered or dropped during the transaction");
    };
    for (const auto& participant : ordered)
        if (Find(participant.table->name()) != participant.table) throw changed(*participant.table);
    // Declared after metadata so every partial cohort releases its leases
    // before another metadata owner can obtain admission.
    std::vector<CatalogTxnLease> leases;
    leases.reserve(ordered.size());
    for (const auto& participant : ordered) {
        auto lease = participant.table->AcquireUntil(deadline, cancelled);
        if (!lease || (participant.snapshot && !lease->store().OwnsTxnSnapshot(*participant.snapshot)))
            throw changed(*participant.table);
        leases.push_back({participant.table, std::move(*lease)});
    }
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
        hook->Run(MigrationTestHook::Point::kBeforeTxnCohortPublication, {});
    if (cancelled.stop_requested() || std::chrono::steady_clock::now() >= deadline) LeaseWaitEnded(cancelled);
    return leases;
}

}  // namespace chunkdb
