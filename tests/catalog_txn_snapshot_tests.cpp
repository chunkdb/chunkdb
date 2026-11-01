#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <future>
#include <string_view>

#include "change_feed.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"

namespace chunkdb {
struct CatalogTxnTestAccess {
    static void WaitPaused(Table& table) {
        std::unique_lock lock(table.mutex_);
        assert(table.cv_.wait_for(lock, std::chrono::seconds{10}, [&] {
            return table.state_.load(std::memory_order_seq_cst) == Table::State::kBusy;
        }));
    }
    static bool SameFeedAndStore(Table& table, const void* feed, const void* store) {
        std::lock_guard lock(table.mutex_);
        return table.feed_.get() == feed && table.store_.get() == store && table.feed_->attached();
    }
    static const void* Feed(Table& table) {
        std::lock_guard lock(table.mutex_);
        return table.feed_.get();
    }
    static void FailFeed(Table& table) {
        table.feed_cleanup_failed_.store(true, std::memory_order_release);
        table.cv_.notify_all();
    }
    static std::shared_lock<BackupMaintenanceGate> Metadata(TableCatalog& catalog) {
        return std::shared_lock(catalog.feature_metadata_gate_);
    }
    static auto Pause(Table& table) {
        return table.PauseAdmission(std::chrono::steady_clock::now() + std::chrono::seconds{10}, {});
    }
    static std::size_t ActiveLeases(const Table& table) {
        return table.active_leases_.load(std::memory_order_seq_cst);
    }
};
}  // namespace chunkdb

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;

CatalogConfig Config(const std::filesystem::path& path) {
    CatalogConfig config;
    config.data_dir = path;
    config.background_maintenance = false;
    config.default_options.durability_mode = DurabilityMode::kRelaxed;
    config.default_options.checkpoint_update_interval = 1'000'000U;
    return config;
}

void CommonCutKeepsFeedAttached() {
    test::ScopedTempDir directory("chunkdb-catalog-snapshot");
    TableCatalog catalog(Config(directory.path()));
    std::vector<std::shared_ptr<Table>> tables;
    for (const char* name : {"a", "b"})
        tables.push_back(catalog.Create(name, {1U, 1U, 2U, 2U, 8U}, catalog.default_options()));
    std::sort(tables.begin(), tables.end(), [](const auto& first, const auto& second) {
        return first->store_id() < second->store_id();
    });
    const auto& first = tables[0];
    const auto& second = tables[1];
    auto feed = first->SubscribeFeed();
    auto lease = first->Acquire();
    auto& store = lease->store();
    store.SetBlockBits(0, 0, "11111111");
    const auto before = store.ReadChunkState(0, 0);
    const auto initial = feed->Next(10s);
    assert(initial && initial->kind == FeedEntry::Kind::kChange);
    const auto* feed_owner = CatalogTxnTestAccess::Feed(*first);
    store.ArmTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish);
    auto capture = std::async(std::launch::async, [&] { return catalog.CaptureTxnSnapshots(60s); });
    CatalogTxnTestAccess::WaitPaused(*first);
    ChunkState late;
    {
        auto other = second->Acquire();
        other->store().SetBlockBits(0, 0, "00110011");
        late = other->store().ReadChunkState(0, 0);
    }
    lease.reset();
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish));
    assert(CatalogTxnTestAccess::SameFeedAndStore(*first, feed_owner, &store));
    store.ResumeTxnForTests(TxnPausePoint::kRegisterBeforePublish);
    auto snapshots = capture.get();
    assert(snapshots.size() == 2U && snapshots[0].table == first && snapshots[1].table == second);
    const std::array<ChunkState, 2> expected{before, late};
    for (std::size_t i = 0; i < snapshots.size(); ++i) {
        auto current = snapshots[i].table->Acquire();
        current->store().SetBlockBits(0, 0, "00000000");
        const auto original = current->store().ReadChunkStateAt(*snapshots[i].snapshot, 0, 0);
        assert(original.payload == expected[i].payload && original.presence_bitmap == expected[i].presence_bitmap);
        assert(original.version == expected[i].version && original.commit_time_ms == expected[i].commit_time_ms);
    }
    const auto next = feed->Next(10s);
    assert(next && next->kind == FeedEntry::Kind::kChange && next->position.revision > initial->position.revision);
    const auto created = catalog.Create("later", {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    assert(std::none_of(snapshots.begin(), snapshots.end(), [&](const auto& entry) { return entry.table == created; }));
}

void CancelledPauseRestoresAdmission(bool fail_feed) {
    test::ScopedTempDir directory("chunkdb-catalog-snapshot-cancel");
    TableCatalog catalog(Config(directory.path()));
    auto table = catalog.Create("t", {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    auto lease = table->Acquire();
    std::stop_source stop;
    auto capture = std::async(std::launch::async, [&] {
        try { (void)catalog.CaptureTxnSnapshots(60s, stop.get_token()); }
        catch (const std::runtime_error& error) {
            return std::string_view(error.what()).find("cancelled") != std::string_view::npos;
        }
        return false;
    });
    CatalogTxnTestAccess::WaitPaused(*table);
    if (fail_feed) CatalogTxnTestAccess::FailFeed(*table);
    stop.request_stop();
    assert(capture.wait_for(10s) == std::future_status::ready && capture.get());
    lease.reset();
    if (fail_feed) {
        bool refused = false;
        try { (void)table->Acquire(); }
        catch (const FeedRecoveryRequiredError&) { refused = true; }
        assert(refused);
    } else {
        auto current = table->Acquire();
        assert(current.has_value());
        current->store().SetBlockBits(0, 0, "11111111");
    }
}

void DurationBoundsAdmissionWaits(bool metadata) {
    test::ScopedTempDir directory("chunkdb-catalog-snapshot-duration");
    TableCatalog catalog(Config(directory.path()));
    auto table = catalog.Create("t", {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    auto lease = metadata ? std::optional<Table::Lease>{} : table->Acquire();
    auto gate = metadata ? CatalogTxnTestAccess::Metadata(catalog) : std::shared_lock<BackupMaintenanceGate>{};
    auto capture = std::async(std::launch::async, [&] {
        try { (void)catalog.CaptureTxnSnapshots(100ms); }
        catch (const TransactionConflictError& error) { return error.reason() == TxnConflictReason::kDuration; }
        return false;
    });
    assert(capture.wait_for(10s) == std::future_status::ready && capture.get());
    if (gate.owns_lock()) gate.unlock();
    lease.reset();
    auto current = table->Acquire();
    assert(current.has_value());
}

void FinalRegistrationChecks(bool cancel) {
    test::ScopedTempDir directory("chunkdb-catalog-snapshot-register");
    TableCatalog catalog(Config(directory.path()));
    auto table = catalog.Create("t", {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    auto lease = table->Acquire();
    auto& store = lease->store();
    store.ArmTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish);
    lease.reset();
    std::stop_source stop;
    const auto duration = cancel ? 60s : 1s;
    auto capture = std::async(std::launch::async, [&] {
        try { (void)catalog.CaptureTxnSnapshots(duration, stop.get_token()); }
        catch (const TransactionConflictError& error) {
            return !cancel && error.reason() == TxnConflictReason::kDuration;
        } catch (const std::runtime_error& error) {
            return cancel && std::string_view(error.what()).find("cancelled") != std::string_view::npos;
        }
        return false;
    });
    assert(store.WaitForTxnPauseForTests(TxnPausePoint::kRegisterBeforePublish));
    assert(capture.wait_for(0s) == std::future_status::timeout);
    if (cancel) stop.request_stop();
    else {
        // Keep the final registration blocked through its duration. This
        // explicit deadline wait tests expiration, not execution ordering.
        std::mutex mutex;
        std::condition_variable cv;
        std::unique_lock lock(mutex);
        (void)cv.wait_for(lock, duration, [] { return false; });
    }
    store.ResumeTxnForTests(TxnPausePoint::kRegisterBeforePublish);
    assert(capture.wait_for(10s) == std::future_status::ready && capture.get());
    auto current = table->Acquire();
    assert(current && current->store().TxnRegisteredCountForTests() == 0U);
}

void CohortOrderAndOriginalOwners() {
    test::ScopedTempDir directory("chunkdb-catalog-cohort");
    TableCatalog catalog(Config(directory.path()));
    for (const char* name : {"a", "b", "untouched"})
        (void)catalog.Create(name, {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    auto snapshots = catalog.CaptureTxnSnapshots(60s);
    std::vector<CatalogTxnParticipant> participants;
    for (const auto& entry : snapshots)
        if (entry.table->name() != "untouched") participants.push_back({entry.table, entry.snapshot.get()});
    std::reverse(participants.begin(), participants.end());
    auto changed_options = catalog.default_options();
    changed_options.checkpoint_update_interval = 2'000'000U;
    catalog.SetOptions("untouched", changed_options);
    {
        auto leases = catalog.AcquireTxnTables(participants, std::chrono::milliseconds::max());
        assert(leases.size() == 2U && leases[0].table->store_id() < leases[1].table->store_id());
        for (const auto& lease : leases) {
            assert(CatalogTxnTestAccess::ActiveLeases(*lease.table) == 1U);
            const auto original = std::find_if(snapshots.begin(), snapshots.end(), [&](const auto& entry) {
                return entry.table == lease.table;
            });
            assert(original != snapshots.end() && lease.lease.store().OwnsTxnSnapshot(*original->snapshot));
        }
    }
    for (const auto& entry : participants) assert(CatalogTxnTestAccess::ActiveLeases(*entry.table) == 0U);
    // ALTER keeps the Table and epoch but replaces its history owner.
    const auto original = participants.front().table;
    catalog.SetOptions(original->name(), changed_options);
    assert(catalog.Find(original->name()) == original);
    bool refused = false;
    try { (void)catalog.AcquireTxnTables(participants, 60s); }
    catch (const TransactionConflictError& error) { refused = error.reason() == TxnConflictReason::kTableChanged; }
    assert(refused);
    for (const auto& entry : participants) assert(CatalogTxnTestAccess::ActiveLeases(*entry.table) == 0U);
    // A new table under an old name cannot join an original cohort.
    const auto old_name = original->name();
    catalog.Drop(old_name);
    (void)catalog.Create(old_name, {1U, 1U, 2U, 2U, 8U}, catalog.default_options());
    refused = false;
    try { (void)catalog.AcquireTxnTables(participants, 60s); }
    catch (const TransactionConflictError& error) { refused = error.reason() == TxnConflictReason::kTableChanged; }
    assert(refused && CatalogTxnTestAccess::ActiveLeases(*original) == 0U);
}

void CohortWaitsAndRefusals() {
    test::ScopedTempDir directory("chunkdb-catalog-cohort-refusals");
    TableCatalog catalog(Config(directory.path()));
    std::vector<CatalogTxnParticipant> participants;
    for (const char* name : {"a", "b"})
        participants.push_back({catalog.Create(name, {1U, 1U, 2U, 2U, 8U}, catalog.default_options()), nullptr});
    std::sort(participants.begin(), participants.end(), [](const auto& left, const auto& right) {
        return left.table->store_id() < right.table->store_id();
    });
    const auto invalid = [&](std::span<const CatalogTxnParticipant> entries, std::chrono::milliseconds duration) {
        bool refused = false;
        try { (void)catalog.AcquireTxnTables(entries, duration); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused);
    };
    invalid({}, 60s);
    invalid(participants, 0ms);
    const std::array<CatalogTxnParticipant, 1U> missing{{{nullptr, nullptr}}};
    invalid(missing, 60s);
    const std::array duplicates{participants[0], participants[0]};
    invalid(duplicates, 60s);
    {
        test::ScopedTempDir foreign_directory("chunkdb-catalog-cohort-foreign");
        TableCatalog foreign(Config(foreign_directory.path()));
        const std::array<CatalogTxnParticipant, 1U> foreign_owner{{{
            foreign.Create("a", {1U, 1U, 2U, 2U, 8U}, foreign.default_options()), nullptr}}};
        bool refused = false;
        try { (void)catalog.AcquireTxnTables(foreign_owner, 60s); }
        catch (const TransactionConflictError& error) { refused = error.reason() == TxnConflictReason::kTableChanged; }
        assert(refused && CatalogTxnTestAccess::ActiveLeases(*foreign_owner[0].table) == 0U);
    }
    {
        // A shared metadata owner can be an earlier DDL operation.
        auto metadata = CatalogTxnTestAccess::Metadata(catalog);
        bool expired = false;
        try { (void)catalog.AcquireTxnTables(participants, 100ms); }
        catch (const TransactionConflictError& error) { expired = error.reason() == TxnConflictReason::kDuration; }
        assert(expired);
        std::stop_source stop;
        stop.request_stop();
        bool cancelled = false;
        try { (void)catalog.AcquireTxnTables(participants, 60s, stop.get_token()); }
        catch (const std::runtime_error& error) {
            cancelled = std::string_view(error.what()).find("cancelled") != std::string_view::npos;
        }
        assert(cancelled);
    }
    {
        // An independent table control can remain busy without catalog admission.
        auto paused = CatalogTxnTestAccess::Pause(*participants.back().table);
        bool expired = false;
        try { (void)catalog.AcquireTxnTables(participants, 100ms); }
        catch (const TransactionConflictError& error) { expired = error.reason() == TxnConflictReason::kDuration; }
        assert(expired);
        for (const auto& entry : participants) assert(CatalogTxnTestAccess::ActiveLeases(*entry.table) == 0U);
    }
    auto leases = catalog.AcquireTxnTables(participants, 60s);
    assert(leases.size() == participants.size());
}

struct CohortPause final : MigrationTestHook {
    Point point;
    std::promise<void> entered;
    std::shared_future<void> resume;
    CohortPause(Point at, std::shared_future<void> resumed) : point(at), resume(std::move(resumed)) {}
    void Run(Point at, std::string_view) override {
        if (at != point) return;
        entered.set_value();
        resume.wait();
    }
};

void CohortCancellationAfterAdmission(bool busy) {
    test::ScopedTempDir directory("chunkdb-catalog-cohort-cancel");
    TableCatalog catalog(Config(directory.path()));
    std::vector<CatalogTxnParticipant> participants;
    for (const char* name : {"a", "b"})
        participants.push_back({catalog.Create(name, {1U, 1U, 2U, 2U, 8U}, catalog.default_options()), nullptr});
    std::sort(participants.begin(), participants.end(), [](const auto& left, const auto& right) {
        return left.table->store_id() < right.table->store_id();
    });
    std::promise<void> resume;
    CohortPause hook(busy ? MigrationTestHook::Point::kBeforeTxnLeaseWait :
        MigrationTestHook::Point::kBeforeTxnCohortPublication, resume.get_future().share());
    auto entered = hook.entered.get_future();
    // The pause type is private to Table; an optional preserves its lifetime
    // while allowing the final-publication case to keep both tables open.
    auto paused = busy ? std::optional{CatalogTxnTestAccess::Pause(*participants.back().table)} : std::nullopt;
    catalog.SetMigrationTestHook(&hook);
    std::stop_source stop;
    auto capture = std::async(std::launch::async, [&] {
        try { (void)catalog.AcquireTxnTables(participants, 60s, stop.get_token()); }
        catch (const std::runtime_error& error) {
            return std::string_view(error.what()).find("cancelled") != std::string_view::npos;
        }
        return false;
    });
    assert(entered.wait_for(10s) == std::future_status::ready);
    assert(CatalogTxnTestAccess::ActiveLeases(*participants.front().table) == 1U);
    assert(CatalogTxnTestAccess::ActiveLeases(*participants.back().table) == static_cast<std::size_t>(!busy));
    stop.request_stop();
    resume.set_value();
    assert(capture.wait_for(10s) == std::future_status::ready && capture.get());
    catalog.SetMigrationTestHook(nullptr);
    for (const auto& entry : participants) assert(CatalogTxnTestAccess::ActiveLeases(*entry.table) == 0U);
}
}  // namespace

int main() {
    CommonCutKeepsFeedAttached();
    CancelledPauseRestoresAdmission(false);
    CancelledPauseRestoresAdmission(true);
    DurationBoundsAdmissionWaits(false);
    DurationBoundsAdmissionWaits(true);
    FinalRegistrationChecks(true);
    FinalRegistrationChecks(false);
    CohortOrderAndOriginalOwners();
    CohortWaitsAndRefusals();
    CohortCancellationAfterAdmission(true);
    CohortCancellationAfterAdmission(false);
}
