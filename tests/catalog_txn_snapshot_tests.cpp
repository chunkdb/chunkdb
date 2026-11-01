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
}  // namespace

int main() {
    CommonCutKeepsFeedAttached();
    CancelledPauseRestoresAdmission(false);
    CancelledPauseRestoresAdmission(true);
    DurationBoundsAdmissionWaits(false);
    DurationBoundsAdmissionWaits(true);
    FinalRegistrationChecks(true);
    FinalRegistrationChecks(false);
}
