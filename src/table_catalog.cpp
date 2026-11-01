#include "chunkdb/table_catalog.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <limits>
#include <regex>
#include <set>
#include <string>
#include <system_error>
#include <utility>

#include "checkpoint.hpp"
#include "chunk_coordinate_index.hpp"
#include "file_permissions.hpp"
#include "change_feed.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/logging.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feature_activation_records.hpp"
#include "feed_slots.hpp"
#include "backup.hpp"
#include "slot_watch.hpp"
#include "snapshot_generation.hpp"
#include "process_lock.hpp"
#include "store_manifest.hpp"
#include "migrations_records.hpp"
#include "user_registry.hpp"

namespace chunkdb {

namespace {

constexpr std::string_view kTablesDirName = "tables";
constexpr std::string_view kStagingDirName = ".chunkdb.staging";
constexpr std::string_view kDroppedDirName = ".chunkdb.dropped";

constexpr std::array<std::string_view, 4> kWindowsDeviceNames = {"con", "prn", "aux", "nul"};

// Why a table is fenced, as DESCRIBE reports it. The failure itself goes to
// the server log.
constexpr const char* kFenceReopen = "a change could not reopen it";
constexpr const char* kFenceDropReopen = "a failed drop could not reopen it";
constexpr const char* kFenceDropUnknown = "a failed drop could not tell whether it moved";
constexpr const char* kFenceCreate = "a failed create could not remove it";
constexpr const char* kFenceDurability = "its store is fail-closed after a durability failure";
constexpr const char* kFenceFeed = "its feed cleanup failed";
constexpr const char* kFenceSnapshot = "its snapshot generation failed";
constexpr const char* kFenceRecovery = "a recovery could not reopen it";

// How watches of a table that left service end.
constexpr const char* kWatchFenced = "watched table is fenced; watch it again after RECOVER TABLE";
constexpr const char* kWatchRecovered = "watched table was recovered; watch it again to resume";

[[nodiscard]] std::string FencedMessage(std::string_view table, std::string_view reason) {
    return "table '" + std::string(table) + "' is fenced: " + std::string(reason) +
           "; run RECOVER TABLE " + std::string(table);
}

[[nodiscard]] std::string OptionMismatchMessage(const std::string& mismatches) {
    return "server option flags differ from the options tables store:" + mismatches +
           ". A table keeps the options it was created with: omit the flag, pass the "
           "stored value, or change the table with ALTER TABLE ... SET";
}

// A name nobody else uses for a directory being created or dropped.
[[nodiscard]] std::string UniqueOperationName(const std::string& table_name) {
    return table_name + "." + StoreIdHex(NewStoreId()).substr(0, 16);
}

// Whether a data-directory entry is something chunkdb creates there, so a
// directory without `chunkdb.manifest` that holds one is not new. The writer
// lock is not: lock bootstrap precedes the manifest.
[[nodiscard]] bool IsDataDirEntryName(const std::string& name) {
    if (name.rfind(".chunkdb.lock", 0) == 0 ||
        name.rfind(std::string(kDataDirManifestFileName) + ".tmp.", 0) == 0) {
        return false;
    }
    return name == kTablesDirName || name == kMigrationsFileName || name == kMigrationPendingFileName || name == kFeaturePendingFileName || IsStoreEntryName(name);
}

[[nodiscard]] std::optional<std::string> FindDataDirEntry(const std::filesystem::path& data_dir) {
    std::error_code ec;
    std::filesystem::directory_iterator it(data_dir, ec);
    const std::filesystem::directory_iterator end;
    for (; !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (IsDataDirEntryName(name)) {
            return name;
        }
    }
    if (ec) {
        throw std::runtime_error(
            "cannot list data directory " + data_dir.string() + ": " + ec.message());
    }
    return std::nullopt;
}

// A data directory that holds chunkdb data but no `chunkdb.manifest` was
// written by an older chunkdb; it is refused, never opened or changed.
[[noreturn]] void ThrowUnconvertedDataDir(
    const std::filesystem::path& data_dir,
    const std::string& entry) {
    throw std::runtime_error(
        "data directory " + data_dir.string() + " has no " +
        std::string(kDataDirManifestFileName) + " but holds chunkdb data (found '" + entry +
        "'): it was written by an older chunkdb, and this build opens only the 2.0 "
        "storage format; the directory is not changed");
}

[[nodiscard]] bool IsDirectory(const std::filesystem::path& path) {
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(path, ec);
    if (ec == std::errc::no_such_file_or_directory) {
        return false;
    }
    if (ec) {
        throw std::runtime_error("cannot inspect " + path.string() + ": " + ec.message());
    }
    return status.type() == std::filesystem::file_type::directory;
}

// Runs `fn` when the scope ends, unless dismissed. `fn` must not throw.
template <typename Fn>
class ScopeExit {
  public:
    explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
    ScopeExit(const ScopeExit&) = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ~ScopeExit() {
        if (active_) {
            fn_();
        }
    }
    void Dismiss() noexcept { active_ = false; }

  private:
    Fn fn_;
    bool active_ = true;
};

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) {
        std::_Exit(86);
    }
}

}  // namespace

bool IsValidTableName(std::string_view name) noexcept {
    if (name.empty() || name.size() > kMaxTableNameLength) {
        return false;
    }
    const auto lower_alnum = [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    };
    if (!lower_alnum(name.front())) {
        return false;
    }
    for (const char c : name) {
        if (!lower_alnum(c) && c != '_' && c != '-') {
            return false;
        }
    }
    if (std::find(kWindowsDeviceNames.begin(), kWindowsDeviceNames.end(), name) !=
        kWindowsDeviceNames.end()) {
        return false;
    }
    if (name.size() == 4 && (name.substr(0, 3) == "com" || name.substr(0, 3) == "lpt") &&
        name[3] >= '0' && name[3] <= '9') {
        return false;
    }
    return true;
}

void RequireValidTableName(std::string_view name) {
    if (!IsValidTableName(name)) {
        throw std::invalid_argument(
            "invalid table name '" + std::string(name) +
            "': use 1-64 characters from a-z, 0-9, '_' and '-', starting with a letter "
            "or digit, and not a Windows device name (con, prn, aux, nul, com0-9, lpt0-9)");
    }
}

void RequireValidTableOptions(const TableOptions& options) {
    if (options.checkpoint_update_interval == 0) {
        throw std::invalid_argument("checkpoint_updates must be > 0");
    }
    if (options.checkpoint_wal_bytes == 0) {
        throw std::invalid_argument("checkpoint_wal_bytes must be > 0");
    }
    if (options.wal_group_commit_updates == 0) {
        throw std::invalid_argument("wal_group_commit_updates must be > 0");
    }
    RequireValidVarLimit(options.var_max_chunk_bytes);
    if (options.feed_buffer_bytes == 0U) throw std::invalid_argument("feed_buffer_bytes must be > 0");
    if (options.slot_max_bytes == 0U) throw std::invalid_argument("slot_max_bytes must be > 0");
}

CatalogConfig CatalogConfigFromStoreConfig(
    const StoreConfig& config,
    std::uint32_t option_fields) {
    CatalogConfig catalog;
    catalog.data_dir = config.data_dir;
    catalog.access_mode = config.access_mode;
    catalog.allow_multiple_processes = config.allow_multiple_processes;
    catalog.default_options = TableOptions{
        .durability_mode = config.durability_mode,
        .checkpoint_update_interval = config.checkpoint_update_interval,
        .checkpoint_wal_bytes = config.checkpoint_wal_bytes,
        .wal_group_commit_updates = config.wal_group_commit_updates,
        .checkpoint_compression = config.checkpoint_compression,
        .var_max_chunk_bytes = config.var_max_chunk_bytes,
    };
    catalog.default_option_fields = option_fields;
    catalog.max_loaded_chunks = config.max_loaded_chunks;
    catalog.max_open_wal_streams = config.max_open_wal_streams;
    catalog.background_maintenance = config.background_maintenance;
    catalog.background_checkpoint_queue_limit = config.background_checkpoint_queue_limit;
    catalog.txn_history_bytes = config.txn_history_bytes;
    catalog.slot_max_bytes = config.slot_max_bytes;
    catalog.slot_sync_interval = config.slot_sync_interval;
    return catalog;
}

Table::Lease::Lease(Table* table, ChunkStore* store) noexcept
    : table_(table),
      store_(store) {}

Table::Lease::Lease(Lease&& other) noexcept
    : table_(std::exchange(other.table_, nullptr)),
      store_(std::exchange(other.store_, nullptr)) {}

Table::Lease::~Lease() {
    if (table_ != nullptr) {
        table_->ReleaseLease();
    }
}

Table::Table(
    std::string name,
    std::filesystem::path dir,
    StoreId store_id,
    Geometry geometry,
    TableOptions options,
    std::shared_ptr<ChunkStore> store,
    std::size_t feed_buffer_bytes,
    std::shared_ptr<MigrationHealth> migration_health,
    std::chrono::milliseconds feed_linger)
    : name_(std::move(name)),
      dir_(std::move(dir)),
      store_id_(store_id),
      lineage_(store->lineage()),
      feed_buffer_bytes_(feed_buffer_bytes),
      effective_feed_buffer_bytes_(options.feed_buffer_bytes.value_or(feed_buffer_bytes)),
      feed_linger_(feed_linger),
      migration_health_(std::move(migration_health)),
      geometry_(std::move(geometry)),
      store_(std::move(store)),
      options_(options) {
    slot_max_bytes_ = store_->feed_slots_->max_bytes();
    slot_ack_state_ = std::make_shared<FeedSlotAckState>();
    store_->feed_slots_->UseAckState(slot_ack_state_);
    if (store_->feed_slots_->active() && store_->access_mode_ == AccessMode::kReadWrite) {
        feed_ = std::make_shared<ChangeFeed>(store_id_, FeedBufferBytes());
        feed_->Resume(*store_);
        store_->feed_slots_->Start(feed_);
    }
    else if (store_->feed_slots_->ArchiveRequired()) store_->feed_slots_->Start(nullptr);
}

Table::~Table() {
    StopFeedLingerTimer();
    if (store_) store_->feed_slots_->Stop();
    if (feed_) {
        if (store_) store_->feed_.store(nullptr, std::memory_order_seq_cst);
        feed_->End();
    }
}

bool Table::RetainIdleFeed() const {
    std::lock_guard lock(feed_timer_mutex_);
    return feed_linger_deadline_ && std::chrono::steady_clock::now() < *feed_linger_deadline_;
}

bool FeedTestAccess::WaitLingerExpired(Table& table, std::chrono::milliseconds timeout) {
    std::unique_lock lock(table.feed_timer_mutex_);
    return table.feed_timer_cv_.wait_for(lock, timeout, [&] {
        return !table.feed_linger_deadline_ && table.feed_timer_finished_.load(std::memory_order_acquire);
    });
}

bool FeedTestAccess::Capturing(Table& table) {
    auto lease = table.Acquire();
    if (!lease) return false;
    return lease->store().feed_.load(std::memory_order_seq_cst) != nullptr;
}

void FeedTestAccess::ExpireLinger(Table& table) {
    std::lock_guard lock(table.feed_timer_mutex_);
    if (!table.feed_linger_deadline_) throw std::logic_error("table is not lingering");
    table.feed_linger_deadline_ = std::chrono::steady_clock::now();
    table.feed_timer_cv_.notify_all();
}

bool FeedTestAccess::WaitLingerDraining(Table& table, std::chrono::milliseconds timeout) {
    std::unique_lock lock(table.mutex_);
    return table.cv_.wait_for(lock, timeout, [&] {
        return table.state_.load(std::memory_order_seq_cst) == Table::State::kBusy;
    });
}

void FeedTestAccess::CancelLingerTimer(Table& table) {
    std::lock_guard control(table.feed_timer_control_mutex_);
    table.StopFeedLingerTimer();
}

void Table::StopFeedLingerTimer() {
    if (!feed_linger_timer_.joinable()) return;
    feed_linger_timer_.request_stop();
    feed_timer_cv_.notify_all();
    feed_linger_timer_.join();
    feed_linger_timer_ = std::jthread{};
    feed_timer_finished_.store(true, std::memory_order_release);
}

void Table::StartFeedLingerTimer() {
    assert(!feed_linger_timer_.joinable());
    feed_linger_timer_ = std::jthread([this](std::stop_token cancelled) {
        std::unique_lock lock(feed_timer_mutex_);
        try {
            while (!cancelled.stop_requested()) {
                if (!feed_timer_cv_.wait(lock, cancelled, [this] { return feed_linger_deadline_.has_value(); })) break;
                const auto deadline = *feed_linger_deadline_;
                if (feed_timer_cv_.wait_until(lock, cancelled, deadline, [this, deadline] {
                    return !feed_linger_deadline_ || *feed_linger_deadline_ != deadline;
                })) continue;
                if (cancelled.stop_requested()) break;
                lock.unlock();
                const bool expired = ExpireFeedLinger(cancelled);
                lock.lock();
                if (expired) break;
            }
        } catch (const std::exception& error) {
            if (lock.owns_lock()) lock.unlock();
            QuarantineFeedLinger(error, nullptr, false);
            lock.lock();
        }
        feed_timer_finished_.store(true, std::memory_order_release);
        feed_timer_cv_.notify_all();
    });
    feed_timer_finished_.store(false, std::memory_order_release);
}

bool Table::ExpireFeedLinger(std::stop_token cancelled) {
    std::shared_ptr<ChunkStore> store;
    bool admitted = false;
    try {
        store = BeginExclusive(/*closing=*/true, cancelled, &admitted);
        if (!store) return true;
        const bool expires = feed_subscriptions_ == 0U && !RetainIdleFeed();
        if (expires) {
            { std::lock_guard lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
            if (!store->feed_slots_->active() && feed_) {
                feed_->End();
                feed_.reset();
            }
        }
        // Keep the store alive if Resume or geometry publication throws.
        if (migration_health_->failed.load(std::memory_order_acquire)) EndExclusive(nullptr, options_);
        else EndExclusive(store, options_, /*check_feed_error=*/true);
        return expires;
    } catch (const std::exception& error) {
        QuarantineFeedLinger(error, std::move(store), admitted);
        return true;
    }
}

void Table::QuarantineFeedLinger(const std::exception& error, std::shared_ptr<ChunkStore> store, bool admitted) noexcept {
    auto report = [&](const std::exception& failure) noexcept {
        try {
            LogMessage(LogLevel::kError, LogComponent::kStore, "feed cleanup failed; the table is fenced until RECOVER TABLE or a restart",
                {{"table", name_}, {"error", failure.what()}});
        } catch (const std::exception& log_error) {
            std::fprintf(stderr, "feed cleanup failed for table %s: %s (logging failed: %s)\n",
                name_.c_str(), failure.what(), log_error.what());
        }
    };
    feed_cleanup_failed_.store(true, std::memory_order_release);
    ended_.store(kWatchFenced, std::memory_order_release);
    cv_.notify_all();
    if (admitted) {
        if (!store) { std::lock_guard lock(mutex_); store = store_; }
        if (store) {
            store->feed_.store(nullptr, std::memory_order_seq_cst);
            store->feed_watchers_active_.store(false, std::memory_order_release);
            try { store->feed_slots_->Stop(); }
            catch (const std::exception& cleanup_error) { report(cleanup_error); }
        }
        if (feed_) {
            try { feed_->End(kWatchFenced); }
            catch (const std::exception& cleanup_error) { report(cleanup_error); }
        }
    }
    {
        std::lock_guard lock(mutex_);
        if (admitted) store_ = std::move(store);
        // An unowned Busy state belongs to another exclusive operation. Its
        // EndExclusive observes the fence and detaches before publishing.
        if (admitted || state_.load(std::memory_order_seq_cst) == State::kOpen)
            state_.store(State::kFailed, std::memory_order_seq_cst);
    }
    { std::lock_guard lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
    cv_.notify_all();
    report(error);
}

std::unique_ptr<FeedSubscription> Table::SubscribeFeed(const FeedOptions& options) {
    if (options.ack_timeout_ms) throw std::invalid_argument("ACK TIMEOUT requires a slot watch");
    if (options.area && (options.area->first.x > options.area->last.x || options.area->first.y > options.area->last.y))
        throw std::invalid_argument("feed area bounds are reversed");
    std::lock_guard timer_control(feed_timer_control_mutex_);
    if (feed_timer_finished_.load(std::memory_order_acquire)) StopFeedLingerTimer();
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ == AccessMode::kReadOnly || store->allow_multiple_processes_)
        throw std::invalid_argument("feed requires a single-process read-write table");
    if (options.column_ids) {
        std::vector<std::uint32_t> seen;
        if (options.column_ids->empty()) throw std::invalid_argument("feed projection must name at least one column");
        const auto& columns = store->geometry().layout().schema().columns;
        for (const auto id : *options.column_ids) {
            if (std::none_of(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; }))
                throw std::invalid_argument("unknown feed projection id");
            if (std::find(seen.begin(), seen.end(), id) != seen.end()) throw std::invalid_argument("duplicate feed projection id");
            seen.push_back(id);
        }
    }
    if (feed_ && options.buffer_bytes && *options.buffer_bytes != feed_->budget()) {
        if (feed_subscriptions_ != 0U || !store->feed_slots_->active())
            throw std::invalid_argument("feed buffer bytes differ from the active feed");
        // Slot-only history is recoverable from its WAL archives. Establish
        // the server's configured ring budget before the first socket reader.
        feed_->End();
        feed_.reset();
    }
    if (!feed_) feed_ = std::make_shared<ChangeFeed>(store_id_, options.buffer_bytes.value_or(FeedBufferBytes()));
    ScopeExit unused_feed([&] {
        if (feed_subscriptions_ == 0U && !store->feed_slots_->active()) {
            { std::lock_guard timer_lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
            StopFeedLingerTimer();
            store->feed_.store(nullptr, std::memory_order_seq_cst);
            feed_->End();
            feed_.reset();
        }
    });
    { std::lock_guard timer_lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
    feed_timer_cv_.notify_all();
    if (feed_linger_.count() > 0 && !feed_linger_timer_.joinable()) StartFeedLingerTimer();
    feed_->Resume(*store);
    auto subscription = feed_->Subscribe(weak_from_this(), options);
    ++feed_subscriptions_;
    store->feed_watchers_active_.store(true, std::memory_order_release);
    unused_feed.Dismiss();
    return subscription;
}

void Table::ReleaseFeed(const std::shared_ptr<ChangeFeed>& feed) {
    // Subscription destruction must finish even after migration admission
    // closes. This admission is restricted to subscription cleanup.
    std::lock_guard timer_control(feed_timer_control_mutex_);
    auto store = BeginExclusive(/*closing=*/true);
    if (!store) return;
    ScopeExit serving([&] {
        if (migration_health_->failed.load(std::memory_order_acquire)) {
            store.reset();
            EndExclusive(nullptr, options_);
        } else EndExclusive(std::move(store), options_);
    });
    if (feed_ != feed) return;
    if (--feed_subscriptions_ == 0U) {
        if (!store->feed_slots_->active() && feed_->HasError()) {
            // A failed ring has no resumable history. Let a fresh reader
            // establish its floor immediately instead of inheriting the error.
            { std::lock_guard timer_lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
            StopFeedLingerTimer();
            feed_->End();
            feed_.reset();
        } else if (feed_linger_.count() > 0 &&
            !migration_health_->failed.load(std::memory_order_acquire)) {
            { std::lock_guard timer_lock(feed_timer_mutex_);
              feed_linger_deadline_ = std::chrono::steady_clock::now() + feed_linger_; }
            feed_timer_cv_.notify_all();
        } else if (!store->feed_slots_->active()) {
            feed_->End();
            feed_.reset();
        } else StopFeedLingerTimer();
    }
}

void Table::StopFeed() {
    std::lock_guard timer_control(feed_timer_control_mutex_);
    StopFeedLingerTimer();
    { std::lock_guard timer_lock(feed_timer_mutex_); feed_linger_deadline_.reset(); }
    auto store = BeginExclusive();
    if (!store) return;
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (feed_) feed_->End();
    feed_.reset();
    feed_subscriptions_ = 0U;
    store->feed_watchers_active_.store(false, std::memory_order_release);
    if (store->feed_slots_->active())
        feed_ = std::make_shared<ChangeFeed>(store_id_, FeedBufferBytes());
}

FeedSlot Table::CreateFeedSlot(std::string_view name, bool if_not_exists, const FeedSlotFilters& filters) {
    RequireValidFeedSlotName(name);
    std::lock_guard ddl(ddl_mutex_);
    if (if_not_exists) {
        auto lease = Acquire();
        if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
        auto& store = lease->store();
        if (store.access_mode_ != AccessMode::kReadWrite || store.allow_multiple_processes_)
            throw std::invalid_argument("feed slots require a single-process read-write table");
        store.ThrowIfDurabilityPoisoned();
        const auto slots = store.feed_slots_->List(true);
        const auto existing = std::find_if(slots.begin(), slots.end(), [&](const auto& slot) { return slot.name == name; });
        if (existing != slots.end()) {
            if (existing->filters != filters) throw FeedSlotFiltersError("feed slot filters differ from existing slot");
            return *existing;
        }
    }
    {
        const auto lease = Acquire();
        if (!lease) throw TableNotFoundError("table was dropped");
        RequireFeature(lease->store().features(), *FindFeature("feed_slots"));
        if (!filters.empty()) RequireFeature(lease->store().features(), *FindFeature("slot_filters"));
    }
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ != AccessMode::kReadWrite || store->allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process read-write table");
    store->ThrowIfDurabilityPoisoned();
    RequireFeature(store->features(), *FindFeature("feed_slots"));
    if (!filters.empty()) RequireFeature(store->features(), *FindFeature("slot_filters"));
    // Validate the complete filter before baseline, feature or record publication.
    (void)SerializeFeedSlotRecords(FeedSlotRecords{store_id_, 0U, {{std::string(name), 0U, false, filters}},
        false, store->geometry_.dims()});
    if (filters.column_ids) {
        const auto& columns = store->geometry_.layout().schema().columns;
        for (const auto id : *filters.column_ids)
            if (std::none_of(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; }))
                throw std::invalid_argument("slot projection references an unknown column id");
    }
    const auto slots = store->feed_slots_->List();
    if (std::any_of(slots.begin(), slots.end(), [&](const auto& slot) { return slot.name == name; }))
        throw std::invalid_argument("feed slot already exists: " + std::string(name));
    // Allocate before publishing a slot. Maintenance and cross-table eviction
    // do not take Table leases, so quiesce both before changing cached flags.
    if (!feed_) feed_ = std::make_shared<ChangeFeed>(store_id_, FeedBufferBytes());
    store->StopMaintenanceThread();
    ScopeExit maintenance([&] {
        if (!store->background_maintenance_) return;
        try { store->StartMaintenanceThread(); }
        catch (const std::exception& error) { store->PoisonDurability("cannot resume maintenance: " + std::string(error.what())); }
    });
    std::unique_lock eviction_lock(store->resources_->stores_mutex_);
    PrepareFeedSlotBaseline(*store);
    return store->feed_slots_->Create(name, store->version_clock_.load(std::memory_order_seq_cst) - 1U, false, filters);
}

void Table::PrepareFeedSlotBaseline(ChunkStore& store) {
    if (!store.feed_slots_->active()) {
        // A pre-slot WAL may outlive a checkpoint whose image also contains
        // staged frames absent from that WAL. Normalize this baseline before
        // promising history, so every future segment has an exact old image.
        std::set<std::pair<std::int64_t, std::int64_t>> coordinates;
        {
            std::lock_guard chunks_lock(store.large_chunks_mutex_);
            for (const auto& [_, large] : store.large_chunks_) {
                std::lock_guard large_lock(large->mutex);
                for (const auto& [coord, chunk] : large->chunks)
                    if (!chunk->wal_batch.empty() || chunk->wal_bytes != 0U)
                        coordinates.emplace(coord.x, coord.y);
            }
        }
        static const std::regex wal_name(R"(^C_(-?[0-9]+)_(-?[0-9]+)\.wal$)");
        for (const auto& directory : std::filesystem::directory_iterator(dir_)) {
            if (!directory.is_directory() || directory.path().filename().string().rfind("L_", 0U) != 0U) continue;
            for (const auto& file : std::filesystem::directory_iterator(directory.path())) {
                std::smatch match;
                const auto filename = file.path().filename().string();
                if (std::regex_match(filename, match, wal_name))
                    coordinates.emplace(std::stoll(match[1].str()), std::stoll(match[2].str()));
            }
        }
        for (const auto& [x, y] : coordinates) {
            const ChunkCoord coord{x, y};
            auto chunk = store.TryGetLoadedChunk(coord);
            if (!chunk) {
                // A temporary off-cache state avoids cache admission invoking
                // the global evictor whose registry is quiesced here.
                auto loaded = store.LoadChunkPayload(coord, true);
                BringToCurrentSchema(store.geometry_, loaded.schema_version, loaded.presence_bitmap, &loaded.payload, &loaded.vars);
                chunk = std::make_shared<ChunkStore::RegularChunk>(std::move(loaded.payload), std::move(loaded.presence_bitmap));
                chunk->vars = std::move(loaded.vars);
                chunk->version = loaded.revision != 0U ? loaded.revision : store.NextChunkVersion();
                chunk->commit_time_ms = loaded.commit_time_ms;
                chunk->wal_bytes = loaded.wal_bytes;
                chunk->wal_header_written = loaded.wal_header_written;
                chunk->wal_path = loaded.wal_path;
                if (loaded.revision != 0U) store.RaiseVersionClockAbove(loaded.revision);
            }
            std::unique_lock chunk_lock(chunk->mutex);
            store.CheckpointChunk(coord, chunk);
        }
        store.feed_slots_->prefix_index().Clear();
    }
}

void Table::DropFeedSlot(std::string_view name, bool if_exists) {
    RequireValidFeedSlotName(name);
    std::lock_guard ddl(ddl_mutex_);
    if (if_exists) {
        auto lease = Acquire();
        if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
        auto& store = lease->store();
        if (store.access_mode_ != AccessMode::kReadWrite || store.allow_multiple_processes_)
            throw std::invalid_argument("feed slots require a single-process read-write table");
        store.ThrowIfDurabilityPoisoned();
        const auto slots = store.feed_slots_->List(true);
        if (std::none_of(slots.begin(), slots.end(), [&](const auto& slot) { return slot.name == name; })) return;
    }
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ != AccessMode::kReadWrite || store->allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process read-write table");
    store->ThrowIfDurabilityPoisoned();
    const auto claimed = slot_claims_.find(std::string(name));
    store->feed_slots_->Drop(name);
    if (claimed != slot_claims_.end()) {
        if (auto claim = claimed->second.lock()) claim->valid.store(false, std::memory_order_release);
        slot_claims_.erase(claimed);
    }
    store->feed_slots_->Retain();
    if (!store->feed_slots_->active() && feed_subscriptions_ == 0U && feed_ && !RetainIdleFeed()) {
        feed_->End();
        feed_.reset();
    }
}

std::vector<FeedSlot> Table::ListFeedSlots(bool include_lost) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    auto slots = lease->store().feed_slots_->List(include_lost);
    const auto& columns = lease->store().geometry().layout().schema().columns;
    for (auto& slot : slots) if (slot.filters.column_ids) {
        for (const auto id : *slot.filters.column_ids) {
            const auto at = std::find_if(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; });
            slot.column_names.push_back(at == columns.end() ? std::nullopt : std::optional<std::string>(at->name));
        }
    }
    return slots;
}

std::vector<std::uint32_t> Table::ResolveFeedColumns(const std::vector<std::string>& names) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    const auto& columns = lease->store().geometry().layout().schema().columns;
    std::vector<std::uint32_t> ids;
    for (const auto& name : names) {
        const auto at = std::find_if(columns.begin(), columns.end(), [&](const auto& column) { return column.name == name; });
        if (at == columns.end()) throw std::invalid_argument("unknown feed column: " + name);
        if (std::find(ids.begin(), ids.end(), at->id) != ids.end()) throw std::invalid_argument("duplicate feed column: " + name);
        ids.push_back(at->id);
    }
    if (ids.empty()) throw std::invalid_argument("feed projection must name at least one column");
    return ids;
}

std::pair<FeedSlot, std::shared_ptr<FeedSlotClaim>> Table::ClaimFeedSlot(std::string_view name,
    const FeedOptions& options, bool takeover, std::string user,
    const std::function<void(std::string_view)>& authorize_owner) {
    std::shared_ptr<FeedSlotClaim> old;
    std::shared_ptr<SlotWatch> owner;
    const auto validate = [&](ChunkStore& store) {
        if (store.access_mode_ != AccessMode::kReadWrite || store.allow_multiple_processes_)
            throw std::invalid_argument("feed slots require a single-process read-write table");
        auto slot = store.feed_slots_->Get(name);
        if (slot.lost) throw FeedSlotLostError(slot.schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
            slot.schema_filter_lost ? "schema_filter" : "retention_limit");
        if ((options.area && options.area != slot.filters.area) ||
            (options.column_ids && options.column_ids != slot.filters.column_ids) ||
            (options.no_before && !slot.filters.no_before) || (options.chunks && !slot.filters.chunks))
            throw FeedSlotFiltersError("feed slot filters differ from WATCH filters");
        return slot;
    };
    {
        auto store = BeginExclusive();
        if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
        ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
        auto slot = validate(*store);
        auto& existing = slot_claims_[std::string(name)];
        old = existing.lock();
        if (old && old->takeover_pending.load(std::memory_order_acquire))
            throw FeedSlotBusyError("feed slot ownership handoff is pending", "takeover_pending");
        if (!old || !old->valid.load(std::memory_order_acquire)) {
            auto claim = std::make_shared<FeedSlotClaim>(std::string(name), store->version_clock_.load(std::memory_order_seq_cst) - 1U, slot_output_bytes_, std::move(user));
            existing = claim;
            return {std::move(slot), std::move(claim)};
        }
        if (!takeover) throw FeedSlotBusyError("feed slot already has a watch");
        if (authorize_owner) authorize_owner(old->user);
        {
            std::lock_guard lock(old->owner_mutex);
            owner = old->owner.lock();
            if (!owner || !owner->io_ready_.load(std::memory_order_acquire))
                throw FeedSlotBusyError("feed slot owner is attaching or returning", "takeover_pending");
            old->takeover_pending.store(true, std::memory_order_release);
        }
    }
    ScopeExit pending([&] { old->takeover_pending.store(false, std::memory_order_release); });
    if (auto* hook = owner->hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterTakeoverReserved, 0U);
    // No table lease or catalog lock survives this wait. The old generation
    // publishes only accepted ACKs and confirms the socket was actually closed.
    owner->RequestTakeoverAndWait();
    // If the old connection ended just before the fence, its normal cleanup
    // may have staged an ACK without forcing the batch. Complete that accepted
    // position before publishing a replacement generation too.
    std::uint64_t acknowledged;
    { std::lock_guard lock(owner->mutex_); acknowledged = owner->acknowledged_; }
    StageClaimedFeedSlotAck(old, {store_id_, acknowledged});
    if (ReadClaimedFeedSlot(old).durable_watermark < acknowledged) SyncClaimedFeedSlot(old);
    FlushClaimedFeedSlotAcks(old, true);
    if (ReadClaimedFeedSlot(old).position.revision < acknowledged)
        throw std::logic_error("takeover did not persist the accepted ACK");
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    const auto found = slot_claims_.find(std::string(name));
    if (!old->valid.load(std::memory_order_acquire) || found == slot_claims_.end() || found->second.lock() != old)
        throw FeedSlotLostError("feed slot was removed during takeover");
    auto slot = validate(*store);
    auto claim = std::make_shared<FeedSlotClaim>(std::string(name), store->version_clock_.load(std::memory_order_seq_cst) - 1U, slot_output_bytes_, std::move(user));
    old->valid.store(false, std::memory_order_release);
    found->second = claim;
    return {std::move(slot), std::move(claim)};
}

FeedSlot Table::ReadClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    const auto slot = lease->store().feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError(slot.schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
        slot.schema_filter_lost ? "schema_filter" : "retention_limit");
    return slot;
}

void Table::SyncClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    auto& store = lease->store();
    const auto slot = store.feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError(slot.schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
        slot.schema_filter_lost ? "schema_filter" : "retention_limit");
    store.feed_slots_->Sync(feed_->CompletedWatermark());
}

FeedArchiveReader Table::ReadClaimedFeedArchive(const std::shared_ptr<FeedSlotClaim>& claim, FeedPosition after) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    const auto slot = lease->store().feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError(slot.schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
        slot.schema_filter_lost ? "schema_filter" : "retention_limit");
    return lease->store().feed_slots_->ReaderCompletedPrefix(claim->name, after);
}

void Table::StageClaimedFeedSlotAck(const std::shared_ptr<FeedSlotClaim>& claim, FeedPosition position) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    lease->store().feed_slots_->StageAck(claim->name, position);
}

void Table::FlushClaimedFeedSlotAcks(const std::shared_ptr<FeedSlotClaim>& claim, bool force) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    auto& slots = *lease->store().feed_slots_;
    const auto slot = slots.Get(claim->name);
    if (slot.lost) throw FeedSlotLostError(slot.schema_filter_lost ? "feed slot projected column was dropped" : "feed slot exceeded its retention limit",
        slot.schema_filter_lost ? "schema_filter" : "retention_limit");
    if (slots.FlushAcks(force)) slots.Retain();
}

void Table::AdvanceFeedSlot(std::string_view name, FeedPosition position) {
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ != AccessMode::kReadWrite || store->allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process read-write table");
    store->feed_slots_->Advance(name, position);
    store->feed_slots_->Retain();
}

FeedArchiveReader Table::ReadFeedArchive(FeedPosition after) {
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    store->StopMaintenanceThread();
    ScopeExit maintenance([&] {
        if (!store->background_maintenance_ || store->access_mode_ != AccessMode::kReadWrite) return;
        try { store->StartMaintenanceThread(); }
        catch (const std::exception& error) { store->PoisonDurability("cannot resume maintenance: " + std::string(error.what())); }
    });
    std::unique_lock eviction_lock(store->resources_->stores_mutex_);
    return store->feed_slots_->Reader(after);
}

Geometry Table::geometry() const {
    std::lock_guard lock(mutex_);
    return geometry_;
}

TableInfo Table::Info() const {
    std::lock_guard lock(mutex_);
    return TableInfo{
        .name = name_,
        .store_id = store_id_,
        .geometry = geometry_.config(),
        .schema = geometry_.layout().schema(),
        .options = options_,
        .feed_buffer_bytes = FeedBufferBytes(),
        .slot_max_bytes = slot_max_bytes_,
        .chunk_schema = geometry_.chunk_schema(),
    };
}

const char* Table::FenceReason() const {
    // store_ cannot close while mutex_ is held: BeginExclusive takes it under mutex_.
    std::lock_guard lock(mutex_);
    if (feed_cleanup_failed_.load(std::memory_order_acquire)) return kFenceFeed;
    return store_ ? StoreFenceReason(*store_) : nullptr;
}

const char* Table::StoreFenceReason(const ChunkStore& store) {
    if (store.durability_poisoned_.load(std::memory_order_acquire)) return kFenceDurability;
    if (store.snapshot_generation_mapping_ && store.snapshot_generation_mapping_->IsFailed()) return kFenceSnapshot;
    return nullptr;
}

bool Table::ServesUnfenced() const {
    std::lock_guard lock(mutex_);
    return state_.load(std::memory_order_seq_cst) == State::kOpen && store_ &&
           !feed_cleanup_failed_.load(std::memory_order_acquire) && StoreFenceReason(*store_) == nullptr;
}

std::optional<Table::Lease> Table::Acquire() {
    while (true) {
        migration_health_->Check();
        active_leases_.fetch_add(1, std::memory_order_seq_cst);
        const State state = state_.load(std::memory_order_seq_cst);
        if (state == State::kOpen) {
            return Lease(this, store_.get());
        }
        // Not open: take the count back (waking an exclusive operation
        // waiting for it to drain), then report the drop or wait for the
        // reopen to finish.
        ReleaseLease();
        // A table recovered after its feed cleanup failed is gone: its name
        // holds the recovered table.
        if (state == State::kGone) {
            return std::nullopt;
        }
        if (state == State::kFailed || feed_cleanup_failed_.load(std::memory_order_acquire))
            throw FeedRecoveryRequiredError();
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this]() {
            return state_.load(std::memory_order_seq_cst) != State::kBusy || feed_cleanup_failed_.load(std::memory_order_acquire);
        });
    }
}

void Table::ReleaseLease() noexcept {
    if (active_leases_.fetch_sub(1, std::memory_order_seq_cst) == 1 &&
        state_.load(std::memory_order_seq_cst) == State::kBusy) {
        // The waiter checks the count under the mutex before it sleeps, so
        // taking the mutex here cannot miss it.
        std::lock_guard lock(mutex_);
        cv_.notify_all();
    }
}

Table::BackupPin::BackupPin(BackupPin&& other) noexcept
    : table_(std::move(other.table_)), store_(std::move(other.store_)) {}
Table::BackupPin::~BackupPin() { if (table_) table_->ReleaseBackupPin(); }
Table::BackupPin Table::PinForBackup(std::stop_token cancelled) {
    std::unique_lock lock(mutex_);
    ++backup_pin_waiters_;
    cv_.notify_all();
    const bool ready = cv_.wait(lock, cancelled, [this] {
        return state_.load(std::memory_order_seq_cst) != State::kBusy || feed_cleanup_failed_.load(std::memory_order_acquire);
    });
    --backup_pin_waiters_;
    if (!ready || cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
    if (feed_cleanup_failed_.load(std::memory_order_acquire)) throw FeedRecoveryRequiredError();
    if (state_.load(std::memory_order_seq_cst) == State::kGone)
        throw TableNotFoundError("table was dropped");
    ++backup_pins_;
    return BackupPin(shared_from_this(), store_);
}
void Table::ReleaseBackupPin() noexcept {
    std::lock_guard lock(mutex_);
    --backup_pins_;
    cv_.notify_all();
}

std::shared_ptr<ChunkStore> Table::BeginExclusive(bool closing, std::stop_token cancelled, bool* admitted) {
    if (admitted) *admitted = false;
    if (!closing) {
        migration_health_->Check();
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
            hook->Run(MigrationTestHook::Point::kBeforeTableExclusive, name_);
    }
    std::unique_lock lock(mutex_);
    if (!cv_.wait(lock, cancelled, [this, closing] {
        return (state_.load(std::memory_order_seq_cst) != State::kBusy && backup_pins_ == 0U) ||
            (!closing && feed_cleanup_failed_.load(std::memory_order_acquire));
    }) ||
        cancelled.stop_requested()) return nullptr;
    if (!closing) migration_health_->Check();
    if (!closing && feed_cleanup_failed_.load(std::memory_order_acquire)) throw FeedRecoveryRequiredError();
    if (state_.load(std::memory_order_seq_cst) == State::kGone) return nullptr;
    state_.store(State::kBusy, std::memory_order_seq_cst);
    if (cancelled.stop_possible()) cv_.notify_all();
    bool drained;
    try {
        drained = cv_.wait(lock, cancelled, [this]() { return active_leases_.load(std::memory_order_seq_cst) == 0; });
    } catch (const std::exception&) {
        state_.store(feed_cleanup_failed_.load(std::memory_order_acquire) ? State::kFailed : State::kOpen, std::memory_order_seq_cst);
        cv_.notify_all();
        throw;
    }
    if (!drained) {
        state_.store(feed_cleanup_failed_.load(std::memory_order_acquire) ? State::kFailed : State::kOpen, std::memory_order_seq_cst);
        cv_.notify_all();
        return nullptr;
    }
    if (admitted) *admitted = true;
    store_->feed_slots_->Stop();
    if (feed_) {
        if (admitted) feed_->RunHook(FeedTestHook::Point::kBeforeLingerPause, 0U);
        store_->feed_.store(nullptr, std::memory_order_seq_cst);
        if (!store_->feed_slots_->active() && feed_subscriptions_ == 0U && !RetainIdleFeed()) {
            // Automatic slot loss can leave an untracked interval. A fresh
            // feed must establish its floor before accepting another resume.
            feed_->End();
            feed_.reset();
        } else {
            feed_->Pause();
        }
    }
    return std::move(store_);
}

void Table::FailFeed(std::exception_ptr error) {
    // Control-only, after BeginExclusive paused and joined the sender.
    if (feed_) feed_->Fail(std::move(error));
}

void Table::EndExclusive(std::shared_ptr<ChunkStore> store, const TableOptions& options, bool check_feed_error) {
    const auto feed_budget = options.feed_buffer_bytes.value_or(feed_buffer_bytes_);
    if (store && feed_budget != FeedBufferBytes()) {
        if (feed_) feed_->ResizeBudget(feed_budget);
        effective_feed_buffer_bytes_.store(feed_budget, std::memory_order_release);
    }
    if (store && (feed_cleanup_failed_.load(std::memory_order_acquire) ||
        migration_health_->failed.load(std::memory_order_acquire))) {
        store->feed_.store(nullptr, std::memory_order_seq_cst);
        store->feed_watchers_active_.store(false, std::memory_order_release);
        store->feed_slots_->Stop();
        if (feed_) feed_->End(kWatchFenced);
        {
            std::lock_guard lock(mutex_);
            store_ = std::move(store);
            options_ = options;
            slot_max_bytes_ = store_->feed_slots_->max_bytes();
            state_.store(State::kFailed, std::memory_order_seq_cst);
        }
        cv_.notify_all();
        return;
    }
    if (store && !store->feed_slots_->active() && feed_subscriptions_ == 0U && feed_ && !RetainIdleFeed()) {
        feed_->End();
        feed_.reset();
    }
    if (store && store->feed_slots_->active() && store->access_mode_ == AccessMode::kReadWrite && !feed_)
        feed_ = std::make_shared<ChangeFeed>(store_id_, FeedBufferBytes());
    if (feed_) {
        if (store == nullptr) feed_->End(ended_.load(std::memory_order_acquire));
        else if (!feed_->attached()) feed_->Resume(*store);
        if (store && check_feed_error) feed_->RethrowError();
    }
    if (store) {
        store->feed_slots_->UseAckState(slot_ack_state_);
        store->feed_watchers_active_.store(feed_subscriptions_ != 0U || RetainIdleFeed(), std::memory_order_release);
        store->feed_slots_->Start(feed_);
    }
    {
        std::lock_guard lock(mutex_);
        const State next = store != nullptr ? State::kOpen : State::kGone;
        if (store != nullptr) {
            // A reopened store may have other columns.
            geometry_ = store->geometry();
            slot_max_bytes_ = store->feed_slots_->max_bytes();
        }
        store_ = std::move(store);
        options_ = options;
        state_.store(next, std::memory_order_seq_cst);
    }
    cv_.notify_all();
}

TableCatalog::TableCatalog(CatalogConfig config)
    : config_(std::move(config)) {
    if (config_.data_dir.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    config_.data_dir = std::filesystem::weakly_canonical(config_.data_dir);
    if (config_.feed_buffer_bytes == 0U) throw std::invalid_argument("feed_buffer_bytes must be positive");
    if (config_.slot_max_bytes == 0U) throw std::invalid_argument("slot_max_bytes must be positive");
    if (config_.feed_linger.count() < 0 || config_.feed_linger.count() > std::numeric_limits<std::int32_t>::max())
        throw std::invalid_argument("feed_linger must be 0..2147483647 milliseconds");
    RequireNotBackupDirectory(config_.data_dir, config_.access_mode);
    RequireValidTableOptions(config_.default_options);
    resources_ = std::make_shared<StoreResources>(
        config_.max_loaded_chunks, config_.max_open_wal_streams);

    const bool writable = config_.access_mode == AccessMode::kReadWrite;
    if (writable) {
        EnsureDirectoryPathExists(config_.data_dir, false);
    } else if (!IsDirectory(config_.data_dir)) {
        throw std::runtime_error(
            "read-only data directory is unavailable: " + config_.data_dir.string());
    }
    // A completed backup is read as it was published: its inventory names
    // every file, so a missing or changed chunk is refused instead of
    // reading as an absent one.
    if (!writable && std::filesystem::symlink_status(config_.data_dir / kBackupMarkerName).type() !=
                         std::filesystem::file_type::not_found) {
        try {
            ValidateBackupInventory(config_.data_dir, ReadBackupRecord(config_.data_dir));
        } catch (const std::exception& e) {
            throw std::runtime_error(
                "backup " + config_.data_dir.string() + " does not match its inventory: " + e.what() +
                "; check it with chunkdb_verify");
        }
    }
    // Refuse an unconverted directory before the writer lock touches it.
    // OpenDataDirManifest repeats the check under the lock.
    if (!std::filesystem::exists(DataDirManifestPath(config_.data_dir))) {
        if (const auto entry = FindDataDirEntry(config_.data_dir)) {
            ThrowUnconvertedDataDir(config_.data_dir, *entry);
        }
    }
    process_lock_ = AcquireWriterLock(
        config_.data_dir, config_.access_mode, config_.allow_multiple_processes);

    if (config_.allow_multiple_processes && std::filesystem::exists(config_.data_dir / kMigrationPendingFileName))
        throw MigrationRecoveryRequiredError("pending migration recovery requires the exclusive writer lock");
    OpenDataDirManifest();
    const auto activation = ReadFeatureActivationJournal(config_.data_dir);
    if (activation && config_.allow_multiple_processes)
        throw std::runtime_error("pending feature activation requires the exclusive writer lock");
    // Validate conflicting decisions before either recovery can publish files.
    if (activation) ValidateFeatureActivationJournal(config_.data_dir, *activation);
    RecoverFeatureActivation(config_.data_dir, config_.access_mode);
    RecoverMigrations(config_.data_dir, config_.access_mode);
    version_floor_ = DataDirVersionFloor(*ReadDataDirManifest(config_.data_dir));
    if (writable) {
        EnsureDirectoryPathExists(TablesDir(), /*durable_sync=*/true);
        RemoveInterruptedOperations();
    }
    OpenExistingTables();

    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "data directory opened",
        {
            {"data_dir", config_.data_dir.string()},
            {"tables", std::to_string(tables_.size())},
            {"access_mode", AccessModeName(config_.access_mode)},
            {"max_loaded_chunks", std::to_string(resources_->max_loaded_chunks())},
            {"max_open_wal_streams", std::to_string(resources_->max_open_wal_streams())},
        });
}

TableCatalog::~TableCatalog() {
    // Close every store while the writer lock is still held.
    std::map<std::string, std::shared_ptr<Table>, std::less<>> tables;
    {
        std::unique_lock lock(tables_mutex_);
        tables.swap(tables_);
    }
    for (auto& [_, table] : tables) {
        const TableOptions options = table->Info().options;
        auto store = table->BeginExclusive(/*closing=*/true);
        store.reset();
        table->EndExclusive(nullptr, options);
    }
    process_lock_.reset();
}

std::filesystem::path TableCatalog::TablesDir() const {
    return config_.data_dir / std::string(kTablesDirName);
}

std::filesystem::path TableCatalog::StagingDir() const {
    return config_.data_dir / std::string(kStagingDirName);
}

std::filesystem::path TableCatalog::DroppedDir() const {
    return config_.data_dir / std::string(kDroppedDirName);
}

void TableCatalog::OpenDataDirManifest() {
    const auto& data_dir = config_.data_dir;
    const auto path = DataDirManifestPath(data_dir);
    auto manifest = ReadDataDirManifest(data_dir);
    if (!manifest.has_value()) {
        if (const auto entry = FindDataDirEntry(data_dir)) {
            ThrowUnconvertedDataDir(data_dir, *entry);
        }
        if (config_.access_mode == AccessMode::kReadOnly) {
            throw std::runtime_error(
                "data directory " + data_dir.string() + " has no " +
                std::string(kDataDirManifestFileName) +
                "; read-only mode opens only an initialized data directory");
        }
        CleanupAtomicTmpArtifacts(path);
        const DataDirManifest created{
            .features = kKnownFeatures,
            .data_dir_id = NewStoreId(),
            .options = {},
        };
        if (PublishNewFile(
                path,
                SerializeDataDirManifest(created),
                "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_BEFORE_PUBLISH_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_DATA_DIR_MANIFEST_AFTER_PUBLISH_ONCE")) {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kStore,
                "created data directory manifest",
                {
                    {"path", path.string()},
                    {"data_dir_id", StoreIdHex(created.data_dir_id)},
                });
            return;
        }
        // Published by another process first (only possible without the
        // writer lock); open what it created.
        manifest = ReadDataDirManifest(data_dir);
        if (!manifest.has_value()) {
            throw std::runtime_error(
                "data directory manifest " + path.string() +
                " disappeared while the data directory was being initialized");
        }
    }
    try {
        RequireOpenableFeatures(manifest->features, config_.access_mode);
    } catch (const std::exception& e) {
        throw std::runtime_error("data directory " + data_dir.string() + ": " + e.what());
    }
    version_floor_ = DataDirVersionFloor(*manifest);
    if (config_.access_mode != AccessMode::kReadOnly) {
        // A crash while TABLEDROP raised the version floor leaves a temp file.
        CleanupAtomicTmpArtifacts(path);
    }
}

void TableCatalog::RaiseVersionFloor(std::uint64_t floor) {
    if (floor <= version_floor_) {
        return;
    }
    auto manifest = ReadDataDirManifest(config_.data_dir);
    if (!manifest.has_value()) {
        throw std::runtime_error(
            "data directory manifest " + DataDirManifestPath(config_.data_dir).string() + " disappeared");
    }
    SetDataDirVersionFloor(&*manifest, floor);
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_VERSION_FLOOR_WRITE_FAIL_ONCE")) {
        throw std::runtime_error("injected version floor write failure");
    }
    AtomicWrite(
        DataDirManifestPath(config_.data_dir),
        SerializeDataDirManifest(*manifest),
        /*fsync_file=*/true,
        /*fsync_directory=*/true,
        /*out_replaced=*/nullptr,
        /*after_rename_failpoint=*/nullptr,
        /*enable_generic_failpoints=*/false);
    version_floor_ = floor;
}

void TableCatalog::RemoveInterruptedOperations(std::string_view table) {
    // Operation directories are named `<table>.<suffix>`; table names hold no dot.
    const std::string prefix = table.empty() ? std::string{} : std::string(table) + ".";
    for (const auto& dir : {StagingDir(), DroppedDir()}) {
        if (!IsDirectory(dir)) continue;
        std::error_code ec;
        std::filesystem::directory_iterator it(dir, ec);
        const std::filesystem::directory_iterator end;
        std::vector<std::filesystem::path> leftovers;
        for (; !ec && it != end; it.increment(ec)) {
            if (it->path().filename().string().starts_with(prefix)) leftovers.push_back(it->path());
        }
        if (ec) {
            throw std::runtime_error("cannot list " + dir.string() + ": " + ec.message());
        }
        for (const auto& leftover : leftovers) {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kRecovery,
                dir == StagingDir() ? "removing a table whose creation was interrupted"
                                    : "removing a table whose drop was interrupted",
                {{"path", leftover.string()}});
            std::error_code remove_ec;
            std::filesystem::remove_all(leftover, remove_ec);
            if (remove_ec) {
                throw std::runtime_error(
                    "cannot remove " + leftover.string() + ": " + remove_ec.message());
            }
        }
    }
    // Backups belong to no table, so recovering one table leaves their staging.
    // Without the writer lock another process may be running a backup of
    // this data directory, so its staging waits for a single-process start.
    if (!table.empty() || process_lock_ == nullptr) return;
    const auto manifest = ReadDataDirManifest(config_.data_dir);
    if (!manifest) throw std::runtime_error("backup staging catalog manifest disappeared");
    RemoveOrphanedBackupStaging(config_.data_dir, manifest->data_dir_id);
}

struct TableCatalog::FoundTable {
    std::string name;
    std::filesystem::path dir;
    StoreManifest manifest;
    TableOptions options;
};

void TableCatalog::OpenExistingTables() {
    const auto tables_dir = TablesDir();
    if (!IsDirectory(tables_dir)) {
        return;
    }
    std::vector<std::pair<std::string, std::filesystem::path>> found;
    std::error_code ec;
    std::filesystem::directory_iterator it(tables_dir, ec);
    const std::filesystem::directory_iterator end;
    for (; !ec && it != end; it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (!IsValidTableName(name) || !IsDirectory(it->path())) {
            // Not something chunkdb creates there (an OS metadata file, say).
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "ignoring an entry of tables/ that is not a table",
                {{"path", it->path().string()}});
            continue;
        }
        found.emplace_back(name, it->path());
    }
    if (ec) {
        throw std::runtime_error("cannot list " + tables_dir.string() + ": " + ec.message());
    }
    std::sort(found.begin(), found.end());

    // Every manifest is read and checked against the option flags before any
    // table opens, so a refused start changes nothing on disk.
    std::vector<FoundTable> tables;
    std::string mismatches;
    for (const auto& [name, dir] : found) {
        tables.push_back(ReadTableToOpen(name, dir));
        mismatches += OptionFlagMismatches(name, tables.back().options);
    }
    if (!mismatches.empty()) throw std::runtime_error(OptionMismatchMessage(mismatches));

    for (auto& table : tables) tables_.emplace(table.name, OpenFoundTable(table));
}

TableCatalog::FoundTable TableCatalog::ReadTableToOpen(const std::string& name, const std::filesystem::path& dir) {
    auto manifest = ReadOpenableManifest(dir);
    const TableOptions options = DecodeTableOptions(manifest.options);
    if (manifest.schema.pending.has_value() && config_.access_mode == AccessMode::kReadWrite) {
        // A narrowing a crash interrupted: the schema stays as it was.
        manifest.schema = WithoutPendingNarrowing(manifest.schema);
        AtomicWrite(
            StoreManifestPath(dir), SerializeStoreManifest(manifest), /*fsync_file=*/true,
            /*fsync_directory=*/true, nullptr, nullptr, /*enable_generic_failpoints=*/false);
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kStore,
            "dropped a column narrowing that did not finish; the table keeps its columns",
            {{"table", name}});
    }
    return FoundTable{
        .name = name,
        .dir = dir,
        .manifest = std::move(manifest),
        .options = options,
    };
}

std::shared_ptr<Table> TableCatalog::OpenFoundTable(FoundTable& table) {
    auto store = OpenStore(
        table.name,
        table.dir,
        table.manifest.geometry,
        0U,
        table.options);
    // Read from the store before it is moved: argument evaluation order
    // is unspecified.
    const StoreId store_id = store->store_id();
    Geometry geometry = store->geometry();
    return std::shared_ptr<Table>(new Table(
        table.name, table.dir, store_id, std::move(geometry), table.options, std::move(store), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
}

std::shared_ptr<ChunkStore> TableCatalog::OpenStore(
    const std::string& name,
    const std::filesystem::path& dir,
    const GeometryConfig& geometry,
    std::uint32_t geometry_fields,
    const TableOptions& options) {
    StoreConfig store_config;
    store_config.geometry = geometry;
    store_config.geometry_fields = geometry_fields;
    store_config.data_dir = dir;
    store_config.durability_mode = options.durability_mode;
    store_config.checkpoint_update_interval = options.checkpoint_update_interval;
    store_config.checkpoint_wal_bytes = options.checkpoint_wal_bytes;
    store_config.wal_group_commit_updates = options.wal_group_commit_updates;
    store_config.checkpoint_compression = options.checkpoint_compression;
    store_config.var_max_chunk_bytes = options.var_max_chunk_bytes;
    store_config.allow_multiple_processes = config_.allow_multiple_processes;
    store_config.access_mode = config_.access_mode;
    store_config.background_maintenance = config_.background_maintenance;
    store_config.background_checkpoint_queue_limit = config_.background_checkpoint_queue_limit;
    store_config.txn_history_bytes = config_.txn_history_bytes;
    store_config.slot_max_bytes = options.slot_max_bytes.value_or(config_.slot_max_bytes);
    store_config.slot_sync_interval = config_.slot_sync_interval;
    store_config.resources = resources_;
    store_config.acquire_process_lock = false;
    store_config.initial_version_floor = version_floor_;
    try {
        return std::make_shared<ChunkStore>(std::move(store_config));
    } catch (const std::exception& e) {
        throw std::runtime_error("table '" + name + "': " + e.what());
    }
}

std::string TableCatalog::OptionFlagMismatches(
    const std::string& name,
    const TableOptions& stored) const {
    const auto& given = config_.default_options;
    const std::uint32_t fields = config_.default_option_fields;
    std::string out;
    const auto check = [&](TableOptionField field, const char* flag, const std::string& flag_value,
                           const std::string& stored_value) {
        if ((fields & field) != 0U && flag_value != stored_value) {
            out += " " + std::string(flag) + " " + flag_value + " (table '" + name + "' stores " +
                   stored_value + ")";
        }
    };
    check(kOptionFieldDurabilityMode, "--durability", DurabilityModeName(given.durability_mode),
          DurabilityModeName(stored.durability_mode));
    check(kOptionFieldCheckpointUpdates, "--checkpoint-updates",
          std::to_string(given.checkpoint_update_interval),
          std::to_string(stored.checkpoint_update_interval));
    check(kOptionFieldCheckpointWalBytes, "--checkpoint-wal-bytes",
          std::to_string(given.checkpoint_wal_bytes), std::to_string(stored.checkpoint_wal_bytes));
    check(kOptionFieldWalGroupCommitUpdates, "--wal-group-commit-updates",
          std::to_string(given.wal_group_commit_updates),
          std::to_string(stored.wal_group_commit_updates));
    check(kOptionFieldCheckpointCompression, "--checkpoint-compression",
          CheckpointCompressionName(given.checkpoint_compression),
          CheckpointCompressionName(stored.checkpoint_compression));
    return out;
}

void TableCatalog::RequireWritable(const char* operation) const {
    migration_health_->Check();
    if (config_.access_mode != AccessMode::kReadWrite) {
        throw std::invalid_argument(
            std::string(operation) + " is not available: the data directory is open read-only");
    }
}

std::shared_ptr<Table> TableCatalog::Find(std::string_view name) const {
    std::shared_lock lock(tables_mutex_);
    const auto it = tables_.find(name);
    return it != tables_.end() ? it->second : nullptr;
}

std::vector<TableInfo> TableCatalog::List() const {
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        tables.reserve(tables_.size());
        for (const auto& [_, table] : tables_) {
            tables.push_back(table);
        }
    }
    std::vector<TableInfo> infos;
    infos.reserve(tables.size());
    for (const auto& table : tables) {
        infos.push_back(table->Info());
    }
    return infos;
}

std::size_t TableCatalog::TableCount() const {
    std::shared_lock lock(tables_mutex_);
    return tables_.size();
}

std::shared_ptr<Table> TableCatalog::Require(std::string_view name) const {
    std::shared_lock lock(tables_mutex_);
    if (const auto it = tables_.find(name); it != tables_.end()) return it->second;
    if (const auto it = fenced_.find(name); it != fenced_.end()) throw TableFencedError(FencedMessage(name, it->second.reason));
    throw TableNotFoundError("table '" + std::string(name) + "' does not exist");
}

void TableCatalog::ThrowIfFenced(std::string_view name) const {
    std::shared_lock lock(tables_mutex_);
    if (const auto it = fenced_.find(name); it != fenced_.end()) throw TableFencedError(FencedMessage(name, it->second.reason));
}

std::vector<TableFence> TableCatalog::Fences() const {
    std::vector<std::pair<std::string, Fenced>> candidates;
    {
        std::shared_lock lock(tables_mutex_);
        for (const auto& [name, table] : tables_) candidates.emplace_back(name, Fenced{table, nullptr});
        for (const auto& [name, fenced] : fenced_) candidates.emplace_back(name, fenced);
    }
    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    std::vector<TableFence> fences;
    for (auto& [name, fenced] : candidates) {
        const char* reason = fenced.reason != nullptr ? fenced.reason : fenced.table->FenceReason();
        if (reason == nullptr) continue;
        fences.push_back(TableFence{
            .table = name,
            .reason = reason,
            .info = fenced.table ? std::optional<TableInfo>(fenced.table->Info()) : std::nullopt,
        });
    }
    return fences;
}

std::optional<TableFence> TableCatalog::Fence(std::string_view name) const {
    Fenced fenced;
    {
        std::shared_lock lock(tables_mutex_);
        if (const auto it = tables_.find(name); it != tables_.end()) fenced.table = it->second;
        else if (const auto it = fenced_.find(name); it != fenced_.end()) fenced = it->second;
        else return std::nullopt;
    }
    const char* reason = fenced.reason != nullptr ? fenced.reason : fenced.table->FenceReason();
    if (reason == nullptr) return std::nullopt;
    return TableFence{
        .table = std::string(name),
        .reason = reason,
        .info = fenced.table ? std::optional<TableInfo>(fenced.table->Info()) : std::nullopt,
    };
}

TableCatalog::FenceNode TableCatalog::PrepareFence(std::shared_ptr<Table> table, const std::string& name, const char* reason) {
    std::map<std::string, Fenced, std::less<>> one;
    one.emplace(name, Fenced{std::move(table), reason});
    return one.extract(one.begin());
}

void TableCatalog::FenceTable(Table& table, const TableOptions& options, FenceNode fence) {
    {
        std::unique_lock lock(tables_mutex_);
        const auto it = tables_.find(table.name_);
        if (it != tables_.end() && it->second.get() == &table) tables_.erase(it);
        // A node insert does not allocate.
        auto inserted = fenced_.insert(std::move(fence));
        if (!inserted.inserted) inserted.position->second = std::move(inserted.node.mapped());
    }
    table.ended_.store(kWatchFenced, std::memory_order_release);
    table.EndExclusive(nullptr, options);
}

void TableCatalog::RecoverTable(std::string_view name, std::stop_token cancelled) {
    RequireWritable("RECOVER TABLE");
    RequireValidTableName(name);
    const std::string table_name(name);
    const auto not_fenced = [&] { return std::invalid_argument("table '" + table_name + "' is not fenced"); };
    // Feature activation takes its table inventory under this gate, so a
    // table does not reopen while a feature is activated.
    if (!feature_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("recovery cancelled");
    std::shared_lock feature_metadata(feature_metadata_gate_, std::adopt_lock);
    // Backup metadata capture and pinning exclude recovery, as they exclude
    // migrations: a backup copies the recovered table or refuses.
    if (!backup_metadata_gate_.lock_shared(cancelled)) throw std::runtime_error("recovery cancelled");
    std::shared_lock metadata(backup_metadata_gate_, std::adopt_lock);
    std::shared_ptr<Table> table;
    {
        std::shared_lock lock(tables_mutex_);
        if (const auto it = tables_.find(name); it != tables_.end()) table = it->second;
        else if (const auto it = fenced_.find(name); it != fenced_.end()) table = it->second.table;
        else throw TableNotFoundError("table '" + table_name + "' does not exist");
    }
    // A table that serves answers at once, without waiting for its statements.
    if (table && table->ServesUnfenced()) throw not_fenced();
    // The admission of table DDL: no command runs on the table from here on.
    // Closing admission, which a table whose feed cleanup failed also gives.
    std::unique_lock<std::mutex> ddl;
    std::shared_ptr<ChunkStore> store;
    TableOptions options;
    if (table) {
        ddl = std::unique_lock(table->ddl_mutex_);
        options = table->Info().options;
        store = table->BeginExclusive(/*closing=*/true);
    }
    ScopeExit serving([&] { table->EndExclusive(std::move(store), options); });
    if (!store) serving.Dismiss();
    std::lock_guard operations(operations_mutex_);
    if (migration_health_->failed.load(std::memory_order_acquire)) {
        // A pending migration decision fences the whole catalog; only a
        // restart completes it.
        if (store) {
            serving.Dismiss();
            store.reset();
            RetireTable(*table, options);
        }
        migration_health_->Check();
    }
    const char* reason = nullptr;
    {
        std::shared_lock lock(tables_mutex_);
        const auto served = tables_.find(name);
        const auto fenced = fenced_.find(name);
        if (fenced != fenced_.end() && fenced->second.table == table) reason = fenced->second.reason;
        // Another recovery finished first, or the name serves a new table.
        else if (served != tables_.end() && served->second != table) throw not_fenced();
        else if (served == tables_.end() || !store) throw TableNotFoundError("table '" + table_name + "' does not exist");
    }
    if (reason == nullptr) {
        // Served, so fenced only while its feed cleanup failed or its store
        // refuses changes: its memory may hold what its files lack.
        reason = table->feed_cleanup_failed_.load(std::memory_order_acquire) ? kFenceFeed : Table::StoreFenceReason(*store);
        if (reason == nullptr) throw not_fenced();
    }
    const auto dir = TablesDir() / table_name;
    // Refuse what opening would refuse while a fail-closed store still
    // serves reads.
    if (IsDirectory(dir)) RequireRecoverableOptions(table_name, ReadOpenableManifest(dir));
    // Made now, so that neither fencing again nor publishing allocates.
    auto fence = PrepareFence(table, table_name, kFenceRecovery);
    auto entry = PrepareServed(table_name);
    serving.Dismiss();
    // Closed as a server shutdown closes it.
    store.reset();
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_RECOVER_TABLE_AFTER_CLOSE_ONCE");
    std::shared_ptr<Table> recovered;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_RECOVER_TABLE_OPEN_FAIL_ONCE"))
            throw std::runtime_error("injected failure recovering a table");
        // What opening the catalog does, for this table only. A pending
        // migration decision cannot be left at this point: it fences the
        // catalog until the restart that completes it.
        RemoveInterruptedOperations(table_name);
        if (IsDirectory(dir)) {
            auto found = ReadTableToOpen(table_name, dir);
            RequireRecoverableOptions(table_name, found.manifest);
            recovered = OpenFoundTable(found);
        }
    } catch (const std::exception& error) {
        if (table) FenceTable(*table, options, std::move(fence));
        else {
            std::unique_lock lock(tables_mutex_);
            if (const auto it = fenced_.find(name); it != fenced_.end()) it->second.reason = kFenceRecovery;
        }
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "table could not be recovered; it stays fenced",
            {{"table", table_name}, {"error", error.what()}});
        throw;
    }
    {
        std::unique_lock lock(tables_mutex_);
        if (const auto it = fenced_.find(name); it != fenced_.end()) fenced_.erase(it);
        if (const auto it = tables_.find(name); it != tables_.end()) {
            if (recovered) it->second = std::move(recovered);
            else tables_.erase(it);
        } else if (recovered) {
            entry.mapped() = std::move(recovered);
            tables_.insert(std::move(entry));
        }
    }
    // Connections holding the old table find the recovered one; its watches
    // end and resume on it.
    if (table) {
        table->ended_.store(kWatchRecovered, std::memory_order_release);
        table->EndExclusive(nullptr, options);
    }
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "table recovered", {{"table", table_name}});
}

TableCatalog::ServedNode TableCatalog::PrepareServed(const std::string& name) {
    std::map<std::string, std::shared_ptr<Table>, std::less<>> one;
    one.emplace(name, nullptr);
    return one.extract(one.begin());
}

StoreManifest TableCatalog::ReadOpenableManifest(const std::filesystem::path& dir) const {
    // Tables are published complete, so a table directory always has its
    // manifest; one without it is damage, never an interrupted create.
    auto manifest = ReadStoreManifest(dir);
    if (!manifest.has_value()) {
        throw std::runtime_error(
            "table directory " + dir.string() + " has no " +
            std::string(kStoreManifestFileName) +
            "; it is damaged or was not created by chunkdb. Move it out of " +
            TablesDir().string() + " to start");
    }
    return std::move(*manifest);
}

void TableCatalog::RequireRecoverableOptions(const std::string& name, const StoreManifest& manifest) const {
    const auto mismatches = OptionFlagMismatches(name, DecodeTableOptions(manifest.options));
    if (mismatches.empty()) return;
    // The flags belong to the server process: only a restart changes them.
    throw std::invalid_argument(
        "server option flags differ from the options the table stores:" + mismatches +
        ". Restart the server without these flags, or with the stored values, to open it");
}

std::shared_ptr<Table> TableCatalog::Create(
    std::string_view name,
    const GeometryConfig& geometry,
    const TableOptions& options,
    const std::optional<TableSchema>& schema,
    bool if_not_exists) {
    return Create(name, [&] { return TableDefinition{geometry, options, schema}; }, if_not_exists);
}

std::shared_ptr<Table> TableCatalog::Create(
    std::string_view name, const std::function<TableDefinition()>& definition,
    bool if_not_exists) {
    RequireWritable("CREATE TABLE");
    std::shared_lock metadata(feature_metadata_gate_);
    RequireValidTableName(name);
    const std::string table_name(name);
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    // A fenced table still has its directory, or may have it again.
    ThrowIfFenced(table_name);
    const auto existing = Find(table_name);
    if (if_not_exists && existing) return existing;
    const auto prepared = definition();
    const auto& geometry = prepared.geometry;
    const auto& options = prepared.options;
    const auto& schema = prepared.schema;
    const TableSchema table_schema = schema.value_or(SingleBitsColumnSchema(geometry.block_bits));
    if (const auto reason = UnsupportedSchemaReason(table_schema); !reason.empty()) {
        throw std::invalid_argument(reason);
    }
    const Geometry table_geometry = prepared.chunk_schema ? Geometry(geometry, table_schema, *prepared.chunk_schema) :
                                                           Geometry(geometry, table_schema);
    if (table_geometry.dims() != 2)
        throw std::invalid_argument("persistent chunk model support is incomplete");
    RequireValidTableOptions(options);
    if (existing != nullptr) {
        throw TableExistsError("table '" + table_name + "' already exists");
    }
    const auto target = TablesDir() / table_name;

    // Build the complete table under a private name, then publish it with
    // one rename: a crash leaves the staging directory (removed at the next
    // start) or the full table, never part of one.
    EnsureDirectoryPathExists(StagingDir(), /*durable_sync=*/true);
    const auto staging = StagingDir() / UniqueOperationName(table_name);
    if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("table staging directory already exists");
    const auto directory_manifest = ReadDataDirManifest(config_.data_dir);
    if (!directory_manifest) throw std::runtime_error("data directory manifest disappeared");
    const StoreManifest manifest{
        .features = TableScopedFeatures(directory_manifest->features),
        .geometry = geometry,
        .store_id = NewStoreId(),
        .options = EncodeTableOptions(options),
        .schema = table_schema,
        .chunk_schema = table_geometry.chunk_schema(),
    };
    try {
        if (!ChunkCoordinateIndex::InitializeEmpty(staging, table_geometry, manifest.store_id))
            throw std::runtime_error("coordinate inventory appeared in table staging");
        if (!PublishNewFile(StoreManifestPath(staging), SerializeStoreManifest(manifest))) {
            throw std::runtime_error("table manifest appeared in " + staging.string());
        }
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_TABLE_CREATE_BEFORE_PUBLISH_ONCE");
        const auto moved = MoveDirectoryNoReplace(staging, target);
        if (moved == std::errc::file_exists) {
            throw TableExistsError(
                "cannot create table '" + table_name + "': " + target.string() +
                " exists but is not a table this server opened");
        }
        if (moved) {
            throw std::runtime_error(
                "cannot publish table " + target.string() + ": " + moved.message());
        }
    } catch (...) {
        std::error_code cleanup_ec;
        std::filesystem::remove_all(staging, cleanup_ec);
        throw;
    }
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_TABLE_CREATE_AFTER_PUBLISH_ONCE");

    // From here the table is in tables/. If it cannot be made durable or
    // opened, take it back out, so the name is not left with a table this
    // server does not serve; the command then fails.
    std::shared_ptr<ChunkStore> store;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TABLE_CREATE_SYNC_FAIL_ONCE")) {
            throw std::runtime_error("injected directory sync failure after publishing a table");
        }
        SyncDirectoryPath(TablesDir());
        SyncDirectoryPath(StagingDir());
        store = OpenStore(table_name, target, geometry, kAllGeometryFields, options);
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "created table cannot be made durable or opened; removing it",
            {{"table", table_name}, {"error", e.what()}});
        try {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TABLE_CREATE_UNDO_FAIL_ONCE"))
                throw std::runtime_error("injected failure removing a created table");
            MoveToDropped(target, table_name);
        } catch (const std::exception& undo_error) {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "created table could not be removed again; it is fenced until RECOVER TABLE or a restart opens it",
                {{"table", table_name}, {"error", undo_error.what()}});
            std::unique_lock lock(tables_mutex_);
            fenced_.insert(PrepareFence(nullptr, table_name, kFenceCreate));
        }
        throw;
    }
    Geometry opened_geometry = store->geometry();
    auto table = std::shared_ptr<Table>(new Table(
        table_name, target, manifest.store_id, std::move(opened_geometry), options, std::move(store), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
    {
        std::unique_lock lock(tables_mutex_);
        tables_.emplace(table_name, table);
    }
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table created",
        {
            {"table", table_name},
            {"store_id", StoreIdHex(manifest.store_id)},
            {"geometry", DescribeGeometry(geometry)},
        });
    return table;
}

void TableCatalog::MoveToDropped(const std::filesystem::path& dir, const std::string& name) {
    EnsureDirectoryPathExists(DroppedDir(), /*durable_sync=*/true);
    const auto dropped = DroppedDir() / UniqueOperationName(name);
    const auto moved = MoveDirectoryNoReplace(dir, dropped);
    if (moved) {
        throw std::runtime_error(
            "cannot move table directory " + dir.string() + " out of " +
            TablesDir().string() + ": " + moved.message());
    }
    // The rename is the commit point of a drop.
    CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_TABLE_DROP_AFTER_RENAME_ONCE");
    SyncDirectoryPath(TablesDir());
    SyncDirectoryPath(DroppedDir());
    RemoveDroppedTree(dropped);
}

void TableCatalog::RemoveDroppedTree(const std::filesystem::path& dropped) noexcept {
    std::error_code ec;
    std::filesystem::remove_all(dropped, ec);
    if (ec) {
        try {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "dropped table directory not fully removed; the next start removes it",
                {{"path", dropped.string()}, {"error", ec.message()}});
        } catch (...) {
        }
    }
}

void TableCatalog::RetireTable(Table& table, const TableOptions& options) {
    {
        std::unique_lock lock(tables_mutex_);
        const auto it = tables_.find(table.name_);
        if (it != tables_.end() && it->second.get() == &table) {
            tables_.erase(it);
        }
    }
    table.EndExclusive(nullptr, options);
}

void TableCatalog::Drop(std::string_view name) {
    Drop(name, nullptr);
}

void TableCatalog::Drop(std::string_view name, bool if_exists) {
    Drop(name, nullptr, if_exists);
}

void TableCatalog::Drop(std::string_view name, UserRegistry* users, bool if_exists) {
    RequireWritable("DROP TABLE");
    std::shared_lock metadata(feature_metadata_gate_);
    auto table = Find(name);
    if (!table && if_exists) {
        RequireValidTableName(name);
        std::lock_guard operations(operations_mutex_);
        migration_health_->Check();
        table = Find(name);
        if (!table) {
            // A fenced table is not absent: its directory may come back.
            ThrowIfFenced(name);
            return;
        }
    }
    if (table == nullptr) table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    const TableOptions options = table->Info().options;
    auto store = table->BeginExclusive();
    if (!store) {
        if (if_exists) {
            std::lock_guard operations(operations_mutex_);
            migration_health_->Check();
            return;
        }
        throw TableNotFoundError("table was dropped");
    }
    ScopeExit admission([&] { table->EndExclusive(std::move(store), options); });
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
        hook->Run(MigrationTestHook::Point::kBeforeCatalogAdmission, table->name_);
    std::lock_guard operations(operations_mutex_);
    if (migration_health_->failed.load(std::memory_order_acquire)) {
        admission.Dismiss();
        store.reset();
        RetireTable(*table, options);
        migration_health_->Check();
    }
    admission.Dismiss();
    // If the drop fails below, the table is reopened as a new store, so its
    // acknowledged batched writes go to the WAL first. A failure here does
    // not stop the drop (a full disk is a reason to drop a table); it only
    // matters if the drop then fails too, so it is logged.
    try {
        store->FlushWalBatchesForReopen();
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kStore,
            "batched writes could not be written before the drop; if the drop fails, the reopened "
            "table lacks them",
            {{"table", table->name_}, {"error", e.what()}});
    }
    {
        // A table created later under this name must not issue this table's
        // tokens again: the data directory keeps a floor above all of them,
        // durably before the drop. Failing here fails the drop.
        ScopeExit restore([&] { table->EndExclusive(std::move(store), options); });
        RaiseVersionFloor(store->version_clock_ceiling_.load(std::memory_order_acquire));
        restore.Dismiss();
    }
    // Whatever goes wrong below, the table must not stay busy: commands on
    // it would wait forever. Unless it is served again or gone, it is fenced.
    auto fence = PrepareFence(table, table->name_, kFenceDropUnknown);
    bool gone = false;
    ScopeExit retire([&] {
        if (gone) RetireTable(*table, options);
        else FenceTable(*table, options, std::move(fence));
    });
    const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
    store->HandOverUnsyncedOnClose(unsynced);
    store.reset();
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TABLE_DROP_RENAME_FAIL_ONCE")) {
            throw std::runtime_error("injected failure before moving a dropped table");
        }
        MoveToDropped(table->dir_, table->name_);
    } catch (const std::exception& e) {
        std::error_code status_ec;
        const bool in_place = std::filesystem::exists(table->dir_, status_ec);
        if (status_ec) {
            throw;  // state unknown: fenced until recovered
        }
        if (in_place) {
            // Still in place: serve it again and report the failure.
            try {
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TABLE_DROP_REOPEN_FAIL_ONCE"))
                    throw std::runtime_error("injected failure reopening a table that was not dropped");
                auto reopened =
                    OpenStore(table->name_, table->dir_, table->geometry().config(), 0U, options);
                reopened->AdoptUnsynced(*unsynced);
                retire.Dismiss();
                table->EndExclusive(std::move(reopened), options);
            } catch (const std::exception& reopen_error) {
                LogMessage(
                    LogLevel::kError,
                    LogComponent::kStore,
                    "table could not be dropped or reopened; it is fenced until RECOVER TABLE or a restart",
                    {{"table", table->name_}, {"error", reopen_error.what()}});
                fence.mapped().reason = kFenceDropReopen;
            }
            throw;
        }
        // Moved out, but a later step (directory sync) failed: the table is
        // gone from this server; the failure is still reported.
        gone = true;
        throw std::runtime_error(
            "table '" + table->name_ + "' was removed but the change may not be durable: " +
            e.what());
    }
    retire.Dismiss();
    RetireTable(*table, options);
    // Keep grant cleanup inside catalog admission: a migration creating or
    // granting the same name must follow the entire DROP operation.
    if (users) users->ForgetTable(table->name_);
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table dropped",
        {{"table", table->name_}, {"store_id", StoreIdHex(table->store_id_)}});
}

void TableCatalog::SetOptions(std::string_view name, const TableOptions& options) {
    SetOptions(name, TableOptionsUpdate::From(options));
}

void TableCatalog::SetOptions(std::string_view name, const TableOptionsUpdate& update) {
    RequireWritable("ALTER TABLE ... SET");
    std::shared_lock metadata(feature_metadata_gate_);
    const auto table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    const TableOptions options = update.ApplyTo(table->Info().options);
    RequireValidTableOptions(options);
    RewriteManifest(
        *table, options, [&](StoreManifest* manifest) { ReplaceTableOptions(manifest, options); },
        "CHUNKDB_FAILPOINT_TABLESET_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table options changed",
        {
            {"table", table->name_},
            {"durability_mode", DurabilityModeName(options.durability_mode)},
            {"checkpoint_updates", std::to_string(options.checkpoint_update_interval)},
            {"checkpoint_wal_bytes", std::to_string(options.checkpoint_wal_bytes)},
            {"wal_group_commit_updates", std::to_string(options.wal_group_commit_updates)},
            {"checkpoint_compression", CheckpointCompressionName(options.checkpoint_compression)},
            {"var_max_chunk_bytes", std::to_string(options.var_max_chunk_bytes)},
        });
}

void TableCatalog::ChangeColumns(
    std::string_view name,
    const std::function<TableSchema(const TableSchema&)>& change) {
    (void)ChangeColumnsIfNeeded(name, change);
}

bool TableCatalog::ChangeColumnsIfNeeded(
    std::string_view name,
    const std::function<std::optional<TableSchema>(const TableSchema&)>& change) {
    RequireWritable("ALTER TABLE");
    std::shared_lock metadata(feature_metadata_gate_);
    const auto table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    std::optional<TableSchema> changed;
    {
        // Check the store before catalog admission: another exclusive control
        // may wait for leases, and must remain free to acquire operations_mutex_.
        auto lease = table->Acquire();
        if (!lease) throw TableNotFoundError("table was dropped");
        lease->store().ThrowIfDurabilityPoisoned();
    }
    {
        std::lock_guard operations(operations_mutex_);
        migration_health_->Check();
        if (Find(name) != table) throw TableNotFoundError("table was dropped");
        changed = change(table->Info().schema);
        if (!changed) return false;
    }
    RewriteManifest(
        *table, table->Info().options,
        [&](StoreManifest* manifest) {
            if (const auto reason = UnsupportedSchemaReason(*changed); !reason.empty()) {
                throw std::invalid_argument(reason);
            }
            manifest->geometry.block_bits = FixedBitsPerBlock(*changed);
            manifest->schema = *changed;
        },
        "CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table columns changed",
        {{"table", table->name_}, {"schema_version", std::to_string(changed->version)}});
    return true;
}

void TableCatalog::NarrowColumn(std::string_view name, std::string_view column, ColumnType type) {
    RequireWritable("ALTER TABLE");
    std::shared_lock metadata(feature_metadata_gate_);
    const auto table = Require(name);
    std::lock_guard ddl(table->ddl_mutex_);
    const TableOptions options = table->Info().options;
    // 1. Every write to the column must fit `type` from here on.
    RewriteManifest(
        *table, options,
        [&](StoreManifest* manifest) { manifest->schema = WithPendingNarrowing(manifest->schema, column, type); },
        "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    const std::uint32_t column_id = table->Info().schema.pending->column_id;
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
        hook->Run(MigrationTestHook::Point::kBeforeNarrowingScan, name);
    // 2. Every value already stored.
    std::optional<std::string> misfit;
    std::exception_ptr scan_failure;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_NARROW_SCAN_FAIL_ONCE")) {
            throw std::runtime_error("injected failure reading the table");
        }
        auto lease = table->Acquire();
        if (!lease.has_value()) {
            throw TableNotFoundError("table '" + std::string(name) + "' is unavailable");
        }
        misfit = lease->store().FindValueNotFitting(column_id, type);
    } catch (...) {
        scan_failure = std::current_exception();
    }
    // 3. The new version, or the schema as it was.
    const bool narrow = scan_failure == nullptr && !misfit.has_value();
    RewriteManifest(
        *table, options,
        [&](StoreManifest* manifest) {
            manifest->schema = narrow ? NarrowColumnType(manifest->schema, column, type)
                                      : WithoutPendingNarrowing(manifest->schema);
            if (narrow) {
                manifest->geometry.block_bits = FixedBitsPerBlock(manifest->schema);
            }
        },
        "CHUNKDB_FAILPOINT_NARROW_END_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    if (scan_failure != nullptr) {
        std::rethrow_exception(scan_failure);
    }
    if (misfit.has_value()) {
        throw std::invalid_argument(
            "column " + std::string(column) + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit + "; " + ColumnTypeRange(type));
    }
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table column narrowed",
        {{"table", table->name_}, {"column", std::string(column)}, {"type", ColumnTypeName(type)}});
}

void TableCatalog::RequireCompatibleColumnAdditions(
    const TableSchema& before, const TableSchema& after, ChunkStore& store) {
    for (const auto& column : after.columns) {
        if (!column.required || column.has_default ||
            std::any_of(before.columns.begin(), before.columns.end(), [&](const auto& old) { return old.id == column.id; })) continue;
        if (store.HasPresentBlocks())
            throw std::invalid_argument("column " + column.name + ": a REQUIRED column added to a table needs a DEFAULT for the blocks it has");
        return;  // All new required columns are safe on this exclusively held empty table.
    }
}

void TableCatalog::RewriteManifest(
    Table& table,
    const TableOptions& options,
    const std::function<void(StoreManifest*)>& change,
    const char* dir_sync_failpoint,
    const std::optional<TableSchema>& chunk_schema) {
    const TableOptions previous = table.Info().options;
    auto store = table.BeginExclusive();
    if (!store) throw TableNotFoundError("table was dropped");
    // Until the manifest is replaced, any failure serves the old store again.
    ScopeExit restore([&] { table.EndExclusive(std::move(store), previous); });
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
        hook->Run(MigrationTestHook::Point::kBeforeCatalogAdmission, table.name_);
    std::lock_guard operations(operations_mutex_);

    if (migration_health_->failed.load(std::memory_order_acquire)) {
        restore.Dismiss();
        store.reset();
        RetireTable(table, previous);
        migration_health_->Check();
    }
    // A fail-closed store keeps state in memory that its files lack (a failed
    // write's WAL bytes, a pending rollback); reopening it now would serve
    // those files. It stays as it is until RecoverTable or a restart runs
    // crash recovery.
    store->ThrowIfDurabilityPoisoned();
    // Acknowledged writes still in group-commit batches reach the WAL now,
    // while a failure can still leave everything as it was.
    store->FlushWalBatchesForReopen();
    bool replaced = false;
    std::exception_ptr write_failure;
    try {
        auto manifest = ReadStoreManifest(table.dir_);
        if (!manifest.has_value()) {
            throw std::runtime_error("table manifest of '" + table.name_ + "' disappeared");
        }
        const auto previous_schema = manifest->schema;
        const auto previous_chunk_schema = *manifest->chunk_schema;
        change(&*manifest);
        if (chunk_schema) manifest->chunk_schema = *chunk_schema;
        AlignManifestSchemaDomains(&*manifest, previous_schema, previous_chunk_schema);
        RequireCompatibleColumnAdditions(previous_schema, manifest->schema, *store);
        for (const auto& column : manifest->chunk_schema->columns) {
            if (!column.required || column.has_default ||
                std::any_of(previous_chunk_schema.columns.begin(), previous_chunk_schema.columns.end(),
                    [&](const auto& old) { return old.id == column.id; })) continue;
            if (store->HasWrittenChunkIdentities())
                throw std::invalid_argument("chunk column " + column.name +
                    ": a REQUIRED column added to a table needs a DEFAULT for its written chunk identities");
            break;
        }
        AtomicWrite(
            StoreManifestPath(table.dir_),
            SerializeStoreManifest(*manifest),
            /*fsync_file=*/true,
            /*fsync_directory=*/true,
            &replaced,
            dir_sync_failpoint,
            /*enable_generic_failpoints=*/false);
    } catch (...) {
        if (!replaced) {
            throw;
        }
        // The manifest holds the change; only its directory sync failed.
        // Serve what the file says, then report the failure.
        write_failure = std::current_exception();
    }
    restore.Dismiss();

    // The old store closes; if the new one cannot open, the table is fenced.
    // What it wrote without a sync goes to the new store, so a later
    // WALFLUSH still syncs it.
    auto fence = PrepareFence(table.shared_from_this(), table.name_, kFenceReopen);
    ScopeExit retire([&] { FenceTable(table, previous, std::move(fence)); });
    const auto unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
    store->HandOverUnsyncedOnClose(unsynced);
    store.reset();
    std::shared_ptr<ChunkStore> reopened;
    try {
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_TABLESET_REOPEN_FAIL_ONCE")) {
            throw std::runtime_error("injected failure reopening a table");
        }
        reopened = OpenStore(table.name_, table.dir_, table.geometry().config(), 0U, options);
        reopened->AdoptUnsynced(*unsynced);
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kStore,
            "table could not be reopened after its manifest changed; it is fenced until RECOVER TABLE or a restart",
            {{"table", table.name_}, {"error", e.what()}});
        throw;
    }
    retire.Dismiss();
    table.EndExclusive(std::move(reopened), options);
    if (write_failure != nullptr) {
        std::rethrow_exception(write_failure);
    }
}

void TableCatalog::WalBarrier() {
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        for (const auto& [_, table] : tables_) {
            tables.push_back(table);
        }
    }
    // One failing table must not leave the others unsynced.
    std::exception_ptr first_failure;
    for (const auto& table : tables) {
        auto lease = table->Acquire();
        if (!lease.has_value()) {
            continue;  // dropped meanwhile; nothing of it is left to sync
        }
        try {
            lease->store().WalBarrier();
        } catch (...) {
            if (first_failure == nullptr) {
                first_failure = std::current_exception();
            }
        }
    }
    if (first_failure != nullptr) {
        std::rethrow_exception(first_failure);
    }
}

TableOptionsUpdate TableOptionsUpdate::From(const TableOptions& options) {
    return TableOptionsUpdate{
        .durability_mode = options.durability_mode,
        .checkpoint_update_interval = options.checkpoint_update_interval,
        .checkpoint_wal_bytes = options.checkpoint_wal_bytes,
        .wal_group_commit_updates = options.wal_group_commit_updates,
        .checkpoint_compression = options.checkpoint_compression,
        .var_max_chunk_bytes = options.var_max_chunk_bytes,
        .feed_buffer_bytes = options.feed_buffer_bytes,
        .slot_max_bytes = options.slot_max_bytes,
    };
}

TableOptions TableOptionsUpdate::ApplyTo(TableOptions options) const {
    options.durability_mode = durability_mode.value_or(options.durability_mode);
    options.checkpoint_update_interval =
        checkpoint_update_interval.value_or(options.checkpoint_update_interval);
    options.checkpoint_wal_bytes = checkpoint_wal_bytes.value_or(options.checkpoint_wal_bytes);
    options.wal_group_commit_updates =
        wal_group_commit_updates.value_or(options.wal_group_commit_updates);
    options.checkpoint_compression =
        checkpoint_compression.value_or(options.checkpoint_compression);
    options.var_max_chunk_bytes = var_max_chunk_bytes.value_or(options.var_max_chunk_bytes);
    if (feed_buffer_bytes) options.feed_buffer_bytes = feed_buffer_bytes;
    if (slot_max_bytes) options.slot_max_bytes = slot_max_bytes;
    return options;
}

bool TableOptionsUpdate::empty() const noexcept {
    return !durability_mode && !checkpoint_update_interval && !checkpoint_wal_bytes &&
           !wal_group_commit_updates && !checkpoint_compression && !var_max_chunk_bytes &&
           !feed_buffer_bytes && !slot_max_bytes;
}

bool TableCatalog::EnableFeature(std::string_view name) {
    RequireWritable("ENABLE FEATURE");
    if (config_.allow_multiple_processes)
        throw std::invalid_argument("ENABLE FEATURE requires a single-process catalog");
    const auto* feature = FindFeature(name);
    if (!feature) throw FeatureAdmissionError("unknown_feature", std::string(name), "unknown feature name");
    std::unique_lock metadata(feature_metadata_gate_);
    migration_health_->Check();
    auto directory = ReadDataDirManifest(config_.data_dir);
    if (!directory) throw std::runtime_error("data directory manifest disappeared");
    if (feature->incompat != kFeatureActivation && (directory->features.incompat & kFeatureActivation) == 0U)
        throw FeatureAdmissionError("feature_required", "feature_activation", "enable feature_activation before activating another feature");
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        // A fenced table is out of the inventory until RecoverTable reopens it.
        if (const auto fenced = fenced_.begin(); fenced != fenced_.end())
            throw TableFencedError(FencedMessage(fenced->first, fenced->second.reason));
        for (const auto& [_, table] : tables_) tables.push_back(table);
    }
    std::vector<FeatureActivationTarget> targets;
    targets.reserve(tables.size());
    bool already_enabled = (directory->features.incompat & feature->incompat) != 0U;
    for (const auto& table : tables) {
        targets.push_back({table->name(), table->store_id()});
        const auto lease = table->Acquire();
        if (!lease) throw TableNotFoundError("table disappeared during feature activation");
        lease->store().ThrowIfDurabilityPoisoned();
        if (feature->table_scoped)
            already_enabled = already_enabled && (lease->store().features().incompat & feature->incompat) != 0U;
    }
    ValidateFeatureInventory(config_.data_dir, directory->data_dir_id, targets);
    // A no-op never pauses an existing feed or maintenance worker.
    if (already_enabled) return false;
    struct Participant { std::shared_ptr<Table> table; TableOptions options; std::shared_ptr<ChunkStore> store; };
    std::vector<Participant> participants;
    participants.reserve(tables.size());
    // Never wait for table admission while holding operations_mutex_: an
    // already-admitted slot/DDL operation may need that mutex to finish.
    ScopeExit resume([&] {
        for (auto& participant : participants) {
            if (participant.store->background_maintenance_ && !migration_health_->failed.load(std::memory_order_acquire)) {
                try { participant.store->StartMaintenanceThread(); }
                catch (const std::exception& error) { participant.store->PoisonDurability("cannot resume maintenance: " + std::string(error.what())); }
            }
            participant.table->EndExclusive(std::move(participant.store), participant.options);
        }
    });
    for (const auto& table : tables) {
        const auto options = table->Info().options;
        auto store = table->BeginExclusive();
        if (!store) throw TableNotFoundError("table disappeared during feature activation");
        participants.push_back({table, options, std::move(store)});
        participants.back().store->ThrowIfDurabilityPoisoned();
        participants.back().store->StopMaintenanceThread();
    }
    std::lock_guard operations(operations_mutex_);
    std::unique_lock eviction(resources_->stores_mutex_);
    migration_health_->Check();
    FeatureActivationJournal journal{directory->data_dir_id, feature->incompat, {}};
    bool enabled = (directory->features.incompat & feature->incompat) != 0U;
    for (const auto& participant : participants) {
        journal.tables.push_back({participant.table->name(), participant.table->store_id()});
        if (feature->table_scoped)
            enabled = enabled && (participant.store->features().incompat & feature->incompat) != 0U;
    }
    ValidateFeatureInventory(config_.data_dir, directory->data_dir_id, journal.tables);
    if (enabled) return false;
    bool published = false;
    try {
        if (feature->incompat == kFeatureActivation) {
            directory->features.incompat |= kFeatureActivation;
            CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_FEATURE_ACTIVATION_BEFORE_PUBLISH_ONCE");
            AtomicWrite(DataDirManifestPath(config_.data_dir), SerializeDataDirManifest(*directory), true, true, &published,
                "CHUNKDB_FAILPOINT_FEATURE_ACTIVATION_SYNC_FAIL_ONCE");
            CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_FEATURE_ACTIVATION_AFTER_PUBLISH_ONCE");
        } else {
            WriteFeatureActivationJournal(config_.data_dir, journal, &published);
            CompleteFeatureActivationJournal(config_.data_dir, journal);
            if (feature->table_scoped)
                for (auto& participant : participants) participant.store->features_.incompat |= feature->incompat;
        }
    } catch (const std::exception& error) {
        if (!published) throw;
        migration_health_->feature_failed.store(true, std::memory_order_release);
        migration_health_->failed.store(true, std::memory_order_release);
        const auto fenced = std::make_exception_ptr(FeatureRecoveryRequiredError(
            "feature activation completion failed; restart the server to recover"));
        for (auto& participant : participants) participant.table->FailFeed(fenced);
        throw WriteOutcomeUnknownError("feature activation may be applied: " + std::string(error.what()),
            WriteOutcomeUnknownError::Stage::kFeatureActivation);
    }
    return true;
}

CatalogFeatureStatus TableCatalog::FeatureStatus() {
    std::unique_lock metadata(feature_metadata_gate_);
    migration_health_->Check();
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        for (const auto& [_, table] : tables_) tables.push_back(table);
    }
    CatalogFeatureStatus status;
    for (const auto& table : tables) {
        const auto lease = table->Acquire();
        if (!lease) throw TableNotFoundError("table disappeared while reading feature status");
        status.tables.emplace_back(table->name(), lease->store().features());
    }
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    const auto directory = ReadDataDirManifest(config_.data_dir);
    if (!directory) throw std::runtime_error("data directory manifest disappeared");
    status.directory = directory->features;
    status.activation_pending = ReadFeatureActivationJournal(config_.data_dir).has_value();
    return status;
}

}  // namespace chunkdb
