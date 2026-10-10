#include "chunkdb/table_catalog.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <regex>
#include <set>
#include <string>
#include <system_error>
#include <utility>

#include "checkpoint.hpp"
#include "change_feed.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/logging.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feed_slots.hpp"
#include "slot_watch.hpp"
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
    return name == kTablesDirName || name == kMigrationsFileName || name == kMigrationPendingFileName || IsStoreEntryName(name);
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
}

CatalogConfig CatalogConfigFromStoreConfig(
    const StoreConfig& config,
    std::uint32_t option_fields) {
    CatalogConfig catalog;
    catalog.data_dir = config.data_dir;
    catalog.access_mode = config.access_mode;
    catalog.allow_multiple_processes = config.allow_multiple_processes;
    catalog.default_geometry = config.geometry;
    catalog.default_geometry_fields = config.geometry_fields;
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
    std::shared_ptr<MigrationHealth> migration_health)
    : name_(std::move(name)),
      dir_(std::move(dir)),
      store_id_(store_id),
      feed_buffer_bytes_(feed_buffer_bytes),
      migration_health_(std::move(migration_health)),
      geometry_(std::move(geometry)),
      store_(std::move(store)),
      options_(options) {
    slot_ack_state_ = std::make_shared<FeedSlotAckState>();
    store_->feed_slots_->UseAckState(slot_ack_state_);
    if (store_->feed_slots_->active() && store_->access_mode_ == AccessMode::kReadWrite) {
        feed_ = std::make_shared<ChangeFeed>(store_id_, feed_buffer_bytes_);
        feed_->Resume(*store_);
        store_->feed_slots_->Start(feed_);
    }
    else if (store_->feed_slots_->ArchiveRequired()) store_->feed_slots_->Start(nullptr);
}

Table::~Table() {
    if (store_) store_->feed_slots_->Stop();
    if (feed_) {
        if (store_) store_->feed_.store(nullptr, std::memory_order_seq_cst);
        feed_->End();
    }
}

std::unique_ptr<FeedSubscription> Table::SubscribeFeed(const FeedOptions& options) {
    if (options.area && (options.area->first.x > options.area->last.x || options.area->first.y > options.area->last.y))
        throw std::invalid_argument("feed area bounds are reversed");
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ == AccessMode::kReadOnly || store->allow_multiple_processes_)
        throw std::invalid_argument("feed requires a single-process read-write table");
    if (feed_ && options.buffer_bytes && *options.buffer_bytes != feed_->budget()) {
        if (feed_subscriptions_ != 0U || !store->feed_slots_->active())
            throw std::invalid_argument("feed buffer bytes differ from the active feed");
        // Slot-only history is recoverable from its WAL archives. Establish
        // the server's configured ring budget before the first socket reader.
        feed_->End();
        feed_.reset();
    }
    if (!feed_) feed_ = std::make_shared<ChangeFeed>(store_id_, options.buffer_bytes.value_or(feed_buffer_bytes_));
    ScopeExit unused_feed([&] {
        if (feed_subscriptions_ == 0U && !store->feed_slots_->active()) {
            store->feed_.store(nullptr, std::memory_order_seq_cst);
            feed_->End();
            feed_.reset();
        }
    });
    feed_->Resume(*store);
    auto subscription = feed_->Subscribe(weak_from_this(), options);
    ++feed_subscriptions_;
    store->feed_watchers_active_.store(true, std::memory_order_release);
    unused_feed.Dismiss();
    return subscription;
}

void Table::ReleaseFeed(const std::shared_ptr<ChangeFeed>& feed) {
    auto store = BeginExclusive();
    if (!store) return;
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (feed_ != feed) return;
    if (--feed_subscriptions_ == 0U) {
        store->feed_watchers_active_.store(false, std::memory_order_release);
        if (!store->feed_slots_->active()) {
            feed_->End();
            feed_.reset();
        }
    }
}

void Table::StopFeed() {
    auto store = BeginExclusive();
    if (!store) return;
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (feed_) feed_->End();
    feed_.reset();
    feed_subscriptions_ = 0U;
    store->feed_watchers_active_.store(false, std::memory_order_release);
    if (store->feed_slots_->active())
        feed_ = std::make_shared<ChangeFeed>(store_id_, feed_buffer_bytes_);
}

FeedSlot Table::CreateFeedSlot(std::string_view name) {
    RequireValidFeedSlotName(name);
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ != AccessMode::kReadWrite || store->allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process read-write table");
    store->ThrowIfDurabilityPoisoned();
    const auto slots = store->feed_slots_->List();
    if (std::any_of(slots.begin(), slots.end(), [&](const auto& slot) { return slot.name == name; }))
        throw std::invalid_argument("feed slot already exists: " + std::string(name));
    // Allocate before publishing a slot. Maintenance and cross-table eviction
    // do not take Table leases, so quiesce both before changing cached flags.
    if (!feed_) feed_ = std::make_shared<ChangeFeed>(store_id_, feed_buffer_bytes_);
    store->StopMaintenanceThread();
    ScopeExit maintenance([&] {
        if (!store->background_maintenance_) return;
        try { store->StartMaintenanceThread(); }
        catch (const std::exception& error) { store->PoisonDurability("cannot resume maintenance: " + std::string(error.what())); }
    });
    std::unique_lock eviction_lock(store->resources_->stores_mutex_);
    PrepareFeedSlotBaseline(*store);
    if ((store->features_.incompat & kFeatureFeedSlots) == 0U) {
        auto manifest = ReadStoreManifest(dir_);
        if (!manifest) throw std::runtime_error("table manifest disappeared");
        manifest->features.incompat |= kFeatureFeedSlots;
        bool published = false;
        try { AtomicWrite(StoreManifestPath(dir_), SerializeStoreManifest(*manifest), true, true, &published); }
        catch (const std::exception& error) {
            if (published) {
                store->features_ = manifest->features;
                store->PoisonDurability("feed slots feature publication failed: " + std::string(error.what()));
            }
            throw;
        }
        store->features_ = manifest->features;
    }
    return store->feed_slots_->Create(name, store->version_clock_.load(std::memory_order_seq_cst) - 1U);
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
                auto loaded = store.LoadChunkPayload(coord);
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

void Table::DropFeedSlot(std::string_view name) {
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
    if (!store->feed_slots_->active() && feed_subscriptions_ == 0U && feed_) {
        feed_->End();
        feed_.reset();
    }
}

std::vector<FeedSlot> Table::ListFeedSlots(bool include_lost) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    return lease->store().feed_slots_->List(include_lost);
}

std::pair<FeedSlot, std::shared_ptr<FeedSlotClaim>> Table::ClaimFeedSlot(std::string_view name) {
    auto store = BeginExclusive();
    if (!store) throw TableNotFoundError("table '" + name_ + "' was dropped");
    ScopeExit serving([&] { EndExclusive(std::move(store), options_); });
    if (store->access_mode_ != AccessMode::kReadWrite || store->allow_multiple_processes_)
        throw std::invalid_argument("feed slots require a single-process read-write table");
    auto slot = store->feed_slots_->Get(name);
    if (slot.lost) throw FeedSlotLostError("feed slot exceeded its retention limit");
    auto& existing = slot_claims_[std::string(name)];
    if (auto claim = existing.lock(); claim && claim->valid.load(std::memory_order_acquire))
        throw FeedSlotBusyError("feed slot already has a watch");
    auto claim = std::make_shared<FeedSlotClaim>(std::string(name), store->version_clock_.load(std::memory_order_seq_cst) - 1U, slot_output_bytes_);
    existing = claim;
    return {std::move(slot), std::move(claim)};
}

FeedSlot Table::ReadClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    const auto slot = lease->store().feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError("feed slot exceeded its retention limit");
    return slot;
}

void Table::SyncClaimedFeedSlot(const std::shared_ptr<FeedSlotClaim>& claim) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    auto& store = lease->store();
    const auto slot = store.feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError("feed slot exceeded its retention limit");
    store.feed_slots_->Sync(feed_->CompletedWatermark());
}

FeedArchiveReader Table::ReadClaimedFeedArchive(const std::shared_ptr<FeedSlotClaim>& claim, FeedPosition after) {
    auto lease = Acquire();
    if (!lease) throw TableNotFoundError("table '" + name_ + "' was dropped");
    if (!claim->valid.load(std::memory_order_acquire)) throw FeedSlotLostError("feed slot was removed");
    const auto slot = lease->store().feed_slots_->Get(claim->name);
    if (slot.lost) throw FeedSlotLostError("feed slot exceeded its retention limit");
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
    if (slot.lost) throw FeedSlotLostError("feed slot exceeded its retention limit");
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
    store->FlushSnapshotGenerationLingerForTests();
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
    };
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
        if (state == State::kGone) {
            return std::nullopt;
        }
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this]() {
            return state_.load(std::memory_order_seq_cst) != State::kBusy;
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

std::shared_ptr<ChunkStore> Table::BeginExclusive(bool closing) {
    if (!closing) {
        migration_health_->Check();
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire))
            hook->Run(MigrationTestHook::Point::kBeforeTableExclusive, name_);
    }
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [this] { return state_.load(std::memory_order_seq_cst) != State::kBusy; });
    if (!closing) migration_health_->Check();
    if (state_.load(std::memory_order_seq_cst) == State::kGone) return nullptr;
    state_.store(State::kBusy, std::memory_order_seq_cst);
    cv_.wait(lock, [this]() {
        return active_leases_.load(std::memory_order_seq_cst) == 0;
    });
    store_->feed_slots_->Stop();
    if (feed_) {
        store_->feed_.store(nullptr, std::memory_order_seq_cst);
        if (!store_->feed_slots_->active() && feed_subscriptions_ == 0U) {
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

void Table::EndExclusive(std::shared_ptr<ChunkStore> store, const TableOptions& options) {
    if (store && !store->feed_slots_->active() && feed_subscriptions_ == 0U && feed_) {
        feed_->End();
        feed_.reset();
    }
    if (store && store->feed_slots_->active() && store->access_mode_ == AccessMode::kReadWrite && !feed_)
        feed_ = std::make_shared<ChangeFeed>(store_id_, feed_buffer_bytes_);
    if (feed_) {
        if (store == nullptr) feed_->End();
        else if (!feed_->attached()) feed_->Resume(*store);
    }
    if (store) {
        store->feed_slots_->UseAckState(slot_ack_state_);
        store->feed_watchers_active_.store(feed_subscriptions_ != 0U, std::memory_order_release);
        store->feed_slots_->Start(feed_);
    }
    {
        std::lock_guard lock(mutex_);
        const State next = store != nullptr ? State::kOpen : State::kGone;
        if (store != nullptr) {
            // A reopened store may have other columns.
            geometry_ = store->geometry();
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
    if (config_.feed_buffer_bytes == 0U) throw std::invalid_argument("feed_buffer_bytes must be positive");
    RequireValidTableOptions(config_.default_options);
    resources_ = std::make_shared<StoreResources>(
        config_.max_loaded_chunks, config_.max_open_wal_streams);

    const bool writable = config_.access_mode == AccessMode::kReadWrite;
    if (writable) {
        std::filesystem::create_directories(config_.data_dir);
    } else if (!IsDirectory(config_.data_dir)) {
        throw std::runtime_error(
            "read-only data directory is unavailable: " + config_.data_dir.string());
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
    RecoverMigrations(config_.data_dir, config_.access_mode);
    version_floor_ = DataDirVersionFloor(*ReadDataDirManifest(config_.data_dir));
    if (writable) {
        EnsureDirectoryPathExists(TablesDir(), /*durable_sync=*/true);
        RemoveInterruptedOperations();
    }
    OpenExistingTables();

    if (tables_.empty()) {
        if (writable) {
            (void)Create(kDefaultTableName, config_.default_geometry, config_.default_options);
        } else {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "data directory has no tables",
                {{"data_dir", config_.data_dir.string()}});
        }
    } else if (!tables_.contains(kDefaultTableName) && config_.default_geometry_fields != 0U) {
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kStore,
            "geometry flags describe the default table, which does not exist; ignored",
            {{"data_dir", config_.data_dir.string()}});
    }
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
            .features = FeatureFlags{},
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

void TableCatalog::RemoveInterruptedOperations() {
    for (const auto& dir : {StagingDir(), DroppedDir()}) {
        if (!IsDirectory(dir)) {
            continue;
        }
        std::error_code ec;
        std::filesystem::directory_iterator it(dir, ec);
        const std::filesystem::directory_iterator end;
        std::vector<std::filesystem::path> leftovers;
        for (; !ec && it != end; it.increment(ec)) {
            leftovers.push_back(it->path());
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
}

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
    struct Found {
        std::string name;
        std::filesystem::path dir;
        StoreManifest manifest;
        TableOptions options;
    };
    std::vector<Found> tables;
    std::string mismatches;
    for (const auto& [name, dir] : found) {
        // Tables are published complete, so a table directory always has its
        // manifest; one without it is damage, never an interrupted create.
        auto manifest = ReadStoreManifest(dir);
        if (!manifest.has_value()) {
            throw std::runtime_error(
                "table directory " + dir.string() + " has no " +
                std::string(kStoreManifestFileName) +
                "; it is damaged or was not created by chunkdb. Move it out of " +
                tables_dir.string() + " to start");
        }
        const TableOptions options = DecodeTableOptions(manifest->options);
        mismatches += OptionFlagMismatches(name, options);
        if (manifest->schema.pending.has_value() && config_.access_mode == AccessMode::kReadWrite) {
            // A narrowing a crash interrupted: the schema stays as it was.
            manifest->schema = WithoutPendingNarrowing(manifest->schema);
            AtomicWrite(
                StoreManifestPath(dir), SerializeStoreManifest(*manifest), /*fsync_file=*/true,
                /*fsync_directory=*/true, nullptr, nullptr, /*enable_generic_failpoints=*/false);
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kStore,
                "dropped a column narrowing that did not finish; the table keeps its columns",
                {{"table", name}});
        }
        tables.push_back(Found{
            .name = name,
            .dir = dir,
            .manifest = std::move(*manifest),
            .options = options,
        });
    }
    if (!mismatches.empty()) {
        throw std::runtime_error(
            "server option flags differ from the options tables store:" + mismatches +
            ". A table keeps the options it was created with: omit the flag, pass the "
            "stored value, or change the table with ALTER TABLE ... SET");
    }

    for (auto& table : tables) {
        const bool is_default = table.name == kDefaultTableName;
        auto store = OpenStore(
            table.name,
            table.dir,
            is_default ? config_.default_geometry : table.manifest.geometry,
            is_default ? config_.default_geometry_fields : 0U,
            table.options);
        // Read from the store before it is moved: argument evaluation order
        // is unspecified.
        const StoreId store_id = store->store_id();
        Geometry geometry = store->geometry();
        tables_.emplace(
            table.name,
            std::shared_ptr<Table>(new Table(
                table.name, table.dir, store_id, std::move(geometry), table.options, std::move(store), config_.feed_buffer_bytes, migration_health_)));
    }
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
    store_config.slot_max_bytes = config_.slot_max_bytes;
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

std::shared_ptr<Table> TableCatalog::Create(
    std::string_view name,
    const GeometryConfig& geometry,
    const TableOptions& options,
    const std::optional<TableSchema>& schema) {
    RequireWritable("CREATE TABLE");
    RequireValidTableName(name);
    const TableSchema table_schema = schema.value_or(SingleBitsColumnSchema(geometry.block_bits));
    if (const auto reason = UnsupportedSchemaReason(table_schema); !reason.empty()) {
        throw std::invalid_argument(reason);
    }
    const Geometry table_geometry(geometry, table_schema);
    RequireValidTableOptions(options);
    const std::string table_name(name);

    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    if (Find(table_name) != nullptr) {
        throw TableExistsError("table '" + table_name + "' already exists");
    }
    const auto target = TablesDir() / table_name;

    // Build the complete table under a private name, then publish it with
    // one rename: a crash leaves the staging directory (removed at the next
    // start) or the full table, never part of one.
    EnsureDirectoryPathExists(StagingDir(), /*durable_sync=*/true);
    const auto staging = StagingDir() / UniqueOperationName(table_name);
    std::filesystem::create_directory(staging);
    const StoreManifest manifest{
        .features = {},
        .geometry = geometry,
        .store_id = NewStoreId(),
        .options = EncodeTableOptions(options),
        .schema = table_schema,
    };
    try {
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
            MoveToDropped(target, table_name);
        } catch (const std::exception& undo_error) {
            LogMessage(
                LogLevel::kError,
                LogComponent::kStore,
                "created table could not be removed again; the next start opens it",
                {{"table", table_name}, {"error", undo_error.what()}});
        }
        throw;
    }
    Geometry opened_geometry = store->geometry();
    auto table = std::shared_ptr<Table>(new Table(
        table_name, target, manifest.store_id, std::move(opened_geometry), options, std::move(store), config_.feed_buffer_bytes, migration_health_));
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

void TableCatalog::Drop(std::string_view name, UserRegistry* users) {
    RequireWritable("DROP TABLE");
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    const auto table = Find(name);
    if (table == nullptr) {
        throw TableNotFoundError("table '" + std::string(name) + "' does not exist");
    }
    const TableOptions options = table->Info().options;
    auto store = table->BeginExclusive();
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
    // it would wait forever. Unless it is served again, it is retired.
    ScopeExit retire([&] { RetireTable(*table, options); });
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
            throw;  // state unknown: retired until restart
        }
        if (in_place) {
            // Still in place: serve it again and report the failure.
            try {
                auto reopened =
                    OpenStore(table->name_, table->dir_, table->geometry().config(), 0U, options);
                reopened->AdoptUnsynced(*unsynced);
                retire.Dismiss();
                table->EndExclusive(std::move(reopened), options);
            } catch (const std::exception& reopen_error) {
                LogMessage(
                    LogLevel::kError,
                    LogComponent::kStore,
                    "table could not be dropped or reopened; it is unavailable until restart",
                    {{"table", table->name_}, {"error", reopen_error.what()}});
            }
            throw;
        }
        // Moved out, but a later step (directory sync) failed: the table is
        // gone from this server; the failure is still reported.
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
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    const auto table = Find(name);
    if (table == nullptr) {
        throw TableNotFoundError("table '" + std::string(name) + "' does not exist");
    }
    const TableOptions options = update.ApplyTo(table->Info().options);
    RequireValidTableOptions(options);
    RewriteManifest(
        *table, options, [&](StoreManifest* manifest) { manifest->options = EncodeTableOptions(options); },
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
    RequireWritable("ALTER TABLE");
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    const auto table = Find(name);
    if (table == nullptr) {
        throw TableNotFoundError("table '" + std::string(name) + "' does not exist");
    }
    TableSchema changed;
    RewriteManifest(
        *table, table->Info().options,
        [&](StoreManifest* manifest) {
            changed = change(manifest->schema);
            if (const auto reason = UnsupportedSchemaReason(changed); !reason.empty()) {
                throw std::invalid_argument(reason);
            }
            manifest->geometry.block_bits = FixedBitsPerBlock(changed);
            (void)Geometry(manifest->geometry, changed);
            manifest->schema = changed;
        },
        "CHUNKDB_FAILPOINT_ALTER_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table columns changed",
        {{"table", table->name_}, {"schema_version", std::to_string(changed.version)}});
}

void TableCatalog::NarrowColumn(std::string_view name, std::string_view column, ColumnType type) {
    RequireWritable("ALTER TABLE");
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    const auto table = Find(name);
    if (table == nullptr) {
        throw TableNotFoundError("table '" + std::string(name) + "' does not exist");
    }
    const TableOptions options = table->Info().options;
    // 1. Every write to the column must fit `type` from here on.
    RewriteManifest(
        *table, options,
        [&](StoreManifest* manifest) { manifest->schema = WithPendingNarrowing(manifest->schema, column, type); },
        "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
    const std::uint32_t column_id = table->Info().schema.pending->column_id;
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
            "column " + std::string(column) + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit);
    }
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "table column narrowed",
        {{"table", table->name_}, {"column", std::string(column)}, {"type", ColumnTypeName(type)}});
}

void TableCatalog::RewriteManifest(
    Table& table,
    const TableOptions& options,
    const std::function<void(StoreManifest*)>& change,
    const char* dir_sync_failpoint) {
    const TableOptions previous = table.Info().options;
    auto store = table.BeginExclusive();

    // Until the manifest is replaced, any failure serves the old store again.
    ScopeExit restore([&] { table.EndExclusive(std::move(store), previous); });
    // A fail-closed store keeps state in memory that its files lack (a failed
    // write's WAL bytes, a pending rollback); reopening it now would serve
    // those files. It stays as it is until the server restarts.
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
        change(&*manifest);
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

    // The old store closes; if the new one cannot open, the table is retired.
    // What it wrote without a sync goes to the new store, so a later
    // WALFLUSH still syncs it.
    ScopeExit retire([&] { RetireTable(table, previous); });
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
            "table could not be reopened after its manifest changed; it is unavailable until restart",
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
    return options;
}

bool TableOptionsUpdate::empty() const noexcept {
    return !durability_mode && !checkpoint_update_interval && !checkpoint_wal_bytes &&
           !wal_group_commit_updates && !checkpoint_compression && !var_max_chunk_bytes;
}

}  // namespace chunkdb
