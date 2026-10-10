#include "migrations.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <mutex>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feed_slots.hpp"
#include "slot_watch.hpp"
#include "user_registry.hpp"

namespace chunkdb {
namespace {

std::optional<std::vector<std::uint8_t>> FileImage(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) return std::nullopt;
    return LoadFile(path);
}

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) std::_Exit(86);
}

std::string OperationName(const std::string& table) {
    return table + "." + StoreIdHex(NewStoreId()).substr(0, 16);
}

}  // namespace

std::vector<MigrationRecord> TableCatalog::Migrations() const {
    std::lock_guard lock(operations_mutex_);
    migration_health_->Check();
    return ReadMigrationRecords(config_.data_dir);
}

bool TableCatalog::Migrate(const MigrationRequest& request, UserRegistry* users) {
    RequireWritable("MIGRATE");
    if (config_.allow_multiple_processes) throw std::invalid_argument("MIGRATE requires a single-process catalog");
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kBeforeAdmission, request.record.name);
    std::lock_guard operations(operations_mutex_);
    migration_health_->Check();
    auto records = ReadMigrationRecords(config_.data_dir);
    for (const auto& record : records) {
        if (record.name != request.record.name) continue;
        if (record.statement != request.record.statement)
            throw MigrationConflictError("migration '" + record.name + "' has a different statement");
        return false;
    }
    auto root = ReadDataDirManifest(config_.data_dir);
    if (!root) throw std::runtime_error("data directory manifest disappeared");
    // Enable incompatibility before an intent can exist. A failed attempt may
    // leave the feature enabled, which is safe and contains no migration.
    if ((root->features.incompat & kFeatureMigrations) == 0U) {
        root->features.incompat |= kFeatureMigrations;
        AtomicWrite(DataDirManifestPath(config_.data_dir), SerializeDataDirManifest(*root), true, true);
    }
    MigrationJournal journal;
    journal.data_dir_id = root->data_dir_id;
    journal.record = request.record;
    journal.record.applied_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    journal.table = request.kind == MigrationRequest::Kind::kGrant ? std::string{} : request.table;
    const auto add_file = [&](const std::filesystem::path& relative, std::vector<std::uint8_t> after) {
        journal.files.push_back({relative.generic_string(), FileImage(config_.data_dir / relative), std::move(after)});
    };
    std::unique_lock<std::mutex> user_lock;
    std::optional<Users> next_users;
    if (users && (request.kind == MigrationRequest::Kind::kGrant || request.kind == MigrationRequest::Kind::kDrop)) {
        if (!std::filesystem::equivalent(users->data_dir_, config_.data_dir))
            throw std::invalid_argument("migration users belong to another data directory");
        user_lock = std::unique_lock(users->mutex_);
        if (users->migration_health_) users->migration_health_->Check();
        users->migration_health_ = migration_health_;
        const auto disk_users = ReadUsersFile(config_.data_dir);
        if (!disk_users) throw std::runtime_error("migration users file disappeared");
        if (users->users_ != *disk_users) {
            users->users_ = *disk_users;
            users->generation_.fetch_add(1, std::memory_order_acq_rel);
        }
        next_users = users->users_;
    }
    if (!users && request.kind == MigrationRequest::Kind::kDrop) next_users = ReadUsersFile(config_.data_dir);
    std::shared_ptr<Table> table;
    std::shared_ptr<ChunkStore> store;
    TableOptions previous;
    TableOptions next_options;
    std::filesystem::path staging;
    bool staging_owned = false;
    bool decided = false;
    bool serving_finished = false;
    try {
        if (request.kind == MigrationRequest::Kind::kCreate) {
            RequireValidTableName(request.table);
            RequireValidTableOptions(request.options);
            if (const auto reason = UnsupportedSchemaReason(request.schema); !reason.empty()) throw std::invalid_argument(reason);
            (void)Geometry(request.geometry, request.schema);
            if (Find(request.table)) throw TableExistsError("table '" + request.table + "' already exists");
            journal.directory_action = MigrationDirectoryAction::kCreate;
            journal.operation_name = OperationName(request.table);
            journal.table_id = NewStoreId();
            staging = StagingDir() / journal.operation_name;
            EnsureDirectoryPathExists(StagingDir(), true);
            if (!std::filesystem::create_directory(staging)) throw std::runtime_error("migration stage already exists");
            staging_owned = true;
            StoreManifest manifest{.features = {}, .geometry = request.geometry, .store_id = journal.table_id,
                                   .options = EncodeTableOptions(request.options), .schema = request.schema};
            const auto bytes = SerializeStoreManifest(manifest);
            if (!PublishNewFile(StoreManifestPath(staging), bytes)) throw std::runtime_error("migration stage manifest already exists");
            SyncDirectoryPath(StagingDir());
            journal.files.push_back({"tables/" + request.table + "/" + std::string(kStoreManifestFileName), std::nullopt, bytes});
            next_options = request.options;
        } else if (request.kind != MigrationRequest::Kind::kGrant) {
            table = Find(request.table);
            if (!table) throw TableNotFoundError("table '" + request.table + "' does not exist");
            previous = table->Info().options;
            next_options = previous;
            store = table->BeginExclusive();
            if (!store) throw TableNotFoundError("table '" + request.table + "' was dropped");
            store->ThrowIfDurabilityPoisoned();
            store->StopMaintenanceThread();
            store->FlushWalBatchesForReopen();
            journal.table_id = table->store_id_;
            auto manifest = ReadStoreManifest(table->dir_);
            if (!manifest || manifest->store_id != journal.table_id) throw std::runtime_error("migration table identity changed");
            if (request.kind == MigrationRequest::Kind::kDrop) {
                journal.directory_action = MigrationDirectoryAction::kDrop;
                journal.operation_name = OperationName(request.table);
                EnsureDirectoryPathExists(DroppedDir(), true);
                auto floor = *root;
                SetDataDirVersionFloor(&floor, std::max(version_floor_, store->version_clock_ceiling_.load(std::memory_order_acquire)));
                add_file(std::string(kDataDirManifestFileName), SerializeDataDirManifest(floor));
                if (next_users) for (auto& [_, user] : next_users->users) user.grants.erase(request.table);
            } else if (request.kind == MigrationRequest::Kind::kAlter) {
                if (request.narrowing) {
                    const auto& [column_name, type] = *request.narrowing;
                    const auto at = std::find_if(manifest->schema.columns.begin(), manifest->schema.columns.end(),
                        [&](const auto& column) { return column.name == column_name; });
                    if (at == manifest->schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                    if (HoldsEveryValue(at->type, type)) manifest->schema = ChangeColumnType(manifest->schema, column_name, type, Conversion::kExact);
                    else {
                        const auto pending = WithPendingNarrowing(manifest->schema, column_name, type);
                        if (const auto misfit = store->FindValueNotFitting(at->id, type))
                            throw std::invalid_argument("column " + column_name + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit);
                        manifest->schema = NarrowColumnType(pending, column_name, type);
                    }
                } else request.alter(*manifest);
                if (const auto reason = UnsupportedSchemaReason(manifest->schema); !reason.empty()) throw std::invalid_argument(reason);
                manifest->geometry.block_bits = FixedBitsPerBlock(manifest->schema);
                (void)Geometry(manifest->geometry, manifest->schema);
                next_options = DecodeTableOptions(manifest->options);
                RequireValidTableOptions(next_options);
                add_file("tables/" + request.table + "/" + std::string(kStoreManifestFileName), SerializeStoreManifest(*manifest));
            } else {
                RequireValidFeedSlotName(request.slot);
                if (store->allow_multiple_processes_) throw std::invalid_argument("feed slots require a single-process table");
                std::unique_lock eviction(store->resources_->stores_mutex_);
                auto slots = ReadFeedSlotRecords(table->dir_, journal.table_id).value_or(FeedSlotRecords{journal.table_id, 0U, {}});
                const auto at = std::find_if(slots.slots.begin(), slots.slots.end(), [&](const auto& slot) { return slot.name == request.slot; });
                if (request.kind == MigrationRequest::Kind::kCreateSlot) {
                    if (at != slots.slots.end()) throw std::invalid_argument("feed slot already exists: " + request.slot);
                    table->PrepareFeedSlotBaseline(*store);
                    const auto completed = store->version_clock_.load(std::memory_order_seq_cst) - 1U;
                    store->feed_slots_->Sync(completed);
                    slots = ReadFeedSlotRecords(table->dir_, journal.table_id).value_or(FeedSlotRecords{journal.table_id, completed, {}});
                    slots.slots.push_back({request.slot, completed, false});
                    manifest->features.incompat |= kFeatureFeedSlots;
                    add_file("tables/" + request.table + "/" + std::string(kStoreManifestFileName), SerializeStoreManifest(*manifest));
                } else {
                    if (at == slots.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + request.slot);
                    slots.slots.erase(at);
                }
                add_file("tables/" + request.table + "/" + std::string(kFeedSlotsFileName), SerializeFeedSlotRecords(slots));
            }
        } else {
            if (!next_users) throw std::invalid_argument("this server runs without users (--auth none)");
            const auto at = next_users->users.find(request.user);
            if (at == next_users->users.end()) throw std::invalid_argument("user " + request.user + " does not exist");
            auto& grants = at->second.grants;
            const auto current = grants.find(request.table);
            if (!request.revoke) {
                if (current == grants.end() || current->second < request.right) grants[request.table] = request.right;
            } else if (current != grants.end() && current->second >= request.right) {
                if (request.right == Right::kRead) grants.erase(current);
                else current->second = static_cast<Right>(static_cast<std::uint8_t>(request.right) - 1U);
            }
        }
        if (next_users) add_file(std::string(kUsersFileName), EncodeUsers(*next_users));
        records.push_back(journal.record);
        add_file(std::string(kMigrationsFileName), EncodeMigrationRecords(root->data_dir_id, records));
        ValidateMigrationJournal(config_.data_dir, journal);
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kPrepared, request.record.name);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_DECISION_ONCE");
        WriteMigrationJournal(config_.data_dir, journal, &decided);
        decided = true;
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kAfterDecision, request.record.name);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_MIGRATION_AFTER_DECISION_ONCE");
        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE"))
            throw std::runtime_error("injected migration completion failure");
        // Closing the store before participant replacement prevents a final
        // destructor WAL flush from writing under the new schema or slot state.
        std::shared_ptr<ChunkStore::UnsyncedArtifacts> unsynced;
        if (store) {
            unsynced = std::make_shared<ChunkStore::UnsyncedArtifacts>();
            store->HandOverUnsyncedOnClose(unsynced);
            store.reset();
        }
        CompleteMigrationJournal(config_.data_dir, journal);
        version_floor_ = DataDirVersionFloor(*ReadDataDirManifest(config_.data_dir));
        if (next_users && users) {
            users->users_ = std::move(*next_users);
            users->generation_.fetch_add(1, std::memory_order_acq_rel);
        }
        if (request.kind == MigrationRequest::Kind::kCreate) {
            auto opened = OpenStore(request.table, TablesDir() / request.table, request.geometry, kAllGeometryFields, request.options);
            auto geometry = opened->geometry();
            auto created = std::shared_ptr<Table>(new Table(request.table, TablesDir() / request.table, journal.table_id,
                std::move(geometry), request.options, std::move(opened), config_.feed_buffer_bytes, migration_health_));
            std::unique_lock lock(tables_mutex_);
            tables_.emplace(request.table, std::move(created));
        } else if (table) {
            if (request.kind == MigrationRequest::Kind::kDrop) {
                RetireTable(*table, previous);
                RemoveDroppedTree(DroppedDir() / journal.operation_name);
            }
            else {
                auto reopened = OpenStore(request.table, table->dir_, table->geometry().config(), 0U, next_options);
                if (unsynced) reopened->AdoptUnsynced(*unsynced);
                if (request.kind == MigrationRequest::Kind::kDropSlot) {
                    const auto at = table->slot_claims_.find(request.slot);
                    if (at != table->slot_claims_.end()) {
                        if (const auto claim = at->second.lock()) claim->valid.store(false, std::memory_order_release);
                        table->slot_claims_.erase(at);
                    }
                    table->slot_ack_state_->pending.erase(request.slot);
                }
                table->EndExclusive(std::move(reopened), next_options);
            }
            serving_finished = true;
        }
        return true;
    } catch (...) {
        const auto failure = std::current_exception();
        if (decided) {
            migration_health_->failed.store(true, std::memory_order_release);
            store.reset();
            if (table && !serving_finished) RetireTable(*table, previous);
        } else {
            if (table && store) {
                try {
                    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_MIGRATION_RESUME_FAIL_ONCE"))
                        throw std::runtime_error("injected migration preparation resume failure");
                    if (store->background_maintenance_) store->StartMaintenanceThread();
                    table->EndExclusive(std::move(store), previous);
                } catch (const std::exception& restore_error) {
                    migration_health_->failed.store(true, std::memory_order_release);
                    store.reset();
                    RetireTable(*table, previous);
                    std::string reason;
                    try { std::rethrow_exception(failure); }
                    catch (const std::exception& original) { reason = original.what(); }
                    throw MigrationRecoveryRequiredError("migration preparation failed: " + reason +
                                             "; cannot resume table: " + restore_error.what());
                }
            }
            if (staging_owned) {
                std::error_code ec;
                std::filesystem::remove_all(staging, ec);
                if (ec) throw std::runtime_error("migration preparation failed and staging cleanup failed: " + ec.message());
            }
        }
        if (decided) {
            try { std::rethrow_exception(failure); }
            catch (const std::exception& error) { throw MigrationRecoveryRequiredError(error.what()); }
        }
        std::rethrow_exception(failure);
    }
}

}  // namespace chunkdb
