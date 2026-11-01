#include "migrations.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <mutex>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "file_permissions.hpp"
#include "durability_io.hpp"
#include "feature_flags.hpp"
#include "feed_slots.hpp"
#include "slot_watch.hpp"
#include "user_registry.hpp"
#include "chunk_coordinate_index.hpp"
#include "commit_owner_bootstrap.hpp"

namespace chunkdb {
namespace {

std::optional<std::vector<std::uint8_t>> FileImage(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) return std::nullopt;
    return LoadFile(path);
}

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) std::_Exit(86);
}

FeedSlotFilters MigrationSlotFilters(const MigrationRequest& request, const Geometry& geometry) {
    auto filters = request.slot_filters;
    if (filters.area && filters.area->dims != geometry.dims())
        throw std::invalid_argument("coordinate dimensions differ from the table");
    const auto& schema = geometry.layout().schema();
    if (request.slot_columns) {
        filters.column_ids.emplace();
        for (const auto& name : *request.slot_columns) {
            const auto at = std::find_if(schema.columns.begin(), schema.columns.end(), [&](const auto& column) { return column.name == name; });
            if (at == schema.columns.end()) throw std::invalid_argument("unknown feed column: " + name);
            filters.column_ids->push_back(at->id);
        }
    }
    for (const auto id : filters.column_ids.value_or(std::vector<std::uint32_t>{}))
        if (std::none_of(schema.columns.begin(), schema.columns.end(), [&](const auto& column) { return column.id == id; }))
            throw std::invalid_argument("unknown feed projection id");
    return filters;
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
    if (!feature_metadata_gate_.lock_shared(request.cancelled)) throw std::runtime_error("migration cancelled");
    std::shared_lock feature_metadata(feature_metadata_gate_, std::adopt_lock);
    if (config_.allow_multiple_processes) throw std::invalid_argument("MIGRATE requires a single-process catalog");
    if (!backup_metadata_gate_.lock_shared(request.cancelled)) throw std::runtime_error("migration cancelled");
    std::shared_lock metadata(backup_metadata_gate_, std::adopt_lock);
    if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kBeforeAdmission, request.record.name);
    std::unique_lock operations(operations_mutex_);
    migration_health_->Check();
    const auto initial_root = ReadDataDirManifest(config_.data_dir);
    if (!initial_root) throw std::runtime_error("data directory manifest disappeared");
    RequireFeature(initial_root->features, *FindFeature("migrations"));
    const auto already_applied = [&] {
        migration_health_->Check();
        for (const auto& record : ReadMigrationRecords(config_.data_dir)) {
            if (record.name != request.record.name) continue;
            if (record.statement != request.record.statement)
                throw MigrationConflictError("migration '" + record.name + "' has a different statement");
            return true;
        }
        // A fenced table is neither present nor absent until it is recovered.
        if (request.kind != MigrationRequest::Kind::kGrant) ThrowIfFenced(request.table);
        return false;
    };
    if (already_applied()) return false;
    operations.unlock();
    std::shared_ptr<Table> table;
    std::shared_ptr<ChunkStore> store;
    TableOptions previous;
    TableOptions next_options;
    std::filesystem::path staging;
    bool staging_owned = false;
    bool decided = false;
    bool serving_finished = false;
    bool maintenance_stopped = false;
    bool table_admitted = false;
    bool no_op = false;
    TableDefinition definition{request.geometry, request.options, request.schema};
    std::unique_lock<std::mutex> ddl;
    std::unique_lock<std::mutex> user_lock;
    try {
        if (request.kind != MigrationRequest::Kind::kCreate && request.kind != MigrationRequest::Kind::kGrant) {
            table = Find(request.table);
            if (!table) {
                operations.lock();
                if (already_applied()) return false;
                // Recheck under catalog admission: a creator may have won
                // while this migration was waiting for the operations lock.
                table = Find(request.table);
                if (!table && request.kind == MigrationRequest::Kind::kDrop && request.if_exists) no_op = true;
                else if (!table) throw TableNotFoundError("table '" + request.table + "' does not exist");
                if (table) operations.unlock();
            }
            if (table) {
                ddl = std::unique_lock(table->ddl_mutex_);
                if (request.kind == MigrationRequest::Kind::kCreateSlot) {
                    const auto lease = table->Acquire();
                    if (!lease) throw TableNotFoundError("table was dropped");
                    RequireFeature(lease->store().features(), *FindFeature("feed_slots"));
                    if (!request.slot_filters.empty() || request.slot_columns)
                        RequireFeature(lease->store().features(), *FindFeature("slot_filters"));
                }
                previous = table->Info().options;
                next_options = previous;
                if (request.kind == MigrationRequest::Kind::kAlter && request.columns_if_needed) {
                    auto lease = table->Acquire();
                    if (!lease) throw TableNotFoundError("table '" + request.table + "' was dropped");
                    lease->store().ThrowIfDurabilityPoisoned();
                    const auto info = table->Info();
                    no_op = !request.columns_if_needed(request.chunk_column ? info.chunk_schema : info.schema);
                }
                if ((request.kind == MigrationRequest::Kind::kCreateSlot && request.if_not_exists) ||
                    (request.kind == MigrationRequest::Kind::kDropSlot && request.if_exists)) {
                    RequireValidFeedSlotName(request.slot);
                    auto lease = table->Acquire();
                    if (!lease) throw TableNotFoundError("table '" + request.table + "' was dropped");
                    lease->store().ThrowIfDurabilityPoisoned();
                    if (lease->store().allow_multiple_processes_) throw std::invalid_argument("feed slots require a single-process table");
                    const auto slots = lease->store().feed_slots_->List(true);
                    const auto existing = std::find_if(slots.begin(), slots.end(), [&](const auto& slot) { return slot.name == request.slot; });
                    const bool exists = existing != slots.end();
                    if (request.kind == MigrationRequest::Kind::kCreateSlot && exists &&
                        existing->filters != MigrationSlotFilters(request, lease->store().geometry()))
                        throw FeedSlotFiltersError("feed slot filters differ from existing slot");
                    no_op = request.kind == MigrationRequest::Kind::kCreateSlot ? exists : !exists;
                }
                if (!no_op) {
                    store = table->BeginExclusive();
                    if (!store) {
                        operations.lock();
                        if (already_applied()) return false;
                        if (request.kind != MigrationRequest::Kind::kDrop || !request.if_exists || Find(request.table))
                            throw TableNotFoundError("table '" + request.table + "' was dropped");
                        table.reset();
                        no_op = true;
                    } else table_admitted = true;
                }
            }
        }
        if (!operations.owns_lock()) operations.lock();
        if (already_applied()) {
            if (store) {
                try { table->EndExclusive(std::move(store), previous); }
                catch (const std::exception& error) {
                    migration_health_->failed.store(true, std::memory_order_release);
                    RetireTable(*table, previous);
                    throw MigrationRecoveryRequiredError("cannot resume table '" + request.table + "' after migration retry: " + error.what());
                }
            }
            return false;
        }
        if (request.cancelled.stop_requested()) throw std::runtime_error("migration cancelled");
        if (table && Find(request.table) != table) throw TableNotFoundError("table '" + request.table + "' was replaced during migration admission");
        if (request.kind == MigrationRequest::Kind::kCreate) {
            RequireValidTableName(request.table);
            const auto existing = Find(request.table);
            if (existing && request.if_not_exists) {
                table = existing;
                no_op = true;
            } else if (request.definition) definition = request.definition();
        }
        auto records = ReadMigrationRecords(config_.data_dir);
        auto root = ReadDataDirManifest(config_.data_dir);
        if (!root) throw std::runtime_error("data directory manifest disappeared");
        RequireFeature(root->features, *FindFeature("migrations"));
        MigrationJournal journal;
        journal.data_dir_id = root->data_dir_id;
        journal.record = request.record;
        journal.record.applied_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        journal.table = request.kind == MigrationRequest::Kind::kGrant ? std::string{} : request.table;
        const auto add_file = [&](const std::filesystem::path& relative, std::vector<std::uint8_t> after) {
            journal.files.push_back({relative.generic_string(), FileImage(config_.data_dir / relative), std::move(after)});
        };
        std::optional<Users> next_users;
        std::shared_ptr<const CommitOwnerContext> creation_admission;
        if (no_op) {
            // Conditional DDL already holds: only the named ledger append is
            // journaled. Existing table identity is still validated on replay.
            journal.table = table ? request.table : std::string{};
            if (table) journal.table_id = table->store_id_;
        } else if (request.kind == MigrationRequest::Kind::kCreate) {
            RequireValidTableName(request.table);
            RequireValidTableOptions(definition.options);
            if (const auto reason = UnsupportedSchemaReason(definition.schema.value_or(SingleBitsColumnSchema(definition.geometry.block_bits))); !reason.empty()) throw std::invalid_argument(reason);
            const auto block_schema = definition.schema.value_or(SingleBitsColumnSchema(definition.geometry.block_bits));
            const auto geometry = definition.chunk_schema ? Geometry(definition.geometry, block_schema, *definition.chunk_schema)
                                                         : Geometry(definition.geometry, block_schema);
            if (Find(request.table)) throw TableExistsError("table '" + request.table + "' already exists");
            journal.directory_action = MigrationDirectoryAction::kCreate;
            journal.operation_name = OperationName(request.table);
            journal.table_id = NewStoreId();
            staging = StagingDir() / journal.operation_name;
            EnsureDirectoryPathExists(StagingDir(), true);
            if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("migration stage already exists");
            staging_owned = true;
            StoreManifest manifest{.features = TableScopedFeatures(root->features), .geometry = definition.geometry, .store_id = journal.table_id,
                                   .options = EncodeTableOptions(definition.options), .schema = block_schema,
                                   .chunk_schema = geometry.chunk_schema(), .commit_owner = root->commit_owner};
            if (!ChunkCoordinateIndex::InitializeEmpty(staging, geometry, journal.table_id))
                throw std::runtime_error("coordinate inventory appeared in migration staging");
            const auto bytes = SerializeStoreManifest(manifest);
            if (!PublishNewFile(StoreManifestPath(staging), bytes)) throw std::runtime_error("migration stage manifest already exists");
            if (commit_owner_context_)
                creation_admission = commit_owner_context_->ForStagedTable(journal.operation_name, journal.table_id);
            SyncDirectoryPath(StagingDir());
            journal.files.push_back({"tables/" + request.table + "/" + std::string(kStoreManifestFileName), std::nullopt, bytes});
            journal.files.push_back({"tables/" + request.table + "/" + std::string(kChunkIndexFileName),
                std::nullopt, LoadFile(staging / kChunkIndexFileName)});
            next_options = definition.options;
        } else if (request.kind != MigrationRequest::Kind::kGrant) {
            store->ThrowIfDurabilityPoisoned();
            store->StopMaintenanceThread();
            maintenance_stopped = true;
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
            } else if (request.kind == MigrationRequest::Kind::kAlter) {
                const auto previous_schema = manifest->schema;
                const auto previous_chunk_schema = *manifest->chunk_schema;
                if (request.narrowing) {
                    const auto& [column_name, type] = *request.narrowing;
                    const auto at = std::find_if(manifest->schema.columns.begin(), manifest->schema.columns.end(),
                        [&](const auto& column) { return column.name == column_name; });
                    if (at == manifest->schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                    if (HoldsEveryValue(at->type, type)) manifest->schema = ChangeColumnType(manifest->schema, column_name, type, Conversion::kExact);
                    else {
                        const auto pending = WithPendingNarrowing(manifest->schema, column_name, type);
                        if (const auto misfit = store->FindValueNotFitting(at->id, type))
                            throw std::invalid_argument("column " + column_name + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit + "; " + ColumnTypeRange(type));
                        manifest->schema = NarrowColumnType(pending, column_name, type);
                    }
                } else request.alter(*manifest);
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
                if (const auto reason = UnsupportedSchemaReason(manifest->schema); !reason.empty()) throw std::invalid_argument(reason);
                manifest->geometry.block_bits = FixedBitsPerBlock(manifest->schema);
                (void)Geometry(manifest->geometry, manifest->schema, *manifest->chunk_schema);
                next_options = DecodeTableOptions(manifest->options);
                RequireValidTableOptions(next_options);
                add_file("tables/" + request.table + "/" + std::string(kStoreManifestFileName), SerializeStoreManifest(*manifest));
            } else {
                RequireValidFeedSlotName(request.slot);
                if (store->allow_multiple_processes_) throw std::invalid_argument("feed slots require a single-process table");
                std::unique_lock eviction(store->resources_->stores_mutex_);
                const auto dims = store->geometry().dims();
                auto slots = ReadFeedSlotRecords(table->dir_, journal.table_id, dims).value_or(
                    FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = 0U, .slots = {}, .dims = dims});
                const auto at = std::find_if(slots.slots.begin(), slots.slots.end(), [&](const auto& slot) { return slot.name == request.slot; });
                if (request.kind == MigrationRequest::Kind::kCreateSlot) {
                    if (at != slots.slots.end()) throw std::invalid_argument("feed slot already exists: " + request.slot);
                    const auto filters = MigrationSlotFilters(request, store->geometry());
                    // Validate every field before syncing a baseline or preparing publication.
                    (void)SerializeFeedSlotRecords(FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = 0U,
                        .slots = {{request.slot, 0U, false, filters}}, .dims = dims});
                    RequireFeature(store->features(), *FindFeature("feed_slots"));
                    if (!filters.empty()) RequireFeature(store->features(), *FindFeature("slot_filters"));
                    table->PrepareFeedSlotBaseline(*store);
                    const auto completed = store->version_clock_.load(std::memory_order_seq_cst) - 1U;
                    store->feed_slots_->Sync(completed);
                    slots = ReadFeedSlotRecords(table->dir_, journal.table_id, dims).value_or(
                        FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = completed, .slots = {}, .dims = dims});
                    slots.slots.push_back({request.slot, completed, false, filters});
                    slots.filter_format = slots.filter_format || !filters.empty();
                    // The unchanged manifest image binds redo to the participant
                    // identity; slot creation must not enable missing bits.
                    add_file("tables/" + request.table + "/" + std::string(kStoreManifestFileName), SerializeStoreManifest(*manifest));
                } else {
                    if (at == slots.slots.end()) throw FeedSlotNotFoundError("unknown feed slot: " + request.slot);
                    slots.slots.erase(at);
                }
                add_file("tables/" + request.table + "/" + std::string(kFeedSlotsFileName), SerializeFeedSlotRecords(slots));
            }
        }
        if (users && (request.kind == MigrationRequest::Kind::kGrant || (!no_op && request.kind == MigrationRequest::Kind::kDrop))) {
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
        if (!users && !no_op && request.kind == MigrationRequest::Kind::kDrop) next_users = ReadUsersFile(config_.data_dir);
        if (!no_op && request.kind == MigrationRequest::Kind::kDrop && next_users)
            for (auto& [_, user] : next_users->users) user.grants.erase(request.table);
        if (request.kind == MigrationRequest::Kind::kGrant) {
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
        if (user_lock.owns_lock()) user_lock.unlock();
        if (!no_op && request.kind == MigrationRequest::Kind::kCreate) {
            auto opened = OpenStore(request.table, TablesDir() / request.table, definition.geometry, kAllGeometryFields,
                definition.options, std::move(creation_admission));
            auto geometry = opened->geometry();
            auto created = std::shared_ptr<Table>(new Table(request.table, TablesDir() / request.table, journal.table_id,
                std::move(geometry), definition.options, std::move(opened), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
            std::unique_lock lock(tables_mutex_);
            tables_.emplace(request.table, std::move(created));
        } else if (table && table_admitted) {
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
            if (table && table_admitted && !serving_finished) RetireTable(*table, previous);
        } else {
            if (table && store && migration_health_->failed.load(std::memory_order_acquire)) {
                store.reset();
                RetireTable(*table, previous);
            }
            if (table && store) {
                try {
                    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_MIGRATION_RESUME_FAIL_ONCE"))
                        throw std::runtime_error("injected migration preparation resume failure");
                    if (maintenance_stopped && store->background_maintenance_) store->StartMaintenanceThread();
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
            catch (const std::exception& error) { throw MigrationOutcomeUnknownError(error.what()); }
        }
        std::rethrow_exception(failure);
    }
}

}  // namespace chunkdb
