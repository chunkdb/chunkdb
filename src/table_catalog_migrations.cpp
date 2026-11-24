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
#include "cql.hpp"

namespace chunkdb {
namespace {

std::optional<std::vector<std::uint8_t>> FileImage(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) return std::nullopt;
    return LoadFile(path);
}

void CrashAtFailpoint(const char* key) {
    if (ConsumeFailpointEnv(key)) ExitAbruptly(86);
}

// The slot filters of `request` on a table of `dims` with block columns `schema`.
FeedSlotFilters MigrationSlotFilters(const MigrationRequest& request, std::uint8_t dims, const TableSchema& schema) {
    auto filters = request.slot_filters;
    if (filters.area && filters.area->dims != dims)
        throw std::invalid_argument("coordinate dimensions differ from the table: the table has " +
                                    std::to_string(dims) + " and the slot area " +
                                    std::to_string(filters.area->dims) +
                                    "; give one coordinate per table dimension (DESCRIBE shows dims)");
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
    if (!request.then.empty()) return MigrateUnit(request, users);
    if (request.kind == MigrationRequest::Kind::kGrant && !request.revoke) RequireValidGrantTable(request.table);
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
            if (request.require_recorded_access) request.require_recorded_access(record);
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
    std::unique_lock<Table::DdlMutex> ddl;
    // Declared before user_lock: runs the users' OnChange after it is released.
    UserRegistry::Wakes users_changed(users);
    std::unique_lock<StatementMutex> user_lock;
    // A narrowing checks the stored values before catalog admission, under the
    // table's DDL lock and a lease; the schema it recorded then is the one the
    // decision may narrow.
    bool narrowing_recorded = false;
    TableSchema scanned_schema;
    // A recorded narrowing that is not decided is dropped again, so a refused or
    // skipped step leaves the schema as it was. A failed catalog or a fenced
    // table drops it when it is recovered; a table whose narrowing cannot be
    // dropped is fenced, and the step reports its own outcome.
    const auto drop_recorded_narrowing = [&] {
        if (!narrowing_recorded) return;
        narrowing_recorded = false;
        if (user_lock.owns_lock()) user_lock.unlock();
        if (operations.owns_lock()) operations.unlock();
        if (migration_health_->failed.load(std::memory_order_acquire) || Find(request.table) != table ||
            !table->Info().schema.pending) return;
        try {
            EndNarrowing(*table, previous, [](StoreManifest* manifest) { manifest->schema = WithoutPendingNarrowing(manifest->schema); });
        } catch (const std::exception&) {
            // Logged by EndNarrowing, which fenced the table if needed.
        }
    };
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
                    // Checked before any outcome is recorded, a conditional no-op included.
                    if (request.slot_owner) RequireValidFeedSlotOwner(*request.slot_owner);
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
                        existing->filters != MigrationSlotFilters(request, lease->store().geometry().dims(), lease->store().geometry().layout().schema()))
                        throw FeedSlotFiltersError("feed slot filters differ from existing slot");
                    if (request.kind == MigrationRequest::Kind::kCreateSlot && exists && request.slot_owner_given &&
                        existing->owner != request.slot_owner)
                        throw FeedSlotFiltersError("feed slot owner differs from existing slot");
                    no_op = request.kind == MigrationRequest::Kind::kCreateSlot ? exists : !exists;
                }
                if (!no_op && request.kind == MigrationRequest::Kind::kAlter && request.add_check) {
                    // An ADD CHECK evaluates every stored block first.
                    operations.lock();
                    if (already_applied()) return false;
                    operations.unlock();
                    narrowing_recorded = !table->Info().schema.pending.has_value();
                    std::string check_name;
                    RewriteManifest(
                        *table, previous,
                        [&](StoreManifest* manifest) {
                            auto check = request.add_check(manifest->schema);
                            check_name = check.name;
                            manifest->schema = WithPendingCheck(manifest->schema, std::move(check));
                        },
                        "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
                    scanned_schema = table->Info().schema;
                    if (const auto failure = ScanNarrowing(*table))
                        throw CheckFailedError(check_name,
                            "cannot add CHECK " + check_name + ": " + *failure + "; change or delete that block first");
                }
                if (!no_op && request.kind == MigrationRequest::Kind::kAlter && (request.narrowing || request.drop_null)) {
                    // A narrowing or a DROP NULL check.
                    const std::string column_name = request.narrowing ? request.narrowing->first : *request.drop_null;
                    const auto schema = table->Info().schema;
                    const auto at = std::find_if(schema.columns.begin(), schema.columns.end(),
                        [&](const auto& column) { return column.name == column_name; });
                    if (at == schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                    if (!request.narrowing || !HoldsEveryValue(at->type, request.narrowing->second)) {
                        operations.lock();
                        if (already_applied()) return false;
                        operations.unlock();
                        narrowing_recorded = !schema.pending.has_value();
                        RewriteManifest(
                            *table, previous,
                            [&](StoreManifest* manifest) {
                                manifest->schema = request.narrowing
                                    ? WithPendingNarrowing(manifest->schema, column_name, request.narrowing->second)
                                    : WithPendingNotNull(manifest->schema, column_name);
                            },
                            "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
                        scanned_schema = table->Info().schema;
                        if (const auto misfit = ScanNarrowing(*table)) {
                            if (!request.narrowing)
                                throw std::invalid_argument("column " + column_name + " cannot drop NULL: " + *misfit + "; set a value there first");
                            const auto& type = request.narrowing->second;
                            throw std::invalid_argument("column " + column_name + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit + "; " + ColumnTypeRange(type));
                        }
                    }
                }
                if (!no_op) {
                    store = table->BeginExclusive();
                    if (!store) {
                        operations.lock();
                        if (already_applied()) {
                            drop_recorded_narrowing();
                            return false;
                        }
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
            drop_recorded_narrowing();
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
        if (no_op) {
            // Conditional DDL already holds: only the named ledger append is
            // journaled. Existing table identity is still validated on replay.
            journal.table = table ? request.table : std::string{};
            if (table) journal.table_id = table->store_id_;
        } else if (request.kind == MigrationRequest::Kind::kCreate) {
            RequireNewTableName(request.table);
            RequireValidTableOptions(definition.options);
            if (const auto reason = UnsupportedSchemaReason(definition.schema.value_or(SingleBitsColumnSchema(definition.geometry.block_bits))); !reason.empty()) throw std::invalid_argument(reason);
            const auto block_schema = definition.schema.value_or(SingleBitsColumnSchema(definition.geometry.block_bits));
            const auto geometry = definition.chunk_schema ? Geometry(definition.geometry, block_schema, *definition.chunk_schema)
                                                         : Geometry(definition.geometry, block_schema);
            if (Find(request.table)) throw TableExistsError("table '" + request.table + "' already exists; use another name, CREATE TABLE IF NOT EXISTS, or DROP TABLE " + request.table + " first");
            journal.directory_action = MigrationDirectoryAction::kCreate;
            journal.operation_name = OperationName(request.table);
            journal.table_id = NewStoreId();
            staging = StagingDir() / journal.operation_name;
            EnsureDirectoryPathExists(StagingDir(), true);
            if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("migration stage already exists");
            staging_owned = true;
            StoreManifest manifest{.features = TableScopedFeatures(root->features), .geometry = definition.geometry, .store_id = journal.table_id,
                                   .options = EncodeTableOptions(definition.options), .schema = block_schema,
                                   .chunk_schema = geometry.chunk_schema()};
            if (!ChunkCoordinateIndex::InitializeEmpty(staging, geometry, journal.table_id))
                throw std::runtime_error("coordinate inventory appeared in migration staging");
            const auto bytes = SerializeStoreManifest(manifest);
            if (!PublishNewFile(StoreManifestPath(staging), bytes)) throw std::runtime_error("migration stage manifest already exists");
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
                    if (narrowing_recorded) {
                        // The scan checked the values of this schema only.
                        if (manifest->schema != scanned_schema)
                            throw std::runtime_error("table '" + request.table + "' changed during the narrowing check");
                        manifest->schema = NarrowColumnType(manifest->schema, column_name, type);
                    } else {
                        const auto at = std::find_if(manifest->schema.columns.begin(), manifest->schema.columns.end(),
                            [&](const auto& column) { return column.name == column_name; });
                        if (at == manifest->schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                        if (!HoldsEveryValue(at->type, type))
                            throw std::runtime_error("table '" + request.table + "' changed before the narrowing check");
                        manifest->schema = ChangeColumnType(manifest->schema, column_name, type, Conversion::kExact);
                    }
                } else if (request.drop_null) {
                    // The scan checked the values of this schema only.
                    if (!narrowing_recorded || manifest->schema != scanned_schema)
                        throw std::runtime_error("table '" + request.table + "' changed during the DROP NULL check");
                    manifest->schema = chunkdb::DropColumnNull(manifest->schema, *request.drop_null);
                } else if (request.add_check) {
                    // The scan evaluated the blocks of this schema only.
                    if (!narrowing_recorded || manifest->schema != scanned_schema)
                        throw std::runtime_error("table '" + request.table + "' changed during the ADD CHECK scan");
                    manifest->schema = AddPendingCheck(manifest->schema);
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
                    const auto filters = MigrationSlotFilters(request, store->geometry().dims(), store->geometry().layout().schema());
                    // Validate every field before syncing a baseline or preparing publication.
                    if (request.slot_owner) RequireValidFeedSlotOwner(*request.slot_owner);
                    (void)SerializeFeedSlotRecords(FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = 0U,
                        .slots = {{request.slot, 0U, false, filters, false, request.slot_owner}}, .dims = dims});
                    RequireFeature(store->features(), *FindFeature("feed_slots"));
                    if (!filters.empty()) RequireFeature(store->features(), *FindFeature("slot_filters"));
                    table->PrepareFeedSlotBaseline(*store);
                    const auto completed = store->version_clock_.load(std::memory_order_seq_cst) - 1U;
                    store->feed_slots_->Sync(completed);
                    slots = ReadFeedSlotRecords(table->dir_, journal.table_id, dims).value_or(
                        FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = completed, .slots = {}, .dims = dims});
                    slots.slots.push_back({request.slot, completed, false, filters, false, request.slot_owner});
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
            users->ConfirmPublication();
            users->migration_health_ = migration_health_;
            const auto disk_users = ReadUsersFile(config_.data_dir);
            if (!disk_users) throw std::runtime_error("migration users file disappeared");
            if (users->users_ != *disk_users) {
                users->CommitPublication(*disk_users,
                    users->PreparePublication(std::make_shared<const Users>(*disk_users)), users_changed);
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
            ApplyGrant(at->second, request.table, request.right, request.server_rights, request.revoke);
        }
        if (next_users) add_file(std::string(kUsersFileName), EncodeUsers(*next_users));
        // Built before the decision, so publishing the users cannot fail after it.
        auto next_published_users = next_users && users ?
            users->PreparePublication(std::make_shared<const Users>(*next_users)) : UserPublication{};
        records.push_back(journal.record);
        add_file(std::string(kMigrationsFileName), EncodeMigrationRecords(root->data_dir_id, records));
        ValidateMigrationJournal(config_.data_dir, journal);
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kPrepared, request.record.name);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_DECISION_ONCE");
        WriteMigrationJournal(config_.data_dir, journal, &decided);
        decided = true;
        // The decision makes the users change durable: publish it now, as a
        // later completion failure must not leave memory behind the decision.
        if (next_users && users) users->CommitPublication(std::move(*next_users), std::move(next_published_users), users_changed);
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
        if (user_lock.owns_lock()) user_lock.unlock();
        if (!no_op && request.kind == MigrationRequest::Kind::kCreate) {
            auto opened = OpenStore(request.table, TablesDir() / request.table, definition.geometry, kAllGeometryFields, definition.options);
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
            drop_recorded_narrowing();
        }
        if (decided) {
            try { std::rethrow_exception(failure); }
            catch (const std::exception& error) { throw MigrationOutcomeUnknownError(error.what()); }
        }
        std::rethrow_exception(failure);
    }
}

namespace {

// ADD CHECK and DROP NULL check every stored value before admission.
bool ChecksStoredValues(const MigrationRequest& step) {
    return step.kind == MigrationRequest::Kind::kAlter && (step.add_check || step.drop_null);
}

// The shape of a migration of several steps, checked before it waits for
// anything: one table, no DROP TABLE, CREATE TABLE and a value check only
// as the first step. Steps across tables need the cross-table commit.
void RequireMigrationUnit(const std::vector<const MigrationRequest*>& steps) {
    using Kind = MigrationRequest::Kind;
    if (steps.size() > cql::kMaxMigrationSteps)
        throw std::invalid_argument("a migration has at most " + std::to_string(cql::kMaxMigrationSteps) + " steps");
    for (std::size_t i = 0; i < steps.size(); ++i) {
        const auto& step = *steps[i];
        const auto where = "step " + std::to_string(i + 1U) + " of the migration ";
        if (step.table != steps.front()->table)
            throw std::invalid_argument(where + "names '" + step.table + "' and step 1 names '" + steps.front()->table +
                                        "'; the steps of one migration change one table, so migrate each table under its own name");
        if (step.kind == Kind::kDrop)
            throw std::invalid_argument(where + "drops the table; run DROP TABLE as a migration of its own");
        if (step.kind == Kind::kCreate && i != 0U)
            throw std::invalid_argument(where + "creates the table; CREATE TABLE can only be the first step");
        if (ChecksStoredValues(step) && i != 0U)
            throw std::invalid_argument(where + "checks stored values; ADD CHECK and ALTER COLUMN ... DROP NULL can only be the first step");
        if (step.kind == Kind::kGrant && !step.revoke) RequireValidGrantTable(step.table);
    }
}

}  // namespace

bool TableCatalog::MigrateUnit(const MigrationRequest& request, UserRegistry* users) {
    using Kind = MigrationRequest::Kind;
    RequireWritable("MIGRATE");
    std::vector<const MigrationRequest*> steps{&request};
    for (const auto& step : request.then) steps.push_back(&step);
    RequireMigrationUnit(steps);
    const bool grants_only = std::all_of(steps.begin(), steps.end(), [](const auto* step) { return step->kind == Kind::kGrant; });
    const bool creates = request.kind == Kind::kCreate;
    const bool creates_slot = std::any_of(steps.begin(), steps.end(), [](const auto* step) { return step->kind == Kind::kCreateSlot; });
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
            if (request.require_recorded_access) request.require_recorded_access(record);
            if (record.statement != request.record.statement)
                throw MigrationConflictError("migration '" + record.name + "' has a different statement");
            return true;
        }
        // A fenced table is neither present nor absent until it is recovered.
        if (!grants_only) ThrowIfFenced(request.table);
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
    std::unique_lock<Table::DdlMutex> ddl;
    // Declared before user_lock: runs the users' OnChange after it is released.
    UserRegistry::Wakes users_changed(users);
    std::unique_lock<StatementMutex> user_lock;
    // As in Migrate: a first step that checks stored values records its check
    // before catalog admission, and a step that is not decided drops it again.
    bool narrowing_recorded = false;
    TableSchema scanned_schema;
    const auto drop_recorded_narrowing = [&] {
        if (!narrowing_recorded) return;
        narrowing_recorded = false;
        if (user_lock.owns_lock()) user_lock.unlock();
        if (operations.owns_lock()) operations.unlock();
        if (migration_health_->failed.load(std::memory_order_acquire) || Find(request.table) != table ||
            !table->Info().schema.pending) return;
        try {
            EndNarrowing(*table, previous, [](StoreManifest* manifest) { manifest->schema = WithoutPendingNarrowing(manifest->schema); });
        } catch (const std::exception&) {
            // Logged by EndNarrowing, which fenced the table if needed.
        }
    };
    try {
        if (!grants_only) {
            table = Find(request.table);
            if (!table && !creates) {
                operations.lock();
                if (already_applied()) return false;
                table = Find(request.table);
                if (!table) throw TableNotFoundError("table '" + request.table + "' does not exist");
                operations.unlock();
            }
            // CREATE TABLE without IF NOT EXISTS of a table that exists is
            // refused under catalog admission, after the name is compared.
            if (table && creates && !request.if_not_exists) table.reset();
            if (table) {
                ddl = std::unique_lock(table->ddl_mutex_);
                previous = table->Info().options;
                next_options = previous;
                if (request.kind == Kind::kAlter && request.add_check) {
                    operations.lock();
                    if (already_applied()) return false;
                    operations.unlock();
                    narrowing_recorded = !table->Info().schema.pending.has_value();
                    std::string check_name;
                    RewriteManifest(
                        *table, previous,
                        [&](StoreManifest* manifest) {
                            auto check = request.add_check(manifest->schema);
                            check_name = check.name;
                            manifest->schema = WithPendingCheck(manifest->schema, std::move(check));
                        },
                        "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
                    scanned_schema = table->Info().schema;
                    if (const auto failure = ScanNarrowing(*table))
                        throw CheckFailedError(check_name,
                            "cannot add CHECK " + check_name + ": " + *failure + "; change or delete that block first");
                }
                if (request.kind == Kind::kAlter && (request.narrowing || request.drop_null)) {
                    const std::string column_name = request.narrowing ? request.narrowing->first : *request.drop_null;
                    const auto schema = table->Info().schema;
                    const auto at = std::find_if(schema.columns.begin(), schema.columns.end(),
                        [&](const auto& column) { return column.name == column_name; });
                    if (at == schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                    if (!request.narrowing || !HoldsEveryValue(at->type, request.narrowing->second)) {
                        operations.lock();
                        if (already_applied()) return false;
                        operations.unlock();
                        narrowing_recorded = !schema.pending.has_value();
                        RewriteManifest(
                            *table, previous,
                            [&](StoreManifest* manifest) {
                                manifest->schema = request.narrowing
                                    ? WithPendingNarrowing(manifest->schema, column_name, request.narrowing->second)
                                    : WithPendingNotNull(manifest->schema, column_name);
                            },
                            "CHUNKDB_FAILPOINT_NARROW_PENDING_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE");
                        scanned_schema = table->Info().schema;
                        if (const auto misfit = ScanNarrowing(*table)) {
                            if (!request.narrowing)
                                throw std::invalid_argument("column " + column_name + " cannot drop NULL: " + *misfit + "; set a value there first");
                            const auto& type = request.narrowing->second;
                            throw std::invalid_argument("column " + column_name + " cannot be narrowed to " + ColumnTypeName(type) + ": " + *misfit + "; " + ColumnTypeRange(type));
                        }
                    }
                }
                // Every step reads and changes the table's files under
                // exclusive access, a step whose condition holds included.
                store = table->BeginExclusive();
                if (!store) {
                    operations.lock();
                    if (already_applied()) {
                        drop_recorded_narrowing();
                        return false;
                    }
                    throw TableNotFoundError("table '" + request.table + "' was dropped");
                }
                table_admitted = true;
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
            drop_recorded_narrowing();
            return false;
        }
        if (request.cancelled.stop_requested()) throw std::runtime_error("migration cancelled");
        const bool created = creates && !table_admitted;
        if (!grants_only) {
            const auto current = Find(request.table);
            if (table_admitted && current != table)
                throw TableNotFoundError("table '" + request.table + "' was replaced during migration admission");
            if (created && current) {
                if (!request.if_not_exists)
                    throw TableExistsError("table '" + request.table + "' already exists; use another name, CREATE TABLE IF NOT EXISTS, or DROP TABLE " + request.table + " first");
                throw TableExistsError("table '" + request.table + "' was created while this migration waited; run the migration again");
            }
            if (created) RequireNewTableName(request.table);
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
        journal.table = grants_only ? std::string{} : request.table;
        const auto add_file = [&](const std::filesystem::path& relative, std::vector<std::uint8_t> after) {
            journal.files.push_back({relative.generic_string(), FileImage(config_.data_dir / relative), std::move(after)});
        };
        // The table's manifest as the steps change it, step by step.
        StoreManifest manifest;
        if (table_admitted) {
            store->ThrowIfDurabilityPoisoned();
            store->StopMaintenanceThread();
            maintenance_stopped = true;
            store->FlushWalBatchesForReopen();
            journal.table_id = table->store_id_;
            const auto current = ReadStoreManifest(table->dir_);
            if (!current || current->store_id != journal.table_id) throw std::runtime_error("migration table identity changed");
            manifest = *current;
        } else if (created) {
            TableDefinition definition{request.geometry, request.options, request.schema};
            if (request.definition) definition = request.definition();
            RequireValidTableOptions(definition.options);
            const auto block_schema = definition.schema.value_or(SingleBitsColumnSchema(definition.geometry.block_bits));
            if (const auto reason = UnsupportedSchemaReason(block_schema); !reason.empty()) throw std::invalid_argument(reason);
            const auto geometry = definition.chunk_schema ? Geometry(definition.geometry, block_schema, *definition.chunk_schema)
                                                         : Geometry(definition.geometry, block_schema);
            journal.table_id = NewStoreId();
            manifest = StoreManifest{.features = TableScopedFeatures(root->features), .geometry = definition.geometry, .store_id = journal.table_id,
                                     .options = EncodeTableOptions(definition.options), .schema = block_schema,
                                     .chunk_schema = geometry.chunk_schema()};
            next_options = definition.options;
        }
        bool manifest_changed = created;
        bool slots_changed = false;
        bool grants_changed = false;
        std::optional<FeedSlotRecords> slots;
        std::vector<std::string> dropped_slots;
        std::optional<Users> next_users;
        std::uint64_t completed = 0U;
        // The table's slot records, read once at the first slot step; a CREATE
        // SLOT step syncs the feed baseline first, as Migrate does.
        const auto load_slots = [&] {
            if (slots) return;
            if (created)
                throw std::invalid_argument("CREATE SLOT and DROP SLOT on a table that the same migration creates run in a later migration");
            if (store->allow_multiple_processes_) throw std::invalid_argument("feed slots require a single-process table");
            std::unique_lock eviction(store->resources_->stores_mutex_);
            const auto dims = store->geometry().dims();
            if (creates_slot) {
                RequireFeature(store->features(), *FindFeature("feed_slots"));
                table->PrepareFeedSlotBaseline(*store);
                completed = store->version_clock_.load(std::memory_order_seq_cst) - 1U;
                store->feed_slots_->Sync(completed);
            }
            slots = ReadFeedSlotRecords(table->dir_, journal.table_id, dims).value_or(
                FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = completed, .slots = {}, .dims = dims});
        };
        // The users as the steps change them, read once at the first GRANT or
        // REVOKE step under the registry's lock, as Migrate reads them.
        const auto load_users = [&] {
            if (next_users) return;
            if (users) {
                if (!std::filesystem::equivalent(users->data_dir_, config_.data_dir))
                    throw std::invalid_argument("migration users belong to another data directory");
                user_lock = std::unique_lock(users->mutex_);
                if (users->migration_health_) users->migration_health_->Check();
                users->ConfirmPublication();
                users->migration_health_ = migration_health_;
                const auto disk_users = ReadUsersFile(config_.data_dir);
                if (!disk_users) throw std::runtime_error("migration users file disappeared");
                if (users->users_ != *disk_users) {
                    users->CommitPublication(*disk_users,
                        users->PreparePublication(std::make_shared<const Users>(*disk_users)), users_changed);
                }
                next_users = users->users_;
            }
            if (!next_users) throw std::invalid_argument("this server runs without users (--auth none)");
        };
        for (std::size_t i = 0; i < steps.size(); ++i) {
            const auto& step = *steps[i];
            const auto number = std::to_string(i + 1U);
            if (ConsumeFailpointEnv(("CHUNKDB_FAILPOINT_MIGRATION_UNIT_STEP_" + number + "_FAIL_ONCE").c_str()))
                throw std::runtime_error("injected failure in migration step " + number);
            switch (step.kind) {
                case Kind::kCreate:
                    // Prepared above, or a CREATE TABLE IF NOT EXISTS of the table that exists.
                    break;
                case Kind::kAlter: {
                    if (step.columns_if_needed && !step.columns_if_needed(step.chunk_column ? *manifest.chunk_schema : manifest.schema))
                        break;
                    const auto previous_schema = manifest.schema;
                    const auto previous_chunk_schema = *manifest.chunk_schema;
                    if (step.narrowing) {
                        const auto& [column_name, type] = *step.narrowing;
                        if (i == 0U && narrowing_recorded) {
                            // The scan checked the values of this schema only.
                            if (manifest.schema != scanned_schema)
                                throw std::runtime_error("table '" + request.table + "' changed during the narrowing check");
                            manifest.schema = NarrowColumnType(manifest.schema, column_name, type);
                        } else {
                            const auto at = std::find_if(manifest.schema.columns.begin(), manifest.schema.columns.end(),
                                [&](const auto& column) { return column.name == column_name; });
                            if (at == manifest.schema.columns.end()) throw std::invalid_argument("the table has no column " + column_name);
                            if (!HoldsEveryValue(at->type, type)) {
                                if (i == 0U) throw std::runtime_error("table '" + request.table + "' changed before the narrowing check");
                                throw std::invalid_argument("step " + number + " of the migration narrows column " + column_name +
                                    ", which checks stored values; a narrowing can only be the first step");
                            }
                            manifest.schema = ChangeColumnType(manifest.schema, column_name, type, Conversion::kExact);
                        }
                    } else if (step.drop_null) {
                        if (!narrowing_recorded || manifest.schema != scanned_schema)
                            throw std::runtime_error("table '" + request.table + "' changed during the DROP NULL check");
                        manifest.schema = chunkdb::DropColumnNull(manifest.schema, *step.drop_null);
                    } else if (step.add_check) {
                        if (!narrowing_recorded || manifest.schema != scanned_schema)
                            throw std::runtime_error("table '" + request.table + "' changed during the ADD CHECK scan");
                        manifest.schema = AddPendingCheck(manifest.schema);
                    } else step.alter(manifest);
                    AlignManifestSchemaDomains(&manifest, previous_schema, previous_chunk_schema);
                    if (!created) {
                        RequireCompatibleColumnAdditions(previous_schema, manifest.schema, *store);
                        for (const auto& column : manifest.chunk_schema->columns) {
                            if (!column.required || column.has_default ||
                                std::any_of(previous_chunk_schema.columns.begin(), previous_chunk_schema.columns.end(),
                                    [&](const auto& old) { return old.id == column.id; })) continue;
                            if (store->HasWrittenChunkIdentities())
                                throw std::invalid_argument("chunk column " + column.name +
                                    ": a REQUIRED column added to a table needs a DEFAULT for its written chunk identities");
                            break;
                        }
                    }
                    if (const auto reason = UnsupportedSchemaReason(manifest.schema); !reason.empty()) throw std::invalid_argument(reason);
                    manifest.geometry.block_bits = FixedBitsPerBlock(manifest.schema);
                    (void)Geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
                    next_options = DecodeTableOptions(manifest.options);
                    RequireValidTableOptions(next_options);
                    manifest_changed = true;
                    break;
                }
                case Kind::kCreateSlot: {
                    RequireValidFeedSlotName(step.slot);
                    load_slots();
                    const auto dims = store->geometry().dims();
                    // Columns are named in the schema the earlier steps left.
                    const auto filters = MigrationSlotFilters(step, dims, manifest.schema);
                    const auto at = std::find_if(slots->slots.begin(), slots->slots.end(), [&](const auto& slot) { return slot.name == step.slot; });
                    if (at != slots->slots.end()) {
                        if (!step.if_not_exists) throw std::invalid_argument("feed slot already exists: " + step.slot);
                        if (at->filters != filters) throw FeedSlotFiltersError("feed slot filters differ from existing slot");
                        if (step.slot_owner_given && at->owner != step.slot_owner)
                            throw FeedSlotFiltersError("feed slot owner differs from existing slot");
                        break;
                    }
                    if (step.slot_owner) RequireValidFeedSlotOwner(*step.slot_owner);
                    (void)SerializeFeedSlotRecords(FeedSlotRecords{.epoch = journal.table_id, .durable_watermark = 0U,
                        .slots = {{step.slot, 0U, false, filters, false, step.slot_owner}}, .dims = dims});
                    if (!filters.empty()) RequireFeature(store->features(), *FindFeature("slot_filters"));
                    slots->slots.push_back({step.slot, completed, false, filters, false, step.slot_owner});
                    slots->filter_format = slots->filter_format || !filters.empty();
                    slots_changed = true;
                    break;
                }
                case Kind::kDropSlot: {
                    RequireValidFeedSlotName(step.slot);
                    load_slots();
                    const auto at = std::find_if(slots->slots.begin(), slots->slots.end(), [&](const auto& slot) { return slot.name == step.slot; });
                    if (at == slots->slots.end()) {
                        if (step.if_exists) break;
                        throw FeedSlotNotFoundError("unknown feed slot: " + step.slot);
                    }
                    slots->slots.erase(at);
                    dropped_slots.push_back(step.slot);
                    slots_changed = true;
                    break;
                }
                case Kind::kGrant: {
                    load_users();
                    const auto at = next_users->users.find(step.user);
                    if (at == next_users->users.end()) throw std::invalid_argument("user " + step.user + " does not exist");
                    ApplyGrant(at->second, step.table, step.right, step.server_rights, step.revoke);
                    grants_changed = true;
                    break;
                }
                case Kind::kDrop:
                    throw std::logic_error("RequireMigrationUnit admits no DROP TABLE step");
            }
            CrashAtFailpoint(("CHUNKDB_FAILPOINT_CRASH_MIGRATION_UNIT_STEP_" + number + "_ONCE").c_str());
        }
        if (created) {
            journal.directory_action = MigrationDirectoryAction::kCreate;
            journal.operation_name = OperationName(request.table);
            staging = StagingDir() / journal.operation_name;
            EnsureDirectoryPathExists(StagingDir(), true);
            if (!CreateOwnerOnlyDirectory(staging)) throw std::runtime_error("migration stage already exists");
            staging_owned = true;
            const Geometry geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
            if (!ChunkCoordinateIndex::InitializeEmpty(staging, geometry, journal.table_id))
                throw std::runtime_error("coordinate inventory appeared in migration staging");
            const auto bytes = SerializeStoreManifest(manifest);
            if (!PublishNewFile(StoreManifestPath(staging), bytes)) throw std::runtime_error("migration stage manifest already exists");
            SyncDirectoryPath(StagingDir());
            journal.files.push_back({"tables/" + request.table + "/" + std::string(kStoreManifestFileName), std::nullopt, bytes});
            journal.files.push_back({"tables/" + request.table + "/" + std::string(kChunkIndexFileName),
                std::nullopt, LoadFile(staging / kChunkIndexFileName)});
        } else if (manifest_changed || slots_changed) {
            // A slot change carries the manifest too, which binds redo to the
            // table identity, as a single CREATE SLOT does.
            add_file("tables/" + request.table + "/" + std::string(kStoreManifestFileName), SerializeStoreManifest(manifest));
            if (slots_changed) add_file("tables/" + request.table + "/" + std::string(kFeedSlotsFileName), SerializeFeedSlotRecords(*slots));
        }
        // When every step's condition already holds, only the ledger append is
        // journaled; the table identity is still validated on replay.
        if (grants_changed) add_file(std::string(kUsersFileName), EncodeUsers(*next_users));
        // Built before the decision, so publishing the users cannot fail after it.
        auto next_published_users = grants_changed && users ?
            users->PreparePublication(std::make_shared<const Users>(*next_users)) : UserPublication{};
        records.push_back(journal.record);
        add_file(std::string(kMigrationsFileName), EncodeMigrationRecords(root->data_dir_id, records));
        ValidateMigrationJournal(config_.data_dir, journal);
        if (auto* hook = migration_health_->hook.load(std::memory_order_acquire)) hook->Run(MigrationTestHook::Point::kPrepared, request.record.name);
        CrashAtFailpoint("CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_DECISION_ONCE");
        WriteMigrationJournal(config_.data_dir, journal, &decided);
        decided = true;
        if (grants_changed && users) users->CommitPublication(std::move(*next_users), std::move(next_published_users), users_changed);
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
        if (user_lock.owns_lock()) user_lock.unlock();
        if (created) {
            auto opened = OpenStore(request.table, TablesDir() / request.table, manifest.geometry, 0U, next_options);
            auto geometry = opened->geometry();
            auto table_created = std::shared_ptr<Table>(new Table(request.table, TablesDir() / request.table, journal.table_id,
                std::move(geometry), next_options, std::move(opened), config_.feed_buffer_bytes, migration_health_, config_.feed_linger));
            std::unique_lock lock(tables_mutex_);
            tables_.emplace(request.table, std::move(table_created));
        } else if (table_admitted) {
            auto reopened = OpenStore(request.table, table->dir_, table->geometry().config(), 0U, next_options);
            if (unsynced) reopened->AdoptUnsynced(*unsynced);
            for (const auto& slot : dropped_slots) {
                const auto at = table->slot_claims_.find(slot);
                if (at != table->slot_claims_.end()) {
                    if (const auto claim = at->second.lock()) claim->valid.store(false, std::memory_order_release);
                    table->slot_claims_.erase(at);
                }
                table->slot_ack_state_->pending.erase(slot);
            }
            table->EndExclusive(std::move(reopened), next_options);
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
            drop_recorded_narrowing();
        }
        if (decided) {
            try { std::rethrow_exception(failure); }
            catch (const std::exception& error) { throw MigrationOutcomeUnknownError(error.what()); }
        }
        std::rethrow_exception(failure);
    }
}

}  // namespace chunkdb
