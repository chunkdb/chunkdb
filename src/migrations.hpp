#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/table_catalog.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "users.hpp"

namespace chunkdb {

struct MigrationRequest {
    enum class Kind { kCreate, kAlter, kDrop, kGrant, kCreateSlot, kDropSlot };
    MigrationRecord record;
    std::stop_token cancelled{};
    Kind kind = Kind::kCreate;
    std::string table;
    GeometryConfig geometry;
    TableOptions options;
    TableSchema schema;
    std::function<void(StoreManifest&)> alter;
    std::optional<std::pair<std::string, ColumnType>> narrowing;
    // ALTER COLUMN ... DROP NULL of this column, checked as a narrowing is.
    std::optional<std::string> drop_null{};
    // ALTER TABLE ... ADD CHECK: the check made from the current schema,
    // whose stored blocks are evaluated as a narrowing checks values.
    std::function<CheckConstraint(const TableSchema&)> add_check{};
    std::string user;
    std::optional<Right> right;
    std::uint8_t server_rights = 0U;
    bool revoke = false;
    std::string slot;
    FeedSlotFilters slot_filters{};
    std::optional<std::vector<std::string>> slot_columns{};
    std::optional<std::string> slot_owner{};
    // OWNER was written: IF NOT EXISTS then also requires that owner.
    bool slot_owner_given = false;
    bool if_not_exists = false;
    bool if_exists = false;
    std::function<TableDefinition()> definition;
    std::function<bool(const TableSchema&)> columns_if_needed;
    bool chunk_column = false;
    // Throws when the caller may not learn whether a recorded name is skipped
    // or conflicts; runs before a recorded name is compared.
    std::function<void(const MigrationRecord&)> require_recorded_access;
    // The steps after this one in a migration of several steps, which apply
    // with it as one unit; `record`, `cancelled` and `require_recorded_access`
    // are this request's.
    std::vector<MigrationRequest> then{};
};

}  // namespace chunkdb
