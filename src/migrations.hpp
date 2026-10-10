#pragma once

#include <functional>
#include <optional>
#include <string>

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
    std::string user;
    Right right = Right::kRead;
    bool revoke = false;
    std::string slot;
    bool if_not_exists = false;
    bool if_exists = false;
    std::function<TableDefinition()> definition;
    std::function<bool(const TableSchema&)> columns_if_needed;
};

}  // namespace chunkdb
