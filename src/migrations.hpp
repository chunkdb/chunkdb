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
};

}  // namespace chunkdb
