#pragma once

#include <cassert>
#include <utility>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb::test {

// Fixtures that exercise the named bits table create it explicitly. Reopening
// fixtures may request conditional creation to preserve its persisted schema.
inline std::shared_ptr<Table> CreateBitsTable(TableCatalog& catalog, const GeometryConfig& geometry,
                                            bool if_not_exists = false) {
    return catalog.Create("default", geometry, catalog.default_options(), std::nullopt, if_not_exists);
}

// Keep the actual catalog owner and table lease alive while exercising its store.
class OwnedTableStore {
  public:
    OwnedTableStore(const CatalogConfig& config, std::string_view name)
        : catalog_(config), table_(catalog_.Find(name)), lease_(Acquire(table_)) {}
    [[nodiscard]] ChunkStore& store() noexcept { return lease_.store(); }
  private:
    static Table::Lease Acquire(const std::shared_ptr<Table>& table) {
        assert(table);
        auto lease = table->Acquire();
        assert(lease);
        return std::move(*lease);
    }
    TableCatalog catalog_;
    std::shared_ptr<Table> table_;
    Table::Lease lease_;
};

}  // namespace chunkdb::test
