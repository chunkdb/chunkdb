#pragma once

#include "chunkdb/table_catalog.hpp"

namespace chunkdb::test {

// Fixtures that exercise the named bits table create it explicitly. Reopening
// fixtures may request conditional creation to preserve its persisted schema.
inline std::shared_ptr<Table> CreateBitsTable(TableCatalog& catalog, const GeometryConfig& geometry,
                                            bool if_not_exists = false) {
    return catalog.Create("default", geometry, catalog.default_options(), std::nullopt, if_not_exists);
}

}  // namespace chunkdb::test
