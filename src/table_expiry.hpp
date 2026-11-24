#pragma once

#include <cstddef>
#include <string_view>

#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

// The feed user of expiry deletions; no user name starts with '@'.
inline constexpr std::string_view kExpiryWriteUser = "@expire";
// Populated chunks one expiry pass reads at most; a sweep of more chunks
// takes several pool turns.
inline constexpr std::size_t kExpiryPassChunks = 64U;

struct ExpiryTestAccess {
    // Starts an expiry sweep of `table` on the shared maintenance pool, as
    // the catalog's ticker does, and waits until its turns end.
    static void RunSweep(TableCatalog& catalog, std::string_view table);
};

}  // namespace chunkdb
