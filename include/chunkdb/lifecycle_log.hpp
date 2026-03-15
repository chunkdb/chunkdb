#pragma once

#include <string_view>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/server.hpp"

namespace chunkdb {

void LogServerStartupContext(
    std::string_view version,
    std::string_view build,
    const ServerConfig& server_config,
    const StoreConfig& store_config);

}  // namespace chunkdb
