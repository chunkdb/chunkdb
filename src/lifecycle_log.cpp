#include "chunkdb/lifecycle_log.hpp"

#include <string>

#include "chunkdb/logging.hpp"

namespace chunkdb {

void LogServerStartupContext(
    std::string_view version,
    std::string_view build,
    const ServerConfig& server_config,
    const StoreConfig& store_config) {
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kServer,
        "server starting",
        {
            {"version", version},
            {"build", build},
        });

    LogMessage(
        LogLevel::kInfo,
        LogComponent::kServer,
        "effective config",
        {
            {"host", server_config.host},
            {"port", std::to_string(server_config.port)},
            {"tls", server_config.tls_enabled ? "on" : "off"},
            {"workers", std::to_string(server_config.worker_threads)},
            {"durability_mode", DurabilityModeName(store_config.durability_mode)},
            {"access_mode", AccessModeName(store_config.access_mode)},
            {"storage_layout_mode", StorageLayoutModeName(store_config.storage_layout_mode)},
            {"data_dir", store_config.data_dir.string()},
        });
}

}  // namespace chunkdb
