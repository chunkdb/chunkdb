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
            {"client_io_timeout_ms", std::to_string(server_config.client_io_timeout_ms)},
            {"idle_connection_timeout_ms", std::to_string(server_config.idle_connection_timeout_ms)},
            {"max_pending_clients", std::to_string(server_config.max_pending_clients)},
            {"durability_mode", DurabilityModeName(store_config.durability_mode)},
            {"wal_group_commit_updates", std::to_string(store_config.wal_group_commit_updates)},
            {"max_loaded_chunks", std::to_string(store_config.max_loaded_chunks)},
            {"access_mode", AccessModeName(store_config.access_mode)},
            {"storage_layout_mode", StorageLayoutModeName(store_config.storage_layout_mode)},
            {"data_dir", store_config.data_dir.string()},
        });
}

}  // namespace chunkdb
