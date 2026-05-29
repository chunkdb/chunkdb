#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/lifecycle_log.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/server_defaults.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/uri.hpp"

namespace {

volatile std::sig_atomic_t g_shutdown_requested = 0;

void OnSignal(int) {
    g_shutdown_requested = 1;
}

std::uint16_t ParsePort(const std::string& value) {
    std::size_t consumed = 0;
    const int port = std::stoi(value, &consumed, 10);
    if (consumed != value.size() || port <= 0 || port > 65535) {
        throw std::invalid_argument("invalid port: " + value);
    }
    return static_cast<std::uint16_t>(port);
}

std::uint32_t ParseU32(const std::string& value, const char* field_name) {
    std::size_t consumed = 0;
    const unsigned long parsed = std::stoul(value, &consumed, 10);
    if (consumed != value.size() || parsed == 0 || parsed > 0xFFFFFFFFUL) {
        throw std::invalid_argument(std::string("invalid ") + field_name + ": " + value);
    }
    return static_cast<std::uint32_t>(parsed);
}

std::size_t ParseSize(const std::string& value, const char* field_name) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(value, &consumed, 10);
    if (consumed != value.size() || parsed == 0) {
        throw std::invalid_argument(std::string("invalid ") + field_name + ": " + value);
    }
    return static_cast<std::size_t>(parsed);
}

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_server [options]\n"
        << "  --host <host>\n"
        << "  --port <port>\n"
        << "  --workers <n>\n"
        << "  --client-io-timeout-ms <ms>\n"
        << "  --idle-connection-timeout-ms <ms>\n"
        << "  --max-pending-clients <n>\n"
        << "  --log-level <info|warn|error>\n"
        << "  --token <token>\n"
        << "  --no-auth\n"
        << "  --data-dir <path>\n"
        << "  --durability <relaxed|fsync-wal|fsync-checkpoint>\n"
        << "  --checkpoint-updates <n>\n"
        << "  --checkpoint-wal-bytes <n>\n"
        << "  --wal-group-commit-updates <n>\n"
        << "  --max-loaded-chunks <n>\n"
        << "  --max-open-wal-streams <n>\n"
        << "  --allow-multi-process\n"
        << "  --large-chunk-width <n>\n"
        << "  --large-chunk-height <n>\n"
        << "  --chunk-width <n>\n"
        << "  --chunk-height <n>\n"
        << "  --block-bits <n>\n"
        << "  --listen-uri <chunk://token@host:port/>\n"
        << "  --tls-cert <path-to-cert.pem>\n"
        << "  --tls-key <path-to-key.pem>\n";
}

}  // namespace

int main(int argc, char** argv) {
    chunkdb::LogLevel log_level = chunkdb::LogLevel::kInfo;
    try {
        chunkdb::ServerConfig server_config;
        chunkdb::StoreConfig store_config;
        chunkdb::EngineConfig engine_config;

        store_config.data_dir = "data";
        store_config.geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        };
        store_config.durability_mode = chunkdb::DurabilityMode::kRelaxed;
        store_config.checkpoint_update_interval = 256;
        store_config.checkpoint_wal_bytes = 1024 * 1024;
        store_config.allow_multiple_processes = false;

        engine_config.require_auth = true;
        engine_config.max_auth_failures = 5;

        const auto hw_threads = std::thread::hardware_concurrency();
        const bool fallback_worker_count = hw_threads == 0;
        bool workers_overridden = false;
        server_config.worker_threads = fallback_worker_count ? 4 : static_cast<std::size_t>(hw_threads);

        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            auto require_value = [&](const char* name) -> std::string {
                if (i + 1 >= argc) {
                    throw std::invalid_argument(std::string("missing value for ") + name);
                }
                ++i;
                return argv[i];
            };

            if (arg == "--host") {
                server_config.host = require_value("--host");
            } else if (arg == "--port") {
                server_config.port = ParsePort(require_value("--port"));
            } else if (arg == "--workers") {
                server_config.worker_threads = ParseSize(require_value("--workers"), "workers");
                workers_overridden = true;
            } else if (arg == "--client-io-timeout-ms") {
                server_config.client_io_timeout_ms =
                    ParseSize(require_value("--client-io-timeout-ms"), "client-io-timeout-ms");
            } else if (arg == "--idle-connection-timeout-ms") {
                server_config.idle_connection_timeout_ms =
                    ParseSize(require_value("--idle-connection-timeout-ms"), "idle-connection-timeout-ms");
            } else if (arg == "--max-pending-clients") {
                server_config.max_pending_clients =
                    ParseSize(require_value("--max-pending-clients"), "max-pending-clients");
            } else if (arg == "--log-level") {
                log_level = chunkdb::ParseLogLevel(require_value("--log-level"));
            } else if (arg == "--token") {
                engine_config.auth_token = require_value("--token");
                engine_config.require_auth = true;
            } else if (arg == "--no-auth") {
                engine_config.require_auth = false;
                engine_config.auth_token.clear();
            } else if (arg == "--data-dir") {
                store_config.data_dir = require_value("--data-dir");
            } else if (arg == "--durability") {
                store_config.durability_mode = chunkdb::ParseDurabilityMode(require_value("--durability"));
            } else if (arg == "--checkpoint-updates") {
                store_config.checkpoint_update_interval =
                    ParseSize(require_value("--checkpoint-updates"), "checkpoint-updates");
            } else if (arg == "--checkpoint-wal-bytes") {
                store_config.checkpoint_wal_bytes =
                    ParseSize(require_value("--checkpoint-wal-bytes"), "checkpoint-wal-bytes");
            } else if (arg == "--wal-group-commit-updates") {
                store_config.wal_group_commit_updates =
                    ParseSize(require_value("--wal-group-commit-updates"), "wal-group-commit-updates");
            } else if (arg == "--max-loaded-chunks") {
                store_config.max_loaded_chunks =
                    ParseSize(require_value("--max-loaded-chunks"), "max-loaded-chunks");
            } else if (arg == "--max-open-wal-streams") {
                store_config.max_open_wal_streams =
                    ParseSize(require_value("--max-open-wal-streams"), "max-open-wal-streams");
            } else if (arg == "--allow-multi-process") {
                store_config.allow_multiple_processes = true;
            } else if (arg == "--large-chunk-width") {
                store_config.geometry.large_chunk_width_chunks =
                    ParseU32(require_value("--large-chunk-width"), "large-chunk-width");
            } else if (arg == "--large-chunk-height") {
                store_config.geometry.large_chunk_height_chunks =
                    ParseU32(require_value("--large-chunk-height"), "large-chunk-height");
            } else if (arg == "--chunk-width") {
                store_config.geometry.chunk_width_blocks =
                    ParseU32(require_value("--chunk-width"), "chunk-width");
            } else if (arg == "--chunk-height") {
                store_config.geometry.chunk_height_blocks =
                    ParseU32(require_value("--chunk-height"), "chunk-height");
            } else if (arg == "--block-bits") {
                store_config.geometry.block_bits =
                    ParseU32(require_value("--block-bits"), "block-bits");
            } else if (arg == "--listen-uri") {
                const auto parsed_uri = chunkdb::ParseConnectionUri(require_value("--listen-uri"));
                server_config.host = parsed_uri.host;
                server_config.port = parsed_uri.port;
                server_config.tls_enabled = parsed_uri.secure;
                if (!parsed_uri.token.empty()) {
                    engine_config.auth_token = parsed_uri.token;
                    engine_config.require_auth = true;
                }
            } else if (arg == "--tls-cert") {
                server_config.tls_cert_path = require_value("--tls-cert");
            } else if (arg == "--tls-key") {
                server_config.tls_key_path = require_value("--tls-key");
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage();
                return 0;
            } else {
                throw std::invalid_argument("unknown argument: " + arg);
            }
        }

        chunkdb::SetLogLevel(log_level);

        if (fallback_worker_count && !workers_overridden) {
            chunkdb::LogMessage(
                chunkdb::LogLevel::kWarn,
                chunkdb::LogComponent::kServer,
                "worker thread count fallback applied",
                {{"workers", "4"}});
        }

        if (engine_config.require_auth && engine_config.auth_token.empty()) {
            throw std::invalid_argument(
                "authentication is enabled but token is empty; set --token or --listen-uri, or use --no-auth");
        }

        if (server_config.tls_enabled &&
            (server_config.tls_cert_path.empty() || server_config.tls_key_path.empty())) {
            throw std::invalid_argument(
                "TLS is enabled (chunks://) but --tls-cert/--tls-key are missing");
        }

        std::string build_type = "unknown";
#ifdef CHUNKDB_BUILD_TYPE_STR
        build_type = CHUNKDB_BUILD_TYPE_STR;
#endif
        std::string version = "unknown";
#ifdef CHUNKDB_VERSION_STR
        version = CHUNKDB_VERSION_STR;
#endif

#ifndef _WIN32
        // The ChunkStore auto-clamps max_open_wal_streams to (rlimit - 32),
        // but that reserve only accounts for non-socket fds. The server also
        // holds open one fd per pending and active client connection. Check
        // whether the total fd budget is tight and warn (or pre-clamp) so
        // the operator knows what happened.
        {
            struct rlimit fd_limit {};
            if (getrlimit(RLIMIT_NOFILE, &fd_limit) == 0 &&
                fd_limit.rlim_cur != RLIM_INFINITY) {
                const std::size_t rlimit_soft =
                    static_cast<std::size_t>(fd_limit.rlim_cur);
                // Fds the server needs beyond WAL streams:
                //   stdin/stdout/stderr (3) + listening socket (1)
                //   + process lock file (1) + pending+active client sockets
                constexpr std::size_t kSystemFds = 5;
                const std::size_t server_socket_fds =
                    server_config.max_pending_clients + server_config.worker_threads;
                const std::size_t non_wal_fds = kSystemFds + server_socket_fds;
                if (rlimit_soft <= non_wal_fds) {
                    chunkdb::LogMessage(
                        chunkdb::LogLevel::kWarn,
                        chunkdb::LogComponent::kServer,
                        "fd budget may be insufficient: rlimit leaves no room for WAL streams",
                        {
                            {"rlimit_nofile_soft", std::to_string(rlimit_soft)},
                            {"server_socket_fds", std::to_string(server_socket_fds)},
                            {"suggestion",
                             "raise ulimit -n or reduce --max-pending-clients / --max-open-wal-streams"},
                        });
                } else {
                    const std::size_t wal_budget = rlimit_soft - non_wal_fds;
                    if (store_config.max_open_wal_streams > wal_budget) {
                        chunkdb::LogMessage(
                            chunkdb::LogLevel::kWarn,
                            chunkdb::LogComponent::kServer,
                            "max_open_wal_streams reduced to fit fd budget after accounting for server sockets",
                            {
                                {"configured", std::to_string(store_config.max_open_wal_streams)},
                                {"effective", std::to_string(wal_budget)},
                                {"rlimit_nofile_soft", std::to_string(rlimit_soft)},
                                {"server_socket_fds", std::to_string(server_socket_fds)},
                                {"suggestion",
                                 "raise ulimit -n or reduce --max-pending-clients"},
                            });
                        store_config.max_open_wal_streams = wal_budget;
                    }
                }
            }
        }
#endif

        chunkdb::LogServerStartupContext(
            version,
            build_type,
            server_config,
            store_config);

        auto store = std::make_shared<chunkdb::ChunkStore>(store_config);
        auto engine = std::make_shared<chunkdb::CommandEngine>(engine_config, store);
        chunkdb::ChunkServer server(server_config, engine);

        std::signal(SIGINT, OnSignal);
        std::signal(SIGTERM, OnSignal);

        std::thread signal_watcher([&server]() {
            while (g_shutdown_requested == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            server.Stop();
        });

        try {
            server.Run();
        } catch (...) {
            g_shutdown_requested = 1;
            signal_watcher.join();
            throw;
        }
        g_shutdown_requested = 1;
        signal_watcher.join();
        return 0;
    } catch (const std::exception& e) {
        chunkdb::SetLogLevel(log_level);
        chunkdb::LogMessage(
            chunkdb::LogLevel::kError,
            chunkdb::LogComponent::kServer,
            "fatal startup error",
            {{"error", e.what()}});
        return 1;
    }
}
