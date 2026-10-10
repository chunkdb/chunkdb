#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
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
#include "crypto.hpp"
#include "scram.hpp"
#include "user_registry.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunkdb/uri.hpp"

namespace {

// Set by the signal handler and read by the main thread. A lock-free atomic
// is async-signal-safe and, unlike volatile sig_atomic_t, also safe across
// threads (the handler may run on any thread).
std::atomic<bool> g_shutdown_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free);

void OnSignal(int) {
    g_shutdown_requested.store(true);
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

// The first line of a file holding a secret (a password), without its line
// ending.
std::string ReadSecretFile(const std::string& path, const char* what) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::invalid_argument(std::string("failed to open ") + what + " file: " + path);
    }
    std::string secret;
    std::getline(input, secret);
    if (!secret.empty() && secret.back() == '\r') {
        secret.pop_back();
    }
    if (secret.empty()) {
        throw std::invalid_argument(std::string(what) + " file is empty: " + path);
    }
    return secret;
}

bool IsLoopbackBindAddress(const std::string& host) {
    return host == "localhost" || host == "::1" || host == "[::1]" ||
           host == "127.0.0.1" || host.rfind("127.", 0) == 0;
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
        << "  --max-handshakes-per-ip <n>\n"
        << "  --max-line-bytes <n>\n"
        << "  --txn-max-duration-ms <ms>\n"
        << "  --txn-max-bytes <n>\n"
        << "  --txn-total-bytes <n>\n"
        << "  --feed-buffer-bytes <n>\n"
        << "  --max-watches <n>\n"
        << "  --slot-max-bytes <n>\n"
        << "  --slot-sync-ms <ms>\n"
        << "  --txn-history-bytes <n>\n"
        << "      Transactions: how long one stays open (5000), the bytes of the\n"
        << "      chunks one writes (16 MiB) and all open ones write (256 MiB), and\n"
        << "      the bytes per table of chunk states kept for them (64 MiB).\n"
        << "  --log-level <info|warn|error>\n"
        << "  --auth <scram|none>\n"
        << "      scram (default): users log in with a password (SCRAM-SHA-256).\n"
        << "      none: no users, every connection has every right; for local development.\n"
        << "  --admin-user <name>\n"
        << "  --admin-password-file <path>\n"
        << "      The first administrator, created when the data directory has no users\n"
        << "      (also CHUNKDB_ADMIN_USER and CHUNKDB_ADMIN_PASSWORD).\n"
        << "  --data-dir <path>\n"
        << "  --backup-dir <path>\n"
        << "      BACKUP destinations must be relative to this directory; unset disables BACKUP.\n"
        << "  --durability <relaxed|fsync-wal|fsync-checkpoint>\n"
        << "  --checkpoint-updates <n>\n"
        << "  --checkpoint-wal-bytes <n>\n"
        << "  --wal-group-commit-updates <n>\n"
        << "  --checkpoint-compression <none|zrle>\n"
        << "      Options of tables this server creates. A given flag must also\n"
        << "      match the options every existing table stores, otherwise the\n"
        << "      server does not start; change a table with ALTER TABLE ... SET.\n"
        << "  --max-loaded-chunks <n>\n"
        << "  --max-open-wal-streams <n>\n"
        << "  --allow-multi-process\n"
        << "  --background-maintenance\n"
        << "  --background-checkpoint-queue-limit <n>\n"
        << "  --large-chunk-width <n>\n"
        << "  --large-chunk-height <n>\n"
        << "  --chunk-width <n>\n"
        << "  --chunk-height <n>\n"
        << "  --block-bits <n>\n"
        << "      Geometry of the default table, which the server creates when the\n"
        << "      data directory has no tables. Geometry is fixed when a table is\n"
        << "      created: for an existing default table these flags may be\n"
        << "      omitted, and a given flag must match it.\n"
        << "  --listen-uri <chunk://host:port/>\n"
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
        // Geometry flags apply when the default table is created. An existing
        // default table keeps its recorded geometry; a flag given must match.
        store_config.geometry_fields = 0;
        store_config.durability_mode = chunkdb::DurabilityMode::kRelaxed;
        store_config.checkpoint_update_interval = 256;
        store_config.checkpoint_wal_bytes = 1024 * 1024;
        store_config.allow_multiple_processes = false;

        engine_config.require_auth = true;
        engine_config.max_auth_failures = 5;

        const auto hw_threads = std::thread::hardware_concurrency();
        const bool fallback_worker_count = hw_threads == 0;
        bool workers_overridden = false;
        // Table options given as flags; each must match what every existing
        // table stores (see TableCatalog).
        std::uint32_t option_fields = 0;
        std::string auth_mode = "scram";
        std::optional<std::string> admin_user;
        std::optional<std::string> admin_password_file;
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
            } else if (arg == "--feed-buffer-bytes") {
                server_config.feed_buffer_bytes = ParseSize(require_value("--feed-buffer-bytes"), "feed-buffer-bytes");
            } else if (arg == "--max-watches") {
                server_config.max_watches = ParseSize(require_value("--max-watches"), "max-watches");
            } else if (arg == "--slot-max-bytes" || arg == "--slot-sync-ms") {
                const auto value = require_value(arg.c_str());
                std::size_t parsed = 0;
                const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
                if (result.ec != std::errc() || result.ptr != value.data() + value.size() || parsed == 0U ||
                    (arg == "--slot-sync-ms" && parsed > static_cast<std::uint64_t>(std::numeric_limits<std::int32_t>::max()))) {
                    throw std::invalid_argument("invalid " + arg + ": " + value);
                }
                if (arg == "--slot-max-bytes") store_config.slot_max_bytes = parsed;
                else store_config.slot_sync_interval = std::chrono::milliseconds(parsed);
            } else if (arg == "--max-handshakes-per-ip") {
                server_config.max_handshakes_per_ip =
                    ParseSize(require_value("--max-handshakes-per-ip"), "max-handshakes-per-ip");
            } else if (arg == "--max-line-bytes") {
                server_config.max_line_bytes =
                    ParseSize(require_value("--max-line-bytes"), "max-line-bytes");
            } else if (arg == "--txn-max-duration-ms") {
                engine_config.txn_max_duration = std::chrono::milliseconds(
                    ParseSize(require_value("--txn-max-duration-ms"), "txn-max-duration-ms"));
            } else if (arg == "--txn-max-bytes") {
                engine_config.txn_max_bytes = ParseSize(require_value("--txn-max-bytes"), "txn-max-bytes");
            } else if (arg == "--txn-total-bytes") {
                engine_config.txn_total_bytes = ParseSize(require_value("--txn-total-bytes"), "txn-total-bytes");
            } else if (arg == "--txn-history-bytes") {
                store_config.txn_history_bytes = ParseSize(require_value("--txn-history-bytes"), "txn-history-bytes");
            } else if (arg == "--log-level") {
                log_level = chunkdb::ParseLogLevel(require_value("--log-level"));
            } else if (arg == "--auth") {
                auth_mode = require_value("--auth");
                if (auth_mode != "scram" && auth_mode != "none") {
                    throw std::invalid_argument("--auth is scram or none, got " + auth_mode);
                }
            } else if (arg == "--admin-user") {
                admin_user = require_value("--admin-user");
            } else if (arg == "--admin-password-file") {
                admin_password_file = require_value("--admin-password-file");
            } else if (arg == "--data-dir") {
                store_config.data_dir = require_value("--data-dir");
            } else if (arg == "--backup-dir") {
                engine_config.backup_dir = require_value("--backup-dir");
                if (engine_config.backup_dir.empty())
                    throw std::invalid_argument("--backup-dir must not be empty");
            } else if (arg == "--durability") {
                store_config.durability_mode = chunkdb::ParseDurabilityMode(require_value("--durability"));
                option_fields |= chunkdb::kOptionFieldDurabilityMode;
            } else if (arg == "--checkpoint-updates") {
                store_config.checkpoint_update_interval =
                    ParseSize(require_value("--checkpoint-updates"), "checkpoint-updates");
                option_fields |= chunkdb::kOptionFieldCheckpointUpdates;
            } else if (arg == "--checkpoint-wal-bytes") {
                store_config.checkpoint_wal_bytes =
                    ParseSize(require_value("--checkpoint-wal-bytes"), "checkpoint-wal-bytes");
                option_fields |= chunkdb::kOptionFieldCheckpointWalBytes;
            } else if (arg == "--wal-group-commit-updates") {
                store_config.wal_group_commit_updates =
                    ParseSize(require_value("--wal-group-commit-updates"), "wal-group-commit-updates");
                option_fields |= chunkdb::kOptionFieldWalGroupCommitUpdates;
            } else if (arg == "--max-loaded-chunks") {
                store_config.max_loaded_chunks =
                    ParseSize(require_value("--max-loaded-chunks"), "max-loaded-chunks");
            } else if (arg == "--max-open-wal-streams") {
                store_config.max_open_wal_streams =
                    ParseSize(require_value("--max-open-wal-streams"), "max-open-wal-streams");
            } else if (arg == "--allow-multi-process") {
                store_config.allow_multiple_processes = true;
            } else if (arg == "--checkpoint-compression") {
                store_config.checkpoint_compression =
                    chunkdb::ParseCheckpointCompression(require_value("--checkpoint-compression"));
                option_fields |= chunkdb::kOptionFieldCheckpointCompression;
            } else if (arg == "--background-maintenance") {
                store_config.background_maintenance = true;
            } else if (arg == "--background-checkpoint-queue-limit") {
                store_config.background_checkpoint_queue_limit = ParseSize(
                    require_value("--background-checkpoint-queue-limit"),
                    "background-checkpoint-queue-limit");
            } else if (arg == "--large-chunk-width") {
                store_config.geometry.large_chunk_width_chunks =
                    ParseU32(require_value("--large-chunk-width"), "large-chunk-width");
                store_config.geometry_fields |= chunkdb::kGeometryLargeChunkWidth;
            } else if (arg == "--large-chunk-height") {
                store_config.geometry.large_chunk_height_chunks =
                    ParseU32(require_value("--large-chunk-height"), "large-chunk-height");
                store_config.geometry_fields |= chunkdb::kGeometryLargeChunkHeight;
            } else if (arg == "--chunk-width") {
                store_config.geometry.chunk_width_blocks =
                    ParseU32(require_value("--chunk-width"), "chunk-width");
                store_config.geometry_fields |= chunkdb::kGeometryChunkWidth;
            } else if (arg == "--chunk-height") {
                store_config.geometry.chunk_height_blocks =
                    ParseU32(require_value("--chunk-height"), "chunk-height");
                store_config.geometry_fields |= chunkdb::kGeometryChunkHeight;
            } else if (arg == "--block-bits") {
                store_config.geometry.block_bits =
                    ParseU32(require_value("--block-bits"), "block-bits");
                store_config.geometry_fields |= chunkdb::kGeometryBlockBits;
            } else if (arg == "--listen-uri") {
                const auto parsed_uri = chunkdb::ParseConnectionUri(require_value("--listen-uri"));
                server_config.host = parsed_uri.host;
                server_config.port = parsed_uri.port;
                server_config.tls_enabled = parsed_uri.secure;
                if (!parsed_uri.user.empty()) {
                    throw std::invalid_argument(
                        "--listen-uri takes no user: users are created with --admin-user and CREATE USER");
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

        engine_config.require_auth = auth_mode == "scram";
        // The first administrator, used only when the data directory has no
        // users yet.
        std::optional<std::pair<std::string, chunkdb::scram::Verifier>> first_admin;
        if (engine_config.require_auth &&
            !std::filesystem::exists(store_config.data_dir / chunkdb::kUsersFileName)) {
            const char* env_user = std::getenv("CHUNKDB_ADMIN_USER");
            const char* env_password = std::getenv("CHUNKDB_ADMIN_PASSWORD");
            std::optional<std::string> user = admin_user;
            if (!user.has_value() && env_user != nullptr && env_user[0] != '\0') {
                user = env_user;
            }
            std::optional<std::string> password;
            if (admin_password_file.has_value()) {
                password = ReadSecretFile(*admin_password_file, "admin password");
            } else if (env_password != nullptr && env_password[0] != '\0') {
                password = env_password;
            }
            if (user.has_value() != password.has_value()) {
                throw std::invalid_argument(
                    "the first administrator needs both a user (--admin-user or CHUNKDB_ADMIN_USER) and a "
                    "password (--admin-password-file or CHUNKDB_ADMIN_PASSWORD)");
            }
            if (user.has_value()) {
                first_admin = std::make_pair(
                    *user,
                    chunkdb::scram::MakeVerifier(*password, chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations));
            }
        }

        if (fallback_worker_count && !workers_overridden) {
            chunkdb::LogMessage(
                chunkdb::LogLevel::kWarn,
                chunkdb::LogComponent::kServer,
                "worker thread count fallback applied",
                {{"workers", "4"}});
        }

        if (!engine_config.require_auth && !IsLoopbackBindAddress(server_config.host)) {
            chunkdb::LogMessage(
                chunkdb::LogLevel::kWarn,
                chunkdb::LogComponent::kServer,
                "authentication disabled on non-loopback bind address",
                {
                    {"host", server_config.host},
                    {"port", std::to_string(server_config.port)},
                });
        }

        if (!server_config.tls_enabled && !IsLoopbackBindAddress(server_config.host)) {
            // SCRAM keeps passwords off the wire, but values and statements
            // travel in the clear (docs/USERS_DESIGN.md).
            chunkdb::LogMessage(
                chunkdb::LogLevel::kWarn,
                chunkdb::LogComponent::kServer,
                "listening beyond localhost without TLS: data and statements travel unencrypted; set --tls-cert "
                "and --tls-key",
                {
                    {"host", server_config.host},
                    {"port", std::to_string(server_config.port)},
                });
        }

        if (server_config.tls_enabled &&
            (server_config.tls_cert_path.empty() || server_config.tls_key_path.empty())) {
            throw std::invalid_argument(
                "TLS is enabled (chunks://) but --tls-cert/--tls-key are missing; set both flags to readable PEM certificate and private key files");
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
        // Automatically fit max_open_wal_streams and max_pending_clients
        // within the process fd budget so users never hit mysterious EMFILE
        // errors. The budget is split: WAL streams get half, server sockets
        // get the rest (after reserving a small amount for system fds).
        //
        // Without this, a server with default max_pending_clients=1024 on a
        // system with ulimit=1024 would try to hold 992 WAL fds + 1028
        // socket fds simultaneously, inevitably hitting EMFILE.
        {
            struct rlimit fd_limit {};
            if (getrlimit(RLIMIT_NOFILE, &fd_limit) == 0 &&
                fd_limit.rlim_cur != RLIM_INFINITY) {
                const std::size_t rlimit_soft =
                    static_cast<std::size_t>(fd_limit.rlim_cur);

                // stdin/stdout/stderr + listening socket + process lock file
                constexpr std::size_t kSystemFds = 8;

                if (rlimit_soft <= kSystemFds) {
                    throw std::runtime_error(
                        "RLIMIT_NOFILE is too small to run the server (soft=" +
                        std::to_string(rlimit_soft) + ")");
                }

                const std::size_t usable = rlimit_soft - kSystemFds;

                // Give WAL streams half the usable budget, capped at what was
                // configured. Server sockets (workers + pending) get the rest.
                const std::size_t wal_cap =
                    std::min(store_config.max_open_wal_streams, usable / 2);
                const std::size_t socket_budget =
                    usable > wal_cap ? usable - wal_cap : 0;
                const std::size_t max_pending_cap =
                    socket_budget > server_config.worker_threads
                        ? socket_budget - server_config.worker_threads
                        : 1;

                bool adjusted = false;
                if (store_config.max_open_wal_streams > wal_cap) {
                    store_config.max_open_wal_streams = wal_cap;
                    adjusted = true;
                }
                if (server_config.max_pending_clients > max_pending_cap) {
                    server_config.max_pending_clients = max_pending_cap;
                    adjusted = true;
                }

                if (adjusted) {
                    chunkdb::LogMessage(
                        chunkdb::LogLevel::kWarn,
                        chunkdb::LogComponent::kServer,
                        "config adjusted to fit RLIMIT_NOFILE fd budget",
                        {
                            {"rlimit_nofile_soft", std::to_string(rlimit_soft)},
                            {"max_open_wal_streams",
                             std::to_string(store_config.max_open_wal_streams)},
                            {"max_pending_clients",
                             std::to_string(server_config.max_pending_clients)},
                            {"tip", "raise 'ulimit -n' to increase these limits"},
                        });
                }
            }
        }
#endif

        chunkdb::LogServerStartupContext(
            version,
            build_type,
            server_config,
            store_config);

        auto catalog_config = chunkdb::CatalogConfigFromStoreConfig(store_config, option_fields);
        catalog_config.feed_buffer_bytes = server_config.feed_buffer_bytes;
        std::shared_ptr<chunkdb::TableCatalog> catalog;
        try {
            catalog = std::make_shared<chunkdb::TableCatalog>(std::move(catalog_config));
        } catch (const std::exception& error) {
            const std::string detail(error.what());
            const auto hint = store_config.access_mode == chunkdb::AccessMode::kReadOnly
                ? "; set --data-dir to an initialized directory; a first start must use read-write mode"
                : "; check --data-dir and its mount/permissions; new data needs a writable empty directory, and existing data must use this storage format";
            throw std::runtime_error(detail + hint);
        }
        engine_config.server_version = version;
        engine_config.max_line_bytes = server_config.max_line_bytes;
        if (engine_config.require_auth) {
            const auto secret_bytes = chunkdb::crypto::RandomBytes(32);
            std::array<std::uint8_t, 32> secret{};
            std::copy(secret_bytes.begin(), secret_bytes.end(), secret.begin());
            engine_config.users =
                std::make_shared<chunkdb::UserRegistry>(store_config.data_dir, std::move(first_admin), secret);
        }
        auto engine = std::make_shared<chunkdb::CommandEngine>(engine_config, catalog);
        chunkdb::ChunkServer server(server_config, engine);

        std::signal(SIGINT, OnSignal);
        std::signal(SIGTERM, OnSignal);
#ifndef _WIN32
        // A write to a connection the peer has reset raises SIGPIPE, whose
        // default action ends the process. Errors are handled per connection.
        std::signal(SIGPIPE, SIG_IGN);
#endif

        std::thread signal_watcher([&server]() {
            while (!g_shutdown_requested.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            server.Stop();
        });

        try {
            server.Run();
        } catch (...) {
            g_shutdown_requested.store(true);
            signal_watcher.join();
            throw;
        }
        g_shutdown_requested.store(true);
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
