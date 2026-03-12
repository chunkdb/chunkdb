#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/uri.hpp"

namespace {

chunkdb::ChunkServer* g_server = nullptr;

void OnSignal(int) {
    if (g_server != nullptr) {
        g_server->Stop();
    }
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

void PrintUsage() {
    std::cout
        << "Usage: chunkdb_server [options]\n"
        << "  --host <host>\n"
        << "  --port <port>\n"
        << "  --token <token>\n"
        << "  --no-auth\n"
        << "  --data-dir <path>\n"
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

        engine_config.require_auth = true;
        engine_config.max_auth_failures = 5;

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
            } else if (arg == "--token") {
                engine_config.auth_token = require_value("--token");
                engine_config.require_auth = true;
            } else if (arg == "--no-auth") {
                engine_config.require_auth = false;
                engine_config.auth_token.clear();
            } else if (arg == "--data-dir") {
                store_config.data_dir = require_value("--data-dir");
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

        if (engine_config.require_auth && engine_config.auth_token.empty()) {
            throw std::invalid_argument(
                "authentication is enabled but token is empty; set --token or --listen-uri, or use --no-auth");
        }

        if (server_config.tls_enabled &&
            (server_config.tls_cert_path.empty() || server_config.tls_key_path.empty())) {
            throw std::invalid_argument(
                "TLS is enabled (chunks://) but --tls-cert/--tls-key are missing");
        }

        auto store = std::make_shared<chunkdb::ChunkStore>(store_config);
        auto engine = std::make_shared<chunkdb::CommandEngine>(engine_config, store);
        chunkdb::ChunkServer server(server_config, engine);

        g_server = &server;
        std::signal(SIGINT, OnSignal);
        std::signal(SIGTERM, OnSignal);

        std::cout << "chunkdb listening on " << server_config.host << ":" << server_config.port;
        if (server_config.tls_enabled) {
            std::cout << " (TLS enabled)";
        }
        std::cout << std::endl;
        server.Run();

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 1;
    }
}
