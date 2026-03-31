#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/server_bench.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

namespace {

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

struct ScopedSocketPlatform {
#ifdef _WIN32
    ScopedSocketPlatform() {
        WSADATA wsa_data;
        const int rc = WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (rc != 0) {
            throw std::runtime_error("WSAStartup failed");
        }
    }
    ~ScopedSocketPlatform() { WSACleanup(); }
#else
    ScopedSocketPlatform() = default;
    ~ScopedSocketPlatform() = default;
#endif
};

std::uint16_t PickFreePort() {
    ScopedSocketPlatform platform;
    (void)platform;

    const SocketHandle s = socket(AF_INET, SOCK_STREAM, 0);
    assert(s != kInvalidSocket);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    assert(bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0);

    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    assert(getsockname(s, reinterpret_cast<sockaddr*>(&bound), &len) == 0);
    const std::uint16_t port = ntohs(bound.sin_port);
    CloseSocket(s);
    return port;
}

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    return base / ("chunkdb-server-bench-test-" + suffix + "-" + std::to_string(tick));
}

struct ExternalServerHarness {
    std::filesystem::path data_dir;
    std::shared_ptr<chunkdb::ChunkStore> store;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;

    explicit ExternalServerHarness(const std::string& suffix) {
        data_dir = TempDataDir(suffix);
        port = PickFreePort();

        store = std::make_shared<chunkdb::ChunkStore>(chunkdb::StoreConfig{
            .geometry = {
                .large_chunk_width_chunks = 8,
                .large_chunk_height_chunks = 8,
                .chunk_width_blocks = 16,
                .chunk_height_blocks = 16,
                .block_bits = 16,
            },
            .data_dir = data_dir,
            .durability_mode = chunkdb::DurabilityMode::kRelaxed,
            .checkpoint_update_interval = 512,
            .checkpoint_wal_bytes = 1024 * 1024,
            .wal_group_commit_updates = 8,
            .max_loaded_chunks = 4096,
            .allow_multiple_processes = false,
        });

        engine = std::make_shared<chunkdb::CommandEngine>(
            chunkdb::EngineConfig{
                .auth_token = "",
                .require_auth = false,
                .max_auth_failures = 5,
            },
            store);

        server = std::make_unique<chunkdb::ChunkServer>(
            chunkdb::ServerConfig{
                .host = "127.0.0.1",
                .port = port,
                .max_line_bytes = 65536,
                .worker_threads = 2,
                .client_io_timeout_ms = 5000,
                .max_pending_clients = 1024,
                .tls_enabled = false,
                .tls_cert_path = "",
                .tls_key_path = "",
            },
            engine);

        thread = std::thread([this]() { server->Run(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ~ExternalServerHarness() {
        if (server != nullptr) {
            server->Stop();
        }
        if (thread.joinable()) {
            thread.join();
        }
        server.reset();
        engine.reset();
        store.reset();
        std::error_code ec;
        std::filesystem::remove_all(data_dir, ec);
    }
};

void TestParseArgsNewFlags() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--server-mode", "external",
        "--host", "127.0.0.1",
        "--port", "4242",
        "--clients", "12",
        "--pipeline", "4",
        "--requests", "777",
        "--tests", "ping,set,get",
        "--keyspace", "2048",
        "--seed", "99",
        "--output", "json",
    });

    assert(args.server_mode == chunkdb::server_bench::ServerMode::kExternal);
    assert(args.host == "127.0.0.1");
    assert(args.port == 4242);
    assert(args.clients == 12);
    assert(args.pipeline == 4);
    assert(args.requests == 777);
    assert(args.keyspace == 2048);
    assert(args.seed == 99);
    assert(args.output_mode == chunkdb::server_bench::OutputMode::kJson);
    assert(args.tests.size() == 3);
    assert(args.tests[0] == chunkdb::server_bench::Scenario::kPing);
    assert(args.tests[1] == chunkdb::server_bench::Scenario::kSet);
    assert(args.tests[2] == chunkdb::server_bench::Scenario::kGet);
}

void TestParseArgsInvalidCombination() {
    bool threw = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--pipeline", "0",
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--server-mode", "invalid",
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);

    threw = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--tests", "ping,unknown",
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
}

void TestParseArgsUriPopulatesEndpointAndToken() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--uri", "chunk://bench-token@bench.local:4321/",
    });

    assert(args.host == "bench.local");
    assert(args.port == 4321);
    assert(args.auth_token == "bench-token");
}

void TestParseArgsExplicitFlagsOverrideUri() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--uri", "chunk://uri-token@uri-host:1999/",
        "--host", "127.0.0.1",
        "--port", "4242",
        "--token", "flag-token",
    });

    assert(args.host == "127.0.0.1");
    assert(args.port == 4242);
    assert(args.auth_token == "flag-token");
}

void TestParseArgsChunksUriRejected() {
    bool threw = false;
    std::string message;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--uri", "chunks://secure-token@127.0.0.1:4242/",
        });
    } catch (const std::invalid_argument& e) {
        threw = true;
        message = e.what();
    }
    assert(threw);
    assert(message.find("chunks:// is not supported by chunkdb_server_bench yet") != std::string::npos);
}

void TestExternalModeDoesNotSpawn() {
    ExternalServerHarness harness("external");
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kExternal,
        .host = "127.0.0.1",
        .port = harness.port,
        .clients = 2,
        .pipeline = 2,
        .requests = 120,
        .tests = {chunkdb::server_bench::Scenario::kPing},
        .keyspace = 128,
        .seed = 7,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .auth_token = "",
    });

    assert(!report.spawned_server);
    assert(report.requested_clients == 2);
    assert(report.active_clients == 2);
    assert(report.results.size() == 1);
    assert(report.results.front().name == "ping");
    assert(report.results.front().completed_requests == 120);
}

void TestSpawnModeStartsAndStops() {
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kSpawn,
        .host = "127.0.0.1",
        .port = PickFreePort(),
        .clients = 2,
        .pipeline = 1,
        .requests = 80,
        .tests = {chunkdb::server_bench::Scenario::kPing},
        .keyspace = 128,
        .seed = 17,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .auth_token = "",
    });

    assert(report.spawned_server);
    assert(report.requested_clients == 2);
    assert(report.active_clients == 2);
    assert(report.results.size() == 1);
    assert(report.results.front().completed_requests == 80);
}

void TestOutputContainsPercentilesAndJsonFields() {
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kSpawn,
        .host = "127.0.0.1",
        .port = PickFreePort(),
        .clients = 2,
        .pipeline = 3,
        .requests = 120,
        .tests = {chunkdb::server_bench::Scenario::kPing},
        .keyspace = 64,
        .seed = 123,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .auth_token = "",
    });

    const std::string human = chunkdb::server_bench::RenderHumanReport(report);
    assert(human.find("Throughput (req/s)") != std::string::npos);
    assert(human.find("Latency (ms)") != std::string::npos);
    assert(human.find("Percentile Distribution (ms)") != std::string::npos);
    assert(human.find("requested_clients=") != std::string::npos);
    assert(human.find("active_clients=") != std::string::npos);

    const std::string json = chunkdb::server_bench::RenderJsonReport(report);
    assert(!json.empty());
    assert(json.front() == '{');
    assert(json.find("\"server_mode\"") != std::string::npos);
    assert(json.find("\"requested_clients\"") != std::string::npos);
    assert(json.find("\"active_clients\"") != std::string::npos);
    assert(json.find("\"results\"") != std::string::npos);
    assert(json.find("\"latency_ms\"") != std::string::npos);
    assert(json.find("\"percentiles_ms\"") != std::string::npos);

    assert(report.results.front().max_in_flight >= 2);
}

void TestIdleClientsNoteWhenRequestsLessThanClients() {
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kSpawn,
        .host = "127.0.0.1",
        .port = PickFreePort(),
        .clients = 8,
        .pipeline = 1,
        .requests = 3,
        .tests = {chunkdb::server_bench::Scenario::kPing},
        .keyspace = 64,
        .seed = 2026,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .auth_token = "",
    });

    assert(report.requested_clients == 8);
    assert(report.active_clients < report.requested_clients);
    assert(report.results.size() == 1);
    assert(report.results.front().completed_requests == 3);

    const std::string human = chunkdb::server_bench::RenderHumanReport(report);
    assert(human.find("requested_clients=8") != std::string::npos);
    assert(human.find("active_clients=3") != std::string::npos);
    assert(human.find("some clients were idle due to requests distribution") != std::string::npos);

    const std::string json = chunkdb::server_bench::RenderJsonReport(report);
    assert(json.find("\"requested_clients\":8") != std::string::npos);
    assert(json.find("\"active_clients\":3") != std::string::npos);
}

}  // namespace

int main() {
    TestParseArgsNewFlags();
    TestParseArgsInvalidCombination();
    TestParseArgsUriPopulatesEndpointAndToken();
    TestParseArgsExplicitFlagsOverrideUri();
    TestParseArgsChunksUriRejected();
    TestExternalModeDoesNotSpawn();
    TestSpawnModeStartsAndStops();
    TestOutputContainsPercentilesAndJsonFields();
    TestIdleClientsNoteWhenRequestsLessThanClients();
    return 0;
}
