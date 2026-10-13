#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"
#include "catalog_test_utils.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"
#include "chunkdb/server_bench.hpp"
#include "login_helpers.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
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
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;

    explicit ExternalServerHarness(
        const std::string& suffix,
        chunkdb::GeometryConfig geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        },
        // Not empty: logins need user `bench` with this password.
        const std::string& bench_password = "") {
        data_dir = TempDataDir(suffix);
        port = PickFreePort();

        catalog = std::make_shared<chunkdb::TableCatalog>(chunkdb::CatalogConfigFromStoreConfig(chunkdb::StoreConfig{
            .geometry = geometry,
            .data_dir = data_dir,
            .durability_mode = chunkdb::DurabilityMode::kRelaxed,
            .checkpoint_update_interval = 512,
            .checkpoint_wal_bytes = 1024 * 1024,
            .wal_group_commit_updates = 8,
            .max_loaded_chunks = 4096,
            .allow_multiple_processes = false,
        }));
        (void)chunkdb::test::CreateBitsTable(*catalog, geometry);

        engine = std::make_shared<chunkdb::CommandEngine>(
            chunkdb::EngineConfig{
                .require_auth = !bench_password.empty(),
                .users = bench_password.empty() ? nullptr
                                                : chunkdb::test::MakeUsers(data_dir, "bench", bench_password),
                .max_auth_failures = 5,
            },
            catalog);

        server = std::make_unique<chunkdb::ChunkServer>(
            chunkdb::ServerConfig{
                .host = "127.0.0.1",
                .port = port,
                .max_line_bytes = 65536,
                .worker_threads = 2,
                // Give SCRAM proof work a fixed budget in every build.
                .client_io_timeout_ms = bench_password.empty() ? std::size_t{5000} : std::size_t{30000},
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
        catalog.reset();
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
        "--io-timeout-ms", "30000",
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
    assert(args.io_timeout_ms == 30000);
    assert(args.output_mode == chunkdb::server_bench::OutputMode::kJson);
    assert(args.tests.size() == 3);
    assert(args.tests[0] == chunkdb::server_bench::Scenario::kPing);
    assert(args.tests[1] == chunkdb::server_bench::Scenario::kSet);
    assert(args.tests[2] == chunkdb::server_bench::Scenario::kGet);
}

void TestIoTimeoutValidation() {
    assert(chunkdb::server_bench::ParseArgs({"bench"}).io_timeout_ms == 5000);
    assert(chunkdb::server_bench::ParseArgs({"bench", "--io-timeout-ms", "1"}).io_timeout_ms == 1);
    assert(chunkdb::server_bench::ParseArgs({"bench", "--io-timeout-ms", "86400000"}).io_timeout_ms == 86400000);
    for (const std::string value : {"0", "86400001", "-1", "invalid", "999999999999999999999999"}) {
        bool threw = false;
        try {
            (void)chunkdb::server_bench::ParseArgs({"bench", "--io-timeout-ms", value});
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw);
    }
    bool missing = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({"bench", "--io-timeout-ms"});
    } catch (const std::invalid_argument&) {
        missing = true;
    }
    assert(missing);
    for (const std::size_t value : {0U, 86400001U}) {
        auto args = chunkdb::server_bench::Args{};
        args.io_timeout_ms = value;
        bool threw = false;
        try {
            (void)chunkdb::server_bench::Run(args);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw);
    }
}

// A connected peer withholds HELLO's reply until the benchmark closes it.
// Named guards bound fixture failures; there are no sleep or latency assertions.
void TestBenchmarkReceiveTimeout() {
    ScopedSocketPlatform platform;
    (void)platform;
    struct SocketOwner {
        SocketHandle socket;
        ~SocketOwner() { CloseSocket(socket); }
    } listener{socket(AF_INET, SOCK_STREAM, 0)};
    assert(listener.socket != kInvalidSocket);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    assert(bind(listener.socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0);
    socklen_t length = sizeof(address);
    assert(getsockname(listener.socket, reinterpret_cast<sockaddr*>(&address), &length) == 0);
    assert(listen(listener.socket, 1) == 0);
    auto peer = std::async(std::launch::async, [&] {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(listener.socket, &readable);
        timeval guard{5, 0};
#ifdef _WIN32
        const int ready = select(0, &readable, nullptr, nullptr, &guard);
#else
        const int ready = select(listener.socket + 1, &readable, nullptr, nullptr, &guard);
#endif
        if (ready != 1) throw std::runtime_error("TestBenchmarkReceiveTimeout: waiting for connection");
        SocketOwner client{accept(listener.socket, nullptr, nullptr)};
        if (client.socket == kInvalidSocket) throw std::runtime_error("TestBenchmarkReceiveTimeout: accept failed");
#ifdef _WIN32
        const DWORD timeout = 5000;
#else
        const timeval timeout{5, 0};
#endif
        if (setsockopt(client.socket, SOL_SOCKET, SO_RCVTIMEO,
                reinterpret_cast<const char*>(&timeout), sizeof(timeout)) != 0) {
            throw std::runtime_error("TestBenchmarkReceiveTimeout: configuring peer guard");
        }
        std::string hello;
        char byte;
        while (hello.empty() || hello.back() != '\n') {
            if (recv(client.socket, &byte, 1, 0) != 1) {
                throw std::runtime_error("TestBenchmarkReceiveTimeout: waiting for HELLO");
            }
            hello += byte;
        }
        assert(hello == "HELLO 3\r\n");
        if (recv(client.socket, &byte, 1, 0) != 0) {
            throw std::runtime_error("TestBenchmarkReceiveTimeout: waiting for benchmark close");
        }
    });
    bool timed_out = false;
    try {
        (void)chunkdb::server_bench::Run(chunkdb::server_bench::Args{
            .port = ntohs(address.sin_port),
            .clients = 1,
            .requests = 1,
            .tests = {chunkdb::server_bench::Scenario::kPing},
            .log_level = chunkdb::LogLevel::kWarn,
            .io_timeout_ms = 50,
        });
    } catch (const std::runtime_error& error) {
        timed_out = std::string(error.what()) == "recv while reading line timed out";
    }
    peer.get();
    assert(timed_out);
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

void TestParseArgsUriPopulatesEndpointAndUser() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--uri", "chunk://bench:bench%40pw@bench.local:4321/",
    });

    assert(args.host == "bench.local");
    assert(args.port == 4321);
    assert(args.user == "bench");
    assert(args.password == "bench@pw");
}

void TestParseArgsExplicitFlagsOverrideUri() {
    const auto password_file = TempDataDir("password");
    {
        std::ofstream out(password_file);
        out << "flag-password\n";
    }
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--uri", "chunk://uri_user:uri-password@uri-host:1999/",
        "--host", "127.0.0.1",
        "--port", "4242",
        "--user", "flag_user",
        "--password-file", password_file.string(),
    });
    std::filesystem::remove(password_file);

    assert(args.host == "127.0.0.1");
    assert(args.port == 4242);
    assert(args.user == "flag_user");
    assert(args.password == "flag-password");

    // A password file that cannot be read is refused.
    bool threw = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--password-file", password_file.string(),
        });
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
}

void TestParseArgsChunksUriRejected() {
    bool threw = false;
    std::string message;
    try {
        (void)chunkdb::server_bench::ParseArgs({
            "chunkdb_server_bench",
            "--uri", "chunks://bench:secret@127.0.0.1:4242/",
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
    });

    const std::string human = chunkdb::server_bench::RenderHumanReport(report);
    assert(human.find("Throughput (req/s)") != std::string::npos);
    assert(human.find("Latency (ms)") != std::string::npos);
    assert(human.find("Percentile Distribution (ms)") != std::string::npos);
    assert(human.find("requested_clients=") != std::string::npos);
    assert(human.find("active_clients=") != std::string::npos);
    assert(human.find("io_timeout_ms=5000") != std::string::npos);

    const std::string json = chunkdb::server_bench::RenderJsonReport(report);
    assert(!json.empty());
    assert(json.front() == '{');
    assert(json.find("\"server_mode\"") != std::string::npos);
    assert(json.find("\"requested_clients\"") != std::string::npos);
    assert(json.find("\"active_clients\"") != std::string::npos);
    assert(json.find("\"io_timeout_ms\":5000") != std::string::npos);
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

// With a user the bench logs in with SCRAM-SHA-256 and checks the server's
// signature; a wrong password, or no user, ends the run.
void TestExternalModeLogsIn() {
    ExternalServerHarness harness(
        "login",
        chunkdb::GeometryConfig{
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = 16,
            .chunk_height_blocks = 16,
            .block_bits = 16,
        },
        "bench-password");
    auto args = chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kExternal,
        .host = "127.0.0.1",
        .port = harness.port,
        .clients = 2,
        .pipeline = 2,
        .requests = 40,
        .tests = {chunkdb::server_bench::Scenario::kPing},
        .keyspace = 64,
        .seed = 11,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .user = "bench",
        .password = "bench-password",
        .io_timeout_ms = 30000,
    };
    const auto report = chunkdb::server_bench::Run(args);
    assert(report.results.size() == 1);
    assert(report.results.front().completed_requests == 40);
    assert(report.io_timeout_ms == 30000);

    for (const auto& [user, password] : {std::pair<std::string, std::string>{"bench", "wrong"},
                                         std::pair<std::string, std::string>{"", ""}}) {
        args.user = user;
        args.password = password;
        bool threw = false;
        try {
            (void)chunkdb::server_bench::Run(args);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        assert(threw);
    }
}

// Spawn mode with a user starts a server that requires logins, with that
// user as its first administrator.
void TestSpawnModeWithUser() {
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kSpawn,
        .host = "127.0.0.1",
        .port = PickFreePort(),
        .clients = 2,
        .pipeline = 1,
        .requests = 60,
        .tests = {chunkdb::server_bench::Scenario::kSet},
        .keyspace = 64,
        .seed = 19,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .user = "bench",
        .password = "bench-password",
        .io_timeout_ms = 30000,
    });
    assert(report.spawned_server);
    assert(report.results.size() == 1);
    assert(report.results.front().completed_requests == 60);
    assert(report.io_timeout_ms == 30000);
}

void TestParseArgsGridScenariosAndDurability() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--tests", "world,canvas,simulation",
        "--durability-mode", "fsync-wal",
        "--server-workers", "16",
    });
    assert(args.tests.size() == 3);
    assert(args.tests[0] == chunkdb::server_bench::Scenario::kWorld);
    assert(args.tests[1] == chunkdb::server_bench::Scenario::kCanvas);
    assert(args.tests[2] == chunkdb::server_bench::Scenario::kSimulation);
    assert(args.durability_mode == "fsync-wal");
    assert(args.server_workers == 16);

    bool threw = false;
    try {
        (void)chunkdb::server_bench::ParseArgs({"chunkdb_server_bench", "--durability-mode", "fast"});
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);
}

// The grid scenarios fill their region and validate every reply, on a
// geometry whose chunk payload and presence bitmap both end mid-byte (3x3
// blocks of 5 bits), so chunk states of the wrong size would be refused.
void TestGridScenariosAgainstPaddedGeometry() {
    ExternalServerHarness harness(
        "grid",
        chunkdb::GeometryConfig{
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 3,
            .chunk_height_blocks = 3,
            .block_bits = 5,
        });
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kExternal,
        .host = "127.0.0.1",
        .port = harness.port,
        .clients = 3,
        .pipeline = 2,
        .requests = 600,
        .tests = {
            chunkdb::server_bench::Scenario::kWorld,
            chunkdb::server_bench::Scenario::kCanvas,
            chunkdb::server_bench::Scenario::kSimulation,
        },
        .keyspace = 12,
        .seed = 5,
        .output_mode = chunkdb::server_bench::OutputMode::kHuman,
        .log_level = chunkdb::LogLevel::kWarn,
        .watch_table = "default",
    });
    assert(report.results.size() == 3);
    assert(report.durability_mode.empty());
    for (const auto& result : report.results) {
        assert(result.completed_requests == 600);
    }
    assert(report.chunk_width_blocks == 3 && report.chunk_height_blocks == 3);
    const auto count = [](const auto& result, const std::string& kind) {
        for (const auto& [name, requests] : result.request_mix) if (name == kind) return requests;
        throw std::runtime_error("request kind missing");
    };
    const auto& world = report.results[0];
    assert(count(world, "get_area") == 12);
    assert(count(world, "set_chunk") == 12);
    assert(count(world, "get_block") == 120);
    assert(count(world, "set_block") == 456);
    assert(world.completed_blocks == 576 + (12 + world.returned_area_chunks) * 9);
    const auto& canvas = report.results[1];
    assert(count(canvas, "get_area") == 30 && count(canvas, "set_block") == 570);
    assert(canvas.returned_area_chunks == 30 * 16);
    assert(canvas.completed_blocks == 570 + 30 * 16 * 9);
    assert(report.results[2].completed_blocks == 600 * 9);
    assert(count(report.results[2], "get_chunk") == 300);
    assert(count(report.results[2], "set_chunk") == 300);
    assert(report.results[0].name == "world");
    assert(report.results[1].name == "canvas");
    assert(report.results[2].name == "simulation");
}

// Eleven requests split 6/5 between clients: each begins with its area read,
// so the observed mix is not the nominal long-run percentage. A 1x1 region
// clips each canvas viewport to one 3x3 chunk, rather than sixteen chunks.
void TestShortGridMixAndClippedAreas() {
    ExternalServerHarness harness("short-grid", chunkdb::GeometryConfig{
        .large_chunk_width_chunks = 2, .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 3, .chunk_height_blocks = 3, .block_bits = 5});
    auto args = chunkdb::server_bench::Args{};
    args.host = "127.0.0.1";
    args.port = harness.port;
    args.clients = 2;
    args.requests = 11;
    args.keyspace = 3;
    args.tests = {chunkdb::server_bench::Scenario::kCanvas, chunkdb::server_bench::Scenario::kSimulation};
    args.log_level = chunkdb::LogLevel::kWarn;
    const auto report = chunkdb::server_bench::Run(args);
    const auto& canvas = report.results[0];
    assert(canvas.returned_area_chunks == 2);
    assert(canvas.completed_blocks == 9 + 2 * 9);
    assert(canvas.blocks_per_request == 27.0 / 11.0);
    assert(canvas.request_mix[1].second == 9 && canvas.request_mix[5].second == 2);
    const auto& simulation = report.results[1];
    assert(simulation.completed_blocks == 11 * 9);
    assert(simulation.request_mix[3].second == 5 && simulation.request_mix[4].second == 6);
    assert(!report.source_commit.empty() && !report.build_type.empty());
    assert(report.benchmark_source_sha256.size() == 64);
    const auto json = chunkdb::server_bench::RenderJsonReport(report);
    assert(json.find("\"completed_blocks\":27") != std::string::npos);
    assert(json.find("\"request_mix\":{") != std::string::npos);
    assert(json.find("\"source_commit\":") != std::string::npos);
}

// The standalone chunk-read workload uses chunk coordinates over keyspace,
// without a prefill. Both unwritten NULLs and written forms count as reads.
void TestChunkReadsCoverNullAndForms() {
    ExternalServerHarness harness("chunk-null");
    chunkdb::SessionState session;
    assert(harness.engine->Execute(session, "HELLO 3\n").rfind("%14\r\n", 0) == 0);
    assert(harness.engine->Execute(session, "GET CHUNK 0 0 FROM default\n") == "_\r\n");
    const chunkdb::server_bench::Args args{
        .server_mode = chunkdb::server_bench::ServerMode::kExternal,
        .host = "127.0.0.1",
        .port = harness.port,
        .clients = 2,
        .pipeline = 3,
        .requests = 12,
        .tests = {chunkdb::server_bench::Scenario::kChunkGetState},
        .keyspace = 1,
        .seed = 5,
        .log_level = chunkdb::LogLevel::kWarn,
    };
    const auto empty = chunkdb::server_bench::Run(args);
    assert(empty.results.size() == 1);
    assert(empty.results[0].completed_requests == args.requests);
    assert(empty.results[0].completed_blocks == args.requests * 16 * 16);
    assert(empty.results[0].request_mix[4].second == args.requests);
    // Reads leave the chunk unwritten; no preparation writes are introduced.
    assert(harness.engine->Execute(session, "GET CHUNK 0 0 FROM default\n") == "_\r\n");
    assert(harness.engine->Execute(session, "SET BLOCK 0 0 IN default bits = b'0000000000000001'\n")[0] == ':');
    assert(harness.engine->Execute(session, "GET CHUNK 0 0 FROM default\n")[0] == '$');
    const auto written = chunkdb::server_bench::Run(args);
    assert(written.results.size() == 1);
    assert(written.results[0].completed_requests == args.requests);
    assert(written.results[0].completed_blocks == args.requests * 16 * 16);
    assert(written.results[0].request_mix[4].second == args.requests);
}

void TestSpawnModeReportsDurability() {
    const auto report = chunkdb::server_bench::Run(chunkdb::server_bench::Args{
        .server_mode = chunkdb::server_bench::ServerMode::kSpawn,
        .host = "127.0.0.1",
        .port = PickFreePort(),
        .clients = 2,
        .pipeline = 1,
        .requests = 120,
        .tests = {chunkdb::server_bench::Scenario::kWorld},
        .keyspace = 64,
        .seed = 3,
        .output_mode = chunkdb::server_bench::OutputMode::kJson,
        .log_level = chunkdb::LogLevel::kWarn,
        .durability_mode = "fsync-checkpoint",
    });
    assert(report.durability_mode == "fsync-checkpoint");
    assert(chunkdb::server_bench::RenderHumanReport(report).find("durability_mode=fsync-checkpoint") !=
           std::string::npos);
    assert(chunkdb::server_bench::RenderJsonReport(report).find("\"durability_mode\":\"fsync-checkpoint\"") !=
           std::string::npos);
}

}  // namespace

// Every scenario runs against a spawned server; the retired protocol 2
// scenarios are unknown.
void TestEveryScenario() {
    const auto args = chunkdb::server_bench::ParseArgs({
        "chunkdb_server_bench",
        "--tests", "ping,set,get,chunkgetstate,mixed,world,canvas,simulation",
    });
    for (const auto* retired : {"info", "chunkget"}) {
        bool threw = false;
        try {
            (void)chunkdb::server_bench::ParseArgs({"chunkdb_server_bench", "--tests", retired});
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw);
    }

    auto run = args;
    run.server_mode = chunkdb::server_bench::ServerMode::kSpawn;
    run.port = PickFreePort();
    run.clients = 2;
    run.pipeline = 4;
    run.requests = 300;
    run.keyspace = 64;
    run.log_level = chunkdb::LogLevel::kWarn;
    run.watch_table = "default";
    const auto report = chunkdb::server_bench::Run(run);
    assert(report.results.size() == 8);
    for (const auto& result : report.results) {
        assert(result.completed_requests == 300);
    }
    assert(report.chunk_lock_mode != "unknown");
}

void TestEnabledBenchmarkOptions() {
    for (const auto metadata : {"version", "schema", "both"}) {
        auto args = chunkdb::server_bench::ParseArgs({"bench", "--with", metadata,
            "--tests", "world,canvas,simulation"});
        args.server_mode = chunkdb::server_bench::ServerMode::kSpawn;
        args.port = PickFreePort(); args.clients = 1; args.requests = 20; args.keyspace = 16;
        args.log_level = chunkdb::LogLevel::kWarn;
        const auto report = chunkdb::server_bench::Run(args);
        assert(report.results.size() == 3U);
        assert(report.results[0].metadata_envelopes > 0U);
        assert(report.results[1].metadata_envelopes == (std::string(metadata) == "version" ? 0U : 1U));
        assert(report.results[2].metadata_envelopes == 0U); // chunk forms already carry metadata.
        for (const auto& result : report.results) assert(result.response_bytes > 0U);
        assert(chunkdb::server_bench::RenderJsonReport(report).find("enabled_config") != std::string::npos);
    }
    for (const auto mode : {"ordinary", "progress", "filtered", "chunks"}) {
        auto args = chunkdb::server_bench::ParseArgs({"bench", "--watch", "default", "--watch-mode", mode});
        args.server_mode = chunkdb::server_bench::ServerMode::kSpawn;
        args.port = PickFreePort(); args.clients = 1; args.requests = 10; args.keyspace = 16;
        args.tests = {chunkdb::server_bench::Scenario::kSimulation};
        args.log_level = chunkdb::LogLevel::kWarn;
        const auto report = chunkdb::server_bench::Run(args);
        const auto& result = report.results.front();
        assert(result.completed_requests == 10U);
        assert(result.watch_bytes >= 5U); // includes the validated UNWATCH completion.
        if (std::string(mode) == "progress") { assert(result.watch_progress > 0U); assert(result.watch_changes == 0U); }
        else if (std::string(mode) == "chunks") assert(result.watch_chunks > 0U);
        else assert(result.watch_changes > 0U);
    }
    auto args = chunkdb::server_bench::ParseArgs({"bench", "--clients", "1", "--tests", "takeover"});
    args.server_mode = chunkdb::server_bench::ServerMode::kSpawn;
    args.port = PickFreePort(); args.requests = 3; args.log_level = chunkdb::LogLevel::kWarn;
    const auto report = chunkdb::server_bench::Run(args);
    const auto& result = report.results.front();
    assert(result.ownership_cycles == 3U && result.completed_requests == 6U);
    assert(result.completed_blocks == 0U && result.response_bytes > 0U);
    assert(result.request_mix == (std::vector<std::pair<std::string, std::size_t>>{{"hello", 3}, {"watch_takeover", 3}}));
    assert(chunkdb::server_bench::RenderJsonReport(report).find("ownership_cycles_s") != std::string::npos);
    args.user = "bench"; args.password = "test-password"; args.port = PickFreePort(); args.requests = 1;
    const auto authenticated = chunkdb::server_bench::Run(args);
    assert(authenticated.results.front().ownership_cycles == 1U && authenticated.results.front().completed_requests == 3U);
    assert(authenticated.results.front().request_mix.back() == (std::pair<std::string, std::size_t>{"auth", 1U}));
    const auto ordinary = chunkdb::server_bench::RenderJsonReport(chunkdb::server_bench::BenchmarkReport{});
    assert(ordinary.find("enabled_config") == std::string::npos);
    assert(ordinary.find("watch_bytes") == std::string::npos);
    assert(ordinary.find("ownership_cycles") == std::string::npos);
    for (const auto& flags : {std::vector<std::string>{"bench", "--with", "wrong"},
                            {"bench", "--watch-mode", "wrong"}, {"bench", "--watch-mode", "chunks"},
                            {"bench", "--tests", "takeover"}}) {
        bool failed = false;
        try { (void)chunkdb::server_bench::ParseArgs(flags); }
        catch (const std::invalid_argument&) { failed = true; }
        assert(failed);
    }
}

// Synthetic wire peers exercise legacy/map start compatibility and negative
// frames through the public benchmark path, without a production test branch.
void TestScriptedWatch(const std::string& start, bool start_fails, bool run_fails,
                       bool takeover = false, const std::string& terminal = "") {
    ScopedSocketPlatform platform;
    (void)platform;
    const auto port = PickFreePort();
    struct Owner { SocketHandle fd; ~Owner() { CloseSocket(fd); } } listener{socket(AF_INET, SOCK_STREAM, 0)};
    assert(listener.fd != kInvalidSocket);
    sockaddr_in address{}; address.sin_family = AF_INET; address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    assert(bind(listener.fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0);
    assert(listen(listener.fd, 4) == 0);
    auto accept_peer = [&] {
        fd_set ready; FD_ZERO(&ready); FD_SET(listener.fd, &ready);
        timeval guard{3, 0};
#ifdef _WIN32
        const auto status = select(0, &ready, nullptr, nullptr, &guard);
#else
        const auto status = select(listener.fd + 1, &ready, nullptr, nullptr, &guard);
#endif
        if (status != 1) throw std::runtime_error("TestScriptedWatch: waited for peer accept");
        const auto fd = accept(listener.fd, nullptr, nullptr);
        if (fd == kInvalidSocket) throw std::runtime_error("TestScriptedWatch: accept failed");
#ifdef _WIN32
        const DWORD timeout = 3000;
#else
        const timeval timeout{3, 0};
#endif
        if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout)) != 0)
            throw std::runtime_error("TestScriptedWatch: receive timeout setup failed");
        return fd;
    };
    auto read = [](SocketHandle fd, std::string_view expected) {
        std::string line;
        for (;;) {
            char byte;
            if (recv(fd, &byte, 1, 0) != 1) throw std::runtime_error("TestScriptedWatch: waited for " + std::string(expected));
            line += byte; if (byte == '\n') break;
        }
        if (line != std::string(expected) + "\r\n") throw std::runtime_error("TestScriptedWatch: unexpected command " + line);
    };
    auto send_reply = [](SocketHandle fd, const std::string& bytes) {
        std::size_t offset = 0;
        while (offset != bytes.size()) {
            const auto sent = send(fd, bytes.data() + offset, static_cast<int>(bytes.size() - offset), 0);
            if (sent <= 0) throw std::runtime_error("TestScriptedWatch: reply send failed");
            offset += static_cast<std::size_t>(sent);
        }
    };
    const std::string hello = "%4\r\n$8\r\nprotocol\r\n:3\r\n$14\r\nserver_version\r\n$4\r\ntest\r\n"
        "$16\r\nserver_signature\r\n_\r\n$7\r\nunknown\r\n*3\r\n*-1\r\n$-1\r\n(184467440737095516160000\r\n";
    auto peer = std::async(std::launch::async, [&] {
        { Owner metadata{accept_peer()}; read(metadata.fd, "HELLO 3"); send_reply(metadata.fd, hello);
          read(metadata.fd, "DESCRIBE default"); send_reply(metadata.fd,
            "%3\r\n$7\r\nversion\r\n:1\r\n$5\r\nchunk\r\n*2\r\n:1\r\n:1\r\n$7\r\ncolumns\r\n*1\r\n"
            "%2\r\n$4\r\nname\r\n$4\r\nbits\r\n$4\r\ntype\r\n$7\r\nbits(8)\r\n"); }
        if (takeover) {
            const std::string epoch(32, 'a');
            const std::string initial = "%2\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$8\r\nrevision\r\n:0\r\n";
            Owner admin{accept_peer()}; read(admin.fd, "HELLO 3"); send_reply(admin.fd, hello);
            // The slot name is deliberately generated by the benchmark; parse
            // only this owned CREATE so later command identities remain exact.
            std::string create;
            for (;;) { char byte; if (recv(admin.fd, &byte, 1, 0) != 1) throw std::runtime_error("TestTakeover: waited for CREATE SLOT");
                create += byte; if (byte == '\n') break; }
            const auto quote = create.find('\''); const auto end = create.find('\'', quote + 1U);
            if (create.rfind("CREATE SLOT '", 0) != 0 || end == std::string::npos || create.substr(end) != "' ON default\r\n")
                throw std::runtime_error("TestTakeover: invalid slot creation");
            const auto slot = create.substr(quote + 1U, end - quote - 1U);
            send_reply(admin.fd, "+OK\r\n");
            Owner old{accept_peer()}; read(old.fd, "HELLO 3"); send_reply(old.fd, hello);
            read(old.fd, "WATCH default SLOT '" + slot + "'"); send_reply(old.fd, initial);
            read(admin.fd, "SHOW SLOTS ON default"); send_reply(admin.fd,
                "*1\r\n%3\r\n$4\r\nname\r\n$" + std::to_string(slot.size()) + "\r\n" + slot +
                "\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$5\r\nacked\r\n:0\r\n");
            Owner next{accept_peer()}; read(next.fd, "HELLO 3"); send_reply(next.fd, hello);
            read(next.fd, "WATCH default SLOT '" + slot + "' TAKEOVER"); send_reply(next.fd, start);
            if (!terminal.empty()) send_reply(old.fd, terminal);
            CloseSocket(old.fd); old.fd = kInvalidSocket;
            if (run_fails) return;
            read(next.fd, "UNWATCH"); send_reply(next.fd, "+OK\r\n");
            read(admin.fd, "DROP SLOT '" + slot + "' ON default"); send_reply(admin.fd, "+OK\r\n");
            return;
        }
        Owner watch{accept_peer()}; read(watch.fd, "HELLO 3"); send_reply(watch.fd, hello);
        read(watch.fd, "WATCH default"); send_reply(watch.fd, start);
        if (start_fails) return;
        { Owner worker{accept_peer()}; read(worker.fd, "HELLO 3"); send_reply(worker.fd, hello);
          read(worker.fd, "PING"); send_reply(worker.fd, "+PONG\r\n"); }
        read(watch.fd, "UNWATCH"); send_reply(watch.fd, "+OK\r\n");
    });
    auto args = chunkdb::server_bench::Args{};
    args.port = port; args.clients = 1; args.requests = 1; args.tests = {chunkdb::server_bench::Scenario::kPing};
    args.watch_table = takeover ? "" : "default"; args.log_level = chunkdb::LogLevel::kWarn;
    if (takeover) args.tests = {chunkdb::server_bench::Scenario::kTakeover};
    bool failed = false;
    try { const auto report = chunkdb::server_bench::Run(args); if (takeover) assert(report.results.front().ownership_cycles == 1U && report.results.front().completed_requests == 2U);
          else assert(report.results.front().watch_bytes >= 5U); }
    catch (const std::runtime_error&) { failed = true; }
    peer.get(); assert(failed == run_fails);
}
void TestCompatibleWatchReplies() {
    const std::string epoch(32, 'a');
    const std::string map = "%2\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$8\r\nrevision\r\n:0\r\n";
    TestScriptedWatch("+OK " + epoch + " 0\r\n", false, false);
    TestScriptedWatch(map, false, false);
    TestScriptedWatch(map, false, false, true);
    TestScriptedWatch(map, false, false, true, "-ERR KILLED reason=slot_takeover replaced\r\n");
    TestScriptedWatch(map, false, true, true, "-ERR KILLED reason=other replaced\r\n");
    TestScriptedWatch(map, false, true, true, ">3\r\n$8\r\nprogress\r\n$32\r\n" + epoch + "\r\n");
    TestScriptedWatch("%2\r\n$5\r\nepoch\r\n$32\r\n" + std::string(32, 'b') + "\r\n$8\r\nrevision\r\n:0\r\n", false, true, true);
    TestScriptedWatch("%2\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$8\r\nrevision\r\n:1\r\n", false, true, true);
    TestScriptedWatch(map + ">4\r\n$8\r\nprogress\r\n$32\r\n" + epoch + "\r\n:0\r\n(184467440737095516160000\r\n", false, false);
    TestScriptedWatch(map + ">2\r\n$7\r\nunknown\r\n%1\r\n$6\r\nfuture\r\n*2\r\n"
        "(184467440737095516160000\r\n*-1\r\n", false, false);
    TestScriptedWatch("%3\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$8\r\nrevision\r\n(18446744073709551615\r\n"
        "$7\r\nunknown\r\n*2\r\n*-1\r\n$-1\r\n", false, false);
    TestScriptedWatch("%2\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n$8\r\nrevision\r\n(18446744073709551616\r\n", true, true);
    TestScriptedWatch("*1048577\r\n", true, true);
    TestScriptedWatch("$67108865\r\n", true, true);
    TestScriptedWatch("%2\r\n$5\r\nepoch\r\n$32\r\n" + epoch.substr(0, 16), true, true);
    std::string deep;
    for (std::size_t i = 0; i < 66U; ++i) deep += "*1\r\n";
    TestScriptedWatch(deep + "_\r\n", true, true);
    TestScriptedWatch("+OK " + epoch + " 0\n", true, true);
    TestScriptedWatch("+OK " + epoch + " -1\r\n", true, true);
    TestScriptedWatch("+OK " + epoch + " 0 trailing\r\n", true, true);
    TestScriptedWatch("%1\r\n$5\r\nepoch\r\n$32\r\n" + epoch + "\r\n", true, true);
    TestScriptedWatch(map + ">3\r\n$8\r\nprogress\r\n$32\r\n" + std::string(32, 'b') + "\r\n:1\r\n", false, true);
    TestScriptedWatch(map + "-ERR KILLED reason=unexpected wrong terminal\r\n", false, true);
    TestScriptedWatch(map + ">3\r\n$8\r\nprogress\r\n$32\r\n" + epoch + "\r\n:-1\r\n", false, true);
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--chunk-null-read") {
        TestChunkReadsCoverNullAndForms();
        return 0;
    }
    TestParseArgsNewFlags();
    TestIoTimeoutValidation();
    TestBenchmarkReceiveTimeout();
    TestParseArgsInvalidCombination();
    TestParseArgsUriPopulatesEndpointAndUser();
    TestParseArgsExplicitFlagsOverrideUri();
    TestParseArgsChunksUriRejected();
    TestExternalModeDoesNotSpawn();
    TestExternalModeLogsIn();
    TestSpawnModeWithUser();
    TestSpawnModeStartsAndStops();
    TestOutputContainsPercentilesAndJsonFields();
    TestIdleClientsNoteWhenRequestsLessThanClients();
    TestParseArgsGridScenariosAndDurability();
    TestGridScenariosAgainstPaddedGeometry();
    TestChunkReadsCoverNullAndForms();
    TestSpawnModeReportsDurability();
    TestShortGridMixAndClippedAreas();
    TestEveryScenario();
    TestEnabledBenchmarkOptions();
    TestCompatibleWatchReplies();
    return 0;
}
