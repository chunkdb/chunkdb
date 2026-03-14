#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/server.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

#ifdef _WIN32
using SocketHandle = SOCKET;
using SocketLen = int;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
using SocketLen = socklen_t;
constexpr SocketHandle kInvalidSocket = -1;
#endif

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

bool IsWouldBlockError() {
#ifdef _WIN32
    const int code = WSAGetLastError();
    return code == WSAEWOULDBLOCK || code == WSAETIMEDOUT;
#else
    return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
}

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-server-it-" + suffix + "-" + std::to_string(tick));
}

void RemoveAllWithRetry(const std::filesystem::path& dir) {
    for (int attempt = 0; attempt < 25; ++attempt) {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        if (!std::filesystem::exists(dir)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

#ifdef _WIN32
class WinsockRuntime {
  public:
    WinsockRuntime() {
        WSADATA wsa_data{};
        const int rc = WSAStartup(MAKEWORD(2, 2), &wsa_data);
        if (rc != 0) {
            throw std::runtime_error("WSAStartup failed: " + std::to_string(rc));
        }
    }

    ~WinsockRuntime() { WSACleanup(); }

    WinsockRuntime(const WinsockRuntime&) = delete;
    WinsockRuntime& operator=(const WinsockRuntime&) = delete;
};

WinsockRuntime& EnsureWinsockRuntime() {
    static WinsockRuntime runtime;
    return runtime;
}
#endif

std::uint16_t PickFreePort() {
#ifdef _WIN32
    (void)EnsureWinsockRuntime();
#endif
    const SocketHandle s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == kInvalidSocket) {
        throw std::runtime_error("failed to create socket for free-port probe");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (bind(s, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0) {
        CloseSocket(s);
        throw std::runtime_error("failed to bind free-port probe socket");
    }

    SocketLen len = static_cast<SocketLen>(sizeof(addr));
    if (getsockname(s, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        CloseSocket(s);
        throw std::runtime_error("failed to read free-port probe socket name");
    }

    const std::uint16_t port = ntohs(addr.sin_port);
    CloseSocket(s);
    return port;
}

class RawClient {
  public:
    RawClient(std::string host, std::uint16_t port)
        : host_(std::move(host)), port_(port), socket_(Connect(host_, port_)) {}

    ~RawClient() {
        if (socket_ != kInvalidSocket) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
        }
    }

    RawClient(const RawClient&) = delete;
    RawClient& operator=(const RawClient&) = delete;

    void SendLine(const std::string& command) {
        SendBytes(command + "\r\n");
    }

    void SendBytes(const std::string& data) {
        std::size_t offset = 0;
        while (offset < data.size()) {
#ifdef _WIN32
            const int written = send(
                socket_,
                data.data() + static_cast<int>(offset),
                static_cast<int>(data.size() - offset),
                0);
#else
            const ssize_t written = send(
                socket_,
                data.data() + offset,
                data.size() - offset,
                0);
#endif
            if (written <= 0) {
                throw std::runtime_error("failed to send client bytes");
            }
            offset += static_cast<std::size_t>(written);
        }
    }

    std::string ReadLine() {
        auto extract = [&]() -> bool {
            const auto pos = pending_.find('\n');
            if (pos == std::string::npos) {
                return false;
            }
            line_cache_ = pending_.substr(0, pos + 1);
            pending_.erase(0, pos + 1);
            return true;
        };

        if (extract()) {
            return line_cache_;
        }

        char buffer[4096];
        while (true) {
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read == 0) {
                throw std::runtime_error("socket closed while waiting for line");
            }
            if (read < 0) {
                if (IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("recv failed while waiting for line");
            }

            pending_.append(buffer, static_cast<std::size_t>(read));
            if (extract()) {
                return line_cache_;
            }
        }
    }

    std::string ReadBulkText() {
        const std::string header = ReadLine();
        const std::size_t len = ParseBulkLength(header);
        const std::string payload = ReadExact(len);
        const std::string crlf = ReadExact(2);
        if (crlf != "\r\n") {
            throw std::runtime_error("invalid bulk text terminator");
        }
        return payload;
    }

    std::vector<std::uint8_t> ReadBulkBytes() {
        const std::string header = ReadLine();
        const std::size_t len = ParseBulkLength(header);
        const std::string payload = ReadExact(len);
        const std::string crlf = ReadExact(2);
        if (crlf != "\r\n") {
            throw std::runtime_error("invalid bulk bytes terminator");
        }
        return std::vector<std::uint8_t>(payload.begin(), payload.end());
    }

    bool WaitForClose(std::chrono::milliseconds timeout) {
        const auto deadline = Clock::now() + timeout;

        while (Clock::now() < deadline) {
            char buffer[256];
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read == 0) {
                return true;
            }
            if (read > 0) {
                pending_.append(buffer, static_cast<std::size_t>(read));
                continue;
            }
            if (IsWouldBlockError()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
                continue;
            }
            return true;
        }

        return false;
    }

  private:
    std::string host_;
    std::uint16_t port_ = 0;
    SocketHandle socket_ = kInvalidSocket;
    std::string pending_;
    std::string line_cache_;

    static SocketHandle Connect(const std::string& host, std::uint16_t port) {
#ifdef _WIN32
        (void)EnsureWinsockRuntime();
#endif
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        struct addrinfo* result = nullptr;
        const std::string port_text = std::to_string(port);
        if (getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result) != 0) {
            throw std::runtime_error("getaddrinfo failed");
        }

        SocketHandle socket = kInvalidSocket;
        for (auto* ai = result; ai != nullptr; ai = ai->ai_next) {
            socket = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (socket == kInvalidSocket) {
                continue;
            }

            SetSocketTimeouts(socket);

            if (::connect(socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                break;
            }

            CloseSocket(socket);
            socket = kInvalidSocket;
        }

        freeaddrinfo(result);
        if (socket == kInvalidSocket) {
            throw std::runtime_error("connect failed");
        }
        return socket;
    }

    static void SetSocketTimeouts(SocketHandle socket) {
#ifdef _WIN32
        const DWORD timeout_ms = 200;
        (void)setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout_ms), sizeof(timeout_ms));
        (void)setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout_ms), sizeof(timeout_ms));
#else
        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = 200000;
        (void)setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        (void)setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
    }

    std::string ReadExact(std::size_t size) {
        std::string out;
        out.reserve(size);

        while (out.size() < size) {
            if (!pending_.empty()) {
                const std::size_t take = std::min(size - out.size(), pending_.size());
                out.append(pending_.data(), take);
                pending_.erase(0, take);
                continue;
            }

            char buffer[4096];
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read <= 0) {
                if (read < 0 && IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("socket closed while reading exact payload");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }

        return out;
    }

    static std::size_t ParseBulkLength(const std::string& header) {
        std::string text = header;
        while (!text.empty() && (text.back() == '\r' || text.back() == '\n')) {
            text.pop_back();
        }
        if (text.empty() || text[0] != '$') {
            throw std::runtime_error("invalid bulk header");
        }

        std::size_t consumed = 0;
        const std::size_t len = std::stoull(text.substr(1), &consumed, 10);
        if (consumed != text.size() - 1) {
            throw std::runtime_error("invalid bulk length");
        }
        return len;
    }
};

struct ServerHarness {
    std::filesystem::path data_dir;
    std::shared_ptr<chunkdb::ChunkStore> store;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;

    ServerHarness(
        std::string name,
        chunkdb::StoreConfig store_config,
        chunkdb::EngineConfig engine_config,
        chunkdb::ServerConfig server_config)
        : data_dir(TempDataDir(std::move(name))),
          port(PickFreePort()) {
        store_config.data_dir = data_dir;
        server_config.host = "127.0.0.1";
        server_config.port = port;

        store = std::make_shared<chunkdb::ChunkStore>(store_config);
        engine = std::make_shared<chunkdb::CommandEngine>(engine_config, store);
        server = std::make_unique<chunkdb::ChunkServer>(server_config, engine);

        thread = std::thread([this]() {
            try {
                server->Run();
            } catch (...) {
                run_error = std::current_exception();
            }
        });

        WaitUntilListening();
    }

    ~ServerHarness() {
        if (server) {
            server->Stop();
        }
        if (thread.joinable()) {
            thread.join();
        }

        server.reset();
        engine.reset();
        store.reset();

        if (run_error) {
            try {
                std::rethrow_exception(run_error);
            } catch (...) {
                // avoid throwing from destructor
            }
        }

        RemoveAllWithRetry(data_dir);
    }

  private:
    std::exception_ptr run_error;

    void WaitUntilListening() {
        const auto deadline = Clock::now() + std::chrono::seconds(3);

        while (Clock::now() < deadline) {
            if (run_error) {
                std::rethrow_exception(run_error);
            }

            try {
                RawClient probe("127.0.0.1", port);
                return;
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }

        throw std::runtime_error("server did not start listening in time");
    }
};

chunkdb::StoreConfig BaseStoreConfig() {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 4,
        },
        .data_dir = "",
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 32,
        .checkpoint_wal_bytes = 4096,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
        .access_mode = chunkdb::AccessMode::kReadWrite,
    };
}

chunkdb::ServerConfig BaseServerConfig() {
    return chunkdb::ServerConfig{
        .host = "127.0.0.1",
        .port = 0,
        .max_line_bytes = 65536,
        .worker_threads = 2,
        .tls_enabled = false,
        .tls_cert_path = "",
        .tls_key_path = "",
    };
}

std::unordered_map<std::string, std::string> ParseInfoMap(const std::string& payload) {
    std::unordered_map<std::string, std::string> fields;
    std::istringstream in(payload);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        const auto sep = line.find('=');
        if (sep == std::string::npos) {
            continue;
        }
        fields.emplace(line.substr(0, sep), line.substr(sep + 1));
    }
    return fields;
}

void TestPing() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("ping", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestAuthAndSetGet() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "secret",
        .require_auth = true,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("auth", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("GET 0 0");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);

    client.SendLine("AUTH bad");
    assert(client.ReadLine().rfind("-ERR AUTH_FAILED", 0) == 0);

    client.SendLine("AUTH secret");
    assert(client.ReadLine() == "+OK\r\n");

    client.SendLine("SET 1 2 1111");
    assert(client.ReadLine() == "+OK\r\n");

    client.SendLine("GET 1 2");
    assert(client.ReadBulkText() == "1111");
}

void TestChunkAndChunkBinLengths() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunk-len", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    const auto& cfg = harness.store->geometry().config();
    const std::size_t expected_bits =
        static_cast<std::size_t>(cfg.chunk_width_blocks) *
        static_cast<std::size_t>(cfg.chunk_height_blocks) *
        static_cast<std::size_t>(cfg.block_bits);

    const std::size_t expected_bytes = (expected_bits + 7U) / 8U;

    client.SendLine("CHUNK 0 0");
    const std::string chunk_text = client.ReadBulkText();
    assert(chunk_text.size() == expected_bits);

    client.SendLine("CHUNKBIN 0 0");
    const auto chunk_bin = client.ReadBulkBytes();
    assert(chunk_bin.size() == expected_bytes);
}

void TestQuitClosesConnection() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("quit", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("QUIT");
    assert(client.ReadLine() == "+BYE\r\n");
    assert(client.WaitForClose(std::chrono::seconds(2)));
}

void TestMaxLineOverflowDisconnects() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.max_line_bytes = 32;

    ServerHarness harness("max-line", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("PING " + std::string(80, 'A'));
    const std::string response = client.ReadLine();
    assert(response.rfind("-ERR BAD_REQUEST", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(2)));
}

void TestMaxAuthFailuresDisconnects() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "secret",
        .require_auth = true,
        .max_auth_failures = 2,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("auth-fail-limit", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("AUTH no1");
    assert(client.ReadLine().rfind("-ERR AUTH_FAILED", 0) == 0);

    client.SendLine("AUTH no2");
    assert(client.ReadLine().rfind("-ERR AUTH_FAILED", 0) == 0);

    assert(client.WaitForClose(std::chrono::seconds(2)));
}

void TestInfoRuntimeCounters() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("info-counters", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("SET 0 0 1010");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("GET 0 0");
    assert(client.ReadBulkText() == "1010");

    client.SendLine("INFO");
    const auto info = ParseInfoMap(client.ReadBulkText());
    assert(info.contains("loaded_chunks"));
    assert(info.contains("evictions"));
    assert(info.contains("checkpoints"));
    assert(info.contains("wal_batch_flushes"));
    assert(info.contains("unique_loaded_chunks"));

    const auto loaded_chunks = std::stoull(info.at("loaded_chunks"));
    const auto unique_loaded_chunks = std::stoull(info.at("unique_loaded_chunks"));
    (void)std::stoull(info.at("evictions"));
    (void)std::stoull(info.at("checkpoints"));
    (void)std::stoull(info.at("wal_batch_flushes"));

    assert(loaded_chunks >= 1);
    assert(unique_loaded_chunks >= loaded_chunks);
}

}  // namespace

int main() {
#ifdef _WIN32
    (void)EnsureWinsockRuntime();
#endif
    TestPing();
    TestAuthAndSetGet();
    TestChunkAndChunkBinLengths();
    TestQuitClosesConnection();
    TestMaxLineOverflowDisconnects();
    TestMaxAuthFailuresDisconnects();
    TestInfoRuntimeCounters();
    return 0;
}
