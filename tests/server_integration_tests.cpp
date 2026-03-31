#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/lifecycle_log.hpp"
#include "chunkdb/logging.hpp"
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
    const auto wall_tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    const auto mono_tick = static_cast<unsigned long long>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto tid = static_cast<unsigned long long>(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return base / (
        "chunkdb-server-it-" + suffix + "-" + std::to_string(wall_tick) + "-" +
        std::to_string(mono_tick) + "-" + std::to_string(tid));
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

    bool ReadLineWithin(std::chrono::milliseconds timeout, std::string* out) {
        auto extract = [&]() -> bool {
            const auto pos = pending_.find('\n');
            if (pos == std::string::npos) {
                return false;
            }
            line_cache_ = pending_.substr(0, pos + 1);
            pending_.erase(0, pos + 1);
            *out = line_cache_;
            return true;
        };

        if (extract()) {
            return true;
        }

        const auto deadline = Clock::now() + timeout;
        char buffer[4096];
        while (Clock::now() < deadline) {
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read == 0) {
                return false;
            }
            if (read < 0) {
                if (IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("recv failed while waiting for line with deadline");
            }

            pending_.append(buffer, static_cast<std::size_t>(read));
            if (extract()) {
                return true;
            }
        }

        return false;
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

    void SetReceiveBuffer(std::size_t size) {
        const int value = static_cast<int>(size);
#ifdef _WIN32
        (void)setsockopt(
            socket_,
            SOL_SOCKET,
            SO_RCVBUF,
            reinterpret_cast<const char*>(&value),
            sizeof(value));
#else
        (void)setsockopt(socket_, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value));
#endif
    }

    bool ReadSomeWithin(
        std::chrono::milliseconds timeout,
        std::size_t max_bytes,
        std::size_t* out_bytes) {
        const auto deadline = Clock::now() + timeout;
        std::array<char, 4096> buffer{};
        const auto read_size = std::min(max_bytes, buffer.size());

        while (Clock::now() < deadline) {
#ifdef _WIN32
            const int read = recv(socket_, buffer.data(), static_cast<int>(read_size), 0);
#else
            const ssize_t read = recv(socket_, buffer.data(), read_size, 0);
#endif
            if (read == 0) {
                *out_bytes = 0;
                return true;
            }
            if (read < 0) {
                if (IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("recv failed while reading partial payload");
            }
            *out_bytes = static_cast<std::size_t>(read);
            return true;
        }

        return false;
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

class ScopedLogCapture {
  public:
    explicit ScopedLogCapture(chunkdb::LogLevel level)
        : previous_level_(chunkdb::GetLogLevel()) {
        chunkdb::SetLogLevel(level);
        chunkdb::SetLogSinkForTests([this](const std::string& line) {
            std::lock_guard lock(lines_mutex_);
            lines_.push_back(line);
            lines_cv_.notify_all();
        });
    }

    ~ScopedLogCapture() {
        chunkdb::ResetLogSinkForTests();
        chunkdb::SetLogLevel(previous_level_);
    }

    [[nodiscard]] bool Contains(std::string_view needle) const {
        std::lock_guard lock(lines_mutex_);
        for (const auto& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::size_t CountContains(std::string_view needle) const {
        std::lock_guard lock(lines_mutex_);
        std::size_t count = 0;
        for (const auto& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                ++count;
            }
        }
        return count;
    }

    [[nodiscard]] std::size_t IndexOf(std::string_view needle) const {
        std::lock_guard lock(lines_mutex_);
        return IndexOfLocked(needle);
    }

    [[nodiscard]] bool WaitContains(
        std::string_view needle,
        std::chrono::milliseconds timeout) const {
        std::unique_lock lock(lines_mutex_);
        if (ContainsLocked(needle)) {
            return true;
        }
        return lines_cv_.wait_for(lock, timeout, [&]() {
            return ContainsLocked(needle);
        });
    }

    [[nodiscard]] std::size_t WaitIndexOf(
        std::string_view needle,
        std::chrono::milliseconds timeout) const {
        std::unique_lock lock(lines_mutex_);
        if (IndexOfLocked(needle) != std::string::npos) {
            return IndexOfLocked(needle);
        }
        const bool ready = lines_cv_.wait_for(lock, timeout, [&]() {
            return IndexOfLocked(needle) != std::string::npos;
        });
        if (!ready) {
            return std::string::npos;
        }
        return IndexOfLocked(needle);
    }

  private:
    [[nodiscard]] bool ContainsLocked(std::string_view needle) const {
        for (const auto& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::size_t IndexOfLocked(std::string_view needle) const {
        for (std::size_t i = 0; i < lines_.size(); ++i) {
            if (lines_[i].find(needle) != std::string::npos) {
                return i;
            }
        }
        return std::string::npos;
    }

    chunkdb::LogLevel previous_level_;
    mutable std::mutex lines_mutex_;
    mutable std::condition_variable lines_cv_;
    std::vector<std::string> lines_;
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
        .client_io_timeout_ms = 5000,
        .idle_connection_timeout_ms = 60000,
        .max_pending_clients = 1024,
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

std::string ExpectedChunkLockMode() {
#if defined(__MINGW32__) && \
    (!defined(CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS) || CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS)
    return "serial-mutex";
#else
    return "shared-mutex";
#endif
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

void TestPipelinedCommandsSinglePacket() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("pipeline-single-packet", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    std::string payload;
    payload.reserve(220 * 6 + 64);
    for (int i = 0; i < 220; ++i) {
        payload += "PING\r\n";
    }
    payload += "SET 0 0 1010\r\n";
    payload += "GET 0 0\r\n";
    payload += "PING\r\n";
    client.SendBytes(payload);

    for (int i = 0; i < 220; ++i) {
        assert(client.ReadLine() == "+PONG\r\n");
    }
    assert(client.ReadLine() == "+OK\r\n");
    assert(client.ReadBulkText() == "1010");
    assert(client.ReadLine() == "+PONG\r\n");
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

void TestPipelinedBadRequestDisconnectPolicy() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.max_line_bytes = 32;

    ServerHarness harness("pipeline-bad-request", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    std::string payload;
    payload.reserve(160);
    payload += "PING\r\n";
    payload += "PING ";
    payload += std::string(80, 'X');
    payload += "\r\n";
    payload += "PING\r\n";
    client.SendBytes(payload);

    assert(client.ReadLine() == "+PONG\r\n");
    const std::string response = client.ReadLine();
    assert(response.rfind("-ERR BAD_REQUEST", 0) == 0);
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
    store_cfg.max_loaded_chunks = 8;
    store_cfg.wal_group_commit_updates = 64;
    store_cfg.checkpoint_update_interval = 10'000;
    store_cfg.checkpoint_wal_bytes = 10'000'000;
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("info-counters", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    for (int i = 0; i < 64; ++i) {
        client.SendLine(
            "SET " + std::to_string(i * static_cast<int>(store_cfg.geometry.chunk_width_blocks)) + " 0 1010");
        assert(client.ReadLine() == "+OK\r\n");
    }
    client.SendLine("GET 0 0");
    assert(client.ReadBulkText() == "1010");

    client.SendLine("INFO");
    const auto info_first = ParseInfoMap(client.ReadBulkText());
    assert(info_first.contains("loaded_chunks"));
    assert(info_first.contains("evictions"));
    assert(info_first.contains("checkpoints"));
    assert(info_first.contains("wal_batch_flushes"));
    assert(info_first.contains("unique_loaded_chunks"));
    assert(info_first.contains("open_wal_streams"));
    assert(info_first.contains("eviction_snapshot_builds"));
    assert(info_first.contains("eviction_probes"));
    assert(info_first.contains("eviction_no_progress_cycles"));
    assert(info_first.contains("eviction_forced_wal_flushes"));
    assert(info_first.contains("eviction_forced_wal_flushes_with_data"));
    assert(info_first.contains("eviction_forced_wal_flushes_empty_batch"));
    assert(info_first.contains("chunk_lock_mode"));

    const auto loaded_chunks_first = std::stoull(info_first.at("loaded_chunks"));
    const auto unique_loaded_chunks_first = std::stoull(info_first.at("unique_loaded_chunks"));
    const auto evictions_first = std::stoull(info_first.at("evictions"));
    const auto checkpoints_first = std::stoull(info_first.at("checkpoints"));
    const auto wal_batch_flushes_first = std::stoull(info_first.at("wal_batch_flushes"));
    const auto open_wal_streams_first = std::stoull(info_first.at("open_wal_streams"));
    const auto snapshot_builds_first = std::stoull(info_first.at("eviction_snapshot_builds"));
    const auto probes_first = std::stoull(info_first.at("eviction_probes"));
    const auto no_progress_first = std::stoull(info_first.at("eviction_no_progress_cycles"));
    const auto forced_flushes_first = std::stoull(info_first.at("eviction_forced_wal_flushes"));
    const auto forced_flushes_with_data_first =
        std::stoull(info_first.at("eviction_forced_wal_flushes_with_data"));
    const auto forced_flushes_empty_first =
        std::stoull(info_first.at("eviction_forced_wal_flushes_empty_batch"));
    assert(info_first.at("chunk_lock_mode") == ExpectedChunkLockMode());

    assert(loaded_chunks_first >= 1);
    assert(unique_loaded_chunks_first >= loaded_chunks_first);
    assert(evictions_first > 0);
    assert(probes_first >= evictions_first);
    assert(forced_flushes_first > 0);
    assert(forced_flushes_first == forced_flushes_with_data_first + forced_flushes_empty_first);

    for (int i = 64; i < 96; ++i) {
        client.SendLine(
            "SET " + std::to_string(i * static_cast<int>(store_cfg.geometry.chunk_width_blocks)) + " 0 0101");
        assert(client.ReadLine() == "+OK\r\n");
    }

    client.SendLine("INFO");
    const auto info_second = ParseInfoMap(client.ReadBulkText());

    assert(std::stoull(info_second.at("evictions")) >= evictions_first);
    assert(std::stoull(info_second.at("checkpoints")) >= checkpoints_first);
    assert(std::stoull(info_second.at("wal_batch_flushes")) >= wal_batch_flushes_first);
    assert(std::stoull(info_second.at("open_wal_streams")) >= open_wal_streams_first);
    assert(std::stoull(info_second.at("eviction_snapshot_builds")) >= snapshot_builds_first);
    assert(std::stoull(info_second.at("eviction_probes")) >= probes_first);
    assert(std::stoull(info_second.at("eviction_no_progress_cycles")) >= no_progress_first);
    assert(std::stoull(info_second.at("eviction_forced_wal_flushes")) >= forced_flushes_first);
    const auto forced_with_data_second =
        std::stoull(info_second.at("eviction_forced_wal_flushes_with_data"));
    const auto forced_empty_second =
        std::stoull(info_second.at("eviction_forced_wal_flushes_empty_batch"));
    const auto forced_total_second = std::stoull(info_second.at("eviction_forced_wal_flushes"));
    assert(forced_total_second == forced_with_data_second + forced_empty_second);
    assert(forced_with_data_second >= forced_flushes_with_data_first);
    assert(forced_empty_second >= forced_flushes_empty_first);
}

void TestSlowClientTimeoutReleasesWorker() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;

    ServerHarness harness("slow-client-timeout", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("PING");

    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("PING");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "+PONG\r\n");
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
}

void TestReadTimeoutLogsPhaseAndReason() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;

    ServerHarness harness("slow-client-timeout-log", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("PING");

    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=read"));
    assert(logs.Contains("reason=timeout"));
    assert(logs.CountContains("connection terminated") == 1);
}

void TestSlowRequestDribbleDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 250;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("slow-request-dribble-deadline", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("P");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));
    stalled.SendBytes("I");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("PING");

    stalled.SendBytes("N");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "+PONG\r\n");
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=read"));
    assert(logs.Contains("reason=timeout"));
}

void TestIdleClientRemainsConnectedBetweenCommands() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("idle-client-kept-alive", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");

    std::this_thread::sleep_for(std::chrono::milliseconds(350));

    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestLongIdleConnectionTimeoutReleasesWorker() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.idle_connection_timeout_ms = 150;

    ServerHarness harness("long-idle-timeout", store_cfg, engine_cfg, server_cfg);
    RawClient idle("127.0.0.1", harness.port);

    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("PING");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "+PONG\r\n");
    assert(idle.WaitForClose(std::chrono::milliseconds(1500)));
}

void TestSlowResponseDrainDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    store_cfg.geometry.chunk_width_blocks = 512;
    store_cfg.geometry.chunk_height_blocks = 512;
    store_cfg.geometry.block_bits = 32;

    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 250;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("slow-response-drain-deadline", store_cfg, engine_cfg, server_cfg);
    RawClient slow("127.0.0.1", harness.port);
    slow.SetReceiveBuffer(1024);
    slow.SendLine("CHUNK 0 0");

    std::size_t bytes_read = 0;
    assert(slow.ReadSomeWithin(std::chrono::milliseconds(1000), 512, &bytes_read));
    assert(bytes_read > 0);

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("PING");

    for (int i = 0; i < 4; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        assert(slow.ReadSomeWithin(std::chrono::milliseconds(500), 512, &bytes_read));
        if (bytes_read == 0) {
            break;
        }
    }

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(2000), &response));
    assert(response == "+PONG\r\n");
    assert(slow.WaitForClose(std::chrono::milliseconds(2000)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=write"));
    assert(logs.Contains("reason=timeout"));
}

void TestIdlePeerCloseDoesNotLogTerminationWarning() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("idle-peer-close-no-log", store_cfg, engine_cfg, server_cfg);
    {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine("PING");
        assert(client.ReadLine() == "+PONG\r\n");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    assert(!logs.Contains("connection terminated"));
}

void TestPendingQueueSaturationRejectsNewConnections() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 2000;
    server_cfg.max_pending_clients = 1;

    ServerHarness harness("pending-queue-saturation", store_cfg, engine_cfg, server_cfg);

    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("PING");
    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    RawClient queued("127.0.0.1", harness.port);
    RawClient rejected1("127.0.0.1", harness.port);
    RawClient rejected2("127.0.0.1", harness.port);

    rejected1.SendLine("PING");
    rejected2.SendLine("PING");
    assert(rejected1.WaitForClose(std::chrono::milliseconds(800)));
    assert(rejected2.WaitForClose(std::chrono::milliseconds(800)));

    assert(logs.WaitContains(
        "pending client queue full; rejecting new connections",
        std::chrono::seconds(2)));
    assert(logs.CountContains("pending client queue full; rejecting new connections") == 1);

    stalled.SendLine("");
    assert(stalled.ReadLine() == "+PONG\r\n");
    stalled.SendLine("QUIT");
    assert(stalled.ReadLine() == "+BYE\r\n");
    assert(stalled.WaitForClose(std::chrono::milliseconds(800)));

    queued.SendLine("PING");
    assert(queued.ReadLine() == "+PONG\r\n");
}

void TestReadinessLogLineExists() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("log-readiness", store_cfg, engine_cfg, server_cfg);
    assert(logs.WaitContains(" INFO server pid=", std::chrono::seconds(2)));
    assert(logs.WaitContains("ready to accept connections", std::chrono::seconds(2)));
    assert(logs.WaitContains("protocol=tcp", std::chrono::seconds(2)));
}

void TestWarnLineOnBadRequest() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.max_line_bytes = 32;

    ServerHarness harness("log-warn", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("PING " + std::string(80, 'A'));
    const std::string response = client.ReadLine();
    assert(response.rfind("-ERR BAD_REQUEST", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(2)));
    assert(logs.Contains(" WARN server pid="));
    assert(logs.Contains("bad request disconnect"));
}

void TestErrorLineOnListenFailure() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.host = "host name with spaces is invalid";
    const std::filesystem::path data_dir = TempDataDir("log-error-listen");
    store_cfg.data_dir = data_dir;

    auto store = std::make_shared<chunkdb::ChunkStore>(store_cfg);
    auto engine = std::make_shared<chunkdb::CommandEngine>(engine_cfg, store);
    auto server = std::make_unique<chunkdb::ChunkServer>(server_cfg, engine);

    bool failed = false;
    try {
        server->Run();
    } catch (...) {
        failed = true;
    }

    server.reset();
    engine.reset();
    store.reset();
    RemoveAllWithRetry(data_dir);

    assert(failed);
    assert(logs.Contains(" ERROR server pid="));
    assert(logs.Contains("server run loop failed"));
}

void TestLogLevelFilteringWarn() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.max_line_bytes = 32;

    ServerHarness harness("log-filter", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("PING " + std::string(80, 'A'));
    const std::string response = client.ReadLine();
    assert(response.rfind("-ERR BAD_REQUEST", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(2)));

    assert(!logs.Contains("ready to accept connections"));
    assert(logs.Contains(" WARN server pid="));
}

void TestLogLevelFilteringError() {
    ScopedLogCapture logs(chunkdb::LogLevel::kError);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.host = "host name with spaces is invalid";
    const std::filesystem::path data_dir = TempDataDir("log-filter-error");
    store_cfg.data_dir = data_dir;

    auto store = std::make_shared<chunkdb::ChunkStore>(store_cfg);
    auto engine = std::make_shared<chunkdb::CommandEngine>(engine_cfg, store);
    auto server = std::make_unique<chunkdb::ChunkServer>(server_cfg, engine);

    try {
        server->Run();
    } catch (...) {
    }

    server.reset();
    engine.reset();
    store.reset();
    RemoveAllWithRetry(data_dir);

    assert(logs.Contains(" ERROR server pid="));
    assert(!logs.Contains(" WARN "));
    assert(!logs.Contains(" INFO "));
}

void TestStartupLogOrder() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    const std::filesystem::path data_dir = TempDataDir("log-order");
    store_cfg.data_dir = data_dir;
    server_cfg.port = PickFreePort();

    chunkdb::LogServerStartupContext(
        "test-version",
        "test-build",
        server_cfg,
        store_cfg);

    ServerHarness harness("log-order", store_cfg, engine_cfg, server_cfg);

    const auto i_starting = logs.WaitIndexOf(" server starting ", std::chrono::seconds(2));
    const auto i_config = logs.WaitIndexOf(" effective config ", std::chrono::seconds(2));
    const auto i_recovery = logs.WaitIndexOf(" startup recovery summary ", std::chrono::seconds(2));
    const auto i_store = logs.WaitIndexOf(" store initialized ", std::chrono::seconds(2));
    const auto i_ready = logs.WaitIndexOf(" ready to accept connections ", std::chrono::seconds(2));

    assert(i_starting != std::string::npos);
    assert(i_config != std::string::npos);
    assert(i_recovery != std::string::npos);
    assert(i_store != std::string::npos);
    assert(i_ready != std::string::npos);

    assert(i_starting < i_config);
    assert(i_config < i_recovery);
    assert(i_recovery < i_store);
    assert(i_store < i_ready);

    assert(logs.Contains("wal_group_commit_updates=1"));
    assert(logs.Contains("max_loaded_chunks=128"));
    assert(logs.Contains("client_io_timeout_ms=5000"));
    assert(logs.Contains("idle_connection_timeout_ms=60000"));
    assert(logs.Contains("max_pending_clients=1024"));
}

}  // namespace

int main() {
#ifdef _WIN32
    (void)EnsureWinsockRuntime();
#endif
    TestPing();
    TestAuthAndSetGet();
    TestChunkAndChunkBinLengths();
    TestPipelinedCommandsSinglePacket();
    TestQuitClosesConnection();
    TestMaxLineOverflowDisconnects();
    TestPipelinedBadRequestDisconnectPolicy();
    TestMaxAuthFailuresDisconnects();
    TestInfoRuntimeCounters();
    TestSlowClientTimeoutReleasesWorker();
    TestReadTimeoutLogsPhaseAndReason();
    TestSlowRequestDribbleDeadlineReleasesWorker();
    TestIdleClientRemainsConnectedBetweenCommands();
    TestLongIdleConnectionTimeoutReleasesWorker();
    TestSlowResponseDrainDeadlineReleasesWorker();
    TestIdlePeerCloseDoesNotLogTerminationWarning();
    TestPendingQueueSaturationRejectsNewConnections();
    TestReadinessLogLineExists();
    TestWarnLineOnBadRequest();
    TestErrorLineOnListenFailure();
    TestLogLevelFilteringWarn();
    TestLogLevelFilteringError();
    TestStartupLogOrder();
    return 0;
}
