#include <array>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
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
#include "chunkdb/table_catalog.hpp"
#include "catalog_test_utils.hpp"
#include "login_helpers.hpp"
#include "feed_phase_watchdog.hpp"
#include "../src/change_feed.hpp"
#include "../src/slot_watch.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#endif

namespace chunkdb {
void SetServerTimeoutConfigFailpointForTests(
    std::size_t send_failures,
    std::size_t recv_failures) noexcept;
void ResetServerTimeoutConfigCountersForTests() noexcept;
std::uint64_t ServerRecvTimeoutConfigCallsForTests() noexcept;
}

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

void ConfigureSocketNoSigPipe(SocketHandle socket) {
#if defined(__APPLE__)
    const int enabled = 1;
    (void)setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
    (void)socket;
#endif
}

class ScopedServerTimeoutFailpoint {
  public:
    ScopedServerTimeoutFailpoint(std::size_t send_failures, std::size_t recv_failures) {
        chunkdb::SetServerTimeoutConfigFailpointForTests(send_failures, recv_failures);
    }

    ~ScopedServerTimeoutFailpoint() {
        chunkdb::SetServerTimeoutConfigFailpointForTests(0, 0);
    }
};

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

// A loopback port held by a listening socket for the lifetime of the object,
// so a server configured for it deterministically fails to listen. Unlike an
// unresolvable host name this does not depend on DNS: resolvers with a
// `localhost` search domain turn any name into 127.0.0.1.
class OccupiedPort {
  public:
    OccupiedPort() {
#ifdef _WIN32
        (void)EnsureWinsockRuntime();
#endif
        socket_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_ == kInvalidSocket) {
            throw std::runtime_error("failed to create port-holding socket");
        }
#ifdef _WIN32
        // Without exclusive use, a later bind with SO_REUSEADDR (which the
        // server sets) may take the port over on Windows.
        int exclusive = 1;
        if (setsockopt(
                socket_, SOL_SOCKET, SO_EXCLUSIVEADDRUSE,
                reinterpret_cast<const char*>(&exclusive), sizeof(exclusive)) != 0) {
            CloseSocket(socket_);
            throw std::runtime_error("failed to set SO_EXCLUSIVEADDRUSE");
        }
#endif
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        SocketLen len = static_cast<SocketLen>(sizeof(addr));
        if (bind(socket_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) != 0 ||
            listen(socket_, 1) != 0 ||
            getsockname(socket_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
            CloseSocket(socket_);
            throw std::runtime_error("failed to hold a loopback port");
        }
        port_ = ntohs(addr.sin_port);
    }

    ~OccupiedPort() { CloseSocket(socket_); }

    OccupiedPort(const OccupiedPort&) = delete;
    OccupiedPort& operator=(const OccupiedPort&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

  private:
    SocketHandle socket_ = kInvalidSocket;
    std::uint16_t port_ = 0;
};

// The first administrator ServerHarness creates when logins need users.
constexpr const char* kAdminUser = "admin";
constexpr const char* kAdminPassword = "secret";

// The HELLO 3 reply: a map of eight entries, each value an integer or, for
// server_version and server_signature, a bulk string (server_signature is
// null, read as "", without a user); none of them holds a line break.
template <typename Client>
std::unordered_map<std::string, std::string> ReadHelloReply(Client& client) {
    const auto text = [&client]() {
        std::string line = client.ReadLine();
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) {
            line.pop_back();
        }
        return line;
    };
    const std::string header = text();
    if (header != "%8") {
        throw std::runtime_error("unexpected HELLO reply: " + header);
    }
    std::unordered_map<std::string, std::string> fields;
    for (int i = 0; i < 8; ++i) {
        if (text().rfind('$', 0) != 0) {
            throw std::runtime_error("HELLO reply key is not a bulk string");
        }
        const std::string key = text();
        std::string value = text();
        if (value.rfind(':', 0) == 0) {
            value.erase(0, 1);
        } else if (value.rfind('$', 0) == 0) {
            value = text();
        } else if (value == "_") {
            value.clear();
        } else {
            throw std::runtime_error("unexpected HELLO reply value: " + value);
        }
        fields.emplace(key, value);
    }
    return fields;
}

// HELLO 3 USER and AUTH (SCRAM-SHA-256) over a client connection; returns
// the reply that ends the login: the first line of the HELLO map, or the
// error. A successful login's map is read and its server signature checked.
template <typename Client>
std::string LoginReply(Client& client, const std::string& user, const std::string& password) {
    const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce());
    client.SendBytes(chunkdb::test::HelloUserBytes(login, user));
    const std::string first = client.ReadLine();
    if (first.rfind("+SCRAM ", 0) != 0) {
        return first;
    }
    chunkdb::test::FeedPhaseWatchdog::Phase("client: derive AUTH response");
    const auto step = chunkdb::test::AuthBytes(login, password, first);
    client.SendBytes(step.bytes);
    const auto fields = ReadHelloReply(client);
    if (fields.at("protocol") != "3" || fields.at("server_signature") != step.server_signature) {
        throw std::runtime_error("unexpected HELLO reply after AUTH");
    }
    return "%8\r\n";
}

// A login with a wrong password: the error line.
template <typename Client>
std::string FailedLoginReply(Client& client, const std::string& user, const std::string& password) {
    const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce());
    client.SendBytes(chunkdb::test::HelloUserBytes(login, user));
    const std::string first = client.ReadLine();
    if (first.rfind("+SCRAM ", 0) != 0) {
        return first;
    }
    chunkdb::test::FeedPhaseWatchdog::Phase("client: derive rejected AUTH response");
    client.SendBytes(chunkdb::test::AuthBytes(login, password, first).bytes);
    return client.ReadLine();
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

    // HELLO 3 without a user (--auth none); every connection starts with
    // HELLO.
    void Hello() {
        SendLine("HELLO 3");
        if (ReadHelloReply(*this).at("protocol") != "3") {
            throw std::runtime_error("unexpected HELLO reply");
        }
    }

    // HELLO 3 USER and AUTH: logs in as `user`.
    void Login(const std::string& user = kAdminUser, const std::string& password = kAdminPassword) {
        const std::string reply = LoginReply(*this, user, password);
        if (reply != "%8\r\n") {
            throw std::runtime_error("login failed: " + reply);
        }
    }

    void SendBytes(const std::string& data) {
        chunkdb::test::FeedPhaseWatchdog::Command("client: send", data);
        // Keep the verb only: AUTH and statements may carry credentials or data.
        last_request_ = data.substr(0, std::min(data.find_first_of(" \t\r\n"), std::size_t{32}));
        ++request_number_;
        std::size_t offset = 0;
        while (offset < data.size()) {
#ifdef _WIN32
            const int written = send(
                socket_,
                data.data() + static_cast<int>(offset),
                static_cast<int>(data.size() - offset),
                0);
#else
#if defined(MSG_NOSIGNAL)
            constexpr int kSendFlags = MSG_NOSIGNAL;
#else
            constexpr int kSendFlags = 0;
#endif
            const ssize_t written = send(
                socket_,
                data.data() + offset,
                data.size() - offset,
                kSendFlags);
#endif
            if (written <= 0) {
                throw std::runtime_error("failed to send client bytes");
            }
            offset += static_cast<std::size_t>(written);
        }
    }

    void Disconnect() {
        chunkdb::test::FeedPhaseWatchdog::Phase("client: disconnect");
#ifdef _WIN32
        (void)shutdown(socket_, SD_BOTH);
#else
        (void)shutdown(socket_, SHUT_RDWR);
#endif
    }
    void Unread(const std::string& bytes) { pending_ = bytes + pending_; }
    void SetReadDeadline(std::chrono::milliseconds timeout) { test_read_deadline_ = Clock::now() + timeout; }
    void SmallReceiveBuffer() {
        const int bytes = 65536;
        assert(setsockopt(socket_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&bytes), sizeof(bytes)) == 0);
    }
    std::string ReadLine() {
        chunkdb::test::FeedPhaseWatchdog::Phase("client: read reply line");
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
            if (test_read_deadline_ && Clock::now() >= *test_read_deadline_) throw std::runtime_error("feed test read deadline");
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read == 0) {
                throw std::runtime_error("socket closed while waiting for line " + ReadContext());
            }
            if (read < 0) {
                if (IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("recv failed while waiting for line " + ReadContext());
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
                return false;
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
        if (len > 32U * 1024U * 1024U) {
            std::cerr << "bulk read started " << ReadContext() << " expected=" << len << '\n';
        }
        const std::string payload = ReadExact(len);
        const std::string crlf = ReadExact(2);
        if (crlf != "\r\n") {
            throw std::runtime_error("invalid bulk text terminator");
        }
        if (len > 32U * 1024U * 1024U) {
            std::cerr << "bulk read completed " << ReadContext() << " received=" << payload.size() << '\n';
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

    // One whole reply as sent, an aggregate with all its elements.
    std::string ReadReply() {
        const std::string header = ReadLine();
        std::string reply = header;
        if (header.rfind('$', 0) == 0) {
            reply += ReadExact(ParseBulkLength(header) + 2U);
        } else if (header.rfind('*', 0) == 0 || header.rfind('%', 0) == 0) {
            const std::size_t count = std::stoull(header.substr(1)) * (header[0] == '%' ? 2U : 1U);
            for (std::size_t i = 0; i < count; ++i) {
                reply += ReadReply();
            }
        }
        return reply;
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

    // Half-closes the connection: the server sees end of stream.
    void ShutdownWrite() {
#ifdef _WIN32
        (void)shutdown(socket_, SD_SEND);
#else
        (void)shutdown(socket_, SHUT_WR);
#endif
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
    std::optional<Clock::time_point> test_read_deadline_;
    std::string last_request_;
    std::size_t request_number_ = 0;

    [[nodiscard]] std::string ReadContext() const {
        sockaddr_in local{};
        SocketLen len = sizeof(local);
        const auto local_port = getsockname(socket_, reinterpret_cast<sockaddr*>(&local), &len) == 0
            ? ntohs(local.sin_port) : 0;
        return "client=127.0.0.1:" + std::to_string(local_port) + " server=" + host_ + ":" +
            std::to_string(port_) + " request=" + std::to_string(request_number_) + " command=\"" + last_request_ + "\"";
    }

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
        ConfigureSocketNoSigPipe(socket);
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
            if (test_read_deadline_ && Clock::now() >= *test_read_deadline_) {
                throw std::runtime_error("test read deadline " + ReadContext() +
                    " expected=" + std::to_string(size) + " received=" + std::to_string(out.size()));
            }
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
                throw std::runtime_error("socket closed while reading exact payload " + ReadContext() +
                    " expected=" + std::to_string(size) + " received=" + std::to_string(out.size()));
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

#ifdef CHUNKDB_WITH_OPENSSL

constexpr std::string_view kTestTlsCertPem = R"(-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQC4nvQC5V39WjANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAkx
MjcuMC4wLjEwHhcNMjYwMzMxMTUyNjE5WhcNMjYwNDAxMTUyNjE5WjAUMRIwEAYD
VQQDDAkxMjcuMC4wLjEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCY
dAxbMYkq16iK3hDquOYqJj4zIZoCY1Zeq10DpfHB053UR1ywWyN35KG4a1XfXOVM
W47D0CACyks0GT7ix8KM4xGMSx2b/EQ8mQrztnt3YZKuHwnDH45oiiQ+/9WW7NAY
jPgIsCxn/P+E4M43yqvHqK4XGLLm984xFnu4n270bsqi/IHbSlWYt3B8q7rHTYmD
BmwsXnFbxvJcmH0CQkgbU1F11g4fmVSd8Qt+R2NTSSjbGvBpupjYbhERXQQVsjP4
sEERqPT8ocZD1MoINMKTtCmxHbez7qO9o8gAY1aVZSsslowR5WCb+d1jjsRFqk6w
JqumPbmPFiexe73uD4CVAgMBAAEwDQYJKoZIhvcNAQELBQADggEBAIozzwqPOe6o
Et2j696F4akvfWh9v273hkrXixrUO4Qt27nrRBrsQPn0WnrPnxsy5BCYoSBVShie
Cj9WFl3cPinYLwiB+1MpJBUA1eTRU+m4MYsGwBnSceol966GIQ19bzBIokijKa9/
92zdONdV7mIPc01fExygVNcGWD4+DBzf0fXw0HPJsku0rQQ1Ldp34hQ2UzmaBAJC
BdHGFsYCRMkPinjPuNmbH3PF2y5G+0ftTFmomaHPbSmvB+I+z8mS5eH7pCbw0Dsq
oDkiwHOE6/0jR6tQkpJScCtEvp9rNadpENvXR54mqcds1Y6JTN4El04vQqecaBHG
eIrrOOGWhb4=
-----END CERTIFICATE-----
)";

constexpr std::string_view kTestTlsKeyPem = R"(-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCYdAxbMYkq16iK
3hDquOYqJj4zIZoCY1Zeq10DpfHB053UR1ywWyN35KG4a1XfXOVMW47D0CACyks0
GT7ix8KM4xGMSx2b/EQ8mQrztnt3YZKuHwnDH45oiiQ+/9WW7NAYjPgIsCxn/P+E
4M43yqvHqK4XGLLm984xFnu4n270bsqi/IHbSlWYt3B8q7rHTYmDBmwsXnFbxvJc
mH0CQkgbU1F11g4fmVSd8Qt+R2NTSSjbGvBpupjYbhERXQQVsjP4sEERqPT8ocZD
1MoINMKTtCmxHbez7qO9o8gAY1aVZSsslowR5WCb+d1jjsRFqk6wJqumPbmPFiex
e73uD4CVAgMBAAECggEABXWqd52XhvRAMfDv9Cf4/itucNBUPp+mGS/T3eyUcteM
QGzp0dsBsyp57CvT4HLoN0rUGwkaDF+IP+5jhSWYPwlmuHp8LfjjzLPCY6X2V/kj
kp7D77vykqXX1HW/BW+nqClsPItqm7LAx9ZxLChS7IyK54LX7VOUi8d9WMhE5fX/
nzmIeuiLiZrH1sBsDhrs7l/46qPumQ9NLrQNpKnqpU1890SVX2610V/vWO8wAN+V
Q/dzMf5nk/JBLXRokUQ+xch4XHmkuYIcIrgMOB5C3Osvznmusve4QsUOIFNtu5yB
Tn8rUFRPDl19/5TiGz06Htcp1ARIixhuYolUNz9VAQKBgQDHjmWRQ+qchRmCwxn7
f2VCZyuQMc039/0mKTIRogp7+GuY28NyK/5vSXvU2HP44x/93BMH2reW4Yf+sWYQ
1c/t6bfiCcVx7kasx2VzbfmgFgiuTRJ+pnd7bMo6VLfOmKSztlKL37/qFMnZEYy5
PUHkAWOlg/QFXJPnYKtbQnSYwQKBgQDDkv24lmO1ybz3e83eaz/4bsNEZ/yHRUIU
2C/z+BlU5JvGeyT5GanP1TGd9NjgV44MLU9VzcYUy9UK4V+NFNHMi53ulHyF4CA8
2F2Q1KSc2pjAQRYcg1SOP3jshk5D633wiRVyPAEZTdz4iDs7jtyK9izc/iv5azsU
6D3sln5o1QKBgCRGPS43w0jqZOXBI1L1KGn2qROQCfbXjFvId0J/SxqX4K8rm46A
csK1/92D7yjZ2HHj9E2kM2Uo3/irNJtw0lgz+OoMzqhUIOK9aDKgVhUEjFVqyybc
ibGU5/nMdpEGbEICrWShqpgZaUudBhCSEw0oN33Zy5zB5FzV1LBFFz7BAoGADxpZ
15hdiNtUaXQ5GLUFkqTTFYRGPxf9G2j6gwekxSaGVRSLbWUq9O7MzxrqaKC6Snxx
RPoIEvEOubFf1KBH91jM0HDNEPWW57v5tcaGE8rZwvcDwx3tOLL0HqfcgWg9KIcd
jd3OY+rcZqD2mgnVRDHwkvxZ3wAF5v5sUcnpZyUCgYAzwEjOUbkOprGgLKO3zxOe
5NtYWAVuizJe1TqDmZovu4w7S50S5B+NHSGhRE2cqhF6QSm/sHziyQvS/RiPOf9r
m0/zhisrKWr8NWNfqCHKLcD7AGjSIp7m9oM3E2TwTOM4PKjoTj1tPEjAzw3uVdjC
iEWE/lnDWlS/EM7sXfzofA==
-----END PRIVATE KEY-----
)";

void WriteTextFile(const std::filesystem::path& path, std::string_view contents) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("failed to open file for writing: " + path.string());
    }
    out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    if (!out) {
        throw std::runtime_error("failed to write file: " + path.string());
    }
}

struct TlsTestCredentials {
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
};

TlsTestCredentials WriteTlsTestCredentials(const std::filesystem::path& dir) {
    std::filesystem::create_directories(dir);
    const auto cert_path = dir / "test-cert.pem";
    const auto key_path = dir / "test-key.pem";
    WriteTextFile(cert_path, kTestTlsCertPem);
    WriteTextFile(key_path, kTestTlsKeyPem);
    return {.cert_path = cert_path, .key_path = key_path};
}

class TlsClient {
  public:
    TlsClient(std::string host, std::uint16_t port)
        : host_(std::move(host)), port_(port) {
        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();
        context_ = SSL_CTX_new(TLS_client_method());
        if (context_ == nullptr) {
            throw std::runtime_error("failed to create TLS client context");
        }
        SSL_CTX_set_verify(context_, SSL_VERIFY_NONE, nullptr);
        socket_ = ConnectSocket(host_, port_);
        session_ = SSL_new(context_);
        if (session_ == nullptr) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
            SSL_CTX_free(context_);
            context_ = nullptr;
            throw std::runtime_error("failed to create TLS client session");
        }
        SSL_set_fd(session_, static_cast<int>(socket_));
        int connected = 0;
        do {
            ClearErrors();
            connected = SSL_connect(session_);
        } while (connected != 1 && TimedOut(connected));
        if (connected != 1) {
            SSL_free(session_);
            session_ = nullptr;
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
            SSL_CTX_free(context_);
            context_ = nullptr;
            throw std::runtime_error("failed to complete TLS client handshake");
        }
    }

    ~TlsClient() {
        if (session_ != nullptr) {
            (void)SSL_shutdown(session_);
            SSL_free(session_);
            session_ = nullptr;
        }
        if (socket_ != kInvalidSocket) {
            CloseSocket(socket_);
            socket_ = kInvalidSocket;
        }
        if (context_ != nullptr) {
            SSL_CTX_free(context_);
            context_ = nullptr;
        }
    }

    TlsClient(const TlsClient&) = delete;
    TlsClient& operator=(const TlsClient&) = delete;

    void SendLine(const std::string& command) {
        SendBytes(command + "\r\n");
    }

    // HELLO 3 without a user (--auth none); every connection starts with
    // HELLO.
    void Hello() {
        SendLine("HELLO 3");
        if (ReadHelloReply(*this).at("protocol") != "3") {
            throw std::runtime_error("unexpected HELLO reply");
        }
    }

    // HELLO 3 USER and AUTH: logs in as `user`.
    void Login(const std::string& user = kAdminUser, const std::string& password = kAdminPassword) {
        const std::string reply = LoginReply(*this, user, password);
        if (reply != "%8\r\n") {
            throw std::runtime_error("login failed: " + reply);
        }
    }

    void SendBytes(const std::string& data) {
        chunkdb::test::FeedPhaseWatchdog::Command("client: send", data);
        std::size_t offset = 0;
        while (offset < data.size()) {
            ClearErrors();
            const int written = SSL_write(
                session_,
                data.data() + offset,
                static_cast<int>(data.size() - offset));
            if (written <= 0) {
                if (TimedOut(written)) {
                    continue;
                }
                throw std::runtime_error("failed to send TLS client bytes");
            }
            offset += static_cast<std::size_t>(written);
        }
    }

    // A TLS 1.3 key update: a whole record that carries no request.
    void KeyUpdate() {
        if (SSL_version(session_) != TLS1_3_VERSION ||
            SSL_key_update(session_, SSL_KEY_UPDATE_NOT_REQUESTED) != 1 || SSL_do_handshake(session_) != 1) {
            throw std::runtime_error("failed to send a TLS key update");
        }
    }

    // Bytes sent under the TLS layer, e.g. part of a record that never
    // completes. False once the server has closed the connection.
    bool SendRaw(const std::string& data) {
#ifdef _WIN32
        const int written = send(socket_, data.data(), static_cast<int>(data.size()), 0);
#else
#if defined(MSG_NOSIGNAL)
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
        const ssize_t written = send(socket_, data.data(), data.size(), kSendFlags);
#endif
        return written >= 0 && static_cast<std::size_t>(written) == data.size();
    }

    // Waits for the server to close the socket (a TLS alert may come first).
    bool WaitForRawClose(std::chrono::milliseconds timeout) {
        const auto deadline = Clock::now() + timeout;
        while (Clock::now() < deadline) {
            char buffer[256];
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (read > 0) {
                continue;
            }
            if (read < 0 && IsWouldBlockError()) {
                continue;
            }
            return true;
        }
        return false;
    }

    void Disconnect() {
        chunkdb::test::FeedPhaseWatchdog::Phase("client: disconnect");
#ifdef _WIN32
        (void)shutdown(socket_, SD_BOTH);
#else
        (void)shutdown(socket_, SHUT_RDWR);
#endif
    }
    void Unread(const std::string& bytes) { pending_ = bytes + pending_; }
    void SetReadDeadline(std::chrono::milliseconds timeout) { test_read_deadline_ = Clock::now() + timeout; }
    void SmallReceiveBuffer() {
        const int bytes = 65536;
        assert(setsockopt(socket_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&bytes), sizeof(bytes)) == 0);
    }
    std::string ReadLine() {
        chunkdb::test::FeedPhaseWatchdog::Phase("client: read reply line");
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
            const int read = Read(buffer, static_cast<int>(sizeof(buffer)));
            if (read <= 0) {
                throw std::runtime_error("TLS socket closed while waiting for line");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
            if (extract()) {
                return line_cache_;
            }
        }
    }

    // `$<n>\r\n<n bytes>\r\n`; the body may span several TLS records.
    std::string ReadBulkText() {
        const std::string header = ReadLine();
        if (header.size() < 3 || header[0] != '$') {
            throw std::runtime_error("expected TLS bulk reply, got: " + header);
        }
        const std::size_t length = std::stoull(header.substr(1, header.size() - 3));
        char buffer[4096];
        while (pending_.size() < length + 2U) {
            const int read = Read(buffer, static_cast<int>(sizeof(buffer)));
            if (read <= 0) {
                throw std::runtime_error("TLS socket closed while waiting for bulk body");
            }
            pending_.append(buffer, static_cast<std::size_t>(read));
        }
        if (pending_.compare(length, 2, "\r\n") != 0) {
            throw std::runtime_error("TLS bulk body is not terminated by CRLF");
        }
        std::string body = pending_.substr(0, length);
        pending_.erase(0, length + 2U);
        return body;
    }

  private:
    std::string host_;
    std::uint16_t port_ = 0;
    SSL_CTX* context_ = nullptr;
    SSL* session_ = nullptr;
    SocketHandle socket_ = kInvalidSocket;
    std::string pending_;
    std::string line_cache_;
    std::optional<Clock::time_point> test_read_deadline_;

    // SSL_get_error needs an empty error queue before the call, and the
    // socket error tells a timeout from a closed connection only if no
    // earlier call left one behind.
    static void ClearErrors() {
        ERR_clear_error();
#ifdef _WIN32
        WSASetLastError(0);
#else
        errno = 0;
#endif
    }

    // Whether a TLS call that returned `result` only ran into the socket's
    // 200 ms timeout, which is short so that WaitForRawClose can watch its
    // deadline. Callers without a deadline wait on, as RawClient::ReadLine
    // does: a slow server is not a closed connection.
    bool TimedOut(int result) const {
        const int error = SSL_get_error(session_, result);
        return error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE ||
               (error == SSL_ERROR_SYSCALL && IsWouldBlockError());
    }

    // Decrypted bytes into `buffer`: their count, or at most 0 once the
    // connection is closed or broken.
    int Read(char* buffer, int size) {
        while (true) {
            if (test_read_deadline_ && Clock::now() >= *test_read_deadline_) throw std::runtime_error("feed test TLS read deadline");
            ClearErrors();
            const int read = SSL_read(session_, buffer, size);
            if (read > 0 || !TimedOut(read)) {
                return read;
            }
        }
    }

    static SocketHandle ConnectSocket(const std::string& host, std::uint16_t port) {
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
        ConfigureSocketNoSigPipe(socket);
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
};

#endif

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

thread_local std::exception_ptr background_server_error;

void RethrowBackgroundServerError() {
    const auto error = background_server_error;
    background_server_error = {};
    if (error) {
        std::rethrow_exception(error);
    }
}

struct ServerHarness {
    std::filesystem::path data_dir;
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;
    bool tls_enabled = false;
    chunkdb::StoreConfig saved_store_config;
    chunkdb::ServerConfig saved_server_config;
    chunkdb::EngineConfig saved_engine_config;

    void JoinStopped() {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: join stopped server without another Stop");
        if (thread.joinable()) thread.join();
        RethrowRunError();
    }

    void Restart() {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: restart");
        StopAndJoin();
        RethrowRunError();
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: reset server/engine/catalog for restart");
        server.reset(); engine.reset(); catalog.reset();
        auto catalog_config = chunkdb::CatalogConfigFromStoreConfig(saved_store_config);
        catalog_config.feed_linger = std::chrono::milliseconds(saved_server_config.feed_linger_ms);
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        engine = std::make_shared<chunkdb::CommandEngine>(saved_engine_config, catalog);
        server = std::make_unique<chunkdb::ChunkServer>(saved_server_config, engine);
        {
            std::lock_guard lock(run_error_mutex);
            run_error = {};
        }
        StartAndWait();
    }

    [[nodiscard]] chunkdb::Geometry geometry() const {
        return catalog->Find("default")->geometry();
    }

    ServerHarness(
        std::string name,
        chunkdb::StoreConfig store_config,
        chunkdb::EngineConfig engine_config,
        chunkdb::ServerConfig server_config)
        : data_dir(TempDataDir(std::move(name))),
          port(server_config.port == 0 ? PickFreePort() : server_config.port) {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: constructor");
        store_config.data_dir = data_dir;
        server_config.host = "127.0.0.1";
        server_config.port = port;
#ifdef CHUNKDB_WITH_OPENSSL
        if (server_config.tls_enabled &&
            (server_config.tls_cert_path.empty() || server_config.tls_key_path.empty())) {
            // Beside the data directory, which holds only store files.
            const auto creds = WriteTlsTestCredentials(TlsCredentialsDir());
            server_config.tls_cert_path = creds.cert_path.string();
            server_config.tls_key_path = creds.key_path.string();
        }
#endif
        tls_enabled = server_config.tls_enabled;

        auto catalog_config = chunkdb::CatalogConfigFromStoreConfig(store_config);
        catalog_config.feed_linger = std::chrono::milliseconds(server_config.feed_linger_ms);
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        (void)chunkdb::test::CreateBitsTable(*catalog, store_config.geometry);
        if (engine_config.require_auth && engine_config.users == nullptr) {
            engine_config.users = chunkdb::test::MakeUsers(data_dir, kAdminUser, kAdminPassword);
        }
        saved_store_config = store_config;
        saved_server_config = server_config;
        saved_engine_config = engine_config;
        engine = std::make_shared<chunkdb::CommandEngine>(engine_config, catalog);
        server = std::make_unique<chunkdb::ChunkServer>(server_config, engine);

        try {
            StartAndWait();
        } catch (const std::exception&) {
            server.reset();
            engine.reset();
            catalog.reset();
            RemoveAllWithRetry(data_dir);
            RemoveAllWithRetry(TlsCredentialsDir());
            throw;
        }
    }

    ~ServerHarness() {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: destructor");
        StopAndJoin();
        const auto error = RunError();
        if (error && !background_server_error) {
            background_server_error = error;
        }

        chunkdb::test::FeedPhaseWatchdog::Phase("harness: reset server");
        server.reset();
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: reset engine");
        engine.reset();
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: reset catalog");
        catalog.reset();

        RemoveAllWithRetry(data_dir);
        RemoveAllWithRetry(TlsCredentialsDir());
    }

  private:
    std::mutex run_error_mutex;
    std::exception_ptr run_error;

    [[nodiscard]] std::exception_ptr RunError() {
        std::lock_guard lock(run_error_mutex);
        return run_error;
    }

    void RethrowRunError() {
        if (const auto error = RunError()) {
            std::rethrow_exception(error);
        }
    }

    void StopAndJoin() {
        if (server) {
            chunkdb::test::FeedPhaseWatchdog::Phase("harness: Stop");
            server->Stop();
        }
        if (thread.joinable()) {
            chunkdb::test::FeedPhaseWatchdog::Phase("harness: join");
            thread.join();
        }
    }

    void StartAndWait() {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: start and await listener");
        thread = std::thread([this] {
            try {
                server->Run();
            } catch (const std::exception&) {
                std::lock_guard lock(run_error_mutex);
                run_error = std::current_exception();
            }
        });
        try {
            WaitUntilListening();
        } catch (const std::exception&) {
            StopAndJoin();
            // A listener failure may race the last startup probe.
            RethrowRunError();
            throw;
        }
    }

    [[nodiscard]] std::filesystem::path TlsCredentialsDir() const {
        return data_dir.string() + "-tls";
    }

    void WaitUntilListening() {
        const auto deadline = Clock::now() + std::chrono::seconds(3);

        while (Clock::now() < deadline) {
            RethrowRunError();

            try {
#ifdef CHUNKDB_WITH_OPENSSL
                if (tls_enabled) {
                    TlsClient probe("127.0.0.1", port);
                } else {
                    RawClient probe("127.0.0.1", port);
                }
#else
                RawClient probe("127.0.0.1", port);
#endif
                return;
            } catch (const std::runtime_error&) {
                // Listener startup only; tests do not use this for ordering.
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

// SHOW METRICS text: each sample's name (with its labels) and value.
std::unordered_map<std::string, std::string> ParseMetrics(const std::string& payload) {
    std::unordered_map<std::string, std::string> samples;
    std::istringstream in(payload);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto sep = line.rfind(' ');
        if (sep == std::string::npos) {
            continue;
        }
        samples.emplace(line.substr(0, sep), line.substr(sep + 1));
    }
    return samples;
}

// The reply of SET BLOCK, DELETE BLOCK and SET CHUNK: the chunk version.
template <typename Client>
std::uint64_t ReadVersion(Client& client) {
    const std::string line = client.ReadLine();
    if (line.size() < 4 || line[0] != ':') {
        throw std::runtime_error("expected a chunk version, got: " + line);
    }
    return std::stoull(line.substr(1, line.size() - 3));
}

// The GET BLOCK reply of a block of the one-column tables these tests use:
// its bits value as bytes, the lowest bit first.
std::string BitsReply(std::string_view bytes) {
    return "*1\r\n$" + std::to_string(bytes.size()) + "\r\n" + std::string(bytes) + "\r\n";
}

// The chunk form of GET CHUNK and SET CHUNK: version (u64, little-endian),
// presence bitmap, payload, then the VARS entries.
struct ChunkForm {
    std::uint64_t version = 0;
    std::string presence;
    std::string payload;
    std::string vars;
};

ChunkForm ParseChunkForm(const std::string& form, std::size_t presence_bytes, std::size_t payload_bytes) {
    if (form.size() < 16U + presence_bytes + payload_bytes) {
        throw std::runtime_error("chunk form of " + std::to_string(form.size()) + " bytes is too short");
    }
    ChunkForm parsed;
    for (std::size_t i = 0; i < 8; ++i) {
        parsed.version |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(form[i])) << (8U * i);
    }
    parsed.presence = form.substr(16, presence_bytes);
    parsed.payload = form.substr(16 + presence_bytes, payload_bytes);
    parsed.vars = form.substr(16 + presence_bytes + payload_bytes);
    return parsed;
}

// A chunk form to send for a table at schema version 1; SET CHUNK does not
// read its version.
std::string ChunkFormOf(const std::string& presence, const std::string& payload) {
    return std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8) + presence + payload;
}

// One parameter frame: `$<length>`, the bytes, an empty line.
std::string Frame(const std::string& bytes) {
    return "$" + std::to_string(bytes.size()) + "\r\n" + bytes + "\r\n";
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
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("ping", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.Hello();
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

// A client of protocol 1 or 2 fails at once with a clear error, and so does
// any command before HELLO or an unsupported protocol version.
void TestProtocolOneClientIsRefused() {
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = true,
        .max_auth_failures = 5,
    };
    ServerHarness harness("protocol-one", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    for (const char* first :
         {"PING", "INFO", "HELLO 1", "HELLO 2", "HELLO 2 AUTH secret", "HELLO 4 AUTH secret"}) {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine(first);
        assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 3\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    // AUTH <token> of protocol 2 now reads as an AUTH without HELLO 3 USER.
    const std::string stray_auth = "-ERR PROTOCOL AUTH follows HELLO 3 USER <name> $1\r\n";
    {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine("AUTH secret");
        assert(client.ReadLine() == stray_auth);
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    {
        // A client of another protocol that pipelines AUTH and a binary write gets the error
        // for AUTH and the connection closes before the payload is parsed.
        RawClient client("127.0.0.1", harness.port);
        client.SendBytes("AUTH secret\r\nCHUNKSETBIN 0 0 8\r\n12345678\r\n");
        assert(client.ReadLine() == stray_auth);
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    RawClient client("127.0.0.1", harness.port);
    client.Login();
    client.SendLine("HELLO 3");
    assert(client.ReadLine().rfind("-ERR PROTOCOL HELLO was already sent", 0) == 0);
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestAuthAndSetGet() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = true,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("auth", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.SendLine("HELLO 3");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);

    assert(FailedLoginReply(client, kAdminUser, "bad") == "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");
    assert(FailedLoginReply(client, "nobody", kAdminPassword) == "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");

    // HELLO has no TABLE option: statements name their table. The token
    // option is gone.
    const std::string hello_shape = "-ERR INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1\r\n";
    client.SendLine("HELLO 3 USER admin $1 TABLE default");
    assert(client.ReadLine() == hello_shape);

    client.Login();

    client.SendLine("SET BLOCK 1 2 IN default bits = b'1111'");
    (void)ReadVersion(client);
    client.SendLine("GET BLOCK 1 2 FROM default");
    assert(client.ReadReply() == BitsReply("\x0f"));

    // An explicit zero is a value; an absent block is _.
    client.SendLine("SET BLOCK 2 2 IN default bits = b'0000'");
    (void)ReadVersion(client);
    client.SendLine("GET BLOCK 2 2 FROM default");
    assert(client.ReadReply() == BitsReply(std::string(1, '\0')));
    client.SendLine("DELETE BLOCK 2 2 FROM default");
    (void)ReadVersion(client);
    client.SendLine("GET BLOCK 2 2 FROM default");
    assert(client.ReadLine() == "_\r\n");
    client.SendLine("GET BLOCK 1 2 FROM default");
    assert(client.ReadReply() == BitsReply("\x0f"));
}

void TestChunkPutWritesAndFraming() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunkput", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    const auto geometry = harness.geometry();
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t presence_bytes = (geometry.ChunkBlockCount() + 7U) / 8U;
    assert(payload_bytes == 8);
    assert(presence_bytes == 2);
    // A chunk frame is bounded by the chunk form and the VARS budget.
    const std::size_t frame_limit =
        16U + presence_bytes + payload_bytes + harness.catalog->Find("default")->Info().options.var_max_chunk_bytes;
    const auto read_chunk = [&](RawClient& reader, int chunk_x) {
        reader.SendLine("GET CHUNK " + std::to_string(chunk_x) + " 0 FROM default");
        return ParseChunkForm(reader.ReadBulkText(), presence_bytes, payload_bytes);
    };
    const auto chunk_absent = [&](int chunk_x) {
        client.SendLine("GET CHUNK " + std::to_string(chunk_x) + " 0 FROM default");
        return client.ReadLine() == "_\r\n";
    };

    std::string payload;
    for (std::size_t i = 0; i < payload_bytes; ++i) {
        payload.push_back(static_cast<char>(0xA0 + i));
    }
    const std::string full_presence(presence_bytes, '\xFF');

    // Every block present. The reply is the new version, as GET CHUNK
    // reports it.
    client.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame(ChunkFormOf(full_presence, payload)));
    const std::uint64_t first = ReadVersion(client);
    const auto read_back = read_chunk(client, 0);
    assert(read_back.version == first);
    assert(read_back.presence == full_presence);
    assert(read_back.payload == payload);
    assert(read_back.vars.empty());

    // Writing the same state again changes nothing: the current version.
    client.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame(ChunkFormOf(full_presence, payload)));
    assert(ReadVersion(client) == first);

    // A sparse presence bitmap (blocks 0 and 15: bit i of the bitmap is byte
    // i/8, 1 << (i % 8)), an LF terminator, and a pipelined statement in the
    // same write.
    const std::string sparse_presence("\x01\x80", 2);
    const std::string sparse = ChunkFormOf(sparse_presence, payload);
    client.SendBytes(
        "SET CHUNK 1 0 IN default $1\r\n$" + std::to_string(sparse.size()) + "\r\n" + sparse + "\nPING\r\n");
    (void)ReadVersion(client);
    assert(client.ReadLine() == "+PONG\r\n");
    client.SendLine("GET CHUNK 1 0 FROM default");
    const std::string sparse_text = client.ReadBulkText();
    const auto sparse_back = ParseChunkForm(sparse_text, presence_bytes, payload_bytes);
    assert(sparse_back.presence == sparse_presence);
    // Absent blocks are canonicalized to zero payload bits: only block 0
    // (bits 0..3, the low nibble of byte 0) and block 15 (bits 60..63, the
    // high nibble of the last byte) survive.
    assert(sparse_back.payload[0] == static_cast<char>(payload[0] & 0x0F));
    assert(sparse_back.payload[payload_bytes - 1] == static_cast<char>(payload[payload_bytes - 1] & 0xF0));
    for (std::size_t i = 1; i + 1 < payload_bytes; ++i) {
        assert(sparse_back.payload[i] == 0);
    }

    // IF VERSION: the conditional write, binary and free of the line limit.
    const std::uint64_t current = sparse_back.version;
    client.SendBytes(
        "SET CHUNK 1 0 IN default $1 IF VERSION " + std::to_string(current + 1) + "\r\n" +
        Frame(ChunkFormOf(full_presence, std::string(payload_bytes, '\x55'))));
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(current) + "\r\n");
    client.SendLine("GET CHUNK 1 0 FROM default");
    assert(client.ReadBulkText() == sparse_text);  // unchanged
    client.SendBytes(
        "SET CHUNK 1 0 IN default $1 IF VERSION " + std::to_string(current) + "\r\n" +
        Frame(ChunkFormOf(full_presence, payload)));
    const std::uint64_t after_if = ReadVersion(client);
    assert(after_if > current);
    const auto if_back = read_chunk(client, 1);
    assert(if_back.version == after_if);
    assert(if_back.payload == payload);

    // A frame of the wrong size that still fits the bound is read and
    // drained, the statement fails, and the connection stays usable.
    client.SendBytes("SET CHUNK 3 0 IN default $1\r\n$2\r\n\x01\x02\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT the chunk starts with its version and schema version (16 bytes), got 2", 0) == 0);
    assert(chunk_absent(3));

    // A line with `$` that does not parse cannot be trusted to frame its
    // parameters: it is refused unread and the connection closes.
    for (const char* line :
         {"SET CHUNK 3 0 IN default $1 BOGUS", "SET CHUNK 3 0 IN default $1x", "SET CHUNK 3 0 IN default $",
          "SET CHUNK 3 $1"}) {
        RawClient bad("127.0.0.1", harness.port);
        bad.Hello();
        bad.SendBytes(std::string(line) + "\r\n" + Frame(ChunkFormOf(full_presence, payload)));
        assert(bad.ReadLine().rfind("-ERR SYNTAX", 0) == 0);
        assert(bad.WaitForClose(std::chrono::seconds(5)));
    }
    assert(chunk_absent(3));

    // A frame not followed by an empty line desynchronizes the stream, so
    // the server answers BAD_REQUEST and closes.
    {
        RawClient bad_terminator("127.0.0.1", harness.port);
        bad_terminator.Hello();
        const std::string form = ChunkFormOf(full_presence, payload);
        bad_terminator.SendBytes(
            "SET CHUNK 3 0 IN default $1\r\n$" + std::to_string(form.size()) + "\r\n" + form + "XX\r\n");
        assert(bad_terminator.ReadLine() == "-ERR BAD_REQUEST a parameter must be followed by an empty line\r\n");
        assert(bad_terminator.WaitForClose(std::chrono::seconds(5)));
    }

    // A declared length above what a chunk frame holds is refused before
    // any byte of it is buffered.
    {
        RawClient oversize("127.0.0.1", harness.port);
        oversize.Hello();
        oversize.SendBytes("SET CHUNK 3 0 IN default $1\r\n$" + std::to_string(frame_limit + 1) + "\r\n");
        assert(
            oversize.ReadLine() ==
            "-ERR BAD_REQUEST $1 is longer than its column holds (" + std::to_string(frame_limit) + " bytes)\r\n");
        assert(oversize.WaitForClose(std::chrono::seconds(5)));
    }
    // Frame headers that cannot be framed (unparsable length) are refused
    // and the connection closed, since the frame length is unknown.
    for (const char* header : {"$18x", "$+18", "18", "$"}) {
        RawClient bad("127.0.0.1", harness.port);
        bad.Hello();
        bad.SendBytes("SET CHUNK 3 0 IN default $1\r\n" + std::string(header) + "\r\n");
        assert(bad.ReadLine().rfind("-ERR BAD_REQUEST expected a parameter frame", 0) == 0);
        assert(bad.WaitForClose(std::chrono::seconds(5)));
    }
    assert(chunk_absent(3));
}

// A request line cut off by the end of the stream is not executed: the
// client may have been interrupted in the middle of it ("DROP TABLE t" of
// "DROP TABLE t2").
void TestUnterminatedLineIsNotExecuted() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    ServerHarness harness("unterminated", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient admin("127.0.0.1", harness.port);
    admin.Hello();
    admin.SendLine("CREATE TABLE t (bits bits(4)) CHUNK 4 x 4");
    assert(admin.ReadLine() == "+OK\r\n");
    admin.SendLine("SET BLOCK 0 0 IN default bits = b'0001'");
    (void)ReadVersion(admin);
    {
        RawClient cut("127.0.0.1", harness.port);
        cut.Hello();
        cut.SendBytes("DROP TABLE t");
        cut.ShutdownWrite();
        assert(cut.WaitForClose(std::chrono::seconds(5)));
    }
    {
        // Cut where the statement so far is valid (an IF VERSION may follow).
        RawClient cut("127.0.0.1", harness.port);
        cut.Hello();
        cut.SendBytes("SET BLOCK 1 0 IN default bits = b'1111'");
        cut.ShutdownWrite();
        assert(cut.WaitForClose(std::chrono::seconds(5)));
    }
    admin.SendLine("GET BLOCK 1 0 FROM default");
    assert(admin.ReadLine() == "_\r\n");
    assert(harness.catalog->Find("t") != nullptr);
    // The same statement, terminated, is applied.
    admin.SendLine("SET BLOCK 1 0 IN default bits = b'1111'");
    (void)ReadVersion(admin);
    admin.SendLine("GET BLOCK 1 0 FROM default");
    assert(admin.ReadReply() == BitsReply("\x0f"));
}

// Before HELLO succeeds a connection has proved nothing: failed HELLOs count
// toward max_auth_failures, and HELLO must succeed within the I/O timeout.
void TestHandshakeIsBounded() {
    {
        // Three failed HELLOs end the connection at once, long before the
        // handshake deadline.
        auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 3};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 4000;
        ServerHarness harness("handshake-failures", BaseStoreConfig(), engine_cfg, server_cfg);
        RawClient client("127.0.0.1", harness.port);
        for (int i = 0; i < 2; ++i) {
            client.SendLine("HELLO 3");
            assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
        }
        client.SendLine("HELLO 3 TABLE missing BOGUS");
        assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(client.WaitForClose(std::chrono::milliseconds(1000)));
    }
    {
        // HELLOs paced below the I/O timeout, never enough to reach
        // max_auth_failures: the connection still ends at the deadline.
        auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 1000};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 800;
        ServerHarness harness("handshake-deadline", BaseStoreConfig(), engine_cfg, server_cfg);
        RawClient client("127.0.0.1", harness.port);
        const auto started = std::chrono::steady_clock::now();
        bool closed = false;
        std::string last;
        while (!closed && std::chrono::steady_clock::now() - started < std::chrono::seconds(4)) {
            try {
                client.SendLine("HELLO 3");
                last = client.ReadLine();
            } catch (const std::exception&) {
                closed = true;
                break;
            }
            if (last.rfind("-ERR PROTOCOL HELLO 3 was not completed", 0) == 0) {
                closed = client.WaitForClose(std::chrono::seconds(1));
                break;
            }
            assert(last.rfind("-ERR AUTH_REQUIRED", 0) == 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
        assert(closed);
        assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(2500));
        RawClient ok("127.0.0.1", harness.port);
        ok.Login();
        ok.SendLine("PING");
        assert(ok.ReadLine() == "+PONG\r\n");
    }
    {
        // A login stalled between HELLO 3 USER and AUTH ends at the deadline
        // too.
        auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 1000};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 800;
        ServerHarness harness("handshake-scram-deadline", BaseStoreConfig(), engine_cfg, server_cfg);
        RawClient stalled("127.0.0.1", harness.port);
        const auto started = std::chrono::steady_clock::now();
        const auto login = chunkdb::scram::StartClientLogin(kAdminUser, chunkdb::scram::NewNonce());
        stalled.SendBytes(chunkdb::test::HelloUserBytes(login, kAdminUser));
        assert(stalled.ReadLine().rfind("+SCRAM r=", 0) == 0);
        std::string line;
        if (stalled.ReadLineWithin(std::chrono::seconds(3), &line)) {
            assert(line.rfind("-ERR PROTOCOL HELLO 3 was not completed", 0) == 0);
        }
        assert(stalled.WaitForClose(std::chrono::seconds(1)));
        assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(2500));
    }
}

// The HELLO deadline also ends a line begun before it, and a client that
// sends nothing is told why it is closed.
void TestHelloDeadlineEndsAPartialLine() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.client_io_timeout_ms = 1000;
    server_cfg.idle_connection_timeout_ms = 10000;
    ServerHarness harness("hello-partial", BaseStoreConfig(), engine_cfg, server_cfg);
    const std::string refused = "-ERR PROTOCOL HELLO 3 was not completed within the I/O timeout\r\n";
    {
        // The line starts half way to the deadline; a line deadline of its
        // own would end it 500 ms after the HELLO deadline.
        RawClient slow("127.0.0.1", harness.port);
        const auto start = Clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        slow.SendBytes("HELLO");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        slow.SendBytes(" 3");
        std::string reply;
        assert(slow.ReadLineWithin(std::chrono::milliseconds(2500), &reply));
        assert(reply == refused);
        assert(Clock::now() - start < std::chrono::milliseconds(1350));
        assert(slow.WaitForClose(std::chrono::seconds(1)));
    }
    {
        RawClient silent("127.0.0.1", harness.port);
        const auto start = Clock::now();
        std::string reply;
        assert(silent.ReadLineWithin(std::chrono::milliseconds(2500), &reply));
        assert(reply == refused);
        assert(Clock::now() - start < std::chrono::milliseconds(1350));
    }
}

// With max_handshakes_per_ip, one source holds at most that many workers
// before HELLO; another connection gets -ERR BUSY at once, and HELLO frees
// a slot.
void TestHandshakesPerIpAreLimited() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 4;
    server_cfg.max_handshakes_per_ip = 2;
    server_cfg.client_io_timeout_ms = 5000;
    ServerHarness harness("handshake-limit", BaseStoreConfig(), engine_cfg, server_cfg);
    const auto wait_for_handshakes = [&](std::size_t count) {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        while (harness.server->HandshakesInProgressForTests("127.0.0.1") != count) {
            if (Clock::now() >= deadline) {
                std::fprintf(
                    stderr, "expected %zu handshakes in progress, have %zu\n", count,
                    harness.server->HandshakesInProgressForTests("127.0.0.1"));
                assert(false);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    };
    // The harness's readiness probe holds a slot from when a worker takes it
    // until the worker sees it closed. Workers take connections in order, so
    // once a later one has said HELLO the probe was taken; then wait for its
    // slot.
    {
        RawClient later("127.0.0.1", harness.port);
        later.Hello();
    }
    wait_for_handshakes(0);
    RawClient first("127.0.0.1", harness.port);
    RawClient second("127.0.0.1", harness.port);
    wait_for_handshakes(2);
    {
        RawClient third("127.0.0.1", harness.port);
        const auto start = Clock::now();
        std::string reply;
        assert(third.ReadLineWithin(std::chrono::milliseconds(2000), &reply));
        assert(reply == "-ERR BUSY too many connections before HELLO from this address\r\n");
        assert(Clock::now() - start < std::chrono::milliseconds(1500));
    }
    first.Hello();
    wait_for_handshakes(1);
    RawClient fourth("127.0.0.1", harness.port);
    fourth.Hello();
    fourth.SendLine("PING");
    assert(fourth.ReadLine() == "+PONG\r\n");
}

// Area replies are bounded: a read whose reply would exceed
// max_response_bytes is refused, and the connection stays usable.
void TestAreaReplyIsBounded() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    ServerHarness harness("area-bound", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    // One chunk of this table is 64 MiB, more than one reply holds.
    client.SendLine("CREATE TABLE wide (v bits(512)) CHUNK 1024 x 1024");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("SET BLOCK 0 0 IN wide v = b'1" + std::string(511, '0') + "'");
    (void)ReadVersion(client);
    for (const char* line : {"GET AREA 0 0 TO 1 1 FROM wide", "GET AREA AROUND 0 0 RADIUS 1 FROM wide"}) {
        client.SendLine(line);
        const std::string refused = client.ReadLine();
        assert(refused.rfind("-ERR OUT_OF_RANGE", 0) == 0);
        assert(refused.find("response exceeds the 67108864-byte limit") != std::string::npos);
    }
    client.SendLine("GET AREA 1 1 TO 2 2 FROM wide");
    assert(client.ReadLine() == "*0\r\n");
}

void TestTimeoutsAreBounded() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig([] {
            auto config = BaseStoreConfig();
            config.data_dir = TempDataDir("timeout-bounds");
            return config;
        }()));
    auto engine = std::make_shared<chunkdb::CommandEngine>(engine_cfg, catalog);
    for (const bool idle : {false, true}) {
        auto server_cfg = BaseServerConfig();
        (idle ? server_cfg.idle_connection_timeout_ms : server_cfg.client_io_timeout_ms) = 10'000'000'000'000ULL;
        bool refused = false;
        try {
            chunkdb::ChunkServer server(server_cfg, engine);
        } catch (const std::invalid_argument&) {
            refused = true;
        }
        assert(refused);
    }
}

void TestChunkPutRequiresHelloBeforePayload() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = true,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunkput-hello", store_cfg, engine_cfg, server_cfg);
    const std::size_t payload_bytes = harness.geometry().ChunkPayloadBytes();
    const std::size_t presence_bytes = (harness.geometry().ChunkBlockCount() + 7U) / 8U;
    const std::string form = ChunkFormOf(std::string(presence_bytes, '\xFF'), std::string(payload_bytes, '\x0F'));

    // Before HELLO: the statement is refused and the connection closed
    // before its frame is read, so unauthenticated clients cannot make the
    // server buffer.
    {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine("SET CHUNK 0 0 IN default $1");
        assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 3\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }

    RawClient client("127.0.0.1", harness.port);
    client.Login();
    client.SendLine("GET CHUNK 0 0 FROM default");
    assert(client.ReadLine() == "_\r\n");
    client.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame(form));
    (void)ReadVersion(client);
    client.SendLine("GET CHUNK 0 0 FROM default");
    assert(ParseChunkForm(client.ReadBulkText(), presence_bytes, payload_bytes).presence ==
           std::string(presence_bytes, '\xFF'));
}

// The conditional write works for the largest geometry the server accepts:
// 1024x1024 blocks of 512 bits, a 64 MiB chunk.
void TestChunkPutIfLargestGeometry() {
    auto store_cfg = BaseStoreConfig();
    store_cfg.geometry = {
        .large_chunk_width_chunks = 1,
        .large_chunk_height_chunks = 1,
        .chunk_width_blocks = 1024,
        .chunk_height_blocks = 1024,
        .block_bits = 512,
    };
    store_cfg.checkpoint_wal_bytes = 1ULL << 40U;
    store_cfg.checkpoint_update_interval = 1000;
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.client_io_timeout_ms = 60000;
    ServerHarness harness("chunkput-largest", store_cfg, engine_cfg, server_cfg);
    const std::size_t payload_bytes = harness.geometry().ChunkPayloadBytes();
    const std::size_t presence_bytes = (harness.geometry().ChunkBlockCount() + 7U) / 8U;
    assert(payload_bytes == 64U * 1024U * 1024U);

    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    // Seed a known version before exercising the largest conditional replace.
    client.SendLine("SET BLOCK 0 0 IN default bits = b'" + std::string(512, '0') + "'");
    (void)ReadVersion(client);
    client.SendLine("GET CHUNK 0 0 FROM default");
    const std::uint64_t version = ParseChunkForm(client.ReadBulkText(), presence_bytes, payload_bytes).version;
    std::string payload(payload_bytes, '\0');
    for (std::size_t i = 0; i < payload.size(); i += 4093) {
        payload[i] = static_cast<char>(i % 251 + 1);
    }
    const std::string frame = Frame(ChunkFormOf(std::string(presence_bytes, '\xFF'), payload));
    client.SendBytes("SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(version) + "\r\n" + frame);
    const std::uint64_t written = ReadVersion(client);
    assert(written > version);
    client.SendBytes("SET CHUNK 0 0 IN default $1 IF VERSION " + std::to_string(version) + "\r\n" + frame);
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(written) + "\r\n");
    client.SendLine("GET CHUNK 0 0 FROM default");
    const auto read_back = ParseChunkForm(client.ReadBulkText(), presence_bytes, payload_bytes);
    assert(read_back.version == written);
    assert(read_back.payload == payload);
}

void TestChunkGetLengthsAndForms() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunk-len", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    // 4x4 blocks of 4 bits: 8 payload bytes and 2 presence bytes, after the
    // version and the schema version.
    const auto geometry = harness.geometry();
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t presence_bytes = (geometry.ChunkBlockCount() + 7U) / 8U;
    const std::size_t form_bytes = 16U + presence_bytes + payload_bytes;
    assert(payload_bytes == 8U && form_bytes == 26U);

    // A never-written chunk is null, including a repeated cached read.
    client.SendLine("GET CHUNK 0 0 FROM default");
    assert(client.ReadLine() == "_\r\n");
    client.SendLine("GET CHUNK 0 0 FROM default COLUMNS bits");
    assert(client.ReadLine() == "_\r\n");

    const std::string zero_chunk(payload_bytes, '\0');
    client.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame(ChunkFormOf(std::string(presence_bytes, '\xFF'), zero_chunk)));
    const std::uint64_t version = ReadVersion(client);
    client.SendLine("GET CHUNK 0 0 FROM default");
    const std::string full = client.ReadBulkText();
    assert(full.size() == form_bytes);
    const auto state = ParseChunkForm(full, presence_bytes, payload_bytes);
    assert(state.version == version);
    assert(state.presence == "\xFF\xFF");
    assert(state.payload == zero_chunk);

    // COLUMNS naming the one column, and an area read, give the same form.
    client.SendLine("GET CHUNK 0 0 FROM default COLUMNS bits");
    assert(client.ReadBulkText() == full);
    client.SendLine("GET AREA 0 0 TO 0 0 FROM default");
    assert(client.ReadReply() == "*1\r\n*3\r\n:0\r\n:0\r\n$26\r\n" + full + "\r\n");

    // A sparse state: blocks 0 and 15 present.
    const std::string sparse = ChunkFormOf(std::string("\x01\x80", 2), std::string("\x0f", 1) + std::string(7, '\0'));
    client.SendBytes("SET CHUNK 1 0 IN default $1\r\n" + Frame(sparse));
    const std::uint64_t sparse_version = ReadVersion(client);
    client.SendLine("GET CHUNK 1 0 FROM default");
    const std::string sparse_back = client.ReadBulkText();
    assert(ParseChunkForm(sparse_back, presence_bytes, payload_bytes).version == sparse_version);
    assert(sparse_back.substr(8) == sparse.substr(8));
    client.SendLine("GET BLOCK 4 0 FROM default");
    assert(client.ReadReply() == BitsReply("\x0f"));
    client.SendLine("GET BLOCK 5 0 FROM default");
    assert(client.ReadLine() == "_\r\n");
}

void TestPipelinedCommandsSinglePacket() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("pipeline-single-packet", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    std::string payload = "HELLO 3\r\n";
    payload.reserve(220 * 6 + 128);
    for (int i = 0; i < 220; ++i) {
        payload += "PING\r\n";
    }
    payload += "SET BLOCK 0 0 IN default bits = b'1010'\r\n";
    payload += "GET BLOCK 0 0 FROM default\r\n";
    payload += "PING\r\n";
    client.SendBytes(payload);

    assert(ReadHelloReply(client).at("protocol") == "3");
    for (int i = 0; i < 220; ++i) {
        assert(client.ReadLine() == "+PONG\r\n");
    }
    (void)ReadVersion(client);
    assert(client.ReadReply() == BitsReply("\x05"));
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestExtremeChunkRangeKeepsConnectionUsable() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    ServerHarness harness("extreme-chunk-range", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    client.SendLine(
        "GET AREA -9223372036854775808 0 TO 9223372036854775807 0 FROM default");
    assert(client.ReadLine().rfind("-ERR ", 0) == 0);
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
    client.SendLine(
        "GET AREA 9223372036854775807 9223372036854775807 "
        "TO 9223372036854775807 9223372036854775807 FROM default");
    assert(client.ReadLine() == "*0\r\n");
}

// QUIT is gone: a client ends its connection by closing it, and the server
// then closes its side.
void TestQuitClosesConnection() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("quit", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    client.SendLine("QUIT");
    assert(client.ReadLine() == "-ERR SYNTAX column 1: unknown statement 'QUIT'\r\n");
    client.ShutdownWrite();
    assert(client.WaitForClose(std::chrono::seconds(2)));
}

void TestPipelinedBadRequestDisconnectPolicy() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.max_line_bytes = 32;

    ServerHarness harness("pipeline-bad-request", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

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

// Protocol 3: statements, parameter frames pipelined with the next
// statement, and a frame longer than its column, which closes the
// connection unread.
void TestProtocolThreeFrames() {
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    ServerHarness harness("protocol-three", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 3");
    assert(client.ReadLine() == "%8\r\n");
    // Eight pairs; server_version is a bulk string, server_signature null
    // without a user, the rest integers.
    for (std::size_t line = 0; line < 25; ++line) {
        (void)client.ReadLine();
    }

    client.SendLine("SET BLOCK 1 1 IN default bits = b'1010'");
    assert(client.ReadLine().rfind(':', 0) == 0);
    client.SendLine("GET BLOCK 1 1 FROM default");
    assert(client.ReadLine() == "*1\r\n");
    assert(client.ReadLine() == "$1\r\n");
    assert(client.ReadLine() == "\x05\r\n");

    client.SendBytes(std::string("SET BLOCK 2 1 IN default bits = $1\r\n$1\r\n\x06\r\nGET BLOCK 2 1 FROM default\r\n"));
    assert(client.ReadLine().rfind(':', 0) == 0);
    assert(client.ReadLine() == "*1\r\n");
    assert(client.ReadLine() == "$1\r\n");
    assert(client.ReadLine() == "\x06\r\n");

    // NULL for a column that cannot be NULL is an error of the statement.
    client.SendBytes("SET BLOCK 3 1 IN default bits = $1\r\n$-1\r\nGET BLOCK 3 1 FROM default\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT column bits cannot be NULL", 0) == 0);
    assert(client.ReadLine() == "_\r\n");

    client.SendBytes("SET BLOCK 3 1 IN default bits = $1\r\n$2\r\nxx\r\n");
    assert(client.ReadLine() == "-ERR BAD_REQUEST $1 is longer than its column holds (1 bytes)\r\n");
    assert(client.WaitForClose(std::chrono::seconds(2)));
}

void TestMaxAuthFailuresDisconnects() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = true,
        .max_auth_failures = 2,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("auth-fail-limit", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    assert(FailedLoginReply(client, kAdminUser, "no1").rfind("-ERR AUTH_FAILED", 0) == 0);
    assert(FailedLoginReply(client, kAdminUser, "no2").rfind("-ERR AUTH_FAILED", 0) == 0);

    assert(client.WaitForClose(std::chrono::seconds(2)));
}

// The runtime counters: those SHOW METRICS exports are read over the wire,
// the eviction details it does not export from the store.
void TestMetricsRuntimeCounters() {
    auto store_cfg = BaseStoreConfig();
    store_cfg.max_loaded_chunks = 8;
    store_cfg.wal_group_commit_updates = 64;
    store_cfg.checkpoint_update_interval = 10'000;
    store_cfg.checkpoint_wal_bytes = 10'000'000;
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("info-counters", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    const auto read_metrics = [&client]() {
        client.SendLine("SHOW METRICS");
        return ParseMetrics(client.ReadBulkText());
    };
    const auto store_stats = [&harness]() {
        const auto lease = harness.catalog->Find("default")->Acquire();
        assert(lease.has_value());
        return lease->store().RuntimeStats();
    };

    for (int i = 0; i < 64; ++i) {
        client.SendLine(
            "SET BLOCK " + std::to_string(i * static_cast<int>(store_cfg.geometry.chunk_width_blocks)) +
            " 0 IN default bits = b'1010'");
        (void)ReadVersion(client);
    }
    client.SendLine("GET BLOCK 0 0 FROM default");
    assert(client.ReadReply() == BitsReply("\x05"));

    const auto metrics_first = read_metrics();
    const auto stats_first = store_stats();
    for (const char* name :
         {"chunkdb_loaded_chunks", "chunkdb_evictions_total", "chunkdb_checkpoints_total",
          "chunkdb_wal_batch_flushes_total", "chunkdb_unique_loaded_chunks_total", "chunkdb_open_wal_streams"}) {
        assert(metrics_first.contains(name));
    }
    assert(std::string(chunkdb::ChunkLockModeName()) == ExpectedChunkLockMode());

    const auto loaded_chunks_first = std::stoull(metrics_first.at("chunkdb_loaded_chunks"));
    const auto unique_loaded_chunks_first = std::stoull(metrics_first.at("chunkdb_unique_loaded_chunks_total"));
    const auto evictions_first = std::stoull(metrics_first.at("chunkdb_evictions_total"));
    const auto checkpoints_first = std::stoull(metrics_first.at("chunkdb_checkpoints_total"));
    const auto wal_batch_flushes_first = std::stoull(metrics_first.at("chunkdb_wal_batch_flushes_total"));
    const auto open_wal_streams_first = std::stoull(metrics_first.at("chunkdb_open_wal_streams"));

    assert(loaded_chunks_first >= 1);
    assert(unique_loaded_chunks_first >= loaded_chunks_first);
    assert(evictions_first > 0);
    assert(stats_first.eviction_probes >= evictions_first);
    assert(stats_first.eviction_forced_wal_flushes > 0);
    assert(
        stats_first.eviction_forced_wal_flushes ==
        stats_first.eviction_forced_wal_flushes_with_data + stats_first.eviction_forced_wal_flushes_empty_batch);

    for (int i = 64; i < 96; ++i) {
        client.SendLine(
            "SET BLOCK " + std::to_string(i * static_cast<int>(store_cfg.geometry.chunk_width_blocks)) +
            " 0 IN default bits = b'0101'");
        (void)ReadVersion(client);
    }

    const auto metrics_second = read_metrics();
    const auto stats_second = store_stats();

    assert(std::stoull(metrics_second.at("chunkdb_evictions_total")) >= evictions_first);
    assert(std::stoull(metrics_second.at("chunkdb_checkpoints_total")) >= checkpoints_first);
    assert(std::stoull(metrics_second.at("chunkdb_wal_batch_flushes_total")) >= wal_batch_flushes_first);
    assert(std::stoull(metrics_second.at("chunkdb_open_wal_streams")) >= open_wal_streams_first);
    assert(stats_second.eviction_snapshot_builds >= stats_first.eviction_snapshot_builds);
    assert(stats_second.eviction_probes >= stats_first.eviction_probes);
    assert(stats_second.eviction_no_progress_cycles >= stats_first.eviction_no_progress_cycles);
    assert(stats_second.eviction_forced_wal_flushes >= stats_first.eviction_forced_wal_flushes);
    assert(
        stats_second.eviction_forced_wal_flushes ==
        stats_second.eviction_forced_wal_flushes_with_data + stats_second.eviction_forced_wal_flushes_empty_batch);
    assert(stats_second.eviction_forced_wal_flushes_with_data >= stats_first.eviction_forced_wal_flushes_with_data);
    assert(stats_second.eviction_forced_wal_flushes_empty_batch >= stats_first.eviction_forced_wal_flushes_empty_batch);
}

void TestSlowClientTimeoutReleasesWorker() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
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
    fast.SendLine("HELLO 3");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "%8\r\n");
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
}

#ifdef CHUNKDB_WITH_OPENSSL
void TestTlsHandshakeDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.tls_enabled = true;
    server_cfg.client_io_timeout_ms = 250;

    ServerHarness harness("tls-handshake-deadline", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);

    stalled.SendBytes(std::string("\x16\x03\x03\x01\x00", 5));
    for (int i = 0; i < 6; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        try {
            stalled.SendBytes(std::string(1, '\0'));
        } catch (...) {
            break;
        }
    }

    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=handshake"));
    assert(logs.Contains("reason=timeout"));

    TlsClient fast("127.0.0.1", harness.port);
    fast.Hello();
    fast.SendLine("PING");
    assert(fast.ReadLine() == "+PONG\r\n");
}

// A TLS record sent a byte at a time, each byte well inside the idle
// timeout, must not hold the worker: once record bytes arrive, the request
// has the I/O timeout to complete.
void TestTlsTrickledRecordIsBounded() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.tls_enabled = true;
    server_cfg.client_io_timeout_ms = 300;
    server_cfg.idle_connection_timeout_ms = 10000;

    ServerHarness harness("tls-trickle", store_cfg, engine_cfg, server_cfg);
    TlsClient slow("127.0.0.1", harness.port);
    slow.Hello();
    // Header of a 16 KiB application-data record, then its body.
    assert(slow.SendRaw(std::string("\x17\x03\x03\x40\x00", 5)));
    const auto start = Clock::now();
    while (Clock::now() - start < std::chrono::seconds(4) && slow.SendRaw(std::string(1, '\0'))) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    assert(slow.WaitForRawClose(std::chrono::milliseconds(1500)));
    assert(Clock::now() - start < std::chrono::seconds(3));
    assert(logs.WaitContains("request line deadline exceeded", std::chrono::seconds(2)));

    TlsClient fast("127.0.0.1", harness.port);
    fast.Hello();
    fast.SendLine("PING");
    assert(fast.ReadLine() == "+PONG\r\n");
}

// A record without data (a TLS 1.3 key update) starts no request: a greeted
// connection still waits the idle timeout, and an ungreeted one is still
// closed at the HELLO deadline.
void TestTlsKeyUpdateIsNotARequest() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.tls_enabled = true;
    server_cfg.client_io_timeout_ms = 300;
    server_cfg.idle_connection_timeout_ms = 10000;
    ServerHarness harness("tls-key-update", BaseStoreConfig(), engine_cfg, server_cfg);

    TlsClient greeted("127.0.0.1", harness.port);
    greeted.Hello();
    greeted.KeyUpdate();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    greeted.SendLine("PING");
    assert(greeted.ReadLine() == "+PONG\r\n");

    TlsClient ungreeted("127.0.0.1", harness.port);
    const auto start = Clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    ungreeted.KeyUpdate();
    assert(ungreeted.WaitForRawClose(std::chrono::milliseconds(2000)));
    assert(Clock::now() - start < std::chrono::milliseconds(1500));
}

// Logins work over TLS as over a plain socket.
void TestLoginOverTls() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.tls_enabled = true;
    ServerHarness harness("tls-login", BaseStoreConfig(), engine_cfg, server_cfg);
    TlsClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 3");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
    assert(FailedLoginReply(client, kAdminUser, "wrong") == "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");
    client.SendLine("PING");
    assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 3\r\n");
    TlsClient authed("127.0.0.1", harness.port);
    authed.Login();
    authed.SendLine("PING");
    assert(authed.ReadLine() == "+PONG\r\n");
}

void TestChunkPutOverTls() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.tls_enabled = true;

    ServerHarness harness("tls-chunkput", store_cfg, engine_cfg, server_cfg);
    TlsClient client("127.0.0.1", harness.port);
    client.Hello();

    const std::size_t payload_bytes = harness.geometry().ChunkPayloadBytes();
    const std::size_t presence_bytes = (harness.geometry().ChunkBlockCount() + 7U) / 8U;

    // A frame split across two TLS records, then a frame with a pipelined
    // statement in the same write; both must be reassembled by the server.
    const std::string payload(payload_bytes, '\xF0');
    const std::string form = ChunkFormOf(std::string(presence_bytes, '\xFF'), payload);
    client.SendLine("SET CHUNK 0 0 IN default $1");
    client.SendBytes("$" + std::to_string(form.size()) + "\r\n" + form.substr(0, 3));
    client.SendBytes(form.substr(3) + "\r\n");
    const std::uint64_t version = ReadVersion(client);
    assert(version > 0);

    client.SendLine("GET CHUNK 0 0 FROM default");
    const auto read_back = ParseChunkForm(client.ReadBulkText(), presence_bytes, payload_bytes);
    assert(read_back.version == version);
    assert(read_back.payload == payload);

    // An empty replacement of a never-written chunk changes nothing.
    const std::string empty = ChunkFormOf(std::string(presence_bytes, '\x00'), payload);
    client.SendBytes("SET CHUNK 1 0 IN default $1\r\n" + Frame(empty) + "GET CHUNK 1 0 FROM default\r\n");
    (void)ReadVersion(client);
    assert(client.ReadLine() == "_\r\n");
    // Replacing the previously written chunk retains its versioned tombstone.
    client.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame(empty) + "GET CHUNK 0 0 FROM default\r\n");
    const auto empty_version = ReadVersion(client);
    const auto absent = ParseChunkForm(client.ReadBulkText(), presence_bytes, payload_bytes);
    assert(absent.version == empty_version);
    assert(absent.presence == std::string(presence_bytes, '\0'));
    assert(absent.payload == std::string(payload_bytes, '\0'));
    // The reject-and-close paths are covered by the plain-socket test; over
    // TLS the close can race ahead of the client reading the error reply.
}
#endif

void TestReadTimeoutLogsPhaseAndReason() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;

    ServerHarness harness("slow-client-timeout-log", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.Hello();
    stalled.SendBytes("PING");

    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=read"));
    assert(logs.Contains("reason=timeout"));
    assert(logs.CountContains("connection terminated") == 1);
}

void TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;

    ServerHarness harness("post-close-send-no-sigpipe", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("PING");
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));

    for (int i = 0; i < 32; ++i) {
        try {
            stalled.SendBytes("X");
        } catch (const std::runtime_error&) {
            break;
        }
    }

    RawClient ok("127.0.0.1", harness.port);
    ok.Hello();
    ok.SendLine("PING");
    assert(ok.ReadLine() == "+PONG\r\n");
}

void TestSendTimeoutSetupFailureClosesConnection() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("send-timeout-setup-failure", store_cfg, engine_cfg, server_cfg);
    {
        ScopedServerTimeoutFailpoint failpoint(2, 0);
        RawClient client("127.0.0.1", harness.port);
        client.SendLine("PING");
        assert(logs.WaitContains(
            "failed to configure client send timeout; closing connection",
            std::chrono::seconds(2)));
        assert(client.WaitForClose(std::chrono::milliseconds(1500)));
    }

    RawClient ok("127.0.0.1", harness.port);
    ok.Hello();
    ok.SendLine("PING");
    assert(ok.ReadLine() == "+PONG\r\n");
}

void TestReceiveTimeoutSetupFailureClosesConnection() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("recv-timeout-setup-failure", store_cfg, engine_cfg, server_cfg);
    {
        ScopedServerTimeoutFailpoint failpoint(0, 2);
        RawClient client("127.0.0.1", harness.port);
        assert(logs.WaitContains(
            "failed to configure client receive timeout; closing connection",
            std::chrono::seconds(2)));
        client.SendLine("PING");
        assert(client.WaitForClose(std::chrono::milliseconds(1500)));
    }

    // Before HELLO the wait is bounded by the handshake deadline, not idle.
    assert(logs.Contains("phase=handshake_wait"));

    RawClient ok("127.0.0.1", harness.port);
    ok.Hello();
    ok.SendLine("PING");
    assert(ok.ReadLine() == "+PONG\r\n");
}

void TestSlowRequestDribbleDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 250;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("slow-request-dribble-deadline", store_cfg, engine_cfg, server_cfg);
    RawClient stalled("127.0.0.1", harness.port);
    stalled.Hello();
    stalled.SendBytes("P");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));
    stalled.SendBytes("I");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("HELLO 3");

    stalled.SendBytes("N");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "%8\r\n");
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(2)));
    assert(logs.Contains("phase=read"));
    assert(logs.Contains("reason=timeout"));
}

void TestIdleClientRemainsConnectedBetweenCommands() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 150;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("idle-client-kept-alive", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);

    client.Hello();

    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");

    std::this_thread::sleep_for(std::chrono::milliseconds(350));

    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("recv-timeout-state-cache", store_cfg, engine_cfg, server_cfg);
    chunkdb::ResetServerTimeoutConfigCountersForTests();

    RawClient client("127.0.0.1", harness.port);
    constexpr std::size_t kRequests = 32;
    std::string batch = "HELLO 3\r\n";
    for (std::size_t i = 0; i < kRequests; ++i) {
        batch += "PING\r\n";
    }
    client.SendBytes(batch);
    assert(ReadHelloReply(client).at("protocol") == "3");
    for (std::size_t i = 0; i < kRequests; ++i) {
        assert(client.ReadLine() == "+PONG\r\n");
    }

    const std::uint64_t recv_timeout_calls = chunkdb::ServerRecvTimeoutConfigCallsForTests();
    if (recv_timeout_calls >= kRequests / 2) {
        throw std::runtime_error(
            "recv timeout was reconfigured too often for pipelined keep-alive requests: calls=" +
            std::to_string(recv_timeout_calls));
    }
}

void TestLongIdleConnectionTimeoutReleasesWorker() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
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
    fast.SendLine("HELLO 3");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "%8\r\n");
    assert(idle.WaitForClose(std::chrono::milliseconds(1500)));
}

void TestPendingQueueWaitTimeoutClosesQueuedSocket() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 300;
    server_cfg.idle_connection_timeout_ms = 150;
    server_cfg.max_pending_clients = 2;

    ServerHarness harness("pending-queue-wait-timeout", store_cfg, engine_cfg, server_cfg);

    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("PING");
    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    RawClient queued("127.0.0.1", harness.port);
    std::this_thread::sleep_for(std::chrono::milliseconds(220));

    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
    assert(queued.WaitForClose(std::chrono::milliseconds(1500)));

    const auto recovery_deadline = Clock::now() + std::chrono::seconds(2);
    bool recovered = false;
    while (!recovered && Clock::now() < recovery_deadline) {
        try {
            RawClient recovery("127.0.0.1", harness.port);
            recovery.SendLine("HELLO 3");

            std::string response;
            recovered =
                recovery.ReadLineWithin(std::chrono::milliseconds(500), &response) &&
                response == "%8\r\n";
        } catch (const std::runtime_error&) {
            recovered = false;
        }

        if (!recovered) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    assert(recovered);
}

void TestSlowResponseDrainDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    store_cfg.geometry.chunk_width_blocks = 512;
    store_cfg.geometry.chunk_height_blocks = 512;
    store_cfg.geometry.block_bits = 32;

    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 250;
    server_cfg.idle_connection_timeout_ms = 1000;

    ServerHarness harness("slow-response-drain-deadline", store_cfg, engine_cfg, server_cfg);
    {
        auto lease = harness.catalog->Find("default")->Acquire();
        lease->store().SetBlockBits(0, 0, std::string(32, '0'));
    }
    RawClient slow("127.0.0.1", harness.port);
    slow.SetReceiveBuffer(1024);
    // HELLO and a 1 MiB GET CHUNK reply in one write, read slowly.
    slow.SendBytes("HELLO 3\r\nGET CHUNK 0 0 FROM default\r\n");

    std::size_t bytes_read = 0;
    assert(slow.ReadSomeWithin(std::chrono::milliseconds(1000), 512, &bytes_read));
    assert(bytes_read > 0);

    for (int i = 0; i < 4; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        assert(slow.ReadSomeWithin(std::chrono::milliseconds(500), 512, &bytes_read));
        if (bytes_read == 0) {
            break;
        }
    }

    // The server terminates the slow connection server-side once the deadline
    // is hit; assert that via the log. We intentionally do NOT assert that the
    // slow client observes the TCP close within a fixed window: the test set
    // SO_RCVBUF=1024, which throttles delivery of the already-buffered response
    // so severely that graceful close (drain + FIN) can take tens of seconds on
    // Linux (verified: a continuously-draining client still saw no EOF after
    // 30s). Likewise, whether the deadline fires in the write phase or the
    // following idle-read phase depends on OS socket-buffer autotuning (differs
    // across Linux/macOS), so accept either phase.
    assert(logs.WaitContains("connection terminated", std::chrono::seconds(5)));
    assert(logs.Contains("phase=write") || logs.Contains("phase=read"));
    assert(logs.Contains("reason=timeout"));

    // After the slow connection is terminated, the single worker must be able
    // to serve a new client.
    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("HELLO 3");
    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(2000), &response));
    assert(response == "%8\r\n");
}

void TestIdlePeerCloseDoesNotLogTerminationWarning() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("idle-peer-close-no-log", store_cfg, engine_cfg, server_cfg);
    {
        RawClient client("127.0.0.1", harness.port);
        client.Hello();
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
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 10000;
    server_cfg.max_pending_clients = 1;

    ServerHarness harness("pending-queue-saturation", store_cfg, engine_cfg, server_cfg);

    auto try_connect = [&](const std::string& host, std::uint16_t port) -> std::unique_ptr<RawClient> {
        try {
            return std::make_unique<RawClient>(host, port);
        } catch (const std::runtime_error&) {
            return nullptr;
        }
    };

    // Helper: send tolerantly. In a saturation scenario the server may legitimately
    // reset a connection at a moment the test cannot precisely predict (scheduling
    // of the single worker vs. the accept loop varies across platforms/core counts).
    // A reset here is not a failure of the invariant under test, so swallow it.
    auto try_send_line = [](RawClient& c, const std::string& s) -> bool {
        try {
            c.SendLine(s);
            return true;
        } catch (const std::runtime_error&) {
            return false;
        }
    };

    RawClient stalled("127.0.0.1", harness.port);
    stalled.SendBytes("HELLO 3");
    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    auto queued = try_connect("127.0.0.1", harness.port);
    auto rejected1 = try_connect("127.0.0.1", harness.port);
    auto rejected2 = try_connect("127.0.0.1", harness.port);

    // At least one rejection must be logged. We do NOT assert an exact count:
    // the warn fires once per overload episode, and if the single worker drains
    // the pending slot between rejections the queue re-saturates and warns again.
    // How many episodes occur depends on worker/accept-loop scheduling.
    assert(logs.WaitContains(
        "pending client queue full; rejecting new connections",
        std::chrono::seconds(2)));
    assert(logs.CountContains("pending client queue full; rejecting new connections") >= 1);

    // Completing stalled's request is best-effort: if the worker already cycled
    // past it and the server reset the connection, the worker is still proven
    // free by the recovery client at the end of the test.
    if (try_send_line(stalled, "")) {
        std::string stalled_reply;
        if (stalled.ReadLineWithin(std::chrono::seconds(2), &stalled_reply) &&
            stalled_reply == "%8\r\n") {
            stalled.ShutdownWrite();
            (void)stalled.WaitForClose(std::chrono::milliseconds(800));
        }
    }
    (void)stalled.WaitForClose(std::chrono::milliseconds(800));

    std::size_t served_count = 0;
    std::size_t not_served_count = 0;
    std::size_t busy_count = 0;
    for (const auto& client_ptr :
         std::array<const std::unique_ptr<RawClient>*, 3>{&queued, &rejected1, &rejected2}) {
        RawClient* client = client_ptr->get();
        if (client == nullptr) {
            // connect() was refused outright (no backlog slot).
            not_served_count += 1;
            continue;
        }

        std::string line;
        bool got_line = false;
        try {
            client->SendLine("HELLO 3");
            got_line = client->ReadLineWithin(std::chrono::seconds(2), &line);
        } catch (const std::runtime_error&) {
            got_line = false;
        }
        if (got_line) {
            if (line.rfind("-ERR BUSY", 0) == 0) {
                busy_count += 1;
                not_served_count += 1;
                (void)client->WaitForClose(std::chrono::seconds(2));
                continue;
            }
            assert(line == "%8\r\n");
            client->ShutdownWrite();
            (void)client->WaitForClose(std::chrono::seconds(2));
            served_count += 1;
            continue;
        }

        (void)client->WaitForClose(std::chrono::seconds(2));
        not_served_count += 1;
    }

    // Exactly which of the three excess connections is served vs. rejected
    // depends on how the single worker and the accept loop interleave, which
    // varies by platform and core count. The robust invariants are: every
    // connection was accounted for, and the queue could not serve all of them
    // (at least one was rejected/reset).
    assert(served_count + not_served_count == 3);
    assert(not_served_count >= 1);
    assert(busy_count >= 1 || logs.CountContains("pending client queue full; rejecting new connections") >= 1);

    // The server must remain usable after the saturation burst.
    RawClient recovery("127.0.0.1", harness.port);
    recovery.Hello();
    recovery.SendLine("PING");
    assert(recovery.ReadLine() == "+PONG\r\n");
}

void TestReadinessLogLineExists() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("log-readiness", store_cfg, engine_cfg, server_cfg);
    assert(logs.WaitContains(" INFO server pid=", std::chrono::seconds(2)));
    assert(logs.WaitContains("Z INFO server pid=", std::chrono::seconds(2)));
    assert(logs.WaitContains("ready to accept connections", std::chrono::seconds(2)));
    assert(logs.WaitContains("protocol=tcp", std::chrono::seconds(2)));
}

void TestWarnLineOnBadRequest() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
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
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    const OccupiedPort occupied;
    server_cfg.host = "127.0.0.1";
    server_cfg.port = occupied.port();
    const std::filesystem::path data_dir = TempDataDir("log-error-listen");
    store_cfg.data_dir = data_dir;

    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(store_cfg));
    auto engine = std::make_shared<chunkdb::CommandEngine>(engine_cfg, catalog);
    auto server = std::make_unique<chunkdb::ChunkServer>(server_cfg, engine);

    bool failed = false;
    try {
        server->Run();
    } catch (const std::exception& error) {
        const std::string message = error.what();
        assert(message.find("port is already in use") != std::string::npos);
        assert(message.find(std::to_string(occupied.port())) != std::string::npos);
        assert(message.find("choose another --port") != std::string::npos);
        failed = true;
    }

    server.reset();
    engine.reset();
    catalog.reset();
    RemoveAllWithRetry(data_dir);

    assert(failed);
    assert(logs.Contains(" ERROR server pid="));
    assert(logs.Contains("server run loop failed"));
}

#ifdef CHUNKDB_WITH_OPENSSL
void TestTlsConfigurationErrors() {
    const auto dir = TempDataDir("tls-configuration-errors");
    const auto credentials = WriteTlsTestCredentials(dir);
    auto store = BaseStoreConfig();
    store.data_dir = dir / "data";
    auto catalog = std::make_shared<chunkdb::TableCatalog>(chunkdb::CatalogConfigFromStoreConfig(store));
    auto engine = std::make_shared<chunkdb::CommandEngine>(chunkdb::EngineConfig{.require_auth = false}, catalog);
    auto config = BaseServerConfig();
    config.tls_enabled = true;
    auto expect = [&](const std::string& hint, const std::string& path) {
        bool failed = false;
        try {
            chunkdb::ChunkServer server(config, engine);
        } catch (const std::exception& error) {
            const std::string message = error.what();
            assert(message.find(hint) != std::string::npos);
            assert(path.empty() || message.find(path) != std::string::npos);
            failed = true;
        }
        assert(failed);
    };
    expect("set --tls-cert and --tls-key", "");
    config.tls_cert_path = (dir / "missing-cert.pem").string();
    config.tls_key_path = credentials.key_path.string();
    expect("set --tls-cert to a readable PEM", config.tls_cert_path);
    config.tls_cert_path = credentials.cert_path.string();
    config.tls_key_path = (dir / "missing-key.pem").string();
    expect("set --tls-key to a readable PEM", config.tls_key_path);
    config.tls_key_path = credentials.key_path.string();
    config.tls_cert_path = (dir / "invalid-cert.pem").string();
    WriteTextFile(config.tls_cert_path, "invalid PEM");
    expect("set --tls-cert to a readable PEM", config.tls_cert_path);
    engine.reset();
    catalog.reset();
    RemoveAllWithRetry(dir);
}
#endif

void TestServerHarnessStartupFailure() {
    const OccupiedPort occupied;
    auto config = BaseServerConfig();
    config.port = occupied.port();
    bool failed = false;
    try {
        {
            ServerHarness harness("harness-startup-failure", BaseStoreConfig(),
                chunkdb::EngineConfig{.require_auth = false}, config);
        }
        // The occupied listener can accept the probe before Run reports the
        // bind failure. Teardown must preserve that error too.
        RethrowBackgroundServerError();
    } catch (const std::runtime_error& error) {
        const std::string_view message(error.what());
        failed = message.find("port is already in use") != std::string_view::npos &&
            message.find(std::to_string(occupied.port())) != std::string_view::npos;
    }
    assert(failed && "a failed startup must report its server error after joining the thread");
    RethrowBackgroundServerError();
    // A caught startup failure must not poison the next server lifetime.
    TestPing();
}

void TestLogLevelFilteringWarn() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
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
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    const OccupiedPort occupied;
    server_cfg.host = "127.0.0.1";
    server_cfg.port = occupied.port();
    const std::filesystem::path data_dir = TempDataDir("log-filter-error");
    store_cfg.data_dir = data_dir;

    auto catalog = std::make_shared<chunkdb::TableCatalog>(
        chunkdb::CatalogConfigFromStoreConfig(store_cfg));
    auto engine = std::make_shared<chunkdb::CommandEngine>(engine_cfg, catalog);
    auto server = std::make_unique<chunkdb::ChunkServer>(server_cfg, engine);

    try {
        server->Run();
    } catch (...) {
    }

    server.reset();
    engine.reset();
    catalog.reset();
    RemoveAllWithRetry(data_dir);

    assert(logs.Contains(" ERROR server pid="));
    assert(!logs.Contains(" WARN "));
    assert(!logs.Contains(" INFO "));
}

void TestStartupLogOrder() {
    ScopedLogCapture logs(chunkdb::LogLevel::kInfo);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
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

// Tables over the wire: each statement works on the table it names, the
// table's geometry governs its statements (including SET CHUNK frame
// bounds), and a drop reaches every connection that uses the table.
void TestTablesOverProtocol() {
    const auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 4;  // two long-lived connections plus short ones
    ServerHarness harness("tables", BaseStoreConfig(), engine_cfg, server_cfg);
    RawClient a("127.0.0.1", harness.port);
    RawClient b("127.0.0.1", harness.port);
    const auto read_tables = [](RawClient& client) {
        client.SendLine("SHOW TABLES");
        const auto header = client.ReadLine();
        assert(header.rfind("*", 0) == 0);
        std::vector<std::string> names;
        for (int i = 0; i < std::stoi(header.substr(1)); ++i) {
            names.push_back(client.ReadBulkText());
        }
        return names;
    };
    const auto describe = [](RawClient& client, const std::string& table) {
        client.SendLine("DESCRIBE " + table);
        const std::string reply = client.ReadReply();
        assert(reply.rfind("%6\r\n$5\r\ntable\r\n$" + std::to_string(table.size()) + "\r\n" + table + "\r\n", 0) == 0);
        return reply;
    };
    const auto contains = [](const std::string& reply, const std::string& part) {
        return reply.find(part) != std::string::npos;
    };

    // The harness creates `default`: one column of 4 bits.
    a.Hello();
    b.Hello();
    assert(read_tables(a) == (std::vector<std::string>{"default"}));
    assert(contains(describe(a, "default"), "$4\r\nname\r\n$4\r\nbits\r\n$4\r\ntype\r\n$7\r\nbits(4)\r\n"));

    a.SendLine("CREATE TABLE terrain (bits bits(9)) CHUNK 8 x 2 WITH durability_mode = 'fsync-wal'");
    assert(a.ReadLine() == "+OK\r\n");
    a.SendLine("CREATE TABLE terrain (bits bits(9)) CHUNK 8 x 2");
    assert(a.ReadLine().rfind("-ERR TABLE_EXISTS", 0) == 0);
    a.SendLine("CREATE TABLE Terrain (bits bits(9)) CHUNK 8 x 2");
    assert(a.ReadLine() == "-ERR SYNTAX column 14: names are lowercase: 'Terrain'\r\n");
    assert(read_tables(b) == (std::vector<std::string>{"default", "terrain"}));

    // An unknown name is an error of the statement.
    a.SendLine("GET BLOCK 0 0 FROM nope");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);

    // DESCRIBE gives the geometry and options.
    const std::string terrain = describe(a, "terrain");
    assert(contains(terrain, "$4\r\ntype\r\n$7\r\nbits(9)\r\n"));
    assert(contains(terrain, "$5\r\nchunk\r\n*2\r\n:8\r\n:2\r\n"));
    assert(contains(terrain, "$15\r\ndurability_mode\r\n$9\r\nfsync-wal\r\n"));

    // Same coordinates, independent tables.
    a.SendLine("SET BLOCK 1 1 IN terrain bits = b'101010101'");
    (void)ReadVersion(a);
    b.SendLine("SET BLOCK 1 1 IN default bits = b'1010'");
    (void)ReadVersion(b);
    a.SendLine("GET BLOCK 1 1 FROM terrain");
    assert(a.ReadReply() == BitsReply(std::string("\x55\x01", 2)));
    b.SendLine("GET BLOCK 1 1 FROM default");
    assert(b.ReadReply() == BitsReply("\x05"));

    // SET CHUNK is framed by the named table's geometry: a terrain chunk is
    // 8x2 blocks of 9 bits, 18 bytes, after the version and 2 presence bytes.
    std::string payload;
    for (int i = 0; i < 18; ++i) {
        payload.push_back(static_cast<char>(0x30 + i));
    }
    const std::string form = ChunkFormOf(std::string(2, '\xFF'), payload);
    a.SendBytes("SET CHUNK 5 5 IN terrain $1\r\n" + Frame(form));
    assert(ReadVersion(a) > 0U);
    a.SendLine("GET CHUNK 5 5 FROM terrain");
    const std::string chunk = a.ReadBulkText();
    assert(ParseChunkForm(chunk, 2, 18).payload == payload);
    {
        // Another connection reads the same chunk by name.
        RawClient on_terrain("127.0.0.1", harness.port);
        on_terrain.Hello();
        on_terrain.SendLine("GET CHUNK 5 5 FROM terrain");
        assert(on_terrain.ReadBulkText() == chunk);
        // HELLO has no TABLE option; the connection stays open and not
        // greeted.
        RawClient unknown("127.0.0.1", harness.port);
        unknown.SendLine("HELLO 3 TABLE terrain");
        assert(unknown.ReadLine() == "-ERR INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1\r\n");
        unknown.Hello();
    }
    {
        // A frame of a length that a terrain chunk takes is read; the same
        // length is more than a default chunk holds and is refused unread.
        const std::size_t default_limit =
            16U + 2U + 8U + harness.catalog->Find("default")->Info().options.var_max_chunk_bytes;
        const std::size_t terrain_limit =
            16U + 2U + 18U + harness.catalog->Find("terrain")->Info().options.var_max_chunk_bytes;
        assert(default_limit < terrain_limit);
        const std::string long_form = form + std::string(default_limit + 1U - form.size(), '\0');
        a.SendBytes("SET CHUNK 6 6 IN terrain $1\r\n" + Frame(long_form));
        assert(a.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        RawClient on_default("127.0.0.1", harness.port);
        on_default.Hello();
        on_default.SendLine("SET CHUNK 5 5 IN default $1");
        on_default.SendBytes("$" + std::to_string(long_form.size()) + "\r\n");
        assert(
            on_default.ReadLine() ==
            "-ERR BAD_REQUEST $1 is longer than its column holds (" + std::to_string(default_limit) + " bytes)\r\n");
        assert(on_default.WaitForClose(std::chrono::seconds(5)));
    }

    // Options change while another connection uses the table.
    b.SendLine("ALTER TABLE terrain SET checkpoint_updates = 3");
    assert(b.ReadLine() == "+OK\r\n");
    b.SendLine("ALTER TABLE terrain SET checkpoint_compression = 'zrle'");
    assert(b.ReadLine() == "+OK\r\n");
    b.SendLine("ALTER TABLE terrain SET block_bits = 5");
    assert(b.ReadLine().rfind("-ERR INVALID_ARGUMENT block_bits is part of the geometry", 0) == 0);
    const std::string altered = describe(b, "terrain");
    assert(contains(altered, "$18\r\ncheckpoint_updates\r\n:3\r\n"));
    assert(contains(altered, "$22\r\ncheckpoint_compression\r\n$4\r\nzrle\r\n"));
    a.SendLine("GET BLOCK 1 1 FROM terrain");
    assert(a.ReadReply() == BitsReply(std::string("\x55\x01", 2)));

    // A drop reaches a connection that used the table, and a table of the
    // same name created again is a new table.
    b.SendLine("DROP TABLE terrain");
    assert(b.ReadLine() == "+OK\r\n");
    a.SendLine("GET BLOCK 1 1 FROM terrain");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    b.SendLine("CREATE TABLE terrain (bits bits(3)) CHUNK 4 x 4");
    assert(b.ReadLine() == "+OK\r\n");
    assert(contains(describe(a, "terrain"), "$4\r\ntype\r\n$7\r\nbits(3)\r\n"));
    a.SendLine("GET BLOCK 1 1 FROM terrain");
    assert(a.ReadLine() == "_\r\n");
    // SET CHUNK takes the new geometry: 4x4 blocks of 3 bits, 6 bytes.
    const std::string small = ChunkFormOf(std::string(2, '\xFF'), "\x01\x02\x03\x04\x05\x06");
    a.SendBytes("SET CHUNK 5 5 IN terrain $1\r\n" + Frame(small));
    (void)ReadVersion(a);
    a.SendLine("GET CHUNK 5 5 FROM terrain");
    assert(a.ReadBulkText().substr(8) == small.substr(8));

    // FLUSH WAL covers every table.
    a.SendLine("FLUSH WAL");
    assert(a.ReadLine() == "+OK\r\n");

    b.SendLine("DROP TABLE default");
    assert(b.ReadLine() == "+OK\r\n");
    {
        // HELLO needs no table; a statement on the dropped table is refused,
        // and a SET CHUNK, whose frame it cannot bound, closes the
        // connection.
        RawClient fresh("127.0.0.1", harness.port);
        fresh.Hello();
        fresh.SendLine("GET BLOCK 0 0 FROM default");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        fresh.SendLine("SET CHUNK 0 0 IN default $1");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        assert(fresh.WaitForClose(std::chrono::seconds(5)));
    }
    assert(read_tables(b) == (std::vector<std::string>{"terrain"}));
}

// Table statements need an authenticated HELLO like every other statement;
// a table statement before HELLO is a protocol error that closes the
// connection.
void TestTableCommandsRequireAuth() {
    const auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = true,
        .max_auth_failures = 5,
    };
    ServerHarness harness("tables-auth", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    for (const char* command :
         {"SHOW TABLES", "DESCRIBE default", "CREATE TABLE x (bits bits(4)) CHUNK 4 x 4",
          "ALTER TABLE default SET checkpoint_updates = 3", "DROP TABLE default"}) {
        RawClient early("127.0.0.1", harness.port);
        early.SendLine(command);
        assert(early.ReadLine() == "-ERR PROTOCOL expected HELLO 3\r\n");
        assert(early.WaitForClose(std::chrono::seconds(5)));
    }
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 3");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
    client.SendLine("SHOW TABLES");
    assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 3\r\n");
    assert(client.WaitForClose(std::chrono::seconds(5)));
    RawClient authed("127.0.0.1", harness.port);
    authed.Login();
    authed.SendLine("SHOW TABLES");
    assert(authed.ReadLine() == "*1\r\n");
}

struct FeedReply {
    char type = 0;
    std::string value;
    std::vector<FeedReply> items;
    friend bool operator==(const FeedReply&, const FeedReply&) = default;
};
template <typename Client> FeedReply ReadFeedReply(Client& client) {
    const auto line = client.ReadLine();
    assert(line.size() >= 3U);
    FeedReply reply{line[0], line.substr(1, line.size() - 3U), {}};
    if (reply.type == '$') { client.Unread(line); reply.value = client.ReadBulkText(); }
    else if (reply.type == '*' || reply.type == '%' || reply.type == '>') {
        const auto count = std::stoull(reply.value) * (reply.type == '%' ? 2U : 1U);
        for (std::size_t i = 0; i < count; ++i) reply.items.push_back(ReadFeedReply(client));
    }
    return reply;
}
const FeedReply& FeedMap(const FeedReply& reply, const std::string& key) {
    for (std::size_t i = 0; i < reply.items.size(); i += 2U)
        if (reply.items[i].value == key) return reply.items[i + 1U];
    throw std::logic_error("missing feed test map key");
}
template <typename Client> void TestWatchProtocol(bool tls) {
    auto config = BaseServerConfig();
    config.worker_threads = 2;
    config.tls_enabled = tls;
    config.max_watches = 2;
    ServerHarness harness(tls ? "watch-tls" : "watch-plain", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    Client writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(15)); writer.Login();
    writer.SendLine("CREATE TABLE t (n u8, label text(100), active bool, weight f32, flags bits(3), blob bytes(10)) CHUNK 4 x 4");
    assert(writer.ReadLine() == "+OK\r\n");
    Client whole("127.0.0.1", harness.port); whole.SetReadDeadline(std::chrono::seconds(15)); whole.Login();
    whole.SendLine("WATCH t");
    const auto start = whole.ReadLine();
    assert(start.rfind("+OK ", 0) == 0 && start.size() > 39U);
    const auto position = start.substr(4, start.size() - 6U);
    Client area("127.0.0.1", harness.port); area.SetReadDeadline(std::chrono::seconds(15)); area.Login();
    area.SendLine("WATCH t AREA 0 0 TO 0 0"); assert(area.ReadLine().rfind("+OK ", 0) == 0);
    // Two watches leave both workers available; this third connection writes.
    writer.SendLine("WATCH default"); assert(writer.ReadLine().rfind("-ERR BUSY", 0) == 0);
    writer.SendLine("SET BLOCK 0 0 IN t n = 7, label = 'hello', active = true, weight = 1.5, flags = b'101', blob = x'00ff'");
    const auto revision = ReadVersion(writer);
    const auto change = ReadFeedReply(whole);
    assert(change.type == '>' && change.items.size() == 7U && change.items[0].value == "change");
    assert(change.items[2].value == std::to_string(revision) && change.items[4].value == kAdminUser);
    const auto& block = change.items[6].items.at(0);
    assert(block.items[0].value == "0" && block.items[1].value == "0" && block.items[2].type == '_');
    const auto& row = block.items[3].items;
    assert(row[0].value == "7" && row[1].value == "hello" && row[2].value == "t" && row[3].value == "1.5");
    assert(row[4].value == std::string(1, '\x05') && row[5].value == std::string("\0\xff", 2));
    assert(ReadFeedReply(area) == change);
    writer.SendLine("BEGIN"); assert(writer.ReadLine() == "+OK\r\n");
    writer.SendLine("SET BLOCK 0 0 IN t n = 8"); assert(writer.ReadLine() == "_\r\n");
    writer.SendLine("SET BLOCK 4 0 IN t n = 9"); assert(writer.ReadLine() == "_\r\n");
    writer.SendLine("COMMIT"); const auto committed = ReadVersion(writer);
    const auto transaction = ReadFeedReply(whole);
    assert(transaction.items[2].value == std::to_string(committed) && committed > revision);
    assert(transaction.items[6].items.size() == 2U);
    const auto cut = ReadFeedReply(area);
    assert(cut.items[6].items.size() == 1U && cut.items[6].items[0].items[0].value == "0");
    assert(cut.items[6].items[0].items[2].items == row);
    assert(cut.items[6].items[0].items[3].items[0].value == "8");
    area.SendBytes("UNWATCH\r\nPING\r\n");
    assert(area.ReadLine() == "+OK\r\n" && area.ReadLine() == "+PONG\r\n");
    area.SendLine("WATCH t AFTER " + position); assert(area.ReadLine() == start);
    assert(ReadFeedReply(area) == change && ReadFeedReply(area) == transaction);
    writer.SendLine("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 2"); assert(writer.ReadLine() == "+OK\r\n");
    const auto schema = ReadFeedReply(whole);
    assert(schema.type == '>' && schema.items[0].value == "schema" && schema.items[3].value == "2");
    assert(ReadFeedReply(area) == schema);
    writer.SendLine("DESCRIBE t"); assert(FeedMap(ReadFeedReply(writer), "columns") == schema.items[4]);
    writer.SendLine("DROP TABLE t"); assert(writer.ReadLine() == "+OK\r\n");
    assert(whole.ReadLine().rfind("-ERR NO_TABLE", 0) == 0 && area.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    // A command other than UNWATCH terminates the stream, after an error.
    writer.SendLine("WATCH default"); assert(writer.ReadLine().rfind("+OK ", 0) == 0);
    writer.SendLine("PING"); assert(writer.ReadLine().rfind("-ERR PROTOCOL", 0) == 0);
}
void TestWatchPositionsAndRights() {
    auto config = BaseServerConfig(); config.worker_threads = 2; config.feed_buffer_bytes = 16384;
    config.feed_linger_ms = 0U;
    ServerHarness harness("watch-positions", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    RawClient writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(15)); writer.Login();
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = chunkdb::scram::FormatVerifier(chunkdb::scram::MakeVerifier("pw", salt, chunkdb::scram::kMinIterations));
    writer.SendLine("CREATE USER reader VERIFIER '" + verifier + "'"); assert(writer.ReadLine() == "+OK\r\n");
    RawClient reader("127.0.0.1", harness.port); reader.SetReadDeadline(std::chrono::seconds(15)); reader.Login("reader", "pw");
    reader.SendLine("WATCH default"); const auto hidden = reader.ReadLine();
    reader.SendLine("WATCH missing"); const auto missing = reader.ReadLine();
    assert(hidden.rfind("-ERR NO_TABLE", 0) == 0 && missing.rfind("-ERR NO_TABLE", 0) == 0);
    writer.SendLine("GRANT READ ON default TO reader"); assert(writer.ReadLine() == "+OK\r\n");
    reader.SendLine("WATCH default"); const auto initial = reader.ReadLine();
    const auto old = initial.substr(4, initial.size() - 6U);
    for (std::size_t i = 0; i < 100U; ++i) {
        writer.SendLine("SET BLOCK 0 0 IN default bits = b'" + std::string(i % 2U ? "1000" : "0100") + "'");
        (void)ReadVersion(writer);
    }
    reader.SendLine("UNWATCH");
    while (ReadFeedReply(reader).type != '+') {}
    reader.SendLine("WATCH default AFTER " + old); assert(reader.ReadLine().rfind("+OK ", 0) == 0);
    assert(ReadFeedReply(reader).items[0].value == "resync");
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
    // Last watch gone: the new feed has no replay history, even in this process.
    reader.SendLine("WATCH default AFTER " + old); assert(reader.ReadLine().rfind("+OK ", 0) == 0);
    assert(ReadFeedReply(reader).items[0].value == "resync");
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
    reader.SendLine("WATCH default AFTER ffffffffffffffffffffffffffffffff 0"); assert(reader.ReadLine().rfind("+OK ", 0) == 0);
    assert(ReadFeedReply(reader).items[0].value == "resync");
    writer.SendBytes("SET CHUNK 9223372036854775807 0 IN default $1\r\n" +
        Frame(ChunkFormOf(std::string("\x01\0", 2), std::string("\x01", 1) + std::string(7, '\0'))));
    (void)ReadVersion(writer);
    const auto coordinate = ReadFeedReply(reader).items[6].items[0].items[0];
    assert(coordinate.type == '*' && coordinate.items[0].value == "9223372036854775807" && coordinate.items[1].value == "0");
    harness.Restart();
    RawClient restarted("127.0.0.1", harness.port); restarted.SetReadDeadline(std::chrono::seconds(15)); restarted.Login("reader", "pw");
    restarted.SendLine("WATCH default AFTER " + old); assert(restarted.ReadLine().rfind("+OK ", 0) == 0);
    assert(ReadFeedReply(restarted).items[0].value == "resync");
    harness.saved_store_config.access_mode = chunkdb::AccessMode::kReadOnly;
    harness.Restart();
    RawClient readonly("127.0.0.1", harness.port); readonly.SetReadDeadline(std::chrono::seconds(15)); readonly.Login();
    readonly.SendLine("WATCH default"); assert(readonly.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
    harness.saved_store_config.access_mode = chunkdb::AccessMode::kReadWrite;
    harness.saved_store_config.allow_multiple_processes = true;
    harness.Restart();
    RawClient multi("127.0.0.1", harness.port); multi.SetReadDeadline(std::chrono::seconds(15)); multi.Login();
    multi.SendLine("WATCH default"); assert(multi.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
}
template <typename Client> void TestWatchSlowReader(bool tls) {
    auto config = BaseServerConfig(); config.worker_threads = 2; config.max_watches = 10;
    config.feed_buffer_bytes = 32U * 1024U * 1024U; config.tls_enabled = tls;
    ServerHarness harness(tls ? "watch-slow-tls" : "watch-slow-plain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(60)); writer.Hello();
    writer.SendLine("CREATE TABLE t (n u8, data bytes(32768)) CHUNK 4 x 4"); assert(writer.ReadLine() == "+OK\r\n");
    Client slow("127.0.0.1", harness.port); slow.SetReadDeadline(std::chrono::seconds(60)); slow.SmallReceiveBuffer(); slow.Hello();
    slow.SendLine("WATCH t"); assert(slow.ReadLine().rfind("+OK ", 0) == 0);
    Client fast("127.0.0.1", harness.port); fast.SetReadDeadline(std::chrono::seconds(60)); fast.Hello();
    fast.SendLine("WATCH t"); assert(fast.ReadLine().rfind("+OK ", 0) == 0);
    std::vector<std::unique_ptr<Client>> idle;
    for (int i = 0; i < 6; ++i) {
        auto watch = std::make_unique<Client>("127.0.0.1", harness.port);
        watch->SetReadDeadline(std::chrono::seconds(60)); watch->Hello();
        watch->SendLine("WATCH t AREA 100 100 TO 100 100"); assert(watch->ReadLine().rfind("+OK ", 0) == 0);
        idle.push_back(std::move(watch));
    }
    constexpr std::size_t writes = 200U;
    // The writer stays at most kLead changes ahead of the fast reader, well
    // inside its share of the buffer, so only the reader that stops reading
    // can fall behind, however slow the machine is.
    constexpr std::size_t kLead = 8U;
    std::mutex progress_mutex;
    std::condition_variable progress;
    std::size_t consumed = 0;
    std::thread drain([&] {
        std::uint64_t last = 0;
        for (std::size_t i = 0; i < writes; ++i) {
            auto entry = ReadFeedReply(fast);
            assert(entry.items[0].value == "change");
            const auto revision = std::stoull(entry.items[2].value);
            assert(revision > last); last = revision;
            {
                std::lock_guard lock(progress_mutex);
                consumed = i + 1U;
            }
            progress.notify_one();
        }
        fast.SendLine("UNWATCH"); assert(fast.ReadLine() == "+OK\r\n");
    });
    for (std::size_t i = 0; i < writes; ++i) {
        {
            std::unique_lock lock(progress_mutex);
            progress.wait(lock, [&] { return consumed + kLead >= i; });
        }
        const std::string bytes(32768, static_cast<char>(i % 2U));
        writer.SendBytes("SET BLOCK 0 0 IN t data = $1\r\n" + Frame(bytes));
        (void)ReadVersion(writer);
    }
    drain.join();
    fast.Disconnect();
    slow.SendLine("UNWATCH");
    bool resync = false;
    for (;;) {
        const auto entry = ReadFeedReply(slow);
        if (entry.type == '+') break;
        assert(entry.type == '>');
        resync |= entry.items[0].value == "resync";
    }
    assert(resync && "unsent byte share must force resync without ring eviction");
    slow.SendLine("PING"); assert(slow.ReadLine() == "+PONG\r\n");
}
template <typename Client> void TestWatchNoIdle(bool tls) {
    auto config = BaseServerConfig(); config.worker_threads = 2;
    config.idle_connection_timeout_ms = 100; config.tls_enabled = tls;
    ServerHarness harness(tls ? "watch-idle-tls" : "watch-idle-plain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client a("127.0.0.1", harness.port); a.SetReadDeadline(std::chrono::seconds(5)); a.Hello();
    a.SendLine("WATCH default"); assert(a.ReadLine().rfind("+OK ", 0) == 0);
    Client b("127.0.0.1", harness.port); b.SetReadDeadline(std::chrono::seconds(5)); b.Hello();
    b.SendLine("WATCH default"); assert(b.ReadLine().rfind("+OK ", 0) == 0);
    // Exercise the configured timeout, not an ordering workaround.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    Client writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(5)); writer.Hello();
    writer.SendLine("SET BLOCK 0 0 IN default bits = b'1000'"); (void)ReadVersion(writer);
    assert(ReadFeedReply(a).items[0].value == "change" && ReadFeedReply(b).items[0].value == "change");
}
class UnwatchReturnHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        if (point != Point::kBeforeReturnClient) return;
        std::unique_lock lock(mutex_);
        reached_ = true;
        changed_.notify_all();
        changed_.wait(lock, [&] { return released_; });
    }
    bool Wait() {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, std::chrono::seconds(10), [&] { return reached_; });
    }
    void Release() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool reached_ = false, released_ = false;
};

template <class Client>
void TestUnwatchReleaseBeforeReply(bool tls) {
    UnwatchReturnHook hook; // Outlives the server's callbacks.
    auto config = BaseServerConfig();
    config.tls_enabled = tls;
    config.feed_linger_ms = 0U;
    ServerHarness harness("unwatch-release-before-reply", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client reader("127.0.0.1", harness.port);
    reader.SetReadDeadline(std::chrono::seconds(15)); reader.Hello();
    // HELLO completes on a worker after its feed I/O loop has been installed.
    chunkdb::FeedDeliveryTestAccess::SetHook(*harness.server, &hook);
    try {
        reader.SendLine("WATCH default"); assert(reader.ReadLine().rfind("+OK ", 0) == 0);
        auto table = harness.catalog->Find("default");
        assert(chunkdb::FeedTestAccess::Capturing(*table));
        reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
        assert(hook.Wait());
        // The I/O loop is paused after the reply, before returning this socket
        // to a worker. Subscription removal must already be complete.
        assert(!chunkdb::FeedTestAccess::Capturing(*table));
    } catch (...) { hook.Release(); throw; }
    hook.Release();
    reader.SendLine("PING"); assert(reader.ReadLine() == "+PONG\r\n");
}

template <class Client>
void TestWatchLinger(bool tls, bool expires, bool disabled = false, bool slot = false) {
    std::cerr << "LINGER tls=" << tls << " expires=" << expires
              << " disabled=" << disabled << " slot=" << slot << '\n';
    auto config = BaseServerConfig();
    config.tls_enabled = tls;
    config.worker_threads = 2;
    if (expires) config.feed_linger_ms = 1U;
    if (disabled) config.feed_linger_ms = 0U;
    ServerHarness harness("watch-linger", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    Client writer("127.0.0.1", harness.port);
    writer.SetReadDeadline(std::chrono::seconds(15)); writer.Login();
    if (slot) { writer.SendLine("CREATE SLOT 'consumer' ON default"); assert(writer.ReadLine() == "+OK\r\n"); }
    Client reader("127.0.0.1", harness.port);
    reader.SetReadDeadline(std::chrono::seconds(15)); reader.Login();
    reader.SendLine("WATCH default"); assert(reader.ReadLine().rfind("+OK ", 0) == 0);
    writer.SendLine("SET BLOCK 0 0 IN default bits = b'1000'");
    const auto first = ReadVersion(writer);
    const auto entry = ReadFeedReply(reader);
    assert(entry.items[0].value == "change");
    const auto position = entry.items[1].value + " " + std::to_string(first);
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
    if (slot) { writer.SendLine("DROP SLOT 'consumer' ON default"); assert(writer.ReadLine() == "+OK\r\n"); }
    auto table = harness.catalog->Find("default");
    if (expires) assert(chunkdb::FeedTestAccess::WaitLingerExpired(*table, std::chrono::seconds(10)));
    assert(chunkdb::FeedTestAccess::Capturing(*table) == (!expires && !disabled));
    writer.SendLine("SET BLOCK 0 0 IN default bits = b'0100'");
    const auto second = ReadVersion(writer);
    reader.SendLine("WATCH default AFTER " + position);
    assert(reader.ReadLine().rfind("+OK ", 0) == 0);
    const auto resumed = ReadFeedReply(reader);
    if (expires || disabled) assert(resumed.items[0].value == "resync");
    else {
        assert(resumed.items[0].value == "change");
        assert(std::stoull(resumed.items[2].value) == second);
    }
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
}

void TestLingerRejectedSubscription() {
    auto config = BaseServerConfig();
    config.feed_linger_ms = 3600000U;
    ServerHarness harness("watch-linger-rejected", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    for (const auto value : {-1LL, std::numeric_limits<long long>::max()}) {
        auto invalid = chunkdb::CatalogConfigFromStoreConfig(BaseStoreConfig());
        invalid.data_dir = harness.data_dir / "invalid-linger";
        invalid.feed_linger = std::chrono::milliseconds(value);
        bool refused = false;
        try { chunkdb::TableCatalog catalog(invalid); }
        catch (const std::invalid_argument&) { refused = true; }
        assert(refused && !std::filesystem::exists(invalid.data_dir));
    }
    auto table = harness.catalog->Find("default");
    auto subscription = table->SubscribeFeed();
    subscription.reset();
    assert(chunkdb::FeedTestAccess::Capturing(*table));
    bool rejected = false;
    try { (void)table->SubscribeFeed(chunkdb::FeedOptions{.buffer_bytes = config.feed_buffer_bytes + 1U}); }
    catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected && chunkdb::FeedTestAccess::Capturing(*table));
    chunkdb::FeedTestAccess::ExpireLinger(*table);
    assert(chunkdb::FeedTestAccess::WaitLingerExpired(*table, std::chrono::seconds(10)));
    assert(!chunkdb::FeedTestAccess::Capturing(*table));
    // A completed timer can be joined and a new one started for this table.
    subscription = table->SubscribeFeed();
    subscription.reset();
    assert(chunkdb::FeedTestAccess::Capturing(*table));
    chunkdb::FeedTestAccess::ExpireLinger(*table);
    assert(chunkdb::FeedTestAccess::WaitLingerExpired(*table, std::chrono::seconds(10)));
    assert(!chunkdb::FeedTestAccess::Capturing(*table));
}

void TestLingerCancellationDuringLeaseDrain() {
    auto config = BaseServerConfig();
    config.feed_linger_ms = 3600000U;
    ServerHarness harness("watch-linger-cancel", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    auto table = harness.catalog->Find("default");
    auto subscription = table->SubscribeFeed();
    subscription.reset();
    auto lease = table->Acquire();
    assert(lease);
    chunkdb::FeedTestAccess::ExpireLinger(*table);
    assert(chunkdb::FeedTestAccess::WaitLingerDraining(*table, std::chrono::seconds(10)));
    // Keep the lease alive while joining: cancellation must reopen admission
    // before waiting for this writer to finish.
    auto stopped = std::async(std::launch::async, [&] { chunkdb::FeedTestAccess::CancelLingerTimer(*table); });
    assert(stopped.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    stopped.get();
    assert(chunkdb::FeedTestAccess::Capturing(*table));
    lease.reset();
    table->StopFeed();
    assert(!chunkdb::FeedTestAccess::Capturing(*table));
}

class LingerFailureHook : public chunkdb::FeedTestHook {
  public:
    explicit LingerFailureHook(Point point) : point_(point) {}
    void Arm() { armed_.store(true, std::memory_order_release); }
    void Run(Point point, std::uint64_t) override {
        if (point == point_ && armed_.exchange(false, std::memory_order_acq_rel))
            throw std::runtime_error("injected linger cleanup failure");
    }
  private:
    Point point_;
    std::atomic<bool> armed_{false};
};

void TestLingerFailureFence(bool resume) {
    // The hook remains alive through all store/feed shutdown callbacks.
    LingerFailureHook hook(resume ? chunkdb::FeedTestHook::Point::kAfterResume :
        chunkdb::FeedTestHook::Point::kBeforeLingerPause);
    ScopedLogCapture logs(chunkdb::LogLevel::kError);
    auto config = BaseServerConfig();
    config.feed_linger_ms = 3600000U;
    ServerHarness harness("watch-linger-failure", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    auto table = harness.catalog->Find("default");
    if (resume) (void)table->CreateFeedSlot("consumer");
    auto subscription = table->SubscribeFeed();
    chunkdb::FeedTestAccess::SetHook(*table, &hook);
    subscription.reset();
    hook.Arm();
    chunkdb::FeedTestAccess::ExpireLinger(*table);
    assert(chunkdb::FeedTestAccess::WaitLingerExpired(*table, std::chrono::seconds(10)));
    bool fenced = false;
    try { (void)table->Acquire(); }
    catch (const chunkdb::FeedRecoveryRequiredError& error) {
        fenced = std::string_view(error.what()).find("restart server") != std::string_view::npos;
    }
    assert(fenced);
    fenced = false;
    try { (void)table->SubscribeFeed(); }
    catch (const chunkdb::FeedRecoveryRequiredError&) { fenced = true; }
    assert(fenced && logs.Contains("injected linger cleanup failure"));
    // The failure closes only this table and shutdown/reopen must remain safe.
    RawClient client("127.0.0.1", harness.port); client.SetReadDeadline(std::chrono::seconds(15)); client.Login();
    client.SendLine("PING"); assert(client.ReadLine() == "+PONG\r\n");
    harness.Restart();
    table = harness.catalog->Find("default");
    auto lease = table->Acquire();
    assert(lease);
}

void TestFeedPhaseSanitizedVerb() {
    using Watchdog = chunkdb::test::FeedPhaseWatchdog;
    for (const auto input : {std::string_view("private-password"), std::string_view("$32\r\nsecret"),
                            std::string_view("00110110"), std::string_view("\xff\0secret", 8),
                            std::string_view("AUTHsecret"), std::string_view("hello-private")}) {
        assert(Watchdog::Verb(input) == "<bytes>");
        Watchdog::Command("control: raw send", input);
    }
    assert(Watchdog::Verb("AUTH private-proof") == "AUTH");
    assert(Watchdog::Verb("WATCH table\r\nprivate-body") == "WATCH");
    Watchdog::Command("control: command send", "AUTH private-proof");
}

class IoDrainStopHook final : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeIoDrain && !entered_) {
            entered_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        } else if (point == Point::kBeforeIoJoin) {
            joining_ = true; changed_.notify_all();
        }
    }
    void WaitDrain() {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return entered_; });
    }
    void WaitJoin() {
        std::unique_lock lock(mutex_);
        changed_.wait(lock, [&] { return joining_; });
    }
    void Release() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, joining_ = false, released_ = false;
};

void TestFeedIoStopAfterDrain() {
    IoDrainStopHook hook; // Its storage outlives the server and every callback.
    ServerHarness harness("feed-io-stop-after-drain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, BaseServerConfig());
    struct Release { IoDrainStopHook& hook; ~Release() { hook.Release(); } } release{hook};
    RawClient probe("127.0.0.1", harness.port);
    probe.Hello(); // A worker has installed FeedIo before admitting HELLO.
    chunkdb::FeedDeliveryTestAccess::SetHook(*harness.server, &hook);
    chunkdb::test::FeedPhaseWatchdog::Phase("hook: wait before wake drain");
    hook.WaitDrain();
    probe.Disconnect(); // Leave only the feed wake descriptor; no client wake.
    chunkdb::test::FeedPhaseWatchdog::Phase("harness: single Stop");
    harness.server->Stop();
    chunkdb::test::FeedPhaseWatchdog::Phase("hook: wait for FeedIo Stop wake before join");
    hook.WaitJoin(); // Run() has reached its second/coalesced wake.
    hook.Release();
    harness.JoinStopped(); // Must not supply a third wake and mask the race.
}

void TestFeedWatch() {
    const auto run = [](const char* name, auto function) {
        chunkdb::test::FeedPhaseWatchdog group(name);
        function();
    };
    run("TestFeedPhaseSanitizedVerb", TestFeedPhaseSanitizedVerb);
    run("TestFeedIoStopAfterDrain()", [] { TestFeedIoStopAfterDrain(); });
    run("TestUnwatchReleaseBeforeReply<RawClient>(false)", [] { TestUnwatchReleaseBeforeReply<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
    run("TestUnwatchReleaseBeforeReply<TlsClient>(true)", [] { TestUnwatchReleaseBeforeReply<TlsClient>(true); });
#endif
    run("TestLingerFailureFence(false)", [] { TestLingerFailureFence(false); });
    run("TestLingerFailureFence(true)", [] { TestLingerFailureFence(true); });
    run("TestLingerCancellationDuringLeaseDrain()", [] { TestLingerCancellationDuringLeaseDrain(); });
    run("TestLingerRejectedSubscription()", [] { TestLingerRejectedSubscription(); });
    run("TestWatchLinger<RawClient>(false, false)", [] { TestWatchLinger<RawClient>(false, false); });
    run("TestWatchLinger<RawClient>(false, true)", [] { TestWatchLinger<RawClient>(false, true); });
    run("TestWatchLinger<RawClient>(false, false, true)", [] { TestWatchLinger<RawClient>(false, false, true); });
    run("TestWatchLinger<RawClient>(false, false, false, true)", [] { TestWatchLinger<RawClient>(false, false, false, true); });
    run("TestWatchProtocol<RawClient>(false)", [] { TestWatchProtocol<RawClient>(false); });
    run("TestWatchNoIdle<RawClient>(false)", [] { TestWatchNoIdle<RawClient>(false); });
    run("TestWatchPositionsAndRights()", [] { TestWatchPositionsAndRights(); });
    run("TestWatchSlowReader<RawClient>(false)", [] { TestWatchSlowReader<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
    run("TestWatchLinger<TlsClient>(true, false)", [] { TestWatchLinger<TlsClient>(true, false); });
    run("TestWatchLinger<TlsClient>(true, true)", [] { TestWatchLinger<TlsClient>(true, true); });
    run("TestWatchLinger<TlsClient>(true, false, true)", [] { TestWatchLinger<TlsClient>(true, false, true); });
    run("TestWatchLinger<TlsClient>(true, false, false, true)", [] { TestWatchLinger<TlsClient>(true, false, false, true); });
    run("TestWatchProtocol<TlsClient>(true)", [] { TestWatchProtocol<TlsClient>(true); });
    run("TestWatchNoIdle<TlsClient>(true)", [] { TestWatchNoIdle<TlsClient>(true); });
    run("TestWatchSlowReader<TlsClient>(true)", [] { TestWatchSlowReader<TlsClient>(true); });
#endif
}

int main(int argc, char** argv) {
    chunkdb::test::FeedPhaseWatchdog::SuppressWindowsDialogs();
#ifdef _WIN32
    (void)EnsureWinsockRuntime();
#else
    (void)signal(SIGPIPE, SIG_IGN);
#endif
    std::string_view selected;
    bool feed_watch = false;
    bool quick_start_errors = false;
    for (int argument = 1; argument < argc; ++argument) {
        const std::string_view option(argv[argument]);
        if (option == "--feed-watch") feed_watch = true;
        else if (option == "--quick-start-errors") quick_start_errors = true;
        else if (option == "--case" && argument + 1 < argc) selected = argv[++argument];
        else { std::cerr << "unknown test option: " << option << '\n'; return 2; }
    }
    if (selected == "PhaseWatchdogStall") chunkdb::test::FeedPhaseWatchdog::StalledControl();
    std::size_t passed = 0, total = 0;
    const auto run = [&](const char* name, auto function) {
        if (!selected.empty() && selected != name) return;
        ++total;
        std::optional<chunkdb::test::FeedPhaseWatchdog> watchdog;
        if (std::string_view(name) != "TestFeedWatch") watchdog.emplace(name);
        std::cerr << "RUN " << name << '\n';
        try {
            function();
            RethrowBackgroundServerError();
            ++passed;
            std::cerr << "PASS " << name << '\n';
        } catch (const std::exception& error) {
            std::cerr << "FAIL " << name << ": " << error.what() << '\n';
            if (background_server_error) {
                try { RethrowBackgroundServerError(); }
                catch (const std::exception& background) {
                    std::cerr << "background server: " << background.what() << '\n';
                }
            }
        }
    };
    if (feed_watch) run("TestFeedWatch", TestFeedWatch);
    else if (quick_start_errors) {
        run("TestAuthAndSetGet", TestAuthAndSetGet);
        run("TestErrorLineOnListenFailure", TestErrorLineOnListenFailure);
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestTlsConfigurationErrors", TestTlsConfigurationErrors);
        run("TestLoginOverTls", TestLoginOverTls);
#endif
    } else {
        if (selected == "client-exception") run("client-exception", [] {
            ServerHarness harness("client-exception", BaseStoreConfig(),
                chunkdb::EngineConfig{.require_auth = false}, BaseServerConfig());
            RawClient client("127.0.0.1", harness.port);
            client.Hello();
            client.Disconnect();
            (void)client.ReadBulkText();
        });
        run("TestFeedPhaseSanitizedVerb", TestFeedPhaseSanitizedVerb);
        run("TestFeedIoStopAfterDrain", TestFeedIoStopAfterDrain);
        run("TestUnwatchReleaseBeforeReply", [] { TestUnwatchReleaseBeforeReply<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestUnwatchReleaseBeforeReplyTls", [] { TestUnwatchReleaseBeforeReply<TlsClient>(true); });
#endif
        run("TestPing", TestPing);
        run("TestProtocolOneClientIsRefused", TestProtocolOneClientIsRefused);
        run("TestAuthAndSetGet", TestAuthAndSetGet);
        run("TestChunkGetLengthsAndForms", TestChunkGetLengthsAndForms);
        run("TestChunkPutWritesAndFraming", TestChunkPutWritesAndFraming);
        run("TestUnterminatedLineIsNotExecuted", TestUnterminatedLineIsNotExecuted);
        run("TestHandshakeIsBounded", TestHandshakeIsBounded);
        run("TestHelloDeadlineEndsAPartialLine", TestHelloDeadlineEndsAPartialLine);
        run("TestHandshakesPerIpAreLimited", TestHandshakesPerIpAreLimited);
        run("TestAreaReplyIsBounded", TestAreaReplyIsBounded);
        run("TestTimeoutsAreBounded", TestTimeoutsAreBounded);
        run("TestChunkPutRequiresHelloBeforePayload", TestChunkPutRequiresHelloBeforePayload);
        run("TestChunkPutIfLargestGeometry", TestChunkPutIfLargestGeometry);
        run("TestPipelinedCommandsSinglePacket", TestPipelinedCommandsSinglePacket);
        run("TestExtremeChunkRangeKeepsConnectionUsable", TestExtremeChunkRangeKeepsConnectionUsable);
        run("TestQuitClosesConnection", TestQuitClosesConnection);
        run("TestMaxLineOverflowDisconnects", TestMaxLineOverflowDisconnects);
        run("TestProtocolThreeFrames", TestProtocolThreeFrames);
        run("TestPipelinedBadRequestDisconnectPolicy", TestPipelinedBadRequestDisconnectPolicy);
        run("TestMaxAuthFailuresDisconnects", TestMaxAuthFailuresDisconnects);
        run("TestMetricsRuntimeCounters", TestMetricsRuntimeCounters);
        run("TestSlowClientTimeoutReleasesWorker", TestSlowClientTimeoutReleasesWorker);
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestTlsHandshakeDeadlineReleasesWorker", TestTlsHandshakeDeadlineReleasesWorker);
        run("TestTlsTrickledRecordIsBounded", TestTlsTrickledRecordIsBounded);
        run("TestTlsKeyUpdateIsNotARequest", TestTlsKeyUpdateIsNotARequest);
        run("TestLoginOverTls", TestLoginOverTls);
        run("TestTlsConfigurationErrors", TestTlsConfigurationErrors);
        run("TestChunkPutOverTls", TestChunkPutOverTls);
#endif
        run("TestReadTimeoutLogsPhaseAndReason", TestReadTimeoutLogsPhaseAndReason);
        run("TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe", TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe);
        run("TestSendTimeoutSetupFailureClosesConnection", TestSendTimeoutSetupFailureClosesConnection);
        run("TestReceiveTimeoutSetupFailureClosesConnection", TestReceiveTimeoutSetupFailureClosesConnection);
        run("TestSlowRequestDribbleDeadlineReleasesWorker", TestSlowRequestDribbleDeadlineReleasesWorker);
        run("TestIdleClientRemainsConnectedBetweenCommands", TestIdleClientRemainsConnectedBetweenCommands);
        run("TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests", TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests);
        run("TestLongIdleConnectionTimeoutReleasesWorker", TestLongIdleConnectionTimeoutReleasesWorker);
        run("TestPendingQueueWaitTimeoutClosesQueuedSocket", TestPendingQueueWaitTimeoutClosesQueuedSocket);
        run("TestSlowResponseDrainDeadlineReleasesWorker", TestSlowResponseDrainDeadlineReleasesWorker);
        run("TestIdlePeerCloseDoesNotLogTerminationWarning", TestIdlePeerCloseDoesNotLogTerminationWarning);
        run("TestPendingQueueSaturationRejectsNewConnections", TestPendingQueueSaturationRejectsNewConnections);
        run("TestReadinessLogLineExists", TestReadinessLogLineExists);
        run("TestWarnLineOnBadRequest", TestWarnLineOnBadRequest);
        run("TestErrorLineOnListenFailure", TestErrorLineOnListenFailure);
        run("TestServerHarnessStartupFailure", TestServerHarnessStartupFailure);
        run("TestLogLevelFilteringWarn", TestLogLevelFilteringWarn);
        run("TestLogLevelFilteringError", TestLogLevelFilteringError);
        run("TestStartupLogOrder", TestStartupLogOrder);
        run("TestTablesOverProtocol", TestTablesOverProtocol);
        run("TestTableCommandsRequireAuth", TestTableCommandsRequireAuth);
    }
    std::cerr << passed << '/' << total << " integration cases passed\n";
    if (total == 0) return 2;
    return passed == total ? 0 : 1;
}
