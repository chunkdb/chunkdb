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
#include "../src/audit_log.hpp"
#include "../src/change_feed.hpp"
#include "../src/proxy_protocol.hpp"
#include "../src/slot_watch.hpp"
#include "../src/statement_control.hpp"
#include "../src/user_registry.hpp"

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
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#endif

namespace chunkdb {
void SetServerSocketConfigFailpointForTests(
    std::size_t send_failures,
    std::size_t nonblocking_failures) noexcept;
void ResetServerTimeoutConfigCountersForTests() noexcept;
std::uint64_t ServerRecvTimeoutConfigCallsForTests() noexcept;
int FeedDeliveryTestAccess::SetActiveClientSendBuffer(ChunkServer& server, int bytes) {
    std::lock_guard lock(server.active_clients_mutex_);
    assert(server.active_clients_.size() == 1U);
    const auto socket = static_cast<
#ifdef _WIN32
        SOCKET
#else
        int
#endif
    >(server.active_clients_.front());
    assert(setsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&bytes), sizeof(bytes)) == 0);
    int actual = 0;
#ifdef _WIN32
    int length = sizeof(actual);
#else
    socklen_t length = sizeof(actual);
#endif
    assert(getsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&actual), &length) == 0);
    return actual;
}
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

class ScopedServerSocketFailpoint {
  public:
    ScopedServerSocketFailpoint(std::size_t send_failures, std::size_t nonblocking_failures) {
        chunkdb::SetServerSocketConfigFailpointForTests(send_failures, nonblocking_failures);
    }

    ~ScopedServerSocketFailpoint() {
        chunkdb::SetServerSocketConfigFailpointForTests(0, 0);
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
    [[nodiscard]] SocketHandle listening_socket() const noexcept { return socket_; }

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
    if (header != "%14") {
        throw std::runtime_error("unexpected HELLO reply: " + header);
    }
    const auto read_value = [&](auto&& self) -> std::string {
        std::string value = text();
        if (value.starts_with('*') || value.starts_with('%')) {
            const auto count = std::stoull(value.substr(1)) * (value[0] == '%' ? 2U : 1U);
            for (std::size_t i = 0; i < count; ++i) (void)self(self);
            return value;
        }
        if (value.starts_with(':') || value.starts_with('(')) return value.substr(1);
        if (value.starts_with('$')) return text();
        if (value == "_") return {};
        throw std::runtime_error("unexpected HELLO reply value: " + value);
    };
    std::unordered_map<std::string, std::string> fields;
    for (int i = 0; i < 14; ++i) {
        if (text().rfind('$', 0) != 0) {
            throw std::runtime_error("HELLO reply key is not a bulk string");
        }
        const std::string key = text();
        std::string value = read_value(read_value);
        fields.emplace(key, value);
    }
    return fields;
}

// HELLO 3 USER and AUTH (SCRAM-SHA-256) over a client connection; returns
// the reply that ends the login: the first line of the HELLO map, or the
// error. A successful login's map is read and its server signature checked.
template <typename Client>
std::string LoginReply(Client& client, const std::string& user, const std::string& password,
    std::uint64_t* connection_id = nullptr) {
    const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce());
    client.SendBytes(chunkdb::test::HelloUserBytes(login, user));
    const std::string first = client.ReadLine();
    if (first.rfind("+SCRAM ", 0) != 0) {
        return first;
    }
    chunkdb::test::FeedPhaseWatchdog::Phase("client: build prepared AUTH response");
    const auto step = chunkdb::test::AuthBytes(login, password, first);
    client.SendBytes(step.bytes);
    const auto fields = ReadHelloReply(client);
    if (fields.at("protocol") != "3" || fields.at("server_signature") != step.server_signature) {
        throw std::runtime_error("unexpected HELLO reply after AUTH");
    }
    if (connection_id) *connection_id = std::stoull(fields.at("connection_id"));
    return "%14\r\n";
}

// A login with a wrong password: the error line.
template <typename Client>
std::string FailedLoginReply(Client& client, const std::string& user, const std::string& /*password*/) {
    const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce());
    client.SendBytes(chunkdb::test::HelloUserBytes(login, user));
    const std::string first = client.ReadLine();
    if (first.rfind("+SCRAM ", 0) != 0) {
        return first;
    }
    chunkdb::test::FeedPhaseWatchdog::Phase("client: send rejected AUTH response");
    // Wrong-password cryptography is covered by the RFC exchange tests.
    // The timed socket fixture needs an invalid proof without key derivation.
    client.SendBytes(chunkdb::test::WrongAuthBytes(first));
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
        if (reply != "%14\r\n") {
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
#ifndef _WIN32
                // The 200 ms send timeout exists for the reads' deadlines. A
                // blocking send that times out has queued nothing (POSIX), so
                // a server that pauses while draining a large request is not
                // a failure; a closed or reset connection still is.
                if (written < 0 && IsWouldBlockError()) {
                    continue;
                }
#endif
                throw std::runtime_error("failed to send client bytes " + ReadContext() +
                    " sent=" + std::to_string(offset) + " of=" + std::to_string(data.size()));
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
        RecvTrace trace;
        while (true) {
            if (test_read_deadline_ && Clock::now() >= *test_read_deadline_) throw std::runtime_error("feed test read deadline");
#ifdef _WIN32
            const int read = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const ssize_t read = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            trace.Returned(read > 0);
            if (read == 0) {
                throw std::runtime_error("socket closed while waiting for line " + ReadContext() + trace.Describe());
            }
            if (read < 0) {
                if (IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("recv failed while waiting for line " + ReadContext() + trace.Describe());
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

    // Bytes received but not yet read as lines.
    [[nodiscard]] const std::string& Received() const noexcept { return pending_; }

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

    // How one read waited. recv returns at least every 200 ms (the receive
    // timeout) while this thread runs, so a gap well above that is time the
    // thread did not run (the host or VM was paused); short gaps after the
    // last byte are time in which no bytes arrived.
    class RecvTrace {
      public:
        void Returned(bool bytes) {
            const auto now = Clock::now();
            longest_gap_ = std::max(longest_gap_, now - last_return_);
            last_return_ = now;
            if (bytes) last_byte_ = now;
            else ++empty_returns_;
        }
        [[nodiscard]] std::string Describe() const {
            const auto ms = [](Clock::duration d) {
                return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
            };
            const auto now = Clock::now();
            return " read_ms=" + ms(now - started_) + " since_last_byte_ms=" + ms(now - last_byte_) +
                " longest_recv_gap_ms=" + ms(longest_gap_) + " empty_recvs=" + std::to_string(empty_returns_);
        }

      private:
        Clock::time_point started_ = Clock::now();
        Clock::time_point last_return_ = started_;
        Clock::time_point last_byte_ = started_;
        Clock::duration longest_gap_{};
        std::size_t empty_returns_ = 0;
    };

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
        RecvTrace trace;

        while (out.size() < size) {
            if (test_read_deadline_ && Clock::now() >= *test_read_deadline_) {
                throw std::runtime_error("test read deadline " + ReadContext() +
                    " expected=" + std::to_string(size) + " received=" + std::to_string(out.size()) + trace.Describe());
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
            trace.Returned(read > 0);
            if (read <= 0) {
                if (read < 0 && IsWouldBlockError()) {
                    continue;
                }
                throw std::runtime_error("socket closed while reading exact payload " + ReadContext() +
                    " expected=" + std::to_string(size) + " received=" + std::to_string(out.size()) + trace.Describe());
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

// The common name of `certificate`'s subject, or empty. Reads the name entry
// itself, which OpenSSL 3 and 4 both offer without deprecation.
std::string CommonNameOf(const X509* certificate) {
    const auto* subject = X509_get_subject_name(certificate);
    const int index = subject == nullptr ? -1 : X509_NAME_get_index_by_NID(subject, NID_commonName, -1);
    if (index < 0) return {};
    const auto* data = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(subject, index));
    if (data == nullptr) return {};
    return std::string(reinterpret_cast<const char*>(ASN1_STRING_get0_data(data)),
                       static_cast<std::size_t>(ASN1_STRING_length(data)));
}

class TlsClient {
  public:
    // `max_version`: the highest TLS version offered, e.g. TLS1_2_VERSION;
    // 0 offers every version the library supports. `prefix`: bytes sent in
    // the clear before the handshake, such as a PROXY protocol header.
    TlsClient(std::string host, std::uint16_t port, int max_version = 0, const std::string& prefix = {})
        : host_(std::move(host)), port_(port) {
        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();
        context_ = SSL_CTX_new(TLS_client_method());
        if (context_ == nullptr) {
            throw std::runtime_error("failed to create TLS client context");
        }
        SSL_CTX_set_verify(context_, SSL_VERIFY_NONE, nullptr);
        if (max_version != 0 && SSL_CTX_set_max_proto_version(context_, max_version) != 1) {
            SSL_CTX_free(context_);
            context_ = nullptr;
            throw std::runtime_error("failed to set the TLS client's highest version");
        }
        try {
            socket_ = ConnectSocket(host_, port_);
            for (std::size_t sent = 0; sent < prefix.size();) {
                const auto n = send(socket_, prefix.data() + sent, static_cast<int>(prefix.size() - sent), 0);
                if (n <= 0) throw std::runtime_error("failed to send bytes before the TLS handshake");
                sent += static_cast<std::size_t>(n);
            }
        } catch (...) {
            // The destructor does not run for a constructor that throws.
            if (socket_ != kInvalidSocket) {
                CloseSocket(socket_);
                socket_ = kInvalidSocket;
            }
            SSL_CTX_free(context_);
            context_ = nullptr;
            throw;
        }
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

    // The common name of the certificate the server presented.
    [[nodiscard]] std::string PeerCommonName() const {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
        X509* certificate = SSL_get1_peer_certificate(session_);
#else
        X509* certificate = SSL_get_peer_certificate(session_);
#endif
        if (certificate == nullptr) return {};
        auto name = CommonNameOf(certificate);
        X509_free(certificate);
        return name;
    }

    // The common names of the certificates the server sent, in order.
    [[nodiscard]] std::vector<std::string> PeerChainCommonNames() const {
        std::vector<std::string> names;
        STACK_OF(X509)* chain = SSL_get_peer_cert_chain(session_);
        for (int i = 0; chain != nullptr && i < sk_X509_num(chain); ++i) {
            names.push_back(CommonNameOf(sk_X509_value(chain, i)));
        }
        return names;
    }

    [[nodiscard]] int Version() const { return SSL_version(session_); }

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
        if (reply != "%14\r\n") {
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

    std::string ReadReply() {
        const auto header = ReadLine();
        std::string reply = header;
        if (header.starts_with('$')) {
            const auto length = std::stoull(header.substr(1));
            std::array<char, 4096> buffer{};
            while (pending_.size() < length + 2U) {
                const auto read = Read(buffer.data(), static_cast<int>(buffer.size()));
                if (read <= 0) throw std::runtime_error("TLS socket closed inside reply body");
                pending_.append(buffer.data(), static_cast<std::size_t>(read));
            }
            if (pending_.compare(length, 2U, "\r\n") != 0U)
                throw std::runtime_error("TLS bulk body is not terminated by CRLF");
            reply.append(pending_.data(), length + 2U);
            pending_.erase(0U, length + 2U);
        } else if (header.starts_with('*') || header.starts_with('%')) {
            const auto count = std::stoull(header.substr(1)) * (header[0] == '%' ? 2U : 1U);
            for (std::size_t i = 0U; i < count; ++i) reply += ReadReply();
        }
        return reply;
    }

    bool WaitForClose(std::chrono::milliseconds timeout) {
        test_read_deadline_ = Clock::now() + timeout;
        std::array<char, 4096> buffer{};
        while (Clock::now() < *test_read_deadline_) {
            const auto count = Read(buffer.data(), static_cast<int>(buffer.size()));
            if (count <= 0) return true;
            pending_.append(buffer.data(), static_cast<std::size_t>(count));
        }
        return false;
    }

    const std::string& Received() const noexcept { return pending_; }

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

    [[nodiscard]] std::string Text() const {
        std::lock_guard lock(lines_mutex_);
        std::string text;
        for (const auto& line : lines_) text += line + '\n';
        return text;
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

    void JoinFailed(std::string_view expected) {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: join expected Run failure without Stop");
        if (thread.joinable()) thread.join();
        const auto error = RunError();
        if (!error) throw std::runtime_error("expected server Run failure was not reported");
        try { std::rethrow_exception(error); }
        catch (const std::runtime_error& reported) {
            if (std::string_view(reported.what()) != expected) throw;
        }
        // Consume only the checked failure; the destructor still reports any
        // unexpected background Run error from ordinary tests.
        { std::lock_guard lock(run_error_mutex); run_error = {}; }
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
                // With PROXY headers a TLS handshake needs one first; a
                // connection alone shows the listener is up.
                if (tls_enabled && !saved_server_config.proxy_protocol_v2) {
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
        // Functional logins include software SCRAM; deadline tests override this budget.
        .client_io_timeout_ms = 30000,
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

template <typename Client>
void TestIdleConnectionsReleaseWorker(bool tls) {
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    config.tls_enabled = tls;
    ServerHarness harness(tls ? "idle-worker-tls" : "idle-worker-plain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    std::vector<std::unique_ptr<Client>> clients;
    for (std::size_t i = 0U; i < 8U; ++i) {
        auto client = std::make_unique<Client>("127.0.0.1", harness.port);
        client->SetReadDeadline(std::chrono::seconds(10));
        client->Hello();
        clients.push_back(std::move(client));
    }
    auto& transaction = *clients.front();
    transaction.SendLine("BEGIN");
    assert(transaction.ReadLine() == "+OK\r\n");
    transaction.SendLine("SET BLOCK 0 0 IN default bits = b'1000'");
    assert(transaction.ReadLine() == "_\r\n");
    for (std::size_t i = 1U; i < clients.size(); ++i) {
        clients[i]->SendBytes("PING\r\nPING\r\nPING\r\n");
        for (std::size_t reply = 0U; reply < 3U; ++reply)
            assert(clients[i]->ReadLine() == "+PONG\r\n");
    }
    transaction.SendLine("GET BLOCK 0 0 FROM default");
    assert(transaction.ReadLine() == "*1\r\n");
    assert(transaction.ReadLine() == "$1\r\n");
    assert(transaction.ReadLine() == "\x01\r\n");
    transaction.SendLine("COMMIT");
    (void)ReadVersion(transaction);
    clients.back()->SendLine("GET BLOCK 0 0 FROM default");
    assert(clients.back()->ReadLine() == "*1\r\n");
    assert(clients.back()->ReadLine() == "$1\r\n");
    assert(clients.back()->ReadLine() == "\x01\r\n");
}

class QueuedOwnershipHook : public chunkdb::FeedDeliveryTestHook {
  public:
    explicit QueuedOwnershipHook(bool pause_scan) : pause_scan_(pause_scan) {}
    void Run(Point point, std::size_t value) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeStatementStart && !worker_) {
            worker_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return release_worker_; });
        }
        if (pause_scan_ && point == Point::kBeforeQueuedStopScan && value != 0U && !io_) {
            io_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return release_io_; });
        }
        if (!pause_scan_ && point == Point::kAfterQueuedStopClaim && value == target_id_) {
            io_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return release_io_; });
        }
        if (point == Point::kAfterStatementStartRefused) { refused_ = true; changed_.notify_all(); }
        if (point == Point::kWorkerReady && refused_) { drained_ = true; changed_.notify_all(); }
    }
    void WaitWorker() { Wait([&] { return worker_; }); }
    void WaitIo() { Wait([&] { return io_; }); }
    void WaitRefused() { Wait([&] { return drained_; }); }
    void SetTarget(std::uint64_t id) { std::lock_guard lock(mutex_); target_id_ = id; }
    void ReleaseWorker() { { std::lock_guard lock(mutex_); release_worker_ = true; } changed_.notify_all(); }
    void ReleaseIo() { { std::lock_guard lock(mutex_); release_io_ = true; } changed_.notify_all(); }
  private:
    template <typename Predicate> void Wait(Predicate predicate) {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), predicate));
    }
    bool pause_scan_, worker_ = false, io_ = false, refused_ = false, drained_ = false, release_worker_ = false, release_io_ = false;
    std::uint64_t target_id_ = 0U;
    std::mutex mutex_;
    std::condition_variable changed_;
};

void TestExpiredQueueRetainsTransportOwner() {
    QueuedOwnershipHook hook(true);
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("expired-queue-owner", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient target("127.0.0.1", harness.port);
    target.SetReadDeadline(std::chrono::seconds(10));
    target.SendLine("HELLO 3 WITH statement_timeout_ms=1000");
    const auto hello = ReadHelloReply(target);
    const auto id = std::stoull(hello.at("connection_id"));
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    target.SendLine("PING");
    hook.WaitWorker(); hook.WaitIo();
    const auto statement = chunkdb::FeedDeliveryTestAccess::CurrentStatement(*harness.server, id);
    assert(statement && statement->deadline());
    std::mutex deadline_mutex;
    std::condition_variable deadline_wait;
    std::unique_lock deadline_lock(deadline_mutex);
    (void)deadline_wait.wait_until(deadline_lock, *statement->deadline(), [] { return false; });
    hook.ReleaseWorker(); hook.WaitRefused();
    assert(chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, id));
    hook.ReleaseIo();
    assert(target.ReadLine().starts_with("-ERR TIMEOUT applied=false"));
    target.SendLine("PING");
    assert(target.ReadLine() == "+PONG\r\n");
}

void TestStopDoesNotCloseQueuedReplyOwner() {
    QueuedOwnershipHook hook(false);
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("stop-queued-reply", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient worker("127.0.0.1", harness.port); worker.Hello();
    RawClient target("127.0.0.1", harness.port);
    target.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(target).at("connection_id"));
    RawClient controller("127.0.0.1", harness.port); controller.Hello();
    hook.SetTarget(id);
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING"); hook.WaitWorker();
    target.SendLine("PING");
    controller.SendLine("CANCEL " + std::to_string(id));
    hook.WaitIo();
    harness.server->Stop();
    assert(chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, id));
    hook.ReleaseIo(); hook.ReleaseWorker();
    harness.JoinStopped();
    assert(target.WaitForClose(std::chrono::seconds(10)));
    assert(!chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, id));
}

class ProgressiveReplyHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t bytes) override {
        if (point == Point::kBeforeReplyBufferWrite) assert(bytes <= 65536U);
        if (point == Point::kAfterReplyBufferWrite && bytes == 65536U) {
            std::unique_lock lock(mutex_);
            if (entered_) return;
            entered_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        }
    }
    void Wait() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return entered_; }));
    }
    void Release() { { std::lock_guard lock(mutex_); released_ = true; } changed_.notify_all(); }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false;
};

template <typename Client>
void TestProgressiveReplyOwnsSnapshot(bool tls, bool area) {
    ProgressiveReplyHook hook;
    auto store = BaseStoreConfig();
    store.geometry.chunk_width_blocks = 256U;
    store.geometry.chunk_height_blocks = 256U;
    store.geometry.block_bits = 16U;
    auto config = BaseServerConfig(); config.tls_enabled = tls; config.client_io_timeout_ms = 30000U;
    ServerHarness harness("progressive-snapshot", store, chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { ProgressiveReplyHook& hook; ~Release() { hook.Release(); } } release{hook};
    chunkdb::SessionState writer;
    (void)harness.engine->Execute(writer, "HELLO 3");
    assert(!harness.engine->Execute(writer, "SET BLOCK 0 0 IN default bits = b'1000000000000000'").starts_with('-'));
    const std::string query = area ? "GET AREA 0 0 TO 0 0 FROM default" : "GET CHUNK 0 0 FROM default";
    const auto expected = harness.engine->Execute(writer, query);
    Client reader("127.0.0.1", harness.port); reader.SetReadDeadline(std::chrono::seconds(20)); reader.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    reader.SendLine(query);
    hook.Wait();
    auto changed = std::async(std::launch::async, [&] {
        assert(!harness.engine->Execute(writer, "SET BLOCK 0 0 IN default bits = b'0100000000000000'").starts_with('-'));
        assert(harness.engine->Execute(writer, "ALTER TABLE default ADD COLUMN light u8") == "+OK\r\n");
    });
    assert(changed.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    changed.get();
    hook.Release();
    assert(reader.ReadReply() == expected);
    reader.SendLine("PING"); assert(reader.ReadLine() == "+PONG\r\n");
}

template <typename Client>
void TestKillReadReplyReleasesCredit(bool tls) {
    ProgressiveReplyHook hook;
    auto store = BaseStoreConfig();
    store.geometry.chunk_width_blocks = 256U;
    store.geometry.chunk_height_blocks = 256U;
    store.geometry.block_bits = 16U;
    auto config = BaseServerConfig(); config.tls_enabled = tls; config.worker_threads = 1U;
    config.client_io_timeout_ms = 30000U;
    ServerHarness harness("kill-read-reply", store, chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { ProgressiveReplyHook& hook; ~Release() { hook.Release(); } } release{hook};
    {
        auto lease = harness.catalog->Require("default")->Acquire();
        lease->store().SetBlockBits(0, 0, "1000000000000000");
    }
    Client reader("127.0.0.1", harness.port); reader.SetReadDeadline(std::chrono::seconds(10));
    reader.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(reader).at("connection_id"));
    Client controller("127.0.0.1", harness.port); controller.SetReadDeadline(std::chrono::seconds(10)); controller.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    reader.SendLine("GET CHUNK 0 0 FROM default");
    hook.Wait();
    controller.SendLine("KILL CONNECTION " + std::to_string(id));
    assert(controller.ReadReply().ends_with("#t\r\n"));
    hook.Release();
    const auto header = reader.ReadLine();
    assert(header.starts_with('$'));
    const auto expected_body = std::stoull(header.substr(1)) + 2U;
    assert(reader.WaitForClose(std::chrono::seconds(5)));
    assert(reader.Received().size() < expected_body);
    assert(!chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, id));
    auto returned = chunkdb::FeedDeliveryTestAccess::ReserveReplyCredit(*harness.server, 2147483648ULL);
    assert(returned);
}

class BufferedPublicationHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        if (point != Point::kBeforeBufferedStatementPublication) return;
        std::unique_lock lock(mutex_);
        entered_ = true; changed_.notify_all();
        changed_.wait(lock, [&] { return released_; });
    }
    void Wait() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(5), [&] { return entered_; }));
    }
    void Release() { { std::lock_guard lock(mutex_); released_ = true; } changed_.notify_all(); }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false;
};
void TestKilledBufferedStatementCannotPublish() {
    BufferedPublicationHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("kill-buffered-publication", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { BufferedPublicationHook& hook; ~Release() { hook.Release(); } } release{hook};
    RawClient target("127.0.0.1", harness.port); target.SetReadDeadline(std::chrono::seconds(10)); target.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(target).at("connection_id"));
    RawClient controller("127.0.0.1", harness.port); controller.SetReadDeadline(std::chrono::seconds(10)); controller.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    target.SendBytes("PING\r\nSET BLOCK 0 0 IN default bits = b'1000'\r\n");
    hook.Wait();
    assert(target.ReadLine() == "+PONG\r\n");
    controller.SendLine("KILL CONNECTION " + std::to_string(id));
    assert(controller.ReadReply().ends_with("#t\r\n"));
    hook.Release();
    assert(target.WaitForClose(std::chrono::seconds(5)));
    auto lease = harness.catalog->Require("default")->Acquire();
    assert(!lease->store().GetBlock(0, 0));
}

class BufferedWorkerHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override { if (point == Point::kBeforeWorkerHandle) ++turns; }
    std::atomic<std::size_t> turns{0U};
};

void TestBufferedRequestsRetainWorker() {
    BufferedWorkerHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("buffered-worker", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient client("127.0.0.1", harness.port); client.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    client.SendBytes("SET BLOCK 0 0 IN default bits = $1\r\n$1\r\n\x01\r\nPING\r\nPING\r\n");
    (void)ReadVersion(client);
    assert(client.ReadLine() == "+PONG\r\n");
    assert(client.ReadLine() == "+PONG\r\n");
    assert(hook.turns.load() == 1U);
}

class IdleDispatchFailureHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        if (point == Point::kBeforeResumedClientQueue && !failed_.exchange(true)) {
            reached_.set_value();
            throw std::bad_alloc();
        }
    }
    void Wait() {
        auto reached = reached_.get_future();
        assert(reached.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
    }
  private:
    std::atomic<bool> failed_{false};
    std::promise<void> reached_;
};

void TestIdleDispatchFailureClosesEveryOwner() {
    IdleDispatchFailureHook failure;
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    ServerHarness harness("idle-dispatch-failure", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient first("127.0.0.1", harness.port);
    first.Hello();
    RawClient second("127.0.0.1", harness.port);
    second.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &failure);
    first.SendLine("PING");
    failure.Wait();
    assert(first.WaitForClose(std::chrono::seconds(10)));
    assert(second.WaitForClose(std::chrono::seconds(10)));
}

class IdleQueuePressureHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t value) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeWorkerHandle && !entered_) {
            entered_ = true;
            changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        }
        if (point == Point::kIdleQueueBackpressure) {
            assert(value == 1U);
            backpressure_ = true;
            changed_.notify_all();
        }
    }
    void WaitWorker() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return entered_; }));
    }
    void WaitBackpressure() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return backpressure_; }));
    }
    void Release() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false, backpressure_ = false;
};

void FreezeUserAdmission(ServerHarness& harness) {
    harness.server->Stop();
    harness.JoinStopped();
    harness.saved_engine_config.users = std::make_shared<chunkdb::UserRegistry>(harness.data_dir, std::nullopt,
        std::array<std::uint8_t, 32>{}, false, [] { return chunkdb::UserAdmissionTime{}; });
    harness.Restart();
}

class RateQueueHook : public IdleQueuePressureHook {
  public:
    void SetTarget(std::uint64_t target) { target_ = target; }
    void Run(Point point, std::size_t value) override {
        IdleQueuePressureHook::Run(point, value);
        if (point == Point::kAfterStatementQueued && value == target_) {
            { std::lock_guard lock(mutex_); queued_ = true; }
            changed_.notify_all();
        }
    }
    void WaitQueued() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return queued_; }));
    }
  private:
    std::uint64_t target_ = 0U;
    std::mutex mutex_;
    std::condition_variable changed_;
    bool queued_ = false;
};

class ShutdownPublicationHook : public RateQueueHook {
  public:
    void Run(Point point, std::size_t value) override {
        RateQueueHook::Run(point, value);
        if (point == Point::kAfterRunningStopPublication && value != 0U) {
            std::unique_lock lock(mutex_);
            stopped_ = true; changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        }
    }
    void WaitPublication() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return stopped_; }));
    }
    void ReleasePublication() { { std::lock_guard lock(mutex_); released_ = true; } changed_.notify_all(); }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool stopped_ = false, released_ = false;
};

void TestStopPublicationGapClosesQueuedOwner() {
    ShutdownPublicationHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("stop-publication-gap", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient worker("127.0.0.1", harness.port); worker.Hello();
    RawClient target("127.0.0.1", harness.port); target.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(target).at("connection_id"));
    hook.SetTarget(id);
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING"); hook.WaitWorker();
    target.SendLine("PING"); hook.WaitQueued();
    auto stopping = std::async(std::launch::async, [&] { harness.server->Stop(); });
    hook.WaitPublication();
    chunkdb::FeedDeliveryTestAccess::WakeTransport(*harness.server);
    assert(target.WaitForClose(std::chrono::seconds(10)));
    assert(!chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, id));
    hook.ReleasePublication(); hook.Release();
    stopping.get();
    harness.JoinStopped();
}

void TestQueuedCancellationConsumesUserRate() {
    RateQueueHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("queue-cancel-rate", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = true}, config);
    FreezeUserAdmission(harness);
    RawClient worker("127.0.0.1", harness.port); worker.Login();
    RawClient target("127.0.0.1", harness.port); target.SetReadDeadline(std::chrono::seconds(10));
    std::uint64_t id = 0U;
    assert(LoginReply(target, kAdminUser, kAdminPassword, &id) == "%14\r\n");
    assert(id != 0U);
    RawClient controller("127.0.0.1", harness.port); controller.Login();
    hook.SetTarget(id);
    struct Release { RateQueueHook& hook; ~Release() { hook.Release(); } } release{hook};
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING"); hook.WaitWorker();
    harness.saved_engine_config.users->SetLimits(kAdminUser, std::nullopt, 1U);
    target.SendLine("PING"); hook.WaitQueued();
    controller.SendLine("CANCEL " + std::to_string(id));
    assert(controller.ReadLine() == "%1\r\n");
    assert(controller.ReadLine() == "$9\r\n");
    assert(controller.ReadLine() == "cancelled\r\n");
    assert(controller.ReadLine() == "#t\r\n");
    assert(target.ReadLine().starts_with("-ERR CANCELLED applied=false"));
    target.SendLine("PING");
    assert(target.ReadLine().starts_with("-ERR RATE_LIMITED reason=statements_per_sec"));
    hook.Release();
    assert(worker.ReadLine() == "+PONG\r\n");
}

class RateFeedPressureHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t bytes) override {
        if (point == Point::kAfterFeedReplyQueue) assert(bytes <= 512U);
        if (point == Point::kFeedInputBackpressure) {
            { std::lock_guard lock(mutex_); paused_ = true; }
            changed_.notify_all();
        }
    }
    void WaitPaused() {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] { return paused_; }));
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    bool paused_ = false;
};

void TestOrdinaryWatchRateRepliesAreBounded() {
    RateFeedPressureHook hook;
    auto config = BaseServerConfig(); config.feed_buffer_bytes = 512U; config.max_watches = 1U;
    ServerHarness harness("watch-rate-bound", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = true}, config);
    FreezeUserAdmission(harness);
    RawClient watch("127.0.0.1", harness.port); watch.Login();
    RawClient caller("127.0.0.1", harness.port); caller.Login();
    caller.SendLine("ALTER TABLE default SET feed_buffer_bytes = 512");
    assert(caller.ReadLine() == "+OK\r\n");
    watch.SendLine("WATCH default");
    assert(watch.ReadReply().starts_with('%'));
    harness.saved_engine_config.users->SetLimits(kAdminUser, std::nullopt, 1U);
    caller.SendLine("PING"); assert(caller.ReadLine() == "+PONG\r\n");
    chunkdb::FeedDeliveryTestAccess::SetHook(*harness.server, &hook);
    std::string flood;
    for (std::size_t i = 0U; i < 64U; ++i) flood += "UNWATCH\r\n";
    watch.SendBytes(flood);
    hook.WaitPaused();
}

void TestIdleDispatchRespectsQueueCredit() {
    IdleQueuePressureHook hook;
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    config.max_pending_clients = 1U;
    ServerHarness harness("idle-dispatch-credit", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { IdleQueuePressureHook& hook; ~Release() { hook.Release(); } } release{hook};
    RawClient worker("127.0.0.1", harness.port);
    worker.Hello();
    worker.SendLine("PING");
    assert(worker.ReadLine() == "+PONG\r\n");
    std::vector<std::unique_ptr<RawClient>> idle;
    for (std::size_t i = 0U; i < 3U; ++i) {
        auto client = std::make_unique<RawClient>("127.0.0.1", harness.port);
        client->SetReadDeadline(std::chrono::seconds(20));
        client->Hello();
        idle.push_back(std::move(client));
    }
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING");
    hook.WaitWorker();
    for (const auto& client : idle) client->SendLine("PING");
    hook.WaitBackpressure();
    hook.Release();
    assert(worker.ReadLine() == "+PONG\r\n");
    for (const auto& client : idle) assert(client->ReadLine() == "+PONG\r\n");
}

template <typename Client>
void TestHelloIndependentOfStatementWorkers(bool tls) {
    IdleQueuePressureHook hook;
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    config.tls_enabled = tls;
    ServerHarness harness(tls ? "hello-busy-tls" : "hello-busy-plain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { IdleQueuePressureHook& hook; ~Release() { hook.Release(); } } release{hook};
    Client worker("127.0.0.1", harness.port);
    worker.SetReadDeadline(std::chrono::seconds(10));
    worker.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING");
    hook.WaitWorker();
    Client newcomer("127.0.0.1", harness.port);
    newcomer.SetReadDeadline(std::chrono::seconds(10));
    newcomer.Hello();
    hook.Release();
    assert(worker.ReadLine() == "+PONG\r\n");
    newcomer.SendLine("PING");
    assert(newcomer.ReadLine() == "+PONG\r\n");
}

template <typename Client>
void TestPartialInputDoesNotHoldWorker(bool tls) {
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    config.tls_enabled = tls;
    ServerHarness harness(tls ? "partial-worker-tls" : "partial-worker-plain", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client partial("127.0.0.1", harness.port);
    partial.SetReadDeadline(std::chrono::seconds(10));
    partial.Hello();
    partial.SendBytes("SET BLOCK 0 0 IN default bits = $1\r\n$1\r\n");
    Client ready("127.0.0.1", harness.port);
    ready.SetReadDeadline(std::chrono::seconds(10));
    ready.Hello();
    ready.SendLine("PING");
    assert(ready.ReadLine() == "+PONG\r\n");
    partial.SendBytes("\x01\r\nPING\r\n");
    (void)ReadVersion(partial);
    assert(partial.ReadLine() == "+PONG\r\n");
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
        assert(client.ReadLine() == "-ERR PROTOCOL reason=protocol expected HELLO 3\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    // AUTH <token> of protocol 2 now reads as an AUTH without HELLO 3 USER.
    const std::string stray_auth = "-ERR PROTOCOL reason=protocol AUTH follows HELLO 3 USER <name> $1\r\n";
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
    assert(client.ReadLine().rfind("-ERR PROTOCOL reason=protocol HELLO was already sent", 0) == 0);
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

    assert(FailedLoginReply(client, kAdminUser, "bad") == "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the username and password in your connection URI\r\n");
    assert(FailedLoginReply(client, "nobody", kAdminPassword) == "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the username and password in your connection URI\r\n");

    // HELLO has no TABLE option: statements name their table. The token
    // option is gone.
    client.SendBytes("HELLO 3 USER admin $1 TABLE default\r\n$-1\r\n");
    assert(client.ReadLine().starts_with("-ERR SYNTAX reason=syntax column"));

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
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(current) + " reason=version_mismatch\r\n");
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
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT reason=invalid_argument the chunk starts with its version and schema version (16 bytes), got 2", 0) == 0);
    assert(chunk_absent(3));

    // Invalid statements drain their lexical frame count, then preserve
    // the next pipelined request.
    for (const char* line :
         {"SET CHUNK 3 0 IN default $1 BOGUS", "SET CHUNK 3 0 IN default $1x",
          "SET CHUNK 3 $1"}) {
        RawClient bad("127.0.0.1", harness.port);
        bad.Hello();
        bad.SendBytes("PING\r\n" + std::string(line) + "\r\n" + Frame(ChunkFormOf(full_presence, payload)) + "PING\r\n");
        assert(bad.ReadLine() == "+PONG\r\n");
        assert(bad.ReadLine().rfind("-ERR SYNTAX", 0) == 0);
        assert(bad.ReadLine() == "+PONG\r\n");
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
        assert(bad_terminator.ReadLine() == "-ERR PROTOCOL reason=protocol a parameter must be followed by an empty line\r\n");
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
            "-ERR PROTOCOL reason=protocol $1 is longer than its column holds (" + std::to_string(frame_limit) + " bytes)\r\n");
        assert(oversize.WaitForClose(std::chrono::seconds(5)));
    }
    // Frame headers that cannot be framed (unparsable length) are refused
    // and the connection closed, since the frame length is unknown.
    for (const char* header : {"$18x", "$+18", "18", "$"}) {
        RawClient bad("127.0.0.1", harness.port);
        bad.Hello();
        bad.SendBytes("SET CHUNK 3 0 IN default $1\r\n" + std::string(header) + "\r\n");
        assert(bad.ReadLine().rfind("-ERR PROTOCOL reason=protocol expected a parameter frame", 0) == 0);
        assert(bad.WaitForClose(std::chrono::seconds(5)));
    }
    assert(chunk_absent(3));
}

void TestParameterFrameSum() {
    ServerHarness harness("parameter-frame-sum", BaseStoreConfig(),
                          chunkdb::EngineConfig{.require_auth = false}, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    client.SendLine("CREATE TABLE large_columns (f u8 REQUIRED, b1 bytes(16777216) NULL, "
                    "b2 bytes(16777216) NULL, b3 bytes(16777216) NULL, "
                    "b4 bytes(16777216) NULL, b5 bytes(16777216) NULL) CHUNK 4 x 4");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendBytes("SET BLOCK 0 0 IN large_columns f = 7, b1 = $1, b2 = $2, "
                     "b3 = $3, b4 = $4, b5 = $5\r\n" +
                     Frame("a") + Frame("b") + Frame("c") + Frame("d") + Frame("e"));
    (void)ReadVersion(client);
    client.SendLine("GET BLOCK 0 0 FROM large_columns");
    assert(client.ReadLine() == "*6\r\n");
    assert(client.ReadLine() == ":7\r\n");
    for (const char* value : {"a", "b", "c", "d", "e"}) assert(client.ReadBulkText() == value);
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");

    // NULL contributes no bytes; the remaining frames and next statement
    // must still be consumed at their exact boundaries.
    client.SendBytes("SET BLOCK 1 0 IN large_columns f = 8, b1 = $1, b2 = $2, "
                     "b3 = $3, b4 = $4, b5 = $5\r\n$-1\r\n" +
                     Frame("b") + "$-1\r\n" + Frame("d") + "$-1\r\nPING\r\n");
    (void)ReadVersion(client);
    assert(client.ReadLine() == "+PONG\r\n");

    constexpr std::size_t kFrameBytes = chunkdb::kMaxVariableValueBytes;
    constexpr std::size_t kRemainder = chunkdb::kMaxColumnsPerTable * 8U;
    const std::string payload(kFrameBytes, 'x');
    for (const bool over_limit : {false, true}) {
        RawClient bounded("127.0.0.1", harness.port);
        bounded.Hello();
        bounded.SendLine("SET BLOCK 0 0 IN large_columns f = 9, "
                         "b1 = $1, b1 = $2, b1 = $3, b1 = $4, b1 = $5");
        // Reuse one client buffer; do not construct a 64MiB combined request.
        for (int frame = 0; frame < 4; ++frame) {
            bounded.SendBytes("$" + std::to_string(kFrameBytes) + "\r\n");
            bounded.SendBytes(payload);
            bounded.SendBytes("\r\n");
        }
        const std::size_t final_length = kRemainder + (over_limit ? 1U : 0U);
        bounded.SendBytes("$" + std::to_string(final_length) + "\r\n");
        if (over_limit) {
            // No fifth payload is sent. Reading an immediate refusal proves
            // the server did not wait to read it; source-order validation
            // also keeps read_bytes allocation behind this check.
            assert(bounded.ReadLine() ==
                   "-ERR INVALID_ARGUMENT reason=invalid_argument the parameters of one statement take at most 67117056 bytes together\r\n");
            assert(bounded.WaitForClose(std::chrono::seconds(5)));
        } else {
            bounded.SendBytes(std::string(final_length, 'y'));
            bounded.SendBytes("\r\n");
            assert(bounded.ReadLine() == "-ERR INVALID_ARGUMENT reason=invalid_argument column b1 is given twice\r\n");
            bounded.SendLine("PING");
            assert(bounded.ReadLine() == "+PONG\r\n");
        }
    }
    // Neither a duplicate assignment nor an incomplete oversized request
    // may execute or replace the original values.
    client.SendLine("GET BLOCK 0 0 FROM large_columns");
    assert(client.ReadLine() == "*6\r\n");
    assert(client.ReadLine() == ":7\r\n");
    for (const char* value : {"a", "b", "c", "d", "e"}) assert(client.ReadBulkText() == value);
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
        assert(client.ReadLine() == "-ERR SYNTAX reason=syntax column 9: expected the end of HELLO\r\n");
        assert(client.WaitForClose(std::chrono::milliseconds(1000)));
    }
    {
        // HELLOs paced below the I/O timeout, never enough to reach
        // max_auth_failures: the connection still ends at the deadline.
        auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 1000};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 800;
        ServerHarness harness("handshake-deadline", BaseStoreConfig(), engine_cfg, server_cfg);
        // Prepare real SCRAM keys before either connection's bounded handshake.
        // Recovery tests worker availability, not software PBKDF2 throughput.
        const auto admin = harness.saved_engine_config.users->Find(kAdminUser);
        assert(admin.has_value());
        const auto& verifier = admin->verifier;
        assert(verifier.iterations == chunkdb::scram::kMinIterations);
        const auto salted = chunkdb::crypto::Pbkdf2Sha256(
            chunkdb::crypto::Bytes(kAdminPassword), verifier.salt, verifier.iterations);
        const auto client_key = chunkdb::crypto::HmacSha256(salted, chunkdb::crypto::Bytes("Client Key"));
        const auto stored_key = chunkdb::crypto::Sha256Of(client_key);
        const auto server_key = chunkdb::crypto::HmacSha256(salted, chunkdb::crypto::Bytes("Server Key"));
        assert(stored_key == verifier.stored_key && server_key == verifier.server_key);
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
            if (last.rfind("-ERR PROTOCOL reason=protocol HELLO 3 was not completed", 0) == 0) {
                closed = client.WaitForClose(std::chrono::seconds(1));
                break;
            }
            assert(last.rfind("-ERR AUTH_REQUIRED", 0) == 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
        assert(closed);
        assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(2500));
        const auto login = chunkdb::scram::StartClientLogin(kAdminUser, chunkdb::scram::NewNonce());
        RawClient ok("127.0.0.1", harness.port);
        ok.SendBytes(chunkdb::test::HelloUserBytes(login, kAdminUser));
        const auto first = ok.ReadLine();
        assert(first.rfind("+SCRAM ", 0) == 0);
        const auto server_first = chunkdb::test::ServerFirstOf(first);
        const auto salt_at = server_first.find(",s=");
        assert(server_first.rfind("r=", 0) == 0 && salt_at != std::string::npos);
        const auto nonce = server_first.substr(2, salt_at - 2);
        assert(nonce.rfind(login.nonce, 0) == 0 && nonce.size() > login.nonce.size());
        assert(server_first.substr(salt_at) == ",s=" + chunkdb::crypto::Base64Encode(verifier.salt) +
               ",i=" + std::to_string(verifier.iterations));
        const std::string without_proof = "c=biws,r=" + nonce;
        const std::string auth_message = login.first_bare + "," + server_first + "," + without_proof;
        const auto signature = chunkdb::crypto::HmacSha256(stored_key, chunkdb::crypto::Bytes(auth_message));
        chunkdb::crypto::Sha256Digest proof{};
        for (std::size_t i = 0; i < proof.size(); ++i) {
            proof[i] = static_cast<std::uint8_t>(client_key[i] ^ signature[i]);
        }
        ok.SendBytes(chunkdb::test::LineWithFrame(
            "AUTH $1", without_proof + ",p=" + chunkdb::crypto::Base64Encode(proof)));
        const auto fields = ReadHelloReply(ok);
        assert(fields.at("protocol") == "3");
        assert(fields.at("server_signature") == "v=" + chunkdb::crypto::Base64Encode(
            chunkdb::crypto::HmacSha256(server_key, chunkdb::crypto::Bytes(auth_message))));
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
            assert(line.rfind("-ERR PROTOCOL reason=protocol HELLO 3 was not completed", 0) == 0);
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
    const std::string refused = "-ERR PROTOCOL reason=protocol HELLO 3 was not completed within the I/O timeout\r\n";
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
        assert(reply == "-ERR BUSY reason=busy too many connections before HELLO from this address\r\n");
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
        assert(client.ReadLine() == "-ERR PROTOCOL reason=protocol expected HELLO 3\r\n");
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
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(written) + " reason=version_mismatch\r\n");
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
    assert(client.ReadLine() == "-ERR UNKNOWN_COMMAND reason=unknown_command column 1: unknown statement 'QUIT'\r\n");
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
    assert(response.rfind("-ERR PROTOCOL", 0) == 0);
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
    assert(response.rfind("-ERR PROTOCOL", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(2)));
}

// Protocol 3: statements, parameter frames pipelined with the next
// statement, and a frame longer than its column, which closes the
// connection unread.
template <typename Client>
void CheckMalformedHelloRecovery(std::uint16_t port) {
    {
        Client malformed("127.0.0.1", port);
        malformed.SendBytes("HELLO 3 USER admin $1 BOGUS\r\n$8\r\nPING\r\nxx\r\nHELLO 3\r\nPING\r\n");
        assert(malformed.ReadLine().starts_with("-ERR SYNTAX reason=syntax "));
        assert(ReadHelloReply(malformed).at("protocol") == "3");
        assert(malformed.ReadLine() == "+PONG\r\n");
    }
}

template <typename Client>
void CheckHelloNegotiation(std::uint16_t port) {
    {
        Client known_duplicate("127.0.0.1", port);
        for (unsigned attempt = 0U; attempt < 4U; ++attempt) {
            known_duplicate.SendBytes("HELLO 3 WITH client_name=$1,CLIENT_NAME=$2\r\n$3\r\none\r\n$3\r\ntwo\r\n");
            assert(known_duplicate.ReadLine().starts_with("-ERR INVALID_ARGUMENT reason=duplicate_option option=client_name "));
        }
        known_duplicate.SendBytes("HELLO 3\r\nPING\r\n");
        assert(ReadHelloReply(known_duplicate).at("protocol") == "3");
        assert(known_duplicate.ReadLine() == "+PONG\r\n");
    }
    {
        Client too_large("127.0.0.1", port);
        too_large.SendBytes("HELLO 3 WITH default_tag=$1\r\n$1025\r\n");
        assert(too_large.ReadLine().starts_with("-ERR PROTOCOL "));
        if constexpr (requires { too_large.WaitForClose(std::chrono::seconds(2)); })
            assert(too_large.WaitForClose(std::chrono::seconds(2)));
        else assert(too_large.WaitForRawClose(std::chrono::seconds(2)));
    }
    {
        Client client("127.0.0.1", port);
        // Unknown payload bytes never become another command, including embedded CRLF.
        client.SendBytes("HELLO 3 WITH future=$1,CLIENT_NAME=$2,statement_timeout_ms=0,default_tag=x'00'\r\n$8\r\nPING\r\nxx\r\n$3\r\napp\r\nPING\r\n");
        const auto hello = ReadHelloReply(client);
        assert(hello.at("protocol") == "3" && std::stoull(hello.at("connection_id")) != 0U);
        assert(hello.at("options") == "%2" && hello.at("not_applied") == "%2");
        assert(hello.at("idle_timeout_ms") == "1234");
        assert(client.ReadLine() == "+PONG\r\n");
    }
    {
        Client duplicate("127.0.0.1", port);
        duplicate.SendBytes("HELLO 3 WITH x=$1,X=$2\r\n$3\r\none\r\n$3\r\ntwo\r\nHELLO 3\r\nPING\r\n");
        assert(duplicate.ReadLine().starts_with("-ERR INVALID_ARGUMENT reason=duplicate_option option=x "));
        assert(ReadHelloReply(duplicate).at("protocol") == "3");
        assert(duplicate.ReadLine() == "+PONG\r\n");
    }
    {
        Client invalid("127.0.0.1", port);
        invalid.SendBytes("HELLO 3 WITH default_tag=$1\r\n$257\r\n" + std::string(257U, 'x') + "\r\nHELLO 3\r\nPING\r\n");
        assert(invalid.ReadLine().starts_with("-ERR INVALID_ARGUMENT reason=option_value option=default_tag "));
        assert(ReadHelloReply(invalid).at("protocol") == "3");
        assert(invalid.ReadLine() == "+PONG\r\n");
    }
    {
        Client unbounded("127.0.0.1", port);
        unbounded.SendBytes("HELLO 3 WITH future=$1\r\n$1025\r\n");
        assert(unbounded.ReadLine().starts_with("-ERR PROTOCOL "));
        if constexpr (requires { unbounded.WaitForClose(std::chrono::seconds(2)); })
            assert(unbounded.WaitForClose(std::chrono::seconds(2)));
        else assert(unbounded.WaitForRawClose(std::chrono::seconds(2)));
    }
}

void TestHelloNegotiation() {
    auto config = BaseServerConfig();
    config.max_unparsed_parameter_bytes = 1024U;
    config.idle_connection_timeout_ms = 1234U;
    ServerHarness harness("hello-negotiation", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 1U}, config);
    CheckHelloNegotiation<RawClient>(harness.port);
    ServerHarness retry("hello-syntax-retry", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 3U}, config);
    CheckMalformedHelloRecovery<RawClient>(retry.port);
#ifdef CHUNKDB_WITH_OPENSSL
    config.tls_enabled = true;
    ServerHarness tls("hello-negotiation-tls", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 1U}, config);
    CheckHelloNegotiation<TlsClient>(tls.port);
    ServerHarness tls_retry("hello-syntax-retry-tls", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 3U}, config);
    CheckMalformedHelloRecovery<TlsClient>(tls_retry.port);
#endif
}

void TestGrammarIndependentFrames() {
    auto config = BaseServerConfig();
    config.max_unparsed_parameter_bytes = 8U;
    ServerHarness harness("lexical-frames", BaseStoreConfig(),
                          chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    const std::vector<std::pair<std::string, std::string>> rejected{
        {"? $1\r\n$3\r\nabc\r\n", "SYNTAX"},
        {"SET BLOCK 0 0 IN default bits = $1, bits = $1\r\n$1\r\na\r\n$1\r\nb\r\n", "SYNTAX"},
        {"SET BLOCK 0 0 IN default bits = $3\r\n$0\r\n\r\n", "SYNTAX"},
        {"FUTURE $0 $65536\r\n$-1\r\n$2\r\nab\r\n", "UNKNOWN_COMMAND"},
        {"SET BLOCK 0 0 IN missing bits = $1\r\n$1\r\nx\r\n", "NO_TABLE"},
        {"SET BLOCK 0 0 IN default bits = 'unfinished $1\r\n", "SYNTAX"},
    };
    for (const auto& [request, code] : rejected) {
        client.SendBytes("PING\r\n" + request + "PING\r\n");
        assert(client.ReadLine() == "+PONG\r\n");
        assert(client.ReadLine().rfind("-ERR " + code, 0U) == 0U);
        assert(client.ReadLine() == "+PONG\r\n");
    }
    client.SendLine("CREATE TABLE framed (a text(8), b text(8), marker u8) CHUNK 4 x 4");
    const auto create_reply = client.ReadLine();
    if (create_reply != "+OK\r\n") throw std::runtime_error("create framed table: " + create_reply);
    client.SendBytes("SET BLOCK 0 0 IN framed a = $2, b = $1\r\n$3\r\none\r\n$3\r\ntwo\r\nGET BLOCK 0 0 FROM framed\r\n");
    assert(client.ReadLine().front() == ':');
    assert(client.ReadLine() == "*3\r\n");
    assert(client.ReadLine() == "$3\r\n");
    assert(client.ReadLine() == "two\r\n");
    assert(client.ReadLine() == "$3\r\n");
    assert(client.ReadLine() == "one\r\n");
    assert(client.ReadLine() == ":0\r\n");
    for (const std::string& request : {std::string("? $1\r\n$9\r\n"),
                                       std::string("? $1 $2\r\n$6\r\nabcdef\r\n$3\r\n")}) {
        RawClient excessive("127.0.0.1", harness.port);
        excessive.Hello();
        excessive.SendBytes(request);
        assert(excessive.ReadLine().rfind("-ERR PROTOCOL", 0U) == 0U);
        assert(excessive.WaitForClose(std::chrono::seconds(2)));
    }
}

void TestProtocolThreeFrames() {
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    ServerHarness harness("protocol-three", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 3");
    assert(ReadHelloReply(client).at("protocol") == "3");

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
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT reason=invalid_argument column bits cannot be NULL", 0) == 0);
    assert(client.ReadLine() == "_\r\n");

    client.SendBytes("SET BLOCK 3 1 IN default bits = $1\r\n$2\r\nxx\r\n");
    assert(client.ReadLine() == "-ERR PROTOCOL reason=protocol $1 is longer than its column holds (1 bytes)\r\n");
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
    assert(response == "%14\r\n");
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
    assert(FailedLoginReply(client, kAdminUser, "wrong") == "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the username and password in your connection URI\r\n");
    client.SendLine("PING");
    assert(client.ReadLine() == "-ERR PROTOCOL reason=protocol expected HELLO 3\r\n");
    TlsClient authed("127.0.0.1", harness.port);
    authed.Login();
    authed.SendLine("PING");
    assert(authed.ReadLine() == "+PONG\r\n");
}

// A fresh P-256 key for test certificates.
EVP_PKEY* NewTestKey() {
    EVP_PKEY_CTX* context = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
    EVP_PKEY* key = nullptr;
    const bool made = context != nullptr && EVP_PKEY_keygen_init(context) == 1 &&
                      EVP_PKEY_CTX_set_ec_paramgen_curve_nid(context, NID_X9_62_prime256v1) == 1 &&
                      EVP_PKEY_keygen(context, &key) == 1;
    EVP_PKEY_CTX_free(context);
    if (!made) throw std::runtime_error("failed to generate a test key");
    return key;
}

struct TestKey {
    EVP_PKEY* key = NewTestKey();
    TestKey() = default;
    TestKey(const TestKey&) = delete;
    TestKey& operator=(const TestKey&) = delete;
    ~TestKey() { EVP_PKEY_free(key); }
};

// A certificate for `subject` with common name `name`, issued by `issuer`
// and signed with `signer`; the caller frees it.
X509* NewTestCertificate(const std::string& name, EVP_PKEY* subject, const std::string& issuer, EVP_PKEY* signer) {
    X509* certificate = X509_new();
    if (certificate == nullptr) throw std::runtime_error("X509_new failed");
    X509_set_version(certificate, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1);
    X509_gmtime_adj(X509_getm_notBefore(certificate), -60);
    X509_gmtime_adj(X509_getm_notAfter(certificate), 3600);
    X509_set_pubkey(certificate, subject);
    // The names are built apart and copied in: OpenSSL 4 returns a
    // certificate's names as const.
    const auto set_name = [&](const std::string& common_name, bool issuer_name) {
        X509_NAME* built = X509_NAME_new();
        const bool ok = built != nullptr &&
                        X509_NAME_add_entry_by_txt(built, "CN", MBSTRING_ASC,
                                                   reinterpret_cast<const unsigned char*>(common_name.c_str()), -1, -1,
                                                   0) == 1 &&
                        (issuer_name ? X509_set_issuer_name(certificate, built) : X509_set_subject_name(certificate, built)) == 1;
        X509_NAME_free(built);
        return ok;
    };
    if (!set_name(name, false) || !set_name(issuer, true) || X509_sign(certificate, signer, EVP_sha256()) <= 0) {
        X509_free(certificate);
        throw std::runtime_error("failed to sign a test certificate");
    }
    return certificate;
}

// Writes `certificates` in order to `cert_path` and `key_on_disk` to
// `key_path`.
void WriteTestFiles(const std::filesystem::path& cert_path, const std::filesystem::path& key_path,
                    const std::vector<X509*>& certificates, EVP_PKEY* key_on_disk) {
    // BIOs rather than FILE*: OpenSSL may use another C runtime on Windows.
    BIO* cert_out = BIO_new_file(cert_path.string().c_str(), "w");
    BIO* key_out = BIO_new_file(key_path.string().c_str(), "w");
    bool written = cert_out != nullptr && key_out != nullptr &&
                   PEM_write_bio_PrivateKey(key_out, key_on_disk, nullptr, nullptr, 0, nullptr, nullptr) == 1;
    for (X509* certificate : certificates) {
        written = written && PEM_write_bio_X509(cert_out, certificate) == 1;
    }
    BIO_free(cert_out);
    BIO_free(key_out);
    if (!written) throw std::runtime_error("failed to write a test certificate");
}

// Writes a self-signed certificate for `subject` with common name `name` to
// `cert_path`, and `key_on_disk` to `key_path`: the subject's own key, or
// another one for a mismatched pair.
void WriteTestCertificate(const std::filesystem::path& cert_path, const std::filesystem::path& key_path,
                          const std::string& name, EVP_PKEY* subject, EVP_PKEY* key_on_disk) {
    const std::unique_ptr<X509, decltype(&X509_free)> certificate(NewTestCertificate(name, subject, name, subject),
                                                                  &X509_free);
    WriteTestFiles(cert_path, key_path, {certificate.get()}, key_on_disk);
}

// Writes a certificate for `leaf`, issued by an intermediate whose own
// certificate follows it in `cert_path`, and the leaf's key to `key_path`.
void WriteTestChain(const std::filesystem::path& cert_path, const std::filesystem::path& key_path,
                    const std::string& leaf_name, EVP_PKEY* leaf, const std::string& intermediate_name,
                    EVP_PKEY* intermediate) {
    const std::unique_ptr<X509, decltype(&X509_free)> leaf_certificate(
        NewTestCertificate(leaf_name, leaf, intermediate_name, intermediate), &X509_free);
    const std::unique_ptr<X509, decltype(&X509_free)> intermediate_certificate(
        NewTestCertificate(intermediate_name, intermediate, "chunkdb-test-root", intermediate), &X509_free);
    WriteTestFiles(cert_path, key_path, {leaf_certificate.get(), intermediate_certificate.get()}, leaf);
}

// RELOAD TLS loads the certificate and key from their files again: new
// connections see the new certificate, open ones keep their session, a key
// that does not match is refused without changing what is served, and the
// statement needs ADMIN on every table.
void TestTlsReload() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.tls_enabled = true;
    ServerHarness harness("tls-reload", BaseStoreConfig(), engine_cfg, server_cfg);
    const std::filesystem::path cert = harness.saved_server_config.tls_cert_path;
    const std::filesystem::path key = harness.saved_server_config.tls_key_path;
    TlsClient open("127.0.0.1", harness.port);
    open.Login();
    const auto original = open.PeerCommonName();
    assert(!original.empty() && original != "chunkdb-reloaded");

    const TestKey reloaded;
    WriteTestCertificate(cert, key, "chunkdb-reloaded", reloaded.key, reloaded.key);
    open.SendLine("RELOAD TLS");
    assert(open.ReadLine() == "+OK\r\n");
    {
        TlsClient fresh("127.0.0.1", harness.port);
        assert(fresh.PeerCommonName() == "chunkdb-reloaded");
        fresh.Login();
        fresh.SendLine("PING");
        assert(fresh.ReadLine() == "+PONG\r\n");
    }
    // The open connection keeps its session and still answers.
    open.SendLine("PING");
    assert(open.ReadLine() == "+PONG\r\n");

    // A certificate issued through an intermediate: new connections receive
    // the whole chain from the file.
    const TestKey leaf;
    const TestKey intermediate;
    WriteTestChain(cert, key, "chunkdb-leaf", leaf.key, "chunkdb-intermediate", intermediate.key);
    open.SendLine("RELOAD TLS");
    assert(open.ReadLine() == "+OK\r\n");
    {
        TlsClient fresh("127.0.0.1", harness.port);
        const auto chain = fresh.PeerChainCommonNames();
        assert((chain == std::vector<std::string>{"chunkdb-leaf", "chunkdb-intermediate"}));
        fresh.Login();
    }
    WriteTestCertificate(cert, key, "chunkdb-reloaded", reloaded.key, reloaded.key);
    open.SendLine("RELOAD TLS");
    assert(open.ReadLine() == "+OK\r\n");

    // A certificate whose key is not the one on disk.
    const TestKey other;
    WriteTestCertificate(cert, key, "chunkdb-mismatch", other.key, reloaded.key);
    open.SendLine("RELOAD TLS");
    const auto refused = open.ReadLine();
    assert(refused.rfind("-ERR INVALID_ARGUMENT reason=invalid_argument TLS reload failed", 0) == 0);
    // OpenSSL 3 reports the mismatch while loading the key, earlier versions
    // when checking the pair.
    assert(refused.find("mismatch") != std::string::npos || refused.find("does not match") != std::string::npos);
    {
        TlsClient fresh("127.0.0.1", harness.port);
        assert(fresh.PeerCommonName() == "chunkdb-reloaded");
        fresh.Login();
    }
    open.SendLine("PING");
    assert(open.ReadLine() == "+PONG\r\n");

    // READ on a table is not enough.
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = chunkdb::scram::FormatVerifier(chunkdb::test::PrepareLoginVerifier("pw", salt, chunkdb::scram::kMinIterations));
    open.SendLine("CREATE USER reader VERIFIER '" + verifier + "'");
    assert(open.ReadLine() == "+OK\r\n");
    open.SendLine("GRANT READ ON default TO reader");
    assert(open.ReadLine() == "+OK\r\n");
    WriteTestCertificate(cert, key, "chunkdb-reloaded", reloaded.key, reloaded.key);
    TlsClient reader("127.0.0.1", harness.port);
    reader.Login("reader", "pw");
    reader.SendLine("RELOAD TLS");
    assert(reader.ReadLine().rfind("-ERR PERMISSION_DENIED", 0) == 0);
}

// --tls-min-version 1.3 refuses a client that offers at most TLS 1.2; the
// default accepts it.
void TestTlsMinimumVersion() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    for (const auto minimum : {chunkdb::TlsMinVersion::k1_2, chunkdb::TlsMinVersion::k1_3}) {
        auto server_cfg = BaseServerConfig();
        server_cfg.tls_enabled = true;
        server_cfg.tls_min_version = minimum;
        ServerHarness harness("tls-minimum", BaseStoreConfig(), engine_cfg, server_cfg);
        bool refused = false;
        try {
            TlsClient old("127.0.0.1", harness.port, TLS1_2_VERSION);
            assert(old.Version() == TLS1_2_VERSION);
            old.Login();
        } catch (const std::runtime_error&) {
            refused = true;
        }
        assert(refused == (minimum == chunkdb::TlsMinVersion::k1_3));
        TlsClient current("127.0.0.1", harness.port);
        assert(current.Version() == TLS1_3_VERSION);
        current.Login();
        // A reloaded context keeps the minimum.
        current.SendLine("RELOAD TLS");
        assert(current.ReadLine() == "+OK\r\n");
        bool refused_after_reload = false;
        try {
            TlsClient old("127.0.0.1", harness.port, TLS1_2_VERSION);
            old.Login();
        } catch (const std::runtime_error&) {
            refused_after_reload = true;
        }
        assert(refused_after_reload == (minimum == chunkdb::TlsMinVersion::k1_3));
    }
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
        ScopedServerSocketFailpoint failpoint(2, 0);
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

void TestNonblockingSetupFailureClosesConnection() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("nonblocking-setup-failure", store_cfg, engine_cfg, server_cfg);
    {
        ScopedServerSocketFailpoint failpoint(0, 2);
        RawClient client("127.0.0.1", harness.port);
        assert(logs.WaitContains(
            "failed to configure nonblocking client I/O; closing connection",
            std::chrono::seconds(2)));
        client.SendLine("PING");
        assert(client.WaitForClose(std::chrono::milliseconds(1500)));
    }

    assert(logs.Contains("phase=handshake"));

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
    assert(response == "%14\r\n");
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
    if (recv_timeout_calls != 0U) {
        throw std::runtime_error(
            "polled requests configured a blocking receive timeout: calls=" +
            std::to_string(recv_timeout_calls));
    }
}

void TestLongIdleConnectionTimeoutReleasesWorker(bool greeted = true) {
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
    if (greeted) idle.Hello();

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("HELLO 3");

    fast.SetReadDeadline(std::chrono::milliseconds(1500));
    assert(ReadHelloReply(fast).at("protocol") == "3");
    assert(idle.WaitForClose(std::chrono::milliseconds(1500)));
}

void TestQueuedStatementTimeoutPreservesConnection() {
    IdleQueuePressureHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U;
    ServerHarness harness("queued-statement-timeout", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { IdleQueuePressureHook& hook; ~Release() { hook.Release(); } } release{hook};
    RawClient worker("127.0.0.1", harness.port); worker.Hello();
    RawClient queued("127.0.0.1", harness.port); queued.SetReadDeadline(std::chrono::seconds(10));
    queued.SendLine("HELLO 3 WITH statement_timeout_ms=1000");
    assert(ReadHelloReply(queued).at("protocol") == "3");
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING"); hook.WaitWorker();
    queued.SendLine("PING");
    assert(queued.ReadLine().starts_with("-ERR TIMEOUT applied=false"));
    hook.Release();
    assert(worker.ReadLine() == "+PONG\r\n");
    queued.SendLine("PING"); assert(queued.ReadLine() == "+PONG\r\n");
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
    // A write deadline says how much of the reply the socket took.
    assert(!logs.Contains("phase=write") || logs.Contains("reply progress deadline exceeded"));

    // After the slow connection is terminated, the single worker must be able
    // to serve a new client.
    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("HELLO 3");
    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(2000), &response));
    assert(response == "%14\r\n");
}

class PayloadCreditHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t id) override {
        if (point != Point::kAfterPayloadCreditQueued && point != Point::kAfterPayloadCreditGranted) return;
        { std::lock_guard lock(mutex_); (point == Point::kAfterPayloadCreditQueued ? queued_ : granted_).push_back(id); }
        changed_.notify_all();
    }
    void Wait(std::uint64_t id, bool granted) {
        std::unique_lock lock(mutex_);
        const auto& entries = granted ? granted_ : queued_;
        assert(changed_.wait_for(lock, std::chrono::seconds(10), [&] {
            return std::find(entries.begin(), entries.end(), id) != entries.end();
        }));
    }
    bool Granted(std::uint64_t id) {
        std::lock_guard lock(mutex_);
        return std::find(granted_.begin(), granted_.end(), id) != granted_.end();
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<std::uint64_t> queued_, granted_;
};

template <typename Client>
void TestKillIdleTransactionReleasesHistory(bool tls) {
    auto config = BaseServerConfig(); config.tls_enabled = tls; config.worker_threads = 1U;
    ServerHarness harness(tls ? "kill-idle-txn-tls" : "kill-idle-txn", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client owner("127.0.0.1", harness.port); owner.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(owner).at("connection_id"));
    Client controller("127.0.0.1", harness.port); controller.Hello();
    controller.SendLine("SET BLOCK 0 0 IN default bits = b'1000'"); (void)ReadVersion(controller);
    owner.SendLine("BEGIN"); assert(owner.ReadLine() == "+OK\r\n");
    owner.SendLine("SET BLOCK 0 0 IN default bits = b'0100'"); assert(owner.ReadLine() == "_\r\n");
    controller.SendLine("SET BLOCK 0 0 IN default bits = b'0010'"); (void)ReadVersion(controller);
    auto lease = harness.catalog->Require("default")->Acquire();
    assert(lease->store().TxnRegisteredCountForTests() == 1U);
    assert(lease->store().TxnKeptStateCountForTests() == 1U);
    controller.SendLine("KILL CONNECTION " + std::to_string(id));
    assert(controller.ReadReply().ends_with("#t\r\n"));
    assert(owner.WaitForClose(std::chrono::seconds(5)));
    assert(lease->store().TxnRegisteredCountForTests() == 0U);
    assert(lease->store().TxnKeptStateCountForTests() == 0U);
    assert(lease->store().GetBlockBits(0, 0) == "0010");
}

template <typename Client>
void TestKillDecidedCommitPreservesOutcome(bool tls) {
    auto config = BaseServerConfig(); config.tls_enabled = tls; config.worker_threads = 1U;
    ServerHarness harness(tls ? "kill-decided-commit-tls" : "kill-decided-commit", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    Client owner("127.0.0.1", harness.port); owner.SetReadDeadline(std::chrono::seconds(10));
    owner.SendLine("HELLO 3");
    const auto id = std::stoull(ReadHelloReply(owner).at("connection_id"));
    Client controller("127.0.0.1", harness.port); controller.SetReadDeadline(std::chrono::seconds(10)); controller.Hello();
    owner.SendLine("BEGIN"); assert(owner.ReadLine() == "+OK\r\n");
    owner.SendLine("SET BLOCK 0 0 IN default bits = b'0100'"); assert(owner.ReadLine() == "_\r\n");
    auto lease = harness.catalog->Require("default")->Acquire();
    auto& store = lease->store();
    constexpr auto point = chunkdb::TxnPausePoint::kBeforePostCommitOutcome;
    store.ArmTxnPauseForTests(point);
    struct Resume {
        chunkdb::ChunkStore& store;
        bool armed = true;
        ~Resume() { if (armed) store.ResumeTxnForTests(point); }
    } resume{store};
    owner.SendLine("COMMIT");
    assert(store.WaitForTxnPauseForTests(point));
    controller.SendLine("KILL CONNECTION " + std::to_string(id));
    assert(controller.ReadReply().ends_with("#t\r\n"));
    store.ResumeTxnForTests(point); resume.armed = false;
    assert(ReadVersion(owner) != 0U);
    assert(owner.WaitForClose(std::chrono::seconds(5)));
    assert(store.GetBlockBits(0, 0) == "0100");
    assert(store.TxnRegisteredCountForTests() == 0U);
}

void TestRemovingCreditHeadAdmitsNextWaiter() {
    PayloadCreditHook hook;
    auto config = BaseServerConfig(); config.client_io_timeout_ms = 30000;
    ServerHarness harness("payload-credit-head", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    RawClient controller("127.0.0.1", harness.port); controller.Hello();
    controller.SendLine("CREATE TABLE wide (v bits(512)) CHUNK 1024 x 1024");
    assert(controller.ReadLine() == "+OK\r\n");
    controller.SendLine("ALTER TABLE wide SET var_max_chunk_bytes = 67108864");
    assert(controller.ReadLine() == "+OK\r\n");
    controller.SendLine("CREATE TABLE small (f u8 REQUIRED, v bytes(16777216) NULL) CHUNK 1 x 1");
    assert(controller.ReadLine() == "+OK\r\n");
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    std::vector<std::unique_ptr<RawClient>> owners;
    std::uint64_t head_id = 0U;
    for (std::size_t i = 0U; i < 16U; ++i) {
        auto client = std::make_unique<RawClient>("127.0.0.1", harness.port);
        client->SendLine("HELLO 3");
        const auto id = std::stoull(ReadHelloReply(*client).at("connection_id"));
        client->SendLine("SET CHUNK 0 0 IN wide $1");
        hook.Wait(id, i < 15U);
        if (i == 15U) { head_id = id; assert(!hook.Granted(id)); }
        owners.push_back(std::move(client));
    }
    RawClient small("127.0.0.1", harness.port); small.SendLine("HELLO 3");
    const auto small_id = std::stoull(ReadHelloReply(small).at("connection_id"));
    small.SendLine("SET BLOCK 0 0 IN small v = $1");
    hook.Wait(small_id, false);
    assert(!hook.Granted(small_id));
    // The removed head owns no payload reservation, so its removal returns no bytes.
    controller.SendLine("KILL CONNECTION " + std::to_string(head_id));
    assert(controller.ReadReply().ends_with("#t\r\n"));
    hook.Wait(small_id, true);
}

void TestWideNullableReadHasBoundedReplyAdmission() {
    ServerHarness harness("wide-nullable-reply", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port); client.SetReadDeadline(std::chrono::seconds(10)); client.Hello();
    std::string create = "CREATE TABLE wide (f u8 REQUIRED";
    for (std::size_t i = 0U; i < 32U; ++i) create += ",b" + std::to_string(i) + " bytes(16777216) NULL";
    client.SendLine(create + ") CHUNK 1 x 1"); assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("SET BLOCK 0 0 IN wide f = 1"); (void)ReadVersion(client);
    client.SendLine("GET BLOCK 0 0 FROM wide COLUMNS f");
    assert(client.ReadReply() == "*1\r\n:1\r\n");
    client.SendLine("GET BLOCK 0 0 FROM wide");
    std::string expected = "*33\r\n:1\r\n";
    for (std::size_t i = 0U; i < 32U; ++i) expected += "_\r\n";
    assert(client.ReadReply() == expected);
    chunkdb::SessionState writer;
    (void)harness.engine->Execute(writer, "HELLO 3");
    const std::vector<std::optional<std::string>> parameters{std::string(1048564U, 'x')};
    assert(!harness.engine->Execute(writer, "SET BLOCK 0 0 IN wide f = 1, b0 = $1", parameters).starts_with('-'));
    std::string repeated = "GET BLOCK 0 0 FROM wide COLUMNS b0";
    for (std::size_t i = 1U; i < 65U; ++i) repeated += ",b0";
    client.SendLine(repeated);
    assert(client.ReadLine().starts_with("-ERR OUT_OF_RANGE"));
    client.SendLine("PING"); assert(client.ReadLine() == "+PONG\r\n");
}
class StatementTrackerHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t count) override {
        if (point != Point::kAfterStatementTrackerScan) return;
        last.store(count);
        maximum.store(std::max(maximum.load(), count));
    }
    std::atomic<std::size_t> last{0U}, maximum{0U};
};
void TestReplyAdmissionPrecedesMutation() {
    StatementTrackerHook hook;
    ServerHarness harness("reply-credit-before-write", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port); client.SetReadDeadline(std::chrono::seconds(5)); client.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    auto credit = chunkdb::FeedDeliveryTestAccess::ReserveReplyCredit(*harness.server, 2147483648ULL);
    assert(credit);
    for (std::size_t i = 0U; i < 8U; ++i) {
        client.SendLine("SET BLOCK 0 0 IN default bits = b'1000'");
        assert(client.ReadLine().starts_with("-ERR BUSY reason=reply_budget"));
    }
    client.SendLine("SHOW CONNECTIONS");
    assert(client.ReadReply().starts_with('*'));
    assert(hook.last.load() == 0U && hook.maximum.load() <= 1U);
    {
        auto lease = harness.catalog->Require("default")->Acquire();
        assert(!lease->store().GetBlock(0, 0).has_value());
    }
    credit.reset();
    client.SendLine("SET BLOCK 0 0 IN default bits = b'1000'"); (void)ReadVersion(client);
    auto lease = harness.catalog->Require("default")->Acquire();
    assert(lease->store().GetBlockBits(0, 0) == "1000");
}

void TestSteadyReaderRenewsReplyProgressDeadline() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    // This response remains in progress across several transport intervals.
    auto store_cfg = BaseStoreConfig();
    store_cfg.geometry = {
        .large_chunk_width_chunks = 1,
        .large_chunk_height_chunks = 1,
        .chunk_width_blocks = 1024,
        .chunk_height_blocks = 1024,
        .block_bits = 512,
    };
    auto engine_cfg = chunkdb::EngineConfig{
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 600;
    server_cfg.idle_connection_timeout_ms = 10000;

    ServerHarness harness("steady-slow-reader-deadline", store_cfg, engine_cfg, server_cfg);
    {
        auto lease = harness.catalog->Find("default")->Acquire();
        lease->store().SetBlockBits(0, 0, std::string(512, '0'));
    }
    RawClient slow("127.0.0.1", harness.port);
    slow.SetReceiveBuffer(1048576);
    slow.Hello();
    assert(chunkdb::FeedDeliveryTestAccess::SetActiveClientSendBuffer(*harness.server, 65536) > 0);
    slow.SendBytes("GET CHUNK 0 0 FROM default\r\nGET CHUNK 0 0 FROM default\r\n");
    assert(slow.ReadLine().starts_with('$'));
    const auto start = Clock::now();
    std::mutex cadence_mutex;
    std::condition_variable cadence;
    std::unique_lock cadence_lock(cadence_mutex);
    for (std::size_t tick = 1U; tick <= 32U; ++tick) {
        for (int read = 0; read < 256; ++read) {
            std::size_t bytes_read = 0;
            assert(slow.ReadSomeWithin(std::chrono::milliseconds(200), 4096, &bytes_read) && bytes_read != 0U);
        }
        (void)cadence.wait_until(cadence_lock, start + std::chrono::milliseconds(50U * tick), [] { return false; });
        if (logs.Contains("connection terminated")) throw std::runtime_error(logs.Text());
    }
    // No further reads return window credit, so output must now stop progressing.
    assert(logs.WaitContains("reply progress deadline exceeded", std::chrono::seconds(6)));
    assert(logs.Contains("phase=write"));
    assert(logs.Contains("reason=timeout"));
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

void TestPendingStatementQueueKeepsHelloAvailable() {
    IdleQueuePressureHook hook;
    auto config = BaseServerConfig(); config.worker_threads = 1U; config.max_pending_clients = 1U;
    ServerHarness harness("pending-queue-hello", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { IdleQueuePressureHook& hook; ~Release() { hook.Release(); } } release{hook};
    RawClient partial("127.0.0.1", harness.port); partial.SendBytes("HELLO 3");
    RawClient worker("127.0.0.1", harness.port); worker.Hello();
    RawClient queued("127.0.0.1", harness.port); queued.Hello();
    RawClient waiting("127.0.0.1", harness.port); waiting.Hello();
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    worker.SendLine("PING"); hook.WaitWorker();
    queued.SendLine("PING"); waiting.SendLine("PING"); hook.WaitBackpressure();
    RawClient newcomer("127.0.0.1", harness.port); newcomer.SetReadDeadline(std::chrono::seconds(10)); newcomer.Hello();
    partial.SendBytes("\r\n");
    assert(ReadHelloReply(partial).at("protocol") == "3");
    hook.Release();
    for (auto* client : {&worker, &queued, &waiting}) assert(client->ReadLine() == "+PONG\r\n");
    newcomer.SendLine("PING"); assert(newcomer.ReadLine() == "+PONG\r\n");
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
    assert(response.rfind("-ERR PROTOCOL", 0) == 0);
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

#ifndef _WIN32
// A request larger than the socket buffers reaches a server that stops
// reading for longer than the client's 200 ms send timeout. CI once failed
// TestChunkPutIfLargestGeometry with "failed to send client bytes": the
// client took a timed-out send of its 64 MiB SET CHUNK for a failure.
void TestClientSendOutlastsAPausedReader() {
    const OccupiedPort listener;
    const std::string request(64U * 1024U * 1024U, 'x');
    std::size_t received = 0;
    std::jthread peer([&] {
        const SocketHandle accepted = accept(listener.listening_socket(), nullptr, nullptr);
        if (accepted == kInvalidSocket) return;
        // The paused server: three of the client's send timeouts.
        std::this_thread::sleep_for(std::chrono::milliseconds(600));
        std::vector<char> buffer(65536);
        while (received < request.size()) {
            const ssize_t read = recv(accepted, buffer.data(), buffer.size(), 0);
            if (read <= 0) break;
            received += static_cast<std::size_t>(read);
        }
        CloseSocket(accepted);
    });
    RawClient client("127.0.0.1", listener.port());
    client.SendBytes(request);
    peer.join();
    assert(received == request.size());
}
#endif

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
    assert(response.rfind("-ERR PROTOCOL", 0) == 0);
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
    assert(logs.Contains("client_io_timeout_ms=30000"));
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
        assert(reply.rfind("%10\r\n$5\r\ntable\r\n$" + std::to_string(table.size()) + "\r\n" + table + "\r\n", 0) == 0);
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
    assert(a.ReadLine() == "-ERR SYNTAX reason=syntax column 14: names are lowercase: 'Terrain'\r\n");
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
        assert(unknown.ReadLine().starts_with("-ERR SYNTAX reason=syntax column"));
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
            "-ERR PROTOCOL reason=protocol $1 is longer than its column holds (" + std::to_string(default_limit) + " bytes)\r\n");
        assert(on_default.WaitForClose(std::chrono::seconds(5)));
    }

    // Options change while another connection uses the table.
    b.SendLine("ALTER TABLE terrain SET checkpoint_updates = 3");
    assert(b.ReadLine() == "+OK\r\n");
    b.SendLine("ALTER TABLE terrain SET checkpoint_compression = 'zrle'");
    assert(b.ReadLine() == "+OK\r\n");
    b.SendLine("ALTER TABLE terrain SET block_bits = 5");
    assert(b.ReadLine().rfind("-ERR INVALID_ARGUMENT reason=invalid_argument block_bits is part of the geometry", 0) == 0);
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
        // and a SET CHUNK drains its bounded opaque frame before replying.
        RawClient fresh("127.0.0.1", harness.port);
        fresh.Hello();
        fresh.SendLine("GET BLOCK 0 0 FROM default");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        fresh.SendBytes("SET CHUNK 0 0 IN default $1\r\n" + Frame("discarded") + "PING\r\n");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        assert(fresh.ReadLine() == "+PONG\r\n");
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
        assert(early.ReadLine() == "-ERR PROTOCOL reason=protocol expected HELLO 3\r\n");
        assert(early.WaitForClose(std::chrono::seconds(5)));
    }
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 3");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
    client.SendLine("SHOW TABLES");
    assert(client.ReadLine() == "-ERR PROTOCOL reason=protocol expected HELLO 3\r\n");
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
const FeedReply& FeedMap(const FeedReply& reply, std::string_view key) {
    for (std::size_t i = 0; i < reply.items.size(); i += 2U)
        if (reply.items[i].value == key) return reply.items[i + 1U];
    throw std::logic_error("missing feed test map key");
}
std::string FeedStart(const FeedReply& reply) {
    assert(reply.type == '%');
    const auto& epoch = FeedMap(reply, "epoch");
    const auto& revision = FeedMap(reply, "revision");
    assert(epoch.type == '$' && epoch.value.size() == 32U);
    assert(revision.type == ':' || revision.type == '(');
    return epoch.value + " " + revision.value;
}
chunkdb::ServerConfig AuthenticatedFeedServerConfig() {
    auto config = BaseServerConfig();
    // Software SCRAM derivation in Debug/sanitizer builds can exceed 5 seconds.
    // Functional feed tests still authenticate, with a bounded handshake budget.
    config.client_io_timeout_ms = 30000;
    // These cases check mutation envelopes, not periodic idle progress.
    config.feed_progress_ms = 60000;
    return config;
}
template <typename Client> void TestWatchProtocol(bool tls) {
    auto config = AuthenticatedFeedServerConfig();
    config.worker_threads = 2;
    config.tls_enabled = tls;
    config.max_watches = 2;
    ServerHarness harness(tls ? "watch-tls" : "watch-plain", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    Client writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(15)); writer.Login();
    writer.SendLine("CREATE TABLE t (n u8, label text(100), active bool, weight f32, flags bits(3), blob bytes(10)) CHUNK 4 x 4");
    assert(writer.ReadLine() == "+OK\r\n");
    Client whole("127.0.0.1", harness.port); whole.SetReadDeadline(std::chrono::seconds(15)); whole.Login();
    whole.SendLine("WATCH t");
    const auto start = ReadFeedReply(whole);
    const auto position = FeedStart(start);
    Client area("127.0.0.1", harness.port); area.SetReadDeadline(std::chrono::seconds(15)); area.Login();
    // Other clients' SCRAM setup must not consume this protocol phase's budget.
    writer.SetReadDeadline(std::chrono::seconds(15));
    whole.SetReadDeadline(std::chrono::seconds(15));
    area.SetReadDeadline(std::chrono::seconds(15));
    area.SendLine("WATCH t AREA 0 0 TO 0 0"); assert(!FeedStart(ReadFeedReply(area)).empty());
    // Two watches leave both workers available; this third connection writes.
    writer.SendLine("WATCH default"); assert(writer.ReadLine().rfind("-ERR BUSY", 0) == 0);
    writer.SendLine("SET BLOCK 0 0 IN t n = 7, label = 'hello', active = true, weight = 1.5, flags = b'101', blob = x'00ff'");
    const auto revision = ReadVersion(writer);
    const auto change = ReadFeedReply(whole);
    assert(change.type == '>' && change.items.size() == 10U && change.items[0].value == "change");
    assert(change.items[2].value == std::to_string(revision) && change.items[4].value == kAdminUser);
    assert(change.items[7].type == '_' && change.items[8].type == '_');
    assert(change.items[9].type == '*' && change.items[9].items.empty());
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
    area.SendLine("WATCH t AFTER " + position); assert(ReadFeedReply(area) == start);
    assert(ReadFeedReply(area) == change && ReadFeedReply(area) == transaction);
    writer.SendLine("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 2"); assert(writer.ReadLine() == "+OK\r\n");
    const auto schema = ReadFeedReply(whole);
    assert(schema.type == '>' && schema.items[0].value == "schema" && FeedMap(schema.items[1], "schema_version").value == "2");
    assert(ReadFeedReply(area) == schema);
    writer.SendLine("DESCRIBE t"); assert(FeedMap(ReadFeedReply(writer), "columns") == FeedMap(schema.items[1], "columns"));
    writer.SendLine("DROP TABLE t"); assert(writer.ReadLine() == "+OK\r\n");
    assert(whole.ReadLine().rfind("-ERR NO_TABLE", 0) == 0 && area.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    // A command other than UNWATCH terminates the stream, after an error.
    writer.SendLine("WATCH default"); assert(!FeedStart(ReadFeedReply(writer)).empty());
    writer.SendLine("PING"); assert(writer.ReadLine().rfind("-ERR PROTOCOL", 0) == 0);
}
template <typename Client> void TestWatchDefaultProgress(bool tls) {
    auto config = BaseServerConfig(); config.tls_enabled = tls;
    assert(config.feed_progress_ms == 1000U);
    ServerHarness harness("watch-default-progress", BaseStoreConfig(), chunkdb::EngineConfig{.require_auth = false}, config);
    Client writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(10)); writer.Hello();
    writer.SendLine("CREATE TABLE t (n u8) CHUNK 4 x 4"); assert(writer.ReadLine() == "+OK\r\n");
    Client watch("127.0.0.1", harness.port); watch.SetReadDeadline(std::chrono::seconds(10)); watch.Hello();
    watch.SendLine("WATCH t AREA 0 0 TO 0 0");
    const auto start = ReadFeedReply(watch);
    writer.SendLine("SET BLOCK 4 0 IN t n = 1");
    const auto revision = ReadVersion(writer);
    const auto progress = ReadFeedReply(watch);
    assert(progress.type == '>' && progress.items.size() == 3U && progress.items[0].value == "progress");
    assert(progress.items[1].value == FeedMap(start, "epoch").value);
    assert(std::stoull(progress.items[2].value) >= revision);
    const auto idle = ReadFeedReply(watch);
    assert(idle == progress);
    watch.SendBytes("UNWATCH\r\nPING\r\n");
    for (;;) {
        const auto event = ReadFeedReply(watch);
        if (event.type == '>') { assert(event.items[0].value == "progress"); continue; }
        assert(event.type == '+' && event.value == "OK"); break;
    }
    assert(watch.ReadLine() == "+PONG\r\n");
}

void TestWatchPositionsAndRights() {
    auto config = AuthenticatedFeedServerConfig(); config.worker_threads = 2; config.feed_buffer_bytes = 16384;
    config.feed_linger_ms = 0U;
    ServerHarness harness("watch-positions", BaseStoreConfig(), chunkdb::EngineConfig{}, config);
    RawClient writer("127.0.0.1", harness.port); writer.SetReadDeadline(std::chrono::seconds(15)); writer.Login();
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = chunkdb::scram::FormatVerifier(chunkdb::test::PrepareLoginVerifier("pw", salt, chunkdb::scram::kMinIterations));
    writer.SetReadDeadline(std::chrono::seconds(15));
    writer.SendLine("CREATE USER reader VERIFIER '" + verifier + "'"); assert(writer.ReadLine() == "+OK\r\n");
    RawClient reader("127.0.0.1", harness.port); reader.SetReadDeadline(std::chrono::seconds(15)); reader.Login("reader", "pw");
    // Re-arm the same bounded read window after verifier/login CPU setup.
    writer.SetReadDeadline(std::chrono::seconds(15));
    reader.SetReadDeadline(std::chrono::seconds(15));
    reader.SendLine("WATCH default"); const auto hidden = reader.ReadLine();
    reader.SendLine("WATCH missing"); const auto missing = reader.ReadLine();
    assert(hidden.rfind("-ERR NO_TABLE", 0) == 0 && missing.rfind("-ERR NO_TABLE", 0) == 0);
    writer.SendLine("GRANT READ ON default TO reader"); assert(writer.ReadLine() == "+OK\r\n");
    reader.SendLine("WATCH default");
    const auto old = FeedStart(ReadFeedReply(reader));
    for (std::size_t i = 0; i < 100U; ++i) {
        writer.SendLine("SET BLOCK 0 0 IN default bits = b'" + std::string(i % 2U ? "1000" : "0100") + "'");
        (void)ReadVersion(writer);
    }
    reader.SendLine("UNWATCH");
    while (ReadFeedReply(reader).type != '+') {}
    reader.SendLine("WATCH default AFTER " + old); assert(!FeedStart(ReadFeedReply(reader)).empty());
    assert(ReadFeedReply(reader).items[0].value == "resync");
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
    // Last watch gone: the new feed has no replay history, even in this process.
    reader.SendLine("WATCH default AFTER " + old); assert(!FeedStart(ReadFeedReply(reader)).empty());
    assert(ReadFeedReply(reader).items[0].value == "resync");
    reader.SendLine("UNWATCH"); assert(reader.ReadLine() == "+OK\r\n");
    reader.SendLine("WATCH default AFTER ffffffffffffffffffffffffffffffff 0"); assert(!FeedStart(ReadFeedReply(reader)).empty());
    assert(ReadFeedReply(reader).items[0].value == "resync");
    writer.SendBytes("SET CHUNK 9223372036854775807 0 IN default $1\r\n" +
        Frame(ChunkFormOf(std::string("\x01\0", 2), std::string("\x01", 1) + std::string(7, '\0'))));
    (void)ReadVersion(writer);
    const auto coordinate = ReadFeedReply(reader).items[6].items[0].items[0];
    assert(coordinate.type == '*' && coordinate.items[0].value == "9223372036854775807" && coordinate.items[1].value == "0");
    harness.Restart();
    RawClient restarted("127.0.0.1", harness.port); restarted.SetReadDeadline(std::chrono::seconds(15)); restarted.Login("reader", "pw");
    restarted.SendLine("WATCH default AFTER " + old); assert(!FeedStart(ReadFeedReply(restarted)).empty());
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
    slow.SendLine("WATCH t"); assert(!FeedStart(ReadFeedReply(slow)).empty());
    Client fast("127.0.0.1", harness.port); fast.SetReadDeadline(std::chrono::seconds(60)); fast.Hello();
    fast.SendLine("WATCH t"); assert(!FeedStart(ReadFeedReply(fast)).empty());
    std::vector<std::unique_ptr<Client>> idle;
    for (int i = 0; i < 6; ++i) {
        auto watch = std::make_unique<Client>("127.0.0.1", harness.port);
        watch->SetReadDeadline(std::chrono::seconds(60)); watch->Hello();
        watch->SendLine("WATCH t AREA 100 100 TO 100 100"); assert(!FeedStart(ReadFeedReply(*watch)).empty());
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
    a.SendLine("WATCH default"); assert(!FeedStart(ReadFeedReply(a)).empty());
    Client b("127.0.0.1", harness.port); b.SetReadDeadline(std::chrono::seconds(5)); b.Hello();
    b.SendLine("WATCH default"); assert(!FeedStart(ReadFeedReply(b)).empty());
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
        reader.SendLine("WATCH default"); assert(!FeedStart(ReadFeedReply(reader)).empty());
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
    auto config = AuthenticatedFeedServerConfig();
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
    writer.SetReadDeadline(std::chrono::seconds(15));
    reader.SetReadDeadline(std::chrono::seconds(15));
    reader.SendLine("WATCH default"); assert(!FeedStart(ReadFeedReply(reader)).empty());
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
    assert(!FeedStart(ReadFeedReply(reader)).empty());
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

// Own the observer before the harness so shutdown callbacks cannot outlive it.
// Stage names and indices contain no commands, credentials or payloads.
class ShutdownTraceHook : public chunkdb::FeedDeliveryTestHook {
  public:
    explicit ShutdownTraceHook(bool block_idle = false)
        : block_idle_(block_idle), report_(chunkdb::test::FeedPhaseWatchdog::Reporter()) {}
    void Run(Point point, std::size_t index) override {
        const char* stage = nullptr;
        switch (point) {
            case Point::kBeforeIoJoin: stage = "before I/O join"; break;
            case Point::kAfterIoJoin: stage = "after I/O join"; break;
            case Point::kBeforeCatchUpJoin: stage = "before catchup join"; break;
            case Point::kAfterCatchUpJoin: stage = "after catchup join"; break;
            case Point::kBeforeClientShutdown: stage = "shutdown socket"; break;
            case Point::kAfterClientShutdown: stage = "shutdown result (0 success)"; break;
            case Point::kAfterClientIoCancel: stage = "cancellation result (Win32 code)"; break;
            case Point::kBeforeWorkerRegister: stage = "before active publication"; break;
            case Point::kBeforeWorkerJoin: stage = "before worker join"; break;
            case Point::kAfterWorkerJoin: stage = "after worker join"; break;
            case Point::kWorkerReady: stage = "worker ready"; break;
            case Point::kBeforePendingWait: stage = "pending false predicate"; break;
            case Point::kAfterPendingWait: stage = "pending wait returned"; break;
            case Point::kBeforeWorkerHandle: stage = "before HandleClient"; break;
            case Point::kAfterWorkerHandle: stage = "after HandleClient"; break;
            case Point::kBeforeWorkerClose: stage = "before CloseClient"; break;
            case Point::kAfterWorkerClose: stage = "after CloseClient"; break;
            case Point::kBeforeClientIdleRead: stage = "before idle read (reply ordinal)"; break;
            case Point::kBeforeClientRead: stage = "before read_line (reply ordinal)"; break;
            case Point::kAfterClientRead: stage = "after read_line (reply ordinal)"; break;
            default: break;
        }
        if (stage) {
            std::ostringstream detail;
            detail << "shutdown: " << stage << " index=" << index
                   << " thread=" << std::this_thread::get_id();
            report_(detail.str());
        }
        std::unique_lock lock(mutex_);
        if (point == Point::kAfterWorkerJoin) ++joined_workers_;
        if (point == Point::kBeforeClientIdleRead && block_idle_ && index >= 2U && !entered_) {
            entered_ = true;
            changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        }
    }
    void WaitIdle() {
        std::unique_lock lock(mutex_);
        if (!changed_.wait_for(lock, std::chrono::seconds(10), [&] { return entered_; }))
            throw std::runtime_error("idle shutdown barrier: waiting for read after PONG");
    }
    void Release() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
    std::size_t JoinedWorkers() {
        std::lock_guard lock(mutex_);
        return joined_workers_;
    }
  private:
    const bool block_idle_;
    const std::function<void(std::string_view)> report_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::size_t joined_workers_ = 0U;
    bool entered_ = false, released_ = false;
};

void TestStopConnectedIdleClient() {
    ShutdownTraceHook hook(true);
    auto config = BaseServerConfig();
    config.idle_connection_timeout_ms = 120000U; // The 60s watchdog must precede natural idle expiry.
    ServerHarness harness("stop-connected-idle-client", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { ShutdownTraceHook& hook; ~Release() { hook.Release(); } } release{hook};
    {
        RawClient probe("127.0.0.1", harness.port);
        probe.Hello(); // FeedIo is published before the hook is installed.
    }
    chunkdb::FeedDeliveryTestAccess::SetHook(*harness.server, &hook);
    RawClient client("127.0.0.1", harness.port);
    client.SetReadDeadline(std::chrono::seconds(15));
    client.Hello();
    client.SendLine("PING"); assert(client.ReadLine() == "+PONG\r\n");
    chunkdb::test::FeedPhaseWatchdog::Phase("hook: wait for connected idle read after PONG");
    hook.WaitIdle();
    chunkdb::test::FeedPhaseWatchdog::Phase("harness: single Stop with connected idle client");
    harness.server->Stop();
    hook.Release(); // Release the handler only after Stop requested closure.
    harness.JoinStopped(); // Keep the client alive and supply no second Stop.
    assert(hook.JoinedWorkers() == config.worker_threads);
}

class WorkerPublicationHook final : public ShutdownTraceHook {
  public:
    void Run(Point point, std::size_t value) override {
        ShutdownTraceHook::Run(point, value);
        if (point == Point::kBeforeWorkerHandle) handled_.store(true);
        if (point != Point::kBeforeWorkerRegister) return;
        std::unique_lock lock(mutex_);
        entered_ = true;
        changed_.notify_all();
        changed_.wait(lock, [&] { return released_; });
    }
    void WaitPublication() {
        std::unique_lock lock(mutex_);
        if (!changed_.wait_for(lock, std::chrono::seconds(10), [&] { return entered_; }))
            throw std::runtime_error("active publication barrier: waiting for dequeued connection");
    }
    void ReleasePublication() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
    bool Handled() const { return handled_.load(); }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    std::atomic<bool> handled_{false};
    bool entered_ = false, released_ = false;
};

void TestStopDequeuedClient() {
    WorkerPublicationHook hook;
    auto config = BaseServerConfig();
    config.idle_connection_timeout_ms = 120000U;
    ServerHarness harness("stop-dequeued-client", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { WorkerPublicationHook& hook; ~Release() { hook.ReleasePublication(); } } release{hook};
    {
        RawClient probe("127.0.0.1", harness.port);
        probe.Hello();
        probe.ShutdownWrite();
        assert(probe.WaitForClose(std::chrono::seconds(5)));
    }
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    RawClient client("127.0.0.1", harness.port);
    chunkdb::test::FeedPhaseWatchdog::Phase("hook: wait before publishing dequeued connection");
    hook.WaitPublication();
    harness.server->Stop();
    hook.ReleasePublication();
    harness.JoinStopped();
    assert(!hook.Handled());
    assert(client.WaitForClose(std::chrono::seconds(5)));
    assert(hook.JoinedWorkers() == config.worker_threads);
}

#ifdef _WIN32
class ScopedWindowsHandle {
  public:
    explicit ScopedWindowsHandle(HANDLE handle) : handle_(handle) {}
    ~ScopedWindowsHandle() { if (handle_ != nullptr) CloseHandle(handle_); }
    ScopedWindowsHandle(const ScopedWindowsHandle&) = delete;
    ScopedWindowsHandle& operator=(const ScopedWindowsHandle&) = delete;
    HANDLE Get() const { return handle_; }
  private:
    HANDLE handle_;
};

class PolledPartialInputHook : public ShutdownTraceHook {
  public:
    PolledPartialInputHook()
        : release_(CreateEvent(nullptr, TRUE, FALSE, nullptr)) {
        if (release_.Get() == nullptr)
            throw std::runtime_error("partial-input-poll: CreateEvent failed");
    }
    void Run(Point point, std::size_t ordinal) override {
        ShutdownTraceHook::Run(point, ordinal);
        if (point == Point::kAfterClientRead && ordinal >= 2U) {
            { std::lock_guard lock(mutex_); returned_ = true; }
            changed_.notify_all();
        }
        if (point != Point::kBeforeClientRead || ordinal < 2U) return;
        bool block = false;
        {
            std::lock_guard lock(mutex_);
            if (thread_id_ == 0U) {
                thread_id_ = GetCurrentThreadId();
                block = true;
                changed_.notify_all();
            }
        }
        // Use the documented nonalertable native wait, independent of the
        // C++ library's condition-variable implementation on Windows.
        if (block && WaitForSingleObject(release_.Get(), INFINITE) != WAIT_OBJECT_0)
            throw std::runtime_error("partial-input-poll: idle barrier wait failed");
    }
    void Release() { (void)SetEvent(release_.Get()); }
    void WaitPolledRead() {
        {
            std::unique_lock lock(mutex_);
            if (!changed_.wait_for(lock, std::chrono::seconds(10), [&] { return thread_id_ != 0U; }))
                throw std::runtime_error("partial-input-poll: waiting for idle read after PONG");
        }
        Release();
        std::unique_lock lock(mutex_);
        if (!changed_.wait_for(lock, std::chrono::seconds(10), [&] { return returned_; }))
            throw std::runtime_error("partial-input-poll: nonblocking read did not return");
    }
  private:
    ScopedWindowsHandle release_;
    std::mutex mutex_;
    std::condition_variable changed_;
    DWORD thread_id_ = 0U;
    bool returned_ = false;
};

void TestStopPolledPartialInputWindows() {
    PolledPartialInputHook hook;
    auto config = BaseServerConfig();
    config.client_io_timeout_ms = 120000U;
    config.idle_connection_timeout_ms = 120000U;
    ServerHarness harness("stop-blocking-idle-read", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { PolledPartialInputHook& hook; ~Release() { hook.Release(); } } release{hook};
    {
        RawClient probe("127.0.0.1", harness.port);
        probe.Hello(); // FeedIo is published before observer installation.
    }
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    RawClient client("127.0.0.1", harness.port);
    client.SetReadDeadline(std::chrono::seconds(15));
    client.Hello();
    client.SendLine("PING"); assert(client.ReadLine() == "+PONG\r\n");
    // A partial request returns to the poller instead of entering a blocking receive.
    client.SendBytes("PI");
    chunkdb::test::FeedPhaseWatchdog::Phase("partial-input-poll: confirm ongoing partial-line recv");
    hook.WaitPolledRead();
    chunkdb::test::FeedPhaseWatchdog::Phase("harness: single Stop with ongoing recv");
    harness.server->Stop();
    harness.JoinStopped(); // Keep client alive; no second Stop or disconnect.
    assert(client.WaitForClose(std::chrono::seconds(5)));
    assert(hook.JoinedWorkers() == config.worker_threads);
}

class PolledInputShutdownHook final : public PolledPartialInputHook {
  public:
    explicit PolledInputShutdownHook(const char* case_name) : case_name_(case_name) {}
    void FailNextAccept() { fail_accept_.store(true); }
    void Run(Point point, std::size_t value) override {
        if (point == Point::kBeforeAccept && fail_accept_.exchange(false))
            throw std::runtime_error("injected accept-loop failure");
        // Stop holds active_clients_mutex_ across these two callbacks. Use only
        // nonthrowing recording/C APIs here; never close or retain for later I/O.
        if (point == Point::kBeforeClientShutdown) {
            if (shutdown_sockets_.fetch_add(1U) == 0U) socket_.store(static_cast<SOCKET>(value));
            else {
                std::fprintf(stderr, "FAIL partial-input fixture: unexpected extra shutdown socket=%zu\n", value);
                std::fflush(stderr);
            }
            return;
        }
        if (point == Point::kAfterClientShutdown) {
            const auto results = shutdown_results_.fetch_add(1U);
            if (results != 0U || shutdown_sockets_.load() != 1U) return;
            shutdown_error_.store(value);
            return;
        }
        if (point == Point::kAfterClientRead && value == 2U) read_returned_.store(true);
        PolledPartialInputHook::Run(point, value);
    }
    void CheckCompleted(std::size_t workers) {
        assert(shutdown_sockets_.load() == 1U && shutdown_results_.load() == 1U);
        assert(shutdown_error_.load() == 0U);
        assert(read_returned_.load());
        assert(JoinedWorkers() == workers);
        std::fprintf(stderr, "POLLED completed pid=%lu case=%s socket=%llu\n", GetCurrentProcessId(), case_name_,
            static_cast<unsigned long long>(socket_.load()));
    }
  private:
    const char* case_name_;
    std::atomic<SOCKET> socket_{INVALID_SOCKET};
    std::atomic<std::size_t> shutdown_sockets_{0U}, shutdown_results_{0U}, shutdown_error_{0U};
    std::atomic<bool> read_returned_{false};
    std::atomic<bool> fail_accept_{false};
};

void TestPolledInputOwnershipWindows(bool run_error = false) {
    const char* case_name = run_error ? "TestRunErrorPolledInputWindows" : "TestPolledInputOwnershipWindows";
    PolledInputShutdownHook hook(case_name);
    struct TerminationTrace {
        chunkdb::LogLevel previous = chunkdb::GetLogLevel();
        std::atomic<bool> observed{false};
        TerminationTrace() {
            chunkdb::SetLogLevel(chunkdb::LogLevel::kWarn);
            chunkdb::SetLogSinkForTests([this](const std::string& line) {
                if (line.find("connection terminated") == std::string::npos) return;
                observed.store(true);
                std::fprintf(stderr, "CANCEL termination: %s\n", line.c_str());
                std::fflush(stderr);
            });
        }
        ~TerminationTrace() {
            chunkdb::ResetLogSinkForTests();
            chunkdb::SetLogLevel(previous);
        }
    } termination; // Survives harness join, including exception cleanup.
    auto config = BaseServerConfig();
    config.client_io_timeout_ms = 120000U;
    config.idle_connection_timeout_ms = 120000U;
    ServerHarness harness("cancel-blocking-idle-read", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { PolledInputShutdownHook& hook; ~Release() { hook.Release(); } } release{hook};
    {
        RawClient probe("127.0.0.1", harness.port);
        probe.Hello();
        probe.ShutdownWrite();
        chunkdb::test::FeedPhaseWatchdog::Phase("cancel fixture: await readiness probe close before observer");
        assert(probe.WaitForClose(std::chrono::seconds(5)));
    }
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    RawClient client("127.0.0.1", harness.port);
    client.SetReadDeadline(std::chrono::seconds(15));
    client.SendLine("HELLO 3");
    const auto connection_id = std::stoull(ReadHelloReply(client).at("connection_id"));
    client.SendLine("PING"); assert(client.ReadLine() == "+PONG\r\n");
    client.SendBytes("PI");
    chunkdb::test::FeedPhaseWatchdog::Phase("partial-input-poll: confirm partial input returned to polling");
    hook.WaitPolledRead();
    if (run_error) {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: fail accept loop with ongoing recv");
        hook.FailNextAccept();
        RawClient trigger("127.0.0.1", harness.port);
        harness.JoinFailed("injected accept-loop failure"); // No public Stop rescues this drain.
    } else {
        chunkdb::test::FeedPhaseWatchdog::Phase("harness: single Stop with production cancellation observer");
        harness.server->Stop();
        harness.JoinStopped(); // Client remains alive; no second Stop/disconnect.
    }
    std::fprintf(stderr, "CANCEL completed pid=%lu case=%s termination_warn=%s\n",
                 GetCurrentProcessId(), case_name, termination.observed.load() ? "present" : "absent");
    std::fflush(stderr); // recv==0 has no WARN; do not invent its termination error.
    hook.CheckCompleted(config.worker_threads);
    assert(client.WaitForClose(std::chrono::seconds(5)));
    assert(!chunkdb::FeedDeliveryTestAccess::ConnectionStillOwned(*harness.server, connection_id));
}
#endif

class PendingShutdownHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t value) override {
        trace_.Run(point, value);
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforePendingWait && !entered_) {
            entered_ = true;
            changed_.notify_all();
            changed_.wait(lock, [&] { return released_; });
        } else if (point == Point::kAfterRunPendingStop) {
            run_stopped_ = true;
            changed_.notify_all();
        } else if (point == Point::kBeforePendingShutdown) {
            // The old ordering notified before taking the pending mutex. Wait
            // for Run's last notification too, so neither can rescue that gap.
            // A locked publication has not changed running yet: release the
            // predicate now, before Stop acquires its mutex and notifies.
            if (value == 0U && !changed_.wait_for(lock, std::chrono::seconds(10), [&] { return run_stopped_; }))
                throw std::runtime_error("pending shutdown barrier: waiting for Run's stop notification");
            released_ = true;
            changed_.notify_all();
        }
    }
    void WaitPending() {
        std::unique_lock lock(mutex_);
        if (!changed_.wait_for(lock, std::chrono::seconds(10), [&] { return entered_; }))
            throw std::runtime_error("pending shutdown barrier: waiting for false predicate snapshot");
    }
    void Release() {
        { std::lock_guard lock(mutex_); released_ = true; }
        changed_.notify_all();
    }
    std::size_t JoinedWorkers() { return trace_.JoinedWorkers(); }
  private:
    ShutdownTraceHook trace_;
    std::mutex mutex_;
    std::condition_variable changed_;
    bool entered_ = false, released_ = false, run_stopped_ = false;
};

void TestStopWaitingWorker() {
    PendingShutdownHook hook;
    auto config = BaseServerConfig();
    config.worker_threads = 1U;
    ServerHarness harness("stop-waiting-worker", BaseStoreConfig(),
        chunkdb::EngineConfig{.require_auth = false}, config);
    struct Release { PendingShutdownHook& hook; ~Release() { hook.Release(); } } release{hook};
    {
        RawClient probe("127.0.0.1", harness.port);
        probe.Hello();
    }
    // Only arm the initial predicate; supply no notification after Stop.
    chunkdb::FeedDeliveryTestAccess::SetPendingObserverHook(*harness.server, &hook);
    hook.WaitPending();
    chunkdb::test::FeedPhaseWatchdog::Phase("harness: single Stop at pending predicate gap");
    harness.server->Stop();
    harness.JoinStopped();
    assert(hook.JoinedWorkers() == config.worker_threads);
}

void TestLingerFailureFence(bool resume) {
    ShutdownTraceHook shutdown;
    // The hook remains alive through all store/feed shutdown callbacks.
    LingerFailureHook hook(resume ? chunkdb::FeedTestHook::Point::kAfterResume :
        chunkdb::FeedTestHook::Point::kBeforeLingerPause);
    ScopedLogCapture logs(chunkdb::LogLevel::kError);
    auto config = AuthenticatedFeedServerConfig();
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
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &shutdown);
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

#ifdef _WIN32
// A phase-watchdog exit must end only this case, never leave its server threads
// alive in the process that will run the next case.
bool IsIsolatedShutdownCase(std::string_view name) {
    return name == "TestPolledInputOwnershipWindows" || name == "TestStopPolledPartialInputWindows" ||
           name == "TestRunErrorPolledInputWindows" ||
           name == "TestLingerFailureFenceFalse" || name == "TestLingerFailureFenceTrue";
}

DWORD RunIsolatedShutdownCase(const char* name) {
    std::vector<wchar_t> executable(32768);
    const DWORD length = GetModuleFileNameW(nullptr, executable.data(), static_cast<DWORD>(executable.size()));
    if (length == 0 || length >= executable.size())
        throw std::runtime_error("shutdown child: cannot find test executable");
    // The executable is a Windows filename (no quote character), ends in a
    // filename rather than a backslash, and case IDs are fixed ASCII tokens.
    std::wstring command = L"\"" + std::wstring(executable.data(), length) + L"\" --case ";
    for (const char* at = name; *at; ++at) command += static_cast<wchar_t>(*at);
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    startup.hStdError = GetStdHandle(STD_ERROR_HANDLE);
    PROCESS_INFORMATION child{};
    if (!CreateProcessW(executable.data(), command.data(), nullptr, nullptr, TRUE, 0,
                        nullptr, nullptr, &startup, &child))
        throw std::runtime_error("shutdown child: CreateProcessW failed for " + std::string(name));
    CloseHandle(child.hThread);
    struct ProcessOwner {
        HANDLE process;
        const char* name;
        void StopAndWait() const {
            if (WaitForSingleObject(process, 0) == WAIT_OBJECT_0) return;
            if (!TerminateProcess(process, 124)) {
                const DWORD error = GetLastError();
                if (WaitForSingleObject(process, 0) == WAIT_OBJECT_0) return;
                std::fprintf(stderr, "FAIL child cleanup case=%s phase=terminate-child error=%lu\n", name, error);
                std::fflush(stderr);
                std::_Exit(125); // Never continue with a possibly live child.
            }
            constexpr DWORD kShutdownChildCleanupGuardMs = 10000;
            const DWORD waited = WaitForSingleObject(process, kShutdownChildCleanupGuardMs);
            if (waited == WAIT_OBJECT_0) return;
            if (waited == WAIT_TIMEOUT)
                std::fprintf(stderr, "TIMEOUT case=%s phase=shutdown-child-cleanup-guard limit_ms=%lu error=0\n",
                             name, kShutdownChildCleanupGuardMs);
            else
                std::fprintf(stderr, "FAIL case=%s phase=shutdown-child-cleanup-guard wait=%lu limit_ms=%lu error=%lu\n",
                             name, waited, kShutdownChildCleanupGuardMs, GetLastError());
            std::fflush(stderr);
            std::_Exit(125);
        }
        ~ProcessOwner() { StopAndWait(); CloseHandle(process); }
    } owner{child.hProcess, name};
    std::fprintf(stderr, "CHILD case=%s pid=%lu\n", name, child.dwProcessId);
    std::fflush(stderr);
    // Secondary total-process guard exceeds the child's 60s phase watchdog.
    constexpr DWORD kShutdownChildProcessGuardMs = 90000;
    const DWORD waited = WaitForSingleObject(owner.process, kShutdownChildProcessGuardMs);
    if (waited == WAIT_TIMEOUT) {
        std::fprintf(stderr, "TIMEOUT case=%s phase=shutdown-child-process-guard limit_ms=%lu\n",
                     name, kShutdownChildProcessGuardMs);
        std::fflush(stderr);
        owner.StopAndWait();
    } else if (waited != WAIT_OBJECT_0) {
        owner.StopAndWait();
        throw std::runtime_error("shutdown child: process wait failed for " + std::string(name));
    }
    DWORD exit_code = 0;
    if (!GetExitCodeProcess(owner.process, &exit_code))
        throw std::runtime_error("shutdown child: cannot read exit code for " + std::string(name));
    std::fprintf(stderr, "CHILD END case=%s pid=%lu exit=%lu\n", name, child.dwProcessId, exit_code);
    std::fflush(stderr);
    return waited == WAIT_TIMEOUT ? 124 : exit_code;
}

// Optional --case control; not part of the normal case count. It proves that
// a watchdog exit and a test failure do not prevent a subsequent child run.
void TestShutdownChildControls() {
    const auto expect = [](const char* name, DWORD expected) {
        const auto actual = RunIsolatedShutdownCase(name);
        if (actual != expected)
            throw std::runtime_error("shutdown child control: " + std::string(name) +
                                     " expected exit=" + std::to_string(expected) +
                                     " actual=" + std::to_string(actual));
    };
    expect("TestFeedPhaseSanitizedVerb", 0);
    expect("PhaseWatchdogStall", 124);
    expect("client-exception", 1);
    expect("TestFeedPhaseSanitizedVerb", 0);
}
#endif

void TestFeedWatch() {
#ifdef _WIN32
    std::size_t failed_children = 0;
#endif
    const auto run = [&](const char* name, auto function) {
#ifdef _WIN32
        if (IsIsolatedShutdownCase(name)) {
            try {
                const auto exit_code = RunIsolatedShutdownCase(name);
                if (exit_code != 0) {
                    ++failed_children;
                    std::cerr << "FAIL " << name << ": child exit=" << exit_code << '\n';
                }
            } catch (const std::exception& error) {
                ++failed_children;
                std::cerr << "FAIL " << name << ": " << error.what() << '\n';
            }
            return;
        }
#endif
        chunkdb::test::FeedPhaseWatchdog group(name);
        function();
    };
    run("TestFeedPhaseSanitizedVerb", TestFeedPhaseSanitizedVerb);
    run("TestFeedIoStopAfterDrain()", [] { TestFeedIoStopAfterDrain(); });
    run("TestStopConnectedIdleClient", TestStopConnectedIdleClient);
    run("TestStopDequeuedClient", TestStopDequeuedClient);
#ifdef _WIN32
    run("TestPolledInputOwnershipWindows", [] { TestPolledInputOwnershipWindows(); });
    run("TestRunErrorPolledInputWindows", [] { TestPolledInputOwnershipWindows(true); });
    run("TestStopPolledPartialInputWindows", TestStopPolledPartialInputWindows);
#endif
    run("TestStopWaitingWorker", TestStopWaitingWorker);
    run("TestUnwatchReleaseBeforeReply<RawClient>(false)", [] { TestUnwatchReleaseBeforeReply<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
    run("TestUnwatchReleaseBeforeReply<TlsClient>(true)", [] { TestUnwatchReleaseBeforeReply<TlsClient>(true); });
#endif
#ifdef _WIN32
    run("TestLingerFailureFenceFalse", [] { TestLingerFailureFence(false); });
    run("TestLingerFailureFenceTrue", [] { TestLingerFailureFence(true); });
#else
    run("TestLingerFailureFence(false)", [] { TestLingerFailureFence(false); });
    run("TestLingerFailureFence(true)", [] { TestLingerFailureFence(true); });
#endif
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
#ifdef _WIN32
    if (failed_children != 0)
        throw std::runtime_error("feed watch: " + std::to_string(failed_children) + " isolated shutdown cases failed");
#endif
}

// A PROXY protocol v2 header: `command` 0x21 (PROXY) for a TCP connection
// from `source` (IPv4 or IPv6) port 4321 to the server, or 0x20 (LOCAL);
// `extra` payload bytes stand for TLVs.
std::string ProxyV2Header(const std::string& source, std::uint8_t command = 0x21, std::size_t extra = 0) {
    std::string header("\r\n\r\n\0\r\nQUIT\n", 12);
    header += static_cast<char>(command);
    std::string payload;
    std::array<unsigned char, 16> address{};
    const bool v6 = source.find(':') != std::string::npos;
    if (inet_pton(v6 ? AF_INET6 : AF_INET, source.c_str(), address.data()) != 1) {
        throw std::runtime_error("bad test address " + source);
    }
    const std::size_t size = v6 ? 16U : 4U;
    std::array<unsigned char, 16> destination{};
    destination[size - 1U] = 1;  // ::1 or 0.0.0.1, unused
    payload.append(reinterpret_cast<const char*>(address.data()), size);
    payload.append(reinterpret_cast<const char*>(destination.data()), size);
    payload += std::string("\x10\xe1", 2);  // source port 4321
    payload += std::string("\x10\x92", 2);  // destination port 4242
    payload += std::string(extra, '\x04');
    header += static_cast<char>(v6 ? 0x21 : 0x11);
    header += static_cast<char>((payload.size() >> 8U) & 0xFFU);
    header += static_cast<char>(payload.size() & 0xFFU);
    return header + payload;
}

std::vector<std::uint8_t> Bytes(const std::string& text) { return {text.begin(), text.end()}; }

bool ProxyHeaderRefused(const std::string& header) {
    try {
        (void)chunkdb::proxy_protocol::Parse(Bytes(header));
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

// The header parser: IPv4, IPv6, LOCAL and an unspecified family, TLVs after
// the addresses, and every malformed form.
void TestProxyProtocolParser() {
    namespace proxy = chunkdb::proxy_protocol;
    const auto v4 = proxy::Parse(Bytes(ProxyV2Header("198.51.100.7")));
    assert(!v4.use_peer && v4.address == "198.51.100.7" && v4.endpoint == "[198.51.100.7]:4321");
    const auto v6 = proxy::Parse(Bytes(ProxyV2Header("2001:db8::5", 0x21, 40)));
    assert(!v6.use_peer && v6.address == "2001:db8::5" && v6.endpoint == "[2001:db8::5]:4321");
    assert(proxy::Parse(Bytes(ProxyV2Header("198.51.100.7", 0x20))).use_peer);
    std::string unspecified = ProxyV2Header("198.51.100.7");
    unspecified[13] = 0x00;
    assert(proxy::Parse(Bytes(unspecified)).use_peer);
    assert(proxy::PayloadLength(Bytes(ProxyV2Header("198.51.100.7"))) == 12U);

    std::string signature = ProxyV2Header("198.51.100.7");
    signature[3] = 'X';
    assert(ProxyHeaderRefused(signature));
    std::string version = ProxyV2Header("198.51.100.7");
    version[12] = 0x11;
    assert(ProxyHeaderRefused(version));
    std::string command = ProxyV2Header("198.51.100.7");
    command[12] = 0x22;
    assert(ProxyHeaderRefused(command));
    std::string udp = ProxyV2Header("198.51.100.7");
    udp[13] = 0x12;
    assert(ProxyHeaderRefused(udp));
    std::string short_v4 = ProxyV2Header("198.51.100.7");
    short_v4[15] = 4;
    assert(ProxyHeaderRefused(short_v4.substr(0, 20)));
    assert(ProxyHeaderRefused(ProxyV2Header("198.51.100.7").substr(0, 20)));
    assert(ProxyHeaderRefused(ProxyV2Header("198.51.100.7", 0x21, proxy::kMaxPayloadBytes)));
    assert(!ProxyHeaderRefused(ProxyV2Header("198.51.100.7", 0x21, proxy::kMaxPayloadBytes - 12U)));
    assert(ProxyHeaderRefused("PROXY TCP4 198.51.100.7 127.0.0.1 4321 4242\r\n"));
}

chunkdb::ServerConfig ProxyServerConfig(bool tls) {
    auto config = BaseServerConfig();
    config.proxy_protocol_v2 = true;
    config.tls_enabled = tls;
    config.worker_threads = 4;
    return config;
}

class ScheduledAuthHook : public chunkdb::FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        if (point != Point::kAfterAuthReplyScheduled) return;
        { std::lock_guard lock(mutex_); ++scheduled_; }
        changed_.notify_all();
    }
    void Wait(std::size_t count) {
        std::unique_lock lock(mutex_);
        assert(changed_.wait_for(lock, std::chrono::seconds(5), [&] { return scheduled_ >= count; }));
    }
  private:
    std::mutex mutex_;
    std::condition_variable changed_;
    std::size_t scheduled_ = 0U;
};

void TestDelayedAuthRepliesDoNotOccupyWorkers() {
    ScheduledAuthHook hook;
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    engine_cfg.max_auth_failures_per_ip = 1;
    engine_cfg.auth_failure_delay_ms = 20000;
    engine_cfg.auth_failure_ban_ms = 20000;
    auto server_cfg = ProxyServerConfig(false);
    server_cfg.worker_threads = 1;
    server_cfg.client_io_timeout_ms = 30000;
    ServerHarness harness("scheduled-auth", BaseStoreConfig(), engine_cfg, server_cfg);
    chunkdb::FeedDeliveryTestAccess::SetObserverHook(*harness.server, &hook);
    std::vector<std::unique_ptr<RawClient>> rejected;
    for (std::size_t i = 0U; i < 8U; ++i) {
        auto client = std::make_unique<RawClient>("127.0.0.1", harness.port);
        client->SetReadDeadline(std::chrono::seconds(5));
        client->SendBytes(ProxyV2Header("198.51.100." + std::to_string(i + 1U)));
        const auto login = chunkdb::scram::StartClientLogin(kAdminUser, chunkdb::scram::NewNonce());
        client->SendBytes(chunkdb::test::HelloUserBytes(login, kAdminUser));
        const auto challenge = client->ReadLine();
        assert(challenge.starts_with("+SCRAM "));
        client->SendBytes(chunkdb::test::WrongAuthBytes(challenge));
        hook.Wait(i + 1U);
        rejected.push_back(std::move(client));
    }
    RawClient banned("127.0.0.1", harness.port);
    const auto banned_login = chunkdb::scram::StartClientLogin(kAdminUser, chunkdb::scram::NewNonce());
    banned.SendBytes(ProxyV2Header("198.51.100.1") + chunkdb::test::HelloUserBytes(banned_login, kAdminUser));
    hook.Wait(9U);
    RawClient accepted("127.0.0.1", harness.port);
    accepted.SetReadDeadline(std::chrono::seconds(5));
    accepted.SendBytes(ProxyV2Header("203.0.113.20"));
    accepted.Login();
    accepted.SendLine("PING");
    assert(accepted.ReadLine() == "+PONG\r\n");
}

std::string ReadText(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// With --proxy-protocol v2 the header's source address is the client's for
// login bans, the per-address handshake limit and the audit log, over IPv4,
// IPv6 and LOCAL headers.
void TestProxyProtocolSourceAddress() {
    const auto audit_path = TempDataDir("proxy-audit") / "audit.log";
    std::filesystem::create_directories(audit_path.parent_path());
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    engine_cfg.max_auth_failures_per_ip = 2;
    engine_cfg.auth_failure_ban_ms = 60000;
    engine_cfg.auth_failure_delay_ms = 0;
    engine_cfg.audit = std::make_shared<chunkdb::AuditLog>(audit_path);
    auto server_cfg = ProxyServerConfig(false);
    // Two, not one: while a header is read the connection also holds a slot
    // for the socket's peer, 127.0.0.1 here, and the startup probe may still
    // hold one.
    server_cfg.max_handshakes_per_ip = 2;
    {
        ServerHarness harness("proxy-source", BaseStoreConfig(), engine_cfg, server_cfg);
        {
            RawClient failing("127.0.0.1", harness.port);
            failing.SendBytes(ProxyV2Header("198.51.100.7"));
            const auto first_failure = FailedLoginReply(failing, kAdminUser, "wrong-1");
            if (!first_failure.starts_with("-ERR AUTH_FAILED")) std::cerr << "unexpected login refusal: " << first_failure;
            assert(first_failure.rfind("-ERR AUTH_FAILED", 0) == 0);
            assert(FailedLoginReply(failing, kAdminUser, "wrong-2").rfind("-ERR AUTH_FAILED", 0) == 0);
        }
        {
            RawClient banned("127.0.0.1", harness.port);
            banned.SendBytes(ProxyV2Header("198.51.100.7"));
            assert(LoginReply(banned, kAdminUser, kAdminPassword).find("temporary auth ban") != std::string::npos);
        }
        for (const auto& source : {std::string("198.51.100.8"), std::string("2001:db8::5")}) {
            RawClient other("127.0.0.1", harness.port);
            other.SendBytes(ProxyV2Header(source));
            other.Login();
            other.SendLine("PING");
            assert(other.ReadLine() == "+PONG\r\n");
        }
        {
            // A LOCAL header (the proxy's own check) keeps the socket's peer.
            RawClient local("127.0.0.1", harness.port);
            local.SendBytes(ProxyV2Header("198.51.100.7", 0x20));
            local.Login();
        }
        // Two connections before HELLO per carried source address.
        const auto wait_for_handshakes = [&](const std::string& source, std::size_t count) {
            const auto deadline = Clock::now() + std::chrono::seconds(5);
            while (harness.server->HandshakesInProgressForTests(source) != count) {
                assert(Clock::now() < deadline);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        };
        RawClient holding("127.0.0.1", harness.port);
        holding.SendBytes(ProxyV2Header("203.0.113.1"));
        wait_for_handshakes("203.0.113.1", 1);
        RawClient holding_second("127.0.0.1", harness.port);
        holding_second.SendBytes(ProxyV2Header("203.0.113.1"));
        wait_for_handshakes("203.0.113.1", 2);
        {
            RawClient busy("127.0.0.1", harness.port);
            busy.SendBytes(ProxyV2Header("203.0.113.1"));
            std::string reply;
            assert(busy.ReadLineWithin(std::chrono::milliseconds(3000), &reply));
            assert(reply == "-ERR BUSY reason=busy too many connections before HELLO from this address\r\n");
        }
        RawClient elsewhere("127.0.0.1", harness.port);
        elsewhere.SendBytes(ProxyV2Header("203.0.113.2"));
        elsewhere.Login();
    }
    const auto audit = ReadText(audit_path);
    const auto has = [&](const std::string& peer, const std::string& outcome) {
        for (std::size_t at = 0; (at = audit.find("\"peer\":\"" + peer + "\"", at)) != std::string::npos; ++at) {
            const auto begin = audit.rfind('{', at);
            const auto end = audit.find('}', at);
            if (audit.substr(begin, end - begin).find(outcome) != std::string::npos) return true;
        }
        return false;
    };
    assert(has("198.51.100.7", "\"reason\":\"wrong_password\""));
    assert(has("198.51.100.7", "\"reason\":\"banned\""));
    assert(has("198.51.100.8", "\"ok\":true"));
    assert(has("2001:db8::5", "\"ok\":true"));
    assert(has("127.0.0.1", "\"ok\":true"));
    assert(has("203.0.113.2", "\"ok\":true"));
    RemoveAllWithRetry(audit_path.parent_path());
}

// Absent, malformed, oversized and truncated headers close the connection
// without a reply, and the server keeps serving.
void TestProxyProtocolRefusesBadHeaders() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    auto server_cfg = ProxyServerConfig(false);
    server_cfg.client_io_timeout_ms = 500;
    ServerHarness harness("proxy-refused", BaseStoreConfig(), engine_cfg, server_cfg);
    std::string bad_signature = ProxyV2Header("198.51.100.7");
    bad_signature[0] = 'H';
    std::string version_one = ProxyV2Header("198.51.100.7");
    version_one[12] = 0x11;
    std::string oversized = ProxyV2Header("198.51.100.7");
    oversized[14] = 0x08;  // 2048 + 12 bytes announced
    for (const std::string& bytes : {std::string("HELLO 3\r\nPING\r\n"), bad_signature, version_one, oversized,
                                     std::string("PROXY TCP4 198.51.100.7 127.0.0.1 4321 4242\r\nHELLO 3\r\n"),
                                     ProxyV2Header("198.51.100.7").substr(0, 10),
                                     ProxyV2Header("198.51.100.7").substr(0, 20)}) {
        RawClient client("127.0.0.1", harness.port);
        client.SendBytes(bytes);
        assert(client.WaitForClose(std::chrono::seconds(5)));
        assert(client.Received().empty());
    }
    RawClient good("127.0.0.1", harness.port);
    good.SendBytes(ProxyV2Header("198.51.100.7") + "HELLO 3\r\n");
    assert(ReadHelloReply(good).at("protocol") == "3");
    good.SendLine("PING");
    assert(good.ReadLine() == "+PONG\r\n");
}

// Without --proxy-protocol v2 a header is never read as one: it reaches the
// server as protocol bytes, which it refuses.
void TestProxyProtocolOffIgnoresHeaders() {
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5};
    ServerHarness harness("proxy-off", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.SendBytes(ProxyV2Header("198.51.100.7") + "HELLO 3\r\n");
    std::string reply;
    assert(client.ReadLineWithin(std::chrono::milliseconds(3000), &reply));
    assert(reply.rfind("-ERR ", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(5)));
}

#ifdef CHUNKDB_WITH_OPENSSL
// Over TLS the header comes before the handshake; a client without one
// cannot complete the handshake.
void TestProxyProtocolOverTls() {
    const auto audit_path = TempDataDir("proxy-tls-audit") / "audit.log";
    std::filesystem::create_directories(audit_path.parent_path());
    auto engine_cfg = chunkdb::EngineConfig{.require_auth = true, .max_auth_failures = 5};
    engine_cfg.audit = std::make_shared<chunkdb::AuditLog>(audit_path);
    auto server_cfg = ProxyServerConfig(true);
    server_cfg.client_io_timeout_ms = 2000;
    {
        ServerHarness harness("proxy-tls", BaseStoreConfig(), engine_cfg, server_cfg);
        for (const auto& source : {std::string("198.51.100.9"), std::string("2001:db8::9")}) {
            TlsClient client("127.0.0.1", harness.port, 0, ProxyV2Header(source));
            client.Login();
            client.SendLine("PING");
            assert(client.ReadLine() == "+PONG\r\n");
        }
        bool refused = false;
        try {
            TlsClient missing("127.0.0.1", harness.port);
        } catch (const std::runtime_error&) {
            refused = true;
        }
        assert(refused);
    }
    const auto audit = ReadText(audit_path);
    assert(audit.find("\"peer\":\"198.51.100.9\",\"ok\":true") != std::string::npos);
    assert(audit.find("\"peer\":\"2001:db8::9\",\"ok\":true") != std::string::npos);
    RemoveAllWithRetry(audit_path.parent_path());
}
#endif

// RELOAD TLS on a server without a TLS listener is refused.
void TestReloadTlsWithoutTls() {
    ServerHarness harness("reload-tls-plain", BaseStoreConfig(),
                          chunkdb::EngineConfig{.require_auth = false, .max_auth_failures = 5}, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    client.SendLine("RELOAD TLS");
    assert(client.ReadLine() == "-ERR INVALID_ARGUMENT reason=invalid_argument this server has no TLS listener to reload\r\n");
    client.SendLine("RELOAD");
    assert(client.ReadLine().rfind("-ERR SYNTAX", 0) == 0);
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
#ifdef _WIN32
        const bool isolated = selected.empty() && IsIsolatedShutdownCase(name);
#else
        constexpr bool isolated = false;
#endif
        if (std::string_view(name) != "TestFeedWatch" && !isolated) watchdog.emplace(name);
        std::cerr << "RUN " << name << '\n';
        try {
#ifdef _WIN32
            if (isolated) {
                const auto exit_code = RunIsolatedShutdownCase(name);
                if (exit_code != 0)
                    throw std::runtime_error("isolated shutdown child exit=" + std::to_string(exit_code));
            } else
#endif
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
#ifdef _WIN32
        if (selected == "ShutdownChildControls") run("ShutdownChildControls", TestShutdownChildControls);
#endif
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
        run("TestStopConnectedIdleClient", TestStopConnectedIdleClient);
        run("TestStopDequeuedClient", TestStopDequeuedClient);
#ifdef _WIN32
        run("TestPolledInputOwnershipWindows", [] { TestPolledInputOwnershipWindows(); });
        run("TestRunErrorPolledInputWindows", [] { TestPolledInputOwnershipWindows(true); });
        run("TestStopPolledPartialInputWindows", TestStopPolledPartialInputWindows);
#endif
        run("TestStopWaitingWorker", TestStopWaitingWorker);
        run("TestLingerFailureFenceFalse", [] { TestLingerFailureFence(false); });
        run("TestLingerFailureFenceTrue", [] { TestLingerFailureFence(true); });
        run("TestUnwatchReleaseBeforeReply", [] { TestUnwatchReleaseBeforeReply<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestUnwatchReleaseBeforeReplyTls", [] { TestUnwatchReleaseBeforeReply<TlsClient>(true); });
#endif
        run("TestWatchDefaultProgress", [] { TestWatchDefaultProgress<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestWatchDefaultProgressTls", [] { TestWatchDefaultProgress<TlsClient>(true); });
#endif
        run("TestPing", TestPing);
        run("TestIdleConnectionsReleaseWorker", [] { TestIdleConnectionsReleaseWorker<RawClient>(false); });
        run("TestIdleDispatchFailureClosesEveryOwner", TestIdleDispatchFailureClosesEveryOwner);
        run("TestIdleDispatchRespectsQueueCredit", TestIdleDispatchRespectsQueueCredit);
        run("TestQueuedCancellationConsumesUserRate", TestQueuedCancellationConsumesUserRate);
        run("TestStopPublicationGapClosesQueuedOwner", TestStopPublicationGapClosesQueuedOwner);
        run("TestOrdinaryWatchRateRepliesAreBounded", TestOrdinaryWatchRateRepliesAreBounded);
        run("TestExpiredQueueRetainsTransportOwner", TestExpiredQueueRetainsTransportOwner);
        run("TestStopDoesNotCloseQueuedReplyOwner", TestStopDoesNotCloseQueuedReplyOwner);
        run("TestBufferedRequestsRetainWorker", TestBufferedRequestsRetainWorker);
        run("TestKilledBufferedStatementCannotPublish", TestKilledBufferedStatementCannotPublish);
        run("TestProgressiveChunkReplyOwnsSnapshot", [] { TestProgressiveReplyOwnsSnapshot<RawClient>(false, false); });
        run("TestProgressiveAreaReplyOwnsSnapshot", [] { TestProgressiveReplyOwnsSnapshot<RawClient>(false, true); });
        run("TestKillReadReplyReleasesCredit", [] { TestKillReadReplyReleasesCredit<RawClient>(false); });
        run("TestHelloIndependentOfStatementWorkers", [] { TestHelloIndependentOfStatementWorkers<RawClient>(false); });
        run("TestPartialInputDoesNotHoldWorker", [] { TestPartialInputDoesNotHoldWorker<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestIdleConnectionsReleaseWorkerTls", [] { TestIdleConnectionsReleaseWorker<TlsClient>(true); });
        run("TestProgressiveChunkReplyOwnsSnapshotTls", [] { TestProgressiveReplyOwnsSnapshot<TlsClient>(true, false); });
        run("TestProgressiveAreaReplyOwnsSnapshotTls", [] { TestProgressiveReplyOwnsSnapshot<TlsClient>(true, true); });
        run("TestKillReadReplyReleasesCreditTls", [] { TestKillReadReplyReleasesCredit<TlsClient>(true); });
        run("TestHelloIndependentOfStatementWorkersTls", [] { TestHelloIndependentOfStatementWorkers<TlsClient>(true); });
        run("TestPartialInputDoesNotHoldWorkerTls", [] { TestPartialInputDoesNotHoldWorker<TlsClient>(true); });
#endif
        run("TestProtocolOneClientIsRefused", TestProtocolOneClientIsRefused);
        run("TestAuthAndSetGet", TestAuthAndSetGet);
        run("TestChunkGetLengthsAndForms", TestChunkGetLengthsAndForms);
        run("TestChunkPutWritesAndFraming", TestChunkPutWritesAndFraming);
        run("TestParameterFrameSum", TestParameterFrameSum);
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
        run("TestHelloNegotiation", TestHelloNegotiation);
        run("TestGrammarIndependentFrames", TestGrammarIndependentFrames);
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
        run("TestTlsReload", TestTlsReload);
        run("TestTlsMinimumVersion", TestTlsMinimumVersion);
#endif
        run("TestReadTimeoutLogsPhaseAndReason", TestReadTimeoutLogsPhaseAndReason);
        run("TestProxyProtocolParser", TestProxyProtocolParser);
        run("TestProxyProtocolSourceAddress", TestProxyProtocolSourceAddress);
        run("TestDelayedAuthRepliesDoNotOccupyWorkers", TestDelayedAuthRepliesDoNotOccupyWorkers);
        run("TestProxyProtocolRefusesBadHeaders", TestProxyProtocolRefusesBadHeaders);
        run("TestProxyProtocolOffIgnoresHeaders", TestProxyProtocolOffIgnoresHeaders);
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestProxyProtocolOverTls", TestProxyProtocolOverTls);
#endif
        run("TestReloadTlsWithoutTls", TestReloadTlsWithoutTls);
        run("TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe", TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe);
        run("TestSendTimeoutSetupFailureClosesConnection", TestSendTimeoutSetupFailureClosesConnection);
        run("TestNonblockingSetupFailureClosesConnection", TestNonblockingSetupFailureClosesConnection);
        run("TestSlowRequestDribbleDeadlineReleasesWorker", TestSlowRequestDribbleDeadlineReleasesWorker);
        run("TestIdleClientRemainsConnectedBetweenCommands", TestIdleClientRemainsConnectedBetweenCommands);
        run("TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests", TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests);
        run("TestLongIdleConnectionTimeoutReleasesWorker", [] { TestLongIdleConnectionTimeoutReleasesWorker(); });
        run("TestBeforeHelloIdleDeadlineRemainsBounded", [] { TestLongIdleConnectionTimeoutReleasesWorker(false); });
        run("TestQueuedStatementTimeoutPreservesConnection", TestQueuedStatementTimeoutPreservesConnection);
        run("TestWideNullableReadHasBoundedReplyAdmission", TestWideNullableReadHasBoundedReplyAdmission);
        run("TestReplyAdmissionPrecedesMutation", TestReplyAdmissionPrecedesMutation);
        run("TestRemovingCreditHeadAdmitsNextWaiter", TestRemovingCreditHeadAdmitsNextWaiter);
        run("TestKillIdleTransactionReleasesHistory", [] { TestKillIdleTransactionReleasesHistory<RawClient>(false); });
        run("TestKillDecidedCommitPreservesOutcome", [] { TestKillDecidedCommitPreservesOutcome<RawClient>(false); });
#ifdef CHUNKDB_WITH_OPENSSL
        run("TestKillIdleTransactionReleasesHistoryTls", [] { TestKillIdleTransactionReleasesHistory<TlsClient>(true); });
        run("TestKillDecidedCommitPreservesOutcomeTls", [] { TestKillDecidedCommitPreservesOutcome<TlsClient>(true); });
#endif
        run("TestSlowResponseDrainDeadlineReleasesWorker", TestSlowResponseDrainDeadlineReleasesWorker);
        run("TestSteadyReaderRenewsReplyProgressDeadline", TestSteadyReaderRenewsReplyProgressDeadline);
        run("TestIdlePeerCloseDoesNotLogTerminationWarning", TestIdlePeerCloseDoesNotLogTerminationWarning);
        run("TestPendingStatementQueueKeepsHelloAvailable", TestPendingStatementQueueKeepsHelloAvailable);
        run("TestReadinessLogLineExists", TestReadinessLogLineExists);
        run("TestWarnLineOnBadRequest", TestWarnLineOnBadRequest);
        run("TestErrorLineOnListenFailure", TestErrorLineOnListenFailure);
        run("TestServerHarnessStartupFailure", TestServerHarnessStartupFailure);
#ifndef _WIN32
        run("TestClientSendOutlastsAPausedReader", TestClientSendOutlastsAPausedReader);
#endif
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
