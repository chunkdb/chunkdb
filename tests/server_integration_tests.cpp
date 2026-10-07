#include <array>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
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
#include "chunkdb/zrle.hpp"

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

    // HELLO 2 [AUTH <token>]; every connection starts with it.
    void Hello(const std::string& token = "") {
        SendLine(token.empty() ? std::string("HELLO 2") : "HELLO 2 AUTH " + token);
        const std::string reply = ReadBulkText();
        if (reply.find("protocol=2\n") == std::string::npos) {
            throw std::runtime_error("unexpected HELLO reply");
        }
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
        if (SSL_connect(session_) != 1) {
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

    // HELLO 2 [AUTH <token>]; every connection starts with it.
    void Hello(const std::string& token = "") {
        SendLine(token.empty() ? std::string("HELLO 2") : "HELLO 2 AUTH " + token);
        const std::string reply = ReadBulkText();
        if (reply.find("protocol=2\n") == std::string::npos) {
            throw std::runtime_error("unexpected HELLO reply");
        }
    }

    void SendBytes(const std::string& data) {
        std::size_t offset = 0;
        while (offset < data.size()) {
            const int written = SSL_write(
                session_,
                data.data() + offset,
                static_cast<int>(data.size() - offset));
            if (written <= 0) {
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
        return written == static_cast<decltype(written)>(data.size());
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
            const int read = SSL_read(session_, buffer, static_cast<int>(sizeof(buffer)));
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
            const int read = SSL_read(session_, buffer, static_cast<int>(sizeof(buffer)));
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

struct ServerHarness {
    std::filesystem::path data_dir;
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;
    bool tls_enabled = false;

    [[nodiscard]] chunkdb::Geometry geometry() const {
        return chunkdb::Geometry(catalog->Find("default")->geometry());
    }

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

        catalog = std::make_shared<chunkdb::TableCatalog>(
            chunkdb::CatalogConfigFromStoreConfig(store_config));
        engine = std::make_shared<chunkdb::CommandEngine>(engine_config, catalog);
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
        catalog.reset();

        if (run_error) {
            try {
                std::rethrow_exception(run_error);
            } catch (...) {
                // avoid throwing from destructor
            }
        }

        RemoveAllWithRetry(data_dir);
        RemoveAllWithRetry(TlsCredentialsDir());
    }

  private:
    std::exception_ptr run_error;

    [[nodiscard]] std::filesystem::path TlsCredentialsDir() const {
        return data_dir.string() + "-tls";
    }

    void WaitUntilListening() {
        const auto deadline = Clock::now() + std::chrono::seconds(3);

        while (Clock::now() < deadline) {
            if (run_error) {
                std::rethrow_exception(run_error);
            }

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

    client.Hello();
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
}

// A protocol 1 client fails at once with a clear error, and so does any
// command before HELLO or an unsupported protocol version.
void TestProtocolOneClientIsRefused() {
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "secret",
        .require_auth = true,
        .max_auth_failures = 5,
    };
    ServerHarness harness("protocol-one", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    for (const char* first : {"AUTH secret", "PING", "INFO", "HELLO 1", "HELLO 3 AUTH secret"}) {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine(first);
        assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 2\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    {
        // A 1.x client that pipelines AUTH and a binary write gets the error
        // for AUTH and the connection closes before the payload is parsed.
        RawClient client("127.0.0.1", harness.port);
        client.SendBytes("AUTH secret\r\nCHUNKSETBIN 0 0 8\r\n12345678\r\n");
        assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 2\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }
    RawClient client("127.0.0.1", harness.port);
    client.Hello("secret");
    client.SendLine("HELLO 2 AUTH secret");
    assert(client.ReadLine().rfind("-ERR PROTOCOL HELLO was already sent", 0) == 0);
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

    client.SendLine("HELLO 2");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);

    client.SendLine("HELLO 2 AUTH bad");
    assert(client.ReadLine().rfind("-ERR AUTH_FAILED", 0) == 0);

    client.SendLine("HELLO 2 AUTH secret TABLE missing");
    assert(client.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);

    client.Hello("secret");

    client.SendLine("SET 1 2 1111");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("GET 1 2");
    assert(client.ReadBulkText() == "1111");

    // An explicit zero is a value; an unset block is $-1.
    client.SendLine("SET 2 2 0000");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("GET 2 2");
    assert(client.ReadBulkText() == "0000");
    client.SendLine("UNSET 2 2");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("GET 2 2");
    assert(client.ReadLine() == "$-1\r\n");
    client.SendLine("MGET 1 2 2 2");
    assert(client.ReadLine() == "*2\r\n");
    assert(client.ReadBulkText() == "1111");
    assert(client.ReadLine() == "$-1\r\n");
}

// Reads a CHUNKPUT / CHUNKBATCH reply: the chunk version after the write.
std::uint64_t ReadVersion(RawClient& client) {
    const std::string text = client.ReadBulkText();
    return std::stoull(text);
}

void TestChunkPutWritesAndFraming() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
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

    std::string payload;
    for (std::size_t i = 0; i < payload_bytes; ++i) {
        payload.push_back(static_cast<char>(0xA0 + i));
    }

    // Plain form: full payload, every block becomes present. The reply is
    // the new version, as CHUNKVER reports it.
    client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
    client.SendBytes(payload + "\r\n");
    const std::uint64_t first = ReadVersion(client);
    client.SendLine("CHUNKVER 0 0");
    assert(ReadVersion(client) == first);
    client.SendLine("CHUNKGET 0 0");
    const auto read_back = client.ReadBulkBytes();
    assert(std::string(read_back.begin(), read_back.end()) == payload);
    client.SendLine("CHUNKGET 0 0 STATE");
    const auto state_back = client.ReadBulkBytes();
    assert(state_back.size() == payload_bytes + presence_bytes);
    assert(state_back[payload_bytes] == 0xFF && state_back[payload_bytes + 1] == 0xFF);

    // Writing the same state again changes nothing: the current version.
    client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
    client.SendBytes(payload + "\r\n");
    assert(ReadVersion(client) == first);

    // STATE form with a sparse presence bitmap (blocks 0 and 15: bit i of the
    // bitmap is byte i/8, 1 << (i % 8)), an LF terminator, and a pipelined
    // command in the same write.
    const std::string state = payload + std::string("\x01\x80", 2);
    client.SendBytes(
        "CHUNKPUT 1 0 STATE " + std::to_string(state.size()) + "\r\n" + state + "\nPING\r\n");
    (void)ReadVersion(client);
    assert(client.ReadLine() == "+PONG\r\n");
    client.SendLine("CHUNKGET 1 0 STATE");
    const auto sparse_back = client.ReadBulkBytes();
    assert(sparse_back.size() == payload_bytes + presence_bytes);
    assert(sparse_back[payload_bytes] == 0x01 && sparse_back[payload_bytes + 1] == 0x80);
    // Absent blocks are canonicalized to zero payload bits: only block 0
    // (bits 0..3, the low nibble of byte 0) and block 15 (bits 60..63, the
    // high nibble of the last byte) survive.
    assert(sparse_back[0] == static_cast<std::uint8_t>(payload[0] & 0x0F));
    assert(sparse_back[payload_bytes - 1] == static_cast<std::uint8_t>(payload[payload_bytes - 1] & 0xF0));
    for (std::size_t i = 1; i + 1 < payload_bytes; ++i) {
        assert(sparse_back[i] == 0);
    }

    // IF: the conditional write, binary and free of the line limit.
    client.SendLine("CHUNKVER 1 0");
    const std::uint64_t current = ReadVersion(client);
    client.SendLine("CHUNKPUT 1 0 STATE IF " + std::to_string(current + 1) + " 10");
    client.SendBytes(std::string(10, '\x55') + "\r\n");
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(current) + "\r\n");
    client.SendLine("CHUNKGET 1 0 STATE");
    assert(client.ReadBulkBytes() == sparse_back);  // unchanged
    client.SendLine("CHUNKPUT 1 0 IF " + std::to_string(current) + " 8");
    client.SendBytes(payload + "\r\n");
    const std::uint64_t after_if = ReadVersion(client);
    assert(after_if > current);
    client.SendLine("CHUNKGET 1 0");
    const auto if_back = client.ReadBulkBytes();
    assert(std::string(if_back.begin(), if_back.end()) == payload);

    // ZRLE upload, options in any order.
    const auto compressed = chunkdb::ZrleCompress(
        std::vector<std::uint8_t>(state.begin(), state.end()));
    client.SendLine("CHUNKPUT 2 0 ZRLE STATE " + std::to_string(compressed.size()));
    client.SendBytes(std::string(compressed.begin(), compressed.end()) + "\r\n");
    (void)ReadVersion(client);
    client.SendLine("CHUNKGET 2 0 STATE");
    assert(client.ReadBulkBytes() == sparse_back);
    // A payload that is not valid zrle for this size is read and refused.
    client.SendLine("CHUNKPUT 2 0 ZRLE 8");
    client.SendBytes(payload + "\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT zrle payload is invalid", 0) == 0);

    // A wrong length that still fits the geometry bound is read and drained,
    // the command fails, and the connection stays usable.
    client.SendLine("CHUNKPUT 3 0 2");
    client.SendBytes("\x01\x02\r\n");
    const std::string short_reply = client.ReadLine();
    assert(short_reply.rfind("-ERR INVALID_ARGUMENT", 0) == 0);
    assert(short_reply.find("does not match expected") != std::string::npos);
    client.SendLine("CHUNKEXISTS 3 0");
    assert(client.ReadLine() == "+0\r\n");

    // A header that does not parse cannot be trusted to frame its payload:
    // it is refused unread and the connection closes.
    {
        RawClient bad_option("127.0.0.1", harness.port);
        bad_option.Hello();
        bad_option.SendLine("CHUNKPUT 3 0 BOGUS 8");
        assert(bad_option.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(bad_option.WaitForClose(std::chrono::seconds(5)));
    }
    client.SendLine("CHUNKEXISTS 3 0");
    assert(client.ReadLine() == "+0\r\n");

    // A payload not followed by an empty line desynchronizes the stream, so
    // the server answers BAD_REQUEST and closes.
    {
        RawClient bad_terminator("127.0.0.1", harness.port);
        bad_terminator.Hello();
        bad_terminator.SendLine("CHUNKPUT 3 0 " + std::to_string(payload_bytes));
        bad_terminator.SendBytes(payload + "XX\r\n");
        assert(bad_terminator.ReadLine().rfind("-ERR BAD_REQUEST", 0) == 0);
        assert(bad_terminator.WaitForClose(std::chrono::seconds(5)));
    }

    // A declared length above the chunk state size (or, with ZRLE, above it
    // plus the codec slack) is refused before any payload is buffered.
    for (const std::string& header :
         {"CHUNKPUT 3 0 STATE " + std::to_string(payload_bytes + presence_bytes + 1),
          "CHUNKPUT 3 0 ZRLE STATE " + std::to_string(payload_bytes + presence_bytes + 17)}) {
        RawClient oversize("127.0.0.1", harness.port);
        oversize.Hello();
        oversize.SendLine(header);
        assert(oversize.ReadLine().rfind("-ERR BAD_REQUEST", 0) == 0);
        assert(oversize.WaitForClose(std::chrono::seconds(5)));
    }
    // Headers that cannot be framed (unparsable length, wrong arity) are
    // refused and the connection closed, since the payload length is unknown.
    for (const char* header : {"CHUNKPUT 3 0 8x", "CHUNKPUT 3 0 STATE 10 extra", "CHUNKPUT 3"}) {
        RawClient bad("127.0.0.1", harness.port);
        bad.Hello();
        bad.SendLine(header);
        assert(bad.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(bad.WaitForClose(std::chrono::seconds(5)));
    }
    client.SendLine("CHUNKEXISTS 3 0");
    assert(client.ReadLine() == "+0\r\n");
}

// Per-block extra data over the wire: XPUT is framed like CHUNKPUT (bounded
// by extra_max_block_bits), CHUNKGET/CHUNKPUT ... STATE EXTRA carry the
// state and the EXTRA section, and HELLO names the capability.
void TestExtraDataOverTcp() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    ServerHarness harness("extra-data", store_cfg, engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 2");
    const auto hello = client.ReadBulkText();
    assert(hello.find("capabilities=zrle,extra-data\n") != std::string::npos);
    assert(hello.find("max_extra_chunk_bytes=16777216\n") != std::string::npos);
    assert(hello.find("extra_max_block_bits=0\nextra_max_chunk_bytes=0\n") != std::string::npos);
    // Enabled on the table the connection already uses.
    client.SendLine("TABLESET default extra_max_block_bits 64 extra_max_chunk_bytes 256");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("TABLEINFO default");
    assert(client.ReadBulkText().find("extra_max_block_bits=64\nextra_max_chunk_bytes=256\n") != std::string::npos);

    client.SendLine("SET 1 1 1010");
    assert(client.ReadLine() == "+OK\r\n");
    // Padding bits are cleared; an LF terminator and a pipelined command.
    client.SendBytes("XPUT 1 1 12 2\r\n\xAB\xFF\nPING\r\n");
    assert(client.ReadLine() == "+OK\r\n");
    assert(client.ReadLine() == "+PONG\r\n");
    client.SendLine("XGET 1 1");
    assert(client.ReadBulkBytes() == (std::vector<std::uint8_t>{12, 0, 0, 0, 0xAB, 0x0F}));

    // Requests that break a table limit are read, dropped and refused; the
    // connection stays usable, also for what follows in the same write.
    client.SendLine("XPUT 1 1 12 1");
    client.SendBytes("\x01\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT XPUT of 12 bits takes 2 bytes", 0) == 0);
    client.SendLine("XPUT 2 2 8 1");
    client.SendBytes("\x01\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT block (2,2) is not set", 0) == 0);
    client.SendBytes("XPUT 1 1 65 9\r\n" + std::string(9, '\x07') + "\r\nPING\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT XPUT bit_length", 0) == 0);
    assert(client.ReadLine() == "+PONG\r\n");
    {
        // A section over extra_max_chunk_bytes, large enough to arrive in
        // many reads.
        const std::size_t length = 8 + 2 + 200000;
        client.SendLine("CHUNKPUT 1 0 STATE EXTRA " + std::to_string(length));
        client.SendBytes(std::string(length, '\0') + "\r\n");
        assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT extra data section of 200000 bytes", 0) == 0);
    }
    // A table without extra data refuses the same way.
    client.SendLine("TABLECREATE plain block_bits 4 chunk_width_blocks 4 chunk_height_blocks 4");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("USE plain");
    assert(client.ReadBulkText().find("extra_max_block_bits=0\n") != std::string::npos);
    client.SendLine("XPUT 0 0 8 1");
    client.SendBytes("\x01\r\n");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT extra data is not enabled on table 'plain'", 0) == 0);
    client.SendLine("XGET 0 0");
    assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT extra data is not enabled", 0) == 0);
    client.SendLine("USE default");
    (void)client.ReadBulkText();

    // The state and the section round-trip through another chunk.
    client.SendLine("CHUNKGET 0 0 STATE EXTRA");
    const auto state_extra = client.ReadBulkBytes();
    assert(state_extra.size() == 8U + 2U + 8U + 2U);
    client.SendLine("CHUNKPUT 1 0 STATE EXTRA " + std::to_string(state_extra.size()));
    client.SendBytes(std::string(state_extra.begin(), state_extra.end()) + "\r\n");
    (void)ReadVersion(client);
    client.SendLine("CHUNKGET 1 0 STATE EXTRA");
    assert(client.ReadBulkBytes() == state_extra);
    client.SendLine("XGET 5 1");
    assert(client.ReadBulkBytes() == (std::vector<std::uint8_t>{12, 0, 0, 0, 0xAB, 0x0F}));
    client.SendLine("XDEL 1 1");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("XGET 1 1");
    assert(client.ReadLine() == "$-1\r\n");

    // Lengths above the protocol caps, or a payload without its empty line,
    // are refused unread and the connection closes.
    for (const std::string& header :
         {std::string("XPUT 1 1 64 16777209"),
          std::string("CHUNKPUT 1 0 STATE EXTRA ") + std::to_string(8 + 2 + 16777216 + 1),
          std::string("CHUNKPUT 1 0 STATE EXTRA ZRLE ") + std::to_string(8 + 2 + 16777216 + 17)}) {
        RawClient oversize("127.0.0.1", harness.port);
        oversize.Hello();
        oversize.SendLine(header);
        assert(oversize.ReadLine().rfind("-ERR BAD_REQUEST", 0) == 0);
        assert(oversize.WaitForClose(std::chrono::seconds(5)));
    }
    {
        RawClient bad_terminator("127.0.0.1", harness.port);
        bad_terminator.Hello();
        bad_terminator.SendLine("XPUT 5 1 8 1");
        bad_terminator.SendBytes("\x01XX\r\n");
        assert(bad_terminator.ReadLine().rfind("-ERR BAD_REQUEST", 0) == 0);
        assert(bad_terminator.WaitForClose(std::chrono::seconds(5)));
    }
    client.SendLine("XGET 5 1");
    assert(client.ReadBulkBytes() == (std::vector<std::uint8_t>{12, 0, 0, 0, 0xAB, 0x0F}));
}

// A request line cut off by the end of the stream is not executed: the
// client may have been interrupted in the middle of it ("TABLEDROP t" of
// "TABLEDROP t2").
void TestUnterminatedLineIsNotExecuted() {
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
    ServerHarness harness("unterminated", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient admin("127.0.0.1", harness.port);
    admin.Hello();
    admin.SendLine("TABLECREATE t block_bits 4");
    assert(admin.ReadLine() == "+OK\r\n");
    admin.SendLine("SET 0 0 0001");
    assert(admin.ReadLine() == "+OK\r\n");
    {
        RawClient cut("127.0.0.1", harness.port);
        cut.Hello();
        cut.SendBytes("TABLEDROP t");
        cut.ShutdownWrite();
        assert(cut.WaitForClose(std::chrono::seconds(5)));
    }
    {
        // Cut at an operation boundary: the batch so far is valid.
        RawClient cut("127.0.0.1", harness.port);
        cut.Hello();
        cut.SendBytes("CHUNKBATCH 0 0 SET 1 0 1111");
        cut.ShutdownWrite();
        assert(cut.WaitForClose(std::chrono::seconds(5)));
    }
    admin.SendLine("GET 1 0");
    assert(admin.ReadLine() == "$-1\r\n");
    assert(harness.catalog->Find("t") != nullptr);
    // The same batch, terminated, is applied.
    admin.SendLine("CHUNKBATCH 0 0 SET 1 0 1111");
    assert(admin.ReadLine().rfind("$", 0) == 0);
    (void)admin.ReadLine();
    admin.SendLine("GET 1 0");
    assert(admin.ReadLine() == "$4\r\n");
    assert(admin.ReadLine() == "1111\r\n");
}

// Before HELLO succeeds a connection has proved nothing: failed HELLOs count
// toward max_auth_failures, and HELLO must succeed within the I/O timeout.
void TestHandshakeIsBounded() {
    {
        // Three failed HELLOs end the connection at once, long before the
        // handshake deadline.
        auto engine_cfg = chunkdb::EngineConfig{.auth_token = "secret", .require_auth = true, .max_auth_failures = 3};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 4000;
        ServerHarness harness("handshake-failures", BaseStoreConfig(), engine_cfg, server_cfg);
        RawClient client("127.0.0.1", harness.port);
        for (int i = 0; i < 2; ++i) {
            client.SendLine("HELLO 2");
            assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
        }
        client.SendLine("HELLO 2 TABLE missing BOGUS");
        assert(client.ReadLine().rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(client.WaitForClose(std::chrono::milliseconds(1000)));
    }
    {
        // HELLOs paced below the I/O timeout, never enough to reach
        // max_auth_failures: the connection still ends at the deadline.
        auto engine_cfg = chunkdb::EngineConfig{.auth_token = "secret", .require_auth = true, .max_auth_failures = 1000};
        auto server_cfg = BaseServerConfig();
        server_cfg.client_io_timeout_ms = 800;
        ServerHarness harness("handshake-deadline", BaseStoreConfig(), engine_cfg, server_cfg);
        RawClient client("127.0.0.1", harness.port);
        const auto started = std::chrono::steady_clock::now();
        bool closed = false;
        std::string last;
        while (!closed && std::chrono::steady_clock::now() - started < std::chrono::seconds(4)) {
            try {
                client.SendLine("HELLO 2");
                last = client.ReadLine();
            } catch (const std::exception&) {
                closed = true;
                break;
            }
            if (last.rfind("-ERR PROTOCOL HELLO 2 was not completed", 0) == 0) {
                closed = client.WaitForClose(std::chrono::seconds(1));
                break;
            }
            assert(last.rfind("-ERR AUTH_REQUIRED", 0) == 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
        assert(closed);
        assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(2500));
        RawClient ok("127.0.0.1", harness.port);
        ok.Hello("secret");
        ok.SendLine("PING");
        assert(ok.ReadLine() == "+PONG\r\n");
    }
}

// The HELLO deadline also ends a line begun before it, and a client that
// sends nothing is told why it is closed.
void TestHelloDeadlineEndsAPartialLine() {
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.client_io_timeout_ms = 1000;
    server_cfg.idle_connection_timeout_ms = 10000;
    ServerHarness harness("hello-partial", BaseStoreConfig(), engine_cfg, server_cfg);
    const std::string refused = "-ERR PROTOCOL HELLO 2 was not completed within the I/O timeout\r\n";
    {
        // The line starts half way to the deadline; a line deadline of its
        // own would end it 500 ms after the HELLO deadline.
        RawClient slow("127.0.0.1", harness.port);
        const auto start = Clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        slow.SendBytes("HELLO");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        slow.SendBytes(" 2");
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

// MGET replies are bounded like area reads.
void TestMGetReplyIsBounded() {
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
    ServerHarness harness("mget-bound", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    client.SendLine("TABLECREATE wide block_bits 65535 chunk_width_blocks 1 chunk_height_blocks 1");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("USE wide");
    (void)client.ReadBulkText();
    std::string line = "MGET";
    for (int i = 0; i < 1100; ++i) {
        line += " 0 0";
    }
    client.SendLine(line);
    assert(client.ReadLine().rfind("-ERR OUT_OF_RANGE MGET reply would exceed", 0) == 0);
    client.SendLine("MGET 0 0 0 0");
    assert(client.ReadLine() == "*2\r\n");
}

// A refused payload gets one deadline for all of it, as a kept one does.
void TestDiscardedPayloadHasOneDeadline() {
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
    auto server_cfg = BaseServerConfig();
    server_cfg.client_io_timeout_ms = 500;
    ServerHarness harness("discard-deadline", BaseStoreConfig(), engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    // The default table has no extra data: the payload is read and dropped.
    const std::size_t length = 8U * 64U * 1024U;
    client.SendLine("XPUT 0 0 " + std::to_string(length * 8U) + " " + std::to_string(length));
    const auto started = std::chrono::steady_clock::now();
    const std::string piece(64U * 1024U, '\0');
    bool closed = false;
    for (int i = 0; i < 8 && !closed; ++i) {
        try {
            client.SendBytes(piece);
        } catch (const std::exception&) {
            closed = true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    assert(closed || client.WaitForClose(std::chrono::seconds(2)));
    assert(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(2500));
}

void TestTimeoutsAreBounded() {
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
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
        .auth_token = "secret",
        .require_auth = true,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunkput-hello", store_cfg, engine_cfg, server_cfg);
    const std::size_t payload_bytes = harness.geometry().ChunkPayloadBytes();

    // Before HELLO: the header is refused and the connection closed before
    // the payload is read, so unauthenticated clients cannot make the
    // server buffer.
    {
        RawClient client("127.0.0.1", harness.port);
        client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
        assert(client.ReadLine() == "-ERR PROTOCOL expected HELLO 2\r\n");
        assert(client.WaitForClose(std::chrono::seconds(5)));
    }

    RawClient client("127.0.0.1", harness.port);
    client.Hello("secret");
    client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
    client.SendBytes(std::string(payload_bytes, '\x0F') + "\r\n");
    (void)ReadVersion(client);
    client.SendLine("CHUNKEXISTS 0 0");
    assert(client.ReadLine() == "+1\r\n");
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
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.client_io_timeout_ms = 60000;
    ServerHarness harness("chunkput-largest", store_cfg, engine_cfg, server_cfg);
    const std::size_t payload_bytes = harness.geometry().ChunkPayloadBytes();
    assert(payload_bytes == 64U * 1024U * 1024U);

    RawClient client("127.0.0.1", harness.port);
    client.Hello();
    client.SendLine("CHUNKVER 0 0");
    const std::uint64_t version = ReadVersion(client);
    std::string payload(payload_bytes, '\0');
    for (std::size_t i = 0; i < payload.size(); i += 4093) {
        payload[i] = static_cast<char>(i % 251 + 1);
    }
    client.SendLine("CHUNKPUT 0 0 IF " + std::to_string(version) + " " + std::to_string(payload_bytes));
    client.SendBytes(payload + "\r\n");
    const std::uint64_t written = ReadVersion(client);
    assert(written > version);
    client.SendLine("CHUNKPUT 0 0 IF " + std::to_string(version) + " " + std::to_string(payload_bytes));
    client.SendBytes(payload + "\r\n");
    assert(client.ReadLine() == "-ERR VERSION_MISMATCH current=" + std::to_string(written) + "\r\n");
    client.SendLine("CHUNKGET 0 0");
    const auto read_back = client.ReadBulkBytes();
    assert(read_back.size() == payload_bytes);
    assert(std::equal(read_back.begin(), read_back.end(), payload.begin(), [](std::uint8_t a, char b) {
        return a == static_cast<std::uint8_t>(b);
    }));
}

void TestChunkGetLengthsAndForms() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("chunk-len", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    // 4x4 blocks of 4 bits: 8 payload bytes and 2 presence bytes.
    const auto geometry = harness.geometry();
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t state_bytes = payload_bytes + (geometry.ChunkBlockCount() + 7U) / 8U;
    assert(payload_bytes == 8U && state_bytes == 10U);

    client.SendLine("CHUNKEXISTS 0 0");
    assert(client.ReadLine() == "+0\r\n");
    // An absent chunk reads as zero bytes, with no block present.
    client.SendLine("CHUNKGET 0 0 STATE");
    assert(client.ReadBulkBytes() == std::vector<std::uint8_t>(state_bytes, 0U));

    const std::string zero_chunk(payload_bytes, '\0');
    client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
    client.SendBytes(zero_chunk + "\r\n");
    assert(client.ReadLine().rfind("$", 0) == 0);  // the new version
    (void)client.ReadLine();
    client.SendLine("CHUNKEXISTS 0 0");
    assert(client.ReadLine() == "+1\r\n");
    client.SendLine("CHUNKGET 0 0");
    assert(client.ReadBulkBytes().size() == payload_bytes);
    client.SendLine("CHUNKGET 0 0 STATE");
    const auto state = client.ReadBulkBytes();
    assert(state.size() == state_bytes);
    assert(state[8] == 0xFFU && state[9] == 0xFFU);

    // ZRLE gives the same bytes, compressed; options in any order.
    client.SendLine("CHUNKGET 0 0 ZRLE STATE");
    const auto compressed = client.ReadBulkBytes();
    assert(chunkdb::ZrleDecompress(compressed, state_bytes) == state);

    // A sparse state: blocks 0 and 15 present.
    const std::string sparse = std::string("\x0f", 1) + std::string(7, '\0') +
                               std::string("\x01\x80", 2);
    client.SendLine("CHUNKPUT 1 0 STATE 10");
    client.SendBytes(sparse + "\r\n");
    assert(client.ReadLine().rfind("$", 0) == 0);
    (void)client.ReadLine();
    client.SendLine("CHUNKGET 1 0 STATE");
    const auto sparse_back = client.ReadBulkBytes();
    assert(std::string(sparse_back.begin(), sparse_back.end()) == sparse);
    client.SendLine("GET 4 0");
    assert(client.ReadBulkText() == "1111");
    client.SendLine("GET 5 0");
    assert(client.ReadLine() == "$-1\r\n");
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

    std::string payload = "HELLO 2\r\n";
    payload.reserve(220 * 6 + 64);
    for (int i = 0; i < 220; ++i) {
        payload += "PING\r\n";
    }
    payload += "SET 0 0 1010\r\n";
    payload += "GET 0 0\r\n";
    payload += "PING\r\n";
    client.SendBytes(payload);

    assert(client.ReadBulkText().find("protocol=2\n") != std::string::npos);
    for (int i = 0; i < 220; ++i) {
        assert(client.ReadLine() == "+PONG\r\n");
    }
    assert(client.ReadLine() == "+OK\r\n");
    assert(client.ReadBulkText() == "1010");
    assert(client.ReadLine() == "+PONG\r\n");
}

void TestExtremeChunkRangeKeepsConnectionUsable() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    ServerHarness harness("extreme-chunk-range", store_cfg, engine_cfg, server_cfg);
    RawClient client("127.0.0.1", harness.port);
    client.Hello();

    client.SendLine(
        "CHUNKRANGE -9223372036854775808 0 9223372036854775807 0");
    assert(client.ReadLine().rfind("-ERR ", 0) == 0);
    client.SendLine("PING");
    assert(client.ReadLine() == "+PONG\r\n");
    client.SendLine(
        "CHUNKRANGE 9223372036854775807 9223372036854775807 "
        "9223372036854775807 9223372036854775807");
    assert(client.ReadLine() == "*0\r\n");
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
    client.Hello();

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

    client.SendLine("HELLO 2 AUTH no1");
    assert(client.ReadLine().rfind("-ERR AUTH_FAILED", 0) == 0);

    client.SendLine("HELLO 2 AUTH no2");
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
    client.Hello();

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
    fast.SendLine("HELLO 2");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response.rfind("$", 0) == 0);
    assert(stalled.WaitForClose(std::chrono::milliseconds(1500)));
}

#ifdef CHUNKDB_WITH_OPENSSL
void TestTlsHandshakeDeadlineReleasesWorker() {
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
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
        .auth_token = "",
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
    auto engine_cfg = chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 5};
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

void TestChunkPutOverTls() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
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

    // Payload split across two TLS records, then STATE form with a pipelined
    // command; both must be reassembled by the server.
    const std::string payload(payload_bytes, '\xF0');
    client.SendLine("CHUNKPUT 0 0 " + std::to_string(payload_bytes));
    client.SendBytes(payload.substr(0, 3));
    client.SendBytes(payload.substr(3) + "\r\n");
    const std::uint64_t version = std::stoull(client.ReadBulkText());
    assert(version > 0);

    client.SendLine("CHUNKGET 0 0");
    assert(client.ReadBulkText() == payload);

    const std::string state = payload + std::string(presence_bytes, '\x00');
    client.SendBytes(
        "CHUNKPUT 1 0 STATE " + std::to_string(state.size()) + "\r\n" + state + "\r\nCHUNKEXISTS 1 0\r\n");
    (void)std::stoull(client.ReadBulkText());
    assert(client.ReadLine() == "+0\r\n");
    // The reject-and-close paths are covered by the plain-socket test; over
    // TLS the close can race ahead of the client reading the error reply.
}
#endif

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
        .auth_token = "",
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
        .auth_token = "",
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
        .auth_token = "",
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
    stalled.Hello();
    stalled.SendBytes("P");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));
    stalled.SendBytes("I");
    std::this_thread::sleep_for(std::chrono::milliseconds(90));

    RawClient fast("127.0.0.1", harness.port);
    fast.SendLine("HELLO 2");

    stalled.SendBytes("N");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response.rfind("$", 0) == 0);
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
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();

    ServerHarness harness("recv-timeout-state-cache", store_cfg, engine_cfg, server_cfg);
    chunkdb::ResetServerTimeoutConfigCountersForTests();

    RawClient client("127.0.0.1", harness.port);
    constexpr std::size_t kRequests = 32;
    std::string batch = "HELLO 2\r\n";
    for (std::size_t i = 0; i < kRequests; ++i) {
        batch += "PING\r\n";
    }
    client.SendBytes(batch);
    assert(client.ReadBulkText().find("protocol=2\n") != std::string::npos);
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
    fast.SendLine("HELLO 2");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response.rfind("$", 0) == 0);
    assert(idle.WaitForClose(std::chrono::milliseconds(1500)));
}

void TestPendingQueueWaitTimeoutClosesQueuedSocket() {
    auto store_cfg = BaseStoreConfig();
    auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
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
            recovery.SendLine("HELLO 2");

            std::string response;
            recovered =
                recovery.ReadLineWithin(std::chrono::milliseconds(500), &response) &&
                response.rfind("$", 0) == 0;
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
    // HELLO and a 1 MiB CHUNKGET reply in one write, read slowly.
    slow.SendBytes("HELLO 2\r\nCHUNKGET 0 0\r\n");

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
    fast.SendLine("HELLO 2");
    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(2000), &response));
    assert(response.rfind("$", 0) == 0);
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
        .auth_token = "",
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
    stalled.SendBytes("HELLO 2");
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
            stalled_reply.rfind("$", 0) == 0) {
            (void)try_send_line(stalled, "QUIT");
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
            client->SendLine("HELLO 2");
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
            assert(line.rfind("$", 0) == 0);
            (void)try_send_line(*client, "QUIT");
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
        .auth_token = "",
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
    } catch (...) {
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

// Tables over the wire: each connection works on the table it selected, the
// table's geometry governs its commands (including CHUNKPUT frame bounds),
// and a drop reaches every connection that selected the table.
void TestTablesOverProtocol() {
    const auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "",
        .require_auth = false,
        .max_auth_failures = 5,
    };
    auto server_cfg = BaseServerConfig();
    server_cfg.worker_threads = 4;  // two long-lived connections plus short ones
    ServerHarness harness("tables", BaseStoreConfig(), engine_cfg, server_cfg);
    RawClient a("127.0.0.1", harness.port);
    RawClient b("127.0.0.1", harness.port);
    const auto read_tables = [](RawClient& client) {
        client.SendLine("TABLES");
        const auto header = client.ReadLine();
        assert(header.rfind("*", 0) == 0);
        std::vector<std::string> names;
        for (int i = 0; i < std::stoi(header.substr(1)); ++i) {
            names.push_back(client.ReadBulkText());
        }
        return names;
    };

    // Without TABLE, HELLO selects `default` and replies with its geometry.
    a.SendLine("HELLO 2");
    auto info = ParseInfoMap(a.ReadBulkText());
    assert(info["protocol"] == "2");
    assert(info["table"] == "default");
    assert(info["block_bits"] == "4");
    b.Hello();
    a.SendLine("INFO");
    info = ParseInfoMap(a.ReadBulkText());
    assert(info["table"] == "default");
    assert(info["tables"] == "1");
    assert(info.count("block_bits") == 0U);

    a.SendLine(
        "TABLECREATE terrain block_bits 9 chunk_width_blocks 8 chunk_height_blocks 2 "
        "durability_mode fsync-wal");
    assert(a.ReadLine() == "+OK\r\n");
    a.SendLine("TABLECREATE terrain block_bits 9");
    assert(a.ReadLine().rfind("-ERR TABLE_EXISTS", 0) == 0);
    a.SendLine("TABLECREATE Terrain block_bits 9");
    assert(a.ReadLine().rfind("-ERR INVALID_ARGUMENT invalid table name", 0) == 0);
    assert(read_tables(b) == (std::vector<std::string>{"default", "terrain"}));

    // An unknown name keeps the current table.
    a.SendLine("USE nope");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    a.SendLine("INFO");
    assert(ParseInfoMap(a.ReadBulkText())["table"] == "default");

    // USE replies with the geometry and options.
    a.SendLine("USE terrain");
    info = ParseInfoMap(a.ReadBulkText());
    assert(info["table"] == "terrain");
    assert(info["block_bits"] == "9");
    assert(info["chunk_width_blocks"] == "8");
    assert(info["chunk_height_blocks"] == "2");
    assert(info["durability_mode"] == "fsync-wal");
    assert(info["store_id"].size() == 32U);

    // Same coordinates, independent tables.
    a.SendLine("SET 1 1 101010101");
    assert(a.ReadLine() == "+OK\r\n");
    b.SendLine("SET 1 1 1010");
    assert(b.ReadLine() == "+OK\r\n");
    a.SendLine("GET 1 1");
    assert(a.ReadBulkText() == "101010101");
    b.SendLine("GET 1 1");
    assert(b.ReadBulkText() == "1010");

    // CHUNKPUT is framed by the selected table's geometry: a terrain chunk
    // is 8x2 blocks of 9 bits, 18 bytes.
    std::string payload;
    for (int i = 0; i < 18; ++i) {
        payload.push_back(static_cast<char>(0x30 + i));
    }
    a.SendLine("CHUNKPUT 5 5 18");
    a.SendBytes(payload + "\r\n");
    assert(ReadVersion(a) > 0U);
    a.SendLine("CHUNKGET 5 5");
    const auto chunk = a.ReadBulkBytes();
    assert(std::string(chunk.begin(), chunk.end()) == payload);
    {
        // HELLO TABLE selects a table up front.
        RawClient on_terrain("127.0.0.1", harness.port);
        on_terrain.SendLine("HELLO 2 TABLE terrain");
        assert(ParseInfoMap(on_terrain.ReadBulkText())["block_bits"] == "9");
        on_terrain.SendLine("CHUNKGET 5 5");
        assert(on_terrain.ReadBulkBytes() == chunk);
        // An unknown table leaves the connection open and not greeted.
        RawClient unknown("127.0.0.1", harness.port);
        unknown.SendLine("HELLO 2 TABLE nope");
        assert(unknown.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        unknown.Hello();
    }
    {
        // 18 bytes exceed a default chunk (8 + 2 bytes): refused unread.
        RawClient on_default("127.0.0.1", harness.port);
        on_default.Hello();
        on_default.SendLine("CHUNKPUT 5 5 18");
        assert(on_default.ReadLine().rfind("-ERR BAD_REQUEST", 0) == 0);
        assert(on_default.WaitForClose(std::chrono::seconds(5)));
    }

    // Options change while another connection uses the table.
    b.SendLine("TABLESET terrain checkpoint_updates 3 checkpoint_compression zrle");
    assert(b.ReadLine() == "+OK\r\n");
    b.SendLine("TABLESET terrain block_bits 5");
    assert(b.ReadLine().rfind("-ERR INVALID_ARGUMENT block_bits is part of the geometry", 0) == 0);
    b.SendLine("TABLEINFO terrain");
    info = ParseInfoMap(b.ReadBulkText());
    assert(info["checkpoint_updates"] == "3");
    assert(info["checkpoint_compression"] == "zrle");
    a.SendLine("GET 1 1");
    assert(a.ReadBulkText() == "101010101");

    // A drop reaches the connection that selected the table, even after a
    // table of the same name exists again.
    b.SendLine("TABLEDROP terrain");
    assert(b.ReadLine() == "+OK\r\n");
    a.SendLine("GET 1 1");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    b.SendLine("TABLECREATE terrain block_bits 3");
    assert(b.ReadLine() == "+OK\r\n");
    a.SendLine("INFO");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    // A CHUNKPUT to the dropped table is still framed by its geometry, then
    // refused; the connection stays usable.
    a.SendLine("CHUNKPUT 5 5 18");
    a.SendBytes(payload + "\r\n");
    assert(a.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
    a.SendLine("USE terrain");
    assert(ParseInfoMap(a.ReadBulkText())["block_bits"] == "3");
    a.SendLine("GET 1 1");
    assert(a.ReadLine() == "$-1\r\n");

    // WALFLUSH covers every table.
    a.SendLine("WALFLUSH");
    assert(a.ReadLine() == "+OK\r\n");

    b.SendLine("TABLEDROP default");
    assert(b.ReadLine() == "+OK\r\n");
    {
        // A new connection has no table to start on now: HELLO succeeds
        // without table info, and table commands need USE first.
        RawClient fresh("127.0.0.1", harness.port);
        fresh.SendLine("HELLO 2");
        const auto fresh_info = ParseInfoMap(fresh.ReadBulkText());
        assert(fresh_info.at("protocol") == "2");
        assert(fresh_info.count("table") == 0U);
        fresh.SendLine("GET 0 0");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        // A `default` created after HELLO is not bound behind the client's
        // back: the connection keeps no table until USE.
        b.SendLine("TABLECREATE default block_bits 2");
        assert(b.ReadLine() == "+OK\r\n");
        fresh.SendLine("GET 0 0");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        fresh.SendLine("CHUNKPUT 0 0 8");
        assert(fresh.ReadLine().rfind("-ERR NO_TABLE", 0) == 0);
        assert(fresh.WaitForClose(std::chrono::seconds(5)));
        b.SendLine("TABLEDROP default");
        assert(b.ReadLine() == "+OK\r\n");
    }
    assert(read_tables(b) == (std::vector<std::string>{"terrain"}));
}

// Table commands need an authenticated HELLO like every other command; a
// table command before HELLO is a protocol error that closes the connection.
void TestTableCommandsRequireAuth() {
    const auto engine_cfg = chunkdb::EngineConfig{
        .auth_token = "secret",
        .require_auth = true,
        .max_auth_failures = 5,
    };
    ServerHarness harness("tables-auth", BaseStoreConfig(), engine_cfg, BaseServerConfig());
    for (const char* command :
         {"TABLES", "TABLEINFO default", "USE default", "TABLECREATE x block_bits 4",
          "TABLESET default checkpoint_updates 3", "TABLEDROP default"}) {
        RawClient early("127.0.0.1", harness.port);
        early.SendLine(command);
        assert(early.ReadLine().rfind("-ERR PROTOCOL expected HELLO 2", 0) == 0);
        assert(early.WaitForClose(std::chrono::seconds(5)));
    }
    RawClient client("127.0.0.1", harness.port);
    client.SendLine("HELLO 2 TABLE default");
    assert(client.ReadLine().rfind("-ERR AUTH_REQUIRED", 0) == 0);
    client.SendLine("TABLES");
    assert(client.ReadLine().rfind("-ERR PROTOCOL expected HELLO 2", 0) == 0);
    assert(client.WaitForClose(std::chrono::seconds(5)));
    RawClient authed("127.0.0.1", harness.port);
    authed.Hello("secret");
    authed.SendLine("TABLES");
    assert(authed.ReadLine() == "*1\r\n");
}

int main() {
#ifdef _WIN32
    (void)EnsureWinsockRuntime();
#else
    (void)signal(SIGPIPE, SIG_IGN);
#endif
    TestPing();
    TestProtocolOneClientIsRefused();
    TestAuthAndSetGet();
    TestChunkGetLengthsAndForms();
    TestChunkPutWritesAndFraming();
    TestExtraDataOverTcp();
    TestUnterminatedLineIsNotExecuted();
    TestHandshakeIsBounded();
    TestHelloDeadlineEndsAPartialLine();
    TestMGetReplyIsBounded();
    TestDiscardedPayloadHasOneDeadline();
    TestTimeoutsAreBounded();
    TestChunkPutRequiresHelloBeforePayload();
    TestChunkPutIfLargestGeometry();
    TestPipelinedCommandsSinglePacket();
    TestExtremeChunkRangeKeepsConnectionUsable();
    TestQuitClosesConnection();
    TestMaxLineOverflowDisconnects();
    TestPipelinedBadRequestDisconnectPolicy();
    TestMaxAuthFailuresDisconnects();
    TestInfoRuntimeCounters();
    TestSlowClientTimeoutReleasesWorker();
#ifdef CHUNKDB_WITH_OPENSSL
    TestTlsHandshakeDeadlineReleasesWorker();
    TestTlsTrickledRecordIsBounded();
    TestTlsKeyUpdateIsNotARequest();
    TestChunkPutOverTls();
#endif
    TestReadTimeoutLogsPhaseAndReason();
    TestSendAfterTimedOutCloseReturnsErrorInsteadOfSigpipe();
    TestSendTimeoutSetupFailureClosesConnection();
    TestReceiveTimeoutSetupFailureClosesConnection();
    TestSlowRequestDribbleDeadlineReleasesWorker();
    TestIdleClientRemainsConnectedBetweenCommands();
    TestReceiveTimeoutIsNotReconfiguredForIdleKeepAliveRequests();
    TestLongIdleConnectionTimeoutReleasesWorker();
    TestPendingQueueWaitTimeoutClosesQueuedSocket();
    TestSlowResponseDrainDeadlineReleasesWorker();
    TestIdlePeerCloseDoesNotLogTerminationWarning();
    TestPendingQueueSaturationRejectsNewConnections();
    TestReadinessLogLineExists();
    TestWarnLineOnBadRequest();
    TestErrorLineOnListenFailure();
    TestLogLevelFilteringWarn();
    TestLogLevelFilteringError();
    TestStartupLogOrder();
    TestTablesOverProtocol();
    TestTableCommandsRequireAuth();
    return 0;
}
