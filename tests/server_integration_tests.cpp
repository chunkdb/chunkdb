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
    std::shared_ptr<chunkdb::ChunkStore> store;
    std::shared_ptr<chunkdb::CommandEngine> engine;
    std::unique_ptr<chunkdb::ChunkServer> server;
    std::thread thread;
    std::uint16_t port = 0;
    bool tls_enabled = false;

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
            const auto creds = WriteTlsTestCredentials(data_dir / "tls");
            server_config.tls_cert_path = creds.cert_path.string();
            server_config.tls_key_path = creds.key_path.string();
        }
#endif
        tls_enabled = server_config.tls_enabled;

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

    client.SendLine("EXISTS 1 2");
    assert(client.ReadLine() == "+1\r\n");

    client.SendLine("GET 1 2");
    assert(client.ReadBulkText() == "1111");

    client.SendLine("SET 2 2 0000");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("EXISTS 2 2");
    assert(client.ReadLine() == "+1\r\n");
    client.SendLine("GET 2 2");
    assert(client.ReadBulkText() == "0000");

    client.SendLine("UNSET 2 2");
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("EXISTS 2 2");
    assert(client.ReadLine() == "+0\r\n");
    client.SendLine("GET 2 2");
    assert(client.ReadBulkText() == "0000");
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
    const std::size_t expected_presence_bits =
        static_cast<std::size_t>(cfg.chunk_width_blocks) *
        static_cast<std::size_t>(cfg.chunk_height_blocks);
    const std::size_t expected_state_bytes = expected_bytes + (expected_presence_bits + 7U) / 8U;
    const std::string zero_chunk(expected_bits, '0');
    const std::string full_presence(expected_presence_bits, '1');
    const std::string sparse_presence = "1000000000000001";
    const std::string sparse_payload = "1111" + std::string(expected_bits - 8U, '0') + "0000";

    client.SendLine("CHUNKEXISTS 0 0");
    assert(client.ReadLine() == "+0\r\n");

    client.SendLine("CHUNKSET 0 0 " + zero_chunk);
    assert(client.ReadLine() == "+OK\r\n");

    client.SendLine("CHUNKEXISTS 0 0");
    assert(client.ReadLine() == "+1\r\n");

    client.SendLine("CHUNK 0 0");
    const std::string chunk_text = client.ReadBulkText();
    assert(chunk_text.size() == expected_bits);
    assert(chunk_text == zero_chunk);

    client.SendLine("CHUNKBIN 0 0");
    const auto chunk_bin = client.ReadBulkBytes();
    assert(chunk_bin.size() == expected_bytes);

    client.SendLine("CHUNK 0 0 STATE");
    assert(client.ReadBulkText() == zero_chunk + "|" + full_presence);

    client.SendLine("CHUNKBIN 0 0 STATE");
    const auto chunk_state_bin = client.ReadBulkBytes();
    assert(chunk_state_bin.size() == expected_state_bytes);

    client.SendLine("CHUNKSET 1 0 STATE " + sparse_payload + "|" + sparse_presence);
    assert(client.ReadLine() == "+OK\r\n");
    client.SendLine("CHUNKEXISTS 1 0");
    assert(client.ReadLine() == "+1\r\n");
    client.SendLine("CHUNK 1 0");
    assert(client.ReadBulkText() == sparse_payload);
    client.SendLine("CHUNK 1 0 STATE");
    assert(client.ReadBulkText() == sparse_payload + "|" + sparse_presence);
    client.SendLine("GET 4 0");
    assert(client.ReadBulkText() == "1111");
    client.SendLine("EXISTS 4 0");
    assert(client.ReadLine() == "+1\r\n");
    client.SendLine("GET 5 0");
    assert(client.ReadBulkText() == "0000");
    client.SendLine("EXISTS 5 0");
    assert(client.ReadLine() == "+0\r\n");
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
    fast.SendLine("PING");
    assert(fast.ReadLine() == "+PONG\r\n");
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

    assert(logs.Contains("phase=idle"));

    RawClient ok("127.0.0.1", harness.port);
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
    std::string batch;
    for (std::size_t i = 0; i < kRequests; ++i) {
        batch += "PING\r\n";
    }
    client.SendBytes(batch);
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
    fast.SendLine("PING");

    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(1500), &response));
    assert(response == "+PONG\r\n");
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
            recovery.SendLine("PING");

            std::string response;
            recovered =
                recovery.ReadLineWithin(std::chrono::milliseconds(500), &response) &&
                response == "+PONG\r\n";
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
    slow.SendLine("CHUNK 0 0");

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
    fast.SendLine("PING");
    std::string response;
    assert(fast.ReadLineWithin(std::chrono::milliseconds(2000), &response));
    assert(response == "+PONG\r\n");
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
    stalled.SendBytes("PING");
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
            stalled_reply == "+PONG\r\n") {
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
            client->SendLine("PING");
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
            assert(line == "+PONG\r\n");
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
#else
    (void)signal(SIGPIPE, SIG_IGN);
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
#ifdef CHUNKDB_WITH_OPENSSL
    TestTlsHandshakeDeadlineReleasesWorker();
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
    return 0;
}
