#pragma once

#include <array>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <exception>
#include <utility>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif
#include "chunkdb/server.hpp"
#include "feed_test_utils.hpp"
#include "login_helpers.hpp"

namespace chunkdb::slot_socket_test {
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
#ifdef _WIN32
using Socket = SOCKET;
inline constexpr Socket kInvalid = INVALID_SOCKET;
inline void CloseSocket(Socket socket) { closesocket(socket); }
inline int ClientSocketError() { return WSAGetLastError(); }
inline bool WouldBlock() { return WSAGetLastError() == WSAETIMEDOUT || WSAGetLastError() == WSAEWOULDBLOCK; }
inline void InitializeSockets() {
    static const int initialized = [] { WSADATA data{}; return WSAStartup(MAKEWORD(2, 2), &data); }();
    assert(initialized == 0);
}
#else
using Socket = int;
inline constexpr Socket kInvalid = -1;
inline void CloseSocket(Socket socket) { close(socket); }
inline int ClientSocketError() { return errno; }
inline bool WouldBlock() { return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR; }
inline void InitializeSockets() {}
#endif
inline sockaddr_in Address(std::uint16_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    return address;
}
inline std::uint16_t FreePort() {
    InitializeSockets();
    const auto socket = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(socket != kInvalid);
    auto address = Address(0);
    assert(bind(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0);
#ifdef _WIN32
    int length = sizeof(address);
#else
    socklen_t length = sizeof(address);
#endif
    assert(getsockname(socket, reinterpret_cast<sockaddr*>(&address), &length) == 0);
    CloseSocket(socket);
    return ntohs(address.sin_port);
}
struct Reply {
    char type = 0;
    std::string value;
    std::vector<Reply> items;
    bool operator==(const Reply&) const = default;
};
inline const Reply& Field(const Reply& map, std::string_view name) {
    assert(map.type == '%');
    for (std::size_t i = 0; i < map.items.size(); i += 2U)
        if (map.items[i].value == name) return map.items[i + 1U];
    throw std::runtime_error("missing map field: " + std::string(name));
}
inline std::uint64_t Number(const Reply& reply) {
    assert(reply.type == ':');
    return std::stoull(reply.value);
}
inline bool Boolean(const Reply& reply) {
    assert(reply.type == '#' && (reply.value == "t" || reply.value == "f"));
    return reply.value == "t";
}

// A real blocking loopback connection. Short socket timeouts let the parser
// impose one deadline over the entire reply, including TLS WANT_READ retries.
class Client {
  public:
    explicit Client(std::uint16_t port, bool tls = false) {
        InitializeSockets();
        peer_ = "127.0.0.1:" + std::to_string(port);
        socket_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socket_ == kInvalid) Fail("socket failed");
        auto address = Address(port);
        if (connect(socket_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
            socket_error_ = ClientSocketError();
            CloseSocket(socket_); socket_ = kInvalid;
            Fail("connect failed");
        }
        sockaddr_in local{};
#ifdef _WIN32
        int local_length = sizeof(local);
#else
        socklen_t local_length = sizeof(local);
#endif
        if (getsockname(socket_, reinterpret_cast<sockaddr*>(&local), &local_length) == 0)
            endpoint_ = "127.0.0.1:" + std::to_string(ntohs(local.sin_port));
        try {
#ifdef _WIN32
            const DWORD timeout = 50;
            setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
#else
            const timeval timeout{0, 50'000};
            setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#ifdef SO_NOSIGPIPE
            const int no_sigpipe = 1;
            setsockopt(socket_, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#endif
#endif
#ifdef CHUNKDB_WITH_OPENSSL
            if (tls) {
                context_ = SSL_CTX_new(TLS_client_method()); assert(context_);
                SSL_CTX_set_verify(context_, SSL_VERIFY_NONE, nullptr);
                ssl_ = SSL_new(context_); assert(ssl_);
                assert(SSL_set_fd(ssl_, static_cast<int>(socket_)) == 1);
                const auto deadline = Clock::now() + 10s;
                while (true) {
                    ERR_clear_error(); errno = 0;
                    const int result = SSL_connect(ssl_);
                    if (result == 1) break;
                    if (!RetryTls(result) || Clock::now() >= deadline) Fail("TLS handshake failed");
                }
            }
#else
            if (tls) Fail("TLS test requested without TLS");
#endif
        } catch (const std::exception&) { Close(); throw; }
    }
    ~Client() { Close(); }
    void Close() noexcept {
#ifdef CHUNKDB_WITH_OPENSSL
        if (ssl_) { SSL_free(ssl_); ssl_ = nullptr; }
        if (context_) { SSL_CTX_free(context_); context_ = nullptr; }
#endif
        if (socket_ != kInvalid) { CloseSocket(socket_); socket_ = kInvalid; }
    }
#ifdef CHUNKDB_WITH_OPENSSL
    // Send the TLS closure alert while leaving the TCP connection open.
    void CloseTlsWrite() {
        assert(ssl_);
        const auto deadline = Clock::now() + 10s;
        for (;;) {
            ERR_clear_error(); errno = 0;
            const int result = SSL_shutdown(ssl_);
            if (result >= 0) return;
            if (!RetryTls(result) || Clock::now() >= deadline)
                throw std::runtime_error("TLS close notification failed");
        }
    }
#endif
    void CloseWrite() {
#ifdef _WIN32
        assert(shutdown(socket_, SD_SEND) == 0);
#else
        assert(shutdown(socket_, SHUT_WR) == 0);
#endif
    }
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    void Send(std::string_view bytes) {
        const auto deadline = Clock::now() + 10s;
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            int count = 0;
#ifdef CHUNKDB_WITH_OPENSSL
            if (ssl_) {
                ERR_clear_error(); errno = 0;
                count = SSL_write(ssl_, bytes.data() + offset, static_cast<int>(bytes.size() - offset));
                if (count <= 0 && RetryTls(count) && Clock::now() < deadline) continue;
            } else
#endif
            {
#ifdef MSG_NOSIGNAL
                constexpr int flags = MSG_NOSIGNAL;
#else
                constexpr int flags = 0;
#endif
                count = static_cast<int>(send(socket_, bytes.data() + offset,
                    static_cast<int>(bytes.size() - offset), flags));
                if (count < 0 && WouldBlock() && Clock::now() < deadline) continue;
            }
            if (count <= 0) Fail("socket write failed");
            offset += static_cast<std::size_t>(count);
        }
    }
    void Line(std::string_view line) { last_request_ = std::string(line.substr(0U, 80U)); Send(std::string(line) + "\r\n"); }
    Reply Read(std::chrono::milliseconds timeout = 10s) { return ReadAt(Clock::now() + timeout); }
    Reply Command(std::string_view line) { Line(line); return Read(); }
    void Ok(std::string_view line) {
        const auto reply = Command(line);
        if (reply.type != '+' || reply.value != "OK") throw std::runtime_error("expected OK for " + std::string(line) + ": " + reply.value);
    }
    bool Ready(std::chrono::milliseconds timeout) {
        if (!pending_.empty()) return true;
        return Fetch(Clock::now() + timeout);
    }
    void Hello() { const auto reply = Command("HELLO 3"); assert(reply.type == '%' && reply.items.size() == 16U); }
    void Login(std::string_view user = "admin", std::string_view password = "secret") {
        const auto login = scram::StartClientLogin(user, scram::NewNonce());
        last_request_ = "HELLO USER " + std::string(user);
        Send(test::HelloUserBytes(login, user));
        const auto first = Read(); assert(first.type == '+');
        const auto auth = test::AuthBytes(login, password, "+" + first.value);
        last_request_ = "AUTH";
        Send(auth.bytes);
        const auto hello = Read();
        assert(Field(hello, "server_signature").value == auth.server_signature);
    }
  private:
#ifdef CHUNKDB_WITH_OPENSSL
    bool RetryTls(int result) {
        socket_error_ = ClientSocketError();
        const int error = SSL_get_error(ssl_, result);
        tls_error_ = error;
        return error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE ||
            (error == SSL_ERROR_SYSCALL && WouldBlock());
    }
    SSL_CTX* context_ = nullptr;
    SSL* ssl_ = nullptr;
#endif
    [[noreturn]] void Fail(std::string_view message) const {
        std::string detail(message);
        detail += " [client=" + endpoint_ + " peer=" + peer_ + " request=" + last_request_;
#ifdef CHUNKDB_WITH_OPENSSL
        if (ssl_) detail += " tls_error=" + std::to_string(tls_error_);
#endif
        detail += " errno=" + std::to_string(socket_error_) + "]";
        throw std::runtime_error(detail);
    }
    std::string endpoint_ = "unbound", peer_ = "unknown", last_request_ = "connect";
    int socket_error_ = 0;
#ifdef CHUNKDB_WITH_OPENSSL
    int tls_error_ = 0;
#endif
    Socket socket_ = kInvalid;
    std::string pending_;
    bool Fetch(Clock::time_point deadline) {
        do {
            char bytes[4096];
            int count = 0;
            bool retry = false;
#ifdef CHUNKDB_WITH_OPENSSL
            if (ssl_) {
                ERR_clear_error(); errno = 0;
                count = SSL_read(ssl_, bytes, sizeof(bytes));
                retry = count <= 0 && RetryTls(count);
            } else
#endif
            {
                count = static_cast<int>(recv(socket_, bytes, sizeof(bytes), 0));
                socket_error_ = count < 0 ? ClientSocketError() : 0;
                retry = count < 0 && WouldBlock();
            }
            if (count > 0) { pending_.append(bytes, static_cast<std::size_t>(count)); return true; }
            if (!retry) Fail("socket closed while reading");
        } while (Clock::now() < deadline);
        return false;
    }
    std::string Take(std::size_t size, Clock::time_point deadline) {
        while (pending_.size() < size)
            if (!Fetch(deadline)) Fail("reply timed out");
        auto result = pending_.substr(0, size); pending_.erase(0, size); return result;
    }
    std::string ReadLine(Clock::time_point deadline) {
        for (;;) {
            const auto end = pending_.find("\r\n");
            if (end != std::string::npos) return Take(end + 2U, deadline).substr(0, end);
            if (!Fetch(deadline)) Fail("reply line timed out");
        }
    }
    Reply ReadAt(Clock::time_point deadline) {
        const auto line = ReadLine(deadline);
        if (line.empty()) throw std::runtime_error("empty reply line");
        Reply reply{line[0], line.substr(1), {}};
        if (reply.type == '$') {
            const auto size = static_cast<std::size_t>(std::stoull(reply.value));
            reply.value = Take(size, deadline);
            assert(Take(2, deadline) == "\r\n");
        } else if (reply.type == '*' || reply.type == '%' || reply.type == '>') {
            auto size = static_cast<std::size_t>(std::stoull(reply.value));
            if (reply.type == '%') size *= 2U;
            for (std::size_t i = 0; i < size; ++i) reply.items.push_back(ReadAt(deadline));
        }
        return reply;
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


#endif

inline thread_local std::exception_ptr background_server_error;
inline void RethrowBackgroundServerError() {
    if (auto error = std::exchange(background_server_error, {})) std::rethrow_exception(error);
}
class Harness {
  public:
    test::ScopedTempDir directory{"chunkdb-slots-socket"};
    std::shared_ptr<TableCatalog> catalog;
    std::uint16_t port = FreePort();
    bool tls;
    bool auth;
    Harness(bool use_tls, bool use_auth = false, std::size_t max_bytes = kDefaultSlotMaxBytes,
            std::chrono::milliseconds sync = 100ms, std::size_t feed_bytes = kDefaultFeedBufferBytes, std::filesystem::path backup_dir = {})
        : tls(use_tls), auth(use_auth) {
        auto config = feed_test::Config(std::filesystem::canonical(directory.path()));
        config.slot_max_bytes = max_bytes;
        config.feed_buffer_bytes = feed_bytes;
        config.slot_sync_interval = sync;
        config.default_options.wal_group_commit_updates = 1000;
        catalog = std::make_shared<TableCatalog>(config);
        (void)feed_test::CreateDefault(*catalog);
        EngineConfig engine_config;
        engine_config.require_auth = auth;
        engine_config.backup_dir = std::move(backup_dir);
        if (auth) engine_config.users = test::MakeUsers(directory.path(), "admin", "secret");
        engine_ = std::make_shared<CommandEngine>(engine_config, catalog);
        ServerConfig server_config;
        server_config.port = port;
        server_config.worker_threads = 4;
        server_config.feed_buffer_bytes = feed_bytes;
        server_config.tls_enabled = tls;
#ifdef CHUNKDB_WITH_OPENSSL
        if (tls) {
            const auto cert = directory.path() / "test-cert.pem";
            const auto key = directory.path() / "test-key.pem";
            std::ofstream(cert) << kTestTlsCertPem;
            std::ofstream(key) << kTestTlsKeyPem;
            server_config.tls_cert_path = cert.string(); server_config.tls_key_path = key.string();
        }
#endif
        server_ = std::make_unique<ChunkServer>(server_config, engine_);
        Start();
    }
    void Restart() { server_->Stop(); thread_.join(); Start(); }
    ~Harness() {
        server_->Stop(); thread_.join();
        if (error_) background_server_error = error_;
    }
    ChunkServer& server() { return *server_; }
    CommandEngine& engine() { return *engine_; }
    std::unique_ptr<Client> Connect() {
        auto client = std::make_unique<Client>(port, tls);
        if (auth) client->Login(); else client->Hello();
        return client;
    }
  private:
    void Start() {
        thread_ = std::thread([this] {
            try { server_->Run(); }
            catch (const std::exception&) { std::lock_guard lock(error_mutex_); error_ = std::current_exception(); }
        });
        try {
            const auto deadline = Clock::now() + 10s;
            while (true) {
                { std::lock_guard lock(error_mutex_); if (error_) std::rethrow_exception(error_); }
                try { Client probe(port, tls); break; }
                catch (const std::runtime_error&) {
                    if (Clock::now() >= deadline) throw;
                    std::this_thread::sleep_for(10ms); // Only listener startup, no test ordering relies on this.
                }
            }
        } catch (const std::exception&) { server_->Stop(); thread_.join(); throw; }
    }
    std::shared_ptr<CommandEngine> engine_;
    std::unique_ptr<ChunkServer> server_;
    std::thread thread_;
    std::mutex error_mutex_;
    std::exception_ptr error_;
};
inline std::pair<std::string, std::uint64_t> Start(const Reply& reply) {
    assert(reply.type == '+' && reply.value.rfind("OK ", 0) == 0);
    std::istringstream input(reply.value.substr(3));
    std::string epoch; std::uint64_t revision = 0;
    input >> epoch >> revision; assert(input && epoch.size() == 32U);
    return {epoch, revision};
}
inline std::uint64_t Change(const Reply& reply, std::size_t blocks = 1U) {
    if (reply.type != '>' || reply.items.size() != 7U || reply.items[0].value != "change")
        std::cerr << "expected change, got type=" << reply.type << " value=" << reply.value << " items=" << reply.items.size()
                  << " kind=" << (reply.items.empty() ? "" : reply.items[0].value) << '\n';
    assert(reply.type == '>' && reply.items.size() == 7U && reply.items[0].value == "change");
    assert(reply.items[6].type == '*' && reply.items[6].items.size() == blocks);
    return Number(reply.items[2]);
}
inline Reply NextChange(Client& client) {
    auto reply = client.Read();
    if (reply.type == '>' && !reply.items.empty() && reply.items[0].value == "schema") {
        assert(reply.items.size() == 5U && reply.items[1].type == '$' && reply.items[1].value.size() == 32U);
        assert(reply.items[4].type == '*' && !reply.items[4].items.empty());
        const auto version = Number(reply.items[3]);
        reply = client.Read();
        assert(reply.type == '>' && reply.items.size() == 7U && Number(reply.items[5]) == version);
    }
    (void)Change(reply, reply.items.size() == 7U ? reply.items[6].items.size() : 0U);
    return reply;
}
inline void Error(const Reply& reply, std::string_view code) {
    assert(reply.type == '-' && reply.value.rfind("ERR " + std::string(code), 0) == 0);
}
inline void Closed(Client& client) {
    try { (void)client.Read(1s); }
    catch (const std::runtime_error& error) {
        assert(std::string_view(error.what()).starts_with("socket closed while reading [client="));
        return;
    }
    throw std::runtime_error("terminal watch error left the connection usable");
}
inline void Unwatch(Client& client) {
    client.Line("UNWATCH");
    for (;;) {
        const auto reply = client.Read();
        if (reply.type == '+') { assert(reply.value == "OK"); return; }
        assert(reply.type == '>');
    }
}
inline const Reply& Slot(const Reply& slots, std::string_view table, std::string_view name) {
    assert(slots.type == '*');
    for (const auto& slot : slots.items)
        if (Field(slot, "table").value == table && Field(slot, "name").value == name) return slot;
    throw std::runtime_error("slot not shown");
}
inline void WaitAck(Client& observer, std::string_view table, std::string_view slot, std::uint64_t revision,
    std::chrono::milliseconds timeout = 10s) {
    const auto deadline = Clock::now() + timeout;
    do {
        const auto listed = observer.Command("SHOW SLOTS ON " + std::string(table));
        if (Number(Field(Slot(listed, table, slot), "acked")) == revision) return;
    } while (Clock::now() < deadline);
    throw std::runtime_error("ACK was not persisted without further traffic on watch");
}
} // namespace chunkdb::slot_socket_test
