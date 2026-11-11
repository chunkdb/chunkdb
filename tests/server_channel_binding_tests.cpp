// Logins bound to their TLS connection over real sockets (docs/PROTOCOL.md):
// both channel binding types on TLS 1.3 and TLS 1.2, a relay that ends TLS
// itself, and servers and users that take only bound logins.

#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "server_slots_test_utils.hpp"

#ifdef CHUNKDB_WITH_OPENSSL

#include <openssl/bio.h>
#include <openssl/pem.h>

namespace {

using namespace chunkdb;
using namespace chunkdb::slot_socket_test;

constexpr std::string_view kWrongLogin = "AUTH_FAILED reason=auth_failed invalid user or password";
constexpr std::string_view kOtherConnection =
    "AUTH_FAILED reason=channel_binding the login is bound to another TLS connection";

void ExpectError(const Reply& reply, std::string_view start) {
    if (reply.type != '-' || !reply.value.starts_with("ERR " + std::string(start))) {
        std::fprintf(stderr, "expected an error starting '%.*s', got '%c%s'\n", static_cast<int>(start.size()),
                     start.data(), reply.type, reply.value.c_str());
        assert(false);
    }
}

void ExpectLoggedIn(const Reply& reply) {
    if (reply.type != '%') {
        std::fprintf(stderr, "expected a HELLO map, got '%c%s'\n", reply.type, reply.value.c_str());
        assert(false);
    }
}

// A TLS server with the user admin (password secret).
class Server {
  public:
    std::uint16_t port = FreePort();

    explicit Server(bool require_channel_binding = false) {
        catalog_ = std::make_shared<TableCatalog>(feed_test::Config(std::filesystem::canonical(directory_.path())));
        (void)feed_test::CreateDefault(*catalog_);
        EngineConfig engine_config;
        users_ = test::MakeUsers(directory_.path(), "admin", "secret");
        engine_config.users = users_;
        engine_config.require_channel_binding = require_channel_binding;
        engine_ = std::make_shared<CommandEngine>(engine_config, catalog_);
        ServerConfig server_config;
        server_config.port = port;
        server_config.worker_threads = 4;
        // Software SCRAM in Debug and sanitizer builds needs a login budget;
        // nothing here tests handshake deadlines.
        server_config.client_io_timeout_ms = 30000;
        server_config.tls_enabled = true;
        const auto cert = directory_.path() / "test-cert.pem";
        const auto key = directory_.path() / "test-key.pem";
        test::WriteTestTlsFiles(cert, key);
        server_config.tls_cert_path = cert.string();
        server_config.tls_key_path = key.string();
        server_ = std::make_unique<ChunkServer>(server_config, engine_);
        thread_ = std::thread([this] { server_->Run(); });
        // The listener is up once a TLS handshake completes.
        const auto deadline = Clock::now() + 10s;
        while (true) {
            try {
                Client probe(port, true);
                break;
            } catch (const std::runtime_error&) {
                if (Clock::now() >= deadline) {
                    server_->Stop();
                    thread_.join();
                    throw;
                }
                std::this_thread::sleep_for(10ms);
            }
        }
    }
    ~Server() {
        server_->Stop();
        thread_.join();
    }

    // Lets a client try `password` for admin: the test login derives keys
    // only for prepared credentials.
    void PrepareWrongPassword(std::string_view password) {
        const auto verifier = users_->Find("admin")->verifier;
        (void)test::PrepareLoginVerifier(password, verifier.salt, verifier.iterations);
    }

    // A user with `password` that takes only bound logins.
    void CreateBoundOnlyUser(Client& admin, std::string_view user, std::string_view password) {
        const auto verifier = test::PrepareLoginVerifier(password, crypto::RandomBytes(16), scram::kMinIterations);
        admin.Ok("CREATE USER " + std::string(user) + " VERIFIER '" + scram::FormatVerifier(verifier) +
                 "' REQUIRE CHANNEL BINDING");
    }

  private:
    test::ScopedTempDir directory_{"chunkdb-channel-binding"};
    std::shared_ptr<TableCatalog> catalog_;
    std::shared_ptr<UserRegistry> users_;
    std::shared_ptr<CommandEngine> engine_;
    std::unique_ptr<ChunkServer> server_;
    std::thread thread_;
};

// Ends the TLS connections it accepts with a certificate of its own and
// passes their bytes on over TLS connections it opens to `target`: what
// stands between a client that accepts a wrong certificate and the server.
class TlsRelay {
  public:
    explicit TlsRelay(std::uint16_t target) : target_(target) {
        const auto material = test::NewTestTls();
        server_context_ = SSL_CTX_new(TLS_server_method());
        client_context_ = SSL_CTX_new(TLS_client_method());
        assert(server_context_ != nullptr && client_context_ != nullptr);
        SSL_CTX_set_verify(client_context_, SSL_VERIFY_NONE, nullptr);
        BIO* cert_bio = BIO_new_mem_buf(material.cert_pem.data(), static_cast<int>(material.cert_pem.size()));
        BIO* key_bio = BIO_new_mem_buf(material.key_pem.data(), static_cast<int>(material.key_pem.size()));
        X509* certificate = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
        EVP_PKEY* key = PEM_read_bio_PrivateKey(key_bio, nullptr, nullptr, nullptr);
        assert(certificate != nullptr && key != nullptr);
        assert(SSL_CTX_use_certificate(server_context_, certificate) == 1);
        assert(SSL_CTX_use_PrivateKey(server_context_, key) == 1);
        X509_free(certificate);
        EVP_PKEY_free(key);
        BIO_free(cert_bio);
        BIO_free(key_bio);

        listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(listener_ != kInvalid);
        auto address = Address(0);
        assert(bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0);
        assert(listen(listener_, 8) == 0);
#ifdef _WIN32
        int length = sizeof(address);
#else
        socklen_t length = sizeof(address);
#endif
        assert(getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        port_ = ntohs(address.sin_port);
        thread_ = std::thread([this] { Run(); });
    }
    ~TlsRelay() {
        stop_ = true;
        thread_.join();
        CloseSocket(listener_);
        SSL_CTX_free(server_context_);
        SSL_CTX_free(client_context_);
    }
    TlsRelay(const TlsRelay&) = delete;
    TlsRelay& operator=(const TlsRelay&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

  private:
    // Whether either socket is readable within 50 ms.
    static bool Readable(Socket first, Socket second) {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(first, &readable);
        if (second != kInvalid) FD_SET(second, &readable);
        timeval wait{.tv_sec = 0, .tv_usec = 50000};
#ifdef _WIN32
        const int width = 0;
#else
        const int width = static_cast<int>(second != kInvalid && second > first ? second : first) + 1;
#endif
        return select(width, &readable, nullptr, nullptr, &wait) > 0;
    }

    // Waits for what a TLS call on a nonblocking socket asked for; false for
    // any other outcome of that call.
    bool Retry(SSL* session, Socket socket, int result) const {
        const int error = SSL_get_error(session, result);
        if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) return false;
        int socket_error = 0;
        (void)server_detail::WaitForSocketReady(
            socket, error == SSL_ERROR_WANT_READ, error == SSL_ERROR_WANT_WRITE, 50ms, &socket_error);
        return !stop_;
    }

    bool Handshake(SSL* session, Socket socket, bool accepting) const {
        while (true) {
            ERR_clear_error();
            const int result = accepting ? SSL_accept(session) : SSL_connect(session);
            if (result == 1) return true;
            if (!Retry(session, socket, result)) return false;
        }
    }

    // Passes on what `from` has to read; false once it is closed or failed.
    bool Move(SSL* from, SSL* to, Socket to_socket, bool* moved) const {
        char bytes[4096];
        ERR_clear_error();
        const int count = SSL_read(from, bytes, sizeof(bytes));
        if (count <= 0) {
            const int error = SSL_get_error(from, count);
            return error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE;
        }
        *moved = true;
        for (int sent = 0; sent < count;) {
            ERR_clear_error();
            const int written = SSL_write(to, bytes + sent, count - sent);
            if (written > 0) sent += written;
            else if (!Retry(to, to_socket, written)) return false;
        }
        return true;
    }

    void Serve(Socket inner_socket) {
        Socket outer_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(outer_socket != kInvalid);
        auto address = Address(target_);
        SSL* inner = SSL_new(server_context_);
        SSL* outer = SSL_new(client_context_);
        assert(inner != nullptr && outer != nullptr);
        std::string error;
        if (connect(outer_socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0 &&
            server_detail::SetSocketNonBlocking(inner_socket, true, &error) &&
            server_detail::SetSocketNonBlocking(outer_socket, true, &error) &&
            SSL_set_fd(inner, static_cast<int>(inner_socket)) == 1 && SSL_set_fd(outer, static_cast<int>(outer_socket)) == 1 &&
            Handshake(inner, inner_socket, true) && Handshake(outer, outer_socket, false)) {
            while (!stop_) {
                bool moved = false;
                if (!Move(inner, outer, outer_socket, &moved) || !Move(outer, inner, inner_socket, &moved)) break;
                if (!moved) (void)Readable(inner_socket, outer_socket);
            }
        }
        SSL_free(inner);
        SSL_free(outer);
        CloseSocket(inner_socket);
        CloseSocket(outer_socket);
    }

    void Run() {
        while (!stop_) {
            if (!Readable(listener_, kInvalid)) continue;
            const Socket accepted = accept(listener_, nullptr, nullptr);
            if (accepted != kInvalid) Serve(accepted);
        }
    }

    std::uint16_t target_;
    std::uint16_t port_ = 0;
    SSL_CTX* server_context_ = nullptr;
    SSL_CTX* client_context_ = nullptr;
    Socket listener_ = kInvalid;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

// Both binding types on a direct connection, and what each TLS version gives.
void TestBoundLogins() {
    Server server;
    server.PrepareWrongPassword("Secret");
    for (const auto type : {scram::kTlsExporter, scram::kTlsServerEndPoint}) {
        Client client(server.port, true);
        assert(client.TlsVersion() == TLS1_3_VERSION);
        const auto binding = client.ChannelBinding(type);
        assert(!binding.empty());
        const auto hello = client.LoginReply("admin", "secret", type, binding);
        ExpectLoggedIn(hello);
        const auto& features = Field(hello, "features").items;
        assert(std::any_of(features.begin(), features.end(), [](const Reply& name) { return name.value == "channel_binding"; }));
        assert(client.Command("PING").value == "PONG");

        Client wrong(server.port, true);
        ExpectError(wrong.LoginReply("admin", "Secret", type, wrong.ChannelBinding(type)), kWrongLogin);
        // The data of another connection to the same server.
        Client other(server.port, true);
        if (type == scram::kTlsExporter) ExpectError(other.LoginReply("admin", "secret", type, binding), kOtherConnection);
        // The server certificate is the same on both, so its hash binds both.
        else ExpectLoggedIn(other.LoginReply("admin", "secret", type, binding));
    }

    Client old(server.port, true, TLS1_2_VERSION);
    assert(old.TlsVersion() == TLS1_2_VERSION && old.ChannelBinding(scram::kTlsExporter).empty());
    const std::vector<std::uint8_t> anything(32, 0x5a);
    ExpectError(
        old.LoginReply("admin", "secret", scram::kTlsExporter, anything),
        "INVALID_ARGUMENT reason=invalid_argument tls-exporter channel binding needs TLS 1.3");
    ExpectLoggedIn(old.LoginReply(
        "admin", "secret", scram::kTlsServerEndPoint, old.ChannelBinding(scram::kTlsServerEndPoint)));

    Client unbound(server.port, true);
    ExpectLoggedIn(unbound.LoginReply("admin", "secret"));
}

// A relay that ends TLS passes an unbound login on, and no bound one.
void TestRelay() {
    Server server;
    TlsRelay relay(server.port);
    {
        Client victim(relay.port(), true);
        ExpectLoggedIn(victim.LoginReply("admin", "secret"));
        assert(victim.Command("PING").value == "PONG");
    }
    for (const auto type : {scram::kTlsExporter, scram::kTlsServerEndPoint}) {
        Client victim(relay.port(), true);
        const auto binding = victim.ChannelBinding(type);
        assert(!binding.empty());
        ExpectError(victim.LoginReply("admin", "secret", type, binding), kOtherConnection);
        // Nothing the victim sends afterwards is a statement.
        ExpectError(victim.Command("PING"), "PROTOCOL");
    }
    {
        Client direct(server.port, true);
        Client victim(relay.port(), true);
        assert(direct.ChannelBinding(scram::kTlsServerEndPoint) != victim.ChannelBinding(scram::kTlsServerEndPoint));
        assert(direct.ChannelBinding(scram::kTlsExporter) != victim.ChannelBinding(scram::kTlsExporter));
    }
}

// --auth-channel-binding require, and a user that takes only bound logins.
void TestRequired() {
    {
        Server strict(true);
        TlsRelay relay(strict.port);
        Client unbound(strict.port, true);
        ExpectError(unbound.LoginReply("admin", "secret"), "AUTH_REQUIRED reason=channel_binding");
        Client victim(relay.port(), true);
        ExpectError(victim.LoginReply("admin", "secret"), "AUTH_REQUIRED reason=channel_binding");
        Client bound(strict.port, true);
        ExpectLoggedIn(bound.LoginReply(
            "admin", "secret", scram::kTlsExporter, bound.ChannelBinding(scram::kTlsExporter)));
    }

    Server server;
    TlsRelay relay(server.port);
    Client admin(server.port, true);
    ExpectLoggedIn(admin.LoginReply("admin", "secret"));
    server.CreateBoundOnlyUser(admin, "bot", "hunter2");
    Client unbound(server.port, true);
    ExpectError(unbound.LoginReply("bot", "hunter2"), kWrongLogin);
    Client victim(relay.port(), true);
    ExpectError(victim.LoginReply("bot", "hunter2"), kWrongLogin);
    Client bound(server.port, true);
    ExpectLoggedIn(bound.LoginReply(
        "bot", "hunter2", scram::kTlsServerEndPoint, bound.ChannelBinding(scram::kTlsServerEndPoint)));
}

}  // namespace

int main() {
#ifndef _WIN32
    (void)std::signal(SIGPIPE, SIG_IGN);
#endif
    TestBoundLogins();
    TestRelay();
    TestRequired();
    return 0;
}

#else

int main() { return 0; }

#endif
