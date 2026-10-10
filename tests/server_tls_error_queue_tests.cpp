#include "server_tls.hpp"
#include "server_slots_test_utils.hpp"

#include <cassert>
#include <future>
#include <string>
#ifndef _WIN32
#include <csignal>
#endif

namespace {
using namespace chunkdb::server_detail;
using namespace std::chrono_literals;
struct TlsPair {
    SSL_CTX* server_context = nullptr;
    SSL_CTX* client_context = nullptr;
    SSL* server = nullptr;
    SSL* client = nullptr;
    SocketHandle listener = kInvalidSocket, client_socket = kInvalidSocket, accepted = kInvalidSocket;
    TlsPair() {
        chunkdb::slot_socket_test::InitializeSockets();
        server_context = SSL_CTX_new(TLS_server_method());
        client_context = SSL_CTX_new(TLS_client_method());
        assert(server_context && client_context);
        SSL_CTX_set_verify(client_context, SSL_VERIFY_NONE, nullptr);
        // Reuse the protocol fixture's certificate, without its Client or Harness.
        const auto cert_text = chunkdb::slot_socket_test::kTestTlsCertPem;
        const auto key_text = chunkdb::slot_socket_test::kTestTlsKeyPem;
        auto* cert_bio = BIO_new_mem_buf(cert_text.data(), static_cast<int>(cert_text.size()));
        auto* key_bio = BIO_new_mem_buf(key_text.data(), static_cast<int>(key_text.size()));
        assert(cert_bio && key_bio);
        auto* certificate = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
        auto* key = PEM_read_bio_PrivateKey(key_bio, nullptr, nullptr, nullptr);
        assert(certificate && key);
        assert(SSL_CTX_use_certificate(server_context, certificate) == 1);
        assert(SSL_CTX_use_PrivateKey(server_context, key) == 1);
        X509_free(certificate); EVP_PKEY_free(key); BIO_free(cert_bio); BIO_free(key_bio);
        listener = CreateListenSocket("127.0.0.1", 0);
        sockaddr_in address{};
#ifdef _WIN32
        int length = sizeof(address);
#else
        socklen_t length = sizeof(address);
#endif
        assert(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        client_socket = socket(AF_INET, SOCK_STREAM, 0); assert(client_socket != kInvalidSocket);
        assert(connect(client_socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        accepted = accept(listener, nullptr, nullptr); assert(accepted != kInvalidSocket);
        server = SSL_new(server_context); client = SSL_new(client_context); assert(server && client);
        assert(SSL_set_fd(server, static_cast<int>(accepted)) == 1);
        assert(SSL_set_fd(client, static_cast<int>(client_socket)) == 1);
        std::string error;
        assert(ConfigureSocketRecvTimeout(client_socket, 5000U, &error));
        assert(ConfigureSocketSendTimeout(client_socket, 5000U, &error));
    }
    ~TlsPair() {
        SSL_free(server); SSL_free(client); SSL_CTX_free(server_context); SSL_CTX_free(client_context);
        for (const auto fd : {accepted, client_socket, listener}) if (fd != kInvalidSocket) CloseSocket(fd);
    }
    void Handshake() {
        auto connected = std::async(std::launch::async, [&] { return SSL_connect(client); });
        std::string error; assert(SetSocketNonBlocking(accepted, true, &error));
        ConnectionTermination ended;
        assert(CompleteTlsHandshake(server, accepted, 5000U, &ended));
        assert(connected.get() == 1);
        assert(SetSocketNonBlocking(accepted, false, &error));
    }
};
// Signal only once a real nonblocking BIO syscall has returned would-block.
// The peer starts sending/draining then, so the original SSL call necessarily
// needs SSL_get_error's WANT result before it can make progress.
struct RetryObserved {
    std::promise<void> retry;
    bool observed = false;
    int operation;
    explicit RetryObserved(int value) : operation(value) {}
    static long Callback(BIO* bio, int operation, const char*, std::size_t, int, long, int result, std::size_t*) {
        auto* state = reinterpret_cast<RetryObserved*>(BIO_get_callback_arg(bio));
        if (operation == (state->operation | BIO_CB_RETURN) && result <= 0 && BIO_should_retry(bio) && !state->observed) {
            state->observed = true; state->retry.set_value();
        }
        return result;
    }
    void Attach(BIO* bio) { BIO_set_callback_arg(bio, reinterpret_cast<char*>(this)); BIO_set_callback_ex(bio, Callback); }
    void Detach(BIO* bio) { BIO_set_callback_ex(bio, nullptr); BIO_set_callback_arg(bio, nullptr); }
};
void PriorThreadErrorDoesNotEndRead(bool syscall = true) {
    TlsPair pair; pair.Handshake();
    RetryObserved retry(BIO_CB_READ); retry.Attach(SSL_get_rbio(pair.server));
    auto ready = retry.retry.get_future();
    auto send = std::async(std::launch::async, [&] {
        assert(ready.wait_for(5s) == std::future_status::ready);
        return SSL_write(pair.client, "x", 1);
    });
    if (syscall) ERR_raise(ERR_LIB_SYS, EPIPE);
    else ERR_raise(ERR_LIB_SSL, ERR_R_INTERNAL_ERROR); // an unrelated earlier operation on this thread
    ConnectionTermination ended; PhaseDeadline deadline; char byte{};
    const auto result = ReadTlsWithin(pair.server, &byte, 1, 5s, 5000U, &deadline, "test read deadline", &ended);
    retry.Detach(SSL_get_rbio(pair.server));
    if (result != 1) std::fprintf(stderr, "TLS read failed result=%d reason=%s error=%s\n", result, ended.reason.c_str(), ended.error.c_str());
    assert(result == 1 && byte == 'x' && !ended.should_log);
    assert(send.get() == 1 && retry.observed);
    assert(ERR_peek_error() == 0);
}
void PriorThreadErrorDoesNotEndHandshake() {
    TlsPair pair;
    RetryObserved retry(BIO_CB_READ); retry.Attach(SSL_get_rbio(pair.server));
    auto ready = retry.retry.get_future();
    auto connect = std::async(std::launch::async, [&] {
        assert(ready.wait_for(5s) == std::future_status::ready);
        return SSL_connect(pair.client);
    });
    std::string error; assert(SetSocketNonBlocking(pair.accepted, true, &error));
    ERR_raise(ERR_LIB_SSL, ERR_R_INTERNAL_ERROR);
    ConnectionTermination ended;
    const bool connected = CompleteTlsHandshake(pair.server, pair.accepted, 5000U, &ended);
    retry.Detach(SSL_get_rbio(pair.server));
    assert(connected && !ended.should_log);
    assert(connect.get() == 1 && retry.observed);
    assert(ERR_peek_error() == 0);
}
void PriorThreadErrorDoesNotEndWrite() {
    TlsPair pair; pair.Handshake();
    const int send_bytes = 1024;
#ifdef _WIN32
    assert(setsockopt(pair.accepted, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&send_bytes), sizeof(send_bytes)) == 0);
#else
    assert(setsockopt(pair.accepted, SOL_SOCKET, SO_SNDBUF, &send_bytes, sizeof(send_bytes)) == 0);
#endif
    RetryObserved retry(BIO_CB_WRITE); retry.Attach(SSL_get_wbio(pair.server));
    auto ready = retry.retry.get_future();
    const std::string payload(1024U * 1024U, 'w');
    auto receive = std::async(std::launch::async, [&] {
        assert(ready.wait_for(5s) == std::future_status::ready);
        std::string received(payload.size(), '\0'); std::size_t at = 0U;
        while (at != received.size()) {
            const int count = SSL_read(pair.client, received.data() + at, static_cast<int>(received.size() - at));
            assert(count > 0); at += static_cast<std::size_t>(count);
        }
        return received;
    });
    ERR_raise(ERR_LIB_SSL, ERR_R_INTERNAL_ERROR);
    ConnectionTermination ended;
    const bool sent = WriteAllTls(pair.server, payload.data(), payload.size(), std::chrono::steady_clock::now() + 10s, &ended);
    retry.Detach(SSL_get_wbio(pair.server));
    if (!sent) std::fprintf(stderr, "TLS write failed reason=%s error=%s\n", ended.reason.c_str(), ended.error.c_str());
    assert(sent && !ended.should_log && retry.observed);
    assert(receive.get() == payload);
    assert(ERR_peek_error() == 0);
}
}
int main(int argc, char** argv) {
#ifndef _WIN32
    (void)signal(SIGPIPE, SIG_IGN);
#endif
    if (argc == 2) {
        if (std::string_view(argv[1]) == "--read") { PriorThreadErrorDoesNotEndRead(); return 0; }
        if (std::string_view(argv[1]) == "--read-ssl") { PriorThreadErrorDoesNotEndRead(false); return 0; }
        if (std::string_view(argv[1]) == "--handshake") { PriorThreadErrorDoesNotEndHandshake(); return 0; }
        if (std::string_view(argv[1]) == "--write") { PriorThreadErrorDoesNotEndWrite(); return 0; }
    }
    PriorThreadErrorDoesNotEndRead(); PriorThreadErrorDoesNotEndRead(false); PriorThreadErrorDoesNotEndHandshake(); PriorThreadErrorDoesNotEndWrite();
    std::puts("TLS error queue passed: 2 reads, handshake, write after unrelated thread error");
}
