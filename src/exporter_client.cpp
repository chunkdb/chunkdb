#include "exporter_client.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <fstream>
#include <optional>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "chunkdb/logging.hpp"
#include "scram.hpp"
#include "server_socket.hpp"

#ifndef _WIN32
#include <sys/stat.h>
#endif

#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#endif

namespace chunkdb::exporter {

using server_detail::CloseSocket;
using server_detail::CurrentSocketErrorCode;
using server_detail::FormatSocketError;
using server_detail::kInvalidSocket;
using server_detail::SocketHandle;
using server_detail::SocketWaitResult;
using server_detail::WaitForSocketReady;
using Clock = std::chrono::steady_clock;

namespace {

// The largest reply the exporter reads: SHOW METRICS of many tables.
constexpr std::size_t kMaxReplyBytes = 256U << 20U;
// How deep and how many values a reply may nest.
constexpr std::size_t kMaxDepth = 16;
constexpr std::size_t kMaxValues = 1U << 20U;
// The longest one waits between checks of the stop token.
constexpr std::chrono::milliseconds kStopCheck{200};

bool WouldBlock(int code) {
#ifdef _WIN32
    return code == WSAEWOULDBLOCK || code == WSAEINPROGRESS;
#else
    return code == EAGAIN || code == EWOULDBLOCK || code == EINPROGRESS;
#endif
}

// A reply value: its type byte, its text (simple, error, bulk, number) and
// the values of an aggregate (a map's keys and values alternate).
struct Value {
    char type = '_';
    std::string text;
    std::vector<Value> children;
};

}  // namespace

std::string ReadPasswordFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot open the password file " + path);
#ifndef _WIN32
    struct stat status {};
    if (::stat(path.c_str(), &status) != 0) throw std::runtime_error("cannot read the mode of the password file " + path);
    if ((status.st_mode & 077) != 0) {
        throw std::runtime_error("the password file " + path +
                                 " is accessible by group or others; make it readable by its owner only (chmod 600)");
    }
#endif
    std::string password;
    std::getline(input, password);
    if (!password.empty() && password.back() == '\r') password.pop_back();
    if (password.empty()) throw std::runtime_error("the password file " + path + " has an empty first line");
    return password;
}

struct ChunkConnection::State {
    SocketHandle socket = kInvalidSocket;
#ifdef CHUNKDB_WITH_OPENSSL
    SSL_CTX* context = nullptr;
    SSL* tls = nullptr;
#endif
    std::string input;
    std::size_t consumed = 0;
    Clock::time_point deadline;
    std::stop_token stop;

    ~State() {
#ifdef CHUNKDB_WITH_OPENSSL
        if (tls != nullptr) SSL_free(tls);
        if (context != nullptr) SSL_CTX_free(context);
#endif
        if (socket != kInvalidSocket) CloseSocket(socket);
    }

    // Waits for the socket until it is ready, the deadline passes or a stop
    // is requested; throws in the last two cases.
    void Wait(bool read, bool write) {
        for (;;) {
            if (stop.stop_requested()) throw std::runtime_error("stopped");
            const auto now = Clock::now();
            if (now >= deadline) throw std::runtime_error("the server did not answer in time");
            const auto wait =
                std::min(std::chrono::ceil<std::chrono::milliseconds>(deadline - now), kStopCheck);
            int error = 0;
            const auto ready = WaitForSocketReady(socket, read, write, wait, &error);
            if (ready == SocketWaitResult::kReady) return;
            if (ready == SocketWaitResult::kError) throw std::runtime_error("wait failed: " + FormatSocketError(error));
        }
    }

    void Connect(const std::string& host, std::uint16_t port) {
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* found = nullptr;
        const std::string port_text = std::to_string(port);
        if (const int error = getaddrinfo(host.c_str(), port_text.c_str(), &hints, &found); error != 0 || !found) {
            throw std::runtime_error("cannot resolve " + host);
        }
        std::string failure = "no address";
        for (auto* address = found; address != nullptr; address = address->ai_next) {
            const SocketHandle candidate = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
            if (candidate == kInvalidSocket) continue;
            std::string error;
            if (!server_detail::SetSocketNonBlocking(candidate, true, &error)) {
                CloseSocket(candidate);
                failure = error;
                continue;
            }
#if defined(SO_NOSIGPIPE)
            const int on = 1;
            (void)setsockopt(candidate, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
            socket = candidate;
            try {
                if (::connect(candidate, address->ai_addr, static_cast<int>(address->ai_addrlen)) != 0) {
                    const int code = CurrentSocketErrorCode();
                    if (!WouldBlock(code)) throw std::runtime_error(FormatSocketError(code));
                    Wait(false, true);
                    int result = 0;
#ifdef _WIN32
                    int size = sizeof(result);
                    (void)getsockopt(candidate, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&result), &size);
#else
                    socklen_t size = sizeof(result);
                    (void)getsockopt(candidate, SOL_SOCKET, SO_ERROR, &result, &size);
#endif
                    if (result != 0) throw std::runtime_error(FormatSocketError(result));
                }
                freeaddrinfo(found);
                return;
            } catch (const std::exception& error) {
                failure = error.what();
                CloseSocket(candidate);
                socket = kInvalidSocket;
                if (stop.stop_requested() || Clock::now() >= deadline) break;
            }
        }
        freeaddrinfo(found);
        throw std::runtime_error("cannot connect to " + host + ":" + port_text + ": " + failure);
    }

#ifdef CHUNKDB_WITH_OPENSSL
    [[noreturn]] static void TlsFail(const std::string& what) {
        const unsigned long code = ERR_get_error();
        ERR_clear_error();
        std::string detail;
        if (code != 0) {
            char buffer[256];
            ERR_error_string_n(code, buffer, sizeof(buffer));
            detail = std::string(": ") + buffer;
        }
        throw std::runtime_error(what + detail);
    }

    // Waits as `result` of a TLS call asks; false when the call failed.
    bool TlsRetry(int result) {
        switch (SSL_get_error(tls, result)) {
            case SSL_ERROR_WANT_READ: Wait(true, false); return true;
            case SSL_ERROR_WANT_WRITE: Wait(false, true); return true;
            default: return false;
        }
    }

    void StartTls(const std::string& host, const std::string& ca_file) {
        context = SSL_CTX_new(TLS_client_method());
        if (context == nullptr) TlsFail("cannot create a TLS context");
        (void)SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION);
        SSL_CTX_set_verify(context, SSL_VERIFY_PEER, nullptr);
        if (!ca_file.empty()) {
            if (SSL_CTX_load_verify_locations(context, ca_file.c_str(), nullptr) != 1) {
                TlsFail("cannot read the TLS CA file " + ca_file);
            }
        } else if (SSL_CTX_set_default_verify_paths(context) != 1) {
            TlsFail("cannot load the system certificates");
        }
        tls = SSL_new(context);
        if (tls == nullptr) TlsFail("cannot create a TLS session");
        if (SSL_set_fd(tls, static_cast<int>(socket)) != 1) TlsFail("cannot attach TLS to the socket");
        // The certificate must name the host: an IP address, or a DNS name,
        // which is also sent as SNI.
        unsigned char address[16];
        const bool is_ip = inet_pton(AF_INET, host.c_str(), address) == 1 || inet_pton(AF_INET6, host.c_str(), address) == 1;
        if (is_ip) {
            if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(tls), host.c_str()) != 1) TlsFail("bad host address");
        } else {
            if (X509_VERIFY_PARAM_set1_host(SSL_get0_param(tls), host.c_str(), host.size()) != 1) TlsFail("bad host name");
            (void)SSL_set_tlsext_host_name(tls, host.c_str());
        }
        for (;;) {
            ERR_clear_error();
            const int result = SSL_connect(tls);
            if (result == 1) break;
            if (!TlsRetry(result)) {
                const long verify = SSL_get_verify_result(tls);
                if (verify != X509_V_OK) {
                    ERR_clear_error();
                    throw std::runtime_error(std::string("the server certificate is not trusted: ") +
                                             X509_verify_cert_error_string(verify));
                }
                TlsFail("TLS handshake failed");
            }
        }
    }

    // What this connection gives a login to bind to (RFC 9266 on TLS 1.3,
    // RFC 5929 below it), and the type's name.
    std::pair<std::string, std::vector<std::uint8_t>> Binding() const {
        if (SSL_version(tls) == TLS1_3_VERSION) {
            constexpr std::string_view kLabel = "EXPORTER-Channel-Binding";
            std::vector<std::uint8_t> exporter(32);
            const unsigned char no_context = 0;
            if (SSL_export_keying_material(tls, exporter.data(), exporter.size(), kLabel.data(), kLabel.size(),
                                           &no_context, 0, 1) != 1) {
                TlsFail("cannot read the TLS exporter");
            }
            return {std::string(scram::kTlsExporter), exporter};
        }
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
        X509* certificate = SSL_get1_peer_certificate(tls);
#else
        X509* certificate = SSL_get_peer_certificate(tls);
#endif
        if (certificate == nullptr) throw std::runtime_error("the server sent no certificate");
        std::vector<std::uint8_t> hash;
        int digest_nid = NID_undef;
        if (X509_get_signature_info(certificate, &digest_nid, nullptr, nullptr, nullptr) == 1 && digest_nid != NID_undef) {
            if (digest_nid == NID_md5 || digest_nid == NID_sha1) digest_nid = NID_sha256;
            if (const EVP_MD* digest = EVP_get_digestbynid(digest_nid); digest != nullptr) {
                unsigned int size = 0;
                hash.resize(EVP_MAX_MD_SIZE);
                if (X509_digest(certificate, digest, hash.data(), &size) == 1) hash.resize(size);
                else hash.clear();
            }
        }
        X509_free(certificate);
        if (hash.empty()) {
            throw std::runtime_error(
                "the server certificate gives no tls-server-end-point binding below TLS 1.3; use TLS 1.3 or "
                "--no-channel-binding");
        }
        return {std::string(scram::kTlsServerEndPoint), hash};
    }
#endif

    void Send(std::string_view bytes) {
        std::size_t sent = 0;
        while (sent < bytes.size()) {
#ifdef CHUNKDB_WITH_OPENSSL
            if (tls != nullptr) {
                ERR_clear_error();
                const int result = SSL_write(tls, bytes.data() + sent, static_cast<int>(std::min<std::size_t>(bytes.size() - sent, 1U << 20U)));
                if (result > 0) {
                    sent += static_cast<std::size_t>(result);
                } else if (!TlsRetry(result)) {
                    TlsFail("TLS write failed");
                }
                continue;
            }
#endif
#ifdef _WIN32
            const int result = ::send(socket, bytes.data() + sent, static_cast<int>(std::min<std::size_t>(bytes.size() - sent, 1U << 20U)), 0);
#elif defined(MSG_NOSIGNAL)
            const auto result = ::send(socket, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
#else
            const auto result = ::send(socket, bytes.data() + sent, bytes.size() - sent, 0);
#endif
            if (result > 0) {
                sent += static_cast<std::size_t>(result);
                continue;
            }
            const int code = CurrentSocketErrorCode();
            if (result < 0 && (WouldBlock(code) || server_detail::IsSocketInterruptedError(code))) {
                Wait(false, true);
                continue;
            }
            throw std::runtime_error("write failed: " + FormatSocketError(code));
        }
    }

    // Reads more input; throws when the server closed the connection.
    void Fill() {
        if (consumed > 0 && consumed == input.size()) {
            input.clear();
            consumed = 0;
        }
        char buffer[16384];
        for (;;) {
#ifdef CHUNKDB_WITH_OPENSSL
            if (tls != nullptr) {
                ERR_clear_error();
                const int result = SSL_read(tls, buffer, sizeof(buffer));
                if (result > 0) {
                    input.append(buffer, static_cast<std::size_t>(result));
                    return;
                }
                if (SSL_get_error(tls, result) == SSL_ERROR_ZERO_RETURN) throw std::runtime_error("the server closed the connection");
                if (!TlsRetry(result)) TlsFail("TLS read failed");
                continue;
            }
#endif
#ifdef _WIN32
            const int result = ::recv(socket, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const auto result = ::recv(socket, buffer, sizeof(buffer), 0);
#endif
            if (result > 0) {
                input.append(buffer, static_cast<std::size_t>(result));
                return;
            }
            if (result == 0) throw std::runtime_error("the server closed the connection");
            const int code = CurrentSocketErrorCode();
            if (WouldBlock(code) || server_detail::IsSocketInterruptedError(code)) {
                Wait(true, false);
                continue;
            }
            throw std::runtime_error("read failed: " + FormatSocketError(code));
        }
    }

    // Frees the input buffer after a large reply once it is all read.
    void Compact() {
        if (consumed != input.size()) return;
        consumed = 0;
        if (input.capacity() > (1U << 20U)) std::string().swap(input);
        else input.clear();
    }

    std::string ReadLine() {
        for (;;) {
            const auto end = input.find("\r\n", consumed);
            if (end != std::string::npos) {
                std::string line = input.substr(consumed, end - consumed);
                consumed = end + 2U;
                return line;
            }
            if (input.size() - consumed > 65536U) throw std::runtime_error("reply line too long");
            Fill();
        }
    }

    std::string ReadExact(std::size_t size) {
        while (input.size() - consumed < size + 2U) Fill();
        std::string bytes = input.substr(consumed, size);
        if (input.compare(consumed + size, 2, "\r\n") != 0) throw std::runtime_error("malformed bulk reply");
        consumed += size + 2U;
        return bytes;
    }

    static std::size_t Count(std::string_view digits, std::size_t limit) {
        std::size_t value = 0;
        const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
        if (digits.empty() || parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || value > limit) {
            throw std::runtime_error("malformed reply length");
        }
        return value;
    }

    Value Read(std::size_t depth, std::size_t* values) {
        if (depth > kMaxDepth || ++*values > kMaxValues) throw std::runtime_error("reply too large");
        const std::string line = ReadLine();
        if (line.empty()) throw std::runtime_error("empty reply line");
        Value value;
        value.type = line[0];
        const std::string_view rest = std::string_view(line).substr(1);
        switch (value.type) {
            case '+': case '-': case ':': case ',': case '(': case '#':
                value.text = std::string(rest);
                return value;
            case '_':
                return value;
            case '$':
                if (rest == "-1") {
                    value.type = '_';
                    return value;
                }
                value.text = ReadExact(Count(rest, kMaxReplyBytes));
                return value;
            case '*': case '~': case '%': {
                std::size_t items = Count(rest, kMaxValues);
                if (value.type == '%') items *= 2U;
                for (std::size_t i = 0; i < items; ++i) value.children.push_back(Read(depth + 1U, values));
                return value;
            }
            default:
                throw std::runtime_error("unknown reply type");
        }
    }

    Value Read() {
        std::size_t values = 0;
        return Read(0, &values);
    }
};

namespace {

std::string Framed(std::string_view line, std::string_view frame) {
    std::string bytes(line);
    bytes += "\r\n$" + std::to_string(frame.size()) + "\r\n";
    bytes += frame;
    bytes += "\r\n";
    return bytes;
}

[[noreturn]] void Refused(const Value& value, std::string_view what) {
    if (value.type == '-') throw ServerReplyError(std::string(what) + ": " + value.text);
    throw std::runtime_error(std::string(what) + ": unexpected reply");
}

// The HELLO map's `server_signature`; std::nullopt for null or when absent.
std::optional<std::string> ServerSignature(const Value& hello) {
    for (std::size_t i = 0; i + 1U < hello.children.size(); i += 2U) {
        if (hello.children[i].text == "server_signature" && hello.children[i + 1U].type == '$') {
            return hello.children[i + 1U].text;
        }
    }
    return std::nullopt;
}

}  // namespace

ChunkConnection::ChunkConnection(const ExporterOptions& options, Clock::time_point deadline, std::stop_token stop)
    : state_(std::make_unique<State>()) {
    state_->deadline = deadline;
    state_->stop = std::move(stop);
    state_->Connect(options.uri.host, options.uri.port);
    std::string binding_type;
    std::vector<std::uint8_t> binding_data;
    if (options.uri.secure) {
#ifdef CHUNKDB_WITH_OPENSSL
        state_->StartTls(options.uri.host, options.tls_ca_file);
        if (options.channel_binding && !options.uri.user.empty()) {
            std::tie(binding_type, binding_data) = state_->Binding();
        }
#else
        throw std::runtime_error("this build has no TLS support for chunks://");
#endif
    }

    if (options.uri.user.empty()) {
        state_->Send("HELLO 3\r\n");
        const auto hello = state_->Read();
        if (hello.type != '%') Refused(hello, "HELLO refused");
        return;
    }
    const std::string password = ReadPasswordFile(options.password_file);
    const auto login = scram::StartClientLogin(options.uri.user, scram::NewNonce(), binding_type);
    state_->Send(Framed("HELLO 3 USER " + options.uri.user + " $1", login.first));
    const auto challenge = state_->Read();
    constexpr std::string_view kScram = "SCRAM ";
    if (challenge.type != '+' || !challenge.text.starts_with(kScram)) Refused(challenge, "login refused");
    const auto final_message = scram::FinishClientLogin(
        login, password, std::string_view(challenge.text).substr(kScram.size()), binding_data);
    state_->Send(Framed("AUTH $1", final_message.message));
    const auto hello = state_->Read();
    if (hello.type != '%') Refused(hello, "login refused");
    if (ServerSignature(hello) != final_message.server_signature) {
        throw std::runtime_error("the server did not prove that it knows the password verifier");
    }
}

ChunkConnection::~ChunkConnection() = default;

std::string ChunkConnection::Query(std::string_view statement, Clock::time_point deadline, std::stop_token stop) {
    state_->deadline = deadline;
    state_->stop = std::move(stop);
    state_->Send(std::string(statement) + "\r\n");
    const auto reply = state_->Read();
    if (reply.type != '$' && reply.type != '+') Refused(reply, std::string(statement) + " refused");
    state_->Compact();
    return reply.text;
}

Exporter::Exporter(ExporterOptions options) : options_(std::move(options)) {}

Exporter::~Exporter() = default;

namespace {

// After a failed connect or login: the first wait before the next, doubled
// after each failure up to the last.
constexpr std::chrono::milliseconds kFirstBackoff{1000};
constexpr std::chrono::milliseconds kMaxBackoff{30000};

constexpr std::string_view kUpHeader =
    "# HELP chunkdb_up Whether chunkdb_exporter read the server's metrics in this scrape.\n"
    "# TYPE chunkdb_up gauge\n";

}  // namespace

std::string DownBody() {
    return std::string(kUpHeader) + "chunkdb_up 0\n";
}

server_detail::MetricsScrape Exporter::Scrape(std::stop_token stop) {
    const auto deadline = Clock::now() + options_.timeout;
    const server_detail::MetricsScrape down{.available = false, .body = DownBody()};
    // After a failed connect or login, none again before retry_at_.
    if (connection_ == nullptr && Clock::now() < retry_at_) return down;
    std::string failure;
    for (int attempt = 0; attempt < 2; ++attempt) {
        const bool reused = connection_ != nullptr;
        try {
            if (!reused) {
                try {
                    connection_ = std::make_unique<ChunkConnection>(options_, deadline, stop);
                } catch (...) {
                    backoff_ = backoff_.count() == 0 ? kFirstBackoff : std::min(backoff_ * 2, kMaxBackoff);
                    retry_at_ = Clock::now() + backoff_;
                    throw;
                }
            }
            auto body = connection_->Query("SHOW METRICS", deadline, stop);
            backoff_ = std::chrono::milliseconds::zero();
            if (!up_) LogMessage(LogLevel::kInfo, LogComponent::kServer, "reading the server's metrics again");
            up_ = true;
            if (!body.empty() && body.back() != '\n') body += '\n';
            body += kUpHeader;
            body += "chunkdb_up 1\n";
            return {.available = true, .body = std::move(body)};
        } catch (const ServerReplyError& error) {
            // A refused statement leaves the connection usable.
            failure = error.what();
            break;
        } catch (const std::exception& error) {
            failure = error.what();
            connection_.reset();
            // A kept connection may have been closed by the server's idle
            // timeout or a restart: one fresh connection, in the same time.
            if (reused && !stop.stop_requested() && Clock::now() < deadline) continue;
            break;
        }
    }
    if (up_) LogMessage(LogLevel::kWarn, LogComponent::kServer, "cannot read the server's metrics", {{"error", failure}});
    up_ = false;
    return down;
}

}  // namespace chunkdb::exporter
