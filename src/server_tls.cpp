#ifdef CHUNKDB_WITH_OPENSSL

#include "server_socket.hpp"

#include "server_io.hpp"
#include "server_tls.hpp"

#include "chunkdb/server.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chunkdb {
namespace server_detail {

std::string LastTlsErrorMessage() {
    const unsigned long code = ERR_get_error();
    if (code == 0) {
        return "unknown TLS error";
    }

    std::array<char, 256> buffer{};
    ERR_error_string_n(code, buffer.data(), buffer.size());
    return std::string(buffer.data());
}

ConnectionTermination ClassifyTlsFailure(
    SSL* tls_session,
    int result,
    std::string_view phase,
    bool log_peer_close) {
    ConnectionTermination termination;
    termination.phase = std::string(phase);
    termination.should_log = true;

    const int ssl_error = SSL_get_error(tls_session, result);
    switch (ssl_error) {
        case SSL_ERROR_ZERO_RETURN:
            termination.reason = "peer_close";
            termination.error = "tls close_notify";
            termination.should_log = log_peer_close;
            return termination;
        case SSL_ERROR_WANT_READ:
        case SSL_ERROR_WANT_WRITE: {
            const int socket_error = CurrentSocketErrorCode();
            if (socket_error != 0 && IsSocketTimeoutError(socket_error)) {
                termination.reason = "timeout";
                termination.error = FormatSocketError(socket_error);
            } else {
                termination.reason = "tls_error";
                termination.error = std::string("ssl_get_error=") + std::to_string(ssl_error);
            }
            return termination;
        }
        case SSL_ERROR_SYSCALL:
            if (result == 0) {
                termination.reason = "peer_close";
                termination.error = "peer closed connection during TLS I/O";
                termination.should_log = log_peer_close;
                return termination;
            }
            if (const int socket_error = CurrentSocketErrorCode(); socket_error != 0) {
                return MakeSocketTermination(phase, socket_error, log_peer_close);
            }
            termination.reason = "tls_error";
            termination.error = "tls syscall failure without socket error";
            return termination;
        case SSL_ERROR_SSL:
            termination.reason = "tls_error";
            termination.error = LastTlsErrorMessage();
            return termination;
        default:
            termination.reason = "tls_error";
            termination.error =
                "ssl_get_error=" + std::to_string(ssl_error) + " " + LastTlsErrorMessage();
            return termination;
    }
}

bool CompleteTlsHandshake(
    SSL* tls_session,
    SocketHandle socket_fd,
    std::size_t total_timeout_ms,
    ConnectionTermination* termination) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(total_timeout_ms);

    while (std::chrono::steady_clock::now() < deadline) {
        const int accept_result = SSL_accept(tls_session);
        if (accept_result == 1) {
            return true;
        }

        const int ssl_error = SSL_get_error(tls_session, accept_result);
        if (ssl_error != SSL_ERROR_WANT_READ && ssl_error != SSL_ERROR_WANT_WRITE) {
            if (termination != nullptr) {
                *termination = ClassifyTlsFailure(tls_session, accept_result, "handshake", true);
            }
            return false;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            break;
        }

        int socket_error_code = 0;
        const auto wait_result = WaitForSocketReady(
            socket_fd,
            ssl_error == SSL_ERROR_WANT_READ,
            ssl_error == SSL_ERROR_WANT_WRITE,
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now),
            &socket_error_code);
        if (wait_result == SocketWaitResult::kReady) {
            continue;
        }
        if (wait_result == SocketWaitResult::kError) {
            if (termination != nullptr) {
                *termination = MakeSocketTermination("handshake", socket_error_code, true);
            }
            return false;
        }
        break;
    }

    if (termination != nullptr) {
        *termination = MakePhaseDeadlineTermination("handshake", "TLS handshake deadline exceeded");
    }
    return false;
}

int ReadTlsWithin(
    SSL* tls_session,
    char* buffer,
    int size,
    std::chrono::milliseconds first_wait,
    std::size_t partial_timeout_ms,
    PhaseDeadline* absolute_deadline,
    std::string_view deadline_detail,
    ConnectionTermination* termination) {
    const int ssl_fd = SSL_get_fd(tls_session);
    if (ssl_fd < 0) {
        if (termination != nullptr) {
            termination->should_log = true;
            termination->phase = "read";
            termination->reason = "tls_error";
            termination->error = "TLS session has no socket fd";
        }
        return -1;
    }
    const auto socket_fd = static_cast<SocketHandle>(ssl_fd);
    std::string mode_error;
    if (!SetSocketNonBlocking(socket_fd, true, &mode_error)) {
        if (termination != nullptr) {
            termination->should_log = true;
            termination->phase = "read";
            termination->reason = "socket_error";
            termination->error = "failed to enable nonblocking TLS read mode: " + mode_error;
        }
        return -1;
    }
    const auto finish = [&](int result) {
        if (!SetSocketNonBlocking(socket_fd, false, &mode_error)) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "read";
                termination->reason = "socket_error";
                termination->error = "failed to restore blocking TLS read mode: " + mode_error;
            }
            return -1;
        }
        return result;
    };
    PhaseDeadline local_deadline;
    PhaseDeadline* deadline = absolute_deadline != nullptr ? absolute_deadline : &local_deadline;
    // Waiting with no record in progress ends `first_wait` after the call
    // began, however many non-data records (alerts, key updates) arrive.
    const auto idle_until = std::chrono::steady_clock::now() + first_wait;
    bool armed_here = false;
    while (true) {
        const int result = SSL_read(tls_session, buffer, size);
        if (result > 0) {
            return finish(result);
        }
        const int ssl_error = SSL_get_error(tls_session, result);
        if (ssl_error != SSL_ERROR_WANT_READ && ssl_error != SSL_ERROR_WANT_WRITE) {
            if (result < 0 && termination != nullptr) {
                *termination = ClassifyTlsFailure(tls_session, result, "read", true);
            }
            return finish(result < 0 ? -1 : 0);
        }
        // Part of a record is buffered: a request is arriving and must end by
        // the deadline. Nothing buffered: the last record was whole and
        // carried no data, so a deadline set for it no longer applies.
        const bool in_record = SSL_has_pending(tls_session) != 0;
        if (in_record && !deadline->has_value()) {
            *deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(partial_timeout_ms);
            armed_here = true;
        } else if (!in_record && armed_here) {
            *deadline = std::nullopt;
            armed_here = false;
        }
        const auto now = std::chrono::steady_clock::now();
        const bool by_deadline = deadline->has_value() && (in_record || **deadline <= idle_until);
        const auto until = by_deadline ? **deadline : idle_until;
        if (now >= until) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination(
                    "read", by_deadline ? deadline_detail : "no data within the receive timeout");
            }
            return finish(-1);
        }
        int socket_error_code = 0;
        const auto wait_result = WaitForSocketReady(
            socket_fd, ssl_error == SSL_ERROR_WANT_READ, ssl_error == SSL_ERROR_WANT_WRITE,
            std::chrono::ceil<std::chrono::milliseconds>(until - now), &socket_error_code);
        if (wait_result == SocketWaitResult::kError) {
            if (termination != nullptr) {
                *termination = MakeSocketTermination("read", socket_error_code, true);
            }
            return finish(-1);
        }
        if (wait_result == SocketWaitResult::kTimeout) {
            continue;  // the check above reports it
        }
        // Bytes of a record are arriving: from now on the record (and the
        // request it carries) must complete before the deadline.
        if (!deadline->has_value()) {
            *deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(partial_timeout_ms);
            armed_here = true;
        }
    }
}

bool WriteAllTls(
    SSL* tls_session,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination) {
    const bool deadline_aware_write = absolute_deadline.has_value();
    const int ssl_fd = SSL_get_fd(tls_session);
    if (deadline_aware_write) {
        if (ssl_fd < 0) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "write";
                termination->reason = "tls_error";
                termination->error = "TLS session has no socket fd";
            }
            return false;
        }
        std::string nonblocking_error;
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(ssl_fd), true, &nonblocking_error)) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "write";
                termination->reason = "socket_error";
                termination->error =
                    "failed to enable nonblocking TLS write mode: " + nonblocking_error;
            }
            return false;
        }
    }

    std::size_t written = 0;
    while (written < size) {
        if (deadline_aware_write && std::chrono::steady_clock::now() >= *absolute_deadline) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination("write", "reply write deadline exceeded");
            }
            return false;
        }

        const int result = SSL_write(
            tls_session,
            data + written,
            static_cast<int>(size - written));
        if (result <= 0) {
            const int ssl_error = SSL_get_error(tls_session, result);
            if (deadline_aware_write &&
                (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE)) {
                const auto now = std::chrono::steady_clock::now();
                if (now >= *absolute_deadline) {
                    if (termination != nullptr) {
                        *termination = MakePhaseDeadlineTermination(
                            "write",
                            "reply write deadline exceeded");
                    }
                    return false;
                }

                int socket_error_code = 0;
                const auto wait_result = WaitForSocketReady(
                    static_cast<SocketHandle>(ssl_fd),
                    ssl_error == SSL_ERROR_WANT_READ,
                    ssl_error == SSL_ERROR_WANT_WRITE,
                    std::chrono::duration_cast<std::chrono::milliseconds>(*absolute_deadline - now),
                    &socket_error_code);
                if (wait_result == SocketWaitResult::kReady) {
                    continue;
                }
                if (wait_result == SocketWaitResult::kTimeout) {
                    if (termination != nullptr) {
                        *termination = MakePhaseDeadlineTermination(
                            "write",
                            "reply write deadline exceeded");
                    }
                    return false;
                }
                if (termination != nullptr) {
                    *termination = MakeSocketTermination("write", socket_error_code, true);
                }
                return false;
            }
            if (termination != nullptr) {
                *termination = ClassifyTlsFailure(tls_session, result, "write", true);
            }
            return false;
        }
        written += static_cast<std::size_t>(result);
        if (written < size && absolute_deadline.has_value() &&
            std::chrono::steady_clock::now() >= *absolute_deadline) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination("write", "reply write deadline exceeded");
            }
            return false;
        }
    }

    if (deadline_aware_write) {
        std::string nonblocking_error;
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(ssl_fd), false, &nonblocking_error)) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "write";
                termination->reason = "socket_error";
                termination->error =
                    "failed to restore blocking TLS write mode: " + nonblocking_error;
            }
            return false;
        }
    }
    return true;
}

SSL_CTX* BuildTlsContext(const ServerConfig& config) {
    SSL_CTX* context = SSL_CTX_new(TLS_server_method());
    if (context == nullptr) {
        throw std::runtime_error("failed to create TLS context: " + LastTlsErrorMessage());
    }

    if (SSL_CTX_set_min_proto_version(context, TLS1_2_VERSION) != 1) {
        SSL_CTX_free(context);
        throw std::runtime_error("failed to set TLS minimum version: " + LastTlsErrorMessage());
    }

    if (SSL_CTX_use_certificate_file(context, config.tls_cert_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        SSL_CTX_free(context);
        throw std::runtime_error("failed to load TLS certificate: " + LastTlsErrorMessage());
    }

    if (SSL_CTX_use_PrivateKey_file(context, config.tls_key_path.c_str(), SSL_FILETYPE_PEM) != 1) {
        SSL_CTX_free(context);
        throw std::runtime_error("failed to load TLS private key: " + LastTlsErrorMessage());
    }

    if (SSL_CTX_check_private_key(context) != 1) {
        SSL_CTX_free(context);
        throw std::runtime_error("TLS private key does not match certificate");
    }

    return context;
}

}  // namespace server_detail
}  // namespace chunkdb

#else

namespace chunkdb {
namespace server_detail {
namespace {
[[maybe_unused]] const int kTlsUnitPlaceholder = 0;
}  // namespace
}  // namespace server_detail
}  // namespace chunkdb

#endif
