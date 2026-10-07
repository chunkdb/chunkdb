#pragma once

#ifdef CHUNKDB_WITH_OPENSSL

#include "server_socket.hpp"

#include "server_io.hpp"

#include "chunkdb/server.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>

namespace chunkdb {
namespace server_detail {

std::string LastTlsErrorMessage();

ConnectionTermination ClassifyTlsFailure(
    SSL* tls_session,
    int result,
    std::string_view phase,
    bool log_peer_close);

bool CompleteTlsHandshake(
    SSL* tls_session,
    SocketHandle socket_fd,
    std::size_t total_timeout_ms,
    ConnectionTermination* termination);

bool WriteAllTls(
    SSL* tls_session,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination);

SSL_CTX* BuildTlsContext(const ServerConfig& config);

// SSL_read with deadlines, on a socket made non-blocking for the call. With
// no record in progress it waits up to `first_wait` (the idle or handshake
// wait the caller chose), or to `*absolute_deadline` if that is sooner. Once
// record bytes arrive, `*absolute_deadline` is set if unset (now +
// partial_timeout_ms) and the read must finish before it, so a peer cannot
// hold the connection by trickling one TLS record; a whole record without
// data (an alert, a key update) clears a deadline set that way. Returns
// SSL_read's result: > 0 bytes, 0 for a closed session, or < 0 with
// `termination` set (`deadline_detail` names a deadline that expired).
int ReadTlsWithin(
    SSL* tls_session,
    char* buffer,
    int size,
    std::chrono::milliseconds first_wait,
    std::size_t partial_timeout_ms,
    PhaseDeadline* absolute_deadline,
    std::string_view deadline_detail,
    ConnectionTermination* termination);

template <typename EnsureRecvTimeoutFn>
bool ReadLineTls(
    SSL* tls_session,
    std::string& out,
    PendingLineBuffer& pending,
    std::size_t max_line_bytes,
    std::size_t partial_timeout_ms,
    std::size_t wait_ms,
    PhaseDeadline* absolute_deadline,
    ConnectionTermination* termination,
    EnsureRecvTimeoutFn&& ensure_recv_timeout) {
    out.clear();
    if (termination != nullptr) {
        *termination = {};
    }
    if (absolute_deadline != nullptr && !pending.empty() && !absolute_deadline->has_value()) {
        *absolute_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(partial_timeout_ms);
    }

    if (pending.extract_line(&out, max_line_bytes)) {
        if (absolute_deadline != nullptr) {
            *absolute_deadline = std::nullopt;
        }
        return true;
    }

    std::array<char, 4096> buffer{};
    while (true) {
        const int read = ReadTlsWithin(
            tls_session, buffer.data(), static_cast<int>(buffer.size()),
            std::chrono::milliseconds(wait_ms), partial_timeout_ms, absolute_deadline,
            "request line deadline exceeded", termination);
        if (read == 0) {
            if (pending.empty()) {
                if (termination != nullptr) {
                    *termination = ClassifyTlsFailure(tls_session, read, "read", false);
                }
                return false;
            }
            // A line without its terminator is never executed (see
            // ReadLinePlain).
            if (termination != nullptr) {
                termination->phase = "read";
                termination->reason = "peer_close";
                termination->error = "peer closed connection inside a request line; it was not executed";
                termination->should_log = true;
            }
            return false;
        }
        if (read < 0) {
            return false;
        }

        pending.append(buffer.data(), static_cast<std::size_t>(read));
        pending.enforce_partial_line_limit(max_line_bytes);
        if (absolute_deadline != nullptr && !absolute_deadline->has_value()) {
            *absolute_deadline =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(partial_timeout_ms);
        }

        if (pending.extract_line(&out, max_line_bytes)) {
            if (absolute_deadline != nullptr) {
                *absolute_deadline = std::nullopt;
            }
            return true;
        }

        if (absolute_deadline != nullptr && absolute_deadline->has_value() &&
            std::chrono::steady_clock::now() >= *absolute_deadline) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination("read", "request line deadline exceeded");
            }
            return false;
        }

        if (!pending.empty() && !ensure_recv_timeout(partial_timeout_ms, "partial_request")) {
            return false;
        }
    }
}

// TLS counterpart of ReadBytesPlain (see server_io.hpp).
template <typename EnsureRecvTimeoutFn>
bool ReadBytesTls(
    SSL* tls_session,
    std::string& out,
    std::size_t total,
    PendingLineBuffer& pending,
    std::size_t partial_timeout_ms,
    PhaseDeadline* absolute_deadline,
    ConnectionTermination* termination,
    EnsureRecvTimeoutFn&& ensure_recv_timeout) {
    out.clear();
    out.reserve(total);
    if (termination != nullptr) {
        *termination = {};
    }
    if (absolute_deadline != nullptr && !absolute_deadline->has_value()) {
        *absolute_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(partial_timeout_ms);
    }

    (void)pending.extract_bytes(total, &out);
    std::array<char, 4096> buffer{};
    while (out.size() < total) {
        const int read = ReadTlsWithin(
            tls_session, buffer.data(), static_cast<int>(buffer.size()),
            std::chrono::milliseconds(partial_timeout_ms), partial_timeout_ms, absolute_deadline,
            "payload deadline exceeded", termination);
        if (read == 0) {
            if (termination != nullptr) {
                *termination = ClassifyTlsFailure(tls_session, read, "read", true);
            }
            return false;
        }
        if (read < 0) {
            return false;
        }

        pending.append(buffer.data(), static_cast<std::size_t>(read));
        (void)pending.extract_bytes(total - out.size(), &out);

        if (absolute_deadline != nullptr && absolute_deadline->has_value() &&
            std::chrono::steady_clock::now() >= *absolute_deadline) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination("read", "payload deadline exceeded");
            }
            return false;
        }

        if (out.size() < total && !ensure_recv_timeout(partial_timeout_ms, "partial_request")) {
            return false;
        }
    }

    if (absolute_deadline != nullptr) {
        *absolute_deadline = std::nullopt;
    }
    return true;
}

}  // namespace server_detail
}  // namespace chunkdb

#endif
