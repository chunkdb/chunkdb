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

template <typename EnsureRecvTimeoutFn>
bool ReadLineTls(
    SSL* tls_session,
    std::string& out,
    PendingLineBuffer& pending,
    std::size_t max_line_bytes,
    std::size_t partial_timeout_ms,
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
        const int read = SSL_read(tls_session, buffer.data(), static_cast<int>(buffer.size()));
        if (read == 0) {
            if (pending.empty()) {
                if (termination != nullptr) {
                    *termination = ClassifyTlsFailure(tls_session, read, "read", false);
                }
                return false;
            }
            const bool has_tail = pending.take_tail_on_close(&out, max_line_bytes);
            if (has_tail && absolute_deadline != nullptr) {
                *absolute_deadline = std::nullopt;
            }
            return has_tail;
        }
        if (read < 0) {
            if (termination != nullptr) {
                *termination = ClassifyTlsFailure(tls_session, read, "read", true);
            }
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

}  // namespace server_detail
}  // namespace chunkdb

#endif
