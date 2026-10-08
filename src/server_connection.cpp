#include <algorithm>
#include "chunkdb/server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"

#include "server_socket.hpp"
#include "server_io.hpp"
#include "server_tls.hpp"
#include "source_address.hpp"

namespace chunkdb {

using namespace server_detail;

void ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
) {
    SessionState session;
    session.remote_address = PeerAddressForSocket(static_cast<SocketHandle>(client_socket));

    // Until HELLO succeeds a connection has proved nothing: with
    // max_handshakes_per_ip, one source may hold only that many workers in
    // that state. The slot is released at HELLO or when the connection ends.
    constexpr std::string_view kTooManyHandshakes = "too many connections before HELLO from this address";
    const std::string source = SourceAddressKey(session.remote_address);
    const bool limited = config_.max_handshakes_per_ip != 0;
    if (limited && !TryAcquireHandshake(source)) {
        engine_->metrics()->CountConnectionRejected();
#ifdef CHUNKDB_WITH_OPENSSL
        if (!config_.tls_enabled) {
            SendPlainBusyResponse(static_cast<SocketHandle>(client_socket), config_.client_io_timeout_ms, kTooManyHandshakes);
        }
#else
        SendPlainBusyResponse(static_cast<SocketHandle>(client_socket), config_.client_io_timeout_ms, kTooManyHandshakes);
#endif
        CloseSocket(static_cast<SocketHandle>(client_socket));
        if (!handshake_limit_warned_.exchange(true, std::memory_order_relaxed)) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kServer,
                "connections before HELLO from one source reached the limit; rejecting more",
                {{"source", source}, {"max_handshakes_per_ip", std::to_string(config_.max_handshakes_per_ip)}});
        }
        return;
    }
    struct HandshakeSlot {
        ChunkServer* server;
        const std::string& source;
        bool held;
        void Release() noexcept {
            if (held) {
                held = false;
                server->ReleaseHandshake(source);
            }
        }
        ~HandshakeSlot() { Release(); }
    } handshake_slot{this, source, limited};
    std::string line;
    PendingLineBuffer pending_buffer;
    PhaseDeadline request_line_deadline;
    ConnectionTermination termination;
    std::optional<std::size_t> current_recv_timeout_ms;

    auto set_recv_timeout = [&](std::size_t timeout_ms, std::string_view phase) -> bool {
        if (current_recv_timeout_ms.has_value() && *current_recv_timeout_ms == timeout_ms) {
            return true;
        }
        std::string timeout_error;
        const bool recv_timeout_ok =
            !ConsumeTestFailureBudget(&g_test_recv_timeout_config_failures) &&
            ConfigureSocketRecvTimeout(static_cast<SocketHandle>(client_socket), timeout_ms, &timeout_error);
        if (!recv_timeout_ok) {
            if (timeout_error.empty()) {
                timeout_error = "injected timeout config failure";
            }
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kServer,
                "failed to configure client receive timeout; closing connection",
                {
                    {"phase", phase},
                    {"timeout_ms", std::to_string(timeout_ms)},
                    {"error", timeout_error},
                });
            return false;
        }
        current_recv_timeout_ms = timeout_ms;
        return true;
    };

#ifdef CHUNKDB_WITH_OPENSSL
    SSL* tls_session = nullptr;
    if (config_.tls_enabled) {
        tls_session = SSL_new(tls_context_);
        if (tls_session == nullptr) {
            CloseSocket(static_cast<SocketHandle>(client_socket));
            return;
        }

        std::string nonblocking_error;
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(client_socket), true, &nonblocking_error)) {
            termination.should_log = true;
            termination.phase = "handshake";
            termination.reason = "socket_error";
            termination.error = "failed to enable nonblocking handshake mode: " + nonblocking_error;
            LogConnectionTermination(termination);
            SSL_free(tls_session);
            CloseSocket(static_cast<SocketHandle>(client_socket));
            return;
        }
        SSL_set_fd(tls_session, static_cast<int>(client_socket));
        if (!CompleteTlsHandshake(
                tls_session,
                static_cast<SocketHandle>(client_socket),
                config_.client_io_timeout_ms,
                &termination)) {
            LogConnectionTermination(termination);
            SSL_free(tls_session);
            CloseSocket(static_cast<SocketHandle>(client_socket));
            return;
        }
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(client_socket), false, &nonblocking_error)) {
            termination.should_log = true;
            termination.phase = "handshake";
            termination.reason = "socket_error";
            termination.error = "failed to restore blocking TLS socket mode: " + nonblocking_error;
            LogConnectionTermination(termination);
            SSL_free(tls_session);
            CloseSocket(static_cast<SocketHandle>(client_socket));
            return;
        }
        if (!set_recv_timeout(config_.idle_connection_timeout_ms, "idle")) {
            SSL_free(tls_session);
            CloseSocket(static_cast<SocketHandle>(client_socket));
            return;
        }
    }
#endif

    auto read_line = [&](std::string& out) -> bool {
#ifdef CHUNKDB_WITH_OPENSSL
        if (config_.tls_enabled) {
            return ReadLineTls(
                tls_session,
                out,
                pending_buffer,
                config_.max_line_bytes,
                config_.client_io_timeout_ms,
                current_recv_timeout_ms.value_or(config_.client_io_timeout_ms),
                &request_line_deadline,
                &termination,
                set_recv_timeout);
        }
#endif
        return ReadLinePlain(
            static_cast<SocketHandle>(client_socket),
            out,
            pending_buffer,
            config_.max_line_bytes,
            config_.client_io_timeout_ms,
            &request_line_deadline,
            &termination,
            set_recv_timeout);
    };
    auto read_bytes = [&](std::string& out, std::size_t total) -> bool {
#ifdef CHUNKDB_WITH_OPENSSL
        if (config_.tls_enabled) {
            return ReadBytesTls(
                tls_session,
                out,
                total,
                pending_buffer,
                config_.client_io_timeout_ms,
                &request_line_deadline,
                &termination,
                set_recv_timeout);
        }
#endif
        return ReadBytesPlain(
            static_cast<SocketHandle>(client_socket),
            out,
            total,
            pending_buffer,
            config_.client_io_timeout_ms,
            &request_line_deadline,
            &termination,
            set_recv_timeout);
    };
    auto write_all = [&](const std::string& data, ConnectionTermination* write_termination) -> bool {
        const PhaseDeadline write_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_io_timeout_ms);
#ifdef CHUNKDB_WITH_OPENSSL
        if (config_.tls_enabled) {
            return WriteAllTls(tls_session, data.data(), data.size(), write_deadline, write_termination);
        }
#endif
        return WriteAllPlain(
            static_cast<SocketHandle>(client_socket),
            data.data(),
            data.size(),
            write_deadline,
            write_termination);
    };
    // Reply with an error and drop the connection: the request stream can no
    // longer be trusted (oversized line, parameter frames that cannot be bounded, bad frame
    // terminator).
    auto reject_and_close = [&](const std::string& response, std::string_view reason) {
        engine_->metrics()->CountMalformedRequest();
        LogMessage(
            LogLevel::kWarn,
            LogComponent::kServer,
            "bad request disconnect",
            {{"reason", std::string(reason)}});
        (void)write_all(response, nullptr);
    };

    // HELLO must succeed within the I/O timeout of the connection's start
    // (after a TLS handshake): before it, a connection has proved nothing,
    // so it cannot hold a worker longer than that.
    const auto handshake_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_io_timeout_ms);
    // A slow client, not a malformed request: told why, then closed.
    auto refuse_late_hello = [&]() {
        (void)write_all(Protocol::Error("PROTOCOL", "HELLO 3 was not completed within the I/O timeout"), nullptr);
        LogConnectionTermination(ConnectionTermination{
            .should_log = true,
            .phase = "handshake",
            .reason = "timeout",
            .error = "HELLO 3 was not completed within the I/O timeout",
        });
    };
    while (running_.load()) {
        bool has_line = false;
        if (!session.greeted && std::chrono::steady_clock::now() >= handshake_deadline) {
            refuse_late_hello();
            break;
        }
        // Whether the HELLO deadline, not the idle timeout, ends this wait.
        bool waiting_on_hello_deadline = false;
        try {
            // Only a greeted connection may idle for the idle timeout; before
            // HELLO succeeds every wait is bounded by the I/O timeout.
            const bool idle = pending_buffer.empty() && session.greeted;
            std::size_t wait_ms = idle ? config_.idle_connection_timeout_ms : config_.client_io_timeout_ms;
            if (!session.greeted) {
                if (pending_buffer.empty()) {
                    wait_ms = std::min(config_.idle_connection_timeout_ms, config_.client_io_timeout_ms);
                }
                const auto left = std::chrono::ceil<std::chrono::milliseconds>(
                    handshake_deadline - std::chrono::steady_clock::now());
                const auto left_ms = static_cast<std::size_t>(std::max<std::int64_t>(left.count(), 1));
                waiting_on_hello_deadline = !pending_buffer.empty() || left_ms <= wait_ms;
                wait_ms = std::min(wait_ms, left_ms);
                // A line begun before the deadline must also end by it.
                if (!request_line_deadline.has_value() || *request_line_deadline > handshake_deadline) {
                    request_line_deadline = handshake_deadline;
                }
            }
            if (!set_recv_timeout(
                    wait_ms,
                    idle ? "idle" : (pending_buffer.empty() ? "handshake_wait" : "partial_request"))) {
                break;
            }
            has_line = read_line(line);
        } catch (const std::exception& e) {
            reject_and_close(Protocol::Error("BAD_REQUEST", e.what()), e.what());
            break;
        }

        if (!has_line) {
            if (!session.greeted && termination.reason == "timeout" &&
                (waiting_on_hello_deadline || std::chrono::steady_clock::now() >= handshake_deadline)) {
                refuse_late_hello();
            } else {
                LogConnectionTermination(termination);
            }
            break;
        }

        const auto payload_request = engine_->PlanPayload(session, line);
        if (payload_request.plan == CommandEngine::PayloadPlan::kReject) {
            reject_and_close(payload_request.reject_response, "rejected parameter frames");
            break;
        }
        std::vector<std::optional<std::string>> parameters;
        if (payload_request.plan == CommandEngine::PayloadPlan::kParameters) {
            // Each frame is `$<length>` (or `$-1` for NULL), the bytes and an
            // empty line; a length above what the parameter's column holds
            // is refused unread.
            bool frames_ok = true;
            try {
                if (!set_recv_timeout(config_.client_io_timeout_ms, "partial_request")) {
                    break;
                }
                parameters.reserve(payload_request.parameter_limits.size());
                for (std::size_t i = 0; frames_ok && i < payload_request.parameter_limits.size(); ++i) {
                    std::string header;
                    frames_ok = read_line(header);
                    if (!frames_ok) {
                        break;
                    }
                    const auto length = Protocol::ParseFrameHeader(header);
                    if (!length.has_value()) {
                        parameters.emplace_back(std::nullopt);
                        continue;
                    }
                    const std::size_t limit = payload_request.parameter_limits[i];
                    if (*length > limit) {
                        throw std::invalid_argument(
                            "$" + std::to_string(i + 1) + " is longer than its column holds (" +
                            std::to_string(limit) + " bytes)");
                    }
                    std::string value;
                    frames_ok = read_bytes(value, *length);
                    if (frames_ok) {
                        std::string terminator;
                        frames_ok = read_line(terminator);
                        if (frames_ok && terminator != "\r\n" && terminator != "\n") {
                            throw std::invalid_argument("a parameter must be followed by an empty line");
                        }
                    }
                    parameters.emplace_back(std::move(value));
                }
            } catch (const std::exception& e) {
                reject_and_close(Protocol::Error("BAD_REQUEST", e.what()), e.what());
                break;
            }
            if (!frames_ok) {
                LogConnectionTermination(termination);
                break;
            }
        }

        const std::string response = engine_->Execute(session, line, parameters);
        if (session.greeted) {
            handshake_slot.Release();
        }
        const PhaseDeadline reply_write_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.client_io_timeout_ms);
#ifdef CHUNKDB_WITH_OPENSSL
        const bool write_ok = config_.tls_enabled
                                  ? WriteAllTls(
                                        tls_session,
                                        response.data(),
                                        response.size(),
                                        reply_write_deadline,
                                        &termination)
                                  : WriteAllPlain(
                                        static_cast<SocketHandle>(client_socket),
                                        response.data(),
                                        response.size(),
                                        reply_write_deadline,
                                        &termination);
#else
        const bool write_ok =
            WriteAllPlain(
                static_cast<SocketHandle>(client_socket),
                response.data(),
                response.size(),
                reply_write_deadline,
                &termination);
#endif
        if (!write_ok) {
            LogConnectionTermination(termination);
            break;
        }

        if (session.close_after_reply) {
            break;
        }
    }

#ifdef CHUNKDB_WITH_OPENSSL
    if (tls_session != nullptr) {
        const int shutdown_result = SSL_shutdown(tls_session);
        if (shutdown_result < 0) {
            termination = ClassifyTlsFailure(tls_session, shutdown_result, "shutdown", false);
            LogConnectionTermination(termination);
        }
        SSL_free(tls_session);
    }
#endif
    CloseSocket(static_cast<SocketHandle>(client_socket));
}

}  // namespace chunkdb
