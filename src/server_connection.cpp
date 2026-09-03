#include "chunkdb/server.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"

#include "server_socket.hpp"
#include "server_io.hpp"
#include "server_tls.hpp"

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
    // longer be trusted (oversized line, rejected payload header, bad payload
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

    while (running_.load()) {
        bool has_line = false;
        try {
            if (!set_recv_timeout(
                    pending_buffer.empty() ? config_.idle_connection_timeout_ms : config_.client_io_timeout_ms,
                    pending_buffer.empty() ? "idle" : "partial_request")) {
                break;
            }
            has_line = read_line(line);
        } catch (const std::exception& e) {
            reject_and_close(Protocol::Error("BAD_REQUEST", e.what()), e.what());
            break;
        }

        if (!has_line) {
            LogConnectionTermination(termination);
            break;
        }

        std::string payload;
        const auto payload_request = engine_->PlanPayload(session, line);
        if (payload_request.plan == CommandEngine::PayloadPlan::kReject) {
            reject_and_close(payload_request.reject_response, "rejected payload header");
            break;
        }
        if (payload_request.plan == CommandEngine::PayloadPlan::kRead) {
            bool payload_ok = false;
            try {
                if (!set_recv_timeout(config_.client_io_timeout_ms, "partial_request")) {
                    break;
                }
                payload_ok = read_bytes(payload, payload_request.bytes);
                if (payload_ok) {
                    std::string terminator;
                    payload_ok = read_line(terminator);
                    if (payload_ok && terminator != "\r\n" && terminator != "\n") {
                        throw std::runtime_error("payload must be followed by an empty line");
                    }
                }
            } catch (const std::exception& e) {
                reject_and_close(Protocol::Error("BAD_REQUEST", e.what()), e.what());
                break;
            }
            if (!payload_ok) {
                LogConnectionTermination(termination);
                break;
            }
        }

        const std::string response = engine_->Execute(session, line, payload);
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
