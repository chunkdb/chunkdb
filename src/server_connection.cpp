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
#include "server_feed.hpp"
#include "source_address.hpp"

namespace chunkdb {

using namespace server_detail;

bool ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
    , ServerConnection& connection
) {
    auto session = std::move(connection.session);
    struct SlotCleanup {
        SessionState& session;
        ~SlotCleanup() { if (session.slot_watch) session.slot_watch->Cancel(); }
    } slot_cleanup{session};
    session.remote_address = PeerAddressForSocket(static_cast<SocketHandle>(client_socket));
        session.watch_options.buffer_bytes = config_.feed_buffer_bytes;
        session.watch_options.notify = [weak = std::weak_ptr<FeedIo>(FeedIoHandle())] {
            if (auto io = weak.lock()) io->Wake();
        };
        session.register_slot_watch = [weak = std::weak_ptr<FeedIo>(FeedIoHandle())](std::shared_ptr<SlotWatch> state) {
            if (auto io = weak.lock()) return io->RegisterSlot(std::move(state));
            return false;
        };


    // Until HELLO succeeds a connection has proved nothing: with
    // max_handshakes_per_ip, one source may hold only that many workers in
    // that state. The slot is released at HELLO or when the connection ends.
    constexpr std::string_view kTooManyHandshakes = "too many connections before HELLO from this address";
    const std::string source = SourceAddressKey(session.remote_address);
    const bool limited = !session.greeted && config_.max_handshakes_per_ip != 0;
    if (limited && !TryAcquireHandshake(source)) {
        engine_->metrics()->CountConnectionRejected();
#ifdef CHUNKDB_WITH_OPENSSL
        if (!config_.tls_enabled) {
            SendPlainBusyResponse(static_cast<SocketHandle>(client_socket), config_.client_io_timeout_ms, kTooManyHandshakes);
        }
#else
        SendPlainBusyResponse(static_cast<SocketHandle>(client_socket), config_.client_io_timeout_ms, kTooManyHandshakes);
#endif

        if (!handshake_limit_warned_.exchange(true, std::memory_order_relaxed)) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kServer,
                "connections before HELLO from one source reached the limit; rejecting more",
                {{"source", source}, {"max_handshakes_per_ip", std::to_string(config_.max_handshakes_per_ip)}});
        }
        return false;
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
    auto pending_buffer = std::move(connection.pending);
    PhaseDeadline request_line_deadline;
    ConnectionTermination termination;
    std::optional<std::size_t> current_recv_timeout_ms;
    const auto socket_text = std::to_string(client_socket);
    const auto peer_endpoint = PeerEndpointForSocket(static_cast<SocketHandle>(client_socket));
    std::string last_command;
    const auto log_termination = [&](const ConnectionTermination& ended) {
        if (!ended.should_log) return;
        LogMessage(LogLevel::kWarn, LogComponent::kServer, "connection terminated", {
            {"phase", ended.phase}, {"reason", ended.reason}, {"error", ended.error},
            {"socket", socket_text}, {"peer", peer_endpoint}, {"last_command", last_command},
            {"recv_timeout_ms", current_recv_timeout_ms ? std::to_string(*current_recv_timeout_ms) : "unset"},
        });
    };

    auto set_recv_timeout = [&](std::size_t timeout_ms, std::string_view phase) -> bool {
        if (current_recv_timeout_ms.has_value() && *current_recv_timeout_ms == timeout_ms) {
            return true;
        }
        std::string timeout_error;
        int timeout_code = 0;
        const bool recv_timeout_ok =
            !ConsumeTestFailureBudget(&g_test_recv_timeout_config_failures) &&
            ConfigureSocketRecvTimeout(static_cast<SocketHandle>(client_socket), timeout_ms, &timeout_error, &timeout_code);
        if (!recv_timeout_ok) {
            if (SocketTimeoutFailureIsPeerClose(static_cast<SocketHandle>(client_socket), timeout_code)) return false;
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
                    {"socket", socket_text}, {"peer", peer_endpoint}, {"last_command", last_command},
                });
            return false;
        }
        current_recv_timeout_ms = timeout_ms;
        return true;
    };

#ifdef CHUNKDB_WITH_OPENSSL
    SSL*& tls_session = connection.tls;
    if (config_.tls_enabled && tls_session == nullptr) {
        tls_session = SSL_new(tls_context_);
        if (tls_session == nullptr) {

            return false;
        }

        std::string nonblocking_error;
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(client_socket), true, &nonblocking_error)) {
            termination.should_log = true;
            termination.phase = "handshake";
            termination.reason = "socket_error";
            termination.error = "failed to enable nonblocking handshake mode: " + nonblocking_error;
            log_termination(termination);


            return false;
        }
        SSL_set_fd(tls_session, static_cast<int>(client_socket));
        if (!CompleteTlsHandshake(
                tls_session,
                static_cast<SocketHandle>(client_socket),
                config_.client_io_timeout_ms,
                &termination)) {
            log_termination(termination);


            return false;
        }
        if (!SetSocketNonBlocking(static_cast<SocketHandle>(client_socket), false, &nonblocking_error)) {
            termination.should_log = true;
            termination.phase = "handshake";
            termination.reason = "socket_error";
            termination.error = "failed to restore blocking TLS socket mode: " + nonblocking_error;
            log_termination(termination);


            return false;
        }
        if (!set_recv_timeout(config_.idle_connection_timeout_ms, "idle")) {


            return false;
        }
    }
#endif

    { std::lock_guard lock(lifecycle_mutex_); session.backup_cancelled = backup_stop_.get_token(); }

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
        log_termination(ConnectionTermination{
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
                log_termination(termination);
            }
            break;
        }

        // Record only the command verb, never credentials or parameter values.
        last_command = line.substr(0U, std::min<std::size_t>(line.find_first_of(" \t\r\n"), 32U));
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
                log_termination(termination);
                break;
            }
        }

        std::string response = engine_->Execute(session, line, parameters);
        if (session.watch || session.slot_watch) {
            auto count = watch_count_.load();
            while (count < config_.max_watches && !watch_count_.compare_exchange_weak(count, count + 1U)) {}
            if (count >= config_.max_watches) {
                session.watch.reset();
                if (session.slot_watch) session.slot_watch->Cancel();
                session.slot_watch.reset();
                response = Protocol::Error("BUSY", "maximum watches reached");
            } else connection.watch_slot = true;
        }
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
            log_termination(termination);
            break;
        }

        if (session.watch || session.slot_watch) {
            connection.session = std::move(session);
            connection.pending = std::move(pending_buffer);
            return HandOff(connection);
        }

        if (session.close_after_reply) {
            break;
        }
    }

#ifdef CHUNKDB_WITH_OPENSSL
    if (tls_session != nullptr) {
        const int result = SSL_shutdown(tls_session);
        if (result < 0) log_termination(ClassifyTlsFailure(tls_session, result, "shutdown", false));
    }
#endif
    return false;
}

}  // namespace chunkdb
