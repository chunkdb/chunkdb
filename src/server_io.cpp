#include "server_io.hpp"

#include "server_socket.hpp"

#include <chrono>
#include <cstddef>
#include <string>

#include "chunkdb/protocol.hpp"

namespace chunkdb {
namespace server_detail {

bool WriteAllPlain(
    SocketHandle socket_fd,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination) {
    const bool deadline_aware_write = absolute_deadline.has_value();
    // A write with a deadline must not block past it. On Linux, send with
    // MSG_DONTWAIT is non-blocking by itself and the socket keeps its mode, so
    // a reply costs no fcntl calls and waits for writability only when the
    // socket buffer is full. macOS defines MSG_DONTWAIT but its send still
    // blocks on a blocking socket, so there and on Windows the socket is
    // switched to non-blocking mode for the write and back afterwards.
#if defined(__linux__) && defined(MSG_DONTWAIT)
    constexpr bool kPerCallNonBlocking = true;
#else
    constexpr bool kPerCallNonBlocking = false;
#endif
    const bool switch_mode = deadline_aware_write && !kPerCallNonBlocking;
    if (switch_mode) {
        std::string nonblocking_error;
        if (!SetSocketNonBlocking(socket_fd, true, &nonblocking_error)) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "write";
                termination->reason = "socket_error";
                termination->error =
                    "failed to enable nonblocking write mode: " + nonblocking_error;
            }
            return false;
        }
    }

    std::size_t written = 0;
    // With per-call non-blocking sends the first attempt needs no wait.
    bool wait_before_send = !kPerCallNonBlocking;
    while (written < size) {
        if (deadline_aware_write) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= *absolute_deadline) {
                if (termination != nullptr) {
                    *termination = MakePhaseDeadlineTermination(
                        "write",
                        "reply write deadline exceeded");
                }
                return false;
            }

            if (wait_before_send) {
                int socket_error_code = 0;
                const auto wait_result = WaitForSocketReady(
                    socket_fd,
                    false,
                    true,
                    std::chrono::duration_cast<std::chrono::milliseconds>(*absolute_deadline - now),
                    &socket_error_code);
                if (wait_result == SocketWaitResult::kTimeout) {
                    if (termination != nullptr) {
                        *termination = MakePhaseDeadlineTermination(
                            "write",
                            "reply write deadline exceeded");
                    }
                    return false;
                }
                if (wait_result == SocketWaitResult::kError) {
                    if (termination != nullptr) {
                        *termination = MakeSocketTermination("write", socket_error_code, true);
                    }
                    return false;
                }
            }
        }

#ifdef _WIN32
        const int result = send(
            socket_fd,
            data + static_cast<int>(written),
            static_cast<int>(size - written),
            0);
#else
#if defined(MSG_NOSIGNAL)
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
#if defined(__linux__) && defined(MSG_DONTWAIT)
        const int send_flags = deadline_aware_write ? (kSendFlags | MSG_DONTWAIT) : kSendFlags;
#else
        const int send_flags = kSendFlags;
#endif
        const ssize_t result = send(
            socket_fd,
            data + written,
            size - written,
            send_flags);
#endif
        if (result <= 0) {
            const int socket_error_code = result < 0 ? CurrentSocketErrorCode() : 0;
            if (result < 0 && IsSocketInterruptedError(socket_error_code)) {
                continue;
            }
            if (deadline_aware_write && result < 0 && IsSocketTimeoutError(socket_error_code)) {
                // The buffer is full: wait for room before the next attempt.
                wait_before_send = true;
                continue;
            }
            if (termination != nullptr) {
                if (result == 0) {
                    termination->should_log = true;
                    termination->phase = "write";
                    termination->reason = "peer_close";
                    termination->error = "send returned 0";
                } else {
                    *termination = MakeSocketTermination("write", socket_error_code, true);
                }
            }
            return false;
        }
        written += static_cast<std::size_t>(result);
        // A short send means the buffer is full, so wait before the next one.
        wait_before_send = !kPerCallNonBlocking || written < size;
        if (written < size && absolute_deadline.has_value() &&
            std::chrono::steady_clock::now() >= *absolute_deadline) {
            if (termination != nullptr) {
                *termination = MakePhaseDeadlineTermination("write", "reply write deadline exceeded");
            }
            return false;
        }
    }

    if (switch_mode) {
        std::string nonblocking_error;
        if (!SetSocketNonBlocking(socket_fd, false, &nonblocking_error)) {
            if (termination != nullptr) {
                termination->should_log = true;
                termination->phase = "write";
                termination->reason = "socket_error";
                termination->error =
                    "failed to restore blocking write mode: " + nonblocking_error;
            }
            return false;
        }
    }
    return true;
}

void SendPlainBusyResponse(SocketHandle client_socket, std::size_t timeout_ms, std::string_view reason) {
    const std::string response = Protocol::Error("BUSY", std::string(reason));
    const PhaseDeadline deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    (void)WriteAllPlain(client_socket, response.data(), response.size(), deadline, nullptr);
}

}  // namespace server_detail
}  // namespace chunkdb
