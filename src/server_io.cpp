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
    if (deadline_aware_write) {
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

#ifdef _WIN32
        const int result = send(
            socket_fd,
            data + static_cast<int>(written),
            static_cast<int>(size - written),
            0);
#else
        const ssize_t result = send(
            socket_fd,
            data + written,
            size - written,
            0);
#endif
        if (result <= 0) {
            const int socket_error_code = result < 0 ? CurrentSocketErrorCode() : 0;
            if (result < 0 && IsSocketInterruptedError(socket_error_code)) {
                continue;
            }
            if (deadline_aware_write && result < 0 && IsSocketTimeoutError(socket_error_code)) {
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

void SendPlainBusyResponse(SocketHandle client_socket, std::size_t timeout_ms) {
    const std::string response = Protocol::Error("BUSY", "pending client queue full");
    const PhaseDeadline deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    (void)WriteAllPlain(client_socket, response.data(), response.size(), deadline, nullptr);
}

}  // namespace server_detail
}  // namespace chunkdb
