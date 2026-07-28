#pragma once

#include "server_socket.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace chunkdb {
namespace server_detail {

struct PendingLineBuffer {
    std::string bytes;
    std::size_t cursor = 0;

    [[nodiscard]] bool empty() const noexcept { return cursor >= bytes.size(); }

    [[nodiscard]] std::size_t unconsumed_size() const noexcept {
        return cursor >= bytes.size() ? 0 : (bytes.size() - cursor);
    }

    void append(const char* data, std::size_t size) {
        if (size == 0) {
            return;
        }
        maybe_compact();
        bytes.append(data, size);
    }

    bool extract_line(std::string* out, std::size_t max_line_bytes) {
        const auto new_line = bytes.find('\n', cursor);
        if (new_line == std::string::npos) {
            return false;
        }

        const std::size_t line_size = new_line - cursor + 1;
        if (line_size > max_line_bytes) {
            throw std::runtime_error("request line exceeds max_line_bytes");
        }
        out->assign(bytes.data() + cursor, line_size);
        cursor = new_line + 1;
        reset_if_empty();
        maybe_compact();
        return true;
    }

    bool take_tail_on_close(std::string* out, std::size_t max_line_bytes) {
        if (empty()) {
            return false;
        }
        const std::size_t tail_size = bytes.size() - cursor;
        if (tail_size > max_line_bytes) {
            throw std::runtime_error("request line exceeds max_line_bytes");
        }
        out->assign(bytes.data() + cursor, tail_size);
        bytes.clear();
        cursor = 0;
        return true;
    }

    void enforce_partial_line_limit(std::size_t max_line_bytes) const {
        if (unconsumed_size() > max_line_bytes) {
            const auto new_line = bytes.find('\n', cursor);
            if (new_line == std::string::npos || (new_line - cursor + 1) > max_line_bytes) {
                throw std::runtime_error("request line exceeds max_line_bytes");
            }
        }
    }

  private:
    void reset_if_empty() {
        if (cursor == bytes.size()) {
            bytes.clear();
            cursor = 0;
        }
    }

    void maybe_compact() {
        if (cursor == 0) {
            return;
        }
        if (cursor < 4096 && cursor * 2 < bytes.size()) {
            return;
        }
        bytes.erase(0, cursor);
        cursor = 0;
    }
};

bool WriteAllPlain(
    SocketHandle socket_fd,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination);

void SendPlainBusyResponse(SocketHandle client_socket, std::size_t timeout_ms);

template <typename EnsureRecvTimeoutFn>
bool ReadLinePlain(
    SocketHandle socket_fd,
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
#ifdef _WIN32
        const int read = recv(socket_fd, buffer.data(), static_cast<int>(buffer.size()), 0);
#else
        const ssize_t read = recv(socket_fd, buffer.data(), buffer.size(), 0);
#endif
        if (read == 0) {
            if (pending.empty()) {
                if (termination != nullptr) {
                    termination->phase = "read";
                    termination->reason = "peer_close";
                    termination->error = "peer closed connection";
                    termination->should_log = false;
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
            const int socket_error_code = CurrentSocketErrorCode();
            if (IsSocketInterruptedError(socket_error_code)) {
                continue;
            }
            if (termination != nullptr) {
                *termination = MakeSocketTermination("read", socket_error_code, true);
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
