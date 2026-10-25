#pragma once

#include "server_socket.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <optional>
#include <span>
#include <vector>

#include "chunkdb/protocol.hpp"

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

    // Moves up to `max_bytes` already-buffered bytes into `out` (appending)
    // and returns how many were moved. Used for raw payloads that follow a
    // request line, which are not subject to the line-length limit.
    std::size_t extract_bytes(std::size_t max_bytes, std::string* out) {
        const std::size_t take = std::min(max_bytes, unconsumed_size());
        if (take == 0) {
            return 0;
        }
        out->append(bytes.data() + cursor, take);
        cursor += take;
        reset_if_empty();
        maybe_compact();
        return take;
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

// What a request's parameter frames may hold, from PlanPayload.
struct ParameterFrameRules {
    // Frames to read.
    std::size_t frame_count = 0;
    // Per-frame byte bound of kept frames.
    std::span<const std::size_t> limits{};
    // Payload bytes of all frames together.
    std::size_t total_limit = std::numeric_limits<std::size_t>::max();
    // Read every frame and keep none, for a statement refused after its frames.
    bool discard = false;
    // Per frame: read without keeping it (an unknown HELLO option value).
    const std::vector<bool>* opaque = nullptr;
    // Per frame: the reply when the frame is longer than its limit; the
    // frame is then drained instead of refused.
    const std::vector<std::string>* rejections = nullptr;
    // Bytes of all frames read without keeping them.
    std::size_t opaque_limit = std::numeric_limits<std::size_t>::max();
};

// The request parameter framing used after PlanPayload: header, raw bytes,
// then an empty LF/CRLF line; NULL is only its $-1 header. Callback readers
// retain the connection's existing buffered bytes and partial deadlines.
// The payload bytes of all frames together may not exceed `total_limit`;
// the check precedes the read, so nothing is allocated or consumed for the
// frame that crosses it. Then `*total_exceeded` is set and true is returned.
// A frame read without keeping it is consumed in fixed-size pieces and kept
// as NULL; `*rejection` receives the reply of an oversized option frame.
template <typename ReadLineFn, typename ReadBytesFn>
bool ReadParameterFrames(
    const ParameterFrameRules& rules,
    std::vector<std::optional<std::string>>* parameters,
    ReadLineFn&& read_line,
    ReadBytesFn&& read_bytes,
    bool* total_exceeded,
    std::string* rejection) {
    bool frames_ok = true;
    std::size_t total = 0;
    std::size_t opaque_total = 0;
    *total_exceeded = false;
    if (!rules.discard) parameters->reserve(rules.frame_count);
    for (std::size_t i = 0; frames_ok && i < rules.frame_count; ++i) {
        std::string header;
        frames_ok = read_line(header);
        if (!frames_ok) break;
        const auto length = Protocol::ParseFrameHeader(header);
        if (!length.has_value()) {
            if (!rules.discard) parameters->emplace_back(std::nullopt);
            continue;
        }
        const bool option_too_long = !rules.discard && *length > rules.limits[i] && rules.rejections != nullptr &&
            !rules.rejections->empty() && !(*rules.rejections)[i].empty();
        if (option_too_long) *rejection = (*rules.rejections)[i];
        const bool opaque = rules.discard || option_too_long ||
            (rules.opaque != nullptr && !rules.opaque->empty() && (*rules.opaque)[i]);
        if (opaque && *length > rules.opaque_limit - opaque_total) {
            throw std::invalid_argument("unparsed parameter bytes exceed the configured limit");
        }
        if (opaque) opaque_total += *length;
        if (!opaque && *length > rules.limits[i]) {
            throw std::invalid_argument(
                "$" + std::to_string(i + 1) + " is longer than its column holds (" +
                std::to_string(rules.limits[i]) + " bytes)");
        }
        // Compare before adding, so the sum cannot wrap.
        if (*length > rules.total_limit - total) {
            *total_exceeded = true;
            break;
        }
        total += *length;
        std::string value;
        if (opaque) {
            // Fixed scratch bound, regardless of the declared frame length.
            std::size_t remaining = *length;
            while (frames_ok && remaining != 0U) {
                const auto amount = std::min<std::size_t>(remaining, 4096U);
                frames_ok = read_bytes(value, amount);
                remaining -= amount;
            }
        } else {
            frames_ok = read_bytes(value, *length);
        }
        if (frames_ok) {
            std::string terminator;
            frames_ok = read_line(terminator);
            if (frames_ok && terminator != "\r\n" && terminator != "\n") {
                throw std::invalid_argument("a parameter must be followed by an empty line");
            }
        }
        if (!rules.discard) {
            if (opaque) parameters->emplace_back(std::nullopt);
            else parameters->emplace_back(std::move(value));
        }
    }
    return frames_ok;
}

bool WriteAllPlain(
    SocketHandle socket_fd,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination);

void SendPlainBusyResponse(
    SocketHandle client_socket,
    std::size_t timeout_ms,
    std::string_view reason = "pending client queue full");

// The receive wait for the next piece of a request: `fallback_ms`, cut to
// end at `deadline` when one is set (rounded up, at least 1 ms), so a request
// cannot outlast its deadline by a whole wait.
inline std::size_t ReadWaitMs(const PhaseDeadline* deadline, std::size_t fallback_ms) {
    if (deadline == nullptr || !deadline->has_value()) {
        return fallback_ms;
    }
    const auto left = std::chrono::ceil<std::chrono::milliseconds>(**deadline - std::chrono::steady_clock::now());
    return std::min<std::size_t>(fallback_ms, static_cast<std::size_t>(std::max<std::int64_t>(left.count(), 1)));
}

// A receive that timed out: past the deadline it is reported as the deadline
// the caller set, otherwise as the socket timeout it is.
inline ConnectionTermination MakeReadTimeoutTermination(
    const PhaseDeadline* deadline,
    int socket_error_code,
    std::string_view deadline_detail) {
    if (IsSocketTimeoutError(socket_error_code) && deadline != nullptr && deadline->has_value() &&
        std::chrono::steady_clock::now() >= **deadline) {
        return MakePhaseDeadlineTermination("read", deadline_detail);
    }
    return MakeSocketTermination("read", socket_error_code, true);
}

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
        // Inside a line, each wait ends by the line's deadline.
        if (!pending.empty() &&
            !ensure_recv_timeout(ReadWaitMs(absolute_deadline, partial_timeout_ms), "partial_request")) {
            return false;
        }
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
            // A line without its terminator is never executed: the peer may
            // have been cut off in the middle of it ("TABLEDROP t" of
            // "TABLEDROP t2").
            if (termination != nullptr) {
                termination->phase = "read";
                termination->reason = "peer_close";
                termination->error = "peer closed connection inside a request line; it was not executed";
                termination->should_log = true;
            }
            return false;
        }
        if (read < 0) {
            const int socket_error_code = CurrentSocketErrorCode();
            if (IsSocketInterruptedError(socket_error_code)) {
                continue;
            }
            if (termination != nullptr) {
                *termination = MakeReadTimeoutTermination(
                    absolute_deadline, socket_error_code, "request line deadline exceeded");
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

    }
}

// Reads exactly `total` raw bytes (a CHUNKPUT payload) into `out`, first
// draining anything already buffered from the request line read. Unlike
// ReadLinePlain this never applies max_line_bytes: the caller has already
// bounded `total` through CommandEngine::PlanPayload.
template <typename EnsureRecvTimeoutFn>
bool ReadBytesPlain(
    SocketHandle socket_fd,
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
        if (!ensure_recv_timeout(ReadWaitMs(absolute_deadline, partial_timeout_ms), "partial_request")) {
            return false;
        }
#ifdef _WIN32
        const int read = recv(socket_fd, buffer.data(), static_cast<int>(buffer.size()), 0);
#else
        const ssize_t read = recv(socket_fd, buffer.data(), buffer.size(), 0);
#endif
        if (read == 0) {
            if (termination != nullptr) {
                termination->phase = "read";
                termination->reason = "peer_close";
                termination->error = "peer closed connection inside a payload";
                termination->should_log = true;
            }
            return false;
        }
        if (read < 0) {
            const int socket_error_code = CurrentSocketErrorCode();
            if (IsSocketInterruptedError(socket_error_code)) {
                continue;
            }
            if (termination != nullptr) {
                *termination =
                    MakeReadTimeoutTermination(absolute_deadline, socket_error_code, "payload deadline exceeded");
            }
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

    }

    if (absolute_deadline != nullptr) {
        *absolute_deadline = std::nullopt;
    }
    return true;
}

}  // namespace server_detail
}  // namespace chunkdb
