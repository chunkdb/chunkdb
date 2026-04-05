#include "chunkdb/server.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>

#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <unistd.h>
#endif

#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

namespace chunkdb {

namespace {

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
constexpr SocketHandle kInvalidSocket = -1;
#endif

struct ConnectionTermination {
    bool should_log = false;
    std::string phase;
    std::string reason;
    std::string error;
};

using PhaseDeadline = std::optional<std::chrono::steady_clock::time_point>;

std::atomic<std::size_t> g_test_send_timeout_config_failures{0};
std::atomic<std::size_t> g_test_recv_timeout_config_failures{0};
std::atomic<std::uint64_t> g_test_recv_timeout_config_calls{0};

bool ConsumeTestFailureBudget(std::atomic<std::size_t>* counter) {
    if (counter == nullptr) {
        return false;
    }
    std::size_t current = counter->load(std::memory_order_relaxed);
    while (current > 0) {
        if (counter->compare_exchange_weak(
                current,
                current - 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

int CurrentSocketErrorCode() {
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

std::string FormatSocketError(int code) {
#ifdef _WIN32
    return "wsa=" + std::to_string(code);
#else
    return "errno=" + std::to_string(code) + " msg='" + std::strerror(code) + "'";
#endif
}

bool IsSocketTimeoutError(int code) {
#ifdef _WIN32
    return code == WSAETIMEDOUT || code == WSAEWOULDBLOCK;
#else
    return code == EAGAIN || code == EWOULDBLOCK;
#endif
}

bool IsSocketPeerCloseError(int code) {
#ifdef _WIN32
    return code == WSAECONNRESET || code == WSAECONNABORTED || code == WSAESHUTDOWN ||
           code == WSAENOTCONN;
#else
    return code == ECONNRESET || code == EPIPE || code == ENOTCONN;
#endif
}

ConnectionTermination MakeSocketTermination(
    std::string_view phase,
    int socket_error_code,
    bool log_peer_close) {
    ConnectionTermination termination;
    termination.phase = std::string(phase);
    termination.error = FormatSocketError(socket_error_code);
    termination.should_log = true;
    if (IsSocketTimeoutError(socket_error_code)) {
        termination.reason = "timeout";
        return termination;
    }
    if (IsSocketPeerCloseError(socket_error_code)) {
        termination.reason = "peer_close";
        termination.should_log = log_peer_close;
        return termination;
    }
    termination.reason = "socket_error";
    return termination;
}

void LogConnectionTermination(const ConnectionTermination& termination) {
    if (!termination.should_log) {
        return;
    }
    LogMessage(
        LogLevel::kWarn,
        LogComponent::kServer,
        "connection terminated",
        {
            {"phase", termination.phase},
            {"reason", termination.reason},
            {"error", termination.error.empty() ? "unknown" : termination.error},
        });
}

ConnectionTermination MakePhaseDeadlineTermination(
    std::string_view phase,
    std::string_view detail) {
    ConnectionTermination termination;
    termination.should_log = true;
    termination.phase = std::string(phase);
    termination.reason = "timeout";
    termination.error = std::string(detail);
    return termination;
}

void CloseSocket(SocketHandle socket_fd) {
#ifdef _WIN32
    closesocket(socket_fd);
#else
    close(socket_fd);
#endif
}

void ShutdownSocket(SocketHandle socket_fd) {
#ifdef _WIN32
    shutdown(socket_fd, SD_BOTH);
#else
    shutdown(socket_fd, SHUT_RDWR);
#endif
}

bool SetSocketNonBlocking(
    SocketHandle socket_fd,
    bool enabled,
    std::string* error);

enum class SocketWaitResult {
    kReady,
    kTimeout,
    kError,
};

SocketWaitResult WaitForSocketReady(
    SocketHandle socket_fd,
    bool want_read,
    bool want_write,
    std::chrono::milliseconds timeout,
    int* socket_error_code);

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

std::string SocketErrorText() {
#ifdef _WIN32
    const int code = WSAGetLastError();
    return "wsa=" + std::to_string(code);
#else
    return "errno=" + std::to_string(errno) + " msg='" + std::strerror(errno) + "'";
#endif
}

bool EnableTcpNoDelay(SocketHandle socket_fd, std::string* error) {
    int value = 1;
#ifdef _WIN32
    const int rc = setsockopt(
        socket_fd,
        IPPROTO_TCP,
        TCP_NODELAY,
        reinterpret_cast<const char*>(&value),
        static_cast<int>(sizeof(value)));
#else
    const int rc = setsockopt(
        socket_fd,
        IPPROTO_TCP,
        TCP_NODELAY,
        &value,
        static_cast<socklen_t>(sizeof(value)));
#endif
    if (rc == 0) {
        return true;
    }
    if (error != nullptr) {
        *error = SocketErrorText();
    }
    return false;
}

bool ConfigureSocketTimeout(
    SocketHandle socket_fd,
    int option_name,
    std::size_t timeout_ms,
    std::string* error) {
#ifdef _WIN32
    const DWORD timeout = static_cast<DWORD>(
        std::min<std::size_t>(timeout_ms, static_cast<std::size_t>(std::numeric_limits<DWORD>::max())));
    if (setsockopt(
            socket_fd,
            SOL_SOCKET,
            option_name,
            reinterpret_cast<const char*>(&timeout),
            static_cast<int>(sizeof(timeout))) != 0) {
        if (error != nullptr) {
            *error = std::string(option_name == SO_RCVTIMEO ? "SO_RCVTIMEO " : "SO_SNDTIMEO ") +
                     SocketErrorText();
        }
        return false;
    }
#else
    timeval timeout{};
    timeout.tv_sec = static_cast<decltype(timeout.tv_sec)>(timeout_ms / 1000);
    timeout.tv_usec = static_cast<decltype(timeout.tv_usec)>((timeout_ms % 1000) * 1000);
    if (setsockopt(socket_fd, SOL_SOCKET, option_name, &timeout, sizeof(timeout)) != 0) {
        if (error != nullptr) {
            *error = std::string(option_name == SO_RCVTIMEO ? "SO_RCVTIMEO " : "SO_SNDTIMEO ") +
                     SocketErrorText();
        }
        return false;
    }
#endif
    return true;
}

bool ConfigureSocketRecvTimeout(
    SocketHandle socket_fd,
    std::size_t timeout_ms,
    std::string* error) {
    g_test_recv_timeout_config_calls.fetch_add(1, std::memory_order_relaxed);
    return ConfigureSocketTimeout(socket_fd, SO_RCVTIMEO, timeout_ms, error);
}

bool ConfigureSocketSendTimeout(
    SocketHandle socket_fd,
    std::size_t timeout_ms,
    std::string* error) {
    return ConfigureSocketTimeout(socket_fd, SO_SNDTIMEO, timeout_ms, error);
}

bool SetSocketNonBlocking(
    SocketHandle socket_fd,
    bool enabled,
    std::string* error) {
#ifdef _WIN32
    u_long mode = enabled ? 1UL : 0UL;
    if (ioctlsocket(socket_fd, FIONBIO, &mode) != 0) {
        if (error != nullptr) {
            *error = "FIONBIO " + SocketErrorText();
        }
        return false;
    }
#else
    const int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags < 0) {
        if (error != nullptr) {
            *error = "fcntl(F_GETFL) " + SocketErrorText();
        }
        return false;
    }
    const int next_flags = enabled ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (fcntl(socket_fd, F_SETFL, next_flags) != 0) {
        if (error != nullptr) {
            *error = "fcntl(F_SETFL) " + SocketErrorText();
        }
        return false;
    }
#endif
    return true;
}

SocketWaitResult WaitForSocketReady(
    SocketHandle socket_fd,
    bool want_read,
    bool want_write,
    std::chrono::milliseconds timeout,
    int* socket_error_code) {
    if (timeout <= std::chrono::milliseconds::zero()) {
        return SocketWaitResult::kTimeout;
    }

    fd_set read_set;
    fd_set write_set;
    FD_ZERO(&read_set);
    FD_ZERO(&write_set);
    if (want_read) {
        FD_SET(socket_fd, &read_set);
    }
    if (want_write) {
        FD_SET(socket_fd, &write_set);
    }

    timeval wait_time{};
    wait_time.tv_sec = static_cast<decltype(wait_time.tv_sec)>(timeout.count() / 1000);
    wait_time.tv_usec =
        static_cast<decltype(wait_time.tv_usec)>((timeout.count() % 1000) * 1000);

#ifdef _WIN32
    const int ready = select(0, want_read ? &read_set : nullptr, want_write ? &write_set : nullptr, nullptr, &wait_time);
#else
    const int ready = select(
        static_cast<int>(socket_fd) + 1,
        want_read ? &read_set : nullptr,
        want_write ? &write_set : nullptr,
        nullptr,
        &wait_time);
#endif
    if (ready > 0) {
        return SocketWaitResult::kReady;
    }
    if (ready == 0) {
        return SocketWaitResult::kTimeout;
    }
    if (socket_error_code != nullptr) {
        *socket_error_code = CurrentSocketErrorCode();
    }
    return SocketWaitResult::kError;
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
            if (termination != nullptr) {
                *termination = MakeSocketTermination("read", CurrentSocketErrorCode(), true);
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

SocketHandle CreateListenSocket(const std::string& host, std::uint16_t port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo* result = nullptr;
    const std::string port_text = std::to_string(port);

    const int gai = getaddrinfo(host.empty() ? nullptr : host.c_str(), port_text.c_str(), &hints, &result);
    if (gai != 0) {
        throw std::runtime_error("getaddrinfo failed");
    }

    SocketHandle listen_socket = kInvalidSocket;

    for (auto* ai = result; ai != nullptr; ai = ai->ai_next) {
        listen_socket = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (listen_socket == kInvalidSocket) {
            continue;
        }

        int reuse = 1;
#ifdef _WIN32
        setsockopt(
            listen_socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            reinterpret_cast<const char*>(&reuse),
            sizeof(reuse));
#else
        setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
#endif

        if (bind(listen_socket, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0 &&
            listen(listen_socket, SOMAXCONN) == 0) {
            break;
        }

        CloseSocket(listen_socket);
        listen_socket = kInvalidSocket;
    }

    freeaddrinfo(result);

    if (listen_socket == kInvalidSocket) {
        throw std::runtime_error("failed to create listening socket");
    }

    return listen_socket;
}

#ifdef CHUNKDB_WITH_OPENSSL

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

bool WriteAllTls(
    SSL* tls_session,
    const char* data,
    std::size_t size,
    const PhaseDeadline& absolute_deadline,
    ConnectionTermination* termination) {
    std::size_t written = 0;
    while (written < size) {
        const int result = SSL_write(
            tls_session,
            data + written,
            static_cast<int>(size - written));
        if (result <= 0) {
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
    return true;
}

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

#endif

}  // namespace

void SetServerTimeoutConfigFailpointForTests(
    std::size_t send_failures,
    std::size_t recv_failures) noexcept {
    g_test_send_timeout_config_failures.store(send_failures, std::memory_order_relaxed);
    g_test_recv_timeout_config_failures.store(recv_failures, std::memory_order_relaxed);
}

void ResetServerTimeoutConfigCountersForTests() noexcept {
    g_test_recv_timeout_config_calls.store(0, std::memory_order_relaxed);
}

std::uint64_t ServerRecvTimeoutConfigCallsForTests() noexcept {
    return g_test_recv_timeout_config_calls.load(std::memory_order_relaxed);
}

ChunkServer::ChunkServer(ServerConfig config, std::shared_ptr<CommandEngine> engine)
    : config_(std::move(config)),
      engine_(std::move(engine)),
      running_(false),
      listen_socket_(kInvalidSocket)
#ifdef CHUNKDB_WITH_OPENSSL
      ,
      tls_context_(nullptr)
#endif
{
    if (!engine_) {
        throw std::invalid_argument("engine must not be null");
    }
    if (config_.max_line_bytes == 0) {
        throw std::invalid_argument("max_line_bytes must be > 0");
    }
    if (config_.worker_threads == 0) {
        throw std::invalid_argument("worker_threads must be > 0");
    }
    if (config_.client_io_timeout_ms == 0) {
        throw std::invalid_argument("client_io_timeout_ms must be > 0");
    }
    if (config_.idle_connection_timeout_ms == 0) {
        throw std::invalid_argument("idle_connection_timeout_ms must be > 0");
    }
    if (config_.max_pending_clients == 0) {
        throw std::invalid_argument("max_pending_clients must be > 0");
    }

#ifndef CHUNKDB_WITH_OPENSSL
    if (config_.tls_enabled) {
        throw std::invalid_argument("TLS requested but build does not include OpenSSL");
    }
#else
    if (config_.tls_enabled) {
        if (config_.tls_cert_path.empty() || config_.tls_key_path.empty()) {
            throw std::invalid_argument("TLS requires both tls_cert_path and tls_key_path");
        }

        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();

        tls_context_ = BuildTlsContext(config_);
    }
#endif
}

ChunkServer::~ChunkServer() {
    Stop();
#ifdef CHUNKDB_WITH_OPENSSL
    if (tls_context_ != nullptr) {
        SSL_CTX_free(tls_context_);
        tls_context_ = nullptr;
    }
#endif
}

void ChunkServer::StartWorkers() {
    workers_.reserve(config_.worker_threads);
    for (std::size_t i = 0; i < config_.worker_threads; ++i) {
        workers_.emplace_back(&ChunkServer::WorkerLoop, this);
    }
}

void ChunkServer::JoinWorkers() {
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void ChunkServer::Run() {
    const auto run_started = std::chrono::steady_clock::now();
#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kServer,
            "server runtime initialization failed",
            {{"error", "WSAStartup failed"}});
        throw std::runtime_error("WSAStartup failed");
    }
#endif

    try {
        const SocketHandle listen_socket = CreateListenSocket(config_.host, config_.port);
        {
            std::lock_guard lock(lifecycle_mutex_);
            listen_socket_ = static_cast<decltype(listen_socket_)>(listen_socket);
        }
        running_.store(true);

        StartWorkers();
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "ready to accept connections",
            {
                {"protocol", config_.tls_enabled ? "tls" : "tcp"},
                {"host", config_.host},
                {"port", std::to_string(config_.port)},
                {"tls", config_.tls_enabled ? "on" : "off"},
                {"workers", std::to_string(config_.worker_threads)},
            });

        while (running_.load()) {
            fd_set read_set;
            FD_ZERO(&read_set);
            FD_SET(listen_socket, &read_set);

            timeval timeout;
            timeout.tv_sec = 0;
            timeout.tv_usec = 200000;
#ifdef _WIN32
            const int ready = select(0, &read_set, nullptr, nullptr, &timeout);
#else
            const int ready = select(listen_socket + 1, &read_set, nullptr, nullptr, &timeout);
#endif
            if (ready == 0) {
                continue;
            }
            if (ready < 0) {
                if (!running_.load()) {
                    break;
                }
                continue;
            }

            sockaddr_storage client_address;
#ifdef _WIN32
            int client_size = sizeof(client_address);
#else
            socklen_t client_size = sizeof(client_address);
#endif

            SocketHandle client_socket = accept(
                listen_socket,
                reinterpret_cast<sockaddr*>(&client_address),
                &client_size);

            if (client_socket == kInvalidSocket) {
                if (!running_.load()) {
                    break;
                }
                continue;
            }

            std::string nodelay_error;
            if (!EnableTcpNoDelay(client_socket, &nodelay_error)) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "failed to set TCP_NODELAY",
                    {{"error", nodelay_error}});
            }

            std::string timeout_error;
            const bool send_timeout_ok =
                !ConsumeTestFailureBudget(&g_test_send_timeout_config_failures) &&
                ConfigureSocketSendTimeout(client_socket, config_.client_io_timeout_ms, &timeout_error);
            if (!send_timeout_ok) {
                if (timeout_error.empty()) {
                    timeout_error = "injected timeout config failure";
                }
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "failed to configure client send timeout; closing connection",
                    {
                        {"timeout_ms", std::to_string(config_.client_io_timeout_ms)},
                        {"error", timeout_error},
                    });
                CloseSocket(client_socket);
                continue;
            }

            bool enqueued = false;
            std::size_t pending_after = 0;
            {
                std::lock_guard lock(pending_clients_mutex_);
                if (pending_clients_.size() < config_.max_pending_clients) {
                    pending_clients_.push(static_cast<decltype(listen_socket_)>(client_socket));
                    pending_after = pending_clients_.size();
                    enqueued = true;
                } else {
                    pending_after = pending_clients_.size();
                }
            }
            if (enqueued) {
                if (pending_after < config_.max_pending_clients) {
                    pending_queue_overload_warned_.store(false, std::memory_order_relaxed);
                }
                pending_clients_cv_.notify_one();
                continue;
            }

            CloseSocket(client_socket);
            if (!pending_queue_overload_warned_.exchange(true, std::memory_order_relaxed)) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kServer,
                    "pending client queue full; rejecting new connections",
                    {
                        {"max_pending_clients", std::to_string(config_.max_pending_clients)},
                        {"pending_clients", std::to_string(pending_after)},
                    });
            }
        }

        running_.store(false);
        {
            std::lock_guard lock(lifecycle_mutex_);
            listen_socket_ = kInvalidSocket;
        }
        pending_clients_cv_.notify_all();
        JoinWorkers();

        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - run_started);
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "shutdown complete",
            {{"elapsed_ms", std::to_string(elapsed.count())}});
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kServer,
            "server run loop failed",
            {{"error", e.what()}});
        running_.store(false);
        pending_clients_cv_.notify_all();
        JoinWorkers();
#ifdef _WIN32
        WSACleanup();
#endif
        throw;
    }

#ifdef _WIN32
    WSACleanup();
#endif
}

void ChunkServer::Stop() {
    const bool was_running = running_.exchange(false);

    SocketHandle listen_socket = kInvalidSocket;
    {
        std::lock_guard lock(lifecycle_mutex_);
        if (listen_socket_ != kInvalidSocket) {
            listen_socket = static_cast<SocketHandle>(listen_socket_);
            listen_socket_ = kInvalidSocket;
        }
    }

    std::size_t pending_count = 0;
    std::size_t active_count = 0;

    if (listen_socket != kInvalidSocket) {
        ShutdownSocket(listen_socket);
        CloseSocket(listen_socket);
    }

    pending_clients_cv_.notify_all();

    {
        std::lock_guard lock(pending_clients_mutex_);
        pending_count = pending_clients_.size();
        while (!pending_clients_.empty()) {
            CloseSocket(static_cast<SocketHandle>(pending_clients_.front()));
            pending_clients_.pop();
        }
    }

    {
        std::lock_guard lock(active_clients_mutex_);
        active_count = active_clients_.size();
        for (const auto client_socket : active_clients_) {
            ShutdownSocket(static_cast<SocketHandle>(client_socket));
        }
        active_clients_.clear();
    }

    if (was_running || listen_socket != kInvalidSocket || pending_count > 0 || active_count > 0) {
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "stop requested",
            {
                {"pending_clients", std::to_string(pending_count)},
                {"active_clients", std::to_string(active_count)},
            });
        LogMessage(
            LogLevel::kInfo,
            LogComponent::kServer,
            "workers and connections draining",
            {
                {"pending_clients", std::to_string(pending_count)},
                {"active_clients", std::to_string(active_count)},
            });
    }
}

void ChunkServer::WorkerLoop() {
    while (true) {
        decltype(listen_socket_) client_socket = kInvalidSocket;

        {
            std::unique_lock lock(pending_clients_mutex_);
            pending_clients_cv_.wait(lock, [&]() {
                return !running_.load() || !pending_clients_.empty();
            });

            if (pending_clients_.empty()) {
                if (!running_.load()) {
                    return;
                }
                continue;
            }

            client_socket = pending_clients_.front();
            pending_clients_.pop();
            if (pending_clients_.size() < config_.max_pending_clients) {
                pending_queue_overload_warned_.store(false, std::memory_order_relaxed);
            }
        }

        {
            std::lock_guard lock(active_clients_mutex_);
            active_clients_.push_back(client_socket);
        }

        HandleClient(client_socket);

        {
            std::lock_guard lock(active_clients_mutex_);
            const auto it = std::find(active_clients_.begin(), active_clients_.end(), client_socket);
            if (it != active_clients_.end()) {
                active_clients_.erase(it);
            }
        }
    }
}

void ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
) {
    SessionState session;
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

    while (running_.load()) {
        bool has_line = false;
        try {
            if (!set_recv_timeout(
                    pending_buffer.empty() ? config_.idle_connection_timeout_ms : config_.client_io_timeout_ms,
                    pending_buffer.empty() ? "idle" : "partial_request")) {
                break;
            }
#ifdef CHUNKDB_WITH_OPENSSL
            if (config_.tls_enabled) {
                has_line = ReadLineTls(
                    tls_session,
                    line,
                    pending_buffer,
                    config_.max_line_bytes,
                    config_.client_io_timeout_ms,
                    &request_line_deadline,
                    &termination,
                    set_recv_timeout);
            } else {
                has_line = ReadLinePlain(
                    static_cast<SocketHandle>(client_socket),
                    line,
                    pending_buffer,
                    config_.max_line_bytes,
                    config_.client_io_timeout_ms,
                    &request_line_deadline,
                    &termination,
                    set_recv_timeout);
            }
#else
            has_line = ReadLinePlain(
                static_cast<SocketHandle>(client_socket),
                line,
                pending_buffer,
                config_.max_line_bytes,
                config_.client_io_timeout_ms,
                &request_line_deadline,
                &termination,
                set_recv_timeout);
#endif
        } catch (const std::exception& e) {
            const std::string response = Protocol::Error("BAD_REQUEST", e.what());
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kServer,
                "bad request disconnect",
                {{"reason", e.what()}});
#ifdef CHUNKDB_WITH_OPENSSL
            if (config_.tls_enabled) {
                (void)WriteAllTls(tls_session, response.data(), response.size(), std::nullopt, nullptr);
            } else {
                (void)WriteAllPlain(
                    static_cast<SocketHandle>(client_socket),
                    response.data(),
                    response.size(),
                    std::nullopt,
                    nullptr);
            }
#else
            (void)WriteAllPlain(
                static_cast<SocketHandle>(client_socket),
                response.data(),
                response.size(),
                std::nullopt,
                nullptr);
#endif
            break;
        }

        if (!has_line) {
            LogConnectionTermination(termination);
            break;
        }

        const std::string response = engine_->Execute(session, line);
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
