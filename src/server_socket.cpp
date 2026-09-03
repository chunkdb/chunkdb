#include "server_socket.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "chunkdb/logging.hpp"

namespace chunkdb {
namespace server_detail {

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

bool IsSocketInterruptedError(int code) {
#ifdef _WIN32
    return code == WSAEINTR;
#else
    return code == EINTR;
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

std::string PeerAddressForSocket(SocketHandle socket_fd) {
    sockaddr_storage peer{};
#ifdef _WIN32
    int peer_len = static_cast<int>(sizeof(peer));
#else
    socklen_t peer_len = static_cast<socklen_t>(sizeof(peer));
#endif

    if (getpeername(socket_fd, reinterpret_cast<sockaddr*>(&peer), &peer_len) != 0) {
        return {};
    }

    std::array<char, NI_MAXHOST> host{};
#ifdef _WIN32
    const DWORD host_len = static_cast<DWORD>(host.size());
#else
    const socklen_t host_len = static_cast<socklen_t>(host.size());
#endif
    if (getnameinfo(
            reinterpret_cast<const sockaddr*>(&peer),
            peer_len,
            host.data(),
            host_len,
            nullptr,
            0,
            NI_NUMERICHOST) != 0) {
        return {};
    }
    return host.data();
}

bool PendingClientExpired(
    std::chrono::steady_clock::time_point accepted_at,
    std::chrono::steady_clock::time_point now,
    std::size_t timeout_ms) {
    return now - accepted_at >= std::chrono::milliseconds(timeout_ms);
}

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

    // poll() rather than select(): select's fd_set is a fixed FD_SETSIZE-wide bitmap
    // (1024 on glibc) and FD_SET performs an unchecked store, so a descriptor at or
    // above that bound writes past the stack object. Descriptor numbers here come
    // straight from accept() and are driven by concurrent connection count, so the
    // bound is reachable under the file-descriptor limits this project ships.
    const auto timeout_ms = timeout.count();
    const int poll_timeout = timeout_ms > static_cast<std::chrono::milliseconds::rep>(std::numeric_limits<int>::max())
        ? std::numeric_limits<int>::max()
        : static_cast<int>(timeout_ms);

    int ready = 0;
    do {
#ifdef _WIN32
        WSAPOLLFD poll_fd{};
        poll_fd.fd = socket_fd;
        poll_fd.events = static_cast<SHORT>((want_read ? POLLRDNORM : 0) | (want_write ? POLLWRNORM : 0));
        ready = WSAPoll(&poll_fd, 1, poll_timeout);
#else
        pollfd poll_fd{};
        poll_fd.fd = socket_fd;
        poll_fd.events = static_cast<short>((want_read ? POLLIN : 0) | (want_write ? POLLOUT : 0));
        ready = poll(&poll_fd, 1, poll_timeout);
#endif
    } while (ready < 0 && IsSocketInterruptedError(CurrentSocketErrorCode()));

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

}  // namespace server_detail
}  // namespace chunkdb
