#pragma once

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace chunkdb {
namespace server_detail {

#ifdef _WIN32
using SocketHandle = SOCKET;
inline constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
inline constexpr SocketHandle kInvalidSocket = -1;
#endif

struct ConnectionTermination {
    bool should_log = false;
    std::string phase;
    std::string reason;
    std::string error;
};

using PhaseDeadline = std::optional<std::chrono::steady_clock::time_point>;

extern std::atomic<std::size_t> g_test_send_timeout_config_failures;
extern std::atomic<std::size_t> g_test_recv_timeout_config_failures;
extern std::atomic<std::uint64_t> g_test_recv_timeout_config_calls;

enum class SocketWaitResult {
    kReady,
    kTimeout,
    kError,
};

bool ConsumeTestFailureBudget(std::atomic<std::size_t>* counter);

int CurrentSocketErrorCode();

std::string FormatSocketError(int code);

bool IsSocketTimeoutError(int code);

bool IsSocketInterruptedError(int code);

bool IsSocketPeerCloseError(int code);

ConnectionTermination MakeSocketTermination(
    std::string_view phase,
    int socket_error_code,
    bool log_peer_close);

void LogConnectionTermination(const ConnectionTermination& termination);

ConnectionTermination MakePhaseDeadlineTermination(
    std::string_view phase,
    std::string_view detail);

void CloseSocket(SocketHandle socket_fd);

void ShutdownSocket(SocketHandle socket_fd);

std::string PeerAddressForSocket(SocketHandle socket_fd);

bool PendingClientExpired(
    std::chrono::steady_clock::time_point accepted_at,
    std::chrono::steady_clock::time_point now,
    std::size_t timeout_ms);

bool SetSocketNonBlocking(
    SocketHandle socket_fd,
    bool enabled,
    std::string* error);

SocketWaitResult WaitForSocketReady(
    SocketHandle socket_fd,
    bool want_read,
    bool want_write,
    std::chrono::milliseconds timeout,
    int* socket_error_code);

std::string SocketErrorText();

bool EnableTcpNoDelay(SocketHandle socket_fd, std::string* error);

bool ConfigureSocketRecvTimeout(
    SocketHandle socket_fd,
    std::size_t timeout_ms,
    std::string* error);

bool ConfigureSocketSendTimeout(
    SocketHandle socket_fd,
    std::size_t timeout_ms,
    std::string* error);

SocketHandle CreateListenSocket(const std::string& host, std::uint16_t port);

}  // namespace server_detail
}  // namespace chunkdb
