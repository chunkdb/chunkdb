#include "metrics_http.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "chunkdb/logging.hpp"

namespace chunkdb {
namespace server_detail {

namespace {

using Clock = std::chrono::steady_clock;


std::string Response(std::string_view status, std::string_view content_type, std::string_view extra_headers,
                     std::string_view body) {
    std::string response;
    response.reserve(160U + body.size());
    response += "HTTP/1.1 ";
    response += status;
    response += "\r\nContent-Type: ";
    response += content_type;
    response += "\r\nContent-Length: ";
    response += std::to_string(body.size());
    response += "\r\n";
    response += extra_headers;
    response += "Connection: close\r\n\r\n";
    response += body;
    return response;
}

std::string ErrorResponse(std::string_view status, std::string_view extra_headers = {}) {
    std::string body(status);
    body += "\n";
    return Response(status, "text/plain; charset=utf-8", extra_headers, body);
}

// Where the empty line ending a request head starts, or npos.
std::size_t HeadEnd(std::string_view bytes) {
    return std::min(bytes.find("\r\n\r\n"), bytes.find("\n\n"));
}

bool IsLoopbackAddress(const sockaddr_storage& address) {
    if (address.ss_family == AF_INET) {
        sockaddr_in v4{};
        std::memcpy(&v4, &address, sizeof(v4));
        unsigned char bytes[4];
        std::memcpy(bytes, &v4.sin_addr, sizeof(bytes));
        return bytes[0] == 127U;
    }
    if (address.ss_family == AF_INET6) {
        sockaddr_in6 v6{};
        std::memcpy(&v6, &address, sizeof(v6));
        unsigned char bytes[16];
        std::memcpy(bytes, &v6.sin6_addr, sizeof(bytes));
        const bool zero_prefix = std::all_of(bytes, bytes + 10, [](unsigned char b) { return b == 0U; });
        if (!zero_prefix) return false;
        // ::1, or an IPv4-mapped 127.0.0.0/8 address.
        if (bytes[10] == 0U && bytes[11] == 0U)
            return std::all_of(bytes + 12, bytes + 15, [](unsigned char b) { return b == 0U; }) && bytes[15] == 1U;
        return bytes[10] == 0xffU && bytes[11] == 0xffU && bytes[12] == 127U;
    }
    return false;
}

std::uint16_t PortOf(const sockaddr_storage& address) {
    if (address.ss_family == AF_INET) {
        sockaddr_in v4{};
        std::memcpy(&v4, &address, sizeof(v4));
        return ntohs(v4.sin_port);
    }
    sockaddr_in6 v6{};
    std::memcpy(&v6, &address, sizeof(v6));
    return ntohs(v6.sin6_port);
}

void ShutdownSend(SocketHandle socket_fd) {
#ifdef _WIN32
    (void)shutdown(socket_fd, SD_SEND);
#else
    (void)shutdown(socket_fd, SHUT_WR);
#endif
}

long Receive(SocketHandle socket_fd, char* buffer, std::size_t size) {
#ifdef _WIN32
    return recv(socket_fd, buffer, static_cast<int>(size), 0);
#else
    return static_cast<long>(recv(socket_fd, buffer, size, 0));
#endif
}

long Send(SocketHandle socket_fd, const char* data, std::size_t size) {
#ifdef _WIN32
    return send(socket_fd, data, static_cast<int>(std::min<std::size_t>(size, 1U << 20U)), 0);
#elif defined(MSG_NOSIGNAL)
    return static_cast<long>(send(socket_fd, data, size, MSG_NOSIGNAL));
#else
    return static_cast<long>(send(socket_fd, data, size, 0));
#endif
}

// How long the listener stops accepting after accept fails for a reason
// other than a closed connection, such as EMFILE.
constexpr std::chrono::milliseconds kAcceptPause{250};

// Whether accept failed only because the client closed the connection first.
bool IsAbortedAccept(int code) {
#ifdef _WIN32
    return code == WSAECONNRESET;
#else
    return code == ECONNABORTED;
#endif
}

// Whether a failed nonblocking call only has to wait for readiness again.
bool ShouldRetry(int code) {
#ifdef _WIN32
    return code == WSAEWOULDBLOCK || IsSocketInterruptedError(code);
#else
    return code == EAGAIN || code == EWOULDBLOCK || IsSocketInterruptedError(code);
#endif
}

}  // namespace

struct MetricsHttpListener::Connection {
    enum class Phase { kRead, kWrite, kDrain };

    SocketHandle socket = kInvalidSocket;
    Phase phase = Phase::kRead;
    // The end of the current phase.
    Clock::time_point deadline;
    std::string head;
    std::string response;
    std::size_t written = 0;
};


std::string MetricsHttpResponse(std::string_view head, const std::function<std::string()>& render) {
    std::string_view line = head.substr(0, head.find('\n'));
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    const auto first = line.find(' ');
    const auto second = first == std::string_view::npos ? first : line.find(' ', first + 1U);
    if (second == std::string_view::npos || line.find(' ', second + 1U) != std::string_view::npos) {
        return ErrorResponse("400 Bad Request");
    }
    const auto method = line.substr(0, first);
    const auto target = line.substr(first + 1U, second - first - 1U);
    const auto version = line.substr(second + 1U);
    const bool method_ok = !method.empty() &&
        std::all_of(method.begin(), method.end(), [](char c) { return c >= 'A' && c <= 'Z'; });
    if (!method_ok || target.empty() || target.front() != '/' || (version != "HTTP/1.0" && version != "HTTP/1.1")) {
        return ErrorResponse("400 Bad Request");
    }
    if (target.substr(0, target.find('?')) != "/metrics") {
        return ErrorResponse("404 Not Found");
    }
    if (method != "GET" && method != "HEAD") {
        return ErrorResponse("405 Method Not Allowed", "Allow: GET, HEAD\r\n");
    }
    auto response = Response("200 OK", "text/plain; version=0.0.4; charset=utf-8", {}, render());
    // HEAD: the headers GET would send, Content-Length included.
    if (method == "HEAD") response.resize(response.find("\r\n\r\n") + 4U);
    return response;
}


MetricsHttpListener::MetricsHttpListener(
    const std::string& host,
    std::uint16_t port,
    bool allow_remote,
    std::size_t timeout_ms,
    std::function<std::string()> render)
    : timeout_ms_(timeout_ms), render_(std::move(render)) {
    listen_socket_ = CreateListenSocket(host, port);
    try {
        sockaddr_storage bound{};
#ifdef _WIN32
        int bound_size = static_cast<int>(sizeof(bound));
#else
        socklen_t bound_size = sizeof(bound);
#endif
        if (getsockname(listen_socket_, reinterpret_cast<sockaddr*>(&bound), &bound_size) != 0) {
            throw std::runtime_error("cannot read the metrics listener address: " + SocketErrorText());
        }
        if (!allow_remote && !IsLoopbackAddress(bound)) {
            throw std::runtime_error(
                "metrics address " + host + " is not a loopback address and the metrics endpoint has no "
                "authentication; listen on a loopback address or set --metrics-allow-remote");
        }
        port_ = PortOf(bound);
        std::string error;
        if (!SetSocketNonBlocking(listen_socket_, true, &error)) {
            throw std::runtime_error("cannot make the metrics listener nonblocking: " + error);
        }
        poller_ = std::make_unique<SocketPoller>(kMetricsMaxConnections + 1U);
        thread_ = std::thread(&MetricsHttpListener::Loop, this);
    } catch (...) {
        CloseSocket(listen_socket_);
        throw;
    }
}

MetricsHttpListener::~MetricsHttpListener() {
    running_.store(false, std::memory_order_release);
    poller_->Wake();
    if (thread_.joinable()) thread_.join();
    if (listen_socket_ != kInvalidSocket) CloseSocket(listen_socket_);
}

void MetricsHttpListener::Loop() {
    const auto timeout = std::chrono::milliseconds(timeout_ms_);
    // In the order they were accepted.
    std::vector<Connection> connections;
    std::vector<SocketInterest> interests;
    // The connection a new one displaces: one whose response is written,
    // else one still reading its head, else the oldest; each the oldest of
    // its kind.
    const auto eviction_victim = [&] {
        for (const auto phase : {Connection::Phase::kDrain, Connection::Phase::kRead}) {
            const auto found = std::find_if(connections.begin(), connections.end(),
                [phase](const Connection& connection) { return connection.phase == phase; });
            if (found != connections.end()) return found;
        }
        return connections.begin();
    };
    auto accept_paused_until = Clock::time_point::min();
    bool accept_failing = false;
    std::string failure;
    const auto respond = [&](Connection* connection, std::string response) {
        connection->response = std::move(response);
        connection->written = 0;
        connection->phase = Connection::Phase::kWrite;
        connection->deadline = Clock::now() + timeout;
    };
    const auto close_finished = [&](const std::vector<bool>& keep) {
        std::size_t kept = 0;
        for (std::size_t i = 0; i < connections.size(); ++i) {
            if (keep[i]) {
                if (kept != i) connections[kept] = std::move(connections[i]);
                ++kept;
            } else {
                CloseSocket(connections[i].socket);
            }
        }
        connections.resize(kept);
    };

    try {
        for (;;) {
            poller_->DrainWake();
            if (!running_.load(std::memory_order_acquire)) break;

            // A head that missed its deadline gets 408; a response or drain
            // that missed its deadline closes the connection.
            auto now = Clock::now();
            std::vector<bool> keep(connections.size(), true);
            for (std::size_t i = 0; i < connections.size(); ++i) {
                auto& connection = connections[i];
                if (now < connection.deadline) continue;
                if (connection.phase == Connection::Phase::kRead) {
                    respond(&connection, ErrorResponse("408 Request Timeout"));
                } else {
                    keep[i] = false;
                }
            }
            close_finished(keep);

            // After an accept failure such as EMFILE the pending connection
            // stays queued and the socket stays readable, so the listen
            // socket is left out of the wait until accept_paused_until.
            const bool accepting = now >= accept_paused_until;
            interests.clear();
            if (accepting) interests.push_back({listen_socket_, POLLIN, 0});
            const std::size_t first = interests.size();
            auto next_deadline = accepting ? Clock::time_point::max() : accept_paused_until;
            for (const auto& connection : connections) {
                const short events = connection.phase == Connection::Phase::kWrite ? POLLOUT : POLLIN;
                interests.push_back({connection.socket, events, 0});
                next_deadline = std::min(next_deadline, connection.deadline);
            }
            int wait_ms = -1;
            if (next_deadline != Clock::time_point::max()) {
                const auto left = std::chrono::ceil<std::chrono::milliseconds>(next_deadline - Clock::now()).count();
                wait_ms = static_cast<int>(std::clamp<long long>(left, 0, std::numeric_limits<int>::max()));
            }
            if (!poller_->Wait(interests, wait_ms)) continue;

            keep.assign(connections.size(), true);
            for (std::size_t i = 0; i < connections.size(); ++i) {
                if (interests[i + first].ready != 0) keep[i] = Advance(&connections[i]);
            }
            close_finished(keep);

            if (!accepting || interests[0].ready == 0) continue;
            // Accepts what is waiting, up to one full set of connections per
            // pass.
            for (std::size_t accepted = 0; accepted < kMetricsMaxConnections; ++accepted) {
                const SocketHandle client = accept(listen_socket_, nullptr, nullptr);
                if (client == kInvalidSocket) {
                    const int code = CurrentSocketErrorCode();
                    if (ShouldRetry(code) || IsAbortedAccept(code)) break;
                    if (!accept_failing) {
                        LogMessage(LogLevel::kWarn, LogComponent::kServer, "metrics listener cannot accept",
                                   {{"error", FormatSocketError(code)}});
                    }
                    accept_failing = true;
                    accept_paused_until = Clock::now() + kAcceptPause;
                    break;
                }
                accept_failing = false;
                std::string error;
                if (!SetSocketNonBlocking(client, true, &error)) {
                    CloseSocket(client);
                    continue;
                }
#if defined(SO_NOSIGPIPE)
                const int on = 1;
                (void)setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &on, sizeof(on));
#endif
                if (connections.size() >= kMetricsMaxConnections) {
                    const auto victim = eviction_victim();
                    CloseSocket(victim->socket);
                    connections.erase(victim);
                }
                Connection connection;
                connection.socket = client;
                connection.deadline = Clock::now() + timeout;
                connections.push_back(std::move(connection));
            }
        }
    } catch (const std::exception& error) {
        failure = *error.what() != 0 ? error.what() : "unknown error";
    } catch (...) {
        failure = "unknown error";
    }
    if (!failure.empty()) {
        // Nothing serves the port any more: close it, before the connections,
        // so that scrapes are refused instead of queued.
        CloseSocket(listen_socket_);
        listen_socket_ = kInvalidSocket;
        LogMessage(LogLevel::kError, LogComponent::kServer, "metrics listener stopped; GET /metrics is no longer served",
                   {{"error", failure}});
    }
    for (const auto& connection : connections) CloseSocket(connection.socket);
}

bool MetricsHttpListener::Advance(Connection* connection) {
    char buffer[4096];
    switch (connection->phase) {
        case Connection::Phase::kRead: {
            const auto received = Receive(
                connection->socket, buffer, std::min(sizeof(buffer), kMetricsMaxRequestBytes - connection->head.size()));
            if (received < 0) return ShouldRetry(CurrentSocketErrorCode());
            if (received == 0) {
                // A client that closes before sending anything gets nothing.
                if (connection->head.empty()) return false;
                connection->response = ErrorResponse("400 Bad Request");
            } else {
                connection->head.append(buffer, static_cast<std::size_t>(received));
                if (const auto end = HeadEnd(connection->head); end != std::string::npos) {
                    connection->head.resize(end);
                    try {
                        connection->response = MetricsHttpResponse(connection->head, render_);
                    } catch (const std::exception& error) {
                        LogMessage(LogLevel::kWarn, LogComponent::kServer, "metrics scrape failed",
                                   {{"error", error.what()}});
                        connection->response = ErrorResponse("500 Internal Server Error");
                    }
                } else if (connection->head.size() >= kMetricsMaxRequestBytes) {
                    connection->response = ErrorResponse("431 Request Header Fields Too Large");
                } else {
                    return true;
                }
            }
            connection->head.clear();
            connection->written = 0;
            connection->phase = Connection::Phase::kWrite;
            connection->deadline = Clock::now() + std::chrono::milliseconds(timeout_ms_);
            return true;
        }
        case Connection::Phase::kWrite: {
            const auto sent = Send(
                connection->socket, connection->response.data() + connection->written,
                connection->response.size() - connection->written);
            if (sent < 0) return ShouldRetry(CurrentSocketErrorCode());
            connection->written += static_cast<std::size_t>(sent);
            if (connection->written < connection->response.size()) return true;
            // Reads what the client still sends until it closes, so that
            // unread input does not make the close reset the connection and
            // discard the response.
            ShutdownSend(connection->socket);
            connection->response = {};
            connection->phase = Connection::Phase::kDrain;
            connection->deadline = Clock::now() + std::chrono::milliseconds(timeout_ms_);
            return true;
        }
        case Connection::Phase::kDrain: {
            const auto received = Receive(connection->socket, buffer, sizeof(buffer));
            if (received < 0) return ShouldRetry(CurrentSocketErrorCode());
            return received > 0;
        }
    }
    return false;
}

}  // namespace server_detail
}  // namespace chunkdb
