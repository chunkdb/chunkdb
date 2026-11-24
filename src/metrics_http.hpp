#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

#include "server_poller.hpp"
#include "server_socket.hpp"

namespace chunkdb {
namespace server_detail {

// The longest request head (request line and headers, terminator included)
// the metrics listener reads.
inline constexpr std::size_t kMetricsMaxRequestBytes = 8192;

// The most connections the metrics listener holds at once; a new connection
// past it closes the oldest one that has its response, else the oldest one
// still reading its head, else the oldest one.
inline constexpr std::size_t kMetricsMaxConnections = 16;

// The complete HTTP response to one request head (without its terminating
// empty line): the text of `render` for GET /metrics, otherwise an error.
[[nodiscard]] std::string MetricsHttpResponse(
    std::string_view head, const std::function<std::string()>& render);

// Serves GET /metrics over plain HTTP on a thread of its own, one response
// per connection. The thread waits on every connection at once with
// nonblocking sockets, so a slow client delays no other one. A request head
// must arrive within `timeout_ms` of the connection and fit
// kMetricsMaxRequestBytes; the response must be written within `timeout_ms`.
class MetricsHttpListener {
  public:
    // Binds and starts serving; throws std::runtime_error when the address
    // cannot be bound, or is not loopback while `allow_remote` is false.
    MetricsHttpListener(
        const std::string& host,
        std::uint16_t port,
        bool allow_remote,
        std::size_t timeout_ms,
        std::function<std::string()> render);
    // Closes every connection and joins the thread without waiting for
    // clients.
    ~MetricsHttpListener();

    MetricsHttpListener(const MetricsHttpListener&) = delete;
    MetricsHttpListener& operator=(const MetricsHttpListener&) = delete;

    // The bound port, which `port` 0 leaves to the system.
    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

  private:
    struct Connection;

    void Loop();
    // Reads, writes or drains `connection`; false when it is done.
    bool Advance(Connection* connection);

    std::size_t timeout_ms_;
    std::function<std::string()> render_;
    SocketHandle listen_socket_ = kInvalidSocket;
    std::uint16_t port_ = 0;
    std::unique_ptr<SocketPoller> poller_;
    std::atomic<bool> running_{true};
    std::thread thread_;
};

}  // namespace server_detail
}  // namespace chunkdb
