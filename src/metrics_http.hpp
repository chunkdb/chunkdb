#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
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
// still reading its head, else the oldest one waiting for its scrape, else
// the oldest one.
inline constexpr std::size_t kMetricsMaxConnections = 16;

// One scrape: the exposition text, served with 200 when `available`, with
// 503 otherwise.
struct MetricsScrape {
    bool available = true;
    std::string body;
};

// Produces a scrape; it must return soon after `stop` is requested.
using MetricsRender = std::function<MetricsScrape(std::stop_token stop)>;

// A request head (without its terminating empty line): the complete error
// response in `refusal`, or GET or HEAD /metrics.
struct MetricsRequest {
    std::optional<std::string> refusal;
    bool head_only = false;
};
[[nodiscard]] MetricsRequest ParseMetricsRequest(std::string_view head);

// The complete HTTP response carrying `scrape`; with `head_only`, its headers.
[[nodiscard]] std::string MetricsResponse(const MetricsScrape& scrape, bool head_only);

// ParseMetricsRequest, then MetricsResponse of `render()` for a metrics
// request.
[[nodiscard]] std::string MetricsHttpResponse(
    std::string_view head, const std::function<MetricsScrape()>& render);

// Serves GET and HEAD /metrics over plain HTTP, one response per connection.
// One thread waits on every connection at once with nonblocking sockets, so
// a slow client delays no other one; another runs `render`, one scrape at a
// time, and requests that arrive while a scrape runs share its result. A
// request head must arrive within `timeout_ms` of the connection and fit
// kMetricsMaxRequestBytes, its scrape must finish within `timeout_ms` (503
// with `timeout_body` otherwise), and the response must be written within
// `timeout_ms`.
class MetricsHttpListener {
  public:
    // Binds and starts serving; throws std::runtime_error when the address
    // cannot be bound, or is not loopback while `allow_remote` is false.
    MetricsHttpListener(
        const std::string& host,
        std::uint16_t port,
        bool allow_remote,
        std::size_t timeout_ms,
        MetricsRender render,
        std::string timeout_body = {});
    // Closes every connection, stops the render and joins both threads
    // without waiting for clients.
    ~MetricsHttpListener();

    MetricsHttpListener(const MetricsHttpListener&) = delete;
    MetricsHttpListener& operator=(const MetricsHttpListener&) = delete;

    // The bound port, which `port` 0 leaves to the system.
    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

  private:
    struct Connection;

    void Loop();
    void RenderLoop(std::stop_token stop);
    // The generation of the scrape a new request waits for.
    [[nodiscard]] std::uint64_t RequestRender();
    // Reads, writes or drains `connection`; false when it is done.
    bool Advance(Connection* connection);

    std::size_t timeout_ms_;
    MetricsRender render_;
    std::string timeout_body_;
    SocketHandle listen_socket_ = kInvalidSocket;
    std::uint16_t port_ = 0;
    std::unique_ptr<SocketPoller> poller_;
    std::atomic<bool> running_{true};
    // Scrape generations: the highest requested, the latest finished and
    // its result.
    std::mutex render_mutex_;
    std::condition_variable_any render_cv_;
    std::uint64_t render_wanted_ = 0;
    std::uint64_t render_done_ = 0;
    std::shared_ptr<const MetricsScrape> render_result_;
    std::jthread render_thread_;
    std::thread thread_;
};

}  // namespace server_detail
}  // namespace chunkdb
