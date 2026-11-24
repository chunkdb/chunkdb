// The metrics HTTP listener: responses, refusals, the request bound, the
// read timeout, concurrent and slow clients, the connection cap and stopping.

#include "metrics_http.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stop_token>
#include <cstdio>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

namespace {

using namespace chunkdb::server_detail;
using Clock = std::chrono::steady_clock;

const std::string kBody = "# HELP test_total A test.\n# TYPE test_total counter\ntest_total{table=\"t\"} 1\n";

void StartSockets() {
#ifdef _WIN32
    static const int started = [] { WSADATA data{}; return WSAStartup(MAKEWORD(2, 2), &data); }();
    assert(started == 0);
#endif
}

class Client {
  public:
    explicit Client(std::uint16_t port) {
        socket_ = socket(AF_INET, SOCK_STREAM, 0);
        assert(socket_ != kInvalidSocket);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        assert(connect(socket_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    }
    ~Client() { CloseSocket(socket_); }
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    void Send(std::string_view data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
#ifdef _WIN32
            const int n = send(socket_, data.data() + sent, static_cast<int>(data.size() - sent), 0);
#else
            const auto n = send(socket_, data.data() + sent, data.size() - sent, 0);
#endif
            assert(n > 0);
            sent += static_cast<std::size_t>(n);
        }
    }

    void ShutdownWrite() {
#ifdef _WIN32
        assert(shutdown(socket_, SD_SEND) == 0);
#else
        assert(shutdown(socket_, SHUT_WR) == 0);
#endif
    }

    // Everything the server sends until it closes, within 10 seconds; with
    // `allow_reset`, until it closes or resets the connection.
    std::string ReadUntilClose(bool allow_reset = false) {
        const auto deadline = Clock::now() + std::chrono::seconds(10);
        std::string all;
        char buffer[4096];
        for (;;) {
            const auto now = Clock::now();
            assert(now < deadline);
            int error = 0;
            const auto ready = WaitForSocketReady(
                socket_, true, false, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now), &error);
            assert(ready == SocketWaitResult::kReady);
#ifdef _WIN32
            const int n = recv(socket_, buffer, static_cast<int>(sizeof(buffer)), 0);
#else
            const auto n = recv(socket_, buffer, sizeof(buffer), 0);
#endif
            if (n < 0 && allow_reset && IsSocketPeerCloseError(CurrentSocketErrorCode())) return all;
            assert(n >= 0);
            if (n == 0) return all;
            all.append(buffer, static_cast<std::size_t>(n));
        }
    }

  private:
    SocketHandle socket_ = kInvalidSocket;
};

std::string Exchange(std::uint16_t port, std::string_view request) {
    Client client(port);
    client.Send(request);
    return client.ReadUntilClose();
}

std::string StatusLine(const std::string& response) {
    return response.substr(0, response.find("\r\n"));
}

std::string BodyOf(const std::string& response) {
    const auto end = response.find("\r\n\r\n");
    assert(end != std::string::npos);
    return response.substr(end + 4U);
}

void WaitFor(const std::function<bool()>& condition) {
    const auto deadline = Clock::now() + std::chrono::seconds(10);
    while (!condition()) {
        assert(Clock::now() < deadline);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

struct Rendering {
    std::atomic<int> calls{0};
    MetricsRender Function() {
        return [this](std::stop_token) {
            calls.fetch_add(1);
            return MetricsScrape{.available = true, .body = kBody};
        };
    }
};

void TestResponses() {
    Rendering rendering;
    const auto render = [function = rendering.Function()] { return function({}); };
    const auto ok = MetricsHttpResponse("GET /metrics HTTP/1.1\r\nHost: localhost:14242\r\nAccept: */*", render);
    assert(StatusLine(ok) == "HTTP/1.1 200 OK");
    assert(ok.find("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n") != std::string::npos);
    assert(ok.find("\r\nContent-Length: " + std::to_string(kBody.size()) + "\r\n") != std::string::npos);
    assert(ok.find("\r\nConnection: close\r\n") != std::string::npos);
    assert(BodyOf(ok) == kBody);
    assert(StatusLine(MetricsHttpResponse("GET /metrics?name[]=x HTTP/1.0", render)) == "HTTP/1.1 200 OK");
    assert(StatusLine(MetricsHttpResponse("GET /metrics HTTP/1.1\nHost: x", render)) == "HTTP/1.1 200 OK");
    // HEAD: the same headers, no body.
    const auto head_only = MetricsHttpResponse("HEAD /metrics HTTP/1.1\r\nHost: x", render);
    assert(StatusLine(head_only) == "HTTP/1.1 200 OK");
    assert(head_only.find("\r\nContent-Length: " + std::to_string(kBody.size()) + "\r\n") != std::string::npos);
    assert(head_only.ends_with("\r\n\r\n") && BodyOf(head_only).empty());
    assert(rendering.calls.load() == 4);

    for (const char* head : {"GET / HTTP/1.1", "GET /metricsx HTTP/1.1", "GET /metrics/ HTTP/1.1",
                             "GET /other HTTP/1.1", "POST /other HTTP/1.1"}) {
        const auto response = MetricsHttpResponse(head, render);
        assert(StatusLine(response) == "HTTP/1.1 404 Not Found");
        assert(BodyOf(response) == "404 Not Found\n");
    }
    for (const char* head : {"POST /metrics HTTP/1.1", "OPTIONS /metrics HTTP/1.1", "PUT /metrics?x HTTP/1.0"}) {
        const auto response = MetricsHttpResponse(head, render);
        assert(StatusLine(response) == "HTTP/1.1 405 Method Not Allowed");
        assert(response.find("\r\nAllow: GET, HEAD\r\n") != std::string::npos);
    }
    for (const char* head : {"", "GARBAGE", "GET /metrics", "GET  /metrics HTTP/1.1", "GET /metrics HTTP/1.1 extra",
                             "get /metrics HTTP/1.1", "GET metrics HTTP/1.1", "GET /metrics HTTP/2.0",
                             "PRI * HTTP/2.0", "GET /metrics FTP/1.1"}) {
        assert(StatusLine(MetricsHttpResponse(head, render)) == "HTTP/1.1 400 Bad Request");
    }
    // Refused requests do not render the metrics.
    assert(rendering.calls.load() == 4);
}

void TestScrapeOverSocket() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 5000, rendering.Function());
    assert(listener.port() != 0);
    const auto response = Exchange(listener.port(), "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
    assert(StatusLine(response) == "HTTP/1.1 200 OK");
    assert(BodyOf(response) == kBody);
    assert(StatusLine(Exchange(listener.port(), "GET /nope HTTP/1.1\r\n\r\n")) == "HTTP/1.1 404 Not Found");
    assert(StatusLine(Exchange(listener.port(), "DELETE /metrics HTTP/1.1\r\n\r\n")) ==
           "HTTP/1.1 405 Method Not Allowed");
    assert(StatusLine(Exchange(listener.port(), "nonsense\r\n\r\n")) == "HTTP/1.1 400 Bad Request");

    // One response per connection: a second pipelined request gets none.
    const auto pipelined = Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\nGET /metrics HTTP/1.1\r\n\r\n");
    assert(pipelined.find("HTTP/1.1 ") == 0 && pipelined.find("HTTP/1.1 ", 1) == std::string::npos);
    assert(rendering.calls.load() == 2);

    // A head that ends before its empty line, because the client closed its side.
    {
        Client client(listener.port());
        client.Send("GET /metrics HTTP/1.1\r\n");
        client.ShutdownWrite();
        assert(StatusLine(client.ReadUntilClose()) == "HTTP/1.1 400 Bad Request");
    }
    assert(rendering.calls.load() == 2);
}

void TestRequestBound() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 5000, rendering.Function());
    // A head at the bound is read; one past it is refused, and the response
    // survives the input left unread.
    std::string head = "GET /metrics HTTP/1.1\r\nX-Pad: ";
    head.append(kMetricsMaxRequestBytes - head.size() - 4U, 'a');
    head += "\r\n\r\n";
    assert(head.size() == kMetricsMaxRequestBytes);
    assert(StatusLine(Exchange(listener.port(), head)) == "HTTP/1.1 200 OK");

    std::string oversized = "GET /metrics HTTP/1.1\r\n";
    while (oversized.size() < 4U * kMetricsMaxRequestBytes) oversized += "X-Pad: aaaaaaaaaaaaaaaaaaaaaaaa\r\n";
    oversized += "\r\n";
    assert(StatusLine(Exchange(listener.port(), oversized)) == "HTTP/1.1 431 Request Header Fields Too Large");
    assert(rendering.calls.load() == 1);
}

void TestReadTimeout() {
    Rendering rendering;
    constexpr std::size_t kTimeoutMs = 300;
    MetricsHttpListener listener("127.0.0.1", 0, false, kTimeoutMs, rendering.Function());
    for (const std::string_view partial : {std::string_view{}, std::string_view{"GET /metr"}}) {
        const auto started = Clock::now();
        Client client(listener.port());
        client.Send(partial);
        const auto response = client.ReadUntilClose();
        const auto elapsed = Clock::now() - started;
        assert(StatusLine(response) == "HTTP/1.1 408 Request Timeout");
        assert(elapsed >= std::chrono::milliseconds(kTimeoutMs - 50U));
    }
    // A slow client does not stop the next scrape.
    assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 200 OK");
    assert(rendering.calls.load() == 1);
}

// A scrape that is not available, or whose render throws, answers 503.
void TestRenderFailure() {
    {
        MetricsHttpListener listener("127.0.0.1", 0, false, 5000, [](std::stop_token) {
            return MetricsScrape{.available = false, .body = "chunkdb_up 0\n"};
        });
        const auto response = Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n");
        assert(StatusLine(response) == "HTTP/1.1 503 Service Unavailable");
        assert(BodyOf(response) == "chunkdb_up 0\n");
    }
    for (const bool standard : {true, false}) {
        MetricsHttpListener listener("127.0.0.1", 0, false, 5000, [standard](std::stop_token) -> MetricsScrape {
            if (standard) throw std::runtime_error("no metrics");
            throw 42;
        });
        for (int i = 0; i < 2; ++i) {
            assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) ==
                   "HTTP/1.1 503 Service Unavailable");
        }
    }
}

// A scrape that takes long delays no other connection, requests that arrive
// while it runs share it, one that outlasts the timeout answers 503, and
// stopping does not wait for it.
void TestSlowRender() {
    std::mutex mutex;
    std::condition_variable_any released_cv;
    bool released = false;
    std::atomic<int> calls{0};
    auto listener = std::make_unique<MetricsHttpListener>("127.0.0.1", 0, false, 2000, [&](std::stop_token stop) {
        calls.fetch_add(1);
        std::unique_lock lock(mutex);
        (void)released_cv.wait(lock, stop, [&] { return released; });
        return MetricsScrape{.available = true, .body = kBody};
    }, "chunkdb_up 0\n");
    const auto port = listener->port();
    std::vector<std::unique_ptr<Client>> waiting;
    for (int i = 0; i < 3; ++i) {
        waiting.push_back(std::make_unique<Client>(port));
        waiting.back()->Send("GET /metrics HTTP/1.1\r\n\r\n");
    }
    WaitFor([&] { return calls.load() == 1; });
    assert(StatusLine(Exchange(port, "GET /other HTTP/1.1\r\n\r\n")) == "HTTP/1.1 404 Not Found");
    {
        std::lock_guard lock(mutex);
        released = true;
    }
    released_cv.notify_all();
    for (auto& client : waiting) {
        const auto response = client->ReadUntilClose();
        assert(StatusLine(response) == "HTTP/1.1 200 OK" && BodyOf(response) == kBody);
    }
    assert(calls.load() == 1);

    // A scrape past the 2-second timeout.
    {
        std::lock_guard lock(mutex);
        released = false;
    }
    const auto started = Clock::now();
    const auto timed_out = Exchange(port, "GET /metrics HTTP/1.1\r\n\r\n");
    assert(StatusLine(timed_out) == "HTTP/1.1 503 Service Unavailable");
    assert(BodyOf(timed_out) == "chunkdb_up 0\n");
    assert(Clock::now() - started >= std::chrono::milliseconds(1900));
    // Still running: stopping requests its stop and does not wait for a release.
    Client pending(port);
    pending.Send("GET /metrics HTTP/1.1\r\n\r\n");
    const auto stopping = Clock::now();
    listener.reset();
    assert(Clock::now() - stopping < std::chrono::seconds(1));
}

// A client that sends part of its head and then nothing, or never reads its
// response, does not delay a scrape on another connection.
void TestSlowClientsDoNotBlock() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 60000, rendering.Function());
    Client silent(listener.port());
    silent.Send("GET /metr");
    Client not_reading(listener.port());
    not_reading.Send("GET /metrics HTTP/1.1\r\n\r\n");
    const auto started = Clock::now();
    for (int i = 0; i < 5; ++i) {
        assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 200 OK");
    }
    assert(Clock::now() - started < std::chrono::seconds(5));
    // The silent client still gets its response once it completes the head.
    silent.Send("ics HTTP/1.1\r\n\r\n");
    assert(StatusLine(silent.ReadUntilClose()) == "HTTP/1.1 200 OK");
    assert(StatusLine(not_reading.ReadUntilClose()) == "HTTP/1.1 200 OK");
    // Concurrent requests may share a scrape.
    assert(rendering.calls.load() >= 1 && rendering.calls.load() <= 7);
}

// Scrapes from several threads at once each get a whole response.
void TestConcurrentScrapes() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 5000, rendering.Function());
    constexpr int kThreads = 8;
    constexpr int kScrapes = 25;
    std::atomic<int> ok{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < kScrapes; ++i) {
                const auto response = Exchange(listener.port(), "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
                if (StatusLine(response) == "HTTP/1.1 200 OK" && BodyOf(response) == kBody) ok.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    assert(ok.load() == kThreads * kScrapes);
    assert(rendering.calls.load() >= 1 && rendering.calls.load() <= kThreads * kScrapes);
}

// Past kMetricsMaxConnections, a new connection closes the oldest one.
void TestConnectionCap() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 60000, rendering.Function());
    std::vector<std::unique_ptr<Client>> idle;
    for (std::size_t i = 0; i < kMetricsMaxConnections; ++i) {
        idle.push_back(std::make_unique<Client>(listener.port()));
    }
    // Accepted in order, so the next connection displaces the first.
    assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 200 OK");
    assert(idle.front()->ReadUntilClose().empty());
    assert(rendering.calls.load() == 1);
}

// Stopping closes every connection at once, whatever its phase.
void TestStopWithClients() {
    Rendering rendering;
    auto listener = std::make_unique<MetricsHttpListener>("127.0.0.1", 0, false, 60000, rendering.Function());
    Client connected(listener->port());
    Client partial(listener->port());
    partial.Send("GET /metr");
    Client not_reading(listener->port());
    not_reading.Send("GET /metrics HTTP/1.1\r\n\r\n");
    assert(StatusLine(Exchange(listener->port(), "GET /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 200 OK");
    const auto stopping = Clock::now();
    listener.reset();
    assert(Clock::now() - stopping < std::chrono::seconds(1));
    assert(StatusLine(connected.ReadUntilClose()).empty());
    assert(StatusLine(partial.ReadUntilClose()).empty());
}

// Past the cap, a connection still reading its head is closed before one
// whose response is being written.
void TestEvictionKeepsResponses() {
    // More than the socket buffers hold, so the response stays in writing
    // while its client does not read.
    const std::string large(32U << 20U, 'x');
    std::atomic<int> calls{0};
    MetricsHttpListener listener("127.0.0.1", 0, false, 60000, [&](std::stop_token) {
        calls.fetch_add(1);
        return MetricsScrape{.available = true, .body = large};
    });
    Client writing(listener.port());
    writing.Send("GET /metrics HTTP/1.1\r\n\r\n");
    WaitFor([&] { return calls.load() == 1; });
    std::vector<std::unique_ptr<Client>> idle;
    for (std::size_t i = 0; i + 1U < kMetricsMaxConnections; ++i) {
        idle.push_back(std::make_unique<Client>(listener.port()));
    }
    Client extra(listener.port());
    assert(idle.front()->ReadUntilClose().empty());
    const auto response = writing.ReadUntilClose();
    assert(StatusLine(response) == "HTTP/1.1 200 OK");
    assert(BodyOf(response).size() == large.size());
}

#ifndef _WIN32
double CpuSeconds() {
    rusage usage{};
    assert(getrusage(RUSAGE_SELF, &usage) == 0);
    const auto seconds = [](const timeval& value) {
        return static_cast<double>(value.tv_sec) + static_cast<double>(value.tv_usec) / 1e6;
    };
    return seconds(usage.ru_utime) + seconds(usage.ru_stime);
}

// When accept fails with EMFILE, the pending connection keeps the listen
// socket readable; the listener pauses accepting instead of spinning, and
// serves the connection once descriptors are free.
void TestAcceptFailureDoesNotSpin() {
    Rendering rendering;
    MetricsHttpListener listener("127.0.0.1", 0, false, 5000, rendering.Function());
    rlimit saved{};
    assert(getrlimit(RLIMIT_NOFILE, &saved) == 0);
    rlimit lowered = saved;
    lowered.rlim_cur = std::min<rlim_t>(saved.rlim_cur, 128);
    assert(setrlimit(RLIMIT_NOFILE, &lowered) == 0);
    std::vector<int> fillers;
    for (;;) {
        const int fd = open("/dev/null", O_RDONLY);
        if (fd < 0) break;
        fillers.push_back(fd);
    }
    assert(!fillers.empty());
    close(fillers.back());
    fillers.pop_back();
    {
        // Takes the last free descriptor; the listener has none to accept it.
        Client client(listener.port());
        client.Send("GET /metrics HTTP/1.1\r\n\r\n");
        const double cpu_before = CpuSeconds();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        const double cpu_used = CpuSeconds() - cpu_before;
        for (const int fd : fillers) close(fd);
        assert(setrlimit(RLIMIT_NOFILE, &saved) == 0);
        assert(cpu_used < 0.5);
        // Linux keeps the connection queued and serves it; macOS resets it
        // when accept fails.
        const auto first = client.ReadUntilClose(true);
#ifdef __linux__
        assert(!first.empty());
#endif
        assert(first.empty() || StatusLine(first) == "HTTP/1.1 200 OK");
    }
    assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) == "HTTP/1.1 200 OK");
}
#endif

void TestBindRefusals() {
    Rendering rendering;
    bool refused = false;
    try {
        MetricsHttpListener listener("0.0.0.0", 0, false, 5000, rendering.Function());
    } catch (const std::runtime_error& error) {
        refused = std::string_view(error.what()).find("--allow-remote") != std::string_view::npos;
    }
    assert(refused);
}

}  // namespace

int main() {
    StartSockets();
    TestResponses();
    TestScrapeOverSocket();
    TestRequestBound();
    TestReadTimeout();
    TestRenderFailure();
    TestSlowClientsDoNotBlock();
    TestConcurrentScrapes();
    TestConnectionCap();
    TestStopWithClients();
    TestEvictionKeepsResponses();
#ifndef _WIN32
    TestAcceptFailureDoesNotSpin();
#endif
    TestSlowRender();
    TestBindRefusals();
    std::puts("metrics_http tests passed");
    return 0;
}
