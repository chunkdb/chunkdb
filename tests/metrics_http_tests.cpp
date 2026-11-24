// The metrics HTTP listener: responses, refusals, the request bound, the
// read timeout, concurrent and slow clients, the connection cap and stopping.

#include "metrics_http.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

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

    // Everything the server sends until it closes, within 10 seconds.
    std::string ReadUntilClose() {
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

struct Rendering {
    std::atomic<int> calls{0};
    std::function<std::string()> Function() {
        return [this] {
            calls.fetch_add(1);
            return kBody;
        };
    }
};

void TestResponses() {
    Rendering rendering;
    const auto render = rendering.Function();
    const auto ok = MetricsHttpResponse("GET /metrics HTTP/1.1\r\nHost: localhost:9100\r\nAccept: */*", render);
    assert(StatusLine(ok) == "HTTP/1.1 200 OK");
    assert(ok.find("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n") != std::string::npos);
    assert(ok.find("\r\nContent-Length: " + std::to_string(kBody.size()) + "\r\n") != std::string::npos);
    assert(ok.find("\r\nConnection: close\r\n") != std::string::npos);
    assert(BodyOf(ok) == kBody);
    assert(StatusLine(MetricsHttpResponse("GET /metrics?name[]=x HTTP/1.0", render)) == "HTTP/1.1 200 OK");
    assert(StatusLine(MetricsHttpResponse("GET /metrics HTTP/1.1\nHost: x", render)) == "HTTP/1.1 200 OK");
    assert(rendering.calls.load() == 3);

    for (const char* head : {"GET / HTTP/1.1", "GET /metricsx HTTP/1.1", "GET /metrics/ HTTP/1.1",
                             "GET /other HTTP/1.1", "POST /other HTTP/1.1"}) {
        const auto response = MetricsHttpResponse(head, render);
        assert(StatusLine(response) == "HTTP/1.1 404 Not Found");
        assert(BodyOf(response) == "404 Not Found\n");
    }
    for (const char* head : {"POST /metrics HTTP/1.1", "HEAD /metrics HTTP/1.1", "PUT /metrics?x HTTP/1.0"}) {
        const auto response = MetricsHttpResponse(head, render);
        assert(StatusLine(response) == "HTTP/1.1 405 Method Not Allowed");
        assert(response.find("\r\nAllow: GET\r\n") != std::string::npos);
    }
    for (const char* head : {"", "GARBAGE", "GET /metrics", "GET  /metrics HTTP/1.1", "GET /metrics HTTP/1.1 extra",
                             "get /metrics HTTP/1.1", "GET metrics HTTP/1.1", "GET /metrics HTTP/2.0",
                             "PRI * HTTP/2.0", "GET /metrics FTP/1.1"}) {
        assert(StatusLine(MetricsHttpResponse(head, render)) == "HTTP/1.1 400 Bad Request");
    }
    // Refused requests do not render the metrics.
    assert(rendering.calls.load() == 3);
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

void TestRenderFailure() {
    MetricsHttpListener listener("127.0.0.1", 0, false, 5000, []() -> std::string {
        throw std::runtime_error("no metrics");
    });
    assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) ==
           "HTTP/1.1 500 Internal Server Error");
    assert(StatusLine(Exchange(listener.port(), "GET /metrics HTTP/1.1\r\n\r\n")) ==
           "HTTP/1.1 500 Internal Server Error");
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
    assert(rendering.calls.load() == 7);
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
    assert(rendering.calls.load() == kThreads * kScrapes);
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

void TestBindRefusals() {
    Rendering rendering;
    bool refused = false;
    try {
        MetricsHttpListener listener("0.0.0.0", 0, false, 5000, rendering.Function());
    } catch (const std::runtime_error& error) {
        refused = std::string_view(error.what()).find("--metrics-allow-remote") != std::string_view::npos;
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
    TestBindRefusals();
    std::puts("metrics_http tests passed");
    return 0;
}
