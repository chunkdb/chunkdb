#include "server_socket.hpp"
#include "server_io.hpp"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace {
using namespace chunkdb::server_detail;
struct Loopback {
    SocketHandle listener = kInvalidSocket, client = kInvalidSocket, accepted = kInvalidSocket;
    Loopback() {
#ifdef _WIN32
        static const int started = [] { WSADATA data{}; return WSAStartup(MAKEWORD(2, 2), &data); }();
        assert(started == 0);
#endif
        listener = CreateListenSocket("127.0.0.1", 0);
        sockaddr_in address{};
#ifdef _WIN32
        int length = sizeof(address);
#else
        socklen_t length = sizeof(address);
#endif
        assert(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);
        client = socket(AF_INET, SOCK_STREAM, 0); assert(client != kInvalidSocket);
        assert(connect(client, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        accepted = accept(listener, nullptr, nullptr); assert(accepted != kInvalidSocket);
    }
    ~Loopback() { for (const auto fd : {accepted, client, listener}) if (fd != kInvalidSocket) CloseSocket(fd); }
    void ClosePeer(bool reset) {
        if (reset) {
            const linger immediate{1, 0};
#ifdef _WIN32
            assert(setsockopt(client, SOL_SOCKET, SO_LINGER, reinterpret_cast<const char*>(&immediate), sizeof(immediate)) == 0);
#else
            assert(setsockopt(client, SOL_SOCKET, SO_LINGER, &immediate, sizeof(immediate)) == 0);
#endif
        }
        CloseSocket(client); client = kInvalidSocket;
        int code = 0;
        assert(WaitForSocketReady(accepted, true, false, std::chrono::seconds(5), &code) == SocketWaitResult::kReady);
    }
};
int InvalidArgument() {
#ifdef _WIN32
    return WSAEINVAL;
#else
    return EINVAL;
#endif
}
void LiveClientAndPendingData() {
    Loopback connection;
    std::string error; int code = -1;
    assert(ConfigureSocketRecvTimeout(connection.accepted, 5000U, &error, &code) && code == 0);
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    const auto endpoint = PeerEndpointForSocket(connection.accepted);
    assert(endpoint.find("127.0.0.1") != std::string::npos && endpoint.find(':') != std::string::npos);
    assert(send(connection.client, "x", 1, 0) == 1);
    assert(WaitForSocketReady(connection.accepted, true, false, std::chrono::seconds(5), &code) == SocketWaitResult::kReady);
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    char byte{}; assert(recv(connection.accepted, &byte, 1, 0) == 1 && byte == 'x');
}
void ClosedPeer(bool reset) {
    Loopback connection; connection.ClosePeer(reset);
    std::string error; int code = -1;
    const bool configured = ConfigureSocketRecvTimeout(connection.accepted, 5000U, &error, &code);
    std::printf("peer=%s configured=%d socket_error=%d error=%s\n", reset ? "reset" : "FIN", configured, code, error.c_str());
#ifdef __APPLE__
    if (reset) assert(!configured && code == EINVAL);
#endif
    assert(SocketTimeoutFailureIsPeerClose(connection.accepted, InvalidArgument()));
    assert(!SocketTimeoutFailureIsPeerClose(connection.accepted, 0));
}
int SocketOption(SocketHandle socket, int level, int option) {
    int value = 0;
#ifdef _WIN32
    int size = sizeof(value);
    assert(getsockopt(socket, level, option, reinterpret_cast<char*>(&value), &size) == 0);
#else
    socklen_t size = sizeof(value);
    assert(getsockopt(socket, level, option, &value, &size) == 0);
#endif
    return value;
}
void Keepalive() {
    Loopback connection;
    std::string error;
    for (const auto interval : {std::size_t{1}, std::size_t{1001}, std::size_t{60000}, kMaxTcpKeepaliveMs}) {
        assert(ConfigureTcpKeepalive(connection.accepted, interval, &error));
        assert(SocketOption(connection.accepted, SOL_SOCKET, SO_KEEPALIVE) != 0);
#ifndef _WIN32
#ifdef __APPLE__
        constexpr int idle_option = TCP_KEEPALIVE;
#else
        constexpr int idle_option = TCP_KEEPIDLE;
#endif
        const int seconds = static_cast<int>((interval + 999U) / 1000U);
        assert(SocketOption(connection.accepted, IPPROTO_TCP, idle_option) == seconds);
        assert(SocketOption(connection.accepted, IPPROTO_TCP, TCP_KEEPINTVL) == seconds);
        assert(SocketOption(connection.accepted, IPPROTO_TCP, TCP_KEEPCNT) == 3);
#endif
    }
    for (const auto interval : {std::size_t{0}, kMaxTcpKeepaliveMs + 1U})
        assert(!ConfigureTcpKeepalive(connection.accepted, interval, &error) && !error.empty());
    assert(!ConfigureTcpKeepalive(kInvalidSocket, 60000U, &error) && !error.empty());
}
void InvalidDescriptorRemainsFailure() {
    std::string error; int code = 0;
    assert(!ConfigureSocketRecvTimeout(kInvalidSocket, 5000U, &error, &code));
    assert(code != 0 && !error.empty());
    assert(!SocketTimeoutFailureIsPeerClose(kInvalidSocket, InvalidArgument()));
}
void SteadyReaderWriteDeadline() {
    Loopback connection;
    const int buffer_bytes = 65536;
    for (const auto& [socket, option] : {
             std::pair{connection.accepted, SO_SNDBUF}, std::pair{connection.client, SO_RCVBUF}}) {
        assert(setsockopt(socket, SOL_SOCKET, option, reinterpret_cast<const char*>(&buffer_bytes),
                          sizeof(buffer_bytes)) == 0);
    }
    std::string error;
    assert(SetSocketNonBlocking(connection.client, true, &error));
    std::atomic<bool> done{false};
    std::mutex mutex;
    std::condition_variable paced;
    auto reader = std::async(std::launch::async, [&] {
        std::size_t received = 0;
        char buffer[4096];
        auto next = std::chrono::steady_clock::now();
        while (!done.load(std::memory_order_acquire)) {
            int code = 0;
            if (WaitForSocketReady(connection.client, true, false, std::chrono::milliseconds(20), &code) !=
                SocketWaitResult::kReady) continue;
            const auto count = recv(connection.client, buffer, sizeof(buffer), 0);
            if (count <= 0) break;
            received += static_cast<std::size_t>(count);
            // Deliberately drain at most 4 MiB/s: progress continues much
            // more often than the deadline, but cannot consume the reply.
            next += std::chrono::milliseconds(1);
            std::unique_lock lock(mutex);
            (void)paced.wait_until(lock, next, [&] { return done.load(std::memory_order_acquire); });
        }
        return received;
    });
    const std::vector<char> reply(16U * 1024U * 1024U, 'x');
    ConnectionTermination termination;
    const auto start = std::chrono::steady_clock::now();
    const bool complete = WriteAllPlain(connection.accepted, reply.data(), reply.size(),
                                        start + std::chrono::milliseconds(300), &termination);
    done.store(true, std::memory_order_release);
    paced.notify_all();
    const auto received = reader.get();
    std::printf("steady reader: complete=%d received=%zu send_buffer=%d %s\n", complete, received,
                SocketOption(connection.accepted, SOL_SOCKET, SO_SNDBUF), termination.error.c_str());
    assert(!complete && received > 0U && received < reply.size());
    assert(termination.phase == "write" && termination.reason == "timeout");
    assert(termination.error.starts_with("reply write deadline exceeded after "));
    assert(std::chrono::steady_clock::now() - start < std::chrono::seconds(2));
}
}
int main() {
    Keepalive(); LiveClientAndPendingData(); ClosedPeer(false); ClosedPeer(true); InvalidDescriptorRemainsFailure();
    SteadyReaderWriteDeadline();
    std::puts("socket timeout passed: live, pending data, FIN, reset, invalid descriptor");
}
