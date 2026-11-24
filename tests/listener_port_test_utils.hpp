#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace chunkdb::test {

// A loopback TCP port for a test server to listen on, free when this returns.
//
// Ports come from 20000-32767, below every common ephemeral range (Linux
// 32768-60999, macOS and Windows 49152-65535). A port from bind(0) is an
// ephemeral port: once it is closed, the kernel hands it out as the local port
// of outgoing connections, and parallel test processes make many of those, so a
// server started on it later can find it taken. Ports below the ephemeral range
// are never assigned that way; they can be taken only by another listener,
// which the bind check sees. The check also skips ports Windows reserves.
inline std::uint16_t PickListenerPort() {
    constexpr std::uint16_t kFirst = 20000;
    constexpr std::uint16_t kLast = 32767;
    constexpr int kCandidates = 64;
#ifdef _WIN32
    using Socket = SOCKET;
    const Socket invalid = INVALID_SOCKET;
    const auto close_socket = [](Socket socket) { closesocket(socket); };
    static const int startup = [] { WSADATA data{}; return WSAStartup(MAKEWORD(2, 2), &data); }();
    if (startup != 0) throw std::runtime_error("WSAStartup failed: " + std::to_string(startup));
#else
    using Socket = int;
    const Socket invalid = -1;
    const auto close_socket = [](Socket socket) { close(socket); };
#endif
    static std::mutex mutex;
    // Parallel test processes must not start from the same sequence.
    static std::mt19937 random(static_cast<std::mt19937::result_type>(
        std::random_device{}() ^ static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())));
    std::uniform_int_distribution<int> distribution(kFirst, kLast);
    for (int candidate = 0; candidate < kCandidates; ++candidate) {
        std::uint16_t port = 0;
        {
            std::lock_guard lock(mutex);
            port = static_cast<std::uint16_t>(distribution(random));
        }
        const Socket socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (socket == invalid) throw std::runtime_error("cannot create a socket to check a listener port");
#ifdef _WIN32
        // Without exclusive use a bind on Windows can share a port another
        // socket already holds.
        int exclusive = 1;
        (void)setsockopt(socket, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&exclusive),
            sizeof(exclusive));
#endif
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        const bool bound = ::bind(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) == 0;
        close_socket(socket);
        if (bound) return port;
    }
    throw std::runtime_error("no free loopback listener port in 20000-32767 after 64 candidates");
}

}  // namespace chunkdb::test
