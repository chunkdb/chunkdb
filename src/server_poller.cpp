#include "server_poller.hpp"

#include <limits>
#include <stdexcept>

namespace chunkdb::server_detail {
namespace {
bool WouldBlock(int error) {
#ifdef _WIN32
    return error == WSAEWOULDBLOCK;
#else
    return error == EAGAIN || error == EWOULDBLOCK;
#endif
}
}  // namespace

SocketPoller::WakeSockets::~WakeSockets() {
    CloseSocket(read);
    CloseSocket(write);
}

SocketPoller::SocketPoller(std::size_t max_sockets) : max_sockets_(max_sockets) {
    // Include the internal wake socket in the portable WSAPoll count. Check
    // before opening descriptors, allocating a list or narrowing its size.
    if (max_sockets > std::numeric_limits<std::uint32_t>::max() - 1U) {
        throw std::invalid_argument("socket poller capacity exceeds the portable descriptor count");
    }
#ifdef _WIN32
    WakeSockets listener;
    listener.read = CreateListenSocket("127.0.0.1", 0);
    sockaddr_in address{};
    int length = sizeof(address);
    if (getsockname(listener.read, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        throw std::runtime_error("socket poller wake getsockname: " + SocketErrorText());
    }
    wake_.write = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (wake_.write == kInvalidSocket || connect(wake_.write, reinterpret_cast<sockaddr*>(&address), length) != 0) {
        throw std::runtime_error("socket poller wake connect: " + SocketErrorText());
    }
    wake_.read = accept(listener.read, nullptr, nullptr);
#else
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
        throw std::runtime_error("socket poller wake socketpair: " + SocketErrorText());
    }
    wake_.read = pair[0];
    wake_.write = pair[1];
#endif
    std::string error;
    if (wake_.read == kInvalidSocket || !SetSocketNonBlocking(wake_.read, true, &error) ||
        !SetSocketNonBlocking(wake_.write, true, &error)) {
        throw std::runtime_error("socket poller wake nonblocking mode: " + error);
    }
#ifdef SO_NOSIGPIPE
    const int enabled = 1;
    if (setsockopt(wake_.write, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) != 0) {
        throw std::runtime_error("socket poller wake SO_NOSIGPIPE: " + SocketErrorText());
    }
#endif
}

void SocketPoller::Wake() noexcept {
    if (signaled_.exchange(true, std::memory_order_acq_rel)) return;
    const char byte = 1;
#ifdef MSG_NOSIGNAL
    constexpr int flags = MSG_NOSIGNAL;
#else
    constexpr int flags = 0;
#endif
    for (;;) {
        const auto result = send(wake_.write, &byte, 1, flags);
        if (result == 1) return;
        const int error = CurrentSocketErrorCode();
        if (result < 0 && WouldBlock(error)) return;
        if (result < 0 && IsSocketInterruptedError(error)) continue;
        ShutdownSocket(wake_.read); // A failed channel wakes the owner too.
        return;
    }
}

void SocketPoller::DrainWake() {
    char bytes[256];
    for (;;) {
        const auto result = recv(wake_.read, bytes, sizeof(bytes), 0);
        if (result > 0) continue;
        const int error = CurrentSocketErrorCode();
        if (result < 0 && IsSocketInterruptedError(error)) continue;
        if (result < 0 && WouldBlock(error)) break;
        throw std::runtime_error("socket poller wake channel closed");
    }
    // Acquire the Wake RMW chain, including publishers that saw true and
    // sent no byte. Their preceding work is visible to the following scan.
    (void)signaled_.exchange(false, std::memory_order_acq_rel);
}

bool SocketPoller::Wait(std::span<SocketInterest> interests, int timeout_ms) {
    if (interests.size() > max_sockets_) {
        throw std::invalid_argument("socket poller descriptor capacity exceeded");
    }
    if (timeout_ms < -1) {
        throw std::invalid_argument("socket poller timeout must be -1 or nonnegative");
    }
    descriptors_.clear();
    descriptors_.push_back({wake_.read, POLLIN, 0});
    for (auto& interest : interests) {
        interest.ready = 0;
        descriptors_.push_back({interest.socket, interest.events, 0});
    }
#ifdef _WIN32
    const int result = WSAPoll(descriptors_.data(), static_cast<ULONG>(descriptors_.size()), timeout_ms);
#else
    const int result = poll(descriptors_.data(), descriptors_.size(), timeout_ms);
#endif
    if (result < 0) {
        if (IsSocketInterruptedError(CurrentSocketErrorCode())) return false;
        throw std::runtime_error("socket poller wait: " + SocketErrorText());
    }
    for (std::size_t i = 0; i < interests.size(); ++i) {
        interests[i].ready = descriptors_[i + 1U].revents;
    }
    return result > 0;
}

}  // namespace chunkdb::server_detail
