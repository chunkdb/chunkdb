#include "server_poller.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <string>

#if defined(CHUNKDB_POLLER_EPOLL)
#include <sys/epoll.h>
#elif defined(CHUNKDB_POLLER_KQUEUE)
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace chunkdb::server_detail {
namespace {
bool WouldBlock(int error) {
#ifdef _WIN32
    return error == WSAEWOULDBLOCK;
#else
    return error == EAGAIN || error == EWOULDBLOCK;
#endif
}

std::size_t CheckedCapacity(std::size_t max_sockets) {
    // Include the internal wake socket in the portable WSAPoll count.
    if (max_sockets > std::numeric_limits<std::uint32_t>::max() - 1U) {
        throw std::invalid_argument("socket poller capacity exceeds the portable descriptor count");
    }
    return max_sockets;
}

// The token of the wake channel; connection tokens count up from one.
constexpr std::uint64_t kWakeToken = std::numeric_limits<std::uint64_t>::max();
}  // namespace

WakeChannel::WakeSockets::~WakeSockets() {
    CloseSocket(read);
    CloseSocket(write);
}

WakeChannel::WakeChannel() {
#ifdef _WIN32
    WakeSockets listener;
    listener.read = CreateListenSocket("127.0.0.1", 0);
    sockaddr_in address{};
    int length = sizeof(address);
    if (getsockname(listener.read, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        throw std::runtime_error("socket poller wake getsockname: " + SocketErrorText());
    }
    wake_.write = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
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

void WakeChannel::Wake() noexcept {
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

void WakeChannel::DrainWake() {
    char bytes[256];
    for (; wake_readable_;) {
        const auto result = recv(wake_.read, bytes, sizeof(bytes), 0);
        if (result > 0) continue;
        const int error = CurrentSocketErrorCode();
        if (result < 0 && IsSocketInterruptedError(error)) continue;
        if (result < 0 && WouldBlock(error)) break;
        throw std::runtime_error("socket poller wake channel closed");
    }
    wake_readable_ = false;
    // Acquire the Wake RMW chain, including publishers that saw true and
    // sent no byte. Their preceding work is visible to the following scan.
    (void)signaled_.exchange(false, std::memory_order_acq_rel);
}

SocketPoller::SocketPoller(std::size_t max_sockets) : max_sockets_(CheckedCapacity(max_sockets)) {}

bool SocketPoller::Wait(std::span<SocketInterest> interests, int timeout_ms) {
    if (interests.size() > max_sockets_) {
        throw std::invalid_argument("socket poller descriptor capacity exceeded");
    }
    if (timeout_ms < -1) {
        throw std::invalid_argument("socket poller timeout must be -1 or nonnegative");
    }
    descriptors_.clear();
    descriptors_.push_back({wake_.socket(), POLLIN, 0});
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
        if (IsSocketInterruptedError(CurrentSocketErrorCode())) { wake_.Interrupted(); return false; }
        throw std::runtime_error("socket poller wait: " + SocketErrorText());
    }
    wake_.Reported(descriptors_[0].revents != 0);
    for (std::size_t i = 0; i < interests.size(); ++i) {
        interests[i].ready = descriptors_[i + 1U].revents;
    }
    return result > 0;
}

ReadinessPoller::ReadinessPoller() {
#if defined(CHUNKDB_POLLER_EPOLL)
    kernel_ = epoll_create1(EPOLL_CLOEXEC);
    if (kernel_ < 0) throw std::runtime_error("readiness poller epoll_create1: " + SocketErrorText());
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = kWakeToken;
    if (epoll_ctl(kernel_, EPOLL_CTL_ADD, wake_.socket(), &event) != 0) {
        const auto error = SocketErrorText();
        (void)close(kernel_);
        throw std::runtime_error("readiness poller wake registration: " + error);
    }
#elif defined(CHUNKDB_POLLER_KQUEUE)
    kernel_ = kqueue();
    if (kernel_ < 0) throw std::runtime_error("readiness poller kqueue: " + SocketErrorText());
    struct kevent change {};
    EV_SET(&change, static_cast<uintptr_t>(wake_.socket()), EVFILT_READ, EV_ADD, 0, 0,
        reinterpret_cast<void*>(static_cast<std::uintptr_t>(kWakeToken)));
    if (kevent(kernel_, &change, 1, nullptr, 0, nullptr) != 0) {
        const auto error = SocketErrorText();
        (void)close(kernel_);
        throw std::runtime_error("readiness poller wake registration: " + error);
    }
#else
    descriptors_.push_back({wake_.socket(), POLLIN, 0});
    tokens_.push_back(kWakeToken);
#endif
}

ReadinessPoller::~ReadinessPoller() {
#if defined(CHUNKDB_POLLER_EPOLL) || defined(CHUNKDB_POLLER_KQUEUE)
    if (kernel_ >= 0) (void)close(kernel_);
#endif
}

void ReadinessPoller::Set(std::uint64_t token, SocketHandle socket, short events) {
    if (token == kWakeToken) throw std::invalid_argument("readiness poller token is reserved");
    events = static_cast<short>(events & (POLLIN | POLLOUT));
    auto [found, added] = registered_.try_emplace(token, Registration{socket, 0});
    if (!added && found->second.socket != socket)
        throw std::logic_error("readiness poller token names another socket");
    if (found->second.events == events) return;
    Apply(token, found->second, events);
}

void ReadinessPoller::Remove(std::uint64_t token) {
    const auto found = registered_.find(token);
    if (found == registered_.end()) return;
    if (found->second.events != 0) Apply(token, found->second, 0);
    registered_.erase(found);
}

void ReadinessPoller::Forget(std::uint64_t token) {
    const auto found = registered_.find(token);
    if (found == registered_.end()) return;
#if !defined(CHUNKDB_POLLER_EPOLL) && !defined(CHUNKDB_POLLER_KQUEUE)
    // The kept array holds the closed descriptor until it is taken out.
    if (found->second.events != 0) Apply(token, found->second, 0);
#endif
    registered_.erase(found);
}

void ReadinessPoller::Apply(std::uint64_t token, Registration& current, short events) {
#if defined(CHUNKDB_POLLER_EPOLL)
    epoll_event event{};
    event.events = ((events & POLLIN) != 0 ? static_cast<std::uint32_t>(EPOLLIN) : 0U) |
        ((events & POLLOUT) != 0 ? static_cast<std::uint32_t>(EPOLLOUT) : 0U);
    event.data.u64 = token;
    // Zero interest leaves the set: epoll would report a hang-up forever.
    const int operation = events == 0 ? EPOLL_CTL_DEL : current.events == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    if (epoll_ctl(kernel_, operation, current.socket, &event) != 0)
        throw std::runtime_error("readiness poller registration: " + SocketErrorText());
#elif defined(CHUNKDB_POLLER_KQUEUE)
    const auto change = [&](short filter, bool wanted, bool held) {
        if (wanted == held) return;
        struct kevent update {};
        EV_SET(&update, static_cast<uintptr_t>(current.socket), filter, wanted ? EV_ADD : EV_DELETE, 0, 0,
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(token)));
        if (kevent(kernel_, &update, 1, nullptr, 0, nullptr) != 0 && wanted)
            throw std::runtime_error("readiness poller registration: " + SocketErrorText());
    };
    change(EVFILT_READ, (events & POLLIN) != 0, (current.events & POLLIN) != 0);
    change(EVFILT_WRITE, (events & POLLOUT) != 0, (current.events & POLLOUT) != 0);
#else
    const auto found = index_.find(token);
    if (events == 0) {
        if (found != index_.end()) {
            const auto at = found->second;
            const auto last = descriptors_.size() - 1U;
            if (at != last) {
                descriptors_[at] = descriptors_[last];
                tokens_[at] = tokens_[last];
                index_[tokens_[at]] = at;
            }
            descriptors_.pop_back();
            tokens_.pop_back();
            index_.erase(found);
        }
    } else if (found == index_.end()) {
        constexpr auto limit = std::numeric_limits<std::uint32_t>::max();
        const auto at = descriptors_.size();
        if (at >= limit)
            throw std::invalid_argument("socket poller descriptor capacity exceeded");
        // Grow both arrays before publishing an index. Allocation failure
        // can change capacity, but must leave the active entries aligned.
        if (at == descriptors_.capacity() || at == tokens_.capacity()) {
            const auto capacity = at + std::min(at, static_cast<std::size_t>(limit) - at);
            descriptors_.reserve(capacity);
            tokens_.reserve(capacity);
        }
        index_.emplace(token, at);
        descriptors_.push_back({current.socket, events, 0});
        tokens_.push_back(token);
    } else {
        descriptors_[found->second].events = events;
    }
#endif
    current.events = events;
}

bool ReadinessPoller::Wait(int timeout_ms, std::vector<Ready>& ready) {
    if (timeout_ms < -1) {
        throw std::invalid_argument("socket poller timeout must be -1 or nonnegative");
    }
    ready.clear();
#if defined(CHUNKDB_POLLER_EPOLL)
    std::array<epoll_event, 256> events{};
    const int count = epoll_wait(kernel_, events.data(), static_cast<int>(events.size()), timeout_ms);
    if (count < 0) {
        if (errno == EINTR) { wake_.Interrupted(); return false; }
        throw std::runtime_error("readiness poller wait: " + SocketErrorText());
    }
    for (int i = 0; i < count; ++i) {
        const auto token = events[static_cast<std::size_t>(i)].data.u64;
        const auto flags = events[static_cast<std::size_t>(i)].events;
        if (token == kWakeToken) { wake_.Reported(true); continue; }
        ready.push_back({token, static_cast<short>(((flags & EPOLLIN) != 0 ? POLLIN : 0) |
            ((flags & EPOLLOUT) != 0 ? POLLOUT : 0) | ((flags & EPOLLERR) != 0 ? POLLERR : 0) |
            ((flags & EPOLLHUP) != 0 ? POLLHUP : 0))});
    }
    return count > 0;
#elif defined(CHUNKDB_POLLER_KQUEUE)
    std::array<struct kevent, 256> events{};
    timespec wait{};
    if (timeout_ms >= 0) {
        wait.tv_sec = timeout_ms / 1000;
        wait.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
    }
    const int count = kevent(kernel_, nullptr, 0, events.data(), static_cast<int>(events.size()),
        timeout_ms < 0 ? nullptr : &wait);
    if (count < 0) {
        if (errno == EINTR) { wake_.Interrupted(); return false; }
        throw std::runtime_error("readiness poller wait: " + SocketErrorText());
    }
    for (int i = 0; i < count; ++i) {
        const auto& event = events[static_cast<std::size_t>(i)];
        const auto token = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(event.udata));
        if (token == kWakeToken) { wake_.Reported(true); continue; }
        auto reported = static_cast<short>(event.filter == EVFILT_READ ? POLLIN : event.filter == EVFILT_WRITE ? POLLOUT : 0);
        if ((event.flags & EV_EOF) != 0) reported = static_cast<short>(reported | POLLHUP);
        if ((event.flags & EV_ERROR) != 0) reported = static_cast<short>(reported | POLLERR);
        ready.push_back({token, reported});
    }
    return count > 0;
#else
    for (auto& descriptor : descriptors_) descriptor.revents = 0;
#ifdef _WIN32
    const int result = WSAPoll(descriptors_.data(), static_cast<ULONG>(descriptors_.size()), timeout_ms);
#else
    const int result = poll(descriptors_.data(), descriptors_.size(), timeout_ms);
#endif
    if (result < 0) {
        if (IsSocketInterruptedError(CurrentSocketErrorCode())) { wake_.Interrupted(); return false; }
        throw std::runtime_error("readiness poller wait: " + SocketErrorText());
    }
    wake_.Reported(descriptors_[0].revents != 0);
    for (std::size_t i = 1U; i < descriptors_.size() && result > 0; ++i)
        if (descriptors_[i].revents != 0) ready.push_back({tokens_[i], descriptors_[i].revents});
    return result > 0;
#endif
}

}  // namespace chunkdb::server_detail
