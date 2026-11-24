#pragma once

#include "server_socket.hpp"

#include <atomic>
#include <cstdint>
#include <span>
#include <unordered_map>
#include <vector>

#if defined(__linux__)
#define CHUNKDB_POLLER_EPOLL 1
#elif defined(__APPLE__) || defined(__FreeBSD__)
#define CHUNKDB_POLLER_KQUEUE 1
#endif

namespace chunkdb::server_detail {

struct SocketInterest {
    SocketHandle socket = kInvalidSocket;
    short events = 0;
    short ready = 0;
};

// The wake channel of one I/O owner. Other owners publish work before
// Wake. DrainWake must precede the owner's state scan, never follow it: a
// coalesced publication during draining is observed by that following scan.
// The owner also checks its stop predicate after scanning, before waiting.
// DrainWake reads the channel only when the previous wait reported it, or
// before the first wait; a byte that arrives later makes the next wait
// return at once, and that drain reads it.
class WakeChannel {
  public:
    WakeChannel();
    WakeChannel(const WakeChannel&) = delete;
    WakeChannel& operator=(const WakeChannel&) = delete;

    void Wake() noexcept;
    void DrainWake();
    [[nodiscard]] SocketHandle socket() const noexcept { return wake_.read; }
    // After a wait: whether it reported the channel.
    void Reported(bool readable) noexcept { wake_readable_ = wake_readable_ || readable; }
    // After an interrupted wait, which reports nothing.
    void Interrupted() noexcept { wake_readable_ = true; }

  private:
    struct WakeSockets {
        SocketHandle read = kInvalidSocket;
        SocketHandle write = kInvalidSocket;
        ~WakeSockets();
    } wake_;
    std::atomic<bool> signaled_{false};
    // Owner-only: whether the wake channel may hold unread bytes.
    bool wake_readable_ = true;
};

// A wait over a list of sockets given anew to each call.
class SocketPoller {
  public:
    explicit SocketPoller(std::size_t max_sockets);
    ~SocketPoller() = default;
    SocketPoller(const SocketPoller&) = delete;
    SocketPoller& operator=(const SocketPoller&) = delete;

    void Wake() noexcept { wake_.Wake(); }
    void DrainWake() { wake_.DrainWake(); }
    // Negative one waits indefinitely; zero only inspects current readiness.
    // An interrupted wait returns to the owner to recompute absolute timers.
    // ready belongs to this invocation only, including on timeout/interruption.
    bool Wait(std::span<SocketInterest> interests, int timeout_ms);

  private:
    // Checked before the wake channel opens any descriptor.
    const std::size_t max_sockets_;
    WakeChannel wake_;
#ifdef _WIN32
    std::vector<WSAPOLLFD> descriptors_;
#else
    std::vector<pollfd> descriptors_;
#endif
};

// Sockets kept registered while their interest is unchanged, so a wait costs
// what is ready rather than what is open: epoll on Linux, kqueue on macOS and
// FreeBSD, elsewhere one poll array kept between waits. One owner calls
// every member but Wake.
class ReadinessPoller {
  public:
    struct Ready {
        std::uint64_t token = 0;
        short events = 0;
    };
    ReadinessPoller();
    ~ReadinessPoller();
    ReadinessPoller(const ReadinessPoller&) = delete;
    ReadinessPoller& operator=(const ReadinessPoller&) = delete;

    void Wake() noexcept { wake_.Wake(); }
    void DrainWake() { wake_.DrainWake(); }
    // Waits for `events` (POLLIN, POLLOUT) on `socket`, reported as `token`;
    // zero events waits for nothing on it until a later Set.
    void Set(std::uint64_t token, SocketHandle socket, short events);
    // Forgets `token`, before its socket is reused for another registration.
    void Remove(std::uint64_t token);
    // Replaces `ready` with what the wait reported; a token can appear once
    // for input and once for output. Negative one waits indefinitely.
    bool Wait(int timeout_ms, std::vector<Ready>& ready);
    [[nodiscard]] std::size_t registered() const noexcept { return registered_.size(); }

  private:
    struct Registration {
        SocketHandle socket = kInvalidSocket;
        short events = 0;
    };
    void Apply(std::uint64_t token, Registration& current, short events);
    WakeChannel wake_;
    std::unordered_map<std::uint64_t, Registration> registered_;
#if defined(CHUNKDB_POLLER_EPOLL) || defined(CHUNKDB_POLLER_KQUEUE)
    int kernel_ = -1;
#else
    // The wake channel first, then every socket waited on, with its token.
#ifdef _WIN32
    std::vector<WSAPOLLFD> descriptors_;
#else
    std::vector<pollfd> descriptors_;
#endif
    std::vector<std::uint64_t> tokens_;
    std::unordered_map<std::uint64_t, std::size_t> index_;
#endif
};

}  // namespace chunkdb::server_detail
