#pragma once

#include "server_socket.hpp"

#include <span>
#include <vector>

namespace chunkdb::server_detail {

struct SocketInterest {
    SocketHandle socket = kInvalidSocket;
    short events = 0;
    short ready = 0;
};

// One I/O owner calls DrainWake and Wait. Other owners publish work before
// Wake. DrainWake must precede the owner's state scan, never follow it: a
// coalesced publication during draining is observed by that following scan.
// The owner also checks its stop predicate after scanning, before waiting.
class SocketPoller {
  public:
    explicit SocketPoller(std::size_t max_sockets);
    ~SocketPoller() = default;
    SocketPoller(const SocketPoller&) = delete;
    SocketPoller& operator=(const SocketPoller&) = delete;

    void Wake() noexcept;
    void DrainWake();
    // Negative one waits indefinitely; zero only inspects current readiness.
    // An interrupted wait returns to the owner to recompute absolute timers.
    // ready belongs to this invocation only, including on timeout/interruption.
    bool Wait(std::span<SocketInterest> interests, int timeout_ms);

  private:
    struct WakeSockets {
        SocketHandle read = kInvalidSocket;
        SocketHandle write = kInvalidSocket;
        ~WakeSockets();
    } wake_;
    const std::size_t max_sockets_;
    std::atomic<bool> signaled_{false};
#ifdef _WIN32
    std::vector<WSAPOLLFD> descriptors_;
#else
    std::vector<pollfd> descriptors_;
#endif
};

}  // namespace chunkdb::server_detail
