#pragma once
#include "chunkdb/server.hpp"
#include "server_io.hpp"
#include <deque>
#include <type_traits>
struct ssl_st;
namespace chunkdb {
struct ServerConnection {
    server_detail::SocketHandle socket = server_detail::kInvalidSocket;
    ssl_st* tls = nullptr;
    SessionState session;
    server_detail::PendingLineBuffer pending;
    bool watch_slot = false;
};
static_assert(std::is_nothrow_move_constructible_v<ServerConnection>);
static_assert(std::is_nothrow_move_assignable_v<ServerConnection>);
class FeedIo : public std::enable_shared_from_this<FeedIo> {
  public:
    explicit FeedIo(ChunkServer& server);
    ~FeedIo();
    void Start();
    void Stop();
    void Wake() noexcept;
    bool Add(std::shared_ptr<ServerConnection> connection);
  private:
    struct Watch {
        std::shared_ptr<ServerConnection> connection;
        std::deque<std::shared_ptr<const std::string>> output;
        std::size_t offset = 0, bytes = 0;
        int write_size = 0; // TLS retries pin the front buffer and this length.
        short write_wait = POLLOUT, read_wait = POLLIN;
        std::array<char, 4096> input{};
        bool unwatch = false, close = false, dead = false;
    };
    void Run();
    void DrainWake();
    void Pull(Watch& watch, std::size_t share);
    void Read(Watch& watch);
    void Write(Watch& watch);
    void WritePlain(Watch& watch);
    void Advance(Watch& watch, std::size_t bytes);
    void Queue(Watch& watch, std::shared_ptr<const std::string> bytes);
    void Reject(Watch& watch, std::string message);
    ChunkServer& server_;
    server_detail::SocketHandle wake_read_ = server_detail::kInvalidSocket;
    server_detail::SocketHandle wake_write_ = server_detail::kInvalidSocket;
    std::atomic<bool> signaled_{false};
    std::mutex mutex_;
    std::vector<std::shared_ptr<ServerConnection>> incoming_;
    std::thread thread_;
};
}
