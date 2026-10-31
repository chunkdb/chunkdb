#pragma once

#include "server_poller.hpp"

#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace chunkdb {
class ChunkServer;
struct ServerConnection;

// Owns greeted connections between requests; workers never wait for idle input.
class IdleIo {
  public:
    explicit IdleIo(ChunkServer& server);
    ~IdleIo();
    void Start();
    void Stop();
    void Wake() noexcept;
    bool Add(std::shared_ptr<ServerConnection> connection);

  private:
    void Run();
    ChunkServer& server_;
    server_detail::SocketPoller poller_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<ServerConnection>> incoming_;
    std::thread thread_;
};
}  // namespace chunkdb
