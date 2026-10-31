#pragma once

#include "server_poller.hpp"
#include "server_credit.hpp"

#include <memory>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

namespace chunkdb {
class ChunkServer;
struct ServerConnection;

// Owns transport phases; statement workers receive only complete requests.
class IdleIo : public std::enable_shared_from_this<IdleIo> {
  public:
    explicit IdleIo(ChunkServer& server);
    ~IdleIo();
    void Start();
    void Stop();
    void Wake() noexcept;
    bool Add(std::shared_ptr<ServerConnection> connection);

  private:
    void Run();
    bool Initialize(ServerConnection& connection);
    bool Progress(ServerConnection& connection);
    bool Read(ServerConnection& connection);
    bool Parse(ServerConnection& connection);
    bool Write(ServerConnection& connection);
    void Reply(ServerConnection& connection, std::string response);
    void PumpCredits();
    ChunkServer& server_;
    server_detail::SocketPoller poller_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<ServerConnection>> incoming_;
    std::thread thread_;
    std::shared_ptr<server_detail::ConnectionByteBudget> data_credit_, control_credit_, line_credit_, data_line_credit_;
    std::deque<std::weak_ptr<ServerConnection>> data_waiters_, control_waiters_;
};
}  // namespace chunkdb
