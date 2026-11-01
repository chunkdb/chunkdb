#pragma once

#include "server_poller.hpp"
#include "server_credit.hpp"

#include <memory>
#include <atomic>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

namespace chunkdb {
class ChunkServer;
class StatementControl;
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
    friend class ChunkServer;
    void Run();
    bool Initialize(ServerConnection& connection);
    bool Progress(ServerConnection& connection);
    bool Read(ServerConnection& connection);
    bool Parse(ServerConnection& connection, bool worker = false);
    bool Write(ServerConnection& connection);
    void Reply(ServerConnection& connection, std::string response);
    bool ReserveReply(ServerConnection& connection, std::size_t bytes, bool control);
    bool RejectProtocol(ServerConnection& connection, const std::exception& error);
    void PumpCredits();
    void CheckStatements();
    ChunkServer& server_;
    server_detail::SocketPoller poller_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<ServerConnection>> incoming_;
    std::thread thread_;
    std::shared_ptr<server_detail::ConnectionByteBudget> data_credit_, control_credit_, line_credit_, data_line_credit_;
    std::shared_ptr<server_detail::ConnectionByteBudget> data_output_credit_, control_output_credit_;
    std::deque<std::weak_ptr<ServerConnection>> data_waiters_, control_waiters_;
    std::atomic<std::size_t> data_waiter_count_{0U};
    struct TrackedStatement {
        std::shared_ptr<ServerConnection> connection;
        std::shared_ptr<StatementControl> statement;
        bool io_claimed = false;
    };
    std::vector<TrackedStatement> statements_;
    std::vector<TrackedStatement> incoming_statements_;
    std::vector<std::shared_ptr<ServerConnection>> clients_returned_;
};
}  // namespace chunkdb
