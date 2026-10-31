#include "server_idle.hpp"

#include "chunkdb/logging.hpp"
#include "server_feed.hpp"

#include <algorithm>
#include <climits>
#include <limits>

namespace chunkdb {
using namespace server_detail;

IdleIo::IdleIo(ChunkServer& server)
    : server_(server), poller_(std::numeric_limits<std::uint32_t>::max() - 1U) {}

IdleIo::~IdleIo() { Stop(); }

void IdleIo::Start() { thread_ = std::thread([this] { Run(); }); }

void IdleIo::Stop() {
    Wake();
    if (thread_.joinable()) thread_.join();
}

void IdleIo::Wake() noexcept { poller_.Wake(); }

bool IdleIo::Add(std::shared_ptr<ServerConnection> connection) {
    {
        std::lock_guard lock(mutex_);
        if (!server_.running_.load()) return false;
        connection->idle_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(server_.config_.idle_connection_timeout_ms);
        incoming_.push_back(std::move(connection));
    }
    Wake();
    return true;
}

void IdleIo::Run() {
    std::vector<std::shared_ptr<ServerConnection>> idle;
    std::vector<SocketInterest> interests;
    const auto fail = [&](const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer,
            "idle connection polling terminated", {{"error", error.what()}});
        server_.Stop();
    };
    try {
        while (server_.running_.load()) {
            poller_.DrainWake();
            {
                std::lock_guard lock(mutex_);
                idle.insert(idle.end(), std::make_move_iterator(incoming_.begin()),
                    std::make_move_iterator(incoming_.end()));
                incoming_.clear();
            }
            const auto now = std::chrono::steady_clock::now();
            bool saturated = false;
            for (auto& connection : idle) {
                if (now >= connection->idle_deadline) {
                    LogMessage(LogLevel::kWarn, LogComponent::kServer, "connection terminated", {
                        {"phase", "read"}, {"reason", "timeout"},
                        {"error", "idle connection deadline exceeded"},
                        {"socket", std::to_string(connection->socket)},
                        {"peer", connection->peer_endpoint},
                    });
                    server_.CloseClient(*connection);
                    connection.reset();
                } else if (connection->read_ready) {
                    if (server_.DispatchClient(connection)) connection.reset();
                    else saturated = true;
                }
            }
            std::erase(idle, nullptr);
            interests.clear();
            interests.reserve(idle.size());
            int timeout = -1;
            for (const auto& connection : idle) {
                if (!saturated) interests.push_back({connection->socket, POLLIN, 0});
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
                    connection->idle_deadline - now).count();
                const auto bounded = static_cast<int>(std::clamp<std::int64_t>(remaining, 0, INT_MAX));
                timeout = timeout < 0 ? bounded : std::min(timeout, bounded);
            }
            // A coalesced stop publication can be consumed by DrainWake.
            if (!server_.running_.load()) break;
            (void)poller_.Wait(interests, timeout);
            if (saturated) continue; // Worker queue credit explicitly wakes us.
            for (std::size_t i = 0U; i < idle.size(); ++i) {
                if (interests[i].ready == 0) continue;
                idle[i]->read_ready = true;
            }
        }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    for (const auto& connection : idle)
        if (connection) server_.CloseClient(*connection);
    std::lock_guard lock(mutex_);
    for (const auto& connection : incoming_) server_.CloseClient(*connection);
    incoming_.clear();
}
}  // namespace chunkdb
