#include "chunkdb/server.hpp"

#include "chunkdb/logging.hpp"
#include "server_feed.hpp"
#include "server_idle.hpp"

namespace chunkdb {
using namespace server_detail;

bool ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
    , const std::shared_ptr<ServerConnection>& connection) {
    (void)client_socket;
    if (connection->phase != ServerConnection::Phase::kReady)
        throw std::logic_error("statement worker requires a complete request");
    auto& session = connection->session;
    const auto& plan = connection->payload;
    connection->response = plan.plan == CommandEngine::PayloadPlan::kDiscard ?
        engine_->RejectPayload(session, connection->request_line, plan.reject_response) :
        !connection->payload_rejection.empty() ? engine_->RejectPayload(session, connection->request_line,
            std::move(connection->payload_rejection)) :
        engine_->Execute(session, connection->request_line, connection->parameters, &connection->reply_continuation);
    connection->parameters.clear();
    connection->payload_credit.reset();
    if (session.watch || session.slot_watch) {
        auto count = watch_count_.load();
        while (count < config_.max_watches && !watch_count_.compare_exchange_weak(count, count + 1U)) {}
        if (count >= config_.max_watches) {
            session.watch.reset();
            if (session.slot_watch) session.slot_watch->Cancel();
            session.slot_watch.reset();
            connection->response = Protocol::Error("BUSY", "maximum watches reached");
        } else connection->watch_slot = true;
    }
    connection->response_offset = 0U;
    connection->response_write_size = 0;
    connection->phase = ServerConnection::Phase::kReply;
    connection->io_wait = POLLOUT;
    connection->output_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(config_.client_io_timeout_ms);
    return ParkClient(connection);
}

bool ChunkServer::ParkClient(const std::shared_ptr<ServerConnection>& connection) {
    try {
        if (auto io = IdleIoHandle(); io && io->Add(connection)) return true;
    } catch (const std::bad_alloc& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer,
            "connection transport hand-off failed", {{"error", error.what()}});
    }
    return false;
}

bool ChunkServer::DispatchClient(const std::shared_ptr<ServerConnection>& connection) {
    {
        std::lock_guard lock(pending_clients_mutex_);
        if (running_.load()) {
            if (pending_clients_.size() >= config_.max_pending_clients) {
                if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                    hook->Run(FeedDeliveryTestHook::Point::kIdleQueueBackpressure, pending_clients_.size());
                return false;
            }
            if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeResumedClientQueue, 0U);
            pending_clients_.push(PendingClient{.resumed = connection,
                .socket = static_cast<decltype(listen_socket_)>(connection->socket),
                .accepted_at = std::chrono::steady_clock::now()});
            pending_clients_cv_.notify_one();
            engine_->metrics()->SetPendingConnections(pending_clients_.size());
            return true;
        }
    }
    CloseClient(*connection);
    return true;
}
}  // namespace chunkdb
