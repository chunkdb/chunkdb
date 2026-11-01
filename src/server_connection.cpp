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
    const auto io = IdleIoHandle();
    if (!io) return false;
    auto& session = connection->session;
    try {
        while (running_.load()) {
            std::shared_ptr<StatementControl> control;
            {
                std::lock_guard lock(connection->record->mutex);
                control = connection->record->statement;
                connection->record->state = "running";
            }
            connection->worker_queued = false;
            {
                ScopedStatementControl context(control.get());
                const std::function<void(std::size_t)> reserve_reply = [&](std::size_t bytes) {
                    if (!io->ReserveReply(*connection, bytes, false))
                        throw ReplyAdmissionError("reply capture budget is full");
                };
                const auto& plan = connection->payload;
                connection->response = plan.plan == CommandEngine::PayloadPlan::kDiscard ?
                    engine_->RejectPayload(session, connection->request_line, plan.reject_response) :
                    !connection->payload_rejection.empty() ? engine_->RejectPayload(session, connection->request_line,
                        std::move(connection->payload_rejection)) :
                    engine_->Execute(session, connection->request_line, connection->parameters,
                        &connection->reply_continuation, &reserve_reply);
                if (const auto stopped = control->Complete()) {
                    connection->reply_continuation.reset();
                    connection->response = StatementStoppedReply(*stopped);
                    session.watch.reset();
                    if (session.slot_watch) session.slot_watch->Cancel();
                    session.slot_watch.reset();
                }
            }
            connection->parameters.clear();
            connection->payload_credit.reset();
            if (connection->response.starts_with('-')) {
                connection->reply_credit.reset();
                connection->reply_credit_bytes = 0U;
            }
            const auto encoded_buffer = connection->response.capacity() + (connection->reply_continuation ? 65536U : 0U);
            if (!io->ReserveReply(*connection, encoded_buffer, false)) {
                connection->reply_continuation.reset();
                std::string{}.swap(connection->response);
                io->Reply(*connection, Protocol::Error("BUSY", "reply capture budget is full", {{"reason", "reply_budget"}}));
                return ParkClient(connection);
            }
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
            { std::lock_guard lock(connection->record->mutex); connection->record->state = "writing"; }
            connection->response_write_size = 0;
            connection->phase = ServerConnection::Phase::kReply;
            connection->io_wait = POLLOUT;
            connection->output_deadline = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(config_.client_io_timeout_ms);
            if (!io->Write(*connection)) return false;
            if (connection->phase != ServerConnection::Phase::kInput || session.watch || session.slot_watch ||
                connection->pending.empty()) return ParkClient(connection);
            try { (void)io->Parse(*connection, true); }
            catch (const std::runtime_error& error) {
                if (!io->RejectProtocol(*connection, error)) return false;
            } catch (const std::invalid_argument& error) {
                if (!io->RejectProtocol(*connection, error)) return false;
            }
            if (connection->phase != ServerConnection::Phase::kReady) return ParkClient(connection);
        }
    } catch (const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer,
            "statement connection closed", {{"error", error.what()}});
    }
    return false;
}

bool ChunkServer::ParkClient(const std::shared_ptr<ServerConnection>& connection) {
    try {
        if (auto io = IdleIoHandle(); io && io->Add(connection)) return true;
    } catch (const std::exception& error) {
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
                .statement = connection->record->statement,
                .socket = static_cast<decltype(listen_socket_)>(connection->socket),
                .accepted_at = std::chrono::steady_clock::now()});
            connection->worker_queued = true;
            if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kAfterStatementQueued, connection->session.connection_id);
            pending_clients_cv_.notify_one();
            engine_->metrics()->SetPendingConnections(pending_clients_.size());
            return true;
        }
    }
    CloseClient(*connection);
    return true;
}
}  // namespace chunkdb
