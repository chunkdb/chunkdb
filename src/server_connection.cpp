#include "chunkdb/server.hpp"

#include "chunkdb/logging.hpp"
#include "server_feed.hpp"
#include "server_idle.hpp"

#include <algorithm>
#include <climits>

namespace chunkdb {
using namespace server_detail;

bool ChunkServer::HandleClient(
#ifdef _WIN32
    std::uintptr_t client_socket
#else
    int client_socket
#endif
    , const std::shared_ptr<ServerConnection>& connection, WorkerWait& wait) {
    (void)client_socket;
    if (connection->phase != ServerConnection::Phase::kReady)
        throw std::logic_error("statement worker requires a complete request");
    const auto io = IdleIoHandle();
    if (!io) return false;
    auto& session = connection->session;
    // A connection handed back is the I/O thread's to dispatch: free the
    // worker first, so that dispatch counts it.
    const auto park = [&] { FreeWorker(wait); return ParkClient(connection); };
    try {
        while (running_.load()) {
            std::shared_ptr<StatementControl> control;
            {
                std::lock_guard lock(connection->record->mutex);
                control = connection->record->statement;
                connection->record->state = "running";
            }
            connection->worker_queued = false;
            if (!io->ReserveReply(*connection, 65536U, false)) {
                const auto stopped = control->Complete();
                connection->parameters.clear();
                connection->payload_credit.reset();
                io->Reply(*connection, stopped ? StatementStoppedReply(*stopped) :
                    Protocol::Error("BUSY", "reply admission budget is full", {{"reason", "reply_budget"}}));
                return park();
            }
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
                connection->irreversible_reply = control->decision_entered();
            }
            connection->parameters.clear();
            connection->payload_credit.reset();
            const auto encoded_buffer = connection->response.capacity() + (connection->reply_continuation ? 65536U : 0U);
            if (connection->record->killed.load() && !connection->irreversible_reply) return false;
            if (!io->ReserveReply(*connection, encoded_buffer, false)) {
                // A lost reply after an irreversible decision is ambiguous, never a retryable refusal.
                if (control->decision_entered()) return false;
                connection->reply_continuation.reset();
                std::string{}.swap(connection->response);
                io->Reply(*connection, Protocol::Error("BUSY", "reply capture budget is full", {{"reason", "reply_budget"}}));
                return park();
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
            if (connection->phase != ServerConnection::Phase::kInput || session.watch || session.slot_watch)
                return park();
            while (connection->pending.empty()) {
                const auto next = AwaitNextRequest(wait, *connection);
                if (next == NextRequest::kClose) return false;
                if (next == NextRequest::kReturn) return park();
                connection->read_ready = false;
                if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                    hook->Run(FeedDeliveryTestHook::Point::kBeforeClientRead, connection->completed_replies);
                const bool alive = io->Read(*connection);
                if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                    hook->Run(FeedDeliveryTestHook::Point::kAfterClientRead, connection->completed_replies);
                if (!alive) return false;
                // Nothing arrived: keep waiting, unless part of a TLS record, a TLS
                // write or credit is waited for, which the I/O owner times.
                if (connection->pending.empty() && (connection->input_deadline || connection->io_wait != POLLIN ||
                    connection->waiting_line_credit || connection->waiting_credit)) return park();
            }
            try { (void)io->Parse(*connection, true); }
            catch (const std::runtime_error& error) {
                if (!io->RejectProtocol(*connection, error)) return false;
            } catch (const std::invalid_argument& error) {
                if (!io->RejectProtocol(*connection, error)) return false;
            }
            if (connection->phase != ServerConnection::Phase::kReady) return park();
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

// The worker that wrote a reply keeps its idle connection while no queued
// request needs a worker, so the next request needs no hand-off. A queued
// request, KILL, shutdown and the idle deadline return or close it first.
ChunkServer::NextRequest ChunkServer::AwaitNextRequest(WorkerWait& wait, ServerConnection& connection) {
    using Clock = std::chrono::steady_clock;
    if (wait.failed) return NextRequest::kReturn;
    // Decrypted bytes may already wait inside the TLS session.
    if (connection.read_ready) return NextRequest::kRead;
    const auto stop_waiting = [&](const std::runtime_error& error) {
        wait.failed = true;
        LogMessage(LogLevel::kError, LogComponent::kServer,
            "statement worker returns idle connections to the I/O thread", {{"error", error.what()}});
        return NextRequest::kReturn;
    };
    for (;;) {
        // Drain before the scan below, so a wake published after it is kept.
        try { wait.poller.DrainWake(); }
        catch (const std::runtime_error& error) { return stop_waiting(error); }
        struct Registration {
            ChunkServer& server;
            WorkerWait& wait;
            bool yielded = false;
            void Leave() noexcept {
                std::lock_guard lock(server.pending_clients_mutex_);
                if (!wait.waiting) return;
                wait.waiting = false;
                wait.connection = nullptr;
                yielded = std::exchange(wait.yield, false);
                if (yielded) --server.yielding_workers_;
            }
            ~Registration() { Leave(); }
        } registration{*this, wait};
        {
            std::lock_guard lock(pending_clients_mutex_);
            // Queued requests that free workers will take need no yield.
            if (!running_.load() || pending_clients_.size() > free_workers_ + yielding_workers_)
                return NextRequest::kReturn;
            wait.waiting = true;
            wait.connection = &connection;
        }
        // A KILL published before the registration sent no wake; one after it did.
        bool ready = false;
        if (!connection.record->killed.load()) {
            // From here on only the wake reports a yield, KILL or stop.
            if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeWorkerIdleWait, connection.session.connection_id);
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
                connection.idle_deadline - Clock::now()).count();
            SocketInterest interest{connection.socket, POLLIN, 0};
            try {
                ready = wait.poller.Wait(std::span(&interest, 1U),
                    static_cast<int>(std::clamp<std::int64_t>(remaining, 0, INT_MAX))) && interest.ready != 0;
            } catch (const std::runtime_error& error) {
                registration.Leave();
                return stop_waiting(error);
            }
        }
        registration.Leave();
        if (connection.record->killed.load()) return NextRequest::kClose;
        if (registration.yielded || !running_.load() || Clock::now() >= connection.idle_deadline) {
            // A request that arrived by then is the I/O thread's to read, as if it had polled.
            connection.read_ready = ready;
            return NextRequest::kReturn;
        }
        if (ready) return NextRequest::kRead;
    }
}

void ChunkServer::FreeWorker(WorkerWait& wait) noexcept {
    std::lock_guard lock(pending_clients_mutex_);
    if (!std::exchange(wait.holding, false)) return;
    ++free_workers_;
}

void ChunkServer::WakeWaitingWorker(const ServerConnection& connection) {
    std::lock_guard lock(pending_clients_mutex_);
    for (const auto& wait : worker_waits_)
        if (wait->waiting && wait->connection == &connection) wait->poller.Wake();
}

bool ChunkServer::DispatchClient(const std::shared_ptr<ServerConnection>& connection) {
    {
        std::lock_guard lock(pending_clients_mutex_);
        if (running_.load()) {
            if (pending_clients_.size() >= config_.max_pending_clients) {
                dispatch_refused_ = true;
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
            // A worker waiting on an idle connection returns it when no free
            // worker or yielding one remains for this request. The search starts
            // after the last worker asked, so no one connection always loses its
            // worker first.
            if (pending_clients_.size() > free_workers_ + yielding_workers_) {
                for (std::size_t k = 0U; k < worker_waits_.size(); ++k) {
                    const auto index = (next_yield_ + k) % worker_waits_.size();
                    auto& wait = *worker_waits_[index];
                    if (!wait.waiting || wait.yield) continue;
                    wait.yield = true;
                    ++yielding_workers_;
                    next_yield_ = index + 1U;
                    wait.poller.Wake();
                    break;
                }
            }
            pending_clients_cv_.notify_one();
            engine_->metrics()->SetPendingConnections(pending_clients_.size());
            return true;
        }
    }
    CloseClient(*connection);
    return true;
}
}  // namespace chunkdb
