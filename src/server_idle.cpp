#include "server_idle.hpp"

#include "chunkdb/logging.hpp"
#include "proxy_protocol.hpp"
#include "server_feed.hpp"
#include "server_tls.hpp"
#include "source_address.hpp"

#include <algorithm>
#include <climits>
#include <limits>

namespace chunkdb {
using namespace server_detail;
namespace {
bool WouldBlock(int error) {
#ifdef _WIN32
    return error == WSAEWOULDBLOCK;
#else
    return error == EAGAIN || error == EWOULDBLOCK;
#endif
}
bool ControlRequest(std::string_view line) {
    const auto first = line.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) return false;
    const auto end = line.find_first_of(" \t\r\n", first);
    const auto verb = line.substr(first, end - first);
    return Protocol::CommandEquals(verb, "HELLO") || Protocol::CommandEquals(verb, "AUTH") ||
        Protocol::CommandEquals(verb, "CANCEL") || Protocol::CommandEquals(verb, "KILL") ||
        (Protocol::CommandEquals(verb, "SHOW") &&
         line.find_first_not_of(" \t", end) != std::string_view::npos &&
         Protocol::CommandEquals(line.substr(line.find_first_not_of(" \t", end), 11U), "CONNECTIONS"));
}
}

IdleIo::IdleIo(ChunkServer& server)
    : server_(server), poller_(std::numeric_limits<std::uint32_t>::max() - 1U) {}
IdleIo::~IdleIo() { Stop(); }
void IdleIo::Start() {
    const auto wake = [weak = weak_from_this()] { if (auto io = weak.lock()) io->Wake(); };
    data_credit_ = std::make_shared<ConnectionByteBudget>(2147483648ULL, wake);
    control_credit_ = std::make_shared<ConnectionByteBudget>(16777216U, wake);
    data_output_credit_ = std::make_shared<ConnectionByteBudget>(2147483648ULL, wake);
    control_output_credit_ = std::make_shared<ConnectionByteBudget>(16777216U, wake);
    const auto line_bound = server_.config_.max_line_bytes + 4096U;
    line_credit_ = std::make_shared<ConnectionByteBudget>(std::max<std::size_t>(16777216U, line_bound), wake);
    data_line_credit_ = std::make_shared<ConnectionByteBudget>(std::max<std::size_t>(67108864U, line_bound), wake);
    thread_ = std::thread([this] { Run(); });
}
void IdleIo::Stop() { Wake(); if (thread_.joinable()) thread_.join(); }
void IdleIo::Wake() noexcept { poller_.Wake(); }
bool IdleIo::Add(std::shared_ptr<ServerConnection> connection) {
    {
        std::lock_guard lock(mutex_);
        if (!server_.running_.load()) return false;
        incoming_.push_back(std::move(connection));
    }
    Wake();
    return true;
}

void IdleIo::Reply(ServerConnection& connection, std::string response) {
    connection.response = std::move(response);
    if (!ReserveReply(connection, connection.response.capacity(), true)) {
        connection.session.close_after_reply = true;
        std::string{}.swap(connection.response);
    }
    connection.response_offset = 0U;
    connection.response_write_size = 0;
    connection.phase = ServerConnection::Phase::kReply;
    connection.io_wait = POLLOUT;
    const auto now = std::chrono::steady_clock::now();
    connection.output_deadline = std::max(now, connection.session.reply_not_before) +
        std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
    if (!connection.session.greeted && connection.session.reply_not_before > now)
        if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kAfterAuthReplyScheduled, connection.session.connection_id);
    {
        std::lock_guard lock(connection.record->mutex);
        connection.record->state = "writing";
        connection.record->user = connection.session.user;
        connection.record->incarnation = connection.session.user_incarnation;
    }
}

bool IdleIo::ReserveReply(ServerConnection& connection, std::size_t bytes, bool control) {
    if (connection.reply_credit && bytes <= connection.reply_credit_bytes) return true;
    const auto limit = control ? 16777216ULL : 2147483648ULL;
    if (bytes > limit) return false;
    const auto& pool = control ? control_output_credit_ : data_output_credit_;
    if (pool->Owns(connection.reply_credit)) {
        if (!pool->TryResize(connection.reply_credit, bytes)) return false;
        connection.reply_credit_bytes = bytes;
        return true;
    }
    auto credit = pool->TryAcquire(bytes);
    if (!credit) return false;
    connection.reply_credit = std::move(credit);
    connection.reply_credit_bytes = bytes;
    return true;
}

bool IdleIo::RejectProtocol(ServerConnection& connection, const std::exception& error) {
    if (connection.phase == ServerConnection::Phase::kProxy) {
        server_.engine_->metrics()->CountConnectionRejected();
        if (!server_.proxy_header_refusal_warned_.exchange(true))
            LogMessage(LogLevel::kWarn, LogComponent::kServer,
                "connection refused without a valid PROXY header; later refusals are only counted", {{"error", error.what()}});
        return false;
    }
    server_.engine_->metrics()->CountMalformedRequest();
    LogMessage(LogLevel::kWarn, LogComponent::kServer, "bad request disconnect", {{"reason", error.what()}});
    connection.session.close_after_reply = true;
    Reply(connection, Protocol::Error("PROTOCOL", error.what()));
    return true;
}

bool IdleIo::Initialize(ServerConnection& connection) {
    std::string error;
    if (ConsumeTestFailureBudget(&g_test_nonblocking_config_failures) ||
        !SetSocketNonBlocking(connection.socket, true, &error)) {
        if (error.empty()) error = "injected nonblocking configuration failure";
        LogMessage(LogLevel::kWarn, LogComponent::kServer,
            "failed to configure nonblocking client I/O; closing connection", {{"phase", "handshake"}, {"error", error}});
        return false;
    }
    auto& session = connection.session;
    session.connection_id = NewConnectionId();
    const auto now = std::chrono::steady_clock::now();
    connection.handshake_deadline = now + std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
    connection.idle_deadline = now + std::chrono::milliseconds(std::min(
        server_.config_.client_io_timeout_ms, server_.config_.idle_connection_timeout_ms));
    {
        std::lock_guard lock(server_.connections_mutex_);
        server_.connections_.emplace(session.connection_id, connection.weak_from_this());
    }
    connection.peer_address = PeerAddressForSocket(connection.socket);
    connection.peer_endpoint = PeerEndpointForSocket(connection.socket);
    { std::lock_guard lock(connection.record->mutex); connection.record->peer = connection.peer_endpoint; }
    connection.handshake_source = SourceAddressKey(connection.peer_address);
    if (server_.config_.max_handshakes_per_ip != 0U) {
        if (!server_.TryAcquireHandshake(connection.handshake_source)) {
            server_.engine_->metrics()->CountConnectionRejected();
            if (server_.config_.tls_enabled) return false;
            session.close_after_reply = true;
            Reply(connection, Protocol::Error("BUSY", "too many connections before HELLO from this address"));
            return true;
        }
        connection.handshake_slot = true;
    }
    session.idle_timeout_ms = server_.config_.idle_connection_timeout_ms;
    session.default_statement_timeout_ms = server_.config_.statement_timeout_ms;
    auto controls = std::make_shared<SessionControlCallbacks>();
    controls->max_response_bytes = 1048576U;
    controls->enumerate = [this](const SessionControlAccess& access, const auto& visitor) {
        std::lock_guard lock(server_.connections_mutex_);
        for (const auto& [id, weak] : server_.connections_) {
            if (const auto target = weak.lock()) {
                std::lock_guard state_lock(target->record->mutex);
                const auto& record = *target->record;
                if (!access.admin_all && (access.user_incarnation == 0U || record.incarnation != access.user_incarnation)) continue;
                visitor(ConnectionInfo{.id = id, .user = record.user, .peer = record.peer,
                    .state = record.state, .statement = record.verb});
            }
        }
    };
    const auto lookup = [this](const SessionControlAccess& access, std::uint64_t id) {
        std::shared_ptr<ServerConnection> target;
        {
            std::lock_guard lock(server_.connections_mutex_);
            const auto found = server_.connections_.find(id);
            if (found != server_.connections_.end()) target = found->second.lock();
        }
        bool authorized = true;
        if (target) {
            std::lock_guard lock(target->record->mutex);
            if (!access.admin_all && (access.user_incarnation == 0U || target->record->incarnation != access.user_incarnation))
                authorized = false;
        }
        if (!authorized) target.reset();
        return target;
    };
    controls->cancel = [this, lookup](const SessionControlAccess& access, std::uint64_t id) {
        if (id == access.requester_connection_id) return false;
        const auto target = lookup(access, id);
        if (!target) return false;
        std::shared_ptr<StatementControl> statement;
        { std::lock_guard lock(target->record->mutex); statement = target->record->statement; }
        const bool cancelled = statement && statement->RequestStop(StatementStopReason::kCancelled);
        if (cancelled) Wake();
        return cancelled;
    };
    controls->kill = [this, lookup](const SessionControlAccess& access, std::uint64_t id) {
        const auto target = lookup(access, id);
        if (!target) return false;
        if (id == access.requester_connection_id) target->record->self_kill_reply.store(true);
        const bool killed = !target->record->killed.exchange(true);
        std::shared_ptr<StatementControl> statement;
        { std::lock_guard lock(target->record->mutex); statement = target->record->statement; }
        if (statement) (void)statement->RequestStop(StatementStopReason::kKilled);
        Wake();
        if (auto feed = server_.FeedIoHandle()) feed->Wake();
        return killed;
    };
    session.connection_controls = std::move(controls);
    session.max_watches = server_.config_.max_watches;
    session.watch_options.progress_interval = std::chrono::milliseconds(server_.config_.feed_progress_ms);
    session.watch_options.notify = [weak = std::weak_ptr<FeedIo>(server_.FeedIoHandle())] {
        if (auto io = weak.lock()) io->Wake();
    };
    session.register_slot_watch = [weak = std::weak_ptr<FeedIo>(server_.FeedIoHandle())](std::shared_ptr<SlotWatch> state) {
        if (auto io = weak.lock()) return io->RegisterSlot(std::move(state));
        return false;
    };
    { std::lock_guard lock(server_.lifecycle_mutex_); session.backup_cancelled = server_.backup_stop_.get_token(); }
    connection.phase = server_.config_.proxy_protocol_v2 ? ServerConnection::Phase::kProxy : ServerConnection::Phase::kTls;
    return true;
}

bool IdleIo::Read(ServerConnection& connection) {
    if (connection.waiting_credit) return true;
    if (!connection.line_credit) {
        if (server_.config_.max_line_bytes > std::numeric_limits<std::size_t>::max() - 4096U)
            throw std::invalid_argument("request line byte bound overflows");
        connection.line_credit = line_credit_->TryAcquire(server_.config_.max_line_bytes + 4096U);
        if (!connection.line_credit) { connection.waiting_line_credit = true; return true; }
    }
    std::array<char, 4096> buffer{};
    int read = 0;
#ifdef CHUNKDB_WITH_OPENSSL
    if (connection.tls) {
        ERR_clear_error();
        read = SSL_read(connection.tls, buffer.data(), static_cast<int>(buffer.size()));
        if (read <= 0) {
            const auto error = SSL_get_error(connection.tls, read);
            if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) return false;
            connection.io_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
            const bool in_record = SSL_has_pending(connection.tls) != 0;
            if (in_record && !connection.input_deadline) {
                connection.input_deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
                connection.record_deadline = true;
            } else if (!in_record && connection.record_deadline) {
                connection.input_deadline.reset();
                connection.record_deadline = false;
            }
            return true;
        }
    } else
#endif
    {
        read = static_cast<int>(recv(connection.socket, buffer.data(), static_cast<int>(buffer.size()), 0));
        if (read < 0) {
            const auto error = CurrentSocketErrorCode();
            if (WouldBlock(error) || IsSocketInterruptedError(error)) return true;
            return false;
        }
        if (read == 0) return false;
    }
    connection.io_wait = POLLIN;
    connection.record_deadline = false;
    if (!connection.input_deadline)
        connection.input_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
    connection.pending.append(buffer.data(), static_cast<std::size_t>(read));
#ifdef CHUNKDB_WITH_OPENSSL
    if (connection.tls && SSL_pending(connection.tls) != 0) connection.read_ready = true;
#endif
    return true;
}

bool IdleIo::Parse(ServerConnection& connection, bool worker) {
    if ((!connection.pending.empty() || !connection.request_line.empty()) && !connection.input_deadline)
        connection.input_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
    if (connection.request_line.empty()) {
        connection.pending.enforce_partial_line_limit(server_.config_.max_line_bytes);
        if (!connection.pending.extract_line(&connection.request_line, server_.config_.max_line_bytes)) return true;
        connection.payload = server_.engine_->PlanPayload(connection.session, connection.request_line);
        connection.control_request = !connection.session.greeted || ControlRequest(connection.request_line);
        if (!connection.control_request) {
            auto credit = data_line_credit_->TryAcquire(server_.config_.max_line_bytes + 4096U);
            if (!credit) {
                connection.session.close_after_reply = true;
                Reply(connection, Protocol::Error("BUSY", "aggregate request line byte bound reached"));
                return true;
            }
            connection.line_credit = std::move(credit);
        }
    }
    // Reserved controls return to their I/O owner even inside a buffered pipeline.
    if (worker && connection.control_request) return true;
    const auto& plan = connection.payload;
    const bool discard = plan.plan == CommandEngine::PayloadPlan::kDiscard;
    if (discard || plan.plan == CommandEngine::PayloadPlan::kParameters) {
        if (!connection.framing_started) {
            if (!connection.payload_credit && !connection.waiting_credit) {
                std::size_t retained = 0U;
                bool opaque = discard;
                for (std::size_t i = 0U; !discard && i < plan.parameter_limits.size(); ++i) {
                    const bool skipped = i < plan.opaque_parameters.size() && plan.opaque_parameters[i];
                    opaque = opaque || skipped || (i < plan.parameter_rejections.size() && !plan.parameter_rejections[i].empty());
                    if (skipped) continue;
                    const auto limit = plan.parameter_limits[i];
                    retained += std::min(limit, plan.parameter_bytes_limit - retained);
                }
                const auto drained = opaque ? server_.config_.max_unparsed_parameter_bytes : 0U;
                if (retained > std::numeric_limits<std::size_t>::max() - drained)
                    throw std::invalid_argument("statement parameter byte bound overflows");
                connection.payload_credit_bytes = retained + drained;
                const auto pool_limit = connection.control_request ? 16777216ULL : 2147483648ULL;
                if (connection.payload_credit_bytes > pool_limit) {
                    connection.session.close_after_reply = true;
                    Reply(connection, Protocol::Error("PROTOCOL", "request exceeds the aggregate input byte bound"));
                    return true;
                }
                if (worker) {
                    if (data_waiter_count_.load(std::memory_order_acquire) != 0U) return true;
                    connection.payload_credit = data_credit_->TryAcquire(connection.payload_credit_bytes);
                    if (!connection.payload_credit) return true;
                } else {
                    (connection.control_request ? control_waiters_ : data_waiters_).push_back(connection.weak_from_this());
                    if (!connection.control_request) data_waiter_count_.fetch_add(1U, std::memory_order_release);
                    connection.waiting_credit = true;
                    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                        hook->Run(FeedDeliveryTestHook::Point::kAfterPayloadCreditQueued, connection.session.connection_id);
                    return true;
                }
            }
            if (connection.waiting_credit) return true;
            connection.frames = std::make_unique<ParameterFrameDecoder>(ParameterFrameRules{
                .frame_count = discard ? plan.frame_count : plan.parameter_limits.size(),
                .limits = plan.parameter_limits,
                .total_limit = discard ? server_.config_.max_unparsed_parameter_bytes : plan.parameter_bytes_limit,
                .discard = discard, .opaque = &plan.opaque_parameters, .rejections = &plan.parameter_rejections,
                .opaque_limit = server_.config_.max_unparsed_parameter_bytes,
            });
            connection.framing_started = true;
        }
    }
    if (connection.frames) {
        if (!connection.frames->Consume(connection.pending, server_.config_.max_line_bytes)) return true;
        if (connection.frames->total_exceeded()) {
            connection.session.close_after_reply = true;
            const auto total_limit = discard ? server_.config_.max_unparsed_parameter_bytes : plan.parameter_bytes_limit;
            Reply(connection, Protocol::Error(discard ? "PROTOCOL" : "INVALID_ARGUMENT",
                "the parameters of one statement take at most " + std::to_string(total_limit) + " bytes together"));
            return true;
        }
        connection.parameters = connection.frames->TakeParameters();
        connection.payload_rejection = connection.frames->rejection();
        connection.frames.reset();
    }
    connection.input_deadline.reset();
    if (auto refused = server_.engine_->AdmitUserStatement(connection.session, connection.request_line)) {
        Reply(connection, std::move(*refused));
        return true;
    }
    connection.session.request_admitted = true;
    connection.phase = ServerConnection::Phase::kReady;
    if (connection.control_request) {
        const auto first = connection.request_line.find_first_not_of(" \t\r\n");
        if (first != std::string::npos && Protocol::CommandEquals(connection.request_line.substr(first, 4U), "SHOW")) {
            if (!ReserveReply(connection, connection.session.connection_controls->max_response_bytes * 2U + 64U, true)) {
                Reply(connection, Protocol::Error("BUSY", "control reply budget is full", {{"reason", "reply_budget"}}));
                return true;
            }
        }
        const auto response = connection.payload.plan == CommandEngine::PayloadPlan::kDiscard ?
            server_.engine_->RejectPayload(connection.session, connection.request_line, connection.payload.reject_response) :
            !connection.payload_rejection.empty() ? server_.engine_->RejectPayload(connection.session,
                connection.request_line, std::move(connection.payload_rejection)) :
            server_.engine_->Execute(connection.session, connection.request_line, connection.parameters);
        Reply(connection, response);
    } else {
        auto control = std::make_shared<StatementControl>(std::chrono::steady_clock::now(),
            connection.session.hello_options.statement_timeout_ms.value_or(server_.config_.statement_timeout_ms));
        if (worker) {
            if (!control->Start()) {
                Reply(connection, StatementStoppedReply(*control->TakeQueuedStop()));
                return true;
            }
            if (control->deadline()) {
                {
                    std::lock_guard lock(mutex_);
                    if (!server_.running_.load()) {
                        (void)control->RequestStop(StatementStopReason::kKilled);
                        connection.session.close_after_reply = true;
                        Reply(connection, StatementStoppedReply(StatementStopReason::kKilled));
                        return true;
                    }
                    incoming_statements_.push_back({connection.shared_from_this(), control});
                }
                Wake();
            }
        } else statements_.push_back({connection.shared_from_this(), control});
        if (worker)
            if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeBufferedStatementPublication, connection.session.connection_id);
        std::shared_ptr<StatementControl> killed_statement;
        {
            std::lock_guard lock(connection.record->mutex);
            connection.record->state = worker ? "running" : "queued";
            connection.record->verb = connection.request_line.substr(0U,
                std::min<std::size_t>(32U, connection.request_line.find_first_of(" \t\r\n")));
            connection.record->statement = std::move(control);
            if (connection.record->killed.load()) killed_statement = connection.record->statement;
        }
        if (killed_statement) (void)killed_statement->RequestStop(StatementStopReason::kKilled);
    }
    return true;
}

void IdleIo::CheckStatements() {
    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kBeforeQueuedStopScan, statements_.size());
    for (auto& tracked : statements_) {
        if (!tracked.statement) continue;
        auto connection = tracked.connection;
        if (!connection || tracked.statement->phase() == StatementControl::Phase::kComplete) {
            tracked.statement.reset();
            continue;
        }
        if (const auto deadline = tracked.statement->deadline(); deadline && std::chrono::steady_clock::now() >= *deadline)
            (void)tracked.statement->RequestStop(StatementStopReason::kTimeout);
        if (const auto stopped = tracked.statement->TakeQueuedStop()) {
            tracked.io_claimed = true;
            if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kAfterQueuedStopClaim, connection->session.connection_id);
            Reply(*connection, StatementStoppedReply(*stopped));
            if (connection->record->killed.load()) connection->session.close_after_reply = true;
            if (connection->worker_queued) {
                connection->worker_queued = false;
                clients_returned_.push_back(std::move(connection));
            }
            tracked.statement.reset();
        }
    }
    std::erase_if(statements_, [](const auto& tracked) { return !tracked.statement; });
    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterStatementTrackerScan, statements_.size());
}

void IdleIo::PumpCredits() {
    const auto pump = [&](auto& waiters, const auto& pool, bool data) {
        while (!waiters.empty()) {
            auto connection = waiters.front().lock();
            if (!connection || connection->socket == kInvalidSocket || !connection->waiting_credit) {
                waiters.pop_front();
                if (data) data_waiter_count_.fetch_sub(1U, std::memory_order_release);
                continue;
            }
            auto credit = pool->TryAcquire(connection->payload_credit_bytes);
            if (!credit) break;
            connection->payload_credit = std::move(credit);
            connection->waiting_credit = false;
            connection->read_ready = true;
            if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kAfterPayloadCreditGranted, connection->session.connection_id);
            waiters.pop_front();
            if (data) data_waiter_count_.fetch_sub(1U, std::memory_order_release);
        }
    };
    pump(control_waiters_, control_credit_, false);
    pump(data_waiters_, data_credit_, true);
}

bool IdleIo::Write(ServerConnection& connection) {
    if (std::chrono::steady_clock::now() < connection.session.reply_not_before) return true;
    if (connection.response_offset == connection.response.size() && connection.reply_continuation) {
        connection.response = connection.reply_continuation->Next(65536U);
        connection.response_offset = 0U;
        if (connection.reply_continuation->done()) connection.reply_continuation.reset();
    }
    const auto remaining = connection.response.size() - connection.response_offset;
    if (remaining != 0U) {
        if (!connection.reply_credit) throw std::logic_error("reply write requires reserved output credit");
        if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kBeforeReplyBufferWrite, connection.response.size());
        int written = 0;
        const auto take = static_cast<int>(std::min<std::size_t>(remaining, 65536U));
#ifdef CHUNKDB_WITH_OPENSSL
        if (connection.tls) {
            if (connection.response_write_size == 0) connection.response_write_size = take;
            ERR_clear_error();
            written = SSL_write(connection.tls, connection.response.data() + connection.response_offset,
                connection.response_write_size);
            if (written <= 0) {
                const auto error = SSL_get_error(connection.tls, written);
                if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) return false;
                connection.io_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
                if (error == SSL_ERROR_WANT_WRITE)
                    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                        hook->Run(FeedDeliveryTestHook::Point::kAfterBlockedReplyWrite, 0U);
                return true;
            }
            connection.response_write_size = 0;
        } else
#endif
        {
            written = static_cast<int>(send(connection.socket,
                connection.response.data() + connection.response_offset, take, 0));
            if (written < 0) {
                const auto error = CurrentSocketErrorCode();
                if (WouldBlock(error)) {
                    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
                        hook->Run(FeedDeliveryTestHook::Point::kAfterBlockedReplyWrite, 0U);
                    return true;
                }
                if (IsSocketInterruptedError(error)) return true;
            }
            if (written <= 0) return false;
        }
        connection.response_offset += static_cast<std::size_t>(written);
        if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kAfterReplyBufferWrite, connection.response_offset);
        connection.io_wait = POLLOUT;
        connection.output_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
        if (connection.response_offset != connection.response.size() || connection.reply_continuation) return true;
    }
    ++connection.completed_replies;
    std::string{}.swap(connection.response);
    connection.reply_credit.reset();
    connection.reply_credit_bytes = 0U;
    std::string{}.swap(connection.request_line);
    connection.parameters.clear();
    connection.frames.reset();
    connection.payload_credit.reset();
    if (connection.pending.empty()) {
        std::string{}.swap(connection.pending.bytes);
        connection.line_credit.reset();
    }
    connection.framing_started = false;
    connection.payload_rejection.clear();
    connection.payload = {};
    connection.output_deadline.reset();
    connection.session.reply_not_before = {};
    connection.session.request_admitted = false;
    connection.irreversible_reply = false;
    {
        std::lock_guard lock(connection.record->mutex);
        connection.record->user = connection.session.user;
        connection.record->incarnation = connection.session.user_incarnation;
        connection.record->state = connection.session.watch || connection.session.slot_watch ? "watch" : "idle";
        connection.record->verb.clear();
        connection.record->statement.reset();
    }
    if (connection.record->killed.load()) connection.session.close_after_reply = true;
    if (connection.session.greeted && connection.handshake_slot) {
        server_.ReleaseHandshake(connection.handshake_source);
        connection.handshake_slot = false;
    }
    if (connection.session.close_after_reply) return false;
    connection.phase = ServerConnection::Phase::kInput;
    connection.io_wait = POLLIN;
    connection.idle_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds(server_.config_.idle_connection_timeout_ms);
    if (!connection.pending.empty()) connection.read_ready = true;
    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kBeforeClientIdleRead, connection.completed_replies);
    return true;
}

bool IdleIo::Progress(ServerConnection& connection) {
    using Phase = ServerConnection::Phase;
    if (connection.phase == Phase::kStarting && !Initialize(connection)) return false;
    if (connection.phase == Phase::kProxy) {
        std::array<char, proxy_protocol::kPrefixBytes + proxy_protocol::kMaxPayloadBytes> bytes{};
        const auto need = connection.proxy_size - connection.proxy_header.size();
        const auto count = static_cast<int>(recv(connection.socket, bytes.data(), static_cast<int>(need), 0));
        if (count < 0 && (WouldBlock(CurrentSocketErrorCode()) || IsSocketInterruptedError(CurrentSocketErrorCode()))) return true;
        if (count <= 0) return false;
        connection.proxy_header.append(bytes.data(), static_cast<std::size_t>(count));
        const auto header = [&] { return std::span(reinterpret_cast<const std::uint8_t*>(connection.proxy_header.data()),
            connection.proxy_header.size()); };
        if (connection.proxy_header.size() == proxy_protocol::kPrefixBytes)
            connection.proxy_size = proxy_protocol::kPrefixBytes + proxy_protocol::PayloadLength(header());
        if (connection.proxy_header.size() != connection.proxy_size) return true;
        const auto source = proxy_protocol::Parse(header());
        if (!source.use_peer) {
            if (connection.handshake_slot) {
                server_.ReleaseHandshake(connection.handshake_source);
                connection.handshake_slot = false;
            }
            connection.peer_address = source.address;
            connection.peer_endpoint = source.endpoint;
            connection.handshake_source = SourceAddressKey(source.address);
            if (server_.config_.max_handshakes_per_ip != 0U) {
                if (!server_.TryAcquireHandshake(connection.handshake_source)) {
                    server_.engine_->metrics()->CountConnectionRejected();
                    if (server_.config_.tls_enabled) return false;
                    connection.session.close_after_reply = true;
                    Reply(connection, Protocol::Error("BUSY", "too many connections before HELLO from this address"));
                    return true;
                }
                connection.handshake_slot = true;
            }
        }
        connection.proxy_header.clear();
        connection.phase = Phase::kTls;
    }
    if (connection.phase == Phase::kTls) {
        connection.session.remote_address = connection.peer_address;
        { std::lock_guard lock(connection.record->mutex); connection.record->peer = connection.peer_endpoint; }
#ifdef CHUNKDB_WITH_OPENSSL
        if (server_.config_.tls_enabled) {
            if (connection.tls == nullptr) {
                std::lock_guard lock(server_.tls_context_mutex_);
                connection.tls = SSL_new(server_.tls_context_);
                if (!connection.tls) return false;
                SSL_set_fd(connection.tls, static_cast<int>(connection.socket));
            }
            ERR_clear_error();
            const auto result = SSL_accept(connection.tls);
            if (result != 1) {
                const auto error = SSL_get_error(connection.tls, result);
                if (error != SSL_ERROR_WANT_READ && error != SSL_ERROR_WANT_WRITE) return false;
                connection.io_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
                return true;
            }
        }
#endif
        connection.phase = Phase::kInput;
        connection.io_wait = POLLIN;
        connection.handshake_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
        connection.idle_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::min(
            server_.config_.client_io_timeout_ms, server_.config_.idle_connection_timeout_ms));
    }
    if (connection.phase == Phase::kReply) return Write(connection);
    if (connection.phase != Phase::kInput) return true;
    if (connection.waiting_line_credit) {
        connection.line_credit = line_credit_->TryAcquire(server_.config_.max_line_bytes + 4096U);
        if (!connection.line_credit) return true;
        connection.waiting_line_credit = false;
        connection.read_ready = true;
    }
    if (!Parse(connection) || connection.phase != Phase::kInput) return true;
    if (!connection.read_ready) return true;
    connection.read_ready = false;
    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kBeforeClientRead, connection.completed_replies);
    const auto alive = Read(connection);
    if (auto* hook = server_.delivery_test_hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterClientRead, connection.completed_replies);
    return alive && Parse(connection);
}

void IdleIo::Run() {
    std::vector<std::shared_ptr<ServerConnection>> clients;
    std::vector<SocketInterest> interests;
    const auto fail = [&](const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer,
            "connection transport terminated", {{"error", error.what()}});
        server_.Stop();
    };
    try {
        while (server_.running_.load()) {
            poller_.DrainWake();
            {
                std::lock_guard lock(mutex_);
                clients.insert(clients.end(), std::make_move_iterator(incoming_.begin()),
                    std::make_move_iterator(incoming_.end()));
                incoming_.clear();
                statements_.insert(statements_.end(), std::make_move_iterator(incoming_statements_.begin()),
                    std::make_move_iterator(incoming_statements_.end()));
                incoming_statements_.clear();
            }
            CheckStatements();
            clients.insert(clients.end(), std::make_move_iterator(clients_returned_.begin()),
                std::make_move_iterator(clients_returned_.end()));
            clients_returned_.clear();
            PumpCredits();
            for (auto& connection : clients) {
                bool alive = true;
                if (connection->record->killed.load() &&
                    (connection->phase != ServerConnection::Phase::kReply ||
                     (!connection->irreversible_reply && !connection->record->self_kill_reply.load()))) alive = false;
                try { if (alive) alive = Progress(*connection); }
                catch (const std::invalid_argument& error) {
                    alive = RejectProtocol(*connection, error);
                } catch (const std::runtime_error& error) {
                    alive = RejectProtocol(*connection, error);
                }
                const auto now = std::chrono::steady_clock::now();
                using Phase = ServerConnection::Phase;
                if (alive && !connection->session.greeted &&
                    now >= connection->handshake_deadline) {
                    if (connection->phase == Phase::kTls || connection->phase == Phase::kProxy) {
                        LogMessage(LogLevel::kWarn, LogComponent::kServer, "connection terminated", {
                            {"phase", "handshake"}, {"reason", "timeout"}, {"error", "handshake deadline exceeded"},
                            {"socket", std::to_string(connection->socket)}, {"peer", connection->peer_endpoint},
                        });
                        alive = false;
                    } else if (connection->phase == Phase::kReply) alive = false;
                    else {
                        connection->session.close_after_reply = true;
                        Reply(*connection, Protocol::Error("PROTOCOL", "HELLO 3 was not completed within the I/O timeout"));
                    }
                } else if (alive && ((connection->input_deadline && now >= *connection->input_deadline) ||
                    (connection->output_deadline && now >= *connection->output_deadline) ||
                    (connection->phase == Phase::kInput && !connection->input_deadline &&
                     now >= connection->idle_deadline))) {
                    LogMessage(LogLevel::kWarn, LogComponent::kServer, "connection terminated", {
                        {"phase", connection->output_deadline ? "write" : "read"}, {"reason", "timeout"},
                        {"error", connection->output_deadline ? "reply progress deadline exceeded" :
                            connection->request_line.empty() ? "request line deadline exceeded" : "payload deadline exceeded"},
                        {"socket", std::to_string(connection->socket)}, {"peer", connection->peer_endpoint},
                    });
                    alive = false;
                }
                if (!alive) {
                    connection->waiting_credit = false;
                    server_.CloseClient(*connection);
                    connection.reset();
                    PumpCredits();
                } else if (connection->phase == Phase::kReady) {
                    if (server_.DispatchClient(connection)) connection.reset();
                } else if (connection->phase == Phase::kInput &&
                    (connection->session.watch || connection->session.slot_watch)) {
                    if (server_.HandOff(connection)) connection.reset();
                    else { server_.CloseClient(*connection); connection.reset(); }
                }
            }
            std::erase(clients, nullptr);
            PumpCredits();
            interests.clear();
            interests.reserve(clients.size());
            int timeout = -1;
            const auto now = std::chrono::steady_clock::now();
            const auto wait_until = [&](auto until) {
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(until - now).count();
                const auto bounded = static_cast<int>(std::clamp<std::int64_t>(remaining, 0, INT_MAX));
                timeout = timeout < 0 ? bounded : std::min(timeout, bounded);
            };
            for (const auto& connection : clients) {
                const bool delayed = connection->phase == ServerConnection::Phase::kReply &&
                    now < connection->session.reply_not_before;
                interests.push_back({connection->socket,
                    static_cast<short>(connection->phase == ServerConnection::Phase::kReady || delayed ||
                        connection->waiting_credit || connection->waiting_line_credit ? 0 : connection->io_wait), 0});
                if (delayed) wait_until(connection->session.reply_not_before);
                if (!connection->session.greeted)
                    wait_until(connection->handshake_deadline);
                if (connection->input_deadline) wait_until(*connection->input_deadline);
                if (connection->output_deadline) wait_until(*connection->output_deadline);
                if (connection->phase == ServerConnection::Phase::kInput && !connection->input_deadline)
                    wait_until(connection->idle_deadline);
                if (connection->read_ready && !connection->waiting_credit && connection->phase == ServerConnection::Phase::kInput) timeout = 0;
            }
            for (const auto& tracked : statements_)
                if (const auto deadline = tracked.statement->deadline(); deadline && *deadline > now) wait_until(*deadline);
            if (!server_.running_.load()) break;
            (void)poller_.Wait(interests, timeout);
            for (std::size_t i = 0U; i < clients.size(); ++i)
                if (interests[i].ready != 0) clients[i]->read_ready = true;
        }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    for (const auto& connection : clients) if (connection) server_.CloseClient(*connection);
    const auto close_queued = [&](const auto& tracked) {
        if (tracked.statement) (void)tracked.statement->RequestStop(StatementStopReason::kKilled);
        if (tracked.io_claimed || (tracked.statement && tracked.statement->TakeQueuedStop()))
            server_.CloseClient(*tracked.connection);
    };
    for (const auto& tracked : statements_) close_queued(tracked);
    statements_.clear();
    std::lock_guard lock(mutex_);
    for (const auto& tracked : incoming_statements_) close_queued(tracked);
    incoming_statements_.clear();
    for (const auto& connection : incoming_) if (connection) server_.CloseClient(*connection);
    incoming_.clear();
}
}  // namespace chunkdb
