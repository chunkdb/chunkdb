#include "server_feed.hpp"
#include "server_idle.hpp"
#include "server_tls.hpp"
#include "feed_protocol.hpp"
#include "cql.hpp"
#include "user_registry.hpp"
#include "chunkdb/logging.hpp"
#include <algorithm>
#include <climits>
#include <limits>
#include <unordered_map>
#ifndef _WIN32
#include <sys/uio.h>
#endif
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
std::shared_ptr<const std::string> Frame(const std::shared_ptr<const FeedEntry>& entry) {
    if (entry->protocol_frame) return entry->protocol_frame;
    return std::make_shared<const std::string>(EncodeFeedEntry(*entry));
}
}
FeedIo::FeedIo(ChunkServer& server) : server_(server), poller_(server.config_.max_watches) {}
void FeedDeliveryTestAccess::SetHook(ChunkServer& server, FeedDeliveryTestHook* hook) {
    InstallHook(server, hook, true);
}
void FeedDeliveryTestAccess::SetObserverHook(ChunkServer& server, FeedDeliveryTestHook* hook) {
    InstallHook(server, hook, false);
}
void FeedDeliveryTestAccess::SetPendingObserverHook(ChunkServer& server, FeedDeliveryTestHook* hook) {
    {
        std::lock_guard lock(server.pending_clients_mutex_);
        server.delivery_test_hook_.store(hook, std::memory_order_release);
    }
    server.pending_clients_cv_.notify_all();
}
std::shared_ptr<StatementControl> FeedDeliveryTestAccess::CurrentStatement(ChunkServer& server, std::uint64_t id) {
    std::lock_guard lock(server.connections_mutex_);
    const auto found = server.connections_.find(id);
    if (found == server.connections_.end()) return {};
    const auto connection = found->second.lock();
    if (!connection) return {};
    std::lock_guard state_lock(connection->record->mutex);
    return connection->record->statement;
}
bool FeedDeliveryTestAccess::ConnectionStillOwned(ChunkServer& server, std::uint64_t id) {
    std::lock_guard lock(server.connections_mutex_);
    const auto found = server.connections_.find(id);
    return found != server.connections_.end() && !found->second.expired();
}
std::optional<bool> FeedDeliveryTestAccess::SocketNonBlocking(ChunkServer& server, std::uint64_t id) {
#ifdef _WIN32
    (void)server; (void)id;
    return std::nullopt;
#else
    std::shared_ptr<ServerConnection> connection;
    {
        std::lock_guard lock(server.connections_mutex_);
        const auto found = server.connections_.find(id);
        if (found != server.connections_.end()) connection = found->second.lock();
    }
    if (!connection || connection->socket == kInvalidSocket) return std::nullopt;
    const int flags = fcntl(connection->socket, F_GETFL, 0);
    if (flags < 0) return std::nullopt;
    return (flags & O_NONBLOCK) != 0;
#endif
}
std::optional<std::pair<long, long>> FeedDeliveryTestAccess::SocketTimeouts(ChunkServer& server, std::uint64_t id) {
#ifdef _WIN32
    (void)server; (void)id;
    return std::nullopt;
#else
    std::shared_ptr<ServerConnection> connection;
    {
        std::lock_guard lock(server.connections_mutex_);
        const auto found = server.connections_.find(id);
        if (found != server.connections_.end()) connection = found->second.lock();
    }
    if (!connection || connection->socket == kInvalidSocket) return std::nullopt;
    const auto read = [&](int option) -> std::optional<long> {
        timeval value{};
        socklen_t size = sizeof(value);
        if (getsockopt(connection->socket, SOL_SOCKET, option, &value, &size) != 0) return std::nullopt;
        return static_cast<long>(value.tv_sec) * 1000L + static_cast<long>(value.tv_usec) / 1000L;
    };
    const auto receive = read(SO_RCVTIMEO), send = read(SO_SNDTIMEO);
    if (!receive || !send) return std::nullopt;
    return std::make_pair(*receive, *send);
#endif
}
bool FeedDeliveryTestAccess::SetSendBuffer(ChunkServer& server, std::uint64_t id, int bytes) {
    std::shared_ptr<ServerConnection> connection;
    {
        std::lock_guard lock(server.connections_mutex_);
        const auto found = server.connections_.find(id);
        if (found != server.connections_.end()) connection = found->second.lock();
    }
    if (!connection || connection->socket == kInvalidSocket) return false;
    return setsockopt(connection->socket, SOL_SOCKET, SO_SNDBUF,
        reinterpret_cast<const char*>(&bytes), sizeof(bytes)) == 0;
}
void FeedDeliveryTestAccess::WakeTransport(ChunkServer& server) {
    if (auto io = server.IdleIoHandle()) io->Wake();
}
std::shared_ptr<void> FeedDeliveryTestAccess::ReserveReplyCredit(ChunkServer& server, std::size_t bytes) {
    if (auto io = server.IdleIoHandle()) return io->data_output_credit_->TryAcquire(bytes);
    return {};
}
std::optional<std::size_t> FeedDeliveryTestAccess::RequestLineCapacity(ChunkServer& server, std::uint64_t id) {
    std::lock_guard lock(server.connections_mutex_);
    const auto found = server.connections_.find(id);
    const auto connection = found == server.connections_.end() ? nullptr : found->second.lock();
    if (!connection) return std::nullopt;
    return connection->request_line.capacity();
}
void FeedDeliveryTestAccess::InstallHook(ChunkServer& server, FeedDeliveryTestHook* hook, bool wake) {
    auto io = server.FeedIoHandle();
    server.delivery_test_hook_.store(hook, std::memory_order_release);
    {
        std::lock_guard lock(io->catchup_mutex_);
        io->hook_.store(hook, std::memory_order_release);
        for (const auto& slot : io->slots_) slot->hook_.store(hook, std::memory_order_release);
    }
    if (wake) io->Wake();
}
FeedIo::~FeedIo() {
    Stop();
}
void FeedIo::Start() {
    catchup_thread_ = std::thread([this] { CatchUp(); });
    thread_ = std::thread([this] { Run(); });
}
void FeedIo::Stop() {
    Wake();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kBeforeIoJoin, 0U);
    if (thread_.joinable()) thread_.join();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterIoJoin, 0U);
    { std::lock_guard lock(catchup_mutex_); catchup_stop_ = true; }
    catchup_cv_.notify_one();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kBeforeCatchUpJoin, 0U);
    if (catchup_thread_.joinable()) catchup_thread_.join();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterCatchUpJoin, 0U);
}
void FeedIo::WakeCatchUp() noexcept {
    // A skipped signal precedes the worker's clear, so its next scan sees it.
    if (slot_count_.load(std::memory_order_acquire) == 0U ||
        catchup_signaled_.exchange(true, std::memory_order_acq_rel)) return;
    { std::lock_guard lock(catchup_mutex_); catchup_wake_ = true; }
    catchup_cv_.notify_one();
}
void FeedIo::Wake() noexcept {
    WakeCatchUp();
    WakePoll();
}
void FeedIo::WakePoll() noexcept {
    poller_.Wake();
}
bool FeedIo::Add(std::shared_ptr<ServerConnection> connection) {
    { std::lock_guard lock(mutex_);
      if (!server_.running_.load()) return false;
      incoming_.push_back(std::move(connection)); }
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterIoAdd, 0U);
    Wake();
    return true;
}
bool FeedIo::RegisterSlot(std::shared_ptr<SlotWatch> state) {
    { std::lock_guard lock(catchup_mutex_);
      if (!server_.running_.load() || slots_.size() >= server_.config_.max_watches) return false;
      state->hook_.store(hook_.load(std::memory_order_acquire), std::memory_order_release);
      slots_.push_back(std::move(state)); catchup_wake_ = true;
      slot_count_.store(slots_.size(), std::memory_order_release); }
    catchup_cv_.notify_one();
    return true;
}
void FeedIo::CatchUp() {
    const auto fail = [&](const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer, "slot catch-up terminated", {{"error", error.what()}});
        server_.Stop();
    };
    try {
    for (;;) {
        std::vector<std::shared_ptr<SlotWatch>> states;
        bool stop;
        {
            std::unique_lock lock(catchup_mutex_);
            // Sweep deferred ACK batches and terminal slot state even when
            // no client traffic arrives. Durable delivery wakes explicitly.
            catchup_cv_.wait_for(lock, std::chrono::milliseconds(100), [&] { return catchup_stop_ || catchup_wake_; });
            stop = catchup_stop_; catchup_wake_ = false; states = slots_;
            catchup_signaled_.exchange(false, std::memory_order_acq_rel);
        }
        for (const auto& state : states) {
            if (stop) state->Cancel();
            // An idle step waits for the wake of the event it needs, or the sweep.
            for (std::size_t step = 0U; step < 64U && !state->Finished(); ++step)
                if (!state->WorkStep()) break;
        }
        { std::lock_guard lock(catchup_mutex_);
          std::erase_if(slots_, [](const auto& state) { return state->Finished(); });
          slot_count_.store(slots_.size(), std::memory_order_release); }
        if (stop) return;
    }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    std::vector<std::shared_ptr<SlotWatch>> states;
    { std::lock_guard lock(catchup_mutex_); states.swap(slots_); slot_count_.store(0U, std::memory_order_release); }
    for (const auto& state : states) { state->Cancel(); state->WorkStep(); }
}
void FeedIo::Queue(Watch& watch, std::shared_ptr<const std::string> bytes, std::optional<std::uint64_t> revision, bool charged, bool excess) {
    watch.output.push_back({std::move(bytes), revision, charged, excess});
    watch.bytes += watch.output.back().bytes->size();
    // While this queue holds frames from the shared excess pool, it must make
    // write progress at least once per client I/O timeout, so a reader that
    // stops cannot hold the pool. Earlier charged output was already sent.
    if (excess && watch.excess_frames++ == 0U) watch.excess_progress = std::chrono::steady_clock::now();
}
bool FeedIo::ExpireStalledWrite(Watch& watch, std::chrono::steady_clock::time_point now) {
    const auto timeout = std::chrono::milliseconds(server_.config_.client_io_timeout_ms);
    if (watch.dead || watch.excess_frames == 0U || now < watch.excess_progress + timeout) return false;
    LogMessage(LogLevel::kWarn, LogComponent::kServer, "slot watch made no write progress",
        {{"unsent_bytes", std::to_string(watch.bytes)}, {"timeout_ms", std::to_string(timeout.count())}});
    watch.dead = true;
    return true;
}
void FeedIo::ReleaseOutput(Watch& watch, const std::shared_ptr<SlotWatch>& slot) {
    // A dropped queue is never written; release its slot charges now.
    bool released = false;
    if (slot) {
        auto offset = watch.offset;
        for (const auto& output : watch.output) {
            if (output.charged && output.bytes->size() > offset) { slot->Consumed(output.bytes->size() - offset); released = true; }
            offset = 0U;
        }
    }
    watch.output.clear(); watch.bytes = 0U; watch.offset = 0U; watch.excess_frames = 0U;
    if (released) Wake();
}
void FeedIo::Reject(Watch& watch, std::string message) {
    watch.close = true;
    Queue(watch, std::make_shared<const std::string>(Protocol::Error("PROTOCOL", message)));
}
bool FeedIo::Permitted(Watch& watch) {
    if (watch.right_lost) return false;
    const auto& right = watch.connection->session.watch_right;
    if (!right) return true;
    const auto state = right->Check();
    if (state == ReadRightCheck::State::kHeld) [[likely]] return true;
    // Every frame pulled after the change is still queued here: drop all
    // that the socket has not started, as on an ACK timeout. A started
    // frame or a pinned TLS retry cannot take an error; close instead.
    watch.right_lost = true;
    if (watch.offset != 0U || watch.write_size != 0) { watch.dead = true; return false; }
    if (const auto& slot = watch.connection->session.slot_watch)
        for (const auto& output : watch.output)
            if (output.charged) slot->Consumed(output.bytes->size());
    watch.output.clear(); watch.bytes = 0U;
    Queue(watch, std::make_shared<const std::string>(
        CommandEngine::LostWatchReply(state == ReadRightCheck::State::kNoUser, right->table())));
    watch.close = true;
    watch.close_immediate = true; // One best-effort nonblocking terminal write, then close.
    return false;
}
void FeedIo::Pull(Watch& watch, std::size_t share) {
    if (watch.close || watch.dead) return;
    if (const auto& slot = watch.connection->session.slot_watch) {
        if (slot->TakingOver() && !slot->Finished()) return;
        if (slot->AckTimedOut() || slot->TakingOver()) {
            // A dead consumer cannot be allowed to hold the connection while
            // buffered mutations drain. Never inject an error inside a partial
            // RESP frame or replace SSL_write's pinned retry buffer.
            if (watch.offset != 0U || watch.write_size != 0) { watch.dead = true; return; }
            ReleaseOutput(watch, slot);
            if (slot->TakingOver()) {
                Queue(watch, std::make_shared<const std::string>(Protocol::Error("KILLED", "slot ownership was taken over",
                    {{"reason", "slot_takeover"}})), {}, false);
            } else if (auto terminal = slot->Take(128U))
                Queue(watch, std::move(terminal->bytes), terminal->revision, terminal->charged);
            watch.close = true;
            watch.close_immediate = true; // One best-effort nonblocking terminal write, then close.
            return;
        }
        slot->SetQuota(share > 128U ? share - 128U : share);
        // Admission bounds all charged output, including what this queue
        // already holds, and frames accepted before another watch reduced the
        // share. Take accepted output whenever it is ready; Read bounds the
        // uncharged replies queued beside it.
        for (std::size_t i = 0U; i < 64U; ++i) {
            auto entry = slot->Take(std::numeric_limits<std::size_t>::max());
            if (!entry) return;
            watch.close = entry->close;
            watch.return_ready = entry->resume;
            Queue(watch, std::move(entry->bytes), entry->revision, entry->charged, entry->excess);
            Wake();
            if (watch.close || watch.return_ready) return;
        }
        return;
    }
    if (watch.unwatch) return;
    const auto resync = [&] {
        // Finish an in-progress frame, preserving SSL_write's exact retry.
        const bool pinned = !watch.output.empty() && (watch.offset != 0U || watch.write_size != 0);
        if (pinned) {
            while (watch.output.size() > 1U) watch.output.pop_back();
            watch.bytes = watch.output.front().bytes->size() - watch.offset;
        } else {
            watch.output.clear(); watch.bytes = 0U;
        }
        Queue(watch, std::make_shared<const std::string>(watch.connection->session.watch->ResyncFrame()));
    };
    for (std::size_t i = 0; i < 64U; ++i) {
        auto entry = watch.connection->session.watch->Next();
        if (!entry) {
            const auto now = std::chrono::steady_clock::now();
            if (now < watch.next_progress) return;
            watch.next_progress = now + std::chrono::milliseconds(server_.config_.feed_progress_ms);
            const auto position = watch.connection->session.watch->Progress();
            if (!position) return;
            FeedEntry progress; progress.kind = FeedEntry::Kind::kProgress; progress.position = *position;
            auto frame = std::make_shared<const std::string>(EncodeFeedEntry(progress));
            // A full queue waits for its socket; a timer never discards data.
            if (frame->size() <= share && watch.bytes <= share - frame->size()) Queue(watch, std::move(frame));
            return;
        }
        if (entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kChunks)
            watch.next_progress = std::chrono::steady_clock::now() + std::chrono::milliseconds(server_.config_.feed_progress_ms);
        auto bytes = Frame(entry);
        if (entry->kind == FeedEntry::Kind::kEnd) {
            watch.close = true;
            Queue(watch, std::move(bytes));
            return;
        }
        if (bytes->size() > share || watch.bytes > share - bytes->size()) {
            resync(); return;
        }
        Queue(watch, std::move(bytes));
    }
    Wake(); // Fair batches; entries still in the ring must not be stranded.
}
void FeedIo::Read(Watch& watch) {
    if (watch.unwatch || watch.close || watch.dead ||
        (watch.connection->session.slot_watch && watch.connection->session.slot_watch->TakingOver())) return;
    auto& connection = *watch.connection;
    for (std::size_t i = 0; i < 64U; ++i) {
        // Reserve room for a bounded invalid-ACK reply before accepting more
        // input. Pausing POLLIN lets output drain without a busy poll loop.
        if (watch.bytes > std::max<std::size_t>(watch.share, 128U) - 128U) {
            watch.read_paused = true;
            if (auto* hook = hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kFeedInputBackpressure, watch.bytes);
            return;
        }
        watch.read_paused = false;
        std::string line;
        bool has_line;
        try { has_line = connection.pending.extract_line(&line, server_.config_.max_line_bytes); }
        catch (const std::runtime_error& error) { Reject(watch, error.what()); return; }
        if (has_line) {
            if (auto refused = server_.engine_->AdmitUserStatement(connection.session, line)) {
                Queue(watch, std::make_shared<const std::string>(std::move(*refused)));
                if (auto* hook = hook_.load(std::memory_order_acquire))
                    hook->Run(FeedDeliveryTestHook::Point::kAfterFeedReplyQueue, watch.bytes);
                continue;
            }
            try {
                auto parsed = cql::Parse(line.substr(0, line.find_first_of("\r\n")));
                if (const auto* ack = std::get_if<cql::Ack>(&parsed.statement); ack && connection.session.slot_watch) {
                    if (auto* hook = hook_.load(std::memory_order_acquire))
                        hook->Run(FeedDeliveryTestHook::Point::kBeforeSlotAck, 0U);
                    // The last check before the ACK is accepted: a change
                    // applied before it refuses the ACK.
                    if (!Permitted(watch)) return;
                    try {
                        // The worker may have queued a terminal error after
                        // the last change was sent. Drain that cause before
                        // handling any more controls on the ended stream.
                        if (connection.session.slot_watch->Ack(ack->revision) == SlotWatch::AckResult::kTerminal) return;
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(FeedDeliveryTestHook::Point::kAfterSlotAck, 0U);
                    }
                    catch (const std::invalid_argument& error) {
                        Queue(watch, std::make_shared<const std::string>(Protocol::Error("INVALID_ARGUMENT", error.what())));
                    }
                    Wake();
                    continue;
                }
                if (!std::holds_alternative<cql::Unwatch>(parsed.statement)) {
                    Reject(watch, "only UNWATCH is accepted while watching"); return;
                }
            } catch (const cql::ParseError&) {
                Reject(watch, "only UNWATCH is accepted while watching"); return;
            }
            watch.unwatch = true;
            if (connection.session.slot_watch) { connection.session.slot_watch->Unwatch(); Wake(); }
            else {
                // Establish removal and the linger policy before acknowledging
                // UNWATCH. Queued frames own their bytes and still precede OK.
                connection.session.watch.reset();
                watch.return_ready = true;
                Queue(watch, std::make_shared<const std::string>(Protocol::SimpleString("OK")));
            }
            return;
        }
        int result;
#ifdef CHUNKDB_WITH_OPENSSL
        if (connection.tls) {
            ERR_clear_error();
            result = SSL_read(connection.tls, watch.input.data(), static_cast<int>(watch.input.size()));
            if (result <= 0) {
                const auto error = SSL_get_error(connection.tls, result);
                if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                    watch.read_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT; return;
                }
                watch.dead = true; return;
            }
            watch.read_wait = POLLIN;
        } else
#endif
        {
            result = static_cast<int>(recv(connection.socket, watch.input.data(), static_cast<int>(watch.input.size()), 0));
            if (result < 0 && IsSocketInterruptedError(CurrentSocketErrorCode())) continue;
            if (result < 0 && WouldBlock(CurrentSocketErrorCode())) { watch.read_wait = POLLIN; return; }
            if (result <= 0) { watch.dead = true; return; }
        }
        connection.pending.append(watch.input.data(), static_cast<std::size_t>(result));
        try { connection.pending.enforce_partial_line_limit(server_.config_.max_line_bytes); }
        catch (const std::runtime_error& error) { Reject(watch, error.what()); return; }
    }
    Wake();
}
void FeedIo::Write(Watch& watch) {
#ifdef CHUNKDB_WITH_OPENSSL
    if (!watch.connection->tls) { WritePlain(watch); return; }
    for (std::size_t i = 0; i < 64U && !watch.output.empty(); ++i) {
        const auto& bytes = *watch.output.front().bytes;
        // One TLS record per call: every completed call is write progress.
        if (watch.write_size == 0) watch.write_size = static_cast<int>(std::min<std::size_t>(bytes.size() - watch.offset, 16384U));
        ERR_clear_error();
        const auto result = SSL_write(watch.connection->tls, bytes.data() + watch.offset, watch.write_size);
        if (result <= 0) {
            const auto error = SSL_get_error(watch.connection->tls, result);
            if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                watch.write_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT;
                if (error == SSL_ERROR_WANT_WRITE)
                    if (auto* hook = hook_.load(std::memory_order_acquire))
                        hook->Run(FeedDeliveryTestHook::Point::kAfterBlockedFeedWrite, 0U);
                return;
            }
            watch.dead = true; return;
        }
        Advance(watch, static_cast<std::size_t>(result));
        watch.write_size = 0;
        watch.write_wait = POLLOUT;
    }
#else
    WritePlain(watch);
#endif
}
void FeedIo::Advance(Watch& watch, std::size_t bytes) {
    watch.bytes -= bytes;
    if (watch.excess_frames != 0U) watch.excess_progress = std::chrono::steady_clock::now();
    while (bytes != 0U) {
        const auto take = std::min(bytes, watch.output.front().bytes->size() - watch.offset);
        if (watch.output.front().charged && watch.connection->session.slot_watch) {
            watch.connection->session.slot_watch->Consumed(take);
            Wake();
        }
        watch.offset += take;
        bytes -= take;
        if (watch.offset == watch.output.front().bytes->size()) {
            if (watch.output.front().revision && watch.connection->session.slot_watch)
                watch.connection->session.slot_watch->Sent(*watch.output.front().revision);
            if (watch.output.front().excess) --watch.excess_frames;
            watch.output.pop_front();
            watch.offset = 0U;
        }
    }
    if (watch.offset != 0U)
        if (auto* hook = hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kAfterPartialFeedWrite, watch.offset);
}
void FeedIo::WritePlain(Watch& watch) {
    if (watch.output.empty()) return;
#ifdef _WIN32
    std::array<WSABUF, 64> buffers{};
#else
    std::array<iovec, 64> buffers{};
#endif
    std::size_t count = 0U, total = 0U;
    for (const auto& frame : watch.output) {
        const auto offset = count == 0U ? watch.offset : 0U;
        const auto size = std::min(frame.bytes->size() - offset, static_cast<std::size_t>(INT_MAX) - total);
#ifdef _WIN32
        buffers[count] = {static_cast<ULONG>(size), const_cast<char*>(frame.bytes->data() + offset)};
#else
        buffers[count] = {const_cast<char*>(frame.bytes->data() + offset), size};
#endif
        total += size;
        if (++count == buffers.size() || total == static_cast<std::size_t>(INT_MAX)) break;
    }
    // Send shared frames directly, preserving the same fair 64-frame batch.
    // A partial scatter write advances across complete frames and pins only
    // the actually started front frame for subsequent resync.
    for (;;) {
#ifdef _WIN32
        DWORD written = 0U;
        const auto status = WSASend(watch.connection->socket, buffers.data(), static_cast<DWORD>(count),
                                   &written, 0U, nullptr, nullptr);
        const auto result = status == 0 ? static_cast<int>(written) : -1;
#else
        const auto result = writev(watch.connection->socket, buffers.data(), static_cast<int>(count));
#endif
        if (result < 0 && IsSocketInterruptedError(CurrentSocketErrorCode())) continue;
        if (result < 0 && WouldBlock(CurrentSocketErrorCode())) {
            watch.write_wait = POLLOUT;
            if (auto* hook = hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kAfterBlockedFeedWrite, 0U);
            return;
        }
        if (result <= 0) { watch.dead = true; return; }
        Advance(watch, static_cast<std::size_t>(result));
        watch.write_wait = POLLOUT;
        return;
    }
}
void FeedIo::Run() {
    std::vector<Watch> watches;
    const auto fail = [&](const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer, "feed I/O terminated", {{"error", error.what()}});
        server_.Stop();
    };
    try {
        while (server_.running_.load()) {
            if (auto* hook = hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeIoDrain, 0U);
            poller_.DrainWake();
            if (auto* hook = hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeIoScan, 0U);
            { std::lock_guard lock(mutex_);
              for (auto& connection : incoming_) {
                  Watch watch; watch.connection = connection;
                  watch.next_progress = std::chrono::steady_clock::now() + std::chrono::milliseconds(server_.config_.feed_progress_ms);
                  watches.push_back(std::move(watch));
                  connection.reset();
              }
              incoming_.clear(); }
            std::unordered_map<Table*, std::size_t> counts;
            for (const auto& watch : watches) ++counts[watch.connection->session.table.get()];
            for (auto& watch : watches) {
                // An ordinary UNWATCH can leave output pending after removal.
                // Keep its last budget until those frames and OK have drained.
                const auto budget = watch.connection->session.slot_watch ? watch.connection->session.slot_watch->budget()
                    : watch.unwatch ? watch.buffer_bytes : watch.connection->session.watch->buffer_bytes();
                watch.buffer_bytes = budget;
                watch.share = std::max<std::size_t>(1U, budget / counts[watch.connection->session.table.get()]);
                if (const auto& slot = watch.connection->session.slot_watch)
                    slot->SetQuota(watch.share > 128U ? watch.share - 128U : watch.share);
            }
            // Steps of a watch not yet active did nothing; its first delivery
            // must not wait for the catch-up sweep.
            for (const auto& watch : watches) if (watch.connection->session.slot_watch)
                if (watch.connection->session.slot_watch->Activate()) WakeCatchUp();
            for (auto& watch : watches) {
                if (watch.connection->record->killed.load()) {
                    if (const auto& slot = watch.connection->session.slot_watch) {
                        slot->Cancel();
                        if (slot->Finished()) watch.dead = true;
                        else WakeCatchUp();
                    } else watch.dead = true;
                }
                if (!watch.dead) {
                    // A TLS write WANT_* must finish before another TLS operation.
                    if (!watch.connection->record->killed.load() && (!watch.connection->tls || watch.write_size == 0)) Read(watch);
                    const auto feed_error = [&](const std::exception& error) {
                        LogMessage(LogLevel::kError, LogComponent::kServer, "watch feed terminated", {{"error", error.what()}});
                        watch.close = true;
                        Queue(watch, std::make_shared<const std::string>(Protocol::Error("INTERNAL", error.what())));
                    };
                    try {
                        Pull(watch, watch.share);
                    } catch (const std::logic_error& error) { feed_error(error); }
                      catch (const std::runtime_error& error) { feed_error(error); }
                    // After Pull and before Write: a frame pulled after a
                    // rights change is never written.
                    if (!watch.dead) (void)Permitted(watch);
                    const auto& slot = watch.connection->session.slot_watch;
                    const bool takeover_wait = slot && slot->TakingOver() && !slot->Finished();
                    if (!watch.dead && !takeover_wait && (!watch.connection->tls || watch.read_wait != POLLOUT)) Write(watch);
                    if (watch.close_immediate) watch.dead = true;
                    (void)ExpireStalledWrite(watch, std::chrono::steady_clock::now());
                    if (watch.read_paused && watch.bytes <= std::max<std::size_t>(watch.share, 128U) - 128U)
                        watch.read_paused = false;
                }
                if (watch.dead || ((watch.close || (watch.unwatch && watch.return_ready)) && watch.output.empty())) {
                    const bool ack_timeout_closed = watch.connection->session.slot_watch &&
                        watch.connection->session.slot_watch->AckTimedOut();
                    if (!watch.dead && !watch.close && watch.unwatch && watch.return_ready && server_.running_.load()) {
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(FeedDeliveryTestHook::Point::kBeforeReturnClient, 0U);
                        server_.ReturnClient(std::move(watch.connection));
                    } else {
                        // The socket closes before its unwritten charges are released.
                        const auto slot = watch.connection->session.slot_watch;
                        server_.CloseClient(*watch.connection);
                        ReleaseOutput(watch, slot);
                        watch.connection.reset();
                    }
                    if (ack_timeout_closed)
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(FeedDeliveryTestHook::Point::kAfterAckTimeoutClose, 0U);
                }
            }
            std::erase_if(watches, [](const Watch& watch) { return !watch.connection; });
            std::vector<SocketInterest> descriptors;
            bool buffered_tls = false;
            int progress_timeout = -1;
            const auto now = std::chrono::steady_clock::now();
            for (const auto& watch : watches) {
                short events = (watch.unwatch || watch.close || watch.read_paused) ? 0 : watch.read_wait;
                if (!watch.output.empty()) events |= watch.write_wait;
                if (watch.connection->session.slot_watch && watch.connection->session.slot_watch->TakingOver()) events = 0;
                if (watch.connection->tls && watch.write_size != 0 &&
                    !(watch.connection->session.slot_watch && watch.connection->session.slot_watch->TakingOver())) events = watch.write_wait;
                descriptors.push_back({watch.connection->socket, events, 0});
                const auto wait_until = [&](std::chrono::steady_clock::time_point at) {
                    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(at - now).count();
                    const auto timeout = remaining < 0 ? 0 : static_cast<int>(std::min<std::int64_t>(remaining + 1, INT_MAX));
                    if (progress_timeout < 0 || timeout < progress_timeout) progress_timeout = timeout;
                };
                if (!watch.connection->session.slot_watch && !watch.unwatch && !watch.close && !watch.dead) wait_until(watch.next_progress);
                if (watch.excess_frames != 0U && !watch.dead) wait_until(watch.excess_progress + std::chrono::milliseconds(server_.config_.client_io_timeout_ms));
#ifdef CHUNKDB_WITH_OPENSSL
                if (watch.write_size == 0 && !watch.unwatch && !watch.close && !watch.read_paused && watch.connection->tls &&
                    SSL_pending(watch.connection->tls) > 0) buffered_tls = true;
#endif
            }
            // Stop may have raced with DrainWake and had its coalesced wake
            // consumed. Do not enter an unbounded poll after that stop.
            if (!server_.running_.load()) break;
            (void)poller_.Wait(descriptors, buffered_tls ? 0 : progress_timeout);
            for (std::size_t i = 0; i < watches.size(); ++i)
                if (descriptors[i].ready & (POLLERR | POLLHUP | POLLNVAL)) watches[i].dead = true;
        }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    for (const auto& watch : watches) if (watch.connection) server_.CloseClient(*watch.connection);
    std::lock_guard lock(mutex_);
    for (const auto& connection : incoming_) if (connection) server_.CloseClient(*connection);
    incoming_.clear();
}
bool ChunkServer::HandOff(const std::shared_ptr<ServerConnection>& connection) {
    if (!running_.load()) return false;
    std::string error;
    if (!SetSocketNonBlocking(connection->socket, true, &error)) {
        LogMessage(LogLevel::kWarn, LogComponent::kServer, "WATCH hand-off failed", {{"error", error}});
        return false;
    }
    try {
        if (FeedIoHandle()->Add(connection)) return true;
    }
    catch (const std::bad_alloc& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer, "WATCH hand-off failed", {{"error", error.what()}});
    }
    return false;
}
void ChunkServer::ReturnClient(std::shared_ptr<ServerConnection> connection) {
    if (connection->session.slot_watch && !connection->session.slot_watch->ReleaseConnectionForReturn()) {
        CloseClient(*connection);
        return;
    }
    if (connection->session.slot_watch) connection->session.slot_watch->Cancel();
    connection->session.slot_watch.reset();
    connection->session.watch.reset();
    connection->session.watch_right.reset();
    connection->watch_slot = false;
    watch_count_.fetch_sub(1);
    connection->phase = ServerConnection::Phase::kInput;
    connection->read_ready = true;
    if (ParkClient(connection)) return;
    CloseClient(*connection);
}
void ChunkServer::CloseClient(ServerConnection& connection) {
    if (connection.socket == kInvalidSocket) return;
    std::shared_ptr<StatementControl> statement;
    {
        std::lock_guard lock(connection.record->mutex);
        statement = connection.record->statement;
        connection.record->state = "closing";
    }
    if (statement) (void)statement->Complete();
    if (connection.handshake_slot) {
        ReleaseHandshake(connection.handshake_source);
        connection.handshake_slot = false;
    }
    const auto slot = connection.session.slot_watch;
    if (connection.watch_slot) { watch_count_.fetch_sub(1); connection.watch_slot = false; }
    connection.session.watch.reset();
    if (connection.session.slot_watch) connection.session.slot_watch->Cancel();
    connection.session.slot_watch.reset();
    connection.session.transaction.reset();
    connection.session.user_ticket.reset();
    connection.session.pending_login.reset();
    connection.session.user_rights.reset();
    connection.session.table.reset();
    {
        std::lock_guard lock(connections_mutex_);
        connections_.erase(connection.session.connection_id);
    }
    connection.parameters.clear();
    connection.frames.reset();
    connection.reply_continuation.reset();
    std::string{}.swap(connection.response);
    std::string{}.swap(connection.request_line);
    std::string{}.swap(connection.pending.bytes);
    connection.pending.cursor = 0U;
    connection.payload = {};
    connection.payload_credit.reset();
    connection.line_credit.reset();
    connection.started_statement.reset();
    connection.reply_credit.reset();
#ifdef CHUNKDB_WITH_OPENSSL
    if (connection.tls) { SSL_free(connection.tls); connection.tls = nullptr; }
#endif
    { std::lock_guard lock(active_clients_mutex_);
      const auto it = std::find(active_clients_.begin(), active_clients_.end(), connection.socket);
      if (it != active_clients_.end()) active_clients_.erase(it); }
    CloseSocket(connection.socket);
    connection.socket = kInvalidSocket;
    if (slot) {
        slot->SocketClosed();
        if (slot->TakingOver())
            if (auto* hook = delivery_test_hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kAfterTakeoverClose, 0U);
    }
    engine_->metrics()->DecActiveConnections();
}
}
