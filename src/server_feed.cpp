#include "server_feed.hpp"
#include "server_tls.hpp"
#include "feed_protocol.hpp"
#include "cql.hpp"
#include "chunkdb/logging.hpp"
#include <algorithm>
#include <climits>
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
FeedIo::FeedIo(ChunkServer& server) : server_(server) {
#ifdef _WIN32
    const auto listener = CreateListenSocket("127.0.0.1", 0);
    sockaddr_in address{};
    int length = sizeof(address);
    if (getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        CloseSocket(listener);
        throw std::runtime_error("feed wake channel getsockname failed");
    }
    wake_write_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (wake_write_ == kInvalidSocket || connect(wake_write_, reinterpret_cast<sockaddr*>(&address), length) != 0) {
        CloseSocket(wake_write_); CloseSocket(listener);
        throw std::runtime_error("feed wake channel connect failed");
    }
    wake_read_ = accept(listener, nullptr, nullptr);
    CloseSocket(listener);
#else
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0)
        throw std::runtime_error("feed wake socketpair failed");
    wake_read_ = pair[0]; wake_write_ = pair[1];
#endif
    std::string error;
    if (wake_read_ == kInvalidSocket || !SetSocketNonBlocking(wake_read_, true, &error) ||
        !SetSocketNonBlocking(wake_write_, true, &error)) {
        CloseSocket(wake_read_); CloseSocket(wake_write_);
        throw std::runtime_error("feed wake channel: " + error);
    }
}
void FeedDeliveryTestAccess::SetHook(ChunkServer& server, FeedDeliveryTestHook* hook) {
    auto io = server.FeedIoHandle();
    {
        std::lock_guard lock(io->catchup_mutex_);
        io->hook_.store(hook, std::memory_order_release);
        for (const auto& slot : io->slots_) slot->hook_.store(hook, std::memory_order_release);
    }
    io->Wake();
}
FeedIo::~FeedIo() {
    Stop();
    CloseSocket(wake_read_); CloseSocket(wake_write_);
}
void FeedIo::Start() {
    catchup_thread_ = std::thread([this] { CatchUp(); });
    thread_ = std::thread([this] { Run(); });
}
void FeedIo::Stop() {
    Wake();
    if (thread_.joinable()) thread_.join();
    { std::lock_guard lock(catchup_mutex_); catchup_stop_ = true; }
    catchup_cv_.notify_one();
    if (catchup_thread_.joinable()) catchup_thread_.join();
}
void FeedIo::Wake() noexcept {
    { std::lock_guard lock(catchup_mutex_); catchup_wake_ = true; }
    catchup_cv_.notify_one();
    if (signaled_.exchange(true, std::memory_order_acq_rel)) return;
    const char byte = 1;
    for (;;) {
        const auto result = send(wake_write_, &byte, 1, 0);
        if (result == 1 || (result < 0 && WouldBlock(CurrentSocketErrorCode()))) return;
        if (result < 0 && IsSocketInterruptedError(CurrentSocketErrorCode())) continue;
        ShutdownSocket(wake_read_); // A broken channel must wake poll too.
        return;
    }
}
void FeedIo::DrainWake() {
    char bytes[256];
    for (;;) {
        const auto result = recv(wake_read_, bytes, sizeof(bytes), 0);
        if (result > 0) continue;
        if (result < 0 && IsSocketInterruptedError(CurrentSocketErrorCode())) continue;
        if (result < 0 && WouldBlock(CurrentSocketErrorCode())) break;
        throw std::runtime_error("feed wake channel closed");
    }
    // A publication racing with the drain is visible to the following scan,
    // or, after this clear, leaves another wake byte. No timed polling.
    signaled_.store(false, std::memory_order_release);
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
      slots_.push_back(std::move(state)); catchup_wake_ = true; }
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
        }
        for (const auto& state : states) {
            if (stop) state->Cancel();
            for (std::size_t step = 0U; step < 64U && !state->Finished(); ++step) state->WorkStep();
        }
        { std::lock_guard lock(catchup_mutex_);
          std::erase_if(slots_, [](const auto& state) { return state->Finished(); }); }
        if (stop) return;
    }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    std::vector<std::shared_ptr<SlotWatch>> states;
    { std::lock_guard lock(catchup_mutex_); states.swap(slots_); }
    for (const auto& state : states) { state->Cancel(); state->WorkStep(); }
}
void FeedIo::Queue(Watch& watch, std::shared_ptr<const std::string> bytes, std::optional<std::uint64_t> revision, bool charged) {
    watch.output.push_back({std::move(bytes), revision, charged});
    watch.bytes += watch.output.back().bytes->size();
}
void FeedIo::Reject(Watch& watch, std::string message) {
    watch.close = true;
    Queue(watch, std::make_shared<const std::string>(Protocol::Error("PROTOCOL", message)));
}
void FeedIo::Pull(Watch& watch, std::size_t share) {
    if (watch.close || watch.dead) return;
    if (const auto& slot = watch.connection->session.slot_watch) {
        slot->SetQuota(share > 128U ? share - 128U : share);
        // Admission already checked the share. Keep room for an accepted
        // frame when another watch subsequently reduces that share. Charges
        // remain in the table budget until the socket consumes the bytes.
        const auto capacity = std::max<std::size_t>(slot->budget(), 128U);
        for (std::size_t i = 0U; i < 64U; ++i) {
            auto entry = slot->Take(capacity - std::min(capacity, watch.bytes));
            if (!entry) return;
            watch.close = entry->close;
            watch.return_ready = entry->resume;
            Queue(watch, std::move(entry->bytes), entry->revision, entry->charged);
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
        FeedEntry entry;
        entry.kind = FeedEntry::Kind::kResync;
        entry.position = watch.connection->session.watch->Resync();
        Queue(watch, std::make_shared<const std::string>(EncodeFeedEntry(entry)));
    };
    for (std::size_t i = 0; i < 64U; ++i) {
        auto entry = watch.connection->session.watch->Next();
        if (!entry) return;
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
    if (watch.unwatch || watch.close || watch.dead) return;
    auto& connection = *watch.connection;
    for (std::size_t i = 0; i < 64U; ++i) {
        // Reserve room for a bounded invalid-ACK reply before accepting more
        // input. Pausing POLLIN lets output drain without a busy poll loop.
        if (connection.session.slot_watch && watch.bytes > std::max<std::size_t>(watch.share, 128U) - 128U) {
            watch.read_paused = true;
            return;
        }
        watch.read_paused = false;
        std::string line;
        bool has_line;
        try { has_line = connection.pending.extract_line(&line, server_.config_.max_line_bytes); }
        catch (const std::runtime_error& error) { Reject(watch, error.what()); return; }
        if (has_line) {
            try {
                auto parsed = cql::Parse(line.substr(0, line.find_first_of("\r\n")));
                if (const auto* ack = std::get_if<cql::Ack>(&parsed.statement); ack && connection.session.slot_watch) {
                    try { connection.session.slot_watch->Ack(ack->revision); }
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
        if (watch.write_size == 0) watch.write_size = static_cast<int>(std::min<std::size_t>(bytes.size() - watch.offset, INT_MAX));
        ERR_clear_error();
        const auto result = SSL_write(watch.connection->tls, bytes.data() + watch.offset, watch.write_size);
        if (result <= 0) {
            const auto error = SSL_get_error(watch.connection->tls, result);
            if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                watch.write_wait = error == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT; return;
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
            watch.output.pop_front();
            watch.offset = 0U;
        }
    }
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
        if (result < 0 && WouldBlock(CurrentSocketErrorCode())) { watch.write_wait = POLLOUT; return; }
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
            DrainWake();
            if (auto* hook = hook_.load(std::memory_order_acquire))
                hook->Run(FeedDeliveryTestHook::Point::kBeforeIoScan, 0U);
            { std::lock_guard lock(mutex_);
              for (auto& connection : incoming_) {
                  Watch watch; watch.connection = connection;
                  watches.push_back(std::move(watch));
                  connection.reset();
              }
              incoming_.clear(); }
            std::unordered_map<Table*, std::size_t> counts;
            for (const auto& watch : watches) ++counts[watch.connection->session.table.get()];
            for (auto& watch : watches) {
                const auto budget = watch.connection->session.slot_watch ? watch.connection->session.slot_watch->budget() : watch.connection->session.watch->buffer_bytes();
                watch.share = std::max<std::size_t>(1U, budget / counts[watch.connection->session.table.get()]);
                if (const auto& slot = watch.connection->session.slot_watch)
                    slot->SetQuota(watch.share > 128U ? watch.share - 128U : watch.share);
            }
            for (const auto& watch : watches) if (watch.connection->session.slot_watch)
                watch.connection->session.slot_watch->Activate();
            for (auto& watch : watches) {
                if (!watch.dead) {
                    // A TLS write WANT_* must finish before another TLS operation.
                    if (!watch.connection->tls || watch.write_size == 0) Read(watch);
                    const auto feed_error = [&](const std::exception& error) {
                        LogMessage(LogLevel::kError, LogComponent::kServer, "watch feed terminated", {{"error", error.what()}});
                        watch.close = true;
                        Queue(watch, std::make_shared<const std::string>(Protocol::Error("INTERNAL", error.what())));
                    };
                    try {
                        Pull(watch, watch.share);
                    } catch (const std::logic_error& error) { feed_error(error); }
                      catch (const std::runtime_error& error) { feed_error(error); }
                    if (!watch.connection->tls || watch.read_wait != POLLOUT) Write(watch);
                    if (watch.read_paused && watch.bytes <= std::max<std::size_t>(watch.share, 128U) - 128U)
                        watch.read_paused = false;
                }
                if (watch.dead || ((watch.close || (watch.unwatch && watch.return_ready)) && watch.output.empty())) {
                    bool returned = false;
                    if (!watch.dead && !watch.close && watch.unwatch && watch.return_ready && server_.running_.load()) {
                        if (auto* hook = hook_.load(std::memory_order_acquire))
                            hook->Run(FeedDeliveryTestHook::Point::kBeforeReturnClient, 0U);
                        std::string error;
                        if (SetSocketNonBlocking(watch.connection->socket, false, &error)) {
                            server_.ReturnClient(watch.connection); returned = true;
                        }
                    }
                    if (!returned) server_.CloseClient(*watch.connection);
                    watch.connection.reset();
                }
            }
            std::erase_if(watches, [](const Watch& watch) { return !watch.connection; });
#ifdef _WIN32
            std::vector<WSAPOLLFD> descriptors;
#else
            std::vector<pollfd> descriptors;
#endif
            descriptors.push_back({wake_read_, POLLIN, 0});
            bool buffered_tls = false;
            for (const auto& watch : watches) {
                short events = (watch.unwatch || watch.close || watch.read_paused) ? 0 : watch.read_wait;
                if (!watch.output.empty()) events |= watch.write_wait;
                if (watch.connection->tls && watch.write_size != 0) events = watch.write_wait;
                descriptors.push_back({watch.connection->socket, events, 0});
#ifdef CHUNKDB_WITH_OPENSSL
                if (watch.write_size == 0 && !watch.unwatch && !watch.close && !watch.read_paused && watch.connection->tls &&
                    SSL_pending(watch.connection->tls) > 0) buffered_tls = true;
#endif
            }
#ifdef _WIN32
            const int result = WSAPoll(descriptors.data(), static_cast<ULONG>(descriptors.size()), buffered_tls ? 0 : -1);
#else
            const int result = poll(descriptors.data(), descriptors.size(), buffered_tls ? 0 : -1);
#endif
            if (result < 0 && !IsSocketInterruptedError(CurrentSocketErrorCode()))
                throw std::runtime_error("feed poll: " + SocketErrorText());
            for (std::size_t i = 0; i < watches.size(); ++i)
                if (descriptors[i + 1U].revents & (POLLERR | POLLHUP | POLLNVAL)) watches[i].dead = true;
        }
    } catch (const std::bad_alloc& error) { fail(error); }
      catch (const std::logic_error& error) { fail(error); }
      catch (const std::runtime_error& error) { fail(error); }
    for (const auto& watch : watches) if (watch.connection) server_.CloseClient(*watch.connection);
    std::lock_guard lock(mutex_);
    for (const auto& connection : incoming_) if (connection) server_.CloseClient(*connection);
    incoming_.clear();
}
bool ChunkServer::HandOff(ServerConnection& connection) {
    if (!running_.load()) return false;
    std::string error;
    if (!SetSocketNonBlocking(connection.socket, true, &error)) {
        LogMessage(LogLevel::kWarn, LogComponent::kServer, "WATCH hand-off failed", {{"error", error}});
        return false;
    }
    std::shared_ptr<ServerConnection> owned;
    try {
        // Ordinary connections keep their state on the worker's stack.
        // Allocate only when ownership actually crosses to the I/O thread.
        owned = std::make_shared<ServerConnection>(std::move(connection));
        connection.socket = kInvalidSocket;
        connection.tls = nullptr;
        connection.watch_slot = false;
        if (FeedIoHandle()->Add(owned)) return true;
    }
    catch (const std::bad_alloc& error) {
        if (owned) {
            connection = std::move(*owned);
            owned.reset();
        }
        LogMessage(LogLevel::kError, LogComponent::kServer, "WATCH hand-off failed", {{"error", error.what()}});
    }
    if (owned) connection = std::move(*owned);
    return false;
}
void ChunkServer::ReturnClient(std::shared_ptr<ServerConnection> connection) {
    if (connection->session.slot_watch) connection->session.slot_watch->Cancel();
    connection->session.slot_watch.reset();
    connection->session.watch.reset();
    connection->watch_slot = false;
    watch_count_.fetch_sub(1);
    { std::lock_guard lock(pending_clients_mutex_);
      if (running_.load()) {
          pending_clients_.push(PendingClient{.resumed = connection,
              .socket = static_cast<decltype(listen_socket_)>(connection->socket),
              .accepted_at = std::chrono::steady_clock::now()});
          pending_clients_cv_.notify_one();
          return;
      } }
    CloseClient(*connection);
}
void ChunkServer::CloseClient(ServerConnection& connection) {
    if (connection.watch_slot) { watch_count_.fetch_sub(1); connection.watch_slot = false; }
    connection.session.watch.reset();
    if (connection.session.slot_watch) connection.session.slot_watch->Cancel();
    connection.session.slot_watch.reset();
#ifdef CHUNKDB_WITH_OPENSSL
    if (connection.tls) { SSL_free(connection.tls); connection.tls = nullptr; }
#endif
    { std::lock_guard lock(active_clients_mutex_);
      const auto it = std::find(active_clients_.begin(), active_clients_.end(), connection.socket);
      if (it != active_clients_.end()) active_clients_.erase(it); }
    CloseSocket(connection.socket);
    connection.socket = kInvalidSocket;
    engine_->metrics()->DecActiveConnections();
}
}
