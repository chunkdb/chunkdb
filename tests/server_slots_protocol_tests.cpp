#include <atomic>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <future>
#include <map>

#include "server_slots_test_utils.hpp"
#include "feed_slots.hpp"
#include "slot_watch.hpp"
#include "server_feed.hpp"
#include "feed_slot_records.hpp"
#ifdef CHUNKDB_WITH_OPENSSL
#include <openssl/bio.h>
#include <openssl/pem.h>
#endif

namespace chunkdb {
struct SlotAckDeadlineTestAccess {
    static bool Publish(SlotWatch& watch, const std::shared_ptr<const FeedEntry>& entry) { return watch.Publish(entry); }
    static void Sent(SlotWatch& watch, std::uint64_t revision, std::chrono::steady_clock::time_point now) {
        watch.SentAt(revision, now);
    }
    static bool Expire(SlotWatch& watch, std::chrono::steady_clock::time_point now) { return watch.ExpireAckDeadline(now); }
    static std::size_t Count(const SlotWatch& watch) { std::lock_guard lock(watch.mutex_); return watch.receipts_.size(); }
    static std::size_t Charged(const SlotWatch& watch) { return watch.table_bytes_->load(std::memory_order_acquire); }
    static std::size_t Excess(const SlotWatch& watch) { return watch.pool_->bytes.load(std::memory_order_acquire); }
    static constexpr std::size_t kReceipt = SlotWatch::kReceiptBytes;
    static std::size_t PreparedSize(const SlotWatch& watch) { return watch.prepared_ ? watch.prepared_->size : 0U; }
    // Stands in for other output charged to the table's within-share counter.
    static void ChargeTable(SlotWatch& watch, std::size_t bytes) { watch.table_bytes_->fetch_add(bytes, std::memory_order_acq_rel); }
    static void ReleaseTable(SlotWatch& watch, std::size_t bytes) { watch.table_bytes_->fetch_sub(bytes, std::memory_order_acq_rel); }
    static std::shared_ptr<const std::string> Prepared(const SlotWatch& watch) {
        return watch.prepared_ && watch.prepared_->built ? watch.prepared_->next.back().bytes : nullptr;
    }
    static void Hook(SlotWatch& watch, FeedDeliveryTestHook* hook) { watch.hook_.store(hook, std::memory_order_release); }
    static std::uint64_t Acked(const SlotWatch& watch) { std::lock_guard lock(watch.mutex_); return watch.acknowledged_; }
};
struct AckDeadlineIoTestAccess {
    static void PendingClose(ChunkServer& server, const std::shared_ptr<SlotWatch>& state, bool partial, bool tls_retry) {
        FeedIo io(server);
        FeedIo::Watch watch;
        watch.connection = std::make_shared<ServerConnection>(); watch.connection->session.slot_watch = state;
        while (auto output = state->Take(1024U * 1024U))
            io.Queue(watch, output->bytes, output->revision, output->charged);
        assert(!watch.output.empty());
        if (partial) { watch.offset = 1U; --watch.bytes; state->Consumed(1U); }
        if (tls_retry) watch.write_size = 1; // A pinned buffer must never be replaced, even at offset zero.
        assert(SlotAckDeadlineTestAccess::Expire(*state, std::chrono::steady_clock::now() + std::chrono::hours(1)));
        state->Activate(); state->WorkStep(); assert(state->AckTimedOut());
        io.Pull(watch, 4096U);
        if (partial || tls_retry) { assert(watch.dead); return; }
        assert(watch.close && watch.close_immediate && watch.output.size() == 1U);
        assert(watch.output.front().bytes->starts_with("-ERR TIMEOUT reason=ack_timeout"));
        state->Consumed(watch.output.front().bytes->size());
    }
    // `holder` holds an admitted pool batch; `waiter` waits for the pool with `entry`.
    static void StalledWrite(ChunkServer& server, const std::shared_ptr<SlotWatch>& holder, const std::shared_ptr<SlotWatch>& waiter,
        const std::shared_ptr<const FeedEntry>& entry, std::chrono::milliseconds timeout) {
        using std::chrono::steady_clock;
        FeedIo io(server);
        FeedIo::Watch watch;
        watch.connection = std::make_shared<ServerConnection>(); watch.connection->session.slot_watch = holder;
        io.Pull(watch, 1024U);
        assert(watch.excess_frames != 0U);
        // Writes spaced half a timeout apart, simulated by moving the last progress
        // back: together they span two timeouts, and each restarts the clock.
        for (int write = 0; write < 4; ++write) {
            watch.excess_progress -= timeout / 2;
            assert(!io.ExpireStalledWrite(watch, steady_clock::now()));
            const auto before = steady_clock::now();
            io.Advance(watch, 1U);
            assert(watch.excess_progress >= before);
        }
        const auto progress = watch.excess_progress;
        assert(!io.ExpireStalledWrite(watch, progress + timeout - std::chrono::milliseconds(1)) && !watch.dead);
        assert(!SlotAckDeadlineTestAccess::Publish(*waiter, entry)); // The pool is still held.
        // No progress for the whole timeout: the watch is closed once.
        assert(io.ExpireStalledWrite(watch, progress + timeout) && watch.dead);
        assert(!io.ExpireStalledWrite(watch, progress + 2 * timeout));
        io.ReleaseOutput(watch, holder); // As the close path does.
        assert(SlotAckDeadlineTestAccess::Excess(*holder) == 0U);
        assert(SlotAckDeadlineTestAccess::Publish(*waiter, entry));
    }
#ifdef CHUNKDB_WITH_OPENSSL
    // A server TLS session whose peer is a memory pipe smaller than one record,
    // so SSL_write blocks without any socket or reader scheduling.
    static void TlsWriteCap(ChunkServer& server) {
        auto* server_context = SSL_CTX_new(TLS_server_method());
        auto* client_context = SSL_CTX_new(TLS_client_method());
        const auto& cert_pem = test::TestTls().cert_pem;
        const auto& key_pem = test::TestTls().key_pem;
        auto* cert_bio = BIO_new_mem_buf(cert_pem.data(), static_cast<int>(cert_pem.size()));
        auto* key_bio = BIO_new_mem_buf(key_pem.data(), static_cast<int>(key_pem.size()));
        auto* cert = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
        auto* key = PEM_read_bio_PrivateKey(key_bio, nullptr, nullptr, nullptr);
        assert(server_context && client_context && cert && key);
        assert(SSL_CTX_use_certificate(server_context, cert) == 1 && SSL_CTX_use_PrivateKey(server_context, key) == 1);
        auto* server_tls = SSL_new(server_context);
        auto* client_tls = SSL_new(client_context);
        BIO* server_bio = nullptr;
        BIO* client_bio = nullptr;
        assert(BIO_new_bio_pair(&server_bio, 8192, &client_bio, 8192) == 1);
        SSL_set_bio(server_tls, server_bio, server_bio);
        SSL_set_bio(client_tls, client_bio, client_bio);
        SSL_set_accept_state(server_tls); SSL_set_connect_state(client_tls);
        for (int step = 0; step < 100 && !(SSL_is_init_finished(server_tls) && SSL_is_init_finished(client_tls)); ++step) {
            (void)SSL_do_handshake(client_tls); (void)SSL_do_handshake(server_tls);
        }
        assert(SSL_is_init_finished(server_tls) && SSL_is_init_finished(client_tls));
        std::string received;
        const auto drain = [&] {
            std::array<char, 4096> buffer{};
            for (;;) {
                const auto count = SSL_read(client_tls, buffer.data(), static_cast<int>(buffer.size()));
                if (count <= 0) { ERR_clear_error(); return; }
                received.append(buffer.data(), static_cast<std::size_t>(count));
            }
        };
        {
            FeedIo io(server);
            FeedIo::Watch watch;
            watch.connection = std::make_shared<ServerConnection>(); watch.connection->tls = server_tls;
            const auto frame = std::make_shared<const std::string>(64U * 1024U, 'x');
            io.Queue(watch, frame);
            io.Write(watch);
            // The first call passed one record and blocked with that length pinned.
            assert(!watch.dead && watch.offset == 0U && watch.write_size == 16384 && watch.write_wait == POLLOUT);
            for (int round = 0; round < 64 && !watch.output.empty(); ++round) {
                const auto offset = watch.offset;
                drain();
                // OpenSSL refuses a retry with another buffer or length, so success proves both are reused.
                io.Write(watch);
                assert(!watch.dead);
                const auto written = (watch.output.empty() ? frame->size() : watch.offset) - offset;
                assert(written <= 16384U && (watch.output.empty() || watch.write_size == 0 || watch.write_size <= 16384));
            }
            drain();
            assert(watch.output.empty() && received == *frame);
            watch.connection->tls = nullptr;
        }
        SSL_free(server_tls); SSL_free(client_tls);
        SSL_CTX_free(server_context); SSL_CTX_free(client_context);
        X509_free(cert); EVP_PKEY_free(key); BIO_free(cert_bio); BIO_free(key_bio);
    }
#endif
    // `state` holds one admitted excess-pool batch of `size` bytes, nothing else.
    static void ExcessQueue(ChunkServer& server, const std::shared_ptr<SlotWatch>& state, std::size_t size, std::size_t budget) {
        FeedIo io(server);
        FeedIo::Watch watch;
        watch.connection = std::make_shared<ServerConnection>(); watch.connection->session.slot_watch = state;
        // Uncharged replies leave less than the batch of the budget's room.
        const auto reply = budget - size + 1U;
        io.Queue(watch, std::make_shared<const std::string>(reply, 'x'));
        const auto before = std::chrono::steady_clock::now();
        io.Pull(watch, 1024U);
        assert(!state->Take(std::numeric_limits<std::size_t>::max())); // The whole accepted batch was taken.
        assert(watch.output.size() >= 2U && watch.excess_frames == watch.output.size() - 1U);
        assert(watch.excess_progress >= before); // The no-progress clock started.
        // A partly written batch dropped with its connection releases the pool at once.
        io.Advance(watch, reply + 1U);
        assert(SlotAckDeadlineTestAccess::Excess(*state) == size - 1U);
        io.ReleaseOutput(watch, state);
        assert(watch.output.empty() && watch.excess_frames == 0U);
        assert(SlotAckDeadlineTestAccess::Excess(*state) == 0U && SlotAckDeadlineTestAccess::Charged(*state) == 0U);
    }
};
}

namespace {
using namespace chunkdb;
using namespace chunkdb::slot_socket_test;

template <typename F> class Cleanup {
  public:
    explicit Cleanup(F action) : action_(std::move(action)) {}
    Cleanup(const Cleanup&) = delete;
    Cleanup& operator=(const Cleanup&) = delete;
    ~Cleanup() { action_(); }
  private:
    F action_;
};
template <typename F> bool RunGroup(bool tls, std::string_view name, F function) {
    test::FeedPhaseWatchdog watchdog(std::string(tls ? "TLS " : "plain ") + std::string(name));
    std::cout << (tls ? "TLS " : "plain ") << name << std::endl;
    try { function(tls); RethrowBackgroundServerError(); return true; }
    catch (const std::exception& error) {
        std::cerr << "FAILED " << (tls ? "TLS " : "plain ") << name << ": " << error.what() << std::endl;
        if (background_server_error) {
            try { RethrowBackgroundServerError(); }
            catch (const std::exception& background) { std::cerr << "background server: " << background.what() << std::endl; }
        }
        return false;
    }
}

std::uint64_t Set(Client& writer, unsigned value, int x = 0) {
    return Number(writer.Command("SET BLOCK " + std::to_string(x) + " 0 IN t n = " + std::to_string(value)));
}
void Create(Client& client, std::string_view filters = {}) {
    client.Ok("CREATE TABLE t (n u8, label text(128)) CHUNK 4 x 4");
    client.Ok("CREATE SLOT 'consumer' ON t" + std::string(filters));
}
class TakeoverSchedule : public FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        std::unique_lock lock(mutex);
        if (point == Point::kAfterTakeoverFence) {
            fenced = true; changed.notify_all();
            changed.wait(lock, [&] { return released; });
        } else if (point == Point::kAfterTakeoverClose) {
            closed = true; changed.notify_all();
        } else if (point == Point::kAfterSlotAck) {
            accepted = true; changed.notify_all();
        }
    }
    void Wait(const bool& flag, const char* cause) {
        std::unique_lock lock(mutex);
        if (!changed.wait_for(lock, 10s, [&] { return flag; }))
            throw std::runtime_error(std::string("takeover schedule: ") + cause);
    }
    void Release() { std::lock_guard lock(mutex); released = true; changed.notify_all(); }
    std::mutex mutex; std::condition_variable changed;
    bool fenced = false, closed = false, released = false, accepted = false;
};
void Takeover(bool tls) {
    TakeoverSchedule schedule;
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    auto old = harness.Connect(); (void)Start(old->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 7); assert(Change(NextChange(*old)) == first);
    FeedDeliveryTestAccess::SetObserverHook(harness.server(), &schedule);
    Cleanup clear([&] { schedule.Release(); FeedDeliveryTestAccess::SetObserverHook(harness.server(), nullptr); });
    old->Line("ACK " + std::to_string(first)); schedule.Wait(schedule.accepted, "accepted old ACK");
    const auto second = Set(*writer, 8); assert(Change(NextChange(*old)) == second);
    auto replacement = harness.Connect(); replacement->Line("WATCH t SLOT 'consumer' TAKEOVER");
    schedule.Wait(schedule.fenced, "old generation fence");
    // The waiting takeover holds no table lease: metadata and a competing
    // ownership request remain usable while its old socket is still alive.
    auto other = harness.Connect();
    const auto busy = other->Command("WATCH t SLOT 'consumer' TAKEOVER");
    Error(busy, "SLOT_BUSY"); assert(busy.value.find("reason=takeover_pending") != std::string::npos);
    writer->Ok("ALTER TABLE t SET feed_buffer_bytes = 16777216");
    schedule.Release();
    const auto start = Start(replacement->Read());
    schedule.Wait(schedule.closed, "actual old socket close");
    assert(start.second == first && Change(NextChange(*replacement)) == second);
    // The boundary was clear, so the one best-effort terminal frame is valid.
    for (;;) { const auto reply = old->Read(); if (reply.type == '-') { Error(reply, "KILLED"); break; } }
    Closed(*old);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == first);
    replacement->Line("ACK " + std::to_string(second)); Unwatch(*replacement);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == second);
}
void TakeoverFence(bool tls) {
    for (const bool drop : {false, true}) {
        TakeoverSchedule schedule;
        Harness harness(tls);
        auto writer = harness.Connect(); Create(*writer);
        const auto table = harness.catalog->Find("t");
        auto old = SlotWatch::Create(table, "consumer", {});
        try { (void)SlotWatch::Create(table, "consumer", {}, true); assert(false); }
        catch (const FeedSlotBusyError& error) { assert(error.reason() == "takeover_pending"); }
        old->Activate();
        const auto first = Set(*writer, 7), second = Set(*writer, 8);
        FeedSlotTestAccess::Sync(*table);
        old->Sent(second); assert(old->Ack(first) == SlotWatch::AckResult::kAccepted);
        SlotAckDeadlineTestAccess::Hook(*old, &schedule);
        auto replacement = std::async(std::launch::async, [&] { return SlotWatch::Create(table, "consumer", {}, true); });
        Cleanup finish([&] {
            schedule.Release(); old->Cancel(); old->WorkStep(); old->SocketClosed();
            if (replacement.valid()) replacement.wait();
            SlotAckDeadlineTestAccess::Hook(*old, nullptr);
        });
        schedule.Wait(schedule.fenced, "native generation fence");
        assert(old->Ack(second) == SlotWatch::AckResult::kTerminal);
        assert(SlotAckDeadlineTestAccess::Acked(*old) == first);
        assert(!old->ReleaseConnectionForReturn()); // A reserved generation cannot become an ordinary connection.
        try { (void)SlotWatch::Create(table, "consumer", {}, true); assert(false); }
        catch (const FeedSlotBusyError& error) { assert(error.reason() == "takeover_pending"); }
        if (drop) {
            table->DropFeedSlot("consumer");
            const auto fresh = table->CreateFeedSlot("consumer");
            assert(fresh.position.revision >= second);
        }
        schedule.Release();
        old->WorkStep(); assert(old->Finished());
        // Durable publication alone must not release the replacement. Its old
        // accepted socket still owns the connection until CloseClient reports.
        assert(replacement.wait_for(100ms) == std::future_status::timeout);
        old->SocketClosed(); schedule.Release();
        if (drop) {
            try { (void)replacement.get(); assert(false); }
            catch (const FeedSlotLostError&) {}
            assert(table->ListFeedSlots().front().position.revision >= second);
        } else {
            auto next = replacement.get();
            assert(next->position().revision == first);
            next->Cancel(); next->WorkStep();
        }
    }
}

void TakeoverUnwatchReturn(bool tls) {
    struct Reserved : FeedDeliveryTestHook {
        std::mutex mutex; std::condition_variable changed; bool reserved = false, released = false;
        void Run(Point point, std::size_t) override {
            if (point != Point::kAfterTakeoverReserved) return;
            std::unique_lock lock(mutex); reserved = true; changed.notify_all();
            changed.wait(lock, [&] { return released; });
        }
        void Wait() {
            std::unique_lock lock(mutex);
            if (!changed.wait_for(lock, 10s, [&] { return reserved; }))
                throw std::runtime_error("takeover did not reserve the old generation");
        }
        void Release() { std::lock_guard lock(mutex); released = true; changed.notify_all(); }
    } reserved;
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    auto old = harness.Connect(); (void)Start(old->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 7); assert(Change(NextChange(*old)) == first);
    old->Line("ACK " + std::to_string(first)); WaitAck(*writer, "t", "consumer", first);
    const auto second = Set(*writer, 8); assert(Change(NextChange(*old)) == second);
    FeedDeliveryTestAccess::SetObserverHook(harness.server(), &reserved);
    Cleanup clear([&] { reserved.Release(); FeedDeliveryTestAccess::SetObserverHook(harness.server(), nullptr); });
    auto replacement = harness.Connect(); replacement->Line("WATCH t SLOT 'consumer' TAKEOVER");
    reserved.Wait(); // Table/catalog locks have already been released.
    Unwatch(*old); Closed(*old); // A pending reservation must prevent ordinary connection return.
    reserved.Release();
    assert(Start(replacement->Read()).second == first);
    assert(Change(NextChange(*replacement)) == second);
    Unwatch(*replacement);
}

void SlotRecordOutcome(bool tls) {
    for (const bool published : {false, true}) {
        Harness harness(tls);
        auto writer = harness.Connect(); Create(*writer);
        {
            txn_test::ScopedEnv failure(published ? "CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE"
                : "CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE", "1");
            const auto reply = writer->Command("DROP SLOT 'consumer' ON t");
            Error(reply, published ? "OUTCOME_UNKNOWN" : "INTERNAL");
            if (published) assert(reply.value.find("stage=slot_records") != std::string::npos);
        }
        if (!published) {
            assert(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer").type == '%');
            writer->Ok("DROP SLOT 'consumer' ON t");
            assert(Set(*writer, 7) != 0U); // Failure before publication does not poison healthy state.
        } else Error(writer->Command("SET BLOCK 0 0 IN t n=7"), "INTERNAL");
    }
    // An ACK publication failure during takeover must reach the replacement
    // with its original typed outcome; no new claim is installed.
    TakeoverSchedule schedule;
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    const auto table = harness.catalog->Find("t");
    auto old = SlotWatch::Create(table, "consumer", {}); old->Activate();
    const auto first = Set(*writer, 7); FeedSlotTestAccess::Sync(*table);
    old->Sent(first); assert(old->Ack(first) == SlotWatch::AckResult::kAccepted);
    SlotAckDeadlineTestAccess::Hook(*old, &schedule);
    auto replacement = std::async(std::launch::async, [&] { return SlotWatch::Create(table, "consumer", {}, true); });
    Cleanup clear([&] {
        schedule.Release(); old->Cancel(); old->WorkStep(); old->SocketClosed();
        if (replacement.valid()) replacement.wait();
        SlotAckDeadlineTestAccess::Hook(*old, nullptr);
    });
    schedule.Wait(schedule.fenced, "takeover ACK publication fence"); schedule.Release();
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        old->WorkStep(); assert(old->Finished());
    }
    old->SocketClosed();
    try { (void)replacement.get(); assert(false); }
    catch (const WriteOutcomeUnknownError& error) { assert(error.stage() == "slot_records"); }
    Error(writer->Command("SET BLOCK 0 0 IN t n=8"), "INTERNAL");
}

void TakeoverRights(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin, " OWNER one");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    for (const auto* user : {"one", "two"}) {
        admin->Ok(std::string("CREATE USER ") + user + " VERIFIER '" + verifier + "'");
        admin->Ok(std::string("GRANT READ ON t TO ") + user);
    }
    Client old(harness.port, tls); old.Login("one", "pw");
    (void)Start(old.Command("WATCH t SLOT 'consumer'"));
    Client other(harness.port, tls); other.Login("two", "pw");
    Error(other.Command("WATCH t SLOT 'consumer' TAKEOVER"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON t TO two"); // Table ADMIN is deliberately insufficient.
    Error(other.Command("WATCH t SLOT 'consumer' TAKEOVER"), "PERMISSION_DENIED");
    const auto first = Set(*admin, 7); assert(Change(NextChange(old)) == first);
    Client same(harness.port, tls); same.Login("one", "pw");
    assert(Start(same.Command("WATCH t SLOT 'consumer' TAKEOVER")).second < first);
    Error(old.Read(), "KILLED"); Closed(old); assert(Change(NextChange(same)) == first);
    admin->Ok("GRANT ADMIN ON * TO two");
    (void)Start(other.Command("WATCH t SLOT 'consumer' TAKEOVER"));
    Error(same.Read(), "KILLED"); Closed(same); assert(Change(NextChange(other)) == first);
    Unwatch(other);
}

void Lifecycle(bool tls) {
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    auto watch = harness.Connect();
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer'"));
    auto listed = writer->Command("SHOW SLOTS ON t");
    const auto& slot = Slot(listed, "t", "consumer");
    assert(slot.items.size() == 18U && Field(slot, "epoch").value == initial.first);
    assert(Number(Field(slot, "acked")) == initial.second && !Boolean(Field(slot, "lost")));
    auto second = harness.Connect();
    Error(second->Command("WATCH t SLOT 'consumer'"), "SLOT_BUSY");
    // A lone relaxed-mode write must be delivered even without another event.
    const auto first = Set(*writer, 7);
    assert(Change(NextChange(*watch)) == first);
    watch->Line("ACK " + std::to_string(first + 1U)); Error(watch->Read(), "INVALID_ARGUMENT");
    const auto next = Set(*writer, 8);
    assert(Change(NextChange(*watch)) == next); // Invalid ACK does not close the stream.
    watch->Line("ACK " + std::to_string(next));
    WaitAck(*writer, "t", "consumer", next); // No subsequent traffic on the watch.
    assert(!watch->Ready(150ms)); // Successful ACK has no reply.
    watch->Send("UNWATCH\r\nPING\r\n");
    const auto unwatch = watch->Read(); assert(unwatch.type == '+' && unwatch.value == "OK");
    const auto pong = watch->Read(); assert(pong.type == '+' && pong.value == "PONG");
    assert(Start(second->Command("WATCH t SLOT 'consumer'")) == std::make_pair(initial.first, next));
    const auto third = Set(*writer, 9);
    assert(Change(NextChange(*second)) == third);
    Unwatch(*second);
    // AFTER represents the consumer's own committed position independently of ACK.
    assert(Start(second->Command("WATCH t SLOT 'consumer' AFTER " + initial.first + " " + std::to_string(third))) ==
        std::make_pair(initial.first, third));
    const auto fourth = Set(*writer, 10); assert(Change(NextChange(*second)) == fourth);
    second->Line("ACK " + std::to_string(fourth)); Unwatch(*second);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS"), "t", "consumer"), "acked")) == fourth);
    assert(Start(second->Command("WATCH t SLOT 'consumer' AFTER " + initial.first + " " + std::to_string(next))) ==
        std::make_pair(initial.first, fourth));
    Unwatch(*second);
    // Epoch mismatch carries the new epoch in a resync rather than replaying old bytes.
    const auto now = Start(second->Command("WATCH t SLOT 'consumer' AFTER ffffffffffffffffffffffffffffffff 0"));
    const auto resync = second->Read();
    assert(resync.type == '>' && resync.items[0].value == "resync" && resync.items[1].value == now.first);
    Unwatch(*second);
    writer->Ok("DROP SLOT 'consumer' ON t");
    assert(writer->Command("SHOW SLOTS ON t").items.empty());
}

void ConditionalObjects(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE IF NOT EXISTS t (n u8) CHUNK 4 x 4");
    const auto table = harness.catalog->Find("t");
    const auto revision = Set(*admin, 7);
    const auto describe = admin->Command("DESCRIBE t");
    assert(Number(Field(describe, "revision")) == revision);
    admin->Ok("CREATE TABLE IF NOT EXISTS t (different i8 DEFAULT 1000) CHUNK 8 x 8 WITH unknown = 1");
    assert(harness.catalog->Find("t") == table && admin->Command("DESCRIBE t") == describe);
    admin->Ok("ALTER TABLE t ADD COLUMN IF NOT EXISTS n i8 DEFAULT 1000");
    admin->Ok("ALTER TABLE t DROP COLUMN IF EXISTS absent");
    assert(admin->Command("DESCRIBE t") == describe);
    admin->Ok("ALTER TABLE t ADD COLUMN IF NOT EXISTS extra u16 DEFAULT 0");
    admin->Ok("ALTER TABLE t DROP COLUMN IF EXISTS extra");
    admin->Ok("CREATE SLOT IF NOT EXISTS 'consumer' ON t");
    auto watch = harness.Connect();
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer'"));
    assert(initial.second >= revision);
    const auto slot_before = Slot(admin->Command("SHOW SLOTS ON t"), "t", "consumer");
    admin->Ok("CREATE SLOT IF NOT EXISTS 'consumer' ON t");
    assert(Slot(admin->Command("SHOW SLOTS ON t"), "t", "consumer") == slot_before);
    const auto next = Set(*admin, 9);
    assert(Change(NextChange(*watch)) == next); // Conditional creation preserves the active claim.
    Unwatch(*watch);
    admin->Ok("DROP SLOT IF EXISTS 'consumer' ON t");
    admin->Ok("DROP SLOT IF EXISTS 'consumer' ON t");
    assert(admin->Command("SHOW SLOTS ON t").items.empty());
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER IF NOT EXISTS reader VERIFIER '" + verifier + "'");
    admin->Ok("CREATE USER IF NOT EXISTS reader VERIFIER 'invalid' MANAGES USERS");
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    for (const auto* command : {
             "CREATE TABLE IF NOT EXISTS t (a u8) CHUNK 4 x 4",
             "CREATE TABLE IF NOT EXISTS absent (a u8) CHUNK 4 x 4",
             "DROP TABLE IF EXISTS t", "DROP TABLE IF EXISTS absent",
             "ALTER TABLE t ADD COLUMN IF NOT EXISTS n u8", "ALTER TABLE absent DROP COLUMN IF EXISTS n",
             "CREATE SLOT IF NOT EXISTS 'consumer' ON t", "DROP SLOT IF EXISTS 'absent' ON t"}) {
        Error(reader.Command(command), command[0] == 'C' && std::string_view(command).starts_with("CREATE TABLE") ?
              "PERMISSION_DENIED" : "NO_TABLE");
    }
    admin->Ok("GRANT READ ON t TO reader");
    for (const auto* command : {"DROP TABLE IF EXISTS t", "ALTER TABLE t ADD COLUMN IF NOT EXISTS n u8",
                               "ALTER TABLE t DROP COLUMN IF EXISTS absent",
                               "CREATE SLOT IF NOT EXISTS 'absent' ON t", "DROP SLOT IF EXISTS 'absent' ON t"})
        Error(reader.Command(command), "PERMISSION_DENIED");
    Error(reader.Command("CREATE USER IF NOT EXISTS reader VERIFIER 'invalid'"), "PERMISSION_DENIED");
    Error(reader.Command("DROP USER IF EXISTS absent"), "PERMISSION_DENIED");
    const auto applied = admin->Command("MIGRATE 'create_again' CREATE TABLE IF NOT EXISTS t (n i8 DEFAULT 1000) CHUNK 8 x 8 WITH unknown = 1");
    if (applied.type != '+' || applied.value != "applied")
        throw std::runtime_error("expected applied conditional migration, got " + applied.value);
    const auto skipped = admin->Command("MIGRATE 'create_again' CREATE TABLE IF NOT EXISTS t (n i8 DEFAULT 1000) CHUNK 8 x 8 WITH unknown = 1");
    assert(skipped.type == '+' && skipped.value == "skipped");
    admin->Ok("DROP USER IF EXISTS reader");
    admin->Ok("DROP USER IF EXISTS reader");
    admin->Ok("DROP TABLE IF EXISTS t");
    admin->Ok("DROP TABLE IF EXISTS t");
}

void Rights(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin);
    admin->Ok("CREATE TABLE hidden (n u8) CHUNK 4 x 4");
    admin->Ok("CREATE SLOT 'private' ON hidden");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    assert(reader.Command("SHOW SLOTS").items.empty());
    Error(reader.Command("SHOW SLOTS ON hidden"), "NO_TABLE");
    Error(reader.Command("WATCH t SLOT 'consumer'"), "NO_TABLE");
    admin->Ok("GRANT READ ON t TO reader");
    auto visible = reader.Command("SHOW SLOTS");
    assert(visible.items.size() == 1U && Field(visible.items[0], "table").value == "t");
    Error(reader.Command("CREATE SLOT 'other' ON t"), "PERMISSION_DENIED");
    Error(reader.Command("DROP SLOT 'consumer' ON t"), "PERMISSION_DENIED");
    Error(reader.Command("WATCH t SLOT 'consumer'"), "PERMISSION_DENIED"); // The slot belongs to admin.
    admin->Ok("GRANT WRITE ON t TO reader");
    Error(reader.Command("CREATE SLOT 'other' ON t"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON t TO reader");
    reader.Ok("CREATE SLOT 'other' ON t"); reader.Ok("DROP SLOT 'other' ON t");
    Error(reader.Command("CREATE SLOT '../bad' ON t"), "SYNTAX");
}

// READ holds while a watch streams, not only when it opens. Changes to other
// users and to other rights leave the watches streaming; a REVOKE ends idle
// plain and slot watches with NO_TABLE at once, without a later write, push
// or ACK to carry it (the harness progress interval is 60 s).
void WatchRevoke(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin, " OWNER reader");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    for (const auto* user : {"reader", "other"}) admin->Ok(std::string("CREATE USER ") + user + " VERIFIER '" + verifier + "'");
    admin->Ok("GRANT READ ON t TO reader");
    Client plain(harness.port, tls); plain.Login("reader", "pw"); (void)Start(plain.Command("WATCH t"));
    Client slot(harness.port, tls); slot.Login("reader", "pw"); (void)Start(slot.Command("WATCH t SLOT 'consumer'"));
    admin->Ok("GRANT READ ON t TO other"); admin->Ok("REVOKE READ ON t FROM other");
    admin->Ok("GRANT WRITE ON t TO reader"); admin->Ok("REVOKE WRITE ON t FROM reader"); // READ stays.
    const auto first = Set(*admin, 7);
    assert(Change(NextChange(plain)) == first && Change(NextChange(slot)) == first);
    admin->Ok("REVOKE READ ON t FROM reader");
    Error(plain.Read(), "NO_TABLE"); Closed(plain);
    Error(slot.Read(), "NO_TABLE"); Closed(slot);
}

// Applies a REVOKE when the feed has read an ACK, before it accepts it.
class RevokeAtAck : public FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        if (point == Point::kBeforeSlotAck && users && !std::exchange(revoked, true)) users->Revoke("reader", "t", Right::kRead);
    }
    std::shared_ptr<UserRegistry> users;
    bool revoked = false; // Only the feed thread runs the hook.
};

// An ACK checked after the REVOKE is applied is refused and never persisted.
void WatchAckRevoke(bool tls) {
    RevokeAtAck revoke;
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin, " OWNER reader");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT READ ON t TO reader");
    Client slot(harness.port, tls); slot.Login("reader", "pw"); (void)Start(slot.Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*admin, 7);
    assert(Change(NextChange(slot)) == first);
    slot.Line("ACK " + std::to_string(first));
    WaitAck(*admin, "t", "consumer", first);
    const auto second = Set(*admin, 8);
    assert(Change(NextChange(slot)) == second);
    revoke.users = harness.engine().users();
    FeedDeliveryTestAccess::SetObserverHook(harness.server(), &revoke);
    Cleanup clear([&] { FeedDeliveryTestAccess::SetObserverHook(harness.server(), nullptr); });
    slot.Line("ACK " + std::to_string(second));
    Error(slot.Read(), "NO_TABLE"); Closed(slot);
    assert(revoke.revoked);
    // Takeover starts from every ACK the old owner accepted.
    auto next = harness.Connect();
    assert(Start(next->Command("WATCH t SLOT 'consumer' TAKEOVER")).second == first);
    Unwatch(*next);
}

// DROP USER ends the idle watches of its sessions with AUTH_REQUIRED at once,
// even when a user of the same name is created again with READ.
void WatchDroppedUser(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin, " OWNER reader");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT READ ON t TO reader");
    Client plain(harness.port, tls); plain.Login("reader", "pw"); (void)Start(plain.Command("WATCH t"));
    Client slot(harness.port, tls); slot.Login("reader", "pw"); (void)Start(slot.Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*admin, 7);
    assert(Change(NextChange(plain)) == first && Change(NextChange(slot)) == first);
    admin->Ok("DROP USER reader");
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT READ ON t TO reader");
    const auto dropped = [](const Reply& reply) {
        Error(reply, "AUTH_REQUIRED");
        assert(reply.value.find("reason=user_dropped") != std::string::npos);
    };
    dropped(plain.Read()); Closed(plain);
    dropped(slot.Read()); Closed(slot);
    // The user created again watches with its own login.
    Client again(harness.port, tls); again.Login("reader", "pw"); (void)Start(again.Command("WATCH t"));
    const auto third = Set(*admin, 9);
    assert(Change(NextChange(again)) == third);
    Unwatch(again);
}

// A slot belongs to its owner: another user with READ cannot attach to it,
// even idle, and its saved position stays where the owner acknowledged it.
// ADMIN on `*` attaches to any slot. Dropping the owner leaves the slot to
// ADMIN on `*` until a user of that name exists again.
void SlotOwners(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin, " OWNER reader");
    admin->Ok("CREATE SLOT 'implicit' ON t");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    for (const auto* user : {"reader", "other"}) {
        admin->Ok(std::string("CREATE USER ") + user + " VERIFIER '" + verifier + "'");
        admin->Ok(std::string("GRANT READ ON t TO ") + user);
    }
    const auto slots = admin->Command("SHOW SLOTS ON t");
    assert(Field(Slot(slots, "t", "consumer"), "owner").value == "reader");
    assert(Field(Slot(slots, "t", "implicit"), "owner").value == "admin");
    Client owner(harness.port, tls); owner.Login("reader", "pw");
    const auto start = Start(owner.Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*admin, 7);
    assert(Change(NextChange(owner)) == first);
    owner.Line("ACK " + std::to_string(first));
    WaitAck(*admin, "t", "consumer", first);
    Unwatch(owner);
    const auto second = Set(*admin, 8);
    Client other(harness.port, tls); other.Login("other", "pw");
    for (const auto& watch : {std::string("WATCH t SLOT 'consumer'"), std::string("WATCH t SLOT 'consumer' TAKEOVER"),
                              "WATCH t SLOT 'consumer' AFTER " + start.first + " 0", std::string("WATCH t SLOT 'implicit'")})
        Error(other.Command(watch), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON t TO other"); // Table ADMIN is not enough.
    Error(other.Command("WATCH t SLOT 'consumer'"), "PERMISSION_DENIED");
    assert(Number(Field(Slot(admin->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == first);
    // An owner is shown to the owner itself and to MANAGES USERS only.
    const auto theirs = other.Command("SHOW SLOTS ON t");
    assert(Field(Slot(theirs, "t", "consumer"), "owner").type == '_' && Field(Slot(theirs, "t", "implicit"), "owner").type == '_');
    const auto mine = owner.Command("SHOW SLOTS ON t");
    assert(Field(Slot(mine, "t", "consumer"), "owner").value == "reader" && Field(Slot(mine, "t", "implicit"), "owner").type == '_');
    assert(Start(owner.Command("WATCH t SLOT 'consumer'")).second == first); Unwatch(owner);
    auto any = harness.Connect(); // admin has ADMIN on *.
    assert(Start(any->Command("WATCH t SLOT 'consumer'")).second == first); Unwatch(*any);
    other.Close();
    // The owner takes over a watch of its own.
    (void)Start(owner.Command("WATCH t SLOT 'consumer'"));
    admin->Ok("GRANT ADMIN ON * TO reader"); admin->Ok("REVOKE ADMIN ON * FROM reader"); // The owner's watch stays.
    assert(Change(NextChange(owner)) == second); // Delivered before the handoff.
    Client again(harness.port, tls); again.Login("reader", "pw");
    (void)Start(again.Command("WATCH t SLOT 'consumer' TAKEOVER"));
    Error(owner.Read(), "KILLED"); Closed(owner); Unwatch(again); again.Close();
    admin->Ok("DROP USER reader");
    assert(Field(Slot(admin->Command("SHOW SLOTS ON t"), "t", "consumer"), "owner").value == "reader");
    Client later(harness.port, tls); later.Login("other", "pw");
    Error(later.Command("WATCH t SLOT 'consumer'"), "PERMISSION_DENIED");
    (void)Start(any->Command("WATCH t SLOT 'consumer'")); Unwatch(*any);
    admin->Ok("CREATE USER reader VERIFIER '" + verifier + "'");
    admin->Ok("GRANT READ ON t TO reader");
    Client created(harness.port, tls); created.Login("reader", "pw");
    assert(Start(created.Command("WATCH t SLOT 'consumer'")).second == first); Unwatch(created);
    // ADMIN on * attaches to another user's slot, and the watch needs that
    // right as long as it lasts; READ on t remains. This comes last: its
    // connection closes before the catch-up worker finishes the watch and
    // frees the slot, so an attach right after it can still be SLOT_BUSY.
    admin->Ok("GRANT ADMIN ON * TO other");
    assert(Start(later.Command("WATCH t SLOT 'consumer'")).second == first);
    assert(Change(NextChange(later)) == second);
    admin->Ok("REVOKE ADMIN ON * FROM other");
    Error(later.Read(), "NO_TABLE"); Closed(later);
}

// A slot without a recorded owner, as from before owners were recorded, takes
// the user of its first granted attach that ADMIN on * does not allow. A
// failed owner write starts no watch and records nothing.
void FirstAttachOwner(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin);
    (void)harness.catalog->Find("t")->CreateFeedSlot("legacy");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(test::PrepareLoginVerifier("pw", salt, scram::kMinIterations));
    for (const auto* user : {"reader", "other"}) {
        admin->Ok(std::string("CREATE USER ") + user + " VERIFIER '" + verifier + "'");
        admin->Ok(std::string("GRANT READ ON t TO ") + user);
    }
    const auto owner = [&] { return Field(Slot(admin->Command("SHOW SLOTS ON t"), "t", "legacy"), "owner"); };
    assert(owner().type == '_');
    (void)Start(admin->Command("WATCH t SLOT 'legacy'")); Unwatch(*admin);
    assert(owner().type == '_'); // ADMIN on * inspects the slot without taking it.
    const auto file = harness.directory.path() / "tables" / "t" / kFeedSlotsFileName;
    const auto before = LoadFile(file);
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    {
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE", "1");
        Error(reader.Command("WATCH t SLOT 'legacy'"), "INTERNAL");
    }
    assert(reader.Command("PING").value == "PONG"); // No watch started.
    assert(owner().type == '_' && LoadFile(file) == before);
    const auto first = Set(*admin, 7);
    (void)Start(reader.Command("WATCH t SLOT 'legacy'"));
    assert(owner().value == "reader");
    assert(Change(NextChange(reader)) == first); // The watch streams as any other.
    Unwatch(reader);
    Client other(harness.port, tls); other.Login("other", "pw");
    Error(other.Command("WATCH t SLOT 'legacy'"), "PERMISSION_DENIED");
}

void ArchiveHandover(bool tls) {
    // Fit the two-chunk transaction's raw state; the separate held-lease test
    // forces a witnessed reset with a 4096-byte single-chunk feed.
    Harness harness(tls, true, kDefaultSlotMaxBytes, 1h, 16384);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    writer->Ok("CREATE SLOT 'archive_area' ON t AREA 0 0 TO 0 0");
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    std::vector<Reply> expected;
    auto checkpoint = [&] {
        auto lease = table->Acquire();
        lease->store().CheckpointForTests(0, 0); lease->store().CheckpointForTests(1, 0);
    };
    for (unsigned n = 1; n <= 40; ++n) {
        (void)Set(*writer, n); expected.push_back(ordinary->Read());
        assert(expected.back().items[4].value == "admin");
        if (n % 8U == 0U) checkpoint();
    }
    writer->Ok("BEGIN");
    assert(writer->Command("SET BLOCK 0 0 IN t n = 41").type == '_');
    assert(writer->Command("SET BLOCK 4 0 IN t n = 42").type == '_');
    const auto transaction = Number(writer->Command("COMMIT"));
    expected.push_back(ordinary->Read()); assert(Change(expected.back(), 2U) == transaction);
    checkpoint();
    (void)Number(writer->Command("DELETE BLOCK 0 0 FROM t")); expected.push_back(ordinary->Read());
    checkpoint(); // Empty-chunk GC is archived but must never appear as a change.
    FeedSlotTestAccess::Sync(*table);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    // Writes race archive catch-up and may evict the in-memory feed ring.
    for (unsigned n = 43; n <= 83; ++n) {
        (void)Set(*writer, n); expected.push_back(ordinary->Read());
    }
    FeedSlotTestAccess::Sync(*table);
    for (const auto& change : expected) assert(NextChange(*watch) == change);
    // A later live change proves the completed hand-over and excludes a duplicate/GC at its boundary.
    const auto live = Set(*writer, 84); FeedSlotTestAccess::Sync(*table);
    const auto live_entry = ordinary->Read();
    assert(Change(live_entry) == live && NextChange(*watch) == live_entry);
    assert(!watch->Ready(100ms));
    Unwatch(*watch); Unwatch(*ordinary);
    (void)Start(watch->Command("WATCH t SLOT 'archive_area' AREA 0 0 TO 0 0"));
    for (auto change : expected) {
        auto& blocks = change.items[6].items;
        std::erase_if(blocks, [](const Reply& block) { return block.items[0].value == "4"; });
        change.items[6].value = std::to_string(blocks.size());
        assert(NextChange(*watch) == change);
    }
    assert(NextChange(*watch) == live_entry); assert(!watch->Ready(100ms)); Unwatch(*watch);
    writer->Ok("CREATE SLOT 'area' ON t AREA 0 0 TO 0 0");
    auto area = harness.Connect();
    const auto start = Start(area->Command("WATCH t SLOT 'area' AREA 0 0 TO 0 0"));
    const auto outside = Set(*writer, 90, 4); FeedSlotTestAccess::Sync(*table);
    // A revision skipped by AREA is not a revision the client has received.
    area->Line("ACK " + std::to_string(outside)); Error(area->Read(), "INVALID_ARGUMENT");
    assert(!area->Ready(100ms));
    const auto inside = Set(*writer, 91); FeedSlotTestAccess::Sync(*table);
    assert(Change(NextChange(*area)) == inside && inside > start.second);
    area->Line("ACK " + std::to_string(inside)); Unwatch(*area);
}

class SyncPause : public FeedSlotTestHook {
  public:
    void Run(Point point, std::uint64_t revision) override {
        if (point != Point::kBeforeSync || revision == 0U) return;
        std::unique_lock lock(mutex_);
        if (entered_) return;
        entered_ = true; cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    void Wait() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    std::mutex mutex_; std::condition_variable cv_; bool entered_ = false; bool released_ = false;
};
void DurableGate(bool tls) {
    SyncPause pause; // Outlives the server and every callback that loaded it.
    std::atomic<std::size_t> notifications{0U};
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    FeedOptions notify;
    notify.notify = [&] { notifications.fetch_add(1U, std::memory_order_relaxed); };
    auto witness = table->SubscribeFeed(notify);
    FeedSlotTestAccess::SetHook(*table, &pause);
    Cleanup hook_cleanup([&] { pause.Release(); FeedSlotTestAccess::SetHook(*table, nullptr); });
    const auto revision = Set(*writer, 7);
    assert(Change(ordinary->Read()) == revision);
    std::exception_ptr sync_error;
    std::thread sync([&] {
        try { FeedSlotTestAccess::Sync(*table); }
        catch (const std::exception&) { sync_error = std::current_exception(); }
    });
    Cleanup thread_cleanup([&] { pause.Release(); if (sync.joinable()) sync.join(); });
    pause.Wait();
    assert(!watch->Ready(150ms)); // WAL bytes flushed, but fsync/written frontier still withheld.
    const auto ping = writer->Command("PING"); assert(ping.value == "PONG");
    const auto before_sync = notifications.load(std::memory_order_relaxed);
    pause.Release(); sync.join(); if (sync_error) std::rethrow_exception(sync_error);
    assert(notifications.load(std::memory_order_relaxed) > before_sync);
    assert(Change(NextChange(*watch)) == revision); // No new change is needed to wake the watch.
    FeedSlotTestAccess::SetHook(*table, nullptr);
    Unwatch(*watch); Unwatch(*ordinary);
}

class AdmissionSchedule : public FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t bytes) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeIoScan && !entered_) {
            entered_ = true; cv_.notify_all();
            cv_.wait(lock, [&] { return released_; });
        } else if (point == Point::kAfterAdmission) {
            admitted_ = bytes; cv_.notify_all();
        } else if (point == Point::kAfterIoAdd) {
            added_ = true; cv_.notify_all();
        }
    }
    void WaitForScan() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return entered_; })); }
    std::size_t WaitForAdmission() {
        std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return admitted_ != 0U; }));
        return admitted_;
    }
    void WaitForJoin() { std::unique_lock lock(mutex_); assert(cv_.wait_for(lock, 10s, [&] { return added_; })); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    std::mutex mutex_; std::condition_variable cv_;
    bool entered_ = false, released_ = false, added_ = false;
    std::size_t admitted_ = 0U;
};
void AdmittedChangeSurvivesShareShrink(bool tls) {
    AdmissionSchedule schedule; // Outlives both delivery threads.
    constexpr std::size_t budget = 1536U;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    // First delivery establishes activation, schema and the full initial quota.
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    assert(Change(NextChange(*watch)) == first);
    FeedDeliveryTestAccess::SetHook(harness.server(), &schedule);
    Cleanup cleanup([&] { schedule.Release(); FeedDeliveryTestAccess::SetHook(harness.server(), nullptr); });
    schedule.WaitForScan(); // Hold I/O before its next incoming-watch/quota scan.
    writer->Ok("BEGIN");
    for (unsigned x = 0U; x < 6U; ++x)
        assert(writer->Command("SET BLOCK " + std::to_string(x % 4U) + " " + std::to_string(x / 4U) +
            " IN t label = '" + std::string(110U, 'x') + "'").type == '_');
    const auto revision = Number(writer->Command("COMMIT"));
    FeedSlotTestAccess::Sync(*table);
    const auto admitted = schedule.WaitForAdmission();
    assert(admitted > budget / 2U && admitted <= budget - 128U);
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    schedule.WaitForJoin(); // Plain WATCH is really handed to the I/O thread.
    schedule.Release(); // The scan now halves the slot share before taking its accepted output.
    const auto change = watch->Read(); assert(Change(change, 6U) == revision);
    watch->Line("ACK " + std::to_string(revision)); Unwatch(*watch);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == revision);
    Unwatch(*ordinary);
}

void LostAndDrop(bool tls) {
    Harness harness(tls, false, 1U, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    (void)Set(*writer, 7);
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    FeedSlotTestAccess::Sync(*table); FeedSlotTestAccess::Retain(*table);
    const auto listed = writer->Command("SHOW SLOTS ON t");
    assert(Boolean(Field(Slot(listed, "t", "consumer"), "lost")));
    auto watch = harness.Connect(); Error(watch->Command("WATCH t SLOT 'consumer'"), "SLOT_LOST");
    Error(writer->Command("CREATE SLOT 'consumer' ON t"), "INVALID_ARGUMENT"); // Lost names require DROP before reuse.
    writer->Ok("DROP SLOT 'consumer' ON t");
    writer->Ok("CREATE SLOT 'fresh' ON t");
    (void)Start(watch->Command("WATCH t SLOT 'fresh'"));
    writer->Ok("DROP TABLE t"); Error(watch->Read(), "NO_TABLE"); Closed(*watch);
    const auto all = writer->Command("SHOW SLOTS");
    for (const auto& slot : all.items) assert(Field(slot, "table").value != "t");
}

class TerminalAckSchedule : public FeedDeliveryTestHook {
  public:
    void Run(Point point, std::size_t) override {
        std::unique_lock lock(mutex_);
        if (point == Point::kBeforeSlotAck && !entered_) {
            entered_ = true; cv_.notify_all();
            cv_.wait(lock, [&] { return released_; });
        } else if (point == Point::kAfterSlotFinish) {
            finished_ = true; cv_.notify_all();
        }
    }
    void WaitForAck() { Wait(entered_, "incoming ACK before terminal publication"); }
    void WaitForFinish() { Wait(finished_, "queued slot terminal error"); }
    void Release() { std::lock_guard lock(mutex_); released_ = true; cv_.notify_all(); }
  private:
    void Wait(const bool& signal, const char* phase) {
        std::unique_lock lock(mutex_);
        if (!cv_.wait_for(lock, 10s, [&] { return signal; }))
            throw std::runtime_error(std::string("terminal ACK schedule timed out waiting for ") + phase);
    }
    std::mutex mutex_; std::condition_variable cv_;
    bool entered_ = false, released_ = false, finished_ = false;
};
void TerminalErrorBeforeAck(bool tls) {
    for (const bool drop_table : {false, true}) {
        TerminalAckSchedule schedule; // Outlives both delivery threads.
        Harness harness(tls);
        auto writer = harness.Connect(); Create(*writer);
        auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
        const auto revision = Set(*writer, 7);
        assert(Change(NextChange(*watch)) == revision);
        FeedDeliveryTestAccess::SetHook(harness.server(), &schedule);
        Cleanup cleanup([&] { schedule.Release(); FeedDeliveryTestAccess::SetHook(harness.server(), nullptr); });
        watch->Line("ACK " + std::to_string(revision));
        schedule.WaitForAck(); // ACK is parsed; I/O cannot pull the terminal frame yet.
        writer->Ok(drop_table ? "DROP TABLE t" : "DROP SLOT 'consumer' ON t");
        schedule.WaitForFinish(); // The worker has queued the actual terminal cause.
        schedule.Release();
        const std::string code = drop_table ? "NO_TABLE" : "SLOT_LOST";
        const auto terminal = watch->Read();
        if (terminal.type != '-' || !terminal.value.starts_with("ERR " + code + " "))
            throw std::runtime_error("terminal ACK expected " + code + ", got " + terminal.value);
        Closed(*watch); // Exactly one terminal error, with no INVALID_ARGUMENT before it.
        if (!drop_table) assert(writer->Command("SHOW SLOTS ON t").items.empty());
    }
}

void ReplacementClaim(bool tls) {
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    auto old = harness.Connect(); (void)Start(old->Command("WATCH t SLOT 'consumer'"));
    const auto old_revision = Set(*writer, 1); assert(Change(NextChange(*old)) == old_revision);
    writer->Ok("DROP SLOT 'consumer' ON t");
    Error(old->Read(), "SLOT_LOST"); Closed(*old);
    writer->Ok("CREATE SLOT 'consumer' ON t");
    const auto before = Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked"));
    assert(before >= old_revision);
    auto replacement = harness.Connect(); (void)Start(replacement->Command("WATCH t SLOT 'consumer'"));
    const auto next = Set(*writer, 2); assert(Change(NextChange(*replacement)) == next);
    // The old watch has a terminal SLOT_LOST and cannot claim the new name.
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == before);
    replacement->Line("ACK " + std::to_string(next)); Unwatch(*replacement);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == next);
}
Reply NextChunks(Client& client) {
    for (;;) {
        const auto reply = client.Read();
        if (reply.type == '>' && reply.items[0].value == "chunks") {
            assert(reply.items.size() == 9U && reply.items[7].type == '_' && reply.items[8].type == '_');
            return reply;
        }
        assert(reply.type == '>' && (reply.items[0].value == "schema" || reply.items[0].value == "progress"));
    }
}
void ChunkForms(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8, label text(128)) CHUNK 4 x 4");
    writer->Ok("CREATE SLOT 'forms' ON t CHUNKS COLUMNS label,n AREA 0 0 TO 1 0");
    const auto old = Number(writer->Command("SET BLOCK 1 0 IN t n=11,label='untouched'"));
    const auto old_form = writer->Command("GET CHUNK 0 0 FROM t COLUMNS label,n").value;
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    auto slot = harness.Connect(); (void)Start(slot->Command("WATCH t SLOT 'forms'"));
    const auto replay_old = NextChunks(*slot);
    assert(Number(replay_old.items[2]) == old && replay_old.items[6].items[0].items[3].value == old_form);
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t CHUNKS COLUMNS label,n AREA 0 0 TO 1 0"));
    const auto first = Number(writer->Command("SET BLOCK 0 0 IN t n=7,label='one'"));
    const auto first_form = writer->Command("GET CHUNK 0 0 FROM t COLUMNS label,n").value;
    const auto second = Number(writer->Command("SET BLOCK 0 0 IN t n=8,label='two'"));
    const auto second_form = writer->Command("GET CHUNK 0 0 FROM t COLUMNS label,n").value;
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    for (auto* watch : {ordinary.get(), slot.get()}) {
        const auto a = NextChunks(*watch), b = NextChunks(*watch);
        assert(Number(a.items[2]) == first && Number(b.items[2]) == second);
        assert(a.items[6].items.size() == 1U && a.items[6].items[0].items.size() == 4U);
        assert(Number(a.items[6].items[0].items[2]) == first && a.items[6].items[0].items[3].value == first_form);
        assert(b.items[6].items[0].items[3].value == second_form); // Reading after second commit must not rewrite first form.
    }
    writer->Ok("BEGIN");
    assert(writer->Command("SET BLOCK 0 0 IN t n=9,label='left'").type == '_');
    assert(writer->Command("SET BLOCK 4 0 IN t n=10,label='right'").type == '_');
    const auto group = Number(writer->Command("COMMIT"));
    const auto left = writer->Command("GET CHUNK 0 0 FROM t COLUMNS label,n").value;
    const auto right = writer->Command("GET CHUNK 1 0 FROM t COLUMNS label,n").value;
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    for (auto* watch : {ordinary.get(), slot.get()}) {
        const auto entry = NextChunks(*watch); assert(Number(entry.items[2]) == group && entry.items[6].items.size() == 2U);
        for (const auto& chunk : entry.items[6].items) {
            assert(Number(chunk.items[2]) == group);
            assert(chunk.items[3].value == (chunk.items[0].value == "0" ? left : right));
        }
    }
    slot->Line("ACK " + std::to_string(group)); Unwatch(*slot);
    (void)Number(writer->Command("DELETE BLOCK 0 0 FROM t"));
    const auto last = Number(writer->Command("DELETE BLOCK 1 0 FROM t"));
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    const auto partial = NextChunks(*ordinary), removed = NextChunks(*ordinary);
    assert(partial.items[6].items[0].items[3].type == '$');
    assert(Number(removed.items[2]) == last && removed.items[6].items[0].items[3].type == '_');
    Unwatch(*ordinary);
    assert(Start(slot->Command("WATCH t SLOT 'forms'")).second == group);
    assert(NextChunks(*slot) == partial && NextChunks(*slot) == removed); Unwatch(*slot);
}

void PersistentFilterDelivery(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8, label text(128)) CHUNK 4 x 4");
    writer->Ok("CREATE SLOT 'filtered' ON t COLUMNS (label,n) NO BEFORE AREA 0 0 TO 0 0");
    writer->Ok("CREATE SLOT IF NOT EXISTS 'filtered' ON t AREA 0 0 TO 0 0 COLUMNS label,n NO BEFORE");
    const auto mismatch = writer->Command("CREATE SLOT IF NOT EXISTS 'filtered' ON t COLUMNS n,label NO BEFORE AREA 0 0 TO 0 0");
    Error(mismatch, "INVALID_ARGUMENT"); assert(mismatch.value.find("reason=slot_filters") != std::string::npos);
    Error(writer->Command("CREATE SLOT 'bad' ON t COLUMNS n,n"), "INVALID_ARGUMENT");
    const auto before = writer->Command("SHOW SLOTS ON t"); assert(before.items.size() == 1U);
    const auto& filter = Field(Slot(before, "t", "filtered"), "filters");
    assert(Field(filter, "area").items.size() == 4U && Boolean(Field(filter, "no_before")));
    assert(Field(filter, "columns").items[0].value == "label" && Number(Field(filter, "column_ids").items[0]) == 2U);
    auto watch = harness.Connect();
    const auto wrong = watch->Command("WATCH t SLOT 'filtered' COLUMNS n,label");
    Error(wrong, "INVALID_ARGUMENT"); assert(wrong.value.find("reason=slot_filters") != std::string::npos);
    (void)Start(watch->Command("WATCH t SLOT 'filtered'"));
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t COLUMNS label,n NO BEFORE AREA 0 0 TO 0 0"));
    (void)Set(*writer, 9, 8); // Outside the fixed area; delivery must skip it.
    const auto first = Number(writer->Command("SET BLOCK 0 0 IN t n=7,label='one'"));
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    const auto ordinary_first = NextChange(*ordinary); assert(Change(ordinary_first) == first);
    assert(ordinary_first.items[6].items[0].items[2].type == '_');
    assert(ordinary_first.items[6].items[0].items[3].items[0].value == "one");
    const auto schema = watch->Read();
    assert(schema.items[0].value == "schema");
    const auto& columns = Field(schema.items[1], "columns").items;
    assert(columns.size() == 2U && Field(columns[0], "name").value == "label" && Field(columns[1], "name").value == "n");
    const auto entry = NextChange(*watch); assert(Change(entry) == first);
    const auto& block = entry.items[6].items[0];
    assert(block.items[2].type == '_' && block.items[3].items[0].value == "one" && Number(block.items[3].items[1]) == 7U);
    Unwatch(*watch); // No ACK: reopen must replay the same projected historical schema.
    writer->Ok("ALTER TABLE t RENAME COLUMN label TO renamed");
    const auto second = Number(writer->Command("SET BLOCK 0 0 IN t n=8,renamed='two'"));
    FeedSlotTestAccess::Sync(*harness.catalog->Find("t"));
    const auto ordinary_schema = ordinary->Read();
    assert(Field(Field(ordinary_schema.items[1], "columns").items[0], "name").value == "renamed");
    assert(Change(NextChange(*ordinary)) == second); Unwatch(*ordinary);
    const auto renamed = writer->Command("SHOW SLOTS ON t");
    assert(Field(Field(Slot(renamed, "t", "filtered"), "filters"), "columns").items[0].value == "renamed");
    (void)Start(watch->Command("WATCH t SLOT 'filtered' COLUMNS renamed,n NO BEFORE AREA 0 0 TO 0 0"));
    const auto replay_schema = watch->Read();
    assert(Field(Field(replay_schema.items[1], "columns").items[0], "name").value == "label");
    assert(NextChange(*watch) == entry);
    const auto new_schema = watch->Read();
    assert(new_schema.items[0].value == "schema" && Field(Field(new_schema.items[1], "columns").items[0], "name").value == "renamed");
    assert(Change(NextChange(*watch)) == second);
    watch->Line("ACK " + std::to_string(second)); Unwatch(*watch);
    writer->Ok("ALTER TABLE t DROP COLUMN renamed");
    const auto lost = writer->Command("SHOW SLOTS ON t");
    const auto& slot = Slot(lost, "t", "filtered");
    assert(Boolean(Field(slot, "lost")) && Field(slot, "lost_reason").value == "schema_filter");
    assert(Field(Field(slot, "filters"), "columns").items[0].type == '_');
    const auto error = watch->Command("WATCH t SLOT 'filtered'"); Error(error, "SLOT_LOST");
    assert(error.value.find("reason=schema_filter") != std::string::npos);
    const auto applied = writer->Command("MIGRATE 'filtered_create' CREATE SLOT 'migration_slot' ON t COLUMNS n NO BEFORE AREA -1 -1 TO 1 1");
    assert(applied.type == '+' && applied.value == "applied");
    assert(Field(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "migration_slot"), "filters"), "columns").items[0].value == "n");
    Error(writer->Command("MIGRATE 'filtered_conflict' CREATE SLOT IF NOT EXISTS 'migration_slot' ON t COLUMNS n"), "INVALID_ARGUMENT");
}

void HistoricalSchemas(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    const auto first = Set(*writer, 7); const auto first_entry = ordinary->Read();
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    const auto alteration = ordinary->Read();
    assert(alteration.items[0].value == "schema" && Number(Field(alteration.items[1], "schema_version")) == 2U);
    const auto second = Set(*writer, 8); const auto second_entry = ordinary->Read();
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    FeedSlotTestAccess::Sync(*table);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto old_schema = watch->Read();
    assert(old_schema.items[0].value == "schema" && Number(Field(old_schema.items[1], "schema_version")) == 1U);
    assert(Field(old_schema.items[1], "columns").items.size() == 2U);
    assert(Field(Field(old_schema.items[1], "columns").items[0], "name").value == "n");
    assert(watch->Read() == first_entry && Change(first_entry) == first);
    const auto new_schema = watch->Read();
    assert(new_schema.items[0].value == "schema" && Number(Field(new_schema.items[1], "schema_version")) == 2U);
    assert(Field(new_schema.items[1], "columns").items == Field(alteration.items[1], "columns").items);
    const auto replay = watch->Read(); assert(replay == second_entry && Change(replay) == second);
    assert(replay.items[6].items[0].items[2].items.size() == 3U);
    assert(replay.items[6].items[0].items[2].items[2].value == "9");
    Unwatch(*watch); Unwatch(*ordinary);
}

void ArchiveWhileLeaseHeld(bool tls) {
    // The sender barrier guarantees a real ring reset while the slot worker
    // still has a live subscription. A witness proves the reset happened.
    feed_test::Pause pause(FeedTestHook::Point::kBeforeMerge);
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, 4096);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect(); const auto start = Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    const auto first_entry = NextChange(*watch);
    assert(Change(first_entry) == first); // Subscribe/setup must be complete before the held lease.
    const auto label = first_entry.items[6].items[0].items[3].items[1];
    assert(label.type == '$' && label.value.empty());
    auto witness = table->SubscribeFeed();
    auto held_lease = table->Acquire(); assert(held_lease);
    FeedTestAccess::SetHook(*table, &pause);
    Cleanup cleanup([&] { pause.Release(); FeedTestAccess::SetHook(*table, nullptr); });
    (void)pause.Wait();
    std::vector<std::uint64_t> revisions;
    for (unsigned value = 2; value <= 101; ++value) revisions.push_back(Set(*writer, value));
    FeedSlotTestAccess::Sync(*table);
    pause.Release();
    const auto reset = witness->Next(10s);
    assert(reset && reset->kind == FeedEntry::Kind::kResync && reset->position.revision == revisions.back());
    for (std::size_t i = 0; i < revisions.size(); ++i) {
        const auto change = NextChange(*watch);
        assert(Change(change) == revisions[i] && change.items[1].value == start.first);
        const auto& block = change.items[6].items[0];
        assert(block.items[0].value == "0" && block.items[1].value == "0");
        assert(block.items[2].items.size() == 2U && block.items[3].items.size() == 2U);
        assert(block.items[2].items[0].value == std::to_string(i + 1U));
        assert(block.items[3].items[0].value == std::to_string(i + 2U));
        assert(block.items[2].items[1] == label && block.items[3].items[1] == label);
    }
    assert(!watch->Ready(100ms));
    held_lease.reset(); // Reader progress above must not have needed an exclusive table lease.
    FeedTestAccess::SetHook(*table, nullptr); witness.reset(); Unwatch(*watch);
}

void AfterWaitsForDurable(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    const auto baseline = writer->Command("SHOW SLOTS ON t");
    const auto& slot = Slot(baseline, "t", "consumer");
    const auto written = Number(Field(slot, "acked"));
    const auto epoch = Field(slot, "epoch").value;
    const auto processed = Set(*writer, 1); assert(processed > written);
    auto watch = harness.Connect();
    assert(Start(watch->Command("WATCH t SLOT 'consumer' AFTER " + epoch + " " + std::to_string(processed))) ==
        std::make_pair(epoch, processed)); // Completed consumer position may exceed the durable frontier.
    assert(!watch->Ready(100ms));
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == written);
    const auto next = Set(*writer, 2);
    assert(!watch->Ready(100ms));
    FeedSlotTestAccess::Sync(*table);
    assert(Change(NextChange(*watch)) == next); // Only changes strictly after AFTER.
    assert(!watch->Ready(100ms)); Unwatch(*watch);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == written);
    const auto pending = Set(*writer, 3);
    assert(Start(watch->Command("WATCH t SLOT 'consumer' AFTER " + epoch + " " + std::to_string(pending))) ==
        std::make_pair(epoch, pending));
    watch->Line("ACK " + std::to_string(pending)); Unwatch(*watch);
    // An explicit ACK of the accepted start must sync its pending WAL prefix
    // rather than waiting for the configured one-hour storage sync interval.
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == pending);
}

void TinyBudgetTerminates(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, 1U);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    (void)Set(*writer, 7); FeedSlotTestAccess::Sync(*table);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    Error(watch->Read(), "OUT_OF_RANGE"); Closed(*watch);
    const auto pong = writer->Command("PING"); assert(pong.type == '+' && pong.value == "PONG");
    const auto listed = writer->Command("SHOW SLOTS ON t");
    assert(!Boolean(Field(Slot(listed, "t", "consumer"), "lost")));
}
// One transaction writing `blocks` fresh blocks with long labels from block row `row`.
std::uint64_t LabelTransaction(Client& writer, unsigned blocks, unsigned row = 0U) {
    writer.Ok("BEGIN");
    for (unsigned i = 0U; i < blocks; ++i)
        assert(writer.Command("SET BLOCK " + std::to_string(i % 64U) + " " + std::to_string(row + i / 64U) +
            " IN t label = '" + std::string(120U, 'x') + "'").type == '_');
    return Number(writer.Command("COMMIT"));
}
std::vector<std::unique_ptr<Client>> OrdinaryWatches(Harness& harness, std::size_t count) {
    std::vector<std::unique_ptr<Client>> watches;
    for (std::size_t i = 0U; i < count; ++i) {
        watches.push_back(harness.Connect()); (void)Start(watches.back()->Command("WATCH t"));
    }
    return watches;
}
void ShareIndependentDelivery(bool tls) {
    constexpr std::size_t budget = 16384U, kOrdinary = 31U; // A share of 512 bytes.
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto ordinary = OrdinaryWatches(harness, kOrdinary);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    // Every watch delivers a change, so the I/O thread has applied the
    // 32-watch share to the slot before the large change is admitted.
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    for (const auto& client : ordinary) assert(Change(NextChange(*client)) == first);
    assert(Change(NextChange(*watch)) == first);
    const auto large = LabelTransaction(*writer, 24U); FeedSlotTestAccess::Sync(*table);
    const auto change = watch->Read();
    assert(Change(change, 24U) == large); // About 4 KiB: above the share, within the table budget.
    watch->Line("ACK " + std::to_string(large)); Unwatch(*watch);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == large);
    for (const auto& client : ordinary) Unwatch(*client); // Ordinary watches above their share resync.
}
void TableBudgetStillTerminates(bool tls) {
    constexpr std::size_t budget = 16384U;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto ordinary = OrdinaryWatches(harness, 7U);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    for (const auto& client : ordinary) assert(Change(NextChange(*client)) == first);
    assert(Change(NextChange(*watch)) == first);
    (void)LabelTransaction(*writer, 128U); FeedSlotTestAccess::Sync(*table); // About 20 KiB.
    const auto refused = watch->Read();
    Error(refused, "OUT_OF_RANGE"); assert(refused.value.find("table feed buffer") != std::string::npos);
    Closed(*watch);
    assert(!Boolean(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "lost")));
    for (const auto& client : ordinary) Unwatch(*client);
}
void ExcessPoolBound(bool tls) {
    constexpr std::size_t budget = 4096U, share = 1024U;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"first", "second", "third"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto small = Set(*writer, 1);
    const auto large = LabelTransaction(*writer, 20U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto small_entry = archive.Next(), large_entry = archive.Next();
    assert(small_entry && large_entry && small_entry->position.revision == small && large_entry->position.revision == large);
    const auto take = [](SlotWatch& state) {
        std::size_t bytes = 0U;
        while (auto output = state.Take(budget)) bytes += output->bytes->size();
        return bytes;
    };
    const auto bounded = [&](const SlotWatch& state) {
        assert(SlotAckDeadlineTestAccess::Charged(state) <= budget && SlotAckDeadlineTestAccess::Excess(state) <= budget);
    };
    auto first = SlotWatch::Create(table, "first", {});
    auto second = SlotWatch::Create(table, "second", {});
    auto third = SlotWatch::Create(table, "third", {});
    for (const auto& state : {first, second, third}) state->SetQuota(share);
    // A frame above the share uses the excess pool; a second one waits for it.
    assert(SlotAckDeadlineTestAccess::Publish(*first, large_entry));
    const auto excess = SlotAckDeadlineTestAccess::Excess(*first);
    assert(excess > budget / 2U && excess <= budget && SlotAckDeadlineTestAccess::Charged(*first) == 0U);
    assert(!SlotAckDeadlineTestAccess::Publish(*second, large_entry));
    assert(SlotAckDeadlineTestAccess::Excess(*second) == excess); bounded(*second);
    // Output within a share does not wait for the excess pool.
    assert(SlotAckDeadlineTestAccess::Publish(*third, small_entry));
    const auto within = take(*third);
    assert(within != 0U && within <= share && SlotAckDeadlineTestAccess::Charged(*third) == within); bounded(*third);
    // The same watch queues nothing above its share while output is unsent.
    assert(take(*first) == excess);
    first->Consumed(excess - 1U);
    assert(SlotAckDeadlineTestAccess::Excess(*first) == 1U && SlotAckDeadlineTestAccess::Charged(*first) == within);
    first->Consumed(1U);
    assert(SlotAckDeadlineTestAccess::Excess(*first) == 0U);
    assert(SlotAckDeadlineTestAccess::Publish(*second, large_entry));
    assert(SlotAckDeadlineTestAccess::Excess(*second) == excess); bounded(*second);
    third->Consumed(within); // Its earlier output was sent, but the pool holds another frame.
    assert(!SlotAckDeadlineTestAccess::Publish(*third, large_entry));
    assert(take(*second) == excess); second->Consumed(excess);
    assert(SlotAckDeadlineTestAccess::Publish(*third, large_entry));
    assert(SlotAckDeadlineTestAccess::Excess(*third) > share); bounded(*third);
    for (const auto& state : {first, second, third}) { state->Cancel(); state->WorkStep(); }
    assert(SlotAckDeadlineTestAccess::Charged(*first) == 0U && SlotAckDeadlineTestAccess::Excess(*first) == 0U);
}
void ExcessPoolReceipts(bool tls) {
    constexpr std::size_t budget = 4096U, share = 1024U, receipt = SlotAckDeadlineTestAccess::kReceipt;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"acker", "holder", "waiter", "queue"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto small = Set(*writer, 1), second = Set(*writer, 2), third = Set(*writer, 3);
    const auto large = LabelTransaction(*writer, 20U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto small_entry = archive.Next(), second_entry = archive.Next(), third_entry = archive.Next(), large_entry = archive.Next();
    assert(small_entry && small_entry->position.revision == small && second_entry && second_entry->position.revision == second);
    assert(third_entry && third_entry->position.revision == third && large_entry && large_entry->position.revision == large);
    std::atomic<std::size_t> notified{0U};
    FeedOptions options; options.ack_timeout_ms = 600000U; options.notify = [&] { notified.fetch_add(1U); };
    const auto drain = [](SlotWatch& state) {
        std::size_t bytes = 0U;
        while (auto output = state.Take(std::numeric_limits<std::size_t>::max())) {
            bytes += output->bytes->size(); state.Consumed(output->bytes->size());
            if (output->revision) state.Sent(*output->revision);
        }
        return bytes;
    };
    {
        // Receipts filling the share make a frame wait for ACK; they never move it to the pool.
        auto acker = SlotWatch::Create(table, "acker", options);
        acker->SetQuota(share);
        assert(SlotAckDeadlineTestAccess::Publish(*acker, small_entry)); (void)drain(*acker);
        assert(SlotAckDeadlineTestAccess::Publish(*acker, second_entry));
        const auto frame = drain(*acker);
        assert(SlotAckDeadlineTestAccess::Count(*acker) == 2U);
        acker->SetQuota(2U * receipt + frame + receipt / 2U); // The next frame fits alone, not beside two receipts.
        assert(!SlotAckDeadlineTestAccess::Publish(*acker, third_entry));
        assert(SlotAckDeadlineTestAccess::Excess(*acker) == 0U);
        assert(acker->Ack(second) == SlotWatch::AckResult::kAccepted);
        assert(SlotAckDeadlineTestAccess::Publish(*acker, third_entry) && SlotAckDeadlineTestAccess::Excess(*acker) == 0U);
        (void)drain(*acker);
        acker->Cancel(); acker->WorkStep();
    }
    auto holder = SlotWatch::Create(table, "holder", options);
    auto waiter = SlotWatch::Create(table, "waiter", options);
    holder->SetQuota(share); waiter->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*holder, large_entry));
    const auto size = SlotAckDeadlineTestAccess::Excess(*holder);
    assert(size > budget / 2U && size <= budget - receipt);
    // A refused pool frame neither keeps a receipt charge nor wakes the worker
    // into an immediate retry, and it is built only once.
    auto unframed = std::make_shared<FeedEntry>(*large_entry); unframed->protocol_frame.reset(); // Encoded per build.
    const auto charged = SlotAckDeadlineTestAccess::Charged(*waiter);
    const auto wakes = notified.load();
    assert(!SlotAckDeadlineTestAccess::Publish(*waiter, unframed));
    const auto prepared = SlotAckDeadlineTestAccess::Prepared(*waiter); // Held: its address is not reused.
    assert(prepared != nullptr);
    for (int retry = 0; retry < 3; ++retry) assert(!SlotAckDeadlineTestAccess::Publish(*waiter, unframed));
    assert(notified.load() == wakes && SlotAckDeadlineTestAccess::Charged(*waiter) == charged);
    assert(SlotAckDeadlineTestAccess::Excess(*waiter) == size && SlotAckDeadlineTestAccess::Prepared(*waiter) == prepared);
    (void)drain(*holder);
    assert(SlotAckDeadlineTestAccess::Excess(*holder) == 0U);
    assert(SlotAckDeadlineTestAccess::Publish(*waiter, unframed) && SlotAckDeadlineTestAccess::Prepared(*waiter) == nullptr);
    const auto schema = waiter->Take(std::numeric_limits<std::size_t>::max());
    const auto data = waiter->Take(std::numeric_limits<std::size_t>::max());
    assert(schema && data && schema->excess && data->excess && data->bytes == prepared);
    // Cancelling keeps the taken, unconsumed part charged until its socket queue is dropped.
    waiter->Consumed(schema->bytes->size() + 1U);
    waiter->Cancel(); waiter->WorkStep();
    assert(SlotAckDeadlineTestAccess::Excess(*waiter) == data->bytes->size() - 1U);
    waiter->Consumed(data->bytes->size() - 1U);
    assert(SlotAckDeadlineTestAccess::Excess(*waiter) == 0U);
    holder->Cancel(); holder->WorkStep();
    assert(SlotAckDeadlineTestAccess::Charged(*holder) == 0U);
    // The I/O side takes a pool batch beside uncharged replies and releases a dropped queue.
    auto queue = SlotWatch::Create(table, "queue", {});
    queue->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*queue, large_entry));
    AckDeadlineIoTestAccess::ExcessQueue(harness.server(), queue, SlotAckDeadlineTestAccess::Excess(*queue), budget);
    queue->Cancel(); queue->WorkStep();
}
void ExcessStalledHolder(bool tls) {
    // Driven through the I/O watch state, so no host's socket buffering decides when the holder stalls.
    constexpr std::size_t budget = 4096U, share = 1024U;
    constexpr auto timeout = 3000ms;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget, {}, 60000ms, timeout);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"holder", "waiter"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto large = LabelTransaction(*writer, 20U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto entry = archive.Next();
    assert(entry && entry->position.revision == large);
    auto holder = SlotWatch::Create(table, "holder", {});
    auto waiter = SlotWatch::Create(table, "waiter", {});
    holder->SetQuota(share); waiter->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*holder, entry) && SlotAckDeadlineTestAccess::Excess(*holder) > budget / 2U);
    assert(!SlotAckDeadlineTestAccess::Publish(*waiter, entry));
    AckDeadlineIoTestAccess::StalledWrite(harness.server(), holder, waiter, entry, timeout);
    for (const auto& state : {holder, waiter}) {
        while (auto output = state->Take(std::numeric_limits<std::size_t>::max())) state->Consumed(output->bytes->size());
        state->Cancel(); state->WorkStep();
    }
    assert(SlotAckDeadlineTestAccess::Charged(*holder) == 0U && SlotAckDeadlineTestAccess::Excess(*holder) == 0U);
}
void TlsWriteCap(bool tls) {
#ifdef CHUNKDB_WITH_OPENSSL
    if (!tls) return; // Independent of the transport: runs once, with the TLS groups.
    Harness harness(tls);
    AckDeadlineIoTestAccess::TlsWriteCap(harness.server());
#else
    (void)tls;
#endif
}
void ExcessPoolFifo(bool tls) {
    constexpr std::size_t budget = 4096U, share = 1024U;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"first", "second", "third"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto one = LabelTransaction(*writer, 20U), two = LabelTransaction(*writer, 20U, 1U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto one_entry = archive.Next(), two_entry = archive.Next();
    assert(one_entry && one_entry->position.revision == one && two_entry && two_entry->position.revision == two);
    const auto drain = [](SlotWatch& state) {
        std::size_t bytes = 0U;
        while (auto output = state.Take(std::numeric_limits<std::size_t>::max())) {
            bytes += output->bytes->size(); state.Consumed(output->bytes->size());
        }
        return bytes;
    };
    auto first = SlotWatch::Create(table, "first", {});
    auto second = SlotWatch::Create(table, "second", {});
    auto third = SlotWatch::Create(table, "third", {});
    for (const auto& state : {first, second, third}) state->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*first, one_entry));
    assert(SlotAckDeadlineTestAccess::Excess(*first) > budget / 2U);
    // Waiters queue in arrival order; only the head keeps its built frame.
    assert(!SlotAckDeadlineTestAccess::Publish(*second, one_entry) && SlotAckDeadlineTestAccess::Prepared(*second));
    assert(!SlotAckDeadlineTestAccess::Publish(*third, one_entry) && !SlotAckDeadlineTestAccess::Prepared(*third));
    // A watch with unsent output does not queue and keeps nothing built.
    assert(!SlotAckDeadlineTestAccess::Publish(*first, two_entry) && !SlotAckDeadlineTestAccess::Prepared(*first));
    assert(drain(*first) > budget / 2U && SlotAckDeadlineTestAccess::Excess(*first) == 0U);
    // The free pool goes to the earlier waiters, not to the first watch's next large frame.
    assert(!SlotAckDeadlineTestAccess::Publish(*first, two_entry));
    assert(SlotAckDeadlineTestAccess::Publish(*second, one_entry)); (void)drain(*second);
    assert(!SlotAckDeadlineTestAccess::Publish(*first, two_entry));
    assert(SlotAckDeadlineTestAccess::Publish(*third, one_entry)); (void)drain(*third); // Rebuilt at its turn.
    assert(SlotAckDeadlineTestAccess::Publish(*first, two_entry)); (void)drain(*first);
    for (const auto& state : {first, second, third}) { state->Cancel(); state->WorkStep(); }
    assert(SlotAckDeadlineTestAccess::Charged(*first) == 0U && SlotAckDeadlineTestAccess::Excess(*first) == 0U);
}
void ExcessPoolControlFrames(bool tls) {
    constexpr std::size_t budget = 4096U, share = 1024U, receipt = SlotAckDeadlineTestAccess::kReceipt;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"holder", "waiter", "first", "second", "acker"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto small = Set(*writer, 1);
    const auto one = LabelTransaction(*writer, 20U), two = LabelTransaction(*writer, 20U, 1U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto small_entry = archive.Next(), one_entry = archive.Next(), two_entry = archive.Next();
    assert(small_entry && small_entry->position.revision == small && one_entry && one_entry->position.revision == one);
    assert(two_entry && two_entry->position.revision == two);
    const auto drain = [](SlotWatch& state) {
        std::size_t bytes = 0U;
        while (auto output = state.Take(std::numeric_limits<std::size_t>::max())) {
            bytes += output->bytes->size(); state.Consumed(output->bytes->size());
            if (output->revision) state.Sent(*output->revision);
        }
        return bytes;
    };
    const auto progress = [&] {
        auto entry = std::make_shared<FeedEntry>(); // A new object for every attempt, as Progress builds it.
        entry->kind = FeedEntry::Kind::kProgress; entry->position = small_entry->position;
        return entry;
    };
    auto holder = SlotWatch::Create(table, "holder", {});
    auto waiter = SlotWatch::Create(table, "waiter", {});
    auto first = SlotWatch::Create(table, "first", {});
    auto second = SlotWatch::Create(table, "second", {});
    holder->SetQuota(share); waiter->SetQuota(share);
    first->SetQuota(16U); second->SetQuota(16U); // Smaller than a progress frame.
    assert(SlotAckDeadlineTestAccess::Publish(*holder, one_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*waiter, one_entry)); // Heads the queue: the pool lacks room for it.
    const auto held = SlotAckDeadlineTestAccess::Excess(*holder);
    // Progress frames above a share never queue behind the waiter, nor swap places with each other.
    for (int round = 0; round < 3; ++round) {
        assert(SlotAckDeadlineTestAccess::Publish(*first, progress()));
        assert(SlotAckDeadlineTestAccess::Publish(*second, progress()));
        assert(SlotAckDeadlineTestAccess::Excess(*holder) > held);
        assert(drain(*first) != 0U && drain(*second) != 0U);
        assert(SlotAckDeadlineTestAccess::Excess(*holder) == held);
    }
    (void)drain(*holder);
    assert(SlotAckDeadlineTestAccess::Publish(*waiter, one_entry)); (void)drain(*waiter);
    // A head that must wait for its own ACKs gives up its place to the next waiter.
    std::atomic<std::size_t> woken{0U};
    FeedOptions options; options.ack_timeout_ms = 600000U; options.notify = [&] { woken.fetch_add(1U); };
    auto acker = SlotWatch::Create(table, "acker", options);
    acker->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*acker, small_entry)); (void)drain(*acker); // One receipt held.
    assert(SlotAckDeadlineTestAccess::Publish(*holder, two_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*acker, one_entry));  // Head of the queue.
    assert(!SlotAckDeadlineTestAccess::Publish(*waiter, two_entry)); // Behind it.
    (void)drain(*holder);
    assert(SlotAckDeadlineTestAccess::Prepared(*acker)); // The head keeps its built change.
    acker->SetQuota(receipt); // A smaller share: its receipt now leaves no room.
    const auto before = woken.load();
    assert(!SlotAckDeadlineTestAccess::Publish(*acker, one_entry));
    assert(!SlotAckDeadlineTestAccess::Prepared(*acker) && woken.load() > before); // It woke the next waiter.
    assert(SlotAckDeadlineTestAccess::Publish(*waiter, two_entry)); (void)drain(*waiter);
    // After its ACK it queues again at the back.
    assert(acker->Ack(small) == SlotWatch::AckResult::kAccepted);
    acker->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*acker, one_entry)); (void)drain(*acker);
    for (const auto& state : {holder, waiter, first, second, acker}) { state->Cancel(); state->WorkStep(); }
    assert(SlotAckDeadlineTestAccess::Charged(*holder) == 0U && SlotAckDeadlineTestAccess::Excess(*holder) == 0U);
}
void ExcessPoolLeave(bool tls) {
    constexpr std::size_t budget = 4096U, share = 1024U, receipt = SlotAckDeadlineTestAccess::kReceipt;
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h, budget);
    auto writer = harness.Connect(); Create(*writer);
    for (const auto* name : {"holder", "leaver", "middle", "last", "schema"}) writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t");
    auto table = harness.catalog->Find("t");
    const auto initial = table->ListFeedSlots().front().position;
    const auto small = Set(*writer, 1);
    const auto one = LabelTransaction(*writer, 20U), two = LabelTransaction(*writer, 20U, 1U);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    const auto small_entry = archive.Next(), one_entry = archive.Next(), two_entry = archive.Next();
    assert(small_entry && small_entry->position.revision == small && one_entry && one_entry->position.revision == one);
    assert(two_entry && two_entry->position.revision == two);
    const auto drain = [](SlotWatch& state) {
        std::size_t bytes = 0U;
        while (auto output = state.Take(std::numeric_limits<std::size_t>::max())) {
            bytes += output->bytes->size(); state.Consumed(output->bytes->size());
            if (output->revision) state.Sent(*output->revision);
        }
        return bytes;
    };
    std::atomic<std::size_t> woken{0U};
    FeedOptions timed; timed.ack_timeout_ms = 600000U; timed.notify = [&] { woken.fetch_add(1U); };
    auto holder = SlotWatch::Create(table, "holder", {});
    auto leaver = SlotWatch::Create(table, "leaver", timed);
    auto middle = SlotWatch::Create(table, "middle", {});
    auto last = SlotWatch::Create(table, "last", {});
    auto schema_watch = SlotWatch::Create(table, "schema", {});
    for (const auto& state : {holder, leaver, middle, last}) state->SetQuota(share);
    assert(SlotAckDeadlineTestAccess::Publish(*leaver, small_entry)); (void)drain(*leaver); // One receipt held.
    assert(SlotAckDeadlineTestAccess::Publish(*holder, two_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, one_entry)); // Queue: leaver, middle, last.
    assert(!SlotAckDeadlineTestAccess::Publish(*middle, one_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*last, one_entry));
    (void)drain(*holder);
    // A larger share fits the change, but not beside the held receipt: the head leaves.
    leaver->SetQuota(SlotAckDeadlineTestAccess::PreparedSize(*leaver) + receipt + receipt / 2U);
    auto before = woken.load();
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, one_entry));
    assert(woken.load() > before && SlotAckDeadlineTestAccess::Prepared(*leaver)); // Within its share it stays built.
    assert(SlotAckDeadlineTestAccess::Publish(*middle, one_entry));
    // Rejoining puts it behind the watch that waited meanwhile.
    assert(leaver->Ack(small) == SlotWatch::AckResult::kAccepted);
    leaver->SetQuota(share);
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, one_entry));
    (void)drain(*middle);
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, one_entry));
    assert(SlotAckDeadlineTestAccess::Publish(*last, one_entry)); (void)drain(*last);
    assert(SlotAckDeadlineTestAccess::Publish(*leaver, one_entry)); (void)drain(*leaver);
    // A head whose receipt finds the table counter full waits for table room, so it leaves too.
    assert(SlotAckDeadlineTestAccess::Publish(*holder, one_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, two_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*middle, two_entry));
    (void)drain(*holder);
    const auto filler = budget - SlotAckDeadlineTestAccess::Charged(*leaver) - receipt + 1U;
    SlotAckDeadlineTestAccess::ChargeTable(*leaver, filler);
    before = woken.load();
    assert(!SlotAckDeadlineTestAccess::Publish(*leaver, two_entry));
    assert(woken.load() > before && !SlotAckDeadlineTestAccess::Prepared(*leaver) && SlotAckDeadlineTestAccess::Excess(*leaver) == 0U);
    SlotAckDeadlineTestAccess::ReleaseTable(*leaver, filler);
    assert(SlotAckDeadlineTestAccess::Publish(*middle, two_entry)); (void)drain(*middle);
    assert(SlotAckDeadlineTestAccess::Publish(*leaver, two_entry)); (void)drain(*leaver);
    // A schema frame above its share queues like a change and keeps nothing built behind the head.
    auto schema_entry = std::make_shared<FeedEntry>();
    schema_entry->kind = FeedEntry::Kind::kSchema; schema_entry->position = small_entry->position;
    schema_entry->schema_version = small_entry->schema_version;
    schema_entry->columns = table->geometry().LayoutAt(schema_entry->schema_version).schema().columns;
    schema_watch->SetQuota(16U);
    assert(SlotAckDeadlineTestAccess::Publish(*holder, two_entry));
    assert(!SlotAckDeadlineTestAccess::Publish(*last, two_entry)); // Head: no room beside the holder.
    assert(!SlotAckDeadlineTestAccess::Publish(*schema_watch, schema_entry)); // The pool has room, but not its turn.
    assert(!SlotAckDeadlineTestAccess::Prepared(*schema_watch) && SlotAckDeadlineTestAccess::PreparedSize(*schema_watch) > 16U);
    (void)drain(*holder);
    assert(SlotAckDeadlineTestAccess::Publish(*last, two_entry)); (void)drain(*last);
    assert(SlotAckDeadlineTestAccess::Publish(*schema_watch, schema_entry)); (void)drain(*schema_watch);
    for (const auto& state : {holder, leaver, middle, last, schema_watch}) { state->Cancel(); state->WorkStep(); }
    assert(SlotAckDeadlineTestAccess::Charged(*holder) == 0U && SlotAckDeadlineTestAccess::Excess(*holder) == 0U);
}
void LiveSchemaAcknowledgement(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect(); const auto initial = Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 7); FeedSlotTestAccess::Sync(*table);
    assert(Change(NextChange(*watch)) == first); // Live subscription exists before ALTER.
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    FeedSlotTestAccess::Sync(*table);
    const auto schema = watch->Read();
    assert(schema.type == '>' && schema.items.size() == 2U && schema.items[0].value == "schema");
    assert(Number(Field(schema.items[1], "schema_version")) == 2U && Field(schema.items[1], "columns").items.size() == 3U);
    const auto revision = Number(Field(schema.items[1], "revision")); assert(revision > first);
    watch->Line("ACK " + std::to_string(revision));
    Error(watch->Read(), "INVALID_ARGUMENT"); // Schema never advances the delivered ACK bound.
    assert(!watch->Ready(100ms));
    const auto next = Set(*writer, 8); FeedSlotTestAccess::Sync(*table);
    const auto change = NextChange(*watch); assert(Change(change) == next && Number(change.items[5]) == 2U);
    const auto& block = change.items[6].items[0];
    assert(block.items[2].items.size() == 3U && block.items[3].items.size() == 3U);
    assert(block.items[2].items[2].value == "9" && block.items[3].items[2].value == "9");
    watch->Line("ACK " + std::to_string(next));
    WaitAck(*writer, "t", "consumer", next);
    Unwatch(*watch);
    assert(Start(watch->Command("WATCH t SLOT 'consumer'")) == std::make_pair(initial.first, next));
    Unwatch(*watch);
}

void FilteredAndIdleProgress(bool tls) {
    struct ProgressObserver : FeedDeliveryTestHook {
        std::mutex mutex;
        std::vector<Clock::time_point> times;
        void Run(Point point, std::size_t) override {
            if (point != Point::kAfterProgressAdmission) return;
            std::lock_guard lock(mutex); times.push_back(Clock::now());
        }
    } observer; // Outlives the server and both delivery threads.
    Harness harness(tls, false, kDefaultSlotMaxBytes, 100ms, kDefaultFeedBufferBytes, {}, 100ms);
    FeedDeliveryTestAccess::SetHook(harness.server(), &observer);
    Cleanup clear_hook([&] { FeedDeliveryTestAccess::SetHook(harness.server(), nullptr); });
    auto writer = harness.Connect(); Create(*writer, " AREA 0 0 TO 0 0");
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect();
    const auto start = Start(watch->Command("WATCH t SLOT 'consumer' AREA 0 0 TO 0 0"));
    const auto revision = Set(*writer, 7, 4);
    FeedSlotTestAccess::Sync(*table);
    const auto deadline = Clock::now() + 10s;
    std::uint64_t progress = start.second;
    do {
        assert(Clock::now() < deadline);
        const auto event = watch->Read();
        assert(event.type == '>' && event.items.size() == 3U && event.items[0].value == "progress");
        assert(event.items[1].value == start.first);
        progress = Number(event.items[2]);
        assert(progress <= table->ListFeedSlots().front().durable_watermark);
    } while (progress < revision);
    // The filtered mutation was never sent; delivered progress now covers it.
    watch->Line("ACK " + std::to_string(progress));
    WaitAck(*writer, "t", "consumer", progress);
    const auto idle = watch->Read();
    assert(idle.items[0].value == "progress" && Number(idle.items[2]) == progress);
    const auto next_idle = watch->Read();
    assert(next_idle.items[0].value == "progress" && Number(next_idle.items[2]) == progress);
    {
        std::lock_guard lock(observer.mutex);
        assert(observer.times.size() >= 3U);
        for (std::size_t index = 1U; index < observer.times.size(); ++index)
            assert(observer.times[index] - observer.times[index - 1U] >= 100ms);
    }
    watch->Line("ACK " + std::to_string(progress + 1U));
    for (;;) {
        const auto event = watch->Read();
        if (event.type == '>') { assert(event.items[0].value == "progress"); continue; }
        Error(event, "INVALID_ARGUMENT"); break;
    }
    Unwatch(*watch);
    assert(Start(watch->Command("WATCH t SLOT 'consumer'")) == std::make_pair(start.first, progress));
    Unwatch(*watch);

    // An ordinary filtered watch receives the same idle cut without ACK state.
    auto ordinary = harness.Connect();
    const auto ordinary_start = Start(ordinary->Command("WATCH t AREA 0 0 TO 0 0"));
    const auto outside = Set(*writer, 8, 4);
    do {
        assert(Clock::now() < deadline);
        const auto event = ordinary->Read();
        assert(event.type == '>' && event.items.size() == 3U && event.items[0].value == "progress");
        assert(event.items[1].value == ordinary_start.first);
        progress = Number(event.items[2]);
    } while (progress < outside);
    Unwatch(*ordinary);
}

void AckDeadlineLedgerMode(bool tls, bool chunks) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer, chunks ? " CHUNKS" : "");
    for (const auto* name : {"without", "socket_clean", "socket_partial", "socket_tls", "budget", "late_ack", "late_unwatch"})
        writer->Ok("CREATE SLOT '" + std::string(name) + "' ON t" + (chunks ? " CHUNKS" : ""));
    auto table = harness.catalog->Find("t");
    auto chunk_feed = chunks ? table->SubscribeFeed({.chunks = true}) : nullptr;
    const auto initial = table->ListFeedSlots().front().position;
    const auto first = Set(*writer, 1), second = Set(*writer, 2);
    FeedSlotTestAccess::Sync(*table);
    auto archive = table->ReadFeedArchive(initial);
    auto first_entry = chunks ? feed_test::Next(*chunk_feed) : archive.Next();
    auto second_entry = chunks ? feed_test::Next(*chunk_feed) : archive.Next();
    assert(first_entry && second_entry && first_entry->position.revision == first && second_entry->position.revision == second);
    FeedOptions invalid; invalid.ack_timeout_ms = 100U;
    bool refused = false;
    try { (void)table->SubscribeFeed(invalid); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
    const auto anchor = Clock::now();
    const auto drain = [&](SlotWatch& state, Clock::time_point sent_at, bool partial) {
        while (auto output = state.Take(1024U * 1024U)) {
            assert(!output->bytes->empty());
            state.Consumed(output->bytes->size() - 1U);
            if (partial) {
                // The I/O pump calls Sent only after this remaining byte is written.
                assert(!SlotAckDeadlineTestAccess::Expire(state, sent_at + 1h));
                bool rejected = false;
                try { (void)state.Ack(first); } catch (const std::invalid_argument&) { rejected = true; }
                assert(rejected);
            }
            state.Consumed(1U);
            if (output->revision) SlotAckDeadlineTestAccess::Sent(state, *output->revision, sent_at);
        }
    };
    {
        auto state = SlotWatch::Create(table, "without", {});
        assert(SlotAckDeadlineTestAccess::Publish(*state, first_entry));
        drain(*state, anchor, false);
        assert(SlotAckDeadlineTestAccess::Count(*state) == 0U && SlotAckDeadlineTestAccess::Charged(*state) == 0U);
        assert(!SlotAckDeadlineTestAccess::Expire(*state, anchor + 1h));
        state->Cancel(); state->WorkStep();
    }
    {
        FeedOptions options; options.ack_timeout_ms = 100U;
        auto state = SlotWatch::Create(table, "consumer", options);
        assert(SlotAckDeadlineTestAccess::Publish(*state, first_entry));
        assert(SlotAckDeadlineTestAccess::Count(*state) == 1U);
        assert(!SlotAckDeadlineTestAccess::Expire(*state, anchor + 1h)); // queued is not sent
        drain(*state, anchor, true);
        assert(SlotAckDeadlineTestAccess::Charged(*state) > 0U); // bounded receipt survives wire consumption
        assert(SlotAckDeadlineTestAccess::Publish(*state, second_entry));
        drain(*state, anchor + 40ms, false);
        assert(!SlotAckDeadlineTestAccess::Expire(*state, anchor + 99ms));
        // A covering ACK removes the first timestamp; the second retains its own original send time.
        assert(state->Ack(first) == SlotWatch::AckResult::kAccepted && SlotAckDeadlineTestAccess::Count(*state) == 1U);
        auto progress = std::make_shared<FeedEntry>(); progress->kind = FeedEntry::Kind::kProgress;
        progress->position = second_entry->position;
        assert(SlotAckDeadlineTestAccess::Publish(*state, progress)); drain(*state, anchor + 90ms, false);
        assert(SlotAckDeadlineTestAccess::Count(*state) == 1U);
        assert(!SlotAckDeadlineTestAccess::Expire(*state, anchor + 139ms));
        assert(SlotAckDeadlineTestAccess::Expire(*state, anchor + 140ms));
        assert(state->Ack(second) == SlotWatch::AckResult::kTerminal);
        assert(SlotAckDeadlineTestAccess::Acked(*state) == first);
        state->Cancel(); state->WorkStep();
        assert(state->Finished() && SlotAckDeadlineTestAccess::Count(*state) == 0U && SlotAckDeadlineTestAccess::Charged(*state) == 0U);
    }
    for (const auto* name : {"late_ack", "late_unwatch"}) {
        FeedOptions options; options.ack_timeout_ms = 100U;
        auto state = SlotWatch::Create(table, name, options);
        assert(SlotAckDeadlineTestAccess::Publish(*state, first_entry)); drain(*state, Clock::now() - 200ms, false);
        if (std::string_view(name) == "late_ack") {
            assert(state->Ack(first) == SlotWatch::AckResult::kTerminal);
            assert(SlotAckDeadlineTestAccess::Acked(*state) == initial.revision);
        } else state->Unwatch();
        state->Activate(); state->WorkStep(); assert(state->AckTimedOut());
    }
    const char* socket_slots[] = {"socket_clean", "socket_partial", "socket_tls"};
    for (const auto mode : {0, 1, 2}) {
        FeedOptions options; options.ack_timeout_ms = 100U;
        auto state = SlotWatch::Create(table, socket_slots[mode], options);
        assert(SlotAckDeadlineTestAccess::Publish(*state, first_entry)); drain(*state, anchor, false);
        assert(SlotAckDeadlineTestAccess::Publish(*state, second_entry));
        AckDeadlineIoTestAccess::PendingClose(harness.server(), state, mode == 1, mode == 2);
    }
    {
        FeedOptions options; options.ack_timeout_ms = UINT64_MAX;
        auto state = SlotWatch::Create(table, "budget", options);
        state->SetQuota(1024U);
        bool blocked = false;
        for (std::uint64_t revision = second; revision < second + 100U; ++revision) {
            auto entry = std::make_shared<FeedEntry>(*second_entry); entry->position.revision = revision; entry->protocol_frame.reset();
            const auto before = SlotAckDeadlineTestAccess::Count(*state);
            if (!SlotAckDeadlineTestAccess::Publish(*state, entry)) {
                assert(before == SlotAckDeadlineTestAccess::Count(*state)); blocked = true; break;
            }
            drain(*state, anchor, false);
            assert(SlotAckDeadlineTestAccess::Charged(*state) <= 1024U);
        }
        assert(blocked && !SlotAckDeadlineTestAccess::Expire(*state, anchor + 1h));
        state->Cancel(); state->WorkStep();
        assert(SlotAckDeadlineTestAccess::Charged(*state) == 0U);
    }
}
void AckDeadlineLedger(bool tls) { AckDeadlineLedgerMode(tls, false); }
void ChunksDeadlineLedger(bool tls) { AckDeadlineLedgerMode(tls, true); }

void AckDeadlineWireMode(bool tls, bool chunks) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 10ms, kDefaultFeedBufferBytes, {}, 100ms);
    auto writer = harness.Connect(); Create(*writer, chunks ? " AREA 0 0 TO 0 0 CHUNKS" : " AREA 0 0 TO 0 0");
    const auto mutation = [&](Client& client) { return chunks ? Number(NextChunks(client).items[2]) : Change(NextChange(client)); };
    auto watch = harness.Connect();
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer' AREA 0 0 TO 0 0 ACK TIMEOUT 50"));
    const auto outside = Set(*writer, 1, 4);
    for (int i = 0; i < 3; ++i) {
        const auto event = watch->Read();
        assert(event.type == '>' && event.items[0].value == "progress");
        if (Number(event.items[2]) >= outside) watch->Line("ACK " + event.items[2].value);
    }
    Unwatch(*watch); // Progress alone cannot start the 50 ms mutation clock.
    const auto resumed = Start(watch->Command("WATCH t SLOT 'consumer' ACK TIMEOUT 300"));
    assert(resumed.first == initial.first && resumed.second >= outside);
    const auto first = Set(*writer, 2);
    assert(mutation(*watch) == first);
    watch->Line("ACK " + std::to_string(first));
    WaitAck(*writer, "t", "consumer", first);
    const auto unacked = Set(*writer, 3); assert(mutation(*watch) == unacked);
    const auto deadline = Clock::now() + 5s;
    for (;;) {
        assert(Clock::now() < deadline);
        const auto event = watch->Read();
        if (event.type == '-') {
            Error(event, "TIMEOUT"); assert(event.value.find("reason=ack_timeout") != std::string::npos); break;
        }
        assert(event.type == '>' && event.items[0].value == "progress");
    }
    Closed(*watch);
    assert(Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", "consumer"), "acked")) == first);
    auto replacement = harness.Connect();
    assert(Start(replacement->Command("WATCH t SLOT 'consumer'")).second == first);
    assert(mutation(*replacement) == unacked); Unwatch(*replacement);
}

void AckDeadlineWire(bool tls) { AckDeadlineWireMode(tls, false); }
void ChunksDeadlineWire(bool tls) { AckDeadlineWireMode(tls, true); }

void TakeoverStalledSocket(bool tls) {
    struct Observer : FeedDeliveryTestHook {
        std::mutex mutex;
        std::condition_variable changed;
        bool partial = false, closed = false, partial_before_close = false;
        void Run(Point point, std::size_t) override {
            if (point != Point::kAfterPartialFeedWrite && point != Point::kAfterBlockedFeedWrite && point != Point::kAfterTakeoverClose) return;
            std::lock_guard lock(mutex);
            if (point != Point::kAfterTakeoverClose) {
                if (!partial) std::cout << "stalled socket milestone: partial or blocked write" << std::endl;
                partial = true;
            } else {
                closed = true; partial_before_close = partial;
                std::cout << "stalled socket milestone: accepted socket closed" << std::endl;
            }
            changed.notify_all();
        }
        void WaitClosed() {
            std::unique_lock lock(mutex);
            if (!changed.wait_for(lock, 10s, [&] { return closed; }))
                throw std::runtime_error("takeover did not close the stalled socket");
            assert(partial_before_close);
        }
    } observer; // Outlives all server observers.
    Harness harness(tls);
    FeedDeliveryTestAccess::SetObserverHook(harness.server(), &observer);
    Cleanup clear([&] { FeedDeliveryTestAccess::SetObserverHook(harness.server(), nullptr); });
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8, label bytes(1048576)) CHUNK 4 x 4");
    writer->Ok("ALTER TABLE t SET var_max_chunk_bytes = 16777216");
    writer->Ok("CREATE SLOT 'consumer' ON t CHUNKS");
    const auto mutation = [&](Client& client) { return Number(NextChunks(client).items[2]); };
    auto watch = harness.Connect(); watch->SetReceiveBuffer(4096);
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto first = Set(*writer, 1); assert(mutation(*watch) == first);
    // Stop reading. Several large mutations fill the real TCP window and leave a partial frame.
    const std::string payload(1024U * 1024U, 'x');
    for (int x = 0; x < 8; ++x) {
        writer->Line("SET BLOCK " + std::to_string(x) + " 0 IN t label = $1");
        writer->Send("$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n");
        const auto reply = writer->Read();
        if (reply.type == '-') throw std::runtime_error("large stalled-socket mutation failed: " + reply.value);
        (void)Number(reply);
    }
    // Explicitly observe a real partial/blocked write before requesting takeover.
    { std::unique_lock lock(observer.mutex);
      if (!observer.changed.wait_for(lock, 10s, [&] { return observer.partial; }))
          throw std::runtime_error("takeover fixture did not stall output"); }
    auto replacement = harness.Connect();
    assert(Start(replacement->Command("WATCH t SLOT 'consumer' TAKEOVER")).second == initial.second);
    observer.WaitClosed(); // Actual socket closure, without reading a terminal error.
    const auto listed = writer->Command("SHOW SLOTS ON t");
    assert(Number(Field(Slot(listed, "t", "consumer"), "acked")) == initial.second);

    assert(mutation(*replacement) == first);
    replacement->Close(); watch->Close();
}


void AckDeadlineStalledSocketMode(bool tls, bool chunks) {
    struct Observer : FeedDeliveryTestHook {
        std::mutex mutex;
        std::condition_variable changed;
        bool partial = false, closed = false, partial_before_close = false;
        void Run(Point point, std::size_t) override {
            if (point != Point::kAfterPartialFeedWrite && point != Point::kAfterBlockedFeedWrite && point != Point::kAfterAckTimeoutClose) return;
            std::lock_guard lock(mutex);
            if (point != Point::kAfterAckTimeoutClose) {
                if (!partial) std::cout << "stalled socket milestone: partial or blocked write" << std::endl;
                partial = true;
            } else {
                closed = true; partial_before_close = partial;
                std::cout << "stalled socket milestone: accepted socket closed" << std::endl;
            }
            changed.notify_all();
        }
        void WaitClosed() {
            std::unique_lock lock(mutex);
            if (!changed.wait_for(lock, 10s, [&] { return closed; }))
                throw std::runtime_error("expired ACK timeout did not close the stalled socket");
            assert(partial_before_close);
        }
    } observer; // Outlives all server observers.
    Harness harness(tls, false, kDefaultSlotMaxBytes, 10ms);
    FeedDeliveryTestAccess::SetObserverHook(harness.server(), &observer);
    Cleanup clear([&] { FeedDeliveryTestAccess::SetObserverHook(harness.server(), nullptr); });
    auto writer = harness.Connect();
    writer->Ok("CREATE TABLE t (n u8, label bytes(1048576)) CHUNK 4 x 4");
    writer->Ok("ALTER TABLE t SET var_max_chunk_bytes = 16777216");
    writer->Ok(chunks ? "CREATE SLOT 'consumer' ON t CHUNKS" : "CREATE SLOT 'consumer' ON t");
    const auto mutation = [&](Client& client) { return chunks ? Number(NextChunks(client).items[2]) : Change(NextChange(client)); };
    auto watch = harness.Connect(); watch->SetReceiveBuffer(4096);
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer' ACK TIMEOUT 1000"));
    const auto first = Set(*writer, 1); assert(mutation(*watch) == first);
    // Stop reading. Several large mutations fill the real TCP window and leave a partial frame.
    const std::string payload(1024U * 1024U, 'x');
    for (int x = 0; x < 8; ++x) {
        writer->Line("SET BLOCK " + std::to_string(x) + " 0 IN t label = $1");
        writer->Send("$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n");
        const auto reply = writer->Read();
        if (reply.type == '-') throw std::runtime_error("large stalled-socket mutation failed: " + reply.value);
        (void)Number(reply);
    }
    observer.WaitClosed(); // Observes actual CloseClient; receiving TIMEOUT is unnecessary.
    const auto listed = writer->Command("SHOW SLOTS ON t");
    assert(Number(Field(Slot(listed, "t", "consumer"), "acked")) == initial.second);
    auto replacement = harness.Connect();
    assert(Start(replacement->Command("WATCH t SLOT 'consumer'")).second == initial.second);
    assert(mutation(*replacement) == first);
    replacement->Close(); watch->Close();
}

void AckDeadlineStalledSocket(bool tls) { AckDeadlineStalledSocketMode(tls, false); }
void ChunksDeadlineStalledSocket(bool tls) { AckDeadlineStalledSocketMode(tls, true); }

class AckPersistCount : public FeedSlotTestHook {
  public:
    void Run(Point point, std::uint64_t) override {
        if (point == Point::kAfterAckPersist) writes_.fetch_add(1U, std::memory_order_relaxed);
    }
    std::size_t writes() const { return writes_.load(std::memory_order_relaxed); }
  private:
    std::atomic<std::size_t> writes_{0U};
};
void TableAckBatching(bool tls) {
    AckPersistCount count; // Outlives the manager's background callbacks.
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    writer->Ok("CREATE SLOT 'second' ON t");
    auto table = harness.catalog->Find("t");
    const auto epoch = table->Info().store_id;
    const auto position = [&](std::string_view name) {
        return Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", name), "acked"));
    };
    FeedSlotTestAccess::SetHook(*table, &count);
    Cleanup hook_cleanup([&] { FeedSlotTestAccess::SetHook(*table, nullptr); });
    // Direct clock control checks actual metadata writes without blocking the
    // single socket catch-up worker. The other groups exercise wire ACKs.
    const auto anchor = std::chrono::steady_clock::now() + 1h;
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, first});
    FeedSlotTestAccess::StageAck(*table, "second", {epoch, first});
    assert(FeedSlotTestAccess::FlushAcks(*table, true, anchor));
    assert(count.writes() == 1U && position("consumer") == first && position("second") == first);

    const auto next = Set(*writer, 2); FeedSlotTestAccess::Sync(*table);
    FeedOptions resume; resume.after = FeedPosition{epoch, next};
    auto cancelled = SlotWatch::Create(table, "consumer", resume);
    assert(cancelled->position().revision == next && position("consumer") == first); // AFTER does not ACK.
    cancelled->Ack(next); cancelled->Cancel(); cancelled->WorkStep();
    assert(cancelled->Finished() && !cancelled->Take(4096U)); cancelled.reset();
    FeedSlotTestAccess::StageAck(*table, "second", {epoch, next});
    assert(!FeedSlotTestAccess::FlushAcks(*table, false, anchor + 99ms));
    assert(count.writes() == 1U && position("consumer") == first && position("second") == first);
    assert(FeedSlotTestAccess::FlushAcks(*table, false, anchor + 100ms));
    assert(count.writes() == 2U && position("consumer") == next && position("second") == next);

    const auto pending = Set(*writer, 3); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, pending});
    FeedSlotTestAccess::StageAck(*table, "second", {epoch, pending});
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    FeedSlotTestAccess::SetHook(*table, &count); // The manager was replaced by ALTER.
    assert(!FeedSlotTestAccess::FlushAcks(*table, false, anchor + 199ms));
    assert(count.writes() == 2U && position("consumer") == next && position("second") == next);
    // A third watch has no new ACK of its own. Its UNWATCH must still flush
    // both other slots before returning OK, bypassing the shared timer.
    writer->Ok("CREATE SLOT 'idle' ON t");
    const auto idle_written = position("idle");
    auto idle = SlotWatch::Create(table, "idle", {});
    idle->Activate(); idle->Unwatch(); idle->WorkStep();
    assert(idle->Finished());
    const auto completed = idle->Take(4096U);
    assert(completed && completed->resume && !completed->close && *completed->bytes == "+OK\r\n");
    idle->Consumed(completed->bytes->size()); idle.reset();
    assert(count.writes() == 3U && position("consumer") == pending && position("second") == pending);
    assert(position("idle") == idle_written);

    const auto dropped = Set(*writer, 4); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, dropped});
    writer->Ok("DROP SLOT 'consumer' ON t"); writer->Ok("CREATE SLOT 'consumer' ON t");
    assert(position("consumer") == dropped);
    assert(!FeedSlotTestAccess::FlushAcks(*table, true, anchor + 200ms));
    // UNWATCH restored the real flush clock. The background sweep may have
    // persisted this one ACK before DROP removed its pending entry.
    const auto writes_after_drop = count.writes();
    assert(writes_after_drop >= 3U && writes_after_drop <= 4U);
    assert(position("consumer") == dropped && position("second") == pending);
    const auto immediate = Set(*writer, 5); FeedSlotTestAccess::Sync(*table);
    table->AdvanceFeedSlot("second", {epoch, immediate}); // Public C++ advance remains immediate.
    assert(position("second") == immediate && count.writes() == writes_after_drop);
    // The last cancelled watch leaves an accepted ACK for the manager's
    // background sweep. No active watch or manual flush may drive it.
    const auto background = Set(*writer, 6); FeedSlotTestAccess::Sync(*table);
    resume.after = FeedPosition{epoch, background};
    cancelled = SlotWatch::Create(table, "consumer", resume);
    assert(cancelled->position().revision == background && position("consumer") == dropped);
    cancelled->Ack(background); cancelled->Cancel(); cancelled->WorkStep();
    assert(cancelled->Finished() && !cancelled->Take(4096U)); cancelled.reset();
    WaitAck(*writer, "t", "consumer", background, 2s);
    assert(count.writes() == writes_after_drop + 1U && position("second") == immediate);
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

void Fence(Client& writer) {
    txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
    const auto migration_error = writer.Command("MIGRATE 'fence' ALTER TABLE default ADD COLUMN extra u8 NULL");
    Error(migration_error, "OUTCOME_UNKNOWN");
    assert(migration_error.value.find("stage=migration_completion") != std::string::npos);
}
void FencedWatch(bool tls) {
    Harness harness(tls); auto writer = harness.Connect(); Create(*writer);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto revision = Set(*writer, 7); assert(Change(NextChange(*watch)) == revision);
    Fence(*writer);
    const auto failure = watch->Read(); Error(failure, "INTERNAL");
    assert(failure.value == "ERR INTERNAL reason=internal " + std::string(kMigrationRecoveryRequiredMessage));
    // A live subscription was destroyed under the fence. The affected stream
    // ends, while the listener and statement workers still answer commands.
    auto observer = harness.Connect();
    const auto reply = observer->Command("PING"); Error(reply, "INTERNAL");
    assert(reply.value == "ERR INTERNAL reason=internal " + std::string(kMigrationRecoveryRequiredMessage));
}
void FencedCancelledAck(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    const auto original = table->ListFeedSlots().front().position;
    const auto revision = Set(*writer, 8); FeedSlotTestAccess::Sync(*table);
    FeedOptions options; options.after = FeedPosition{original.epoch, revision};
    auto watch = SlotWatch::Create(table, "consumer", options);
    watch->Ack(revision); watch->Cancel();
    Fence(*writer);
    watch->WorkStep();
    assert(watch->Finished() && !watch->Take(4096U)); watch.reset();
    const auto slots = ReadFeedSlotRecords(harness.directory.path()/"tables/t", original.epoch);
    assert(slots && slots->slots.front().written == original.revision);
    auto observer = harness.Connect(); Error(observer->Command("PING"), "INTERNAL");
}

// Every watch of a table receives the same feed, whatever their number: each
// committed change once, in revision order, with its previous and new values.
// Writers on several connections finish together, so the sender can also be
// woken for scans that publish nothing.
void ManySubscribers(bool tls) {
    constexpr std::size_t kOrdinary = 4U, kSlots = 3U, kWriters = 3U, kWrites = 25U;
    Harness harness(tls);
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE t (n u8, label text(128)) CHUNK 4 x 4");
    for (std::size_t i = 0; i < kSlots; ++i) admin->Ok("CREATE SLOT 'fan_" + std::to_string(i) + "' ON t");
    std::vector<std::unique_ptr<Client>> ordinary, slots, writers;
    for (std::size_t i = 0; i < kOrdinary; ++i) {
        ordinary.push_back(harness.Connect());
        (void)Start(ordinary.back()->Command("WATCH t"));
    }
    for (std::size_t i = 0; i < kSlots; ++i) {
        slots.push_back(harness.Connect());
        (void)Start(slots.back()->Command("WATCH t SLOT 'fan_" + std::to_string(i) + "'"));
    }
    for (std::size_t i = 0; i < kWriters; ++i) writers.push_back(harness.Connect());
    // Each writer owns one block of its own chunk: values 1..kWrites, then a delete.
    std::vector<std::exception_ptr> errors(kWriters);
    std::vector<std::thread> threads;
    for (std::size_t w = 0; w < kWriters; ++w) threads.emplace_back([&, w] {
        try {
            const auto x = std::to_string(w * 4U);
            for (std::size_t i = 1; i <= kWrites; ++i)
                (void)Number(writers[w]->Command("SET BLOCK " + x + " 0 IN t n = " + std::to_string(i) +
                    ", label = 'w" + std::to_string(w) + "-" + std::to_string(i) + "'"));
            (void)Number(writers[w]->Command("DELETE BLOCK " + x + " 0 FROM t"));
        } catch (const std::exception&) { errors[w] = std::current_exception(); }
    });
    // One transaction changes two chunks in one change.
    admin->Ok("BEGIN");
    assert(admin->Command("SET BLOCK 1 5 IN t n = 7, label = 'a'").type == '_');
    assert(admin->Command("SET BLOCK 9 5 IN t n = 8, label = 'b'").type == '_');
    const auto committed = Number(admin->Command("COMMIT"));
    for (auto& thread : threads) thread.join();
    for (const auto& error : errors) if (error) std::rethrow_exception(error);
    const std::size_t changes = kWriters * (kWrites + 1U) + 1U;
    std::vector<Reply> expected;
    for (std::size_t i = 0; i < changes; ++i) expected.push_back(ordinary.front()->Read());
    for (std::size_t c = 1; c < kOrdinary; ++c)
        for (std::size_t i = 0; i < changes; ++i) assert(ordinary[c]->Read() == expected[i]);
    for (const auto& slot : slots)
        for (std::size_t i = 0; i < changes; ++i) assert((i == 0U ? NextChange(*slot) : slot->Read()) == expected[i]);
    // The shared feed itself: increasing revisions, each block's previous
    // value is its last new value, and every write is present.
    std::map<std::string, Reply> last;
    std::map<std::string, std::vector<Reply>> history;
    std::uint64_t previous = 0U;
    bool transaction = false;
    for (const auto& change : expected) {
        const auto revision = Change(change, change.items[6].items.size());
        assert(revision > previous);
        previous = revision;
        if (revision == committed) transaction = change.items[6].items.size() == 2U;
        for (const auto& block : change.items[6].items) {
            assert(block.type == '*' && block.items.size() == 4U);
            const auto key = block.items[0].value + " " + block.items[1].value;
            const auto known = last.find(key);
            assert(known == last.end() ? block.items[2].type == '_' : block.items[2] == known->second);
            last[key] = block.items[3];
            history[key].push_back(block.items[3]);
        }
    }
    assert(transaction);
    for (std::size_t w = 0; w < kWriters; ++w) {
        const auto& values = history[std::to_string(w * 4U) + " 0"];
        assert(values.size() == kWrites + 1U && values.back().type == '_');
        for (std::size_t i = 1; i <= kWrites; ++i) {
            const auto& row = values[i - 1U];
            assert(row.type == '*' && row.items.size() == 2U && Number(row.items[0]) == i);
            assert(row.items[1].value == "w" + std::to_string(w) + "-" + std::to_string(i));
        }
    }
    for (const auto& watch : ordinary) Unwatch(*watch);
    for (const auto& watch : slots) Unwatch(*watch);
}

} // namespace

int main(int argc, char** argv) {
    test::FeedPhaseWatchdog::SuppressWindowsDialogs();
    std::string_view selected;
    std::optional<bool> transport;
    bool fence_only = false, shares_only = false;
    for (int argument = 1; argument < argc; ++argument) {
        const std::string_view option(argv[argument]);
        if (option == "--tls") transport = true;
        else if (option == "--plain") transport = false;
        else if (option == "--migration-fence-only") fence_only = true;
        else if (option == "--shares-only") shares_only = true;
        else if (option == "--group" && argument + 1 < argc) selected = argv[++argument];
        else { std::cerr << "unknown test option: " << option << '\n'; return 2; }
    }
    if (selected == "PhaseWatchdogStall") test::FeedPhaseWatchdog::StalledControl();
    std::size_t failures = 0, total = 0;
    for (const bool tls : {false, true}) {
#ifndef CHUNKDB_WITH_OPENSSL
        if (tls) continue;
#endif
        if (transport && *transport != tls) continue;
        std::size_t passed = 0, executed = 0;
        const auto run = [&](const char* name, auto function) {
            if (!selected.empty() && selected != name) return;
            ++executed; ++total;
            if (RunGroup(tls, name, function)) ++passed;
            else ++failures;
        };
        if (selected == "client-exception") run("client-exception", [](bool use_tls) {
            Harness harness(use_tls); auto client = harness.Connect(); client->Close(); (void)client->Read();
        });
        // Feed share and excess-pool groups run as their own ctest entry.
        if (shares_only || !selected.empty()) {
            run("share-independent-delivery", ShareIndependentDelivery); run("table-budget-terminates", TableBudgetStillTerminates);
            run("excess-pool-bound", ExcessPoolBound); run("excess-pool-receipts", ExcessPoolReceipts);
            run("excess-stalled-holder", ExcessStalledHolder); run("TLS-write-cap", TlsWriteCap);
            run("excess-pool-FIFO", ExcessPoolFifo); run("excess-pool-control", ExcessPoolControlFrames);
            run("excess-pool-leave", ExcessPoolLeave);
        }
        if (shares_only) {
            std::cout << (tls ? "TLS" : "plain") << ": " << passed << "/" << executed << " feed share groups passed\n";
            continue;
        }
        run("migration-fence-watch", FencedWatch); run("migration-fence-cancelled-ACK", FencedCancelledAck);
        if (fence_only) {
            std::cout << (tls ? "TLS" : "plain") << ": " << passed << "/" << executed << " migration fence groups passed\n";
            continue;
        }
        run("conditional-objects", ConditionalObjects);
        run("slot-record-outcome", SlotRecordOutcome);
        run("takeover-fence", TakeoverFence); run("takeover", Takeover); run("takeover-rights", TakeoverRights); run("takeover-UNWATCH-return", TakeoverUnwatchReturn); run("takeover-stalled-socket", TakeoverStalledSocket);
        run("lifecycle", Lifecycle); run("rights", Rights); run("watch-revoke", WatchRevoke); run("watch-ACK-revoke", WatchAckRevoke); run("watch-dropped-user", WatchDroppedUser); run("slot-owners", SlotOwners); run("first-attach-owner", FirstAttachOwner); run("archive-handover", ArchiveHandover);
        run("durable-gate", DurableGate); run("lost-and-drop", LostAndDrop); run("replacement", ReplacementClaim);
        run("terminal-ACK", TerminalErrorBeforeAck);
        run("chunk-forms", ChunkForms);
        run("persistent-filters", PersistentFilterDelivery);
        run("historical-schema", HistoricalSchemas); run("held-lease", ArchiveWhileLeaseHeld);
        run("AFTER-durable", AfterWaitsForDurable); run("tiny-budget", TinyBudgetTerminates);
        run("live-schema-ACK", LiveSchemaAcknowledgement);
        run("filtered-idle-progress", FilteredAndIdleProgress);
        run("admitted-share", AdmittedChangeSurvivesShareShrink);
        run("table-ACK-batch", TableAckBatching);
        run("ACK-deadline-ledger", AckDeadlineLedger);
        run("CHUNKS-ACK-deadline-ledger", ChunksDeadlineLedger);
        run("ACK-deadline-wire", AckDeadlineWire);
        run("CHUNKS-ACK-deadline-wire", ChunksDeadlineWire);
        run("ACK-deadline-stalled-socket", AckDeadlineStalledSocket);
        run("CHUNKS-ACK-deadline-stalled-socket", ChunksDeadlineStalledSocket);
        run("many-subscribers", ManySubscribers);
        std::cout << (tls ? "TLS" : "plain") << ": " << passed << "/" << executed << " slot protocol groups passed\n";
    }
    if (total == 0U) { std::cerr << "no matching test group\n"; return 2; }
    return failures == 0U ? 0 : 1;
}
