#include <atomic>
#include <condition_variable>
#include <iostream>

#include "server_slots_test_utils.hpp"
#include "feed_slots.hpp"
#include "slot_watch.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::slot_socket_test;

std::uint64_t Set(Client& writer, unsigned value, int x = 0) {
    return Number(writer.Command("SET BLOCK " + std::to_string(x) + " 0 IN t n = " + std::to_string(value)));
}
void Create(Client& client) {
    client.Ok("CREATE TABLE t (n u8, label text(128)) CHUNK 4 x 4");
    client.Ok("CREATE SLOT 'consumer' ON t");
}
void Lifecycle(bool tls) {
    Harness harness(tls);
    auto writer = harness.Connect(); Create(*writer);
    auto watch = harness.Connect();
    const auto initial = Start(watch->Command("WATCH t SLOT 'consumer'"));
    auto listed = writer->Command("SHOW SLOTS ON t");
    const auto& slot = Slot(listed, "t", "consumer");
    assert(slot.items.size() == 12U && Field(slot, "epoch").value == initial.first);
    assert(Number(Field(slot, "acked")) == initial.second && !Boolean(Field(slot, "lost")));
    auto second = harness.Connect();
    Error(second->Command("WATCH t SLOT 'consumer'"), "BUSY");
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

void Rights(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect(); Create(*admin);
    admin->Ok("CREATE TABLE hidden (n u8) CHUNK 4 x 4");
    admin->Ok("CREATE SLOT 'private' ON hidden");
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(scram::MakeVerifier("pw", salt, scram::kMinIterations));
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
    (void)Start(reader.Command("WATCH t SLOT 'consumer'")); Unwatch(reader);
    admin->Ok("GRANT WRITE ON t TO reader");
    Error(reader.Command("CREATE SLOT 'other' ON t"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON t TO reader");
    reader.Ok("CREATE SLOT 'other' ON t"); reader.Ok("DROP SLOT 'other' ON t");
    Error(reader.Command("CREATE SLOT '../bad' ON t"), "SYNTAX");
}

void ArchiveHandover(bool tls) {
    // Fit the two-chunk transaction's raw state; the separate held-lease test
    // forces a witnessed reset with a 4096-byte single-chunk feed.
    Harness harness(tls, true, kDefaultSlotMaxBytes, 1h, 16384);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    writer->Ok("CREATE SLOT 'archive_area' ON t");
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
    writer->Ok("CREATE SLOT 'area' ON t");
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
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    SyncPause pause; FeedSlotTestAccess::SetHook(*table, &pause);
    const auto revision = Set(*writer, 7);
    assert(Change(ordinary->Read()) == revision);
    std::thread sync([&] { FeedSlotTestAccess::Sync(*table); }); pause.Wait();
    assert(!watch->Ready(150ms)); // WAL bytes flushed, but fsync/written frontier still withheld.
    const auto ping = writer->Command("PING"); assert(ping.value == "PONG");
    pause.Release(); sync.join();
    assert(Change(NextChange(*watch)) == revision); // No new change is needed to wake the watch.
    FeedSlotTestAccess::SetHook(*table, nullptr);
    Unwatch(*watch); Unwatch(*ordinary);
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
void HistoricalSchemas(bool tls) {
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    auto table = harness.catalog->Find("t");
    auto ordinary = harness.Connect(); (void)Start(ordinary->Command("WATCH t"));
    const auto first = Set(*writer, 7); const auto first_entry = ordinary->Read();
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    writer->Ok("ALTER TABLE t ADD COLUMN extra u16 DEFAULT 9");
    const auto alteration = ordinary->Read();
    assert(alteration.items[0].value == "schema" && Number(alteration.items[3]) == 2U);
    const auto second = Set(*writer, 8); const auto second_entry = ordinary->Read();
    { auto lease = table->Acquire(); lease->store().CheckpointForTests(0, 0); }
    FeedSlotTestAccess::Sync(*table);
    auto watch = harness.Connect(); (void)Start(watch->Command("WATCH t SLOT 'consumer'"));
    const auto old_schema = watch->Read();
    assert(old_schema.items[0].value == "schema" && Number(old_schema.items[3]) == 1U);
    assert(old_schema.items[4].items.size() == 2U);
    assert(Field(old_schema.items[4].items[0], "name").value == "n");
    assert(watch->Read() == first_entry && Change(first_entry) == first);
    const auto new_schema = watch->Read();
    assert(new_schema.items[0].value == "schema" && Number(new_schema.items[3]) == 2U);
    assert(new_schema.items[4].items == alteration.items[4].items);
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
    FeedTestAccess::SetHook(*table, &pause); (void)pause.Wait();
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
    assert(schema.type == '>' && schema.items.size() == 5U && schema.items[0].value == "schema");
    assert(Number(schema.items[3]) == 2U && schema.items[4].items.size() == 3U);
    const auto revision = Number(schema.items[2]); assert(revision > first);
    watch->Line("ACK " + std::to_string(revision));
    WaitAck(*writer, "t", "consumer", revision); // Schema can be acknowledged without any later data.
    assert(!watch->Ready(100ms)); Unwatch(*watch);
    assert(Start(watch->Command("WATCH t SLOT 'consumer'")) == std::make_pair(initial.first, revision));
    const auto next = Set(*writer, 8); FeedSlotTestAccess::Sync(*table);
    const auto change = NextChange(*watch); assert(Change(change) == next && Number(change.items[5]) == 2U);
    const auto& block = change.items[6].items[0];
    assert(block.items[2].items.size() == 3U && block.items[3].items.size() == 3U);
    assert(block.items[2].items[2].value == "9" && block.items[3].items[2].value == "9");
    Unwatch(*watch);
}

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
    Harness harness(tls, false, kDefaultSlotMaxBytes, 1h);
    auto writer = harness.Connect(); Create(*writer);
    writer->Ok("CREATE SLOT 'second' ON t");
    auto table = harness.catalog->Find("t");
    const auto epoch = table->Info().store_id;
    const auto position = [&](std::string_view name) {
        return Number(Field(Slot(writer->Command("SHOW SLOTS ON t"), "t", name), "acked"));
    };
    AckPersistCount count; FeedSlotTestAccess::SetHook(*table, &count);
    // Direct clock control checks actual metadata writes without blocking the
    // single socket catch-up worker. The other groups exercise wire ACKs.
    const auto anchor = std::chrono::steady_clock::now() + 1h;
    const auto first = Set(*writer, 1); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, first});
    FeedSlotTestAccess::StageAck(*table, "second", {epoch, first});
    assert(FeedSlotTestAccess::FlushAcks(*table, true, anchor));
    assert(count.writes() == 1U && position("consumer") == first && position("second") == first);

    const auto next = Set(*writer, 2); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, next});
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
    const auto completed = idle->Take(4096U, 4096U);
    assert(completed && completed->resume && !completed->close && *completed->bytes == "+OK\r\n");
    idle->Consumed(completed->bytes->size()); idle.reset();
    assert(count.writes() == 3U && position("consumer") == pending && position("second") == pending);
    assert(position("idle") == idle_written);

    const auto dropped = Set(*writer, 4); FeedSlotTestAccess::Sync(*table);
    FeedSlotTestAccess::StageAck(*table, "consumer", {epoch, dropped});
    writer->Ok("DROP SLOT 'consumer' ON t"); writer->Ok("CREATE SLOT 'consumer' ON t");
    assert(position("consumer") == dropped);
    assert(!FeedSlotTestAccess::FlushAcks(*table, true, anchor + 200ms));
    assert(count.writes() == 3U && position("consumer") == dropped && position("second") == pending);
    const auto immediate = Set(*writer, 5); FeedSlotTestAccess::Sync(*table);
    table->AdvanceFeedSlot("second", {epoch, immediate}); // Public C++ advance remains immediate.
    assert(position("second") == immediate && count.writes() == 3U);
    FeedSlotTestAccess::SetHook(*table, nullptr);
}

} // namespace

int main() {
    for (const bool tls : {false, true}) {
#ifndef CHUNKDB_WITH_OPENSSL
        if (tls) continue;
#endif
        const auto run = [&](const char* name, auto function) {
            std::cout << (tls ? "TLS " : "plain ") << name << std::endl;
            function(tls);
        };
        run("lifecycle", Lifecycle); run("rights", Rights); run("archive-handover", ArchiveHandover);
        run("durable-gate", DurableGate); run("lost-and-drop", LostAndDrop); run("replacement", ReplacementClaim);
        run("historical-schema", HistoricalSchemas); run("held-lease", ArchiveWhileLeaseHeld);
        run("AFTER-durable", AfterWaitsForDurable); run("tiny-budget", TinyBudgetTerminates);
        run("live-schema-ACK", LiveSchemaAcknowledgement);
        run("table-ACK-batch", TableAckBatching);
        std::cout << (tls ? "TLS" : "plain") << ": 12 slot protocol groups passed\n";
    }
}
