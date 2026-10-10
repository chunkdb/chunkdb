#include <cassert>
#include <functional>

#include "feed_slot_records.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;

void Reject(const std::function<void()>& operation) {
    bool threw = false;
    try { operation(); } catch (const std::exception&) { threw = true; }
    assert(threw);
}

void Records() {
    StoreId epoch{};
    epoch[0] = 1U;
    FeedSlotRecords records{epoch, 987U, {{"consumer_1", 123U, false}, {"lost", 456U, true}}};
    const auto bytes = SerializeFeedSlotRecords(records);
    const auto decoded = ParseFeedSlotRecords(bytes, epoch);
    assert(decoded.epoch == epoch && decoded.durable_watermark == 987U);
    assert(decoded.slots.size() == 2U && decoded.slots[0].name == "consumer_1");
    assert(decoded.slots[0].written == 123U && !decoded.slots[0].lost);
    assert(decoded.slots[1].written == 456U && decoded.slots[1].lost);
    for (std::size_t size = 0; size < bytes.size(); ++size) {
        auto partial = bytes;
        partial.resize(size);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
    }
    for (std::size_t i = 0; i < bytes.size(); ++i) {
        auto corrupt = bytes;
        corrupt[i] ^= 1U;
        Reject([&] { (void)ParseFeedSlotRecords(corrupt, epoch); });
    }
    auto foreign = epoch;
    foreign[0] = 2U;
    Reject([&] { (void)ParseFeedSlotRecords(bytes, foreign); });
    records.slots.push_back(records.slots[0]);
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.slots.pop_back();
    records.slots[0].written = 988U;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    for (const auto name : {"", "1name", "Upper", "a-b", "a.b"})
        Reject([&] { RequireValidFeedSlotName(name); });
    RequireValidFeedSlotName("_valid_123");
    Reject([&] { RequireValidFeedSlotName(std::string(64U, 'a')); });

    chunkdb::test::ScopedTempDir dir("chunkdb-feed-slot-records");
    assert(!ReadFeedSlotRecords(dir.path(), epoch));
    records.slots[0].written = 123U;
    WriteFeedSlotRecords(dir.path(), records);
    assert(ReadFeedSlotRecords(dir.path(), epoch)->slots[0].written == 123U);
    records.slots[0].written = 789U;
    WriteFeedSlotRecords(dir.path(), records);
    assert(ReadFeedSlotRecords(dir.path(), epoch)->slots[0].written == 789U);
}
}  // namespace

int main() { Records(); }
