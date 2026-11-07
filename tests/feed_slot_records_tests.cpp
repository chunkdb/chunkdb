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
    assert(ReadLe16(bytes, 4U) == 1U && !decoded.filter_format);
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
void Filters() {
    StoreId epoch{}; epoch[0] = 9U;
    FeedSlotRecords records{epoch, UINT64_MAX, {{"filtered", UINT64_MAX, false}}};
    auto& slot = records.slots.front();
    slot.filters = {.area = FeedArea{{INT64_MIN, -3}, {INT64_MAX, 7}},
        .column_ids = std::vector<std::uint32_t>{UINT32_MAX, 2U, 1U}, .no_before = true, .chunks = true};
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(ReadLe16(bytes, 4U) == 2U);
    const auto parsed = ParseFeedSlotRecords(bytes, epoch);
    assert(parsed.filter_format && parsed.slots.front().filters == slot.filters);
    assert(parsed.slots.front().written == UINT64_MAX);
    for (std::size_t at = 0; at < bytes.size(); ++at) {
        auto partial = bytes; partial.resize(at);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
        auto corrupt = bytes; corrupt[at] ^= 1U;
        Reject([&] { (void)ParseFeedSlotRecords(corrupt, epoch); });
    }
    const auto reject_checked = [&](std::vector<std::uint8_t> changed) {
        const auto crc = changed.size() - 4U;
        changed.resize(crc); WriteLe32(changed, Crc32(changed.data(), changed.size()));
        Reject([&] { (void)ParseFeedSlotRecords(changed, epoch); });
    };
    auto malformed = bytes; malformed[37U] |= 64U; reject_checked(malformed);
    malformed = bytes; malformed[38U] = 0U; malformed[39U] = 0U; reject_checked(malformed);
    malformed = bytes; malformed[37U] |= 32U; reject_checked(malformed); // loss reason requires lost
    const auto area_at = 36U + 12U + slot.name.size();
    const auto ids_at = area_at + 32U;
    malformed = bytes;
    std::fill_n(malformed.begin() + static_cast<std::ptrdiff_t>(ids_at), 4U, 0U); reject_checked(malformed);
    malformed = bytes;
    std::copy_n(malformed.begin() + static_cast<std::ptrdiff_t>(ids_at), 4U,
        malformed.begin() + static_cast<std::ptrdiff_t>(ids_at + 4U)); reject_checked(malformed);
    malformed = bytes;
    std::swap_ranges(malformed.begin() + static_cast<std::ptrdiff_t>(area_at),
        malformed.begin() + static_cast<std::ptrdiff_t>(area_at + 8U),
        malformed.begin() + static_cast<std::ptrdiff_t>(area_at + 16U)); reject_checked(malformed);
    auto defaults = parsed; defaults.slots.clear();
    assert(ReadLe16(SerializeFeedSlotRecords(defaults), 4U) == 2U); // never implicitly downgrade
    slot.lost = true; slot.schema_filter_lost = true;
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch).slots.front().schema_filter_lost);
    slot.lost = false; Reject([&] { (void)SerializeFeedSlotRecords(records); }); slot.lost = true;
    slot.filters.column_ids = std::vector<std::uint32_t>{0U}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids = std::vector<std::uint32_t>{1U, 1U}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids = std::vector<std::uint32_t>{}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids.reset(); slot.filters.area = FeedArea{{1, 0}, {0, 0}};
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
}

// Owners need version 3; files without an owner keep the version older
// readers know, and their slots read back without one.
void Owners() {
    StoreId epoch{}; epoch[0] = 5U;
    FeedSlotRecords records{epoch, 10U, {{"owned", 4U, false}, {"legacy", 5U, false}}};
    records.slots[0].owner = "alice";
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(ReadLe16(bytes, 4U) == 3U && ReadLe16(bytes, 6U) == 0U);
    const auto parsed = ParseFeedSlotRecords(bytes, epoch);
    assert(!parsed.filter_format && parsed.slots[0].owner == "alice" && !parsed.slots[1].owner);
    assert(parsed.slots[0].written == 4U && parsed.slots[1].written == 5U);
    for (std::size_t at = 0; at < bytes.size(); ++at) {
        auto partial = bytes; partial.resize(at);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
        auto corrupt = bytes; corrupt[at] ^= 1U;
        Reject([&] { (void)ParseFeedSlotRecords(corrupt, epoch); });
    }
    const auto reject_checked = [&](std::vector<std::uint8_t> changed) {
        const auto crc = changed.size() - 4U;
        changed.resize(crc); WriteLe32(changed, Crc32(changed.data(), changed.size()));
        Reject([&] { (void)ParseFeedSlotRecords(changed, epoch); });
    };
    auto malformed = bytes; malformed[6U] = 2U; reject_checked(malformed); // unknown header flag
    malformed = bytes; malformed[37U] |= 8U; reject_checked(malformed); // filter without the filter format
    malformed = bytes; malformed[36U + 12U + 5U] = 0U; reject_checked(malformed); // empty owner
    malformed = bytes; malformed[36U + 12U + 5U + 1U] = 'A'; reject_checked(malformed); // not a user name
    malformed = bytes; malformed[4U] = 2U; reject_checked(malformed); // owner flag before version 3
    malformed = bytes; malformed[4U] = 4U; reject_checked(malformed);

    records.slots[1].filters.no_before = true;
    const auto filtered = SerializeFeedSlotRecords(records);
    assert(ReadLe16(filtered, 4U) == 3U && ReadLe16(filtered, 6U) == 1U);
    const auto both = ParseFeedSlotRecords(filtered, epoch);
    assert(both.filter_format && both.slots[0].owner == "alice" && both.slots[1].filters.no_before);
    auto unowned = both; unowned.slots.erase(unowned.slots.begin());
    assert(ReadLe16(SerializeFeedSlotRecords(unowned), 4U) == 2U); // the filter format stays
    unowned = parsed; unowned.slots.erase(unowned.slots.begin());
    const auto legacy = SerializeFeedSlotRecords(unowned);
    assert(ReadLe16(legacy, 4U) == 1U && !ParseFeedSlotRecords(legacy, epoch).slots[0].owner);

    for (const auto owner : {"", "Alice", "1user", "a-b"}) {
        records.slots[0].owner = owner;
        Reject([&] { (void)SerializeFeedSlotRecords(records); });
        Reject([&] { RequireValidFeedSlotOwner(owner); });
    }
    records.slots[0].owner = std::string(64U, 'a'); Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.slots[0].owner = std::string(63U, 'a');
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch).slots[0].owner->size() == 63U);
}

}  // namespace

int main() { Records(); Filters(); Owners(); }
