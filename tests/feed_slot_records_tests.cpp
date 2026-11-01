#include <cassert>
#include <functional>

#include "feed_slot_records.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "test_utils.hpp"
#include "store_manifest.hpp"

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
    assert(ReadLe16(bytes, 4U) == 3U && bytes[6U] == 2U && bytes[7U] == 0U && !decoded.filter_format);
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
    assert(ReadLe16(bytes, 4U) == 3U && bytes[6U] == 2U && bytes[7U] == 1U);
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
    assert(SerializeFeedSlotRecords(defaults)[7U] == 1U); // Preserve the filtered-record capability.
    slot.lost = true; slot.schema_filter_lost = true;
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch).slots.front().schema_filter_lost);
    slot.lost = false; Reject([&] { (void)SerializeFeedSlotRecords(records); }); slot.lost = true;
    slot.filters.column_ids = std::vector<std::uint32_t>{0U}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids = std::vector<std::uint32_t>{1U, 1U}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids = std::vector<std::uint32_t>{}; Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.column_ids.reset(); slot.filters.area = FeedArea{{1, 0}, {0, 0}};
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
}

void Checksum(std::vector<std::uint8_t>& bytes) {
    assert(bytes.size() >= 4U);
    bytes.resize(bytes.size() - 4U);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
}

void Dimensions() {
    StoreId epoch{}; epoch[0] = 4U;
    FeedSlotRecords records{epoch, UINT64_MAX, {{"space", UINT64_MAX, true}}, true, 3U};
    auto& slot = records.slots.front();
    slot.filters = {.area = FeedArea{{INT64_MIN, -9, INT64_MIN}, {INT64_MAX, 9, INT64_MAX}, 3U},
        .column_ids = std::vector<std::uint32_t>{7U, 1U}, .no_before = true, .chunks = true};
    slot.schema_filter_lost = true;
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(bytes.size() == 40U + 12U + slot.name.size() + 48U + 8U);
    assert(bytes[6U] == 3U && bytes[7U] == 1U);
    const auto parsed = ParseFeedSlotRecords(bytes, epoch, 3U);
    assert(parsed.dims == 3U && parsed.filter_format);
    assert(parsed.slots.front().filters == slot.filters && parsed.slots.front().schema_filter_lost);
    assert(ParseFeedSlotRecords(bytes, epoch).slots.front().filters == slot.filters);
    Reject([&] { (void)ParseFeedSlotRecords(bytes, epoch, 2U); });
    Reject([&] { (void)ParseFeedSlotRecords(bytes, epoch, 4U); });
    for (std::size_t size = 0U; size < bytes.size(); ++size) {
        auto truncated = bytes; truncated.resize(size);
        Reject([&] { (void)ParseFeedSlotRecords(truncated, epoch, 3U); });
    }
    const auto reject_checked = [&](std::vector<std::uint8_t> bad) {
        Checksum(bad); Reject([&] { (void)ParseFeedSlotRecords(bad, epoch, 3U); });
    };
    for (const auto dims : {0U, 1U, 4U, 255U}) {
        auto bad = bytes; bad[6U] = static_cast<std::uint8_t>(dims); reject_checked(bad);
    }
    auto bad = bytes; bad[6U] = 2U; Checksum(bad);
    Reject([&] { (void)ParseFeedSlotRecords(bad, epoch); }); // Extra active-axis bytes cannot become a 2D filter.
    bad = bytes; bad[7U] = 0U; reject_checked(bad);
    bad = bytes; bad[7U] |= 2U; reject_checked(bad);
    const auto area = 36U + 12U + slot.name.size();
    bad = bytes;
    std::swap_ranges(bad.begin() + static_cast<std::ptrdiff_t>(area + 16U),
        bad.begin() + static_cast<std::ptrdiff_t>(area + 24U),
        bad.begin() + static_cast<std::ptrdiff_t>(area + 40U));
    reject_checked(bad); // Z must be ordered independently of X and Y.
    for (const auto tag : {1U, 2U}) {
        bad = bytes; bad[4U] = static_cast<std::uint8_t>(tag); Checksum(bad);
        bool earlier = false;
        try { (void)ParseFeedSlotRecords(bad, epoch); }
        catch (const EarlierDevelopmentFormatError&) { earlier = true; }
        assert(earlier);
    }
    slot.filters.area->first.z = 0; slot.filters.area->last.z = 0;
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch, 3U).slots.front().filters == slot.filters);
    slot.filters.area->dims = 2U;
    Reject([&] { (void)SerializeFeedSlotRecords(records); }); // Zero Z still has explicit arity.
    records.dims = 2U;
    slot.filters.area->first.z = 1;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.area->first.z = 0; slot.filters.area->last.z = 1;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.area->dims = 3U;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.dims = 3U;
    slot.filters.area->first.z = 2;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.area.reset();
    const auto no_area = SerializeFeedSlotRecords(records);
    assert(ParseFeedSlotRecords(no_area, epoch, 3U).dims == 3U);
    Reject([&] { (void)ParseFeedSlotRecords(no_area, epoch, 2U); });
    test::ScopedTempDir directory("chunkdb-dimensional-slot-records");
    WriteFeedSlotRecords(directory.path(), records);
    assert(ReadFeedSlotRecords(directory.path(), epoch, 3U)->dims == 3U);
    Reject([&] { (void)ReadFeedSlotRecords(directory.path(), epoch, 2U); });
    const auto original = LoadFile(directory.path() / kFeedSlotsFileName);
    records.dims = 1U;
    Reject([&] { WriteFeedSlotRecords(directory.path(), records); });
    assert(LoadFile(directory.path() / kFeedSlotsFileName) == original);
}

void AdmissionBounds() {
    StoreId epoch{}; epoch[0] = 5U;
    FeedSlotRecords records{epoch, 0U, {}};
    auto bytes = SerializeFeedSlotRecords(records);
    for (std::size_t at = 32U; at < 36U; ++at) bytes[at] = 0xffU;
    Checksum(bytes);
    Reject([&] { (void)ParseFeedSlotRecords(bytes, epoch); });
    bytes.resize(1024U * 1024U + 1U, 0U);
    Reject([&] { (void)ParseFeedSlotRecords(bytes, epoch); });
    for (std::size_t i = 0U; i < 1024U; ++i) {
        FeedSlotRecord slot{"slot_" + std::to_string(i), 0U, false};
        slot.filters.column_ids.emplace();
        for (std::uint32_t id = 1U; id <= 1024U; ++id) slot.filters.column_ids->push_back(id);
        records.slots.push_back(std::move(slot));
    }
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.slots.assign(1025U, {"a", 0U, false});
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
}

}  // namespace

int main() { Records(); Filters(); Dimensions(); AdmissionBounds(); }
