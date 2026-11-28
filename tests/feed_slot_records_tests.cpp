#include <algorithm>
#include <cassert>
#include <functional>
#include <stdexcept>
#include <string>

#include "feed_slot_partitions.hpp"
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
    assert(ReadLe16(bytes, 4U) == 4U && bytes[6U] == 2U && bytes[7U] == 0U && !decoded.filter_format);
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
    slot.filters = {.areas = {FeedArea{{INT64_MIN, -3}, {INT64_MAX, 7}}},
        .column_ids = std::vector<std::uint32_t>{UINT32_MAX, 2U, 1U}, .no_before = true, .chunks = true};
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(ReadLe16(bytes, 4U) == 4U && bytes[6U] == 2U && bytes[7U] == 1U);
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
    auto malformed = bytes; malformed[37U] |= 128U | 1U; reject_checked(malformed); // resync on a lost slot
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
    slot.filters.column_ids.reset(); slot.filters.areas = {FeedArea{{1, 0}, {0, 0}}};
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
}

void Checksum(std::vector<std::uint8_t>& bytes) {
    assert(bytes.size() >= 4U);
    bytes.resize(bytes.size() - 4U);
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
}

// A slot with several areas sets header flag 0x2, and each slot with areas
// then gives their count; with at most one area per slot the layout is the
// one without counts. Areas are ordered and distinct, at most 64.
void SeveralAreas() {
    StoreId epoch{}; epoch[0] = 5U;
    FeedSlotRecords records{epoch, 9U, {{"one", 9U, false}, {"several", 9U, false}, {"whole", 9U, false}}, true};
    records.slots[0].filters.areas = {FeedArea{{5, 5}, {6, 6}}};
    records.slots[1].filters.areas = {FeedArea{{-4, -4}, {-3, -3}}, FeedArea{{0, 0}, {1, 1}}, FeedArea{{0, 0}, {2, 2}}};
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(bytes[7U] == 3U);
    assert(bytes.size() == 40U + (12U + 3U + 1U + 32U) + (12U + 7U + 1U + 3U * 32U) + (12U + 5U));
    const auto parsed = ParseFeedSlotRecords(bytes, epoch, 2U);
    for (std::size_t i = 0U; i < records.slots.size(); ++i) assert(parsed.slots[i].filters == records.slots[i].filters);
    for (std::size_t at = 0; at < bytes.size(); ++at) {
        auto partial = bytes; partial.resize(at);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
    }
    const auto count_at = 36U + 12U + 3U;
    assert(bytes[count_at] == 1U && bytes[count_at + 1U + 32U + 12U + 7U] == 3U);
    for (const auto count : {0U, 2U, 65U}) {
        auto bad = bytes; bad[count_at] = static_cast<std::uint8_t>(count); Checksum(bad);
        Reject([&] { (void)ParseFeedSlotRecords(bad, epoch); });
    }
    auto bad = bytes; bad[7U] = 2U; Checksum(bad);
    Reject([&] { (void)ParseFeedSlotRecords(bad, epoch); }); // Counts belong to the filter format.
    // Swapped areas are out of order.
    bad = bytes;
    const auto several_at = count_at + 1U + 32U + 12U + 7U + 1U;
    std::swap_ranges(bad.begin() + static_cast<std::ptrdiff_t>(several_at), bad.begin() + static_cast<std::ptrdiff_t>(several_at + 32U),
        bad.begin() + static_cast<std::ptrdiff_t>(several_at + 32U));
    Checksum(bad);
    Reject([&] { (void)ParseFeedSlotRecords(bad, epoch); });
    auto& several = records.slots[1].filters.areas;
    std::swap(several[0], several[1]);
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    several[0] = several[1];
    Reject([&] { (void)SerializeFeedSlotRecords(records); }); // Repeated.
    several.clear();
    for (std::int64_t i = 0; i < 65; ++i) several.push_back(FeedArea{{i, i}, {i, i}});
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    several.pop_back();
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch).slots[1].filters.areas.size() == 64U);
    several.resize(1U);
    const auto single = SerializeFeedSlotRecords(records);
    assert(single[7U] == 1U && single.size() == 40U + (12U + 3U + 32U) + (12U + 7U + 32U) + (12U + 5U));
    assert(ParseFeedSlotRecords(single, epoch).slots[1].filters == records.slots[1].filters);
}

void Dimensions() {
    StoreId epoch{}; epoch[0] = 4U;
    FeedSlotRecords records{epoch, UINT64_MAX, {{"space", UINT64_MAX, true}}, true, 3U};
    auto& slot = records.slots.front();
    slot.filters = {.areas = {FeedArea{{INT64_MIN, -9, INT64_MIN}, {INT64_MAX, 9, INT64_MAX}, 3U}},
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
    bad = bytes; bad[7U] |= 8U; reject_checked(bad);
    const auto area = 36U + 12U + slot.name.size();
    bad = bytes;
    std::swap_ranges(bad.begin() + static_cast<std::ptrdiff_t>(area + 16U),
        bad.begin() + static_cast<std::ptrdiff_t>(area + 24U),
        bad.begin() + static_cast<std::ptrdiff_t>(area + 40U));
    reject_checked(bad); // Z must be ordered independently of X and Y.
    for (const auto tag : {1U, 2U, 3U}) {
        bad = bytes; bad[4U] = static_cast<std::uint8_t>(tag); Checksum(bad);
        bool earlier = false;
        try { (void)ParseFeedSlotRecords(bad, epoch); }
        catch (const EarlierDevelopmentFormatError&) { earlier = true; }
        assert(earlier);
    }
    slot.filters.areas[0].first.z = 0; slot.filters.areas[0].last.z = 0;
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch, 3U).slots.front().filters == slot.filters);
    slot.filters.areas[0].dims = 2U;
    Reject([&] { (void)SerializeFeedSlotRecords(records); }); // Zero Z still has explicit arity.
    records.dims = 2U;
    slot.filters.areas[0].first.z = 1;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.areas[0].first.z = 0; slot.filters.areas[0].last.z = 1;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.areas[0].dims = 3U;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.dims = 3U;
    slot.filters.areas[0].first.z = 2;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    slot.filters.areas.clear();
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

// Version 4 records an optional owner for each slot next to the dimensionality
// and the filter format; a slot without an owner reads back without one.
void Owners() {
    StoreId epoch{}; epoch[0] = 6U;
    FeedSlotRecords records{epoch, 10U, {{"owned", 4U, false}, {"unowned", 5U, false}}};
    records.slots[0].owner = "alice";
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(ReadLe16(bytes, 4U) == 4U && bytes[6U] == 2U && bytes[7U] == 0U);
    const auto parsed = ParseFeedSlotRecords(bytes, epoch, 2U);
    assert(!parsed.filter_format && parsed.slots[0].owner == "alice" && !parsed.slots[1].owner);
    assert(parsed.slots[0].written == 4U && parsed.slots[1].written == 5U);
    for (std::size_t at = 0; at < bytes.size(); ++at) {
        auto partial = bytes; partial.resize(at);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
        auto corrupt = bytes; corrupt[at] ^= 1U;
        Reject([&] { (void)ParseFeedSlotRecords(corrupt, epoch); });
    }
    const auto reject_checked = [&](std::vector<std::uint8_t> changed) {
        Checksum(changed); Reject([&] { (void)ParseFeedSlotRecords(changed, epoch); });
    };
    const auto owner_at = 36U + 12U + records.slots[0].name.size();
    auto malformed = bytes; malformed[7U] = 8U; reject_checked(malformed); // unknown header flag
    malformed = bytes; malformed[7U] = 4U; reject_checked(malformed); // partition format without partition counts
    malformed = bytes; malformed[37U] |= 8U; reject_checked(malformed); // filter without the filter format
    malformed = bytes; malformed[37U] |= 128U | 1U; reject_checked(malformed); // resync on a lost slot
    malformed = bytes; malformed[owner_at] = 0U; reject_checked(malformed); // empty owner
    malformed = bytes; malformed[owner_at + 1U] = 'A'; reject_checked(malformed); // not a user name
    malformed = bytes; malformed[owner_at] = 64U; reject_checked(malformed); // owner past the record
    malformed = bytes; malformed[4U] = 5U; reject_checked(malformed);
    for (const auto tag : {1U, 2U, 3U}) {
        malformed = bytes; malformed[4U] = static_cast<std::uint8_t>(tag); Checksum(malformed);
        bool earlier = false;
        try { (void)ParseFeedSlotRecords(malformed, epoch); }
        catch (const EarlierDevelopmentFormatError&) { earlier = true; }
        assert(earlier);
    }

    records.slots[1].filters.no_before = true;
    const auto filtered = SerializeFeedSlotRecords(records);
    assert(ReadLe16(filtered, 4U) == 4U && filtered[7U] == 1U);
    const auto both = ParseFeedSlotRecords(filtered, epoch);
    assert(both.filter_format && both.slots[0].owner == "alice" && both.slots[1].filters.no_before);
    // A slot a restore or swap moved reads back as needing resync.
    records.slots[0].resync = true;
    const auto moved = SerializeFeedSlotRecords(records);
    assert((moved[37U] & 128U) != 0U);
    const auto resyncing = ParseFeedSlotRecords(moved, epoch);
    assert(resyncing.slots[0].resync && !resyncing.slots[1].resync && resyncing.slots[0].owner == "alice");
    records.slots[0].lost = true;
    Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.slots[0].lost = false; records.slots[0].resync = false;
    auto unowned = parsed; unowned.slots.erase(unowned.slots.begin());
    const auto plain = SerializeFeedSlotRecords(unowned);
    assert(ReadLe16(plain, 4U) == 4U && !ParseFeedSlotRecords(plain, epoch).slots[0].owner);

    FeedSlotRecords space{epoch, 3U, {{"space", 3U, false}}, true, 3U};
    space.slots[0].owner = "bob";
    space.slots[0].filters.areas = {FeedArea{{-1, -2, -3}, {1, 2, 3}, 3U}};
    space.slots[0].filters.column_ids = std::vector<std::uint32_t>{2U};
    const auto spatial = SerializeFeedSlotRecords(space);
    assert(spatial.size() == 40U + 12U + 5U + 48U + 4U + 1U + 3U);
    const auto spatial_parsed = ParseFeedSlotRecords(spatial, epoch, 3U);
    assert(spatial_parsed.slots[0].owner == "bob" && spatial_parsed.slots[0].filters == space.slots[0].filters);
    for (std::size_t size = 0U; size < spatial.size(); ++size) {
        auto truncated = spatial; truncated.resize(size);
        Reject([&] { (void)ParseFeedSlotRecords(truncated, epoch, 3U); });
    }

    for (const auto owner : {"", "Alice", "1user", "a-b"}) {
        records.slots[0].owner = owner;
        Reject([&] { (void)SerializeFeedSlotRecords(records); });
        Reject([&] { RequireValidFeedSlotOwner(owner); });
    }
    records.slots[0].owner = std::string(64U, 'a'); Reject([&] { (void)SerializeFeedSlotRecords(records); });
    records.slots[0].owner = std::string(63U, 'a');
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(records), epoch).slots[0].owner->size() == 63U);

    // Owner bytes count toward the record size limit.
    FeedSlotRecords large{epoch, 0U, {}};
    for (std::size_t i = 0U; i < 1024U; ++i) {
        FeedSlotRecord slot{"slot_" + std::to_string(i), 0U, false};
        slot.filters.column_ids.emplace();
        for (std::uint32_t id = 1U; id <= 239U; ++id) slot.filters.column_ids->push_back(id);
        large.slots.push_back(std::move(slot));
    }
    const auto fits = SerializeFeedSlotRecords(large);
    assert(fits.size() <= 1024U * 1024U);
    const auto room = 1024U * 1024U - fits.size();
    std::size_t owned = 0U;
    for (auto& slot : large.slots) {
        if (owned > room) break;
        slot.owner = std::string(63U, 'o');
        owned += 64U;
    }
    Reject([&] { (void)SerializeFeedSlotRecords(large); });
}

// Header flag 0x4 gives every slot its partition count; a partitioned slot's
// position and resync are the lowest and any of its partitions'.
void Partitions() {
    StoreId epoch{}; epoch[0] = 7U;
    FeedSlotRecords records{epoch, 20U, {{"parts", 5U, false}, {"single", 9U, false}}};
    records.slots[0].partitions = {{5U, false}, {20U, true}, {7U, false}};
    records.slots[0].resync = true;
    const auto bytes = SerializeFeedSlotRecords(records);
    assert(bytes[7U] == 4U && bytes.size() == 40U + (12U + 5U + 2U + 27U) + (12U + 6U + 2U));
    const auto parsed = ParseFeedSlotRecords(bytes, epoch, 2U);
    assert(parsed.partition_format && !parsed.filter_format);
    assert(parsed.slots[0].partitions == records.slots[0].partitions && parsed.slots[0].resync && parsed.slots[0].written == 5U);
    assert(parsed.slots[1].partitions.empty() && parsed.slots[1].written == 9U && !parsed.slots[1].resync);
    for (std::size_t at = 0; at < bytes.size(); ++at) {
        auto partial = bytes; partial.resize(at);
        Reject([&] { (void)ParseFeedSlotRecords(partial, epoch); });
        auto corrupt = bytes; corrupt[at] ^= 1U;
        Reject([&] { (void)ParseFeedSlotRecords(corrupt, epoch); });
    }
    const auto reject_checked = [&](std::vector<std::uint8_t> changed) {
        Checksum(changed); Reject([&] { (void)ParseFeedSlotRecords(changed, epoch); });
    };
    const auto count_at = 36U + 12U + records.slots[0].name.size();
    assert(ReadLe16(bytes, count_at) == 3U);
    auto malformed = bytes; malformed[count_at] = 0U; reject_checked(malformed); // no partitions
    malformed = bytes; malformed[count_at + 2U + 8U] = 2U; reject_checked(malformed); // unknown partition flag
    malformed = bytes; malformed[count_at + 2U] = 6U; reject_checked(malformed); // position is not the lowest
    malformed = bytes; malformed[count_at + 2U + 9U + 8U] = 0U; reject_checked(malformed); // resync no partition has
    malformed = bytes; malformed[count_at + 2U + 9U + 1U] = 1U; reject_checked(malformed); // above the watermark
    malformed = bytes; malformed[7U] = 0U; reject_checked(malformed); // counts without the partition format
    const auto refused = [&](const std::function<void(FeedSlotRecord&)>& change) {
        auto bad = records; change(bad.slots[0]);
        Reject([&] { (void)SerializeFeedSlotRecords(bad); });
    };
    refused([](auto& slot) { slot.written = 6U; });
    refused([](auto& slot) { slot.resync = false; });
    refused([](auto& slot) { slot.partitions[1].written = 21U; });
    refused([](auto& slot) { slot.partitions.resize(1U); slot.resync = false; });
    refused([](auto& slot) { slot.partitions.assign(257U, {5U, false}); slot.resync = false; });
    auto most = records; most.slots[0].partitions.assign(256U, {5U, false}); most.slots[0].resync = false;
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(most), epoch).slots[0].partitions.size() == 256U);
    // The partition format stays once written.
    auto plain = parsed; plain.slots.erase(plain.slots.begin());
    const auto kept = SerializeFeedSlotRecords(plain);
    assert(kept[7U] == 4U && ParseFeedSlotRecords(kept, epoch).partition_format);
    FeedSlotRecords unpartitioned{epoch, 20U, {{"single", 9U, false}}};
    assert(SerializeFeedSlotRecords(unpartitioned)[7U] == 0U);

    auto slot = records.slots[0];
    assert(AdvanceFeedSlotRecord(slot, {0U, 2U}, 8U) && slot.written == 8U && slot.resync);
    assert(!AdvanceFeedSlotRecord(slot, {1U}, 20U));
    assert(AdvanceFeedSlotRecord(slot, {}, 9U) && slot.written == 9U && slot.resync && slot.partitions[1].written == 20U);
    MarkFeedSlotRecordLost(slot);
    assert(slot.lost && !slot.resync && std::none_of(slot.partitions.begin(), slot.partitions.end(), [](const auto& p) { return p.resync; }));
    slot.lost = false; MarkFeedSlotRecordResync(slot);
    assert(slot.resync && std::all_of(slot.partitions.begin(), slot.partitions.end(), [](const auto& p) { return p.resync; }));
    LowerFeedSlotRecordPositions(slot, 9U);
    assert(slot.written == 9U && slot.partitions[1].written == 9U);
    SetFeedSlotRecordPosition(slot, 3U);
    assert(slot.written == 3U && std::all_of(slot.partitions.begin(), slot.partitions.end(), [](const auto& p) { return p.written == 3U; }));
    assert(AdvanceFeedSlotRecord(slot, {}, 4U, false) && slot.written == 4U && slot.resync);
    assert(AdvanceFeedSlotRecord(slot, {2U}, 5U) && slot.written == 4U && slot.resync && !slot.partitions[2].resync);
    FeedSlotRecords updated{epoch, 20U, {slot}};
    assert(ParseFeedSlotRecords(SerializeFeedSlotRecords(updated), epoch).slots[0].partitions == slot.partitions);

    // CREATE SLOT checks the whole file: slots of 256 partitions fill 1 MiB at about 450.
    FeedSlotRecords full{epoch, 0U, {}};
    for (std::size_t i = 0U;; ++i) {
        FeedSlotRecord wide{"wide_" + std::to_string(i), 0U, false};
        wide.partitions.assign(256U, {0U, false});
        bool room = true;
        try { RequireFeedSlotRoom(full, wide); } catch (const std::out_of_range&) { room = false; }
        if (!room) break;
        full.slots.push_back(std::move(wide));
    }
    assert(full.slots.size() > 400U && full.slots.size() < 460U);
    assert(SerializeFeedSlotRecords(full).size() <= 1024U * 1024U);
    RequireFeedSlotRoom(full, FeedSlotRecord{"narrow", 0U, false});
    FeedSlotRecords many{epoch, 0U, {}};
    many.slots.assign(1024U, FeedSlotRecord{"a", 0U, false});
    bool counted = false;
    try { RequireFeedSlotRoom(many, FeedSlotRecord{"b", 0U, false}); } catch (const std::out_of_range&) { counted = true; }
    assert(counted);

    // The partition of a chunk is fixed by the storage format.
    assert(FeedSlotPartitionOf({0, 0, 0}, 256U) == 145U && FeedSlotPartitionOf({1, 0, 0}, 256U) == 195U);
    assert(FeedSlotPartitionOf({-1, -1, 0}, 256U) == 10U && FeedSlotPartitionOf({5, 7, 3}, 7U) == 0U);
    assert(FeedSlotPartitionOf({5, 7, 3}, 4U) == 3U && FeedSlotPartitionOf({5, 7, 3}, 1U) == 0U);
    assert(NormalizeFeedSlotPartitions({3U, 1U}, 4U) == (std::vector<std::uint16_t>{1U, 3U}));
    assert(NormalizeFeedSlotPartitions({0U, 2U, 1U}, 3U).empty() && NormalizeFeedSlotPartitions({0U}, 1U).empty());
    Reject([] { (void)NormalizeFeedSlotPartitions({1U, 1U}, 4U); });
    Reject([] { (void)NormalizeFeedSlotPartitions({4U}, 4U); });
    assert(FeedSlotPartitionsOverlap({}, {2U}) && FeedSlotPartitionsOverlap({0U, 2U}, {2U}));
    assert(!FeedSlotPartitionsOverlap({0U, 2U}, {1U, 3U}));

    FeedSlot view_slot{"parts", {epoch, 5U}, 20U};
    view_slot.partitions = {{5U, false}, {20U, true}, {7U, false}};
    view_slot.resync = true;
    const auto low = FeedSlotPartitionView(view_slot, {0U, 2U});
    assert(low.position.revision == 5U && !low.resync);
    const auto high = FeedSlotPartitionView(view_slot, {1U});
    assert(high.position.revision == 20U && high.resync);
    assert(FeedSlotPartitionFloors(view_slot, {1U, 2U}) == (std::vector<std::uint64_t>{UINT64_MAX, 20U, 7U}));
    assert(FeedSlotPartitionFloors(view_slot, {}) == (std::vector<std::uint64_t>{5U, 20U, 7U}));

    // With three partitions, chunks (0, 0), (3, 0) and (2, 0) are in partitions 0, 1 and 2.
    assert(FeedSlotPartitionOf({0, 0, 0}, 3U) == 0U && FeedSlotPartitionOf({3, 0, 0}, 3U) == 1U && FeedSlotPartitionOf({2, 0, 0}, 3U) == 2U);
    FeedEntry change;
    change.kind = FeedEntry::Kind::kChange; change.position = {epoch, 10U};
    for (const std::int64_t x : {0, 3, 2}) {
        FeedBlockChange block;
        block.chunk = {x, 0, 0};
        change.blocks.push_back(block);
    }
    change.protocol_frame = std::make_shared<const std::string>("frame");
    const auto original = std::make_shared<const FeedEntry>(change);
    const auto kept_change = PartitionFeedEntry(original, {5U, UINT64_MAX, 10U});
    assert(kept_change && kept_change != original && kept_change->blocks.size() == 1U);
    assert(kept_change->blocks[0].chunk.x == 0 && !kept_change->protocol_frame && kept_change->position.revision == 10U);
    assert(PartitionFeedEntry(original, {5U, 5U, 5U}) == original && PartitionFeedEntry(original, {}) == original);
    assert(!PartitionFeedEntry(original, {10U, UINT64_MAX, 10U}));
    FeedEntry chunks;
    chunks.kind = FeedEntry::Kind::kChunks; chunks.position = {epoch, 10U};
    chunks.chunk_forms.emplace_back().chunk = {3, 0, 0};
    chunks.chunk_changes.emplace_back().chunk = {2, 0, 0};
    const auto forms = PartitionFeedEntry(std::make_shared<const FeedEntry>(chunks), {0U, 0U, UINT64_MAX});
    assert(forms && forms->chunk_forms.size() == 1U && forms->chunk_changes.empty());
    FeedEntry empty;
    empty.kind = FeedEntry::Kind::kChange; empty.position = {epoch, 10U};
    assert(!PartitionFeedEntry(std::make_shared<const FeedEntry>(empty), {0U, 0U, 0U}));
    FeedEntry progress;
    progress.kind = FeedEntry::Kind::kProgress; progress.position = {epoch, 10U};
    const auto pushed = std::make_shared<const FeedEntry>(progress);
    assert(PartitionFeedEntry(pushed, {UINT64_MAX, UINT64_MAX, UINT64_MAX}) == pushed);
}

void PartitionedAreaRecords() {
    StoreId epoch{}; epoch[0] = 8U;
    FeedSlotRecords records{epoch, 30U, {{"workers", 5U, false}, {"single", 10U, false}}};
    auto& slot = records.slots.front();
    slot.filters = {.areas = {FeedArea{{-4, -2}, {-1, 0}}, FeedArea{{7, 1}, {9, 5}}},
        .column_ids = std::vector<std::uint32_t>{7U, 1U}, .no_before = true};
    slot.owner = "bot";
    slot.partitions = {{5U, false}, {22U, true}};
    slot.resync = true;
    records.slots[1].filters.areas = {FeedArea{{0, 0}, {1, 1}}};

    const auto bytes = SerializeFeedSlotRecords(records);
    assert(bytes[7U] == 0x7U);
    const auto areas_at = 36U + 12U + slot.name.size();
    const auto projection_at = areas_at + 1U + 2U * 32U;
    const auto owner_at = projection_at + 2U * 4U;
    const auto partitions_at = owner_at + 1U + slot.owner->size();
    assert(bytes[areas_at] == 2U && ReadLe32(bytes, projection_at) == 7U);
    assert(bytes[owner_at] == 3U && bytes[owner_at + 1U] == 'b');
    assert(ReadLe16(bytes, partitions_at) == 2U);
    assert(ReadLe64(bytes, partitions_at + 2U) == 5U && bytes[partitions_at + 10U] == 0U);
    assert(ReadLe64(bytes, partitions_at + 11U) == 22U && bytes[partitions_at + 19U] == 1U);
    const auto single_areas_at = partitions_at + 2U + 2U * 9U + 12U + records.slots[1].name.size();
    assert(bytes[single_areas_at] == 1U && ReadLe16(bytes, single_areas_at + 1U + 32U) == 1U);
    assert(bytes.size() == single_areas_at + 1U + 32U + 2U + 4U);

    const auto parsed = ParseFeedSlotRecords(bytes, epoch, 2U);
    assert(parsed.filter_format && parsed.partition_format && parsed.slots.size() == 2U);
    assert(parsed.slots[0].filters == slot.filters && parsed.slots[0].owner == slot.owner);
    assert(parsed.slots[0].written == 5U && parsed.slots[0].resync && parsed.slots[0].partitions == slot.partitions);
    assert(parsed.slots[1].filters == records.slots[1].filters && parsed.slots[1].partitions.empty());
    assert(SerializeFeedSlotRecords(parsed) == bytes);

    const auto check = [&](std::size_t offset, std::uint8_t value) {
        auto malformed = bytes;
        malformed[offset] = value;
        Checksum(malformed);
        Reject([&] { (void)ParseFeedSlotRecords(malformed, epoch); });
    };
    check(7U, 0x6U);                 // Area counts require the filter flag.
    check(7U, 0xfU);                 // Unknown header flag.
    check(areas_at, 0U);
    check(areas_at, 65U);
    check(partitions_at, 0U);
    check(partitions_at + 1U, 1U);   // 258 partitions.
    auto truncated = bytes;
    truncated.resize(partitions_at + 2U + 9U + 4U); // One of two partition entries, plus CRC.
    Checksum(truncated);
    Reject([&] { (void)ParseFeedSlotRecords(truncated, epoch); });
}

}  // namespace

int main() { Records(); Filters(); SeveralAreas(); Dimensions(); AdmissionBounds(); Owners(); Partitions(); PartitionedAreaRecords(); }
