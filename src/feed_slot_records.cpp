#include "feed_slot_records.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <stdexcept>

#include "chunk_store_internal.hpp"
#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {
constexpr std::array<std::uint8_t, 4> kMagic{'C', 'K', 'S', 'L'};
constexpr std::size_t kFixedBytes = 4U + 2U + 2U + 16U + 8U + 4U + 4U;
constexpr std::size_t kMaxRecordsBytes = 1024U * 1024U;
constexpr std::size_t kMaxSlots = 1024U;

[[noreturn]] void BadRecords(const std::string& why) {
    throw std::runtime_error("invalid chunkdb.slots: " + why);
}
void ValidateRecords(const FeedSlotRecords& records) {
    if (std::all_of(records.epoch.begin(), records.epoch.end(), [](auto b) { return b == 0U; }))
        BadRecords("zero epoch");
    if (records.slots.size() > kMaxSlots) BadRecords("too many slot records");
    std::set<std::string> names;
    for (const auto& slot : records.slots) {
        RequireValidFeedSlotName(slot.name);
        if (!names.insert(slot.name).second) BadRecords("duplicate name");
        if (slot.written > records.durable_watermark) BadRecords("position above durable watermark");
    }
}
}  // namespace

void RequireValidFeedSlotName(std::string_view name) {
    if (name.empty() || name.size() > 63U) throw std::invalid_argument("slot names are 1 to 63 bytes: [a-z_][a-z0-9_]*");
    for (std::size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        if (c == '_' || (c >= 'a' && c <= 'z') || (i > 0U && c >= '0' && c <= '9')) continue;
        throw std::invalid_argument("slot names are 1 to 63 bytes: [a-z_][a-z0-9_]*");
    }
}

std::vector<std::uint8_t> SerializeFeedSlotRecords(const FeedSlotRecords& records) {
    ValidateRecords(records);
    std::vector<std::uint8_t> bytes(kMagic.begin(), kMagic.end());
    WriteLe16(bytes, 1U);
    WriteLe16(bytes, 0U);
    bytes.insert(bytes.end(), records.epoch.begin(), records.epoch.end());
    WriteLe64(bytes, records.durable_watermark);
    WriteLe32(bytes, static_cast<std::uint32_t>(records.slots.size()));
    for (const auto& slot : records.slots) {
        bytes.push_back(static_cast<std::uint8_t>(slot.name.size()));
        bytes.push_back(slot.lost ? 1U : 0U);
        WriteLe16(bytes, 0U);
        WriteLe64(bytes, slot.written);
        bytes.insert(bytes.end(), slot.name.begin(), slot.name.end());
    }
    WriteLe32(bytes, Crc32(bytes.data(), bytes.size()));
    return bytes;
}

FeedSlotRecords ParseFeedSlotRecords(const std::vector<std::uint8_t>& bytes, const StoreId& epoch) {
    if (bytes.size() < kFixedBytes || bytes.size() > kMaxRecordsBytes) BadRecords("size");
    if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) || ReadLe16(bytes, 4U) != 1U || ReadLe16(bytes, 6U) != 0U)
        BadRecords("magic, version or reserved field");
    const auto crc_at = bytes.size() - 4U;
    if (ReadLe32(bytes, crc_at) != Crc32(bytes.data(), crc_at)) BadRecords("checksum");
    FeedSlotRecords records;
    std::copy_n(bytes.begin() + 8, records.epoch.size(), records.epoch.begin());
    if (records.epoch != epoch) BadRecords("epoch differs from table");
    records.durable_watermark = ReadLe64(bytes, 24U);
    const auto count = ReadLe32(bytes, 32U);
    if (count > kMaxSlots) BadRecords("too many slot records");
    std::size_t at = 36U;
    for (std::uint32_t i = 0; i < count; ++i) {
        if (crc_at - at < 12U) BadRecords("truncated slot record");
        const auto length = bytes[at];
        const auto flags = bytes[at + 1U];
        if (flags > 1U || ReadLe16(bytes, at + 2U) != 0U) BadRecords("slot flags or reserved field");
        if (crc_at - at - 12U < length) BadRecords("truncated slot name");
        records.slots.push_back(FeedSlotRecord{
            .name = std::string(bytes.begin() + static_cast<std::ptrdiff_t>(at + 12U),
                                bytes.begin() + static_cast<std::ptrdiff_t>(at + 12U + length)),
            .written = ReadLe64(bytes, at + 4U),
            .lost = flags != 0U,
        });
        at += 12U + length;
    }
    if (at != crc_at) BadRecords("trailing bytes");
    ValidateRecords(records);
    return records;
}

std::optional<FeedSlotRecords> ReadFeedSlotRecords(const std::filesystem::path& dir, const StoreId& epoch) {
    const auto path = dir / kFeedSlotsFileName;
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        if (ec) throw std::runtime_error("cannot inspect " + path.string() + ": " + ec.message());
        return std::nullopt;
    }
    const auto size = std::filesystem::file_size(path, ec);
    if (ec) throw std::runtime_error("cannot size " + path.string() + ": " + ec.message());
    if (size > kMaxRecordsBytes) BadRecords("size");
    return ParseFeedSlotRecords(LoadFile(path), epoch);
}

void WriteFeedSlotRecords(const std::filesystem::path& dir, const FeedSlotRecords& records, bool* published) {
    AtomicWrite(dir / kFeedSlotsFileName, SerializeFeedSlotRecords(records), true, true, published);
}

}  // namespace chunkdb
