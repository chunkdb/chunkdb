#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/feed_slots.hpp"

namespace chunkdb {

inline constexpr std::string_view kFeedSlotsFileName = "chunkdb.slots";
inline constexpr std::string_view kFeedArchiveDirName = ".chunkdb.feed";

struct FeedSlotRecord {
    std::string name;
    std::uint64_t written = 0;
    // A limit loss survives restart so the later streaming layer can distinguish
    // SLOT_LOST from an unknown name. Explicit drop removes the record.
    bool lost = false;
    FeedSlotFilters filters{};
    bool schema_filter_lost = false;
    // The user name the slot belongs to. A slot created without authentication
    // has none; such a slot takes the user of its first granted attach.
    std::optional<std::string> owner{};
    // A restore or SWAP TABLES moved the slot to other data or another name:
    // its next watch starts with resync, until an acknowledgement advances it.
    bool resync = false;
    // Empty without partitions; otherwise 2 to 256 entries, `written` is the
    // lowest of their positions and `resync` is set when any of them is.
    std::vector<FeedSlotPartition> partitions{};
};

struct FeedSlotRecords {
    StoreId epoch{};
    std::uint64_t durable_watermark = 0;
    std::vector<FeedSlotRecord> slots;
    bool filter_format = false;
    std::uint8_t dims = 2U;
    // Every slot record carries its partition count; requires slot_partitions.
    bool partition_format = false;
};

void RequireValidFeedSlotName(std::string_view name);
// Owners are user names; the user need not exist.
void RequireValidFeedSlotOwner(std::string_view owner);
[[nodiscard]] std::vector<std::uint8_t> SerializeFeedSlotRecords(const FeedSlotRecords& records);
[[nodiscard]] FeedSlotRecords ParseFeedSlotRecords(const std::vector<std::uint8_t>& bytes, const StoreId& epoch);
[[nodiscard]] FeedSlotRecords ParseFeedSlotRecords(
    const std::vector<std::uint8_t>& bytes, const StoreId& epoch, std::uint8_t expected_dims);
[[nodiscard]] std::optional<FeedSlotRecords> ReadFeedSlotRecords(const std::filesystem::path& dir, const StoreId& epoch);
[[nodiscard]] std::optional<FeedSlotRecords> ReadFeedSlotRecords(
    const std::filesystem::path& dir, const StoreId& epoch, std::uint8_t expected_dims);
void WriteFeedSlotRecords(const std::filesystem::path& dir, const FeedSlotRecords& records, bool* published = nullptr);

}  // namespace chunkdb
