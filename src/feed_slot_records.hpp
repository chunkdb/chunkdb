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
};

struct FeedSlotRecords {
    StoreId epoch{};
    std::uint64_t durable_watermark = 0;
    std::vector<FeedSlotRecord> slots;
};

void RequireValidFeedSlotName(std::string_view name);
[[nodiscard]] std::vector<std::uint8_t> SerializeFeedSlotRecords(const FeedSlotRecords& records);
[[nodiscard]] FeedSlotRecords ParseFeedSlotRecords(const std::vector<std::uint8_t>& bytes, const StoreId& epoch);
[[nodiscard]] std::optional<FeedSlotRecords> ReadFeedSlotRecords(const std::filesystem::path& dir, const StoreId& epoch);
void WriteFeedSlotRecords(const std::filesystem::path& dir, const FeedSlotRecords& records, bool* published = nullptr);

}  // namespace chunkdb
