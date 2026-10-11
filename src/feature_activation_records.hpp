#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

inline constexpr std::string_view kFeaturePendingFileName = "chunkdb.feature.pending";
inline constexpr std::size_t kMaxFeatureJournalBytes = 16U * 1024U * 1024U;
inline constexpr std::size_t kMaxFeatureJournalTables = 65536U;

struct FeatureActivationTarget {
    std::string name;
    StoreId table_id{};
    friend bool operator==(const FeatureActivationTarget&, const FeatureActivationTarget&) = default;
};

// The decision owns only this bit. Recovery unions it into freshly validated
// manifests rather than replaying stale schema, options or unknown masks.
struct FeatureActivationJournal {
    StoreId data_dir_id{};
    std::uint32_t feature = 0U;
    std::vector<FeatureActivationTarget> tables;
    friend bool operator==(const FeatureActivationJournal&, const FeatureActivationJournal&) = default;
};

[[nodiscard]] std::vector<std::uint8_t> EncodeFeatureActivationJournal(const FeatureActivationJournal& journal);
[[nodiscard]] FeatureActivationJournal DecodeFeatureActivationJournal(const std::vector<std::uint8_t>& bytes);
[[nodiscard]] std::optional<FeatureActivationJournal> ReadFeatureActivationJournal(const std::filesystem::path& root);
void WriteFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal,
    bool* published = nullptr);
// Caller holds the directory writer lock and excludes all metadata changes.
// Every participant and the complete inventory are checked before mutation.
void ValidateFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal);
void CompleteFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal);
void RecoverFeatureActivation(const std::filesystem::path& root, AccessMode access_mode);

}  // namespace chunkdb
