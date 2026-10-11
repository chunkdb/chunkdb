#include "feature_activation_records.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <fstream>
#include <map>
#include <stdexcept>
#include <system_error>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {
constexpr std::array<std::uint8_t, 4> kMagic{'C', 'K', 'F', 'A'};
[[noreturn]] void Bad(const std::string& why) {
    throw std::runtime_error("invalid feature activation metadata: " + why);
}
void Crash(const char* name) { if (ConsumeFailpointEnv(name)) std::_Exit(86); }

void ValidateStructure(const FeatureActivationJournal& journal) {
    if (journal.data_dir_id == StoreId{}) Bad("zero data-directory identity");
    const auto known = std::find_if(kFeatureRegistry.begin(), kFeatureRegistry.end(),
        [&](const auto& entry) { return entry.incompat == journal.feature && entry.incompat != kFeatureActivation; });
    if (known == kFeatureRegistry.end()) Bad("unknown or directory-only feature");
    if (journal.tables.size() > kMaxFeatureJournalTables) Bad("too many tables");
    std::string_view previous;
    for (const auto& target : journal.tables) {
        if (!IsValidTableName(target.name)) Bad("invalid table name");
        if (!previous.empty() && previous >= target.name) Bad("tables are duplicated or unordered");
        if (target.table_id == StoreId{}) Bad("zero table identity");
        previous = target.name;
    }
}

class Reader {
  public:
    explicit Reader(const std::vector<std::uint8_t>& bytes) : bytes_(bytes) {
        if (bytes.size() < 36U || bytes.size() > kMaxFeatureJournalBytes) Bad("file size");
        end_ = bytes.size() - 4U;
        if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
            ReadLe16(bytes, 4U) != 1U || ReadLe16(bytes, 6U) != 0U)
            Bad("magic, version or reserved field");
        if (ReadLe32(bytes, end_) != Crc32(bytes.data(), end_)) Bad("checksum");
    }
    std::uint32_t U32() { Need(4U); auto value = ReadLe32(bytes_, at_); at_ += 4U; return value; }
    StoreId Id() {
        Need(16U); StoreId id;
        std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), id.size(), id.begin());
        at_ += id.size(); return id;
    }
    std::string Name() {
        Need(1U); const auto length = bytes_[at_++]; Need(length);
        std::string value(bytes_.begin() + static_cast<std::ptrdiff_t>(at_),
            bytes_.begin() + static_cast<std::ptrdiff_t>(at_ + length));
        at_ += length; return value;
    }
    void End() const { if (at_ != end_) Bad("trailing bytes"); }
  private:
    void Need(std::size_t count) const { if (count > end_ - at_) Bad("truncated field"); }
    const std::vector<std::uint8_t>& bytes_;
    std::size_t at_ = 8U, end_ = 0U;
};

struct Participants {
    DataDirManifest directory;
    std::vector<StoreManifest> tables;
};

Participants ValidateParticipants(const std::filesystem::path& root, const FeatureActivationJournal& journal) {
    ValidateStructure(journal);
    const auto directory = ReadDataDirManifest(root);
    if (!directory || directory->data_dir_id != journal.data_dir_id) Bad("data-directory identity changed");
    RequireOpenableFeatures(directory->features, AccessMode::kReadWrite);
    if ((directory->features.incompat & kFeatureActivation) == 0U) Bad("missing feature_activation ownership");
    std::error_code pending_error;
    const auto migration = std::filesystem::symlink_status(root / "chunkdb.migration.pending", pending_error);
    if (pending_error && pending_error != std::errc::no_such_file_or_directory)
        throw std::filesystem::filesystem_error("cannot inspect migration decision", root, pending_error);
    if (!pending_error && migration.type() != std::filesystem::file_type::not_found)
        Bad("migration and feature activation decisions conflict");
    const auto tables_dir = root / "tables";
    const auto directory_status = std::filesystem::symlink_status(tables_dir);
    if (directory_status.type() != std::filesystem::file_type::directory) Bad("tables directory is not a directory");
    std::map<std::string, std::filesystem::path> inventory;
    // Match catalog inventory rules: foreign names and non-directory entries
    // are not tables. Listing or inspection errors never become an empty set.
    for (const auto& entry : std::filesystem::directory_iterator(tables_dir)) {
        const auto name = entry.path().filename().string();
        if (!IsValidTableName(name)) continue;
        if (entry.symlink_status().type() != std::filesystem::file_type::directory) continue;
        inventory.emplace(name, entry.path());
        if (inventory.size() > kMaxFeatureJournalTables) Bad("too many tables");
    }
    if (inventory.size() != journal.tables.size()) Bad("table inventory changed");
    Participants result{*directory, {}};
    result.tables.reserve(journal.tables.size());
    auto found = inventory.begin();
    for (const auto& target : journal.tables) {
        if (found->first != target.name) Bad("table inventory changed");
        auto manifest = ReadStoreManifest(found->second);
        if (!manifest || manifest->store_id != target.table_id) Bad("table identity changed: " + target.name);
        RequireOpenableFeatures(manifest->features, AccessMode::kReadWrite);
        if ((manifest->features.incompat & kFeatureActivation) != 0U) Bad("directory-only feature on table: " + target.name);
        result.tables.push_back(std::move(*manifest)); ++found;
    }
    return result;
}
}  // namespace

std::vector<std::uint8_t> EncodeFeatureActivationJournal(const FeatureActivationJournal& journal) {
    ValidateStructure(journal);
    std::vector<std::uint8_t> bytes(kMagic.begin(), kMagic.end());
    WriteLe16(bytes, 1U); WriteLe16(bytes, 0U);
    bytes.insert(bytes.end(), journal.data_dir_id.begin(), journal.data_dir_id.end());
    WriteLe32(bytes, journal.feature); WriteLe32(bytes, static_cast<std::uint32_t>(journal.tables.size()));
    for (const auto& target : journal.tables) {
        bytes.push_back(static_cast<std::uint8_t>(target.name.size()));
        bytes.insert(bytes.end(), target.name.begin(), target.name.end());
        bytes.insert(bytes.end(), target.table_id.begin(), target.table_id.end());
    }
    if (bytes.size() > kMaxFeatureJournalBytes - 4U) Bad("file size");
    WriteLe32(bytes, Crc32(bytes)); return bytes;
}

FeatureActivationJournal DecodeFeatureActivationJournal(const std::vector<std::uint8_t>& bytes) {
    Reader reader(bytes);
    FeatureActivationJournal journal;
    journal.data_dir_id = reader.Id(); journal.feature = reader.U32();
    const auto count = reader.U32();
    if (count > kMaxFeatureJournalTables || count > (bytes.size() - 36U) / 18U) Bad("table count");
    journal.tables.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) journal.tables.push_back({reader.Name(), reader.Id()});
    reader.End(); ValidateStructure(journal); return journal;
}

std::optional<FeatureActivationJournal> ReadFeatureActivationJournal(const std::filesystem::path& root) {
    const auto path = root / kFeaturePendingFileName;
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory ||
        (!error && status.type() == std::filesystem::file_type::not_found)) return std::nullopt;
    if (error) throw std::filesystem::filesystem_error("cannot inspect feature activation decision", path, error);
    if (status.type() != std::filesystem::file_type::regular) Bad("decision is not a regular file");
    const auto size = std::filesystem::file_size(path);
    if (size > kMaxFeatureJournalBytes) Bad("file size");
    std::ifstream input(path, std::ios::binary);
    if (!input) Bad("cannot read decision");
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (size != 0U) input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
    if (!input || input.peek() != std::char_traits<char>::eof()) Bad("decision changed while reading");
    if (input.bad()) Bad("cannot read decision");
    return DecodeFeatureActivationJournal(bytes);
}

void ValidateFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal) {
    (void)ValidateParticipants(root, journal);
}

void WriteFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal, bool* published) {
    ValidateFeatureActivationJournal(root, journal);
    if (ReadFeatureActivationJournal(root)) Bad("a decision is already pending");
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_BEFORE_DECISION_ONCE");
    AtomicWrite(root / kFeaturePendingFileName, EncodeFeatureActivationJournal(journal), true, true,
        published, "CHUNKDB_FAILPOINT_FEATURE_DECISION_SYNC_FAIL_ONCE", false);
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_AFTER_DECISION_ONCE");
}

void CompleteFeatureActivationJournal(const std::filesystem::path& root, const FeatureActivationJournal& journal) {
    auto participants = ValidateParticipants(root, journal);
    const auto decision = ReadFeatureActivationJournal(root);
    if (!decision || *decision != journal) Bad("decision identity changed");
    participants.directory.features.incompat |= journal.feature;
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_ROOT_BEFORE_PUBLISH_ONCE");
    AtomicWrite(DataDirManifestPath(root), SerializeDataDirManifest(participants.directory), true, true,
        nullptr, "CHUNKDB_FAILPOINT_FEATURE_ROOT_SYNC_FAIL_ONCE", false);
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_ROOT_AFTER_PUBLISH_ONCE");
    const bool table_scoped = (journal.feature & kTableBaselineFeatures.incompat) != 0U;
    for (std::size_t i = 0; table_scoped && i < journal.tables.size(); ++i) {
        participants.tables[i].features.incompat |= journal.feature;
        const auto prefix = "CHUNKDB_FAILPOINT_FEATURE_TABLE_" + std::to_string(i);
        Crash((prefix + "_BEFORE_PUBLISH_ONCE").c_str());
        const auto failure = prefix + "_SYNC_FAIL_ONCE";
        AtomicWrite(StoreManifestPath(root / "tables" / journal.tables[i].name),
            SerializeStoreManifest(participants.tables[i]), true, true, nullptr, failure.c_str(), false);
        Crash((prefix + "_AFTER_PUBLISH_ONCE").c_str());
    }
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_BEFORE_PENDING_REMOVE_ONCE");
    if (!std::filesystem::remove(root / kFeaturePendingFileName)) Bad("decision disappeared before completion");
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_FEATURE_PENDING_REMOVE_SYNC_FAIL_ONCE"))
        throw std::system_error(std::make_error_code(std::errc::io_error), "feature decision removal directory sync failed");
    SyncDirectoryPath(root);
    Crash("CHUNKDB_FAILPOINT_CRASH_FEATURE_AFTER_PENDING_REMOVE_ONCE");
}

void RecoverFeatureActivation(const std::filesystem::path& root, AccessMode access_mode) {
    if (const auto journal = ReadFeatureActivationJournal(root)) {
        ValidateFeatureActivationJournal(root, *journal);
        if (access_mode == AccessMode::kReadOnly)
            throw std::runtime_error("feature activation recovery requires a read-write open");
        CompleteFeatureActivationJournal(root, *journal);
    }
    if (access_mode == AccessMode::kReadWrite) CleanupAtomicTmpArtifacts(root / kFeaturePendingFileName);
}

}  // namespace chunkdb
