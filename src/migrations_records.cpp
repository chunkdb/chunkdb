#include "migrations_records.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <set>
#include <span>
#include <sstream>
#include <stdexcept>
#include <type_traits>

#include "checkpoint.hpp"
#include "statement_control.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "cql.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "users.hpp"
#include "chunk_coordinate_index.hpp"

namespace chunkdb {
namespace {
using Bytes = std::vector<std::uint8_t>;
constexpr std::array<std::uint8_t, 4> kRecordsMagic{'C', 'K', 'M', 'L'};
constexpr std::array<std::uint8_t, 4> kJournalMagic{'C', 'K', 'M', 'J'};
constexpr std::size_t kMaxRecords = 16384U;
constexpr std::size_t kMaxStatement = 65536U;
constexpr std::size_t kMaxFiles = 5U;
constexpr std::uint16_t kCatalogOperationJournalVersion = 2U;
constexpr std::size_t kHeaderBytes = 8U;
constexpr std::size_t kChecksumBytes = 4U;
constexpr std::size_t kLengthBytes = 4U;
constexpr std::size_t kDropNameSuffixBytes = 17U;
constexpr std::size_t kCatalogOperationMetadataBytes =
    kHeaderBytes + 2U * StoreId{}.size() + 1U + // identities and action
    2U * (kLengthBytes + kMaxTableNameLength) + kDropNameSuffixBytes +
    kLengthBytes + // participant count
    kLengthBytes + std::string_view(kUsersFileName).size() + 1U + // path and before-image marker
    2U * kLengthBytes + kChecksumBytes;
constexpr std::size_t kMaxCatalogOperationImageBytes = static_cast<std::size_t>(kMaxUsersFileBytes);
static_assert(kMaxUsersFileBytes <= std::numeric_limits<std::uint32_t>::max());
static_assert(kMaxUsersFileBytes <= (std::numeric_limits<std::size_t>::max() - kCatalogOperationMetadataBytes) / 2U);
constexpr std::size_t kMaxCatalogOperationJournalBytes = 2U * kMaxCatalogOperationImageBytes + kCatalogOperationMetadataBytes;

std::size_t JournalLimit(bool catalog_operation) {
    return catalog_operation ? kMaxCatalogOperationJournalBytes : kMaxMigrationJournalBytes;
}

[[noreturn]] void Bad(const std::string& why) { throw std::runtime_error("invalid migration metadata: " + why); }
bool Zero(const StoreId& id) { return std::all_of(id.begin(), id.end(), [](auto b) { return b == 0U; }); }

bool ValidName(std::string_view name) {
    if (name.empty() || name.size() > 63U) return false;
    for (std::size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        if (c != '_' && !(c >= 'a' && c <= 'z') && !(i > 0U && c >= '0' && c <= '9')) return false;
    }
    return true;
}

void ValidateRecord(const MigrationRecord& record) {
    RequireValidMigrationName(record.name);
    if (!record.user.empty() && !ValidName(record.user)) Bad("applying user");
    if (record.applied_ms == 0U || record.applied_ms > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
        Bad("application time");
    if (record.statement.empty() || record.statement.size() > kMaxStatement ||
        record.statement.find_first_of(std::string("\r\n\0", 3)) != std::string::npos) Bad("statement size or line break");
    if (!IsUtf8({reinterpret_cast<const std::uint8_t*>(record.statement.data()), record.statement.size()}))
        Bad("statement is not UTF-8");
}

void PutString(Bytes& out, std::string_view text) {
    WriteLe32(out, static_cast<std::uint32_t>(text.size()));
    out.insert(out.end(), text.begin(), text.end());
}
void PutRecord(Bytes& out, const MigrationRecord& record) {
    PutString(out, record.name);
    WriteLe64(out, record.applied_ms);
    PutString(out, record.user);
    PutString(out, record.statement);
}

template <typename UInt>
UInt LittleEndian(std::span<const std::uint8_t> bytes) {
    std::uint64_t result = 0U;
    for (std::size_t i = 0; i < bytes.size(); ++i) result |= std::uint64_t{bytes[i]} << (8U * i);
    return static_cast<UInt>(result);
}

void CheckHeader(std::span<const std::uint8_t> header, const std::array<std::uint8_t, 4>& magic, std::uint16_t version) {
    if (!std::equal(magic.begin(), magic.end(), header.begin()) ||
        LittleEndian<std::uint16_t>(header.subspan(4U, 2U)) != version || header[6] != 0U || header[7] != 0U)
        Bad("magic, version or reserved field");
}

void ReadExact(std::istream& input, std::span<std::uint8_t> output) {
    if (!output.empty() && !input.read(reinterpret_cast<char*>(output.data()), static_cast<std::streamsize>(output.size())))
        Bad("truncated field");
}

class Reader {
  public:
    Reader(const Bytes& bytes, const std::array<std::uint8_t, 4>& magic, std::size_t limit, std::uint16_t version = 1U)
        : bytes_(bytes) {
        if (bytes.size() < kHeaderBytes + kChecksumBytes || bytes.size() > limit) Bad("file size");
        end_ = bytes.size() - kChecksumBytes;
        CheckHeader(bytes_, magic, version);
        if (ReadLe32(bytes, end_) != Crc32(bytes.data(), end_)) Bad("checksum");
    }
    // The caller sized the file and read and checked its header.
    Reader(std::istream& input, std::size_t size, const std::array<std::uint8_t, kHeaderBytes>& header)
        : input_(&input), end_(size - kChecksumBytes), crc_(Crc32(header.data(), header.size())) {}

    std::uint8_t Byte() { return Number<std::uint8_t>(); }
    std::uint32_t U32() { return Number<std::uint32_t>(); }
    std::uint64_t U64() { return Number<std::uint64_t>(); }
    StoreId Id() {
        StoreId id{};
        Take(id);
        return id;
    }
    Bytes Blob(std::size_t limit) {
        const auto size = U32();
        if (size > limit) Bad("field size");
        Need(size);
        Bytes result(size);
        Take(result);
        return result;
    }
    std::string String(std::size_t limit) { const auto bytes = Blob(limit); return {bytes.begin(), bytes.end()}; }
    MigrationRecord Record() {
        MigrationRecord record;
        record.name = String(63U); record.applied_ms = U64();
        record.user = String(63U); record.statement = String(kMaxStatement);
        ValidateRecord(record); return record;
    }
    void End() const {
        if (at_ != end_) Bad("trailing bytes");
        if (!input_) return;
        std::array<std::uint8_t, kChecksumBytes> checksum{};
        ReadExact(*input_, checksum);
        if (LittleEndian<std::uint32_t>(checksum) != crc_) Bad("checksum");
        if (input_->peek() != std::char_traits<char>::eof() || input_->bad()) Bad("journal changed while reading");
    }
  private:
    void Need(std::size_t size) const { if (size > end_ - at_) Bad("truncated field"); }
    void Take(std::span<std::uint8_t> output) {
        if (output.empty()) return;
        Need(output.size());
        if (input_) {
            ReadExact(*input_, output);
            crc_ = Crc32Extend(crc_, output.data(), output.size());
        } else {
            std::copy_n(bytes_.data() + at_, output.size(), output.data());
        }
        at_ += output.size();
    }
    template <typename UInt>
    UInt Number() {
        std::array<std::uint8_t, sizeof(UInt)> bytes{};
        Take(bytes);
        return LittleEndian<UInt>(bytes);
    }
    std::span<const std::uint8_t> bytes_;
    std::istream* input_ = nullptr;
    std::size_t at_ = kHeaderBytes;
    std::size_t end_ = 0;
    std::uint32_t crc_ = 0U;
};

Bytes Header(const std::array<std::uint8_t, 4>& magic, std::uint16_t version = 1U) {
    Bytes bytes(magic.begin(), magic.end()); WriteLe16(bytes, version); WriteLe16(bytes, 0U); return bytes;
}
void Finish(Bytes& bytes, std::size_t limit) {
    if (bytes.size() > limit - 4U) Bad("file size");
    WriteLe32(bytes, Crc32(bytes));
}

std::optional<std::uintmax_t> RegularFileSize(const std::filesystem::path& path) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory || (!error && status.type() == std::filesystem::file_type::not_found))
        return std::nullopt;
    if (error) throw std::filesystem::filesystem_error("cannot inspect migration metadata", path, error);
    if (status.type() != std::filesystem::file_type::regular) Bad("participant is not a regular file: " + path.string());
    return std::filesystem::file_size(path);
}

std::optional<Bytes> ReadBytes(const std::filesystem::path& path, std::size_t limit) {
    const auto size = RegularFileSize(path);
    if (!size) return std::nullopt;
    if (*size > limit) Bad("participant size: " + path.string());
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot read " + path.string());
    Bytes bytes(static_cast<std::size_t>(*size));
    if (*size != 0U) input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(*size));
    if (!input || input.peek() != std::char_traits<char>::eof()) Bad("participant changed while reading: " + path.string());
    if (input.bad()) throw std::runtime_error("cannot read " + path.string());
    return bytes;
}

void SafePath(const std::filesystem::path& root, const std::string& relative) {
    auto path = root;
    for (const auto& part : std::filesystem::path(relative)) {
        path /= part;
        std::error_code error;
        const auto status = std::filesystem::symlink_status(path, error);
        if (error && error != std::errc::no_such_file_or_directory)
            throw std::filesystem::filesystem_error("cannot inspect migration path", path, error);
        if (status.type() == std::filesystem::file_type::symlink) Bad("symlink participant: " + path.string());
    }
}

void ValidateImage(const MigrationJournal& journal, const std::string& path, const Bytes& image) {
    if (path == kMigrationsFileName) {
        (void)DecodeMigrationRecords(image, journal.data_dir_id);
    } else if (path == kDataDirManifestFileName) {
        const auto manifest = ParseDataDirManifest(image);
        if (manifest.data_dir_id != journal.data_dir_id || (manifest.features.incompat & kFeatureMigrations) == 0U)
            Bad("data-directory identity or migration feature");
        RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
    } else if (path == kUsersFileName) {
        ValidateUsersImage(image);
    } else if (path == "tables/" + journal.table + "/" + std::string(kStoreManifestFileName)) {
        const auto manifest = ParseStoreManifest(image);
        if (manifest.store_id != journal.table_id) Bad("table participant identity");
        RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
    } else if (path == "tables/" + journal.table + "/" + std::string(kFeedSlotsFileName)) {
        const auto records = ParseFeedSlotRecords(image, journal.table_id);
        const auto manifest_path = "tables/" + journal.table + "/" + std::string(kStoreManifestFileName);
        const auto at = std::find_if(journal.files.begin(), journal.files.end(),
            [&](const auto& file) { return file.path == manifest_path; });
        if (at != journal.files.end()) {
            const auto manifest = ParseStoreManifest(at->after);
            if (manifest.store_id != journal.table_id || manifest.geometry.dims != records.dims)
                Bad("slot participant dimensionality differs from table");
        }
    } else if (path == "tables/" + journal.table + "/" + std::string(kChunkIndexFileName)) {
        if (journal.directory_action != MigrationDirectoryAction::kCreate) Bad("index participant outside table creation");
        const auto manifest_path = "tables/" + journal.table + "/" + std::string(kStoreManifestFileName);
        const auto at = std::find_if(journal.files.begin(), journal.files.end(),
            [&](const auto& file) { return file.path == manifest_path; });
        if (at == journal.files.end()) Bad("index participant has no table manifest");
        const auto manifest = ParseStoreManifest(at->after);
        if (manifest.store_id != journal.table_id) Bad("index participant table identity");
        const Geometry geometry(manifest.geometry, manifest.schema, *manifest.chunk_schema);
        std::istringstream input(std::string(image.begin(), image.end()), std::ios::binary);
        const auto inventory = ReadIndexInventory(input, image.size(), geometry, journal.table_id);
        if (inventory.count != 0U || inventory.generation != 1U || inventory.compaction_pending ||
            inventory.retained_deletion_floor != 0U || inventory.compaction_through != 0U)
            Bad("new table index is not an initial empty inventory");
    } else {
        Bad("participant path");
    }
}

bool LedgerOnly(const MigrationJournal& journal) {
    return journal.directory_action == MigrationDirectoryAction::kNone && journal.files.size() == 1U &&
           journal.files.front().path == kMigrationsFileName;
}

bool ConditionalNoOp(const cql::Statement& statement) {
    if (const auto* create = std::get_if<cql::CreateTable>(&statement)) return create->if_not_exists;
    if (const auto* drop = std::get_if<cql::DropTable>(&statement)) return drop->if_exists;
    if (const auto* create = std::get_if<cql::CreateSlot>(&statement)) return create->if_not_exists;
    if (const auto* drop = std::get_if<cql::DropSlot>(&statement)) return drop->if_exists;
    if (const auto* alter = std::get_if<cql::AlterTable>(&statement)) {
        if (const auto* add = std::get_if<cql::AddColumn>(&alter->change)) return add->if_not_exists;
        if (const auto* drop = std::get_if<cql::DropColumn>(&alter->change)) return drop->if_exists;
        if (const auto* drop = std::get_if<cql::DropCheck>(&alter->change)) return drop->if_exists;
    }
    return false;
}

bool IsOperationName(const std::string& table, const std::string& name) {
    const auto prefix = table + ".";
    return name.size() == table.size() + kDropNameSuffixBytes && name.rfind(prefix, 0) == 0U &&
           std::all_of(name.begin() + static_cast<std::ptrdiff_t>(prefix.size()), name.end(),
               [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}

// A DROP TABLE or RENAME TO outside MIGRATE: one table, its directory move,
// and at most the users file, which a recovery writes once the move happened.
void ValidateCatalogOperation(const MigrationJournal& journal) {
    if (Zero(journal.data_dir_id)) Bad("zero data-directory identity");
    if (journal.directory_action != MigrationDirectoryAction::kDrop &&
        journal.directory_action != MigrationDirectoryAction::kRename) Bad("catalog operation directory action");
    if (!IsValidTableName(journal.table) || Zero(journal.table_id)) Bad("table name or identity");
    if (journal.directory_action == MigrationDirectoryAction::kDrop
            ? !IsOperationName(journal.table, journal.operation_name)
            : !IsValidTableName(journal.operation_name) || journal.operation_name == journal.table)
        Bad("operation name");
    if (journal.files.size() > 1U) Bad("participant count");
    for (const auto& file : journal.files) {
        if (file.path != kUsersFileName) Bad("unrelated operation participant");
        if (!file.before || file.before->size() > kMaxCatalogOperationImageBytes || file.after.size() > kMaxCatalogOperationImageBytes)
            Bad("users participant images");
        ValidateImage(journal, file.path, *file.before);
        ValidateImage(journal, file.path, file.after);
    }
}

void ValidateStructure(const MigrationJournal& journal) {
    if (journal.catalog_operation) {
        ValidateCatalogOperation(journal);
        return;
    }
    if (Zero(journal.data_dir_id)) Bad("zero data-directory identity");
    ValidateRecord(journal.record);
    if (journal.directory_action != MigrationDirectoryAction::kNone &&
        journal.directory_action != MigrationDirectoryAction::kCreate &&
        journal.directory_action != MigrationDirectoryAction::kDrop) Bad("directory action");
    if (journal.table.empty()) {
        if (!Zero(journal.table_id) || journal.directory_action != MigrationDirectoryAction::kNone) Bad("missing table identity");
    } else if (!IsValidTableName(journal.table) || Zero(journal.table_id)) {
        Bad("table name or identity");
    }
    const auto parsed = cql::Parse(journal.record.statement);
    if (parsed.parameters != 0U ||
        !(std::holds_alternative<cql::CreateTable>(parsed.statement) ||
          std::holds_alternative<cql::AlterTable>(parsed.statement) ||
          std::holds_alternative<cql::DropTable>(parsed.statement) ||
          std::holds_alternative<cql::GrantRight>(parsed.statement) ||
          std::holds_alternative<cql::CreateSlot>(parsed.statement) ||
          std::holds_alternative<cql::DropSlot>(parsed.statement))) Bad("unsupported pending statement or parameters");
    const auto& statement = parsed.statement;
    const bool no_op = LedgerOnly(journal);
    if (no_op) {
        if (!ConditionalNoOp(statement)) Bad("ledger-only statement is not conditional DDL");
        const auto table = std::visit([](const auto& inner) -> std::string {
            using T = std::decay_t<decltype(inner)>;
            if constexpr (std::is_same_v<T, cql::CreateTable> || std::is_same_v<T, cql::DropTable> ||
                          std::is_same_v<T, cql::AlterTable> || std::is_same_v<T, cql::CreateSlot> || std::is_same_v<T, cql::DropSlot>)
                return inner.table;
            return {};
        }, statement);
        if (!IsValidTableName(table)) Bad("conditional statement table name");
        if (std::holds_alternative<cql::DropTable>(statement)) {
            if (!journal.table.empty() || !Zero(journal.table_id)) Bad("absent table no-op identity");
        } else if (journal.table != table) Bad("conditional statement table identity");
    } else if (const auto* create = std::get_if<cql::CreateTable>(&statement)) {
        if (journal.directory_action != MigrationDirectoryAction::kCreate || journal.table != create->table)
            Bad("CREATE TABLE participants");
    } else if (const auto* drop = std::get_if<cql::DropTable>(&statement)) {
        if (journal.directory_action != MigrationDirectoryAction::kDrop || journal.table != drop->table)
            Bad("DROP TABLE participants");
    } else {
        if (journal.directory_action != MigrationDirectoryAction::kNone) Bad("unexpected directory action");
        const auto table = std::visit([](const auto& inner) -> std::string {
            using T = std::decay_t<decltype(inner)>;
            if constexpr (std::is_same_v<T, cql::AlterTable> || std::is_same_v<T, cql::CreateSlot> || std::is_same_v<T, cql::DropSlot>)
                return inner.table;
            return {};
        }, statement);
        if (journal.table != table) Bad("statement table differs from participants");
    }
    if (journal.directory_action == MigrationDirectoryAction::kNone) {
        if (!journal.operation_name.empty()) Bad("unexpected operation name");
    } else {
        const auto prefix = journal.table + ".";
        if (journal.operation_name.size() != prefix.size() + 16U || journal.operation_name.rfind(prefix, 0) != 0U ||
            !std::all_of(journal.operation_name.begin() + static_cast<std::ptrdiff_t>(prefix.size()),
                         journal.operation_name.end(), [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }))
            Bad("operation name");
    }
    if (journal.files.empty() || journal.files.size() > kMaxFiles || journal.files.back().path != kMigrationsFileName)
        Bad("participant count or ledger order");
    std::set<std::string> paths;
    for (std::size_t i = 0; i < journal.files.size(); ++i) {
        const auto& file = journal.files[i];
        if (!paths.insert(file.path).second || file.after.size() > kMaxMigrationRecordsBytes ||
            (file.before && file.before->size() > kMaxMigrationRecordsBytes)) Bad("duplicate or oversized participant");
        if (file.path == kDataDirManifestFileName && i != 0U) Bad("version floor must precede directory publication");
        if (file.before) ValidateImage(journal, file.path, *file.before);
        ValidateImage(journal, file.path, file.after);
    }
    const std::string manifest_path = "tables/" + journal.table + "/" + std::string(kStoreManifestFileName);
    const std::string slots_path = "tables/" + journal.table + "/" + std::string(kFeedSlotsFileName);
    const std::string index_path = "tables/" + journal.table + "/" + std::string(kChunkIndexFileName);
    std::set<std::string> allowed{std::string(kMigrationsFileName)};
    std::set<std::string> required = allowed;
    if (no_op) {
        // Existing objects are validation-only: completion writes the ledger,
        // never the manifest, slot records or an unchanged table directory.
    } else if (std::holds_alternative<cql::CreateTable>(statement) || std::holds_alternative<cql::AlterTable>(statement)) {
        required.insert(manifest_path);
        if (std::holds_alternative<cql::CreateTable>(statement)) required.insert(index_path);
    } else if (std::holds_alternative<cql::DropTable>(statement)) {
        required.insert(std::string(kDataDirManifestFileName));
        allowed.insert(kUsersFileName);
    } else if (std::holds_alternative<cql::GrantRight>(statement)) {
        required.insert(kUsersFileName);
    } else if (std::holds_alternative<cql::CreateSlot>(statement)) {
        required.insert(manifest_path); required.insert(slots_path);
    } else if (std::holds_alternative<cql::DropSlot>(statement)) {
        required.insert(slots_path);
    }
    allowed.insert(required.begin(), required.end());
    if (!std::includes(paths.begin(), paths.end(), required.begin(), required.end()) ||
        !std::includes(allowed.begin(), allowed.end(), paths.begin(), paths.end())) Bad("missing or unrelated operation participant");
    for (const auto& file : journal.files) {
        if (file.path == kMigrationsFileName) continue;
        const bool newly_created = (journal.directory_action == MigrationDirectoryAction::kCreate &&
                                       (file.path == manifest_path || file.path == index_path)) ||
                                   (std::holds_alternative<cql::CreateSlot>(statement) && file.path == slots_path);
        if (journal.directory_action == MigrationDirectoryAction::kCreate && file.before) Bad("new table has a before-image");
        if (!newly_created && !file.before) Bad("existing participant has no before-image");
    }
    const auto& ledger = journal.files.back();
    const auto before = ledger.before ? DecodeMigrationRecords(*ledger.before, journal.data_dir_id) : std::vector<MigrationRecord>{};
    const auto after = DecodeMigrationRecords(ledger.after, journal.data_dir_id);
    if (after.size() != before.size() + 1U || !std::equal(before.begin(), before.end(), after.begin()) || after.back() != journal.record)
        Bad("ledger is not the exact next append");
}

void CheckDirectory(const std::filesystem::path& path, const StoreId& id) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error || status.type() != std::filesystem::file_type::directory) Bad("missing or invalid table directory: " + path.string());
    const auto image = ReadBytes(path / kStoreManifestFileName, kStoreManifestMaxSize);
    if (!image || ParseStoreManifest(*image).store_id != id) Bad("directory table identity: " + path.string());
}
bool Present(const std::filesystem::path& path) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory) return false;
    if (error) throw std::filesystem::filesystem_error("cannot inspect migration directory", path, error);
    return status.type() != std::filesystem::file_type::not_found;
}
}  // namespace

void RequireValidMigrationName(std::string_view name) {
    if (!ValidName(name)) throw std::invalid_argument("migration names are 1 to 63 bytes: [a-z_][a-z0-9_]*");
}

Bytes EncodeMigrationRecords(const StoreId& data_dir_id, const std::vector<MigrationRecord>& records) {
    if (Zero(data_dir_id)) Bad("ledger identity");
    if (records.size() > kMaxRecords)
        throw std::out_of_range("migration ledger record limit is " + std::to_string(kMaxRecords));
    auto bytes = Header(kRecordsMagic);
    bytes.insert(bytes.end(), data_dir_id.begin(), data_dir_id.end());
    WriteLe32(bytes, static_cast<std::uint32_t>(records.size()));
    std::set<std::string> names;
    for (const auto& record : records) {
        ValidateRecord(record);
        if (!names.insert(record.name).second) Bad("duplicate migration name");
        PutRecord(bytes, record);
        if (bytes.size() > kMaxMigrationRecordsBytes - 4U)
            throw std::out_of_range("migration ledger byte limit is " + std::to_string(kMaxMigrationRecordsBytes) + " bytes (16 MiB)");
    }
    Finish(bytes, kMaxMigrationRecordsBytes); return bytes;
}

std::vector<MigrationRecord> DecodeMigrationRecords(const Bytes& bytes, const StoreId& data_dir_id) {
    Reader reader(bytes, kRecordsMagic, kMaxMigrationRecordsBytes);
    const auto id = reader.Id();
    if (Zero(id) || id != data_dir_id) Bad("ledger data-directory identity");
    const auto count = reader.U32();
    if (count > kMaxRecords) Bad("ledger count");
    std::vector<MigrationRecord> records;
    std::set<std::string> names;
    for (std::uint32_t i = 0; i < count; ++i) {
        auto record = reader.Record();
        if (!names.insert(record.name).second) Bad("duplicate migration name");
        records.push_back(std::move(record));
    }
    reader.End(); return records;
}

std::vector<MigrationRecord> ReadMigrationRecords(const std::filesystem::path& root) {
    const auto bytes = ReadBytes(root / kMigrationsFileName, kMaxMigrationRecordsBytes);
    if (!bytes) return {};
    SafePath(root, std::string(kDataDirManifestFileName));
    const auto manifest = ReadDataDirManifest(root);
    if (!manifest || (manifest->features.incompat & kFeatureMigrations) == 0U) Bad("ledger without migration feature");
    return DecodeMigrationRecords(*bytes, manifest->data_dir_id);
}

namespace {

// Emit the same encoding to memory or to the atomic temporary file.
void EmitJournal(const MigrationJournal& journal, const AtomicFileSink& sink) {
    const auto body_limit = JournalLimit(journal.catalog_operation) - kChecksumBytes;
    std::size_t size = 0U;
    std::uint32_t crc = 0U;
    const auto emit = [&](std::span<const std::uint8_t> bytes) {
        if (bytes.size() > body_limit - size) Bad("journal size");
        sink(bytes);
        size += bytes.size();
        crc = Crc32Extend(crc, bytes.data(), bytes.size());
    };
    // A catalog operation has format version 2 and no record, so a build
    // that knows only version 1 refuses it instead of misreading it.
    auto bytes = Header(kJournalMagic, journal.catalog_operation ? kCatalogOperationJournalVersion : 1U);
    bytes.insert(bytes.end(), journal.data_dir_id.begin(), journal.data_dir_id.end());
    if (!journal.catalog_operation) PutRecord(bytes, journal.record);
    bytes.push_back(static_cast<std::uint8_t>(journal.directory_action));
    PutString(bytes, journal.table);
    bytes.insert(bytes.end(), journal.table_id.begin(), journal.table_id.end());
    PutString(bytes, journal.operation_name);
    WriteLe32(bytes, static_cast<std::uint32_t>(journal.files.size()));
    emit(bytes);
    const auto emit_image = [&](const Bytes& image) {
        bytes.clear();
        WriteLe32(bytes, static_cast<std::uint32_t>(image.size()));
        emit(bytes);
        emit(image);
    };
    for (const auto& file : journal.files) {
        bytes.clear();
        PutString(bytes, file.path); bytes.push_back(file.before ? 1U : 0U);
        emit(bytes);
        if (file.before) emit_image(*file.before);
        emit_image(file.after);
    }
    bytes.clear();
    WriteLe32(bytes, crc);
    sink(bytes);
}

MigrationJournal ReadJournal(Reader& reader, bool catalog_operation) {
    MigrationJournal journal;
    journal.catalog_operation = catalog_operation;
    journal.data_dir_id = reader.Id();
    if (!catalog_operation) journal.record = reader.Record();
    journal.directory_action = static_cast<MigrationDirectoryAction>(reader.Byte());
    journal.table = reader.String(kMaxTableNameLength); journal.table_id = reader.Id();
    journal.operation_name = reader.String(kMaxTableNameLength + kDropNameSuffixBytes);
    const auto count = reader.U32();
    if (catalog_operation ? count > 1U : count == 0U || count > kMaxFiles) Bad("participant count");
    const auto image_limit = catalog_operation ? kMaxCatalogOperationImageBytes : kMaxMigrationRecordsBytes;
    for (std::uint32_t i = 0; i < count; ++i) {
        MigrationFileChange file;
        file.path = reader.String(128U);
        if (catalog_operation && file.path != kUsersFileName) Bad("unrelated operation participant");
        const auto present = reader.Byte();
        if (present > 1U || (catalog_operation && present == 0U)) Bad("before-image presence");
        if (present != 0U) file.before = reader.Blob(image_limit);
        file.after = reader.Blob(image_limit);
        journal.files.push_back(std::move(file));
    }
    reader.End(); ValidateStructure(journal); return journal;
}

}  // namespace

Bytes EncodeMigrationJournal(const MigrationJournal& journal) {
    ValidateStructure(journal);
    Bytes bytes;
    EmitJournal(journal, [&](std::span<const std::uint8_t> part) { bytes.insert(bytes.end(), part.begin(), part.end()); });
    return bytes;
}

MigrationJournal DecodeMigrationJournal(const Bytes& bytes) {
    const bool catalog_operation = bytes.size() >= 6U && ReadLe16(bytes, 4U) == kCatalogOperationJournalVersion;
    Reader reader(bytes, kJournalMagic, JournalLimit(catalog_operation), catalog_operation ? kCatalogOperationJournalVersion : 1U);
    return ReadJournal(reader, catalog_operation);
}

std::optional<MigrationJournal> ReadMigrationJournal(const std::filesystem::path& root) {
    const auto path = root / kMigrationPendingFileName;
    const auto size = RegularFileSize(path);
    if (!size) return std::nullopt;
    if (*size < kHeaderBytes + kChecksumBytes) Bad("journal size");
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot read " + path.string());
    std::array<std::uint8_t, kHeaderBytes> header{};
    ReadExact(input, header);
    const bool catalog_operation = LittleEndian<std::uint16_t>(std::span(header).subspan(4U, 2U)) == kCatalogOperationJournalVersion;
    CheckHeader(header, kJournalMagic, catalog_operation ? kCatalogOperationJournalVersion : 1U);
    if (*size > JournalLimit(catalog_operation)) Bad("journal size");
    Reader reader(input, static_cast<std::size_t>(*size), header);
    return ReadJournal(reader, catalog_operation);
}

void WriteMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal, bool* published) {
    if (!journal.catalog_operation) {
        AtomicWrite(root / kMigrationPendingFileName, EncodeMigrationJournal(journal), true, true, published,
                    "CHUNKDB_FAILPOINT_MIGRATION_DECISION_SYNC_FAIL_ONCE", false, EnterStatementDecision);
        return;
    }
    ValidateStructure(journal);
    AtomicWriteStream(root / kMigrationPendingFileName, [&](const AtomicFileSink& sink) { EmitJournal(journal, sink); },
                true, true, published,
                "CHUNKDB_FAILPOINT_MIGRATION_DECISION_SYNC_FAIL_ONCE", false, EnterStatementDecision);
}

namespace {
// Whether `tables/<table>` still holds the journal's table: a table created
// under the name since is another identity.
bool SourceHoldsTable(const std::filesystem::path& root, const MigrationJournal& journal) {
    const auto source = root / "tables" / journal.table;
    if (!Present(source)) return false;
    const auto image = ReadBytes(source / kStoreManifestFileName, kStoreManifestMaxSize);
    return image && ParseStoreManifest(*image).store_id == journal.table_id;
}

// The directory `tables/<table>` moves to when a catalog operation commits.
std::string CatalogOperationTarget(const MigrationJournal& journal) {
    return journal.directory_action == MigrationDirectoryAction::kDrop
        ? ".chunkdb.dropped/" + journal.operation_name : "tables/" + journal.operation_name;
}

void ValidateCatalogOperationJournal(const std::filesystem::path& root, const MigrationJournal& journal) {
    SafePath(root, std::string(kDataDirManifestFileName));
    const auto manifest = ReadDataDirManifest(root);
    if (!manifest || manifest->data_dir_id != journal.data_dir_id) Bad("source data-directory identity");
    RequireOpenableFeatures(manifest->features, AccessMode::kReadWrite);
    if (Present(root / kUsersFileName) && journal.files.empty()) Bad("catalog operation users participant is missing");
    for (const auto& file : journal.files) {
        SafePath(root, file.path);
        const auto current = ReadBytes(root / file.path, kMaxCatalogOperationImageBytes);
        if (current != file.before && (!current || *current != file.after)) Bad("participant differs from before and after: " + file.path);
    }
    const auto source = "tables/" + journal.table;
    const auto target = CatalogOperationTarget(journal);
    SafePath(root, source);
    SafePath(root, target);
    const bool at_source = SourceHoldsTable(root, journal);
    if (at_source == Present(root / target)) Bad("ambiguous directory operation");
    CheckDirectory(root / (at_source ? source : target), journal.table_id);
}
}  // namespace

bool CatalogOperationMoved(const std::filesystem::path& root, const MigrationJournal& journal) {
    return !SourceHoldsTable(root, journal);
}

void ValidateMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal) {
    ValidateStructure(journal);
    if (journal.catalog_operation) {
        ValidateCatalogOperationJournal(root, journal);
        return;
    }
    SafePath(root, std::string(kDataDirManifestFileName));
    const auto manifest = ReadDataDirManifest(root);
    if (!manifest || manifest->data_dir_id != journal.data_dir_id ||
        (manifest->features.incompat & kFeatureMigrations) == 0U) Bad("source data-directory identity or feature");
    RequireOpenableFeatures(manifest->features, AccessMode::kReadWrite);
    if (journal.directory_action == MigrationDirectoryAction::kDrop && Present(root / kUsersFileName) &&
        std::none_of(journal.files.begin(), journal.files.end(), [](const auto& file) { return file.path == kUsersFileName; }))
        Bad("dropped table users participant is missing");
    for (const auto& file : journal.files) {
        SafePath(root, file.path);
        if (file.path == "tables/" + journal.table + "/" + std::string(kFeedSlotsFileName)) {
            SafePath(root, "tables/" + journal.table + "/" + std::string(kStoreManifestFileName));
            const auto table = ReadStoreManifest(root / "tables" / journal.table);
            if (!table || table->store_id != journal.table_id) Bad("slot participant table identity");
            if (file.before) (void)ParseFeedSlotRecords(*file.before, journal.table_id, table->geometry.dims);
            (void)ParseFeedSlotRecords(file.after, journal.table_id, table->geometry.dims);
        }
        const auto current = ReadBytes(root / file.path, kMaxMigrationRecordsBytes);
        if (current != file.before && (!current || *current != file.after)) Bad("participant differs from before and after: " + file.path);
        if (!current && journal.directory_action == MigrationDirectoryAction::kCreate &&
            (file.path == "tables/" + journal.table + "/" + std::string(kStoreManifestFileName) ||
             file.path == "tables/" + journal.table + "/" + std::string(kChunkIndexFileName))) {
            const auto staged = ".chunkdb.staging/" + journal.operation_name + "/" + std::filesystem::path(file.path).filename().string();
            SafePath(root, staged);
            if (ReadBytes(root / staged, kMaxMigrationRecordsBytes) != std::optional<Bytes>(file.after))
                Bad("staged table participant differs from prepared image");
        }
    }
    if (!journal.table.empty()) {
        const auto target = "tables/" + journal.table;
        SafePath(root, target);
        if (journal.directory_action == MigrationDirectoryAction::kNone) {
            CheckDirectory(root / target, journal.table_id);
        } else {
            const auto operation = std::string(journal.directory_action == MigrationDirectoryAction::kCreate ?
                                               ".chunkdb.staging/" : ".chunkdb.dropped/") + journal.operation_name;
            SafePath(root, operation);
            const bool table_present = Present(root / target);
            const bool operation_present = Present(root / operation);
            if (table_present == operation_present) Bad("ambiguous directory operation");
            CheckDirectory(root / (table_present ? target : operation), journal.table_id);
        }
    }
    if (LedgerOnly(journal)) {
        const auto parsed = cql::Parse(journal.record.statement);
        const auto& statement = parsed.statement;
        if (const auto* drop = std::get_if<cql::DropTable>(&statement)) {
            const auto target = "tables/" + drop->table;
            SafePath(root, target);
            if (Present(root / target)) Bad("conditional drop no-op table is present");
        } else if (const auto* alter = std::get_if<cql::AlterTable>(&statement)) {
            const auto table = ReadStoreManifest(root / "tables" / journal.table);
            if (!table || table->store_id != journal.table_id) Bad("conditional no-op table identity");
            const auto exists = [&](std::string_view name, bool chunks) {
                const auto& schema = chunks ? *table->chunk_schema : table->schema;
                return std::any_of(schema.columns.begin(), schema.columns.end(),
                    [&](const auto& column) { return column.name == name; });
            };
            if (const auto* add = std::get_if<cql::AddColumn>(&alter->change)) {
                if (!exists(add->column.name, add->chunk_column)) Bad("conditional add no-op column is absent");
            } else if (const auto* drop = std::get_if<cql::DropColumn>(&alter->change)) {
                if (exists(drop->column, drop->chunk_column)) Bad("conditional drop no-op column is present");
            } else if (const auto* drop = std::get_if<cql::DropCheck>(&alter->change)) {
                if (HasCheck(table->schema, drop->name)) Bad("conditional drop no-op check is present");
            }
        } else if (std::holds_alternative<cql::CreateSlot>(statement) || std::holds_alternative<cql::DropSlot>(statement)) {
            const auto relative = "tables/" + journal.table + "/" + std::string(kFeedSlotsFileName);
            SafePath(root, relative);
            const auto image = ReadBytes(root / relative, kMaxMigrationRecordsBytes);
            const auto table = ReadStoreManifest(root / "tables" / journal.table);
            if (!table || table->store_id != journal.table_id) Bad("conditional slot no-op table identity");
            const auto slots = image ? std::optional<FeedSlotRecords>(ParseFeedSlotRecords(*image, journal.table_id,
                table->geometry.dims)) : std::nullopt;
            const bool create = std::holds_alternative<cql::CreateSlot>(statement);
            const auto& name = create ? std::get<cql::CreateSlot>(statement).name : std::get<cql::DropSlot>(statement).name;
            const bool exists = slots && std::any_of(slots->slots.begin(), slots->slots.end(),
                [&](const auto& slot) { return slot.name == name; });
            if (create != exists) Bad("conditional slot no-op existence differs");
        }
    }
}

}  // namespace chunkdb
