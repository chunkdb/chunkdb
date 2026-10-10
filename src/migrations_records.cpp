#include "migrations_records.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <type_traits>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/table_catalog.hpp"
#include "cql.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "users.hpp"

namespace chunkdb {
namespace {
using Bytes = std::vector<std::uint8_t>;
constexpr std::array<std::uint8_t, 4> kRecordsMagic{'C', 'K', 'M', 'L'};
constexpr std::array<std::uint8_t, 4> kJournalMagic{'C', 'K', 'M', 'J'};
constexpr std::size_t kMaxRecords = 16384U;
constexpr std::size_t kMaxStatement = 65536U;
constexpr std::size_t kMaxFiles = 5U;

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

void PutBytes(Bytes& out, const Bytes& bytes) {
    if (bytes.size() > kMaxMigrationRecordsBytes) Bad("participant size");
    WriteLe32(out, static_cast<std::uint32_t>(bytes.size()));
    out.insert(out.end(), bytes.begin(), bytes.end());
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

class Reader {
  public:
    Reader(const Bytes& bytes, const std::array<std::uint8_t, 4>& magic, std::size_t limit) : bytes_(bytes) {
        if (bytes.size() < 12U || bytes.size() > limit) Bad("file size");
        end_ = bytes.size() - 4U;
        if (!std::equal(magic.begin(), magic.end(), bytes.begin()) || ReadLe16(bytes, 4U) != 1U ||
            ReadLe16(bytes, 6U) != 0U) Bad("magic, version or reserved field");
        if (ReadLe32(bytes, end_) != Crc32(bytes.data(), end_)) Bad("checksum");
    }
    std::uint8_t Byte() { Need(1U); return bytes_[at_++]; }
    std::uint32_t U32() { Need(4U); const auto n = ReadLe32(bytes_, at_); at_ += 4U; return n; }
    std::uint64_t U64() { Need(8U); const auto n = ReadLe64(bytes_, at_); at_ += 8U; return n; }
    StoreId Id() {
        Need(16U); StoreId id{};
        std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), 16U, id.begin()); at_ += 16U;
        return id;
    }
    Bytes Blob(std::size_t limit) {
        const auto size = U32();
        if (size > limit) Bad("field size");
        Need(size);
        Bytes result(bytes_.begin() + static_cast<std::ptrdiff_t>(at_),
                     bytes_.begin() + static_cast<std::ptrdiff_t>(at_ + size));
        at_ += size;
        return result;
    }
    std::string String(std::size_t limit) { const auto bytes = Blob(limit); return {bytes.begin(), bytes.end()}; }
    MigrationRecord Record() {
        MigrationRecord record;
        record.name = String(63U); record.applied_ms = U64();
        record.user = String(63U); record.statement = String(kMaxStatement);
        ValidateRecord(record); return record;
    }
    void End() const { if (at_ != end_) Bad("trailing bytes"); }
  private:
    void Need(std::size_t size) const { if (size > end_ - at_) Bad("truncated field"); }
    const Bytes& bytes_;
    std::size_t at_ = 8U;
    std::size_t end_ = 0;
};

Bytes Header(const std::array<std::uint8_t, 4>& magic) {
    Bytes bytes(magic.begin(), magic.end()); WriteLe16(bytes, 1U); WriteLe16(bytes, 0U); return bytes;
}
void Finish(Bytes& bytes, std::size_t limit) {
    if (bytes.size() > limit - 4U) Bad("file size");
    WriteLe32(bytes, Crc32(bytes));
}

std::optional<Bytes> ReadBytes(const std::filesystem::path& path, std::size_t limit) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory || (!error && status.type() == std::filesystem::file_type::not_found))
        return std::nullopt;
    if (error) throw std::filesystem::filesystem_error("cannot inspect migration metadata", path, error);
    if (status.type() != std::filesystem::file_type::regular) Bad("participant is not a regular file: " + path.string());
    const auto size = std::filesystem::file_size(path);
    if (size > limit) Bad("participant size: " + path.string());
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot read " + path.string());
    Bytes bytes(static_cast<std::size_t>(size));
    if (size != 0U) input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
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
        (void)DecodeUsers(image);
    } else if (path == "tables/" + journal.table + "/" + std::string(kStoreManifestFileName)) {
        const auto manifest = ParseStoreManifest(image);
        if (manifest.store_id != journal.table_id) Bad("table participant identity");
        RequireOpenableFeatures(manifest.features, AccessMode::kReadWrite);
    } else if (path == "tables/" + journal.table + "/" + std::string(kFeedSlotsFileName)) {
        (void)ParseFeedSlotRecords(image, journal.table_id);
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
    }
    return false;
}

void ValidateStructure(const MigrationJournal& journal) {
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
    std::set<std::string> allowed{std::string(kMigrationsFileName)};
    std::set<std::string> required = allowed;
    if (no_op) {
        // Existing objects are validation-only: completion writes the ledger,
        // never the manifest, slot records or an unchanged table directory.
    } else if (std::holds_alternative<cql::CreateTable>(statement) || std::holds_alternative<cql::AlterTable>(statement)) {
        required.insert(manifest_path);
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
        const bool newly_created = (journal.directory_action == MigrationDirectoryAction::kCreate && file.path == manifest_path) ||
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

Bytes EncodeMigrationJournal(const MigrationJournal& journal) {
    ValidateStructure(journal);
    auto bytes = Header(kJournalMagic);
    bytes.insert(bytes.end(), journal.data_dir_id.begin(), journal.data_dir_id.end());
    PutRecord(bytes, journal.record);
    bytes.push_back(static_cast<std::uint8_t>(journal.directory_action));
    PutString(bytes, journal.table);
    bytes.insert(bytes.end(), journal.table_id.begin(), journal.table_id.end());
    PutString(bytes, journal.operation_name);
    WriteLe32(bytes, static_cast<std::uint32_t>(journal.files.size()));
    for (const auto& file : journal.files) {
        PutString(bytes, file.path); bytes.push_back(file.before ? 1U : 0U);
        if (file.before) PutBytes(bytes, *file.before);
        PutBytes(bytes, file.after);
        if (bytes.size() > kMaxMigrationJournalBytes - 4U) Bad("journal size");
    }
    Finish(bytes, kMaxMigrationJournalBytes); return bytes;
}

MigrationJournal DecodeMigrationJournal(const Bytes& bytes) {
    Reader reader(bytes, kJournalMagic, kMaxMigrationJournalBytes);
    MigrationJournal journal;
    journal.data_dir_id = reader.Id(); journal.record = reader.Record();
    journal.directory_action = static_cast<MigrationDirectoryAction>(reader.Byte());
    journal.table = reader.String(kMaxTableNameLength); journal.table_id = reader.Id();
    journal.operation_name = reader.String(kMaxTableNameLength + 17U);
    const auto count = reader.U32();
    if (count == 0U || count > kMaxFiles) Bad("participant count");
    for (std::uint32_t i = 0; i < count; ++i) {
        MigrationFileChange file;
        file.path = reader.String(128U);
        const auto present = reader.Byte();
        if (present > 1U) Bad("before-image presence");
        if (present != 0U) file.before = reader.Blob(kMaxMigrationRecordsBytes);
        file.after = reader.Blob(kMaxMigrationRecordsBytes);
        journal.files.push_back(std::move(file));
    }
    reader.End(); ValidateStructure(journal); return journal;
}

std::optional<MigrationJournal> ReadMigrationJournal(const std::filesystem::path& root) {
    const auto bytes = ReadBytes(root / kMigrationPendingFileName, kMaxMigrationJournalBytes);
    return bytes ? std::optional<MigrationJournal>(DecodeMigrationJournal(*bytes)) : std::nullopt;
}

void WriteMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal, bool* published) {
    AtomicWrite(root / kMigrationPendingFileName, EncodeMigrationJournal(journal), true, true, published,
                "CHUNKDB_FAILPOINT_MIGRATION_DECISION_SYNC_FAIL_ONCE", false);
}

void ValidateMigrationJournal(const std::filesystem::path& root, const MigrationJournal& journal) {
    ValidateStructure(journal);
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
        const auto current = ReadBytes(root / file.path, kMaxMigrationRecordsBytes);
        if (current != file.before && (!current || *current != file.after)) Bad("participant differs from before and after: " + file.path);
        if (!current && journal.directory_action == MigrationDirectoryAction::kCreate &&
            file.path == "tables/" + journal.table + "/" + std::string(kStoreManifestFileName)) {
            const auto staged = ".chunkdb.staging/" + journal.operation_name + "/" + std::string(kStoreManifestFileName);
            SafePath(root, staged);
            if (ReadBytes(root / staged, kStoreManifestMaxSize) != std::optional<Bytes>(file.after))
                Bad("staged manifest differs from prepared image");
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
            const auto exists = [&](std::string_view name) {
                return std::any_of(table->schema.columns.begin(), table->schema.columns.end(),
                    [&](const auto& column) { return column.name == name; });
            };
            if (const auto* add = std::get_if<cql::AddColumn>(&alter->change)) {
                if (!exists(add->column.name)) Bad("conditional add no-op column is absent");
            } else if (const auto* drop = std::get_if<cql::DropColumn>(&alter->change)) {
                if (exists(drop->column)) Bad("conditional drop no-op column is present");
            }
        } else if (std::holds_alternative<cql::CreateSlot>(statement) || std::holds_alternative<cql::DropSlot>(statement)) {
            const auto slots = ReadFeedSlotRecords(root / "tables" / journal.table, journal.table_id);
            const bool create = std::holds_alternative<cql::CreateSlot>(statement);
            const auto& name = create ? std::get<cql::CreateSlot>(statement).name : std::get<cql::DropSlot>(statement).name;
            const bool exists = slots && std::any_of(slots->slots.begin(), slots->slots.end(),
                [&](const auto& slot) { return slot.name == name; });
            if (create != exists) Bad("conditional slot no-op existence differs");
        }
    }
}

}  // namespace chunkdb
