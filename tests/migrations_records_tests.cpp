#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>

#include "chunkdb/crc32.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunk_store_internal.hpp"
#include "feature_flags.hpp"
#include "migrations_records.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(output.good());
}
template <typename Fn> void Reject(Fn&& fn) {
    bool rejected = false;
    try { fn(); } catch (const std::exception&) { rejected = true; }
    assert(rejected);
}
void Checksum(Bytes& bytes) {
    const auto crc = Crc32(bytes.data(), bytes.size() - 4U);
    for (unsigned i = 0; i < 4U; ++i) bytes[bytes.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-migrations-records"};
    std::filesystem::path root = directory.path();
    StoreId data_id{};
    StoreManifest table;
    Bytes before;
    Bytes after;
    MigrationRecord record{"set_options", 1234U, "admin", "ALTER TABLE default SET checkpoint_updates = 512"};
    Fixture() {
        StoreConfig config;
        config.data_dir = root;
        config.geometry = {2U, 2U, 2U, 1U, 8U};
        { TableCatalog catalog(CatalogConfigFromStoreConfig(config)); }
        auto manifest = *ReadDataDirManifest(root);
        data_id = manifest.data_dir_id;
        manifest.features.incompat |= kFeatureMigrations;
        Save(DataDirManifestPath(root), SerializeDataDirManifest(manifest));
        table = *ReadStoreManifest(root / "tables/default");
        before = SerializeStoreManifest(table);
        auto next = table;
        auto options = DecodeTableOptions(next.options);
        options.checkpoint_update_interval = 512U;
        next.options = EncodeTableOptions(options);
        after = SerializeStoreManifest(next);
    }
    MigrationJournal Journal() const {
        MigrationJournal journal;
        journal.data_dir_id = data_id; journal.record = record;
        journal.table = "default"; journal.table_id = table.store_id;
        journal.files = {{"tables/default/table.manifest", before, after},
                         {std::string(kMigrationsFileName), std::nullopt, EncodeMigrationRecords(data_id, {record})}};
        return journal;
    }
    std::pair<VerifyCounters, std::string> Verify() const {
        std::ostringstream output;
        const auto counters = VerifyDataDirectory(root, output);
        return {counters, output.str()};
    }
};

void Records() {
    Fixture fixture;
    assert(ReadMigrationRecords(fixture.root).empty());
    auto second = fixture.record;
    second.name = std::string(63U, 'a'); second.user.clear(); second.applied_ms = 1U;
    second.statement = "ALTER TABLE default  SET checkpoint_updates = 512";
    const std::vector<MigrationRecord> records{fixture.record, second};
    const auto bytes = EncodeMigrationRecords(fixture.data_id, records);
    assert(DecodeMigrationRecords(bytes, fixture.data_id) == records); // Wall clock need not grow.
    Save(fixture.root / kMigrationsFileName, bytes);
    assert(ReadMigrationRecords(fixture.root) == records);
    assert(fixture.Verify().first.errors == 0U);
    auto foreign = fixture.data_id; foreign[0] ^= 1U;
    Reject([&] { (void)DecodeMigrationRecords(bytes, foreign); });
    Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {fixture.record, fixture.record}); });
    for (const auto& name : {"", "9name", "UPPER", "a/b", "a b", "a'quote"})
        Reject([&] { RequireValidMigrationName(name); });
    Reject([&] { RequireValidMigrationName(std::string(64U, 'a')); });
    auto empty = fixture.record; empty.statement.clear();
    Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {empty}); });
    auto newline = fixture.record; newline.statement += "\nPING";
    Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {newline}); });
}

void RecordTextWithoutGrammar() {
    Fixture fixture;
    auto record = fixture.record;
    record.statement = "FUTURE SCHEMA STATEMENT 'данные'";
    const auto encoded = EncodeMigrationRecords(fixture.data_id, {record});
    assert(DecodeMigrationRecords(encoded, fixture.data_id) == std::vector<MigrationRecord>{record});
    Save(fixture.root / kMigrationsFileName, encoded);
    assert(ReadMigrationRecords(fixture.root) == std::vector<MigrationRecord>{record});
    StoreConfig config; config.data_dir = fixture.root; config.geometry_fields = 0U;
    {
        auto catalog = std::make_shared<TableCatalog>(CatalogConfigFromStoreConfig(config));
        CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
        SessionState session;
        assert(engine.Execute(session, "HELLO 3").front() == '%');
        assert(engine.Execute(session, "MIGRATE 'set_options' ALTER TABLE default SET checkpoint_updates = 512").rfind("-ERR CONFLICT ", 0U) == 0U);
        assert(engine.Execute(session, "MIGRATE 'new_step' ALTER TABLE default SET checkpoint_updates = 1024") == "+applied\r\n");
        assert(catalog->Migrations().size() == 2U && catalog->Migrations().front() == record);
    }
    assert(fixture.Verify().first.errors == 0U);
}

void RecordTextUtf8() {
    Fixture fixture;
    const std::string prefix = "ALTER TABLE default ADD COLUMN label text(128) NULL DEFAULT '";
    for (const auto& invalid : {std::string("\xc0\xaf", 2), std::string("\xed\xa0\x80", 3),
                               std::string("\xf4\x90\x80\x80", 4), std::string("\xe2\x82", 2),
                               std::string("\x80", 1)}) {
        auto bad = fixture.record; bad.statement = prefix + invalid + "'";
        Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {bad}); });
        auto good = bad; good.statement = prefix + std::string(invalid.size(), 'a') + "'";
        auto bytes = EncodeMigrationRecords(fixture.data_id, {good});
        const auto at = 28U + 20U + good.name.size() + good.user.size() + prefix.size();
        std::copy(invalid.begin(), invalid.end(), bytes.begin() + static_cast<std::ptrdiff_t>(at));
        Checksum(bytes);
        Reject([&] { (void)DecodeMigrationRecords(bytes, fixture.data_id); });
    }
    auto maximum = fixture.record; maximum.statement = std::string(65536U, 'x');
    assert(DecodeMigrationRecords(EncodeMigrationRecords(fixture.data_id, {maximum}), fixture.data_id)[0] == maximum);
    maximum.statement += 'x';
    Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {maximum}); });
    for (const char forbidden : {'\r', '\n', '\0'}) {
        auto bad = fixture.record; bad.statement += forbidden;
        Reject([&] { (void)EncodeMigrationRecords(fixture.data_id, {bad}); });
    }
}

void RecordDamage() {
    Fixture fixture;
    const auto valid = EncodeMigrationRecords(fixture.data_id, {fixture.record});
    for (std::size_t cut = 0; cut < valid.size(); ++cut) {
        Bytes short_bytes(valid.begin(), valid.begin() + static_cast<std::ptrdiff_t>(cut));
        Reject([&] { (void)DecodeMigrationRecords(short_bytes, fixture.data_id); });
    }
    for (const auto offset : {0U, 4U, 6U, 8U, 24U, 28U}) {
        auto corrupt = valid; corrupt[offset] ^= 0x80U; Checksum(corrupt);
        Reject([&] { (void)DecodeMigrationRecords(corrupt, fixture.data_id); });
    }
    auto corrupt = valid; corrupt.back() ^= 1U;
    Save(fixture.root / kMigrationsFileName, corrupt);
    assert(fixture.Verify().second.find("migration_records_invalid") != std::string::npos);
    assert(LoadFile(fixture.root / kMigrationsFileName) == corrupt);
    auto trailing = valid; trailing.insert(trailing.end() - 4, 0U); Checksum(trailing);
    Reject([&] { (void)DecodeMigrationRecords(trailing, fixture.data_id); });
    auto second = fixture.record; second.name = "set_optiooz"; // Same length as first name.
    auto duplicate = EncodeMigrationRecords(fixture.data_id, {fixture.record, second});
    const auto first_bytes = 20U + fixture.record.name.size() + fixture.record.user.size() + fixture.record.statement.size();
    std::copy(fixture.record.name.begin(), fixture.record.name.end(), duplicate.begin() + static_cast<std::ptrdiff_t>(28U + first_bytes + 4U));
    Checksum(duplicate);
    Reject([&] { (void)DecodeMigrationRecords(duplicate, fixture.data_id); });
}

void JournalAndVerification() {
    Fixture fixture;
    const auto journal = fixture.Journal();
    const auto encoded = EncodeMigrationJournal(journal);
    const auto decoded = DecodeMigrationJournal(encoded);
    assert(decoded.data_dir_id == journal.data_dir_id && decoded.record == journal.record);
    assert(decoded.files.size() == 2U && decoded.files.front().after == fixture.after);
    ValidateMigrationJournal(fixture.root, decoded);
    bool published = false;
    WriteMigrationJournal(fixture.root, journal, &published); assert(published);
    assert(ReadMigrationJournal(fixture.root)->record == fixture.record);
    const auto [counts, output] = fixture.Verify();
    assert(counts.errors == 0U && output.find("migration_recovery_pending") != std::string::npos);
    assert(LoadFile(fixture.root / "tables/default/table.manifest") == fixture.before);
    assert(LoadFile(fixture.root / kMigrationPendingFileName) == encoded); // Verifier never completes it.
    Save(fixture.root / "tables/default/table.manifest", fixture.after);
    ValidateMigrationJournal(fixture.root, journal); // Partial publication is resumable.
    Save(fixture.root / kMigrationsFileName, journal.files.back().after);
    ValidateMigrationJournal(fixture.root, journal); // Completed publication is resumable too.
}

void JournalDamage() {
    Fixture fixture;
    const auto valid = fixture.Journal();
    const auto encoded = EncodeMigrationJournal(valid);
    for (std::size_t cut = 0; cut < encoded.size(); ++cut) {
        Bytes short_bytes(encoded.begin(), encoded.begin() + static_cast<std::ptrdiff_t>(cut));
        Reject([&] { (void)DecodeMigrationJournal(short_bytes); });
    }
    for (const auto path : {"../chunkdb.users", "/chunkdb.users", "tables/other/table.manifest", "tables/default/../../chunkdb.users"}) {
        auto bad = valid; bad.files.front().path = path;
        Reject([&] { (void)EncodeMigrationJournal(bad); });
    }
    auto bad = valid; bad.files.erase(bad.files.begin()); // Ledger alone must not claim ALTER.
    Reject([&] { (void)EncodeMigrationJournal(bad); });
    bad = valid; std::swap(bad.files.front(), bad.files.back());
    Reject([&] { (void)EncodeMigrationJournal(bad); });
    bad = valid; bad.files.insert(bad.files.begin(), bad.files.front());
    Reject([&] { (void)EncodeMigrationJournal(bad); });
    bad = valid; bad.files.back().after = EncodeMigrationRecords(fixture.data_id, {});
    Reject([&] { (void)EncodeMigrationJournal(bad); });
    bad = valid; bad.table_id[0] ^= 1U;
    Reject([&] { (void)EncodeMigrationJournal(bad); });
    auto corrupted = encoded; corrupted.back() ^= 1U;
    Save(fixture.root / kMigrationPendingFileName, corrupted);
    assert(fixture.Verify().second.find("migration_pending_invalid") != std::string::npos);
    assert(LoadFile(fixture.root / kMigrationPendingFileName) == corrupted);
}

void DiskIdentity() {
    Fixture fixture;
    const auto journal = fixture.Journal();
    auto foreign = journal; foreign.data_dir_id[0] ^= 1U;
    Reject([&] { ValidateMigrationJournal(fixture.root, foreign); });
    auto third = fixture.table;
    auto options = DecodeTableOptions(third.options); options.checkpoint_update_interval = 1024U;
    third.options = EncodeTableOptions(options);
    Save(fixture.root / "tables/default/table.manifest", SerializeStoreManifest(third));
    Reject([&] { ValidateMigrationJournal(fixture.root, journal); });
    Save(fixture.root / "tables/default/table.manifest", fixture.before);
    auto manifest = *ReadDataDirManifest(fixture.root); manifest.features.incompat &= ~kFeatureMigrations;
    Save(DataDirManifestPath(fixture.root), SerializeDataDirManifest(manifest));
    Reject([&] { ValidateMigrationJournal(fixture.root, journal); });
}

void CompletionPreflight() {
    Fixture fixture;
    const auto journal = fixture.Journal();
    WriteMigrationJournal(fixture.root, journal);
    auto unrelated = fixture.record; unrelated.name = "other_step";
    const auto unexpected = EncodeMigrationRecords(fixture.data_id, {unrelated});
    Save(fixture.root / kMigrationsFileName, unexpected);
    const auto pending = LoadFile(fixture.root / kMigrationPendingFileName);
    Reject([&] { CompleteMigrationJournal(fixture.root, journal); });
    assert(LoadFile(fixture.root / "tables/default/table.manifest") == fixture.before);
    assert(LoadFile(fixture.root / kMigrationsFileName) == unexpected);
    assert(LoadFile(fixture.root / kMigrationPendingFileName) == pending);
}

void DirectoryPublication() {
    Fixture fixture;
    MigrationJournal journal;
    journal.data_dir_id = fixture.data_id;
    journal.record = {"create_world", 100U, "", "CREATE TABLE world (n u8) CHUNK 2 x 2"};
    journal.directory_action = MigrationDirectoryAction::kCreate;
    journal.table = "world"; journal.table_id = NewStoreId(); journal.operation_name = "world.0123456789abcdef";
    auto manifest = fixture.table; manifest.store_id = journal.table_id;
    manifest.schema.columns.front().name = "n";
    manifest.schema.columns.front().type = {ColumnKind::kUnsigned, 8U};
    manifest.geometry.chunk_height_blocks = 2U;
    const auto image = SerializeStoreManifest(manifest);
    const auto stage = fixture.root / ".chunkdb.staging" / journal.operation_name;
    Save(stage / kStoreManifestFileName, image);
    journal.files = {{"tables/world/table.manifest", std::nullopt, image},
                     {std::string(kMigrationsFileName), std::nullopt, EncodeMigrationRecords(fixture.data_id, {journal.record})}};
    ValidateMigrationJournal(fixture.root, journal);
    std::filesystem::rename(stage, fixture.root / "tables/world");
    ValidateMigrationJournal(fixture.root, journal);
    std::filesystem::create_directory(stage);
    Reject([&] { ValidateMigrationJournal(fixture.root, journal); }); // Both names is ambiguous.
    std::filesystem::remove(stage);
    std::filesystem::rename(fixture.root / "tables/world", stage);
    manifest.store_id = NewStoreId(); Save(stage / kStoreManifestFileName, SerializeStoreManifest(manifest));
    Reject([&] { ValidateMigrationJournal(fixture.root, journal); });
}

void PathAliases() {
#ifndef _WIN32
    Fixture fixture;
    const auto alias = fixture.directory.path() / "alias";
    std::filesystem::create_directory_symlink(fixture.root, alias);
    ValidateMigrationJournal(alias, fixture.Journal()); // Trusted root itself may be an alias.
    const auto real = fixture.root / "tables/default/table.manifest";
    const auto saved = fixture.root / "saved.manifest";
    std::filesystem::rename(real, saved); std::filesystem::create_symlink(saved, real);
    Reject([&] { ValidateMigrationJournal(fixture.root, fixture.Journal()); });
    std::filesystem::remove(real); std::filesystem::rename(saved, real);
    const auto tables = fixture.root / "tables";
    std::filesystem::rename(tables, fixture.root / "saved-tables");
    std::filesystem::create_directory_symlink(fixture.root / "saved-tables", tables);
    Reject([&] { ValidateMigrationJournal(fixture.root, fixture.Journal()); });
#endif
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--record-grammar") { RecordTextWithoutGrammar(); return 0; }
    if (argc == 2 && std::string(argv[1]) == "--record-utf8") { RecordTextUtf8(); return 0; }
    RecordTextWithoutGrammar(); RecordTextUtf8();
    Records(); RecordDamage(); JournalAndVerification(); JournalDamage(); DiskIdentity(); CompletionPreflight(); DirectoryPublication(); PathAliases();
    std::cout << "10 migration codec and verification groups passed\n";
}
