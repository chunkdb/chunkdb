#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "migrations_test_utils.hpp"
#include "../src/chunk_store_internal.hpp"
#include "../src/feed_slot_records.hpp"
#include "../src/store_manifest.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;

struct Operation { const char* name; const char* statement; std::size_t files; bool directory; };
const Operation kOperations[] = {
    {"noop_create", "CREATE TABLE IF NOT EXISTS realm (ignored i8 DEFAULT 1000) CHUNK 8 x 8", 1U, false},
    {"noop_add", "ALTER TABLE realm ADD COLUMN IF NOT EXISTS v u8", 1U, false},
    {"noop_drop_column", "ALTER TABLE realm DROP COLUMN IF EXISTS absent", 1U, false},
    {"noop_create_slot", "CREATE SLOT IF NOT EXISTS 'first' ON realm", 1U, false},
    {"noop_drop_slot", "DROP SLOT IF EXISTS 'absent' ON realm", 1U, false},
    {"noop_drop", "DROP TABLE IF EXISTS absent", 1U, false},
    {"create", kCreate, 2U, true},
    {"add", "ALTER TABLE realm ADD COLUMN extra u8 NULL", 2U, false},
    {"rename", "ALTER TABLE realm RENAME COLUMN v TO renamed", 2U, false},
    {"drop_column", "ALTER TABLE realm DROP COLUMN extra", 2U, false},
    {"narrow", "ALTER TABLE realm ALTER COLUMN v TYPE u8", 2U, false},
    {"clamp", "ALTER TABLE realm ALTER COLUMN v TYPE u8 USING CLAMP", 2U, false},
    {"option", "ALTER TABLE realm SET checkpoint_updates = 64", 2U, false},
    {"drop", "DROP TABLE realm", 3U, true},
    {"grant", "GRANT WRITE ON realm TO limited", 2U, false},
    {"revoke", "REVOKE READ ON realm FROM limited", 2U, false},
    {"create_slot", "CREATE SLOT 'second' ON realm", 3U, false},
    {"drop_slot", "DROP SLOT 'second' ON realm", 2U, false},
};

const Operation& Find(const std::string& name) {
    for (const auto& op : kOperations) if (op.name == name) return op;
    std::abort();
}

int RunChild(const std::string& executable, const std::vector<std::string>& arguments) {
    std::string command = "\"" + executable + "\"";
    for (const auto& argument : arguments) command += " \"" + argument + "\"";
#ifdef _WIN32
    command = "\"" + command + "\"";
    return std::system(command.c_str());
#else
    const auto result = std::system(command.c_str());
    return WIFEXITED(result) ? WEXITSTATUS(result) : -1;
#endif
}

int CrashChild(const std::filesystem::path& root, const Operation& operation, const std::string& point) {
    Engine e(root, true);
    txn_test::ScopedEnv armed(point.c_str(), "1");
    const auto reply = e.Run(std::string("MIGRATE 'once' ") + operation.statement);
    if (point.find("SYNC_FAIL") != std::string::npos) {
        Error(reply, "INTERNAL");
        assert(ReadMigrationJournal(root));
        std::_Exit(86);
    }
    return 3;  // Every named point must have actually terminated the process.
}

StoreId Seed(const std::filesystem::path& root, const Operation& operation) {
    Engine e(root, true);
    e.users->Create("limited", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("limited", "realm", Right::kRead);
    if (std::string(operation.name) == "create") return {};
    Reply(e.Run(kCreate), "+OK\r\n");
    assert(e.Run(std::string("SET BLOCK 0 0 IN realm v=") + (std::string(operation.name) == "clamp" ? "300" : "7")).front() == ':');
    Reply(e.Run("FLUSH WAL"), "+OK\r\n");
    if (std::string(operation.name) == "drop_column") Reply(e.Run("ALTER TABLE realm ADD COLUMN extra u8 NULL"), "+OK\r\n");
    if (std::string(operation.name).find("slot") != std::string::npos) {
        Reply(e.Run("CREATE SLOT 'first' ON realm"), "+OK\r\n");
        if (std::string(operation.name) == "drop_slot") Reply(e.Run("CREATE SLOT 'second' ON realm"), "+OK\r\n");
    }
    return e.catalog->Find("realm")->store_id();
}

void Check(const std::filesystem::path& root, const Operation& operation, const StoreId& before, bool applied) {
    Engine e(root, true);
    assert(!ReadMigrationJournal(root));
    const auto records = e.catalog->Migrations();
    assert(records.size() == (applied ? 1U : 0U));
    if (applied) {
        assert(records[0].name == "once" && records[0].statement == operation.statement && records[0].user == "admin");
        Reply(e.Run(std::string("MIGRATE 'once' ") + operation.statement), "+skipped\r\n");
    }
    const std::string name = operation.name;
    const auto table = e.catalog->Find("realm");
    if (name == "create") {
        assert(static_cast<bool>(table) == applied);
        if (applied) {
            const auto info = table->Info();
            assert(info.schema.version == 1U && info.schema.columns.size() == 1U);
            assert(info.schema.columns.front().name == "v" && info.schema.columns.front().type.size == 16U);
            assert(info.geometry.chunk_width_blocks == 2U && info.geometry.chunk_height_blocks == 2U);
            assert(info.geometry.large_chunk_width_chunks == 1U && info.geometry.large_chunk_height_chunks == 1U);
            const auto image = ReadStoreManifest(root / "tables" / "realm");
            assert(image && image->store_id == table->store_id());
        }
        return;
    }
    if (name == "drop") {
        assert(static_cast<bool>(table) != applied);
        assert(e.users->Find("limited")->grants.contains("realm") != applied);
        return;
    }
    assert(table && table->store_id() == before);
    const auto info = table->Info();
    if (name.starts_with("noop_")) {
        assert(info.schema.version == 1U && info.schema.columns.size() == 1U);
        assert(info.schema.columns.front().name == "v" && info.schema.columns.front().type.size == 16U);
        assert(e.users->Find("limited")->grants.at("realm") == Right::kRead);
        if (name.find("slot") != std::string::npos) {
            const auto slots = table->ListFeedSlots();
            assert(slots.size() == 1U && slots.front().name == "first");
        }
        assert(e.Run("GET BLOCK 0 0 FROM realm COLUMNS v").find(":7\r\n") != std::string::npos);
        assert(!e.catalog->Find("absent"));
        return;
    }
    if (name == "add" || name == "rename" || name == "narrow" || name == "clamp") {
        assert(info.schema.version == (applied ? 2U : 1U));
        if (name == "add") assert(info.schema.columns.size() == (applied ? 2U : 1U));
        if (name == "rename") assert(info.schema.columns.front().name == (applied ? "renamed" : "v"));
        if (name == "narrow" || name == "clamp") assert(info.schema.columns.front().type.size == (applied ? 8U : 16U));
    } else if (name == "drop_column") {
        assert(info.schema.version == (applied ? 3U : 2U));
        assert(info.schema.columns.size() == (applied ? 1U : 2U));
    } else if (name == "option") {
        assert(info.options.checkpoint_update_interval == (applied ? 64U : Config(root).default_options.checkpoint_update_interval));
    } else if (name == "grant") {
        assert(e.users->Find("limited")->grants.at("realm") == (applied ? Right::kWrite : Right::kRead));
    } else if (name == "revoke") {
        assert(e.users->Find("limited")->grants.contains("realm") != applied);
    } else {
        const auto slots = table->ListFeedSlots();
        const auto expected = name == "create_slot" ? (applied ? 2U : 1U) : (applied ? 1U : 2U);
        assert(slots.size() == expected && slots[0].name == "first");
    }
    const auto read = e.Run(name == "rename" && applied ? "GET BLOCK 0 0 FROM realm COLUMNS renamed" : "GET BLOCK 0 0 FROM realm COLUMNS v");
    const auto value = name == "clamp" ? (applied ? 255U : 300U) : 7U;
    assert(read.find(":" + std::to_string(value) + "\r\n") != std::string::npos);
}

void Matrix(const std::string& executable) {
    std::size_t cases = 0;
    for (const auto& operation : kOperations) {
        std::vector<std::string> points{
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_DECISION_ONCE",
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_AFTER_DECISION_ONCE",
            "CHUNKDB_FAILPOINT_MIGRATION_DECISION_SYNC_FAIL_ONCE",
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_PENDING_REMOVE_ONCE",
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_AFTER_PENDING_REMOVE_ONCE"};
        for (std::size_t i = 0; i < operation.files; ++i) {
            const auto base = "CHUNKDB_FAILPOINT_CRASH_MIGRATION_FILE_" + std::to_string(i);
            points.push_back(base + "_BEFORE_PUBLISH_ONCE"); points.push_back(base + "_AFTER_PUBLISH_ONCE");
            points.push_back("CHUNKDB_FAILPOINT_MIGRATION_FILE_" + std::to_string(i) + "_SYNC_FAIL_ONCE");
        }
        if (operation.directory) {
            points.push_back("CHUNKDB_FAILPOINT_CRASH_MIGRATION_DIRECTORY_BEFORE_PUBLISH_ONCE");
            points.push_back("CHUNKDB_FAILPOINT_CRASH_MIGRATION_DIRECTORY_AFTER_PUBLISH_ONCE");
        }
        for (const auto& point : points) {
            test::ScopedTempDir dir("chunkdb-migration-crash");
            const auto before = Seed(dir.path(), operation);
            const bool no_op = std::string(operation.name).starts_with("noop_");
            const auto table_before = no_op ? LoadFile(dir.path() / "tables/realm/table.manifest") : std::vector<std::uint8_t>{};
            const auto slots_before = no_op && std::string(operation.name).find("slot") != std::string::npos ?
                LoadFile(dir.path() / "tables/realm/chunkdb.slots") : std::vector<std::uint8_t>{};
            std::cout << operation.name << ' ' << point << '\n' << std::flush;
            assert(RunChild(executable, {"--crash", dir.path().string(), operation.name, point}) == 86);
            const bool applied = point != "CHUNKDB_FAILPOINT_CRASH_MIGRATION_BEFORE_DECISION_ONCE";
            std::optional<StoreId> planned_id;
            if (const auto pending = ReadMigrationJournal(dir.path())) planned_id = pending->table_id;
            Check(dir.path(), operation, before, applied);
            if (no_op) {
                assert(LoadFile(dir.path() / "tables/realm/table.manifest") == table_before);
                if (!slots_before.empty()) assert(LoadFile(dir.path() / "tables/realm/chunkdb.slots") == slots_before);
            }
            const auto first_image = ReadStoreManifest(dir.path() / "tables" / "realm");
            Check(dir.path(), operation, before, applied);
            if (std::string(operation.name) == "create" && applied) {
                const auto second_image = ReadStoreManifest(dir.path() / "tables" / "realm");
                assert(first_image && second_image && first_image->store_id == second_image->store_id);
                if (planned_id) assert(first_image->store_id == *planned_id);
            }
            ++cases;
        }
    }
    std::cout << "migration crash cases: " << cases << '\n';
}

void RecoveryCrash(const std::string& executable) {
    for (const auto& operation : kOperations) {
        test::ScopedTempDir dir("chunkdb-migration-recovery-crash");
        const auto before = Seed(dir.path(), operation);
        assert(RunChild(executable, {"--crash", dir.path().string(), operation.name,
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_AFTER_DECISION_ONCE"}) == 86);
        const auto image = LoadFile(dir.path() / kMigrationPendingFileName);
        auto read_only = Config(dir.path()); read_only.access_mode = AccessMode::kReadOnly;
        bool refused = false;
        try { TableCatalog catalog(read_only); } catch (const std::runtime_error&) { refused = true; }
        assert(refused && LoadFile(dir.path() / kMigrationPendingFileName) == image);
        auto multi = Config(dir.path()); multi.allow_multiple_processes = true;
        refused = false;
        try { TableCatalog catalog(multi); } catch (const std::runtime_error&) { refused = true; }
        assert(refused && LoadFile(dir.path() / kMigrationPendingFileName) == image);
        assert(RunChild(executable, {"--recover", dir.path().string(), operation.name,
            "CHUNKDB_FAILPOINT_CRASH_MIGRATION_FILE_0_AFTER_PUBLISH_ONCE"}) == 86);
        Check(dir.path(), operation, before, true);
        Check(dir.path(), operation, before, true);
    }
}

}  // namespace
int main(int argc, char** argv) {
    if (argc == 5 && std::string(argv[1]) == "--crash") return CrashChild(argv[2], Find(argv[3]), argv[4]);
    if (argc == 5 && std::string(argv[1]) == "--recover") {
        txn_test::ScopedEnv armed(argv[4], "1");
        Engine e(argv[2], true);
        return 3;
    }
    Matrix(argv[0]); RecoveryCrash(argv[0]);
}
