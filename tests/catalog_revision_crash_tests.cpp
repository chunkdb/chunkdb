// A crash around a catalog statement (docs/design/CATALOG_REVISION.md): a
// start after a crash between the statement's mark and its revision takes a
// new revision, so a consumer at the old one rebuilds; a start after the
// revision was recorded keeps it, and a consumer there continues.
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "migrations_test_utils.hpp"
#include "../src/catalog_feed.hpp"
#include "../src/catalog_revision.hpp"
#include "../src/chunk_store_internal.hpp"
#include "../src/slot_watch.hpp"
#include "../src/store_manifest.hpp"
#include "../src/verify.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::migration_test;

constexpr const char* kStatement = "CREATE TABLE crash_t (n u8) CHUNK 4 x 4";

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

int CrashChild(const std::filesystem::path& root, const std::string& point) {
    Engine e(root, true);
    txn_test::ScopedEnv armed(point.c_str(), "1");
    (void)e.Run(kStatement);
    return 3;  // Every named point must have ended the process.
}

struct Point {
    const char* name;
    // The statement's change is on disk after the crash.
    bool table;
    // Its revision was recorded before the crash.
    bool revision;
};
const Point kPoints[] = {
    {"CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_MARK_ONCE", false, false},
    {"CHUNKDB_FAILPOINT_CRASH_TABLE_CREATE_AFTER_PUBLISH_ONCE", true, false},
    {"CHUNKDB_FAILPOINT_CRASH_CATALOG_AFTER_REVISION_ONCE", true, true},
};

void Case(const std::string& executable, const Point& point) {
    test::ScopedTempDir dir("chunkdb-catalog-crash");
    {
        Engine seed(dir.path(), true);
        Reply(seed.Run("CREATE TABLE seed (n u8) CHUNK 4 x 4"), "+OK\r\n");
    }
    const auto before = *ReadCatalogRevision(dir.path());
    assert(!before.in_progress);
    std::cout << point.name << '\n' << std::flush;
    assert(RunChild(executable, {"--crash", dir.path().string(), point.name}) == 86);
    const auto crashed = *ReadCatalogRevision(dir.path());
    assert(crashed.epoch == before.epoch && crashed.in_progress == !point.revision);
    assert(crashed.revision == before.revision + (point.revision ? 1U : 0U));

    Engine e(dir.path(), true);
    assert((e.catalog->Find("crash_t") != nullptr) == point.table);
    // A consumer at the last revision recorded before the crash.
    const auto position = StoreIdHex(before.epoch) + " " + std::to_string(crashed.revision);
    const auto reply = e.Run("WATCH CATALOG AFTER " + position);
    assert(reply.rfind("%2\r\n", 0) == 0);
    // The read recorded the revision it took: a mark that was not cleared
    // moved the start to a new one.
    const auto started = *ReadCatalogRevision(dir.path());
    assert(started.epoch == before.epoch && !started.in_progress && started.revision == before.revision + 1U);
    const auto first = e.session.catalog_watch->Next();
    if (point.revision) {
        // Its revision names the catalog with the table: it continues.
        assert(!first);
    } else {
        // The catalog may differ from what that revision named: rebuild.
        assert(first && first->bytes->find("$6\r\nresync\r\n") != std::string::npos);
        const auto whole = e.session.catalog_watch->Next();
        assert(whole && (whole->bytes->find("$7\r\ncrash_t\r\n") != std::string::npos) == point.table);
    }
}

// chunkdb_verify reads the record, and reports a damaged one as a warning: the
// next writer start replaces it.
void VerifyRecord() {
    test::ScopedTempDir dir("chunkdb-catalog-verify");
    {
        Engine e(dir.path(), true);
        Reply(e.Run("CREATE TABLE seed (n u8) CHUNK 4 x 4"), "+OK\r\n");
    }
    std::ostringstream clean;
    const auto checked = VerifyDataDirectory(dir.path(), clean);
    const auto clean_text = clean.str();
    assert(checked.errors == 0U);
    // No finding names the record.
    assert(clean_text.find("chunkdb.catalog") == std::string::npos);
    {
        std::ofstream damage(dir.path() / std::string(kCatalogRevisionFileName), std::ios::binary | std::ios::app);
        damage << 'x';
    }
    std::ostringstream damaged;
    const auto found = VerifyDataDirectory(dir.path(), damaged);
    const auto damaged_text = damaged.str();
    assert(found.errors == 0U && found.warnings == checked.warnings + 1U);
    assert(damaged_text.find("catalog_revision_invalid") != std::string::npos);
    // The next catalog statement replaces it with a new epoch.
    Engine e(dir.path(), true);
    Reply(e.Run("CREATE TABLE later (n u8) CHUNK 4 x 4"), "+OK\r\n");
    assert(ReadCatalogRevision(dir.path()).has_value());
}

// Only an attach that records a slot's owner changes the catalog: it takes one
// revision, and an attach that records nothing leaves the clock alone.
void SlotOwnerRevision() {
    test::ScopedTempDir dir("chunkdb-catalog-slot-owner");
    {
        // Without users a slot records no owner.
        Engine open(dir.path(), false);
        Reply(open.Run("CREATE TABLE realm (n u8) CHUNK 4 x 4"), "+OK\r\n");
        Reply(open.Run("CREATE SLOT 'a' ON realm"), "+OK\r\n");
        Reply(open.Run("CREATE SLOT 'b' ON realm"), "+OK\r\n");
    }
    Engine e(dir.path(), true);
    e.users->Create("reader", scram::MakeVerifier("pw", crypto::RandomBytes(16), scram::kMinIterations), false);
    e.users->Grant("reader", "realm", Right::kRead);
    const auto revision = [&] { return ReadCatalogRevision(dir.path())->revision; };
    const auto before = revision();
    // ADMIN on * records nobody.
    assert(e.Run("WATCH realm SLOT 'b'").rfind("%2\r\n", 0) == 0);
    assert(revision() == before);
    SessionState reader;
    assert(test::LoginOnEngine(*e.engine, reader, "reader", "pw").rfind("%", 0) == 0);
    assert(e.engine->Execute(reader, "WATCH realm SLOT 'a'\r\n").rfind("%2\r\n", 0) == 0);
    assert(reader.slot_watch && reader.slot_watch->owner() == "reader");
    assert(revision() == before + 1U);
    assert(!ReadCatalogRevision(dir.path())->in_progress);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--crash") return CrashChild(argv[2], argv[3]);
    for (const auto& point : kPoints) Case(argv[0], point);
    VerifyRecord();
    SlotOwnerRevision();
    std::cout << "catalog revision crash cases: " << std::size(kPoints) << '\n';
}
