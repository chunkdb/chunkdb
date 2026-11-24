// chunkdb_restore --table [--as] [--area ... TO ...] [--allow-split]
// (docs/BACKUP.md): one table of a backup, or the chunks of an area, added
// offline to a data directory as a new table; an area that splits a commit
// needs --allow-split.

#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"
#include "verify.hpp"

namespace {
using namespace chunkdb;

bool Contains(const std::string& text, std::string_view part) { return text.find(part) != std::string::npos; }

void Reply(const std::string& actual, std::string_view expected) {
    if (actual != expected) {
        std::cerr << "expected " << expected << ", received " << actual << '\n';
        assert(false);
    }
}

int ExitCode(int status) {
#ifdef _WIN32
    return status;
#else
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
#endif
}

struct Fixture {
    std::string restore;
    test::ScopedTempDir directory{"chunkdb-restore-table-area"};
    std::filesystem::path root = std::filesystem::canonical(directory.path());
    std::filesystem::path source = root / "source", backups = root / "backups", backup = backups / "b";
    std::uint64_t split = 0U;

    // Table t: chunks (0,0) and (1,0), a transaction over (0,0) and (3,0),
    // chunk (5,5), and a tombstone at (4,4).
    explicit Fixture(std::string executable) : restore(std::move(executable)) {
        std::filesystem::create_directories(backups);
        std::filesystem::permissions(backups, std::filesystem::perms::owner_all);
        CatalogConfig config;
        config.data_dir = source;
        auto catalog = std::make_shared<TableCatalog>(config);
        CommandEngine engine(EngineConfig{.require_auth = false, .backup_dir = backups}, catalog);
        SessionState session;
        const auto run = [&](std::string_view statement) { return engine.Execute(session, std::string(statement) + "\r\n"); };
        assert(run("HELLO 3").starts_with("%"));
        Reply(run("CREATE TABLE t (a u8 DEFAULT 0) CHUNK 2 x 2"), "+OK\r\n");
        for (const auto* statement : {"SET BLOCK 0 0 IN t a = 1", "SET BLOCK 2 0 IN t a = 2", "SET BLOCK 10 10 IN t a = 3",
                 "SET BLOCK 8 8 IN t a = 4", "DELETE BLOCK 8 8 FROM t"})
            assert(run(statement).starts_with(":"));
        {
            auto lease = catalog->Require("t")->Acquire();
            lease->store().CheckpointForTests(4, 4);
        }
        Reply(run("BEGIN"), "+OK\r\n");
        assert(!run("SET BLOCK 0 1 IN t a = 5").starts_with("-ERR"));
        assert(!run("SET BLOCK 6 0 IN t a = 6").starts_with("-ERR"));
        const auto commit = run("COMMIT");
        assert(!commit.starts_with("-ERR"));
        split = catalog->Require("t")->Acquire()->store().GetChunkVersion(3, 0);
        assert(split == catalog->Require("t")->Acquire()->store().GetChunkVersion(0, 0));
        assert(run("SHOW TABLE STATS t").find("$10\r\ntombstones\r\n:1\r\n") != std::string::npos);
        assert(run("BACKUP TO 'b'").starts_with("%4\r\n"));
    }
    int Run(const std::string& args, std::string* output) const {
        const auto log = root / "restore.log";
        std::string command = "\"" + restore + "\" " + args + " > \"" + log.string() + "\" 2>&1";
#ifdef _WIN32
        command = "\"" + command + "\"";
#endif
        const int status = std::system(command.c_str());
        std::ifstream input(log, std::ios::binary);
        output->assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
        return ExitCode(status);
    }
    [[nodiscard]] std::string Args(const std::string& options, const std::filesystem::path& target) const {
        return options + " \"" + backup.string() + "\" \"" + target.string() + "\"";
    }
};

// Statements against `data`, whose catalog opens for each call.
std::string Query(const std::filesystem::path& data, std::string_view statement) {
    CatalogConfig config;
    config.data_dir = data;
    auto catalog = std::make_shared<TableCatalog>(config);
    CommandEngine engine(EngineConfig{.require_auth = false}, catalog);
    SessionState session;
    assert(engine.Execute(session, "HELLO 3\r\n").starts_with("%"));
    return engine.Execute(session, std::string(statement) + "\r\n");
}

void WholeTable(const Fixture& f) {
    std::string output;
    const auto target = f.root / "whole";
    assert(f.Run(f.Args("--table t", target), &output) == 0);
    assert(Contains(output, "restored table t as t: epoch ") && !Contains(output, "split commits"));
    Reply(Query(target, "GET BLOCK 10 10 FROM t"), "*1\r\n:3\r\n");
    Reply(Query(target, "GET BLOCK 6 0 FROM t"), "*1\r\n:6\r\n");
    assert(Query(target, "SHOW TABLE STATS t").find("$10\r\ntombstones\r\n:1\r\n") != std::string::npos);
    std::ostringstream findings;
    assert(VerifyDataDirectory(target, findings).errors == 0U);
    // The name is taken now; another one is not.
    assert(f.Run(f.Args("--table t", target), &output) == 1 && Contains(output, "already exists"));
    assert(f.Run(f.Args("--table t --as copy", target), &output) == 0 && Contains(output, "as copy"));
    Reply(Query(target, "GET BLOCK 2 0 FROM copy"), "*1\r\n:2\r\n");
    // A running server holds the writer lock.
    {
        CatalogConfig config;
        config.data_dir = target;
        TableCatalog running(config);
        assert(f.Run(f.Args("--table t --as third", target), &output) == 1);
    }
    assert(f.Run(f.Args("--table missing --as fourth", target), &output) == 1 && Contains(output, "no table 'missing'"));
}

void Areas(const Fixture& f) {
    std::string output;
    const auto target = f.root / "areas";
    // The transaction changed (0,0) inside and (3,0) outside the area.
    assert(f.Run(f.Args("--table t --area 0 0 TO 1 0", target), &output) == 1);
    assert(Contains(output, "splits 1 commits") && Contains(output, std::to_string(f.split)) && Contains(output, "--allow-split"));
    assert(f.Run(f.Args("--table t --as split --area 0 0 TO 1 0 --allow-split", target), &output) == 0);
    assert(Contains(output, "2 chunks in the area") && Contains(output, "split commits: " + std::to_string(f.split)));
    Reply(Query(target, "GET BLOCK 0 1 FROM split"), "*1\r\n:5\r\n");
    Reply(Query(target, "GET BLOCK 2 0 FROM split"), "*1\r\n:2\r\n");
    Reply(Query(target, "GET BLOCK 6 0 FROM split"), "_\r\n");
    Reply(Query(target, "GET BLOCK 10 10 FROM split"), "_\r\n");
    assert(Query(target, "SHOW TABLE STATS split").find("$10\r\ntombstones\r\n:0\r\n") != std::string::npos);
    assert(Query(target, "SHOW TABLE STATS split").find("$6\r\nchunks\r\n:2\r\n") != std::string::npos);
    // An area holding every chunk of the transaction splits nothing.
    assert(f.Run(f.Args("--table t --as whole_row --area 0 0 TO 3 0", target), &output) == 0);
    assert(Contains(output, "3 chunks in the area") && !Contains(output, "split commits"));
    Reply(Query(target, "GET BLOCK 6 0 FROM whole_row"), "*1\r\n:6\r\n");
    // The tombstone at (4,4) stays with an area that holds it.
    assert(f.Run(f.Args("--table t --as corner --area 4 4 TO 5 5", target), &output) == 0);
    assert(Query(target, "SHOW TABLE STATS corner").find("$10\r\ntombstones\r\n:1\r\n") != std::string::npos);
    Reply(Query(target, "GET BLOCK 10 10 FROM corner"), "*1\r\n:3\r\n");
    std::ostringstream findings;
    assert(VerifyDataDirectory(target, findings).errors == 0U);
    assert(f.Run(f.Args("--table t --as cube --area 0 0 0 TO 1 1 1", target), &output) == 1 && Contains(output, "dimensions"));
}

void Usage(const Fixture& f) {
    std::string output;
    const auto target = f.root / "usage";
    assert(f.Run(f.Args("--table t --area 0 0 1 1", target), &output) == 2 && Contains(output, "TO"));
    assert(f.Run(f.Args("--table t --area 1 0 TO 0 0", target), &output) == 2 && Contains(output, "reversed"));
    assert(f.Run(f.Args("--as u", target), &output) == 2 && Contains(output, "need --table"));
    assert(f.Run(f.Args("--allow-split", target), &output) == 2);
    assert(f.Run("--table t", &output) == 2 && Contains(output, "Usage"));
    assert(f.Run(f.Args("--table t", f.backup / "tables"), &output) == 1 && Contains(output, "contain each other"));
    assert(!std::filesystem::exists(target));
}

// A crash after the area is selected leaves no table; the next run restores it.
void Crash(const Fixture& f) {
    std::string output;
    const auto target = f.root / "crash";
    {
        txn_test::ScopedEnv crash("CHUNKDB_FAILPOINT_CRASH_RESTORE_TABLE_AFTER_AREA_ONCE", "1");
        assert(f.Run(f.Args("--table t --area 0 0 TO 3 0", target), &output) == 86);
    }
    Reply(Query(target, "SHOW TABLES"), "*0\r\n");
    assert(f.Run(f.Args("--table t --area 0 0 TO 3 0", target), &output) == 0);
    Reply(Query(target, "GET BLOCK 0 0 FROM t"), "*1\r\n:1\r\n");
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: chunkdb_restore_table_area_test <chunkdb_restore>\n";
        return 2;
    }
    const Fixture f(std::filesystem::absolute(argv[1]).string());
    WholeTable(f);
    Areas(f);
    Usage(f);
    Crash(f);
    std::cout << "restore_table_area tests passed\n";
    return 0;
}
