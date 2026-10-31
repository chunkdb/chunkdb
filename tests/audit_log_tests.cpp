// The audit log, the slow-statement log and the login limits, through the
// engine: every audited event, what the lines hold and leave out, reopening
// the file, and the flags' limits.

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "audit_log.hpp"
#include "catalog_test_utils.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "login_helpers.hpp"
#include "scram.hpp"
#include "test_utils.hpp"
#include "user_registry.hpp"

#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace {

using chunkdb::CommandEngine;
using chunkdb::SessionState;
using Parameters = std::vector<std::optional<std::string>>;

constexpr const char* kAdmin = "admin";
constexpr const char* kPassword = "admin-private-password";
constexpr const char* kPeer = "198.51.100.23";

bool Contains(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

void Expect(bool ok, const std::string& what) {
    if (!ok) {
        std::fprintf(stderr, "audit log test: %s\n", what.c_str());
        assert(false);
    }
}

std::vector<std::string> Lines(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);) lines.push_back(line);
    return lines;
}

std::string ReadAll(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// The one line of `lines` holding every part; fails unless exactly one does.
std::string LineWith(const std::vector<std::string>& lines, const std::vector<std::string>& parts) {
    std::vector<std::string> found;
    for (const auto& line : lines) {
        bool all = true;
        for (const auto& part : parts) all = all && Contains(line, part);
        if (all) found.push_back(line);
    }
    std::string wanted;
    for (const auto& part : parts) wanted += " " + part;
    Expect(found.size() == 1U, "expected one audit line with" + wanted + ", found " + std::to_string(found.size()));
    return found.front();
}

struct Engine {
    chunkdb::test::ScopedTempDir dir{"chunkdb-audit"};
    std::filesystem::path audit_path = dir.path() / "audit" / "audit.log";
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::shared_ptr<chunkdb::UserRegistry> users;
    std::shared_ptr<chunkdb::AuditLog> audit;
    std::unique_ptr<CommandEngine> engine;

    explicit Engine(chunkdb::EngineConfig config = {}, bool with_audit = true) {
        std::filesystem::create_directories(dir.path() / "data");
        std::filesystem::create_directories(dir.path() / "backups");
        std::filesystem::create_directories(audit_path.parent_path());
        chunkdb::CatalogConfig catalog_config;
        catalog_config.data_dir = dir.path() / "data";
        catalog_config.default_options.wal_group_commit_updates = 1U;
        catalog = std::make_shared<chunkdb::TableCatalog>(catalog_config);
        users = chunkdb::test::MakeUsers(catalog_config.data_dir, kAdmin, kPassword);
        if (with_audit) audit = std::make_shared<chunkdb::AuditLog>(audit_path);
        config.require_auth = true;
        config.users = users;
        config.audit = audit;
        config.backup_dir = dir.path() / "backups";
        config.auth_failure_delay_ms = 0;
        engine = std::make_unique<CommandEngine>(config, catalog);
    }

    SessionState Session(std::uint64_t id, const std::string& peer = kPeer) {
        SessionState session;
        session.connection_id = id;
        session.remote_address = peer;
        return session;
    }

    std::string Login(SessionState& session, const std::string& user, const std::optional<std::string>& password) {
        return chunkdb::test::LoginOnEngine(*engine, session, user, password);
    }

    SessionState Admin(std::uint64_t id) {
        auto session = Session(id);
        Expect(Login(session, kAdmin, std::string(kPassword)).rfind("%", 0) == 0, "admin login");
        return session;
    }

    std::string Run(SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }
};

std::string Verifier(const std::string& password) {
    return chunkdb::scram::FormatVerifier(
        chunkdb::scram::MakeVerifier(password, chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations));
}

// Logins: success, a wrong password, an unknown user, and a banned source,
// with the user, the peer and the connection of each.
void TestLoginEvents() {
    chunkdb::EngineConfig config;
    config.max_auth_failures_per_ip = 2;
    config.auth_failure_ban_ms = 60000;
    Engine e(config);
    auto ok = e.Session(7);
    assert(e.Login(ok, kAdmin, std::string(kPassword)).rfind("%", 0) == 0);
    auto wrong = e.Session(8, "203.0.113.9");
    assert(e.Login(wrong, kAdmin, std::nullopt).rfind("-ERR AUTH_FAILED", 0) == 0);
    auto unknown = e.Session(9, "203.0.113.9");
    assert(e.Login(unknown, "nobody", std::nullopt).rfind("-ERR AUTH_FAILED", 0) == 0);
    auto banned = e.Session(10, "203.0.113.9");
    assert(Contains(e.Login(banned, kAdmin, std::string(kPassword)), "temporary auth ban"));

    const auto lines = Lines(e.audit_path);
    assert(lines.size() == 4U);
    const auto success = LineWith(lines, {"\"event\":\"login\"", "\"connection\":7", "\"ok\":true"});
    assert(Contains(success, "\"user\":\"admin\"") && Contains(success, std::string("\"peer\":\"") + kPeer + "\""));
    assert(success.rfind("{\"time_ms\":", 0) == 0 && success.back() == '}');
    assert(!Contains(success, "error_code") && !Contains(success, "reason"));
    LineWith(lines, {"\"connection\":8", "\"user\":\"admin\"", "\"peer\":\"203.0.113.9\"", "\"ok\":false",
                     "\"error_code\":\"AUTH_FAILED\"", "\"reason\":\"wrong_password\""});
    LineWith(lines, {"\"connection\":9", "\"user\":\"nobody\"", "\"reason\":\"unknown_user\""});
    LineWith(lines, {"\"connection\":10", "\"user\":\"admin\"", "\"reason\":\"banned\""});
    // The SCRAM step between HELLO and AUTH is not an event.
    assert(!Contains(ReadAll(e.audit_path), "SCRAM"));
}

// Every audited statement, with the names it acts on, and a failure with its
// code; reads and writes of blocks are not audited.
void TestStatementEvents() {
    Engine e;
    auto admin = e.Admin(3);
    const std::string verifier = Verifier("bot-private-password");
    assert(e.Run(admin, "CREATE TABLE t (kind u8) CHUNK 2 x 2") == "+OK\r\n");
    assert(e.Run(admin, "ALTER TABLE t ADD COLUMN label text(8) NULL") == "+OK\r\n");
    assert(e.Run(admin, "SET BLOCK 0 0 IN t kind = 77, label = 'hidden'").rfind(":", 0) == 0);
    assert(e.Run(admin, "GET BLOCK 0 0 FROM t").rfind("-", 0) != 0);
    assert(e.Run(admin, "CREATE SLOT 'consumer' ON t") == "+OK\r\n");
    assert(e.Run(admin, "DROP SLOT 'consumer' ON t") == "+OK\r\n");
    assert(e.Run(admin, "CREATE USER bot VERIFIER $1", {verifier}) == "+OK\r\n");
    assert(e.Run(admin, "ALTER USER bot MANAGES USERS") == "+OK\r\n");
    assert(e.Run(admin, "GRANT READ ON t TO bot") == "+OK\r\n");
    assert(e.Run(admin, "REVOKE READ ON t FROM bot") == "+OK\r\n");
    assert(e.Run(admin, "DROP USER bot") == "+OK\r\n");
    assert(e.Run(admin, "BACKUP TO 'backup-sentinel'").rfind("-", 0) != 0);
    assert(e.Run(admin, "MIGRATE 'add_note' ALTER TABLE t ADD COLUMN note text(4) NULL").rfind("-", 0) != 0);
    assert(e.Run(admin, "DROP TABLE t") == "+OK\r\n");
    assert(e.Run(admin, "DROP TABLE missing").rfind("-ERR NO_TABLE", 0) == 0);
    // The engine here has no TLS listener, so the reload is refused and audited as such.
    assert(e.Run(admin, "RELOAD TLS").rfind("-ERR INVALID_ARGUMENT", 0) == 0);

    const auto lines = Lines(e.audit_path);
    const auto statement = [&](const std::vector<std::string>& parts) {
        auto all = parts;
        all.push_back("\"connection\":3");
        all.push_back("\"user\":\"admin\"");
        return LineWith(lines, all);
    };
    statement({"\"event\":\"create_table\"", "\"table\":\"t\"", "\"ok\":true"});
    statement({"\"event\":\"alter_table\"", "\"table\":\"t\""});
    statement({"\"event\":\"create_slot\"", "\"table\":\"t\"", "\"slot\":\"consumer\""});
    statement({"\"event\":\"drop_slot\"", "\"table\":\"t\"", "\"slot\":\"consumer\""});
    statement({"\"event\":\"create_user\"", "\"target_user\":\"bot\""});
    statement({"\"event\":\"alter_user\"", "\"target_user\":\"bot\""});
    statement({"\"event\":\"grant\"", "\"table\":\"t\"", "\"target_user\":\"bot\""});
    statement({"\"event\":\"revoke\"", "\"table\":\"t\"", "\"target_user\":\"bot\""});
    statement({"\"event\":\"drop_user\"", "\"target_user\":\"bot\""});
    statement({"\"event\":\"backup\"", "\"ok\":true"});
    statement({"\"event\":\"migrate\"", "\"table\":\"t\"", "\"migration\":\"add_note\""});
    statement({"\"event\":\"drop_table\"", "\"table\":\"t\"", "\"ok\":true"});
    statement({"\"event\":\"drop_table\"", "\"table\":\"missing\"", "\"ok\":false", "\"error_code\":\"NO_TABLE\""});
    statement({"\"event\":\"reload_tls\"", "\"ok\":false", "\"error_code\":\"INVALID_ARGUMENT\""});
    // One login and the fourteen statements above, nothing else.
    assert(lines.size() == 15U);

    // No password, verifier, value or parameter reaches the file.
    const auto text = ReadAll(e.audit_path);
    assert(!Contains(text, kPassword) && !Contains(text, "bot-private-password"));
    assert(!Contains(text, verifier) && !Contains(text, "SCRAM-SHA-256"));
    // Text sentinels: digits could appear in times and ids.
    assert(!Contains(text, "hidden") && !Contains(text, "backup-sentinel"));
    assert(!Contains(text, "SET BLOCK") && !Contains(text, "GET BLOCK"));
}

// The file is private, and Reopen (SIGHUP) moves later lines to a new file
// at the path, leaving a renamed file as it was.
void TestFileAndReopen() {
    Engine e;
    auto admin = e.Admin(1);
#ifndef _WIN32
    struct stat status {};
    assert(::stat(e.audit_path.c_str(), &status) == 0);
    assert((status.st_mode & 0777U) == 0600U);
#endif
    const auto rotated = e.audit_path.string() + ".1";
    std::filesystem::rename(e.audit_path, rotated);
    assert(e.Run(admin, "CREATE TABLE before_reopen (kind u8)") == "+OK\r\n");
    e.audit->Reopen();
    assert(e.Run(admin, "CREATE TABLE after_reopen (kind u8)") == "+OK\r\n");
    const auto old_text = ReadAll(rotated);
    const auto new_text = ReadAll(e.audit_path);
    assert(Contains(old_text, "before_reopen") && !Contains(old_text, "after_reopen"));
    assert(Contains(new_text, "after_reopen") && !Contains(new_text, "before_reopen"));
#ifndef _WIN32
    assert(::stat(e.audit_path.c_str(), &status) == 0);
    assert((status.st_mode & 0777U) == 0600U);
#endif
    // A path that cannot be opened is refused at once.
    bool refused = false;
    try {
        chunkdb::AuditLog missing(e.dir.path() / "no-such-dir" / "audit.log");
    } catch (const std::exception&) {
        refused = true;
    }
    assert(refused);
}

// JSON strings are escaped.
void TestJsonEscaping() {
    std::string out;
    chunkdb::AppendJsonString(out, "a\"b\\c\n\x01");
    assert(out == "\"a\\\"b\\\\c\\u000a\\u0001\"");

    // Well-formed UTF-8 is kept; every byte that does not start a
    // well-formed sequence becomes U+FFFD.
    const auto json = [](std::string_view text) {
        std::string result;
        chunkdb::AppendJsonString(result, text);
        return result;
    };
    assert(json("\xc3\xa9\xe2\x82\xac\xf0\x9f\x98\x80") == "\"\xc3\xa9\xe2\x82\xac\xf0\x9f\x98\x80\"");
    assert(json("a\xff" "b") == "\"a\\ufffdb\"");
    assert(json("\xc0\xaf") == "\"\\ufffd\\ufffd\"");                 // overlong
    assert(json("\xed\xa0\x80") == "\"\\ufffd\\ufffd\\ufffd\"");      // surrogate
    assert(json("\xf4\x90\x80\x80") == "\"\\ufffd\\ufffd\\ufffd\\ufffd\"");  // above U+10FFFF
    assert(json("x\xe2\x82") == "\"x\\ufffd\\ufffd\"");               // truncated
}

// A failed login whose name could not name a user is written without the
// name, so a client cannot put arbitrary or oversized bytes into the log.
void TestInvalidUserNames() {
    Engine e;
    const std::string names[] = {"Admin", std::string(200, 'a'), "\xff\xfe"};
    std::uint64_t id = 20;
    for (const auto& name : names) {
        auto session = e.Session(id++);
        assert(e.Login(session, name, std::nullopt).rfind("-ERR", 0) == 0);
    }
    const auto lines = Lines(e.audit_path);
    assert(lines.size() == 3U);
    for (const auto& line : lines) {
        assert(Contains(line, "\"user\":\"\"") && Contains(line, "\"reason\":\"invalid_user_name\""));
        assert(line.size() < 300U);
    }
    const auto all = ReadAll(e.audit_path);
    assert(!Contains(all, "Admin") && !Contains(all, std::string(200, 'a')) && !Contains(all, "\xff"));
}

class LogCapture {
  public:
    LogCapture() {
        previous_ = chunkdb::GetLogLevel();
        chunkdb::SetLogLevel(chunkdb::LogLevel::kInfo);
        chunkdb::SetLogSinkForTests([this](const std::string& line) {
            std::lock_guard lock(mutex_);
            lines_.push_back(line);
        });
    }
    ~LogCapture() {
        chunkdb::ResetLogSinkForTests();
        chunkdb::SetLogLevel(previous_);
    }
    std::vector<std::string> With(const std::string& part) {
        std::lock_guard lock(mutex_);
        std::vector<std::string> found;
        for (const auto& line : lines_) if (Contains(line, part)) found.push_back(line);
        return found;
    }

  private:
    chunkdb::LogLevel previous_;
    std::mutex mutex_;
    std::vector<std::string> lines_;
};

void SetEnv(const char* name, const char* value) {
#ifdef _WIN32
    _putenv_s(name, value);
#else
    setenv(name, value, 1);
#endif
}

// A statement slower than --slow-statement-ms is logged at WARN with its
// verb, table, user, peer, duration and chunks, never its values; faster
// ones and every statement with the log off are not.
void TestSlowStatements() {
    for (const std::size_t threshold : {std::size_t{0}, std::size_t{10000}, std::size_t{20}}) {
        chunkdb::EngineConfig config;
        config.slow_statement_ms = threshold;
        Engine e(config, false);
        auto admin = e.Admin(4);
        assert(e.Run(admin, "CREATE TABLE t (kind u8, label text(16) NULL) CHUNK 2 x 2") == "+OK\r\n");
        LogCapture logs;
        SetEnv("CHUNKDB_FAILPOINT_WAL_APPEND_HOLD_MS_ONCE", "60");
        assert(e.Run(admin, "SET BLOCK 1 1 IN t kind = 9, label = 'slow-sentinel'").rfind(":", 0) == 0);
        const auto slow = logs.With("slow statement");
        if (threshold != 20U) {
            // Off, or longer than the statement took.
            assert(slow.empty());
            continue;
        }
        Expect(slow.size() == 1U, "one slow statement line, got " + std::to_string(slow.size()));
        const auto& line = slow.front();
        assert(Contains(line, "WARN") && Contains(line, "statement=\"SET BLOCK\""));
        assert(Contains(line, "table=t") && Contains(line, "user=admin") && Contains(line, std::string("peer=") + kPeer));
        assert(Contains(line, "chunks=1") && Contains(line, "duration_ms="));
        assert(!Contains(line, "slow-sentinel"));
    }
}

// --auth-max-failures-per-connection closes a connection after that many
// failed logins; --auth-ban-failures and --auth-ban-ms ban a source address
// for that long; 0 ban failures never bans.
void TestLoginLimits() {
    {
        chunkdb::EngineConfig config;
        config.max_auth_failures = 2;
        config.max_auth_failures_per_ip = 0;
        Engine e(config, false);
        auto session = e.Session(1);
        assert(e.Login(session, kAdmin, std::nullopt).rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(!session.close_after_reply);
        assert(e.Login(session, kAdmin, std::nullopt).rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(session.close_after_reply);
        // Without bans, more failures from the address never ban it.
        for (int i = 0; i < 6; ++i) {
            auto again = e.Session(2U + static_cast<std::uint64_t>(i));
            assert(e.Login(again, kAdmin, std::nullopt).rfind("-ERR AUTH_FAILED", 0) == 0);
        }
        auto fine = e.Session(9);
        assert(e.Login(fine, kAdmin, std::string(kPassword)).rfind("%", 0) == 0);
    }
    {
        chunkdb::EngineConfig config;
        config.max_auth_failures_per_ip = 3;
        config.auth_failure_ban_ms = 300;
        Engine e(config, false);
        for (int i = 0; i < 3; ++i) {
            auto session = e.Session(1U + static_cast<std::uint64_t>(i));
            const auto reply = e.Login(session, kAdmin, std::nullopt);
            assert(reply.rfind("-ERR AUTH_FAILED", 0) == 0 && !Contains(reply, "temporary auth ban"));
        }
        auto banned = e.Session(10);
        assert(Contains(e.Login(banned, kAdmin, std::string(kPassword)), "temporary auth ban"));
        auto other = e.Session(11, "198.51.100.99");
        assert(e.Login(other, kAdmin, std::string(kPassword)).rfind("%", 0) == 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        auto after = e.Session(12);
        assert(e.Login(after, kAdmin, std::string(kPassword)).rfind("%", 0) == 0);
    }
}

}  // namespace

int main() {
    {
        Engine e;
        auto session = e.Admin(42U);
        auto callbacks = std::make_shared<chunkdb::SessionControlCallbacks>();
        callbacks->cancel = [](const auto&, auto) { return true; };
        callbacks->kill = [](const auto&, auto) { return false; };
        session.connection_controls = callbacks;
        assert(e.Run(session, "CANCEL 18446744073709551615").find("#t") != std::string::npos);
        assert(e.Run(session, "KILL CONNECTION 10").find("#f") != std::string::npos);
        const auto lines = Lines(e.audit_path);
        (void)LineWith(lines, {"\"event\":\"cancel\"", "\"target_connection\":18446744073709551615", "\"connection\":42", "\"ok\":true"});
        (void)LineWith(lines, {"\"event\":\"kill_connection\"", "\"target_connection\":10", "\"ok\":true"});
        assert(!Contains(ReadAll(e.audit_path), kPassword));
    }
    TestLoginEvents();
    TestStatementEvents();
    TestFileAndReopen();
    TestJsonEscaping();
    TestInvalidUserNames();
    TestSlowStatements();
    TestLoginLimits();
    std::printf("audit log tests passed\n");
    return 0;
}
