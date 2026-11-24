// Users through the engine (docs/design/USERS_DESIGN.md): SCRAM-SHA-256 login in
// HELLO, the user statements, and the right every statement needs.

#include <algorithm>
#include <cassert>
#include <array>
#include <barrier>
#include <thread>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <filesystem>

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "catalog_test_utils.hpp"
#include "checkpoint.hpp"
#include "audit_log.hpp"
#include "scram.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"
#include "user_registry.hpp"

namespace {

using chunkdb::CommandEngine;
using chunkdb::SessionState;
using Parameters = std::vector<std::optional<std::string>>;

bool Contains(const std::string& text, const std::string& part) {
    return text.find(part) != std::string::npos;
}

void ExpectReply(const std::string& reply, const std::string& want) {
    if (reply != want) {
        std::fprintf(stderr, "expected '%s', got '%s'\n", want.c_str(), reply.c_str());
        assert(false);
    }
}

void ExpectError(const std::string& reply, const std::string& part) {
    const auto space = part.find(' ');
    const auto code = part.substr(0, space);
    constexpr std::string_view known_codes[] = {
        "PROTOCOL", "AUTH_REQUIRED", "AUTH_FAILED", "SYNTAX", "UNKNOWN_COMMAND",
        "INVALID_ARGUMENT", "OUT_OF_RANGE", "NO_TABLE", "PERMISSION_DENIED", "TABLE_EXISTS",
        "VERSION_MISMATCH", "SCHEMA_MISMATCH", "CONFLICT", "MIGRATION_MISMATCH", "SLOT_BUSY",
        "SLOT_LOST", "BUSY", "INTERNAL", "OUTCOME_UNKNOWN", "DISK_FULL", "RATE_LIMITED"};
    const bool has_code = std::find(std::begin(known_codes), std::end(known_codes), code) != std::end(known_codes);
    const bool matches = has_code
        ? reply.starts_with("-ERR " + code + " ") &&
          (space == std::string::npos || Contains(reply, part.substr(space + 1U)))
        : Contains(reply, part);
    if (reply.rfind("-ERR ", 0) != 0 || !matches) {
        std::fprintf(stderr, "expected an error with '%s', got '%s'\n", part.c_str(), reply.c_str());
        assert(false);
    }
}

std::string VerifierText(const std::string& password) {
    return chunkdb::scram::FormatVerifier(
        chunkdb::scram::MakeVerifier(password, chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations));
}

struct Server {
    chunkdb::test::ScopedTempDir dir{"chunkdb-users-engine"};
    std::shared_ptr<chunkdb::TableCatalog> catalog;
    std::shared_ptr<chunkdb::UserRegistry> users;
    std::unique_ptr<CommandEngine> engine;

    explicit Server(bool require_auth = true, bool require_channel_binding = false, std::filesystem::path backup_dir = {}) {
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        catalog = std::make_shared<chunkdb::TableCatalog>(config);
        (void)chunkdb::test::CreateBitsTable(*catalog, chunkdb::GeometryConfig{});
        if (require_auth) {
            users = std::make_shared<chunkdb::UserRegistry>(
                dir.path(),
                std::make_pair(
                    std::string("admin"),
                    chunkdb::scram::MakeVerifier("secret", chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations)),
                std::array<std::uint8_t, 32>{7});
        }
        engine = std::make_unique<CommandEngine>(
            chunkdb::EngineConfig{
                .require_auth = require_auth,
                .backup_dir = std::move(backup_dir),
                .users = users,
                .max_auth_failures = 5,
                .require_channel_binding = require_channel_binding,
            },
            catalog);
    }

    std::string Run(SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }

    // HELLO 3 USER and AUTH; the final reply, after checking the server's
    // signature when the login succeeds. With a `binding_type` the login is
    // bound to `binding_data`, what the client sees of its connection.
    std::string Login(
        SessionState& session,
        const std::string& user,
        const std::string& password,
        std::string_view binding_type = {},
        std::span<const std::uint8_t> binding_data = {}) {
        const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce(), binding_type);
        const std::string first = Run(session, "HELLO 3 USER " + user + " $1", Parameters{login.first});
        if (first.rfind("+SCRAM ", 0) != 0) {
            return first;
        }
        const std::string server_first = first.substr(7, first.size() - 9);
        const auto final_message = chunkdb::scram::FinishClientLogin(login, password, server_first, binding_data);
        const std::string reply = Run(session, "AUTH $1", Parameters{final_message.message});
        if (reply.rfind("%14\r\n", 0) == 0) {
            assert(Contains(
                reply,
                "$16\r\nserver_signature\r\n$" + std::to_string(final_message.server_signature.size()) + "\r\n" +
                    final_message.server_signature + "\r\n"));
        }
        return reply;
    }

    SessionState LoggedIn(const std::string& user, const std::string& password) {
        SessionState session;
        const std::string reply = Login(session, user, password);
        assert(reply.rfind("%14\r\n", 0) == 0 && session.greeted && session.user == user);
        return session;
    }
};

void TestLogin() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    ExpectError(server.Run(admin, "HELLO 3"), "PROTOCOL reason=protocol HELLO was already sent");

    SessionState wrong;
    ExpectReply(server.Login(wrong, "admin", "Secret"), "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the user name and password your client sends, or reset the password with chunkdb_admin reset-password\r\n");
    assert(!wrong.greeted);
    SessionState ghost;
    ExpectReply(server.Login(ghost, "ghost", "secret"), "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the user name and password your client sends, or reset the password with chunkdb_admin reset-password\r\n");

    SessionState anonymous;
    ExpectError(server.Run(anonymous, "HELLO 3"), "AUTH_REQUIRED reason=auth_required log in with a user name and password");
    SessionState token;
    ExpectError(server.Run(token, "HELLO 3 AUTH secret"), "SYNTAX column");
    SessionState early;
    ExpectError(server.Run(early, "AUTH $1", Parameters{std::string("c=biws,r=x,p=AAAA")}), "PROTOCOL AUTH follows HELLO 3 USER");
    assert(early.close_after_reply);
    SessionState authzid;
    ExpectError(server.Run(authzid, "HELLO 3 USER admin $1", Parameters{std::string("n,a=bot,n=admin,r=abc")}), "INVALID_ARGUMENT");
    SessionState other_name;
    ExpectError(
        server.Run(other_name, "HELLO 3 USER admin $1", Parameters{std::string("n,,n=bot,r=abc")}),
        "names another user than HELLO");

    // Before HELLO SCRAM is bounded; other parameter frames drain opaquely.
    SessionState fresh;
    const auto hello_plan = server.engine->PlanPayload(fresh, "HELLO 3 USER admin $1\r\n");
    assert(hello_plan.plan == CommandEngine::PayloadPlan::kParameters && hello_plan.parameter_limits.size() == 1);
    assert(server.engine->PlanPayload(fresh, "AUTH $1\r\n").plan == CommandEngine::PayloadPlan::kParameters);
    assert(server.engine->PlanPayload(fresh, "SET BLOCK 0 0 IN default bits = $1\r\n").plan == CommandEngine::PayloadPlan::kDiscard);

    // A statement before logging in closes the connection.
    SessionState idle;
    ExpectError(server.Run(idle, "GET BLOCK 0 0 FROM default"), "PROTOCOL expected HELLO 3");
    assert(idle.close_after_reply);
}

// A right proof made against credentials that changed before AUTH: a new
// verifier or a new incarnation fails like a wrong password, and a verifier
// changed and changed back is a stale login that counts no failure.
void TestStaleAuthenticationProofs() {
    enum class Change { kReplace, kRecreate, kRestore };
    for (const auto change : {Change::kReplace, Change::kRecreate, Change::kRestore}) {
        Server server;
        const auto verifier = chunkdb::scram::MakeVerifier("original", std::vector<std::uint8_t>(16U, 7U), chunkdb::scram::kMinIterations);
        const auto replacement = chunkdb::scram::MakeVerifier("replacement", std::vector<std::uint8_t>(16U, 8U), chunkdb::scram::kMinIterations);
        server.users->Create("bot", verifier, false);
        SessionState session;
        const auto client = chunkdb::scram::StartClientLogin("bot", chunkdb::scram::NewNonce());
        const auto first = server.Run(session, "HELLO 3 USER bot $1", Parameters{client.first});
        assert(first.starts_with("+SCRAM "));
        const auto final = chunkdb::scram::FinishClientLogin(client, "original", first.substr(7, first.size() - 9));
        if (change == Change::kRecreate) {
            server.users->Drop("bot");
            server.users->Create("bot", verifier, false);
        } else {
            server.users->SetVerifier("bot", replacement);
            if (change == Change::kRestore) server.users->SetVerifier("bot", verifier);
        }
        const auto reply = server.Run(session, "AUTH $1", Parameters{final.message});
        if (change == Change::kRestore) {
            ExpectError(reply, "AUTH_FAILED reason=stale_login");
            assert(session.failed_auth_attempts == 0U);
        } else {
            ExpectError(reply, "AUTH_FAILED reason=auth_failed");
            assert(session.failed_auth_attempts == 1U);
        }
        assert(!session.greeted && !session.authenticated && !session.user_ticket);
        assert(server.users->AuthenticationSnapshot("bot")->state->connections == 0U);
    }
}

void TestUserLimitStatementsAndAdmissionHook() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    server.users->Create("bot", chunkdb::scram::MakeVerifier("secret", std::vector<std::uint8_t>(16U, 7U), chunkdb::scram::kMinIterations), false);
    server.users->Grant("bot", "default", chunkdb::Right::kRead);
    ExpectReply(server.Run(admin, "ALTER USER bot LIMIT max_connections = 1"), "+OK\r\n");
    auto bot = server.LoggedIn("bot", "secret");
    assert(bot.user_ticket && bot.user_incarnation != 0U);
    SessionState refused;
    ExpectError(server.Login(refused, "bot", "secret"), "RATE_LIMITED reason=max_connections");
    assert(!refused.greeted && !refused.user_ticket && refused.failed_auth_attempts == 0U);
    ExpectReply(server.Run(bot, "BEGIN"), "+OK\r\n");
    auto* transaction = bot.transaction.get();
    server.users->SetLimits("bot", std::nullopt, 1U);
    assert(!server.engine->AdmitUserStatement(bot, "PING"));
    const auto limited = server.engine->AdmitUserStatement(bot, "SET BLOCK 0 0 IN default bits = b'1000'");
    assert(limited && limited->starts_with("-ERR RATE_LIMITED reason=statements_per_sec "));
    assert(bot.transaction.get() == transaction);
    for (const auto line : {"CANCEL 1", "kill connection 1", " \tROLLBACK\t", "cancel\t1"})
        assert(!server.engine->AdmitUserStatement(bot, line));
    ExpectReply(server.Run(bot, "ROLLBACK"), "+OK\r\n");
    server.users->SetLimits("bot", std::nullopt, 0U);
    ExpectError(server.Run(bot, "ALTER USER bot LIMIT max_connections = 4"), "PERMISSION_DENIED");
    const auto before = server.users->Snapshot();
    ExpectError(server.Run(admin, "ALTER USER bot LIMIT max_connections = 0, max_connections = 2"), "SYNTAX");
    assert(server.users->Snapshot() == before);
    const auto old_incarnation = bot.user_incarnation;
    server.users->Drop("bot");
    server.users->Create("bot", chunkdb::scram::MakeVerifier("secret", std::vector<std::uint8_t>(16U, 7U), chunkdb::scram::kMinIterations), false);
    server.users->Grant("bot", "default", chunkdb::Right::kAdmin);
    const auto stale = server.Run(bot, "GET BLOCK 0 0 FROM default");
    assert(stale.starts_with("-ERR NO_TABLE ") || stale.starts_with("-ERR AUTH_REQUIRED "));
    auto replacement = server.LoggedIn("bot", "secret");
    assert(replacement.user_incarnation > old_incarnation);
    ExpectReply(server.Run(replacement, "GET BLOCK 0 0 FROM default"), "_\r\n");
}

void TestRights() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE TABLE world (bits bits(4)) CHUNK 4 x 4"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE TABLE other (bits bits(4)) CHUNK 4 x 4"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE SLOT 'consumer' ON world"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE SLOT 'hidden' ON other"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("hunter2")}), "+OK\r\n");
    ExpectError(server.Run(admin, "CREATE USER bot2 VERIFIER 'pencil'"), "INVALID_ARGUMENT a verifier is SCRAM-SHA-256$");

    SessionState bot = server.LoggedIn("bot", "hunter2");
    // No right at all: the tables read as absent.
    ExpectError(server.Run(bot, "GET BLOCK 0 0 FROM world"), "NO_TABLE table 'world' does not exist");
    ExpectReply(server.Run(bot, "SHOW TABLES"), "*0\r\n");
    ExpectReply(server.Run(bot, "SHOW SLOTS"), "*0\r\n");
    ExpectError(server.Run(bot, "SHOW SLOTS ON world"), "NO_TABLE");
    ExpectError(server.Run(bot, "CREATE SLOT 'mine' ON world"), "NO_TABLE");
    const auto hidden = server.engine->PlanPayload(bot, "SET BLOCK 0 0 IN world bits = $1\r\n");
    assert(hidden.plan == CommandEngine::PayloadPlan::kDiscard && Contains(hidden.reject_response, "NO_TABLE"));

    ExpectReply(server.Run(admin, "GRANT READ ON world TO bot"), "+OK\r\n");
    ExpectReply(server.Run(bot, "GET BLOCK 0 0 FROM world"), "_\r\n");
    ExpectReply(server.Run(bot, "SHOW TABLES"), "*1\r\n$5\r\nworld\r\n");
    const auto visible_slots = server.Run(bot, "SHOW SLOTS");
    assert(visible_slots.starts_with("*1\r\n") && Contains(visible_slots, "consumer") && !Contains(visible_slots, "hidden"));
    assert(server.Run(bot, "SHOW SLOTS ON world") == visible_slots);
    ExpectError(server.Run(bot, "CREATE SLOT 'mine' ON world"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "DROP SLOT 'consumer' ON world"), "PERMISSION_DENIED ADMIN on world");
    assert(server.Run(bot, "DESCRIBE world").rfind("%10\r\n", 0) == 0);
    const auto scan = server.Run(bot, "SCAN CHUNKS FROM world");
    assert(scan.starts_with("%5\r\n$6\r\nchunks\r\n*0\r\n$4\r\nmore\r\n#f\r\n$5\r\nepoch\r\n$32\r\n"));
    assert(scan.ends_with("\r\n$8\r\nposition\r\n:0\r\n$23\r\nretained_deletion_floor\r\n:0\r\n"));
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "PERMISSION_DENIED WRITE on world");
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "ask an administrator to grant this right");
    ExpectError(server.Run(bot, "FLUSH WAL"), "PERMISSION_DENIED WRITE on a table");

    ExpectReply(server.Run(admin, "GRANT WRITE ON world TO bot"), "+OK\r\n");
    assert(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'").rfind(":", 0) == 0);
    assert(server.Run(bot, "DELETE BLOCK 0 0 FROM world").rfind(":", 0) == 0);
    ExpectReply(server.Run(bot, "FLUSH WAL"), "+OK\r\n");
    ExpectError(server.Run(bot, "ALTER TABLE world ADD COLUMN extra u8 NULL"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "DROP TABLE world"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "CREATE TABLE mine (bits bits(4)) CHUNK 4 x 4"), "PERMISSION_DENIED ADMIN on mine");
    ExpectError(server.Run(bot, "SHOW METRICS"), "PERMISSION_DENIED MONITOR or ADMIN on *");
    ExpectError(server.Run(bot, "SHOW STATUS"), "PERMISSION_DENIED MONITOR or ADMIN on *");
    ExpectError(server.Run(bot, "ENABLE FEATURE 'feed_slots'"), "PERMISSION_DENIED ADMIN on *");
    ExpectError(server.Run(bot, "GET BLOCK 0 0 FROM other"), "NO_TABLE");

    // A user may change their own password, nothing else on users.
    ExpectError(server.Run(bot, "CREATE USER eve VERIFIER $1", Parameters{VerifierText("x")}), "PERMISSION_DENIED MANAGES USERS");
    ExpectError(server.Run(bot, "GRANT ADMIN ON * TO bot"), "PERMISSION_DENIED MANAGES USERS");
    ExpectError(server.Run(bot, "SHOW USERS"), "PERMISSION_DENIED MANAGES USERS");
    ExpectError(server.Run(bot, "ALTER USER admin VERIFIER $1", Parameters{VerifierText("x")}), "PERMISSION_DENIED MANAGES USERS");
    ExpectReply(server.Run(bot, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("new")}), "+OK\r\n");
    SessionState old_password;
    ExpectError(server.Login(old_password, "bot", "hunter2"), "AUTH_FAILED");
    (void)server.LoggedIn("bot", "new");

    // Rights change for a session already logged in.
    ExpectReply(server.Run(admin, "REVOKE WRITE ON world FROM bot"), "+OK\r\n");
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "PERMISSION_DENIED WRITE on world");
    ExpectReply(server.Run(bot, "GET BLOCK 0 0 FROM world"), "_\r\n");
    ExpectReply(server.Run(admin, "GRANT READ ON * TO bot"), "+OK\r\n");
    ExpectReply(server.Run(bot, "SHOW TABLES"), "*3\r\n$7\r\ndefault\r\n$5\r\nother\r\n$5\r\nworld\r\n");

    // Dropping a table forgets its grants; * still covers a new one.
    ExpectReply(server.Run(admin, "DROP TABLE world"), "+OK\r\n");
    assert(!server.users->Find("bot")->grants.contains("world"));
    ExpectReply(server.Run(admin, "CREATE TABLE world (bits bits(4)) CHUNK 4 x 4"), "+OK\r\n");
    ExpectReply(server.Run(bot, "GET BLOCK 0 0 FROM world"), "_\r\n");
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "PERMISSION_DENIED WRITE on world");

    const std::string users = server.Run(admin, "SHOW USERS");
    assert(users.rfind("*2\r\n", 0) == 0 && Contains(users, "$5\r\nadmin\r\n") && Contains(users, "$1\r\n*\r\n$4\r\nREAD\r\n"));
    ExpectError(server.Run(admin, "ALTER USER admin NO MANAGES USERS"), "the last user who manages users");

    // A session whose user was dropped has no user any more and closes.
    ExpectReply(server.Run(admin, "DROP USER bot"), "+OK\r\n");
    ExpectError(server.Run(bot, "GET BLOCK 0 0 FROM world"), "AUTH_REQUIRED reason=user_dropped");
    assert(bot.close_after_reply);
    SessionState gone;
    ExpectError(server.Login(gone, "bot", "new"), "AUTH_FAILED");
}

// MONITOR, BACKUP and REPLICATION: server rights granted ON *, apart from
// table rights and MANAGES USERS.
void TestServerRights() {
    chunkdb::test::ScopedTempDir backups{"chunkdb-users-backups"};
    Server server(true, false, backups.path());
    auto admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE TABLE world (bits bits(4)) CHUNK 4 x 4"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE SLOT 'consumer' ON world"), "+OK\r\n");
    for (const std::string name : {"mon", "bk", "rep"})
        ExpectReply(server.Run(admin, "CREATE USER " + name + " VERIFIER $1", {VerifierText("pw")}), "+OK\r\n");
    auto mon = server.LoggedIn("mon", "pw");
    auto bk = server.LoggedIn("bk", "pw");
    auto rep = server.LoggedIn("rep", "pw");
    for (auto* session : {&mon, &bk, &rep}) {
        ExpectError(server.Run(*session, "SHOW METRICS"), "PERMISSION_DENIED MONITOR or ADMIN on *");
        ExpectError(server.Run(*session, "SHOW STATUS"), "PERMISSION_DENIED MONITOR or ADMIN on *");
        ExpectError(server.Run(*session, "BACKUP TO 'b0'"), "PERMISSION_DENIED BACKUP or MANAGES USERS");
    }
    ExpectError(server.Run(admin, "GRANT MONITOR ON world TO mon"), "SYNTAX");
    ExpectReply(server.Run(admin, "GRANT MONITOR ON * TO mon"), "+OK\r\n");
    ExpectReply(server.Run(admin, "GRANT BACKUP ON * TO bk"), "+OK\r\n");
    ExpectReply(server.Run(admin, "MIGRATE 'replication' GRANT REPLICATION ON * TO rep"), "+applied\r\n");

    // MONITOR: status statements over every table, without data or changes.
    assert(server.Run(mon, "SHOW METRICS").rfind("-ERR", 0) != 0);
    assert(server.Run(mon, "SHOW STATUS").starts_with("%7\r\n"));
    assert(server.Run(mon, "SHOW TABLE STATS world").starts_with("%5\r\n"));
    ExpectError(server.Run(mon, "SHOW TABLE STATS absent"), "NO_TABLE");
    const auto slots = server.Run(mon, "SHOW SLOTS");
    assert(slots.starts_with("*1\r\n") && Contains(slots, "consumer"));
    assert(server.Run(mon, "SHOW SLOTS ON world") == slots);
    ExpectError(server.Run(mon, "GET BLOCK 0 0 FROM world"), "NO_TABLE");
    ExpectError(server.Run(mon, "SET BLOCK 0 0 IN world bits = b'1010'"), "NO_TABLE");
    ExpectReply(server.Run(mon, "SHOW TABLES"), "*0\r\n");
    ExpectError(server.Run(mon, "CREATE TABLE mine (bits bits(4)) CHUNK 4 x 4"), "PERMISSION_DENIED ADMIN on mine");
    ExpectError(server.Run(mon, "RELOAD TLS"), "PERMISSION_DENIED ADMIN on *");
    ExpectError(server.Run(mon, "SHOW USERS"), "PERMISSION_DENIED MANAGES USERS");
    // MONITOR lists every connection and controls only its own.
    auto callbacks = std::make_shared<chunkdb::SessionControlCallbacks>();
    chunkdb::SessionControlAccess seen;
    callbacks->enumerate = [&](const auto& access, const auto&) { seen = access; };
    callbacks->cancel = [&](const auto& access, std::uint64_t) { seen = access; return false; };
    mon.connection_controls = callbacks;
    bk.connection_controls = callbacks;
    admin.connection_controls = callbacks;
    ExpectReply(server.Run(mon, "SHOW CONNECTIONS"), "*0\r\n");
    assert(seen.list_all && !seen.admin_all);
    ExpectReply(server.Run(mon, "CANCEL 71"), "%1\r\n$9\r\ncancelled\r\n#f\r\n");
    assert(seen.list_all && !seen.admin_all);
    ExpectReply(server.Run(bk, "SHOW CONNECTIONS"), "*0\r\n");
    assert(!seen.list_all && !seen.admin_all);
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), "*0\r\n");
    assert(seen.list_all && seen.admin_all);

    // BACKUP: BACKUP TO and RESTORE TABLE without MANAGES USERS.
    const auto backup = server.Run(bk, "BACKUP TO 'b1'");
    assert(backup.starts_with("%4\r\n") && Contains(backup, "cuts"));
    // RESTORE TABLE needs ADMIN on the new name too, as CREATE TABLE does.
    ExpectError(server.Run(bk, "RESTORE TABLE world FROM 'b1' AS world_copy"), "PERMISSION_DENIED ADMIN on world_copy");
    ExpectReply(server.Run(admin, "GRANT READ ON world_copy TO bk"), "+OK\r\n");
    ExpectError(server.Run(bk, "RESTORE TABLE world FROM 'b1' AS world_copy"), "PERMISSION_DENIED ADMIN on world_copy");
    ExpectReply(server.Run(admin, "GRANT ADMIN ON bk_* TO bk"), "+OK\r\n");
    assert(server.Run(bk, "RESTORE TABLE world FROM 'b1' AS bk_world").starts_with("%6\r\n"));
    ExpectReply(server.Run(bk, "GET BLOCK 0 0 FROM bk_world"), "_\r\n");
    ExpectError(server.Run(bk, "GET BLOCK 0 0 FROM world"), "NO_TABLE");
    ExpectError(server.Run(admin, "GET BLOCK 0 0 FROM world_copy"), "NO_TABLE");
    // A user change once RESTORE TABLE has read the user: the checks keep
    // reading that user, not one read again and the first freed.
    struct RevokeBackup final : chunkdb::CommandEngineTestHook {
        chunkdb::UserRegistry* users = nullptr;
        bool revoked = false;
        void Run(Point point, std::string_view table) override {
            if (point != Point::kAfterRestoreUserRead || table != "bk_again" || revoked) return;
            users->Revoke("bk", "*", std::nullopt, static_cast<std::uint8_t>(chunkdb::ServerRight::kBackup));
            revoked = true;
        }
    } revoke_backup;
    revoke_backup.users = server.users.get();
    server.engine->SetHookForTests(&revoke_backup);
    assert(server.Run(bk, "RESTORE TABLE world FROM 'b1' AS bk_again").starts_with("%6\r\n"));
    server.engine->SetHookForTests(nullptr);
    assert(revoke_backup.revoked);
    ExpectError(server.Run(bk, "RESTORE TABLE world FROM 'b1' AS bk_third"), "PERMISSION_DENIED BACKUP, MANAGES USERS or ADMIN on *");
    ExpectReply(server.Run(admin, "GRANT BACKUP ON * TO bk"), "+OK\r\n");
    ExpectError(server.Run(bk, "SHOW METRICS"), "PERMISSION_DENIED");
    ExpectError(server.Run(bk, "CREATE USER eve VERIFIER $1", {VerifierText("x")}), "PERMISSION_DENIED MANAGES USERS");
    ExpectError(server.Run(mon, "RESTORE TABLE world FROM 'b1' AS world_other"),
                "PERMISSION_DENIED BACKUP, MANAGES USERS or ADMIN on *");

    // REPLICATION is kept and shown; no statement needs it yet.
    ExpectError(server.Run(rep, "SHOW METRICS"), "PERMISSION_DENIED");
    ExpectError(server.Run(rep, "BACKUP TO 'b2'"), "PERMISSION_DENIED");
    ExpectError(server.Run(rep, "GET BLOCK 0 0 FROM world"), "NO_TABLE");
    const auto users = server.Run(admin, "SHOW USERS");
    assert(Contains(users, "$13\r\nserver_rights\r\n*1\r\n$7\r\nMONITOR\r\n"));
    assert(Contains(users, "$13\r\nserver_rights\r\n*1\r\n$6\r\nBACKUP\r\n"));
    assert(Contains(users, "$13\r\nserver_rights\r\n*1\r\n$11\r\nREPLICATION\r\n"));
    assert(Contains(users, "$13\r\nserver_rights\r\n*0\r\n"));

    // The rights are in the file and change sessions already logged in.
    const auto saved = chunkdb::ReadUsersFile(server.dir.path());
    assert(saved && saved->users.at("mon").server_rights == static_cast<std::uint8_t>(chunkdb::ServerRight::kMonitor));
    assert(saved->users.at("rep").server_rights == static_cast<std::uint8_t>(chunkdb::ServerRight::kReplication));
    ExpectReply(server.Run(admin, "REVOKE MONITOR ON * FROM mon"), "+OK\r\n");
    ExpectError(server.Run(mon, "SHOW METRICS"), "PERMISSION_DENIED MONITOR or ADMIN on *");
    ExpectError(server.Run(mon, "SHOW TABLE STATS world"), "NO_TABLE");
    ExpectReply(server.Run(mon, "SHOW SLOTS"), "*0\r\n");
    assert(chunkdb::ReadUsersFile(server.dir.path())->users.at("mon").server_rights == 0U);
}

// GRANT ... ON prefix*: every table whose name starts with the prefix, also
// one not yet created, and CREATE TABLE with ADMIN on it.
void TestPrefixGrants() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    const auto table = [](const std::string& name) { return "CREATE TABLE " + name + " (bits bits(4)) CHUNK 4 x 4"; };
    const auto bulk = [](const std::string& text) { return "$" + std::to_string(text.size()) + "\r\n" + text + "\r\n"; };
    ExpectReply(server.Run(admin, "CREATE USER t1 VERIFIER $1", {VerifierText("pw")}), "+OK\r\n");
    ExpectReply(server.Run(admin, table("tenant2_world")), "+OK\r\n");
    auto t1 = server.LoggedIn("t1", "pw");
    ExpectError(server.Run(t1, table("tenant1_world")), "PERMISSION_DENIED ADMIN on tenant1_world");
    ExpectReply(server.Run(admin, "GRANT ADMIN ON tenant1_* TO t1"), "+OK\r\n");

    // A name longer than the prefix, the name equal to it, the longest name.
    const std::string longest = "tenant1_" + std::string(chunkdb::kMaxTableNameLength - 8U, 'z');
    for (const auto& name : {std::string("tenant1_world"), std::string("tenant1_"), longest})
        ExpectReply(server.Run(t1, table(name)), "+OK\r\n");
    // Shorter than the prefix, another prefix, the prefix inside a name; an
    // existing table without a right answers as an absent one would.
    for (const auto& name : {std::string("tenant1"), std::string("tenant2_world"), std::string("xtenant1_world")})
        ExpectError(server.Run(t1, table(name)), "PERMISSION_DENIED ADMIN on " + name);
    ExpectError(server.Run(t1, "GET BLOCK 0 0 FROM tenant2_world"), "NO_TABLE");
    assert(server.Run(t1, "SET BLOCK 0 0 IN tenant1_world bits = b'1010'").front() == ':');
    ExpectReply(server.Run(t1, "FLUSH WAL"), "+OK\r\n");
    ExpectReply(server.Run(t1, "SHOW TABLES"), "*3\r\n" + bulk("tenant1_") + bulk("tenant1_world") + bulk(longest));
    ExpectReply(server.Run(t1, "CREATE SLOT 's' ON tenant1_world"), "+OK\r\n");
    ExpectReply(server.Run(t1, "MIGRATE 'tenant_logs' " + table("tenant1_logs")), "+applied\r\n");
    ExpectError(server.Run(t1, "MIGRATE 'other_logs' " + table("tenant2_logs")), "PERMISSION_DENIED ADMIN on tenant2_logs");
    assert(Contains(server.Run(t1, "SHOW MIGRATIONS"), "tenant_logs"));

    // Dropping a table keeps the prefix grant, as it keeps a grant on *.
    ExpectReply(server.Run(t1, "DROP TABLE tenant1_world"), "+OK\r\n");
    ExpectReply(server.Run(t1, table("tenant1_world")), "+OK\r\n");

    // An exact grant and a prefix grant give the higher of the two.
    ExpectReply(server.Run(admin, "REVOKE WRITE ON tenant1_* FROM t1"), "+OK\r\n");
    ExpectError(server.Run(t1, "SET BLOCK 0 0 IN tenant1_world bits = b'1010'"), "PERMISSION_DENIED WRITE on tenant1_world");
    ExpectError(server.Run(t1, table("tenant1_more")), "PERMISSION_DENIED ADMIN on tenant1_more");
    ExpectReply(server.Run(admin, "GRANT WRITE ON tenant1_world TO t1"), "+OK\r\n");
    assert(server.Run(t1, "SET BLOCK 0 0 IN tenant1_world bits = b'1010'").front() == ':');

    ExpectError(server.Run(admin, "GRANT READ ON _t* TO t1"), "INVALID_ARGUMENT invalid table prefix '_t*'");
    ExpectError(server.Run(admin, "GRANT READ ON " + std::string(65, 'a') + "* TO t1"), "INVALID_ARGUMENT invalid table prefix");
    ExpectReply(server.Run(admin, "GRANT READ ON " + std::string(64, 'a') + "* TO t1"), "+OK\r\n");
    ExpectError(server.Run(admin, "GRANT READ ON tenant1_ * TO t1"), "SYNTAX");
    const auto users = server.Run(admin, "SHOW USERS");
    assert(Contains(users, bulk("tenant1_*") + bulk("READ")) && Contains(users, bulk("tenant1_world") + bulk("WRITE")));
    const auto saved = chunkdb::ReadUsersFile(server.dir.path());
    assert(saved && saved->users.at("t1").prefix_grants.at("tenant1_") == chunkdb::Right::kRead);
    assert(saved->users.at("t1").grants.at("tenant1_world") == chunkdb::Right::kWrite);

    // A watch the prefix allows ends once the prefix grant is gone.
    auto watcher = server.LoggedIn("t1", "pw");
    assert(server.Run(watcher, "WATCH tenant1_").starts_with("%2\r\n") && watcher.watch_right);
    assert(watcher.watch_right->Check() == chunkdb::ReadRightCheck::State::kHeld);
    ExpectReply(server.Run(admin, "REVOKE READ ON tenant1_* FROM t1"), "+OK\r\n");
    assert(watcher.watch_right->Check() == chunkdb::ReadRightCheck::State::kNoRight);
    ExpectError(server.Run(t1, "GET BLOCK 0 0 FROM tenant1_"), "NO_TABLE");
    assert(server.Run(t1, "GET BLOCK 1 1 FROM tenant1_world").rfind("-ERR", 0) != 0);
}

void TestRecreatedUser() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("old")}), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE USER other VERIFIER $1", Parameters{VerifierText("other")}), "+OK\r\n");
    ExpectReply(server.Run(admin, "GRANT READ ON default TO bot"), "+OK\r\n");
    auto statement = server.LoggedIn("bot", "old");
    auto alter = server.LoggedIn("bot", "old");
    auto frames = server.LoggedIn("bot", "old");
    auto hello = server.LoggedIn("bot", "old");
    auto hello_frames = server.LoggedIn("bot", "old");
    auto bystander = server.LoggedIn("other", "other");
    ExpectReply(server.Run(statement, "GET BLOCK 0 0 FROM default"), "_\r\n");

    // Another user under the same name, with more rights: the old sessions
    // get none of them.
    ExpectReply(server.Run(admin, "DROP USER bot"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("new")}), "+OK\r\n");
    ExpectReply(server.Run(admin, "GRANT ADMIN ON * TO bot"), "+OK\r\n");
    ExpectError(server.Run(statement, "GET BLOCK 0 0 FROM default"), "AUTH_REQUIRED reason=user_dropped");
    assert(statement.close_after_reply);
    const auto verifier = server.users->Find("bot")->verifier;
    ExpectError(server.Run(alter, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("taken")}),
                "AUTH_REQUIRED reason=user_dropped");
    assert(alter.close_after_reply && server.users->Find("bot")->verifier == verifier);
    const auto plan = server.engine->PlanPayload(frames, "SET BLOCK 0 0 IN default bits = $1\r\n");
    assert(plan.plan == CommandEngine::PayloadPlan::kDiscard && frames.close_after_reply);
    ExpectError(plan.reject_response, "AUTH_REQUIRED reason=user_dropped");
    ExpectError(server.Run(hello, "HELLO 3"), "AUTH_REQUIRED reason=user_dropped");
    assert(hello.close_after_reply);
    const std::string hello_line = "HELLO 3 USER bot $1\r\n";
    const auto hello_plan = server.engine->PlanPayload(hello_frames, hello_line);
    assert(hello_plan.plan == CommandEngine::PayloadPlan::kDiscard);
    ExpectError(server.engine->RejectPayload(hello_frames, hello_line, hello_plan.reject_response),
                "AUTH_REQUIRED reason=user_dropped");
    assert(hello_frames.close_after_reply);

    // The new user logs in with its own password; others are unaffected by
    // changes to users they are not.
    auto current = server.LoggedIn("bot", "new");
    ExpectReply(server.Run(current, "GET BLOCK 0 0 FROM default"), "_\r\n");
    ExpectReply(server.Run(admin, "GRANT READ ON default TO other"), "+OK\r\n");
    ExpectReply(server.Run(admin, "REVOKE WRITE ON default FROM bot"), "+OK\r\n");
    ExpectReply(server.Run(bystander, "GET BLOCK 0 0 FROM default"), "_\r\n");
    ExpectReply(server.Run(current, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("newer")}), "+OK\r\n");
    assert(!bystander.close_after_reply && !current.close_after_reply && !admin.close_after_reply);
    (void)server.LoggedIn("bot", "newer");
}

void TestAtomicRightsLists() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE TABLE world (v u8)"), "+OK\r\n");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", {VerifierText("secret")}), "+OK\r\n");
    auto bot = server.LoggedIn("bot", "secret");
    auto generation = server.users->Generation();
    ExpectReply(server.Run(admin, "GRANT READ, WRITE ON world TO bot"), "+OK\r\n");
    assert(server.users->Generation() == generation + 1);
    assert(server.users->Find("bot")->grants.at("world") == chunkdb::Right::kWrite);
    assert(server.Run(bot, "SET BLOCK 0 0 IN world v = 7").front() == ':');
    generation = server.users->Generation();
    ExpectError(server.Run(admin, "REVOKE ADMIN, INVALID ON world FROM bot"), "SYNTAX");
    assert(server.users->Generation() == generation && server.users->Find("bot")->grants.at("world") == chunkdb::Right::kWrite);
    {
        chunkdb::txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE", "1");
        ExpectError(server.Run(admin, "REVOKE READ, WRITE ON world FROM bot"), "INTERNAL");
    }
    assert(server.users->Generation() == generation && server.users->Find("bot")->grants.at("world") == chunkdb::Right::kWrite);
    ExpectError(server.Run(bot, "GRANT ADMIN, READ ON world TO bot"), "PERMISSION_DENIED");
    ExpectReply(server.Run(admin, "REVOKE ADMIN, WRITE ON world FROM bot"), "+OK\r\n");
    assert(server.users->Generation() == generation + 1);
    assert(server.users->Find("bot")->grants.at("world") == chunkdb::Right::kRead);
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world v = 8"), "PERMISSION_DENIED");
    ExpectReply(server.Run(admin, "MIGRATE 'rights' GRANT WRITE, READ ON world TO bot"), "+applied\r\n");
    ExpectReply(server.Run(admin, "MIGRATE 'rights' GRANT WRITE, READ ON world TO bot"), "+skipped\r\n");
    assert(server.users->Find("bot")->grants.at("world") == chunkdb::Right::kWrite);
    ExpectReply(server.Run(admin, "MIGRATE 'remove_rights' REVOKE WRITE, READ ON world FROM bot"), "+applied\r\n");
    assert(!server.users->Find("bot")->grants.contains("world"));
}

void TestConditionalUsers() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE USER IF NOT EXISTS bot VERIFIER $1", Parameters{VerifierText("original")}), "+OK\r\n");
    ExpectReply(server.Run(admin, "GRANT WRITE ON default TO bot"), "+OK\r\n");
    const auto before = server.users->Snapshot();
    const auto generation = server.users->Generation();
    const auto bytes = chunkdb::LoadFile(server.dir.path() / chunkdb::kUsersFileName);
    // A pre-existing user does not even convert the replacement verifier.
    ExpectReply(server.Run(admin, "CREATE USER IF NOT EXISTS bot VERIFIER 'invalid' MANAGES USERS"), "+OK\r\n");
    ExpectReply(server.Run(admin, "DROP USER IF EXISTS missing"), "+OK\r\n");
    assert(server.users->Snapshot() == before && server.users->Generation() == generation);
    assert(chunkdb::LoadFile(server.dir.path() / chunkdb::kUsersFileName) == bytes);
    auto bot = server.LoggedIn("bot", "original");
    for (const auto* name : {"bot", "missing"}) {
        ExpectError(server.Run(bot, std::string("CREATE USER IF NOT EXISTS ") + name + " VERIFIER 'invalid'"), "PERMISSION_DENIED");
        ExpectError(server.Run(bot, std::string("DROP USER IF EXISTS ") + name), "PERMISSION_DENIED");
    }
    ExpectError(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("new")}), "already exists");
    ExpectError(server.Run(admin, "DROP USER missing"), "does not exist");
    ExpectError(server.Run(admin, "DROP USER IF EXISTS admin"), "last user");
    ExpectReply(server.Run(admin, "DROP USER IF EXISTS bot"), "+OK\r\n");
    ExpectReply(server.Run(admin, "DROP USER IF EXISTS bot"), "+OK\r\n");
    assert(!server.users->Find("bot"));
}

void TestConcurrentConditionalUsers() {
    Server server;
    std::array sessions{server.LoggedIn("admin", "secret"), server.LoggedIn("admin", "secret")};
    const std::array verifiers{VerifierText("first"), VerifierText("second")};
    const auto generation = server.users->Generation();
    std::barrier start(3);
    std::array<std::string, 2> replies;
    std::array<std::exception_ptr, 2> errors;
    std::array<std::thread, 2> threads;
    for (std::size_t i = 0U; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] {
            start.arrive_and_wait();
            try { replies[i] = server.Run(sessions[i], "CREATE USER IF NOT EXISTS bot VERIFIER $1", Parameters{verifiers[i]}); }
            catch (const std::exception&) { errors[i] = std::current_exception(); }
        });
    }
    start.arrive_and_wait();
    for (auto& thread : threads) thread.join();
    for (const auto& error : errors) if (error) std::rethrow_exception(error);
    for (const auto& reply : replies) ExpectReply(reply, "+OK\r\n");
    assert(server.users->Generation() == generation + 1U && server.users->Snapshot().users.size() == 2U);
    const auto verifier = server.users->Find("bot")->verifier;
    assert(verifier == chunkdb::scram::ParseVerifier(verifiers[0]) || verifier == chunkdb::scram::ParseVerifier(verifiers[1]));
}

constexpr const char* kWrongLogin =
    "-ERR AUTH_FAILED reason=auth_failed invalid user or password; check the user name and password your client sends, or reset the password with chunkdb_admin reset-password\r\n";

// A session as the server fills it after a TLS handshake; `exporter` empty
// stands for TLS 1.2.
SessionState TlsSession(std::vector<std::uint8_t> exporter, std::vector<std::uint8_t> end_point) {
    SessionState session;
    session.channel_bindings = chunkdb::ChannelBindings{
        .tls = true, .tls_exporter = std::move(exporter), .tls_server_end_point = std::move(end_point)};
    return session;
}

// A login checked under one channel binding requirement does not finish
// after the requirement changes, in either direction: an unbound login that
// now needs binding fails like a wrong password, and a bound login is stale.
void TestStaleLoginAfterBindingRequirementChange() {
    const std::vector<std::uint8_t> exporter(32, 0x31);
    const std::vector<std::uint8_t> end_point(32, 0x32);
    for (const bool required_before : {false, true}) {
        Server server;
        server.users->Create("bot", chunkdb::scram::MakeVerifier("hunter2", std::vector<std::uint8_t>(16U, 7U),
            chunkdb::scram::kMinIterations), false, false, required_before);
        SessionState session = TlsSession(exporter, end_point);
        // Bound while the user requires it, unbound while it does not: a
        // login that would succeed if the requirement stayed.
        const auto type = required_before ? chunkdb::scram::kTlsExporter : std::string_view{};
        const auto client = chunkdb::scram::StartClientLogin("bot", chunkdb::scram::NewNonce(), type);
        const auto first = server.Run(session, "HELLO 3 USER bot $1", Parameters{client.first});
        assert(first.starts_with("+SCRAM "));
        const auto final = chunkdb::scram::FinishClientLogin(client, "hunter2", first.substr(7, first.size() - 9),
            required_before ? std::span<const std::uint8_t>(exporter) : std::span<const std::uint8_t>{});
        server.users->SetRequiresChannelBinding("bot", !required_before);
        const auto reply = server.Run(session, "AUTH $1", Parameters{final.message});
        if (required_before) {
            ExpectError(reply, "AUTH_FAILED reason=stale_login");
            assert(session.failed_auth_attempts == 0U);
        } else {
            ExpectReply(reply, kWrongLogin);
            assert(session.failed_auth_attempts == 1U);
        }
        assert(!session.greeted && !session.authenticated && !session.user_ticket);
        assert(server.users->AuthenticationSnapshot("bot")->state->connections == 0U);
    }
}

// Logins bound to their TLS connection (docs/PROTOCOL.md): both binding
// types, a login made on another connection, and what a connection cannot
// give.
void TestChannelBinding() {
    Server server;
    const std::vector<std::uint8_t> exporter(32, 0x31);
    const std::vector<std::uint8_t> end_point(32, 0x32);
    const std::vector<std::uint8_t> relay(32, 0x33);

    for (const auto& [type, data] :
         {std::pair{chunkdb::scram::kTlsExporter, exporter}, std::pair{chunkdb::scram::kTlsServerEndPoint, end_point}}) {
        SessionState bound = TlsSession(exporter, end_point);
        const std::string reply = server.Login(bound, "admin", "secret", type, data);
        assert(reply.rfind("%14\r\n", 0) == 0 && bound.greeted && bound.user == "admin");
        assert(Contains(reply, "$17\r\nstatement_timeout\r\n$15\r\nchannel_binding\r\n"));

        // The right password, bound to the connection of whoever relays it.
        SessionState relayed = TlsSession(exporter, end_point);
        ExpectError(
            server.Login(relayed, "admin", "secret", type, relay),
            "AUTH_FAILED reason=channel_binding the login is bound to another TLS connection");
        assert(!relayed.greeted && relayed.failed_auth_attempts == 1U);
        // An unknown user gets the same answer: it tells nothing about users.
        SessionState ghost = TlsSession(exporter, end_point);
        ExpectError(server.Login(ghost, "ghost", "secret", type, relay), "AUTH_FAILED reason=channel_binding");
        SessionState bound_ghost = TlsSession(exporter, end_point);
        ExpectReply(server.Login(bound_ghost, "ghost", "secret", type, data), kWrongLogin);
        SessionState wrong = TlsSession(exporter, end_point);
        ExpectReply(server.Login(wrong, "admin", "Secret", type, data), kWrongLogin);
    }

    // A login that is not bound stays possible unless something requires it.
    SessionState unbound = TlsSession(exporter, end_point);
    assert(server.Login(unbound, "admin", "secret").rfind("%14\r\n", 0) == 0);

    // A client that could bind but was told the server cannot: refused on
    // TLS, where this server can, and an ordinary login without TLS.
    SessionState downgraded = TlsSession(exporter, end_point);
    ExpectError(
        server.Run(downgraded, "HELLO 3 USER admin $1", Parameters{std::string("y,,n=admin,r=abc")}),
        "INVALID_ARGUMENT this server binds logins to TLS connections");
    SessionState plain_unused;
    assert(server.Run(plain_unused, "HELLO 3 USER admin $1", Parameters{std::string("y,,n=admin,r=abc")}).starts_with("+SCRAM "));

    // What a connection cannot give.
    SessionState plain;
    ExpectError(
        server.Login(plain, "admin", "secret", chunkdb::scram::kTlsExporter, exporter),
        "INVALID_ARGUMENT channel binding needs a TLS connection that ends at this server");
    SessionState tls12 = TlsSession({}, end_point);
    ExpectError(
        server.Login(tls12, "admin", "secret", chunkdb::scram::kTlsExporter, exporter),
        "INVALID_ARGUMENT tls-exporter channel binding needs TLS 1.3");
    assert(server.Login(tls12, "admin", "secret", chunkdb::scram::kTlsServerEndPoint, end_point).rfind("%14\r\n", 0) == 0);
    SessionState no_hash = TlsSession(exporter, {});
    ExpectError(
        server.Login(no_hash, "admin", "secret", chunkdb::scram::kTlsServerEndPoint, end_point),
        "INVALID_ARGUMENT tls-server-end-point channel binding is not defined");
    SessionState unknown = TlsSession(exporter, end_point);
    ExpectError(
        server.Login(unknown, "admin", "secret", "tls-unique", exporter),
        "INVALID_ARGUMENT unknown channel binding type");
    assert(unknown.failed_auth_attempts == 1U && !unknown.close_after_reply);
}

// A server, and a user, that take only bound logins.
void TestRequiredChannelBinding() {
    const std::vector<std::uint8_t> exporter(32, 0x31);
    const std::vector<std::uint8_t> end_point(32, 0x32);
    {
        Server strict(true, true);
        SessionState unbound = TlsSession(exporter, end_point);
        ExpectError(
            strict.Login(unbound, "admin", "secret"),
            "AUTH_REQUIRED reason=channel_binding this server takes only logins bound to their TLS connection");
        // Before the user is looked up: the same for a user that does not exist.
        SessionState ghost = TlsSession(exporter, end_point);
        ExpectError(strict.Login(ghost, "ghost", "secret"), "AUTH_REQUIRED reason=channel_binding");
        SessionState plain;
        ExpectError(strict.Login(plain, "admin", "secret"), "AUTH_REQUIRED reason=channel_binding");
        SessionState bound = TlsSession(exporter, end_point);
        assert(strict.Login(bound, "admin", "secret", chunkdb::scram::kTlsExporter, exporter).rfind("%14\r\n", 0) == 0);
    }

    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    ExpectReply(
        server.Run(admin, "CREATE USER bot VERIFIER $1 REQUIRE CHANNEL BINDING", Parameters{VerifierText("hunter2")}),
        "+OK\r\n");
    assert(server.users->Find("bot")->requires_channel_binding && !server.users->Find("admin")->requires_channel_binding);
    assert(Contains(
        server.Run(admin, "SHOW USERS"),
        "$3\r\nbot\r\n$13\r\nmanages_users\r\n#f\r\n$24\r\nrequires_channel_binding\r\n#t\r\n"));

    // An unbound login of that user fails like a wrong password, so the
    // reply does not tell that the user exists.
    SessionState unbound = TlsSession(exporter, end_point);
    ExpectReply(server.Login(unbound, "bot", "hunter2"), kWrongLogin);
    SessionState plain;
    ExpectReply(server.Login(plain, "bot", "hunter2"), kWrongLogin);
    assert(!unbound.greeted && !plain.greeted);
    SessionState bound = TlsSession(exporter, end_point);
    assert(server.Login(bound, "bot", "hunter2", chunkdb::scram::kTlsServerEndPoint, end_point).rfind("%14\r\n", 0) == 0);

    // At the user's connection limit, which `bound` reaches, an unbound login
    // with the right password still fails like a wrong one, and a login
    // bound to another connection still says so; only a valid login learns
    // of the limit.
    server.users->SetLimits("bot", 1U, std::nullopt);
    SessionState limited = TlsSession(exporter, end_point);
    ExpectReply(server.Login(limited, "bot", "hunter2"), kWrongLogin);
    SessionState relayed = TlsSession(exporter, end_point);
    ExpectError(server.Login(relayed, "bot", "hunter2", chunkdb::scram::kTlsExporter, std::vector<std::uint8_t>(32, 0x33)),
        "AUTH_FAILED reason=channel_binding");
    SessionState valid = TlsSession(exporter, end_point);
    ExpectError(server.Login(valid, "bot", "hunter2", chunkdb::scram::kTlsExporter, exporter), "RATE_LIMITED reason=max_connections");
    assert(!limited.greeted && !relayed.greeted && !valid.greeted && !limited.user_ticket && !relayed.user_ticket);
    server.users->SetLimits("bot", 0U, std::nullopt);

    ExpectError(server.Run(bound, "ALTER USER bot NO REQUIRE CHANNEL BINDING"), "PERMISSION_DENIED MANAGES USERS");
    ExpectReply(server.Run(admin, "ALTER USER bot NO REQUIRE CHANNEL BINDING"), "+OK\r\n");
    SessionState relaxed;
    assert(server.Login(relaxed, "bot", "hunter2").rfind("%14\r\n", 0) == 0);
    ExpectReply(server.Run(admin, "ALTER USER bot REQUIRE CHANNEL BINDING"), "+OK\r\n");
    SessionState again;
    ExpectReply(server.Login(again, "bot", "hunter2"), kWrongLogin);
    ExpectError(server.Run(admin, "ALTER USER ghost REQUIRE CHANNEL BINDING"), "INVALID_ARGUMENT");
}

// A login checks the user's credentials as they are when AUTH finishes it,
// not as HELLO found them.
void TestCredentialsChangedDuringLogin() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    const std::string bot_verifier = VerifierText("hunter2");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{bot_verifier}), "+OK\r\n");
    // HELLO, then `change` from another session, then AUTH with the password
    // that was right at HELLO.
    const auto login_around = [&server](const std::string& password, const auto& change) {
        SessionState session;
        const auto login = chunkdb::scram::StartClientLogin("bot", chunkdb::scram::NewNonce());
        const std::string first = server.Run(session, "HELLO 3 USER bot $1", Parameters{login.first});
        assert(first.rfind("+SCRAM ", 0) == 0);
        change();
        const auto final_message =
            chunkdb::scram::FinishClientLogin(login, password, first.substr(7, first.size() - 9));
        const std::string reply = server.Run(session, "AUTH $1", Parameters{final_message.message});
        assert(session.greeted == (reply.rfind("%14\r\n", 0) == 0));
        return reply;
    };

    // A change that leaves the credentials alone does not stop the login.
    assert(login_around("hunter2", [&] {
        ExpectReply(server.Run(admin, "GRANT READ ON default TO bot"), "+OK\r\n");
    }).rfind("%14\r\n", 0) == 0);

    // A new verifier: the old password no longer logs in.
    ExpectReply(login_around("hunter2", [&] {
        ExpectReply(server.Run(admin, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("rotated")}), "+OK\r\n");
    }), kWrongLogin);
    (void)server.LoggedIn("bot", "rotated");

    // The user dropped and created again with the same verifier.
    ExpectReply(server.Run(admin, "ALTER USER bot VERIFIER $1", Parameters{bot_verifier}), "+OK\r\n");
    ExpectReply(login_around("hunter2", [&] {
        ExpectReply(server.Run(admin, "DROP USER bot"), "+OK\r\n");
        ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{bot_verifier}), "+OK\r\n");
    }), kWrongLogin);
    (void)server.LoggedIn("bot", "hunter2");

    // The user dropped.
    ExpectReply(login_around("hunter2", [&] {
        ExpectReply(server.Run(admin, "DROP USER bot"), "+OK\r\n");
    }), kWrongLogin);
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{bot_verifier}), "+OK\r\n");
    (void)server.LoggedIn("bot", "hunter2");

    // The user now takes only bound logins: this unbound one fails.
    ExpectReply(login_around("hunter2", [&] {
        ExpectReply(server.Run(admin, "ALTER USER bot REQUIRE CHANNEL BINDING"), "+OK\r\n");
    }), kWrongLogin);
    SessionState unbound;
    ExpectReply(server.Login(unbound, "bot", "hunter2"), kWrongLogin);
    ExpectReply(server.Run(admin, "ALTER USER bot NO REQUIRE CHANNEL BINDING"), "+OK\r\n");
    (void)server.LoggedIn("bot", "hunter2");
}

// The verifier text of `password` with `salt`.
std::string SaltedVerifier(const std::string& password, const std::vector<std::uint8_t>& salt,
    std::uint32_t iterations = chunkdb::scram::kMinIterations) {
    return chunkdb::scram::FormatVerifier(chunkdb::scram::MakeVerifier(password, salt, iterations));
}

// ALTER USER ... ADD VERIFIER and DROP OLD VERIFIER rotate a password with
// both passwords logging in between: at most two, with one salt and
// iteration count, kept across a restart; a new password replaces both.
void TestTwoVerifiers() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    const std::vector<std::uint8_t> salt(16U, 9U);
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{SaltedVerifier("old", salt)}), "+OK\r\n");
    ExpectReply(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("new", salt)}), "+OK\r\n");
    SessionState bot_old = server.LoggedIn("bot", "old");
    (void)server.LoggedIn("bot", "new");
    SessionState wrong;
    ExpectReply(server.Login(wrong, "bot", "other"), kWrongLogin);

    // A verifier the user has changes nothing; a third one, or one with
    // another salt or iteration count, is refused.
    ExpectReply(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("new", salt)}), "+OK\r\n");
    ExpectReply(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("old", salt)}), "+OK\r\n");
    ExpectError(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("third", salt)}),
        "INVALID_ARGUMENT user bot already has two verifiers; DROP OLD VERIFIER first");
    const auto both = server.users->Find("bot");
    assert(both && both->added_verifier.has_value());
    // SHOW USERS tells a user with two verifiers from one with one.
    const std::string users = server.Run(admin, "SHOW USERS");
    assert(Contains(users, "$5\r\nadmin\r\n$13\r\nmanages_users\r\n#t\r\n$24\r\nrequires_channel_binding\r\n#f\r\n"
                           "$9\r\nverifiers\r\n:1\r\n"));
    assert(Contains(users, "$3\r\nbot\r\n$13\r\nmanages_users\r\n#f\r\n$24\r\nrequires_channel_binding\r\n#f\r\n"
                           "$9\r\nverifiers\r\n:2\r\n"));
    {
        const chunkdb::UserRegistry restarted(server.dir.path(), std::nullopt, std::array<std::uint8_t, 32>{7}, true);
        assert(restarted.Find("bot") == both);
    }

    ExpectReply(server.Run(admin, "ALTER USER bot DROP OLD VERIFIER"), "+OK\r\n");
    assert(Contains(server.Run(admin, "SHOW USERS"), "$3\r\nbot\r\n$13\r\nmanages_users\r\n#f\r\n$24\r\nrequires_channel_binding\r\n#f\r\n"
                                                     "$9\r\nverifiers\r\n:1\r\n"));
    SessionState dropped;
    ExpectReply(server.Login(dropped, "bot", "old"), kWrongLogin);
    (void)server.LoggedIn("bot", "new");
    // A connection logged in with the dropped password stays open.
    ExpectReply(server.Run(bot_old, "PING"), "+PONG\r\n");
    ExpectReply(server.Run(admin, "ALTER USER bot DROP OLD VERIFIER"), "+OK\r\n");
    (void)server.LoggedIn("bot", "new");
    ExpectError(server.Run(admin, "ALTER USER bot ADD VERIFIER $1",
                    Parameters{SaltedVerifier("next", std::vector<std::uint8_t>(16U, 10U))}),
        "INVALID_ARGUMENT an added verifier must use the salt and iteration count of the user's verifier");
    ExpectError(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("next", salt, 8192U)}),
        "INVALID_ARGUMENT an added verifier must use the salt and iteration count");
    ExpectError(server.Run(admin, "ALTER USER ghost ADD VERIFIER $1", Parameters{SaltedVerifier("next", salt)}),
        "INVALID_ARGUMENT user ghost does not exist");
    ExpectError(server.Run(admin, "ALTER USER ghost DROP OLD VERIFIER"), "INVALID_ARGUMENT user ghost does not exist");

    // A user rotates its own password without MANAGES USERS, but not another's.
    SessionState bot = server.LoggedIn("bot", "new");
    ExpectReply(server.Run(bot, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("own", salt)}), "+OK\r\n");
    (void)server.LoggedIn("bot", "own");
    ExpectError(server.Run(bot, "ALTER USER admin ADD VERIFIER $1", Parameters{SaltedVerifier("own", salt)}),
        "PERMISSION_DENIED MANAGES USERS");
    ExpectError(server.Run(bot, "ALTER USER admin DROP OLD VERIFIER"), "PERMISSION_DENIED MANAGES USERS");
    ExpectReply(server.Run(bot, "ALTER USER bot DROP OLD VERIFIER"), "+OK\r\n");
    SessionState replaced;
    ExpectReply(server.Login(replaced, "bot", "new"), kWrongLogin);
    (void)server.LoggedIn("bot", "own");

    // A new password, also the user's own, leaves one verifier.
    ExpectReply(server.Run(admin, "ALTER USER bot ADD VERIFIER $1", Parameters{SaltedVerifier("second", salt)}), "+OK\r\n");
    ExpectReply(server.Run(bot, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("reset")}), "+OK\r\n");
    assert(!server.users->Find("bot")->added_verifier.has_value());
    SessionState second;
    ExpectReply(server.Login(second, "bot", "second"), kWrongLogin);
    (void)server.LoggedIn("bot", "reset");
}

// A login between HELLO and AUTH while the verifiers change: one started
// before ADD VERIFIER finishes, one whose verifier DROP OLD VERIFIER removes
// fails like a wrong password, and one with the kept verifier is stale.
void TestTwoVerifiersDuringLogin() {
    Server server;
    const std::vector<std::uint8_t> salt(16U, 9U);
    const auto verifier = [&salt](const std::string& password) {
        return chunkdb::scram::MakeVerifier(password, salt, chunkdb::scram::kMinIterations);
    };
    server.users->Create("bot", verifier("old"), false);
    // HELLO, then `change`, then AUTH: the reply and the failures counted.
    const auto login_around = [&server](const std::string& password, const auto& change) {
        SessionState session;
        const auto login = chunkdb::scram::StartClientLogin("bot", chunkdb::scram::NewNonce());
        const std::string first = server.Run(session, "HELLO 3 USER bot $1", Parameters{login.first});
        assert(first.rfind("+SCRAM ", 0) == 0);
        change();
        const auto final_message = chunkdb::scram::FinishClientLogin(login, password, first.substr(7, first.size() - 9));
        const std::string reply = server.Run(session, "AUTH $1", Parameters{final_message.message});
        assert(session.greeted == (reply.rfind("%14\r\n", 0) == 0));
        return std::make_pair(reply, session.failed_auth_attempts);
    };
    const auto added = login_around("old", [&] { server.users->AddVerifier("bot", verifier("new")); });
    assert(added.first.rfind("%14\r\n", 0) == 0 && added.second == 0U);
    const auto removed = login_around("old", [&] { server.users->DropOldVerifier("bot"); });
    ExpectReply(removed.first, kWrongLogin);
    assert(removed.second == 1U);
    server.users->AddVerifier("bot", verifier("next"));
    const auto kept = login_around("next", [&] { server.users->DropOldVerifier("bot"); });
    ExpectError(kept.first, "AUTH_FAILED reason=stale_login");
    assert(kept.second == 0U);
    (void)server.LoggedIn("bot", "next");
}

// --auth none: no users, every right.
void TestWithoutUsers() {
    Server server(false);
    SessionState session;
    assert(server.Run(session, "HELLO 3").rfind("%14\r\n", 0) == 0);
    assert(Contains(server.Run(session, "SHOW TABLES"), "default"));
    ExpectReply(server.Run(session, "GET BLOCK 0 0 FROM default"), "_\r\n");
    ExpectError(server.Run(session, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("x")}), "runs without users");
    SessionState named;
    ExpectError(server.Run(named, "HELLO 3 USER bot $1", Parameters{std::string("n,,n=bot,r=abc")}), "runs without users");
}

// A data directory opened read-only: user and right statements are refused
// before any right is checked, SHOW USERS still answers, and chunkdb.users
// is not written.
void TestReadOnly() {
    Server server;
    {
        SessionState admin = server.LoggedIn("admin", "secret");
        ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("pw")}), "+OK\r\n");
    }
    server.engine.reset();
    server.catalog.reset();
    chunkdb::CatalogConfig config;
    config.data_dir = server.dir.path();
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    server.catalog = std::make_shared<chunkdb::TableCatalog>(config);
    const auto users_path = server.dir.path() / chunkdb::kUsersFileName;
#ifndef _WIN32
    // A users file this process cannot change: wider than owner-only and,
    // on macOS, immutable, so a chmod fails as it does on a read-only mount.
    constexpr auto kWide = std::filesystem::perms::owner_read | std::filesystem::perms::owner_write |
                           std::filesystem::perms::group_read | std::filesystem::perms::others_read;
    std::filesystem::permissions(users_path, kWide);
#ifdef __APPLE__
    assert(chflags(users_path.c_str(), UF_IMMUTABLE) == 0);
#endif
#endif
    server.users =
        std::make_shared<chunkdb::UserRegistry>(server.dir.path(), std::nullopt, std::array<std::uint8_t, 32>{7}, true);
    server.engine = std::make_unique<CommandEngine>(
        chunkdb::EngineConfig{.require_auth = true, .users = server.users, .max_auth_failures = 5}, server.catalog);
    const auto users_file = chunkdb::LoadFile(server.dir.path() / chunkdb::kUsersFileName);

    SessionState admin = server.LoggedIn("admin", "secret");
    SessionState bot = server.LoggedIn("bot", "pw");
    auto controls = std::make_shared<chunkdb::SessionControlCallbacks>();
    controls->enumerate = [](const auto&, const auto&) {};
    controls->cancel = [](const auto&, auto) { return false; };
    controls->kill = [](const auto&, auto) { return false; };
    admin.connection_controls = controls;
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), "*0\r\n");
    ExpectReply(server.Run(admin, "CANCEL 0"), "%1\r\n$9\r\ncancelled\r\n#f\r\n");
    ExpectReply(server.Run(admin, "KILL CONNECTION 0"), "%1\r\n$6\r\nkilled\r\n#f\r\n");
    const auto refused = [&](SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        ExpectError(server.Run(session, line, parameters),
                    "INVALID_ARGUMENT " + line.substr(0, line.find(' ', line.find(' ') + 1U)) +
                        " is not available: the server opened its data directory read-only");
    };
    refused(admin, "CREATE USER other VERIFIER $1", Parameters{VerifierText("x")});
    refused(admin, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("y")});
    refused(bot, "ALTER USER bot VERIFIER $1", Parameters{VerifierText("y")});
    refused(admin, "ALTER USER bot MANAGES USERS");
    refused(admin, "DROP USER bot");
    // A user without the right gets the same answer.
    refused(bot, "DROP USER admin");
    ExpectError(server.Run(admin, "GRANT READ ON default TO bot"),
                "INVALID_ARGUMENT GRANT is not available: the server opened its data directory read-only");
    ExpectError(server.Run(admin, "REVOKE READ ON default FROM bot"),
                "INVALID_ARGUMENT REVOKE is not available: the server opened its data directory read-only");
    ExpectError(server.Run(admin, "MIGRATE 'grant' GRANT READ ON default TO bot"),
                "INVALID_ARGUMENT MIGRATE is not available: the server opened its data directory read-only");
    assert(Contains(server.Run(admin, "SHOW USERS"), "bot"));
    ExpectReply(server.Run(admin, "GET BLOCK 0 0 FROM default"), "_\r\n");
    // Rights still decide what a user reads.
    // The registry itself refuses changes too.
    bool refused_change = false;
    try { server.users->Drop("bot"); } catch (const std::invalid_argument& error) {
        refused_change = Contains(error.what(), "read-only");
    }
    assert(refused_change);
    ExpectError(server.Run(bot, "GET BLOCK 0 0 FROM default"), "NO_TABLE");
    assert(chunkdb::LoadFile(server.dir.path() / chunkdb::kUsersFileName) == users_file);
#ifndef _WIN32
    assert((std::filesystem::status(users_path).permissions() & std::filesystem::perms::mask) == kWide);
#ifdef __APPLE__
    assert(chflags(users_path.c_str(), 0) == 0);
#endif
#endif
}

std::string VerifierWithSalt(const std::string& password, std::size_t salt_bytes) {
    return chunkdb::scram::FormatVerifier(chunkdb::scram::MakeVerifier(
        password, std::vector<std::uint8_t>(salt_bytes, 0x5a), chunkdb::scram::kMinIterations));
}

// A verifier salt must fit the users file field in both ALTER USER forms, and
// a grant names a table the file reads back: refused statements leave the
// file as it was, and the server reopens it.
void TestVerifierAndGrantBounds() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("hunter2")}), "+OK\r\n");
    SessionState bot = server.LoggedIn("bot", "hunter2");
    const auto users_path = server.dir.path() / chunkdb::kUsersFileName;
    const auto before = chunkdb::LoadFile(users_path);

    const std::string long_salt = VerifierWithSalt("new", chunkdb::scram::kMaxSaltBytes + 1U);
    ExpectError(server.Run(bot, "ALTER USER bot VERIFIER $1", Parameters{long_salt}),
                "INVALID_ARGUMENT a verifier's salt must be 16 to 256 bytes");
    ExpectError(server.Run(bot, "ALTER USER bot VERIFIER '" + long_salt + "'"),
                "INVALID_ARGUMENT a verifier's salt must be 16 to 256 bytes");
    ExpectError(server.Run(admin, "CREATE USER eve VERIFIER '" + long_salt + "'"),
                "INVALID_ARGUMENT a verifier's salt must be 16 to 256 bytes");
    const std::string oversized = "SCRAM-SHA-256$4096:" + std::string(600, 'A') + "$x:y";
    ExpectError(server.Run(bot, "ALTER USER bot VERIFIER '" + oversized + "'"),
                "INVALID_ARGUMENT a verifier has at most 512 bytes");
    ExpectError(server.Run(admin, "GRANT READ ON con TO bot"), "INVALID_ARGUMENT invalid table name 'con'");
    ExpectError(server.Run(admin, "GRANT READ ON _hidden TO bot"), "INVALID_ARGUMENT invalid table name '_hidden'");
    ExpectError(server.Run(admin, "GRANT READ ON " + std::string(65, 'a') + " TO bot"), "INVALID_ARGUMENT invalid table name");
    assert(chunkdb::LoadFile(users_path) == before);

    // The longest salt is kept in both forms and survives a reopen.
    ExpectReply(server.Run(bot, "ALTER USER bot VERIFIER '" + VerifierWithSalt("literal", chunkdb::scram::kMaxSaltBytes) + "'"),
                "+OK\r\n");
    (void)server.LoggedIn("bot", "literal");
    ExpectReply(server.Run(bot, "ALTER USER bot VERIFIER $1",
                           Parameters{VerifierWithSalt("longest", chunkdb::scram::kMaxSaltBytes)}), "+OK\r\n");
    ExpectReply(server.Run(admin, "GRANT READ ON not_yet_created TO bot"), "+OK\r\n");
    const auto reopened = chunkdb::ReadUsersFile(server.dir.path());
    assert(reopened.has_value() && reopened->users.at("bot").verifier.salt.size() == chunkdb::scram::kMaxSaltBytes);
    assert(reopened->users.at("bot").grants.at("not_yet_created") == chunkdb::Right::kRead);
    (void)server.LoggedIn("bot", "longest");
}

}  // namespace

void TestSessionControls() {
    Server server;
    auto admin = server.LoggedIn("admin", "secret");
    ExpectReply(server.Run(admin, "CREATE USER bot VERIFIER $1", {VerifierText("secret")}), "+OK\r\n");
    auto bot = server.LoggedIn("bot", "secret");
    ExpectError(server.Run(admin, "SHOW CONNECTIONS"), "INVALID_ARGUMENT");
    auto callbacks = std::make_shared<chunkdb::SessionControlCallbacks>();
    chunkdb::SessionControlAccess seen;
    bool accept = true;
    unsigned calls = 0U;
    callbacks->cancel = [&](const auto& access, std::uint64_t id) { seen = access; assert(id == 71U); ++calls; return accept; };
    callbacks->kill = [&](const auto& access, std::uint64_t id) { seen = access; assert(id == 71U); ++calls; return accept; };
    admin.connection_controls = callbacks;
    bot.connection_controls = callbacks;
    const auto cancelled = "%1\r\n$9\r\ncancelled\r\n#t\r\n";
    const auto not_cancelled = "%1\r\n$9\r\ncancelled\r\n#f\r\n";
    ExpectReply(server.Run(admin, "CANCEL 71"), cancelled);
    assert(seen.admin_all && seen.requester_connection_id == admin.connection_id);
    ExpectReply(server.Run(bot, "CANCEL 71"), cancelled);
    assert(!seen.admin_all && seen.user_incarnation == bot.user_incarnation);
    server.users->Grant("bot", "default", chunkdb::Right::kAdmin);
    ExpectReply(server.Run(bot, "CANCEL 71"), cancelled);
    assert(!seen.admin_all);
    server.users->Grant("bot", "*", chunkdb::Right::kAdmin);
    ExpectReply(server.Run(bot, "CANCEL 71"), cancelled);
    assert(seen.admin_all);
    server.users->Revoke("bot", "*", chunkdb::Right::kAdmin);
    ExpectReply(server.Run(bot, "CANCEL 71"), cancelled);
    assert(!seen.admin_all);
    const auto previous = calls;
    ExpectReply(server.Run(bot, "CANCEL " + std::to_string(bot.connection_id)), not_cancelled);
    assert(calls == previous);
    accept = false;
    ExpectReply(server.Run(admin, "CANCEL 71"), not_cancelled);
    ExpectReply(server.Run(admin, "KILL CONNECTION 71"), "%1\r\n$6\r\nkilled\r\n#f\r\n");
    assert(!admin.close_after_reply);
    callbacks->kill = [&](const auto&, std::uint64_t id) { assert(id == bot.connection_id); return true; };
    ExpectReply(server.Run(bot, "KILL CONNECTION " + std::to_string(bot.connection_id)), "%1\r\n$6\r\nkilled\r\n#t\r\n");
    assert(bot.close_after_reply);
    bot.close_after_reply = false;
    callbacks->enumerate = [&](const auto& access, const auto& visitor) {
        seen = access;
        visitor(chunkdb::ConnectionInfo{.id = UINT64_MAX, .user = "bot", .peer = "127.0.0.1", .state = "idle", .statement = ""});
        visitor(chunkdb::ConnectionInfo{.id = 1U, .user = "", .peer = "local", .state = "running", .statement = "RESTORE",
            .progress = chunkdb::StatementProgress{.files_done = 1U, .files_total = 3U, .bytes_done = 10U, .bytes_total = UINT64_MAX}});
    };
    const std::string expected = "*2\r\n%6\r\n$2\r\nid\r\n(18446744073709551615\r\n$4\r\nuser\r\n$3\r\nbot\r\n$4\r\npeer\r\n$9\r\n127.0.0.1\r\n$5\r\nstate\r\n$4\r\nidle\r\n$9\r\nstatement\r\n_\r\n$8\r\nprogress\r\n_\r\n"
        "%6\r\n$2\r\nid\r\n:1\r\n$4\r\nuser\r\n_\r\n$4\r\npeer\r\n$5\r\nlocal\r\n$5\r\nstate\r\n$7\r\nrunning\r\n$9\r\nstatement\r\n$7\r\nRESTORE\r\n"
        "$8\r\nprogress\r\n%4\r\n$10\r\nfiles_done\r\n:1\r\n$11\r\nfiles_total\r\n:3\r\n$10\r\nbytes_done\r\n:10\r\n$11\r\nbytes_total\r\n(18446744073709551615\r\n";
    callbacks->max_response_bytes = expected.size();
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), expected);
    --callbacks->max_response_bytes;
    ExpectError(server.Run(admin, "SHOW CONNECTIONS"), "OUT_OF_RANGE reason=response_limit");
    callbacks->max_response_bytes = expected.size();
    ExpectReply(server.Run(admin, "BEGIN"), "+OK\r\n");
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), expected);
    admin.transaction->Abort("-ERR CONFLICT reason=test\r\n");
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), expected);
    ExpectReply(server.Run(admin, "CANCEL 71"), not_cancelled);
    assert(admin.transaction && admin.transaction->aborted);
    ExpectReply(server.Run(admin, "ROLLBACK"), "+OK\r\n");
    callbacks->enumerate = [](const auto&, const auto&) {};
    callbacks->max_response_bytes = 4U;
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), "*0\r\n");
    callbacks->max_response_bytes = 3U;
    ExpectError(server.Run(admin, "SHOW CONNECTIONS"), "OUT_OF_RANGE reason=response_limit");
    callbacks->enumerate = [](const auto&, const auto& visitor) {
        for (unsigned i = 0U; i < 10U; ++i)
            visitor(chunkdb::ConnectionInfo{.id = i, .user = "", .peer = "", .state = "idle", .statement = ""});
    };
    callbacks->max_response_bytes = 10000U;
    const auto ten = server.Run(admin, "SHOW CONNECTIONS");
    assert(ten.starts_with("*10\r\n"));
    callbacks->max_response_bytes = ten.size();
    ExpectReply(server.Run(admin, "SHOW CONNECTIONS"), ten);
    --callbacks->max_response_bytes;
    ExpectError(server.Run(admin, "SHOW CONNECTIONS"), "OUT_OF_RANGE reason=response_limit");
    // Control access must stop matching a retired user, even after name reuse:
    // the session of the dropped user controls nothing and closes.
    server.users->Drop("bot");
    server.users->Create("bot", chunkdb::scram::ParseVerifier(VerifierText("secret")), false);
    const auto before_retired = calls;
    ExpectError(server.Run(bot, "CANCEL 71"), "AUTH_REQUIRED reason=user_dropped");
    assert(bot.close_after_reply && calls == before_retired);
    Server plain(false);
    SessionState anonymous;
    ExpectReply(plain.Run(anonymous, "HELLO 3").substr(0U, 1U), "%");
    anonymous.connection_controls = callbacks;
    ExpectReply(plain.Run(anonymous, "CANCEL 71"), not_cancelled);
    assert(seen.admin_all);
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "controls") { TestSessionControls(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "read_only") { TestReadOnly(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "stale_auth") { TestStaleAuthenticationProofs(); TestStaleLoginAfterBindingRequirementChange(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "admission") { TestUserLimitStatementsAndAdmissionHook(); return 0; }
    if (argc != 1) return 2;
    TestAtomicRightsLists();
    TestSessionControls();
    TestVerifierAndGrantBounds();
    TestLogin();
    TestStaleAuthenticationProofs();
    TestUserLimitStatementsAndAdmissionHook();
    TestRights();
    TestServerRights();
    TestPrefixGrants();
    TestRecreatedUser();
    TestConditionalUsers();
    TestConcurrentConditionalUsers();
    TestChannelBinding();
    TestRequiredChannelBinding();
    TestStaleLoginAfterBindingRequirementChange();
    TestCredentialsChangedDuringLogin();
    TestTwoVerifiers();
    TestTwoVerifiersDuringLogin();
    TestWithoutUsers();
    TestReadOnly();
    return 0;
}
