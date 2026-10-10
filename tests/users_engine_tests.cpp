// Users through the engine (docs/design/USERS_DESIGN.md): SCRAM-SHA-256 login in
// HELLO, the user statements, and the right every statement needs.

#include <cassert>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"
#include "scram.hpp"
#include "test_utils.hpp"
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
    if (reply.rfind("-ERR ", 0) != 0 || !Contains(reply, part)) {
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

    explicit Server(bool require_auth = true) {
        chunkdb::CatalogConfig config;
        config.data_dir = dir.path();
        catalog = std::make_shared<chunkdb::TableCatalog>(config);
        if (require_auth) {
            users = std::make_shared<chunkdb::UserRegistry>(
                dir.path(),
                std::make_pair(
                    std::string("admin"),
                    chunkdb::scram::MakeVerifier("secret", chunkdb::crypto::RandomBytes(16), chunkdb::scram::kMinIterations)),
                std::array<std::uint8_t, 32>{7});
        }
        engine = std::make_unique<CommandEngine>(
            chunkdb::EngineConfig{.require_auth = require_auth, .users = users, .max_auth_failures = 5}, catalog);
    }

    std::string Run(SessionState& session, const std::string& line, const Parameters& parameters = {}) {
        return engine->Execute(session, line + "\r\n", parameters);
    }

    // HELLO 3 USER and AUTH; the final reply, after checking the server's
    // signature when the login succeeds.
    std::string Login(SessionState& session, const std::string& user, const std::string& password) {
        const auto login = chunkdb::scram::StartClientLogin(user, chunkdb::scram::NewNonce());
        const std::string first = Run(session, "HELLO 3 USER " + user + " $1", Parameters{login.first});
        if (first.rfind("+SCRAM ", 0) != 0) {
            return first;
        }
        const std::string server_first = first.substr(7, first.size() - 9);
        const auto final_message = chunkdb::scram::FinishClientLogin(login, password, server_first);
        const std::string reply = Run(session, "AUTH $1", Parameters{final_message.message});
        if (reply.rfind("%8\r\n", 0) == 0) {
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
        assert(reply.rfind("%8\r\n", 0) == 0 && session.greeted && session.user == user);
        return session;
    }
};

void TestLogin() {
    Server server;
    SessionState admin = server.LoggedIn("admin", "secret");
    ExpectError(server.Run(admin, "HELLO 3"), "PROTOCOL HELLO was already sent");

    SessionState wrong;
    ExpectReply(server.Login(wrong, "admin", "Secret"), "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");
    assert(!wrong.greeted);
    SessionState ghost;
    ExpectReply(server.Login(ghost, "ghost", "secret"), "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");

    SessionState anonymous;
    ExpectError(server.Run(anonymous, "HELLO 3"), "AUTH_REQUIRED use HELLO 3 USER <name> $1");
    SessionState token;
    ExpectError(server.Run(token, "HELLO 3 AUTH secret"), "INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1");
    SessionState early;
    ExpectError(server.Run(early, "AUTH $1", Parameters{std::string("c=biws,r=x,p=AAAA")}), "PROTOCOL AUTH follows HELLO 3 USER");
    assert(early.close_after_reply);
    SessionState binding;
    ExpectError(server.Run(binding, "HELLO 3 USER admin $1", Parameters{std::string("y,,n=admin,r=abc")}), "INVALID_ARGUMENT");
    SessionState other_name;
    ExpectError(
        server.Run(other_name, "HELLO 3 USER admin $1", Parameters{std::string("n,,n=bot,r=abc")}),
        "names another user than HELLO");

    // Before HELLO only the SCRAM frames are read, each bounded.
    SessionState fresh;
    const auto hello_plan = server.engine->PlanPayload(fresh, "HELLO 3 USER admin $1\r\n");
    assert(hello_plan.plan == CommandEngine::PayloadPlan::kParameters && hello_plan.parameter_limits.size() == 1);
    assert(server.engine->PlanPayload(fresh, "AUTH $1\r\n").plan == CommandEngine::PayloadPlan::kParameters);
    assert(server.engine->PlanPayload(fresh, "SET BLOCK 0 0 IN default bits = $1\r\n").plan == CommandEngine::PayloadPlan::kNone);

    // A statement before logging in closes the connection.
    SessionState idle;
    ExpectError(server.Run(idle, "GET BLOCK 0 0 FROM default"), "PROTOCOL expected HELLO 3");
    assert(idle.close_after_reply);
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
    assert(hidden.plan == CommandEngine::PayloadPlan::kReject && Contains(hidden.reject_response, "NO_TABLE"));

    ExpectReply(server.Run(admin, "GRANT READ ON world TO bot"), "+OK\r\n");
    ExpectReply(server.Run(bot, "GET BLOCK 0 0 FROM world"), "_\r\n");
    ExpectReply(server.Run(bot, "SHOW TABLES"), "*1\r\n$5\r\nworld\r\n");
    const auto visible_slots = server.Run(bot, "SHOW SLOTS");
    assert(visible_slots.starts_with("*1\r\n") && Contains(visible_slots, "consumer") && !Contains(visible_slots, "hidden"));
    assert(server.Run(bot, "SHOW SLOTS ON world") == visible_slots);
    ExpectError(server.Run(bot, "CREATE SLOT 'mine' ON world"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "DROP SLOT 'consumer' ON world"), "PERMISSION_DENIED ADMIN on world");
    assert(server.Run(bot, "DESCRIBE world").rfind("%6", 0) == 0);
    assert(server.Run(bot, "SCAN CHUNKS FROM world").rfind("%2", 0) == 0);
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "PERMISSION_DENIED WRITE on world");
    ExpectError(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'"), "ask an administrator to grant this right");
    ExpectError(server.Run(bot, "FLUSH WAL"), "PERMISSION_DENIED WRITE on a table");

    ExpectReply(server.Run(admin, "GRANT WRITE ON world TO bot"), "+OK\r\n");
    assert(server.Run(bot, "SET BLOCK 0 0 IN world bits = b'1010'").rfind(":", 0) == 0);
    assert(server.Run(bot, "DELETE BLOCK 0 0 FROM world").rfind(":", 0) == 0);
    ExpectReply(server.Run(bot, "FLUSH WAL"), "+OK\r\n");
    ExpectError(server.Run(bot, "ALTER TABLE world ADD COLUMN extra u8 NULL"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "DROP TABLE world"), "PERMISSION_DENIED ADMIN on world");
    ExpectError(server.Run(bot, "CREATE TABLE mine (bits bits(4)) CHUNK 4 x 4"), "PERMISSION_DENIED ADMIN on *");
    ExpectError(server.Run(bot, "SHOW METRICS"), "PERMISSION_DENIED ADMIN on *");
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

    // A dropped user keeps no right in a session still open.
    ExpectReply(server.Run(admin, "DROP USER bot"), "+OK\r\n");
    ExpectError(server.Run(bot, "GET BLOCK 0 0 FROM world"), "NO_TABLE");
    SessionState gone;
    ExpectError(server.Login(gone, "bot", "new"), "AUTH_FAILED");
}

// --auth none: no users, every right.
void TestWithoutUsers() {
    Server server(false);
    SessionState session;
    assert(server.Run(session, "HELLO 3").rfind("%8\r\n", 0) == 0);
    assert(Contains(server.Run(session, "SHOW TABLES"), "default"));
    ExpectReply(server.Run(session, "GET BLOCK 0 0 FROM default"), "_\r\n");
    ExpectError(server.Run(session, "CREATE USER bot VERIFIER $1", Parameters{VerifierText("x")}), "runs without users");
    SessionState named;
    ExpectError(server.Run(named, "HELLO 3 USER bot $1", Parameters{std::string("n,,n=bot,r=abc")}), "runs without users");
}

}  // namespace

int main() {
    TestLogin();
    TestRights();
    TestWithoutUsers();
    return 0;
}
