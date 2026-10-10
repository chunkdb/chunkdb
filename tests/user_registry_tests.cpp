// The user registry (docs/design/USERS_DESIGN.md): the first administrator, the
// rules of user changes, ordered grants, and that every change reaches
// chunkdb.users.

#include <cassert>
#include <cstdio>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

#include "admin.hpp"
#include "checkpoint.hpp"
#include "migrations_test_utils.hpp"
#include "process_lock.hpp"
#include "test_utils.hpp"
#include "user_registry.hpp"

namespace {

using chunkdb::Right;
using chunkdb::UserRegistry;

void ExpectInvalid(const std::function<void()>& action, const std::string& part) {
    try {
        action();
    } catch (const std::invalid_argument& e) {
        if (std::string(e.what()).find(part) == std::string::npos) {
            std::fprintf(stderr, "expected '%s', got '%s'\n", part.c_str(), e.what());
            assert(false);
        }
        return;
    }
    std::fprintf(stderr, "expected an error with '%s'\n", part.c_str());
    assert(false);
}

chunkdb::scram::Verifier VerifierOf(const std::string& password) {
    return chunkdb::scram::MakeVerifier(password, std::vector<std::uint8_t>(16, 0x11), 4096);
}

constexpr std::array<std::uint8_t, 32> kSecret{1, 2, 3};

void TestFirstAdministrator() {
    chunkdb::test::ScopedTempDir dir("chunkdb-user-registry-first");
    ExpectInvalid([&] { UserRegistry registry(dir.path(), std::nullopt, kSecret); }, "has no users");
    ExpectInvalid(
        [&] { UserRegistry registry(dir.path(), std::make_pair(std::string("Admin"), VerifierOf("x")), kSecret); },
        "a user name is");
    {
        UserRegistry registry(dir.path(), std::make_pair(std::string("admin"), VerifierOf("secret")), kSecret);
        const auto admin = registry.Find("admin");
        assert(admin.has_value() && admin->manages_users && chunkdb::RightOn(*admin, "anything") == Right::kAdmin);
        assert(admin->verifier == VerifierOf("secret"));
    }
    // A later start reads the file; the first-start settings are not used.
    UserRegistry reopened(dir.path(), std::make_pair(std::string("other"), VerifierOf("x")), kSecret);
    assert(reopened.Find("admin").has_value() && !reopened.Find("other").has_value());
    assert(reopened.Snapshot().secret == kSecret);
}

void TestChanges() {
    chunkdb::test::ScopedTempDir dir("chunkdb-user-registry-changes");
    UserRegistry registry(dir.path(), std::make_pair(std::string("admin"), VerifierOf("secret")), kSecret);
    const auto before = registry.Generation();
    registry.Create("bot", VerifierOf("hunter2"), false);
    assert(registry.Generation() > before);
    const auto unchanged = registry.Generation();
    ExpectInvalid([&] { registry.Create("bot", VerifierOf("x"), false); }, "already exists");
    assert(registry.Generation() == unchanged);  // a refused change is not one
    ExpectInvalid([&] { registry.Create("bot", VerifierOf("x"), false); }, "already exists");
    ExpectInvalid([&] { registry.Create("no space", VerifierOf("x"), false); }, "a user name is");
    ExpectInvalid([&] { registry.Drop("ghost"); }, "does not exist");

    registry.Grant("bot", "world", Right::kWrite);
    registry.Grant("bot", "world", Right::kRead);  // a lower grant keeps the higher one
    assert(chunkdb::RightOn(*registry.Find("bot"), "world") == Right::kWrite);
    registry.Grant("bot", "*", Right::kRead);
    assert(chunkdb::RightOn(*registry.Find("bot"), "terrain") == Right::kRead);
    registry.Revoke("bot", "world", Right::kWrite);
    assert(chunkdb::RightOn(*registry.Find("bot"), "world") == Right::kRead);
    registry.Revoke("bot", "*", Right::kRead);
    registry.Revoke("bot", "world", Right::kRead);
    assert(!chunkdb::RightOn(*registry.Find("bot"), "world").has_value());

    registry.Grant("bot", "world", Right::kAdmin);
    registry.ForgetTable("world");
    assert(!chunkdb::RightOn(*registry.Find("bot"), "world").has_value());

    registry.SetVerifier("bot", VerifierOf("new"));
    assert(registry.Find("bot")->verifier == VerifierOf("new"));

    // Someone must always be able to manage users.
    ExpectInvalid([&] { registry.SetManagesUsers("admin", false); }, "the last user who manages users");
    ExpectInvalid([&] { registry.Drop("admin"); }, "the last user who manages users");
    registry.SetManagesUsers("bot", true);
    registry.SetManagesUsers("admin", false);
    registry.Drop("admin");

    // Every change reached the file.
    UserRegistry reopened(dir.path(), std::nullopt, kSecret);
    assert(reopened.Snapshot() == registry.Snapshot());
    assert(!reopened.Find("admin").has_value() && reopened.Find("bot")->manages_users);
}

// The offline reset writes a new verifier only, and refuses while another
// process holds the directory.
void TestResetPassword() {
    chunkdb::test::ScopedTempDir dir("chunkdb-user-registry-reset");
    ExpectInvalid([&] { chunkdb::ResetPassword(dir.path(), "admin", "new"); }, "has no users");
    {
        UserRegistry registry(dir.path(), std::make_pair(std::string("admin"), VerifierOf("old")), kSecret);
        registry.Grant("admin", "world", Right::kRead);
    }
    ExpectInvalid([&] { chunkdb::ResetPassword(dir.path(), "ghost", "new"); }, "does not exist");
    ExpectInvalid([&] { chunkdb::ResetPassword(dir.path(), "admin", ""); }, "empty");
    chunkdb::ResetPassword(dir.path(), "admin", "new");
    UserRegistry reopened(dir.path(), std::nullopt, kSecret);
    const auto admin = reopened.Find("admin");
    assert(admin.has_value() && !(admin->verifier == VerifierOf("old")) && admin->manages_users);
    assert(admin->grants.at("world") == Right::kRead);
    {
        const auto held = chunkdb::AcquireWriterLock(dir.path(), chunkdb::AccessMode::kReadWrite, false);
        bool refused = false;
        try {
            chunkdb::ResetPassword(dir.path(), "admin", "again");
        } catch (const std::exception&) {
            refused = true;
        }
        assert(refused);
    }
}

void TestResetPasswordRecoversMigration() {
    using namespace chunkdb;
    using namespace chunkdb::migration_test;
    for (const auto* statement : {"GRANT WRITE ON realm TO limited", "REVOKE READ ON realm FROM limited", "DROP TABLE realm"}) {
        test::ScopedTempDir dir("chunkdb-user-reset-pending-migration");
        {
            Engine e(dir.path(), true);
            Reply(e.Run(kCreate), "+OK\r\n");
            e.users->Create("limited", VerifierOf("old"), false);
            e.users->Grant("limited", "realm", Right::kRead);
            txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
            Error(e.Run(std::string("MIGRATE 'pending' ") + statement), "INTERNAL");
        }
        assert(ReadMigrationJournal(dir.path()));
        ResetPassword(dir.path(), "limited", "new-password");
        assert(!ReadMigrationJournal(dir.path()));
        auto catalog = std::make_shared<TableCatalog>(Config(dir.path()));
        auto users = std::make_shared<UserRegistry>(dir.path(), std::nullopt, kSecret);
        CommandEngine engine(EngineConfig{.require_auth = true, .users = users}, catalog);
        SessionState session;
        assert(test::LoginOnEngine(engine, session, "limited", "new-password").front() == '%');
        assert(catalog->Migrations().size() == 1U);
        const auto limited = users->Find("limited");
        if (std::string_view(statement).starts_with("GRANT")) assert(limited->grants.at("realm") == Right::kWrite);
        else assert(!limited->grants.contains("realm"));
        assert(static_cast<bool>(catalog->Find("realm")) == !std::string_view(statement).starts_with("DROP"));
    }
    test::ScopedTempDir bad("chunkdb-user-reset-bad-pending-migration");
    {
        Engine e(bad.path(), true);
        txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MIGRATION_AFTER_DECISION_FAIL_ONCE", "1");
        Error(e.Run("MIGRATE 'pending' GRANT READ ON realm TO admin"), "INTERNAL");
    }
    auto bytes = LoadFile(bad.path()/kMigrationPendingFileName); bytes.back() ^= 1U;
    AtomicWrite(bad.path()/kMigrationPendingFileName, bytes, true, true);
    const auto users_before = LoadFile(bad.path()/kUsersFileName);
    bool rejected = false;
    try { ResetPassword(bad.path(), "admin", "new-password"); } catch (const std::exception&) { rejected = true; }
    assert(rejected && LoadFile(bad.path()/kUsersFileName) == users_before);
}

}  // namespace

int main() {
    TestFirstAdministrator();
    TestChanges();
    TestResetPassword();
    TestResetPasswordRecoversMigration();
    return 0;
}
