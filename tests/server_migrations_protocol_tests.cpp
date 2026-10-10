#include <array>
#include <barrier>
#include <iostream>

#include "server_slots_test_utils.hpp"

namespace {
using namespace chunkdb;
using namespace chunkdb::slot_socket_test;

void Result(const Reply& reply, std::string_view expected) {
    if (reply.type != '+' || reply.value != expected)
        throw std::runtime_error("expected migration " + std::string(expected) + ", got " + reply.type + reply.value);
}
std::string Command(std::string_view name, std::string_view statement) {
    return "MIGRATE '" + std::string(name) + "' " + std::string(statement);
}
void Record(const Reply& record, std::string_view name, std::string_view user, std::string_view statement) {
    assert(record.type == '%' && record.items.size() == 8U);
    assert(Field(record, "name").type == '$' && Field(record, "name").value == name);
    assert(Number(Field(record, "applied_ms")) > 0U);
    assert(Field(record, "user").type == '$' && Field(record, "user").value == user);
    assert(Field(record, "statement").type == '$' && Field(record, "statement").value == statement);
}
void CreateUser(Client& admin, std::string_view name) {
    const std::array<std::uint8_t, 16> salt{};
    const auto verifier = scram::FormatVerifier(scram::MakeVerifier("pw", salt, scram::kMinIterations));
    admin.Ok("CREATE USER " + std::string(name) + " VERIFIER '" + verifier + "'");
}
void AppliedSkippedConflict(bool tls) {
    Harness harness(tls, true);
    auto client = harness.Connect();
    constexpr std::string_view create = "CREATE TABLE realm (n u8) CHUNK 4 x 4";
    constexpr std::string_view alter = "ALTER TABLE realm ADD COLUMN extra u16 DEFAULT 9";
    Result(client->Command(Command("zz_create", create)), "applied");
    Result(client->Command(Command("zz_create", create)), "skipped");
    const auto original = client->Command("SHOW MIGRATIONS");
    assert(original.type == '*' && original.items.size() == 1U);
    Record(original.items[0], "zz_create", "admin", create);
    for (const auto* changed : {"CREATE TABLE realm (n u16) CHUNK 4 x 4", "CREATE  TABLE realm (n u8) CHUNK 4 x 4", "create TABLE realm (n u8) CHUNK 4 x 4"}) {
        const auto conflict = client->Command(Command("zz_create", changed));
        Error(conflict, "CONFLICT");
        assert(conflict.value.find("zz_create") != std::string::npos);
    }
    Result(client->Command(Command("aa_alter", alter)), "applied");
    const auto description = client->Command("DESCRIBE realm");
    assert(Field(description, "columns").items.size() == 2U);
    const auto schema_version = Number(Field(description, "version"));
    Result(client->Command(Command("aa_alter", alter)), "skipped");
    assert(Number(Field(client->Command("DESCRIBE realm"), "version")) == schema_version);
    const auto listed = client->Command("SHOW MIGRATIONS");
    assert(listed.type == '*' && listed.items.size() == 2U);
    assert(listed.items[0] == original.items[0]);
    Record(listed.items[1], "aa_alter", "admin", alter); // Applied order, not name order.
    Error(client->Command("MIGRATE unquoted CREATE TABLE denied (n u8) CHUNK 4 x 4"), "SYNTAX");
    for (const auto& invalid : {std::string{}, std::string("../bad"), std::string("Upper"), std::string(64U, 'a')})
        Error(client->Command(Command(invalid, create)), "SYNTAX");
    Error(client->Command("MIGRATE 'bad' SET BLOCK 0 0 IN realm n = 1"), "SYNTAX");
    Error(client->Command("MIGRATE 'bad' CREATE USER unsupported VERIFIER 'not-a-verifier'"), "SYNTAX");
    assert(client->Command("SHOW MIGRATIONS") == listed);
    Error(client->Command(Command("retry", create)), "TABLE_EXISTS");
    assert(client->Command("SHOW MIGRATIONS") == listed);
    Result(client->Command(Command("retry", "CREATE TABLE retried (n u8) CHUNK 4 x 4")), "applied");
}
void SupportedStatements(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect();
    CreateUser(*admin, "reader");
    const std::array<std::pair<std::string_view, std::string_view>, 7U> steps{{
        {"create", "CREATE TABLE realm (n u8) CHUNK 4 x 4"},
        {"grant", "GRANT READ ON realm TO reader"},
        {"alter", "ALTER TABLE realm ADD COLUMN extra u16 DEFAULT 9"},
        {"slot_create", "CREATE SLOT 'consumer' ON realm"},
        {"slot_drop", "DROP SLOT 'consumer' ON realm"},
        {"revoke", "REVOKE READ ON realm FROM reader"},
        {"drop", "DROP TABLE realm"},
    }};
    Client reader(harness.port, tls); reader.Login("reader", "pw");
    for (std::size_t i = 0; i != steps.size(); ++i) {
        const auto& [name, statement] = steps[i];
        Result(admin->Command(Command(name, statement)), "applied");
        Result(admin->Command(Command(name, statement)), "skipped");
        if (i == 1U) assert(reader.Command("DESCRIBE realm").type == '%');
        if (i == 2U) assert(Field(admin->Command("DESCRIBE realm"), "columns").items.size() == 2U);
        if (i == 3U) assert(admin->Command("SHOW SLOTS ON realm").items.size() == 1U);
        if (i == 4U) assert(admin->Command("SHOW SLOTS ON realm").items.empty());
        if (i == 5U) Error(reader.Command("DESCRIBE realm"), "NO_TABLE");
        if (i == 6U) Error(admin->Command("DESCRIBE realm"), "NO_TABLE");
    }
    const auto listed = admin->Command("SHOW MIGRATIONS");
    assert(listed.type == '*' && listed.items.size() == steps.size());
    for (std::size_t i = 0; i != steps.size(); ++i) Record(listed.items[i], steps[i].first, "admin", steps[i].second);
}
void RightsAndTransactions(bool tls) {
    Harness harness(tls, true);
    auto admin = harness.Connect();
    admin->Ok("CREATE TABLE realm (n u8) CHUNK 4 x 4");
    CreateUser(*admin, "limited");
    Client limited(harness.port, tls); limited.Login("limited", "pw");
    Error(limited.Command(Command("create_denied", "CREATE TABLE denied (n u8) CHUNK 4 x 4")), "PERMISSION_DENIED");
    constexpr std::string_view alter = "ALTER TABLE realm ADD COLUMN extra u16 DEFAULT 9";
    Error(limited.Command(Command("alter", alter)), "NO_TABLE");
    admin->Ok("GRANT READ ON realm TO limited");
    Error(limited.Command(Command("alter", alter)), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON realm TO limited");
    Result(limited.Command(Command("alter", alter)), "applied");
    Result(limited.Command(Command("alter", alter)), "skipped");
    admin->Ok("REVOKE ADMIN ON realm FROM limited");
    Error(limited.Command(Command("alter", alter)), "PERMISSION_DENIED");
    Error(limited.Command(Command("alter", "ALTER TABLE realm ADD COLUMN forbidden u8 NULL")), "PERMISSION_DENIED");
    Error(limited.Command(Command("grant_denied", "GRANT READ ON realm TO limited")), "PERMISSION_DENIED");
    Error(limited.Command(Command("revoke_denied", "REVOKE READ ON realm FROM limited")), "PERMISSION_DENIED");
    Error(limited.Command("SHOW MIGRATIONS"), "PERMISSION_DENIED");
    admin->Ok("GRANT ADMIN ON * TO limited");
    Error(limited.Command("SHOW MIGRATIONS"), "PERMISSION_DENIED"); // MANAGES USERS is the listing right.
    admin->Ok("ALTER USER limited MANAGES USERS");
    const auto listed = limited.Command("SHOW MIGRATIONS");
    assert(listed.type == '*' && listed.items.size() == 1U);
    Record(listed.items[0], "alter", "limited", alter);
    admin->Ok("REVOKE READ ON * FROM limited");
    admin->Ok("REVOKE READ ON realm FROM limited");
    assert(limited.Command("SHOW MIGRATIONS") == listed); // Managing users alone suffices.
    admin->Ok("BEGIN");
    Error(admin->Command(Command("alter", alter)), "INVALID_ARGUMENT");
    Error(admin->Command(Command("txn_denied", "ALTER TABLE realm ADD COLUMN forbidden u8 NULL")), "INVALID_ARGUMENT");
    admin->Ok("ROLLBACK");
    assert(admin->Command("SHOW MIGRATIONS") == listed);
    assert(Field(admin->Command("DESCRIBE realm"), "columns").items.size() == 2U);
}
void ConcurrentRequests(bool tls) {
    Harness harness(tls, true);
    auto first = harness.Connect(), second = harness.Connect(), observer = harness.Connect();
    const auto run = [&](std::string_view name, std::string_view left, std::string_view right) {
        std::array<Reply, 2U> results;
        std::array<std::exception_ptr, 2U> errors;
        std::barrier start(3);
        std::thread a([&] {
            start.arrive_and_wait();
            try { results[0] = first->Command(Command(name, left)); }
            catch (const std::exception&) { errors[0] = std::current_exception(); }
        });
        std::thread b([&] {
            start.arrive_and_wait();
            try { results[1] = second->Command(Command(name, right)); }
            catch (const std::exception&) { errors[1] = std::current_exception(); }
        });
        start.arrive_and_wait(); a.join(); b.join();
        for (const auto& error : errors) if (error) std::rethrow_exception(error);
        return results;
    };
    constexpr std::string_view create = "CREATE TABLE same (n u8) CHUNK 4 x 4";
    const auto same = run("same", create, create);
    const auto winner = same[0].value == "applied" ? 0U : 1U;
    Result(same[winner], "applied"); Result(same[1U - winner], "skipped");
    const std::array<std::string_view, 2U> choices{
        "CREATE TABLE different (n u8) CHUNK 4 x 4", "CREATE TABLE different (n u16) CHUNK 4 x 4"};
    const auto different = run("different", choices[0], choices[1]);
    const auto applied = different[0].value == "applied" ? 0U : 1U;
    Result(different[applied], "applied"); Error(different[1U - applied], "CONFLICT");
    assert(different[1U - applied].value.find("different") != std::string::npos);
    const auto listed = observer->Command("SHOW MIGRATIONS");
    assert(listed.type == '*' && listed.items.size() == 2U);
    Record(listed.items[0], "same", "admin", create);
    Record(listed.items[1], "different", "admin", choices[applied]);
    const auto columns = Field(observer->Command("DESCRIBE different"), "columns");
    assert(columns.items.size() == 1U && Field(columns.items[0], "type").value == (applied == 0U ? "u8" : "u16"));
    Result(observer->Command(Command("different", choices[applied])), "skipped");
    assert(harness.catalog->Find("same") && harness.catalog->Find("different"));
}
void NoAuthentication(bool tls) {
    Harness harness(tls);
    auto client = harness.Connect();
    constexpr std::string_view create = "CREATE TABLE realm (n u8) CHUNK 4 x 4";
    Result(client->Command(Command("create", create)), "applied");
    Result(client->Command(Command("create", create)), "skipped");
    const auto listed = client->Command("SHOW MIGRATIONS");
    assert(listed.type == '*' && listed.items.size() == 1U);
    Record(listed.items[0], "create", "", create);
}

// A local fixture so restart persistence does not alter the shared socket
// harness used by other protocol tests.
class PersistentServer {
  public:
    PersistentServer(std::filesystem::path directory, bool tls) : directory_(std::move(directory)), tls_(tls) { Start(); }
    ~PersistentServer() { Stop(); }
    std::unique_ptr<Client> Connect() {
        auto client = std::make_unique<Client>(port_, tls_); client->Login(); return client;
    }
  private:
    void Start() {
        catalog_ = std::make_shared<TableCatalog>(feed_test::Config(directory_));
        EngineConfig engine; engine.require_auth = true; engine.users = test::MakeUsers(directory_, "admin", "secret");
        engine_ = std::make_shared<CommandEngine>(engine, catalog_);
        ServerConfig config; config.port = port_ = FreePort(); config.worker_threads = 4; config.tls_enabled = tls_;
        // Match the bounded functional SCRAM budget of the shared harness.
        config.client_io_timeout_ms = 30000;
#ifdef CHUNKDB_WITH_OPENSSL
        if (tls_) {
            const auto cert = directory_ / "test-cert.pem", key = directory_ / "test-key.pem";
            std::ofstream(cert) << kTestTlsCertPem; std::ofstream(key) << kTestTlsKeyPem;
            config.tls_cert_path = cert.string(); config.tls_key_path = key.string();
        }
#endif
        server_ = std::make_unique<ChunkServer>(config, engine_);
        thread_ = std::thread([&] {
            try { server_->Run(); }
            catch (const std::exception&) { std::lock_guard lock(error_mutex_); error_ = std::current_exception(); }
        });
        try {
            const auto deadline = Clock::now() + 10s;
            for (;;) {
                { std::lock_guard lock(error_mutex_); if (error_) std::rethrow_exception(error_); }
                try { Client probe(port_, tls_); break; }
                catch (const std::runtime_error&) { if (Clock::now() >= deadline) throw; std::this_thread::yield(); }
            }
        } catch (const std::exception&) { Stop(); throw; }
    }
    void Stop() {
        if (server_) server_->Stop();
        if (thread_.joinable()) thread_.join();
        server_.reset(); engine_.reset(); catalog_.reset();
    }
    std::filesystem::path directory_;
    bool tls_;
    std::uint16_t port_{};
    std::shared_ptr<TableCatalog> catalog_;
    std::shared_ptr<CommandEngine> engine_;
    std::unique_ptr<ChunkServer> server_;
    std::thread thread_;
    std::mutex error_mutex_;
    std::exception_ptr error_;
};
void RestartPersistence(bool tls) {
    test::ScopedTempDir directory("chunkdb-migrations-restart");
    constexpr std::string_view create = "CREATE TABLE realm (n u8) CHUNK 4 x 4";
    constexpr std::string_view alter = "ALTER TABLE realm ADD COLUMN extra u16 DEFAULT 9";
    Reply before;
    {
        PersistentServer server(directory.path(), tls); auto client = server.Connect();
        Result(client->Command(Command("create", create)), "applied");
        Result(client->Command(Command("alter", alter)), "applied");
        before = client->Command("SHOW MIGRATIONS");
        assert(before.type == '*' && before.items.size() == 2U);
    }
    {
        PersistentServer server(directory.path(), tls); auto client = server.Connect();
        assert(client->Command("SHOW MIGRATIONS") == before);
        Result(client->Command(Command("create", create)), "skipped");
        Result(client->Command(Command("alter", alter)), "skipped");
        assert(client->Command("SHOW MIGRATIONS") == before);
        assert(Field(client->Command("DESCRIBE realm"), "columns").items.size() == 2U);
        auto conflict = client->Command(Command("create", "DROP TABLE realm"));
        Error(conflict, "CONFLICT"); assert(conflict.value.find("create") != std::string::npos);
    }
}
void Run(bool tls) {
    AppliedSkippedConflict(tls); SupportedStatements(tls); RightsAndTransactions(tls);
    ConcurrentRequests(tls); RestartPersistence(tls);
    NoAuthentication(tls);
}
} // namespace
int main() {
    Run(false);
#ifdef CHUNKDB_WITH_OPENSSL
    Run(true);
    std::cout << "12 migration protocol groups passed (plain and TLS)\n";
#else
    std::cout << "6 migration protocol groups passed (plain)\n";
#endif
}
