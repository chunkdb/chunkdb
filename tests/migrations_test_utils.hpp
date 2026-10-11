#pragma once

#include <cassert>
#include <memory>
#include <string>

#include "chunkdb/engine.hpp"
#include "login_helpers.hpp"
#include "txn_test_utils.hpp"
#include "../src/migrations_records.hpp"

namespace chunkdb::migration_test {

inline CatalogConfig Config(const std::filesystem::path& root) {
    auto config = CatalogConfigFromStoreConfig(txn_test::Config(root));
    config.slot_sync_interval = std::chrono::hours(1);
    return config;
}

struct Engine {
    std::shared_ptr<TableCatalog> catalog;
    std::shared_ptr<UserRegistry> users;
    std::unique_ptr<CommandEngine> engine;
    SessionState session;
    Engine(const std::filesystem::path& root, bool auth = false) {
        catalog = std::make_shared<TableCatalog>(Config(root));
        EngineConfig config;
        config.require_auth = auth;
        if (auth) config.users = users = test::MakeUsers(root, "admin", "admin-password");
        engine = std::make_unique<CommandEngine>(config, catalog);
        const auto hello = auth ? test::LoginOnEngine(*engine, session, "admin", "admin-password") : Run("HELLO 3");
        assert(!hello.empty() && hello.front() == '%');
    }
    std::string Run(const std::string& text) { return engine->Execute(session, text); }
};

inline void Reply(const std::string& actual, const std::string& expected) {
    if (actual != expected) { std::fprintf(stderr, "expected [%s], got [%s]\n", expected.c_str(), actual.c_str()); std::abort(); }
}

inline void Error(const std::string& actual, const std::string& code) {
    assert(actual.rfind("-ERR " + code + " ", 0U) == 0U);
}

inline constexpr const char* kCreate = "CREATE TABLE realm (v u16 REQUIRED) CHUNK 2 x 2 LARGE 1 x 1";

}  // namespace chunkdb::migration_test
