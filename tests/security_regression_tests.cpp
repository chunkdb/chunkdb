// Regression tests for the security fixes in the 2026-07-28 audit remediation.
//
// These exist because every one of the fixes below was silently reverted once,
// when a refactor moved the affected function to a new translation unit from a
// base that predated the fix. The full suite stayed green throughout: none of
// these behaviors had a test. Each case here pins one fix so that a future move
// of the same code fails loudly instead of quietly restoring the defect.

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/table_catalog.hpp"

#include "chunk_store_internal.hpp"
#include "test_utils.hpp"

namespace {

// WalPathForConditionalIntent reconstructs a WAL path from a filename read off
// disk by splitting on "__". Without component validation a planted name such
// as "..__..__etc__passwd.rollback" resolves outside the data directory, and
// the result feeds std::filesystem::resize_file() and remove() during startup
// recovery. Legitimate names are always coordinate-derived and can never
// contain an empty, "." or ".." component.
void TestWalPathForConditionalIntentRejectsTraversal() {
    const std::filesystem::path data_dir = "/tmp/chunkdb-intent-grammar";

    // A legitimate WAL path must round-trip unchanged.
    const std::filesystem::path wal_path = data_dir / "L_0_0" / "C_1_2.wal";
    const auto intent_path = chunkdb::ConditionalIntentPathForWal(data_dir, wal_path);
    assert(chunkdb::WalPathForConditionalIntent(data_dir, intent_path) == wal_path);

    // Negative coordinates round-trip too: std::to_string emits '-', never '_'.
    const std::filesystem::path negative_wal = data_dir / "L_-1_-3" / "C_-5_-8.wal";
    const auto negative_intent = chunkdb::ConditionalIntentPathForWal(data_dir, negative_wal);
    assert(chunkdb::WalPathForConditionalIntent(data_dir, negative_intent) == negative_wal);

    const auto intent_dir = chunkdb::ConditionalIntentDirectory(data_dir);
    const auto rejects = [&](const std::string& file_name) {
        bool threw = false;
        try {
            (void)chunkdb::WalPathForConditionalIntent(data_dir, intent_dir / file_name);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        assert(threw && "malformed intent file name must be rejected");
    };

    rejects("..__..__etc__passwd.rollback");   // classic traversal
    rejects("..__C_0_0.wal.rollback");         // single parent escape
    rejects("__L_0_0__C_0_0.wal.rollback");    // leading empty component
    rejects("L_0_0____C_0_0.wal.rollback");    // empty component in the middle
    rejects("L_0_0__..__C_0_0.wal.rollback");  // parent escape in the middle
    rejects("L_0_0__.__C_0_0.wal.rollback");   // current-dir component
    rejects("L_0_0__C_0_0.wal__.rollback");    // trailing empty component

    // A name that does not carry the suffix at all is rejected as before.
    bool threw = false;
    try {
        (void)chunkdb::WalPathForConditionalIntent(data_dir, intent_dir / "L_0_0__C_0_0.wal");
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw && "a non-intent file name must be rejected");
}

// The server must fail closed rather than listen with authentication silently
// disabled. This is the guardrail that the container image's baked-in
// CHUNKDB_TOKEN=dev-token used to bypass: with a default credential present
// the process always started, and the credential was publicly known. Logins
// now need users; an engine that requires them but has none must not start.
void TestEngineRefusesAuthEnabledWithoutUsers() {
    chunkdb::test::ScopedTempDir dir("chunkdb-security-regression");
    chunkdb::CatalogConfig config;
    config.data_dir = dir.path();
    const auto catalog = std::make_shared<chunkdb::TableCatalog>(config);
    bool threw = false;
    try {
        const chunkdb::EngineConfig engine_config{
            .require_auth = true,
            .users = nullptr,
        };
        chunkdb::CommandEngine engine(engine_config, catalog);
        (void)engine;
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw && "auth enabled without users must be rejected at construction");
}

}  // namespace

int main() {
    TestWalPathForConditionalIntentRejectsTraversal();
    TestEngineRefusesAuthEnabledWithoutUsers();
    return 0;
}
