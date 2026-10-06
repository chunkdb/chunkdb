#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunkdb/engine.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto wall_tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    const auto mono_tick = static_cast<unsigned long long>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto tid = static_cast<unsigned long long>(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    return base / (
        "chunkdb-auth-test-" + std::to_string(wall_tick) + "-" +
        std::to_string(mono_tick) + "-" + std::to_string(tid));
}

void RemoveAllWithRetry(const std::filesystem::path& dir) {
    for (int attempt = 0; attempt < 25; ++attempt) {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        if (!std::filesystem::exists(dir)) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    if (std::filesystem::exists(dir)) {
        throw std::runtime_error("failed to remove auth test data dir: " + dir.string());
    }
}

std::shared_ptr<chunkdb::TableCatalog> BuildCatalog(const std::filesystem::path& dir) {
    chunkdb::StoreConfig config{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 4,
        },
        .data_dir = dir,
    };
    return std::make_shared<chunkdb::TableCatalog>(chunkdb::CatalogConfigFromStoreConfig(config));
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    {
        auto catalog = BuildCatalog(data_dir);

        chunkdb::CommandEngine engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 5,
            },
            catalog);

        chunkdb::SessionState session;

        // Nothing but HELLO before HELLO; without the token it is refused.
        {
            chunkdb::SessionState early;
            assert(engine.Execute(early, "GET 0 0\r\n").rfind("-ERR PROTOCOL", 0) == 0);
            assert(early.close_after_reply);
        }
        assert(engine.Execute(session, "HELLO 2\r\n").rfind("-ERR AUTH_REQUIRED", 0) == 0);
        assert(engine.Execute(session, "HELLO 2 AUTH bad\r\n").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(!session.authenticated && !session.greeted);

        const std::string auth_ok = engine.Execute(session, "HELLO 2 AUTH secret\r\n");
        assert(auth_ok.rfind("$", 0) == 0);
        assert(auth_ok.find("protocol=2\n") != std::string::npos);
        assert(session.authenticated && session.greeted);

        const std::string set_ok = engine.Execute(session, "SET 0 0 1111\r\n");
        assert(set_ok == "+OK\r\n");
        assert(engine.Execute(session, "GET 0 0\r\n") == "$4\r\n1111\r\n");

        const std::string unset_ok = engine.Execute(session, "UNSET 0 0\r\n");
        assert(unset_ok == "+OK\r\n");
        assert(engine.Execute(session, "GET 0 0\r\n") == "$-1\r\n");

        // Chunks: 4x4 blocks of 4 bits, 8 payload bytes and 2 presence bytes.
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+0\r\n");
        const std::string zero_payload(8, '\0');
        assert(engine.Execute(session, "CHUNKPUT 0 0 8\r\n", zero_payload).rfind("$", 0) == 0);
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+1\r\n");
        assert(engine.Execute(session, "CHUNKGET 0 0\r\n") == "$8\r\n" + zero_payload + "\r\n");
        assert(engine.Execute(session, "CHUNKGET 0 0 STATE\r\n") ==
               "$10\r\n" + zero_payload + std::string("\xff\xff", 2) + "\r\n");

        // Blocks 0 and 15 present: presence bit i is byte i/8, 1 << (i % 8).
        const std::string sparse = std::string("\x0f", 1) + std::string(7, '\0') +
                                   std::string("\x01\x80", 2);
        assert(engine.Execute(session, "CHUNKPUT 1 0 STATE 10\r\n", sparse).rfind("$", 0) == 0);
        assert(engine.Execute(session, "CHUNKEXISTS 1 0\r\n") == "+1\r\n");
        assert(engine.Execute(session, "CHUNKGET 1 0 STATE\r\n") == "$10\r\n" + sparse + "\r\n");
        assert(engine.Execute(session, "GET 4 0\r\n") == "$4\r\n1111\r\n");
        assert(engine.Execute(session, "GET 5 0\r\n") == "$-1\r\n");

        chunkdb::SessionState brute;
        for (int i = 0; i < 5; ++i) {
            (void)engine.Execute(brute, "HELLO 2 AUTH nope\r\n");
        }
        assert(brute.close_after_reply);

        chunkdb::CommandEngine throttled_engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 10,
                .max_auth_failures_per_ip = 2,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 10,
            },
            catalog);

        chunkdb::SessionState first_client;
        first_client.remote_address = "203.0.113.10";
        assert(throttled_engine.Execute(first_client, "HELLO 2 AUTH no1\r\n").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(throttled_engine.Execute(first_client, "HELLO 2 AUTH no2\r\n").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(!first_client.close_after_reply);

        chunkdb::SessionState blocked_client;
        blocked_client.remote_address = "203.0.113.10";
        assert(throttled_engine.Execute(blocked_client, "HELLO 2 AUTH secret\r\n").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(blocked_client.close_after_reply);

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        chunkdb::SessionState later_client;
        later_client.remote_address = "203.0.113.10";
        assert(throttled_engine.Execute(later_client, "HELLO 2 AUTH secret\r\n").rfind("$", 0) == 0);
        assert(later_client.authenticated);

        // The failure-tracking table is hard-bounded: an address spray far
        // beyond the bound must not grow it past the documented cap (4096).
        chunkdb::CommandEngine spray_engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 1'000'000,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 0,
            },
            catalog);
        for (int i = 0; i < 5000; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address =
                "203.0." + std::to_string(i / 250) + "." + std::to_string(i % 250);
            (void)spray_engine.Execute(spray, "HELLO 2 AUTH nope\r\n");
        }
        assert(spray_engine.AuthFailureTrackedSourcesForTests() <= 4096U);

        // IPv6 sources are bucketed per /64 prefix: varying interface
        // identifiers within one prefix share a single tracked entry.
        chunkdb::CommandEngine v6_engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 1'000'000,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 0,
            },
            catalog);
        for (int i = 0; i < 64; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address = "2001:db8:0:1::" + std::to_string(i + 1);
            (void)v6_engine.Execute(spray, "HELLO 2 AUTH nope\r\n");
        }
        assert(v6_engine.AuthFailureTrackedSourcesForTests() == 1U);

        // IPv4-mapped IPv6 peers (as a dual-stack listener reports them) must
        // be tracked by their embedded IPv4 address, NOT collapsed into one
        // ::/64 bucket — otherwise one attacker would ban every IPv4 client.
        chunkdb::CommandEngine mapped_engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 1'000'000,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 0,
            },
            catalog);
        for (int i = 0; i < 10; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address = "::ffff:198.51.100." + std::to_string(i + 1);
            (void)mapped_engine.Execute(spray, "HELLO 2 AUTH nope\r\n");
        }
        assert(mapped_engine.AuthFailureTrackedSourcesForTests() == 10U);

        // An active ban must not be evictable by an address spray: after a
        // source is banned, filling the (small-capacity) table with fresh
        // sources must keep the banned source banned.
        chunkdb::CommandEngine ban_engine(
            chunkdb::EngineConfig{
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 3,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 60'000,
            },
            catalog);
        {
            // Three failures from one source trip the per-ip threshold and ban
            // it; spray sources below (one failure each) stay unbanned and are
            // the eligible eviction victims.
            chunkdb::SessionState victim;
            victim.remote_address = "198.51.100.200";
            for (int i = 0; i < 3; ++i) {
                assert(ban_engine.Execute(victim, "HELLO 2 AUTH nope\r\n").rfind("-ERR", 0) == 0);
            }
        }
        // Spray far more distinct (single-failure, unbanned) sources than the
        // table can hold, so eviction runs repeatedly.
        for (int i = 0; i < 6000; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address =
                "203.0." + std::to_string(i / 250) + "." + std::to_string(i % 250);
            (void)ban_engine.Execute(spray, "HELLO 2 AUTH nope\r\n");
        }
        // The banned source must still be banned (its entry survived the spray).
        {
            chunkdb::SessionState victim;
            victim.remote_address = "198.51.100.200";
            const auto reply = ban_engine.Execute(victim, "HELLO 2 AUTH secret\r\n");
            assert(reply.rfind("-ERR AUTH_FAILED", 0) == 0);
            assert(victim.close_after_reply);
        }
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
