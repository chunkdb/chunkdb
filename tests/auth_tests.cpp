#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"
#include "catalog_test_utils.hpp"
#include "chunkdb/engine.hpp"
#include "login_helpers.hpp"

namespace {

using Parameters = std::vector<std::optional<std::string>>;

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
    auto catalog = std::make_shared<chunkdb::TableCatalog>(chunkdb::CatalogConfigFromStoreConfig(config));
    (void)chunkdb::test::CreateBitsTable(*catalog, config.geometry, true);
    return catalog;
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    {
        auto catalog = BuildCatalog(data_dir);
        const auto users = chunkdb::test::MakeUsers(data_dir, "admin", "secret");
        using chunkdb::test::FailedLoginOnEngine;
        using chunkdb::test::LoginOnEngine;

        chunkdb::CommandEngine engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
                .max_auth_failures = 5,
            },
            catalog);

        chunkdb::SessionState session;

        // Nothing but HELLO before HELLO; without a user it is refused.
        {
            chunkdb::SessionState early;
            assert(engine.Execute(early, "GET BLOCK 0 0 FROM default\r\n") ==
                   "-ERR PROTOCOL expected HELLO 3\r\n");
            assert(early.close_after_reply);
        }
        const auto auth_required = engine.Execute(session, "HELLO 3\r\n");
        assert(auth_required.rfind("-ERR AUTH_REQUIRED use HELLO 3 USER <name> $1", 0) == 0);
        assert(auth_required.find("set the username and password in your client connection URI") != std::string::npos);
        // The token option is gone.
        assert(engine.Execute(session, "HELLO 3 AUTH secret\r\n") ==
               "-ERR INVALID_ARGUMENT HELLO is HELLO 3, or HELLO 3 USER <name> $1\r\n");
        // A wrong password and an unknown user fail alike.
        assert(LoginOnEngine(engine, session, "admin", "bad") == "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");
        assert(LoginOnEngine(engine, session, "nobody", "secret") == "-ERR AUTH_FAILED invalid user or password; check the username and password in your connection URI\r\n");
        assert(!session.authenticated && !session.greeted);
        assert(!session.close_after_reply);

        const std::string auth_ok = LoginOnEngine(engine, session, "admin", "secret");
        assert(auth_ok.rfind("%8\r\n$8\r\nprotocol\r\n:3\r\n", 0) == 0);
        assert(auth_ok.find("$14\r\nserver_version\r\n") != std::string::npos);
        assert(auth_ok.find("$14\r\nmax_line_bytes\r\n:") != std::string::npos);
        assert(auth_ok.find("$14\r\nmax_parameters\r\n:") != std::string::npos);
        assert(auth_ok.find("$15\r\nmax_area_chunks\r\n:") != std::string::npos);
        assert(auth_ok.find("$18\r\nmax_response_bytes\r\n:") != std::string::npos);
        assert(auth_ok.find("$14\r\nmax_scan_limit\r\n:") != std::string::npos);
        assert(auth_ok.find("$16\r\nserver_signature\r\n$") != std::string::npos);
        assert(auth_ok.find("capabilities") == std::string::npos);
        assert(session.authenticated && session.greeted);
        assert(session.user == "admin");

        // The default table: one bits(4) column, read back as one byte,
        // lowest bit first.
        const std::string set_ok = engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'1111'\r\n");
        assert(set_ok.rfind(":", 0) == 0);
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM default\r\n") == "*1\r\n$1\r\n\x0f\r\n");
        // Commands of protocol 2 are not statements.
        assert(engine.Execute(session, "XGET 0 0\r\n").rfind("-ERR SYNTAX", 0) == 0);

        const std::string unset_ok = engine.Execute(session, "DELETE BLOCK 0 0 FROM default\r\n");
        assert(unset_ok.rfind(":", 0) == 0);
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM default\r\n") == "_\r\n");

        // Chunks: 4x4 blocks of 4 bits, 8 payload bytes and 2 presence bytes.
        // The chunk form is the version and the schema version (u64 LE
        // each), presence, then payload.
        const auto chunk_of = [&](const std::string& reply) {
            assert(reply.rfind("$26\r\n", 0) == 0 && reply.size() == 5 + 26 + 2);
            return reply.substr(5 + 16, 10);
        };
        const auto version_of = [&](const std::string& reply) {
            std::uint64_t version = 0;
            for (std::size_t i = 0; i < 8; ++i) {
                version |= static_cast<std::uint64_t>(static_cast<unsigned char>(reply[5 + i])) << (8U * i);
            }
            return std::to_string(version);
        };
        // Version (not read) and schema version 1.
        const std::string header = std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8);
        const std::string zero_payload(8, '\0');
        const std::string no_blocks(2, '\0');
        assert(chunk_of(engine.Execute(session, "GET CHUNK 0 0 FROM default\r\n")) == no_blocks + zero_payload);
        const std::string all_blocks("\xff\xff", 2);
        const std::string put_ok = engine.Execute(
            session, "SET CHUNK 0 0 IN default $1\r\n", Parameters{header + all_blocks + zero_payload});
        assert(put_ok.rfind(":", 0) == 0);
        const std::string chunk_ok = engine.Execute(session, "GET CHUNK 0 0 FROM default\r\n");
        assert(chunk_of(chunk_ok) == all_blocks + zero_payload);
        assert(put_ok == ":" + version_of(chunk_ok) + "\r\n");
        assert(engine.Execute(session, "GET BLOCK 0 0 FROM default\r\n") == std::string("*1\r\n$1\r\n\0\r\n", 11));

        // Blocks 0 and 15 present: presence bit i is byte i/8, 1 << (i % 8).
        const std::string sparse_presence("\x01\x80", 2);
        const std::string sparse_payload = std::string("\x0f", 1) + std::string(7, '\0');
        assert(engine.Execute(
                   session, "SET CHUNK 1 0 IN default $1\r\n", Parameters{header + sparse_presence + sparse_payload})
                   .rfind(":", 0) == 0);
        assert(chunk_of(engine.Execute(session, "GET CHUNK 1 0 FROM default\r\n")) == sparse_presence + sparse_payload);
        assert(engine.Execute(session, "GET BLOCK 4 0 FROM default\r\n") == "*1\r\n$1\r\n\x0f\r\n");
        assert(engine.Execute(session, "GET BLOCK 5 0 FROM default\r\n") == "_\r\n");

        // AUTH without a pending HELLO 3 USER closes the connection.
        {
            chunkdb::SessionState stray;
            assert(engine.Execute(stray, "AUTH $1\r\n", Parameters{std::string("c=biws,r=x,p=x")}) ==
                   "-ERR PROTOCOL AUTH follows HELLO 3 USER <name> $1\r\n");
            assert(stray.close_after_reply);
        }

        chunkdb::SessionState brute;
        for (int i = 0; i < 4; ++i) {
            assert(FailedLoginOnEngine(engine, brute, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
            assert(!brute.close_after_reply);
        }
        assert(FailedLoginOnEngine(engine, brute, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(brute.close_after_reply);

        chunkdb::CommandEngine throttled_engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
                .max_auth_failures = 10,
                .max_auth_failures_per_ip = 2,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 10,
            },
            catalog);

        chunkdb::SessionState first_client;
        first_client.remote_address = "203.0.113.10";
        assert(LoginOnEngine(throttled_engine, first_client, "admin", "no1").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(LoginOnEngine(throttled_engine, first_client, "admin", "no2").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(!first_client.close_after_reply);

        // The right password from a banned source is refused at HELLO.
        chunkdb::SessionState blocked_client;
        blocked_client.remote_address = "203.0.113.10";
        assert(LoginOnEngine(throttled_engine, blocked_client, "admin", "secret") ==
               "-ERR AUTH_FAILED temporary auth ban\r\n");
        assert(blocked_client.close_after_reply);
        assert(!blocked_client.authenticated);

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        chunkdb::SessionState later_client;
        later_client.remote_address = "203.0.113.10";
        assert(LoginOnEngine(throttled_engine, later_client, "admin", "secret").rfind("%8\r\n", 0) == 0);
        assert(later_client.authenticated);

        // A login whose HELLO came before its source was banned does not
        // finish after it. The ban outlasts the test, so load cannot lift it.
        {
            chunkdb::CommandEngine banning_engine(
                chunkdb::EngineConfig{
                    .require_auth = true,
                    .users = users,
                    .max_auth_failures = 10,
                    .max_auth_failures_per_ip = 2,
                    .auth_failure_delay_ms = 0,
                    .auth_failure_ban_ms = 600000,
                },
                catalog);
            using FrameList = std::vector<std::optional<std::string>>;
            chunkdb::SessionState pending_client;
            pending_client.remote_address = "203.0.113.20";
            const auto pending_login = chunkdb::scram::StartClientLogin("admin", chunkdb::scram::NewNonce());
            const std::string pending_first = banning_engine.Execute(
                pending_client, chunkdb::test::HelloUserLine("admin") + "\r\n", FrameList{pending_login.first});
            assert(pending_first.rfind("+SCRAM ", 0) == 0);
            for (int i = 0; i < 2; ++i) {
                chunkdb::SessionState failing;
                failing.remote_address = "203.0.113.20";
                assert(FailedLoginOnEngine(banning_engine, failing, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
            }
            const auto pending_final =
                chunkdb::scram::FinishClientLogin(pending_login, "secret", chunkdb::test::ServerFirstOf(pending_first));
            assert(banning_engine.Execute(pending_client, "AUTH $1\r\n", FrameList{pending_final.message}) ==
                   "-ERR AUTH_FAILED temporary auth ban\r\n");
            assert(!pending_client.greeted);
        }

        // The failure-tracking table is hard-bounded: an address spray far
        // beyond the bound must not grow it past the documented cap (4096).
        chunkdb::CommandEngine spray_engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
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
            assert(FailedLoginOnEngine(spray_engine, spray, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
        }
        assert(spray_engine.AuthFailureTrackedSourcesForTests() <= 4096U);

        // IPv6 sources are bucketed per /64 prefix: varying interface
        // identifiers within one prefix share a single tracked entry.
        chunkdb::CommandEngine v6_engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 1'000'000,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 0,
            },
            catalog);
        for (int i = 0; i < 64; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address = "2001:db8:0:1::" + std::to_string(i + 1);
            assert(FailedLoginOnEngine(v6_engine, spray, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
        }
        assert(v6_engine.AuthFailureTrackedSourcesForTests() == 1U);

        // IPv4-mapped IPv6 peers (as a dual-stack listener reports them) must
        // be tracked by their embedded IPv4 address, NOT collapsed into one
        // ::/64 bucket — otherwise one attacker would ban every IPv4 client.
        chunkdb::CommandEngine mapped_engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
                .max_auth_failures = 1'000'000,
                .max_auth_failures_per_ip = 1'000'000,
                .auth_failure_delay_ms = 0,
                .auth_failure_ban_ms = 0,
            },
            catalog);
        for (int i = 0; i < 10; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address = "::ffff:198.51.100." + std::to_string(i + 1);
            assert(FailedLoginOnEngine(mapped_engine, spray, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
        }
        assert(mapped_engine.AuthFailureTrackedSourcesForTests() == 10U);

        // An active ban must not be evictable by an address spray: after a
        // source is banned, filling the (small-capacity) table with fresh
        // sources must keep the banned source banned.
        chunkdb::CommandEngine ban_engine(
            chunkdb::EngineConfig{
                .require_auth = true,
                .users = users,
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
                assert(FailedLoginOnEngine(ban_engine, victim, "admin").rfind("-ERR AUTH_FAILED", 0) == 0);
            }
        }
        // Spray far more distinct (single-failure, unbanned) sources than the
        // table can hold, so eviction runs repeatedly.
        for (int i = 0; i < 6000; ++i) {
            chunkdb::SessionState spray;
            spray.remote_address =
                "203.0." + std::to_string(i / 250) + "." + std::to_string(i % 250);
            (void)FailedLoginOnEngine(ban_engine, spray, "admin");
        }
        // The banned source must still be banned (its entry survived the spray).
        {
            chunkdb::SessionState victim;
            victim.remote_address = "198.51.100.200";
            const auto reply = LoginOnEngine(ban_engine, victim, "admin", "secret");
            assert(reply == "-ERR AUTH_FAILED temporary auth ban\r\n");
            assert(victim.close_after_reply);
        }

        // --auth none: HELLO 3 logs in without a user; HELLO 3 USER is refused.
        chunkdb::CommandEngine open_engine(chunkdb::EngineConfig{.require_auth = false}, catalog);
        {
            chunkdb::SessionState open;
            const std::string reply = open_engine.Execute(open, "HELLO 3\r\n");
            assert(reply.rfind("%8\r\n", 0) == 0);
            assert(reply.find("$16\r\nserver_signature\r\n_\r\n") != std::string::npos);
            assert(open.authenticated && open.greeted && open.user.empty());
            chunkdb::SessionState named;
            assert(LoginOnEngine(open_engine, named, "admin", "secret").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
            assert(!named.greeted);
        }

        // Logins with users need the users.
        bool refused = false;
        try {
            chunkdb::CommandEngine without_users(chunkdb::EngineConfig{.require_auth = true}, catalog);
        } catch (const std::invalid_argument&) {
            refused = true;
        }
        assert(refused);
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
