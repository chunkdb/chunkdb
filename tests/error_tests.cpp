#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/table_catalog.hpp"
#include "chunkdb/engine.hpp"

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
        "chunkdb-error-test-" + std::to_string(wall_tick) + "-" +
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
        throw std::runtime_error("failed to remove error test data dir: " + dir.string());
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

std::string ExtractBulkPayload(const std::string& framed) {
    const auto first_crlf = framed.find("\r\n");
    if (first_crlf == std::string::npos || framed.empty() || framed[0] != '$') {
        throw std::runtime_error("invalid bulk framing");
    }

    const std::size_t payload_len = static_cast<std::size_t>(std::stoull(framed.substr(1, first_crlf - 1)));
    const std::size_t payload_begin = first_crlf + 2;
    if (payload_begin + payload_len + 2 > framed.size()) {
        throw std::runtime_error("truncated bulk payload");
    }
    return framed.substr(payload_begin, payload_len);
}

// SHOW METRICS text: each sample line `name value` by its name (with labels).
std::unordered_map<std::string, std::string> ParseMetrics(const std::string& payload) {
    std::unordered_map<std::string, std::string> result;
    std::istringstream in(payload);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto sep = line.rfind(' ');
        if (sep == std::string::npos) {
            continue;
        }
        result.emplace(line.substr(0, sep), line.substr(sep + 1));
    }
    return result;
}

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    {
        auto catalog = BuildCatalog(data_dir);

        chunkdb::CommandEngine engine(
            chunkdb::EngineConfig{
                .auth_token = "",
                .require_auth = false,
                .max_auth_failures = 3,
            },
            catalog);

        chunkdb::SessionState session;

        const auto err = [&](const std::string& line, const char* code, const Parameters& parameters = {}) {
            const auto reply = engine.Execute(session, line + "\r\n", parameters);
            return reply.rfind(std::string("-ERR ") + code, 0) == 0;
        };
        // The chunk form of the default table: version, schema version, 2
        // presence bytes and 8 payload bytes (4x4 blocks of 4 bits).
        const auto chunk_of = [&](std::uint32_t cx, std::uint32_t cy) {
            const std::string reply = engine.Execute(
                session, "GET CHUNK " + std::to_string(cx) + " " + std::to_string(cy) + " FROM default\r\n");
            assert(reply.rfind("$26\r\n", 0) == 0 && reply.size() == 5 + 26 + 2);
            return reply.substr(5 + 16, 10);
        };
        const std::string no_chunk(10, '\0');

        // HELLO options: AUTH is the only one, at most once.
        for (const char* bad : {"HELLO 3 TABLE", "HELLO 3 TABLE default", "HELLO 3 COLOR red", "HELLO 3 AUTH",
                                "HELLO 3 AUTH x AUTH x", "HELLO 3 TABLE default TABLE default"}) {
            chunkdb::SessionState hello;
            assert(engine.Execute(hello, std::string(bad) + "\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
            assert(!hello.greeted);
        }
        // Other protocol versions are refused and the connection closes.
        for (const char* other : {"HELLO 2", "HELLO 4", "HELLO", "HELLO 2 TABLE default"}) {
            chunkdb::SessionState hello;
            assert(engine.Execute(hello, std::string(other) + "\r\n") == "-ERR PROTOCOL expected HELLO 3\r\n");
            assert(hello.close_after_reply && !hello.greeted);
        }

        // Protocol 1 and 2 commands: before HELLO the client learns which
        // protocol this server speaks; after it they are not statements.
        const auto removed = {"UNKNOWN", "AUTH x", "EXISTS 1 2", "CHUNK 0 0", "CHUNKSET 0 0 0000",
                              "CHUNKBIN 0 0", "CHUNKBINC 0 0", "CHUNKCAS 0 0 1 STATE 0|0", "CHUNKSETBIN 0 0 8",
                              "GET 1 2", "SET 1 2 1111", "UNSET 1 2", "MGET 1 2", "MSET 1 2 1111",
                              "CHUNKGET 0 0", "CHUNKPUT 0 0 8", "CHUNKEXISTS 0 0", "CHUNKVER 0 0",
                              "CHUNKBATCH 0 0 SET 0 0 1111", "CHUNKRANGE 0 0 1 1", "CHUNKRADIUS 0 0 1",
                              "CHUNKSCAN", "USE default", "INFO", "TABLEINFO default", "WALFLUSH", "QUIT"};
        for (const char* command : removed) {
            chunkdb::SessionState early;
            assert(engine.Execute(early, std::string(command) + "\r\n") == "-ERR PROTOCOL expected HELLO 3\r\n");
            assert(early.close_after_reply);
        }
        assert(engine.Execute(session, "hello 3\r\n").rfind("%7\r\n", 0) == 0);
        for (const char* command : removed) {
            assert(err(command, "SYNTAX column"));
            assert(!session.close_after_reply);
        }

        // Block statements.
        assert(err("DELETE BLOCK 1 FROM default", "SYNTAX"));
        assert(err("SET BLOCK 1 2 IN default", "SYNTAX"));
        assert(err("SET BLOCK x 2 IN default bits = b'1111'", "SYNTAX"));
        assert(err("SET BLOCK 1 2 IN default bits = b'12AB'", "SYNTAX"));
        assert(err("SET BLOCK 1 2 IN default bits = b'11111'", "INVALID_ARGUMENT"));
        assert(err("SET BLOCK 1 2 IN default bits = 12", "INVALID_ARGUMENT"));
        assert(err("SET BLOCK 1 2 IN default nope = b'1111'", "INVALID_ARGUMENT"));
        assert(err("SET BLOCK 1 2 IN nope bits = b'1111'", "NO_TABLE"));
        assert(err("SET BLOCK 1 2 IN default bits = b'1111' IF", "SYNTAX"));
        assert(err("SET BLOCK 1 2 IN default bits = b'1111' IF 1", "SYNTAX"));
        assert(err("SET BLOCK 1 2 IN default bits = b'1111' IF VERSION x", "SYNTAX"));
        // A parameter frame of the wrong size, or with bits past the width.
        assert(err("SET BLOCK 1 2 IN default bits = $1", "INVALID_ARGUMENT", Parameters{std::string(2, '\x01')}));
        assert(err("SET BLOCK 1 2 IN default bits = $1", "INVALID_ARGUMENT", Parameters{std::string("\x10", 1)}));
        assert(err("SET BLOCK 1 2 IN default bits = $1", "PROTOCOL"));
        assert(err("GET BLOCK 1 FROM default", "SYNTAX"));
        assert(err("GET BLOCK 1 2 FROM nope", "NO_TABLE"));
        assert(err("GET BLOCK 1 2 FROM default COLUMNS nope", "INVALID_ARGUMENT"));

        // Chunk reads.
        assert(err("GET CHUNK 0 FROM default", "SYNTAX"));
        assert(err("GET CHUNK 0 0 FROM default BADMODE", "SYNTAX"));
        assert(err("GET CHUNK 0 0 FROM default COLUMNS", "SYNTAX"));
        assert(err("GET CHUNK 0 0 FROM default COLUMNS nope", "INVALID_ARGUMENT"));
        assert(err("GET CHUNK x 0 FROM default", "SYNTAX"));

        // Chunk writes: the form is version, schema version, presence,
        // payload, then VARS.
        const std::string version = std::string(8, '\0') + std::string("\x01\0\0\0\0\0\0\0", 8);
        const std::string payload(8, '\x11');
        const std::string state = version + std::string("\xff\xff", 2) + payload;
        assert(err("SET CHUNK 0 0 IN default $1", "INVALID_ARGUMENT", Parameters{std::string(4, '\0')}));
        assert(err("SET CHUNK 0 0 IN default $1", "INVALID_ARGUMENT", Parameters{version + payload}));
        assert(err("SET CHUNK 0 0 IN default $1", "INVALID_ARGUMENT", Parameters{state + std::string(3, '\0')}));
        assert(err("SET CHUNK 0 0 IN default $1", "INVALID_ARGUMENT", Parameters{std::nullopt}));
        assert(err("SET CHUNK 0 0 IN default $1 BADMODE", "SYNTAX", Parameters{state}));
        assert(err("SET CHUNK 0 0 IN default $1 IF VERSION x", "SYNTAX", Parameters{state}));
        assert(err("SET CHUNK 0 0 IN default $1", "PROTOCOL"));
        assert(err("SET CHUNK 0 0 IN default $1", "PROTOCOL", Parameters{state, state}));
        assert(err("SET CHUNK 0 0 IN default", "SYNTAX"));
        assert(err("SET CHUNK 0 0 IN default x'00'", "SYNTAX"));
        assert(err("SET CHUNK 0 0 IN nope $1", "NO_TABLE", Parameters{state}));
        assert(chunk_of(0, 0) == no_chunk);
        // Frames are bounded by their column before they are read; frames
        // that cannot be bounded are refused with the connection.
        {
            using Plan = chunkdb::CommandEngine::PayloadPlan;
            const auto block = engine.PlanPayload(session, "SET BLOCK 0 0 IN default bits = $1\r\n");
            assert(block.plan == Plan::kParameters && block.parameter_limits == std::vector<std::size_t>{1});
            const auto chunk = engine.PlanPayload(session, "SET CHUNK 0 0 IN default $1\r\n");
            assert(chunk.plan == Plan::kParameters &&
                   chunk.parameter_limits == std::vector<std::size_t>{26 + chunkdb::kDefaultVarMaxChunkBytes});
            assert(engine.PlanPayload(session, "GET CHUNK 0 0 FROM default\r\n").plan == Plan::kNone);
            const auto bad = engine.PlanPayload(session, "SET CHUNK 0 0 IN default $x\r\n");
            assert(bad.plan == Plan::kReject && bad.reject_response.rfind("-ERR SYNTAX", 0) == 0);
            const auto missing = engine.PlanPayload(session, "SET CHUNK 0 0 IN nope $1\r\n");
            assert(missing.plan == Plan::kReject && missing.reject_response.rfind("-ERR NO_TABLE", 0) == 0);
            const auto column = engine.PlanPayload(session, "SET BLOCK 0 0 IN default nope = $1\r\n");
            assert(column.plan == Plan::kReject && column.reject_response.rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        }

        // Area reads.
        assert(err("GET AREA 0 0 TO 1 FROM default", "SYNTAX"));
        assert(err("GET AREA 0 0 TO 1 1 FROM default FAST", "SYNTAX"));
        assert(err("GET AREA AROUND 0 0 RADIUS 1 FROM default ZRLE", "SYNTAX"));
        assert(err("GET AREA AROUND 0 0 FROM default", "SYNTAX"));

        assert(engine.Execute(session, "GET BLOCK 1 2 FROM default\r\n") == "_\r\n");
        assert(chunk_of(0, 0) == no_chunk);

        (void)engine.Execute(session, "SET BLOCK 3 3 IN default bits = b'1111'\r\n");
        assert(engine.Execute(session, "GET BLOCK 3 3 FROM default\r\n") == "*1\r\n$1\r\n\x0f\r\n");
        assert(engine.Execute(session, "SET BLOCK 2 2 IN default bits = b'0000'\r\n").rfind(":", 0) == 0);
        assert(engine.Execute(session, "GET BLOCK 2 2 FROM default\r\n") == std::string("*1\r\n$1\r\n\0\r\n", 11));
        assert(engine.Execute(session, "DELETE BLOCK 2 2 FROM default\r\n").rfind(":", 0) == 0);
        assert(engine.Execute(session, "GET BLOCK 2 2 FROM default\r\n") == "_\r\n");

        // A write with one bad value writes none of them.
        assert(engine.Execute(session, "CREATE TABLE pair (a u4, b u4) CHUNK 4 x 4\r\n") == "+OK\r\n");
        assert(err("SET BLOCK 10 10 IN pair a = 5, b = 16", "INVALID_ARGUMENT"));
        assert(engine.Execute(session, "GET BLOCK 10 10 FROM pair\r\n") == "_\r\n");
        assert(err("SET BLOCK 10 10 IN pair a = 5, b = -1", "INVALID_ARGUMENT"));
        assert(err("SET BLOCK 10 10 IN pair a = 5, nope = 1", "INVALID_ARGUMENT"));
        assert(err("SET BLOCK 10 10 IN pair a = 5, b = $1", "INVALID_ARGUMENT", Parameters{std::string(7, '\0')}));
        assert(engine.Execute(session, "GET BLOCK 10 10 FROM pair\r\n") == "_\r\n");
        assert(engine.Execute(session, "SET BLOCK 10 10 IN pair a = 5, b = 10\r\n").rfind(":", 0) == 0);
        assert(engine.Execute(session, "GET BLOCK 10 10 FROM pair\r\n") == "*2\r\n:5\r\n:10\r\n");
        assert(engine.Execute(session, "SET CHUNK 0 0 IN default $1\r\n", Parameters{state}).rfind(":", 0) == 0);
        assert(chunk_of(0, 0) == std::string("\xff\xff", 2) + payload);
        (void)engine.Execute(session, "GET BLOCK 3 3 FROM default\r\n");

        // Store counters, summed over the tables.
        const std::string metrics_payload =
            ExtractBulkPayload(engine.Execute(session, "SHOW METRICS\r\n"));
        const auto metrics = ParseMetrics(metrics_payload);

        assert(metrics.contains("chunkdb_loaded_chunks"));
        assert(metrics.contains("chunkdb_evictions_total"));
        assert(metrics.contains("chunkdb_checkpoints_total"));
        assert(metrics.contains("chunkdb_wal_batch_flushes_total"));
        assert(metrics.contains("chunkdb_unique_loaded_chunks_total"));
        assert(metrics.contains("chunkdb_open_wal_streams"));
        for (const char* eviction :
             {"chunkdb_eviction_snapshot_builds_total", "chunkdb_eviction_probes_total",
              "chunkdb_eviction_no_progress_cycles_total", "chunkdb_eviction_forced_wal_flushes_with_data_total",
              "chunkdb_eviction_forced_wal_flushes_empty_batch_total"}) {
            assert(metrics.contains(eviction));
            (void)std::stoull(metrics.at(eviction));
        }

        assert(std::stoull(metrics.at("chunkdb_loaded_chunks")) >= 1U);
        (void)std::stoull(metrics.at("chunkdb_evictions_total"));
        (void)std::stoull(metrics.at("chunkdb_checkpoints_total"));
        (void)std::stoull(metrics.at("chunkdb_wal_batch_flushes_total"));
        assert(std::stoull(metrics.at("chunkdb_unique_loaded_chunks_total")) >= 1U);
        (void)std::stoull(metrics.at("chunkdb_open_wal_streams"));
    }

    RemoveAllWithRetry(data_dir);

    // A write whose failed append cannot be repaired may be replayed after a
    // restart: the reply says the outcome is unknown, not just "internal".
    {
        const auto dir = TempDataDir();
        chunkdb::StoreConfig config{
            .geometry = {
                .large_chunk_width_chunks = 2,
                .large_chunk_height_chunks = 2,
                .chunk_width_blocks = 4,
                .chunk_height_blocks = 4,
                .block_bits = 4,
            },
            .data_dir = dir,
            .durability_mode = chunkdb::DurabilityMode::kFsyncWal,
        };
        {
            auto catalog = std::make_shared<chunkdb::TableCatalog>(chunkdb::CatalogConfigFromStoreConfig(config));
            chunkdb::CommandEngine engine(
                chunkdb::EngineConfig{.auth_token = "", .require_auth = false, .max_auth_failures = 3}, catalog);
            chunkdb::SessionState session;
            assert(engine.Execute(session, "HELLO 3\r\n").rfind("%7\r\n", 0) == 0);
            assert(engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'0001'\r\n").rfind(":", 0) == 0);
#ifdef _WIN32
            _putenv_s("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1");
            _putenv_s("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1");
#else
            setenv("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "1", 1);
            setenv("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "1", 1);
#endif
            const auto reply = engine.Execute(session, "SET BLOCK 0 0 IN default bits = b'0010'\r\n");
#ifdef _WIN32
            _putenv_s("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE", "");
            _putenv_s("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", "");
#else
            unsetenv("CHUNKDB_FAILPOINT_WAL_BATCH_SYNC_FAIL_ONCE");
            unsetenv("CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE");
#endif
            assert(reply.rfind("-ERR INTERNAL write outcome unknown", 0) == 0);
        }
        // The engine holds the catalog: both close before the files go
        // (Windows cannot remove the files of an open store).
        RemoveAllWithRetry(dir);
    }
    return 0;
}
