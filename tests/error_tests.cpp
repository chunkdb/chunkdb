#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

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

std::unordered_map<std::string, std::string> ParseInfoMap(const std::string& payload) {
    std::unordered_map<std::string, std::string> result;
    std::istringstream in(payload);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        const auto sep = line.find('=');
        if (sep == std::string::npos) {
            continue;
        }
        result.emplace(line.substr(0, sep), line.substr(sep + 1));
    }
    return result;
}

std::string ExpectedChunkLockMode() {
#if defined(__MINGW32__) && \
    (!defined(CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS) || CHUNKDB_MINGW_SERIAL_CHUNK_LOCKS)
    return "serial-mutex";
#else
    return "shared-mutex";
#endif
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

        const auto err = [&](const std::string& line, const char* code,
                             std::string_view payload = {}) {
            const auto reply = engine.Execute(session, line + "\r\n", payload);
            return reply.rfind(std::string("-ERR ") + code, 0) == 0;
        };

        // HELLO options.
        assert(engine.Execute(session, "HELLO 2 TABLE\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "HELLO 2 COLOR red\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "HELLO 2 TABLE default TABLE default\r\n")
                   .rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(!session.greeted);
        assert(engine.Execute(session, "hello 2 table default\r\n").rfind("$", 0) == 0);

        assert(err("UNKNOWN", "UNKNOWN_COMMAND"));
        // Protocol 1 commands are gone.
        for (const char* removed : {"AUTH x", "EXISTS 1 2", "CHUNK 0 0", "CHUNKSET 0 0 0000",
                                    "CHUNKBIN 0 0", "CHUNKBINC 0 0", "CHUNKCAS 0 0 1 STATE 0|0",
                                    "CHUNKSETBIN 0 0 8"}) {
            assert(err(removed, "UNKNOWN_COMMAND"));
        }
        assert(err("UNSET 1", "INVALID_ARGUMENT"));
        assert(err("CHUNKEXISTS 1", "INVALID_ARGUMENT"));
        assert(err("SET 1 2", "INVALID_ARGUMENT"));
        assert(err("SET x 2 1111", "INVALID_ARGUMENT"));
        assert(err("SET 1 2 12AB", "INVALID_ARGUMENT"));
        assert(err("SET 1 2 11111", "INVALID_ARGUMENT"));
        assert(err("GET 1", "INVALID_ARGUMENT"));

        // CHUNKGET options.
        assert(err("CHUNKGET 0", "INVALID_ARGUMENT"));
        assert(err("CHUNKGET 0 0 BADMODE", "INVALID_ARGUMENT"));
        assert(err("CHUNKGET 0 0 STATE STATE", "INVALID_ARGUMENT"));
        assert(err("CHUNKGET x 0", "INVALID_ARGUMENT"));

        // CHUNKPUT: 4x4 blocks of 4 bits, 8 payload bytes, 2 presence bytes.
        const std::string payload(8, '\x11');
        const std::string state = payload + std::string("\xff\xff", 2);
        assert(err("CHUNKPUT 0 0 4", "INVALID_ARGUMENT", std::string(4, '\0')));
        assert(err("CHUNKPUT 0 0 STATE 8", "INVALID_ARGUMENT", payload));
        assert(err("CHUNKPUT 0 0 BADMODE 8", "INVALID_ARGUMENT", payload));
        assert(err("CHUNKPUT 0 0 IF x 8", "INVALID_ARGUMENT", payload));
        assert(err("CHUNKPUT 0 0 9", "INVALID_ARGUMENT", payload));  // length mismatch
        assert(err("CHUNKPUT 0 0 ZRLE 8", "INVALID_ARGUMENT", payload));  // not zrle
        assert(err("CHUNKPUT 0 0", "INVALID_ARGUMENT"));
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+0\r\n");

        // CHUNKBATCH.
        assert(err("CHUNKBATCH 0 0 IF", "INVALID_ARGUMENT"));
        assert(err("CHUNKBATCH 0 0 IF 1", "INVALID_ARGUMENT"));
        assert(err("CHUNKBATCH 0 0 - SET 0 0 1111", "INVALID_ARGUMENT"));
        assert(err("CHUNKBATCH 0 0 MOVE 0 0", "INVALID_ARGUMENT"));
        assert(err("CHUNKBATCH 0 0 SET 9 9 1111", "INVALID_ARGUMENT"));  // other chunk

        // Area reads.
        assert(err("CHUNKRANGE 0 0 1", "INVALID_ARGUMENT"));
        assert(err("CHUNKRANGE 0 0 1 1 FAST", "INVALID_ARGUMENT"));
        assert(err("CHUNKRADIUS 0 0 1 ZRLE ZRLE", "INVALID_ARGUMENT"));

        assert(engine.Execute(session, "GET 1 2\r\n") == "$-1\r\n");
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+0\r\n");
        assert(engine.Execute(session, "CHUNKGET 0 0\r\n").rfind("$8\r\n", 0) == 0);
        assert(engine.Execute(session, "CHUNKGET 0 0 STATE\r\n").rfind("$10\r\n", 0) == 0);

        (void)engine.Execute(session, "SET 3 3 1111\r\n");
        assert(engine.Execute(session, "GET 3 3\r\n") == "$4\r\n1111\r\n");
        assert(engine.Execute(session, "SET 2 2 0000\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "GET 2 2\r\n") == "$4\r\n0000\r\n");
        assert(engine.Execute(session, "UNSET 2 2\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "GET 2 2\r\n") == "$-1\r\n");
        assert(engine.Execute(session, "MSET 10 10 1010 11 11 12AB\r\n")
                   .rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "GET 10 10\r\n") == "$-1\r\n");
        assert(engine.Execute(session, "MSET 10 10 1010 x 11 0101\r\n")
                   .rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "MSET 10 10 1010 11 11 0101\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "MGET 10 10 11 11 12 12\r\n") ==
               "*3\r\n$4\r\n1010\r\n$4\r\n0101\r\n$-1\r\n");
        assert(engine.Execute(session, "CHUNKPUT 0 0 STATE 10\r\n", state).rfind("$", 0) == 0);
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+1\r\n");
        (void)engine.Execute(session, "GET 3 3\r\n");
        const std::string info_payload = ExtractBulkPayload(engine.Execute(session, "INFO\r\n"));
        const auto info = ParseInfoMap(info_payload);

        assert(info.contains("loaded_chunks"));
        assert(info.contains("evictions"));
        assert(info.contains("checkpoints"));
        assert(info.contains("wal_batch_flushes"));
        assert(info.contains("unique_loaded_chunks"));
        assert(info.contains("open_wal_streams"));
        assert(info.contains("eviction_snapshot_builds"));
        assert(info.contains("eviction_probes"));
        assert(info.contains("eviction_no_progress_cycles"));
        assert(info.contains("eviction_forced_wal_flushes"));
        assert(info.contains("eviction_forced_wal_flushes_with_data"));
        assert(info.contains("eviction_forced_wal_flushes_empty_batch"));
        assert(info.contains("chunk_lock_mode"));

        (void)std::stoull(info.at("loaded_chunks"));
        (void)std::stoull(info.at("evictions"));
        (void)std::stoull(info.at("checkpoints"));
        (void)std::stoull(info.at("wal_batch_flushes"));
        (void)std::stoull(info.at("unique_loaded_chunks"));
        (void)std::stoull(info.at("open_wal_streams"));
        (void)std::stoull(info.at("eviction_snapshot_builds"));
        (void)std::stoull(info.at("eviction_probes"));
        (void)std::stoull(info.at("eviction_no_progress_cycles"));
        const auto forced_total = std::stoull(info.at("eviction_forced_wal_flushes"));
        const auto forced_with_data = std::stoull(info.at("eviction_forced_wal_flushes_with_data"));
        const auto forced_empty = std::stoull(info.at("eviction_forced_wal_flushes_empty_batch"));
        assert(forced_total == forced_with_data + forced_empty);
        assert(info.at("chunk_lock_mode") == ExpectedChunkLockMode());
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
