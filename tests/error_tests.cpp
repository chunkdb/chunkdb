#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"

namespace {

std::filesystem::path TempDataDir() {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-error-test-" + std::to_string(tick));
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

std::shared_ptr<chunkdb::ChunkStore> BuildStore(const std::filesystem::path& dir) {
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
    return std::make_shared<chunkdb::ChunkStore>(config);
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

}  // namespace

int main() {
    const auto data_dir = TempDataDir();

    {
        auto store = BuildStore(data_dir);

        chunkdb::CommandEngine engine(
            chunkdb::EngineConfig{
                .auth_token = "",
                .require_auth = false,
                .max_auth_failures = 3,
            },
            store);

        chunkdb::SessionState session;

        assert(engine.Execute(session, "UNKNOWN\r\n").rfind("-ERR UNKNOWN_COMMAND", 0) == 0);
        assert(engine.Execute(session, "SET 1 2\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "SET x 2 1111\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "SET 1 2 12AB\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);
        assert(engine.Execute(session, "SET 1 2 11111\r\n").rfind("-ERR INVALID_ARGUMENT", 0) == 0);

        const auto reply = engine.Execute(session, "GET 1 2\r\n");
        assert(reply == "$4\r\n0000\r\n");
        assert(engine.Execute(session, "CHUNKBIN 0 0\r\n").rfind("$8\r\n", 0) == 0);

        (void)engine.Execute(session, "SET 3 3 1111\r\n");
        (void)engine.Execute(session, "GET 3 3\r\n");
        const std::string info_payload = ExtractBulkPayload(engine.Execute(session, "INFO\r\n"));
        const auto info = ParseInfoMap(info_payload);

        assert(info.contains("loaded_chunks"));
        assert(info.contains("evictions"));
        assert(info.contains("checkpoints"));
        assert(info.contains("wal_batch_flushes"));
        assert(info.contains("unique_loaded_chunks"));

        (void)std::stoull(info.at("loaded_chunks"));
        (void)std::stoull(info.at("evictions"));
        (void)std::stoull(info.at("checkpoints"));
        (void)std::stoull(info.at("wal_batch_flushes"));
        (void)std::stoull(info.at("unique_loaded_chunks"));
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
