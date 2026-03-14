#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

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
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
