#include <cassert>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "chunkdb/chunk_store.hpp"
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
                .auth_token = "secret",
                .require_auth = true,
                .max_auth_failures = 5,
            },
            store);

        chunkdb::SessionState session;

        assert(engine.Execute(session, "GET 0 0\r\n").rfind("-ERR AUTH_REQUIRED", 0) == 0);
        assert(engine.Execute(session, "AUTH bad\r\n").rfind("-ERR AUTH_FAILED", 0) == 0);
        assert(!session.authenticated);

        const std::string auth_ok = engine.Execute(session, "AUTH secret\r\n");
        assert(auth_ok == "+OK\r\n");
        assert(session.authenticated);

        const std::string set_ok = engine.Execute(session, "SET 0 0 1111\r\n");
        assert(set_ok == "+OK\r\n");

        const std::string exists_set = engine.Execute(session, "EXISTS 0 0\r\n");
        assert(exists_set == "+1\r\n");

        const std::string get_reply = engine.Execute(session, "GET 0 0\r\n");
        assert(get_reply == "$4\r\n1111\r\n");

        const std::string unset_ok = engine.Execute(session, "UNSET 0 0\r\n");
        assert(unset_ok == "+OK\r\n");
        assert(engine.Execute(session, "EXISTS 0 0\r\n") == "+0\r\n");
        assert(engine.Execute(session, "GET 0 0\r\n") == "$4\r\n0000\r\n");

        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+0\r\n");
        assert(engine.Execute(session, "CHUNKSET 0 0 0000000000000000000000000000000000000000000000000000000000000000\r\n") == "+OK\r\n");
        assert(engine.Execute(session, "CHUNKEXISTS 0 0\r\n") == "+1\r\n");
        assert(engine.Execute(session, "CHUNK 0 0\r\n") ==
               "$64\r\n0000000000000000000000000000000000000000000000000000000000000000\r\n");

        const std::string chunk_bin = engine.Execute(session, "CHUNKBIN 0 0\r\n");
        assert(chunk_bin.rfind("$8\r\n", 0) == 0);

        chunkdb::SessionState brute;
        for (int i = 0; i < 5; ++i) {
            (void)engine.Execute(brute, "AUTH nope\r\n");
        }
        assert(brute.close_after_reply);
    }

    RemoveAllWithRetry(data_dir);
    return 0;
}
