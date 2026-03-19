#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "chunkdb/chunk_store.hpp"

#ifndef _WIN32
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

using Clock = std::chrono::steady_clock;

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-process-lock-test-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(
    const std::filesystem::path& path,
    chunkdb::AccessMode mode = chunkdb::AccessMode::kReadWrite,
    bool allow_multi = false) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 4,
        },
        .data_dir = path,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 8,
        .checkpoint_wal_bytes = 256,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = allow_multi,
        .access_mode = mode,
    };
}

std::filesystem::path LockDir(const std::filesystem::path& data_dir) {
    return data_dir / ".chunkdb.lock";
}

std::filesystem::path WriterMetaPath(const std::filesystem::path& data_dir) {
    return LockDir(data_dir) / "writer.meta";
}

std::unordered_map<std::string, std::string> ReadKvFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        throw std::runtime_error("failed to open metadata file: " + path.string());
    }

    std::unordered_map<std::string, std::string> map;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos || eq == 0 || eq + 1 >= line.size()) {
            continue;
        }
        map.emplace(line.substr(0, eq), line.substr(eq + 1));
    }
    return map;
}

std::unordered_map<std::string, std::string> WaitReadMeta(
    const std::filesystem::path& data_dir,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(2000)) {
    const auto deadline = Clock::now() + timeout;
    const auto meta_path = WriterMetaPath(data_dir);

    while (Clock::now() < deadline) {
        std::error_code ec;
        if (std::filesystem::exists(meta_path, ec) && !ec) {
            const auto map = ReadKvFile(meta_path);
            if (!map.empty() && map.contains("session_id") && map.contains("pid") && map.contains("heartbeat_ms")) {
                return map;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    throw std::runtime_error("timed out waiting for writer metadata");
}

void WriteTextFile(const std::filesystem::path& path, std::string_view text) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    assert(out.is_open());
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
    out.flush();
    assert(out.good());
}

void TestSecondWriterBlockedAndReaderAllowed() {
    const auto data_dir = TempDataDir("second-writer");

    auto first = std::make_unique<chunkdb::ChunkStore>(BuildConfig(data_dir));
    first->SetBlockBits(0, 0, "1111");

    const auto meta = WaitReadMeta(data_dir);
    assert(meta.at("mode") == "writer");

    bool blocked = false;
    try {
        auto second = std::make_unique<chunkdb::ChunkStore>(BuildConfig(data_dir));
        (void)second;
    } catch (const std::exception&) {
        blocked = true;
    }
    assert(blocked);

    auto reader = std::make_unique<chunkdb::ChunkStore>(
        BuildConfig(data_dir, chunkdb::AccessMode::kReadOnly));
    assert(reader->GetBlockBits(0, 0) == "1111");

    bool read_only_rejected_set = false;
    try {
        reader->SetBlockBits(0, 0, "0000");
    } catch (const std::exception&) {
        read_only_rejected_set = true;
    }
    assert(read_only_rejected_set);

    reader.reset();
    first.reset();

    std::filesystem::remove_all(data_dir);
}

void TestStaleLockTakeover() {
    const auto data_dir = TempDataDir("stale-takeover");
    const auto lock_dir = LockDir(data_dir);
    const auto meta_path = WriterMetaPath(data_dir);

    std::filesystem::create_directories(lock_dir);
    WriteTextFile(
        meta_path,
        "session_id=stale-session\n"
        "pid=999999\n"
        "heartbeat_ms=1\n"
        "mode=writer\n");

    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        const auto meta = WaitReadMeta(data_dir);
        assert(meta.at("session_id") != "stale-session");
        assert(meta.at("mode") == "writer");
        writer.SetBlockBits(1, 1, "1010");
        assert(writer.GetBlockBits(1, 1) == "1010");
    }

    std::filesystem::remove_all(data_dir);
}

void TestCleanShutdownReleasesWriterOwnership() {
    const auto data_dir = TempDataDir("clean-shutdown");
    std::string session_a;

    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        const auto meta = WaitReadMeta(data_dir);
        session_a = meta.at("session_id");
        assert(!session_a.empty());
    }

    // After clean shutdown, writer lock state should be removable/re-creatable.
    {
        chunkdb::ChunkStore writer_again(BuildConfig(data_dir));
        const auto meta = WaitReadMeta(data_dir);
        assert(meta.at("session_id") != session_a);
    }

    std::filesystem::remove_all(data_dir);
}

void TestLegacyLockFileMigratedToDirectory() {
    const auto data_dir = TempDataDir("legacy-lock-file");
    const auto lock_path = LockDir(data_dir);
    WriteTextFile(lock_path, "legacy-lock-file\n");

    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        writer.SetBlockBits(0, 0, "1100");
        assert(writer.GetBlockBits(0, 0) == "1100");
        assert(std::filesystem::is_directory(lock_path));
    }

    std::vector<std::filesystem::path> legacy_files;
    for (const auto& entry : std::filesystem::directory_iterator(data_dir)) {
        std::error_code ec;
        if (!entry.is_regular_file(ec) || ec) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (name.rfind(".chunkdb.lock.legacy.", 0) == 0) {
            legacy_files.push_back(entry.path());
        }
    }

    assert(!legacy_files.empty());
    assert(std::filesystem::is_directory(lock_path));
    std::filesystem::remove_all(data_dir);
}

#ifndef _WIN32
void TestUnsupportedLockPathTypeRejected() {
    const auto data_dir = TempDataDir("unsupported-lock-type");
    const auto lock_path = LockDir(data_dir);
    const auto target = data_dir / "lock-symlink-target";

    std::filesystem::create_directories(data_dir);
    WriteTextFile(target, "target\n");
    std::error_code symlink_ec;
    std::filesystem::create_symlink(target, lock_path, symlink_ec);
    if (symlink_ec) {
        // Symlink creation can be blocked by environment policy; skip deterministically.
        std::filesystem::remove_all(data_dir);
        return;
    }

    bool rejected = false;
    try {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        (void)writer;
    } catch (const std::exception& ex) {
        const std::string msg = ex.what();
        rejected = msg.find("unsupported process lock path type") != std::string::npos;
    }
    assert(rejected);
    std::filesystem::remove_all(data_dir);
}
#endif

#ifndef _WIN32
[[noreturn]] void ChildWriterLoop(const std::filesystem::path& data_dir) {
    chunkdb::ChunkStore writer(BuildConfig(data_dir));
    writer.SetBlockBits(0, 0, "1010");

    while (true) {
        writer.SetBlockBits(0, 0, "1010");
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
}

void TestKill9AndRestartAfterCrash(const char* argv0) {
    const auto data_dir = TempDataDir("kill9-restart");

    const pid_t child = fork();
    assert(child >= 0);

    if (child == 0) {
        execl(argv0, argv0, "--child-writer", data_dir.c_str(), nullptr);
        _exit(127);
    }

    const auto child_meta = WaitReadMeta(data_dir, std::chrono::milliseconds(4000));
    const std::string child_session = child_meta.at("session_id");
    assert(!child_session.empty());

    kill(child, SIGKILL);
    int status = 0;
    waitpid(child, &status, 0);
    assert(WIFSIGNALED(status));

    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        const auto meta = WaitReadMeta(data_dir);
        assert(meta.at("session_id") != child_session);
        writer.SetBlockBits(0, 0, "0101");
        assert(writer.GetBlockBits(0, 0) == "0101");
    }

    // Restart once more to ensure post-crash recovery is stable across another open cycle.
    {
        chunkdb::ChunkStore writer(BuildConfig(data_dir));
        writer.SetBlockBits(0, 1, "0011");
        assert(writer.GetBlockBits(0, 1) == "0011");
    }

    std::filesystem::remove_all(data_dir);
}
#endif

}  // namespace

int main(int argc, char** argv) {
#ifndef _WIN32
    if (argc == 3 && std::string(argv[1]) == "--child-writer") {
        ChildWriterLoop(argv[2]);
    }
#endif

    TestSecondWriterBlockedAndReaderAllowed();
    TestStaleLockTakeover();
    TestCleanShutdownReleasesWriterOwnership();
    TestLegacyLockFileMigratedToDirectory();

#ifndef _WIN32
    TestUnsupportedLockPathTypeRejected();
#endif

#ifndef _WIN32
    TestKill9AndRestartAfterCrash(argv[0]);
#endif

    return 0;
}
