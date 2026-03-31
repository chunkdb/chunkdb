#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/logging.hpp"

namespace {

std::filesystem::path TempDataDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path();
    const auto tick = static_cast<long long>(
        std::filesystem::file_time_type::clock::now().time_since_epoch().count());
    return base / ("chunkdb-crash-hardening-" + suffix + "-" + std::to_string(tick));
}

chunkdb::StoreConfig BuildConfig(
    const std::filesystem::path& data_dir,
    chunkdb::DurabilityMode mode) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 8,
        },
        .data_dir = data_dir,
        .durability_mode = mode,
        .checkpoint_update_interval = 1,
        .checkpoint_wal_bytes = 128,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 64,
        .allow_multiple_processes = true,
    };
}

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    const int rc = _putenv_s(key, value);
    assert(rc == 0);
#else
    const int rc = setenv(key, value, 1);
    assert(rc == 0);
#endif
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    const int rc = _putenv_s(key, "");
    assert(rc == 0);
#else
    const int rc = unsetenv(key);
    assert(rc == 0);
#endif
}

class ScopedEnv {
  public:
    ScopedEnv(const char* key, const char* value) : key_(key) {
        const char* previous = std::getenv(key_);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        SetEnvVar(key_, value);
    }

    ~ScopedEnv() {
        if (had_previous_) {
            SetEnvVar(key_, previous_.c_str());
        } else {
            UnsetEnvVar(key_);
        }
    }

  private:
    const char* key_;
    bool had_previous_ = false;
    std::string previous_;
};

class ScopedLogCapture {
  public:
    explicit ScopedLogCapture(chunkdb::LogLevel level)
        : previous_level_(chunkdb::GetLogLevel()) {
        chunkdb::SetLogLevel(level);
        chunkdb::SetLogSinkForTests([this](const std::string& line) {
            std::lock_guard lock(mutex_);
            lines_.push_back(line);
        });
    }

    ~ScopedLogCapture() {
        chunkdb::ResetLogSinkForTests();
        chunkdb::SetLogLevel(previous_level_);
    }

    [[nodiscard]] bool Contains(std::string_view needle) const {
        std::lock_guard lock(mutex_);
        for (const auto& line : lines_) {
            if (line.find(needle) != std::string::npos) {
                return true;
            }
        }
        return false;
    }

  private:
    chunkdb::LogLevel previous_level_;
    mutable std::mutex mutex_;
    std::vector<std::string> lines_;
};

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in.is_open());

    in.seekg(0, std::ios::end);
    const std::streamoff size = in.tellg();
    assert(size >= 0);
    in.seekg(0, std::ios::beg);

    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        in.read(reinterpret_cast<char*>(bytes.data()), size);
    }
    assert(in.good() || in.eof());
    return bytes;
}

void AppendRaw(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    assert(out.is_open());
    if (!bytes.empty()) {
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }
    out.flush();
    assert(out.good());
}

void WriteLe16(std::vector<std::uint8_t>* out, std::uint16_t v) {
    out->push_back(static_cast<std::uint8_t>(v & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
}

void WriteLe32(std::vector<std::uint8_t>* out, std::uint32_t v) {
    out->push_back(static_cast<std::uint8_t>(v & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 8U) & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 16U) & 0xFFU));
    out->push_back(static_cast<std::uint8_t>((v >> 24U) & 0xFFU));
}

std::size_t CountTmpArtifactsForTarget(const std::filesystem::path& target_path) {
    const auto parent = target_path.parent_path();
    if (!std::filesystem::exists(parent)) {
        return 0;
    }

    const std::string prefix = target_path.filename().string() + ".tmp.";
    std::size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto name = entry.path().filename().string();
        if (name.rfind(prefix, 0) == 0) {
            ++count;
        }
    }
    return count;
}

void TestCrashPointAfterTempFlushBeforeRename() {
    const auto data_dir = TempDataDir("failpoint-after-temp-flush");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    std::vector<std::uint8_t> baseline;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        assert(std::filesystem::exists(data_path));
        baseline = ReadBytes(data_path);

        bool threw = false;
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE", "1");
            try {
                store.SetBlockBits(0, 0, "00001111");
            } catch (const std::exception&) {
                threw = true;
            }
        }

        assert(threw);
        assert(std::filesystem::exists(data_path));
        const auto after_fail = ReadBytes(data_path);
        assert(after_fail == baseline);
        assert(CountTmpArtifactsForTarget(data_path) >= 1);
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }
    assert(CountTmpArtifactsForTarget(data_path) == 0);

    std::filesystem::remove_all(data_dir);
}

void TestOrphanTempArtifactCleanup() {
    const auto data_dir = TempDataDir("orphan-cleanup");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(1, 1);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);
    const auto orphan_path = std::filesystem::path(data_path.string() + ".tmp.orphan-manual");

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(1, 1, "10101010");
    }

    {
        std::ofstream out(orphan_path, std::ios::binary | std::ios::trunc);
        assert(out.is_open());
        out << "orphan";
    }
    assert(std::filesystem::exists(orphan_path));

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(1, 1) == "10101010");
    }

    assert(!std::filesystem::exists(orphan_path));
    std::filesystem::remove_all(data_dir);
}

void TestFlushFailureInjectionFailsLoudly() {
    const auto data_dir = TempDataDir("sync-fail");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "00110011");
        const auto baseline = ReadBytes(data_path);

        bool threw = false;
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE", "1");
            try {
                store.SetBlockBits(0, 0, "11001100");
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(ReadBytes(data_path) == baseline);
    }

    std::filesystem::remove_all(data_dir);
}

void TestCrashPointAfterRenameBeforeDirSync() {
    const auto data_dir = TempDataDir("failpoint-after-rename-before-dirsync");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, "01010101");
    }

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "01010101");
        ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "10101010");
        } catch (const std::exception&) {
            threw = true;
        }
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        const std::string bits = recovered.GetBlockBits(0, 0);
        // Replace boundary fault must never leave a torn final state.
        // The persisted value must be old-or-new only.
        assert(bits == "01010101" || bits == "10101010");
    }

    std::filesystem::remove_all(data_dir);
}

void TestReplaceBoundaryOldOrNewInvariantRepeated() {
    const auto data_dir = TempDataDir("replace-old-or-new-repeated");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);
    std::string expected = "00000000";

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, expected);
    }

    for (int i = 0; i < 8; ++i) {
        const std::string next = (i % 2 == 0) ? "11110000" : "00001111";
        bool threw = false;
        {
            chunkdb::ChunkStore store(config);
            assert(store.GetBlockBits(0, 0) == expected);
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
            try {
                store.SetBlockBits(0, 0, next);
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);

        {
            chunkdb::ChunkStore recovered(config);
            const std::string persisted = recovered.GetBlockBits(0, 0);
            assert(persisted == expected || persisted == next);
            expected = persisted;
        }
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalFirstCreateAfterFileSyncBeforeDirSync() {
    const auto data_dir = TempDataDir("wal-first-create-after-file-sync-before-dirsync");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore seed(config);
        seed.SetBlockBits(0, 0, "01010101");
    }

    if (std::filesystem::exists(wal_path)) {
        std::filesystem::remove(wal_path);
    }
    assert(!std::filesystem::exists(wal_path));

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "01010101");
        ScopedEnv fp("CHUNKDB_FAILPOINT_WAL_AFTER_FILE_SYNC_BEFORE_DIR_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "10101010");
        } catch (const std::exception&) {
            threw = true;
        }
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        const std::string bits = recovered.GetBlockBits(0, 0);
        // On first WAL create boundary fault (file sync complete, dir sync missing),
        // persisted state after restart must remain old-or-new only.
        assert(bits == "01010101" || bits == "10101010");
    }

    std::filesystem::remove_all(data_dir);
}

void TestCheckpointFirstCreateAfterDirectoryCreateBeforeParentSync() {
    const auto data_dir = TempDataDir("checkpoint-dir-create-before-parent-sync");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncCheckpoint);

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const chunkdb::LargeChunkCoord large_coord = geometry.ChunkToLarge(coord);
    const auto large_dir = chunkdb::LargeChunkDirectory(data_dir, large_coord);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        ScopedEnv fp("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "11110000");
        } catch (const std::exception&) {
            threw = true;
        }
        assert(std::filesystem::exists(large_dir));
        assert(!std::filesystem::exists(data_path));
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        recovered.SetBlockBits(0, 0, "11110000");
        assert(recovered.GetBlockBits(0, 0) == "11110000");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "11110000");
    }

    std::filesystem::remove_all(data_dir);
}

void TestWalFirstCreateAfterDirectoryCreateBeforeParentSync() {
    const auto data_dir = TempDataDir("wal-dir-create-before-parent-sync");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const chunkdb::LargeChunkCoord large_coord = geometry.ChunkToLarge(coord);
    const auto large_dir = chunkdb::LargeChunkDirectory(data_dir, large_coord);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);
    const auto data_path = chunkdb::ChunkDataPath(data_dir, geometry, coord);

    bool threw = false;
    {
        chunkdb::ChunkStore store(config);
        ScopedEnv fp("CHUNKDB_FAILPOINT_DIR_CREATE_BEFORE_PARENT_SYNC_ONCE", "1");
        try {
            store.SetBlockBits(0, 0, "00001111");
        } catch (const std::exception&) {
            threw = true;
        }
        assert(std::filesystem::exists(large_dir));
        assert(!std::filesystem::exists(wal_path));
        assert(!std::filesystem::exists(data_path));
    }
    assert(threw);

    {
        chunkdb::ChunkStore recovered(config);
        recovered.SetBlockBits(0, 0, "00001111");
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00001111");
    }

    std::filesystem::remove_all(data_dir);
}

void TestTornWalTailIgnored() {
    const auto data_dir = TempDataDir("torn-wal-tail");
    auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kRelaxed);
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    const chunkdb::Geometry geometry(config.geometry);
    const chunkdb::ChunkCoord coord = geometry.BlockToChunk(0, 0);
    const auto wal_path = chunkdb::ChunkWalPath(data_dir, geometry, coord);

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11110000");
        store.SetBlockBits(1, 0, "00001111");
    }

    assert(std::filesystem::exists(wal_path));

    // Append one intentionally corrupted tail record:
    // DLT1 + byte_offset + data_size + bad_crc + one byte data.
    std::vector<std::uint8_t> corrupt;
    corrupt.push_back('D');
    corrupt.push_back('L');
    corrupt.push_back('T');
    corrupt.push_back('1');
    WriteLe32(&corrupt, 0U);
    WriteLe16(&corrupt, 1U);
    WriteLe32(&corrupt, 0xDEADBEEFU);
    corrupt.push_back(0xAAU);
    AppendRaw(wal_path, corrupt);

    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "11110000");
        assert(recovered.GetBlockBits(1, 0) == "00001111");
    }

    std::filesystem::remove_all(data_dir);
}

void TestWindowsDirectorySyncCapabilityUnavailableFailsClosed() {
#ifdef _WIN32
    const auto data_dir = TempDataDir("windows-dir-sync-unavailable");
    const auto config = BuildConfig(data_dir, chunkdb::DurabilityMode::kFsyncWal);
    ScopedLogCapture logs(chunkdb::LogLevel::kWarn);

    {
        ScopedEnv fp("CHUNKDB_FAILPOINT_WINDOWS_DIRECTORY_SYNC_CAPABILITY_ERROR_ONCE", "1");
        bool threw = false;
        try {
            chunkdb::ChunkStore store(config);
            store.SetBlockBits(0, 0, "11110000");
        } catch (const std::runtime_error& error) {
            threw = true;
            const std::string message = error.what();
            assert(message.find("strict durability requires Windows directory sync capability") !=
                   std::string::npos);
        }
        assert(threw);
    }

    assert(logs.Contains("ERROR store pid="));
    assert(logs.Contains(
        "strict durability unavailable on Windows; directory sync capability missing"));
    assert(logs.Contains("step=directory_sync"));
    assert(logs.Contains(
        "impact=\"strict durability mode cannot be honored on this filesystem/runtime\""));

    std::filesystem::remove_all(data_dir);
#endif
}

}  // namespace

int main() {
    TestCrashPointAfterTempFlushBeforeRename();
    TestCrashPointAfterRenameBeforeDirSync();
    TestReplaceBoundaryOldOrNewInvariantRepeated();
    TestCheckpointFirstCreateAfterDirectoryCreateBeforeParentSync();
    TestWalFirstCreateAfterFileSyncBeforeDirSync();
    TestWalFirstCreateAfterDirectoryCreateBeforeParentSync();
    TestOrphanTempArtifactCleanup();
    TestFlushFailureInjectionFailsLoudly();
    TestTornWalTailIgnored();
    TestWindowsDirectorySyncCapabilityUnavailableFailsClosed();
    return 0;
}
