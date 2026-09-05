#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "chunk_store_internal.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/crc32.hpp"
#include "test_utils.hpp"

namespace {

enum class ConditionalKind {
    kCas,
    kBatch,
};

using DirectoryBytes = std::map<std::string, std::vector<std::uint8_t>>;

void SetEnvVar(const char* key, const char* value) {
#ifdef _WIN32
    _putenv_s(key, value);
#else
    setenv(key, value, 1);
#endif
}

void UnsetEnvVar(const char* key) {
#ifdef _WIN32
    _putenv_s(key, "");
#else
    unsetenv(key);
#endif
}

class ScopedEnv {
  public:
    ScopedEnv(const char* key, const char* value) : key_(key) {
        SetEnvVar(key_, value);
    }
    ~ScopedEnv() { UnsetEnvVar(key_); }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

  private:
    const char* key_;
};

std::vector<std::uint8_t> ReadBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.is_open());
    return {
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
}

void WriteBytes(
    const std::filesystem::path& path,
    const std::vector<std::uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    assert(output.is_open());
    output.write(
        reinterpret_cast<const char*>(bytes.data()),
        static_cast<std::streamsize>(bytes.size()));
    output.flush();
    assert(output.good());
}

DirectoryBytes SnapshotDirectory(
    const std::filesystem::path& root,
    bool ignore_active_writer_metadata = false) {
    DirectoryBytes snapshot;
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto relative =
            std::filesystem::relative(entry.path(), root).generic_string();
        if (ignore_active_writer_metadata &&
            relative.rfind(".chunkdb.lock/", 0) == 0) {
            continue;
        }
        snapshot.emplace(relative, ReadBytes(entry.path()));
    }
    return snapshot;
}

chunkdb::StoreConfig BaseConfig(
    const std::filesystem::path& data_dir,
    chunkdb::StorageLayoutMode layout =
        chunkdb::StorageLayoutMode::kFsSplitV1) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 5,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kFsyncWal,
        .checkpoint_update_interval = 1'000'000,
        .checkpoint_wal_bytes = 1'000'000,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
        .storage_layout_mode = layout,
        .experimental_region_span_chunks = 2,
    };
}

chunkdb::StoreConfig ReadOnlyConfig(chunkdb::StoreConfig config) {
    config.access_mode = chunkdb::AccessMode::kReadOnly;
    return config;
}

std::filesystem::path WalPath(
    const chunkdb::StoreConfig& config,
    const chunkdb::ChunkCoord& coord = {0, 0}) {
    return chunkdb::LayoutWalPath(
        config.data_dir,
        chunkdb::Geometry(config.geometry),
        coord,
        config.storage_layout_mode);
}

std::filesystem::path IntentPath(
    const std::filesystem::path& data_dir,
    const std::filesystem::path& wal_path) {
    const auto path = chunkdb::ConditionalIntentPathForWal(data_dir, wal_path);
    std::filesystem::create_directories(path.parent_path());
    return path;
}

void WriteLe64(std::vector<std::uint8_t>* bytes, std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        bytes->push_back(
            static_cast<std::uint8_t>((value >> shift) & 0xFFU));
    }
}

std::vector<std::uint8_t> IntentBytes(
    bool committed,
    std::uint64_t boundary) {
    std::vector<std::uint8_t> bytes = committed
        ? std::vector<std::uint8_t>{'C', 'K', 'R', 'C'}
        : std::vector<std::uint8_t>{'C', 'K', 'R', 'B'};
    WriteLe64(&bytes, boundary);
    const std::uint32_t crc = chunkdb::Crc32(bytes);
    for (int shift = 0; shift < 32; shift += 8) {
        bytes.push_back(
            static_cast<std::uint8_t>((crc >> shift) & 0xFFU));
    }
    return bytes;
}

std::vector<std::uint8_t> SnapshotGenerationBytes(
    std::uint64_t generation) {
    std::vector<std::uint8_t> bytes = {'C', 'K', 'S', 'G'};
    WriteLe64(&bytes, generation);
    const std::uint32_t crc = chunkdb::Crc32(bytes);
    for (int shift = 0; shift < 32; shift += 8) {
        bytes.push_back(
            static_cast<std::uint8_t>((crc >> shift) & 0xFFU));
    }
    return bytes;
}

std::string CommittedBits(ConditionalKind kind) {
    return kind == ConditionalKind::kCas ? "11111" : "01010";
}

void ApplyConditional(
    ConditionalKind kind,
    chunkdb::ChunkStore* store,
    std::uint64_t expected_version) {
    if (kind == ConditionalKind::kCas) {
        const auto result = store->CasChunkState(
            0,
            0,
            expected_version,
            std::string(store->geometry().ChunkPayloadBits(), '1'),
            std::string(store->geometry().ChunkBlockCount(), '1'));
        assert(result.ok);
        return;
    }
    const std::vector<chunkdb::ChunkBatchOp> ops = {
        {.set = true, .x = 0, .y = 0, .bits = "01010"}};
    const auto result =
        store->ApplyChunkBatch(0, 0, true, expected_version, ops);
    assert(result.ok);
}

void AssertReadOnlyBitsWithoutMutation(
    const chunkdb::StoreConfig& config,
    std::string_view expected_bits,
    bool ignore_active_writer_metadata = false) {
    const auto before = SnapshotDirectory(
        config.data_dir, ignore_active_writer_metadata);
    {
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        assert(reader.GetBlockBits(0, 0) == expected_bits);
    }
    assert(SnapshotDirectory(
               config.data_dir, ignore_active_writer_metadata) == before);
}

void TestPersistedCkrbRejectedMutation() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const auto kind :
             {ConditionalKind::kCas, ConditionalKind::kBatch}) {
            for (const bool existing_prefix : {false, true}) {
                chunkdb::test::ScopedTempDir dir(
                    "chunkdb-read-only-persisted-ckrb");
                auto config = BaseConfig(dir.path(), layout);
                {
                    chunkdb::ChunkStore writer(config);
                    if (existing_prefix) {
                        writer.SetBlockBits(0, 0, "10101");
                    }
                    const auto expected = writer.GetChunkVersion(0, 0);
                    bool threw = false;
                    {
                        ScopedEnv after_append(
                            "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE",
                            "1");
                        ScopedEnv rollback_failure(
                            existing_prefix
                                ? "CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE"
                                : "CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE",
                            "1");
                        try {
                            ApplyConditional(kind, &writer, expected);
                        } catch (const std::exception&) {
                            threw = true;
                        }
                    }
                    assert(threw);
                    assert(writer.GetBlockBits(0, 0) ==
                           (existing_prefix ? "10101" : "00000"));
                }

                assert(std::filesystem::exists(IntentPath(config.data_dir, WalPath(config))));
                AssertReadOnlyBitsWithoutMutation(
                    config, existing_prefix ? "10101" : "00000");

                {
                    chunkdb::ChunkStore recovered(config);
                    assert(recovered.GetBlockBits(0, 0) ==
                           (existing_prefix ? "10101" : "00000"));
                }
                assert(!std::filesystem::exists(IntentPath(config.data_dir, WalPath(config))));
            }
        }
    }
}

void TestRetainedCkrcIsCommittedAndNonMutating() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const auto kind :
             {ConditionalKind::kCas, ConditionalKind::kBatch}) {
            chunkdb::test::ScopedTempDir dir(
                "chunkdb-read-only-retained-ckrc");
            auto config = BaseConfig(dir.path(), layout);
            {
                chunkdb::ChunkStore writer(config);
                writer.SetBlockBits(0, 0, "10101");
                ScopedEnv retain(
                    "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE",
                    "1");
                ApplyConditional(
                    kind, &writer, writer.GetChunkVersion(0, 0));
                assert(writer.GetBlockBits(0, 0) == CommittedBits(kind));
            }

            const auto intent_path = IntentPath(config.data_dir, WalPath(config));
            assert(std::filesystem::exists(intent_path));
            const auto intent_before = ReadBytes(intent_path);
            assert(intent_before.size() == 16U);
            assert(intent_before[0] == 'C');
            assert(intent_before[1] == 'K');
            assert(intent_before[2] == 'R');
            assert(intent_before[3] == 'C');
            AssertReadOnlyBitsWithoutMutation(config, CommittedBits(kind));
            assert(ReadBytes(intent_path) == intent_before);
        }
    }
}

void ExpectReadOnlyLoadFailure(
    const chunkdb::StoreConfig& config,
    std::string_view expected_message,
    bool ignore_active_writer_metadata = false) {
    const auto before = SnapshotDirectory(
        config.data_dir, ignore_active_writer_metadata);
    bool threw = false;
    try {
        chunkdb::ChunkStore reader(ReadOnlyConfig(config));
        (void)reader.GetBlockBits(0, 0);
    } catch (const std::exception& error) {
        threw = true;
        if (std::string(error.what()).find(expected_message) == std::string::npos) {
            std::fprintf(
                stderr,
                "read-only load failed with %s (expected %.*s)\n",
                error.what(),
                static_cast<int>(expected_message.size()),
                expected_message.data());
        }
        assert(std::string(error.what()).find(expected_message) !=
               std::string::npos);
    }
    assert(threw);
    assert(SnapshotDirectory(
               config.data_dir, ignore_active_writer_metadata) == before);
}

void TestInvalidIntentAndWalStatesFailClosed() {
    {
        chunkdb::test::ScopedTempDir dir("chunkdb-read-only-malformed-intent");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        WriteBytes(IntentPath(config.data_dir, WalPath(config)), {'C', 'K', 'R'});
        ExpectReadOnlyLoadFailure(config, "malformed conditional intent");
    }

    {
        chunkdb::test::ScopedTempDir dir("chunkdb-read-only-missing-wal");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        const auto wal_path = WalPath(config);
        const auto boundary = ReadBytes(wal_path).size();
        WriteBytes(IntentPath(config.data_dir, wal_path), IntentBytes(false, boundary));
        assert(std::filesystem::remove(wal_path));
        ExpectReadOnlyLoadFailure(config, "missing the WAL required");
    }

    {
        chunkdb::test::ScopedTempDir dir("chunkdb-read-only-short-wal");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        const auto wal_path = WalPath(config);
        const auto boundary = ReadBytes(wal_path).size();
        WriteBytes(IntentPath(config.data_dir, wal_path), IntentBytes(false, boundary));
        std::filesystem::resize_file(wal_path, boundary - 1U);
        ExpectReadOnlyLoadFailure(config, "shorter than CKRB boundary");
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-read-only-corrupt-before-boundary");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        const auto wal_path = WalPath(config);
        auto wal = ReadBytes(wal_path);
        const auto boundary = wal.size();
        assert(boundary > chunkdb::kWalHeaderSize);
        wal.back() ^= 0x80U;
        WriteBytes(wal_path, wal);
        WriteBytes(IntentPath(config.data_dir, wal_path), IntentBytes(false, boundary));
        // Flipping the last byte hits the frame trailer CRC.
        ExpectReadOnlyLoadFailure(config, "frame_crc_mismatch");
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-read-only-corrupt-after-boundary");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        const auto wal_path = WalPath(config);
        auto wal = ReadBytes(wal_path);
        const auto boundary = wal.size();
        wal.insert(wal.end(), {'b', 'a', 'd', 't', 'a', 'i', 'l'});
        WriteBytes(wal_path, wal);
        WriteBytes(IntentPath(config.data_dir, wal_path), IntentBytes(false, boundary));
        AssertReadOnlyBitsWithoutMutation(config, "10101");
    }
}

void TestReadOnlyAtConditionalPublicationPhases() {
    const std::vector<chunkdb::ConditionalMutationPausePoint> phases = {
        chunkdb::ConditionalMutationPausePoint::
            kBeforeRollbackIntentPublish,
        chunkdb::ConditionalMutationPausePoint::
            kAfterRollbackIntentPublish,
        chunkdb::ConditionalMutationPausePoint::kAfterWalAppend,
        chunkdb::ConditionalMutationPausePoint::
            kAfterCommitIntentPublish,
        chunkdb::ConditionalMutationPausePoint::
            kAfterCommitIntentUnlink,
    };

    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const auto kind :
             {ConditionalKind::kCas, ConditionalKind::kBatch}) {
            for (const auto phase : phases) {
                chunkdb::test::ScopedTempDir dir(
                    "chunkdb-read-only-live-conditional");
                auto config = BaseConfig(dir.path(), layout);
                chunkdb::ChunkStore writer(config);
                writer.SetBlockBits(0, 0, "10101");
                writer.ArmConditionalMutationPauseForTests(phase);

                std::exception_ptr writer_error;
                std::thread mutation([&] {
                    try {
                        ApplyConditional(
                            kind,
                            &writer,
                            writer.GetChunkVersion(0, 0));
                    } catch (...) {
                        writer_error = std::current_exception();
                    }
                });
                assert(writer.WaitForConditionalMutationPauseForTests());

                ExpectReadOnlyLoadFailure(
                    config,
                    "unstable after 8 bounded attempts",
                    true);

                writer.ResumeConditionalMutationForTests();
                mutation.join();
                if (writer_error != nullptr) {
                    std::rethrow_exception(writer_error);
                }
                assert(writer.GetBlockBits(0, 0) == CommittedBits(kind));
                AssertReadOnlyBitsWithoutMutation(
                    config, CommittedBits(kind), true);
            }
        }
    }
}

void TestReadOnlyDuringCheckpointAndGcReplacement() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        {
            chunkdb::test::ScopedTempDir dir(
                "chunkdb-read-only-checkpoint-replace");
            auto config = BaseConfig(dir.path(), layout);
            config.durability_mode =
                chunkdb::DurabilityMode::kFsyncCheckpoint;
            config.checkpoint_update_interval = 2;
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
            writer.SetBlockBits(0, 0, "11111");
            writer.ArmCheckpointBeforeWalRemovalPauseForTests();

            std::exception_ptr writer_error;
            std::thread checkpoint([&] {
                try {
                    writer.SetBlockBits(0, 0, "01010");
                } catch (...) {
                    writer_error = std::current_exception();
                }
            });
            assert(writer.WaitForCheckpointBeforeWalRemovalForTests());
            ExpectReadOnlyLoadFailure(
                config,
                "unstable after 8 bounded attempts",
                true);
            writer.ResumeCheckpointBeforeWalRemovalForTests();
            checkpoint.join();
            if (writer_error != nullptr) {
                std::rethrow_exception(writer_error);
            }
            AssertReadOnlyBitsWithoutMutation(config, "01010", true);
        }

        {
            chunkdb::test::ScopedTempDir dir(
                "chunkdb-read-only-gc-replace");
            auto config = BaseConfig(dir.path(), layout);
            config.durability_mode =
                chunkdb::DurabilityMode::kFsyncCheckpoint;
            config.checkpoint_update_interval = 1;
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
            writer.ArmCheckpointBeforeWalRemovalPauseForTests();

            std::exception_ptr writer_error;
            std::thread gc([&] {
                try {
                    writer.UnsetBlock(0, 0);
                } catch (...) {
                    writer_error = std::current_exception();
                }
            });
            assert(writer.WaitForCheckpointBeforeWalRemovalForTests());
            ExpectReadOnlyLoadFailure(
                config,
                "unstable after 8 bounded attempts",
                true);
            writer.ResumeCheckpointBeforeWalRemovalForTests();
            gc.join();
            if (writer_error != nullptr) {
                std::rethrow_exception(writer_error);
            }
            AssertReadOnlyBitsWithoutMutation(config, "00000", true);
        }
    }
}

void TestExactTwoTransactionAbaSchedule() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const auto kind :
             {ConditionalKind::kCas, ConditionalKind::kBatch}) {
            for (const bool existing_prefix : {false, true}) {
                chunkdb::test::ScopedTempDir dir(
                    "chunkdb-read-only-two-transaction-aba");
                auto config = BaseConfig(dir.path(), layout);
                chunkdb::ChunkStore writer(config);
                if (existing_prefix) {
                    writer.SetBlockBits(0, 0, "10101");
                }
                writer.SetBlockBits(4, 0, "00110");

                const auto wal_path = WalPath(config);
                const bool w0_present =
                    std::filesystem::exists(wal_path);
                const auto w0 =
                    w0_present ? ReadBytes(wal_path)
                               : std::vector<std::uint8_t>{};
                const auto expected_version =
                    writer.GetChunkVersion(0, 0);

                chunkdb::ChunkStore reader(ReadOnlyConfig(config));
                reader.ArmReadOnlySnapshotPausesForTests({
                    {1, chunkdb::ReadOnlySnapshotArtifact::kWal},
                    {1, chunkdb::ReadOnlySnapshotArtifact::kIntent},
                    {2, chunkdb::ReadOnlySnapshotArtifact::kWal},
                    {2, chunkdb::ReadOnlySnapshotArtifact::kIntent},
                });

                std::exception_ptr first_writer_error;
                std::thread first_writer;
                std::vector<std::uint8_t> w1;
                {
                    ScopedEnv reject(
                        "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE",
                        "1");
                    writer.ArmConditionalMutationPauseForTests(
                        chunkdb::ConditionalMutationPausePoint::
                            kAfterWalAppend);
                    first_writer = std::thread([&] {
                        try {
                            ApplyConditional(
                                kind, &writer, expected_version);
                        } catch (...) {
                            first_writer_error =
                                std::current_exception();
                        }
                    });
                    assert(
                        writer.WaitForConditionalMutationPauseForTests());
                    w1 = ReadBytes(wal_path);
                    assert(w1.size() > w0.size());

                    std::string reader_bits;
                    std::exception_ptr reader_error;
                    std::thread reader_thread([&] {
                        try {
                            reader_bits =
                                reader.GetBlockBits(0, 0);
                        } catch (...) {
                            reader_error =
                                std::current_exception();
                        }
                    });

                    // Collect 1 has read the rejected W1 tail but not CKRB.
                    assert(
                        reader.WaitForReadOnlySnapshotPauseForTests());
                    writer.ResumeConditionalMutationForTests();
                    first_writer.join();
                    assert(first_writer_error != nullptr);
                    assert(!std::filesystem::exists(
                        IntentPath(config.data_dir, wal_path)));
                    assert(
                        std::filesystem::exists(wal_path) ==
                        w0_present);
                    if (w0_present) {
                        assert(ReadBytes(wal_path) == w0);
                    }

                    // Let collect 1 observe absent intent, then stop it before
                    // the generation recheck and the next collection.
                    reader.ResumeReadOnlySnapshotForTests();
                    assert(
                        reader.WaitForReadOnlySnapshotPauseForTests());

                    std::exception_ptr second_writer_error;
                    std::thread second_writer;
                    {
                        ScopedEnv reject_again(
                            "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE",
                            "1");
                        writer.ArmConditionalMutationPauseForTests(
                            chunkdb::ConditionalMutationPausePoint::
                                kAfterWalAppend);
                        second_writer = std::thread([&] {
                            try {
                                ApplyConditional(
                                    kind,
                                    &writer,
                                    expected_version);
                            } catch (...) {
                                second_writer_error =
                                    std::current_exception();
                            }
                        });
                        assert(
                            writer.WaitForConditionalMutationPauseForTests());
                        // Format v2 stamps each frame with its revision, so
                        // T2's frame differs from T1's only in the frame
                        // header (revision and header CRC); the state
                        // records are byte-identical, which is what the
                        // generation bracket must still tell apart.
                        {
                            const auto w2 = ReadBytes(wal_path);
                            assert(w2.size() == w1.size());
                            // The frame starts after W0, or after the WAL
                            // header when the WAL did not exist before T1.
                            const std::size_t frame_start =
                                w0_present ? w0.size() : chunkdb::kWalHeaderSize;
                            const std::size_t records_begin =
                                frame_start + chunkdb::kWalFrameHeaderSize;
                            assert(std::equal(
                                w1.begin(), w1.begin() + static_cast<std::ptrdiff_t>(w0.size()),
                                w2.begin()));
                            assert(std::equal(
                                w1.begin() + static_cast<std::ptrdiff_t>(records_begin), w1.end(),
                                w2.begin() + static_cast<std::ptrdiff_t>(records_begin)));
                        }

                        // Collect 2 reads T2's W1: same state records as T1.
                        reader.ResumeReadOnlySnapshotForTests();
                        assert(
                            reader.WaitForReadOnlySnapshotPauseForTests());
                        writer.ResumeConditionalMutationForTests();
                        second_writer.join();
                        assert(second_writer_error != nullptr);
                        assert(!std::filesystem::exists(
                            IntentPath(config.data_dir, wal_path)));
                        assert(
                            std::filesystem::exists(wal_path) ==
                            w0_present);
                        if (w0_present) {
                            assert(ReadBytes(wal_path) == w0);
                        }

                        // Collect 2 also observes absent intent. The two byte
                        // observations are ABA-identical, but their generation
                        // brackets differ and therefore cannot be accepted.
                        reader.ResumeReadOnlySnapshotForTests();
                        assert(
                            reader.WaitForReadOnlySnapshotPauseForTests());
                    }

                    const auto before_final_reader =
                        SnapshotDirectory(config.data_dir, true);
                    reader.ResumeReadOnlySnapshotForTests();
                    reader_thread.join();
                    if (reader_error != nullptr) {
                        std::rethrow_exception(reader_error);
                    }
                    assert(
                        reader_bits ==
                        (existing_prefix ? "10101" : "00000"));
                    assert(reader.GetBlockBits(4, 0) == "00110");
                    assert(
                        SnapshotDirectory(config.data_dir, true) ==
                        before_final_reader);
                }
            }
        }
    }
}

void TestCheckpointAndGcBetweenGenerationBrackets() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const bool gc : {false, true}) {
            chunkdb::test::ScopedTempDir dir(
                "chunkdb-read-only-generation-checkpoint");
            auto config = BaseConfig(dir.path(), layout);
            config.durability_mode =
                chunkdb::DurabilityMode::kFsyncCheckpoint;
            config.checkpoint_update_interval = 1;
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");

            chunkdb::ChunkStore reader(ReadOnlyConfig(config));
            reader.ArmReadOnlySnapshotPausesForTests({
                {1, chunkdb::ReadOnlySnapshotArtifact::kIntent},
            });
            std::string reader_bits;
            std::exception_ptr reader_error;
            std::thread reader_thread([&] {
                try {
                    reader_bits = reader.GetBlockBits(0, 0);
                } catch (...) {
                    reader_error = std::current_exception();
                }
            });
            assert(reader.WaitForReadOnlySnapshotPauseForTests());

            if (gc) {
                writer.UnsetBlock(0, 0);
            } else {
                writer.SetBlockBits(0, 0, "01010");
            }

            reader.ResumeReadOnlySnapshotForTests();
            reader_thread.join();
            if (reader_error != nullptr) {
                std::rethrow_exception(reader_error);
            }
            assert(reader_bits == (gc ? "00000" : "01010"));
        }
    }
}

void TestTwoIdenticalCommittedTransactions() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const auto kind :
             {ConditionalKind::kCas, ConditionalKind::kBatch}) {
            chunkdb::test::ScopedTempDir dir(
                "chunkdb-read-only-identical-commits");
            auto config = BaseConfig(dir.path(), layout);
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");

            ApplyConditional(
                kind, &writer, writer.GetChunkVersion(0, 0));
            const auto first_wal = ReadBytes(WalPath(config));
            const std::size_t state_bytes =
                writer.geometry().ChunkPayloadBytes() +
                (writer.geometry().ChunkBlockCount() + 7U) / 8U;
            // The full-state frame: a payload record and a presence record.
            const std::size_t record_bytes =
                chunkdb::kWalFrameHeaderSize + 2U * chunkdb::kWalFrameRecordOverhead +
                state_bytes + chunkdb::kWalFrameTrailerSize;
            assert(first_wal.size() >= record_bytes);
            const std::vector<std::uint8_t> first_record(
                first_wal.end() -
                    static_cast<std::ptrdiff_t>(record_bytes),
                first_wal.end());

            writer.SetBlockBits(0, 0, "10101");
            ApplyConditional(
                kind, &writer, writer.GetChunkVersion(0, 0));
            const auto second_wal = ReadBytes(WalPath(config));
            assert(second_wal.size() >= record_bytes);
            const std::vector<std::uint8_t> second_record(
                second_wal.end() -
                    static_cast<std::ptrdiff_t>(record_bytes),
                second_wal.end());
            // Identical state bytes: every record (and the frame CRC over
            // them) matches, so the commits are byte-identical except for the
            // revision, which format v2 makes distinct by construction.
            const auto records_of = [](const std::vector<std::uint8_t>& frame) {
                return std::vector<std::uint8_t>(
                    frame.begin() + static_cast<std::ptrdiff_t>(chunkdb::kWalFrameHeaderSize),
                    frame.end());
            };
            assert(records_of(first_record) == records_of(second_record));
            const auto revision_of = [](const std::vector<std::uint8_t>& frame) {
                return std::vector<std::uint8_t>(frame.begin() + 4, frame.begin() + 12);
            };
            assert(revision_of(first_record) != revision_of(second_record));
            AssertReadOnlyBitsWithoutMutation(
                config, CommittedBits(kind), true);
        }
    }
}

void TestSnapshotGenerationFailuresFailClosed() {
    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-read-only-legacy-generation-zero");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        assert(std::filesystem::remove(
            config.data_dir / "chunkdb.snapshot"));
        AssertReadOnlyBitsWithoutMutation(config, "10101");
        assert(!std::filesystem::exists(
            config.data_dir / "chunkdb.snapshot"));
        {
            chunkdb::ChunkStore migrated(config);
            assert(migrated.GetBlockBits(0, 0) == "10101");
        }
        assert(std::filesystem::exists(
            config.data_dir / "chunkdb.snapshot"));
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-read-only-malformed-generation");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        WriteBytes(
            config.data_dir / "chunkdb.snapshot",
            {'C', 'K', 'S', 'G'});
        ExpectReadOnlyLoadFailure(
            config, "snapshot generation metadata is malformed");
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-snapshot-generation-begin-failure");
        auto config = BaseConfig(dir.path());
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "10101");
        {
            ScopedEnv fail_begin(
                "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_BEGIN_FAIL_ONCE",
                "1");
            bool threw = false;
            try {
                ApplyConditional(
                    ConditionalKind::kCas,
                    &writer,
                    writer.GetChunkVersion(0, 0));
            } catch (const std::exception&) {
                threw = true;
            }
            assert(threw);
        }
        assert(writer.GetBlockBits(0, 0) == "10101");
        AssertReadOnlyBitsWithoutMutation(config, "10101", true);
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-snapshot-generation-end-failure");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
            ScopedEnv fail_end(
                "CHUNKDB_FAILPOINT_SNAPSHOT_GENERATION_END_FAIL_ONCE",
                "1");
            // The even-generation republication fails only after the
            // mutation is committed, so the command succeeds; the epoch
            // stays odd and readers fail closed until writer restart.
            ApplyConditional(
                ConditionalKind::kBatch,
                &writer,
                writer.GetChunkVersion(0, 0));
            assert(writer.GetBlockBits(0, 0) == "01010");
            ExpectReadOnlyLoadFailure(
                config,
                "unstable after 8 bounded attempts",
                true);
        }
        {
            chunkdb::ChunkStore recovered(config);
            assert(recovered.GetBlockBits(0, 0) == "01010");
        }
    }

    {
        chunkdb::test::ScopedTempDir dir(
            "chunkdb-snapshot-generation-wrap");
        auto config = BaseConfig(dir.path());
        {
            chunkdb::ChunkStore writer(config);
            writer.SetBlockBits(0, 0, "10101");
        }
        WriteBytes(
            config.data_dir / "chunkdb.snapshot",
            SnapshotGenerationBytes(
                std::numeric_limits<std::uint64_t>::max() - 1U));
        bool threw = false;
        try {
            chunkdb::ChunkStore writer(config);
        } catch (const std::exception& error) {
            threw =
                std::string(error.what()).find(
                    "snapshot generation exhausted") !=
                std::string::npos;
        }
        assert(threw);
    }
}

void TestBoundedRetryExhaustionLeavesOtherChunksUsable() {
    chunkdb::test::ScopedTempDir dir("chunkdb-read-only-retry-exhaustion");
    auto config = BaseConfig(dir.path());
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "10101");
        writer.SetBlockBits(4, 0, "01010");
    }

    chunkdb::ChunkStore reader(ReadOnlyConfig(config));
    {
        ScopedEnv unstable(
            "CHUNKDB_FAILPOINT_READ_ONLY_SNAPSHOT_RETRY_EXHAUST", "0,0");
        bool threw = false;
        try {
            (void)reader.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            threw = true;
            assert(std::string(error.what()).find(
                       "unstable after 8 bounded attempts") !=
                   std::string::npos);
        }
        assert(threw);
        assert(reader.GetBlockBits(4, 0) == "01010");
    }
    assert(reader.GetBlockBits(0, 0) == "10101");
}

}  // namespace

int main() {
    TestPersistedCkrbRejectedMutation();
    TestRetainedCkrcIsCommittedAndNonMutating();
    TestInvalidIntentAndWalStatesFailClosed();
    TestReadOnlyAtConditionalPublicationPhases();
    TestReadOnlyDuringCheckpointAndGcReplacement();
    TestExactTwoTransactionAbaSchedule();
    TestCheckpointAndGcBetweenGenerationBrackets();
    TestTwoIdenticalCommittedTransactions();
    TestSnapshotGenerationFailuresFailClosed();
    TestBoundedRetryExhaustionLeavesOtherChunksUsable();
    return 0;
}
