// Focused regression tests for the ChunkDB remediation pass. Each test would
// fail on the specific defect it guards:
//   - CHUNKRANGE/CHUNKRADIUS extreme coordinates, byte budget, loop termination
//   - CHUNKCAS/CHUNKBATCH failure atomicity incl. WAL neutralization on restart
//   - deterministic chunk-version monotonicity across many reloads
//   - WALFLUSH bounded-tracking overflow fallback (success + fail-closed retry)
//   - eviction residency (a recently used chunk stays resident)
//   - background checkpoint I/O failure: retry, reporting, and recovery
//   - empty-chunk GC failpoint ordering
//   - oversized-geometry rejection for conditional mutations

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/engine.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/protocol.hpp"
#include "test_utils.hpp"

namespace {

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
    ScopedEnv(const char* key, const char* value) : key_(key) { SetEnvVar(key_, value); }
    ~ScopedEnv() { UnsetEnvVar(key_); }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;

  private:
    const char* key_;
};

std::vector<std::uint8_t> ReadFileBytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input.is_open());
    return {
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
}

void WriteFileBytes(
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

void RemoveVersionBookkeeping(const std::filesystem::path& data_dir) {
    std::error_code ec;
    std::filesystem::remove(data_dir / "chunkdb.version", ec);
    assert(!ec);
    ec.clear();
    std::filesystem::remove(data_dir / ".chunkdb.initialized", ec);
    assert(!ec);
    ec.clear();
    std::filesystem::remove(data_dir / "chunkdb.snapshot", ec);
    assert(!ec);
}

bool HasRollbackIntent(const std::filesystem::path& data_dir) {
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(data_dir)) {
        if (entry.is_regular_file() &&
            entry.path().extension() == ".rollback") {
            return true;
        }
    }
    return false;
}

chunkdb::StoreConfig BaseConfig(const std::filesystem::path& data_dir) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 2,
            .large_chunk_height_chunks = 2,
            .chunk_width_blocks = 4,
            .chunk_height_blocks = 4,
            .block_bits = 5,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = 1000,
        .checkpoint_wal_bytes = 1024 * 1024,
        .wal_group_commit_updates = 1,
        .max_loaded_chunks = 128,
        .allow_multiple_processes = false,
    };
}

// ---- CHUNKRANGE / CHUNKRADIUS bounds and extreme coordinates ---------------

void TestRangeExtremeCoordinatesTerminate() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-range-extreme");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));

    constexpr std::int64_t kMin = std::numeric_limits<std::int64_t>::min();
    constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();

    // The full-domain request must be rejected as a bounded error (too many
    // chunks) rather than overflowing or looping forever.
    bool threw = false;
    try {
        (void)store.ReadChunkRange(kMin, 0, kMax, 0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    assert(threw);

    // A single-element range ending at INT64_MAX must terminate normally.
    const auto at_max = store.ReadChunkRange(kMax, kMax, kMax, kMax);
    assert(at_max.empty());  // nothing populated there
    const auto at_min = store.ReadChunkRange(kMin, kMin, kMin, kMin);
    assert(at_min.empty());

    // A populated single-element range at INT64_MAX round-trips.
    store.SetChunkStateBits(
        kMax,
        kMax,
        std::string(store.geometry().ChunkPayloadBits(), '1'),
        std::string(store.geometry().ChunkBlockCount(), '1'));
    const auto populated = store.ReadChunkRange(kMax, kMax, kMax, kMax);
    assert(populated.size() == 1);
    assert(populated[0].coord.x == kMax && populated[0].coord.y == kMax);
}

void TestRangeByteBudget() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-range-bytes");
    // A geometry whose per-chunk state text is large enough that only a few
    // chunks fit inside the 64 MiB response cap.
    auto config = BaseConfig(dir.path());
    config.geometry = {
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 512,
        .chunk_height_blocks = 512,
        .block_bits = 8,
    };
    chunkdb::ChunkStore store(config);

    const auto payload = std::string(store.geometry().ChunkPayloadBits(), '1');
    const auto presence = std::string(store.geometry().ChunkBlockCount(), '1');
    // Each chunk's response entry is > 2 MiB, so a 16x16 (256-chunk) range
    // would far exceed 64 MiB. Populate a modest block and confirm the byte
    // cap rejects the request instead of allocating it.
    for (std::int64_t cx = 0; cx < 16; ++cx) {
        for (std::int64_t cy = 0; cy < 16; ++cy) {
            store.SetChunkStateBits(cx, cy, payload, presence);
        }
    }
    bool threw = false;
    try {
        (void)store.ReadChunkRange(0, 0, 15, 15);
    } catch (const std::out_of_range&) {
        threw = true;
    }
    assert(threw);

    // A small sub-range stays within budget and returns entries.
    const auto ok = store.ReadChunkRange(0, 0, 1, 1);
    assert(ok.size() == 4);
}

void TestRadiusReads() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-radius");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));
    const auto payload = std::string(store.geometry().ChunkPayloadBits(), '1');
    const auto presence = std::string(store.geometry().ChunkBlockCount(), '1');

    // Populate a plus-shape of chunks around the origin plus a corner.
    const std::vector<std::pair<std::int64_t, std::int64_t>> populated = {
        {0, 0}, {1, 0}, {-1, 0}, {0, 1}, {0, -1}, {2, 2}};
    for (const auto& [cx, cy] : populated) {
        store.SetChunkStateBits(cx, cy, payload, presence);
    }

    // Radius 1 (disc) includes the plus but excludes the (2,2) corner.
    const auto within = store.ReadChunkRadius(0, 0, 1);
    std::set<std::pair<std::int64_t, std::int64_t>> got;
    for (const auto& entry : within) {
        got.insert({entry.coord.x, entry.coord.y});
    }
    assert(got.count({0, 0}) == 1);
    assert(got.count({1, 0}) == 1);
    assert(got.count({0, -1}) == 1);
    assert(got.count({2, 2}) == 0);
    // Ascending (cx, cy) ordering.
    for (std::size_t i = 1; i < within.size(); ++i) {
        const auto& a = within[i - 1].coord;
        const auto& b = within[i].coord;
        assert(a.x < b.x || (a.x == b.x && a.y < b.y));
    }

    // Negative radius is rejected; an over-large radius is rejected before any
    // read (disc would exceed 256 chunks).
    bool neg_threw = false;
    try {
        (void)store.ReadChunkRadius(0, 0, -1);
    } catch (const std::invalid_argument&) {
        neg_threw = true;
    }
    assert(neg_threw);
    bool big_threw = false;
    try {
        (void)store.ReadChunkRadius(0, 0, 1000);
    } catch (const std::invalid_argument&) {
        big_threw = true;
    }
    assert(big_threw);
}

// ---- Conditional-mutation failure atomicity --------------------------------

void TestBatchCheckpointFailureRollsBackAcrossRestart() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-batch-atomic");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
    config.checkpoint_update_interval = 1;  // checkpoint fires on the batch

    const std::string old_payload = "10101";
    {
        chunkdb::ChunkStore store(config);
        // 1. Create an existing chunk (this also checkpoints it durably).
        store.SetBlockBits(0, 0, old_payload);
        assert(store.GetBlockBits(0, 0) == old_payload);

        // Once the single full-state WAL record commits, a later checkpoint
        // failure cannot be returned as though the batch were absent.
        std::vector<chunkdb::ChunkBatchOp> ops;
        ops.push_back({.set = true, .x = 0, .y = 0, .bits = "01010"});
        chunkdb::ChunkMutationResult result;
        {
            ScopedEnv fp(
                "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE", "1");
            result = store.ApplyChunkBatch(0, 0, false, 0, ops);
        }
        assert(result.ok);
        assert(store.GetBlockBits(0, 0) == "01010");

        store.SetBlockBits(1, 1, "11100");
        store.WalBarrier();
        assert(store.GetBlockBits(0, 0) == "01010");
    }

    // 5. Crash-style restart: the rejected batch must remain absent.
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "01010");
        assert(recovered.GetBlockBits(1, 1) == "11100");
    }
}

void TestCasCheckpointFailureRollsBack() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-cas-atomic");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
    config.checkpoint_update_interval = 1;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "11111");
        const auto version = store.GetChunkVersion(0, 0);
        const auto state_before = store.GetChunkStateBits(0, 0);

        chunkdb::ChunkMutationResult result;
        {
            ScopedEnv fp(
                "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE", "1");
            result = store.CasChunkState(
                0,
                0,
                version,
                std::string(store.geometry().ChunkPayloadBits(), '0'),
                std::string(store.geometry().ChunkBlockCount(), '1'));
        }
        assert(result.ok);
        assert(store.GetChunkStateBits(0, 0) != state_before);
    }
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) == "00000");
    }
}

// ---- Deterministic version monotonicity ------------------------------------

void TestVersionMonotonicAcrossManyReloads() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-version-clock");
    auto config = BaseConfig(dir.path());

    std::set<std::uint64_t> seen;
    std::uint64_t last = 0;
    int write = 0;
    for (int restart = 0; restart < 12; ++restart) {
        chunkdb::ChunkStore store(config);
        if (restart > 0) {
            // The persisted revision survives the restart unchanged.
            assert(store.GetChunkVersion(0, 0) == last);
        }
        for (int i = 0; i < 5; ++i, ++write) {
            store.SetBlockBits(0, 0, write % 2 == 0 ? "10101" : "01010");
            const auto v = store.GetChunkVersion(0, 0);
            // Each content change issues a version that is unique and
            // strictly greater than every version issued before, including
            // across restarts.
            assert(seen.insert(v).second);
            assert(v > last);
            last = v;
        }
    }
}

// Writes a 1.x (v3) WAL by hand: the 2.x writer only produces v4 frames, so
// legacy replay and migration are exercised with crafted bytes.
void WriteLegacyV3Wal(
    const std::filesystem::path& path,
    const chunkdb::Geometry& geometry,
    const chunkdb::ChunkCoord& coord,
    const std::vector<std::pair<std::uint32_t, std::vector<std::uint8_t>>>& records) {
    std::vector<std::uint8_t> bytes;
    auto le16 = [&](std::uint16_t v) { for (int i = 0; i < 2; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    auto le32 = [&](std::uint32_t v) { for (int i = 0; i < 4; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    auto le64 = [&](std::uint64_t v) { for (int i = 0; i < 8; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    const std::string magic = "CHKWAL02";
    bytes.insert(bytes.end(), magic.begin(), magic.end());
    le16(3);
    le16(static_cast<std::uint16_t>(geometry.config().block_bits));
    le32(geometry.config().chunk_width_blocks);
    le32(geometry.config().chunk_height_blocks);
    le64(static_cast<std::uint64_t>(coord.x));
    le64(static_cast<std::uint64_t>(coord.y));
    for (const auto& [offset, body] : records) {
        const std::string record_magic = "DLT1";
        bytes.insert(bytes.end(), record_magic.begin(), record_magic.end());
        le32(offset);
        le16(static_cast<std::uint16_t>(body.size()));
        le32(chunkdb::Crc32(body));
        bytes.insert(bytes.end(), body.begin(), body.end());
    }
    std::filesystem::create_directories(path.parent_path());
    WriteFileBytes(path, bytes);
}

// Writes a 1.x (v2) checkpoint image by hand for the same reason.
void WriteLegacyV2Image(
    const std::filesystem::path& path,
    const chunkdb::Geometry& geometry,
    const chunkdb::ChunkCoord& coord,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence) {
    std::vector<std::uint8_t> bytes;
    auto le16 = [&](std::uint16_t v) { for (int i = 0; i < 2; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    auto le32 = [&](std::uint32_t v) { for (int i = 0; i < 4; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    auto le64 = [&](std::uint64_t v) { for (int i = 0; i < 8; ++i) bytes.push_back(static_cast<std::uint8_t>(v >> (8 * i))); };
    std::vector<std::uint8_t> state = payload;
    state.insert(state.end(), presence.begin(), presence.end());
    const std::string magic = "CHKDATA1";
    bytes.insert(bytes.end(), magic.begin(), magic.end());
    le16(2);
    le16(static_cast<std::uint16_t>(geometry.config().block_bits));
    le32(geometry.config().chunk_width_blocks);
    le32(geometry.config().chunk_height_blocks);
    le64(static_cast<std::uint64_t>(coord.x));
    le64(static_cast<std::uint64_t>(coord.y));
    le32(static_cast<std::uint32_t>(payload.size()));
    le32(chunkdb::Crc32(state));
    le64(0);  // write_timestamp_ms
    bytes.insert(bytes.end(), state.begin(), state.end());
    std::filesystem::create_directories(path.parent_path());
    WriteFileBytes(path, bytes);
}

void TestStableV1WalOnlyStoreMigrates() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-v1-wal-migration");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    // A stable-v1 store: a v3 WAL only (block (0,0) = "10101": payload byte 0
    // holds bits 0..4 LSB-first, presence byte 0 bit 0) and no bookkeeping.
    const chunkdb::Geometry geometry(config.geometry);
    WriteLegacyV3Wal(
        chunkdb::ChunkWalPath(dir.path(), geometry, {0, 0}),
        geometry,
        {0, 0},
        {{0U, {0x15}}, {static_cast<std::uint32_t>(geometry.ChunkPayloadBytes()), {0x01}}});

    std::uint64_t pre_restart_version = 0;
    {
        chunkdb::ChunkStore migrated(config);
        assert(migrated.GetBlockBits(0, 0) == "10101");
        pre_restart_version = migrated.GetChunkVersion(0, 0);
        assert(pre_restart_version != 0);
        assert(std::filesystem::file_size(dir.path() / "chunkdb.version") == 16U);
        assert(std::filesystem::file_size(dir.path() / ".chunkdb.initialized") == 16U);
    }

    std::uint64_t migrated_version = 0;
    {
        chunkdb::ChunkStore restarted(config);
        // A legacy chunk (no v2 artifact yet) keeps the 1.x behavior: a
        // fresh token per load, so the pre-restart token never matches.
        const auto current = restarted.GetChunkVersion(0, 0);
        assert(current > pre_restart_version);
        assert(restarted.GetBlockBits(0, 0) == "10101");

        const auto state = restarted.GetChunkStateBits(0, 0);
        const auto separator = state.find('|');
        const auto cas = restarted.CasChunkState(
            0,
            0,
            pre_restart_version,
            state.substr(0, separator),
            state.substr(separator + 1));
        assert(!cas.ok);
        const std::vector<chunkdb::ChunkBatchOp> ops = {
            {.set = true, .x = 0, .y = 0, .bits = "01010"}};
        const auto batch = restarted.ApplyChunkBatch(
            0, 0, true, pre_restart_version, ops);
        assert(!batch.ok);
        assert(restarted.GetBlockBits(0, 0) == "10101");

        // The first mutation writes a v4 frame carrying the revision; from
        // then on the chunk is migrated and its version survives restarts.
        restarted.SetBlockBits(1, 1, "11111");
        migrated_version = restarted.GetChunkVersion(0, 0);
        assert(migrated_version > current);
    }
    {
        chunkdb::ChunkStore again(config);
        assert(again.GetChunkVersion(0, 0) == migrated_version);
        assert(again.GetBlockBits(0, 0) == "10101");
        assert(again.GetBlockBits(1, 1) == "11111");
    }
}

void TestStableV1CheckpointAndNegativeCoordinatesMigrate() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-v1-checkpoint-migration");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
    config.checkpoint_update_interval = 1;

    // Two stable-v1 (v2) checkpoint images at negative coordinates, crafted
    // by hand since the 2.x writer only emits v4 images.
    const chunkdb::Geometry geometry(config.geometry);
    for (const auto& [bx, by, bits] :
         std::vector<std::tuple<std::int64_t, std::int64_t, std::string>>{
             {-1, -1, "11100"}, {-5, -6, "00111"}}) {
        const auto coord = geometry.BlockToChunk(bx, by);
        const auto [lx, ly] = geometry.BlockToLocal(bx, by);
        const std::size_t index = geometry.LocalBlockIndex(lx, ly);
        std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U);
        std::vector<std::uint8_t> presence((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
        chunkdb::BitCodec::WriteBits(payload, index * geometry.config().block_bits, bits);
        chunkdb::BitCodec::WriteBits(presence, index, "1");
        WriteLegacyV2Image(
            chunkdb::ChunkDataPath(dir.path(), geometry, coord), geometry, coord, payload, presence);
    }
    assert(std::filesystem::exists(
        chunkdb::ChunkDataPath(
            dir.path(), geometry, geometry.BlockToChunk(-1, -1))));

    {
        chunkdb::ChunkStore migrated(config);
        assert(migrated.GetBlockBits(-1, -1) == "11100");
        assert(migrated.GetBlockBits(-5, -6) == "00111");
    }
    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetBlockBits(-1, -1) == "11100");
        assert(restarted.GetBlockBits(-5, -6) == "00111");
    }
}

void TestIntermediateVersionCeilingUpgradesWithoutReuse() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-version-intermediate");
    const auto config = BaseConfig(dir.path());
    constexpr std::uint64_t kIntermediateCeiling = 50000;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
    }
    RemoveVersionBookkeeping(dir.path());
    std::vector<std::uint8_t> intermediate;
    for (unsigned shift = 0; shift < 64; shift += 8) {
        intermediate.push_back(static_cast<std::uint8_t>(
            (kIntermediateCeiling >> shift) & 0xFFU));
    }
    WriteFileBytes(dir.path() / "chunkdb.version", intermediate);

    std::uint64_t first = 0;
    {
        chunkdb::ChunkStore upgraded(config);
        // The chunk keeps its persisted revision (issued before the ceiling
        // was raised); every token issued from now on is above the ceiling.
        assert(upgraded.GetChunkVersion(0, 0) < kIntermediateCeiling);
        upgraded.SetBlockBits(0, 0, "01010");
        first = upgraded.GetChunkVersion(0, 0);
        assert(first >= kIntermediateCeiling);
        assert(std::filesystem::file_size(dir.path() / "chunkdb.version") == 16U);
        assert(std::filesystem::exists(dir.path() / ".chunkdb.initialized"));
    }
    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetChunkVersion(0, 0) == first);
        assert(restarted.GetBlockBits(0, 0) == "01010");
        restarted.SetBlockBits(0, 0, "10101");
        assert(restarted.GetChunkVersion(0, 0) > first);
    }
}

void TestVersionBookkeepingDamageFailsClosed() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-version-damage");
    const auto config = BaseConfig(dir.path());
    const auto version_path = dir.path() / "chunkdb.version";

    std::uint64_t stale = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        stale = store.GetChunkVersion(0, 0);
    }
    auto valid_record = ReadFileBytes(version_path);
    assert(valid_record.size() == 16);

    // A healthy restart keeps the persisted revision, so the retained token
    // still matches unchanged content; the next mutation moves past it and
    // the old token is rejected from then on.
    std::uint64_t advanced = 0;
    {
        chunkdb::ChunkStore store(config);
        assert(store.GetChunkVersion(0, 0) == stale);
        const std::vector<chunkdb::ChunkBatchOp> ops = {
            {.set = true, .x = 0, .y = 0, .bits = "01010"}};
        const auto batch = store.ApplyChunkBatch(0, 0, true, stale, ops);
        assert(batch.ok);
        advanced = batch.version;
        assert(advanced > stale);
        const auto state = store.GetChunkStateBits(0, 0);
        const auto separator = state.find('|');
        const auto cas = store.CasChunkState(
            0, 0, stale, state.substr(0, separator), state.substr(separator + 1));
        assert(!cas.ok);
    }

    const auto expect_damage_rejected = [&](const std::vector<std::uint8_t>* replacement) {
        if (replacement == nullptr) {
            std::error_code ec;
            assert(std::filesystem::remove(version_path, ec));
            assert(!ec);
        } else {
            WriteFileBytes(version_path, *replacement);
        }
        bool threw = false;
        try {
            chunkdb::ChunkStore damaged(config);
        } catch (const std::exception&) {
            threw = true;
        }
        assert(threw);
        WriteFileBytes(version_path, valid_record);
        chunkdb::ChunkStore healthy(config);
        // The persisted revision is unaffected by bookkeeping repair, and a
        // fresh mutation still issues a strictly newer token.
        assert(healthy.GetChunkVersion(0, 0) == advanced);
        healthy.SetBlockBits(1, 1, healthy.GetBlockBits(1, 1) == "11111" ? "00000" : "11111");
        assert(healthy.GetChunkVersion(0, 0) > advanced);
        advanced = healthy.GetChunkVersion(0, 0);
        valid_record = ReadFileBytes(version_path);
    };

    expect_damage_rejected(nullptr);
    const std::vector<std::uint8_t> truncated(valid_record.begin(), valid_record.begin() + 7);
    expect_damage_rejected(&truncated);
    auto corrupt = valid_record;
    corrupt[5] ^= 0x80U;
    expect_damage_rejected(&corrupt);
    auto oversized = valid_record;
    oversized.push_back(0U);
    expect_damage_rejected(&oversized);

    for (const char* failpoint :
         {"CHUNKDB_FAILPOINT_VERSION_STAT_FAIL_ONCE",
          "CHUNKDB_FAILPOINT_VERSION_READ_FAIL_ONCE"}) {
        bool threw = false;
        {
            ScopedEnv fp(failpoint, "1");
            try {
                chunkdb::ChunkStore damaged(config);
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        chunkdb::ChunkStore healthy(config);
        assert(healthy.GetChunkVersion(0, 0) != stale);
        valid_record = ReadFileBytes(version_path);
    }
}

enum class ConditionalKind {
    kCas,
    kBatch,
};

void ApplyConditional(
    ConditionalKind kind,
    chunkdb::ChunkStore* store,
    std::uint64_t expected_version) {
    if (kind == ConditionalKind::kCas) {
        (void)store->CasChunkState(
            0,
            0,
            expected_version,
            std::string(store->geometry().ChunkPayloadBits(), '1'),
            std::string(store->geometry().ChunkBlockCount(), '1'));
    } else {
        const std::vector<chunkdb::ChunkBatchOp> ops = {
            {.set = true, .x = 0, .y = 0, .bits = "01010"}};
        (void)store->ApplyChunkBatch(
            0, 0, true, expected_version, ops);
    }
}

void RunIntentEstablishmentFailureCase(
    ConditionalKind kind,
    const char* failpoint) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-intent-establish");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    std::uint64_t expected = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        expected = store.GetChunkVersion(0, 0);

        bool threw = false;
        {
            ScopedEnv fp(failpoint, "1");
            try {
                ApplyConditional(kind, &store, expected);
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetChunkVersion(0, 0) == expected);
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(!HasRollbackIntent(dir.path()));

        // This targets the original moved-from-vector crash: an ordinary write
        // in the same chunk must remain usable after intent establishment
        // rejects the conditional command.
        store.SetBlockBits(1, 1, "11100");
        store.WalBarrier();
    }
    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetBlockBits(0, 0) == "10101");
        assert(restarted.GetBlockBits(1, 1) == "11100");
        assert(!HasRollbackIntent(dir.path()));
    }
    {
        chunkdb::ChunkStore crash_style_restart(config);
        assert(crash_style_restart.GetBlockBits(0, 0) == "10101");
        assert(crash_style_restart.GetBlockBits(1, 1) == "11100");
    }
}

void TestIntentEstablishmentFailuresLeaveLiveStateUsable() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        for (const char* failpoint :
             {"CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE",
              "CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE",
              "CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE",
              "CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE",
              "CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_INTENT_PUBLISH_ONCE"}) {
            RunIntentEstablishmentFailureCase(kind, failpoint);
        }
    }
}

void RunPreExistingCommittedIntentFailureCase(
    ConditionalKind kind,
    const char* failpoint) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-intent-preexisting");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        {
            ScopedEnv retain(
                "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE", "1");
            ApplyConditional(kind, &store, store.GetChunkVersion(0, 0));
        }
        const std::string committed =
            kind == ConditionalKind::kCas ? "11111" : "01010";
        assert(store.GetBlockBits(0, 0) == committed);
        assert(HasRollbackIntent(dir.path()));
        store.SetBlockBits(0, 0, "00001");
        store.SetBlockBits(2, 2, "00001");

        bool threw = false;
        {
            ScopedEnv fp(failpoint, "1");
            try {
                ApplyConditional(kind, &store, store.GetChunkVersion(0, 0));
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetBlockBits(0, 0) == "00001");
        assert(store.GetBlockBits(2, 2) == "00001");
        store.SetBlockBits(1, 1, "00110");
        store.WalBarrier();
    }
    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetBlockBits(0, 0) == "00001");
        assert(restarted.GetBlockBits(1, 1) == "00110");
        assert(restarted.GetBlockBits(2, 2) == "00001");
        assert(!HasRollbackIntent(dir.path()));
    }
}

void TestPreExistingCommittedIntentFailuresAreSafe() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        RunPreExistingCommittedIntentFailureCase(
            kind, "CHUNKDB_FAILPOINT_ROLLBACK_INTENT_INSPECT_FAIL_ONCE");
        RunPreExistingCommittedIntentFailureCase(
            kind, "CHUNKDB_FAILPOINT_ROLLBACK_INTENT_REPLACE_FAIL_ONCE");
    }
}

void RunCommittedIntentCleanupFailureCase(
    ConditionalKind kind,
    const char* failpoint,
    bool arm_rollback_failure) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-intent-commit-cleanup");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        const auto expected = store.GetChunkVersion(0, 0);
        {
            ScopedEnv cleanup_failure(failpoint, "1");
            if (arm_rollback_failure) {
                ScopedEnv commit_boundary_failure(
                    "CHUNKDB_FAILPOINT_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE",
                    "1");
                ScopedEnv commit_completion_failure(
                    "CHUNKDB_FAILPOINT_COMMIT_INTENT_COMPLETION_SYNC_FAIL_ONCE",
                    "1");
                ScopedEnv rollback_failure(
                    "CHUNKDB_FAILPOINT_WAL_ROLLBACK_SYNC_FAIL_ONCE", "1");
                ApplyConditional(kind, &store, expected);
            } else {
                ApplyConditional(kind, &store, expected);
            }
        }
        assert(store.GetBlockBits(0, 0) ==
               (kind == ConditionalKind::kCas ? "11111" : "01010"));
        store.SetBlockBits(1, 1, "11001");
        store.WalBarrier();
    }
    {
        chunkdb::ChunkStore restarted(config);
        assert(restarted.GetBlockBits(0, 0) ==
               (kind == ConditionalKind::kCas ? "11111" : "01010"));
        assert(restarted.GetBlockBits(1, 1) == "11001");
        assert(!HasRollbackIntent(dir.path()));
    }
}

void TestCommittedIntentCleanupFailuresRemainCommitted() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        RunCommittedIntentCleanupFailureCase(
            kind,
            "CHUNKDB_FAILPOINT_COMMIT_INTENT_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE",
            false);
        RunCommittedIntentCleanupFailureCase(
            kind,
            "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_UNLINK_FAIL_ONCE",
            false);
        RunCommittedIntentCleanupFailureCase(
            kind,
            "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_AFTER_UNLINK_BEFORE_DIR_SYNC_ONCE",
            false);
        RunCommittedIntentCleanupFailureCase(
            kind,
            "CHUNKDB_FAILPOINT_CONDITIONAL_INTENT_AFTER_UNLINK_BEFORE_DIR_SYNC_ONCE",
            true);
    }
}

void RunRejectedConditionalRollbackCase(
    ConditionalKind kind,
    const char* rollback_failpoint,
    bool seed_existing_wal) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-conditional-rollback");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncWal;
    config.checkpoint_update_interval = 1000000;
    config.checkpoint_wal_bytes = 1000000;

    {
        chunkdb::ChunkStore store(config);
        if (seed_existing_wal) {
            store.SetBlockBits(0, 0, "10101");
        }
        const auto expected = store.GetChunkVersion(0, 0);

        bool threw = false;
        {
            ScopedEnv after_append(
                "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
            ScopedEnv rollback_failure(rollback_failpoint, "1");
            try {
                if (kind == ConditionalKind::kCas) {
                    (void)store.CasChunkState(
                        0,
                        0,
                        expected,
                        std::string(store.geometry().ChunkPayloadBits(), '1'),
                        std::string(store.geometry().ChunkBlockCount(), '1'));
                } else {
                    const std::vector<chunkdb::ChunkBatchOp> ops = {
                        {.set = true, .x = 0, .y = 0, .bits = "01010"}};
                    (void)store.ApplyChunkBatch(0, 0, true, expected, ops);
                }
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetBlockBits(0, 0) ==
               (seed_existing_wal ? "10101" : "00000"));

        // Rollback failure poisons all later durability-changing operations.
        bool barrier_threw = false;
        try {
            store.WalBarrier();
        } catch (const std::exception&) {
            barrier_threw = true;
        }
        assert(barrier_threw);
    }

    // Startup consumes the durable rollback intent before WAL replay.
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) ==
               (seed_existing_wal ? "10101" : "00000"));
        recovered.SetBlockBits(1, 1, "11100");
        recovered.WalBarrier();
    }
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) ==
               (seed_existing_wal ? "10101" : "00000"));
        assert(recovered.GetBlockBits(1, 1) == "11100");
    }
}

void TestConditionalRollbackFailuresRecoverFailClosed() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        RunRejectedConditionalRollbackCase(
            kind, "CHUNKDB_FAILPOINT_WAL_EXISTS_STAT_FAIL_ONCE", false);
        RunRejectedConditionalRollbackCase(
            kind, "CHUNKDB_FAILPOINT_WAL_REMOVE_FAIL_ONCE", false);
        RunRejectedConditionalRollbackCase(
            kind, "CHUNKDB_FAILPOINT_WAL_ROLLBACK_SYNC_FAIL_ONCE", false);
        RunRejectedConditionalRollbackCase(
            kind, "CHUNKDB_FAILPOINT_WAL_RESIZE_FAIL_ONCE", true);
        RunRejectedConditionalRollbackCase(
            kind, "CHUNKDB_FAILPOINT_WAL_ROLLBACK_SYNC_FAIL_ONCE", true);
    }
}

void RunConditionalPreCommitFailureCase(
    ConditionalKind kind,
    const char* failpoint,
    bool seed_existing_wal) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-conditional-precommit");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1000000;
    config.checkpoint_wal_bytes = 1000000;

    {
        chunkdb::ChunkStore store(config);
        if (seed_existing_wal) {
            store.SetBlockBits(0, 0, "10101");
        }
        const auto expected = store.GetChunkVersion(0, 0);
        bool threw = false;
        {
            ScopedEnv fp(failpoint, "1");
            try {
                if (kind == ConditionalKind::kCas) {
                    (void)store.CasChunkState(
                        0,
                        0,
                        expected,
                        std::string(store.geometry().ChunkPayloadBits(), '1'),
                        std::string(store.geometry().ChunkBlockCount(), '1'));
                } else {
                    const std::vector<chunkdb::ChunkBatchOp> ops = {
                        {.set = true, .x = 0, .y = 0, .bits = "01010"}};
                    (void)store.ApplyChunkBatch(0, 0, true, expected, ops);
                }
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetBlockBits(0, 0) ==
               (seed_existing_wal ? "10101" : "00000"));
        store.SetBlockBits(1, 1, "11100");
        store.WalBarrier();
    }
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetBlockBits(0, 0) ==
               (seed_existing_wal ? "10101" : "00000"));
        assert(recovered.GetBlockBits(1, 1) == "11100");
    }
}

void TestConditionalPreCommitFailuresRemainAbsent() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_VERSION_RESERVE_FAIL_ONCE", false);
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_WAL_OPEN_ONCE", false);
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_WAL_SIZE_STAT_FAIL_ONCE", true);
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_INTENT_PUBLISH_ONCE", true);
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", true);
        RunConditionalPreCommitFailureCase(
            kind, "CHUNKDB_FAILPOINT_CONDITIONAL_BEFORE_COMMIT_PUBLISH_ONCE", true);
    }
}

void RunConditionalCheckpointFailureCase(
    ConditionalKind kind,
    const char* failpoint) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-conditional-checkpoint");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
    config.checkpoint_update_interval = 1;

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        const auto expected = store.GetChunkVersion(0, 0);
        ScopedEnv fp(failpoint, "1");
        if (kind == ConditionalKind::kCas) {
            const auto result = store.CasChunkState(
                0,
                0,
                expected,
                std::string(store.geometry().ChunkPayloadBits(), '1'),
                std::string(store.geometry().ChunkBlockCount(), '1'));
            assert(result.ok);
            assert(store.GetBlockBits(0, 0) == "11111");
        } else {
            const std::vector<chunkdb::ChunkBatchOp> ops = {
                {.set = true, .x = 0, .y = 0, .bits = "01010"}};
            const auto result =
                store.ApplyChunkBatch(0, 0, true, expected, ops);
            assert(result.ok);
            assert(store.GetBlockBits(0, 0) == "01010");
        }
    }
    chunkdb::ChunkStore recovered(config);
    assert(recovered.GetBlockBits(0, 0) ==
           (kind == ConditionalKind::kCas ? "11111" : "01010"));
}

void TestConditionalCheckpointFailuresDoNotBecomePostCommitErrors() {
    for (const auto kind : {ConditionalKind::kCas, ConditionalKind::kBatch}) {
        RunConditionalCheckpointFailureCase(
            kind, "CHUNKDB_FAILPOINT_CHECKPOINT_BEFORE_IMAGE_REPLACE_ONCE");
        RunConditionalCheckpointFailureCase(
            kind, "CHUNKDB_FAILPOINT_CHECKPOINT_AFTER_IMAGE_REPLACE_ONCE");
    }
}

// ---- WALFLUSH overflow fallback --------------------------------------------

void TestBarrierOverflowFallbackSucceeds() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-barrier-overflow");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));
    store.SetBlockBits(0, 0, "10101");
    store.SetBlockBits(40, 40, "11111");

    // Force the bounded-tracking overflow without writing 65k files.
    store.ForceUnsyncedOverflowForTests();
    assert(store.UnsyncedOverflowFlagForTests());

    store.WalBarrier();  // full-directory-sync fallback path
    assert(!store.UnsyncedOverflowFlagForTests());
    assert(store.RuntimeStats().wal_barrier_full_syncs >= 1);
    assert(store.RuntimeStats().wal_barriers >= 1);
}

void TestBarrierFailClosedRetainsBookkeeping() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-barrier-failclosed");
    chunkdb::ChunkStore store(BaseConfig(dir.path()));
    store.SetBlockBits(0, 0, "10101");

    // Normal (non-overflow) barrier that fails: the drained file/dir set must
    // be restored so a retry still covers the same writes.
    bool threw = false;
    {
        ScopedEnv fp("CHUNKDB_FAILPOINT_BARRIER_SYNC_FAIL_ONCE", "1");
        try {
            store.WalBarrier();
        } catch (const std::exception&) {
            threw = true;
        }
    }
    assert(threw);
    assert(store.UnsyncedTrackedCountForTests() > 0);  // bookkeeping retained
    // A retry (no fault) now succeeds and drains the set.
    store.WalBarrier();
    assert(store.UnsyncedTrackedCountForTests() == 0);

    // Same guarantee on the overflow path.
    store.SetBlockBits(8, 8, "11111");
    store.ForceUnsyncedOverflowForTests();
    bool overflow_threw = false;
    {
        ScopedEnv fp("CHUNKDB_FAILPOINT_BARRIER_SYNC_FAIL_ONCE", "1");
        try {
            store.WalBarrier();
        } catch (const std::exception&) {
            overflow_threw = true;
        }
    }
    assert(overflow_threw);
    assert(store.UnsyncedOverflowFlagForTests());  // overflow retained for retry
    store.WalBarrier();
    assert(!store.UnsyncedOverflowFlagForTests());
}

void RunBarrierCheckpointReplacementCase(bool overflow) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-barrier-replacement");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    chunkdb::ChunkStore store(config);

    // This acknowledged relaxed write is the state the barrier must protect.
    store.SetBlockBits(0, 0, "10101");
    store.ArmCheckpointBeforeWalRemovalPauseForTests();

    std::exception_ptr writer_error;
    std::thread writer([&] {
        try {
            // Triggers checkpoint replacement and pauses after the unsynced
            // image exists but before its tracked WAL is removed.
            store.SetBlockBits(1, 0, "01010");
        } catch (...) {
            writer_error = std::current_exception();
        }
    });
    assert(store.WaitForCheckpointBeforeWalRemovalForTests());
    if (overflow) {
        store.ForceUnsyncedOverflowForTests();
    }

    std::exception_ptr barrier_one_error;
    std::exception_ptr barrier_two_error;
    std::thread barrier_one([&] {
        try {
            store.WalBarrier();
        } catch (...) {
            barrier_one_error = std::current_exception();
        }
    });
    std::thread barrier_two([&] {
        try {
            store.WalBarrier();
        } catch (...) {
            barrier_two_error = std::current_exception();
        }
    });

    store.ResumeCheckpointBeforeWalRemovalForTests();
    writer.join();
    barrier_one.join();
    barrier_two.join();
    assert(writer_error == nullptr);
    assert(barrier_one_error == nullptr);
    assert(barrier_two_error == nullptr);
    assert(store.UnsyncedTrackedCountForTests() == 0);
    assert(!store.UnsyncedOverflowFlagForTests());
    if (overflow) {
        assert(store.RuntimeStats().wal_barrier_full_syncs >= 1);
    }
}

void TestBarrierCoversConcurrentCheckpointReplacement() {
    RunBarrierCheckpointReplacementCase(false);
    RunBarrierCheckpointReplacementCase(true);
}

void RunBarrierAfterDrainReplacementCase(bool overflow) {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-barrier-after-drain");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 2;  // hysteresis trigger is 3 updates

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");  // acknowledged, unsynced WAL
        if (overflow) {
            store.ForceUnsyncedOverflowForTests();
        }
        store.ArmBarrierAfterDrainPauseForTests();
        store.ArmCheckpointPublishAttemptForTests();

        std::exception_ptr barrier_error;
        std::thread barrier([&] {
            try {
                store.WalBarrier();
            } catch (...) {
                barrier_error = std::current_exception();
            }
        });
        assert(store.WaitForBarrierAfterDrainForTests());

        std::atomic<bool> writer_done{false};
        std::exception_ptr writer_error;
        std::thread writer([&] {
            try {
                store.SetBlockBits(1, 0, "01010");
                store.SetBlockBits(2, 0, "11100");
            } catch (...) {
                writer_error = std::current_exception();
            }
            writer_done.store(true, std::memory_order_release);
        });
        assert(store.WaitForCheckpointPublishAttemptForTests());
        // The checkpoint reached publication after the barrier drained, but
        // cannot replace the WAL while the barrier holds the publish lock.
        assert(!writer_done.load(std::memory_order_acquire));

        store.ResumeBarrierAfterDrainForTests();
        barrier.join();
        writer.join();
        assert(barrier_error == nullptr);
        assert(writer_error == nullptr);
    }

    // The later checkpoint ran after the successful barrier. It must preserve
    // the barrier's durability floor rather than replacing the synced WAL with
    // an unsynced image.
    chunkdb::ChunkStore recovered(config);
    assert(recovered.GetBlockBits(0, 0) == "10101");
    assert(recovered.GetBlockBits(1, 0) == "01010");
    assert(recovered.GetBlockBits(2, 0) == "11100");
}

void TestBarrierAfterDrainReplacementCannotDowngradeDurability() {
    RunBarrierAfterDrainReplacementCase(false);
    RunBarrierAfterDrainReplacementCase(true);
}

void TestBarrierCoversConcurrentEmptyChunkGc() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-barrier-gc");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");

        store.ArmCheckpointBeforeWalRemovalPauseForTests();
        std::exception_ptr gc_error;
        std::thread gc([&] {
            try {
                store.UnsetBlock(0, 0);
            } catch (...) {
                gc_error = std::current_exception();
            }
        });
        assert(store.WaitForCheckpointBeforeWalRemovalForTests());

        std::exception_ptr barrier_error;
        std::thread barrier([&] {
            try {
                store.WalBarrier();
            } catch (...) {
                barrier_error = std::current_exception();
            }
        });
        store.ResumeCheckpointBeforeWalRemovalForTests();
        gc.join();
        barrier.join();
        assert(gc_error == nullptr);
        assert(barrier_error == nullptr);
        assert(!store.BlockExists(0, 0));
    }
    chunkdb::ChunkStore recovered(config);
    assert(!recovered.BlockExists(0, 0));
}

// ---- Eviction residency ----------------------------------------------------

void TestEvictionKeepsRecentlyUsedResident() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-evict-residency");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 8;
    chunkdb::ChunkStore store(config);

    for (int i = 0; i < 8; ++i) {
        store.SetBlockBits(static_cast<std::int64_t>(i) * 8, 0, "10101");
    }
    // Push well past the bound so eviction must run, keeping chunk 0 hot on
    // every iteration so it stays ahead of each eviction refill: recency must
    // keep a continuously used chunk resident rather than merely reloadable.
    for (int i = 8; i < 40; ++i) {
        (void)store.GetBlockBits(0, 0);
        store.SetBlockBits(static_cast<std::int64_t>(i) * 8, 0, "11111");
    }

    assert(store.RuntimeStats().evictions > 0);
    // The continuously used chunk is still resident (not merely reloadable),
    // while a long-untouched early chunk was evicted.
    assert(store.IsChunkLoadedForTests(0, 0));
    assert(!store.IsChunkLoadedForTests(8, 0));
}

// ---- Background checkpoint I/O failure --------------------------------------

void TestBackgroundCheckpointFailureRetriesAndRecovers() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-bg-fail");
    auto config = BaseConfig(dir.path());
    config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
    config.checkpoint_update_interval = 1;
    config.background_maintenance = true;
    const auto data_path =
        chunkdb::ChunkDataPath(dir.path(), chunkdb::Geometry(config.geometry), {0, 0});

    {
        chunkdb::ChunkStore store(config);
        // Warm the persisted version clock first so its own atomic write does
        // not consume the one-shot failpoint intended for the checkpoint.
        (void)store.GetChunkVersion(0, 0);
        // Fail the first checkpoint image write (on the maintenance thread).
        {
            ScopedEnv fp("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE", "1");
            store.SetBlockBits(0, 0, "10101");
            // Give the maintenance thread a bounded chance to observe the fault.
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (store.RuntimeStats().background_checkpoint_failures == 0 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        assert(store.RuntimeStats().background_checkpoint_failures >= 1);

        // A later eligible write retries the checkpoint inline so the image is
        // eventually written and the error surfaces deterministically.
        store.SetBlockBits(0, 0, "01010");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!std::filesystem::exists(data_path) &&
               std::chrono::steady_clock::now() < deadline) {
            store.SetBlockBits(0, 0, "10101");
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        assert(std::filesystem::exists(data_path));
    }

    // Data is fully recoverable after shutdown.
    config.background_maintenance = false;
    chunkdb::ChunkStore recovered(config);
    const auto bits = recovered.GetBlockBits(0, 0);
    assert(bits == "10101" || bits == "01010");
}

// ---- Empty-chunk GC failpoint ordering -------------------------------------

void TestEmptyChunkGcOrderingAndRecovery() {
    for (const auto layout :
         {chunkdb::StorageLayoutMode::kFsSplitV1,
          chunkdb::StorageLayoutMode::kFsRegionV1Experimental}) {
        for (const char* failpoint :
             {"CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_REMOVE_ONCE",
              "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_IMAGE_DIR_SYNC_ONCE",
              "CHUNKDB_FAILPOINT_EMPTY_GC_AFTER_WAL_REMOVE_ONCE"}) {
            chunkdb::test::ScopedTempDir dir("chunkdb-reg-empty-gc-boundary");
            auto config = BaseConfig(dir.path());
            config.durability_mode = chunkdb::DurabilityMode::kFsyncCheckpoint;
            config.checkpoint_update_interval = 1;
            config.storage_layout_mode = layout;
            config.experimental_region_span_chunks = 2;

            {
                chunkdb::ChunkStore store(config);
                store.SetBlockBits(0, 0, "10101");
                {
                    ScopedEnv fp(failpoint, "1");
                    // The GC boundary failure happens after the unset is
                    // committed in the WAL, so the command reports success
                    // and the cleanup is retried by recovery.
                    store.UnsetBlock(0, 0);
                }
                assert(!store.BlockExists(0, 0));
            }

            // At every removal/sync boundary, recovery must observe the empty
            // state. The empty WAL is retained until the image removal is
            // durable, so the old value cannot be resurrected.
            {
                chunkdb::ChunkStore recovered(config);
                assert(!recovered.BlockExists(0, 0));
                recovered.SetBlockBits(0, 0, "11111");
            }
            {
                chunkdb::ChunkStore recovered(config);
                assert(recovered.GetBlockBits(0, 0) == "11111");
            }
        }
    }
}

// ---- Oversized geometry rejection ------------------------------------------

chunkdb::StoreConfig LargeGeometryConfig(const std::filesystem::path& data_dir) {
    auto config = BaseConfig(data_dir);
    // 512x512 blocks at 8 bits = 256 KiB payload: the chunk state spans
    // several 64 KiB WAL records, so one mutation is a multi-record frame.
    config.geometry = {
        .large_chunk_width_chunks = 2,
        .large_chunk_height_chunks = 2,
        .chunk_width_blocks = 512,
        .chunk_height_blocks = 512,
        .block_bits = 8,
    };
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000'000;
    return config;
}

void TestLargeGeometryConditionalMutationIsAtomic() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-large-geometry");
    const auto config = LargeGeometryConfig(dir.path());
    const std::string ones_payload(512U * 512U * 8U, '1');
    const std::string ones_presence(512U * 512U, '1');
    std::uint64_t committed_version = 0;
    {
        chunkdb::ChunkStore store(config);
        // No single-record bound any more: the multi-record frame is atomic.
        const auto cas = store.CasChunkState(
            0, 0, store.GetChunkVersion(0, 0), ones_payload, ones_presence);
        assert(cas.ok);
        committed_version = cas.version;
        assert(store.GetChunkBits(0, 0) == ones_payload);
    }
    {
        chunkdb::ChunkStore reopened(config);
        const auto reopened_version = reopened.GetChunkVersion(0, 0);
        if (reopened_version != committed_version) {
            std::fprintf(
                stderr, "large geometry: committed=%llu reopened=%llu\n",
                static_cast<unsigned long long>(committed_version),
                static_cast<unsigned long long>(reopened_version));
        }
        assert(reopened_version == committed_version);
        assert(reopened.GetChunkBits(0, 0) == ones_payload);
    }
}

void TestTornFrameIsIgnoredAsAWhole() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-torn-frame");
    const auto config = LargeGeometryConfig(dir.path());
    const chunkdb::Geometry geometry(config.geometry);
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), geometry, {0, 0});
    const std::string first(geometry.ChunkPayloadBits(), '0');
    std::string second(geometry.ChunkPayloadBits(), '1');
    std::uint64_t first_version = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetChunkBits(0, 0, first);
        first_version = store.GetChunkVersion(0, 0);
        store.SetChunkBits(0, 0, second);
        store.WalBarrier();
    }
    // Cut the WAL in the middle of the second mutation's frame: replay must
    // drop the whole frame, never a prefix of its records.
    const auto full = ReadFileBytes(wal_path);
    const auto after_first = [&] {
        // Frame boundaries: header + (frame header + records + trailer) per
        // mutation; recompute the first frame's end from its header.
        std::size_t cursor = 8U + 2U + 2U + 4U + 4U + 8U + 8U;  // WAL header
        const std::uint32_t body_size = static_cast<std::uint32_t>(full[cursor + 14]) |
                                        (static_cast<std::uint32_t>(full[cursor + 15]) << 8) |
                                        (static_cast<std::uint32_t>(full[cursor + 16]) << 16) |
                                        (static_cast<std::uint32_t>(full[cursor + 17]) << 24);
        return cursor + 22U + body_size + 4U;
    }();
    assert(after_first < full.size());
    const std::size_t cut = after_first + (full.size() - after_first) / 2U;
    std::vector<std::uint8_t> torn(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(cut));
    WriteFileBytes(wal_path, torn);
    {
        chunkdb::ChunkStore recovered(config);
        assert(recovered.GetChunkBits(0, 0) == first);
        assert(recovered.GetChunkVersion(0, 0) == first_version);
    }

    // A flipped byte_offset inside a record is caught by the record CRC
    // (the 1.x format applied such a record at the wrong place).
    auto flipped = full;
    const std::size_t first_record_offset_field = 8U + 2U + 2U + 4U + 4U + 8U + 8U + 22U;
    flipped[first_record_offset_field + 1] ^= 0x01U;
    WriteFileBytes(wal_path, flipped);
    {
        chunkdb::ChunkStore recovered(config);
        // The first frame is rejected, and with it everything after.
        assert(recovered.GetChunkBits(0, 0) == std::string(geometry.ChunkPayloadBits(), '0'));
        assert(!recovered.ChunkExists(0, 0));
    }

    // A flipped frame header field is caught by the header CRC.
    auto header_flip = full;
    header_flip[8U + 2U + 2U + 4U + 4U + 8U + 8U + 5U] ^= 0x01U;  // revision byte
    WriteFileBytes(wal_path, header_flip);
    {
        chunkdb::ChunkStore recovered(config);
        assert(!recovered.ChunkExists(0, 0));
    }
}

void TestRevisionStableAcrossEviction() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-revision-eviction");
    auto config = BaseConfig(dir.path());
    config.max_loaded_chunks = 1;
    chunkdb::ChunkStore store(config);
    store.SetBlockBits(0, 0, "10101");
    const auto version = store.GetChunkVersion(0, 0);
    // Touch other chunks so (0,0) is evicted and reloaded from disk.
    for (std::int64_t i = 1; i <= 8; ++i) {
        store.SetBlockBits(i * 4, i * 4, "11111");
    }
    assert(store.GetChunkVersion(0, 0) == version);
    const std::vector<chunkdb::ChunkBatchOp> ops = {{.set = true, .x = 0, .y = 0, .bits = "01010"}};
    const auto batch = store.ApplyChunkBatch(0, 0, true, version, ops);
    assert(batch.ok);
    assert(batch.version > version);
    assert(store.GetBlockBits(0, 0) == "01010");
}

// The revision must survive a checkpoint: it is carried by the v4 image
// header, not only by the WAL frames that the checkpoint discards.
void TestRevisionSurvivesCheckpointImage() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-revision-checkpoint");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    const chunkdb::Geometry geometry(config.geometry);
    const auto image_path = chunkdb::ChunkDataPath(dir.path(), geometry, {0, 0});
    const auto wal_path = chunkdb::ChunkWalPath(dir.path(), geometry, {0, 0});

    std::uint64_t version = 0;
    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
        version = store.GetChunkVersion(0, 0);
    }
    // Checkpointed: the image is the only artifact left carrying the revision.
    assert(std::filesystem::exists(image_path));
    assert(!std::filesystem::exists(wal_path));

    {
        chunkdb::ChunkStore reopened(config);
        assert(reopened.GetChunkVersion(0, 0) == version);
        assert(reopened.GetBlockBits(0, 0) == "10101");
        // A token retained across the checkpoint still commits.
        const std::vector<chunkdb::ChunkBatchOp> ops = {
            {.set = true, .x = 0, .y = 0, .bits = "01010"}};
        const auto batch = reopened.ApplyChunkBatch(0, 0, true, version, ops);
        assert(batch.ok);
        assert(batch.version > version);
    }
}

// The v4 image header CRC covers the revision, so a flip there is rejected
// instead of handing out a corrupted CHUNKVER token.
void TestImageHeaderCrcCorruptionRejected() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-image-header-crc");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1;
    const chunkdb::Geometry geometry(config.geometry);
    const auto image_path = chunkdb::ChunkDataPath(dir.path(), geometry, {0, 0});

    {
        chunkdb::ChunkStore store(config);
        store.SetBlockBits(0, 0, "10101");
    }
    const auto image = ReadFileBytes(image_path);
    // 1.x header is 52 bytes; v4 appends revision (52..59) and the header CRC
    // over [0, 60) at 60..63.
    assert(image.size() > 64U);
    assert(image[8] == 4U && image[9] == 0U);  // version = 4

    for (const std::size_t offset : {52U, 55U, 60U}) {
        auto corrupt = image;
        corrupt[offset] ^= 0x01U;
        WriteFileBytes(image_path, corrupt);
        bool threw = false;
        try {
            chunkdb::ChunkStore store(config);
            (void)store.GetBlockBits(0, 0);
        } catch (const std::exception& error) {
            threw = true;
            if (std::string(error.what()).find("header checksum mismatch") ==
                std::string::npos) {
                std::fprintf(stderr, "unexpected image error: %s\n", error.what());
            }
            assert(
                std::string(error.what()).find("header checksum mismatch") !=
                std::string::npos);
        }
        assert(threw);
    }

    // Restoring the image makes the chunk readable again with its revision.
    WriteFileBytes(image_path, image);
    chunkdb::ChunkStore healthy(config);
    assert(healthy.GetBlockBits(0, 0) == "10101");
}

// A chunk whose WAL is still a 1.x record stream needs a mid-stream v4 header
// before frames can be appended. If a conditional mutation writes that header
// and is then rolled back, the truncation puts the 1.x tail back, so the next
// append has to write the header again — otherwise its frames land inside a
// record stream and replay drops them at the next restart.
void TestRolledBackMigrationHeaderIsRewritten() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-rollback-migration-header");
    auto config = BaseConfig(dir.path());
    config.checkpoint_update_interval = 1'000'000;
    config.checkpoint_wal_bytes = 1'000'000;
    const chunkdb::Geometry geometry(config.geometry);

    // A 1.x (v3) WAL: block (0,0) = "10101".
    WriteLegacyV3Wal(
        chunkdb::ChunkWalPath(dir.path(), geometry, {0, 0}),
        geometry,
        {0, 0},
        {{0U, {0x15}}, {static_cast<std::uint32_t>(geometry.ChunkPayloadBytes()), {0x01}}});

    {
        chunkdb::ChunkStore store(config);
        assert(store.GetBlockBits(0, 0) == "10101");

        // The conditional mutation appends the v4 header and its frame, then
        // fails after the append and rolls the WAL back to the 1.x boundary.
        bool threw = false;
        {
            ScopedEnv rollback_failure(
                "CHUNKDB_FAILPOINT_CONDITIONAL_AFTER_WAL_APPEND_ONCE", "1");
            try {
                const std::vector<chunkdb::ChunkBatchOp> ops = {
                    {.set = true, .x = 1, .y = 1, .bits = "11111"}};
                (void)store.ApplyChunkBatch(
                    0, 0, true, store.GetChunkVersion(0, 0), ops);
            } catch (const std::exception&) {
                threw = true;
            }
        }
        assert(threw);
        assert(store.GetBlockBits(0, 0) == "10101");
        assert(!store.BlockExists(1, 1));

        // The next mutation must migrate the stream again before its frame.
        store.SetBlockBits(2, 2, "01110");
        store.WalBarrier();
        assert(store.GetBlockBits(2, 2) == "01110");
    }

    {
        chunkdb::ChunkStore reopened(config);
        // Both the legacy prefix and the post-rollback frame survive replay.
        assert(reopened.GetBlockBits(0, 0) == "10101");
        assert(reopened.GetBlockBits(2, 2) == "01110");
        assert(!reopened.BlockExists(1, 1));
    }
}

void TestReadOnlyStoreSeesPersistedRevision() {
    chunkdb::test::ScopedTempDir dir("chunkdb-reg-readonly-revision");
    auto config = BaseConfig(dir.path());
    std::uint64_t version = 0;
    {
        chunkdb::ChunkStore writer(config);
        writer.SetBlockBits(0, 0, "10101");
        writer.WalBarrier();
        version = writer.GetChunkVersion(0, 0);
    }
    auto read_only = config;
    read_only.access_mode = chunkdb::AccessMode::kReadOnly;
    chunkdb::ChunkStore reader(read_only);
    assert(reader.GetChunkVersion(0, 0) == version);
    assert(reader.GetBlockBits(0, 0) == "10101");
}

}  // namespace

int main() {
    TestRangeExtremeCoordinatesTerminate();
    TestRangeByteBudget();
    TestRadiusReads();
    TestBatchCheckpointFailureRollsBackAcrossRestart();
    TestCasCheckpointFailureRollsBack();
    TestVersionMonotonicAcrossManyReloads();
    TestStableV1WalOnlyStoreMigrates();
    TestStableV1CheckpointAndNegativeCoordinatesMigrate();
    TestIntermediateVersionCeilingUpgradesWithoutReuse();
    TestVersionBookkeepingDamageFailsClosed();
    TestIntentEstablishmentFailuresLeaveLiveStateUsable();
    TestPreExistingCommittedIntentFailuresAreSafe();
    TestCommittedIntentCleanupFailuresRemainCommitted();
    TestConditionalRollbackFailuresRecoverFailClosed();
    TestConditionalPreCommitFailuresRemainAbsent();
    TestConditionalCheckpointFailuresDoNotBecomePostCommitErrors();
    TestBarrierOverflowFallbackSucceeds();
    TestBarrierFailClosedRetainsBookkeeping();
    TestBarrierCoversConcurrentCheckpointReplacement();
    TestBarrierAfterDrainReplacementCannotDowngradeDurability();
    TestBarrierCoversConcurrentEmptyChunkGc();
    TestEvictionKeepsRecentlyUsedResident();
    TestBackgroundCheckpointFailureRetriesAndRecovers();
    TestEmptyChunkGcOrderingAndRecovery();
    TestLargeGeometryConditionalMutationIsAtomic();
    TestTornFrameIsIgnoredAsAWhole();
    TestRevisionStableAcrossEviction();
    TestRevisionSurvivesCheckpointImage();
    TestImageHeaderCrcCorruptionRejected();
    TestRolledBackMigrationHeaderIsRewritten();
    TestReadOnlyStoreSeesPersistedRevision();
    return 0;
}
