#include <cassert>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/wait.h>
#endif

#include "chunk_coordinate_index.hpp"
#include "checkpoint.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunkdb/crc32.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
const StoreId kTable{1U};

Geometry Shape(std::uint8_t dims = 2U) {
    GeometryConfig config{2U, 2U, 2U, 2U, 8U};
    config.dims = dims;
    config.chunk_depth_blocks = dims == 3U ? 2U : 1U;
    config.large_chunk_depth_chunks = dims == 3U ? 2U : 1U;
    return Geometry(config);
}
void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::exception&) { refused = true; }
    assert(refused);
}
ChunkState State(const Geometry& geometry, std::uint64_t revision, bool populated = false) {
    ChunkState state;
    state.version = revision;
    state.written = revision != 0U;
    state.commit_time_ms = revision * 10U;
    state.payload.assign(geometry.ChunkPayloadBytes(), 0U);
    state.presence_bitmap.assign(ChunkPresenceBitmapBytes(geometry), 0U);
    if (populated) { state.payload[0] = 7U; state.presence_bitmap[0] = 1U; }
    return state;
}
void Publish(ChunkCoordinateIndex& owner, const ChunkCoord& coord, ChunkCoordinateNode*& cached,
    bool populated, std::uint64_t revision) {
    auto reservation = owner.Prepare(coord, &cached);
    reservation.Publish(populated, revision);
}
void Retain(ChunkCoordinateIndex& owner, const ChunkCoord& coord, std::uint64_t revision) {
    owner.PublishTombstone({ChunkIndexKind::kDeleted, coord, revision, revision * 10U, 1U});
}
std::filesystem::path Log(const std::filesystem::path& root, const Geometry& geometry, ChunkCoord coord) {
    return LargeChunkDirectory(root, geometry.ChunkToLarge(coord), geometry.dims()) / kChunkIndexFileName;
}

void ReservationsAndRetainedIdentity() {
    for (const auto dims : {std::uint8_t{2}, std::uint8_t{3}}) {
        test::ScopedTempDir directory("chunkdb-coordinate-index");
        const auto geometry = Shape(dims);
        const ChunkCoord coord{-1, 2, dims == 3U ? -3 : 0};
        assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
        assert(ChunkCoordinateIndex::UnpublishedEmptyIdentity(directory.path(), dims) == kTable);
        assert(!ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
        {
            ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
            ChunkCoordinateNode* cached = nullptr;
            {
                auto reservation = owner.Prepare(coord, &cached);
                assert(cached && owner.Find(coord) == cached);
                assert(owner.Page(false, {}, 10U, 0U).coords.empty());
            }
            assert(!cached && !owner.Find(coord));
            Publish(owner, coord, cached, true, 1U);
            assert(owner.Page(false, {}, 10U).coords == std::vector{coord});
            Publish(owner, coord, cached, false, 2U);
            assert(owner.Page(false, {}, 10U).coords.empty());
            assert(owner.Page(false, {}, 10U, 1U).coords == std::vector{coord});
            Retain(owner, coord, 2U);
            const auto log = LoadFile(Log(directory.path(), geometry, coord));
            Retain(owner, coord, 2U);
            assert(LoadFile(Log(directory.path(), geometry, coord)) == log);
            Reject([&] { owner.PublishTombstone({ChunkIndexKind::kDeleted, coord, 2U, 21U, 1U}); });
            Reject([&] { owner.PublishTombstone({ChunkIndexKind::kDeleted, coord, 2U, 20U, 2U}); });
            assert(!owner.failed());
            Publish(owner, coord, cached, true, 3U);
            assert(owner.EmptyBoundary(coord)->revision == 2U);
            Publish(owner, coord, cached, false, 4U);
            Retain(owner, coord, 4U);
        }
        ChunkCoordinateIndex reopened(directory.path(), geometry, kTable, false);
        assert(reopened.EmptyBoundary(coord)->revision == 4U);
        assert(reopened.maximum_recovered_revision() == 4U);
        assert(reopened.Page(false, {}, 10U).coords.empty());
        assert(reopened.Page(false, {}, 10U, 3U).coords == std::vector{coord});
        Reject([&] { (void)ChunkCoordinateIndex::UnpublishedEmptyIdentity(directory.path(), dims); });
    }
}

void DurableCutsAndOrphanAdmission() {
    test::ScopedTempDir directory("chunkdb-index-cuts");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    const ChunkCoord coord{0, 0};
    const auto path = Log(directory.path(), geometry, coord);
    auto bytes = SerializeChunkIndexHeader({kTable, 2U, geometry.ChunkToLarge(coord)});
    const auto record = SerializeChunkIndexRecord({ChunkIndexKind::kDeleted, coord, 2U, 20U, 1U}, geometry,
        geometry.ChunkToLarge(coord));
    bytes.insert(bytes.end(), record.begin(), record.end());
    AtomicWrite(path, bytes, true, true);
    const auto inventory = LoadFile(directory.path() / kChunkIndexFileName);
    {
        ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
        owner.AdmitUnlistedLog(geometry.ChunkToLarge(coord));
        Reject([&] { owner.CompleteRecovery(); });
        assert(owner.failed() && LoadFile(directory.path() / kChunkIndexFileName) == inventory);
        assert(LoadFile(path) == bytes);
    }
    {
        ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
        owner.AdmitUnlistedLog(geometry.ChunkToLarge(coord));
        owner.ObserveRecovered(coord, State(geometry, 2U));
        owner.CompleteRecovery();
        assert(owner.EmptyBoundary(coord)->revision == 2U);
    }
    auto suffix = record; suffix.back() ^= 1U;
    auto with_suffix = bytes; with_suffix.insert(with_suffix.end(), suffix.begin(), suffix.end());
    AtomicWrite(path, with_suffix, true, true);
    {
        ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
        assert(owner.EmptyBoundary(coord)->revision == 2U && LoadFile(path) == with_suffix);
#ifdef _WIN32
        const auto original_link = path.parent_path() / "prefix-source";
        std::filesystem::create_hard_link(path, original_link);
        unsigned observed = 0U;
        struct ResetObserver {
            ~ResetObserver() { SetDirectorySyncObserverForTests({}); }
        } reset_observer;
        SetDirectorySyncObserverForTests([&](const std::filesystem::path& synced) {
            if (synced != path.parent_path()) return;
            // The replacement has published, but RewritePrefix has not yet
            // returned. Its own reader must no longer pin the old inode.
            const auto handle = CreateFileW(original_link.c_str(), GENERIC_READ, 0U,
                nullptr, OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
            const auto error = handle == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
            if (handle == INVALID_HANDLE_VALUE)
                std::cerr << "prefix source still has a native reader: " << error << '\n';
            assert(handle != INVALID_HANDLE_VALUE);
            assert(CloseHandle(handle) != 0);
            ++observed;
        });
        if (kFailpointsCompiled) {
            assert(_putenv_s("CHUNKDB_FAILPOINT_RENAME_INFO_EX_UNSUPPORTED_ONCE", "1") == 0);
            const auto* armed = std::getenv("CHUNKDB_FAILPOINT_RENAME_INFO_EX_UNSUPPORTED_ONCE");
            assert(armed != nullptr && armed[0] == '1');
        }
#endif
        owner.CompleteRecovery();
#ifdef _WIN32
        if (kFailpointsCompiled) {
            const auto* remaining = std::getenv("CHUNKDB_FAILPOINT_RENAME_INFO_EX_UNSUPPORTED_ONCE");
            assert(remaining == nullptr || remaining[0] == '\0');
        }
        assert(observed == 1U && LoadFile(original_link) == with_suffix);
#endif
        assert(LoadFile(path) == bytes);
        assert(owner.EmptyBoundary(coord)->revision == 2U && owner.EmptyBoundary(coord)->commit_time_ms == 20U);
    }
    auto damaged = bytes; damaged.back() ^= 1U;
    AtomicWrite(path, damaged, true, true);
    Reject([&] { ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false); });
    assert(LoadFile(path) == damaged);
}

void CompactionDropsOnlyItsExactBound() {
    test::ScopedTempDir directory("chunkdb-index-compaction");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    const ChunkCoord old{0, 0}, recent{4, 0};
    {
        ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
        ChunkCoordinateNode *first = nullptr, *second = nullptr;
        Publish(owner, old, first, false, 2U); Retain(owner, old, 2U);
        Publish(owner, recent, second, false, 4U); Retain(owner, recent, 4U);
        owner.Compact(4U, 10U);
        assert(owner.failed());
    }
    {
        ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
        owner.CompleteRecovery();
        assert(!owner.EmptyBoundary(old) && owner.EmptyBoundary(recent)->revision == 4U);
        assert(owner.retained_deletion_floor() == 3U);
        Reject([&] { (void)owner.Page(false, {}, 10U, 2U); });
        assert(owner.Page(false, {}, 10U, 3U).coords == std::vector{recent});
        owner.Compact(2U, 10U); // An older operation cannot reuse cumulative floor 3.
    }
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    assert(owner.EmptyBoundary(recent)->revision == 4U && owner.retained_deletion_floor() == 3U);
}

void InventoryGenerationNeverWraps() {
    test::ScopedTempDir directory("chunkdb-index-generation");
    const auto geometry = Shape();
    IndexInventoryHeader header{.table_id = kTable, .dims = 2U,
        .generation = std::numeric_limits<std::uint64_t>::max()};
    std::ostringstream output;
    WriteIndexInventory(output, header, {});
    const auto text = output.str();
    const Bytes original(text.begin(), text.end());
    AtomicWrite(directory.path() / kChunkIndexFileName, original, true, true);
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    assert(owner.inventory_generation() == header.generation);
    ChunkCoordinateNode* node = nullptr;
    Publish(owner, {0, 0}, node, false, 2U);
    Reject([&] { Retain(owner, {0, 0}, 2U); });
    Reject([&] { owner.Compact(3U, 10U); });
    assert(!owner.failed() && LoadFile(directory.path() / kChunkIndexFileName) == original);
    assert(!std::filesystem::exists(Log(directory.path(), geometry, {0, 0})));
}

void ForgetRequiresItsPublishedRefusalFloor() {
    test::ScopedTempDir directory("chunkdb-index-forget-floor");
    const auto geometry = Shape();
    const LargeChunkCoord large{};
    auto bytes = SerializeChunkIndexHeader({kTable, 2U, large});
    for (const auto kind : {ChunkIndexKind::kDeleted, ChunkIndexKind::kForget}) {
        const auto record = SerializeChunkIndexRecord({kind, {}, 2U, 20U, 1U}, geometry, large);
        bytes.insert(bytes.end(), record.begin(), record.end());
    }
    AtomicWrite(Log(directory.path(), geometry, {}), bytes, true, true);
    std::ostringstream output;
    WriteIndexInventory(output, {.table_id = kTable, .dims = 2U, .count = 1U},
        std::vector<IndexInventoryEntry>{{large, bytes.size()}});
    const auto encoded = output.str();
    AtomicWrite(directory.path() / kChunkIndexFileName, Bytes(encoded.begin(), encoded.end()), true, true);
    Reject([&] { ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false); });
}

void ReplacementIdentityBindsValidSameLengthContent() {
    for (const auto dims : {std::uint8_t{2U}, std::uint8_t{3U}}) {
        test::ScopedTempDir directory("chunkdb-index-replacement-digest");
        const auto geometry = Shape(dims);
        const LargeChunkCoord large{};
        const ChunkCoord kept{1, 0, dims == 3U ? 1 : 0};
        const auto header = SerializeChunkIndexHeader({kTable, dims, large});
        const auto live = [&](std::uint64_t revision) {
            auto bytes = header;
            const auto record = SerializeChunkIndexRecord(
                {ChunkIndexKind::kLive, kept, revision, revision * 10U, 1U}, geometry, large);
            bytes.insert(bytes.end(), record.begin(), record.end());
            return bytes;
        };
        const auto expected = live(11U), wrong = live(12U);
        assert(expected.size() == wrong.size() && Crc32(expected) == Crc32(wrong));
        auto old = header;
        const auto deleted = SerializeChunkIndexRecord({ChunkIndexKind::kDeleted, {}, 2U, 20U, 1U}, geometry, large);
        old.insert(old.end(), deleted.begin(), deleted.end());
        old.insert(old.end(), expected.begin() + static_cast<std::ptrdiff_t>(header.size()), expected.end());
        const auto path = Log(directory.path(), geometry, {});
        const auto stage = std::filesystem::path(path.string() + ".compact");
        AtomicWrite(path, old, true, true);
        AtomicWrite(stage, wrong, true, true);
        std::istringstream expected_input(std::string(expected.begin(), expected.end()));
        const auto digest = IndexFileDigest(expected_input, expected.size());
        std::ostringstream output;
        WriteIndexInventory(output, {.table_id = kTable, .dims = dims, .compaction_pending = true,
            .retained_deletion_floor = 5U, .compaction_through = 5U, .count = 1U, .generation = 7U},
            std::vector<IndexInventoryEntry>{{large, old.size(), expected.size(), digest}});
        const auto text = output.str();
        const Bytes inventory(text.begin(), text.end());
        AtomicWrite(directory.path() / kChunkIndexFileName, inventory, true, true);
        Reject([&] { ChunkCoordinateIndex recovered(directory.path(), geometry, kTable, false); });
        assert(LoadFile(path) == old && LoadFile(stage) == wrong);
        assert(LoadFile(directory.path() / kChunkIndexFileName) == inventory);
        AtomicWrite(path, wrong, true, true);
        std::filesystem::remove(stage);
        Reject([&] { ChunkCoordinateIndex recovered(directory.path(), geometry, kTable, false); });
        assert(LoadFile(path) == wrong && !std::filesystem::exists(stage));
        assert(LoadFile(directory.path() / kChunkIndexFileName) == inventory);
        AtomicWrite(stage, expected, true, true);
        {
            ChunkCoordinateIndex recovered(directory.path(), geometry, kTable, false);
            recovered.CompleteRecovery();
            assert(recovered.inventory_generation() == 8U && recovered.retained_deletion_floor() == 5U);
        }
        assert(LoadFile(path) == expected && !std::filesystem::exists(stage));
    }
}

void SetEnv(const char* key) {
#ifdef _WIN32
    _putenv_s(key, "1");
#else
    setenv(key, "1", 1);
#endif
}
std::string Quote(const std::string& text) {
#ifdef _WIN32
    return '"' + text + '"';
#else
    std::string result = "'";
    for (const char c : text) result += c == '\'' ? "'\\''" : std::string(1, c);
    return result + "'";
#endif
}
int CrashChild(const char* executable, const std::filesystem::path& root, const char* point) {
    const auto command = Quote(std::filesystem::absolute(executable).string()) + " --crash " +
        Quote(root.string()) + " " + Quote(point);
#ifdef _WIN32
    return std::system(('"' + command + '"').c_str());
#else
    const auto result = std::system(command.c_str());
    assert(result != -1 && WIFEXITED(result)); return WEXITSTATUS(result);
#endif
}
void CrashCompactionBoundaries(const char* executable) {
    if (!kFailpointsCompiled) return;
    for (const auto* suffix : {"AFTER_STAGE_RENAME", "AFTER_STAGE_DIRECTORY_SYNC", "AFTER_DECISION",
            "AFTER_REPLACE", "AFTER_DIRECTORY_SYNC", "AFTER_FINAL_INVENTORY"}) {
        test::ScopedTempDir directory("chunkdb-index-compaction-crash");
        const auto geometry = Shape();
        const auto point = "CHUNKDB_FAILPOINT_CRASH_INDEX_COMPACTION_" + std::string(suffix) + "_ONCE";
        assert(CrashChild(executable, directory.path(), point.c_str()) == 86);
        const bool decided = std::string_view(suffix) != "AFTER_STAGE_RENAME" &&
            std::string_view(suffix) != "AFTER_STAGE_DIRECTORY_SYNC";
        ChunkCoordinateIndex recovered(directory.path(), geometry, kTable, false);
        recovered.CompleteRecovery();
        assert(recovered.EmptyBoundary({4, 0})->revision == 4U);
        assert(recovered.EmptyBoundary({0, 0}).has_value() == !decided);
        assert(recovered.retained_deletion_floor() == (decided ? 3U : 0U));
        ChunkCoordinateIndex readonly(directory.path(), geometry, kTable, true);
        assert(readonly.EmptyBoundary({4, 0})->revision == 4U);
    }
}
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--binding-only") {
        ReplacementIdentityBindsValidSameLengthContent();
        return 0;
    }
    if (argc == 4 && std::string_view(argv[1]) == "--crash") {
        const auto geometry = Shape();
        assert(ChunkCoordinateIndex::InitializeEmpty(argv[2], geometry, kTable));
        ChunkCoordinateIndex owner(argv[2], geometry, kTable, false);
        ChunkCoordinateNode *first = nullptr, *second = nullptr;
        Publish(owner, {0, 0}, first, false, 2U); Retain(owner, {0, 0}, 2U);
        Publish(owner, {4, 0}, second, false, 4U); Retain(owner, {4, 0}, 4U);
        SetEnv(argv[3]); owner.Compact(4U, 10U); return 1;
    }
    ReservationsAndRetainedIdentity();
    DurableCutsAndOrphanAdmission();
    CompactionDropsOnlyItsExactBound();
    InventoryGenerationNeverWraps();
    ForgetRequiresItsPublishedRefusalFloor();
    ReplacementIdentityBindsValidSameLengthContent();
    CrashCompactionBoundaries(argv[0]);
}
