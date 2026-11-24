#include <algorithm>
#include <atomic>
#include <cassert>
#include <map>
#include <set>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
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

// A snapshot page lists the coordinates populated at its revision: one a
// later write empties stays listed while that write is between its
// population change and its return, since the revision is published first;
// the walk releases the index lock between batches and keeps its order.
void SnapshotPageKeepsLaterEmptiedCoordinates() {
    test::ScopedTempDir directory("chunkdb-coordinate-snapshot");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    const ChunkCoord emptied{0, 0};
    const ChunkCoord kept{2, 0};
    ChunkCoordinateNode* emptied_node = nullptr;
    ChunkCoordinateNode* kept_node = nullptr;
    Publish(owner, emptied, emptied_node, true, 1U);
    Publish(owner, kept, kept_node, true, 2U);
    const std::uint64_t snapshot = 2U;

    struct Hold final : CoordinatePublicationTestHook {
        std::mutex mutex;
        std::condition_variable changed;
        bool reached = false;
        bool released = false;
        void AfterPopulationChange(const ChunkCoord&) noexcept override {
            std::unique_lock lock(mutex);
            reached = true;
            changed.notify_all();
            changed.wait(lock, [&] { return released; });
        }
    } hold;
    SetCoordinatePublicationTestHook(&hold);
    std::thread writer([&] { Publish(owner, emptied, emptied_node, false, 3U); });
    {
        std::unique_lock lock(hold.mutex);
        hold.changed.wait(lock, [&] { return hold.reached; });
    }
    // The write has removed the coordinate from the populated set.
    assert(owner.Page(false, {}, 10U).coords == std::vector{kept});
    assert((owner.SnapshotPage(false, {}, 10U, snapshot).coords == std::vector{emptied, kept}));
    assert(owner.SnapshotPage(false, {}, 10U, 3U).coords == std::vector{kept});
    {
        std::lock_guard lock(hold.mutex);
        hold.released = true;
    }
    hold.changed.notify_all();
    writer.join();
    SetCoordinatePublicationTestHook(nullptr);
    assert((owner.SnapshotPage(false, {}, 10U, snapshot).coords == std::vector{emptied, kept}));

    // More than one batch of nodes: every other one emptied after revision R.
    constexpr std::int64_t kCount = 700;
    std::vector<ChunkCoordinateNode*> nodes(kCount, nullptr);
    std::uint64_t revision = 10U;
    for (std::int64_t x = 0; x < kCount; ++x) Publish(owner, {x, 9}, nodes[x], true, ++revision);
    const auto before = revision;
    for (std::int64_t x = 0; x < kCount; x += 2) Publish(owner, {x, 9}, nodes[x], false, ++revision);
    const auto all = owner.SnapshotPage(false, {}, kMaxChunkScanLimit + 1U, before);
    assert(!all.more && all.coords.size() == static_cast<std::size_t>(kCount) + 1U);
    assert(std::is_sorted(all.coords.begin(), all.coords.end()));
    const auto now = owner.SnapshotPage(false, {}, kMaxChunkScanLimit + 1U, revision);
    assert(!now.more && now.coords.size() == static_cast<std::size_t>(kCount) / 2U + 1U);
    // A limited page resumes after its cursor.
    const auto first = owner.SnapshotPage(false, {}, 300U, before);
    assert(first.more && first.coords.size() == 300U);
    const auto rest = owner.SnapshotPage(true, first.coords.back(), 500U, before);
    assert(!rest.more && first.coords.size() + rest.coords.size() == all.coords.size());
    assert(std::equal(rest.coords.begin(), rest.coords.end(), all.coords.begin() + 300));
}

struct ModelEntry {
    std::uint64_t revision = 0U;
    bool populated = false;
    ChunkCoordinateNode* node = nullptr;
};
using Model = std::map<ChunkCoord, ModelEntry>;

std::vector<ChunkCoord> Collect(const std::function<CoordinatePage(bool, const ChunkCoord&)>& page) {
    std::vector<ChunkCoord> all;
    bool has_cursor = false;
    ChunkCoord cursor{};
    for (;;) {
        const auto next = page(has_cursor, cursor);
        assert(next.more ? !next.coords.empty() : true);
        all.insert(all.end(), next.coords.begin(), next.coords.end());
        if (!next.more) return all;
        has_cursor = true;
        cursor = next.coords.back();
    }
}

// Pages of changes skip runs of coordinates whose revisions are all at or
// below the requested one; every page must still equal a full comparison.
void ExpectChangedPages(const ChunkCoordinateIndex& owner, const Model& model, std::uint64_t since) {
    std::vector<ChunkCoord> changed, at_snapshot, populated;
    for (const auto& [coord, entry] : model) {
        if (entry.revision == 0U) continue;
        if (entry.revision > since) changed.push_back(coord);
        if (entry.populated || entry.revision > since) at_snapshot.push_back(coord);
        if (entry.populated) populated.push_back(coord);
    }
    for (const std::size_t limit : {std::size_t{1}, std::size_t{7}, std::size_t{64}, kMaxChunkScanLimit + 1U}) {
        assert(Collect([&](bool has, const ChunkCoord& at) { return owner.Page(has, at, limit, since); }) == changed);
        assert(Collect([&](bool has, const ChunkCoord& at) { return owner.SnapshotPage(has, at, limit, since); }) ==
            at_snapshot);
        assert(Collect([&](bool has, const ChunkCoord& at) { return owner.Page(has, at, limit); }) == populated);
    }
}

void ChangedPagesSkipUnchangedRuns() {
    test::ScopedTempDir directory("chunkdb-index-runs");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path() / "writer", geometry, kTable));
    ChunkCoordinateIndex owner(directory.path() / "writer", geometry, kTable, false);
    Model model;
    std::uint64_t revision = 0U;
    std::uint32_t seed = 12345U;
    const auto next = [&](std::uint32_t bound) {
        seed = seed * 1664525U + 1013904223U;
        return static_cast<std::int64_t>((seed >> 8U) % bound);
    };
    std::vector<std::uint64_t> marks{0U};
    for (int step = 1; step <= 6000; ++step) {
        const ChunkCoord coord{next(40U) - 20, next(200U) - 100};
        auto& entry = model[coord];
        if (next(8U) == 0) {
            // An abandoned first write leaves no coordinate behind.
            auto reservation = owner.Prepare(coord, &entry.node);
            continue;
        }
        const bool populated = next(3U) != 0;
        Publish(owner, coord, entry.node, populated, ++revision);
        entry.revision = revision;
        entry.populated = populated;
        if (step % 750 == 0) marks.push_back(revision);
    }
    marks.push_back(revision);
    for (const auto since : marks) ExpectChangedPages(owner, model, since);

    // Recovery builds the same runs from observed identities.
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path() / "recovered", geometry, kTable));
    ChunkCoordinateIndex recovered(directory.path() / "recovered", geometry, kTable, false);
    for (const auto& [coord, entry] : model) recovered.ObserveRecoveredIdentity(coord, entry.revision, entry.populated);
    for (const auto since : marks) ExpectChangedPages(recovered, model, since);
}

// A writer raising its run races a split moving its coordinate to a new run.
void ConcurrentSplitsKeepEveryRevision() {
    test::ScopedTempDir directory("chunkdb-index-run-race");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    constexpr std::int64_t kRows = 2000;
    std::atomic<std::uint64_t> clock{0U};
    Model rewritten, inserted;
    for (std::int64_t y = 0; y < kRows; y += 2) {
        auto& entry = rewritten[{0, y}];
        entry.revision = ++clock;
        Publish(owner, {0, y}, entry.node, true, entry.revision);
    }
    const auto before = clock.load();
    std::thread writer([&] {
        for (int round = 0; round < 4; ++round) {
            for (auto& [coord, entry] : rewritten) {
                entry.revision = ++clock;
                entry.populated = (round % 2) == 0;
                Publish(owner, coord, entry.node, entry.populated, entry.revision);
            }
        }
    });
    std::thread splitter([&] {
        for (std::int64_t y = 1; y < kRows; y += 2) {
            auto& entry = inserted[{0, y}];
            entry.revision = ++clock;
            entry.populated = true;
            Publish(owner, {0, y}, entry.node, true, entry.revision);
        }
    });
    writer.join();
    splitter.join();
    Model model = rewritten;
    model.insert(inserted.begin(), inserted.end());
    for (const auto since : {std::uint64_t{0U}, before, (before + clock.load()) / 2U, clock.load() - 1U})
        ExpectChangedPages(owner, model, since);
}


// A reader pages while a writer inserts coordinates between others, splitting
// their runs, and rewrites some. Each page must ascend past its cursor and
// list every coordinate in its range written after R before the reader began.
void PagesDuringSplitsKeepEarlierChanges() {
    test::ScopedTempDir directory("chunkdb-index-run-reader");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    constexpr std::int64_t kRows = 3000;
    std::uint64_t clock = 0U;
    std::vector<ChunkCoordinateNode*> nodes(kRows, nullptr);
    for (std::int64_t y = 1; y < kRows; y += 3) Publish(owner, {0, y}, nodes[y], true, ++clock);
    const auto since = clock;
    std::set<ChunkCoord> kept;
    for (std::int64_t y = 0; y < kRows; y += 3) {
        Publish(owner, {0, y}, nodes[y], true, ++clock);
        kept.insert(ChunkCoord{0, y});
    }
    std::atomic<std::uint64_t> next_revision{clock};
    std::atomic<bool> done{false};
    std::thread writer([&] {
        for (std::int64_t y = 2; y < kRows; y += 3) Publish(owner, {0, y}, nodes[y], y % 2 == 0, ++next_revision);
        for (std::int64_t y = 1; y < kRows; y += 6) Publish(owner, {0, y}, nodes[y], false, ++next_revision);
        done.store(true);
    });
    const auto check = [&](bool changes_only, std::size_t limit) {
        bool has_cursor = false;
        ChunkCoord cursor{};
        for (;;) {
            const auto page = changes_only ? owner.Page(has_cursor, cursor, limit, since)
                                           : owner.SnapshotPage(has_cursor, cursor, limit, since);
            for (std::size_t at = 0U; at < page.coords.size(); ++at) {
                const auto& coord = page.coords[at];
                assert(at == 0U ? !has_cursor || cursor < coord : page.coords[at - 1U] < coord);
                if (changes_only) {
                    assert(owner.Find(coord)->revision.load() > since);
                }
            }
            auto expected = has_cursor ? kept.upper_bound(cursor) : kept.begin();
            for (; expected != kept.end() && (!page.more || !(page.coords.back() < *expected)); ++expected)
                assert(std::binary_search(page.coords.begin(), page.coords.end(), *expected));
            if (!page.more) return;
            has_cursor = true;
            cursor = page.coords.back();
        }
    };
    do {
        for (const std::size_t limit : {std::size_t{5}, std::size_t{97}}) {
            check(true, limit);
            check(false, limit);
        }
    } while (!done.load());
    writer.join();
    check(true, 97U);
    check(false, 97U);
}

// A read-only refresh that forgets a stretch of coordinates empties whole
// runs, which are removed; coordinates published there again split the
// surrounding run and reuse them.
void ReadOnlyRefreshRemovesAndReusesRuns() {
    test::ScopedTempDir directory("chunkdb-index-run-refresh");
    const auto geometry = Shape();
    assert(ChunkCoordinateIndex::InitializeEmpty(directory.path(), geometry, kTable));
    const ChunkCoordinateIndex retained(directory.path(), geometry, kTable, true);
    ChunkCoordinateIndex reader(directory.path(), geometry, kTable, true);
    Model model;
    std::uint64_t revision = 0U;
    for (std::int64_t y = 0; y < 400; ++y) {
        reader.ObserveRecoveredIdentity({0, y}, ++revision, true);
        model[{0, y}] = ModelEntry{.revision = revision, .populated = true};
    }
    const auto refresh = [&](const std::vector<RecoveredCoordinateIdentity>& updates) {
        reader.RefreshReadOnly(updates, retained, [] {});
        for (const auto& update : updates)
            model[update.coord] = ModelEntry{.revision = update.revision, .populated = update.populated};
    };
    std::vector<RecoveredCoordinateIdentity> updates;
    for (std::int64_t y = 100; y < 300; ++y) updates.push_back({{0, y}, 0U, false});
    refresh(updates);
    for (const auto since : {std::uint64_t{0U}, std::uint64_t{200U}, revision}) ExpectChangedPages(reader, model, since);
    const auto before = revision;
    updates.clear();
    for (std::int64_t y = 100; y < 300; y += 2) updates.push_back({{0, y}, ++revision, y % 4 == 0});
    refresh(updates);
    for (const auto since : {std::uint64_t{0U}, before, revision - 50U, revision}) ExpectChangedPages(reader, model, since);
}

// A coordinate recovered from a forget record joins a run, so a later write
// to it raises that run and SINCE pages find it.
void AdmittedForgetJoinsItsRun() {
    test::ScopedTempDir directory("chunkdb-index-run-forget");
    const auto geometry = Shape();
    const LargeChunkCoord large{};
    const ChunkCoord forgotten{0, 0}, deleted{1, 0};
    auto bytes = SerializeChunkIndexHeader({kTable, 2U, large});
    for (const auto& record : {ChunkIndexRecord{ChunkIndexKind::kDeleted, forgotten, 2U, 20U, 1U},
             ChunkIndexRecord{ChunkIndexKind::kForget, forgotten, 2U, 20U, 1U},
             ChunkIndexRecord{ChunkIndexKind::kDeleted, deleted, 4U, 40U, 1U}}) {
        const auto encoded = SerializeChunkIndexRecord(record, geometry, large);
        bytes.insert(bytes.end(), encoded.begin(), encoded.end());
    }
    AtomicWrite(Log(directory.path(), geometry, {}), bytes, true, true);
    std::ostringstream output;
    WriteIndexInventory(output, {.table_id = kTable, .dims = 2U, .retained_deletion_floor = 2U, .count = 1U},
        std::vector<IndexInventoryEntry>{{large, bytes.size()}});
    const auto encoded = output.str();
    AtomicWrite(directory.path() / kChunkIndexFileName, Bytes(encoded.begin(), encoded.end()), true, true);
    ChunkCoordinateIndex owner(directory.path(), geometry, kTable, false);
    owner.CompleteRecovery();
    assert(owner.Page(false, {}, 10U, 2U).coords == std::vector{deleted});
    ChunkCoordinateNode* node = nullptr;
    Publish(owner, forgotten, node, true, 5U);
    assert(owner.Page(false, {}, 10U, 4U).coords == std::vector{forgotten});
    assert((owner.Page(false, {}, 10U, 2U).coords == std::vector{forgotten, deleted}));
    assert(owner.SnapshotPage(false, {}, 10U, 4U).coords == std::vector{forgotten});
}


int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--binding-only") {
        ReplacementIdentityBindsValidSameLengthContent();
        return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--crash-boundaries") {
        CrashCompactionBoundaries(argv[0]);
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
    SnapshotPageKeepsLaterEmptiedCoordinates();
    ChangedPagesSkipUnchangedRuns();
    ConcurrentSplitsKeepEveryRevision();
    PagesDuringSplitsKeepEarlierChanges();
    ReadOnlyRefreshRemovesAndReusesRuns();
    AdmittedForgetJoinsItsRun();
}
