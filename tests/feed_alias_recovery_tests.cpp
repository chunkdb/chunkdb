#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <new>
#include <sstream>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "feed_slots.hpp"
#include "feed_test_utils.hpp"
#include "test_utils.hpp"
#include "wal_replay.hpp"
#include "wal_writer.hpp"

namespace allocation {
thread_local bool measuring = false;
thread_local std::size_t largest = 0U;
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t bytes) {
    if (allocation::measuring) allocation::largest = std::max(allocation::largest, bytes);
    if (auto* memory = std::malloc(bytes == 0U ? 1U : bytes)) return memory;
    throw std::bad_alloc();
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer) noexcept { std::free(pointer); }
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { ::operator delete(pointer); }
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

template <typename Fn> void Refuse(Fn&& fn) {
    bool refused = false;
    try { fn(); } catch (const std::runtime_error&) { refused = true; }
    assert(refused);
}

void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    assert(output.good());
}

struct Fixture {
    test::ScopedTempDir directory{"chunkdb-feed-alias-stream"};
    CatalogConfig config = feed_test::Config(directory.path());
    std::filesystem::path archive, live;
    std::uint64_t expected_version = 0U;
    std::uint32_t original_crc = 0U;
    explicit Fixture(bool independent, bool second_archive = false) {
        config.slot_sync_interval = std::chrono::hours(1);
        {
            TableCatalog catalog(config);
            const GeometryConfig shape{2U, 2U, 256U, 256U, 8U};
            auto table = catalog.Create("default", shape, catalog.default_options());
            (void)table->CreateFeedSlot("consumer");
            auto lease = table->Acquire();
            auto& store = lease->store();
            ChunkState state{.payload = Bytes(store.geometry().ChunkPayloadBytes(), 0U),
                .presence_bitmap = Bytes(ChunkPresenceBitmapBytes(store.geometry()), 0U)};
            state.presence_bitmap[0] = 1U;
            const auto writes = [&] {
                for (unsigned i = 1U; i <= 128U; ++i) {
                    state.payload[0] = static_cast<std::uint8_t>(i);
                    (void)store.WriteChunkState(0, 0, state, {});
                }
            };
            writes();
            store.CheckpointForTests(0, 0);
            live = ChunkWalPath(directory.path() / "tables/default", store.geometry(), {0, 0});
            if (independent || second_archive) writes();
            if (second_archive) store.CheckpointForTests(0, 0);
            expected_version = store.ReadChunkState(0, 0).version;
        }
        const auto archive_dir = directory.path() / "tables/default" / kFeedArchiveDirName;
        for (const auto& file : std::filesystem::directory_iterator(archive_dir))
            if (file.path().filename().string().starts_with("C_0_0.1-") && file.path().extension() == ".wal") archive = file.path();
        assert(!archive.empty() && std::filesystem::file_size(archive) > 8U * 1024U * 1024U);
        original_crc = Crc32(LoadFile(archive));
        if (!independent) std::filesystem::create_hard_link(archive, live);
        else assert(!std::filesystem::equivalent(archive, live));
    }
};

void BoundedReopen(bool independent) {
    Fixture fixture(independent);
    allocation::largest = 0U;
    {
        struct Measure {
            Measure() { allocation::measuring = true; }
            ~Measure() { allocation::measuring = false; }
        } measure;
        TableCatalog catalog(fixture.config);
        auto lease = catalog.Require("default")->Acquire();
        const auto state = lease->store().ReadChunkState(0, 0);
        assert(state.version == fixture.expected_version && state.payload[0] == 128U);
        assert(std::filesystem::exists(fixture.live) == independent);
    }
    if (allocation::largest >= 2U * 1024U * 1024U)
        std::cerr << "recovery allocated a single " << allocation::largest << " byte buffer\n";
    assert(allocation::largest < 2U * 1024U * 1024U);
    assert(Crc32(LoadFile(fixture.archive)) == fixture.original_crc);
}

void RefusedAliasPreservesBothNames(bool corrupt) {
    Fixture fixture(false);
    if (corrupt) {
        auto bytes = LoadFile(fixture.archive);
        bytes.back() ^= 1U;
        Save(fixture.archive, bytes);
    } else {
        const auto wrong = fixture.archive.parent_path() / "C_0_0.1-129.wal";
        std::filesystem::rename(fixture.archive, wrong);
        fixture.archive = wrong;
    }
    const auto before = LoadFile(fixture.archive);
    Refuse([&] { TableCatalog catalog(fixture.config); });
    assert(std::filesystem::equivalent(fixture.live, fixture.archive));
    assert(LoadFile(fixture.archive) == before && LoadFile(fixture.live) == before);
}

void FirstFrameAdmission() {
    const Geometry geometry({2U, 2U, 2U, 1U, 8U});
    const StoreId epoch{1U};
    const FeatureFlags features{.incompat = kFeatureChunkModel};
    auto bytes = BuildWalHeader({0, 0}, epoch, features);
    Bytes frame;
    WalFrameBuilder builder(&frame, 1U);
    const Bytes state{7U, 0U, 1U};
    builder.AppendSpan(0U, state.data(), state.size());
    (void)builder.Finish(7U, 70U);
    const auto header_bytes = bytes.size();
    bytes.insert(bytes.end(), frame.begin(), frame.end());
    const auto inspect = [&](const Bytes& input) {
        std::istringstream stream(std::string(input.begin(), input.end()), std::ios::binary);
        return InspectFirstWalFrameStream(stream, input.size(), geometry, {0, 0}, epoch, features);
    };
    const auto complete = inspect(bytes);
    assert(complete && complete->revision == 7U && complete->commit_time_ms == 70U);
    for (std::size_t cut = header_bytes; cut < bytes.size(); ++cut)
        assert(!inspect(Bytes(bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(cut))));
    for (const auto at : {header_bytes, header_bytes + 10U, bytes.size() - 1U}) {
        auto bad = bytes; bad[at] ^= 1U;
        assert(!inspect(bad));
    }
    auto invalid = bytes;
    invalid[header_bytes + kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize] = 255U;
    const auto body_start = header_bytes + kWalFrameFixedHeaderSize + kWalFrameHeaderCrcSize;
    const auto crc = Crc32(invalid.data() + body_start, invalid.size() - body_start - kWalFrameTrailerSize);
    for (unsigned i = 0U; i < 4U; ++i) invalid[invalid.size() - 4U + i] = static_cast<std::uint8_t>(crc >> (8U * i));
    Refuse([&] { (void)inspect(invalid); });
    auto with_tail = bytes;
    with_tail.insert(with_tail.end(), 1024U * 1024U, 0xffU);
    const auto first = inspect(with_tail);
    assert(first && first->revision == 7U);
    auto oversized = bytes;
    const auto body_bytes = static_cast<std::uint32_t>(kMaxWalFrameBytes);
    for (unsigned i = 0U; i < 4U; ++i)
        oversized[header_bytes + 28U + i] = static_cast<std::uint8_t>(body_bytes >> (8U * i));
    const auto header_crc = Crc32(oversized.data() + header_bytes + kWalFrameMagicSize,
        kWalFrameFixedHeaderSize - kWalFrameMagicSize);
    for (unsigned i = 0U; i < 4U; ++i)
        oversized[header_bytes + kWalFrameFixedHeaderSize + i] = static_cast<std::uint8_t>(header_crc >> (8U * i));
    Refuse([&] { (void)inspect(oversized); });
    std::istringstream sparse(std::string(oversized.begin(), oversized.end()), std::ios::binary);
    Refuse([&] { (void)InspectFirstWalFrameStream(sparse, header_bytes + kWalFrameFixedHeaderSize +
        kWalFrameHeaderCrcSize + body_bytes + kWalFrameTrailerSize, geometry, {0, 0}, epoch, features); });
}

void MultipleArchivesAdmittedBeforeCleanup(bool corrupt) {
    Fixture fixture(false, true);
    const auto second = fixture.archive.parent_path() / "C_0_0.129-256.wal";
    assert(std::filesystem::exists(second));
    if (corrupt) {
        auto bytes = LoadFile(second); bytes.back() ^= 1U; Save(second, bytes);
        const auto before = LoadFile(second);
        Refuse([&] { TableCatalog catalog(fixture.config); });
        assert(std::filesystem::equivalent(fixture.live, fixture.archive));
        assert(Crc32(LoadFile(fixture.archive)) == fixture.original_crc && LoadFile(second) == before);
    } else {
        TableCatalog catalog(fixture.config);
        auto lease = catalog.Require("default")->Acquire();
        assert(lease->store().ReadChunkState(0, 0).version == fixture.expected_version);
        assert(!std::filesystem::exists(fixture.live));
    }
}

void InteriorFirstFrameDamage() {
    const Geometry geometry({2U, 2U, 2U, 1U, 8U});
    const StoreId epoch{1U};
    const FeatureFlags features{.incompat = kFeatureChunkModel};
    const auto header = BuildWalHeader({0, 0}, epoch, features);
    Bytes frame;
    WalFrameBuilder builder(&frame, 1U);
    const Bytes state{7U, 0U, 1U};
    builder.AppendSpan(0U, state.data(), state.size());
    (void)builder.Finish(7U, 70U);
    for (const auto at : {std::size_t{0U}, std::size_t{10U}, frame.size() - 1U}) {
        auto bytes = header;
        auto bad = frame; bad[at] ^= 1U;
        bytes.insert(bytes.end(), bad.begin(), bad.end());
        bytes.insert(bytes.end(), frame.begin(), frame.end());
        std::istringstream stream(std::string(bytes.begin(), bytes.end()), std::ios::binary);
        Refuse([&] { (void)InspectFirstWalFrameStream(stream, bytes.size(), geometry, {0, 0}, epoch, features); });
    }
}

void PartialExtentMatchesReplayPolicy() {
    const Geometry geometry({2U, 2U, 2U, 1U, 8U});
    const StoreId epoch{1U};
    const ChunkCoord coord{0, 0};
    const FeatureFlags features{.incompat = kFeatureChunkModel};
    const auto header = BuildWalHeader(coord, epoch, features);
    Bytes frame;
    WalFrameBuilder builder(&frame, 1U);
    const Bytes state{7U, 0U, 1U};
    builder.AppendSpan(0U, state.data(), state.size());
    (void)builder.Finish(7U, 70U);
    for (const bool partial_header : {false, true}) {
        auto first = frame;
        if (partial_header) {
            first[22U] = 0U; first[23U] = 1U; // A cut 256-byte TLV area.
        } else {
            for (unsigned i = 0U; i < 4U; ++i) first[28U + i] = i == 1U ? 1U : 0U;
            const auto crc = Crc32(first.data() + kWalFrameMagicSize, kWalFrameFixedHeaderSize - kWalFrameMagicSize);
            for (unsigned i = 0U; i < 4U; ++i)
                first[kWalFrameFixedHeaderSize + i] = static_cast<std::uint8_t>(crc >> (8U * i));
        }
        auto bytes = header;
        bytes.insert(bytes.end(), first.begin(), first.end());
        bytes.insert(bytes.end(), frame.begin(), frame.end());
        std::istringstream inspect_stream(std::string(bytes.begin(), bytes.end()), std::ios::binary);
        assert(!InspectFirstWalFrameStream(inspect_stream, bytes.size(), geometry, coord, epoch, features));
        for (const bool stream : {false, true}) {
            Bytes payload, presence(ChunkPresenceBitmapBytes(geometry), 0U);
            ChunkVars vars;
            std::istringstream input(std::string(bytes.begin(), bytes.end()), std::ios::binary);
            const auto result = stream ? ReplayWalStream(input, bytes.size(), geometry, coord, epoch, features,
                0U, 0U, &payload, &presence, &vars) : ReplayWal(bytes, geometry, coord, epoch, features,
                0U, 0U, &payload, &presence, &vars);
            assert(result.replayable && result.applied_frames == 0U && result.stopped_at_crash_tail);
            assert(result.stop_reason == (partial_header ? "partial_frame_header" : "partial_frame_body"));
        }
    }
}
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--alias") { BoundedReopen(false); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--independent") { BoundedReopen(true); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--first-frame") { FirstFrameAdmission(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--multiple") { MultipleArchivesAdmittedBeforeCleanup(false); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--interior") { InteriorFirstFrameDamage(); return 0; }
    if (argc == 2 && std::string_view(argv[1]) == "--partial-extent") { PartialExtentMatchesReplayPolicy(); return 0; }
    BoundedReopen(false); BoundedReopen(true);
    RefusedAliasPreservesBothNames(false); RefusedAliasPreservesBothNames(true);
    FirstFrameAdmission();
    MultipleArchivesAdmittedBeforeCleanup(false); MultipleArchivesAdmittedBeforeCleanup(true);
    InteriorFirstFrameDamage();
    PartialExtentMatchesReplayPolicy();
    std::cout << "9 bounded archive alias recovery groups passed\n";
}
