#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <new>
#include <limits>
#include <sstream>

#include "verify.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/file_layout.hpp"
#include "chunk_coordinate_index.hpp"
#include "store_manifest.hpp"
#include "test_utils.hpp"
#include "wal_writer.hpp"

namespace allocation {
std::atomic<bool> measuring{false};
std::atomic<std::size_t> live{0U}, peak{0U};
struct alignas(std::max_align_t) Header { std::size_t counted; };
}

void* operator new(std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - sizeof(allocation::Header)) throw std::bad_alloc();
    auto* block = static_cast<allocation::Header*>(std::malloc(sizeof(allocation::Header) + (bytes == 0U ? 1U : bytes)));
    if (block == nullptr) throw std::bad_alloc();
    block->counted = allocation::measuring.load(std::memory_order_relaxed) && bytes >= 16384U ? bytes : 0U;
    if (block->counted != 0U) {
        const auto live = allocation::live.fetch_add(bytes, std::memory_order_relaxed) + bytes;
        auto peak = allocation::peak.load(std::memory_order_relaxed);
        while (peak < live && !allocation::peak.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {}
    }
    return block + 1;
}
// Replacement deallocation must not be folded into GCC's allocation heuristic.
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer) noexcept {
    if (pointer == nullptr) return;
    auto* block = static_cast<allocation::Header*>(pointer) - 1;
    if (block->counted != 0U) allocation::live.fetch_sub(block->counted, std::memory_order_relaxed);
    std::free(block);
}
void* operator new[](std::size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { ::operator delete(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
void Save(const std::filesystem::path& path, const Bytes& bytes) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close(); assert(output);
}
auto Snapshot(const std::filesystem::path& root) {
    std::map<std::filesystem::path, Bytes> files;
    for (const auto& file : std::filesystem::recursive_directory_iterator(root))
        if (file.is_regular_file()) files.emplace(file.path(), LoadFile(file.path()));
    return files;
}
}

int main() {
    test::ScopedTempDir directory("chunkdb-verify-memory");
    const auto root = directory.path() / "tables/t";
    const Geometry geometry({2U, 2U, 256U, 256U, 8U});
    const StoreId id{1U};
    Save(DataDirManifestPath(directory.path()), SerializeDataDirManifest({.data_dir_id = StoreId{9U}, .options = {}}));
    Save(StoreManifestPath(root), SerializeStoreManifest({.geometry = geometry.config(), .store_id = id,
        .options = EncodeTableOptions(TableOptions{}), .schema = geometry.layout().schema()}));
    std::ostringstream inventory;
    WriteIndexInventory(inventory, {.table_id = id}, {});
    const auto text = inventory.str();
    Save(root / kChunkIndexFileName, Bytes(text.begin(), text.end()));
    Save(root / "chunkdb.version", SerializeVersionClockRecord(1000U));
    const Bytes payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
    for (std::int64_t x = 0; x < 128; ++x) {
        const ChunkCoord coord{x, 0};
        Save(ChunkDataPath(root, geometry, coord), SerializeChunkImage(geometry, coord, payload, presence,
            CheckpointCompression::kNone, 10U, 100U, id));
        const auto large = geometry.ChunkToLarge(coord);
        const auto path = LargeChunkDirectory(root, large) / kChunkIndexFileName;
        auto bytes = std::filesystem::exists(path) ? LoadFile(path) : SerializeChunkIndexHeader({id, 2U, large});
        const auto record = SerializeChunkIndexRecord({ChunkIndexKind::kDeleted, coord, 10U, 100U, 1U}, geometry, large);
        bytes.insert(bytes.end(), record.begin(), record.end()); Save(path, bytes);
    }
    // A measured WAL remains bounded by one frame even with many frames.
    auto wal = BuildWalHeader({0, 0}, id, ReadStoreManifest(root)->features);
    for (std::uint64_t revision = 11U; revision < 139U; ++revision) {
        WalFrameBuilder frame(&wal, 1U);
        frame.AppendSpan(0U, payload.data(), payload.size());
        (void)frame.Finish(revision, revision * 10U);
    }
    Save(ChunkWalPath(root, geometry, {0, 0}), wal);
    const auto before = Snapshot(directory.path());
    std::ostringstream findings;
    allocation::measuring.store(true, std::memory_order_relaxed);
    const auto checked = VerifyDataDirectory(directory.path(), findings);
    allocation::measuring.store(false, std::memory_order_relaxed);
    const auto peak = allocation::peak.load(std::memory_order_relaxed);
    if (checked.errors != 0U) std::cerr << findings.str();
    assert(checked.errors == 0U);
    if (peak > 2U*1024U*1024U) {
        std::cerr << "verification retained " << peak << " large-allocation bytes\n";
        return 1;
    }
    assert(Snapshot(directory.path()) == before);
}
