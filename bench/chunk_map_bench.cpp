// Chunk-map lookup benchmark.
//
// A point read or write finds its chunk through two hash maps, each under
// its own mutex: the store's large chunks, then the chunks of that large
// chunk (ChunkStore::GetOrLoadRegularChunk). Inside the chunk the block is
// found by arithmetic. This benchmark prices those lookups on a warm cache
// (docs/design/COLUMNS_DESIGN.md, "Positions instead of keys").
//
// Each run opens a fresh store and loads `--chunks` chunks with every block
// present. With nothing loaded or evicted while timing, it measures
//
//   read    GetBlockBits
//   write   SetBlockBits, each write changing its block
//   lookup  the same two map lookups and mutexes on maps of the same types,
//           hash, keys, sizes and insertion order, outside the store
//
// on two sides: `many`, where each operation's chunk is drawn uniformly from
// all loaded chunks, and `one`, where every operation is in one chunk. Both
// sides use the same sequence of block positions inside the chunk, so
// many - one is what spreading the operations over the chunks costs.
//
// Sequences come from mt19937_64 with a fixed seed. Every timed window
// follows an untimed pass of the same sequence and is timed in segments of
// kSegmentOps operations; its figure is the median segment's ns/op, which
// leaves out rare stalls such as the version-clock reservation (a durable
// write every 16384 versions). Writes keep their WAL frames in each chunk's
// in-memory batch: group commit and checkpoint thresholds are above anything
// a run writes, and the untimed pass, drained by WalBarrier, has already
// grown each batch's capacity, so a timed write does no file I/O and no WAL
// allocation. Runs alternate which side goes first.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif

#include "chunk_map_bench_args.hpp"
#include "chunkdb/bench_build.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/logging.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using chunkdb::chunk_map_bench::Args;
using chunkdb::chunk_map_bench::DataDirs;
using chunkdb::chunk_map_bench::kSegmentOps;
using chunkdb::chunk_map_bench::RunDir;

constexpr std::uint64_t kSeed = 135;
// Chunks per world row: 16 large chunks of 8x8 chunks.
constexpr std::int64_t kGridWidthChunks = 128;
constexpr std::int64_t kChunkSide = 16;
constexpr std::size_t kBlocksPerChunk = static_cast<std::size_t>(kChunkSide * kChunkSide);
// Above any number of updates or WAL bytes one run gives a chunk.
constexpr std::size_t kNoThreshold = std::size_t{1} << 40;
// Test failure injection compiled into the engine (CHUNKDB_FAILPOINTS) reads
// the environment on every write; a published run is built without it.
#ifdef CHUNKDB_FAILPOINTS
constexpr const char* kFailpoints = "on";
#else
constexpr const char* kFailpoints = "off";
#endif

// 15 bits, so the std::string a read returns fits the small-string buffer of
// libstdc++ and libc++ and no timed read allocates.
constexpr std::uint32_t kBlockBits = 15;
// Value 0 and value 1 differ in every bit.
constexpr std::string_view kBits[2] = {"010101010101010", "101010101010101"};
static_assert(kBits[0].size() == kBlockBits && kBits[1].size() == kBlockBits);

chunkdb::StoreConfig BuildStoreConfig(const std::filesystem::path& data_dir, std::size_t chunks) {
    return chunkdb::StoreConfig{
        .geometry = {
            .large_chunk_width_chunks = 8,
            .large_chunk_height_chunks = 8,
            .chunk_width_blocks = static_cast<std::uint32_t>(kChunkSide),
            .chunk_height_blocks = static_cast<std::uint32_t>(kChunkSide),
            .block_bits = kBlockBits,
        },
        .data_dir = data_dir,
        .durability_mode = chunkdb::DurabilityMode::kRelaxed,
        .checkpoint_update_interval = kNoThreshold,
        .checkpoint_wal_bytes = kNoThreshold,
        .wal_group_commit_updates = kNoThreshold,
        .max_loaded_chunks = chunks * 2,
        .allow_multiple_processes = false,
        .background_maintenance = false,
    };
}

chunkdb::ChunkCoord ChunkOf(std::uint32_t index) {
    return {static_cast<std::int64_t>(index) % kGridWidthChunks, static_cast<std::int64_t>(index) / kGridWidthChunks};
}

// ---------------------------------------------------------------------------
// Host and build facts printed with the numbers.
// ---------------------------------------------------------------------------

std::string OsName() {
#if defined(__APPLE__)
    return "macos";
#elif defined(__linux__)
    return "linux";
#elif defined(_WIN32)
    return "windows";
#else
    return "unknown";
#endif
}

std::string ArchName() {
#if defined(__aarch64__) || defined(_M_ARM64)
    return "arm64";
#elif defined(__x86_64__) || defined(_M_X64)
    return "x86_64";
#else
    return "unknown";
#endif
}

std::string CpuName() {
#if defined(__APPLE__)
    char buffer[256] = {};
    std::size_t size = sizeof(buffer);
    if (::sysctlbyname("machdep.cpu.brand_string", buffer, &size, nullptr, 0) == 0 && size > 1) {
        return std::string(buffer);
    }
    return "unknown";
#elif defined(__linux__)
    std::ifstream cpuinfo("/proc/cpuinfo");
    std::string line;
    while (std::getline(cpuinfo, line)) {
        if (line.rfind("model name", 0) == 0) {
            const auto colon = line.find(':');
            if (colon != std::string::npos && colon + 2 <= line.size()) {
                return line.substr(colon + 2);
            }
        }
    }
    return "unknown";
#else
    return "unknown";
#endif
}

std::string CompilerName() {
#if defined(__clang__)
    return std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
    return std::string("gcc ") + __VERSION__;
#elif defined(_MSC_VER)
    return "msvc " + std::to_string(_MSC_FULL_VER);
#else
    return "unknown";
#endif
}

// ---------------------------------------------------------------------------
// Operation sequences and the expected block values.
// ---------------------------------------------------------------------------

struct Sequence {
    // Per operation: the chunk's index and the block's index inside it.
    std::vector<std::uint32_t> chunk;
    std::vector<std::uint16_t> local;
    std::vector<std::int64_t> block_x;
    std::vector<std::int64_t> block_y;
    std::vector<chunkdb::ChunkCoord> chunk_coord;
};

Sequence BuildSequence(const std::vector<std::uint32_t>& chunks, const std::vector<std::uint16_t>& locals) {
    Sequence sequence;
    sequence.chunk = chunks;
    sequence.local = locals;
    sequence.block_x.reserve(chunks.size());
    sequence.block_y.reserve(chunks.size());
    sequence.chunk_coord.reserve(chunks.size());
    for (std::size_t i = 0; i < chunks.size(); ++i) {
        const auto coord = ChunkOf(chunks[i]);
        sequence.chunk_coord.push_back(coord);
        sequence.block_x.push_back(coord.x * kChunkSide + static_cast<std::int64_t>(locals[i] % kChunkSide));
        sequence.block_y.push_back(coord.y * kChunkSide + static_cast<std::int64_t>(locals[i] / kChunkSide));
    }
    return sequence;
}

// The value of every block, 0 or 1 (kBits).
class Shadow {
  public:
    explicit Shadow(std::size_t chunks) : values_(chunks * kBlocksPerChunk, 0U) {}

    // The values the writes of `sequence` give, each the other one than
    // its block has before it, applied to the shadow.
    std::vector<std::uint8_t> ApplyWrites(const Sequence& sequence) {
        std::vector<std::uint8_t> written(sequence.chunk.size());
        for (std::size_t i = 0; i < written.size(); ++i) {
            auto& value = values_[Index(sequence, i)];
            value = static_cast<std::uint8_t>(1U - value);
            written[i] = value;
        }
        return written;
    }

    // The value each read of `sequence` finds.
    [[nodiscard]] std::vector<std::uint8_t> Values(const Sequence& sequence) const {
        std::vector<std::uint8_t> values(sequence.chunk.size());
        for (std::size_t i = 0; i < values.size(); ++i) {
            values[i] = values_[Index(sequence, i)];
        }
        return values;
    }

  private:
    static std::size_t Index(const Sequence& sequence, std::size_t i) {
        return static_cast<std::size_t>(sequence.chunk[i]) * kBlocksPerChunk + sequence.local[i];
    }

    std::vector<std::uint8_t> values_;
};

// ---------------------------------------------------------------------------
// The store's two chunk-level lookups alone: the same map types, hash and
// mutexes, a large-chunk object made with make_shared and a chunk object
// with its own control block, as the store's cache holds them.
// ---------------------------------------------------------------------------

struct ReplicaChunk {
    std::uint64_t marker = 0;
};

struct ReplicaLargeChunk {
    std::mutex mutex;
    std::unordered_map<chunkdb::ChunkCoord, std::shared_ptr<ReplicaChunk>, chunkdb::ChunkCoordHash> chunks;
};

class ReplicaMaps {
  public:
    void Insert(const chunkdb::Geometry& geometry, const chunkdb::ChunkCoord& coord, std::uint64_t marker) {
        auto& large = large_chunks_[geometry.ChunkToLarge(coord)];
        if (large == nullptr) {
            large = std::make_shared<ReplicaLargeChunk>();
        }
        large->chunks.emplace(coord, std::shared_ptr<ReplicaChunk>(new ReplicaChunk{marker}));
    }

    [[nodiscard]] std::uint64_t Find(const chunkdb::Geometry& geometry, const chunkdb::ChunkCoord& coord) {
        const chunkdb::LargeChunkCoord large_coord = geometry.ChunkToLarge(coord);
        std::shared_ptr<ReplicaLargeChunk> large;
        {
            std::lock_guard lock(mutex_);
            const auto it = large_chunks_.find(large_coord);
            if (it == large_chunks_.end()) {
                throw std::logic_error("replica has no large chunk for a loaded chunk");
            }
            large = it->second;
        }
        std::shared_ptr<ReplicaChunk> chunk;
        {
            std::lock_guard lock(large->mutex);
            const auto it = large->chunks.find(coord);
            if (it == large->chunks.end()) {
                throw std::logic_error("replica has no entry for a loaded chunk");
            }
            chunk = it->second;
        }
        return chunk->marker;
    }

  private:
    std::mutex mutex_;
    std::unordered_map<chunkdb::LargeChunkCoord, std::shared_ptr<ReplicaLargeChunk>, chunkdb::LargeChunkCoordHash>
        large_chunks_;
};

// ---------------------------------------------------------------------------
// Timing.
// ---------------------------------------------------------------------------

double Median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const std::size_t mid = values.size() / 2;
    return values.size() % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2.0;
}

struct WindowTiming {
    double ns_per_op = 0.0;
    double mean_ns_per_op = 0.0;
};

template <typename Op>
WindowTiming TimeWindow(std::size_t ops, Op&& op) {
    std::vector<double> segment_ns_per_op;
    segment_ns_per_op.reserve(ops / kSegmentOps);
    const auto window_started = Clock::now();
    for (std::size_t begin = 0; begin < ops; begin += kSegmentOps) {
        const auto started = Clock::now();
        for (std::size_t i = begin; i < begin + kSegmentOps; ++i) {
            op(i);
        }
        const auto finished = Clock::now();
        segment_ns_per_op.push_back(
            std::chrono::duration<double, std::nano>(finished - started).count() / static_cast<double>(kSegmentOps));
    }
    const auto window_finished = Clock::now();
    return WindowTiming{
        .ns_per_op = Median(std::move(segment_ns_per_op)),
        .mean_ns_per_op = std::chrono::duration<double, std::nano>(window_finished - window_started).count() /
                          static_cast<double>(ops),
    };
}

enum Metric { kRead = 0, kWrite = 1, kLookup = 2, kMetricCount = 3 };
enum Side { kMany = 0, kOne = 1, kSideCount = 2 };
constexpr const char* kMetricNames[kMetricCount] = {"read", "write", "lookup"};
constexpr const char* kSideNames[kSideCount] = {"many", "one"};

using RunTimings = std::array<std::array<WindowTiming, kSideCount>, kMetricCount>;

struct World {
    std::unique_ptr<chunkdb::ChunkStore> store;
    Shadow shadow;
    ReplicaMaps replica;
};

void ReadWindow(World& world, const Sequence& sequence, bool timed, WindowTiming* timing) {
    const std::vector<std::uint8_t> expected = world.shadow.Values(sequence);
    std::uint64_t mismatches = 0;
    auto op = [&](std::size_t i) {
        const std::string bits = world.store->GetBlockBits(sequence.block_x[i], sequence.block_y[i]);
        mismatches += bits == kBits[expected[i]] ? 0U : 1U;
    };
    if (timed) {
        *timing = TimeWindow(sequence.chunk.size(), op);
    } else {
        for (std::size_t i = 0; i < sequence.chunk.size(); ++i) {
            op(i);
        }
    }
    if (mismatches != 0) {
        throw std::runtime_error("point reads returned values other than the ones written");
    }
}

void WriteWindow(World& world, const Sequence& sequence, bool timed, WindowTiming* timing) {
    const std::vector<std::uint8_t> values = world.shadow.ApplyWrites(sequence);
    auto op = [&](std::size_t i) {
        world.store->SetBlockBits(sequence.block_x[i], sequence.block_y[i], kBits[values[i]]);
    };
    if (timed) {
        *timing = TimeWindow(sequence.chunk.size(), op);
    } else {
        for (std::size_t i = 0; i < sequence.chunk.size(); ++i) {
            op(i);
        }
    }
}

void LookupWindow(World& world, const Sequence& sequence, bool timed, WindowTiming* timing) {
    const chunkdb::Geometry& geometry = world.store->geometry();
    std::uint64_t expected = 0;
    for (const auto chunk : sequence.chunk) {
        expected += chunk;
    }
    std::uint64_t markers = 0;
    auto op = [&](std::size_t i) { markers += world.replica.Find(geometry, sequence.chunk_coord[i]); };
    if (timed) {
        *timing = TimeWindow(sequence.chunk.size(), op);
    } else {
        for (std::size_t i = 0; i < sequence.chunk.size(); ++i) {
            op(i);
        }
    }
    if (markers != expected) {
        throw std::runtime_error("replica lookups found other chunks than the ones asked for");
    }
}

void RequireUnchanged(const chunkdb::StoreRuntimeStats& before, const chunkdb::StoreRuntimeStats& after) {
    if (after.unique_loaded_chunks != before.unique_loaded_chunks || after.evictions != before.evictions ||
        after.checkpoints != before.checkpoints || after.wal_batch_flushes != before.wal_batch_flushes) {
        throw std::runtime_error(
            "the timed windows loaded, evicted, checkpointed or flushed chunks; the cache was not warm");
    }
}

RunTimings RunOnce(
    const Args& args, const DataDirs& data_dirs, std::size_t run, const Sequence (&sequences)[kSideCount]) {
    // Declared before the store, so the store is closed before its
    // directory is deleted.
    const RunDir dir(data_dirs, run);

    World world{
        .store = std::make_unique<chunkdb::ChunkStore>(BuildStoreConfig(dir.path(), args.chunks)),
        .shadow = Shadow(args.chunks),
        .replica = {},
    };
    const chunkdb::Geometry& geometry = world.store->geometry();

    // Every block present with value 0, chunks loaded in index order.
    std::string chunk_bits;
    for (std::size_t block = 0; block < kBlocksPerChunk; ++block) {
        chunk_bits += kBits[0];
    }
    for (std::uint32_t index = 0; index < args.chunks; ++index) {
        const auto coord = ChunkOf(index);
        world.store->SetChunkBits(coord.x, coord.y, chunk_bits);
        world.replica.Insert(geometry, coord, index);
    }

    // Untimed passes of every window; the barrier empties the WAL batches
    // and keeps their capacity.
    for (int side = 0; side < kSideCount; ++side) {
        WriteWindow(world, sequences[side], false, nullptr);
    }
    world.store->WalBarrier();
    for (int side = 0; side < kSideCount; ++side) {
        ReadWindow(world, sequences[side], false, nullptr);
        LookupWindow(world, sequences[side], false, nullptr);
    }

    const auto before = world.store->RuntimeStats();
    RunTimings timings{};
    const bool many_first = run % 2 == 0;
    for (int order = 0; order < kSideCount; ++order) {
        const int side = (order == 0) == many_first ? kMany : kOne;
        ReadWindow(world, sequences[side], true, &timings[kRead][side]);
    }
    for (int order = 0; order < kSideCount; ++order) {
        const int side = (order == 0) == many_first ? kMany : kOne;
        WriteWindow(world, sequences[side], true, &timings[kWrite][side]);
    }
    for (int order = 0; order < kSideCount; ++order) {
        const int side = (order == 0) == many_first ? kMany : kOne;
        LookupWindow(world, sequences[side], true, &timings[kLookup][side]);
    }
    RequireUnchanged(before, world.store->RuntimeStats());

    return timings;
}

struct Spread {
    double median = 0.0;
    double min = 0.0;
    double max = 0.0;
};

Spread SpreadOf(const std::vector<double>& values) {
    return Spread{
        .median = Median(values),
        .min = *std::min_element(values.begin(), values.end()),
        .max = *std::max_element(values.begin(), values.end()),
    };
}

void PrintSpread(const char* metric, const char* side, std::size_t runs, const Spread& spread) {
    std::cout << "summary metric=" << metric << " side=" << side << " runs=" << runs << std::fixed
              << std::setprecision(1) << " median_ns_per_op=" << spread.median << " min_ns_per_op=" << spread.min
              << " max_ns_per_op=" << spread.max << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Args args = chunkdb::chunk_map_bench::ParseArgs(argc, argv);
        if (args.help) {
            std::cout << chunkdb::chunk_map_bench::kUsage;
            return 0;
        }
        chunkdb::SetLogLevel(chunkdb::LogLevel::kWarn);

        // One sequence of block positions for both sides; the many side
        // draws each operation's chunk from all chunks, the one side keeps
        // chunk 0.
        std::mt19937_64 rng(kSeed);
        std::vector<std::uint32_t> many_chunks(args.ops);
        std::vector<std::uint16_t> locals(args.ops);
        for (std::size_t i = 0; i < args.ops; ++i) {
            many_chunks[i] = static_cast<std::uint32_t>(rng() % args.chunks);
            locals[i] = static_cast<std::uint16_t>(rng() % kBlocksPerChunk);
        }
        const Sequence sequences[kSideCount] = {
            BuildSequence(many_chunks, locals),
            BuildSequence(std::vector<std::uint32_t>(args.ops, 0U), locals),
        };

        std::cout << "machine os=" << OsName() << " arch=" << ArchName() << " cpu=\"" << CpuName() << "\""
                  << " logical_cpus=" << std::thread::hardware_concurrency() << " compiler=\"" << CompilerName()
                  << "\" build_type=" << CHUNKDB_BENCH_BUILD_TYPE << '\n'
                  << "source commit=" << CHUNKDB_BENCH_SOURCE_COMMIT << " state=" << CHUNKDB_BENCH_SOURCE_STATE
                  << '\n'
                  << std::flush;

        std::array<std::array<std::vector<double>, kSideCount + 1>, kMetricCount> values;
        std::set<std::pair<std::int64_t, std::int64_t>> large_chunks;
        for (std::uint32_t index = 0; index < args.chunks; ++index) {
            const auto coord = ChunkOf(index);
            large_chunks.emplace(coord.x / 8, coord.y / 8);
        }
        std::cout << "profile geometry=8x8x" << kChunkSide << "x" << kChunkSide << "x" << kBlockBits << " chunks=" << args.chunks
                  << " large_chunks=" << large_chunks.size() << " grid_width_chunks=" << kGridWidthChunks
                  << " ops=" << args.ops << " segment_ops=" << kSegmentOps << " runs=" << args.runs
                  << " seed=" << kSeed << " durability=relaxed max_loaded_chunks=" << args.chunks * 2
                  << " wal_group_commit_updates=" << kNoThreshold << " checkpoints=off"
                  << " chunk_lock_mode=" << chunkdb::ChunkLockModeName()
                  << " failpoints=" << kFailpoints << '\n';
        const DataDirs data_dirs(args.data_dir, args.keep_data);
        for (std::size_t run = 0; run < args.runs; ++run) {
            const RunTimings timings = RunOnce(args, data_dirs, run, sequences);
            for (int metric = 0; metric < kMetricCount; ++metric) {
                for (int side = 0; side < kSideCount; ++side) {
                    const auto& timing = timings[metric][side];
                    values[metric][side].push_back(timing.ns_per_op);
                    std::cout << "run=" << run + 1 << " order=" << (run % 2 == 0 ? "many-first" : "one-first")
                              << " metric=" << kMetricNames[metric] << " side=" << kSideNames[side] << std::fixed
                              << std::setprecision(1) << " ns_per_op=" << timing.ns_per_op
                              << " window_mean_ns_per_op=" << timing.mean_ns_per_op << '\n';
                }
                values[metric][kSideCount].push_back(timings[metric][kMany].ns_per_op - timings[metric][kOne].ns_per_op);
            }
            std::cout << std::flush;
        }

        for (int metric = 0; metric < kMetricCount; ++metric) {
            for (int side = 0; side < kSideCount; ++side) {
                PrintSpread(kMetricNames[metric], kSideNames[side], args.runs, SpreadOf(values[metric][side]));
            }
            PrintSpread(kMetricNames[metric], "many-minus-one", args.runs, SpreadOf(values[metric][kSideCount]));
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "benchmark failed: " << e.what() << std::endl;
        return 1;
    }
}
