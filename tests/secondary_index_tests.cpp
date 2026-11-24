#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/wait.h>
#endif

#include "chunk_store_internal.hpp"
#include "secondary_index.hpp"
#include "test_utils.hpp"

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;
constexpr std::size_t kBlocks = 16U;

Column Make(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t size, bool nullable = false) {
    Column column;
    column.id = id;
    column.name = std::move(name);
    column.type = ColumnType{.kind = kind, .size = size};
    column.nullable = nullable;
    return column;
}

TableSchema Schema() {
    TableSchema schema;
    schema.next_column_id = 7U;
    schema.columns = {Make(1U, "u", ColumnKind::kUnsigned, 16U), Make(2U, "i", ColumnKind::kSigned, 8U, true),
        Make(3U, "f", ColumnKind::kFloat32, 32U), Make(4U, "t", ColumnKind::kText, 8U, true),
        Make(5U, "b", ColumnKind::kBytes, 300U), Make(6U, "flag", ColumnKind::kBool, 1U)};
    return schema;
}

std::size_t ColumnIndex(const ChunkLayout& layout, std::string_view name) {
    const auto index = layout.FindColumn(name);
    assert(index != static_cast<std::size_t>(-1));
    return index;
}

// One block's values; a missing one is NULL (or absent text and bytes).
struct Block {
    std::uint64_t u = 0U;
    std::optional<std::int64_t> i;
    float f = 0.0F;
    std::optional<std::string> t;
    std::optional<std::string> b;
    bool flag = false;
};

ChunkState State(const ChunkLayout& layout, std::uint64_t revision, const std::map<std::uint32_t, Block>& blocks) {
    ChunkState state;
    state.version = revision;
    state.written = true;
    state.payload.assign(layout.payload_bytes(), 0U);
    state.presence_bitmap.assign((kBlocks + 7U) / 8U, 0U);
    std::vector<VarChange> vars;
    const auto& columns = layout.schema().columns;
    const auto put = [&](std::size_t index, std::uint32_t block, const ColumnValue& value) {
        const auto* fixed = layout.FixedColumnAt(index);
        const auto bytes = EncodeColumnValue(columns[index], value);
        WriteValueBits(state.payload.data(), fixed->values * 8U + block * fixed->width, bytes.data(), fixed->width);
        if (fixed->validity != ChunkLayout::kNoValidity)
            state.payload[fixed->validity + block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
    };
    for (const auto& [block, values] : blocks) {
        state.presence_bitmap[block / 8U] |= static_cast<std::uint8_t>(1U << (block % 8U));
        put(ColumnIndex(layout, "u"), block, values.u);
        if (values.i) put(ColumnIndex(layout, "i"), block, *values.i);
        put(ColumnIndex(layout, "f"), block, values.f);
        put(ColumnIndex(layout, "flag"), block, values.flag);
        if (values.t) vars.push_back({{4U, block}, Bytes(values.t->begin(), values.t->end())});
        if (values.b && !values.b->empty()) vars.push_back({{5U, block}, Bytes(values.b->begin(), values.b->end())});
    }
    std::sort(vars.begin(), vars.end(), [](const VarChange& a, const VarChange& b) { return a.key < b.key; });
    state.vars = ChunkVars::Merge(ChunkVars{}, vars);
    return state;
}

// The chunks a table holds, as its coordinate index and chunk files would
// report them.
struct Model {
    struct Chunk {
        std::uint64_t revision = 0U;
        bool populated = false;
        ChunkState state;
    };
    std::map<ChunkCoord, Chunk> chunks;
    std::uint64_t clock = 0U;
    std::uint32_t seed = 7U;

    std::uint32_t Next(std::uint32_t bound) {
        seed = seed * 1664525U + 1013904223U;
        return (seed >> 8U) % bound;
    }
    void Write(const ChunkLayout& layout, const ChunkCoord& coord) {
        std::map<std::uint32_t, Block> blocks;
        for (std::uint32_t block = 0U; block < kBlocks; ++block) {
            if (Next(3U) == 0U) continue;
            Block values;
            values.u = Next(50U);
            if (Next(4U) != 0U) values.i = static_cast<std::int64_t>(Next(256U)) - 128;
            values.f = Next(5U) == 0U ? std::numeric_limits<float>::quiet_NaN() : static_cast<float>(Next(40U)) / 4.0F - 5.0F;
            if (Next(3U) != 0U) values.t = std::string(1U + Next(4U), static_cast<char>('a' + Next(3U)));
            if (Next(2U) != 0U) values.b = std::string(Next(20U), static_cast<char>(Next(3U)));
            values.flag = Next(2U) != 0U;
            blocks.emplace(block, values);
        }
        auto& chunk = chunks[coord];
        chunk.revision = ++clock;
        chunk.populated = !blocks.empty();
        chunk.state = chunk.populated ? State(layout, chunk.revision, blocks) : ChunkState{.version = chunk.revision};
    }
    void Empty(const ChunkCoord& coord) {
        auto& chunk = chunks[coord];
        chunk.revision = ++clock;
        chunk.populated = false;
        chunk.state = ChunkState{.version = chunk.revision};
    }
    IndexChunkSource Source() const {
        return IndexChunkSource{
            .for_each = [this](const std::function<void(const ChunkCoord&, std::uint64_t, bool)>& visit) {
                for (const auto& [coord, chunk] : chunks) visit(coord, chunk.revision, chunk.populated);
            },
            .read = [this](const ChunkCoord& coord) { return chunks.at(coord).state; },
        };
    }
};

std::vector<IndexEntry> Expected(const ChunkLayout& layout, std::size_t column, std::uint32_t prefix, const Model& model) {
    std::vector<IndexEntry> all;
    for (const auto& [coord, chunk] : model.chunks) {
        if (!chunk.populated) continue;
        auto entries = ChunkIndexEntries(layout, column, prefix, coord, chunk.state.presence_bitmap, chunk.state.payload,
            chunk.state.vars);
        all.insert(all.end(), entries.begin(), entries.end());
    }
    std::sort(all.begin(), all.end(), IndexEntryLess);
    return all;
}

std::vector<IndexEntry> Effective(const SecondaryIndex& index) {
    std::vector<IndexEntry> all;
    index.ForEach([&](const IndexEntry& entry) {
        all.push_back(entry);
        return true;
    });
    return all;
}

bool Same(const std::vector<IndexEntry>& a, const std::vector<IndexEntry>& b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](const IndexEntry& x, const IndexEntry& y) {
        return x.key == y.key && x.coord == y.coord && x.block == y.block && x.removed == y.removed;
    });
}

IndexIdentity Identity(std::uint32_t column_id, std::uint32_t prefix = 0U, std::uint8_t epoch = 1U, std::uint8_t dims = 2U) {
    IndexIdentity identity;
    identity.epoch[0] = epoch;
    identity.dims = dims;
    identity.index_id = 1U;
    identity.column_id = column_id;
    identity.prefix = prefix;
    return identity;
}

void Reject(const std::function<void()>& action) {
    bool refused = false;
    try { action(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

Bytes FixedKey(const Column& column, const ColumnValue& value) {
    Bytes key;
    const auto bytes = EncodeColumnValue(column, value);
    assert(AppendFixedIndexKey(column, bytes.data(), &key));
    return key;
}

void KeysKeepValueOrder() {
    const auto u16 = Make(1U, "u", ColumnKind::kUnsigned, 16U);
    std::vector<Bytes> keys;
    for (const std::uint64_t value : {0ULL, 1ULL, 255ULL, 256ULL, 65535ULL}) keys.push_back(FixedKey(u16, value));
    assert(std::is_sorted(keys.begin(), keys.end()) && std::adjacent_find(keys.begin(), keys.end()) == keys.end());
    for (const std::uint32_t width : {2U, 8U, 13U, 64U}) {
        const auto column = Make(2U, "i", ColumnKind::kSigned, width);
        const auto low = width == 64U ? std::numeric_limits<std::int64_t>::min() : -(std::int64_t{1} << (width - 1U));
        const auto high = width == 64U ? std::numeric_limits<std::int64_t>::max() : (std::int64_t{1} << (width - 1U)) - 1;
        keys.clear();
        for (const std::int64_t value : {low, std::int64_t{-1}, std::int64_t{0}, std::int64_t{1}, high})
            keys.push_back(FixedKey(column, value));
        assert(std::is_sorted(keys.begin(), keys.end()) && std::adjacent_find(keys.begin(), keys.end()) == keys.end());
    }
    const auto f32 = Make(3U, "f", ColumnKind::kFloat32, 32U);
    const auto f64 = Make(3U, "g", ColumnKind::kFloat64, 64U);
    const float floats[]{-std::numeric_limits<float>::infinity(), -2.5F, -1e-30F, 0.0F, 1e-30F, 3.0F,
        std::numeric_limits<float>::infinity()};
    keys.clear();
    for (const float value : floats) keys.push_back(FixedKey(f32, value));
    assert(std::is_sorted(keys.begin(), keys.end()) && std::adjacent_find(keys.begin(), keys.end()) == keys.end());
    assert(FixedKey(f32, -0.0F) == FixedKey(f32, 0.0F));
    assert(FixedKey(f64, -0.0) == FixedKey(f64, 0.0));
    assert(FixedKey(f64, -1.0) < FixedKey(f64, -0.5) && FixedKey(f64, 0.5) < FixedKey(f64, 1.0));
    Bytes none;
    const auto nan = EncodeColumnValue(f32, std::numeric_limits<float>::quiet_NaN());
    assert(!AppendFixedIndexKey(f32, nan.data(), &none) && none.empty());
    const auto nan64 = EncodeColumnValue(f64, std::numeric_limits<double>::quiet_NaN());
    assert(!AppendFixedIndexKey(f64, nan64.data(), &none) && none.empty());
    const auto flag = Make(6U, "flag", ColumnKind::kBool, 1U);
    assert(FixedKey(flag, false) < FixedKey(flag, true));

    // Shorter values first, and a zero byte inside a value keeps its order.
    const std::vector<std::string> texts{"", std::string(1, '\0'), std::string(2, '\0'), "a", std::string("a\0", 2),
        "ab", "b"};
    keys.clear();
    for (const auto& text : texts) {
        Bytes key;
        AppendVarIndexKey(std::span(reinterpret_cast<const std::uint8_t*>(text.data()), text.size()), 0U, &key);
        keys.push_back(key);
    }
    assert(std::is_sorted(keys.begin(), keys.end()) && std::adjacent_find(keys.begin(), keys.end()) == keys.end());
    Bytes cut, whole;
    const std::string long_value = "abcdef", short_value = "abc";
    AppendVarIndexKey(std::span(reinterpret_cast<const std::uint8_t*>(long_value.data()), long_value.size()), 3U, &cut);
    AppendVarIndexKey(std::span(reinterpret_cast<const std::uint8_t*>(short_value.data()), short_value.size()), 0U, &whole);
    assert(cut == whole);

    RequireIndexableColumn(Make(5U, "b", ColumnKind::kBytes, 300U), 16U);
    RequireIndexableColumn(Make(4U, "t", ColumnKind::kText, 256U), 0U);
    RequireIndexableColumn(Make(7U, "bits", ColumnKind::kBits, 2048U), 0U);
    Reject([] { RequireIndexableColumn(Make(5U, "b", ColumnKind::kBytes, 300U), 0U); });
    Reject([] { RequireIndexableColumn(Make(5U, "b", ColumnKind::kBytes, 300U), 257U); });
    Reject([] { RequireIndexableColumn(Make(1U, "u", ColumnKind::kUnsigned, 16U), 4U); });
    Reject([] { RequireIndexableColumn(Make(7U, "bits", ColumnKind::kBits, 2056U), 0U); });
}

void EntriesSkipNullNanAndAbsentBlocks() {
    const ChunkLayout layout(Schema(), kBlocks, 1U << 20U);
    std::map<std::uint32_t, Block> blocks;
    blocks[1] = Block{.u = 9U, .i = -3, .f = std::numeric_limits<float>::quiet_NaN(), .t = "x", .b = "zz"};
    blocks[4] = Block{.u = 2U, .f = 1.5F};
    const auto state = State(layout, 5U, blocks);
    const ChunkCoord coord{3, -2, 0};
    const auto entries = [&](std::string_view name, std::uint32_t prefix = 0U) {
        return ChunkIndexEntries(layout, ColumnIndex(layout, name), prefix, coord, state.presence_bitmap, state.payload,
            state.vars);
    };
    const auto u = entries("u");
    assert(u.size() == 2U && u[0].block == 4U && u[1].block == 1U && u[0].coord == coord);
    assert(entries("i").size() == 1U && entries("i")[0].block == 1U);
    assert(entries("f").size() == 1U && entries("f")[0].block == 4U);
    assert(entries("t").size() == 1U && entries("t")[0].block == 1U);
    // bytes cannot be NULL: block 4 has the empty value, which sorts first.
    const auto b = entries("b", 1U);
    assert(b.size() == 2U && b[0].block == 4U && b[1].block == 1U);
    assert((b[1].key == Bytes{'z', 0U, 0U}));
    assert(ChunkIndexEntries(layout, 0U, 0U, coord, {}, {}, ChunkVars{}).empty());
}

void ManifestRoundTripAndDamage() {
    IndexManifest manifest;
    manifest.identity = Identity(5U, 16U, 3U, 3U);
    manifest.identity.schema_version = 4U;
    manifest.state = IndexState::kReady;
    manifest.generation = 9U;
    manifest.runs = {{.generation = 9U, .bytes = 100U}, {.generation = 4U, .bytes = 50U}};
    manifest.runs[0].sha256[0] = 1U;
    const auto bytes = SerializeIndexManifest(manifest);
    const auto parsed = ParseIndexManifest(bytes);
    assert(parsed.identity == manifest.identity && parsed.state == manifest.state && parsed.generation == 9U);
    assert(parsed.runs == manifest.runs);
    for (std::size_t at = 0U; at < bytes.size(); at += 7U) {
        auto damaged = bytes;
        damaged[at] ^= 0x10U;
        bool refused = false;
        try { (void)ParseIndexManifest(damaged); } catch (const std::runtime_error&) { refused = true; }
        assert(refused);
    }
    bool refused = false;
    try { (void)ParseIndexManifest(std::span(bytes).first(bytes.size() - 1U)); } catch (const std::runtime_error&) { refused = true; }
    assert(refused);
}

std::vector<std::filesystem::path> Files(const std::filesystem::path& directory) {
    std::vector<std::filesystem::path> files;
    for (const auto& item : std::filesystem::directory_iterator(directory)) files.push_back(item.path().filename());
    std::sort(files.begin(), files.end());
    return files;
}

void FlipByte(const std::filesystem::path& path, std::uint64_t at) {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekg(static_cast<std::streamoff>(at));
    char byte = 0;
    file.read(&byte, 1);
    byte = static_cast<char>(byte ^ 0x40);
    file.seekp(static_cast<std::streamoff>(at));
    file.write(&byte, 1);
}

void Populate(const ChunkLayout& layout, Model& model, std::uint8_t dims) {
    for (std::int64_t x = 0; x < 30; ++x)
        for (std::int64_t y = 0; y < 10; ++y) model.Write(layout, {x, y, dims == 3U ? (x + y) % 3 - 1 : 0});
}

// Open builds, reopening recomputes only what changed, flushes stay bounded
// and merge, damage or another epoch rebuilds, and verify checks it all.
void LayersFollowChunkState() {
    for (const std::uint8_t dims : {std::uint8_t{2U}, std::uint8_t{3U}}) {
        for (const auto& [name, prefix] : {std::pair<std::string, std::uint32_t>{"u", 0U}, {"f", 0U}, {"b", 4U}}) {
            test::ScopedTempDir directory("chunkdb-secondary-index");
            const ChunkLayout layout(Schema(), kBlocks, 1U << 20U);
            const auto column = ColumnIndex(layout, name);
            const auto identity = Identity(layout.schema().columns[column].id, prefix, 1U, dims);
            const auto path = directory.path() / std::string(kIndexesDirectoryName) / "1";
            Model model;
            Populate(layout, model, dims);
            const auto open = [&](const IndexIdentity& with) {
                auto index = std::make_unique<SecondaryIndex>(path, layout, column, with);
                const auto report = index->Open(model.Source());
                return std::make_pair(std::move(index), report);
            };
            {
                auto [index, report] = open(identity);
                assert(!report.rebuilt.empty() && index->state() == IndexState::kReady && index->runs() == 1U);
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
                assert(index->Verify(model.Source()).empty());
            }
            {
                auto [index, report] = open(identity);
                assert(report.rebuilt.empty() && report.recomputed == 0U && index->delta_chunks() == 0U);
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
            }
            // Chunks written, emptied or gone since the runs: only they are recomputed.
            std::size_t changed = 0U;
            for (auto at = model.chunks.begin(); at != model.chunks.end(); ++at, ++changed) {
                if (changed == 20U) break;
                model.Write(layout, at->first);
            }
            std::vector<ChunkCoord> coords;
            for (const auto& [coord, chunk] : model.chunks) coords.push_back(coord);
            for (std::size_t i = 0U; i < 5U; ++i) model.Empty(coords[100U + i]);
            for (std::size_t i = 0U; i < 3U; ++i) model.chunks.erase(coords[200U + i]);
            {
                auto [index, report] = open(identity);
                assert(report.rebuilt.empty() && report.recomputed == 28U);
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
                assert(index->Verify(model.Source()).empty());
                // Each flush adds a run; merges keep them bounded.
                for (std::size_t round = 0U; round < 3U * kMaxIndexRuns; ++round) {
                    const auto& coord = coords[round % 50U];
                    model.Write(layout, coord);
                    const auto& chunk = model.chunks.at(coord);
                    if (chunk.populated)
                        index->Replace(coord, chunk.revision, ChunkIndexEntries(layout, column, prefix, coord,
                            chunk.state.presence_bitmap, chunk.state.payload, chunk.state.vars));
                    else index->Forget(coord);
                    index->Flush();
                    assert(index->runs() <= kMaxIndexRuns && index->delta_chunks() == 0U);
                    assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
                }
                assert(index->Verify(model.Source()).empty());
            }
            {
                auto [index, report] = open(identity);
                assert(report.rebuilt.empty() && report.recomputed == 0U);
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
            }
            // Files the manifest does not list are removed.
            { std::ofstream(path / "run-999999") << "x"; std::ofstream(path / "stray") << "x"; }
            {
                auto [index, report] = open(identity);
                assert(report.rebuilt.empty());
                for (const auto& file : Files(path)) assert(file != "run-999999" && file != "stray");
            }
            // A run whose chunk table is damaged, or that is truncated, is rebuilt.
            const auto run = [&] {
                for (const auto& file : Files(path))
                    if (file.string().starts_with("run-")) return path / file;
                return path;
            };
            FlipByte(run(), 70U);
            {
                auto [index, report] = open(identity);
                assert(!report.rebuilt.empty());
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
            }
            std::filesystem::resize_file(run(), std::filesystem::file_size(run()) - 1U);
            {
                auto [index, report] = open(identity);
                assert(!report.rebuilt.empty() && index->Verify(model.Source()).empty());
            }
            // An entry block is checked by verify, which reports the run.
            FlipByte(run(), std::filesystem::file_size(run()) / 2U);
            {
                auto index = std::make_unique<SecondaryIndex>(path, layout, column, identity);
                const auto report = index->Open(model.Source());
                assert(report.rebuilt.empty() && !index->Verify(model.Source()).empty());
                index->Rebuild(model.Source());
                assert(index->Verify(model.Source()).empty());
            }
            // Another table epoch, or a missing manifest, rebuilds.
            {
                auto [index, report] = open(Identity(identity.column_id, prefix, 2U, dims));
                assert(!report.rebuilt.empty() && index->Verify(model.Source()).empty());
            }
            std::filesystem::remove(SecondaryIndex::ManifestPath(path));
            {
                auto [index, report] = open(Identity(identity.column_id, prefix, 2U, dims));
                assert(!report.rebuilt.empty() && index->runs() == 1U);
                assert(Same(Effective(*index), Expected(layout, column, prefix, model)));
            }
        }
    }
}

// The deterministic chunk state of the crash scenario at each step.
void CrashModel(const ChunkLayout& layout, Model& model, int steps) {
    Populate(layout, model, 2U);
    if (steps < 1) return;
    std::size_t at = 0U;
    for (auto& [coord, chunk] : model.chunks) {
        (void)chunk;
        if (at++ % 7U == 0U) model.Write(layout, coord);
    }
}

int CrashChildBody(const std::filesystem::path& root, const char* point) {
    const ChunkLayout layout(Schema(), kBlocks, 1U << 20U);
    const auto column = ColumnIndex(layout, "u");
    Model model;
    CrashModel(layout, model, 0);
    SecondaryIndex index(root, layout, column, Identity(1U));
    (void)index.Open(model.Source());
    Model changed;
    CrashModel(layout, changed, 1);
    for (const auto& [coord, chunk] : changed.chunks) {
        if (chunk.revision == model.chunks.at(coord).revision) continue;
        if (chunk.populated)
            index.Replace(coord, chunk.revision, ChunkIndexEntries(layout, column, 0U, coord, chunk.state.presence_bitmap,
                chunk.state.payload, chunk.state.vars));
        else index.Forget(coord);
    }
#ifdef _WIN32
    _putenv_s(point, "1");
#else
    setenv(point, "1", 1);
#endif
    if (std::string_view(point).find("MERGE") != std::string_view::npos) index.Merge();
    else index.Flush();
    return 1;
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
    const auto command = Quote(std::filesystem::absolute(executable).string()) + " --crash " + Quote(root.string()) +
        " " + Quote(point);
#ifdef _WIN32
    return std::system(('"' + command + '"').c_str());
#else
    const auto result = std::system(command.c_str());
    assert(result != -1 && WIFEXITED(result));
    return WEXITSTATUS(result);
#endif
}

// A crash after a run is in place but before its manifest, or after a merge's
// manifest but before the replaced runs are removed, recovers the index of
// the current chunk state with no stray file.
void CrashBoundaries(const char* executable) {
    if (!kFailpointsCompiled) return;
    for (const auto* point : {"CHUNKDB_FAILPOINT_CRASH_INDEX_RUN_AFTER_RENAME_ONCE",
             "CHUNKDB_FAILPOINT_CRASH_INDEX_MERGE_AFTER_MANIFEST_ONCE"}) {
        test::ScopedTempDir directory("chunkdb-secondary-index-crash");
        const auto root = directory.path() / "index";
        assert(CrashChild(executable, root, point) == 86);
        const ChunkLayout layout(Schema(), kBlocks, 1U << 20U);
        const auto column = ColumnIndex(layout, "u");
        Model model;
        CrashModel(layout, model, 1);
        SecondaryIndex index(root, layout, column, Identity(1U));
        const auto report = index.Open(model.Source());
        assert(report.rebuilt.empty());
        assert(Same(Effective(index), Expected(layout, column, 0U, model)));
        assert(index.Verify(model.Source()).empty());
        std::size_t runs = 0U;
        for (const auto& file : Files(root)) {
            assert(file == "manifest" || file.string().starts_with("run-"));
            if (file != "manifest") ++runs;
        }
        assert(runs == index.runs());
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string_view(argv[1]) == "--crash") return CrashChildBody(argv[2], argv[3]);
    if (argc == 2 && std::string_view(argv[1]) == "--crash-boundaries") {
        CrashBoundaries(argv[0]);
        return 0;
    }
    KeysKeepValueOrder();
    EntriesSkipNullNanAndAbsentBlocks();
    ManifestRoundTripAndDamage();
    LayersFollowChunkState();
}
