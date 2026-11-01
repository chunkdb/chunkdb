#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>

#include "chunkdb/chunk_columns.hpp"
#include "typed_chunk_form.hpp"

namespace allocation {
std::atomic<bool> deny_large{false};
}

// Keep replacement allocation and deallocation as one opaque matched family.
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void* operator new(std::size_t size) {
    if (allocation::deny_large.load(std::memory_order_relaxed) && size >= 65536U) throw std::bad_alloc();
    auto* memory = std::malloc(size == 0U ? 1U : size);
    if (memory == nullptr) throw std::bad_alloc();
    return memory;
}
#if defined(__GNUC__)
__attribute__((noinline))
#endif
void operator delete(void* pointer) noexcept { std::free(pointer); }
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete[](void* pointer) noexcept { ::operator delete(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { ::operator delete(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { ::operator delete(pointer); }

namespace {
using namespace chunkdb;
using Bytes = std::vector<std::uint8_t>;

Column Col(std::uint32_t id, std::string name, ColumnKind kind, std::uint32_t width, bool nullable = false) {
    Column column;
    column.id = id; column.name = std::move(name); column.type = {kind, width}; column.nullable = nullable;
    return column;
}
TableSchema Schema(std::vector<Column> columns) {
    TableSchema schema;
    schema.columns = std::move(columns);
    for (const auto& column : schema.columns) schema.next_column_id = column.id + 1U;
    return schema;
}
ChunkState Empty(const Geometry& geometry) {
    ChunkState state;
    state.payload.resize(geometry.ChunkPayloadBytes(), 0U);
    state.presence_bitmap.resize((geometry.ChunkBlockCount() + 7U) / 8U, 0U);
    state.chunk_columns = geometry.default_chunk_columns();
    return state;
}

template <typename Call> void Invalid(Call call) {
    bool refused = false;
    try { call(); } catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

void ExactSharedPlans() {
    GeometryConfig config;
    config.chunk_width_blocks = 9U; config.chunk_height_blocks = 1U;
    const auto blocks = Schema({Col(1U, "small", ColumnKind::kUnsigned, 3U, true),
        Col(2U, "wide", ColumnKind::kUnsigned, 9U), Col(3U, "text", ColumnKind::kText, 64U, true)});
    config.block_bits = FixedBitsPerBlock(blocks);
    const Geometry geometry(config, blocks,
        Schema({Col(4U, "owner", ColumnKind::kUnsigned, 16U), Col(5U, "tag", ColumnKind::kText, 128U, true)}));
    auto state = Empty(geometry);
    state.written = true; state.version = 31U; state.commit_time_ms = 47U;
    state.presence_bitmap = {1U, 1U};
    state.vars.Assign({3U, 0U}, Bytes{'a', 0U, 'b'});
    state.vars.Assign({3U, 8U}, Bytes{});
    state.chunk_columns = std::make_shared<const ChunkColumnState>(EncodeChunkColumns(geometry.chunk_schema(),
        {std::uint64_t{17U}, std::string("metadata")}));
    for (const auto& projection : {std::vector<std::size_t>{}, {0U}, {2U}, {2U, 1U, 0U}}) {
        const auto size = SectionedChunkStateBytes(geometry, state, kVarMaxChunkBytesLimit, projection);
        for (const bool private_state : {false, true}) {
            const auto encoded = EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection, private_state, size);
            assert(encoded.size() == size);
            Invalid([&] { (void)EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, projection, private_state, size - 1U); });
        }
    }
    auto broken = state;
    broken.payload.pop_back();
    Invalid([&] { (void)SectionedChunkStateBytes(geometry, broken, kVarMaxChunkBytesLimit); });
    Invalid([&] { (void)EncodeSectionedChunkState(geometry, broken, kVarMaxChunkBytesLimit); });
    Invalid([&] { (void)SectionedChunkStateBytes(geometry, state, 1U); });
}

void RefusalPrecedesBodyAllocation() {
    GeometryConfig config;
    config.chunk_width_blocks = 1024U; config.chunk_height_blocks = 1024U; config.block_bits = 8U;
    const Geometry geometry(config);
    const auto state = Empty(geometry);
    allocation::deny_large.store(true, std::memory_order_relaxed);
    const auto size = SectionedChunkStateBytes(geometry, state, kVarMaxChunkBytesLimit);
    assert(size > 1024U * 1024U);
    Invalid([&] { (void)EncodeSectionedChunkState(geometry, state, kVarMaxChunkBytesLimit, {}, false, 1024U); });
    allocation::deny_large.store(false, std::memory_order_relaxed);
}
}  // namespace

int main() {
    ExactSharedPlans();
    RefusalPrecedesBodyAllocation();
}
