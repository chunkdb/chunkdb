#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "chunkdb/crc32.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"

namespace chunkdb::fuzz {
using Bytes = std::vector<std::uint8_t>;
inline constexpr StoreId kId{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
inline constexpr ChunkCoord kCoord{-3, 7};
inline constexpr std::size_t kMaxInput = 1024U * 1024U;

inline TableSchema TypedSchema() {
    TableSchema schema;
    schema.version = 1; schema.next_column_id = 4;
    schema.columns = {
        Column{.id = 1, .name = "n", .type = {ColumnKind::kUnsigned, 1}, .nullable = true},
        Column{.id = 2, .name = "label", .type = {ColumnKind::kText, 64}, .nullable = true},
        Column{.id = 3, .name = "data", .type = {ColumnKind::kBytes, 64}},
    };
    return schema;
}
inline const Geometry& Model(unsigned index) {
    // Padded fixed bits, a byte-aligned layout, and nullable variable columns.
    static const std::array<Geometry, 3> models{
        Geometry({2, 2, 3, 3, 5}), Geometry({2, 2, 4, 4, 8}),
        Geometry({2, 2, 3, 3, 1}, TypedSchema())};
    return models[index % models.size()];
}
// Only documented malformed-input exceptions are expected. Allocation,
// out-of-range, assertion and sanitizer failures remain visible to libFuzzer.
template <typename Function> void Rejectable(Function&& function) {
    try { function(); }
    catch (const std::invalid_argument&) {}
    catch (const std::runtime_error&) {}
}
inline std::string_view Text(const std::uint8_t* data, std::size_t size) {
    return {reinterpret_cast<const char*>(data), size};
}
inline std::uint32_t Le32(const Bytes& bytes, std::size_t offset) {
    std::uint32_t value = 0;
    for (unsigned i = 0; i < 4; ++i) value |= std::uint32_t(bytes[offset + i]) << (8U * i);
    return value;
}
inline void Put32(Bytes& bytes, std::size_t offset, std::uint32_t value) {
    for (unsigned i = 0; i < 4; ++i) bytes[offset + i] = static_cast<std::uint8_t>(value >> (8U * i));
}
inline void RepairTrailingCrc(Bytes& bytes) {
    if (bytes.size() >= 4) Put32(bytes, bytes.size() - 4, Crc32(bytes.data(), bytes.size() - 4));
}
}  // namespace chunkdb::fuzz
