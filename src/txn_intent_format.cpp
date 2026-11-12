#include "txn_intent_format.hpp"

#include <algorithm>
#include <bit>
#include <limits>
#include <stdexcept>

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace chunkdb {
namespace {

constexpr std::size_t kFixedBytes = 40;
constexpr std::size_t kEntriesOffset = 36;

void RequireDimensions(std::uint8_t dims) {
    if (dims != 2 && dims != 3) throw std::invalid_argument("transaction intent dimensions must be 2 or 3");
}

std::size_t CheckedSize(std::size_t count, std::uint8_t dims, std::size_t written_chunk_limit) {
    RequireDimensions(dims);
    if (count == 0 || count > written_chunk_limit || count > std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument("transaction intent count exceeds written-chunk limit");
    const auto entry_bytes = 8U * static_cast<std::size_t>(dims) + 8U;
    if (count > (std::numeric_limits<std::size_t>::max() - kFixedBytes) / entry_bytes)
        throw std::invalid_argument("transaction intent size overflows");
    return kFixedBytes + count * entry_bytes;
}

std::uint64_t ReadLe(const std::uint8_t* at, unsigned width) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= static_cast<std::uint64_t>(at[i]) << (8U * i);
    return value;
}

void WriteLe(std::vector<std::uint8_t>* out, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}

ChunkCoord ReadCoord(const std::uint8_t* at, std::uint8_t dims) noexcept {
    return {std::bit_cast<std::int64_t>(ReadLe(at, 8)),
            std::bit_cast<std::int64_t>(ReadLe(at + 8, 8)),
            dims == 3 ? std::bit_cast<std::int64_t>(ReadLe(at + 16, 8)) : 0};
}

void RequireOrder(const ChunkCoord& coord, const ChunkCoord& previous, std::size_t index) {
    if (index != 0 && !(previous < coord))
        throw std::invalid_argument("transaction intent coordinates must be unique and increasing");
}

}  // namespace

std::vector<std::uint8_t> SerializeCheckedTxnIntent(
    const TxnIntent& intent, std::uint8_t dims, const StoreId& table_id,
    std::size_t written_chunk_limit) {
    const auto size = CheckedSize(intent.entries.size(), dims, written_chunk_limit);
    if (intent.version == 0) throw std::invalid_argument("transaction intent revision is zero");
    if (intent.state != TxnIntentState::kRollback && intent.state != TxnIntentState::kCommitted)
        throw std::invalid_argument("transaction intent state is invalid");
    for (std::size_t i = 0; i < intent.entries.size(); ++i) {
        const auto coord = intent.entries[i].coord;
        if (dims == 2 && coord.z != 0) throw std::invalid_argument("inactive transaction intent z coordinate must be 0");
        RequireOrder(coord, i == 0 ? ChunkCoord{} : intent.entries[i - 1].coord, i);
    }
    std::vector<std::uint8_t> bytes;
    bytes.reserve(size);
    bytes.insert(bytes.end(), {'C', 'K', 'T', 'I'});
    WriteLe(&bytes, 2, 2);
    bytes.push_back(intent.state == TxnIntentState::kRollback ? 0 : 1);
    bytes.push_back(dims);
    bytes.insert(bytes.end(), table_id.begin(), table_id.end());
    WriteLe(&bytes, intent.version, 8);
    WriteLe(&bytes, intent.entries.size(), 4);
    for (const auto& entry : intent.entries) {
        WriteLe(&bytes, std::bit_cast<std::uint64_t>(entry.coord.x), 8);
        WriteLe(&bytes, std::bit_cast<std::uint64_t>(entry.coord.y), 8);
        if (dims == 3) WriteLe(&bytes, std::bit_cast<std::uint64_t>(entry.coord.z), 8);
        WriteLe(&bytes, entry.wal_boundary, 8);
    }
    WriteLe(&bytes, Crc32(bytes.data(), bytes.size()), 4);
    return bytes;
}

TxnIntent ParseCheckedTxnIntent(
    std::span<const std::uint8_t> bytes, std::uint8_t expected_dims,
    const StoreId& expected_table_id, std::size_t written_chunk_limit) {
    RequireDimensions(expected_dims);
    if (bytes.size() < kFixedBytes) throw std::invalid_argument("transaction intent is truncated");
    if (bytes[0] != 'C' || bytes[1] != 'K' || bytes[2] != 'T' || bytes[3] != 'I')
        throw std::invalid_argument("not a CKTI transaction intent");
    if (ReadLe(bytes.data() + 4, 2) != 2) throw std::invalid_argument("unsupported transaction intent version");
    if (bytes[6] > 1) throw std::invalid_argument("transaction intent state is invalid");
    const auto dims = bytes[7];
    RequireDimensions(dims);
    if (dims != expected_dims) throw std::invalid_argument("transaction intent dimensions differ from table");
    if (!std::equal(expected_table_id.begin(), expected_table_id.end(), bytes.begin() + 8))
        throw std::invalid_argument("transaction intent belongs to another table");
    const auto revision = ReadLe(bytes.data() + 24, 8);
    if (revision == 0) throw std::invalid_argument("transaction intent revision is zero");
    const auto count = static_cast<std::size_t>(ReadLe(bytes.data() + 32, 4));
    if (bytes.size() != CheckedSize(count, dims, written_chunk_limit))
        throw std::invalid_argument("transaction intent has wrong length");
    if (ReadLe(bytes.data() + bytes.size() - 4, 4) != Crc32(bytes.data(), bytes.size() - 4))
        throw std::invalid_argument("transaction intent checksum mismatch");
    const auto entry_bytes = 8U * static_cast<std::size_t>(dims) + 8U;
    ChunkCoord previous;
    for (std::size_t i = 0; i < count; ++i) {
        const auto coord = ReadCoord(bytes.data() + kEntriesOffset + i * entry_bytes, dims);
        RequireOrder(coord, previous, i);
        previous = coord;
    }
    TxnIntent intent{
        .state = bytes[6] == 0 ? TxnIntentState::kRollback : TxnIntentState::kCommitted,
        .version = revision,
        .entries = {},
    };
    intent.entries.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        const auto* entry = bytes.data() + kEntriesOffset + i * entry_bytes;
        intent.entries.push_back({ReadCoord(entry, dims), ReadLe(entry + 8U * dims, 8)});
    }
    return intent;
}

std::string CanonicalConditionalIntentFileName(const Geometry& geometry, const ChunkCoord& coord) {
    const auto relative = ChunkWalPath({}, geometry, coord).generic_string();
    std::string name;
    name.reserve(relative.size() + 10U);
    for (const auto c : relative) {
        if (c == '/') name += "__";
        else name += c;
    }
    return name + ".rollback";
}

bool ParseConditionalIntentFileName(std::string_view name, const Geometry& geometry, ChunkCoord* out) {
    // Two coordinate stems, at most 20 chars per signed i64, their axis
    // separators, the flattened directory separator and both suffixes.
    if (out == nullptr || name.size() > 17U + 42U * geometry.dims() || !name.ends_with(".rollback"))
        return false;
    const auto flattened = name.substr(0, name.size() - 9U);
    const auto separator = flattened.find("__");
    if (separator == std::string_view::npos || flattened.find("__", separator + 2U) != std::string_view::npos)
        return false;
    const auto relative = std::filesystem::path(flattened.substr(0, separator)) /
                          std::filesystem::path(flattened.substr(separator + 2U));
    ChunkCoord coord;
    if (!ParseChunkFilePath(relative, geometry, &coord) || relative.extension() != ".wal" ||
        CanonicalConditionalIntentFileName(geometry, coord) != name)
        return false;
    *out = coord;
    return true;
}

}  // namespace chunkdb
