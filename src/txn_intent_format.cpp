#include "txn_intent_format.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <istream>
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

std::size_t CheckedSize(std::size_t count, std::uint8_t dims) {
    RequireDimensions(dims);
    if (count == 0 || count > std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument("transaction intent count is outside its encoded range");
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

std::size_t ValidateHeader(std::span<const std::uint8_t> bytes, std::uint64_t measured_bytes,
    std::uint8_t expected_dims, const StoreId& expected_table_id) {
    RequireDimensions(expected_dims);
    if (bytes.size() < kEntriesOffset || measured_bytes < kFixedBytes)
        throw std::invalid_argument("transaction intent is truncated");
    if (bytes[0] != 'C' || bytes[1] != 'K' || bytes[2] != 'T' || bytes[3] != 'I')
        throw std::invalid_argument("not a CKTI transaction intent");
    if (ReadLe(bytes.data() + 4, 2) != 2) throw std::invalid_argument("unsupported transaction intent version");
    if (bytes[6] > 1) throw std::invalid_argument("transaction intent state is invalid");
    const auto dims = bytes[7];
    RequireDimensions(dims);
    if (dims != expected_dims) throw std::invalid_argument("transaction intent dimensions differ from table");
    if (!std::equal(expected_table_id.begin(), expected_table_id.end(), bytes.begin() + 8))
        throw std::invalid_argument("transaction intent belongs to another table");
    if (ReadLe(bytes.data() + 24, 8) == 0)
        throw std::invalid_argument("transaction intent revision is zero");
    const auto count = static_cast<std::size_t>(ReadLe(bytes.data() + 32, 4));
    if (measured_bytes != CheckedSize(count, dims))
        throw std::invalid_argument("transaction intent has wrong length");
    return count;
}

void ReadExact(std::istream& input, std::span<std::uint8_t> bytes) {
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (input.gcount() != static_cast<std::streamsize>(bytes.size()))
        throw std::invalid_argument("transaction intent is truncated");
}

}  // namespace

std::vector<std::uint8_t> SerializeCheckedTxnIntent(
    const TxnIntent& intent, std::uint8_t dims, const StoreId& table_id,
    std::size_t written_chunk_limit) {
    if (intent.entries.empty() || intent.entries.size() > written_chunk_limit)
        throw std::invalid_argument("transaction intent count exceeds written-chunk limit");
    const auto size = CheckedSize(intent.entries.size(), dims);
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

void ValidateCheckedTxnIntent(std::span<const std::uint8_t> bytes,
    std::uint8_t expected_dims, const StoreId& expected_table_id) {
    const auto count = ValidateHeader(bytes, bytes.size(), expected_dims, expected_table_id);
    if (ReadLe(bytes.data() + bytes.size() - 4, 4) != Crc32(bytes.data(), bytes.size() - 4))
        throw std::invalid_argument("transaction intent checksum mismatch");
    const auto entry_bytes = 8U * static_cast<std::size_t>(expected_dims) + 8U;
    ChunkCoord previous;
    for (std::size_t i = 0; i < count; ++i) {
        const auto coord = ReadCoord(bytes.data() + kEntriesOffset + i * entry_bytes, expected_dims);
        RequireOrder(coord, previous, i);
        previous = coord;
    }
}

void ValidateCheckedTxnIntentStream(std::istream& input, std::uint64_t measured_bytes,
    std::uint8_t expected_dims, const StoreId& expected_table_id) {
    std::array<std::uint8_t, kEntriesOffset> header{};
    ReadExact(input, header);
    const auto count = ValidateHeader(header, measured_bytes, expected_dims, expected_table_id);
    auto checksum = Crc32(header.data(), header.size());
    const auto entry_bytes = 8U * static_cast<std::size_t>(expected_dims) + 8U;
    std::array<std::uint8_t, 32> entry{};
    ChunkCoord previous;
    for (std::size_t i = 0; i < count; ++i) {
        const auto bytes = std::span(entry).first(entry_bytes);
        ReadExact(input, bytes);
        const auto coord = ReadCoord(bytes.data(), expected_dims);
        RequireOrder(coord, previous, i);
        previous = coord;
        checksum = Crc32Extend(checksum, bytes.data(), bytes.size());
    }
    std::array<std::uint8_t, 4> trailer{};
    ReadExact(input, trailer);
    if (ReadLe(trailer.data(), trailer.size()) != checksum)
        throw std::invalid_argument("transaction intent checksum mismatch");
    if (input.peek() != std::char_traits<char>::eof() || input.bad())
        throw std::invalid_argument("transaction intent has wrong length");
}

TxnIntent ParseCheckedTxnIntent(
    std::span<const std::uint8_t> bytes, std::uint8_t expected_dims,
    const StoreId& expected_table_id) {
    ValidateCheckedTxnIntent(bytes, expected_dims, expected_table_id);
    const auto dims = expected_dims;
    const auto count = static_cast<std::size_t>(ReadLe(bytes.data() + 32, 4));
    const auto entry_bytes = 8U * static_cast<std::size_t>(dims) + 8U;
    TxnIntent intent{
        .state = bytes[6] == 0 ? TxnIntentState::kRollback : TxnIntentState::kCommitted,
        .version = ReadLe(bytes.data() + 24, 8),
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
