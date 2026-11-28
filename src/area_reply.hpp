#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "chunkdb/protocol.hpp"
#include "chunkdb/types.hpp"

// The framing of a GET AREA reply around its rows, and its sizes: an area page
// admits each cell it visits by the sizes, then builds the framing once.
namespace chunkdb::detail {

// `[schema, ` before a reply WITH SCHEMA, `{chunks: ` before a page, and the
// header of the array of `count` rows.
inline std::string AreaReplyPrefix(bool with_schema, std::uint64_t schema, bool paged, std::size_t count) {
    std::string prefix;
    if (with_schema) {
        Protocol::AppendArrayHeader(prefix, 2U);
        Protocol::AppendInteger(prefix, schema);
    }
    if (paged) {
        Protocol::AppendMapHeader(prefix, 3U);
        Protocol::AppendBulk(prefix, "chunks");
    }
    Protocol::AppendArrayHeader(prefix, count);
    return prefix;
}

// `more` and `after` of a page: the last admitted cell when more remain.
inline std::string AreaReplySuffix(bool paged, bool more, const ChunkCoord& coord, std::uint8_t dims) {
    std::string suffix;
    if (!paged) return suffix;
    Protocol::AppendBulk(suffix, "more");
    Protocol::AppendBoolean(suffix, more);
    Protocol::AppendBulk(suffix, "after");
    if (!more) Protocol::AppendNull(suffix);
    else {
        Protocol::AppendArrayHeader(suffix, dims);
        Protocol::AppendInteger(suffix, coord.x);
        Protocol::AppendInteger(suffix, coord.y);
        if (dims == 3U) Protocol::AppendInteger(suffix, coord.z);
    }
    return suffix;
}

// The bytes Protocol::AppendInteger and AppendArrayHeader write for `value`:
// a prefix, its decimal digits and CRLF.
[[nodiscard]] inline std::size_t RespUnsignedBytes(std::uint64_t value) noexcept {
    std::size_t digits = 1U;
    while (value >= 10U) { value /= 10U; ++digits; }
    return digits + 3U;
}
[[nodiscard]] inline std::size_t RespIntegerBytes(std::int64_t value) noexcept {
    return value < 0 ? 1U + RespUnsignedBytes(0U - static_cast<std::uint64_t>(value))
                     : RespUnsignedBytes(static_cast<std::uint64_t>(value));
}

// The sizes of AreaReplyPrefix and AreaReplySuffix, without building them.
[[nodiscard]] inline std::size_t AreaReplyPrefixBytes(
    bool with_schema, std::uint64_t schema, bool paged, std::size_t count) noexcept {
    // "*2" and the schema; "%3" and the bulk "chunks"; the array header.
    return (with_schema ? 4U + RespUnsignedBytes(schema) : 0U) + (paged ? 4U + 12U : 0U) + RespUnsignedBytes(count);
}
[[nodiscard]] inline std::size_t AreaReplySuffixBytes(
    bool paged, bool more, const ChunkCoord& coord, std::uint8_t dims) noexcept {
    if (!paged) return 0U;
    // The bulk "more", a boolean, the bulk "after"; then null or the cursor.
    const std::size_t fixed = 10U + 4U + 11U;
    if (!more) return fixed + 3U;
    return fixed + RespUnsignedBytes(dims) + RespIntegerBytes(coord.x) + RespIntegerBytes(coord.y) +
        (dims == 3U ? RespIntegerBytes(coord.z) : 0U);
}

}  // namespace chunkdb::detail
