#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"
#include "store_manifest.hpp"
#include <charconv>
#include <stdexcept>
namespace chunkdb {
namespace {
template <class Integer>
void Number(std::string& out, char prefix, Integer value) {
    static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
    // A prefix, every decimal digit of int64/uint64, and the terminator.
    char bytes[32];
    bytes[0] = prefix;
    const auto result = std::to_chars(bytes + 1, bytes + sizeof(bytes) - 2U, value);
    *result.ptr = '\r';
    *(result.ptr + 1) = '\n';
    out.append(bytes, static_cast<std::size_t>(result.ptr - bytes) + 2U);
}
void Value(std::string& out, const ColumnValue& value) {
    const auto* bits = std::get_if<BitsValue>(&value);
    if (bits == nullptr) { Protocol::AppendValue(out, value); return; }
    const auto& digits = bits->digits;
    if (digits.empty() || digits.size() > 65535U || digits.find_first_not_of("01") != std::string::npos)
        throw std::invalid_argument("invalid bits value");
    const auto size = (digits.size() + 7U) / 8U;
    Number(out, '$', size);
    const auto offset = out.size();
    out.resize(offset + size + 2U);
    for (std::size_t byte = 0U; byte < size; ++byte) {
        unsigned packed = 0U;
        const auto at = byte * 8U;
        if (digits.size() - at >= 8U) {
            // A full byte has no tail check per bit and can be unrolled.
            for (std::size_t bit = 0U; bit < 8U; ++bit)
                packed |= static_cast<unsigned>(digits[at + bit] == '1') << bit;
        } else {
            for (std::size_t bit = 0U; bit < digits.size() - at; ++bit)
                packed |= static_cast<unsigned>(digits[at + bit] == '1') << bit;
        }
        out[offset + byte] = static_cast<char>(packed);
    }
    out[offset + size] = '\r';
    out[offset + size + 1U] = '\n';
}
struct EncodedRange {
    std::size_t offset = 0U;
    std::size_t size = 0U;
    void Begin(const std::string& out) { offset = out.size(); }
    void End(const std::string& out) { size = out.size() - offset; }
    void Append(std::string& out) const {
        // Offsets survive growth. basic_string self-append also handles aliasing
        // when this append reallocates; the earlier range remains unchanged.
        out.append(out, offset, size);
    }
};
struct RowEncoding {
    const std::vector<ColumnValue>* previous = nullptr;
    EncodedRange range;
    void Append(std::string& out, const std::optional<std::vector<ColumnValue>>& row) {
        if (!row) { Protocol::AppendNull(out); return; }
        if (previous != nullptr && SameFeedValues(*previous, *row)) { range.Append(out); return; }
        range.Begin(out);
        Number(out, '*', row->size());
        for (const auto& value : *row) Value(out, value);
        range.End(out);
        previous = &*row;
    }
};
struct CoordinateEncoding {
    bool present = false;
    std::optional<std::int64_t> absolute;
    std::int64_t chunk = 0;
    std::uint32_t local = 0;
    EncodedRange range;
    void Append(std::string& out, std::optional<std::int64_t> next_absolute,
                std::int64_t next_chunk, std::uint32_t next_local) {
        if (present && absolute == next_absolute &&
            (absolute || (chunk == next_chunk && local == next_local))) { range.Append(out); return; }
        range.Begin(out);
        if (next_absolute) Number(out, ':', *next_absolute);
        else {
            out += "*2\r\n";
            Number(out, ':', next_chunk);
            Number(out, ':', next_local);
        }
        range.End(out);
        present = true; absolute = next_absolute; chunk = next_chunk; local = next_local;
    }
};
}
std::string EncodeFeedEntry(const FeedEntry& entry) {
    if (entry.kind == FeedEntry::Kind::kEnd) return Protocol::Error("NO_TABLE", "watched table was dropped");
    std::string out = entry.kind == FeedEntry::Kind::kChange ? ">7\r\n" :
        entry.kind == FeedEntry::Kind::kSchema ? ">5\r\n" : ">3\r\n";
    Protocol::AppendBulk(out, entry.kind == FeedEntry::Kind::kChange ? "change" :
        entry.kind == FeedEntry::Kind::kSchema ? "schema" : "resync");
    Protocol::AppendBulk(out, StoreIdHex(entry.position.epoch));
    Number(out, ':', entry.position.revision);
    if (entry.kind == FeedEntry::Kind::kResync) return out;
    if (entry.kind == FeedEntry::Kind::kSchema) {
        Number(out, ':', entry.schema_version);
        Protocol::AppendColumns(out, entry.columns);
        return out;
    }
    Number(out, ':', entry.commit_time_ms);
    if (entry.user) Protocol::AppendBulk(out, *entry.user); else Protocol::AppendNull(out);
    Number(out, ':', entry.schema_version);
    Number(out, '*', entry.blocks.size());
    RowEncoding before, after;
    CoordinateEncoding x, y;
    for (const auto& block : entry.blocks) {
        out += "*4\r\n";
        x.Append(out, block.x, block.chunk.x, block.local_x);
        y.Append(out, block.y, block.chunk.y, block.local_y);
        before.Append(out, block.before);
        after.Append(out, block.after);
    }
    return out;
}
}
