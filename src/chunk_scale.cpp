#include "chunk_scale.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "statement_control.hpp"

namespace chunkdb {
namespace {

using Bytes = std::vector<std::uint8_t>;

bool Bit(const Bytes& bytes, std::size_t at) { return (bytes[at / 8U] & (1U << (at % 8U))) != 0U; }
void SetBit(Bytes& bytes, std::size_t at) {
    bytes[at / 8U] = static_cast<std::uint8_t>(bytes[at / 8U] | (1U << (at % 8U)));
}

const char* ReductionName(ChunkFormReduction reduction) {
    switch (reduction) {
        case ChunkFormReduction::kMax: return "MAX";
        case ChunkFormReduction::kMin: return "MIN";
        case ChunkFormReduction::kMode: return "MODE";
        case ChunkFormReduction::kFirst: return "FIRST";
        case ChunkFormReduction::kAny: return "ANY";
    }
    return "an unknown reduction";
}

bool Accepts(ChunkFormReduction reduction, ColumnKind kind) {
    const bool number = kind == ColumnKind::kUnsigned || kind == ColumnKind::kSigned || kind == ColumnKind::kBool;
    switch (reduction) {
        case ChunkFormReduction::kMax:
        case ChunkFormReduction::kMin:
            return number || kind == ColumnKind::kFloat32 || kind == ColumnKind::kFloat64;
        case ChunkFormReduction::kMode: return number || kind == ColumnKind::kBits;
        case ChunkFormReduction::kFirst:
        case ChunkFormReduction::kAny: return true;
    }
    return false;
}

std::vector<std::size_t> Indexes(const ChunkLayout& layout, std::span<const std::size_t> projection) {
    if (!projection.empty()) return {projection.begin(), projection.end()};
    std::vector<std::size_t> all(layout.schema().columns.size());
    std::iota(all.begin(), all.end(), 0U);
    return all;
}

// The present blocks of each cell in ascending block order: cell c holds
// blocks[begin[c]] up to blocks[begin[c + 1]].
struct Groups {
    std::vector<std::uint32_t> blocks;
    std::vector<std::size_t> begin;
};

Groups GroupBlocks(const Geometry& geometry, const Geometry& scaled, std::uint32_t factor, const Bytes& presence) {
    const auto& sides = geometry.config();
    const auto& cells = scaled.config();
    const std::uint32_t depth = geometry.dims() == 3U ? factor : 1U;
    Groups groups;
    groups.begin.reserve(scaled.ChunkBlockCount() + 1U);
    groups.begin.push_back(0U);
    for (std::uint32_t cz = 0U; cz < cells.chunk_depth_blocks; ++cz)
        for (std::uint32_t cy = 0U; cy < cells.chunk_height_blocks; ++cy)
            for (std::uint32_t cx = 0U; cx < cells.chunk_width_blocks; ++cx) {
                for (std::uint32_t z = cz * depth; z < (cz + 1U) * depth; ++z)
                    for (std::uint32_t y = cy * factor; y < (cy + 1U) * factor; ++y) {
                        const auto row = static_cast<std::size_t>(sides.chunk_width_blocks) *
                            (y + static_cast<std::size_t>(sides.chunk_height_blocks) * z);
                        for (std::uint32_t x = cx * factor; x < (cx + 1U) * factor; ++x)
                            if (Bit(presence, row + x)) groups.blocks.push_back(static_cast<std::uint32_t>(row + x));
                    }
                groups.begin.push_back(groups.blocks.size());
            }
    return groups;
}

// The fixed-width values of one column in a chunk payload.
class FixedValues {
  public:
    FixedValues(const ChunkLayout::FixedColumn& span, const Bytes& payload)
        : payload_(payload), values_bit_(span.values * 8U), validity_(span.validity), width_(span.width),
          value_((span.width + 7U) / 8U) {}
    [[nodiscard]] bool Null(std::uint32_t block) const {
        return validity_ != ChunkLayout::kNoValidity && !Bit(payload_, validity_ * 8U + block);
    }
    // The value bytes, lowest bit first; valid until the next call.
    [[nodiscard]] const Bytes& Value(std::uint32_t block) {
        ReadValueBits(payload_.data(), values_bit_ + static_cast<std::size_t>(block) * width_, value_.data(), width_);
        return value_;
    }
    // A value of at most 64 bits as an integer.
    [[nodiscard]] std::uint64_t Raw(std::uint32_t block) {
        const auto& bytes = Value(block);
        std::uint64_t raw = 0U;
        for (std::size_t i = 0U; i < bytes.size(); ++i) raw |= static_cast<std::uint64_t>(bytes[i]) << (8U * i);
        return raw;
    }
    [[nodiscard]] std::uint32_t width() const noexcept { return width_; }

  private:
    const Bytes& payload_;
    std::size_t values_bit_;
    std::size_t validity_;
    std::uint32_t width_;
    Bytes value_;
};

bool IsNaN(ColumnKind kind, std::uint64_t raw) {
    if (kind == ColumnKind::kFloat32) return std::isnan(std::bit_cast<float>(static_cast<std::uint32_t>(raw)));
    if (kind == ColumnKind::kFloat64) return std::isnan(std::bit_cast<double>(raw));
    return false;
}

bool Less(ColumnKind kind, std::uint32_t width, std::uint64_t left, std::uint64_t right) {
    switch (kind) {
        case ColumnKind::kSigned: {
            const auto shift = 64U - width;
            return (static_cast<std::int64_t>(left << shift) >> shift) <
                   (static_cast<std::int64_t>(right << shift) >> shift);
        }
        case ColumnKind::kFloat32:
            return std::bit_cast<float>(static_cast<std::uint32_t>(left)) <
                   std::bit_cast<float>(static_cast<std::uint32_t>(right));
        case ColumnKind::kFloat64: return std::bit_cast<double>(left) < std::bit_cast<double>(right);
        default: return left < right;
    }
}

// MAX or MIN of the non-NULL values; NaN only when every value is NaN.
// Equal values keep the earliest block.
std::optional<std::uint32_t> Extreme(bool max, ColumnKind kind, FixedValues& values, std::span<const std::uint32_t> group) {
    std::optional<std::uint32_t> best;
    std::uint64_t best_raw = 0U;
    bool best_nan = false;
    for (const auto block : group) {
        if (values.Null(block)) continue;
        const auto raw = values.Raw(block);
        const bool nan = IsNaN(kind, raw);
        if (best && (nan || (!best_nan && !(max ? Less(kind, values.width(), best_raw, raw)
                                                : Less(kind, values.width(), raw, best_raw)))))
            continue;
        best = block;
        best_raw = raw;
        best_nan = nan;
    }
    return best;
}

// The most frequent non-NULL value by its bits; a tie goes to the value whose
// first block comes earliest. Returns that first block.
std::optional<std::uint32_t> Mode(FixedValues& values, std::span<const std::uint32_t> group,
    Bytes& keys, std::vector<std::uint32_t>& members, std::vector<std::uint32_t>& order) {
    const auto size = (values.width() + 7U) / 8U;
    keys.clear();
    members.clear();
    for (const auto block : group) {
        if (values.Null(block)) continue;
        const auto& value = values.Value(block);
        keys.insert(keys.end(), value.begin(), value.end());
        members.push_back(block);
    }
    if (members.empty()) return std::nullopt;
    order.resize(members.size());
    std::iota(order.begin(), order.end(), 0U);
    const auto key = [&](std::uint32_t at) { return keys.data() + static_cast<std::size_t>(at) * size; };
    std::sort(order.begin(), order.end(), [&](std::uint32_t left, std::uint32_t right) {
        const auto compared = std::memcmp(key(left), key(right), size);
        return compared != 0 ? compared < 0 : left < right;
    });
    std::size_t best_count = 0U;
    std::uint32_t best = 0U;
    for (std::size_t run = 0U; run < order.size();) {
        auto end = run + 1U;
        while (end < order.size() && std::memcmp(key(order[run]), key(order[end]), size) == 0) ++end;
        // order[run] is the run's earliest member.
        if (end - run > best_count || (end - run == best_count && order[run] < best)) {
            best_count = end - run;
            best = order[run];
        }
        run = end;
    }
    return members[best];
}

std::optional<std::uint32_t> Select(ChunkFormReduction reduction, ColumnKind kind, FixedValues& values,
    std::span<const std::uint32_t> group, Bytes& keys, std::vector<std::uint32_t>& members,
    std::vector<std::uint32_t>& order) {
    switch (reduction) {
        case ChunkFormReduction::kMax: return Extreme(true, kind, values, group);
        case ChunkFormReduction::kMin: return Extreme(false, kind, values, group);
        case ChunkFormReduction::kMode: return Mode(values, group, keys, members, order);
        case ChunkFormReduction::kFirst:
            if (values.Null(group.front())) return std::nullopt;
            return group.front();
        case ChunkFormReduction::kAny:
            for (const auto block : group) if (!values.Null(block)) return block;
            return std::nullopt;
    }
    throw std::invalid_argument("unknown area reduction");
}

}  // namespace

Geometry ScaledGeometry(const Geometry& geometry, const ChunkFormScale& scale, std::span<const std::size_t> projection) {
    const auto& sides = geometry.config();
    const auto factor = scale.factor;
    const bool three = geometry.dims() == 3U;
    if (factor == 0U || sides.chunk_width_blocks % factor != 0U || sides.chunk_height_blocks % factor != 0U ||
        (three && sides.chunk_depth_blocks % factor != 0U)) {
        throw std::invalid_argument("SCALE " + std::to_string(factor) + " must divide every chunk side (" +
            std::to_string(sides.chunk_width_blocks) + " x " + std::to_string(sides.chunk_height_blocks) +
            (three ? " x " + std::to_string(sides.chunk_depth_blocks) : std::string()) + ")");
    }
    const auto& layout = geometry.layout();
    for (const auto index : Indexes(layout, projection)) {
        const auto& column = layout.schema().columns[index];
        if (!Accepts(scale.reduction, column.type.kind))
            throw std::invalid_argument(std::string(ReductionName(scale.reduction)) + " does not accept column " +
                column.name + " of type " + ColumnTypeName(column.type));
    }
    auto cells = sides;
    cells.chunk_width_blocks /= factor;
    cells.chunk_height_blocks /= factor;
    if (three) cells.chunk_depth_blocks /= factor;
    return Geometry(cells, layout.schema(), geometry.chunk_schema());
}

ChunkState ScaleChunkState(const Geometry& geometry, const Geometry& scaled, const ChunkFormScale& scale,
    std::span<const std::size_t> projection, const ChunkState& state) {
    const auto& layout = geometry.layout();
    const auto& cells = scaled.layout();
    const auto count = scaled.ChunkBlockCount();
    const auto groups = GroupBlocks(geometry, scaled, scale.factor, state.presence_bitmap);
    const auto group = [&groups](std::size_t cell) {
        return std::span<const std::uint32_t>(groups.blocks).subspan(
            groups.begin[cell], groups.begin[cell + 1U] - groups.begin[cell]);
    };
    ChunkState out;
    out.version = state.version;
    out.written = state.written;
    out.commit_time_ms = state.commit_time_ms;
    out.chunk_columns = state.chunk_columns;
    out.presence_bitmap.assign((count + 7U) / 8U, 0U);
    out.payload.assign(cells.payload_bytes(), 0U);
    for (std::size_t cell = 0U; cell < count; ++cell)
        if (!group(cell).empty()) SetBit(out.presence_bitmap, cell);

    auto indexes = Indexes(layout, projection);
    // Variable values are appended in key order: by column ID, then cell.
    std::sort(indexes.begin(), indexes.end(), [&layout](std::size_t left, std::size_t right) {
        return layout.schema().columns[left].id < layout.schema().columns[right].id;
    });
    Bytes keys;
    std::vector<std::uint32_t> members, order;
    std::vector<std::pair<VarKey, std::span<const std::uint8_t>>> vars;
    std::size_t var_bytes = 0U;
    for (const auto index : indexes) {
        CheckStatementCancellation();
        const auto& column = layout.schema().columns[index];
        const auto* from = layout.FixedColumnAt(index);
        if (from == nullptr) {
            for (std::size_t cell = 0U; cell < count; ++cell) {
                const auto blocks = group(cell);
                if (blocks.empty()) continue;
                // No entry is NULL, or the empty value of a column without NULL.
                std::optional<std::span<const std::uint8_t>> value;
                for (const auto block : blocks) {
                    value = state.vars.Find({column.id, block});
                    if (value || scale.reduction == ChunkFormReduction::kFirst) break;
                }
                if (!value) continue;
                vars.emplace_back(VarKey{column.id, static_cast<std::uint32_t>(cell)}, *value);
                var_bytes += kVarEntryHeaderBytes + value->size();
            }
            continue;
        }
        const auto* to = cells.FixedColumnAt(index);
        FixedValues values(*from, state.payload);
        for (std::size_t cell = 0U; cell < count; ++cell) {
            const auto blocks = group(cell);
            if (blocks.empty()) continue;
            const auto chosen = Select(scale.reduction, column.type.kind, values, blocks, keys, members, order);
            if (!chosen) continue;
            WriteValueBits(out.payload.data(), to->values * 8U + cell * to->width, values.Value(*chosen).data(), to->width);
            if (to->validity != ChunkLayout::kNoValidity) SetBit(out.payload, to->validity * 8U + cell);
        }
    }
    out.vars.reserve(vars.size(), var_bytes);
    for (const auto& [key, value] : vars) out.vars.Assign(key, value);
    return out;
}

}  // namespace chunkdb
