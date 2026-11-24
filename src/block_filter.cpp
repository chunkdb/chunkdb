#include "block_filter.hpp"

#include <algorithm>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "chunk_store_internal.hpp"
#include "statement_control.hpp"

namespace chunkdb {

namespace {

// Blocks evaluated between statement cancellation checks.
constexpr std::size_t kBlocksPerCheck = 1024U;

// The table's columns by name; records which ones the condition names.
class RecordingSchema final : public expr::Schema {
  public:
    RecordingSchema(const TableSchema& block, const TableSchema& chunk) : block_(block), chunk_(chunk) {}

    [[nodiscard]] std::optional<expr::ColumnBinding> BlockColumn(std::string_view name) const override {
        return Find(block_, name, &block_used_);
    }
    [[nodiscard]] std::optional<expr::ColumnBinding> ChunkColumn(std::string_view name) const override {
        return Find(chunk_, name, &chunk_used_);
    }

    [[nodiscard]] const std::vector<std::size_t>& block_used() const noexcept { return block_used_; }
    [[nodiscard]] bool chunk_used() const noexcept { return !chunk_used_.empty(); }

  private:
    static std::optional<expr::ColumnBinding> Find(
        const TableSchema& schema, std::string_view name, std::vector<std::size_t>* used) {
        for (std::size_t index = 0U; index < schema.columns.size(); ++index) {
            const auto& column = schema.columns[index];
            if (column.name != name) continue;
            if (std::find(used->begin(), used->end(), index) == used->end()) used->push_back(index);
            return expr::ColumnBinding{.id = column.id, .slot = index, .type = column.type, .nullable = column.nullable};
        }
        return std::nullopt;
    }

    const TableSchema& block_;
    const TableSchema& chunk_;
    mutable std::vector<std::size_t> block_used_;
    mutable std::vector<std::size_t> chunk_used_;
};

}  // namespace

BlockFilter::BlockFilter(const Geometry& geometry, const expr::Expression& where) : geometry_(geometry) {
    const auto& layout = geometry.layout();
    const RecordingSchema schema(layout.schema(), geometry.chunk_schema());
    plan_ = expr::PlanCondition(where, schema);
    block_columns_ = schema.block_used();
    reads_chunk_columns_ = schema.chunk_used();
    reads_positions_ = std::any_of(where.nodes.begin(), where.nodes.end(), [](const expr::Node& node) {
        return node.op == expr::Op::kPosX || node.op == expr::Op::kPosY || node.op == expr::Op::kPosZ;
    });
    needs_vars_ = std::any_of(block_columns_.begin(), block_columns_.end(),
        [&layout](std::size_t index) { return layout.FixedColumnAt(index) == nullptr; });
    block_.resize(layout.schema().columns.size());
}

void BlockFilter::Bind(std::vector<ColumnValue> parameters, std::uint64_t now_ms) {
    parameters_ = std::move(parameters);
    now_ms_ = now_ms;
}

std::size_t BlockFilter::Apply(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<std::uint8_t> presence, const ChunkVars& vars, const ChunkColumnState* chunk_columns) {
    const auto& layout = geometry_.layout();
    const auto block_count = layout.block_count();
    if (presence.empty()) return 0U;
    if (payload.size() < layout.payload_bytes() || presence.size() < (block_count + 7U) / 8U)
        throw std::logic_error("a chunk state does not match the table's layout");
    std::vector<ColumnValue> chunk_values;
    if (reads_chunk_columns_) {
        const auto* stored = chunk_columns != nullptr ? chunk_columns : geometry_.default_chunk_columns().get();
        if (stored != nullptr) chunk_values = DecodeChunkColumns(geometry_.chunk_schema(), stored->bytes);
    }
    const auto& config = geometry_.config();
    const std::size_t width = config.chunk_width_blocks;
    const std::size_t height = config.chunk_height_blocks;
    std::size_t kept = 0U;
    std::size_t evaluated = 0U;
    for (std::size_t block = 0U; block < block_count; ++block) {
        if (((presence[block / 8U] >> (block % 8U)) & 1U) == 0U) continue;
        if (evaluated++ % kBlocksPerCheck == 0U) CheckStatementCancellation();
        for (const auto index : block_columns_) {
            const auto& column = layout.schema().columns[index];
            const auto* fixed = layout.FixedColumnAt(index);
            auto& value = block_[index];
            if (fixed == nullptr) {
                const auto bytes = vars.Find(VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)});
                if (bytes.has_value()) value = DecodeVarValue(column, *bytes);
                else if (column.nullable) value = std::monostate{};
                else value = DecodeVarValue(column, {});
            } else if (fixed->validity != ChunkLayout::kNoValidity &&
                       ((payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U) {
                value = std::monostate{};
            } else {
                scratch_.assign((fixed->width + 7U) / 8U, 0U);
                ReadValueBits(payload.data(), fixed->values * 8U + block * fixed->width, scratch_.data(), fixed->width);
                value = DecodeColumnValue(column, scratch_.data());
            }
        }
        expr::Inputs inputs{.exists = true, .block = block_, .chunk = chunk_values, .now = now_ms_, .parameters = parameters_};
        if (reads_positions_) {
            const auto local_x = block % width;
            const auto local_y = (block / width) % height;
            const auto local_z = block / (width * height);
            const auto x = BlockCoordinate(coord.x, config.chunk_width_blocks, local_x);
            const auto y = BlockCoordinate(coord.y, config.chunk_height_blocks, local_y);
            const auto z = geometry_.dims() == 3U ? BlockCoordinate(coord.z, config.chunk_depth_blocks, local_z)
                                                  : std::optional<std::int64_t>(0);
            if (!x || !y || !z)
                throw std::out_of_range("WHERE reads the position of a block outside the 64-bit coordinate range");
            inputs.x = *x;
            inputs.y = *y;
            inputs.z = *z;
        }
        if (expr::EvaluateCondition(plan_, inputs)) ++kept;
        else presence[block / 8U] = static_cast<std::uint8_t>(presence[block / 8U] & ~(1U << (block % 8U)));
    }
    return kept;
}

}  // namespace chunkdb
