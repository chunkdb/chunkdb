#include "block_checks.hpp"

#include <algorithm>
#include <stdexcept>
#include <string>
#include <utility>

#include "chunk_store_internal.hpp"
#include "chunkdb/geometry.hpp"
#include "expr.hpp"

namespace chunkdb {

struct BlockChecks::Planned {
    CheckConstraint check;
    expr::ConditionPlan plan;
    // The block columns it reads, by index in the schema (their slots).
    std::vector<std::size_t> columns;
    bool reads_positions = false;
    // The ADD CHECK in progress.
    bool pending = false;
};

namespace {

[[nodiscard]] BlockChecks::Planned Plan(const CheckConstraint& check, const TableSchema& schema, bool pending) {
    BlockChecks::Planned planned;
    planned.check = check;
    planned.pending = pending;
    const auto condition = expr::ParseCondition(check.expression);
    planned.plan = expr::PlanCondition(condition.expression, expr::ColumnList(schema.columns));
    for (const auto& node : condition.expression.nodes) {
        if (node.op == expr::Op::kPosX || node.op == expr::Op::kPosY || node.op == expr::Op::kPosZ)
            planned.reads_positions = true;
        if (node.op != expr::Op::kColumn) continue;
        const auto at = std::find_if(schema.columns.begin(), schema.columns.end(),
            [&](const Column& column) { return column.name == node.name; });
        const auto index = static_cast<std::size_t>(at - schema.columns.begin());
        if (at != schema.columns.end() &&
            std::find(planned.columns.begin(), planned.columns.end(), index) == planned.columns.end())
            planned.columns.push_back(index);
    }
    return planned;
}

}  // namespace

std::shared_ptr<const BlockChecks> BlockChecks::Make(
    const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout) {
    const auto& schema = layout->schema();
    const auto* pending = schema.pending.has_value() && schema.pending->check.has_value() ? &*schema.pending->check : nullptr;
    if (schema.checks.empty() && pending == nullptr) return nullptr;
    std::vector<Planned> checks;
    checks.reserve(schema.checks.size() + 1U);
    for (const auto& check : schema.checks) checks.push_back(Plan(check, schema, false));
    if (pending != nullptr) checks.push_back(Plan(*pending, schema, true));
    return std::make_shared<const BlockChecks>(config, std::move(layout), std::move(checks));
}

BlockChecks::BlockChecks(const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout, std::vector<Planned> checks)
    : width_(config.chunk_width_blocks),
      height_(config.chunk_height_blocks),
      depth_(config.chunk_depth_blocks),
      dims_(config.dims),
      layout_(std::move(layout)),
      checks_(std::move(checks)) {}

BlockChecks::~BlockChecks() = default;

std::string BlockChecks::DescribeBlock(const ChunkCoord& coord, std::size_t block) const {
    const auto axis = [](std::int64_t chunk, std::uint32_t blocks, std::size_t local) {
        const auto absolute = BlockCoordinate(chunk, blocks, local);
        return absolute.has_value() ? std::to_string(*absolute)
                                    : "[" + std::to_string(chunk) + ", " + std::to_string(local) + "]";
    };
    const std::size_t plane = static_cast<std::size_t>(width_) * height_;
    std::string text = "block (" + axis(coord.x, width_, block % width_) + ", " + axis(coord.y, height_, (block / width_) % height_);
    if (dims_ == 3U) text += ", " + axis(coord.z, depth_, block / plane);
    return text + ")";
}

bool BlockChecks::Fails(const Planned& check, const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    const ChunkVars& vars, std::size_t block, std::vector<ColumnValue>& values) const {
    const auto& layout = *layout_;
    const auto& columns = layout.schema().columns;
    std::vector<std::uint8_t> bytes;
    for (const auto index : check.columns) {
        const auto& column = columns[index];
        const auto* fixed = layout.FixedColumnAt(index);
        auto& value = values[index];
        if (fixed == nullptr) {
            const auto stored = vars.Find(VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)});
            if (stored.has_value()) value = DecodeVarValue(column, *stored);
            else if (column.nullable) value = std::monostate{};
            else value = DecodeVarValue(column, {});
        } else if (fixed->validity != ChunkLayout::kNoValidity &&
                   ((payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U) {
            value = std::monostate{};
        } else {
            bytes.assign((fixed->width + 7U) / 8U, 0U);
            ReadValueBits(payload.data(), fixed->values * 8U + block * fixed->width, bytes.data(), fixed->width);
            value = DecodeColumnValue(column, bytes.data());
        }
    }
    expr::Inputs inputs{.exists = true, .block = values};
    const auto refusal = [&](const std::exception& error) {
        return "CHECK " + check.check.name + " cannot be evaluated for " + DescribeBlock(coord, block) + ": " + error.what();
    };
    if (check.reads_positions) {
        const auto x = BlockCoordinate(coord.x, width_, block % width_);
        const auto y = BlockCoordinate(coord.y, height_, (block / width_) % height_);
        const auto z = dims_ == 3U ? BlockCoordinate(coord.z, depth_, block / (static_cast<std::size_t>(width_) * height_))
                                   : std::optional<std::int64_t>(0);
        if (!x || !y || !z) {
            throw std::out_of_range(refusal(std::out_of_range("the position is outside the 64-bit coordinate range")));
        }
        inputs.x = *x;
        inputs.y = *y;
        inputs.z = *z;
    }
    try {
        return expr::EvaluatesFalse(check.plan, inputs);
    } catch (const std::out_of_range& error) {
        throw std::out_of_range(refusal(error));
    } catch (const std::invalid_argument& error) {
        throw std::invalid_argument(refusal(error));
    }
}

void BlockChecks::RequireBlockWith(const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars,
    std::size_t block, std::vector<ColumnValue>& values) const {
    for (const auto& check : checks_) {
        if (!Fails(check, coord, payload, vars, block, values)) continue;
        throw CheckFailedError(check.check.name,
            DescribeBlock(coord, block) + " fails CHECK " + check.check.name + " (" + check.check.expression + ")" +
                (check.pending ? ", which ADD CHECK is checking" : ""));
    }
}

void BlockChecks::RequireBlock(
    const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars, std::size_t block) const {
    std::vector<ColumnValue> values(layout_->schema().columns.size());
    RequireBlockWith(coord, payload, vars, block, values);
}

void BlockChecks::RequireChunk(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars) const {
    std::vector<ColumnValue> values(layout_->schema().columns.size());
    for (std::size_t block = 0U; block < layout_->block_count(); ++block) {
        if (BlockPresent(presence, block)) RequireBlockWith(coord, payload, vars, block, values);
    }
}

void BlockChecks::RequireChunkAfter(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& current, const VarUpdate& update) const {
    if (update.replace.has_value()) return RequireChunk(coord, payload, presence, *update.replace);
    if (update.changes.empty()) return RequireChunk(coord, payload, presence, current);
    ChunkVars next = current;
    (void)ApplyVarUpdate(&next, VarUpdate{.changes = update.changes, .replace = std::nullopt});
    RequireChunk(coord, payload, presence, next);
}

std::optional<std::string> BlockChecks::FindPendingFailure(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars) const {
    const auto pending = std::find_if(checks_.begin(), checks_.end(), [](const Planned& check) { return check.pending; });
    if (pending == checks_.end()) throw std::logic_error("no ADD CHECK is in progress");
    std::vector<ColumnValue> values(layout_->schema().columns.size());
    for (std::size_t block = 0U; block < layout_->block_count(); ++block) {
        if (!BlockPresent(presence, block)) continue;
        try {
            if (Fails(*pending, coord, payload, vars, block, values)) return DescribeBlock(coord, block) + " fails it";
        } catch (const std::invalid_argument& error) {
            return std::string(error.what());
        } catch (const std::out_of_range& error) {
            return std::string(error.what());
        }
    }
    return std::nullopt;
}

}  // namespace chunkdb
