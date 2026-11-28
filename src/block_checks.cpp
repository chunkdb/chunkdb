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
    // It reads a DEFAULT COMMIT_TIME column.
    bool reads_commit_time = false;
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
        if (at != schema.columns.end() && at->default_commit_time) planned.reads_commit_time = true;
    }
    return planned;
}

[[nodiscard]] bool Bit(std::span<const std::uint8_t> bytes, std::size_t at) {
    return ((bytes[at / 8U] >> (at % 8U)) & 1U) != 0U;
}

}  // namespace

bool BlockChecks::Needed(const TableSchema& schema) noexcept {
    return !schema.checks.empty() || (schema.pending.has_value() && schema.pending->check.has_value());
}

std::shared_ptr<const BlockChecks> BlockChecks::Make(
    const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout) {
    const auto& schema = layout->schema();
    if (!Needed(schema)) return nullptr;
    std::vector<Planned> checks;
    checks.reserve(schema.checks.size() + 1U);
    for (const auto& check : schema.checks) checks.push_back(Plan(check, schema, false));
    if (schema.pending.has_value() && schema.pending->check.has_value())
        checks.push_back(Plan(*schema.pending->check, schema, true));
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

namespace {

// The text or bytes value of `key` in `source`: an override first.
template <typename Source>
[[nodiscard]] std::optional<std::span<const std::uint8_t>> VarOf(const Source& source, const VarKey& key) {
    for (const auto& change : source.overrides) {
        if (!(change.key == key)) continue;
        if (!change.value.has_value()) return std::nullopt;
        return std::span<const std::uint8_t>(*change.value);
    }
    return source.vars->Find(key);
}

}  // namespace

bool BlockChecks::Fails(
    const Planned& check, const ChunkCoord& coord, const Source& source, std::size_t block, Scratch& scratch) const {
    const auto& layout = *layout_;
    const auto& columns = layout.schema().columns;
    for (const auto index : check.columns) {
        const auto& column = columns[index];
        const auto* fixed = layout.FixedColumnAt(index);
        auto& value = scratch.values[index];
        if (fixed == nullptr) {
            const auto stored = VarOf(source, VarKey{.column_id = column.id, .block_index = static_cast<std::uint32_t>(block)});
            if (stored.has_value()) value = DecodeVarValue(column, *stored);
            else if (column.nullable) value = std::monostate{};
            else value = DecodeVarValue(column, {});
        } else if (fixed->validity != ChunkLayout::kNoValidity && !Bit(source.payload.subspan(fixed->validity), block)) {
            value = std::monostate{};
        } else {
            scratch.bytes.assign((fixed->width + 7U) / 8U, 0U);
            ReadValueBits(source.payload.data(), fixed->values * 8U + block * fixed->width, scratch.bytes.data(), fixed->width);
            value = DecodeColumnValue(column, scratch.bytes.data());
        }
    }
    expr::Inputs inputs{.exists = true, .block = scratch.values};
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

void BlockChecks::Require(
    const Planned& check, const ChunkCoord& coord, const Source& source, std::size_t block, Scratch& scratch) const {
    if (!Fails(check, coord, source, block, scratch)) return;
    throw CheckFailedError(check.check.name,
        DescribeBlock(coord, block) + " fails CHECK " + check.check.name + " (" + check.check.expression + ")" +
            (check.pending ? ", which ADD CHECK is checking" : ""));
}

bool BlockChecks::Differs(
    std::size_t index, std::size_t block, const Source& state, const Source& base, Scratch& scratch) const {
    const auto& layout = *layout_;
    const auto* fixed = layout.FixedColumnAt(index);
    if (fixed == nullptr) {
        const VarKey key{.column_id = layout.schema().columns[index].id, .block_index = static_cast<std::uint32_t>(block)};
        const auto now = VarOf(state, key);
        const auto before = VarOf(base, key);
        if (now.has_value() != before.has_value()) return true;
        return now.has_value() && !std::equal(now->begin(), now->end(), before->begin(), before->end());
    }
    if (fixed->validity != ChunkLayout::kNoValidity &&
        Bit(state.payload.subspan(fixed->validity), block) != Bit(base.payload.subspan(fixed->validity), block))
        return true;
    const auto bytes = (fixed->width + 7U) / 8U;
    scratch.bytes.assign(bytes, 0U);
    scratch.other.assign(bytes, 0U);
    const auto bit = fixed->values * 8U + block * fixed->width;
    ReadValueBits(state.payload.data(), bit, scratch.bytes.data(), fixed->width);
    ReadValueBits(base.payload.data(), bit, scratch.other.data(), fixed->width);
    return scratch.bytes != scratch.other;
}

void BlockChecks::RequireBlock(const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars,
    std::size_t block, std::span<const VarChange> overrides, bool defer_commit_time) const {
    Scratch scratch;
    scratch.values.resize(layout_->schema().columns.size());
    const Source source{.payload = payload, .vars = &vars, .overrides = overrides};
    for (const auto& check : checks_) {
        if (defer_commit_time && check.reads_commit_time) continue;
        Require(check, coord, source, block, scratch);
    }
}

void BlockChecks::RequireChanged(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars, std::span<const std::uint8_t> base_payload,
    std::span<const std::uint8_t> base_presence, const ChunkVars& base_vars, bool defer_commit_time) const {
    Scratch scratch;
    scratch.values.resize(layout_->schema().columns.size());
    const Source state{.payload = payload, .vars = &vars};
    const Source base{.payload = base_payload, .vars = &base_vars};
    for (std::size_t block = 0U; block < layout_->block_count(); ++block) {
        if (!BlockPresent(presence, block)) continue;
        const bool added = !BlockPresent(base_presence, block);
        for (const auto& check : checks_) {
            if (defer_commit_time && check.reads_commit_time) continue;
            const bool changed = added || std::any_of(check.columns.begin(), check.columns.end(),
                [&](std::size_t index) { return Differs(index, block, state, base, scratch); });
            if (changed) Require(check, coord, state, block, scratch);
        }
    }
}

void BlockChecks::RequireChangedAfter(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const VarUpdate& update, std::span<const std::uint8_t> base_payload,
    std::span<const std::uint8_t> base_presence, const ChunkVars& base_vars) const {
    if (update.replace.has_value())
        return RequireChanged(coord, payload, presence, *update.replace, base_payload, base_presence, base_vars);
    if (update.changes.empty())
        return RequireChanged(coord, payload, presence, base_vars, base_payload, base_presence, base_vars);
    ChunkVars next = base_vars;
    (void)ApplyVarUpdate(&next, VarUpdate{.changes = update.changes, .replace = std::nullopt});
    RequireChanged(coord, payload, presence, next, base_payload, base_presence, base_vars);
}

std::optional<std::string> BlockChecks::FindPendingFailure(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
    std::span<const std::uint8_t> presence, const ChunkVars& vars) const {
    const auto pending = std::find_if(checks_.begin(), checks_.end(), [](const Planned& check) { return check.pending; });
    if (pending == checks_.end()) throw std::logic_error("no ADD CHECK is in progress");
    Scratch scratch;
    scratch.values.resize(layout_->schema().columns.size());
    const Source source{.payload = payload, .vars = &vars};
    for (std::size_t block = 0U; block < layout_->block_count(); ++block) {
        if (!BlockPresent(presence, block)) continue;
        try {
            if (Fails(*pending, coord, source, block, scratch)) return DescribeBlock(coord, block) + " fails it";
        } catch (const std::invalid_argument& error) {
            return std::string(error.what());
        } catch (const std::out_of_range& error) {
            return std::string(error.what());
        }
    }
    return std::nullopt;
}

}  // namespace chunkdb
