#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "chunkdb/chunk_columns.hpp"
#include "chunkdb/chunk_vars.hpp"
#include "chunkdb/geometry.hpp"
#include "chunkdb/types.hpp"
#include "expr.hpp"

// The WHERE of a read (AGGREGATE, SCAN CHUNKS): the present blocks of a
// chunk for which a condition (docs/design/EXPRESSIONS_DESIGN.md) is TRUE.
// A block for which it is false or NULL is dropped.
namespace chunkdb {

class BlockFilter {
  public:
    // Plans `where` against the block and chunk columns of `geometry`, which
    // outlives the filter. Throws as expr::PlanCondition.
    BlockFilter(const Geometry& geometry, const expr::Expression& where);

    // The parameters the condition takes, with the type of each.
    [[nodiscard]] const std::vector<expr::ParameterType>& parameters() const { return plan_.parameters(); }
    // Whether the condition reads text or bytes block columns, which the
    // chunk read must then carry.
    [[nodiscard]] bool needs_vars() const noexcept { return needs_vars_; }
    // $n at index n - 1, of the types parameters() gives, and NOW() in
    // milliseconds.
    void Bind(std::vector<ColumnValue> parameters, std::uint64_t now_ms);

    // Clears in `presence` the bit of every present block for which the
    // condition is not TRUE, and returns the blocks that stay. `chunk_columns`
    // null means the defaults. Throws as expr::EvaluateCondition, and
    // std::out_of_range when the condition reads the position of a block
    // outside the 64-bit range. Checks statement cancellation between groups
    // of blocks.
    std::size_t Apply(const ChunkCoord& coord, std::span<const std::uint8_t> payload, std::span<std::uint8_t> presence,
        const ChunkVars& vars, const ChunkColumnState* chunk_columns);

  private:
    const Geometry& geometry_;
    expr::ConditionPlan plan_;
    // The block columns the condition reads, by index in the layout's schema.
    std::vector<std::size_t> block_columns_;
    bool reads_chunk_columns_ = false;
    bool reads_positions_ = false;
    bool needs_vars_ = false;
    std::vector<ColumnValue> parameters_;
    std::uint64_t now_ms_ = 0U;
    // Reused for every block.
    std::vector<ColumnValue> block_;
    std::vector<std::uint8_t> scratch_;
};

}  // namespace chunkdb
