#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/chunk_layout.hpp"
#include "chunkdb/chunk_vars.hpp"
#include "chunkdb/schema.hpp"
#include "chunkdb/types.hpp"

// The CHECK constraints of a table's current schema version
// (docs/reference/TABLES.md), planned once and evaluated for the blocks a
// write leaves present. A block fails a check when its condition is false;
// NULL (unknown) passes. A table without checks has no BlockChecks, so its
// writes evaluate nothing.
namespace chunkdb {

struct GeometryConfig;

class BlockChecks {
  public:
    // The checks of `layout`'s schema, and an ADD CHECK in progress; null
    // when it has neither. Throws as expr::PlanCondition.
    [[nodiscard]] static std::shared_ptr<const BlockChecks> Make(
        const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout);

    // Throws CheckFailedError for the first check present block `block` of
    // the state fails, and std::invalid_argument or std::out_of_range,
    // naming the check and the block, when a check cannot be evaluated
    // (division by zero, a position past the 64-bit range).
    void RequireBlock(
        const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars, std::size_t block) const;
    // The same for every present block of the state.
    void RequireChunk(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
        std::span<const std::uint8_t> presence, const ChunkVars& vars) const;
    // The same for a chunk holding `current` before `update`.
    void RequireChunkAfter(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
        std::span<const std::uint8_t> presence, const ChunkVars& current, const VarUpdate& update) const;
    // For the ADD CHECK scan: the first present block of the state the check
    // in progress refuses, as "block (x, y) fails it" or "block (x, y)
    // cannot evaluate it: <reason>"; std::nullopt when none does.
    [[nodiscard]] std::optional<std::string> FindPendingFailure(const ChunkCoord& coord,
        std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence, const ChunkVars& vars) const;

    struct Planned;
    BlockChecks(const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout, std::vector<Planned> checks);
    ~BlockChecks();
    BlockChecks(const BlockChecks&) = delete;
    BlockChecks& operator=(const BlockChecks&) = delete;

  private:
    // Whether check `check` is false for block `block`; throws as
    // RequireBlock when it cannot be evaluated.
    [[nodiscard]] bool Fails(const Planned& check, const ChunkCoord& coord, std::span<const std::uint8_t> payload,
        const ChunkVars& vars, std::size_t block, std::vector<ColumnValue>& values) const;
    void RequireBlockWith(const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars,
        std::size_t block, std::vector<ColumnValue>& values) const;
    // "block (x, y)", "block (x, y, z)"; an axis past int64 as [chunk, local].
    [[nodiscard]] std::string DescribeBlock(const ChunkCoord& coord, std::size_t block) const;

    std::uint32_t width_ = 0;
    std::uint32_t height_ = 0;
    std::uint32_t depth_ = 0;
    std::uint8_t dims_ = 2;
    std::shared_ptr<const ChunkLayout> layout_;
    // The table's checks, then the one in progress, if any.
    std::vector<Planned> checks_;
};

}  // namespace chunkdb
