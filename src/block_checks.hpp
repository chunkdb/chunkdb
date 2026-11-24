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
    // Whether `schema` has checks or an ADD CHECK in progress, so Make
    // returns a BlockChecks.
    [[nodiscard]] static bool Needed(const TableSchema& schema) noexcept;

    // Throws CheckFailedError for the first check block `block` of the state
    // fails, and std::invalid_argument or std::out_of_range, naming the check
    // and the block, when a check cannot be evaluated (division by zero, a
    // position past the 64-bit range). `overrides` are text and bytes values
    // that replace those of `vars` (a block write's changes).
    // With `defer_commit_time`, a transaction statement's check: checks that
    // read a DEFAULT COMMIT_TIME column, which COMMIT stamps, wait for
    // COMMIT.
    void RequireBlock(const ChunkCoord& coord, std::span<const std::uint8_t> payload, const ChunkVars& vars,
        std::size_t block, std::span<const VarChange> overrides = {}, bool defer_commit_time = false) const;
    // The same for every present block of the state that `base` lacks or
    // holds other values of, evaluating each check only when a column it
    // reads changed or the block is new.
    void RequireChanged(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
        std::span<const std::uint8_t> presence, const ChunkVars& vars, std::span<const std::uint8_t> base_payload,
        std::span<const std::uint8_t> base_presence, const ChunkVars& base_vars, bool defer_commit_time = false) const;
    // The same for a chunk holding `base_vars` that `update` changes.
    void RequireChangedAfter(const ChunkCoord& coord, std::span<const std::uint8_t> payload,
        std::span<const std::uint8_t> presence, const VarUpdate& update, std::span<const std::uint8_t> base_payload,
        std::span<const std::uint8_t> base_presence, const ChunkVars& base_vars) const;
    // For the ADD CHECK scan: the first present block of the state the check
    // in progress refuses, as "block (x, y) fails it" or "CHECK name cannot
    // be evaluated for block (x, y): <reason>"; std::nullopt when none does.
    [[nodiscard]] std::optional<std::string> FindPendingFailure(const ChunkCoord& coord,
        std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence, const ChunkVars& vars) const;

    struct Planned;
    BlockChecks(const GeometryConfig& config, std::shared_ptr<const ChunkLayout> layout, std::vector<Planned> checks);
    ~BlockChecks();
    BlockChecks(const BlockChecks&) = delete;
    BlockChecks& operator=(const BlockChecks&) = delete;

  private:
    // Where a block's values are read from.
    struct Source {
        std::span<const std::uint8_t> payload;
        const ChunkVars* vars = nullptr;
        std::span<const VarChange> overrides{};
    };
    // Reused buffers of one evaluation.
    struct Scratch {
        std::vector<ColumnValue> values;
        std::vector<std::uint8_t> bytes;
        std::vector<std::uint8_t> other;
    };

    // Whether check `check` is false for block `block`; throws as
    // RequireBlock when it cannot be evaluated.
    [[nodiscard]] bool Fails(
        const Planned& check, const ChunkCoord& coord, const Source& source, std::size_t block, Scratch& scratch) const;
    // Throws CheckFailedError when `check` fails for `block`.
    void Require(
        const Planned& check, const ChunkCoord& coord, const Source& source, std::size_t block, Scratch& scratch) const;
    // Whether column `index` of `block` differs between the two states.
    [[nodiscard]] bool Differs(std::size_t index, std::size_t block, const Source& state, const Source& base,
        Scratch& scratch) const;
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
