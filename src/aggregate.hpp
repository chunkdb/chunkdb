#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "chunkdb/chunk_layout.hpp"
#include "cql.hpp"

// AGGREGATE (docs/reference/AGGREGATE.md): COUNT(*), SUM, MIN, MAX and AVG
// over the present blocks of chunks.
namespace chunkdb {

// The functions of one AGGREGATE over the blocks added so far.
//
// COUNT(*) counts present blocks. SUM, MIN, MAX and AVG take an integer or
// float column and skip NULL values; over no value they are NULL. Integer
// SUM is exact in 128 bits and refused with std::out_of_range beyond them;
// integer AVG is that sum and the count, each rounded to f64, divided in
// f64. Float values are widened to f64 and summed in the order they are
// added; MIN and MAX order NaN above every other value.
class Aggregator {
  public:
    // Throws std::invalid_argument for an unknown column or one that holds
    // no integer or float values. `layout` outlives the aggregator.
    Aggregator(const ChunkLayout& layout, std::span<const cql::AggregateFunction> functions);

    // Adds the present blocks of one chunk laid out by the layout, in block
    // index order. Checks statement cancellation between groups of blocks.
    void Add(std::span<const std::uint8_t> payload, std::span<const std::uint8_t> presence);
    // Forgets every added block.
    void Reset() noexcept;
    // The present blocks added.
    [[nodiscard]] std::uint64_t blocks() const noexcept { return blocks_; }
    [[nodiscard]] std::size_t functions() const noexcept { return accumulators_.size(); }
    // The RESP3 value of function `index`.
    void AppendResult(std::string& out, std::size_t index) const;
    // One RESP3 value per function, in the order of the functions.
    void AppendResults(std::string& out) const;
    // The most bytes AppendResults appends.
    [[nodiscard]] std::size_t ResultBytesBound() const noexcept;

  private:
    struct Accumulator {
        cql::AggregateFunction function{};
        // The column's place in the payload; null for COUNT(*).
        const ChunkLayout::FixedColumn* fixed = nullptr;
        const Column* column = nullptr;
        bool floating = false;
        // Values added, NULL values not counted.
        std::uint64_t values = 0U;
        // The integer sum or extreme.
        __int128 integer = 0;
        // The float sum or extreme.
        double number = 0.0;
    };

    void AddValue(Accumulator& accumulator, const std::uint8_t* payload, std::size_t block);

    std::size_t block_count_ = 0U;
    std::size_t payload_bytes_ = 0U;
    std::vector<Accumulator> accumulators_;
    std::uint64_t blocks_ = 0U;
};

}  // namespace chunkdb
