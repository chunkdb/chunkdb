#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "history_format.hpp"
#include "history_store.hpp"

// Reading block history: the events of one chunk in a window, with the
// block's value before and after each.
namespace chunkdb::history {

// Where an event is: mutations in revision order, the events of one
// mutation in block order. kBeforeBlocks and kAfterBlocks stand for the
// places before and after every event of a revision.
struct Position {
    static constexpr std::int64_t kBeforeBlocks = -1;
    static constexpr std::int64_t kAfterBlocks = std::int64_t{1} << 32;

    std::uint64_t revision = 0;
    std::int64_t block = 0;

    friend auto operator<=>(const Position&, const Position&) = default;
};

struct ChunkEvent {
    std::uint64_t revision = 0;
    std::uint64_t time_ms = 0;
    std::uint32_t block_index = 0;
    std::vector<std::uint8_t> tag{};
    // std::nullopt for an absent block.
    std::optional<BlockValue> before{};
    std::optional<BlockValue> after{};

    [[nodiscard]] Position position() const noexcept {
        return Position{.revision = revision, .block = block_index};
    }
};

// The events a read asks for: strictly between `lo` and `hi`, with a time
// in [since_ms, until_ms], the tag `tag` and the block `block` when given,
// at most `limit`, oldest first or (`descending`) newest first.
struct EventWindow {
    Position lo{.revision = 0, .block = Position::kBeforeBlocks};
    Position hi{.revision = UINT64_MAX, .block = Position::kAfterBlocks};
    std::optional<std::uint64_t> since_ms{};
    std::optional<std::uint64_t> until_ms{};
    std::optional<std::vector<std::uint8_t>> tag{};
    std::optional<std::uint32_t> block{};
    bool descending = false;
    std::size_t limit = 0;
};

// Record bytes a read may still decode. A chunk read always gets through
// at least one record, so a read makes progress whatever the budget.
struct ScanBudget {
    std::size_t remaining = 0;
};

struct ChunkEvents {
    // In the window's order; at most `limit`.
    std::vector<ChunkEvent> events;
    // More events of the window follow the last of them.
    bool more = false;
    // Set when the budget ran out: the read looked at nothing past this
    // position (in the window's direction).
    std::optional<Position> frontier;
};

// A chunk's history: its segments, and the mutations above them that its
// image and WAL hold (with the state they start from).
struct ChunkHistorySource {
    const HistoryFiles* files = nullptr;
    ChunkCoord chunk{};
    const ChunkHistory* segments = nullptr;
    const std::vector<Mutation>* pending = nullptr;
    const ChunkState* pending_base = nullptr;
};

// Throws HistoryDamagedError for history that cannot be read.
[[nodiscard]] ChunkEvents CollectChunkEvents(
    const Geometry& geometry,
    const ChunkHistorySource& source,
    const EventWindow& window,
    ScanBudget* budget);

// The chunk's state just after its last mutation at or below `revision`, or
// (with `time_ms`) with a commit time at or below it; `current` is its
// state after every mutation the source holds. The point must not lie
// before what the source keeps. Throws HistoryDamagedError.
[[nodiscard]] ChunkState StateAt(
    const Geometry& geometry,
    const ChunkHistorySource& source,
    const ChunkState& current,
    std::optional<std::uint64_t> revision,
    std::optional<std::uint64_t> time_ms);

}  // namespace chunkdb::history
