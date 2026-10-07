#include "history_read.hpp"

#include <algorithm>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <utility>

namespace chunkdb::history {

namespace {

// A record of a segment, or (segment -1) the pending mutations.
struct Unit {
    int segment = -1;
    const RecordRef* record = nullptr;
    // A keyframe record: no events, the state after first_revision.
    bool keyframe = false;
    std::uint64_t first_revision = 0;
    std::uint64_t last_revision = 0;
    std::uint64_t first_time_ms = 0;
    std::uint64_t last_time_ms = 0;
};

class UnitReader {
  public:
    UnitReader(const Geometry& geometry, const ChunkHistorySource& source, ScanBudget* budget)
        : geometry_(geometry), source_(source), budget_(budget) {
        const auto& segments = source.segments->segments;
        for (std::size_t s = 0; s < segments.size(); ++s) {
            for (const auto& record : segments[s].records) {
                units.push_back(Unit{
                    .segment = static_cast<int>(s),
                    .record = &record,
                    .keyframe = record.summary.keyframe,
                    .first_revision = record.summary.first_revision,
                    .last_revision = record.summary.last_revision,
                    .first_time_ms = record.summary.first_time_ms,
                    .last_time_ms = record.summary.last_time_ms,
                });
            }
        }
        if (source.pending != nullptr && !source.pending->empty()) {
            units.push_back(Unit{
                .first_revision = source.pending->front().revision,
                .last_revision = source.pending->back().revision,
                .first_time_ms = source.pending->front().time_ms,
                .last_time_ms = source.pending->back().time_ms,
            });
        }
    }

    [[nodiscard]] std::size_t Cost(const Unit& unit) const noexcept {
        return unit.record == nullptr ? 0U : unit.record->summary.size;
    }

    // Shows the unit's mutations to `visit` in order until it returns false.
    void Visit(std::size_t index, const std::function<bool(const MutationView&)>& visit) {
        const Unit& unit = units[index];
        if (unit.record == nullptr) {
            if (pending_views_.empty()) {
                pending_views_ = ViewMutations(*source_.pending, &pending_storage_);
            }
            for (const auto& mutation : pending_views_) {
                if (!visit(mutation)) {
                    return;
                }
            }
            return;
        }
        const auto& segment = source_.segments->segments[static_cast<std::size_t>(unit.segment)];
        const auto& bytes = Records(unit.segment);
        const std::size_t offset = unit.record->offset;
        const auto read = VisitRecord(geometry_, bytes.data() + offset, bytes.size() - offset, &visit);
        if (read.status != RecordStatus::kOk || read.summary.size != unit.record->summary.size ||
            read.summary.first_revision != unit.first_revision) {
            throw HistoryDamagedError(
                "history of " + segment.path.string() + " is damaged: a record changed after it was read" +
                (read.status == RecordStatus::kDamaged ? std::string(" (") + read.problem + ")" : std::string()));
        }
        budget_->remaining -= std::min(budget_->remaining, unit.record->summary.size);
    }

    // The keyframe a segment starts from.
    const ChunkState& Keyframe(int index) {
        auto& file = File(index);
        if (!file.header.has_value()) {
            const auto& segment = source_.segments->segments[static_cast<std::size_t>(index)];
            std::size_t header_size = 0;
            try {
                file.header = ReadSegmentHeader(geometry_, file.bytes, segment_store_id(), source_.chunk, &header_size);
            } catch (const std::exception& e) {
                throw HistoryDamagedError("history of " + segment.path.string() + " is damaged: " + e.what());
            }
            if (header_size != segment.header_size || !file.header->keyframe.has_value()) {
                throw HistoryDamagedError("history of " + segment.path.string() + " is damaged: its header changed");
            }
        }
        return *file.header->keyframe;
    }

    // The newest state the history holds that `accept` takes by its
    // revision and time (a keyframe record, a segment's keyframe or a first
    // segment's empty base, or what the pending mutations start from), and
    // the first unit to replay after it.
    struct Start {
        const ChunkState* state = nullptr;
        std::size_t unit = 0;
    };
    Start FindStart(const std::function<bool(std::uint64_t, std::uint64_t)>& accept) {
        const auto& segments = source_.segments->segments;
        for (std::size_t u = units.size(); u > 0; --u) {
            const Unit& unit = units[u - 1U];
            if (unit.record == nullptr) {
                if (accept(source_.pending_base_revision, source_.pending_base_time_ms)) {
                    return Start{.state = source_.pending_base, .unit = u - 1U};
                }
                continue;
            }
            if (unit.keyframe && accept(unit.first_revision, unit.first_time_ms)) {
                return Start{.state = &KeyframeRecord(u - 1U), .unit = u};
            }
            if (u == 1U || units[u - 2U].segment != unit.segment) {
                const auto& segment = segments[static_cast<std::size_t>(unit.segment)];
                if ((segment.keyframe || segment.first) && accept(segment.base_revision, segment.base_time_ms)) {
                    if (segment.keyframe) {
                        return Start{.state = &Keyframe(unit.segment), .unit = u - 1U};
                    }
                    empty_ = EmptyChunkState(geometry_);
                    return Start{.state = &empty_, .unit = u - 1U};
                }
            }
        }
        throw HistoryDamagedError(
            "history of chunk (" + std::to_string(source_.chunk.x) + "," + std::to_string(source_.chunk.y) +
            ") has no state to start from at the point read");
    }

    std::vector<Unit> units;

  private:
    const ChunkState& KeyframeRecord(std::size_t index) {
        const Unit& unit = units[index];
        const auto& segment = source_.segments->segments[static_cast<std::size_t>(unit.segment)];
        const auto& bytes = Records(unit.segment);
        const auto read = ReadKeyframeRecord(
            geometry_, bytes.data() + unit.record->offset, bytes.size() - unit.record->offset, &keyframe_record_);
        if (read.status != RecordStatus::kOk || read.summary.first_revision != unit.first_revision) {
            throw HistoryDamagedError(
                "history of " + segment.path.string() + " is damaged: a keyframe record changed after it was read");
        }
        return keyframe_record_;
    }

    struct SegmentFile {
        std::vector<std::uint8_t> bytes{};
        std::optional<SegmentHeader> header{};
    };

    [[nodiscard]] const StoreId& segment_store_id() const noexcept { return source_.files->store_id(); }

    // A segment's valid bytes, read once per read.
    SegmentFile& File(int index) {
        if (const auto it = files_.find(index); it != files_.end()) {
            return it->second;
        }
        const auto& segment = source_.segments->segments[static_cast<std::size_t>(index)];
        return files_.emplace(index, SegmentFile{.bytes = source_.files->ReadValidBytes(segment)}).first->second;
    }

    const std::vector<std::uint8_t>& Records(int index) { return File(index).bytes; }

    const Geometry& geometry_;
    const ChunkHistorySource& source_;
    ScanBudget* budget_;
    std::map<int, SegmentFile> files_;
    ChunkState keyframe_record_;
    ChunkState empty_;
    std::vector<std::vector<ChangeView>> pending_storage_;
    std::vector<MutationView> pending_views_;
};

[[nodiscard]] bool Overlaps(const Geometry& geometry, const Unit& unit, const EventWindow& window) {
    if (unit.keyframe) {
        return false;
    }
    // Some event of the unit can lie strictly between the bounds.
    if (!(window.lo < Position{.revision = unit.last_revision, .block = Position::kAfterBlocks - 1}) ||
        !(Position{.revision = unit.first_revision, .block = 0} < window.hi)) {
        return false;
    }
    if ((window.since_ms.has_value() && unit.last_time_ms < *window.since_ms) ||
        (window.until_ms.has_value() && unit.first_time_ms > *window.until_ms)) {
        return false;
    }
    return !(window.block.has_value() && unit.record != nullptr &&
             !MaskHasBlock(geometry, unit.record->summary.block_mask, *window.block));
}

[[nodiscard]] bool MutationMatches(const MutationView& mutation, const EventWindow& window) {
    if ((window.since_ms.has_value() && mutation.time_ms < *window.since_ms) ||
        (window.until_ms.has_value() && mutation.time_ms > *window.until_ms)) {
        return false;
    }
    return !window.tag.has_value() ||
           std::equal(mutation.tag.begin(), mutation.tag.end(), window.tag->begin(), window.tag->end());
}

[[nodiscard]] bool ChangeMatches(const MutationView& mutation, const ChangeView& change, const EventWindow& window) {
    if (window.block.has_value() && change.block_index != *window.block) {
        return false;
    }
    const Position position{.revision = mutation.revision, .block = change.block_index};
    return window.lo < position && position < window.hi;
}

// The value a change leaves its block with, starting from `before`.
[[nodiscard]] std::optional<BlockValue> ValueAfter(
    const Geometry& geometry,
    const ChangeView& change,
    const std::optional<BlockValue>& before) {
    if (!change.present) {
        return std::nullopt;
    }
    BlockValue value;
    CopyChangeBits(geometry, change, &value.bits);
    switch (change.extra_change) {
        case ExtraChangeKind::kUnchanged:
            if (before.has_value()) {
                value.extra = before->extra;
            }
            break;
        case ExtraChangeKind::kSet:
            value.extra = change.extra.ToValue();
            break;
        case ExtraChangeKind::kRemoved:
            break;
    }
    return value;
}

}  // namespace

ChunkEvents CollectChunkEvents(
    const Geometry& geometry,
    const ChunkHistorySource& source,
    const EventWindow& window,
    ScanBudget* budget) {
    ChunkEvents out;
    UnitReader reader(geometry, source, budget);
    const auto& units = reader.units;

    // The events of the window, in its order, one past the limit. Tags
    // point into bytes the reader keeps for the whole read.
    struct Found {
        std::uint64_t revision = 0;
        std::uint64_t time_ms = 0;
        std::uint32_t block_index = 0;
        std::span<const std::uint8_t> tag{};
    };
    std::vector<Found> found;
    std::vector<Found> in_unit;
    bool decoded_any = false;
    const std::size_t unit_count = units.size();
    for (std::size_t step = 0; step < unit_count && found.size() <= window.limit; ++step) {
        const std::size_t u = window.descending ? unit_count - 1U - step : step;
        const Unit& unit = units[u];
        if (!Overlaps(geometry, unit, window)) {
            continue;
        }
        if (decoded_any && reader.Cost(unit) > budget->remaining) {
            out.frontier = window.descending ? Position{.revision = unit.last_revision + 1U, .block = Position::kBeforeBlocks}
                                             : Position{.revision = unit.first_revision - 1U, .block = Position::kAfterBlocks};
            break;
        }
        decoded_any = true;
        // Oldest first a unit can stop at the limit; newest first needs all
        // of its matches to take them from the end.
        const std::size_t wanted = window.limit + 1U - found.size();
        in_unit.clear();
        reader.Visit(u, [&](const MutationView& mutation) {
            if (!MutationMatches(mutation, window)) {
                return true;
            }
            for (const auto& change : mutation.changes) {
                if (ChangeMatches(mutation, change, window)) {
                    in_unit.push_back(Found{
                        .revision = mutation.revision,
                        .time_ms = mutation.time_ms,
                        .block_index = change.block_index,
                        .tag = mutation.tag,
                    });
                    if (!window.descending && in_unit.size() == wanted) {
                        return false;
                    }
                }
            }
            return true;
        });
        if (window.descending) {
            for (auto it = in_unit.rbegin(); it != in_unit.rend() && found.size() <= window.limit; ++it) {
                found.push_back(*it);
            }
        } else {
            found.insert(found.end(), in_unit.begin(), in_unit.end());
        }
    }
    if (found.size() > window.limit) {
        found.resize(window.limit);
        out.more = true;
    }
    if (out.frontier.has_value()) {
        out.more = true;
    }
    if (found.empty()) {
        return out;
    }
    out.events.reserve(found.size());
    for (const auto& event : found) {
        out.events.push_back(ChunkEvent{
            .revision = event.revision,
            .time_ms = event.time_ms,
            .block_index = event.block_index,
            .tag = std::vector<std::uint8_t>(event.tag.begin(), event.tag.end()),
        });
    }

    std::uint64_t lowest = UINT64_MAX;
    std::uint64_t highest = 0;
    std::vector<std::uint32_t> blocks;
    for (const auto& event : out.events) {
        lowest = std::min(lowest, event.revision);
        highest = std::max(highest, event.revision);
        blocks.push_back(event.block_index);
    }
    std::sort(blocks.begin(), blocks.end());
    blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
    BlockMask needed{};
    for (const auto block : blocks) {
        const std::size_t bit = BlockMaskBit(geometry, block);
        needed[bit / 8U] = static_cast<std::uint8_t>(needed[bit / 8U] | (1U << (bit % 8U)));
    }
    std::vector<std::pair<Position, std::size_t>> wanted;
    for (std::size_t i = 0; i < out.events.size(); ++i) {
        wanted.emplace_back(out.events[i].position(), i);
    }
    std::sort(wanted.begin(), wanted.end());

    // The values before and after each event: from the newest state the
    // history holds below the oldest event (a keyframe, an empty first
    // segment, or what the pending mutations start from), forward.
    const auto start = reader.FindStart([&](std::uint64_t revision, std::uint64_t) { return revision < lowest; });
    const std::size_t start_unit = start.unit;
    // Per block of the chunk, its place in `blocks` (or none).
    std::vector<std::uint32_t> slot(geometry.ChunkBlockCount(), UINT32_MAX);
    std::vector<std::optional<BlockValue>> values;
    values.reserve(blocks.size());
    for (std::size_t i = 0; i < blocks.size(); ++i) {
        slot[blocks[i]] = static_cast<std::uint32_t>(i);
        values.push_back(BlockValueOf(geometry, *start.state, blocks[i]));
    }
    std::size_t filled = 0;
    for (std::size_t u = start_unit; u < units.size() && units[u].first_revision <= highest; ++u) {
        if (units[u].record != nullptr) {
            const auto& mask = units[u].record->summary.block_mask;
            bool intersects = false;
            for (std::size_t i = 0; i < mask.size() && !intersects; ++i) {
                intersects = (mask[i] & needed[i]) != 0U;
            }
            if (!intersects) {
                continue;
            }
        }
        reader.Visit(u, [&](const MutationView& mutation) {
            if (mutation.revision > highest) {
                return false;
            }
            for (const auto& change : mutation.changes) {
                if (slot[change.block_index] == UINT32_MAX) {
                    continue;
                }
                auto& value = values[slot[change.block_index]];
                auto after = ValueAfter(geometry, change, value);
                const Position position{.revision = mutation.revision, .block = change.block_index};
                const auto event = std::lower_bound(
                    wanted.begin(), wanted.end(), std::pair<Position, std::size_t>{position, 0},
                    [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
                if (event != wanted.end() && event->first == position) {
                    out.events[event->second].before = value;
                    out.events[event->second].after = after;
                    ++filled;
                }
                value = std::move(after);
            }
            return true;
        });
    }
    if (filled != out.events.size()) {
        throw HistoryDamagedError(
            "history of chunk (" + std::to_string(source.chunk.x) + "," + std::to_string(source.chunk.y) +
            ") does not reach its events from its keyframes");
    }
    return out;
}

ChunkState StateAt(
    const Geometry& geometry,
    const ChunkHistorySource& source,
    const ChunkState& current,
    std::optional<std::uint64_t> revision,
    std::optional<std::uint64_t> time_ms) {
    const auto included = [&](std::uint64_t mutation_revision, std::uint64_t mutation_time) {
        return time_ms.has_value() ? mutation_time <= *time_ms : mutation_revision <= *revision;
    };
    const bool has_pending = source.pending != nullptr && !source.pending->empty();
    const std::uint64_t last_revision = has_pending ? source.pending->back().revision : source.segments->last_revision();
    const std::uint64_t last_time = has_pending ? source.pending->back().time_ms : source.segments->last_time_ms();
    if (last_revision == 0U || included(last_revision, last_time)) {
        return current;
    }
    ScanBudget budget{.remaining = SIZE_MAX};
    UnitReader reader(geometry, source, &budget);
    const auto& units = reader.units;
    const auto start = reader.FindStart(included);
    ChunkState state = *start.state;
    const std::size_t start_unit = start.unit;
    bool done = false;
    for (std::size_t u = start_unit; u < units.size() && !done; ++u) {
        if (!included(units[u].first_revision, units[u].first_time_ms)) {
            break;
        }
        reader.Visit(u, [&](const MutationView& mutation) {
            if (!included(mutation.revision, mutation.time_ms)) {
                done = true;
                return false;
            }
            for (const auto& change : mutation.changes) {
                ApplyChange(geometry, change, &state);
            }
            return true;
        });
    }
    return state;
}

}  // namespace chunkdb::history
