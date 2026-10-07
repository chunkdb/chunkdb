#include "history_read.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <utility>

namespace chunkdb::history {

namespace {

// A record of a segment, or (segment -1) the pending mutations.
struct Unit {
    int segment = -1;
    const RecordRef* record = nullptr;
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

    const std::vector<Mutation>& Mutations(std::size_t index) {
        const Unit& unit = units[index];
        if (unit.record == nullptr) {
            return *source_.pending;
        }
        if (const auto it = decoded_.find(index); it != decoded_.end()) {
            return it->second;
        }
        const auto& contents = Segment(unit.segment);
        const auto& segment = source_.segments->segments[static_cast<std::size_t>(unit.segment)];
        const std::size_t offset = unit.record->offset;
        auto read = ReadRecord(
            geometry_, contents.bytes.data() + offset, contents.bytes.size() - offset, /*decode_body=*/true);
        if (read.status != RecordStatus::kOk || read.summary.size != unit.record->summary.size ||
            read.summary.first_revision != unit.first_revision) {
            throw HistoryDamagedError(
                "history of " + segment.path.string() + " is damaged: a record changed after it was read" +
                (read.status == RecordStatus::kDamaged ? std::string(" (") + read.problem + ")" : std::string()));
        }
        budget_->remaining -= std::min(budget_->remaining, unit.record->summary.size);
        return decoded_.emplace(index, std::move(read.mutations)).first->second;
    }

    const HistoryFiles::SegmentContents& Segment(int index) {
        if (const auto it = segments_.find(index); it != segments_.end()) {
            return it->second;
        }
        const auto& segment = source_.segments->segments[static_cast<std::size_t>(index)];
        return segments_.emplace(index, source_.files->ReadSegment(source_.chunk, segment)).first->second;
    }

    std::vector<Unit> units;

  private:
    const Geometry& geometry_;
    const ChunkHistorySource& source_;
    ScanBudget* budget_;
    std::map<std::size_t, std::vector<Mutation>> decoded_;
    std::map<int, HistoryFiles::SegmentContents> segments_;
};

[[nodiscard]] bool Overlaps(const Geometry& geometry, const Unit& unit, const EventWindow& window) {
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

[[nodiscard]] bool MutationMatches(const Mutation& mutation, const EventWindow& window) {
    if ((window.since_ms.has_value() && mutation.time_ms < *window.since_ms) ||
        (window.until_ms.has_value() && mutation.time_ms > *window.until_ms)) {
        return false;
    }
    return !window.tag.has_value() || mutation.tag == *window.tag;
}

[[nodiscard]] bool ChangeMatches(const Mutation& mutation, const BlockChange& change, const EventWindow& window) {
    if (window.block.has_value() && change.block_index != *window.block) {
        return false;
    }
    const Position position{.revision = mutation.revision, .block = change.block_index};
    return window.lo < position && position < window.hi;
}

struct EventRef {
    std::size_t unit = 0;
    std::size_t mutation = 0;
    std::size_t change = 0;
};

}  // namespace

ChunkEvents CollectChunkEvents(
    const Geometry& geometry,
    const ChunkHistorySource& source,
    const EventWindow& window,
    ScanBudget* budget) {
    ChunkEvents out;
    UnitReader reader(geometry, source, budget);
    const auto& units = reader.units;

    // The events of the window, in its order, one past the limit.
    std::vector<EventRef> found;
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
        const auto& mutations = reader.Mutations(u);
        for (std::size_t mstep = 0; mstep < mutations.size() && found.size() <= window.limit; ++mstep) {
            const std::size_t m = window.descending ? mutations.size() - 1U - mstep : mstep;
            const auto& mutation = mutations[m];
            if (!MutationMatches(mutation, window)) {
                continue;
            }
            const auto& changes = mutation.changes;
            for (std::size_t cstep = 0; cstep < changes.size() && found.size() <= window.limit; ++cstep) {
                const std::size_t c = window.descending ? changes.size() - 1U - cstep : cstep;
                if (ChangeMatches(mutation, changes[c], window)) {
                    found.push_back(EventRef{.unit = u, .mutation = m, .change = c});
                }
            }
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
    std::uint64_t lowest = UINT64_MAX;
    std::uint64_t highest = 0;
    std::vector<std::uint32_t> blocks;
    for (const auto& ref : found) {
        const auto& mutation = reader.Mutations(ref.unit)[ref.mutation];
        out.events.push_back(ChunkEvent{
            .revision = mutation.revision,
            .time_ms = mutation.time_ms,
            .block_index = mutation.changes[ref.change].block_index,
            .tag = mutation.tag,
        });
        lowest = std::min(lowest, mutation.revision);
        highest = std::max(highest, mutation.revision);
        blocks.push_back(mutation.changes[ref.change].block_index);
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
    const auto& segments = source.segments->segments;
    const ChunkState* start = nullptr;
    ChunkState start_storage;
    std::size_t start_unit = 0;
    const bool from_pending = source.pending != nullptr && !source.pending->empty() &&
                              lowest > source.segments->last_revision();
    if (from_pending) {
        start = source.pending_base;
        start_unit = units.size() - 1U;
    } else {
        std::optional<std::size_t> base_segment;
        for (std::size_t s = segments.size(); s > 0; --s) {
            const auto& segment = segments[s - 1U];
            if ((segment.keyframe || segment.first) && segment.base_revision < lowest) {
                base_segment = s - 1U;
                break;
            }
        }
        if (!base_segment.has_value()) {
            throw HistoryDamagedError(
                "history of chunk (" + std::to_string(source.chunk.x) + "," + std::to_string(source.chunk.y) +
                ") has no state to start from below revision " + std::to_string(lowest));
        }
        const auto& contents = reader.Segment(static_cast<int>(*base_segment));
        start_storage = contents.header.keyframe.has_value() ? *contents.header.keyframe : EmptyChunkState(geometry);
        start = &start_storage;
        while (start_unit < units.size() && units[start_unit].segment != static_cast<int>(*base_segment)) {
            ++start_unit;
        }
    }
    std::vector<std::optional<BlockValue>> values;
    values.reserve(blocks.size());
    for (const auto block : blocks) {
        values.push_back(BlockValueOf(geometry, *start, block));
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
        for (const auto& mutation : reader.Mutations(u)) {
            if (mutation.revision > highest) {
                break;
            }
            for (const auto& change : mutation.changes) {
                const auto it = std::lower_bound(blocks.begin(), blocks.end(), change.block_index);
                if (it == blocks.end() || *it != change.block_index) {
                    continue;
                }
                auto& value = values[static_cast<std::size_t>(it - blocks.begin())];
                auto after = BlockValueAfter(change, value);
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
        }
    }
    if (filled != out.events.size()) {
        throw HistoryDamagedError(
            "history of chunk (" + std::to_string(source.chunk.x) + "," + std::to_string(source.chunk.y) +
            ") does not reach its events from its keyframes");
    }
    return out;
}

}  // namespace chunkdb::history
