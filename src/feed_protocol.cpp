#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"
#include "store_manifest.hpp"
#include "chunkdb/feed_slots.hpp"
#include "chunk_store_internal.hpp"
#include "typed_chunk_form.hpp"
#include "sectioned_chunk_form.hpp"
#include <algorithm>
#include <charconv>
#include <stdexcept>
#include <limits>
#include <type_traits>
namespace chunkdb {
namespace {
void ValidateDims(std::uint8_t dims) {
    if (dims != 2U && dims != 3U) throw std::logic_error("feed entry has invalid dimensions");
}
void ValidateArea(std::uint8_t dims, const std::optional<FeedArea>& area) {
    ValidateDims(dims);
    if (!area) return;
    if (area->dims != dims || (dims == 2U && (area->first.z != 0 || area->last.z != 0)))
        throw std::invalid_argument("feed area dimensions differ from entry");
    if (area->first.x > area->last.x || area->first.y > area->last.y ||
        (dims == 3U && area->first.z > area->last.z)) throw std::invalid_argument("feed area is reversed");
}
bool InArea(ChunkCoord coord, std::uint8_t dims, const std::optional<FeedArea>& area) {
    return !area || (coord.x >= area->first.x && coord.x <= area->last.x &&
        coord.y >= area->first.y && coord.y <= area->last.y &&
        (dims != 3U || (coord.z >= area->first.z && coord.z <= area->last.z)));
}
void ValidateCoord(ChunkCoord coord, std::uint8_t dims) {
    if (dims == 2U && coord.z != 0) throw std::logic_error("feed entry has an inactive chunk coordinate");
}

template <class Integer>
void Number(std::string& out, char prefix, Integer value) {
    static_assert(sizeof(Integer) <= sizeof(std::uint64_t));
    // A prefix, every decimal digit of int64/uint64, and the terminator.
    char bytes[32];
    if constexpr (std::is_unsigned_v<Integer>) {
        if (prefix == ':' && value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) prefix = '(';
    }
    bytes[0] = prefix;
    const auto result = std::to_chars(bytes + 1, bytes + sizeof(bytes) - 2U, value);
    *result.ptr = '\r';
    *(result.ptr + 1) = '\n';
    out.append(bytes, static_cast<std::size_t>(result.ptr - bytes) + 2U);
}
void Value(std::string& out, const ColumnValue& value) {
    const auto* bits = std::get_if<BitsValue>(&value);
    if (bits == nullptr) { Protocol::AppendValue(out, value); return; }
    const auto& digits = bits->digits;
    if (digits.empty() || digits.size() > 65535U || digits.find_first_not_of("01") != std::string::npos)
        throw std::invalid_argument("invalid bits value");
    const auto size = (digits.size() + 7U) / 8U;
    Number(out, '$', size);
    const auto offset = out.size();
    out.resize(offset + size + 2U);
    for (std::size_t byte = 0U; byte < size; ++byte) {
        unsigned packed = 0U;
        const auto at = byte * 8U;
        if (digits.size() - at >= 8U) {
            // A full byte has no tail check per bit and can be unrolled.
            for (std::size_t bit = 0U; bit < 8U; ++bit)
                packed |= static_cast<unsigned>(digits[at + bit] == '1') << bit;
        } else {
            for (std::size_t bit = 0U; bit < digits.size() - at; ++bit)
                packed |= static_cast<unsigned>(digits[at + bit] == '1') << bit;
        }
        out[offset + byte] = static_cast<char>(packed);
    }
    out[offset + size] = '\r';
    out[offset + size + 1U] = '\n';
}
struct EncodedRange {
    std::size_t offset = 0U;
    std::size_t size = 0U;
    void Begin(const std::string& out) { offset = out.size(); }
    void End(const std::string& out) { size = out.size() - offset; }
    void Append(std::string& out) const {
        // Offsets survive growth. basic_string self-append also handles aliasing
        // when this append reallocates; the earlier range remains unchanged.
        out.append(out, offset, size);
    }
};
struct RowEncoding {
    const std::vector<ColumnValue>* previous = nullptr;
    EncodedRange range;
    void Append(std::string& out, const std::optional<std::vector<ColumnValue>>& row) {
        if (!row) { Protocol::AppendNull(out); return; }
        if (previous != nullptr && SameFeedValues(*previous, *row)) { range.Append(out); return; }
        range.Begin(out);
        Number(out, '*', row->size());
        for (const auto& value : *row) Value(out, value);
        range.End(out);
        previous = &*row;
    }
};
struct CoordinateEncoding {
    bool present = false;
    std::optional<std::int64_t> absolute;
    std::int64_t chunk = 0;
    std::uint32_t local = 0;
    EncodedRange range;
    void Append(std::string& out, std::optional<std::int64_t> next_absolute,
                std::int64_t next_chunk, std::uint32_t next_local) {
        if (present && absolute == next_absolute &&
            (absolute || (chunk == next_chunk && local == next_local))) { range.Append(out); return; }
        range.Begin(out);
        if (next_absolute) Number(out, ':', *next_absolute);
        else {
            out += "*2\r\n";
            Number(out, ':', next_chunk);
            Number(out, ':', next_local);
        }
        range.End(out);
        present = true; absolute = next_absolute; chunk = next_chunk; local = next_local;
    }
};
}
namespace {
[[nodiscard]] std::pair<std::size_t, std::size_t> SectionOf(
    const ChunkLayout& layout,
    const ChunkLayout::FixedColumn& fixed) {
    const auto& all = layout.fixed_columns();
    const auto next = std::find_if(all.begin(), all.end(), [&fixed](const ChunkLayout::FixedColumn& other) {
        return other.values > fixed.values;
    });
    const std::size_t end = next == all.end() ? layout.payload_bytes() : next->values;
    return {fixed.values, end - fixed.values};
}

}
std::string EncodeChunkForm(
    const ChunkLayout& layout,
    std::uint64_t version,
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence,
    const ChunkVars& vars,
    const std::vector<std::size_t>& columns) {
    const auto little = [](std::string& out, std::uint64_t value, std::size_t bytes) {
        for (std::size_t i = 0U; i < bytes; ++i) out.push_back(static_cast<char>((value >> (8U * i)) & 0xffU));
    };
    const auto bytes_of = [](std::span<const std::uint8_t> data, std::size_t offset, std::size_t size) {
        return std::string_view(reinterpret_cast<const char*>(data.data()) + offset, size);
    };
    std::string form;
    little(form, version, 8);
    little(form, layout.schema().version, 8);
    form += bytes_of(presence, 0, presence.size());
    if (columns.empty()) {
        form += bytes_of(payload, 0, payload.size());
        form += bytes_of(vars.Encode(), 0, vars.Encode().size());
        return form;
    }
    std::vector<std::uint32_t> var_ids;
    for (const std::size_t index : columns) {
        if (const auto* fixed = layout.FixedColumnAt(index); fixed != nullptr) {
            const auto [offset, size] = SectionOf(layout, *fixed);
            form += bytes_of(payload, offset, size);
        } else {
            var_ids.push_back(layout.schema().columns[index].id);
        }
    }
    for (const auto entry : vars) {
        if (std::find(var_ids.begin(), var_ids.end(), entry.key.column_id) == var_ids.end()) {
            continue;
        }
        little(form, entry.key.column_id, 4);
        little(form, entry.key.block_index, 4);
        little(form, entry.value.size(), 4);
        form.append(reinterpret_cast<const char*>(entry.value.data()), entry.value.size());
    }
    return form;
}

std::shared_ptr<const FeedEntry> ChunkFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const Geometry& geometry, const std::optional<FeedArea>& area,
    const std::optional<std::vector<std::uint32_t>>& ids) {
    if (entry->kind != FeedEntry::Kind::kChange && entry->kind != FeedEntry::Kind::kChunks) return entry;
    ValidateArea(entry->dims, area);
    if (entry->dims != geometry.dims()) throw std::logic_error("feed entry dimensions differ from table");
    if (ids && ids->empty()) throw std::invalid_argument("feed projection must not be empty");
    if (!entry->chunk_forms_complete) throw std::logic_error("chunk feed requires a complete preimage");
    auto result = std::make_shared<FeedEntry>();
    result->chunk_forms_complete = true;
    result->kind = FeedEntry::Kind::kChunks; result->position = entry->position;
    result->schema_version = entry->schema_version; result->commit_time_ms = entry->commit_time_ms; result->user = entry->user;
    result->dims = entry->dims;
    result->chunk_columns = entry->chunk_columns;
    const auto& layout = geometry.LayoutAt(entry->schema_version);
    std::vector<std::size_t> selected;
    if (ids) for (const auto id : *ids) {
        const auto& columns = layout.schema().columns;
        const auto at = std::find_if(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; });
        if (at == columns.end()) throw FeedSlotLostError("feed projected column was dropped", "schema_filter");
        const auto index = static_cast<std::size_t>(at - columns.begin());
        if (std::find(selected.begin(), selected.end(), index) != selected.end())
            throw std::invalid_argument("duplicate feed projection column");
        selected.push_back(index);
    }
    std::optional<Geometry> historical;
    for (auto chunk : entry->chunk_forms) {
        ValidateCoord(chunk.chunk, entry->dims);
        if (!InArea(chunk.chunk, entry->dims, area)) continue;
        if (ids && chunk.form) {
            const auto& form = *chunk.form;
            if (!historical) {
                auto config = geometry.config();
                config.block_bits = FixedBitsPerBlock(layout.schema());
                historical.emplace(config, layout.schema(), SchemaAtVersion(geometry.chunk_schema(), entry->schema_version));
            }
            const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(form.data()), form.size());
            const auto header = ParseChunkFormSections(bytes, bytes.size(), ChunkFormReadMode::kMutation).header;
            if (header.chunk_version != chunk.version || header.schema_version != entry->schema_version ||
                (header.flags & kChunkFormPrivate) != 0U) throw std::logic_error("captured chunk form identity mismatch");
            auto state = DecodeSectionedChunkState(*historical, bytes, kVarMaxChunkBytesLimit);
            state.version = header.chunk_version;
            state.commit_time_ms = header.commit_time_ms;
            state.written = true;
            const auto projected = EncodeSectionedChunkState(*historical, state, kVarMaxChunkBytesLimit, selected);
            chunk.form = std::make_shared<const std::string>(reinterpret_cast<const char*>(projected.data()), projected.size());
        }
        result->chunk_forms.push_back(std::move(chunk));
    }
    return result->chunk_forms.empty() ? nullptr : result;
}

std::shared_ptr<const FeedEntry> FilterFeedEntry(const std::shared_ptr<const FeedEntry>& entry,
    const std::optional<FeedArea>& area, const std::optional<std::vector<std::uint32_t>>& ids,
    bool no_before, const std::vector<Column>& columns) {
    if (entry->kind != FeedEntry::Kind::kChange && entry->kind != FeedEntry::Kind::kSchema) return entry;
    ValidateArea(entry->dims, area);
    if (ids && ids->empty()) throw std::invalid_argument("feed projection must not be empty");
    if (!area && !ids && !no_before) return entry;
    if (entry->kind == FeedEntry::Kind::kSchema && !ids) return entry;
    if (entry->kind == FeedEntry::Kind::kChange && area && !ids && !no_before &&
        std::all_of(entry->blocks.begin(), entry->blocks.end(), [&](const auto& block) {
            return InArea(block.chunk, entry->dims, area);
        }) && std::all_of(entry->chunk_changes.begin(), entry->chunk_changes.end(), [&](const auto& change) {
            return InArea(change.chunk, entry->dims, area);
        })) return entry->blocks.empty() && entry->chunk_changes.empty() ? nullptr : entry;
    std::vector<std::size_t> selected;
    if (ids) for (const auto id : *ids) {
        const auto at = std::find_if(columns.begin(), columns.end(), [&](const auto& column) { return column.id == id; });
        if (at == columns.end()) throw FeedSlotLostError("feed projected column was dropped", "schema_filter");
        const auto index = static_cast<std::size_t>(at - columns.begin());
        if (std::find(selected.begin(), selected.end(), index) != selected.end())
            throw std::invalid_argument("duplicate feed projection column");
        selected.push_back(index);
    }
    auto filtered = std::make_shared<FeedEntry>(*entry);
    filtered->protocol_frame.reset();
    if (entry->kind == FeedEntry::Kind::kSchema) {
        if (ids) {
            filtered->columns.clear();
            for (const auto index : selected) filtered->columns.push_back(columns[index]);
        }
        return filtered;
    }
    if (area) std::erase_if(filtered->blocks, [&](const auto& block) {
        return !InArea(block.chunk, entry->dims, area);
    });
    if (area) std::erase_if(filtered->chunk_changes, [&](const auto& change) {
        return !InArea(change.chunk, entry->dims, area);
    });
    if (filtered->blocks.empty() && filtered->chunk_changes.empty()) return nullptr;
    const auto project = [&](auto& row) {
        if (!row || !ids) return;
        if (row->size() != columns.size()) throw std::logic_error("feed row does not match its historical schema");
        std::vector<ColumnValue> values; values.reserve(selected.size());
        for (const auto index : selected) values.push_back((*row)[index]);
        row = std::move(values);
    };
    for (auto& block : filtered->blocks) {
        if (no_before) block.before.reset(); else project(block.before);
        project(block.after);
    }
    if (no_before) for (auto& change : filtered->chunk_changes) change.before.reset();
    return filtered;
}

std::string EncodeFeedEntry(const FeedEntry& entry) {
    if (entry.kind == FeedEntry::Kind::kEnd) return Protocol::Error("NO_TABLE", "watched table was dropped");
    if (entry.kind == FeedEntry::Kind::kResync) return EncodeFeedResync(entry.position, std::nullopt);
    if (entry.kind != FeedEntry::Kind::kProgress) ValidateDims(entry.dims);
    std::string out;
    if (entry.kind == FeedEntry::Kind::kSchema) {
        out = ">2\r\n";
        Protocol::AppendBulk(out, "schema");
        Protocol::AppendMapHeader(out, 6U);
        Protocol::AppendBulk(out, "epoch");
        Protocol::AppendBulk(out, StoreIdHex(entry.position.epoch));
        Protocol::AppendBulk(out, "revision");
        Number(out, ':', entry.position.revision);
        Protocol::AppendBulk(out, "schema_version");
        Number(out, ':', entry.schema_version);
        Protocol::AppendBulk(out, "dims");
        Number(out, ':', entry.dims);
        Protocol::AppendBulk(out, "columns");
        Protocol::AppendColumns(out, entry.columns);
        Protocol::AppendBulk(out, "chunk_columns");
        Protocol::AppendColumns(out, entry.chunk_columns);
        return out;
    }
    if (entry.kind == FeedEntry::Kind::kProgress) {
        out = ">3\r\n";
        Protocol::AppendBulk(out, "progress");
        Protocol::AppendBulk(out, StoreIdHex(entry.position.epoch));
        Number(out, ':', entry.position.revision);
        return out;
    }
    if (entry.kind == FeedEntry::Kind::kChunks) {
        out = ">9\r\n"; Protocol::AppendBulk(out, "chunks");
        Protocol::AppendBulk(out, StoreIdHex(entry.position.epoch)); Number(out, ':', entry.position.revision);
        Number(out, ':', entry.commit_time_ms);
        if (entry.user) Protocol::AppendBulk(out, *entry.user); else Protocol::AppendNull(out);
        Number(out, ':', entry.schema_version); Protocol::AppendArrayHeader(out, entry.chunk_forms.size());
        for (const auto& chunk : entry.chunk_forms) {
            ValidateCoord(chunk.chunk, entry.dims);
            Protocol::AppendArrayHeader(out, static_cast<std::size_t>(entry.dims) + 2U);
            Number(out, ':', chunk.chunk.x); Number(out, ':', chunk.chunk.y);
            if (entry.dims == 3U) Number(out, ':', chunk.chunk.z);
            Number(out, ':', chunk.version);
            if (chunk.form) Protocol::AppendBulk(out, *chunk.form); else Protocol::AppendNull(out);
        }
        out += "_\r\n_\r\n";
        return out;
    }
    if (entry.kind != FeedEntry::Kind::kChange) throw std::logic_error("unknown feed entry kind");
    out = ">10\r\n";
    Protocol::AppendBulk(out, "change");
    Protocol::AppendBulk(out, StoreIdHex(entry.position.epoch));
    Number(out, ':', entry.position.revision);
    Number(out, ':', entry.commit_time_ms);
    if (entry.user) Protocol::AppendBulk(out, *entry.user); else Protocol::AppendNull(out);
    Number(out, ':', entry.schema_version);
    Number(out, '*', entry.blocks.size());
    RowEncoding before, after;
    CoordinateEncoding x, y, z;
    for (const auto& block : entry.blocks) {
        ValidateCoord(block.chunk, entry.dims);
        if (entry.dims == 2U && (block.z.value_or(0) != 0 || block.local_z != 0U))
            throw std::logic_error("feed entry has an inactive block coordinate");
        Number(out, '*', static_cast<std::size_t>(entry.dims) + 2U);
        x.Append(out, block.x, block.chunk.x, block.local_x);
        y.Append(out, block.y, block.chunk.y, block.local_y);
        if (entry.dims == 3U) z.Append(out, block.z, block.chunk.z, block.local_z);
        before.Append(out, block.before);
        after.Append(out, block.after);
    }
    out += "_\r\n_\r\n";  // Reserved tag and transaction id.
    Number(out, '*', entry.chunk_changes.size());
    for (const auto& change : entry.chunk_changes) {
        ValidateCoord(change.chunk, entry.dims);
        Number(out, '*', static_cast<std::size_t>(entry.dims) + 2U);
        Number(out, ':', change.chunk.x); Number(out, ':', change.chunk.y);
        if (entry.dims == 3U) Number(out, ':', change.chunk.z);
        before.Append(out, change.before);
        after.Append(out, change.after);
    }
    return out;
}

namespace {
struct RetainedBytes {
    std::size_t total = 0U;
    void Add(std::size_t part) {
        if (part > std::numeric_limits<std::size_t>::max() - total)
            throw std::length_error("feed retained byte count overflows");
        total += part;
    }
    void Storage(std::size_t count, std::size_t width) {
        if (count > std::numeric_limits<std::size_t>::max() / width)
            throw std::length_error("feed retained storage size overflows");
        Add(count * width);
    }
};
}

std::size_t FeedRowRetainedBytes(const std::optional<std::vector<ColumnValue>>& row) {
    RetainedBytes bytes;
    if (row) {
        bytes.Storage(row->capacity(), sizeof(ColumnValue));
        for (const auto& value : *row) bytes.Add(std::visit([](const auto& v) -> std::size_t {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) return v.capacity();
            else if constexpr (std::is_same_v<T, BitsValue>) return v.digits.capacity();
            else if constexpr (std::is_same_v<T, BytesValue>) return v.bytes.capacity();
            else return 0U;
        }, value));
    }
    return bytes.total;
}

std::size_t FeedEntryRetainedBytes(const FeedEntry& entry) {
    RetainedBytes bytes;
    const auto columns = [&](const std::vector<Column>& list) {
        bytes.Storage(list.capacity(), sizeof(Column));
        for (const auto& column : list) { bytes.Add(column.name.capacity()); bytes.Add(column.default_value.capacity()); }
    };
    bytes.Add(sizeof(FeedEntry)); bytes.Add(2U * sizeof(void*));
    if (entry.protocol_frame) { bytes.Add(entry.protocol_frame->capacity()); bytes.Add(sizeof(std::string)); bytes.Add(2U * sizeof(void*)); }
    if (entry.user) bytes.Add(entry.user->capacity());
    bytes.Storage(entry.blocks.capacity(), sizeof(FeedBlockChange));
    for (const auto& block : entry.blocks) { bytes.Add(FeedRowRetainedBytes(block.before)); bytes.Add(FeedRowRetainedBytes(block.after)); }
    bytes.Storage(entry.chunk_changes.capacity(), sizeof(FeedChunkChange));
    for (const auto& change : entry.chunk_changes) { bytes.Add(FeedRowRetainedBytes(change.before)); bytes.Add(FeedRowRetainedBytes(change.after)); }
    bytes.Storage(entry.chunk_forms.capacity(), sizeof(FeedChunkForm));
    for (const auto& chunk : entry.chunk_forms) if (chunk.form) {
        bytes.Add(chunk.form->capacity()); bytes.Add(sizeof(std::string)); bytes.Add(2U * sizeof(void*));
    }
    columns(entry.columns); columns(entry.chunk_columns);
    return bytes.total;
}
std::string EncodeFeedResync(FeedPosition position, const std::optional<TableLineage>& lineage) {
    std::string out = ">5\r\n";
    Protocol::AppendBulk(out, "resync"); Protocol::AppendBulk(out, StoreIdHex(position.epoch));
    Number(out, ':', position.revision);
    if (lineage) {
        Protocol::AppendBulk(out, StoreIdHex(lineage->parent_epoch)); Number(out, ':', lineage->fork_revision);
    } else out += "_\r\n_\r\n";
    return out;
}
}
