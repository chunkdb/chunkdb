#include "populated_index_format.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <stdexcept>

#include "chunkdb/crc32.hpp"

namespace chunkdb {
namespace {

std::uint64_t ReadLe(const std::uint8_t* at, unsigned width) noexcept {
    std::uint64_t value = 0;
    for (unsigned i = 0; i < width; ++i) value |= static_cast<std::uint64_t>(at[i]) << (8U * i);
    return value;
}

void WriteLe(std::vector<std::uint8_t>* out, std::uint64_t value, unsigned width) {
    for (unsigned i = 0; i < width; ++i) out->push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}

void RequireDimensions(std::uint8_t dims) {
    if (dims != 2 && dims != 3) throw std::invalid_argument("index dimensions must be 2 or 3");
}

void RequireLargeCoord(const LargeChunkCoord& coord, std::uint8_t dims) {
    RequireDimensions(dims);
    if (dims == 2 && coord.z != 0) throw std::invalid_argument("inactive index z coordinate must be 0");
}

void WriteCoord(std::vector<std::uint8_t>* bytes, const LargeChunkCoord& coord, std::uint8_t dims) {
    WriteLe(bytes, std::bit_cast<std::uint64_t>(coord.x), 8);
    WriteLe(bytes, std::bit_cast<std::uint64_t>(coord.y), 8);
    if (dims == 3) WriteLe(bytes, std::bit_cast<std::uint64_t>(coord.z), 8);
}

LargeChunkCoord ReadCoord(const std::uint8_t* bytes, std::uint8_t dims) noexcept {
    return {std::bit_cast<std::int64_t>(ReadLe(bytes, 8)),
            std::bit_cast<std::int64_t>(ReadLe(bytes + 8, 8)),
            dims == 3 ? std::bit_cast<std::int64_t>(ReadLe(bytes + 16, 8)) : 0};
}

void RequireChecksum(std::span<const std::uint8_t> bytes) {
    if (ReadLe(bytes.data() + bytes.size() - 4U, 4) != Crc32(bytes.data(), bytes.size() - 4U))
        throw std::invalid_argument("index checksum mismatch");
}

void AddChecksum(std::vector<std::uint8_t>* bytes) {
    WriteLe(bytes, Crc32(bytes->data(), bytes->size()), 4);
}

void RequireRecord(const ChunkIndexRecord& record, const Geometry& geometry, const LargeChunkCoord& large_coord) {
    RequireLargeCoord(large_coord, geometry.dims());
    if (record.kind != ChunkIndexKind::kLive && record.kind != ChunkIndexKind::kDeleted &&
        record.kind != ChunkIndexKind::kForget)
        throw std::invalid_argument("unknown index record kind");
    if (geometry.ChunkToLarge(record.coord) != large_coord)
        throw std::invalid_argument("index record belongs to another large chunk");
    if (record.revision == 0 || record.schema_version == 0)
        throw std::invalid_argument("index record revision and schema version must be nonzero");
}

void RequireInventory(const IndexInventoryHeader& header) {
    RequireDimensions(header.dims);
    if (header.generation == 0U || (header.compaction_pending &&
        header.generation == std::numeric_limits<std::uint64_t>::max()))
        throw std::invalid_argument("index inventory generation cannot advance");
    if ((!header.compaction_pending && header.compaction_through != 0) ||
        header.compaction_through > header.retained_deletion_floor)
        throw std::invalid_argument("index inventory compaction bounds are invalid");
}

void RequireInventoryEntry(const IndexInventoryHeader& header, const IndexInventoryEntry& entry) {
    RequireLargeCoord(entry.coord, header.dims);
    const auto header_bytes = 28U + 8U * header.dims;
    const auto record_bytes = 36U + 8U * header.dims;
    const auto whole_log = [&](std::uint64_t bytes) {
        return bytes >= header_bytes && (bytes - header_bytes) % record_bytes == 0U;
    };
    if (!whole_log(entry.committed_bytes))
        throw std::invalid_argument("index inventory committed boundary is not a whole log");
    if (header.compaction_pending) {
        if (!whole_log(entry.replacement_bytes))
            throw std::invalid_argument("index inventory replacement boundary is not a whole log");
    } else if (entry.replacement_bytes != 0U || entry.replacement_crc32 != 0U) {
        throw std::invalid_argument("index inventory has an undecided replacement");
    }
}

void ReadExact(std::istream& input, std::span<std::uint8_t> bytes) {
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!input) throw std::invalid_argument("index inventory is truncated or unreadable");
}

void WriteExact(std::ostream& output, std::span<const std::uint8_t> bytes) {
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!output) throw std::runtime_error("failed to write index inventory");
}

void RequireEnd(std::istream& input) {
    try {
        if (input.get() != std::char_traits<char>::eof())
            throw std::invalid_argument("bytes after index file");
    } catch (const std::ios_base::failure&) {
        if (input.bad() || !input.eof()) throw;
    }
    if (input.bad() || !input.eof()) throw std::invalid_argument("index stream is unreadable");
}

bool HasRecordChecksum(std::span<const std::uint8_t> bytes) noexcept {
    return ReadLe(bytes.data() + bytes.size() - 4U, 4) == Crc32(bytes.data(), bytes.size() - 4U);
}

void AddRecord(CheckedChunkIndexLog* log, const ChunkIndexRecord& record) {
    const auto previous = log->latest.find(record.coord);
    if (previous == log->latest.end()) {
        if (record.kind == ChunkIndexKind::kForget)
            throw std::invalid_argument("index forget record has no deleted predecessor");
        log->latest.emplace(record.coord, record);
        if (record.kind != ChunkIndexKind::kLive) log->empty_boundaries[record.coord] = record;
        return;
    }
    const auto& old = previous->second;
    if (record.revision < old.revision || record.schema_version < old.schema_version)
        throw std::invalid_argument("index record producing identity moves backwards");
    if (record.revision == old.revision) {
        if (record.commit_time_ms != old.commit_time_ms || record.schema_version != old.schema_version)
            throw std::invalid_argument("index record conflicts with producing identity");
        const bool same = record.kind == old.kind;
        const bool collection = old.kind == ChunkIndexKind::kLive && record.kind == ChunkIndexKind::kDeleted;
        const bool forget = old.kind == ChunkIndexKind::kDeleted && record.kind == ChunkIndexKind::kForget;
        if (!same && !collection && !forget)
            throw std::invalid_argument("index record has an invalid equal-version transition");
    } else if (record.kind == ChunkIndexKind::kForget) {
        throw std::invalid_argument("index forget record changes the deleted version");
    }
    previous->second = record;
    if (record.kind != ChunkIndexKind::kLive) log->empty_boundaries[record.coord] = record;
}

}  // namespace

std::vector<std::uint8_t> SerializeChunkIndexHeader(const ChunkIndexHeader& header) {
    RequireLargeCoord(header.large_coord, header.dims);
    std::vector<std::uint8_t> bytes;
    bytes.reserve(28U + 8U * header.dims);
    bytes.insert(bytes.end(), {'C', 'K', 'I', 'X'});
    WriteLe(&bytes, 1, 2);
    bytes.push_back(header.dims);
    bytes.push_back(0);
    bytes.insert(bytes.end(), header.table_id.begin(), header.table_id.end());
    WriteCoord(&bytes, header.large_coord, header.dims);
    AddChecksum(&bytes);
    return bytes;
}

ChunkIndexHeader ParseChunkIndexHeader(
    std::span<const std::uint8_t> bytes, const Geometry& geometry,
    const StoreId& table_id, const LargeChunkCoord& large_coord) {
    RequireLargeCoord(large_coord, geometry.dims());
    if (bytes.size() != 28U + 8U * geometry.dims()) throw std::invalid_argument("index header has wrong length");
    if (bytes[0] != 'C' || bytes[1] != 'K' || bytes[2] != 'I' || bytes[3] != 'X')
        throw std::invalid_argument("not a CKIX index");
    if (ReadLe(bytes.data() + 4, 2) != 1) throw std::invalid_argument("unsupported index version");
    if (bytes[6] != geometry.dims() || bytes[7] != 0)
        throw std::invalid_argument("index header dimensions or reserved field are invalid");
    RequireChecksum(bytes);
    if (!std::equal(table_id.begin(), table_id.end(), bytes.begin() + 8))
        throw std::invalid_argument("index belongs to another table");
    const auto stored_large = ReadCoord(bytes.data() + 24, geometry.dims());
    if (stored_large != large_coord) throw std::invalid_argument("index belongs to another large chunk");
    return {table_id, geometry.dims(), stored_large};
}

std::vector<std::uint8_t> SerializeChunkIndexRecord(
    const ChunkIndexRecord& record, const Geometry& geometry, const LargeChunkCoord& large_coord) {
    RequireRecord(record, geometry, large_coord);
    std::vector<std::uint8_t> bytes;
    bytes.reserve(36U + 8U * geometry.dims());
    WriteLe(&bytes, 36U + 8U * geometry.dims(), 4);
    bytes.push_back(static_cast<std::uint8_t>(record.kind));
    bytes.insert(bytes.end(), 3, 0);
    WriteCoord(&bytes, {record.coord.x, record.coord.y, record.coord.z}, geometry.dims());
    WriteLe(&bytes, record.revision, 8);
    WriteLe(&bytes, record.commit_time_ms, 8);
    WriteLe(&bytes, record.schema_version, 8);
    AddChecksum(&bytes);
    return bytes;
}

ChunkIndexRecord ParseChunkIndexRecord(
    std::span<const std::uint8_t> bytes, const Geometry& geometry, const LargeChunkCoord& large_coord) {
    if (bytes.size() != 36U + 8U * geometry.dims() || ReadLe(bytes.data(), 4) != bytes.size())
        throw std::invalid_argument("index record has wrong length");
    if (bytes[5] != 0 || bytes[6] != 0 || bytes[7] != 0)
        throw std::invalid_argument("index record reserved field is nonzero");
    RequireChecksum(bytes);
    const auto coord = ReadCoord(bytes.data() + 8, geometry.dims());
    const auto fields = 8U + 8U * geometry.dims();
    ChunkIndexRecord record{
        .kind = static_cast<ChunkIndexKind>(bytes[4]), .coord = {coord.x, coord.y, coord.z},
        .revision = ReadLe(bytes.data() + fields, 8),
        .commit_time_ms = ReadLe(bytes.data() + fields + 8U, 8),
        .schema_version = ReadLe(bytes.data() + fields + 16U, 8),
    };
    RequireRecord(record, geometry, large_coord);
    return record;
}

CheckedChunkIndexLog ReadChunkIndexLog(
    std::istream& input, std::uint64_t file_bytes, const Geometry& geometry,
    const StoreId& table_id, const LargeChunkCoord& large_coord,
    std::optional<std::uint64_t> committed_bytes) {
    const auto header_bytes = static_cast<std::size_t>(28U + 8U * geometry.dims());
    const auto record_bytes = static_cast<std::size_t>(36U + 8U * geometry.dims());
    if (file_bytes < header_bytes) throw std::invalid_argument("index log header is truncated");
    if (committed_bytes && (*committed_bytes < header_bytes ||
        (*committed_bytes - header_bytes) % record_bytes != 0U))
        throw std::invalid_argument("index log committed boundary is not whole");
    if (committed_bytes && file_bytes < *committed_bytes)
        throw std::invalid_argument("index log is shorter than its committed boundary");
    std::array<std::uint8_t, 60> storage{};
    auto header = std::span(storage).first(header_bytes);
    ReadExact(input, header);
    (void)ParseChunkIndexHeader(header, geometry, table_id, large_coord);
    CheckedChunkIndexLog log{.latest = {}, .empty_boundaries = {},
        .valid_bytes = header_bytes, .interrupted_tail = false};
    auto unread = committed_bytes.value_or(file_bytes) - header_bytes;
    const auto record = std::span(storage).first(record_bytes);
    while (unread != 0) {
        const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(unread, record_bytes));
        ReadExact(input, record.first(count));
        unread -= count;
        if (count == record_bytes) {
            if (!HasRecordChecksum(record))
                throw std::invalid_argument("index log complete record checksum mismatch");
            const auto parsed = ParseChunkIndexRecord(record, geometry, large_coord);
            if (parsed.schema_version > geometry.layout().schema().version)
                throw std::invalid_argument("index record schema is newer than table schema");
            AddRecord(&log, parsed);
            log.valid_bytes += record_bytes;
            continue;
        }
        // A complete invalid final record may be a previously synced deletion
        // boundary whose artifacts are gone. Never silently discard it.
        // Only a shorter final append prefix can reach this repair decision.
        log.interrupted_tail = true;
    }
    if (committed_bytes) {
        auto suffix = file_bytes - *committed_bytes;
        log.interrupted_tail = suffix != 0U;
        while (suffix != 0U) {
            const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(suffix, storage.size()));
            ReadExact(input, std::span(storage).first(count));
            suffix -= count;
        }
    }
    RequireEnd(input);
    return log;
}

std::uint64_t IndexInventoryFileBytes(const IndexInventoryHeader& header) {
    RequireInventory(header);
    const auto entry_bytes = static_cast<std::uint64_t>(8U * header.dims + 24U);
    if (header.count > (std::numeric_limits<std::uint64_t>::max() - 64U) / entry_bytes)
        throw std::invalid_argument("index inventory length overflows");
    return 64U + header.count * entry_bytes;
}

namespace {
IndexInventoryHeader ParseInventoryHeader(std::span<const std::uint8_t> fixed,
    std::uint64_t file_bytes, const Geometry& geometry, const StoreId& table_id) {
    if (file_bytes < 64U) throw std::invalid_argument("index inventory is truncated");
    if (fixed[0] != 'C' || fixed[1] != 'K' || fixed[2] != 'I' || fixed[3] != 'I')
        throw std::invalid_argument("not a CKII index inventory");
    if (ReadLe(fixed.data() + 4, 2) != 3) throw std::invalid_argument("unsupported index inventory version");
    if (fixed[6] != geometry.dims() || fixed[7] > 1)
        throw std::invalid_argument("index inventory dimensions or flags are invalid");
    if (!std::equal(table_id.begin(), table_id.end(), fixed.begin() + 8))
        throw std::invalid_argument("index inventory belongs to another table");
    RequireChecksum(fixed);
    IndexInventoryHeader header{
        .table_id = table_id, .dims = fixed[6], .compaction_pending = fixed[7] != 0,
        .retained_deletion_floor = ReadLe(fixed.data() + 24, 8),
        .compaction_through = ReadLe(fixed.data() + 32, 8), .count = ReadLe(fixed.data() + 40, 8),
        .generation = ReadLe(fixed.data() + 48, 8),
    };
    if (file_bytes != IndexInventoryFileBytes(header))
        throw std::invalid_argument("index inventory has wrong length");
    return header;
}
}  // namespace

IndexInventoryHeader ReadIndexInventoryHeader(std::istream& input, std::uint64_t file_bytes,
    const Geometry& geometry, const StoreId& table_id) {
    std::array<std::uint8_t, 60> fixed{};
    ReadExact(input, fixed);
    return ParseInventoryHeader(fixed, file_bytes, geometry, table_id);
}

IndexInventoryHeader ReadIndexInventory(
    std::istream& input, std::uint64_t file_bytes, const Geometry& geometry, const StoreId& table_id,
    const std::function<void(const IndexInventoryEntry&)>& candidate) {
    std::array<std::uint8_t, 60> fixed{};
    ReadExact(input, fixed);
    const auto header = ParseInventoryHeader(fixed, file_bytes, geometry, table_id);
    auto crc = Crc32(fixed.data(), fixed.size());
    LargeChunkCoord previous;
    std::array<std::uint8_t, 48> entry{};
    const auto fields = 8U * header.dims;
    const auto active_entry = std::span(entry).first(fields + 24U);
    for (std::uint64_t i = 0; i < header.count; ++i) {
        ReadExact(input, active_entry);
        crc = Crc32Extend(crc, active_entry.data(), active_entry.size());
        const auto coord = ReadCoord(entry.data(), header.dims);
        if (i != 0 && !(previous < coord))
            throw std::invalid_argument("index inventory coordinates must be unique and increasing");
        if (ReadLe(entry.data() + fields + 20U, 4) != 0U)
            throw std::invalid_argument("index inventory entry reserved field is nonzero");
        const IndexInventoryEntry parsed{coord, ReadLe(entry.data() + fields, 8),
            ReadLe(entry.data() + fields + 8U, 8),
            static_cast<std::uint32_t>(ReadLe(entry.data() + fields + 16U, 4))};
        RequireInventoryEntry(header, parsed);
        if (candidate) candidate(parsed);
        previous = coord;
    }
    std::array<std::uint8_t, 4> checksum{};
    ReadExact(input, checksum);
    if (ReadLe(checksum.data(), 4) != crc) throw std::invalid_argument("index inventory checksum mismatch");
    try {
        if (input.get() != std::char_traits<char>::eof())
            throw std::invalid_argument("bytes after index inventory");
    } catch (const std::ios_base::failure&) {
        // A caller may enable failbit exceptions. The deliberate end probe is
        // successful only at clean EOF; every actual stream error propagates.
        if (input.bad() || !input.eof()) throw;
    }
    if (input.bad() || !input.eof()) throw std::invalid_argument("index inventory stream is unreadable");
    return header;
}

void WriteIndexInventory(
    std::ostream& output, const IndexInventoryHeader& header, std::span<const IndexInventoryEntry> entries) {
    (void)IndexInventoryFileBytes(header);
    if (header.count != entries.size()) throw std::invalid_argument("index inventory count differs from entries");
    for (std::size_t i = 0; i < entries.size(); ++i) {
        RequireInventoryEntry(header, entries[i]);
        if (i != 0 && !(entries[i - 1].coord < entries[i].coord))
            throw std::invalid_argument("index inventory coordinates must be unique and increasing");
    }
    std::vector<std::uint8_t> fixed;
    fixed.reserve(60);
    fixed.insert(fixed.end(), {'C', 'K', 'I', 'I'});
    WriteLe(&fixed, 3, 2);
    fixed.push_back(header.dims);
    fixed.push_back(header.compaction_pending ? 1 : 0);
    fixed.insert(fixed.end(), header.table_id.begin(), header.table_id.end());
    WriteLe(&fixed, header.retained_deletion_floor, 8);
    WriteLe(&fixed, header.compaction_through, 8);
    WriteLe(&fixed, header.count, 8);
    WriteLe(&fixed, header.generation, 8);
    AddChecksum(&fixed);
    auto crc = Crc32(fixed);
    WriteExact(output, fixed);
    std::vector<std::uint8_t> entry;
    entry.reserve(48);
    for (const auto& shard : entries) {
        entry.clear();
        WriteCoord(&entry, shard.coord, header.dims);
        WriteLe(&entry, shard.committed_bytes, 8);
        WriteLe(&entry, shard.replacement_bytes, 8);
        WriteLe(&entry, shard.replacement_crc32, 4);
        WriteLe(&entry, 0, 4);
        crc = Crc32Extend(crc, entry.data(), entry.size());
        WriteExact(output, entry);
    }
    entry.clear();
    WriteLe(&entry, crc, 4);
    WriteExact(output, entry);
}

std::optional<TombstoneCompactionBounds> ComputeTombstoneCompactionBounds(
    std::uint64_t before, std::uint64_t completed_frontier, std::uint64_t old_floor) noexcept {
    if (before == 0) return std::nullopt;
    const auto through = std::min(before - 1U, completed_frontier);
    return TombstoneCompactionBounds{through, std::max(old_floor, through)};
}

bool TombstoneVersionDiscarded(std::uint64_t revision, std::uint64_t through) noexcept {
    return revision != 0 && revision <= through;
}

}  // namespace chunkdb
