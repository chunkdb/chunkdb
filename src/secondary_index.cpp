#include "secondary_index.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <set>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/chunk_layout.hpp"
#include "durability_io.hpp"

namespace chunkdb {
namespace {

constexpr std::array<std::uint8_t, 4> kManifestMagic{'C', 'K', 'X', 'M'};
constexpr std::array<std::uint8_t, 4> kRunMagic{'C', 'K', 'X', 'R'};
constexpr std::array<std::uint8_t, 4> kRunEndMagic{'C', 'K', 'X', 'E'};
constexpr std::uint16_t kFormatVersion = 1U;
constexpr std::size_t kRunHeaderBytes = 68U;
constexpr std::size_t kRunFooterBytes = 44U;
// An entry block holds at most this many entries or about this many bytes.
constexpr std::size_t kBlockEntries = 512U;
constexpr std::size_t kBlockBytes = 64U * 1024U;
constexpr std::string_view kManifestName = "manifest";
constexpr std::string_view kManifestTemporaryName = "manifest.tmp";
constexpr std::string_view kRunPrefix = "run-";

void Crash(const char* point) { if (ConsumeFailpointEnv(point)) ExitAbruptly(86); }

struct FenceOnFailure {
    bool& failed;
    bool complete = false;
    ~FenceOnFailure() { if (!complete) failed = true; }
};

void PutU8(std::vector<std::uint8_t>& out, std::uint8_t value) { out.push_back(value); }
void PutU16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    for (unsigned i = 0U; i < 2U; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}
void PutU32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    for (unsigned i = 0U; i < 4U; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}
void PutU64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (unsigned i = 0U; i < 8U; ++i) out.push_back(static_cast<std::uint8_t>(value >> (8U * i)));
}
void PutCoord(std::vector<std::uint8_t>& out, const ChunkCoord& coord, std::uint8_t dims) {
    PutU64(out, static_cast<std::uint64_t>(coord.x));
    PutU64(out, static_cast<std::uint64_t>(coord.y));
    if (dims == 3U) PutU64(out, static_cast<std::uint64_t>(coord.z));
}

// Reads little-endian fields from bytes it does not own, refusing to read
// past them.
class Reader {
  public:
    Reader(std::span<const std::uint8_t> bytes, const char* what) : bytes_(bytes), what_(what) {}
    [[nodiscard]] std::uint64_t Unsigned(std::size_t width) {
        Need(width);
        std::uint64_t value = 0U;
        for (std::size_t i = 0U; i < width; ++i) value |= static_cast<std::uint64_t>(bytes_[at_ + i]) << (8U * i);
        at_ += width;
        return value;
    }
    [[nodiscard]] std::uint8_t U8() { return static_cast<std::uint8_t>(Unsigned(1U)); }
    [[nodiscard]] std::uint16_t U16() { return static_cast<std::uint16_t>(Unsigned(2U)); }
    [[nodiscard]] std::uint32_t U32() { return static_cast<std::uint32_t>(Unsigned(4U)); }
    [[nodiscard]] std::uint64_t U64() { return Unsigned(8U); }
    [[nodiscard]] std::span<const std::uint8_t> Bytes(std::size_t count) {
        Need(count);
        const auto span = bytes_.subspan(at_, count);
        at_ += count;
        return span;
    }
    [[nodiscard]] ChunkCoord Coord(std::uint8_t dims) {
        ChunkCoord coord;
        coord.x = static_cast<std::int64_t>(U64());
        coord.y = static_cast<std::int64_t>(U64());
        if (dims == 3U) coord.z = static_cast<std::int64_t>(U64());
        return coord;
    }
    [[nodiscard]] std::size_t offset() const noexcept { return at_; }
    [[nodiscard]] bool done() const noexcept { return at_ == bytes_.size(); }

  private:
    void Need(std::size_t count) const {
        if (count > bytes_.size() - at_) throw std::runtime_error(std::string(what_) + " is truncated");
    }
    std::span<const std::uint8_t> bytes_;
    std::size_t at_ = 0U;
    const char* what_;
};

void RequireCrc(std::span<const std::uint8_t> covered, std::uint32_t expected, const char* what) {
    if (Crc32(covered.data(), covered.size()) != expected) throw std::runtime_error(std::string(what) + " checksum mismatch");
}

std::vector<std::uint8_t> ReadRange(std::istream& input, std::uint64_t offset, std::uint64_t count, const char* what) {
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(count));
    input.clear();
    input.seekg(static_cast<std::streamoff>(offset));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!input || input.gcount() != static_cast<std::streamsize>(bytes.size()))
        throw std::runtime_error(std::string(what) + " cannot be read");
    return bytes;
}

std::uint64_t FileBytes(std::istream& input) {
    input.clear();
    input.seekg(0, std::ios::end);
    const auto size = input.tellg();
    if (size < 0) throw std::runtime_error("index file size cannot be read");
    return static_cast<std::uint64_t>(size);
}

void PutEntry(std::vector<std::uint8_t>& out, const IndexEntry& entry, std::uint8_t dims) {
    PutU16(out, static_cast<std::uint16_t>(entry.key.size()));
    out.insert(out.end(), entry.key.begin(), entry.key.end());
    PutCoord(out, entry.coord, dims);
    PutU32(out, entry.block);
    PutU64(out, entry.removed);
}

IndexEntry ReadEntry(Reader& reader, std::uint8_t dims) {
    IndexEntry entry;
    const auto length = reader.U16();
    const auto key = reader.Bytes(length);
    entry.key.assign(key.begin(), key.end());
    entry.coord = reader.Coord(dims);
    entry.block = reader.U32();
    entry.removed = reader.U64();
    return entry;
}

// Digest the same open file that supplies the header and entries.
crypto::Sha256Digest FileDigest(std::istream& input, std::uint64_t* bytes) {
    input.clear();
    input.seekg(0, std::ios::beg);
    if (!input) throw std::runtime_error("index run cannot be read for its digest");
    crypto::Sha256 digest;
    std::array<char, 64U * 1024U> buffer{};
    *bytes = 0U;
    while (input.read(buffer.data(), static_cast<std::streamsize>(buffer.size())) || input.gcount() > 0) {
        const auto count = static_cast<std::size_t>(input.gcount());
        digest.Update(std::span(reinterpret_cast<const std::uint8_t*>(buffer.data()), count));
        *bytes += count;
    }
    if (input.bad() || !input.eof()) throw std::runtime_error("index run cannot be read for its digest");
    return digest.Finish();
}

// The header, chunk table and footer of one run, checked against what the
// manifest says of it; entries are read block by block.
class RunReader {
  public:
    RunReader(const std::filesystem::path& path, const IndexIdentity& identity, std::uint64_t generation,
        std::uint64_t expected_bytes, SharedFileInput* pinned = nullptr)
        : input_(pinned == nullptr ? owned_input_.emplace(path) : *pinned), dims_(identity.dims) {
        if (!input_.is_open()) throw std::runtime_error("index run is missing: " + path.filename().string());
        const auto size = FileBytes(input_);
        if (size != expected_bytes) throw std::runtime_error("index run has another size than its manifest lists");
        if (size < kRunHeaderBytes + kRunFooterBytes) throw std::runtime_error("index run is truncated");
        const auto header = ReadRange(input_, 0U, kRunHeaderBytes, "index run header");
        Reader head(header, "index run header");
        const auto magic = head.Bytes(4U);
        if (!std::equal(magic.begin(), magic.end(), kRunMagic.begin()) || head.U16() != kFormatVersion || head.U16() != 0U)
            throw std::runtime_error("index run has an unknown header");
        StoreId epoch{};
        const auto epoch_bytes = head.Bytes(epoch.size());
        std::copy(epoch_bytes.begin(), epoch_bytes.end(), epoch.begin());
        const auto dims = head.U8();
        if (head.U8() != 0U || head.U16() != 0U) throw std::runtime_error("index run has reserved bits set");
        const auto index_id = head.U32();
        const auto column_id = head.U32();
        const auto prefix = head.U32();
        (void)head.U64();
        const auto run_generation = head.U64();
        chunk_count_ = head.U64();
        const auto header_crc_at = head.offset();
        RequireCrc(std::span(header).first(header_crc_at), head.U32(), "index run header");
        if (epoch != identity.epoch || dims != identity.dims || index_id != identity.index_id ||
            column_id != identity.column_id || prefix != identity.prefix || run_generation != generation)
            throw std::runtime_error("index run belongs to another index or generation");

        const auto footer = ReadRange(input_, size - kRunFooterBytes, kRunFooterBytes, "index run footer");
        Reader foot(footer, "index run footer");
        chunks_at_ = foot.U64();
        blocks_at_ = foot.U64();
        block_index_at_ = foot.U64();
        entry_count_ = foot.U64();
        block_count_ = foot.U32();
        const auto end = foot.Bytes(4U);
        const auto footer_crc_at = foot.offset();
        RequireCrc(std::span(footer).first(footer_crc_at), foot.U32(), "index run footer");
        if (!std::equal(end.begin(), end.end(), kRunEndMagic.begin()) || chunks_at_ != kRunHeaderBytes ||
            chunks_at_ > blocks_at_ || blocks_at_ > block_index_at_ || block_index_at_ > size - kRunFooterBytes)
            throw std::runtime_error("index run has inconsistent section offsets");

        const std::size_t row = static_cast<std::size_t>(dims_) * 8U + 12U;
        if (chunk_count_ > (blocks_at_ - chunks_at_) / row) throw std::runtime_error("index run chunk table is truncated");
        const auto table = ReadRange(input_, chunks_at_, blocks_at_ - chunks_at_, "index run chunk table");
        if (table.size() != chunk_count_ * row + 4U) throw std::runtime_error("index run chunk table has another size");
        Reader rows(table, "index run chunk table");
        chunks_.reserve(static_cast<std::size_t>(chunk_count_));
        for (std::uint64_t i = 0U; i < chunk_count_; ++i) {
            IndexRunChunk chunk;
            chunk.coord = rows.Coord(dims_);
            chunk.revision = rows.U64();
            chunk.entries = rows.U32();
            if (!chunks_.empty() && !(chunks_.back().coord < chunk.coord))
                throw std::runtime_error("index run chunk table is out of order");
            chunks_.push_back(chunk);
        }
        const auto table_crc_at = rows.offset();
        RequireCrc(std::span(table).first(table_crc_at), rows.U32(), "index run chunk table");
    }

    [[nodiscard]] const std::vector<IndexRunChunk>& chunks() const noexcept { return chunks_; }
    [[nodiscard]] std::uint64_t entry_count() const noexcept { return entry_count_; }
    [[nodiscard]] bool MatchesDigest(const IndexRunRef& ref) {
        std::uint64_t bytes = 0U;
        return FileDigest(input_, &bytes) == ref.sha256 && bytes == ref.bytes;
    }

    // The next entry, or false after the last; checks each block's checksum
    // and the entry count.
    [[nodiscard]] bool Next(IndexEntry* entry) {
        while (pending_.empty() || next_ == pending_.size()) {
            if (offset_ == 0U) offset_ = blocks_at_;
            if (offset_ == block_index_at_) {
                if (seen_ != entry_count_ || blocks_read_ != block_count_)
                    throw std::runtime_error("index run holds another number of entries than its footer says");
                return false;
            }
            const auto head = ReadRange(input_, offset_, 8U, "index run block");
            Reader sizes(head, "index run block");
            const auto count = sizes.U32();
            const auto body = sizes.U32();
            if (block_index_at_ - offset_ < 12U || body > block_index_at_ - offset_ - 12U)
                throw std::runtime_error("index run block is truncated");
            auto block = ReadRange(input_, offset_, 8U + body + 4U, "index run block");
            Reader crc{std::span<const std::uint8_t>(block).subspan(8U + body), "index run block"};
            RequireCrc(std::span(block).first(8U + body), crc.U32(), "index run block");
            Reader reader{std::span<const std::uint8_t>(block).subspan(8U, body), "index run block"};
            pending_.clear();
            for (std::uint32_t i = 0U; i < count; ++i) pending_.push_back(ReadEntry(reader, dims_));
            if (!reader.done()) throw std::runtime_error("index run block has trailing bytes");
            next_ = 0U;
            offset_ += 8U + body + 4U;
            ++blocks_read_;
        }
        *entry = std::move(pending_[next_++]);
        ++seen_;
        return true;
    }

    // Each block of the block index names an entry block and its first entry.
    void CheckBlockIndex() {
        const auto bytes = ReadRange(input_, block_index_at_, input_size() - kRunFooterBytes - block_index_at_,
            "index run block index");
        Reader reader(bytes, "index run block index");
        const auto count = reader.U32();
        if (count != block_count_) throw std::runtime_error("index run block index has another number of blocks");
        // Each listed block must start where the previous one ends and begin
        // with the entry the index names.
        std::uint64_t block_at = blocks_at_;
        for (std::uint32_t i = 0U; i < count; ++i) {
            const auto offset = reader.U64();
            if (reader.U32() != 0U) throw std::runtime_error("index run block index has reserved bits set");
            const auto first = ReadEntry(reader, dims_);
            if (offset != block_at || block_index_at_ - block_at < 12U)
                throw std::runtime_error("index run block index does not match its blocks");
            const auto head = ReadRange(input_, block_at, 8U, "index run block");
            Reader sizes(head, "index run block");
            const auto entries = sizes.U32();
            const auto body = sizes.U32();
            if (entries == 0U || body > block_index_at_ - block_at - 12U)
                throw std::runtime_error("index run block index does not match its blocks");
            const auto block = ReadRange(input_, block_at + 8U, body, "index run block");
            Reader entry_reader(block, "index run block");
            const auto actual = ReadEntry(entry_reader, dims_);
            if (actual.key != first.key || !(actual.coord == first.coord) || actual.block != first.block ||
                actual.removed != first.removed)
                throw std::runtime_error("index run block index names another first entry");
            block_at += 8U + body + 4U;
        }
        if (block_at != block_index_at_) throw std::runtime_error("index run block index does not cover its blocks");
        const auto crc_at = reader.offset();
        RequireCrc(std::span(bytes).first(crc_at), reader.U32(), "index run block index");
        if (!reader.done()) throw std::runtime_error("index run block index has trailing bytes");
    }

  private:
    [[nodiscard]] std::uint64_t input_size() { return FileBytes(input_); }

    std::optional<SharedFileInput> owned_input_{};
    SharedFileInput& input_;
    std::uint8_t dims_;
    std::uint64_t chunk_count_ = 0U;
    std::uint64_t chunks_at_ = 0U;
    std::uint64_t blocks_at_ = 0U;
    std::uint64_t block_index_at_ = 0U;
    std::uint64_t entry_count_ = 0U;
    std::uint32_t block_count_ = 0U;
    std::vector<IndexRunChunk> chunks_;
    std::uint64_t offset_ = 0U;
    std::vector<IndexEntry> pending_;
    std::size_t next_ = 0U;
    std::uint64_t seen_ = 0U;
    std::uint32_t blocks_read_ = 0U;
};

// The largest manifest: its fixed fields, kMaxIndexRuns runs and the CRC.
constexpr std::uint64_t kMaxManifestBytes = 60U + kMaxIndexRuns * 48U + 4U;

std::vector<std::uint8_t> LoadWhole(const std::filesystem::path& path) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("index manifest cannot be opened");
    const auto size = FileBytes(input);
    if (size > kMaxManifestBytes) throw std::runtime_error("index manifest is larger than any manifest");
    return ReadRange(input, 0U, size, "index manifest");
}

bool RunGeneration(const std::string& name, std::uint64_t* generation) {
    if (!name.starts_with(kRunPrefix)) return false;
    return TryParseUint64(name.substr(kRunPrefix.size()), generation) && *generation != 0U;
}

bool RemovePublicationTemporary(const std::filesystem::path& path, const std::string& name) {
    // Like CleanupAtomicTmpArtifacts, remove only known temporary names here.
    // The sole index writer owns both the fixed manifest name and the run temps.
    if (name != kManifestTemporaryName) {
        const auto suffix = name.find(".tmp.", kRunPrefix.size());
        std::uint64_t generation = 0U;
        if (suffix == std::string::npos || !RunGeneration(name.substr(0U, suffix), &generation)) return false;
    }
    std::filesystem::remove(path);
    return true;
}

}  // namespace

void RequireIndexableColumn(const Column& column, std::uint32_t prefix) {
    switch (column.type.kind) {
        case ColumnKind::kText:
        case ColumnKind::kBytes:
            if (prefix > kMaxIndexKeyBytes)
                throw std::invalid_argument("PREFIX must be between 1 and " + std::to_string(kMaxIndexKeyBytes));
            if (prefix == 0U && column.type.size > kMaxIndexKeyBytes)
                throw std::invalid_argument("column " + column.name + " holds values of up to " +
                    std::to_string(column.type.size) + " bytes; index it with PREFIX n, n at most " +
                    std::to_string(kMaxIndexKeyBytes));
            return;
        case ColumnKind::kBits:
            if ((column.type.size + 7U) / 8U > kMaxIndexKeyBytes)
                throw std::invalid_argument("column " + column.name + " is wider than " +
                    std::to_string(kMaxIndexKeyBytes * 8U) + " bits and cannot be indexed");
            [[fallthrough]];
        default:
            if (prefix != 0U) throw std::invalid_argument("PREFIX applies only to text and bytes columns");
            return;
    }
}

bool IndexEntryLess(const IndexEntry& first, const IndexEntry& second) noexcept {
    if (first.key != second.key) return first.key < second.key;
    if (!(first.coord == second.coord)) return first.coord < second.coord;
    return first.block < second.block;
}

bool AppendFixedIndexKey(const Column& column, const std::uint8_t* value, std::vector<std::uint8_t>* key) {
    const auto width = column.type.size;
    const auto bytes = (width + 7U) / 8U;
    const auto raw = [&] {
        std::uint64_t bits = 0U;
        for (std::uint32_t i = 0U; i < bytes && i < 8U; ++i) bits |= static_cast<std::uint64_t>(value[i]) << (8U * i);
        return bits;
    };
    const auto big_endian = [&](std::uint64_t bits, std::uint32_t count) {
        for (std::uint32_t i = count; i-- > 0U;) key->push_back(static_cast<std::uint8_t>(bits >> (8U * i)));
    };
    switch (column.type.kind) {
        case ColumnKind::kUnsigned:
            big_endian(raw(), bytes);
            return true;
        case ColumnKind::kSigned:
            // Flipping the sign bit of the field orders two's complement values.
            big_endian(raw() ^ (std::uint64_t{1} << (width - 1U)), bytes);
            return true;
        case ColumnKind::kBool:
            key->push_back(static_cast<std::uint8_t>(value[0] & 1U));
            return true;
        case ColumnKind::kFloat32: {
            auto bits = static_cast<std::uint32_t>(raw());
            if ((bits & 0x7F800000U) == 0x7F800000U && (bits & 0x007FFFFFU) != 0U) return false;
            if (bits == 0x80000000U) bits = 0U;
            bits = (bits & 0x80000000U) != 0U ? ~bits : (bits | 0x80000000U);
            big_endian(bits, 4U);
            return true;
        }
        case ColumnKind::kFloat64: {
            auto bits = raw();
            constexpr std::uint64_t sign = std::uint64_t{1} << 63U;
            constexpr std::uint64_t exponent = UINT64_C(0x7FF0000000000000);
            if ((bits & exponent) == exponent && (bits & ~(sign | exponent)) != 0U) return false;
            if (bits == sign) bits = 0U;
            bits = (bits & sign) != 0U ? ~bits : (bits | sign);
            big_endian(bits, 8U);
            return true;
        }
        case ColumnKind::kBits:
            key->insert(key->end(), value, value + bytes);
            return true;
        case ColumnKind::kText:
        case ColumnKind::kBytes:
            break;
    }
    throw std::logic_error("a text or bytes value has no fixed-width key");
}

void AppendVarIndexKey(std::span<const std::uint8_t> value, std::uint32_t prefix, std::vector<std::uint8_t>* key) {
    const auto count = prefix != 0U ? std::min<std::size_t>(prefix, value.size()) : value.size();
    for (std::size_t i = 0U; i < count; ++i) {
        key->push_back(value[i]);
        if (value[i] == 0U) key->push_back(0xFFU);
    }
    key->push_back(0U);
    key->push_back(0U);
}

std::vector<IndexEntry> ChunkIndexEntries(const ChunkLayout& layout, std::size_t column_index, std::uint32_t prefix,
    const ChunkCoord& coord, std::span<const std::uint8_t> presence, std::span<const std::uint8_t> payload,
    const ChunkVars& vars) {
    std::vector<IndexEntry> entries;
    if (presence.empty()) return entries;
    const auto& column = layout.schema().columns.at(column_index);
    const auto blocks = layout.block_count();
    if (const auto* fixed = layout.FixedColumnAt(column_index)) {
        std::vector<std::uint8_t> value((fixed->width + 7U) / 8U);
        for (std::size_t block = 0U; block < blocks; ++block) {
            if (!BlockPresent(presence, block)) continue;
            if (fixed->validity != ChunkLayout::kNoValidity &&
                ((payload[fixed->validity + block / 8U] >> (block % 8U)) & 1U) == 0U)
                continue;
            std::fill(value.begin(), value.end(), std::uint8_t{0});
            ReadValueBits(payload.data(), fixed->values * 8U + block * fixed->width, value.data(), fixed->width);
            IndexEntry entry{.coord = coord, .block = static_cast<std::uint32_t>(block)};
            if (!AppendFixedIndexKey(column, value.data(), &entry.key)) continue;
            entries.push_back(std::move(entry));
        }
    } else {
        std::vector<std::pair<std::uint32_t, std::span<const std::uint8_t>>> values;
        for (const auto value : vars)
            if (value.key.column_id == column.id) values.emplace_back(value.key.block_index, value.value);
        std::size_t at = 0U;
        for (std::size_t block = 0U; block < blocks; ++block) {
            if (!BlockPresent(presence, block)) continue;
            while (at < values.size() && values[at].first < block) ++at;
            const bool has = at < values.size() && values[at].first == block;
            if (!has && column.nullable) continue;
            IndexEntry entry{.coord = coord, .block = static_cast<std::uint32_t>(block)};
            AppendVarIndexKey(has ? values[at].second : std::span<const std::uint8_t>{}, prefix, &entry.key);
            entries.push_back(std::move(entry));
        }
    }
    std::sort(entries.begin(), entries.end(), IndexEntryLess);
    return entries;
}

std::vector<std::uint8_t> SerializeIndexManifest(const IndexManifest& manifest) {
    std::vector<std::uint8_t> out(kManifestMagic.begin(), kManifestMagic.end());
    PutU16(out, kFormatVersion);
    PutU16(out, 0U);
    out.insert(out.end(), manifest.identity.epoch.begin(), manifest.identity.epoch.end());
    PutU8(out, manifest.identity.dims);
    PutU8(out, static_cast<std::uint8_t>(manifest.state));
    PutU16(out, 0U);
    PutU32(out, manifest.identity.index_id);
    PutU32(out, manifest.identity.column_id);
    PutU32(out, manifest.identity.prefix);
    PutU64(out, manifest.identity.schema_version);
    PutU64(out, manifest.generation);
    PutU32(out, static_cast<std::uint32_t>(manifest.runs.size()));
    for (const auto& run : manifest.runs) {
        PutU64(out, run.generation);
        PutU64(out, run.bytes);
        out.insert(out.end(), run.sha256.begin(), run.sha256.end());
    }
    PutU32(out, Crc32(out));
    return out;
}

IndexManifest ParseIndexManifest(std::span<const std::uint8_t> bytes) {
    if (bytes.size() < 4U) throw std::runtime_error("index manifest is truncated");
    Reader crc(bytes.subspan(bytes.size() - 4U), "index manifest");
    RequireCrc(bytes.first(bytes.size() - 4U), crc.U32(), "index manifest");
    Reader reader(bytes.first(bytes.size() - 4U), "index manifest");
    const auto magic = reader.Bytes(4U);
    if (!std::equal(magic.begin(), magic.end(), kManifestMagic.begin()) || reader.U16() != kFormatVersion ||
        reader.U16() != 0U)
        throw std::runtime_error("index manifest has an unknown header");
    IndexManifest manifest;
    const auto epoch = reader.Bytes(manifest.identity.epoch.size());
    std::copy(epoch.begin(), epoch.end(), manifest.identity.epoch.begin());
    manifest.identity.dims = reader.U8();
    const auto state = reader.U8();
    if (reader.U16() != 0U) throw std::runtime_error("index manifest has reserved bits set");
    if ((manifest.identity.dims != 2U && manifest.identity.dims != 3U) ||
        (state != static_cast<std::uint8_t>(IndexState::kBuilding) && state != static_cast<std::uint8_t>(IndexState::kReady)))
        throw std::runtime_error("index manifest has an unknown dimension count or state");
    manifest.state = static_cast<IndexState>(state);
    manifest.identity.index_id = reader.U32();
    manifest.identity.column_id = reader.U32();
    manifest.identity.prefix = reader.U32();
    manifest.identity.schema_version = reader.U64();
    manifest.generation = reader.U64();
    const auto runs = reader.U32();
    if (runs > kMaxIndexRuns) throw std::runtime_error("index manifest lists too many runs");
    for (std::uint32_t i = 0U; i < runs; ++i) {
        IndexRunRef run;
        run.generation = reader.U64();
        run.bytes = reader.U64();
        const auto digest = reader.Bytes(run.sha256.size());
        std::copy(digest.begin(), digest.end(), run.sha256.begin());
        if (run.generation == 0U || run.generation > manifest.generation ||
            (!manifest.runs.empty() && run.generation >= manifest.runs.back().generation))
            throw std::runtime_error("index manifest lists runs out of order");
        manifest.runs.push_back(run);
    }
    if (!reader.done()) throw std::runtime_error("index manifest has trailing bytes");
    return manifest;
}

SecondaryIndex::SecondaryIndex(std::filesystem::path directory, const ChunkLayout& layout, std::size_t column_index,
    IndexIdentity identity, bool read_only)
    : directory_(std::move(directory)), layout_(layout), column_index_(column_index), identity_(identity),
      read_only_(read_only) {
    RequireIndexableColumn(layout_.schema().columns.at(column_index_), identity_.prefix);
    if (layout_.schema().columns.at(column_index_).id != identity_.column_id)
        throw std::invalid_argument("index column id does not match its layout");
    manifest_.identity = identity_;
}

std::filesystem::path SecondaryIndex::ManifestPath(const std::filesystem::path& directory) {
    return directory / kManifestName;
}

std::filesystem::path SecondaryIndex::RunPath(const std::filesystem::path& directory, std::uint64_t generation) {
    return directory / (std::string(kRunPrefix) + std::to_string(generation));
}

void SecondaryIndex::RequireUsable() const {
    if (failed_) throw std::runtime_error("the index must be reopened before its layers can be used");
}

void SecondaryIndex::RequireWriter() const {
    if (read_only_) throw std::logic_error("a read-only index writes no file");
    RequireUsable();
}

std::uint64_t SecondaryIndex::NextGeneration() {
    issued_ = std::max(issued_, manifest_.generation);
    if (issued_ == std::numeric_limits<std::uint64_t>::max())
        throw std::overflow_error("index generations exhausted");
    return ++issued_;
}

void SecondaryIndex::Clear() noexcept {
    delta_.clear();
    covers_.clear();
    run_entries_.clear();
    run_inputs_.clear();
}

void SecondaryIndex::LoadOrThrow(bool load_runs) {
    Clear();
    const auto path = ManifestPath(directory_);
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) throw std::runtime_error("the index has no manifest");
    auto manifest = ParseIndexManifest(LoadWhole(path));
    issued_ = std::max(issued_, manifest.generation);
    if (!load_runs) return;
    const auto& identity = manifest.identity;
    if (identity.epoch != identity_.epoch || identity.dims != identity_.dims || identity.index_id != identity_.index_id ||
        identity.column_id != identity_.column_id || identity.prefix != identity_.prefix)
        throw std::runtime_error("the index belongs to another table epoch, column or prefix");
    for (std::size_t layer = 0U; layer < manifest.runs.size(); ++layer) {
        const auto& run = manifest.runs[layer];
        const auto path = RunPath(directory_, run.generation);
        auto pinned = read_only_ ? std::make_unique<SharedFileInput>(path) : nullptr;
        RunReader reader(path, identity_, run.generation, run.bytes, pinned.get());
        // A flipped byte anywhere in a run is found here, not by a later read.
        if (!reader.MatchesDigest(run))
            throw std::runtime_error("index run does not match the digest its manifest lists");
        if (read_only_) run_inputs_.push_back(std::move(pinned));
        // Runs are newest first, so the first layer to cover a chunk wins.
        for (const auto& chunk : reader.chunks()) covers_.try_emplace(chunk.coord, Cover{layer, chunk.revision});
        run_entries_.push_back(reader.entry_count());
    }
    manifest.identity.schema_version = identity_.schema_version;
    manifest_ = std::move(manifest);
}

void SecondaryIndex::RemoveUnlisted() {
    std::set<std::uint64_t> listed;
    for (const auto& run : manifest_.runs) listed.insert(run.generation);
    auto generation_floor = manifest_.generation;
    bool has_unlisted_run = false;
    std::vector<std::filesystem::path> unlisted;
    // Remove stale temporaries first, then publish and sync the generation
    // floor, then remove the orphaned runs that supplied it.
    for (const auto& item : std::filesystem::directory_iterator(directory_)) {
        const auto name = item.path().filename().string();
        std::uint64_t generation = 0U;
        if (name == kManifestName || RemovePublicationTemporary(item.path(), name)) continue;
        if (RunGeneration(name, &generation)) {
            if (listed.contains(generation)) continue;
            generation_floor = std::max(generation_floor, generation);
            has_unlisted_run = true;
        }
        unlisted.push_back(item.path());
    }
    // An interrupted publication can leave a run newer than the manifest.
    // Record its generation durably before deleting the last record of it.
    if (generation_floor > manifest_.generation) {
        auto next = manifest_;
        next.generation = generation_floor;
        PublishManifest(std::move(next));
        failed_ = false;
    } else if (has_unlisted_run) {
        // A prior replacement could be visible after a failed directory sync.
        SyncDirectoryPath(directory_);
    }
    std::error_code ec;
    for (const auto& path : unlisted) std::filesystem::remove(path, ec);
}

SecondaryIndex::OpenReport SecondaryIndex::Open(const IndexChunkSource& source) {
    return OpenLayers(source, false);
}

SecondaryIndex::OpenReport SecondaryIndex::OpenLayers(const IndexChunkSource& source, bool rebuild) {
    OpenReport report;
    FenceOnFailure fence{.failed = failed_};
    failed_ = false;
    // A manifest or run this index cannot trust is replaced from chunk state,
    // which is the only source of truth; the reason is returned.
    try {
        LoadOrThrow(!rebuild);
        if (!rebuild && manifest_.state == IndexState::kBuilding) report.rebuilt = "the index was still building";
    } catch (const std::runtime_error& error) {
        report.rebuilt = error.what();
    }
    if (rebuild || !report.rebuilt.empty()) {
        if (read_only_) RebuildInMemory(source);
        else RebuildFromSource(source);
    } else {
        if (!read_only_) RemoveUnlisted();
        RefreshCovers(source, &report);
    }
    fence.complete = true;
    return report;
}

std::vector<IndexEntry> SecondaryIndex::EntriesOf(const ChunkState& state, const ChunkCoord& coord) const {
    return ChunkIndexEntries(layout_, column_index_, identity_.prefix, coord, state.presence_bitmap, state.payload,
        state.vars);
}

void SecondaryIndex::RefreshCovers(const IndexChunkSource& source, OpenReport* report) {
    std::set<ChunkCoord> seen;
    source.for_each([&](const ChunkCoord& coord, std::uint64_t revision, bool populated) {
        seen.insert(coord);
        std::uint64_t covered = 0U;
        const bool has = Covered(coord, &covered);
        if (populated) {
            if (has && covered == revision) return;
            const auto state = source.read(coord);
            if (state.presence_bitmap.empty()) Forget(coord);
            else Replace(coord, state.version, EntriesOf(state, coord));
            ++report->recomputed;
        } else if (has && covered != 0U) {
            Forget(coord);
            ++report->recomputed;
        }
    });
    std::vector<ChunkCoord> gone;
    for (const auto& [coord, cover] : covers_)
        if (cover.revision != 0U && !seen.contains(coord)) gone.push_back(coord);
    for (const auto& coord : gone) {
        Forget(coord);
        ++report->recomputed;
    }
}

void SecondaryIndex::RebuildInMemory(const IndexChunkSource& source) {
    Clear();
    manifest_ = IndexManifest{.identity = identity_, .state = IndexState::kReady};
    source.for_each([&](const ChunkCoord& coord, std::uint64_t, bool populated) {
        if (!populated) return;
        const auto state = source.read(coord);
        if (!state.presence_bitmap.empty()) Replace(coord, state.version, EntriesOf(state, coord));
    });
}

void SecondaryIndex::Rebuild(const IndexChunkSource& source) {
    RequireWriter();
    (void)OpenLayers(source, true);
}

void SecondaryIndex::RebuildFromSource(const IndexChunkSource& source) {
    std::uint64_t generation = manifest_.generation;
    EnsureDirectoryPathExists(directory_, true);
    for (const auto& item : std::filesystem::directory_iterator(directory_)) {
        const auto name = item.path().filename().string();
        if (RemovePublicationTemporary(item.path(), name)) continue;
        std::uint64_t found = 0U;
        if (RunGeneration(name, &found)) generation = std::max(generation, found);
    }
    Clear();
    issued_ = std::max(issued_, generation);
    manifest_ = IndexManifest{.identity = identity_, .state = IndexState::kBuilding, .generation = issued_};
    source.for_each([&](const ChunkCoord& coord, std::uint64_t, bool populated) {
        if (!populated) return;
        const auto state = source.read(coord);
        if (!state.presence_bitmap.empty()) Replace(coord, state.version, EntriesOf(state, coord));
    });
    Merge();
    Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_REBUILD_BEFORE_READY_ONCE");
    SetState(IndexState::kReady);
    RemoveUnlisted();
}

void SecondaryIndex::Replace(const ChunkCoord& coord, std::uint64_t revision, std::vector<IndexEntry> entries) {
    RequireUsable();
    if (revision == 0U) throw std::invalid_argument("an index chunk needs its producing revision");
    delta_[coord] = DeltaChunk{revision, std::move(entries)};
    covers_[coord] = Cover{kDelta, revision};
}

void SecondaryIndex::Forget(const ChunkCoord& coord) {
    RequireUsable();
    delta_[coord] = DeltaChunk{};
    covers_[coord] = Cover{kDelta, 0U};
}

bool SecondaryIndex::Covered(const ChunkCoord& coord, std::uint64_t* revision) const {
    RequireUsable();
    const auto at = covers_.find(coord);
    if (at == covers_.end()) return false;
    *revision = at->second.revision;
    return true;
}

std::uint64_t SecondaryIndex::DeltaEntries() const noexcept {
    std::uint64_t count = 0U;
    for (const auto& [coord, chunk] : delta_) {
        (void)coord;
        count += chunk.entries.size();
    }
    return count;
}

IndexRunRef SecondaryIndex::WriteRun(std::uint64_t generation, const std::vector<IndexRunChunk>& chunks,
    const std::function<void(const std::function<void(const IndexEntry&)>&)>& entries) const {
    const auto dims = identity_.dims;
    IndexRunRef ref{.generation = generation};
    crypto::Sha256 digest;
    AtomicWriteStream(RunPath(directory_, generation), [&](const AtomicFileSink& sink) {
        std::uint64_t offset = 0U;
        const auto emit = [&](const std::vector<std::uint8_t>& bytes) {
            sink(bytes);
            digest.Update(std::span<const std::uint8_t>(bytes));
            offset += bytes.size();
        };
        std::vector<std::uint8_t> header(kRunMagic.begin(), kRunMagic.end());
        PutU16(header, kFormatVersion);
        PutU16(header, 0U);
        header.insert(header.end(), identity_.epoch.begin(), identity_.epoch.end());
        PutU8(header, dims);
        PutU8(header, 0U);
        PutU16(header, 0U);
        PutU32(header, identity_.index_id);
        PutU32(header, identity_.column_id);
        PutU32(header, identity_.prefix);
        PutU64(header, identity_.schema_version);
        PutU64(header, generation);
        PutU64(header, chunks.size());
        PutU32(header, Crc32(header));
        emit(header);

        const auto chunks_at = offset;
        std::vector<std::uint8_t> table;
        for (const auto& chunk : chunks) {
            PutCoord(table, chunk.coord, dims);
            PutU64(table, chunk.revision);
            PutU32(table, chunk.entries);
        }
        PutU32(table, Crc32(table));
        emit(table);

        const auto blocks_at = offset;
        std::vector<std::uint8_t> block_index;
        std::vector<std::uint8_t> body;
        std::uint32_t in_block = 0U;
        std::uint32_t blocks = 0U;
        std::uint64_t count = 0U;
        const auto close_block = [&] {
            if (in_block == 0U) return;
            std::vector<std::uint8_t> block;
            PutU32(block, in_block);
            PutU32(block, static_cast<std::uint32_t>(body.size()));
            block.insert(block.end(), body.begin(), body.end());
            PutU32(block, Crc32(block));
            emit(block);
            body.clear();
            in_block = 0U;
        };
        entries([&](const IndexEntry& entry) {
            if (in_block == kBlockEntries || body.size() >= kBlockBytes) close_block();
            if (in_block == 0U) {
                PutU64(block_index, offset);
                PutU32(block_index, 0U);
                PutEntry(block_index, entry, dims);
                ++blocks;
            }
            PutEntry(body, entry, dims);
            ++in_block;
            ++count;
        });
        close_block();
        const auto block_index_at = offset;
        // The u32 after each block offset is reserved for a block's entry count.
        std::vector<std::uint8_t> index_section;
        PutU32(index_section, blocks);
        index_section.insert(index_section.end(), block_index.begin(), block_index.end());
        PutU32(index_section, Crc32(index_section));
        emit(index_section);

        std::vector<std::uint8_t> footer;
        PutU64(footer, chunks_at);
        PutU64(footer, blocks_at);
        PutU64(footer, block_index_at);
        PutU64(footer, count);
        PutU32(footer, blocks);
        footer.insert(footer.end(), kRunEndMagic.begin(), kRunEndMagic.end());
        PutU32(footer, Crc32(footer));
        emit(footer);
        ref.bytes = offset;
    }, true, true);
    ref.sha256 = digest.Finish();
    Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_RUN_AFTER_RENAME_ONCE");
    return ref;
}

void SecondaryIndex::PublishManifest(IndexManifest next) {
    RequireWriter();
    next.identity = identity_;
    EnsureDirectoryPathExists(directory_, true);
    // A synced temporary manifest replaces the old one atomically. Discard
    // the sole writer's fixed temporary before creating it exclusively.
    const auto path = ManifestPath(directory_);
    const auto temporary = directory_ / kManifestTemporaryName;
    std::filesystem::remove(temporary);
    WriteAtomicTempFile(temporary, SerializeIndexManifest(next), true, true);
    Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_MANIFEST_BEFORE_RENAME_ONCE");
    // From here the manifest on disk may already be the new one while memory
    // still holds the old layers: the index fails closed until reopened.
    failed_ = true;
    if (const auto error = ReplacePathAtomically(temporary, path); error)
        throw std::system_error(error, "failed to publish index manifest " + path.string());
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_INDEX_MANIFEST_AFTER_REPLACE_FAIL_ONCE"))
        throw std::runtime_error("injected failure after replacing the index manifest");
    SyncDirectoryPath(directory_);
    manifest_ = std::move(next);
}

void SecondaryIndex::SetState(IndexState state) {
    RequireWriter();
    auto next = manifest_;
    next.state = state;
    next.generation = NextGeneration();
    PublishManifest(std::move(next));
    failed_ = false;
}

namespace {
// An entry a snapshot may still read: current, or removed after the oldest
// open snapshot.
bool Kept(const IndexEntry& entry, std::uint64_t oldest_snapshot) noexcept {
    return entry.removed == 0U || entry.removed > oldest_snapshot;
}
}  // namespace

void SecondaryIndex::Flush(std::uint64_t oldest_snapshot) {
    RequireWriter();
    if (delta_.empty()) return;
    // One more run would exceed what a manifest lists: merge instead.
    if (manifest_.runs.size() >= kMaxIndexRuns) {
        Merge(oldest_snapshot);
        return;
    }
    const auto generation = NextGeneration();
    std::vector<IndexRunChunk> chunks;
    chunks.reserve(delta_.size());
    std::vector<const IndexEntry*> ordered;
    for (const auto& [coord, chunk] : delta_) {
        std::uint32_t kept = 0U;
        for (const auto& entry : chunk.entries) {
            if (!Kept(entry, oldest_snapshot)) continue;
            ordered.push_back(&entry);
            ++kept;
        }
        chunks.push_back({coord, chunk.revision, kept});
    }
    std::sort(ordered.begin(), ordered.end(), [](const IndexEntry* a, const IndexEntry* b) { return IndexEntryLess(*a, *b); });
    const auto ref = WriteRun(generation, chunks, [&](const std::function<void(const IndexEntry&)>& emit) {
        for (const auto* entry : ordered) emit(*entry);
    });
    auto next = manifest_;
    next.generation = generation;
    next.runs.insert(next.runs.begin(), ref);
    run_entries_.reserve(run_entries_.size() + 1U);
    PublishManifest(std::move(next));
    run_entries_.insert(run_entries_.begin(), ordered.size());
    for (auto& [coord, cover] : covers_) {
        (void)coord;
        cover.layer = cover.layer == kDelta ? 0U : cover.layer + 1U;
    }
    delta_.clear();
    failed_ = false;
    const bool large = run_entries_.size() > 1U && run_entries_.front() * 4U >= run_entries_.back();
    if (large) Merge(oldest_snapshot);
}

void SecondaryIndex::Merge(std::uint64_t oldest_snapshot) {
    RequireWriter();
    if (manifest_.runs.size() <= 1U && delta_.empty()) return;
    const auto previous = manifest_.runs;
    const auto generation = NextGeneration();
    std::map<ChunkCoord, std::uint32_t> counts;
    for (const auto& [coord, cover] : covers_)
        if (cover.revision != 0U) counts.emplace(coord, 0U);
    ForEach([&](const IndexEntry& entry) {
        if (Kept(entry, oldest_snapshot)) ++counts[entry.coord];
        return true;
    });
    std::vector<IndexRunChunk> chunks;
    chunks.reserve(counts.size());
    std::uint64_t total = 0U;
    for (const auto& [coord, count] : counts) {
        chunks.push_back({coord, covers_.at(coord).revision, count});
        total += count;
    }
    const auto ref = WriteRun(generation, chunks, [&](const std::function<void(const IndexEntry&)>& emit) {
        ForEach([&](const IndexEntry& entry) {
            if (Kept(entry, oldest_snapshot)) emit(entry);
            return true;
        });
    });
    auto next = manifest_;
    next.generation = generation;
    next.runs = {ref};
    run_entries_.reserve(1U);
    PublishManifest(std::move(next));
    Crash("CHUNKDB_FAILPOINT_CRASH_INDEX_MERGE_AFTER_MANIFEST_ONCE");
    delta_.clear();
    for (auto at = covers_.begin(); at != covers_.end();) {
        if (at->second.revision == 0U) at = covers_.erase(at);
        else (at++)->second.layer = 0U;
    }
    run_entries_.clear();
    run_entries_.push_back(total);
    failed_ = false;
    std::error_code ec;
    for (const auto& run : previous) std::filesystem::remove(RunPath(directory_, run.generation), ec);
}

void SecondaryIndex::ForEach(const std::function<bool(const IndexEntry&)>& visit) const {
    RequireUsable();
    // One stream per layer: the delta's entries in order, then each run's.
    struct Stream {
        std::size_t layer = 0U;
        std::unique_ptr<RunReader> run{};
        std::vector<const IndexEntry*> delta{};
        std::size_t next = 0U;
        IndexEntry head{};
    };
    std::vector<Stream> streams;
    {
        Stream delta{.layer = kDelta};
        for (const auto& [coord, chunk] : delta_) {
            (void)coord;
            for (const auto& entry : chunk.entries) delta.delta.push_back(&entry);
        }
        std::sort(delta.delta.begin(), delta.delta.end(),
            [](const IndexEntry* a, const IndexEntry* b) { return IndexEntryLess(*a, *b); });
        streams.push_back(std::move(delta));
    }
    for (std::size_t layer = 0U; layer < manifest_.runs.size(); ++layer) {
        const auto& run = manifest_.runs[layer];
        Stream stream{.layer = layer};
        auto* pinned = read_only_ ? run_inputs_[layer].get() : nullptr;
        stream.run = std::make_unique<RunReader>(RunPath(directory_, run.generation), identity_, run.generation, run.bytes,
            pinned);
        streams.push_back(std::move(stream));
    }
    // Advances a stream to its next entry from the layer that covers it.
    const auto advance = [&](Stream& stream) {
        for (;;) {
            if (stream.run) {
                if (!stream.run->Next(&stream.head)) return false;
            } else {
                if (stream.next == stream.delta.size()) return false;
                stream.head = *stream.delta[stream.next++];
            }
            const auto cover = covers_.find(stream.head.coord);
            if (cover != covers_.end() && cover->second.layer == stream.layer) return true;
        }
    };
    const auto later = [&](std::size_t a, std::size_t b) { return IndexEntryLess(streams[b].head, streams[a].head); };
    std::priority_queue<std::size_t, std::vector<std::size_t>, decltype(later)> heads(later);
    for (std::size_t i = 0U; i < streams.size(); ++i)
        if (advance(streams[i])) heads.push(i);
    while (!heads.empty()) {
        const auto i = heads.top();
        heads.pop();
        if (!visit(streams[i].head)) return;
        if (advance(streams[i])) heads.push(i);
    }
}

std::vector<std::string> SecondaryIndex::Verify(const IndexChunkSource& source) const {
    RequireUsable();
    std::vector<std::string> problems;
    for (std::size_t layer = 0U; layer < manifest_.runs.size(); ++layer) {
        const auto& run = manifest_.runs[layer];
        const auto path = RunPath(directory_, run.generation);
        const auto name = path.filename().string();
        try {
            auto* pinned = read_only_ ? run_inputs_[layer].get() : nullptr;
            RunReader reader(path, identity_, run.generation, run.bytes, pinned);
            if (!reader.MatchesDigest(run))
                throw std::runtime_error("does not match the digest its manifest lists");
            reader.CheckBlockIndex();
            std::map<ChunkCoord, std::uint32_t> counts;
            std::set<std::uint64_t> removal_revisions;
            IndexEntry previous;
            bool first = true;
            for (IndexEntry entry; reader.Next(&entry);) {
                if (!first && !IndexEntryLess(previous, entry)) {
                    if (IndexEntryLess(entry, previous)) throw std::runtime_error("holds entries out of order");
                    // A value may have been removed and reinserted at this block.
                    if (removal_revisions.empty()) removal_revisions.insert(previous.removed);
                    if (!removal_revisions.insert(entry.removed).second)
                        throw std::runtime_error("holds duplicate entries");
                } else {
                    removal_revisions.clear();
                }
                ++counts[entry.coord];
                previous = entry;
                first = false;
            }
            for (const auto& chunk : reader.chunks()) {
                const auto found = counts.find(chunk.coord);
                if ((found == counts.end() ? 0U : found->second) != chunk.entries)
                    throw std::runtime_error("lists another entry count for a chunk than it holds");
                if (found != counts.end()) counts.erase(found);
            }
            if (!counts.empty()) throw std::runtime_error("holds entries of a chunk its chunk table does not list");
        } catch (const std::runtime_error& error) {
            problems.push_back("index run " + name + " " + error.what());
        }
    }
    if (!problems.empty()) return problems;
    std::map<ChunkCoord, std::vector<IndexEntry>> actual;
    // Only current entries follow from chunk state; removal-marked ones are
    // kept for snapshots.
    ForEach([&](const IndexEntry& entry) {
        if (entry.removed == 0U) actual[entry.coord].push_back(entry);
        return true;
    });
    const auto same = [](const std::vector<IndexEntry>& a, const std::vector<IndexEntry>& b) {
        return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](const IndexEntry& x, const IndexEntry& y) {
            return x.key == y.key && x.coord == y.coord && x.block == y.block && x.removed == y.removed;
        });
    };
    const auto label = [](const ChunkCoord& coord) {
        return std::to_string(coord.x) + "," + std::to_string(coord.y) + "," + std::to_string(coord.z);
    };
    source.for_each([&](const ChunkCoord& coord, std::uint64_t, bool populated) {
        const auto found = actual.find(coord);
        if (!populated) {
            if (found != actual.end()) problems.push_back("index lists entries of unpopulated chunk " + label(coord));
            return;
        }
        const auto state = source.read(coord);
        std::uint64_t covered = 0U;
        if (!Covered(coord, &covered) || covered != state.version)
            problems.push_back("index does not cover chunk " + label(coord) + " at its revision");
        const auto expected = EntriesOf(state, coord);
        if (!same(found == actual.end() ? std::vector<IndexEntry>{} : found->second, expected))
            problems.push_back("index entries of chunk " + label(coord) + " differ from its state");
        if (found != actual.end()) actual.erase(found);
    });
    for (const auto& [coord, entries] : actual) {
        (void)entries;
        problems.push_back("index lists entries of chunk " + label(coord) + " that the table does not list");
    }
    return problems;
}

}  // namespace chunkdb
