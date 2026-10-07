#include "history_format.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>

#include "chunkdb/crc32.hpp"
#include "chunkdb/zrle.hpp"

namespace chunkdb::history {

namespace {

constexpr std::uint8_t kRecordMagic[4] = {'H', 'R', 'E', 'C'};
constexpr std::uint8_t kSegmentMagic[8] = {'C', 'H', 'K', 'H', 'S', 'E', 'G', '1'};
constexpr std::uint16_t kSegmentVersion = 1;
constexpr std::uint16_t kSegmentFlagKeyframe = 1;
constexpr std::size_t kSegmentHeaderSize = 8U + 2U + 2U + 16U + 8U * 5U + 4U + 4U + 4U;

[[nodiscard]] bool GetBit(const std::uint8_t* data, std::size_t bit) noexcept {
    return ((data[bit / 8U] >> (bit % 8U)) & 1U) != 0U;
}

void SetBit(std::uint8_t* data, std::size_t bit) noexcept {
    data[bit / 8U] = static_cast<std::uint8_t>(data[bit / 8U] | (1U << (bit % 8U)));
}

// Copies `count` bits from src at `src_bit` to dst at `dst_bit`; dst bits
// must be zero beforehand.
void CopyBits(
    const std::uint8_t* src,
    std::size_t src_bit,
    std::size_t count,
    std::uint8_t* dst,
    std::size_t dst_bit) noexcept {
    if (src_bit % 8U == 0U && dst_bit % 8U == 0U) {
        std::memcpy(dst + dst_bit / 8U, src + src_bit / 8U, count / 8U);
        src_bit += count / 8U * 8U;
        dst_bit += count / 8U * 8U;
        count %= 8U;
    }
    for (std::size_t i = 0; i < count; ++i) {
        if (GetBit(src, src_bit + i)) {
            SetBit(dst, dst_bit + i);
        }
    }
}

[[nodiscard]] bool SameBits(
    const std::uint8_t* lhs,
    const std::uint8_t* rhs,
    std::size_t bit,
    std::size_t count) noexcept {
    for (std::size_t i = 0; i < count; ++i) {
        if (GetBit(lhs, bit + i) != GetBit(rhs, bit + i)) {
            return false;
        }
    }
    return true;
}

void PutVarint(std::vector<std::uint8_t>* out, std::uint64_t value) {
    while (value >= 0x80U) {
        out->push_back(static_cast<std::uint8_t>(value | 0x80U));
        value >>= 7U;
    }
    out->push_back(static_cast<std::uint8_t>(value));
}

[[nodiscard]] std::size_t VarintSize(std::uint64_t value) noexcept {
    std::size_t size = 1;
    while (value >= 0x80U) {
        value >>= 7U;
        ++size;
    }
    return size;
}

class Reader {
  public:
    Reader(const std::uint8_t* data, std::size_t size) : data_(data), size_(size) {}

    [[nodiscard]] bool Varint(std::uint64_t* value) noexcept {
        std::uint64_t result = 0;
        for (unsigned shift = 0; shift < 64U; shift += 7U) {
            if (at_ == size_) {
                return false;
            }
            const std::uint8_t byte = data_[at_++];
            if (shift == 63U && (byte & 0xFEU) != 0U) {
                return false;
            }
            result |= static_cast<std::uint64_t>(byte & 0x7FU) << shift;
            if ((byte & 0x80U) == 0U) {
                // The shortest encoding only, so a value has one form.
                if (byte == 0U && shift != 0U) {
                    return false;
                }
                *value = result;
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] const std::uint8_t* Take(std::size_t count) noexcept {
        if (size_ - at_ < count) {
            return nullptr;
        }
        const std::uint8_t* out = data_ + at_;
        at_ += count;
        return out;
    }

    [[nodiscard]] bool done() const noexcept { return at_ == size_; }

  private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t at_ = 0;
};

[[nodiscard]] std::uint64_t ReadU64(const std::uint8_t* data) noexcept {
    std::uint64_t value = 0;
    for (int i = 7; i >= 0; --i) {
        value = (value << 8U) | data[i];
    }
    return value;
}

[[nodiscard]] std::uint32_t ReadU32(const std::uint8_t* data) noexcept {
    return static_cast<std::uint32_t>(data[0]) | (static_cast<std::uint32_t>(data[1]) << 8U) |
           (static_cast<std::uint32_t>(data[2]) << 16U) | (static_cast<std::uint32_t>(data[3]) << 24U);
}

[[nodiscard]] std::size_t ValueBytes(const Geometry& geometry) noexcept {
    return (static_cast<std::size_t>(geometry.config().block_bits) + 7U) / 8U;
}

// Throws std::invalid_argument unless `change` is a valid change for this
// geometry.
void RequireValidChange(const Geometry& geometry, const BlockChange& change) {
    if (change.block_index >= geometry.ChunkBlockCount()) {
        throw std::invalid_argument("history change for a block outside the chunk");
    }
    if (!change.present) {
        if (!change.bits.empty()) {
            throw std::invalid_argument("history change of an absent block carries bits");
        }
        return;
    }
    const std::size_t block_bits = geometry.config().block_bits;
    if (change.bits.size() != ValueBytes(geometry)) {
        throw std::invalid_argument("history change has the wrong number of value bytes");
    }
    if (block_bits % 8U != 0U && (change.bits.back() >> (block_bits % 8U)) != 0U) {
        throw std::invalid_argument("history change has bits past block_bits");
    }
}

}  // namespace

std::size_t BlockMaskBit(const Geometry& geometry, std::uint32_t block_index) noexcept {
    return static_cast<std::size_t>(
        static_cast<std::uint64_t>(block_index) * kBlockMaskBits / geometry.ChunkBlockCount());
}

std::vector<BlockChange> DiffBlocks(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& payload_before,
    const std::vector<std::uint8_t>& presence_before,
    const std::vector<std::uint8_t>& payload_after,
    const std::vector<std::uint8_t>& presence_after,
    const std::vector<std::uint32_t>* candidates) {
    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::size_t block_bits = geometry.config().block_bits;
    if (payload_before.size() != geometry.ChunkPayloadBytes() ||
        payload_after.size() != geometry.ChunkPayloadBytes() ||
        presence_before.size() != ChunkPresenceBitmapBytes(geometry) ||
        presence_after.size() != ChunkPresenceBitmapBytes(geometry)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    std::vector<BlockChange> changes;
    const auto consider = [&](std::uint32_t block) {
        const bool was = GetBit(presence_before.data(), block);
        const bool is = GetBit(presence_after.data(), block);
        if (!was && !is) {
            return;
        }
        const std::size_t bit = static_cast<std::size_t>(block) * block_bits;
        if (was && is && SameBits(payload_before.data(), payload_after.data(), bit, block_bits)) {
            return;
        }
        BlockChange change{.block_index = block, .present = is};
        if (is) {
            change.bits.assign(ValueBytes(geometry), 0U);
            CopyBits(payload_after.data(), bit, block_bits, change.bits.data(), 0);
        }
        changes.push_back(std::move(change));
    };
    if (candidates == nullptr) {
        for (std::size_t block = 0; block < block_count; ++block) {
            consider(static_cast<std::uint32_t>(block));
        }
    } else {
        for (const auto block : *candidates) {
            if (block >= block_count) {
                throw std::invalid_argument("candidate block outside the chunk");
            }
            consider(block);
        }
    }
    return changes;
}

std::vector<std::uint32_t> BlocksTouchedBySpan(
    const Geometry& geometry,
    std::size_t offset,
    std::size_t size) {
    std::vector<std::uint32_t> blocks;
    if (size == 0U) {
        return blocks;
    }
    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::size_t block_bits = geometry.config().block_bits;
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    const std::size_t end = offset + size;
    std::size_t first = block_count;
    std::size_t last = 0;
    if (offset < payload_bytes) {
        const std::size_t payload_end = std::min(end, payload_bytes);
        first = std::min(first, offset * 8U / block_bits);
        last = std::max(last, std::min(block_count - 1U, (payload_end * 8U - 1U) / block_bits));
    }
    if (end > payload_bytes) {
        const std::size_t presence_begin = std::max(offset, payload_bytes) - payload_bytes;
        const std::size_t presence_end = end - payload_bytes;
        if (presence_begin * 8U < block_count) {
            first = std::min(first, presence_begin * 8U);
            last = std::max(last, std::min(block_count - 1U, presence_end * 8U - 1U));
        }
    }
    for (std::size_t block = first; block <= last && block < block_count; ++block) {
        blocks.push_back(static_cast<std::uint32_t>(block));
    }
    return blocks;
}

void EncodeRecord(
    const Geometry& geometry,
    const std::vector<Mutation>& mutations,
    std::vector<std::uint8_t>* out) {
    if (mutations.empty()) {
        throw std::invalid_argument("a history record needs at least one mutation");
    }
    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::size_t block_bits = geometry.config().block_bits;
    BlockMask mask{};
    std::uint64_t events = 0;
    std::vector<std::uint8_t> body;
    for (std::size_t m = 0; m < mutations.size(); ++m) {
        const auto& mutation = mutations[m];
        if (mutation.revision == 0U || (m > 0 && mutation.revision <= mutations[m - 1].revision)) {
            throw std::invalid_argument("history mutations need strictly increasing nonzero revisions");
        }
        if (m > 0 && mutation.time_ms < mutations[m - 1].time_ms) {
            throw std::invalid_argument("history mutation times must not decrease");
        }
        if (mutation.tag.size() > kMaxTagBytes) {
            throw std::invalid_argument("history tag is too long");
        }
        if (mutation.changes.empty()) {
            throw std::invalid_argument("a history mutation needs at least one change");
        }
        std::size_t present = 0;
        std::size_t list_size = 0;
        for (std::size_t c = 0; c < mutation.changes.size(); ++c) {
            const auto& change = mutation.changes[c];
            RequireValidChange(geometry, change);
            if (c > 0 && change.block_index <= mutation.changes[c - 1].block_index) {
                throw std::invalid_argument("history changes must be in strictly ascending block order");
            }
            present += change.present ? 1U : 0U;
            list_size += VarintSize((static_cast<std::uint64_t>(change.block_index) << 1U) | (change.present ? 1U : 0U));
            const std::size_t bit = BlockMaskBit(geometry, change.block_index);
            mask[bit / 8U] = static_cast<std::uint8_t>(mask[bit / 8U] | (1U << (bit % 8U)));
        }
        const std::size_t n = mutation.changes.size();
        const std::size_t bitmap_size = (block_count + 7U) / 8U + (n + 7U) / 8U;
        const bool bitmap_form = bitmap_size < list_size;
        events += n;

        PutVarint(&body, mutation.revision - (m == 0 ? mutation.revision : mutations[m - 1].revision));
        PutVarint(&body, mutation.time_ms - (m == 0 ? mutation.time_ms : mutations[m - 1].time_ms));
        const bool tagged = !mutation.tag.empty();
        PutVarint(
            &body,
            (static_cast<std::uint64_t>(n) << 2U) | (tagged ? 2U : 0U) | (bitmap_form ? 1U : 0U));
        if (tagged) {
            PutVarint(&body, mutation.tag.size());
            body.insert(body.end(), mutation.tag.begin(), mutation.tag.end());
        }
        if (bitmap_form) {
            const std::size_t changed_at = body.size();
            body.resize(body.size() + (block_count + 7U) / 8U + (n + 7U) / 8U, 0U);
            for (std::size_t c = 0; c < n; ++c) {
                SetBit(body.data() + changed_at, mutation.changes[c].block_index);
                if (mutation.changes[c].present) {
                    SetBit(body.data() + changed_at + (block_count + 7U) / 8U, c);
                }
            }
        } else {
            for (const auto& change : mutation.changes) {
                PutVarint(&body, (static_cast<std::uint64_t>(change.block_index) << 1U) | (change.present ? 1U : 0U));
            }
        }
        const std::size_t values_at = body.size();
        body.resize(body.size() + (present * block_bits + 7U) / 8U, 0U);
        std::size_t value_bit = 0;
        for (const auto& change : mutation.changes) {
            if (change.present) {
                CopyBits(change.bits.data(), 0, block_bits, body.data() + values_at, value_bit);
                value_bit += block_bits;
            }
        }
    }
    if (body.size() > kMaxRecordBodyBytes || mutations.size() > 0xFFFFFFFFU || events > 0xFFFFFFFFU) {
        throw std::invalid_argument("history record is too large");
    }
    const std::size_t header_at = out->size();
    out->insert(out->end(), kRecordMagic, kRecordMagic + 4);
    WriteLe64(*out, mutations.front().revision);
    WriteLe64(*out, mutations.back().revision);
    WriteLe64(*out, mutations.front().time_ms);
    WriteLe64(*out, mutations.back().time_ms);
    WriteLe32(*out, static_cast<std::uint32_t>(mutations.size()));
    WriteLe32(*out, static_cast<std::uint32_t>(events));
    out->insert(out->end(), mask.begin(), mask.end());
    WriteLe32(*out, static_cast<std::uint32_t>(body.size()));
    WriteLe32(*out, Crc32(out->data() + header_at + 4U, out->size() - header_at - 4U));
    out->insert(out->end(), body.begin(), body.end());
    WriteLe32(*out, Crc32(body));
}

RecordReadResult ReadRecord(
    const Geometry& geometry,
    const std::uint8_t* data,
    std::size_t size,
    bool decode_body) {
    RecordReadResult result;
    const auto damaged = [&result](const char* problem) {
        result.status = RecordStatus::kDamaged;
        result.problem = problem;
        result.mutations.clear();
        return result;
    };
    const std::size_t magic_bytes = std::min<std::size_t>(size, 4U);
    if (std::memcmp(data, kRecordMagic, magic_bytes) != 0) {
        return damaged("record magic mismatch");
    }
    if (size < kRecordHeaderSize) {
        result.status = RecordStatus::kTruncated;
        return result;
    }
    const std::size_t crc_at = kRecordHeaderSize - 4U;
    if (Crc32(data + 4U, crc_at - 4U) != ReadU32(data + crc_at)) {
        return damaged("record header checksum mismatch");
    }
    auto& summary = result.summary;
    summary.first_revision = ReadU64(data + 4U);
    summary.last_revision = ReadU64(data + 12U);
    summary.first_time_ms = ReadU64(data + 20U);
    summary.last_time_ms = ReadU64(data + 28U);
    summary.mutation_count = ReadU32(data + 36U);
    summary.event_count = ReadU32(data + 40U);
    std::copy_n(data + 44U, summary.block_mask.size(), summary.block_mask.begin());
    const std::uint32_t body_size = ReadU32(data + 44U + summary.block_mask.size());
    if (summary.first_revision == 0U || summary.last_revision < summary.first_revision ||
        summary.last_time_ms < summary.first_time_ms || summary.mutation_count == 0U ||
        summary.event_count < summary.mutation_count || body_size > kMaxRecordBodyBytes) {
        return damaged("record header fields are inconsistent");
    }
    const std::size_t total = kRecordHeaderSize + body_size + 4U;
    if (size < total) {
        result.status = RecordStatus::kTruncated;
        return result;
    }
    const std::uint8_t* body = data + kRecordHeaderSize;
    if (Crc32(body, body_size) != ReadU32(body + body_size)) {
        return damaged("record body checksum mismatch");
    }
    summary.size = total;
    result.status = RecordStatus::kOk;
    if (!decode_body) {
        return result;
    }

    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::size_t block_bits = geometry.config().block_bits;
    Reader reader(body, body_size);
    BlockMask mask{};
    std::uint64_t revision = summary.first_revision;
    std::uint64_t time_ms = summary.first_time_ms;
    std::uint64_t events = 0;
    for (std::uint32_t m = 0; m < summary.mutation_count; ++m) {
        std::uint64_t revision_delta = 0;
        std::uint64_t time_delta = 0;
        std::uint64_t count_form = 0;
        if (!reader.Varint(&revision_delta) || !reader.Varint(&time_delta) || !reader.Varint(&count_form)) {
            return damaged("record body ends inside a mutation header");
        }
        if ((m == 0) != (revision_delta == 0U) || revision_delta > summary.last_revision - revision ||
            time_delta > summary.last_time_ms - time_ms) {
            return damaged("record mutation header is inconsistent");
        }
        revision += revision_delta;
        time_ms += time_delta;
        Mutation mutation{.revision = revision, .time_ms = time_ms};
        if ((count_form & 2U) != 0U) {
            std::uint64_t tag_size = 0;
            if (!reader.Varint(&tag_size) || tag_size == 0U || tag_size > kMaxTagBytes) {
                return damaged("record mutation tag is invalid");
            }
            const std::uint8_t* tag = reader.Take(static_cast<std::size_t>(tag_size));
            if (tag == nullptr) {
                return damaged("record body ends inside a tag");
            }
            mutation.tag.assign(tag, tag + tag_size);
        }
        const std::uint64_t n = count_form >> 2U;
        const bool bitmap_form = (count_form & 1U) != 0U;
        if (n == 0U || n > block_count) {
            return damaged("record mutation has an invalid change count");
        }
        mutation.changes.reserve(static_cast<std::size_t>(n));
        if (bitmap_form) {
            const std::uint8_t* changed = reader.Take((block_count + 7U) / 8U);
            const std::uint8_t* present = reader.Take((static_cast<std::size_t>(n) + 7U) / 8U);
            if (changed == nullptr || present == nullptr) {
                return damaged("record body ends inside a change bitmap");
            }
            std::size_t c = 0;
            for (std::size_t block = 0; block < block_count; ++block) {
                if (!GetBit(changed, block)) {
                    continue;
                }
                if (c == n) {
                    return damaged("record change bitmap disagrees with its count");
                }
                mutation.changes.push_back(BlockChange{
                    .block_index = static_cast<std::uint32_t>(block), .present = GetBit(present, c)});
                ++c;
            }
            const std::size_t padding_bits = ((block_count + 7U) / 8U) * 8U - block_count;
            if (c != n || (padding_bits != 0U && (changed[(block_count - 1U) / 8U] >> (8U - padding_bits)) != 0U)) {
                return damaged("record change bitmap disagrees with its count");
            }
        } else {
            for (std::uint64_t c = 0; c < n; ++c) {
                std::uint64_t entry = 0;
                if (!reader.Varint(&entry)) {
                    return damaged("record body ends inside a change list");
                }
                const std::uint64_t block = entry >> 1U;
                if (block >= block_count ||
                    (!mutation.changes.empty() && block <= mutation.changes.back().block_index)) {
                    return damaged("record change list is out of order or out of range");
                }
                mutation.changes.push_back(BlockChange{
                    .block_index = static_cast<std::uint32_t>(block), .present = (entry & 1U) != 0U});
            }
        }
        std::size_t present_count = 0;
        for (const auto& change : mutation.changes) {
            present_count += change.present ? 1U : 0U;
        }
        const std::size_t values_bits = present_count * block_bits;
        const std::uint8_t* values = reader.Take((values_bits + 7U) / 8U);
        if (values == nullptr) {
            return damaged("record body ends inside values");
        }
        if (values_bits % 8U != 0U && (values[values_bits / 8U] >> (values_bits % 8U)) != 0U) {
            return damaged("record values have set padding bits");
        }
        std::size_t value_bit = 0;
        for (auto& change : mutation.changes) {
            if (change.present) {
                change.bits.assign(ValueBytes(geometry), 0U);
                CopyBits(values, value_bit, block_bits, change.bits.data(), 0);
                value_bit += block_bits;
            }
            const std::size_t bit = BlockMaskBit(geometry, change.block_index);
            mask[bit / 8U] = static_cast<std::uint8_t>(mask[bit / 8U] | (1U << (bit % 8U)));
        }
        events += n;
        result.mutations.push_back(std::move(mutation));
    }
    if (!reader.done()) {
        return damaged("record body has bytes after its last mutation");
    }
    if (revision != summary.last_revision || time_ms != summary.last_time_ms ||
        events != summary.event_count || mask != summary.block_mask) {
        return damaged("record header disagrees with its body");
    }
    return result;
}

std::vector<std::uint8_t> EncodeSegmentHeader(const Geometry& geometry, const SegmentHeader& header) {
    std::vector<std::uint8_t> keyframe;
    std::uint32_t keyframe_crc = 0;
    if (header.keyframe_state.has_value()) {
        if (header.keyframe_state->size() != ChunkStateBytes(geometry)) {
            throw std::invalid_argument("history keyframe size does not match geometry");
        }
        keyframe = ZrleCompress(*header.keyframe_state);
        keyframe_crc = Crc32(*header.keyframe_state);
    }
    std::vector<std::uint8_t> out;
    out.reserve(kSegmentHeaderSize + keyframe.size());
    out.insert(out.end(), kSegmentMagic, kSegmentMagic + 8);
    WriteLe16(out, kSegmentVersion);
    WriteLe16(out, header.keyframe_state.has_value() ? kSegmentFlagKeyframe : 0U);
    out.insert(out.end(), header.store_id.begin(), header.store_id.end());
    WriteLe64(out, static_cast<std::uint64_t>(header.chunk.x));
    WriteLe64(out, static_cast<std::uint64_t>(header.chunk.y));
    WriteLe64(out, header.history_start);
    WriteLe64(out, header.base_revision);
    WriteLe64(out, header.base_time_ms);
    WriteLe32(out, static_cast<std::uint32_t>(keyframe.size()));
    WriteLe32(out, keyframe_crc);
    WriteLe32(out, Crc32(out.data(), out.size()));
    out.insert(out.end(), keyframe.begin(), keyframe.end());
    return out;
}

SegmentHeader ReadSegmentHeader(
    const Geometry& geometry,
    const std::vector<std::uint8_t>& bytes,
    const StoreId& store_id,
    const ChunkCoord& chunk,
    std::size_t* header_size) {
    if (bytes.size() < kSegmentHeaderSize) {
        throw std::runtime_error("history segment too small");
    }
    if (std::memcmp(bytes.data(), kSegmentMagic, 8) != 0) {
        throw std::runtime_error("not a history segment (bad magic)");
    }
    if (ReadU32(bytes.data() + kSegmentHeaderSize - 4U) != Crc32(bytes.data(), kSegmentHeaderSize - 4U)) {
        throw std::runtime_error("history segment header checksum mismatch");
    }
    if (ReadLe16(bytes, 8U) != kSegmentVersion) {
        throw std::runtime_error("unsupported history segment version");
    }
    const std::uint16_t flags = ReadLe16(bytes, 10U);
    if ((flags & ~kSegmentFlagKeyframe) != 0U) {
        throw std::runtime_error("history segment has unknown flags");
    }
    SegmentHeader header;
    std::copy_n(bytes.begin() + 12, header.store_id.size(), header.store_id.begin());
    if (header.store_id != store_id) {
        throw std::runtime_error("history segment belongs to another store");
    }
    header.chunk = ChunkCoord{
        static_cast<std::int64_t>(ReadLe64(bytes, 28U)), static_cast<std::int64_t>(ReadLe64(bytes, 36U))};
    if (!(header.chunk == chunk)) {
        throw std::runtime_error("history segment chunk coordinate mismatch");
    }
    header.history_start = ReadLe64(bytes, 44U);
    header.base_revision = ReadLe64(bytes, 52U);
    header.base_time_ms = ReadLe64(bytes, 60U);
    const std::uint32_t keyframe_size = ReadLe32(bytes, 68U);
    const std::uint32_t keyframe_crc = ReadLe32(bytes, 72U);
    const bool has_keyframe = (flags & kSegmentFlagKeyframe) != 0U;
    if (!has_keyframe && (keyframe_size != 0U || keyframe_crc != 0U)) {
        throw std::runtime_error("history segment without a keyframe declares one");
    }
    if (bytes.size() - kSegmentHeaderSize < keyframe_size) {
        throw std::runtime_error("history segment keyframe extends past the file");
    }
    if (has_keyframe) {
        std::vector<std::uint8_t> state;
        try {
            state = ZrleDecompress(bytes.data() + kSegmentHeaderSize, keyframe_size, ChunkStateBytes(geometry));
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(std::string("history segment keyframe is damaged: ") + e.what());
        }
        if (Crc32(state) != keyframe_crc) {
            throw std::runtime_error("history segment keyframe checksum mismatch");
        }
        header.keyframe_state = std::move(state);
    }
    *header_size = kSegmentHeaderSize + keyframe_size;
    return header;
}

}  // namespace chunkdb::history
