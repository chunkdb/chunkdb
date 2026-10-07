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
constexpr std::uint16_t kSegmentFlagCut = 2;
constexpr std::uint16_t kSegmentFlagFirst = 4;

[[nodiscard]] bool GetBit(const std::uint8_t* data, std::size_t bit) noexcept {
    return ((data[bit / 8U] >> (bit % 8U)) & 1U) != 0U;
}

void SetBit(std::uint8_t* data, std::size_t bit) noexcept {
    data[bit / 8U] = static_cast<std::uint8_t>(data[bit / 8U] | (1U << (bit % 8U)));
}

void WriteBit(std::uint8_t* data, std::size_t bit, bool on) noexcept {
    const auto mask = static_cast<std::uint8_t>(1U << (bit % 8U));
    data[bit / 8U] = static_cast<std::uint8_t>(on ? data[bit / 8U] | mask : data[bit / 8U] & ~mask);
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

// Writes `count` bits of src at bit 0 to dst at `dst_bit`, whatever dst held.
void OverwriteBits(const std::uint8_t* src, std::size_t count, std::uint8_t* dst, std::size_t dst_bit) noexcept {
    for (std::size_t i = 0; i < count; ++i) {
        WriteBit(dst, dst_bit + i, GetBit(src, i));
    }
}

[[nodiscard]] bool SameBits(
    const std::uint8_t* lhs,
    std::size_t lhs_bit,
    const std::uint8_t* rhs,
    std::size_t rhs_bit,
    std::size_t count) noexcept {
    for (std::size_t i = 0; i < count; ++i) {
        if (GetBit(lhs, lhs_bit + i) != GetBit(rhs, rhs_bit + i)) {
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

[[nodiscard]] bool ValidExtraValue(const ExtraValue& value) noexcept {
    if (value.bit_length == 0U || value.bit_length > kExtraMaxBlockBitsLimit ||
        value.bytes.size() != ExtraValueBytes(value.bit_length)) {
        return false;
    }
    const unsigned used = value.bit_length % 8U;
    return used == 0U || (value.bytes.back() >> used) == 0U;
}

// Throws std::invalid_argument unless `change` is a valid change for this
// geometry.
void RequireValidChange(const Geometry& geometry, const BlockChange& change) {
    if (change.block_index >= geometry.ChunkBlockCount()) {
        throw std::invalid_argument("history change for a block outside the chunk");
    }
    if (change.extra_change != ExtraChangeKind::kSet && change.extra != ExtraValue{}) {
        throw std::invalid_argument("history change carries an extra value it does not set");
    }
    if (!change.present) {
        if (!change.bits.empty() || change.extra_change != ExtraChangeKind::kUnchanged) {
            throw std::invalid_argument("history change of an absent block carries bits or extra data");
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
    if (change.extra_change == ExtraChangeKind::kSet && !ValidExtraValue(change.extra)) {
        throw std::invalid_argument("history change has an invalid extra value");
    }
    if (static_cast<std::uint8_t>(change.extra_change) > 2U) {
        throw std::invalid_argument("history change has an unknown extra kind");
    }
}

void AddToMask(const Geometry& geometry, BlockMask* mask, std::uint32_t block_index) noexcept {
    const std::size_t bit = BlockMaskBit(geometry, block_index);
    (*mask)[bit / 8U] = static_cast<std::uint8_t>((*mask)[bit / 8U] | (1U << (bit % 8U)));
}

// A record being built: the body so far and what its header summarizes.
struct RecordBuilder {
    std::vector<std::uint8_t> body;
    BlockMask mask{};
    std::uint64_t events = 0;
    const Mutation* first = nullptr;
    const Mutation* last = nullptr;
    std::size_t mutations = 0;
};

// Appends one mutation to `record`, after the ones it holds.
void AppendMutation(const Geometry& geometry, const Mutation& mutation, RecordBuilder* record) {
    const std::size_t block_count = geometry.ChunkBlockCount();
    const std::size_t block_bits = geometry.config().block_bits;
    const Mutation* previous = record->last;
    if (mutation.revision == 0U || (previous != nullptr && mutation.revision <= previous->revision)) {
        throw std::invalid_argument("history mutations need strictly increasing nonzero revisions");
    }
    if (previous != nullptr && mutation.time_ms < previous->time_ms) {
        throw std::invalid_argument("history mutation times must not decrease");
    }
    if (mutation.tag.size() > kMaxTagBytes) {
        throw std::invalid_argument("history tag is too long");
    }
    if (mutation.changes.empty()) {
        throw std::invalid_argument("a history mutation needs at least one change");
    }
    std::size_t present = 0;
    bool extra = false;
    for (std::size_t c = 0; c < mutation.changes.size(); ++c) {
        const auto& change = mutation.changes[c];
        RequireValidChange(geometry, change);
        if (c > 0 && change.block_index <= mutation.changes[c - 1].block_index) {
            throw std::invalid_argument("history changes must be in strictly ascending block order");
        }
        present += change.present ? 1U : 0U;
        extra = extra || change.extra_change != ExtraChangeKind::kUnchanged;
    }
    // The extra kind is in a list entry only when the mutation changes extra
    // data.
    const auto list_entry = [extra](const BlockChange& change) {
        const std::uint64_t present_bit = change.present ? 1U : 0U;
        return extra ? (static_cast<std::uint64_t>(change.block_index) << 3U) |
                           (static_cast<std::uint64_t>(change.extra_change) << 1U) | present_bit
                     : (static_cast<std::uint64_t>(change.block_index) << 1U) | present_bit;
    };
    std::size_t list_size = 0;
    for (const auto& change : mutation.changes) {
        list_size += VarintSize(list_entry(change));
    }
    const std::size_t n = mutation.changes.size();
    const std::size_t bitmap_size = (block_count + 7U) / 8U + (n + 7U) / 8U + (extra ? (2U * n + 7U) / 8U : 0U);
    const bool bitmap_form = bitmap_size < list_size;

    auto& body = record->body;
    PutVarint(&body, previous == nullptr ? 0U : mutation.revision - previous->revision);
    PutVarint(&body, previous == nullptr ? 0U : mutation.time_ms - previous->time_ms);
    const bool tagged = !mutation.tag.empty();
    PutVarint(
        &body, (static_cast<std::uint64_t>(n) << 3U) | (extra ? 4U : 0U) | (tagged ? 2U : 0U) |
                   (bitmap_form ? 1U : 0U));
    if (tagged) {
        PutVarint(&body, mutation.tag.size());
        body.insert(body.end(), mutation.tag.begin(), mutation.tag.end());
    }
    if (bitmap_form) {
        const std::size_t changed_bytes = (block_count + 7U) / 8U;
        const std::size_t present_bytes = (n + 7U) / 8U;
        const std::size_t at = body.size();
        body.resize(at + bitmap_size, 0U);
        for (std::size_t c = 0; c < n; ++c) {
            const auto& change = mutation.changes[c];
            SetBit(body.data() + at, change.block_index);
            if (change.present) {
                SetBit(body.data() + at + changed_bytes, c);
            }
            if (extra) {
                const auto kind = static_cast<std::uint8_t>(change.extra_change);
                std::uint8_t* kinds = body.data() + at + changed_bytes + present_bytes;
                WriteBit(kinds, 2U * c, (kind & 1U) != 0U);
                WriteBit(kinds, 2U * c + 1U, (kind & 2U) != 0U);
            }
        }
    } else {
        for (const auto& change : mutation.changes) {
            PutVarint(&body, list_entry(change));
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
        AddToMask(geometry, &record->mask, change.block_index);
    }
    for (const auto& change : mutation.changes) {
        if (change.extra_change == ExtraChangeKind::kSet) {
            PutVarint(&body, change.extra.bit_length);
            body.insert(body.end(), change.extra.bytes.begin(), change.extra.bytes.end());
        }
    }
    if (body.size() > kMaxRecordBodyBytes) {
        throw std::invalid_argument("history record is too large");
    }
    record->events += n;
    record->mutations += 1;
    if (record->first == nullptr) {
        record->first = &mutation;
    }
    record->last = &mutation;
}

void FinishRecord(const RecordBuilder& record, std::vector<std::uint8_t>* out) {
    if (record.mutations > 0xFFFFFFFFU || record.events > 0xFFFFFFFFU) {
        throw std::invalid_argument("history record is too large");
    }
    const std::size_t header_at = out->size();
    out->insert(out->end(), kRecordMagic, kRecordMagic + 4);
    WriteLe64(*out, record.first->revision);
    WriteLe64(*out, record.last->revision);
    WriteLe64(*out, record.first->time_ms);
    WriteLe64(*out, record.last->time_ms);
    WriteLe32(*out, static_cast<std::uint32_t>(record.mutations));
    WriteLe32(*out, static_cast<std::uint32_t>(record.events));
    out->insert(out->end(), record.mask.begin(), record.mask.end());
    WriteLe32(*out, static_cast<std::uint32_t>(record.body.size()));
    WriteLe32(*out, Crc32(out->data() + header_at + 4U, out->size() - header_at - 4U));
    out->insert(out->end(), record.body.begin(), record.body.end());
    WriteLe32(*out, Crc32(record.body));
}

}  // namespace

ChunkState EmptyChunkState(const Geometry& geometry) {
    return ChunkState{.state = std::vector<std::uint8_t>(ChunkStateBytes(geometry), 0U)};
}

std::size_t BlockMaskBit(const Geometry& geometry, std::uint32_t block_index) noexcept {
    return static_cast<std::size_t>(
        static_cast<std::uint64_t>(block_index) * kBlockMaskBits / geometry.ChunkBlockCount());
}

bool MaskHasBlock(const Geometry& geometry, const BlockMask& mask, std::uint32_t block_index) noexcept {
    const std::size_t bit = BlockMaskBit(geometry, block_index);
    return ((mask[bit / 8U] >> (bit % 8U)) & 1U) != 0U;
}

BlockDiffer::BlockDiffer(const Geometry& geometry)
    : geometry_(geometry), value_bytes_(ValueBytes(geometry)) {}

void BlockDiffer::Capture(
    const std::vector<std::uint8_t>& state,
    const ChunkExtra& extra,
    const std::vector<std::uint32_t>& blocks) {
    const std::size_t block_count = geometry_.ChunkBlockCount();
    const std::size_t block_bits = geometry_.config().block_bits;
    if (state.size() != ChunkStateBytes(geometry_)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    const std::uint8_t* presence = state.data() + geometry_.ChunkPayloadBytes();
    blocks_ = blocks;
    bits_.assign(blocks.size() * value_bytes_, 0U);
    present_.assign(blocks.size(), false);
    extra_.assign(extra.empty() ? 0U : blocks.size(), std::nullopt);
    for (std::size_t i = 0; i < blocks.size(); ++i) {
        const std::uint32_t block = blocks[i];
        if (block >= block_count || (i > 0 && block <= blocks[i - 1])) {
            throw std::invalid_argument("captured blocks must be ascending and inside the chunk");
        }
        present_[i] = GetBit(presence, block);
        if (!present_[i]) {
            continue;
        }
        CopyBits(state.data(), static_cast<std::size_t>(block) * block_bits, block_bits,
                 bits_.data() + i * value_bytes_, 0);
        if (!extra.empty()) {
            if (const auto value = extra.Find(block); value.has_value()) {
                extra_[i] = value->ToValue();
            }
        }
    }
}

std::vector<BlockChange> BlockDiffer::Changes(
    const std::vector<std::uint8_t>& state,
    const ChunkExtra& extra) const {
    const std::size_t block_bits = geometry_.config().block_bits;
    if (state.size() != ChunkStateBytes(geometry_)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    const std::uint8_t* presence = state.data() + geometry_.ChunkPayloadBytes();
    std::vector<BlockChange> changes;
    for (std::size_t i = 0; i < blocks_.size(); ++i) {
        const std::uint32_t block = blocks_[i];
        const bool was = present_[i];
        const bool is = GetBit(presence, block);
        if (!was && !is) {
            continue;
        }
        if (!is) {
            changes.push_back(BlockChange{.block_index = block, .present = false});
            continue;
        }
        const std::size_t bit = static_cast<std::size_t>(block) * block_bits;
        const bool same_bits = was && SameBits(bits_.data() + i * value_bytes_, 0, state.data(), bit, block_bits);
        const std::optional<ExtraValue>* before = was && !extra_.empty() ? &extra_[i] : nullptr;
        const auto after = extra.Find(block);
        ExtraChangeKind kind = ExtraChangeKind::kUnchanged;
        if (after.has_value()) {
            if (before == nullptr || !before->has_value() || !(*after == **before)) {
                kind = ExtraChangeKind::kSet;
            }
        } else if (before != nullptr && before->has_value()) {
            kind = ExtraChangeKind::kRemoved;
        }
        if (same_bits && kind == ExtraChangeKind::kUnchanged) {
            continue;
        }
        BlockChange change{.block_index = block, .present = true, .extra_change = kind};
        change.bits.assign(value_bytes_, 0U);
        CopyBits(state.data(), bit, block_bits, change.bits.data(), 0);
        if (kind == ExtraChangeKind::kSet) {
            change.extra = after->ToValue();
        }
        changes.push_back(std::move(change));
    }
    return changes;
}

std::vector<BlockChange> DiffBlocks(
    const Geometry& geometry,
    const ChunkState& before,
    const ChunkState& after,
    const std::vector<std::uint32_t>* candidates) {
    std::vector<std::uint32_t> all;
    if (candidates == nullptr) {
        all.resize(geometry.ChunkBlockCount());
        for (std::size_t block = 0; block < all.size(); ++block) {
            all[block] = static_cast<std::uint32_t>(block);
        }
        candidates = &all;
    }
    BlockDiffer differ(geometry);
    differ.Capture(before.state, before.extra, *candidates);
    if (after.state.size() != before.state.size()) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    return differ.Changes(after.state, after.extra);
}

void ApplyMutation(const Geometry& geometry, const Mutation& mutation, ChunkState* state, bool reject_unchanged) {
    const std::size_t block_bits = geometry.config().block_bits;
    const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
    if (state->state.size() != ChunkStateBytes(geometry)) {
        throw std::invalid_argument("chunk state size does not match geometry");
    }
    std::uint8_t* payload = state->state.data();
    std::uint8_t* presence = state->state.data() + payload_bytes;
    const std::vector<std::uint8_t> zero(ValueBytes(geometry), 0U);
    for (const auto& change : mutation.changes) {
        if (change.block_index >= geometry.ChunkBlockCount()) {
            throw std::runtime_error("history change for a block outside the chunk");
        }
        const std::size_t bit = static_cast<std::size_t>(change.block_index) * block_bits;
        const bool was = GetBit(presence, change.block_index);
        if (!change.present) {
            if (reject_unchanged && !was) {
                throw std::runtime_error("history change removes a block that is absent");
            }
            OverwriteBits(zero.data(), block_bits, payload, bit);
            WriteBit(presence, change.block_index, false);
            (void)state->extra.Remove(change.block_index);
            continue;
        }
        const bool same_bits = was && SameBits(payload, bit, change.bits.data(), 0, block_bits);
        const auto current = was ? state->extra.Find(change.block_index) : std::nullopt;
        bool extra_same = true;
        switch (change.extra_change) {
            case ExtraChangeKind::kUnchanged:
                break;
            case ExtraChangeKind::kSet:
                extra_same = current.has_value() && *current == change.extra;
                break;
            case ExtraChangeKind::kRemoved:
                if (!current.has_value()) {
                    throw std::runtime_error("history change removes extra data the block does not have");
                }
                extra_same = false;
                break;
        }
        if (reject_unchanged && same_bits && extra_same) {
            throw std::runtime_error("history change leaves its block as it was");
        }
        OverwriteBits(change.bits.data(), block_bits, payload, bit);
        WriteBit(presence, change.block_index, true);
        if (change.extra_change == ExtraChangeKind::kSet) {
            try {
                state->extra.Assign(change.block_index, change.extra);
            } catch (const std::invalid_argument& e) {
                throw std::runtime_error(std::string("history change has an invalid extra value: ") + e.what());
            }
        } else if (change.extra_change == ExtraChangeKind::kRemoved) {
            (void)state->extra.Remove(change.block_index);
        }
    }
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
    std::span<const Mutation> mutations,
    std::vector<std::uint8_t>* out) {
    if (mutations.empty()) {
        throw std::invalid_argument("a history record needs at least one mutation");
    }
    RecordBuilder record;
    for (const auto& mutation : mutations) {
        AppendMutation(geometry, mutation, &record);
    }
    FinishRecord(record, out);
}

void EncodeRecords(
    const Geometry& geometry,
    std::span<const Mutation> mutations,
    std::vector<std::uint8_t>* out) {
    if (mutations.empty()) {
        throw std::invalid_argument("a history record needs at least one mutation");
    }
    const std::size_t out_size = out->size();
    try {
        RecordBuilder record;
        for (const auto& mutation : mutations) {
            if (record.body.size() >= kTargetRecordBodyBytes) {
                FinishRecord(record, out);
                record = RecordBuilder{};
            }
            AppendMutation(geometry, mutation, &record);
        }
        FinishRecord(record, out);
    } catch (...) {
        out->resize(out_size);
        throw;
    }
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
        summary.event_count < summary.mutation_count || body_size > kMaxRecordBodyBytes ||
        (summary.mutation_count == 1U) != (summary.first_revision == summary.last_revision)) {
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
        if ((m == 0) != (revision_delta == 0U) || (m == 0 && time_delta != 0U) ||
            revision_delta > summary.last_revision - revision || time_delta > summary.last_time_ms - time_ms) {
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
        const std::uint64_t n = count_form >> 3U;
        const bool extra = (count_form & 4U) != 0U;
        const bool bitmap_form = (count_form & 1U) != 0U;
        if (n == 0U || n > block_count) {
            return damaged("record mutation has an invalid change count");
        }
        mutation.changes.reserve(static_cast<std::size_t>(n));
        if (bitmap_form) {
            const std::uint8_t* changed = reader.Take((block_count + 7U) / 8U);
            const std::uint8_t* present = reader.Take((static_cast<std::size_t>(n) + 7U) / 8U);
            const std::uint8_t* kinds = extra ? reader.Take((2U * static_cast<std::size_t>(n) + 7U) / 8U) : nullptr;
            if (changed == nullptr || present == nullptr || (extra && kinds == nullptr)) {
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
                const std::uint8_t kind =
                    extra ? static_cast<std::uint8_t>((GetBit(kinds, 2U * c) ? 1U : 0U) | (GetBit(kinds, 2U * c + 1U) ? 2U : 0U))
                          : 0U;
                mutation.changes.push_back(BlockChange{
                    .block_index = static_cast<std::uint32_t>(block),
                    .present = GetBit(present, c),
                    .extra_change = static_cast<ExtraChangeKind>(kind)});
                ++c;
            }
            const auto padding_clear = [](const std::uint8_t* bytes, std::size_t bits) {
                return bits % 8U == 0U || (bytes[bits / 8U] >> (bits % 8U)) == 0U;
            };
            if (c != n || !padding_clear(changed, block_count) || !padding_clear(present, c) ||
                (extra && !padding_clear(kinds, 2U * c))) {
                return damaged("record change bitmap disagrees with its count");
            }
        } else {
            for (std::uint64_t c = 0; c < n; ++c) {
                std::uint64_t entry = 0;
                if (!reader.Varint(&entry)) {
                    return damaged("record body ends inside a change list");
                }
                const std::uint64_t block = entry >> (extra ? 3U : 1U);
                if (block >= block_count ||
                    (!mutation.changes.empty() && block <= mutation.changes.back().block_index)) {
                    return damaged("record change list is out of order or out of range");
                }
                mutation.changes.push_back(BlockChange{
                    .block_index = static_cast<std::uint32_t>(block),
                    .present = (entry & 1U) != 0U,
                    .extra_change = static_cast<ExtraChangeKind>(extra ? (entry >> 1U) & 3U : 0U)});
            }
        }
        std::size_t present_count = 0;
        bool any_extra = false;
        for (const auto& change : mutation.changes) {
            present_count += change.present ? 1U : 0U;
            const auto kind = static_cast<std::uint8_t>(change.extra_change);
            if (kind > 2U || (!change.present && kind != 0U)) {
                return damaged("record change has an invalid extra kind");
            }
            any_extra = any_extra || kind != 0U;
        }
        if (any_extra != extra) {
            return damaged("record mutation extra flag disagrees with its changes");
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
            AddToMask(geometry, &mask, change.block_index);
        }
        for (auto& change : mutation.changes) {
            if (change.extra_change != ExtraChangeKind::kSet) {
                continue;
            }
            std::uint64_t bit_length = 0;
            if (!reader.Varint(&bit_length) || bit_length == 0U || bit_length > kExtraMaxBlockBitsLimit) {
                return damaged("record extra value has an invalid length");
            }
            const auto length = static_cast<std::uint32_t>(bit_length);
            const std::uint8_t* bytes = reader.Take(ExtraValueBytes(length));
            if (bytes == nullptr) {
                return damaged("record body ends inside an extra value");
            }
            change.extra = ExtraValue{.bit_length = length, .bytes = {bytes, bytes + ExtraValueBytes(length)}};
            if (!ValidExtraValue(change.extra)) {
                return damaged("record extra value has set padding bits");
            }
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
    if (header.keyframe.has_value()) {
        if (header.keyframe->state.size() != ChunkStateBytes(geometry)) {
            throw std::invalid_argument("history keyframe size does not match geometry");
        }
        std::vector<std::uint8_t> raw = header.keyframe->state;
        header.keyframe->extra.EncodeTo(&raw);
        keyframe = ZrleCompress(raw);
        keyframe_crc = Crc32(raw);
    } else if (header.cut) {
        throw std::invalid_argument("a history cut needs a keyframe");
    }
    if (header.cut && header.first) {
        throw std::invalid_argument("a chunk's first history segment is not a cut");
    }
    std::vector<std::uint8_t> out;
    out.reserve(kSegmentHeaderSize + keyframe.size());
    out.insert(out.end(), kSegmentMagic, kSegmentMagic + 8);
    WriteLe16(out, kSegmentVersion);
    WriteLe16(
        out, static_cast<std::uint16_t>(
                 (header.keyframe.has_value() ? kSegmentFlagKeyframe : 0U) | (header.cut ? kSegmentFlagCut : 0U) |
                 (header.first ? kSegmentFlagFirst : 0U)));
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
    if ((flags & ~(kSegmentFlagKeyframe | kSegmentFlagCut | kSegmentFlagFirst)) != 0U) {
        throw std::runtime_error("history segment has unknown flags");
    }
    if ((flags & kSegmentFlagCut) != 0U && (flags & kSegmentFlagFirst) != 0U) {
        throw std::runtime_error("history segment is both a cut and a chunk's first");
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
    header.cut = (flags & kSegmentFlagCut) != 0U;
    header.first = (flags & kSegmentFlagFirst) != 0U;
    const std::uint32_t keyframe_size = ReadLe32(bytes, 68U);
    const std::uint32_t keyframe_crc = ReadLe32(bytes, 72U);
    const bool has_keyframe = (flags & kSegmentFlagKeyframe) != 0U;
    if (!has_keyframe && (keyframe_size != 0U || keyframe_crc != 0U || header.cut)) {
        throw std::runtime_error("history segment without a keyframe declares one");
    }
    if (bytes.size() - kSegmentHeaderSize < keyframe_size) {
        throw std::runtime_error("history segment keyframe extends past the file");
    }
    if (has_keyframe) {
        const std::uint8_t* stored = bytes.data() + kSegmentHeaderSize;
        const std::size_t state_size = ChunkStateBytes(geometry);
        ChunkState state;
        try {
            const std::size_t declared = ZrleDeclaredSize(stored, keyframe_size);
            if (declared < state_size || declared - state_size > kExtraMaxChunkBytesLimit) {
                throw std::runtime_error("declared size " + std::to_string(declared) + " does not fit the chunk");
            }
            auto raw = ZrleDecompress(stored, keyframe_size, declared);
            if (Crc32(raw) != keyframe_crc) {
                throw std::runtime_error("checksum mismatch");
            }
            state.extra = ChunkExtra::Decode(
                raw.data() + state_size, raw.size() - state_size, geometry.ChunkBlockCount(), ExtraPadding::kReject);
            raw.resize(state_size);
            state.state = std::move(raw);
        } catch (const std::exception& e) {
            throw std::runtime_error(std::string("history segment keyframe is damaged: ") + e.what());
        }
        for (const auto entry : state.extra) {
            if (!GetBit(state.state.data() + geometry.ChunkPayloadBytes(), entry.block_index)) {
                throw std::runtime_error("history segment keyframe has extra data on an absent block");
            }
        }
        header.keyframe = std::move(state);
    }
    *header_size = kSegmentHeaderSize + keyframe_size;
    return header;
}

}  // namespace chunkdb::history
