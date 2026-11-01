#include "wal_replay_source.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

#include "chunkdb/crc32.hpp"
#include "feature_flags.hpp"

namespace chunkdb {
namespace {

class StreamSource final : public wal_detail::ReplaySource {
  public:
    StreamSource(std::istream& input, std::uint64_t measured_bytes, std::uint8_t dims,
        const std::function<void()>& read_checkpoint) : input_(input), read_checkpoint_(read_checkpoint) {
        if (measured_bytes > std::numeric_limits<std::size_t>::max() ||
            measured_bytes > static_cast<std::uint64_t>(std::numeric_limits<std::streamoff>::max()))
            throw std::length_error("WAL extent exceeds stream position limits");
        size_ = static_cast<std::size_t>(measured_bytes);
        header_.resize(std::min(size_, ChunkWalHeaderBytes(dims)));
        ReadAt(0U, header_.data(), header_.size());
    }

    std::size_t Size() const override { return size_; }
    const std::vector<std::uint8_t>& Header() const override { return header_; }

    wal_detail::FrameWindow Frame(std::size_t position) override {
        const auto remaining = size_ - position;
        frame_.resize(std::min(remaining, kWalFrameFixedHeaderSize));
        ReadAt(position, frame_.data(), frame_.size());
        if (frame_.size() < kWalFrameFixedHeaderSize ||
            std::memcmp(frame_.data(), kWalFrameMagic, kWalFrameMagicSize) != 0) return {frame_, 0U};
        const auto header_size = kWalFrameFixedHeaderSize + ReadLe16(frame_, 22U) + kWalFrameHeaderCrcSize;
        frame_.resize(std::min(remaining, header_size));
        ReadAt(position + kWalFrameFixedHeaderSize, frame_.data() + kWalFrameFixedHeaderSize,
            frame_.size() - kWalFrameFixedHeaderSize);
        if (frame_.size() < header_size ||
            Crc32(frame_.data() + kWalFrameMagicSize, header_size - kWalFrameMagicSize - kWalFrameHeaderCrcSize) !=
                ReadLe32(frame_, header_size - kWalFrameHeaderCrcSize)) return {frame_, 0U};
        const auto total = static_cast<std::uint64_t>(header_size) + ReadLe32(frame_, 28U) + kWalFrameTrailerSize;
        // The shared parser reports an oversized CRC-valid frame before it
        // accesses its body. Do not allocate an unadmitted declared extent.
        if (total > kMaxWalFrameBytes) return {frame_, 0U};
        frame_.resize(static_cast<std::size_t>(std::min<std::uint64_t>(remaining, total)));
        ReadAt(position + header_size, frame_.data() + header_size, frame_.size() - header_size);
        return {frame_, 0U};
    }

    bool AllZeroAfterHeader() override {
        std::vector<std::uint8_t> block(kScanBytes);
        for (auto position = header_.size(); position < size_;) {
            const auto count = std::min(block.size(), size_ - position);
            ReadAt(position, block.data(), count);
            if (std::any_of(block.begin(), block.begin() + static_cast<std::ptrdiff_t>(count),
                    [](auto byte) { return byte != 0U; })) return false;
            position += count;
        }
        return true;
    }

    bool HasLaterFrameHeader(std::size_t cursor) override {
        std::vector<std::uint8_t> block(kScanBytes + kWalFrameMagicSize - 1U);
        std::vector<std::uint8_t> candidate;
        for (auto position = cursor + 1U; position < size_;) {
            const auto count = std::min(block.size(), size_ - position);
            ReadAt(position, block.data(), count);
            const auto step = std::min(kScanBytes, size_ - position);
            for (std::size_t offset = 0U; offset < step && offset + kWalFrameMagicSize <= count; ++offset) {
                if (std::memcmp(block.data() + offset, kWalFrameMagic, kWalFrameMagicSize) != 0) continue;
                const auto at = position + offset;
                if (size_ - at < kWalFrameFixedHeaderSize) continue;
                candidate.resize(kWalFrameFixedHeaderSize);
                ReadAt(at, candidate.data(), candidate.size());
                const auto header_size = kWalFrameFixedHeaderSize + ReadLe16(candidate, 22U) + kWalFrameHeaderCrcSize;
                if (header_size > size_ - at) continue;
                candidate.resize(header_size);
                ReadAt(at + kWalFrameFixedHeaderSize, candidate.data() + kWalFrameFixedHeaderSize,
                    header_size - kWalFrameFixedHeaderSize);
                if (Crc32(candidate.data() + kWalFrameMagicSize, header_size - kWalFrameMagicSize - kWalFrameHeaderCrcSize) ==
                    ReadLe32(candidate, header_size - kWalFrameHeaderCrcSize)) return true;
            }
            position += step;
        }
        return false;
    }

  private:
    void ReadAt(std::size_t position, std::uint8_t* output, std::size_t count) {
        if (read_checkpoint_) read_checkpoint_();
        if (input_.bad()) throw std::runtime_error("failed to read WAL stream");
        if (!position_ || *position_ != position) {
            input_.clear();
            input_.seekg(static_cast<std::streamoff>(position), std::ios::beg);
            if (!input_) throw std::runtime_error("failed to seek WAL stream");
            position_ = position;
        }
        if (count == 0U) return;
        for (std::size_t used = 0U; used < count;) {
            if (used != 0U && read_checkpoint_) read_checkpoint_();
            const auto part = std::min(count - used, kScanBytes);
            input_.read(reinterpret_cast<char*>(output + used), static_cast<std::streamsize>(part));
            if (input_.gcount() != static_cast<std::streamsize>(part) || input_.bad())
                throw std::runtime_error("failed to read measured WAL extent");
            used += part;
            position_ = position + used;
        }
    }

    static constexpr std::size_t kScanBytes = 64U * 1024U;
    std::istream& input_;
    const std::function<void()>& read_checkpoint_;
    std::size_t size_ = 0U;
    std::optional<std::size_t> position_{};
    std::vector<std::uint8_t> header_, frame_;
};
}  // namespace

WalReplayResult ReplayWalStream(std::istream& input, std::uint64_t measured_bytes,
    const Geometry& geometry, const ChunkCoord& coord, const StoreId& table_id, FeatureFlags features,
    std::uint64_t base_revision, std::uint64_t base_schema_version,
    std::vector<std::uint8_t>* payload, std::vector<std::uint8_t>* presence, ChunkVars* vars,
    std::vector<WalFrameBoundary>* boundaries, std::shared_ptr<const ChunkColumnState>* chunk_columns,
    const std::function<void()>& read_checkpoint) {
    return ReplayWalStream(input, measured_bytes, geometry, coord, table_id, features,
        base_revision, base_schema_version, payload, presence, vars, boundaries, chunk_columns, read_checkpoint, {});
}

WalReplayResult ReplayWalStream(std::istream& input, std::uint64_t measured_bytes,
    const Geometry& geometry, const ChunkCoord& coord, const StoreId& table_id, FeatureFlags features,
    std::uint64_t base_revision, std::uint64_t base_schema_version,
    std::vector<std::uint8_t>* payload, std::vector<std::uint8_t>* presence, ChunkVars* vars,
    std::vector<WalFrameBoundary>* boundaries, std::shared_ptr<const ChunkColumnState>* chunk_columns,
    const std::function<void()>& read_checkpoint, std::optional<std::uint64_t> base_commit_time_ms,
    std::optional<WalKnownIdentity> retained_identity) {
    StreamSource source(input, measured_bytes, geometry.dims(), read_checkpoint);
    return wal_detail::ReplayFromSource(source, geometry, coord, table_id, features,
        base_revision, base_schema_version, payload, presence, vars, boundaries, chunk_columns, base_commit_time_ms, retained_identity);
}

std::optional<FeedFrameInfo> InspectFirstWalFrameStream(std::istream& input,
    std::uint64_t measured_bytes, const Geometry& geometry, const ChunkCoord& coord,
    const StoreId& table_id, FeatureFlags features) {
    const auto header_bytes = ChunkWalHeaderBytes(geometry.dims());
    if (measured_bytes < header_bytes + kWalFrameFixedHeaderSize) return std::nullopt;
    const std::function<void()> read_checkpoint;
    StreamSource source(input, measured_bytes, geometry.dims(), read_checkpoint);
    ValidateWalHeader(source.Header(), coord, table_id, features, geometry.dims());
    const auto crash_tail = [&]() -> std::optional<FeedFrameInfo> {
        if (source.HasLaterFrameHeader(header_bytes))
            throw std::runtime_error("invalid first WAL frame precedes a later frame header");
        return std::nullopt;
    };
    const auto window = source.Frame(header_bytes);
    const auto& frame = window.bytes;
    if (frame.size() < kWalFrameFixedHeaderSize ||
        std::memcmp(frame.data(), kWalFrameMagic, kWalFrameMagicSize) != 0) return crash_tail();
    const auto header_size = kWalFrameFixedHeaderSize + static_cast<std::uint64_t>(ReadLe16(frame, 22U)) + kWalFrameHeaderCrcSize;
    const auto size = header_size + ReadLe32(frame, 28U) + kWalFrameTrailerSize;
    if (frame.size() < header_size) return std::nullopt;
    if (Crc32(frame.data() + kWalFrameMagicSize, static_cast<std::size_t>(header_size) - kWalFrameMagicSize - kWalFrameHeaderCrcSize) !=
        ReadLe32(frame, static_cast<std::size_t>(header_size) - kWalFrameHeaderCrcSize)) return crash_tail();
    if (size > kMaxWalFrameBytes) throw std::runtime_error("first WAL frame exceeds the frame byte limit");
    if (size > measured_bytes - header_bytes) return std::nullopt;
    if (frame.size() != size) throw std::runtime_error("first WAL frame extent is inconsistent");
    if (Crc32(frame.data() + static_cast<std::size_t>(header_size), ReadLe32(frame, 28U)) !=
        ReadLe32(frame, static_cast<std::size_t>(size) - kWalFrameTrailerSize)) {
        if (size < measured_bytes - header_bytes) return crash_tail();
        return std::nullopt;
    }
    return InspectFeedFrame(frame, geometry, features);
}

WalStreamInfo InspectWalStream(std::istream& input, std::uint64_t measured_bytes,
    const Geometry& geometry, const ChunkCoord& coord, const StoreId& table_id, FeatureFlags features,
    const std::function<void()>& read_checkpoint) {
    StreamSource source(input, measured_bytes, geometry.dims(), read_checkpoint);
    const auto& header = source.Header();
    ValidateWalHeader(header, coord, table_id, features, geometry.dims());
    features = UnionFeatures(features,
        {.incompat = ReadLe32(header, 16U), .ro_compat = ReadLe32(header, 20U), .compat = ReadLe32(header, 24U)});
    WalStreamInfo result;
    for (auto position = ChunkWalHeaderBytes(geometry.dims()); position < source.Size();) {
        const auto window = source.Frame(position);
        const auto info = InspectFeedFrame(window.bytes, geometry, features);
        if (info.revision <= result.last) throw std::runtime_error("archive WAL revisions do not increase");
        if (result.first == 0U) { result.first = info.revision; result.first_schema = info.schema_version; }
        result.last = info.revision;
        position += window.bytes.size();
    }
    return result;
}

}  // namespace chunkdb
