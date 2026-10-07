#pragma once

#include <filesystem>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err);
// Stages one mutation as a WAL frame at the end of `batch`. Construct (with
// the mutation's tag, if any), append every changed span, then Finish() with
// the mutation's revision and commit time; a caller that abandons the frame
// truncates the batch back to its previous size (the ordinary rollback path
// already does).
class WalFrameBuilder {
  public:
    // A non-empty `tag` becomes the frame's TAG entry.
    explicit WalFrameBuilder(std::vector<std::uint8_t>* batch, MutationTag tag = {});

    // Appends one SPAN record writing `size` bytes at `byte_offset` of the
    // chunk state.
    void AppendSpan(std::uint32_t byte_offset, const std::uint8_t* bytes, std::size_t size);
    // Extra-data records (kWalRecordExtra*). A frame takes EXTRA_PUT and
    // EXTRA_DEL in strictly ascending block order, or one EXTRA_REPLACE.
    void AppendExtraPut(std::uint32_t block_index, const ExtraValue& value);
    void AppendExtraPut(
        std::uint32_t block_index,
        std::uint32_t bit_length,
        std::span<const std::uint8_t> bytes);
    void AppendExtraDel(std::uint32_t block_index);
    void AppendExtraReplace(const ChunkExtra& extra);
    // The records of an applied extra-data update: `extra` is the chunk's
    // extra data afterwards, `undo` what ApplyExtraUpdate returned.
    void AppendExtraUpdate(const ChunkExtra& extra, const ExtraUndo& undo);

    // Writes the frame header and trailer; returns the total bytes the frame
    // added to the batch. Requires at least one record.
    [[nodiscard]] std::size_t Finish(std::uint64_t revision, std::uint64_t commit_time_ms);

    [[nodiscard]] std::size_t record_count() const noexcept { return record_count_; }

  private:
    void BeginRecord(std::uint8_t type, std::size_t body_size);
    void RequireExtraOrder(std::uint32_t block_index);

    std::vector<std::uint8_t>* batch_;
    std::size_t header_index_;
    std::size_t records_begin_;
    std::uint16_t tlv_size_ = 0;
    std::size_t record_count_ = 0;
    bool finished_ = false;
    bool has_extra_replace_ = false;
    std::optional<std::uint32_t> last_extra_block_;
};

}  // namespace chunkdb
