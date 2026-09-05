#pragma once

#include <filesystem>
#include <stdexcept>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

[[nodiscard]] std::runtime_error BuildWalOpenError(
    const std::filesystem::path& path,
    int err);
[[nodiscard]] std::vector<std::uint8_t> BuildWalHeader(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord);
// Stages one mutation as a v4 WAL frame at the end of `batch`. Construct,
// append every changed span, then Finish() with the mutation's revision; a
// caller that abandons the frame truncates the batch back to its previous
// size (the ordinary rollback path already does).
class WalFrameBuilder {
  public:
    explicit WalFrameBuilder(std::vector<std::uint8_t>* batch);

    // Appends `size` bytes to be written at `byte_offset` of the chunk state,
    // split into records of at most 65535 bytes.
    void AppendSpan(std::uint32_t byte_offset, const std::uint8_t* bytes, std::size_t size);

    // Writes the frame header and trailer; returns the total bytes the frame
    // added to the batch. Requires at least one record.
    [[nodiscard]] std::size_t Finish(std::uint64_t revision);

    [[nodiscard]] std::size_t record_count() const noexcept { return record_count_; }

  private:
    std::vector<std::uint8_t>* batch_;
    std::size_t header_index_;
    std::size_t record_count_ = 0;
    bool finished_ = false;
};

}  // namespace chunkdb
