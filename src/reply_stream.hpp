#pragma once

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/reply.hpp"

#include <memory>
#include <vector>

namespace chunkdb {
[[nodiscard]] std::size_t ChunkReplyCaptureBound(const ChunkLayout& layout, std::size_t state_bytes);
[[nodiscard]] std::size_t BlockReplyCaptureBound(const ChunkLayout& layout, std::span<const std::size_t> columns);
[[nodiscard]] std::unique_ptr<ReplyContinuation> ChunkReply(
    ChunkLayout layout, std::vector<ChunkRangeEntry> entries, std::vector<std::size_t> columns,
    bool area, bool with_schema, std::size_t max_bytes);
}  // namespace chunkdb
