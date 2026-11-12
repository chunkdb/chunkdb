#pragma once

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/reply.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chunkdb {
[[nodiscard]] std::size_t ChunkReplyCaptureBound(const ChunkLayout& layout, std::size_t state_bytes);
[[nodiscard]] std::size_t BlockReplyCaptureBound(const ChunkLayout& layout, std::span<const std::size_t> columns);

// A reply built in order from framing text and encoded bulk bodies it owns.
// Joined, it is the ordinary reply; as a continuation, it writes the same
// bytes in bounded pieces after every storage lock is released.
class ReplyParts {
  public:
    // Text written before its body.
    struct Piece {
        std::string text;
        std::vector<std::uint8_t> body;
    };
    // Framing appended here precedes the next bulk body.
    [[nodiscard]] std::string& text() noexcept { return text_; }
    void AppendBulk(std::vector<std::uint8_t> body);
    [[nodiscard]] std::size_t size() const noexcept { return size_ + text_.size(); }
    [[nodiscard]] std::string Join() &&;
    [[nodiscard]] std::unique_ptr<ReplyContinuation> Continuation() &&;
  private:
    std::vector<Piece> pieces_;
    std::string text_;
    std::size_t size_ = 0U;
};
}  // namespace chunkdb
