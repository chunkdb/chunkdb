#include "reply_stream.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace chunkdb {
namespace {
void AddCapture(std::size_t& total, std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - total)
        throw ReplyAdmissionError("reply capture bound overflows");
    total += bytes;
}
std::size_t CaptureWithBuffers(std::size_t bytes) {
    if (bytes > (std::numeric_limits<std::size_t>::max() - 65536U) / 4U)
        throw ReplyAdmissionError("reply capture bound overflows");
    return bytes * 4U + 65536U;
}
void AddColumnCapture(std::size_t& bytes, const Column& column) {
    AddCapture(bytes, sizeof(Column) + 256U);
    AddCapture(bytes, column.name.size());
    AddCapture(bytes, column.default_value.size());
}
// Writes owned pieces in order; each piece's text precedes its body.
class PartsContinuation final : public ReplyContinuation {
  public:
    using Piece = ReplyParts::Piece;
    PartsContinuation(std::vector<Piece> pieces, std::size_t size) : pieces_(std::move(pieces)), size_(size) {}
    std::string Next(std::size_t max_bytes) override {
        if (max_bytes == 0U) throw std::invalid_argument("reply continuation needs output credit");
        std::string out;
        out.reserve(std::min(max_bytes, size_ - written_));
        while (out.size() < max_bytes && piece_ < pieces_.size()) {
            auto& piece = pieces_[piece_];
            const std::string_view current = offset_ < piece.text.size()
                ? std::string_view(piece.text).substr(offset_)
                : std::string_view(reinterpret_cast<const char*>(piece.body.data()), piece.body.size())
                      .substr(offset_ - piece.text.size());
            const auto take = std::min(max_bytes - out.size(), current.size());
            out.append(current.data(), take);
            offset_ += take;
            if (offset_ == piece.text.size() + piece.body.size()) {
                // A written piece's bytes are released at once.
                std::string{}.swap(piece.text);
                std::vector<std::uint8_t>{}.swap(piece.body);
                ++piece_;
                offset_ = 0U;
            }
        }
        written_ += out.size();
        return out;
    }
    bool done() const noexcept override { return piece_ == pieces_.size(); }
    std::size_t encoded_size() const noexcept override { return size_; }
  private:
    std::vector<Piece> pieces_;
    const std::size_t size_;
    std::size_t piece_ = 0U, offset_ = 0U, written_ = 0U;
};
}
std::size_t ChunkReplyCaptureBound(const ChunkLayout& layout, std::size_t state_bytes) {
    auto bytes = state_bytes;
    AddCapture(bytes, sizeof(ChunkLayout));
    for (const auto& column : layout.schema().columns) AddColumnCapture(bytes, column);
    for (const auto& step : layout.schema().history) {
        AddCapture(bytes, sizeof(SchemaStep));
        for (const auto& change : step.changes) {
            AddCapture(bytes, sizeof(SchemaChange));
            AddCapture(bytes, change.old_name.size());
            AddColumnCapture(bytes, change.column);
            AddColumnCapture(bytes, change.previous);
        }
    }
    return CaptureWithBuffers(bytes);
}
std::size_t BlockReplyCaptureBound(const ChunkLayout& layout, std::span<const std::size_t> columns) {
    std::size_t bytes = 1024U;
    std::size_t variable = 0U;
    for (const auto& column : layout.schema().columns) {
        AddCapture(bytes, sizeof(ColumnValue) + 64U);
        if (IsFixedWidth(column.type.kind)) AddCapture(bytes, std::max<std::size_t>(32U, column.type.size));
        else variable += std::min<std::size_t>(column.type.size, kVarMaxChunkBytesLimit - variable);
    }
    AddCapture(bytes, variable);
    std::size_t encoded = 0U;
    for (const auto index : columns) {
        AddCapture(bytes, 64U);
        const auto& type = layout.schema().columns.at(index).type;
        encoded += std::min<std::size_t>(std::max<std::size_t>(32U, type.size), kMaxChunkRangeResponseBytes - encoded);
    }
    AddCapture(bytes, encoded);
    return CaptureWithBuffers(bytes);
}
void ReplyParts::AppendBulk(std::vector<std::uint8_t> body) {
    text_ += "$" + std::to_string(body.size()) + "\r\n";
    size_ += text_.size() + body.size();
    pieces_.push_back({std::move(text_), std::move(body)});
    text_ = "\r\n";
}
std::string ReplyParts::Join() && {
    std::string reply;
    reply.reserve(size());
    for (const auto& piece : pieces_) {
        reply += piece.text;
        reply.append(reinterpret_cast<const char*>(piece.body.data()), piece.body.size());
    }
    reply += text_;
    return reply;
}
std::unique_ptr<ReplyContinuation> ReplyParts::Continuation() && {
    const auto bytes = size();
    if (!text_.empty()) pieces_.push_back({std::move(text_), {}});
    return std::make_unique<PartsContinuation>(std::move(pieces_), bytes);
}
}  // namespace chunkdb
