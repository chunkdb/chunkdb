#include "returning.hpp"

#include <algorithm>
#include <limits>
#include <utility>
#include <variant>

#include "chunkdb/protocol.hpp"

namespace chunkdb {
namespace {

std::size_t HeaderBytes(std::size_t items) {
    return std::to_string(items).size() + 3U;
}

std::size_t BulkBytes(std::size_t size) {
    return size + std::to_string(size).size() + 5U;
}

// The bytes Protocol::AppendValue writes for `value`, without copying text
// or bytes; refuses a value it would refuse.
std::size_t ValueBytes(const ColumnValue& value) {
    if (const auto* text = std::get_if<std::string>(&value)) return BulkBytes(text->size());
    if (const auto* bytes = std::get_if<BytesValue>(&value)) return BulkBytes(bytes->bytes.size());
    if (const auto* bits = std::get_if<BitsValue>(&value)) {
        if (bits->digits.empty() || bits->digits.size() > 65535U ||
            bits->digits.find_first_not_of("01") != std::string::npos)
            throw std::invalid_argument("invalid bits value");
        return BulkBytes((bits->digits.size() + 7U) / 8U);
    }
    std::string scalar;
    Protocol::AppendValue(scalar, value);
    return scalar.size();
}

std::size_t CoordinateBytes(std::int64_t value) {
    std::string scalar;
    Protocol::AppendInteger(scalar, value);
    return scalar.size();
}

void AppendRow(std::string& out, std::uint8_t dims, const ReturningRows::Row& row) {
    Protocol::AppendArrayHeader(out, dims + row.values.size());
    Protocol::AppendInteger(out, row.x);
    Protocol::AppendInteger(out, row.y);
    if (dims == 3U) Protocol::AppendInteger(out, row.z);
    for (const auto& value : row.values) Protocol::AppendValue(out, value);
}

// Writes `head`, then each row, encoded when the connection reaches it and
// released once encoded, then `tail`.
class RowsContinuation final : public ReplyContinuation {
  public:
    RowsContinuation(std::string head, std::vector<ReturningRows::Row> rows, std::string tail, std::uint8_t dims,
        std::size_t size)
        : head_(std::move(head)), rows_(std::move(rows)), tail_(std::move(tail)), dims_(dims), size_(size) {}

    std::string Next(std::size_t max_bytes) override {
        if (max_bytes == 0U) throw std::invalid_argument("reply continuation needs output credit");
        std::string out;
        out.reserve(std::min(max_bytes, size_ - written_));
        while (out.size() < max_bytes && Ready()) {
            const auto take = std::min(max_bytes - out.size(), pending_.size() - offset_);
            out.append(pending_, offset_, take);
            offset_ += take;
        }
        written_ += out.size();
        // done() is exact once the last byte is out.
        (void)Ready();
        return out;
    }
    bool done() const noexcept override { return stage_ == Stage::kDone && offset_ == pending_.size(); }
    std::size_t encoded_size() const noexcept override { return size_; }

  private:
    enum class Stage { kHead, kRows, kDone };
    // Whether `pending_` holds unwritten bytes, loading the next part when
    // it has none: the head, one row, then the tail.
    bool Ready() {
        while (offset_ == pending_.size()) {
            if (stage_ == Stage::kDone) return false;
            std::string{}.swap(pending_);
            offset_ = 0U;
            if (stage_ == Stage::kHead) {
                pending_.swap(head_);
                stage_ = Stage::kRows;
            } else if (next_row_ < rows_.size()) {
                auto& row = rows_[next_row_++];
                AppendRow(pending_, dims_, row);
                std::vector<ColumnValue>{}.swap(row.values);
            } else {
                pending_.swap(tail_);
                stage_ = Stage::kDone;
            }
        }
        return true;
    }

    std::string head_;
    std::vector<ReturningRows::Row> rows_;
    std::string tail_;
    const std::uint8_t dims_;
    const std::size_t size_;
    Stage stage_ = Stage::kHead;
    std::size_t next_row_ = 0U;
    std::string pending_;
    std::size_t offset_ = 0U;
    std::size_t written_ = 0U;
};

}  // namespace

ReturningLimitError::ReturningLimitError(std::string_view limit_name, std::size_t limit)
    : std::out_of_range("RETURNING exceeds " + std::string(limit_name) + " (" + std::to_string(limit) +
                        "); nothing changed; return fewer columns or change fewer blocks"),
      limit_name_(limit_name),
      limit_(limit) {}

ReturningRows::ReturningRows(std::uint8_t dims, std::size_t max_rows, std::size_t max_bytes, std::size_t framing_bytes)
    : dims_(dims), max_rows_(max_rows), max_bytes_(max_bytes), framing_bytes_(framing_bytes) {}

void ReturningRows::Add(std::int64_t x, std::int64_t y, std::int64_t z, std::vector<ColumnValue> values) {
    if (rows_.size() >= max_rows_) throw ReturningLimitError("max_returning_rows", max_rows_);
    std::size_t bytes = HeaderBytes(dims_ + values.size()) + CoordinateBytes(x) + CoordinateBytes(y) +
                        (dims_ == 3U ? CoordinateBytes(z) : 0U);
    for (const auto& value : values) {
        const auto value_bytes = ValueBytes(value);
        if (value_bytes > max_bytes_ - std::min(bytes, max_bytes_)) throw ReturningLimitError("max_response_bytes", max_bytes_);
        bytes += value_bytes;
    }
    // The whole reply, with the rows array header for one more row.
    const std::size_t fixed = framing_bytes_ + HeaderBytes(rows_.size() + 1U);
    if (fixed > max_bytes_ || row_bytes_ > max_bytes_ - fixed || bytes > max_bytes_ - fixed - row_bytes_)
        throw ReturningLimitError("max_response_bytes", max_bytes_);
    rows_.push_back(Row{.x = x, .y = y, .z = z, .values = std::move(values)});
    row_bytes_ += bytes;
}

std::size_t ReturningRows::encoded_bytes() const noexcept {
    return HeaderBytes(rows_.size()) + row_bytes_;
}

std::string ReturningRows::Reply(std::string prefix, std::string suffix,
    std::unique_ptr<ReplyContinuation>* continuation) && {
    const std::size_t size = prefix.size() + encoded_bytes() + suffix.size();
    Protocol::AppendArrayHeader(prefix, rows_.size());
    if (continuation == nullptr) {
        std::string reply = std::move(prefix);
        reply.reserve(size);
        for (const auto& row : rows_) AppendRow(reply, dims_, row);
        reply += suffix;
        return reply;
    }
    *continuation = std::make_unique<RowsContinuation>(std::move(prefix), std::move(rows_), std::move(suffix), dims_, size);
    return std::string{};
}

}  // namespace chunkdb
