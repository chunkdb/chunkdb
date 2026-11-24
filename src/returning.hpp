#pragma once

// RETURNING of block writes (docs/DESIGN-2.0.md E4): the rows a write
// returns, kept as values under the write's locks and encoded row by row as
// the connection takes the reply, after the locks are released.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/reply.hpp"
#include "chunkdb/schema.hpp"

namespace chunkdb {

// The most rows one statement returns; HELLO reports it as
// limits.max_returning_rows.
inline constexpr std::size_t kMaxReturningRows = 65536U;

// The rows would pass max_returning_rows or the response bound. Thrown
// before the write, so the statement changes nothing
// (OUT_OF_RANGE reason=returning_limit).
class ReturningLimitError : public std::out_of_range {
  public:
    ReturningLimitError(std::string_view limit_name, std::size_t limit);
    // `max_returning_rows` or `max_response_bytes`.
    [[nodiscard]] const std::string& limit_name() const noexcept { return limit_name_; }
    [[nodiscard]] std::size_t limit() const noexcept { return limit_; }

  private:
    std::string limit_name_;
    std::size_t limit_;
};

// Rows `[x, y(, z), values...]`, in the order added.
class ReturningRows {
  public:
    // At most `max_rows` rows, in a reply of at most `max_bytes` whose
    // text besides the rows array takes at most `framing_bytes`.
    ReturningRows(std::uint8_t dims, std::size_t max_rows, std::size_t max_bytes, std::size_t framing_bytes);

    // Throws ReturningLimitError when the row would pass a bound, and
    // keeps the rows added before.
    void Add(std::int64_t x, std::int64_t y, std::int64_t z, std::vector<ColumnValue> values);

    [[nodiscard]] std::size_t size() const noexcept { return rows_.size(); }
    // The encoded bytes of the rows array.
    [[nodiscard]] std::size_t encoded_bytes() const noexcept;

    // The reply: `prefix`, the rows array, `suffix`. Whole when
    // `continuation` is null; otherwise it returns an empty string and sets
    // `*continuation`, which encodes each row when the connection reaches it.
    [[nodiscard]] std::string Reply(std::string prefix, std::string suffix,
        std::unique_ptr<ReplyContinuation>* continuation) &&;

    struct Row {
        std::int64_t x = 0;
        std::int64_t y = 0;
        std::int64_t z = 0;
        std::vector<ColumnValue> values;
    };

  private:
    std::uint8_t dims_;
    std::size_t max_rows_;
    std::size_t max_bytes_;
    std::size_t framing_bytes_;
    std::size_t row_bytes_ = 0U;
    std::vector<Row> rows_;
};

}  // namespace chunkdb
