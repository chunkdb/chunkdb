#pragma once

#include <cstddef>
#include <string>

namespace chunkdb {
// Owns the immutable result until a transport has consumed every byte.
class ReplyContinuation {
  public:
    virtual ~ReplyContinuation() = default;
    [[nodiscard]] virtual std::string Next(std::size_t max_bytes) = 0;
    [[nodiscard]] virtual bool done() const noexcept = 0;
    [[nodiscard]] virtual std::size_t encoded_size() const noexcept = 0;
};
}  // namespace chunkdb
