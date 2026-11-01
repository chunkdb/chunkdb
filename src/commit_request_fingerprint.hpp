#pragma once

#include <cstdint>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "crypto.hpp"

namespace chunkdb {

// Incremental statement transcript for one user-scoped request binding.
// Record every transactional statement, including after private validation
// fails, and finish with the complete group's effective tag.
class CommitRequestFingerprint {
  public:
    CommitRequestFingerprint() noexcept;
    void AddStatement(std::string_view text,
        std::span<const std::optional<std::string>> parameters);
    // Payload admission can hash discarded bodies in bounded pieces.
    void BeginStatement(std::string_view text, std::size_t parameter_count);
    void BeginParameter(std::optional<std::size_t> bytes);
    void ParameterBytes(std::span<const std::uint8_t> bytes);
    [[nodiscard]] crypto::Sha256Digest Finish(
        std::optional<std::span<const std::uint8_t>> tag = {}) const;

  private:
    crypto::Sha256 hash_;
    std::size_t parameters_left_ = 0U;
    std::size_t bytes_left_ = 0U;
    bool invalid_ = false;
    void Require(bool valid);
};

}  // namespace chunkdb
