#pragma once

#include <cstdint>
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
        std::span<const std::optional<std::string>> parameters) noexcept;
    [[nodiscard]] crypto::Sha256Digest Finish(
        std::optional<std::span<const std::uint8_t>> tag = {}) const noexcept;

  private:
    crypto::Sha256 hash_;
};

}  // namespace chunkdb
