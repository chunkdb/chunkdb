#include "commit_request_fingerprint.hpp"

#include <array>
#include <cstddef>

namespace chunkdb {
namespace {

void Length(crypto::Sha256& hash, std::size_t value) noexcept {
    static_assert(sizeof(std::size_t) <= sizeof(std::uint64_t));
    std::array<std::uint8_t, 8> bytes{};
    const auto wide = static_cast<std::uint64_t>(value);
    for (unsigned i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<std::uint8_t>(wide >> (8U * i));
    hash.Update(bytes);
}

}  // namespace

CommitRequestFingerprint::CommitRequestFingerprint() noexcept {
    constexpr char domain[] = "chunkdb.commit.request\0\1";
    hash_.Update(std::string_view{domain, sizeof(domain) - 1U});
}

void CommitRequestFingerprint::AddStatement(std::string_view text,
    std::span<const std::optional<std::string>> parameters) noexcept {
    const std::array<std::uint8_t, 1> kind{1U};
    hash_.Update(kind);
    Length(hash_, text.size());
    hash_.Update(text);
    Length(hash_, parameters.size());
    for (const auto& parameter : parameters) {
        const std::array<std::uint8_t, 1> present{static_cast<std::uint8_t>(parameter.has_value())};
        hash_.Update(present);
        if (parameter) {
            Length(hash_, parameter->size());
            hash_.Update(*parameter);
        }
    }
}

crypto::Sha256Digest CommitRequestFingerprint::Finish(
    std::optional<std::span<const std::uint8_t>> tag) const noexcept {
    auto complete = hash_;
    const std::array<std::uint8_t, 2> kind{2U, static_cast<std::uint8_t>(tag.has_value())};
    complete.Update(kind);
    if (tag) {
        Length(complete, tag->size());
        complete.Update(*tag);
    }
    return complete.Finish();
}

}  // namespace chunkdb
