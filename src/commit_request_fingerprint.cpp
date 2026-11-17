#include "commit_request_fingerprint.hpp"

#include <array>
#include <cstddef>
#include <stdexcept>

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
    std::span<const std::optional<std::string>> parameters) {
    BeginStatement(text, parameters.size());
    for (const auto& parameter : parameters) {
        BeginParameter(parameter ? std::optional{parameter->size()} : std::nullopt);
        if (parameter) ParameterBytes(crypto::Bytes(*parameter));
    }
}

void CommitRequestFingerprint::Require(bool valid) {
    if (invalid_ || !valid) {
        invalid_ = true;
        throw std::logic_error("invalid request fingerprint framing");
    }
}

void CommitRequestFingerprint::BeginStatement(std::string_view text, std::size_t parameter_count) {
    Require(parameters_left_ == 0U && bytes_left_ == 0U);
    const std::array<std::uint8_t, 1> kind{1U};
    hash_.Update(kind);
    Length(hash_, text.size());
    hash_.Update(text);
    Length(hash_, parameter_count);
    parameters_left_ = parameter_count;
}

void CommitRequestFingerprint::BeginParameter(std::optional<std::size_t> bytes) {
    Require(parameters_left_ != 0U && bytes_left_ == 0U);
    const std::array<std::uint8_t, 1> present{static_cast<std::uint8_t>(bytes.has_value())};
    hash_.Update(present);
    if (bytes) Length(hash_, *bytes);
    --parameters_left_;
    bytes_left_ = bytes.value_or(0U);
}

void CommitRequestFingerprint::ParameterBytes(std::span<const std::uint8_t> bytes) {
    Require(bytes.size() <= bytes_left_);
    hash_.Update(bytes);
    bytes_left_ -= bytes.size();
}

crypto::Sha256Digest CommitRequestFingerprint::Finish(
    std::optional<std::span<const std::uint8_t>> tag) const {
    if (invalid_ || parameters_left_ != 0U || bytes_left_ != 0U)
        throw std::logic_error("incomplete or invalid request fingerprint");
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
