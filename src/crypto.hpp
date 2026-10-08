#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// The hash functions SCRAM-SHA-256 login needs (docs/USERS_DESIGN.md), built
// in so that builds without OpenSSL log in too.
namespace chunkdb::crypto {

using Sha256Digest = std::array<std::uint8_t, 32>;

// SHA-256 (FIPS 180-4), fed in pieces.
class Sha256 {
  public:
    Sha256() noexcept;
    void Update(std::span<const std::uint8_t> data) noexcept;
    void Update(std::string_view data) noexcept;
    [[nodiscard]] Sha256Digest Finish() noexcept;

  private:
    void Block(const std::uint8_t* block) noexcept;

    std::array<std::uint32_t, 8> state_{};
    std::array<std::uint8_t, 64> buffer_{};
    std::size_t buffered_ = 0;
    std::uint64_t length_ = 0;
};

[[nodiscard]] Sha256Digest Sha256Of(std::span<const std::uint8_t> data) noexcept;
// HMAC-SHA-256 (RFC 2104).
[[nodiscard]] Sha256Digest HmacSha256(std::span<const std::uint8_t> key, std::span<const std::uint8_t> data) noexcept;
// PBKDF2 with HMAC-SHA-256 (RFC 8018), one 32-byte block: what SCRAM calls
// Hi(password, salt, iterations).
[[nodiscard]] Sha256Digest Pbkdf2Sha256(
    std::span<const std::uint8_t> password,
    std::span<const std::uint8_t> salt,
    std::uint32_t iterations) noexcept;

// Base64 (RFC 4648, with padding).
[[nodiscard]] std::string Base64Encode(std::span<const std::uint8_t> data);
// std::nullopt for anything that is not canonical padded base64.
[[nodiscard]] std::optional<std::vector<std::uint8_t>> Base64Decode(std::string_view text);

// `size` bytes from the operating system's random source (std::random_device:
// arc4random, getrandom or /dev/urandom, rand_s), for salts, nonces and the
// users file's secret.
[[nodiscard]] std::vector<std::uint8_t> RandomBytes(std::size_t size);

// Whether two byte strings are equal, in time that depends only on their
// length.
[[nodiscard]] bool ConstantTimeEqual(std::span<const std::uint8_t> a, std::span<const std::uint8_t> b) noexcept;

[[nodiscard]] inline std::span<const std::uint8_t> Bytes(std::string_view text) noexcept {
    return {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()};
}

}  // namespace chunkdb::crypto
