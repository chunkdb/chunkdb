#include "crypto.hpp"

#include <algorithm>

namespace chunkdb::crypto {

namespace {

constexpr std::array<std::uint32_t, 64> kRoundConstants = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

[[nodiscard]] constexpr std::uint32_t Rotr(std::uint32_t x, unsigned n) noexcept {
    return (x >> n) | (x << (32U - n));
}

constexpr std::string_view kBase64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

}  // namespace

Sha256::Sha256() noexcept
    : state_{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19} {}

void Sha256::Block(const std::uint8_t* block) noexcept {
    std::array<std::uint32_t, 64> w{};
    for (std::size_t i = 0; i < 16; ++i) {
        w[i] = (static_cast<std::uint32_t>(block[4 * i]) << 24U) | (static_cast<std::uint32_t>(block[4 * i + 1]) << 16U) |
               (static_cast<std::uint32_t>(block[4 * i + 2]) << 8U) | static_cast<std::uint32_t>(block[4 * i + 3]);
    }
    for (std::size_t i = 16; i < 64; ++i) {
        const std::uint32_t s0 = Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3U);
        const std::uint32_t s1 = Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10U);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    auto [a, b, c, d, e, f, g, h] = state_;
    for (std::size_t i = 0; i < 64; ++i) {
        const std::uint32_t s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
        const std::uint32_t choose = (e & f) ^ (~e & g);
        const std::uint32_t t1 = h + s1 + choose + kRoundConstants[i] + w[i];
        const std::uint32_t s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
        const std::uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        const std::uint32_t t2 = s0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + t1;
        d = c;
        c = b;
        b = a;
        a = t1 + t2;
    }
    state_[0] += a;
    state_[1] += b;
    state_[2] += c;
    state_[3] += d;
    state_[4] += e;
    state_[5] += f;
    state_[6] += g;
    state_[7] += h;
}

void Sha256::Update(std::span<const std::uint8_t> data) noexcept {
    length_ += data.size();
    std::size_t at = 0;
    while (at < data.size()) {
        const std::size_t take = std::min(data.size() - at, buffer_.size() - buffered_);
        std::copy_n(data.data() + at, take, buffer_.data() + buffered_);
        buffered_ += take;
        at += take;
        if (buffered_ == buffer_.size()) {
            Block(buffer_.data());
            buffered_ = 0;
        }
    }
}

void Sha256::Update(std::string_view data) noexcept {
    Update(Bytes(data));
}

Sha256Digest Sha256::Finish() noexcept {
    const std::uint64_t bits = length_ * 8U;
    const std::uint8_t one = 0x80;
    Update(std::span<const std::uint8_t>(&one, 1));
    const std::uint8_t zero = 0;
    while (buffered_ != 56) {
        Update(std::span<const std::uint8_t>(&zero, 1));
    }
    std::array<std::uint8_t, 8> length{};
    for (std::size_t i = 0; i < 8; ++i) {
        length[i] = static_cast<std::uint8_t>(bits >> (56U - 8U * i));
    }
    Update(length);
    Sha256Digest digest{};
    for (std::size_t i = 0; i < 8; ++i) {
        for (std::size_t j = 0; j < 4; ++j) {
            digest[4 * i + j] = static_cast<std::uint8_t>(state_[i] >> (24U - 8U * j));
        }
    }
    return digest;
}

Sha256Digest Sha256Of(std::span<const std::uint8_t> data) noexcept {
    Sha256 hash;
    hash.Update(data);
    return hash.Finish();
}

Sha256Digest HmacSha256(std::span<const std::uint8_t> key, std::span<const std::uint8_t> data) noexcept {
    std::array<std::uint8_t, 64> block{};
    if (key.size() > block.size()) {
        const auto hashed = Sha256Of(key);
        std::copy(hashed.begin(), hashed.end(), block.begin());
    } else {
        std::copy(key.begin(), key.end(), block.begin());
    }
    std::array<std::uint8_t, 64> pad{};
    for (std::size_t i = 0; i < block.size(); ++i) {
        pad[i] = static_cast<std::uint8_t>(block[i] ^ 0x36U);
    }
    Sha256 inner;
    inner.Update(pad);
    inner.Update(data);
    const auto inner_digest = inner.Finish();
    for (std::size_t i = 0; i < block.size(); ++i) {
        pad[i] = static_cast<std::uint8_t>(block[i] ^ 0x5cU);
    }
    Sha256 outer;
    outer.Update(pad);
    outer.Update(inner_digest);
    return outer.Finish();
}

Sha256Digest Pbkdf2Sha256(
    std::span<const std::uint8_t> password,
    std::span<const std::uint8_t> salt,
    std::uint32_t iterations) noexcept {
    std::vector<std::uint8_t> first(salt.begin(), salt.end());
    first.insert(first.end(), {0, 0, 0, 1});
    Sha256Digest u = HmacSha256(password, first);
    Sha256Digest result = u;
    for (std::uint32_t i = 1; i < iterations; ++i) {
        u = HmacSha256(password, u);
        for (std::size_t j = 0; j < result.size(); ++j) {
            result[j] = static_cast<std::uint8_t>(result[j] ^ u[j]);
        }
    }
    return result;
}

std::string Base64Encode(std::span<const std::uint8_t> data) {
    std::string out;
    out.reserve((data.size() + 2) / 3 * 4);
    for (std::size_t i = 0; i < data.size(); i += 3) {
        const std::size_t left = data.size() - i;
        const std::uint32_t chunk = (static_cast<std::uint32_t>(data[i]) << 16U) |
                                    (left > 1 ? static_cast<std::uint32_t>(data[i + 1]) << 8U : 0U) |
                                    (left > 2 ? static_cast<std::uint32_t>(data[i + 2]) : 0U);
        out += kBase64Alphabet[(chunk >> 18U) & 63U];
        out += kBase64Alphabet[(chunk >> 12U) & 63U];
        out += left > 1 ? kBase64Alphabet[(chunk >> 6U) & 63U] : '=';
        out += left > 2 ? kBase64Alphabet[chunk & 63U] : '=';
    }
    return out;
}

std::optional<std::vector<std::uint8_t>> Base64Decode(std::string_view text) {
    if (text.size() % 4U != 0U) {
        return std::nullopt;
    }
    std::vector<std::uint8_t> out;
    out.reserve(text.size() / 4 * 3);
    for (std::size_t i = 0; i < text.size(); i += 4) {
        std::uint32_t chunk = 0;
        std::size_t padding = 0;
        for (std::size_t j = 0; j < 4; ++j) {
            const char c = text[i + j];
            if (c == '=' && i + 4 == text.size() && j >= 2) {
                ++padding;
                chunk <<= 6U;
                continue;
            }
            const auto at = kBase64Alphabet.find(c);
            if (at == std::string_view::npos || padding != 0U) {
                return std::nullopt;
            }
            chunk = (chunk << 6U) | static_cast<std::uint32_t>(at);
        }
        out.push_back(static_cast<std::uint8_t>(chunk >> 16U));
        if (padding < 2U) {
            out.push_back(static_cast<std::uint8_t>(chunk >> 8U));
        }
        if (padding < 1U) {
            out.push_back(static_cast<std::uint8_t>(chunk));
        }
    }
    // Only the canonical encoding: bits under the padding are zero.
    if (Base64Encode(out) != text) {
        return std::nullopt;
    }
    return out;
}

bool ConstantTimeEqual(std::span<const std::uint8_t> a, std::span<const std::uint8_t> b) noexcept {
    if (a.size() != b.size()) {
        return false;
    }
    volatile std::uint8_t diff = 0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        diff = static_cast<std::uint8_t>(diff | (a[i] ^ b[i]));
    }
    return diff == 0;
}

}  // namespace chunkdb::crypto
