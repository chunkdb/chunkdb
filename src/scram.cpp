#include "scram.hpp"

#include <charconv>
#include <stdexcept>

namespace chunkdb::scram {

namespace {

constexpr std::string_view kPrefix = "SCRAM-SHA-256$";
// base64("n,,"): no channel binding.
constexpr std::string_view kChannelBinding = "c=biws";

[[nodiscard]] crypto::Sha256Digest DigestOf(std::string_view base64, const char* what) {
    const auto bytes = crypto::Base64Decode(base64);
    if (!bytes.has_value() || bytes->size() != 32U) {
        throw std::invalid_argument(std::string(what) + " must be 32 bytes in base64");
    }
    crypto::Sha256Digest digest{};
    std::copy(bytes->begin(), bytes->end(), digest.begin());
    return digest;
}

// A nonce is printable ASCII without ','.
[[nodiscard]] bool ValidNonce(std::string_view nonce) noexcept {
    if (nonce.empty()) {
        return false;
    }
    for (const char c : nonce) {
        if (c < 0x21 || c > 0x7e || c == ',') {
            return false;
        }
    }
    return true;
}

}  // namespace

Verifier MakeVerifier(std::string_view password, std::span<const std::uint8_t> salt, std::uint32_t iterations) {
    const auto salted = crypto::Pbkdf2Sha256(crypto::Bytes(password), salt, iterations);
    const auto client_key = crypto::HmacSha256(salted, crypto::Bytes("Client Key"));
    return Verifier{
        .iterations = iterations,
        .salt = std::vector<std::uint8_t>(salt.begin(), salt.end()),
        .stored_key = crypto::Sha256Of(client_key),
        .server_key = crypto::HmacSha256(salted, crypto::Bytes("Server Key")),
    };
}

std::string FormatVerifier(const Verifier& verifier) {
    return std::string(kPrefix) + std::to_string(verifier.iterations) + ":" + crypto::Base64Encode(verifier.salt) +
           "$" + crypto::Base64Encode(verifier.stored_key) + ":" + crypto::Base64Encode(verifier.server_key);
}

Verifier ParseVerifier(std::string_view text) {
    const auto malformed = [] {
        return std::invalid_argument(
            "a verifier is SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey> with base64 parts");
    };
    if (text.substr(0, kPrefix.size()) != kPrefix) {
        throw malformed();
    }
    text.remove_prefix(kPrefix.size());
    const auto colon = text.find(':');
    const auto dollar = text.find('$');
    if (colon == std::string_view::npos || dollar == std::string_view::npos || colon > dollar) {
        throw malformed();
    }
    const auto keys = text.substr(dollar + 1);
    const auto key_colon = keys.find(':');
    if (key_colon == std::string_view::npos) {
        throw malformed();
    }
    Verifier verifier;
    const auto iterations = text.substr(0, colon);
    const auto result = std::from_chars(iterations.data(), iterations.data() + iterations.size(), verifier.iterations);
    if (result.ec != std::errc() || result.ptr != iterations.data() + iterations.size()) {
        throw malformed();
    }
    if (verifier.iterations < kMinIterations) {
        throw std::invalid_argument("a verifier needs at least " + std::to_string(kMinIterations) + " iterations");
    }
    const auto salt = crypto::Base64Decode(text.substr(colon + 1, dollar - colon - 1));
    if (!salt.has_value() || salt->size() < 16U) {
        throw std::invalid_argument("a verifier's salt must be at least 16 bytes in base64");
    }
    verifier.salt = *salt;
    verifier.stored_key = DigestOf(keys.substr(0, key_colon), "StoredKey");
    verifier.server_key = DigestOf(keys.substr(key_colon + 1), "ServerKey");
    return verifier;
}

ClientFirst ParseClientFirst(std::string_view message) {
    if (message.substr(0, 3) != "n,,") {
        throw std::invalid_argument("the SCRAM client-first message starts with n,, (no channel binding)");
    }
    const std::string_view bare = message.substr(3);
    if (bare.substr(0, 2) != "n=") {
        throw std::invalid_argument("the SCRAM client-first message names the user (n=)");
    }
    const auto comma = bare.find(',');
    if (comma == std::string_view::npos || bare.substr(comma + 1, 2) != "r=") {
        throw std::invalid_argument("the SCRAM client-first message carries a nonce (r=)");
    }
    const auto nonce = bare.substr(comma + 3);
    if (!ValidNonce(nonce)) {
        throw std::invalid_argument("a SCRAM nonce is printable ASCII without commas");
    }
    return ClientFirst{
        .user = std::string(bare.substr(2, comma - 2)),
        .nonce = std::string(nonce),
        .bare = std::string(bare),
    };
}

ServerExchange::ServerExchange(ClientFirst first, Verifier verifier, std::string_view server_nonce)
    : first_(std::move(first)),
      verifier_(std::move(verifier)),
      nonce_(first_.nonce + std::string(server_nonce)) {
    if (!ValidNonce(server_nonce)) {
        throw std::invalid_argument("a SCRAM nonce is printable ASCII without commas");
    }
    server_first_ = "r=" + nonce_ + ",s=" + crypto::Base64Encode(verifier_.salt) + ",i=" +
                    std::to_string(verifier_.iterations);
}

std::optional<std::string> ServerExchange::Finish(std::string_view client_final) const {
    const std::string expected_start = std::string(kChannelBinding) + ",r=" + nonce_ + ",p=";
    if (client_final.substr(0, expected_start.size()) != expected_start) {
        throw std::invalid_argument("the SCRAM client-final message does not continue this exchange");
    }
    const auto proof = crypto::Base64Decode(client_final.substr(expected_start.size()));
    if (!proof.has_value() || proof->size() != 32U) {
        throw std::invalid_argument("the SCRAM proof must be 32 bytes in base64");
    }
    const std::string without_proof(client_final.substr(0, expected_start.size() - 3));
    const std::string auth_message = first_.bare + "," + server_first_ + "," + without_proof;
    const auto client_signature = crypto::HmacSha256(verifier_.stored_key, crypto::Bytes(auth_message));
    crypto::Sha256Digest client_key{};
    for (std::size_t i = 0; i < client_key.size(); ++i) {
        client_key[i] = static_cast<std::uint8_t>((*proof)[i] ^ client_signature[i]);
    }
    if (!crypto::ConstantTimeEqual(crypto::Sha256Of(client_key), verifier_.stored_key)) {
        return std::nullopt;
    }
    return "v=" + crypto::Base64Encode(crypto::HmacSha256(verifier_.server_key, crypto::Bytes(auth_message)));
}

}  // namespace chunkdb::scram
