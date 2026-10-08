#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "crypto.hpp"

// The server side of SCRAM-SHA-256 (RFC 5802, RFC 7677) as HELLO uses it
// (docs/USERS_DESIGN.md): no channel binding, no extensions.
namespace chunkdb::scram {

inline constexpr std::uint32_t kMinIterations = 4096;

// What the server stores for a user instead of the password.
struct Verifier {
    std::uint32_t iterations = 0;
    std::vector<std::uint8_t> salt;
    crypto::Sha256Digest stored_key{};
    crypto::Sha256Digest server_key{};

    friend bool operator==(const Verifier&, const Verifier&) = default;
};

// The verifier of `password`; clients compute it, the server only for the
// first administrator and the reset tool.
[[nodiscard]] Verifier MakeVerifier(
    std::string_view password,
    std::span<const std::uint8_t> salt,
    std::uint32_t iterations);
// The verifier a login of unknown user `name` runs against: a salt derived
// from `secret` and the name, so the same name always gets the same reply,
// and keys no proof matches. The exchange then fails like a wrong password.
[[nodiscard]] Verifier DecoyVerifier(std::span<const std::uint8_t> secret, std::string_view name);
// A fresh server nonce: printable, without commas.
[[nodiscard]] std::string NewNonce();

// `SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`, base64
// parts: the form CREATE USER and ALTER USER take.
[[nodiscard]] std::string FormatVerifier(const Verifier& verifier);
// Throws std::invalid_argument naming what is wrong, also for fewer than
// kMinIterations iterations.
[[nodiscard]] Verifier ParseVerifier(std::string_view text);

// `n,,n=<user>,r=<nonce>`.
struct ClientFirst {
    std::string user;
    std::string nonce;
    // Without the `n,,` header: part of the signed message.
    std::string bare;
};
// Throws std::invalid_argument for anything else, channel binding included.
[[nodiscard]] ClientFirst ParseClientFirst(std::string_view message);

// One login attempt after the client-first message.
class ServerExchange {
  public:
    ServerExchange(ClientFirst first, Verifier verifier, std::string_view server_nonce);
    // `r=<client nonce + server nonce>,s=<salt>,i=<iterations>`.
    [[nodiscard]] const std::string& ServerFirst() const noexcept { return server_first_; }
    // The server-final message `v=<signature>` when the client's proof is
    // right, std::nullopt when it is wrong. Throws std::invalid_argument for
    // a message that is not a client-final message of this exchange.
    [[nodiscard]] std::optional<std::string> Finish(std::string_view client_final) const;

  private:
    ClientFirst first_;
    Verifier verifier_;
    std::string nonce_;
    std::string server_first_;
};

// The client side, for tests, the bench and tools: the messages a client
// sends and the server signature it must then receive.
struct ClientLogin {
    std::string first;       // n,,n=<user>,r=<nonce>
    std::string first_bare;  // n=<user>,r=<nonce>
    std::string nonce;
};
[[nodiscard]] ClientLogin StartClientLogin(std::string_view user, std::string_view nonce);
struct ClientFinal {
    std::string message;           // c=biws,r=<nonce>,p=<proof>
    std::string server_signature;  // v=<signature> the server must answer
};
// Throws std::invalid_argument when `server_first` is not a server-first
// message continuing `login`.
[[nodiscard]] ClientFinal FinishClientLogin(
    const ClientLogin& login,
    std::string_view password,
    std::string_view server_first);

}  // namespace chunkdb::scram
