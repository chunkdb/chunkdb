#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "chunkdb/engine.hpp"
// Not in the include path of every test target; relative to this file.
#include "../src/scram.hpp"
#include "../src/user_registry.hpp"

// Logins with users (SCRAM-SHA-256, docs/design/USERS_DESIGN.md) for tests: a
// registry with a first administrator, a login through CommandEngine, and
// the bytes a socket client sends.
namespace chunkdb::test {

namespace login_detail {
struct Keys {
    crypto::Sha256Digest client;
    crypto::Sha256Digest server;
};
inline std::mutex keys_mutex;
inline std::map<std::pair<std::string, std::string>, Keys> prepared_keys;
}  // namespace login_detail

// Prepare both sides before starting a socket fixture's handshake deadline.
// A new salt gives each fixture independent keys; only the test helpers use
// this cache, and an unprepared credential is refused rather than derived.
inline scram::Verifier PrepareLoginVerifier(std::string_view password, std::span<const std::uint8_t> salt,
                                           std::uint32_t iterations) {
    const auto salted = crypto::Pbkdf2Sha256(crypto::Bytes(password), salt, iterations);
    const login_detail::Keys keys{crypto::HmacSha256(salted, crypto::Bytes("Client Key")),
                                  crypto::HmacSha256(salted, crypto::Bytes("Server Key"))};
    const auto challenge = crypto::Base64Encode(salt) + ",i=" + std::to_string(iterations);
    {
        std::lock_guard lock(login_detail::keys_mutex);
        login_detail::prepared_keys.insert_or_assign({std::string(password), challenge}, keys);
    }
    return {iterations, std::vector<std::uint8_t>(salt.begin(), salt.end()), crypto::Sha256Of(keys.client), keys.server};
}

// The users of `data_dir` (created when missing) with `admin` as the first
// administrator: manages users, ADMIN on `*`.
inline std::shared_ptr<UserRegistry> MakeUsers(
    const std::filesystem::path& data_dir,
    const std::string& admin,
    const std::string& password) {
    std::filesystem::create_directories(data_dir);
    const auto salt = crypto::RandomBytes(16);
    const auto random = crypto::RandomBytes(32);
    std::array<std::uint8_t, 32> secret{};
    std::copy(random.begin(), random.end(), secret.begin());
    const auto registry = std::make_shared<UserRegistry>(
        data_dir,
        std::make_pair(admin, PrepareLoginVerifier(password, salt, scram::kMinIterations)),
        secret);
    if (const auto existing = registry->Find(admin); existing &&
        (existing->verifier.salt != salt || existing->verifier.iterations != scram::kMinIterations)) {
        (void)PrepareLoginVerifier(password, existing->verifier.salt, existing->verifier.iterations);
    }
    return registry;
}

// A line followed by one parameter frame, as a socket client sends it.
inline std::string LineWithFrame(std::string_view line, std::string_view frame) {
    std::string bytes(line);
    bytes += "\r\n$" + std::to_string(frame.size()) + "\r\n";
    bytes += frame;
    bytes += "\r\n";
    return bytes;
}

// The server-first message of a `+SCRAM <server-first>` reply; the reply
// itself when it is not one.
inline std::string ServerFirstOf(std::string_view reply) {
    while (!reply.empty() && (reply.back() == '\r' || reply.back() == '\n')) {
        reply.remove_suffix(1);
    }
    constexpr std::string_view kPrefix = "+SCRAM ";
    if (reply.substr(0, kPrefix.size()) == kPrefix) {
        reply.remove_prefix(kPrefix.size());
    }
    return std::string(reply);
}

// A client-final message continuing `server_first` with a proof no password
// gives, without the PBKDF2 a real one costs: a failed login.
inline std::string WrongProofFinal(std::string_view server_first) {
    const auto salt_at = server_first.find(",s=");
    if (server_first.substr(0, 2) != "r=" || salt_at == std::string_view::npos) {
        throw std::runtime_error("not a SCRAM server-first message: " + std::string(server_first));
    }
    const std::vector<std::uint8_t> proof(32, 0);
    return "c=biws,r=" + std::string(server_first.substr(2, salt_at - 2)) + ",p=" + crypto::Base64Encode(proof);
}

// HELLO 3 USER <user> $1: the line, and the line with its frame.
inline std::string HelloUserLine(std::string_view user) {
    return "HELLO 3 USER " + std::string(user) + " $1";
}
inline std::string HelloUserBytes(const scram::ClientLogin& login, std::string_view user) {
    return LineWithFrame(HelloUserLine(user), login.first);
}

// AUTH $1, line and frame, answering `server_reply` (`+SCRAM <server-first>`
// or the server-first message), and the server signature the HELLO map must
// then carry.
struct AuthStep {
    std::string bytes;
    std::string server_signature;
};
inline AuthStep AuthBytes(const scram::ClientLogin& login, std::string_view password, std::string_view server_reply) {
    const auto first = ServerFirstOf(server_reply);
    const auto salt_at = first.find(",s=");
    if (!first.starts_with("r=") || salt_at == std::string::npos)
        throw std::invalid_argument("not a SCRAM server-first message");
    const auto nonce = std::string_view(first).substr(2U, salt_at - 2U);
    if (!nonce.starts_with(login.nonce) || nonce.size() == login.nonce.size())
        throw std::invalid_argument("the server nonce does not continue the client nonce");
    login_detail::Keys keys;
    {
        std::lock_guard lock(login_detail::keys_mutex);
        const auto found = login_detail::prepared_keys.find({std::string(password), first.substr(salt_at + 3U)});
        if (found == login_detail::prepared_keys.end())
            throw std::logic_error("prepare login credentials before opening the test connection");
        keys = found->second;
    }
    const auto without_proof = "c=biws,r=" + std::string(nonce);
    const auto message = login.first_bare + "," + first + "," + without_proof;
    const auto signature = crypto::HmacSha256(crypto::Sha256Of(keys.client), crypto::Bytes(message));
    crypto::Sha256Digest proof{};
    for (std::size_t i = 0; i < proof.size(); ++i)
        proof[i] = static_cast<std::uint8_t>(keys.client[i] ^ signature[i]);
    return AuthStep{
        .bytes = LineWithFrame("AUTH $1", without_proof + ",p=" + crypto::Base64Encode(proof)),
        .server_signature = "v=" + crypto::Base64Encode(crypto::HmacSha256(keys.server, crypto::Bytes(message))),
    };
}
// AUTH $1 with a wrong proof: a failed login.
inline std::string WrongAuthBytes(std::string_view server_reply) {
    return LineWithFrame("AUTH $1", WrongProofFinal(ServerFirstOf(server_reply)));
}

// HELLO 3 USER and AUTH through CommandEngine::Execute. Returns the first
// reply that is not +SCRAM: the HELLO map, whose server_signature is checked,
// or the error. A null `password` sends a wrong proof without PBKDF2.
inline std::string LoginOnEngine(
    CommandEngine& engine,
    SessionState& session,
    const std::string& user,
    const std::optional<std::string>& password) {
    using Parameters = std::vector<std::optional<std::string>>;
    const auto login = scram::StartClientLogin(user, scram::NewNonce());
    const std::string first_reply = engine.Execute(session, HelloUserLine(user) + "\r\n", Parameters{login.first});
    if (first_reply.rfind("+SCRAM ", 0) != 0) {
        return first_reply;
    }
    const std::string server_first = ServerFirstOf(first_reply);
    if (!password.has_value()) {
        return engine.Execute(session, "AUTH $1\r\n", Parameters{WrongProofFinal(server_first)});
    }
    const auto client_final = scram::FinishClientLogin(login, *password, server_first);
    std::string reply = engine.Execute(session, "AUTH $1\r\n", Parameters{client_final.message});
    if (reply.rfind("%14\r\n", 0) == 0) {
        const std::string signature_entry = "$16\r\nserver_signature\r\n$" +
                                            std::to_string(client_final.server_signature.size()) + "\r\n" +
                                            client_final.server_signature + "\r\n";
        if (reply.find(signature_entry) == std::string::npos) {
            throw std::runtime_error("the HELLO map does not carry the expected server signature");
        }
    }
    return reply;
}
// A login with a wrong proof.
inline std::string FailedLoginOnEngine(CommandEngine& engine, SessionState& session, const std::string& user) {
    return LoginOnEngine(engine, session, user, std::nullopt);
}

}  // namespace chunkdb::test
