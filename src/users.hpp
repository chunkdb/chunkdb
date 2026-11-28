#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "scram.hpp"

// Users, their verifiers and rights (docs/design/USERS_DESIGN.md), kept in
// `chunkdb.users` in the data directory.
namespace chunkdb {

inline constexpr const char* kUsersFileName = "chunkdb.users";
inline constexpr std::uint64_t kMaxUserStatementsPerSecond = 65536U;
// The writer refuses a larger users file and the reader refuses one before
// reading it.
inline constexpr std::uint64_t kMaxUsersFileBytes = std::uint64_t{64} << 20U;

class UsersOutcomeUnknownError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Ordered: each right includes the ones before it.
enum class Right : std::uint8_t {
    kRead = 1,
    kWrite = 2,
    kAdmin = 3,
};

// Rights on the server rather than on tables, granted ON *; bits of
// User::server_rights. They neither include nor are included by table rights.
enum class ServerRight : std::uint8_t {
    kMonitor = 1,
    kBackup = 2,
    // Kept and shown; no statement needs it yet.
    kReplication = 4,
};
inline constexpr std::uint8_t kKnownServerRights = 7U;

// The table name `*` stands for every table; a grant name ending in `*`
// after a prefix stands for every table whose name starts with that prefix.
inline constexpr const char* kEveryTable = "*";

struct User {
    // The older verifier while the user has two.
    scram::Verifier verifier;
    // A verifier added by ALTER USER ... ADD VERIFIER: the user logs in with
    // either until DROP OLD VERIFIER makes it the only one. It shares the salt
    // and iteration count of `verifier`, which the server sends before the
    // client proves its password.
    std::optional<scram::Verifier> added_verifier{};
    bool manages_users = false;
    // Only logins bound to their TLS connection are accepted for this user.
    bool requires_channel_binding = false;
    // Table name, or kEveryTable, to the right granted on it.
    std::map<std::string, Right> grants;
    // Zero disables either limit. The runtime enforces them at admission.
    std::uint64_t max_connections = 0U;
    std::uint64_t statements_per_sec = 0U;
    // Table name prefix, without its `*`, to the right granted on it.
    std::map<std::string, Right> prefix_grants = {};
    // ServerRight bits.
    std::uint8_t server_rights = 0U;

    friend bool operator==(const User&, const User&) = default;
};

[[nodiscard]] inline bool HasServerRight(const User& user, ServerRight right) noexcept {
    return (user.server_rights & static_cast<std::uint8_t>(right)) != 0U;
}

struct Users {
    // Derives the salts of unknown users, so a login reply does not tell
    // whether a user exists.
    std::array<std::uint8_t, 32> secret{};
    std::map<std::string, User> users;

    friend bool operator==(const Users&, const Users&) = default;
};

// Whether a grant can name `table`: kEveryTable, a valid table name, also
// one not yet created, or a prefix grant `p*` whose `p` could start one.
[[nodiscard]] bool IsValidGrantTable(std::string_view table) noexcept;
// Throws std::invalid_argument when IsValidGrantTable(table) is false.
void RequireValidGrantTable(std::string_view table);
// Whether `table` names a prefix grant: a prefix followed by `*`.
[[nodiscard]] bool IsPrefixGrant(std::string_view table) noexcept;

// The highest right `user` has on `table`, from a grant on it, on a prefix
// of its name, or on `*`. A user without prefix grants pays two lookups.
[[nodiscard]] std::optional<Right> RightOn(const User& user, const std::string& table);
// The same from grants alone.
[[nodiscard]] std::optional<Right> RightOn(const std::map<std::string, Right>& grants,
    const std::map<std::string, Right>& prefix_grants, const std::string& table);
// Whether `user` has at least `right` on some table, name or prefix.
[[nodiscard]] bool HasAnyGrant(const User& user, Right right) noexcept;

// GRANT, or REVOKE when `revoke`, on `user`: `right` on `table` (a table, a
// prefix grant or kEveryTable) and the ServerRight bits `server_rights`,
// which need kEveryTable. Granting keeps a higher right; revoking removes
// `right` and those above it. Throws std::invalid_argument for server
// rights on another target or unknown bits.
void ApplyGrant(User& user, const std::string& table, std::optional<Right> right, std::uint8_t server_rights, bool revoke);

// Whether `verifier` is one of the verifiers of `user`.
[[nodiscard]] bool HoldsVerifier(const User& user, const scram::Verifier& verifier) noexcept;
// Throws std::invalid_argument unless `added` can be added to a user whose
// verifier is `verifier`: the same salt and iteration count, and other keys.
void RequireAddableVerifier(const scram::Verifier& verifier, const scram::Verifier& added);

// Throws std::invalid_argument for a field that does not fit the file: a
// verifier salt outside scram::kMinSaltBytes..scram::kMaxSaltBytes, an added
// verifier with another salt or iteration count, a name over 65535 bytes, or
// a count over 2^32 - 1.
[[nodiscard]] std::vector<std::uint8_t> EncodeUsers(const Users& users);
// Throws std::invalid_argument naming the defect: a damaged, truncated or
// unknown file, or a user name, grant table name, iteration count or salt
// length outside what CREATE USER, ALTER USER and GRANT accept.
[[nodiscard]] Users DecodeUsers(const std::vector<std::uint8_t>& bytes);
// Checks the same format and duplicates without building a users registry.
void ValidateUsersImage(const std::vector<std::uint8_t>& bytes);

// The exact bytes for journaling, bounded by kMaxUsersFileBytes and not decoded.
// Both readers return std::nullopt when the file does not exist.
[[nodiscard]] std::optional<std::vector<std::uint8_t>> ReadUsersFileImage(const std::filesystem::path& data_dir);
[[nodiscard]] std::optional<Users> ReadUsersFile(const std::filesystem::path& data_dir);
// Replaces the file atomically, the file and its directory synced: a crash
// leaves the old or the new file.
void WriteUsersFile(const std::filesystem::path& data_dir, const Users& users, bool* out_replaced = nullptr);

}  // namespace chunkdb
