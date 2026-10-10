#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "scram.hpp"

// Users, their verifiers and rights (docs/design/USERS_DESIGN.md), kept in
// `chunkdb.users` in the data directory.
namespace chunkdb {

inline constexpr const char* kUsersFileName = "chunkdb.users";

// Ordered: each right includes the ones before it.
enum class Right : std::uint8_t {
    kRead = 1,
    kWrite = 2,
    kAdmin = 3,
};

// The table name `*` stands for every table.
inline constexpr const char* kEveryTable = "*";

struct User {
    scram::Verifier verifier;
    bool manages_users = false;
    // Table name, or kEveryTable, to the right granted on it.
    std::map<std::string, Right> grants;

    friend bool operator==(const User&, const User&) = default;
};

struct Users {
    // Derives the salts of unknown users, so a login reply does not tell
    // whether a user exists.
    std::array<std::uint8_t, 32> secret{};
    std::map<std::string, User> users;

    friend bool operator==(const Users&, const Users&) = default;
};

// The highest right `user` has on `table`, from a grant on it or on `*`.
[[nodiscard]] std::optional<Right> RightOn(const User& user, const std::string& table);

[[nodiscard]] std::vector<std::uint8_t> EncodeUsers(const Users& users);
// Throws std::invalid_argument naming the defect: a damaged, truncated or
// unknown file.
[[nodiscard]] Users DecodeUsers(const std::vector<std::uint8_t>& bytes);

// std::nullopt when the file does not exist.
[[nodiscard]] std::optional<Users> ReadUsersFile(const std::filesystem::path& data_dir);
// Replaces the file atomically, the file and its directory synced: a crash
// leaves the old or the new file.
void WriteUsersFile(const std::filesystem::path& data_dir, const Users& users);

}  // namespace chunkdb
