#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "users.hpp"

// The users of a data directory in memory, written through to
// chunkdb.users (docs/USERS_DESIGN.md). Every change is checked against
// the rules, written to the file, and only then made visible; a failed write
// changes nothing.
namespace chunkdb {

class UserRegistry {
  public:
    // Opens the users of `data_dir`. A directory without chunkdb.users gets
    // one with `first_admin` (a user who manages users, with ADMIN on `*`),
    // or none when `first_admin` is empty; throws std::invalid_argument when
    // neither exists. `secret` seeds a new file.
    UserRegistry(
        std::filesystem::path data_dir,
        std::optional<std::pair<std::string, scram::Verifier>> first_admin,
        const std::array<std::uint8_t, 32>& secret);

    [[nodiscard]] std::optional<User> Find(const std::string& name) const;
    [[nodiscard]] Users Snapshot() const;

    // Each throws std::invalid_argument for a change the rules refuse: an
    // existing or unknown user, a bad name, removing the last user who
    // manages users.
    void Create(const std::string& name, scram::Verifier verifier, bool manages_users);
    void SetVerifier(const std::string& name, scram::Verifier verifier);
    void SetManagesUsers(const std::string& name, bool manages_users);
    void Drop(const std::string& name);
    void Grant(const std::string& name, const std::string& table, Right right);
    // Removes `right` and those above it on `table`: a user who had ADMIN
    // keeps WRITE after REVOKE ADMIN, and keeps nothing after REVOKE READ.
    void Revoke(const std::string& name, const std::string& table, Right right);
    // Removes the grants on a dropped table.
    void ForgetTable(const std::string& table);

  private:
    // Applies `change` to a copy, writes it, then publishes it.
    template <typename Change>
    void Update(Change&& change);

    const std::filesystem::path data_dir_;
    mutable std::mutex mutex_;
    Users users_;
};

}  // namespace chunkdb
