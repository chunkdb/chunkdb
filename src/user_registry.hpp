#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <memory>
#include "chunkdb/migration_health.hpp"
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
    // The secret that derives the salts of unknown users.
    [[nodiscard]] std::array<std::uint8_t, 32> Secret() const;
    // Grows with every change, so a connection can tell its copy of its
    // user is stale with one atomic read.
    [[nodiscard]] std::uint64_t Generation() const noexcept { return generation_.load(std::memory_order_acquire); }

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
    [[nodiscard]] const std::filesystem::path& data_dir() const noexcept { return data_dir_; }
    void SetMigrationHealth(std::shared_ptr<MigrationHealth> health);
    void SetMigrationTestHook(MigrationTestHook* hook) noexcept { test_hook_.store(hook, std::memory_order_release); }

  private:
    friend class TableCatalog;
    std::atomic<MigrationTestHook*> test_hook_{nullptr};
    std::shared_ptr<MigrationHealth> migration_health_;
    // Applies `change` to a copy, writes it, then publishes it.
    template <typename Change>
    void Update(Change&& change);

    const std::filesystem::path data_dir_;
    mutable std::mutex mutex_;
    Users users_;
    std::atomic<std::uint64_t> generation_{1};
};

}  // namespace chunkdb
