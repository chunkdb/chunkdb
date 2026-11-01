#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <exception>
#include <functional>
#include <mutex>
#include <memory>
#include "chunkdb/migration_health.hpp"
#include "chunkdb/statement_mutex.hpp"
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "users.hpp"
#include "user_admission.hpp"

// The users of a data directory in memory, written through to
// chunkdb.users (docs/design/USERS_DESIGN.md). Every change is checked against
// the rules, written to the file, and only then made visible; a failed write
// changes nothing.
namespace chunkdb {

// Whether `name` can name a user: [a-z_][a-z0-9_]*, at most 63 bytes.
[[nodiscard]] bool IsValidUserName(std::string_view name) noexcept;

class UserRegistry {
  public:
    // Opens the users of `data_dir`. A directory without chunkdb.users gets
    // one with `first_admin` (a user who manages users, with ADMIN on `*`),
    // or none when `first_admin` is empty; throws std::invalid_argument when
    // neither exists. `secret` seeds a new file. A `read_only` registry
    // changes neither the file nor its permissions, and refuses every change.
    UserRegistry(
        std::filesystem::path data_dir,
        std::optional<std::pair<std::string, scram::Verifier>> first_admin,
        const std::array<std::uint8_t, 32>& secret,
        bool read_only = false,
        UserAdmissionClock admission_clock = {});

    [[nodiscard]] std::optional<User> Find(const std::string& name) const;
    [[nodiscard]] Users Snapshot() const;
    // The secret that derives the salts of unknown users.
    [[nodiscard]] std::array<std::uint8_t, 32> Secret() const;
    [[nodiscard]] std::optional<UserAuthenticationSnapshot> AuthenticationSnapshot(const std::string& name) const;
    [[nodiscard]] bool AuthenticationCurrent(const UserAuthenticationSnapshot& expected) const;
    [[nodiscard]] UserAdmissionResult AcquireConnection(const UserAuthenticationSnapshot& expected) const;
    [[nodiscard]] std::optional<User> FindForConnection(const std::string& name,
        const std::shared_ptr<UserConnectionTicket>& ticket) const;
    [[nodiscard]] UserAdmissionResult AdmitStatement(const std::shared_ptr<UserConnectionTicket>& ticket) const;
    void SetLimits(const std::string& name, std::optional<std::uint64_t> max_connections,
        std::optional<std::uint64_t> statements_per_sec);
    // Grows with every change, so a connection can tell its copy of its
    // user is stale with one atomic read.
    [[nodiscard]] std::uint64_t Generation() const noexcept { return generation_.load(std::memory_order_acquire); }

    // Each throws std::invalid_argument for a change the rules refuse: an
    // existing or unknown user, a bad name, removing the last user who
    // manages users.
    void Create(const std::string& name, scram::Verifier verifier, bool manages_users, bool if_not_exists = false);
    // An existing conditional user keeps its verifier and rights; the factory
    // is evaluated only after the registry lock confirms the user is absent.
    void Create(const std::string& name, const std::function<scram::Verifier()>& make_verifier,
        bool manages_users, bool if_not_exists = false);
    void SetVerifier(const std::string& name, scram::Verifier verifier);
    void SetManagesUsers(const std::string& name, bool manages_users);
    void Drop(const std::string& name, bool if_exists = false);
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
    friend struct UserRegistryTestAccess;
    std::atomic<MigrationTestHook*> test_hook_{nullptr};
    std::shared_ptr<MigrationHealth> migration_health_;
    // Applies `change` to a copy, writes it, then publishes it.
    template <typename Change>
    void Update(Change&& change);
    template <typename Change>
    void UpdateIf(Change&& change);
    [[nodiscard]] std::shared_ptr<const Users> Published() const;
    [[nodiscard]] UserPublication PreparePublication(std::shared_ptr<const Users> next) const;
    void Publish(UserPublication next);
    void ConfirmPublication();

    const std::filesystem::path data_dir_;
    const bool read_only_;
    const UserAdmissionClock admission_clock_;
    mutable StatementMutex mutex_;
    Users users_;
    std::exception_ptr publication_unknown_;
    // Readers use one published image while the serialized writer performs I/O.
    mutable std::mutex published_mutex_;
    std::shared_ptr<const Users> published_;
    std::shared_ptr<const UserAdmissionMap> admissions_;
    std::atomic<std::uint64_t> generation_{1};
};

}  // namespace chunkdb
