#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <memory>
#include "chunkdb/migration_health.hpp"
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "users.hpp"

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
        bool read_only = false);

    [[nodiscard]] std::optional<User> Find(const std::string& name) const;
    // A user with its incarnation: a number the registry assigns when it
    // creates or loads the user and never assigns again, so a user dropped
    // and created again under the same name has another. Kept in memory
    // only; sessions do not outlive the process.
    struct Incarnated {
        User user;
        std::uint64_t incarnation = 0;
    };
    [[nodiscard]] std::optional<Incarnated> FindIncarnation(const std::string& name) const;
    [[nodiscard]] Users Snapshot() const;
    // The secret that derives the salts of unknown users.
    [[nodiscard]] std::array<std::uint8_t, 32> Secret() const;
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
    // Sets the verifier of `name` only while it is still `incarnation`;
    // false, changing nothing, once that user was dropped.
    [[nodiscard]] bool SetOwnVerifier(const std::string& name, std::uint64_t incarnation, scram::Verifier verifier);
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
    std::atomic<MigrationTestHook*> test_hook_{nullptr};
    std::shared_ptr<MigrationHealth> migration_health_;
    // Applies `change` to a copy, writes it, then publishes it.
    template <typename Change>
    void Update(Change&& change);
    template <typename Change>
    void UpdateIf(Change&& change);
    // Under mutex_, after users_ changed: a user no longer present retires
    // its incarnation, a new one gets the next.
    void IncarnateLocked();

    const std::filesystem::path data_dir_;
    const bool read_only_;
    mutable std::mutex mutex_;
    Users users_;
    std::map<std::string, std::uint64_t> incarnations_;
    std::uint64_t next_incarnation_ = 1;
    std::atomic<std::uint64_t> generation_{1};
};

// Whether one logged-in user still has READ on one table, for a watch that
// outlives the statement that checked it. While the users are unchanged a
// check is one atomic load and compare; after a change it reads the user
// again. A lost right or user stays lost. One thread checks.
class ReadRightCheck {
  public:
    enum class State : std::uint8_t { kHeld, kNoRight, kNoUser };
    // `generation` is the registry generation at which READ was confirmed.
    ReadRightCheck(std::shared_ptr<const UserRegistry> users, std::string user, std::uint64_t incarnation,
        std::string table, std::uint64_t generation) noexcept
        : users_(std::move(users)), user_(std::move(user)), table_(std::move(table)),
          incarnation_(incarnation), generation_(generation) {}
    [[nodiscard]] State Check() {
        const std::uint64_t generation = users_->Generation();
        if (generation == generation_) [[likely]] return state_;
        return Recheck(generation);
    }
    [[nodiscard]] const std::string& table() const noexcept { return table_; }

  private:
    State Recheck(std::uint64_t generation);
    const std::shared_ptr<const UserRegistry> users_;
    const std::string user_;
    const std::string table_;
    const std::uint64_t incarnation_;
    std::uint64_t generation_;
    State state_ = State::kHeld;
};

}  // namespace chunkdb
