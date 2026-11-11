#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <exception>
#include <functional>
#include <map>
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
    // A user with its incarnation: a number assigned when the registry
    // creates or loads the user and never assigned again, so a user dropped
    // and created again under the same name has another. It is the
    // incarnation of the user's admission state and connection tickets. Kept
    // in memory only; sessions do not outlive the process.
    struct Incarnated {
        User user;
        std::uint64_t incarnation = 0;
    };
    [[nodiscard]] std::optional<Incarnated> FindIncarnation(const std::string& name) const;
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
    // The rights and incarnations of every user as of the latest change,
    // published before Generation() moves past it. Reading it never waits
    // for the registry lock, which a change holds while it syncs the file.
    using RightsView = UserRightsView;
    [[nodiscard]] std::shared_ptr<const RightsView> Rights() const;
    // Runs `wake` after every change while the returned handle is held,
    // once the registry lock is released. It must not block or change users.
    [[nodiscard]] std::shared_ptr<const std::function<void()>> OnChange(std::function<void()> wake);
    // Runs the OnChange callbacks of the changes committed while it lives as
    // it goes out of scope: declared before the registry lock is taken, it
    // runs them after that lock is released.
    class Wakes {
      public:
        explicit Wakes(UserRegistry* registry) noexcept : registry_(registry) {}
        Wakes(const Wakes&) = delete;
        Wakes& operator=(const Wakes&) = delete;
        ~Wakes();

      private:
        friend class UserRegistry;
        UserRegistry* const registry_;
        bool changed_ = false;
    };

    // Each throws std::invalid_argument for a change the rules refuse: an
    // existing or unknown user, a bad name, removing the last user who
    // manages users.
    void Create(const std::string& name, scram::Verifier verifier, bool manages_users, bool if_not_exists = false,
        bool requires_channel_binding = false);
    // An existing conditional user keeps its verifier and rights; the factory
    // is evaluated only after the registry lock confirms the user is absent.
    void Create(const std::string& name, const std::function<scram::Verifier()>& make_verifier,
        bool manages_users, bool if_not_exists = false, bool requires_channel_binding = false);
    void SetVerifier(const std::string& name, scram::Verifier verifier);
    // Sets the verifier of `name` only while it is still `incarnation`;
    // false, changing nothing, once that user was dropped.
    [[nodiscard]] bool SetOwnVerifier(const std::string& name, std::uint64_t incarnation, scram::Verifier verifier);
    void SetManagesUsers(const std::string& name, bool manages_users);
    void SetRequiresChannelBinding(const std::string& name, bool requires_channel_binding);
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
    // What publishing `next` changes besides it: admission state, the
    // incarnation of each user and the rights view. Prepared under mutex_
    // before anything changes, so a failure leaves users, file and view as
    // they were.
    [[nodiscard]] UserPublication PreparePublication(std::shared_ptr<const Users> next) const;
    // Under mutex_: makes `next` visible. The caller moves the generation
    // past it and marks `wakes` changed.
    void Publish(UserPublication next);
    // Under mutex_: makes `next` current and visible, moves the generation
    // past it, and marks `wakes` to run every OnChange once the lock is
    // released.
    void CommitPublication(Users next, UserPublication publication, Wakes& wakes);
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
    // Guards only the pointer: held for a copy, never across I/O or another lock.
    mutable std::mutex rights_mutex_;
    std::shared_ptr<const RightsView> rights_;
    std::mutex listeners_mutex_;
    std::vector<std::weak_ptr<const std::function<void()>>> listeners_;
};

// Whether one logged-in user still has READ on one table, and ADMIN on `*`
// when the watch needs it, for a watch that outlives the statement that
// checked it. While the users are unchanged a
// check is one atomic load and compare; after a change it looks up that
// user and table in the published rights view, without the registry lock.
// A lost right or user stays lost. One thread checks.
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
    // Also requires ADMIN on `*` from now on: a watch of a slot another user
    // owns. Set by the statement thread before the watch reaches the feed.
    void RequireEveryTableAdmin() noexcept { every_table_admin_ = true; }

  private:
    State Recheck(std::uint64_t generation);
    const std::shared_ptr<const UserRegistry> users_;
    const std::string user_;
    const std::string table_;
    const std::uint64_t incarnation_;
    std::uint64_t generation_;
    State state_ = State::kHeld;
    bool every_table_admin_ = false;
};

}  // namespace chunkdb
