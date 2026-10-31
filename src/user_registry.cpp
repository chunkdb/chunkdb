#include "user_registry.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>

#include "file_permissions.hpp"
#include "chunkdb/logging.hpp"

namespace chunkdb {

bool IsValidUserName(std::string_view name) noexcept {
    if (name.empty() || name.size() > 63U || !((name[0] >= 'a' && name[0] <= 'z') || name[0] == '_')) {
        return false;
    }
    return std::all_of(name.begin(), name.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
    });
}

namespace {

[[nodiscard]] bool ValidName(const std::string& name) noexcept {
    return IsValidUserName(name);
}

void RequireName(const std::string& name) {
    if (!ValidName(name)) {
        throw std::invalid_argument("a user name is [a-z_][a-z0-9_]*, at most 63 bytes: " + name);
    }
}

[[nodiscard]] User& Existing(Users& users, const std::string& name) {
    const auto found = users.users.find(name);
    if (found == users.users.end()) {
        throw std::invalid_argument("user " + name + " does not exist");
    }
    return found->second;
}

void RequireUserManager(const Users& users) {
    if (std::none_of(users.users.begin(), users.users.end(), [](const auto& entry) { return entry.second.manages_users; })) {
        throw std::invalid_argument("the last user who manages users cannot be dropped or lose that right");
    }
}

}  // namespace

UserRegistry::UserRegistry(
    std::filesystem::path data_dir,
    std::optional<std::pair<std::string, scram::Verifier>> first_admin,
    const std::array<std::uint8_t, 32>& secret,
    bool read_only)
    : data_dir_(std::filesystem::weakly_canonical(data_dir)), read_only_(read_only) {
    const auto users_path = data_dir_ / kUsersFileName;
    // Refuse unsupported records before changing permissions or bookkeeping.
    auto existing = ReadUsersFile(data_dir_);
    if (read_only_) {
        if (std::filesystem::exists(users_path) && !FileIsOwnerOnly(users_path)) {
            LogMessage(LogLevel::kWarn, LogComponent::kStore,
                "users file is not owner-only; read-only mode leaves its permissions as they are",
                {{"path", users_path.string()}});
        }
    } else if (std::filesystem::exists(users_path) && RestrictFileToOwner(users_path)) {
        LogMessage(LogLevel::kInfo, LogComponent::kStore, "restricted users file to owner-only access",
            {{"path", users_path.string()}});
    }
    if (existing.has_value()) {
        users_ = std::move(*existing);
        return;
    }
    if (read_only_) {
        throw std::invalid_argument("the data directory has no users, and a read-only registry creates none");
    }
    if (!first_admin.has_value()) {
        throw std::invalid_argument(
            "the data directory has no users: set CHUNKDB_ADMIN_USER and CHUNKDB_ADMIN_PASSWORD (or "
            "--admin-user and --admin-password-file) for the first start, or run with --auth none");
    }
    RequireName(first_admin->first);
    Users created;
    created.secret = secret;
    created.users[first_admin->first] = User{
        .verifier = std::move(first_admin->second),
        .manages_users = true,
        .grants = {{kEveryTable, Right::kAdmin}},
    };
    WriteUsersFile(data_dir_, created);
    users_ = std::move(created);
}

void UserRegistry::SetMigrationHealth(std::shared_ptr<MigrationHealth> health) {
    std::lock_guard lock(mutex_);
    health->Check();
    const auto stored = ReadUsersFile(data_dir_);
    if (!stored) throw std::runtime_error("users file disappeared while binding the catalog");
    if (users_ != *stored) {
        users_ = *stored;
        generation_.fetch_add(1, std::memory_order_acq_rel);
    }
    migration_health_ = std::move(health);
}

std::optional<User> UserRegistry::Find(const std::string& name) const {
    std::lock_guard lock(mutex_);
    const auto found = users_.users.find(name);
    if (found == users_.users.end()) {
        return std::nullopt;
    }
    return found->second;
}

Users UserRegistry::Snapshot() const {
    std::lock_guard lock(mutex_);
    return users_;
}

std::array<std::uint8_t, 32> UserRegistry::Secret() const {
    std::lock_guard lock(mutex_);
    return users_.secret;
}

template <typename Change>
void UserRegistry::UpdateIf(Change&& change) {
    if (auto* hook = test_hook_.load(std::memory_order_acquire))
        hook->Run(MigrationTestHook::Point::kBeforeUserUpdate, {});
    if (read_only_) {
        throw std::invalid_argument("users cannot change: the data directory is open read-only");
    }
    std::lock_guard lock(mutex_);
    if (migration_health_) migration_health_->Check();
    Users next = users_;
    if (!change(next)) return;
    WriteUsersFile(data_dir_, next);
    users_ = std::move(next);
    generation_.fetch_add(1, std::memory_order_acq_rel);
}

template <typename Change>
void UserRegistry::Update(Change&& change) {
    UpdateIf([&](Users& users) {
        change(users);
        return true;
    });
}

void UserRegistry::Create(const std::string& name, scram::Verifier verifier, bool manages_users, bool if_not_exists) {
    Create(name, [&] { return std::move(verifier); }, manages_users, if_not_exists);
}

void UserRegistry::Create(const std::string& name, const std::function<scram::Verifier()>& make_verifier,
    bool manages_users, bool if_not_exists) {
    RequireName(name);
    UpdateIf([&](Users& users) {
        if (users.users.contains(name)) {
            if (if_not_exists) return false;
            throw std::invalid_argument("user " + name + " already exists");
        }
        users.users.emplace(name, User{.verifier = make_verifier(), .manages_users = manages_users, .grants = {}});
        return true;
    });
}

void UserRegistry::SetVerifier(const std::string& name, scram::Verifier verifier) {
    Update([&](Users& users) { Existing(users, name).verifier = std::move(verifier); });
}

void UserRegistry::SetManagesUsers(const std::string& name, bool manages_users) {
    Update([&](Users& users) {
        Existing(users, name).manages_users = manages_users;
        RequireUserManager(users);
    });
}

void UserRegistry::Drop(const std::string& name, bool if_exists) {
    RequireName(name);
    UpdateIf([&](Users& users) {
        const auto found = users.users.find(name);
        if (found == users.users.end()) {
            if (if_exists) return false;
            throw std::invalid_argument("user " + name + " does not exist");
        }
        users.users.erase(found);
        RequireUserManager(users);
        return true;
    });
}

void UserRegistry::Grant(const std::string& name, const std::string& table, Right right) {
    Update([&](Users& users) {
        auto& grants = Existing(users, name).grants;
        const auto found = grants.find(table);
        if (found == grants.end() || found->second < right) {
            grants[table] = right;
        }
    });
}

void UserRegistry::Revoke(const std::string& name, const std::string& table, Right right) {
    Update([&](Users& users) {
        auto& grants = Existing(users, name).grants;
        const auto found = grants.find(table);
        if (found == grants.end() || found->second < right) {
            return;
        }
        if (right == Right::kRead) {
            grants.erase(found);
        } else {
            found->second = static_cast<Right>(static_cast<std::uint8_t>(right) - 1U);
        }
    });
}

void UserRegistry::ForgetTable(const std::string& table) {
    Update([&](Users& users) {
        for (auto& [name, user] : users.users) {
            (void)name;
            user.grants.erase(table);
        }
    });
}

}  // namespace chunkdb
