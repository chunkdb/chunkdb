#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "users.hpp"

namespace chunkdb {
using UserAdmissionTime = std::chrono::steady_clock::time_point;
using UserAdmissionClock = std::function<UserAdmissionTime()>;

struct UserAdmissionState {
    const std::uint64_t incarnation;
    std::atomic<bool> live{true};
    std::mutex mutex;
    std::uint64_t verifier_generation = 1U;
    std::uint64_t connections = 0U;
    std::uint64_t max_connections = 0U;
    std::atomic<std::uint64_t> statements_per_sec{0U};
    std::vector<UserAdmissionTime> history;
    std::size_t history_head = 0U, history_size = 0U;
    UserAdmissionClock clock;
    UserAdmissionState(std::uint64_t id, const User& user, UserAdmissionClock now);
};

class UserConnectionTicket {
  public:
    explicit UserConnectionTicket(std::shared_ptr<UserAdmissionState> state) : state_(std::move(state)) {}
    ~UserConnectionTicket();
    UserConnectionTicket(const UserConnectionTicket&) = delete;
    UserConnectionTicket& operator=(const UserConnectionTicket&) = delete;
    [[nodiscard]] std::uint64_t incarnation() const noexcept { return state_->incarnation; }
  private:
    friend class UserRegistry;
    std::shared_ptr<UserAdmissionState> state_;
    bool admitted_ = false;
};

enum class UserAdmissionStatus { kAllowed, kStale, kConnectionLimit, kRateLimit };
struct UserAdmissionResult {
    UserAdmissionStatus status = UserAdmissionStatus::kAllowed;
    std::uint64_t retry_after_ms = 0U;
    std::shared_ptr<UserConnectionTicket> ticket{};
};
struct UserAuthenticationSnapshot {
    User user;
    std::shared_ptr<UserAdmissionState> state;
    std::uint64_t verifier_generation = 0U;
};

struct UserAdmissionEntry {
    std::shared_ptr<UserAdmissionState> state;
    std::uint64_t verifier_generation = 1U;
};
using UserAdmissionMap = std::map<std::string, UserAdmissionEntry>;
struct UserAdmissionUpdate {
    std::shared_ptr<UserAdmissionState> state;
    bool retire = false;
    std::uint64_t verifier_generation = 0U;
    std::uint64_t max_connections = 0U, statements_per_sec = 0U;
    std::vector<UserAdmissionTime> grown_history{};
};
struct UserPublication {
    UserPublication() = default;
    UserPublication(const UserPublication&) = delete;
    UserPublication& operator=(const UserPublication&) = delete;
    UserPublication(UserPublication&&) = default;
    UserPublication& operator=(UserPublication&&) = default;
    std::shared_ptr<const Users> users;
    std::shared_ptr<const UserAdmissionMap> admissions;
    std::vector<UserAdmissionUpdate> updates;
};
}  // namespace chunkdb
