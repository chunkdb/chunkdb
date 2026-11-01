#include "user_registry.hpp"
#include "chunk_store_internal.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace chunkdb {
namespace {
std::atomic<std::uint64_t> next_incarnation{1U};
std::uint64_t NewIncarnation() {
    auto current = next_incarnation.load(std::memory_order_relaxed);
    for (;;) {
        if (current == std::numeric_limits<std::uint64_t>::max())
            throw std::runtime_error("user incarnation space exhausted");
        if (next_incarnation.compare_exchange_weak(current, current + 1U, std::memory_order_relaxed))
            return current;
    }
}
}

UserAdmissionState::UserAdmissionState(std::uint64_t id, const User& user, UserAdmissionClock now)
    : incarnation(id), max_connections(user.max_connections), statements_per_sec(user.statements_per_sec),
      history(static_cast<std::size_t>(user.statements_per_sec)), clock(std::move(now)) {}

UserConnectionTicket::~UserConnectionTicket() {
    if (!admitted_) return;
    std::lock_guard lock(state_->mutex);
    if (state_->live.load(std::memory_order_acquire)) --state_->connections;
}

UserPublication UserRegistry::PreparePublication(std::shared_ptr<const Users> next) const {
    std::shared_ptr<const Users> before;
    std::shared_ptr<const UserAdmissionMap> old;
    {
        std::lock_guard lock(published_mutex_);
        before = published_;
        old = admissions_;
    }
    auto entries = std::make_shared<UserAdmissionMap>();
    UserPublication result;
    result.users = std::move(next);
    result.admissions = entries;
    for (const auto& [name, user] : result.users->users) {
        if (user.statements_per_sec > kMaxUserStatementsPerSecond)
            throw std::invalid_argument("statements_per_sec exceeds the admission history bound");
        const auto existing = old ? old->find(name) : UserAdmissionMap::const_iterator{};
        if (!old || existing == old->end()) {
            entries->emplace(name, UserAdmissionEntry{std::make_shared<UserAdmissionState>(NewIncarnation(), user, admission_clock_)});
            continue;
        }
        auto state = existing->second.state;
        auto entry = existing->second;
        const auto& previous = before->users.at(name);
        if (user.verifier == previous.verifier && user.max_connections == previous.max_connections &&
            user.statements_per_sec == previous.statements_per_sec) {
            entries->emplace(name, entry);
            continue;
        }
        UserAdmissionUpdate update{.state = state};
        {
            std::lock_guard lock(state->mutex);
            update.verifier_generation = entry.verifier_generation;
            if (user.verifier != previous.verifier) {
                if (update.verifier_generation == std::numeric_limits<std::uint64_t>::max())
                    throw std::runtime_error("user verifier generation space exhausted");
                ++update.verifier_generation;
            }
            if (user.statements_per_sec > state->history.size()) {
#ifdef CHUNKDB_FAILPOINTS
                if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_USER_ADMISSION_HISTORY_ALLOC_FAIL_ONCE")) throw std::bad_alloc();
#endif
                update.grown_history.resize(static_cast<std::size_t>(user.statements_per_sec));
            }
        }
        update.max_connections = user.max_connections;
        update.statements_per_sec = user.statements_per_sec;
        entry.verifier_generation = update.verifier_generation;
        entries->emplace(name, std::move(entry));
        result.updates.push_back(std::move(update));
    }
    if (old) for (const auto& [name, entry] : *old) {
        if (!result.users->users.contains(name)) result.updates.push_back(UserAdmissionUpdate{.state = entry.state, .retire = true});
    }
    return result;
}

void UserRegistry::Publish(UserPublication next) {
    for (auto& update : next.updates) {
        auto& state = *update.state;
        std::lock_guard lock(state.mutex);
        if (update.retire) {
            state.live.store(false, std::memory_order_release);
            state.connections = 0U;
            state.history.swap(update.grown_history);
            state.history_head = state.history_size = 0U;
            state.statements_per_sec.store(0U, std::memory_order_release);
            continue;
        }
        if (!update.grown_history.empty()) {
            for (std::size_t i = 0U; i < state.history_size; ++i)
                update.grown_history[i] = state.history[(state.history_head + i) % state.history.size()];
            state.history.swap(update.grown_history);
            state.history_head = 0U;
        }
        state.verifier_generation = update.verifier_generation;
        state.max_connections = update.max_connections;
        state.statements_per_sec.store(update.statements_per_sec, std::memory_order_release);
    }
    {
        std::lock_guard lock(published_mutex_);
        published_.swap(next.users);
        admissions_.swap(next.admissions);
    }
}

std::optional<UserAuthenticationSnapshot> UserRegistry::AuthenticationSnapshot(const std::string& name) const {
    std::shared_ptr<const Users> users;
    std::shared_ptr<UserAdmissionState> state;
    std::uint64_t generation = 0U;
    {
        std::lock_guard lock(published_mutex_);
        const auto at = admissions_->find(name);
        if (at == admissions_->end()) return std::nullopt;
        state = at->second.state;
        generation = at->second.verifier_generation;
        users = published_;
    }
    return UserAuthenticationSnapshot{users->users.at(name), state, generation};
}

UserAdmissionResult UserRegistry::AcquireConnection(const UserAuthenticationSnapshot& expected) const {
    auto ticket = std::make_shared<UserConnectionTicket>(expected.state);
    std::lock_guard lock(expected.state->mutex);
    auto& state = *expected.state;
    if (!state.live.load(std::memory_order_acquire) || state.verifier_generation != expected.verifier_generation)
        return {.status = UserAdmissionStatus::kStale};
    if ((state.max_connections != 0U && state.connections >= state.max_connections) ||
        state.connections == std::numeric_limits<std::uint64_t>::max())
        return {.status = UserAdmissionStatus::kConnectionLimit, .retry_after_ms = 1000U};
    ++state.connections;
    ticket->admitted_ = true;
    return {.status = UserAdmissionStatus::kAllowed, .ticket = std::move(ticket)};
}

bool UserRegistry::AuthenticationCurrent(const UserAuthenticationSnapshot& expected) const {
    std::lock_guard lock(expected.state->mutex);
    return expected.state->live.load(std::memory_order_acquire) &&
        expected.state->verifier_generation == expected.verifier_generation;
}

std::optional<User> UserRegistry::FindForConnection(const std::string& name,
    const std::shared_ptr<UserConnectionTicket>& ticket) const {
    if (!ticket || !ticket->state_->live.load(std::memory_order_acquire)) return std::nullopt;
    std::shared_ptr<const Users> users;
    {
        std::lock_guard lock(published_mutex_);
        const auto state = admissions_->find(name);
        if (state == admissions_->end() || state->second.state != ticket->state_) return std::nullopt;
        users = published_;
    }
    return users->users.at(name);
}

UserAdmissionResult UserRegistry::AdmitStatement(const std::shared_ptr<UserConnectionTicket>& ticket) const {
    if (!ticket || !ticket->state_->live.load(std::memory_order_acquire))
        return {.status = UserAdmissionStatus::kStale};
    auto& state = *ticket->state_;
    if (state.statements_per_sec.load(std::memory_order_acquire) == 0U) return {};
    std::lock_guard lock(state.mutex);
    if (!state.live.load(std::memory_order_acquire)) return {.status = UserAdmissionStatus::kStale};
    const auto rate = state.statements_per_sec.load(std::memory_order_relaxed);
    if (rate == 0U) return {};
    const auto now = state.clock ? state.clock() : std::chrono::steady_clock::now();
    constexpr auto window = std::chrono::seconds(1);
    while (state.history_size != 0U && now - state.history[state.history_head] >= window) {
        state.history_head = (state.history_head + 1U) % state.history.size();
        --state.history_size;
    }
    if (state.history_size >= rate) {
        const auto index = (state.history_head + state.history_size - static_cast<std::size_t>(rate)) % state.history.size();
        const auto remaining = window - (now - state.history[index]);
        const auto retry = std::chrono::ceil<std::chrono::milliseconds>(remaining).count();
        return {.status = UserAdmissionStatus::kRateLimit, .retry_after_ms = static_cast<std::uint64_t>(retry)};
    }
    state.history[(state.history_head + state.history_size) % state.history.size()] = now;
    ++state.history_size;
    return {};
}
}  // namespace chunkdb
