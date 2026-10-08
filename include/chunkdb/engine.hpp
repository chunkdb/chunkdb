#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/metrics.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

// The wire protocol this engine speaks: CQL statements with typed replies
// (docs/PROTOCOL.md).
inline constexpr int kProtocolVersion = 3;

// docs/USERS_DESIGN.md; defined in src/.
class UserRegistry;
struct User;
struct PendingLogin;
enum class Right : std::uint8_t;

// A statement the logged-in user has no right to run: PERMISSION_DENIED.
class PermissionDeniedError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

struct EngineConfig {
    // Logins need a user and password of `users` (SCRAM-SHA-256). When false
    // (--auth none, for local development), HELLO 3 logs in without a user,
    // with every right.
    bool require_auth = true;
    std::shared_ptr<UserRegistry> users{};
    std::size_t max_auth_failures = 5;
    std::size_t max_auth_failures_per_ip = 5;
    std::size_t auth_failure_delay_ms = 50;
    std::size_t auth_failure_ban_ms = 1000;
    // Reported by HELLO.
    std::string server_version = "unknown";
    std::size_t max_line_bytes = 65536;
};

struct SessionState {
    std::string remote_address;
    bool authenticated = false;
    std::size_t failed_auth_attempts = 0;
    bool close_after_reply = false;
    // HELLO succeeded; every other command needs it.
    bool greeted = false;
    // The logged-in user; empty when logins need no user.
    std::string user;
    // That user as of `user_generation` of the registry; null once dropped.
    std::shared_ptr<const User> user_rights;
    std::uint64_t user_generation = 0;
    // Between HELLO 3 USER and AUTH: the SCRAM exchange in progress.
    std::shared_ptr<PendingLogin> pending_login;
    // The table the last statement named, kept so the next statement on it
    // skips the catalog; a dropped table is looked up again.
    std::shared_ptr<Table> table;
};

class CommandEngine {
  public:
    CommandEngine(
        EngineConfig config,
        std::shared_ptr<TableCatalog> catalog,
        std::shared_ptr<MetricsRegistry> metrics = nullptr);

    // A statement with parameters ($1 ... $n) is followed by n frames
    // (`$<length>` or `$-1`, then the bytes). PlanPayload tells the
    // connection the most bytes each frame may hold, by the column it is a
    // value of (kParameters), or to send `reject_response` and close because
    // the frames cannot be bounded (kReject).
    enum class PayloadPlan { kNone, kParameters, kReject };
    struct PayloadRequest {
        PayloadPlan plan = PayloadPlan::kNone;
        std::vector<std::size_t> parameter_limits{};
        std::string reject_response;
    };
    [[nodiscard]] PayloadRequest PlanPayload(SessionState& session, std::string_view line) const;

    // `parameters` are the frames kParameters asked for, std::nullopt for
    // `$-1`.
    [[nodiscard]] std::string Execute(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters = {});
    [[nodiscard]] const std::shared_ptr<MetricsRegistry>& metrics() const noexcept {
        return metrics_;
    }
    // Test-only visibility into the bounded auth-failure tracking table.
    [[nodiscard]] std::size_t AuthFailureTrackedSourcesForTests();

  private:
    struct IpAuthFailureState {
        std::size_t failures = 0;
        std::chrono::steady_clock::time_point banned_until{};
        std::chrono::steady_clock::time_point last_update{};
    };

    EngineConfig config_;
    std::shared_ptr<TableCatalog> catalog_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::mutex auth_failures_mutex_;
    std::unordered_map<std::string, IpAuthFailureState> auth_failures_by_ip_;

    // The error reply for an exception a command threw.
    [[nodiscard]] static std::string ErrorReply(const std::exception& error);
    // A protocol 3 statement (engine_cql.cpp); sets `command_class` for the
    // metrics once the statement is known.
    [[nodiscard]] std::string ExecuteStatement(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters,
        MetricsRegistry::CommandClass& command_class);
    [[nodiscard]] PayloadRequest PlanParameters(SessionState& session, std::string_view line) const;
    // The table a statement names, leased for it; the session keeps the
    // table so the next statement on it skips the catalog.
    [[nodiscard]] Table::Lease AcquireNamedTable(SessionState& session, std::string_view name) const;
    // Login (engine.cpp): HELLO 3 [USER <name> $1], then AUTH $1.
    [[nodiscard]] std::string HandleHello(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters);
    [[nodiscard]] std::string HandleAuth(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters);
    // The HELLO map that ends a login; `server_signature` is the SCRAM
    // server-final message, empty without a user.
    [[nodiscard]] std::string HelloReply(std::string_view server_signature) const;
    // The per-source failure tracking of logins: the reply for a banned
    // source (empty when not banned), a failed login (AUTH_FAILED), and a
    // successful one.
    [[nodiscard]] std::string AuthBanReply(SessionState& session);
    [[nodiscard]] std::string RecordAuthFailure(SessionState& session);
    void RecordAuthSuccess(SessionState& session);

    // Rights (engine_cql.cpp, docs/USERS_DESIGN.md). Each throws
    // TableNotFoundError when the user has no right at all on the table, so
    // names do not leak, and PermissionDeniedError when the right is lower
    // than `needed`.
    [[nodiscard]] const User* CurrentUser(SessionState& session) const;
    [[nodiscard]] std::optional<Right> RightOnTable(SessionState& session, const std::string& table) const;
    void RequireRight(SessionState& session, const std::string& table, Right needed) const;
    void RequireRightOnEveryTable(SessionState& session, Right needed) const;
    void RequireManagesUsers(SessionState& session) const;
    // Records a reply in the command metrics.
    void ObserveReply(
        MetricsRegistry::CommandClass command_class,
        std::chrono::steady_clock::time_point started,
        const std::string& response);
    [[nodiscard]] std::string HandleMetrics() const;
};

}  // namespace chunkdb
