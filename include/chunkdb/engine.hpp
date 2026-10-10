#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/change_feed.hpp"
#include "chunkdb/metrics.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

// The wire protocol this engine speaks: CQL statements with typed replies
// (docs/PROTOCOL.md).
inline constexpr int kProtocolVersion = 3;

// docs/USERS_DESIGN.md; defined in src/.
class UserRegistry;
class SlotWatch;
struct User;
struct PendingLogin;
enum class Right : std::uint8_t;

// Deterministic interleavings for slot-listing tests, following the feed hooks.
struct CommandEngineTestHook {
    enum class Point { kAfterSlotTablesListed, kBeforeSlotTableList };
    virtual ~CommandEngineTestHook() = default;
    virtual void Run(Point point, std::string_view table) = 0;
};

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
    // Transactions (docs/TRANSACTIONS_DESIGN.md): how long one may stay
    // open, and the bytes of the private chunk copies of one and of all.
    std::chrono::milliseconds txn_max_duration{5000};
    std::size_t txn_max_bytes = 16ULL * 1024ULL * 1024ULL;
    std::size_t txn_total_bytes = 256ULL * 1024ULL * 1024ULL;
};

// Orders chunk coordinates (x, then y).
struct ChunkCoordLess {
    bool operator()(const ChunkCoord& lhs, const ChunkCoord& rhs) const noexcept {
        return lhs.x != rhs.x ? lhs.x < rhs.x : lhs.y < rhs.y;
    }
};

// A connection's open transaction (docs/TRANSACTIONS_DESIGN.md).
struct SessionTransaction {
    SessionTransaction(std::atomic<std::size_t>* total, std::chrono::steady_clock::time_point started)
        : total_bytes(total), started(started) {}
    SessionTransaction(const SessionTransaction&) = delete;
    SessionTransaction& operator=(const SessionTransaction&) = delete;
    ~SessionTransaction() { total_bytes->fetch_sub(bytes); }

    // The engine's count of the private copies' bytes of all transactions.
    std::atomic<std::size_t>* total_bytes;
    std::chrono::steady_clock::time_point started;
    // The table of the first statement and the snapshot it took; empty
    // before.
    std::string table{};
    std::unique_ptr<TxnSnapshot> snapshot{};
    // The chunks read, and the private copies of the chunks written.
    std::set<ChunkCoord, ChunkCoordLess> read_set{};
    std::map<ChunkCoord, ChunkState, ChunkCoordLess> writes{};
    // The bytes of `writes`, also counted in *total_bytes.
    std::size_t bytes = 0;
    // Set when a statement got CONFLICT: the snapshot and the writes are
    // gone, and every statement but COMMIT and ROLLBACK answers this reply,
    // so a statement sent after the conflict never runs outside the
    // transaction.
    std::optional<std::string> aborted{};

    // Drops the snapshot and the writes, keeping `reply` as the answer.
    void Abort(std::string reply) {
        snapshot.reset();
        writes.clear();
        read_set.clear();
        total_bytes->fetch_sub(bytes);
        bytes = 0;
        aborted = std::move(reply);
    }
};

struct SessionState {
    std::unique_ptr<FeedSubscription> watch;
    std::shared_ptr<SlotWatch> slot_watch;
    std::function<bool(std::shared_ptr<SlotWatch>)> register_slot_watch;
    FeedOptions watch_options;
    // Checked while a backup runs; empty for in-process statement callers.
    std::function<bool()> backup_cancelled;
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
    // Between BEGIN and COMMIT or ROLLBACK; a closed connection rolls it
    // back.
    std::unique_ptr<SessionTransaction> transaction;
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
    void SetHookForTests(CommandEngineTestHook* hook) noexcept {
        hook_.store(hook, std::memory_order_release);
    }

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
    // The bytes of the private chunk copies of all open transactions.
    std::atomic<std::size_t> txn_total_bytes_{0};
    std::atomic<CommandEngineTestHook*> hook_{nullptr};

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

    // Transactions (engine_cql.cpp, docs/TRANSACTIONS_DESIGN.md).
    [[nodiscard]] std::string TxnBegin(SessionState& session);
    [[nodiscard]] std::string TxnCommit(SessionState& session);
    // The table's lease for a statement inside the transaction. The first
    // statement binds the transaction to its table and takes the snapshot;
    // a later one on another table is refused, and a table altered or
    // dropped since ends the transaction (TransactionConflictError).
    [[nodiscard]] Table::Lease TxnLease(SessionState& session, const std::string& table);
    // Adds chunks to the transaction's read set, or throws past its limit
    // leaving it as it was.
    static void TxnRead(SessionTransaction& txn, std::span<const ChunkCoord> coords);
    // Applies `change` to the transaction's private copy of the chunk, made
    // from the snapshot when it has none. The copy is unchanged when
    // `change` or a limit throws.
    void TxnWrite(
        SessionTransaction& txn,
        ChunkStore& store,
        const ChunkCoord& coord,
        const std::function<void(ChunkState&)>& change);
};

}  // namespace chunkdb
