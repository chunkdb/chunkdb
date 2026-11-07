#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/change_feed.hpp"
#include "chunkdb/metrics.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

// The wire protocol this engine speaks: CQL statements with typed replies
// (docs/PROTOCOL.md).
inline constexpr int kProtocolVersion = 3;

// docs/design/USERS_DESIGN.md; defined in src/.
class UserRegistry;
class AuditLog;
class SlotWatch;
struct User;
class ReadRightCheck;
struct PendingLogin;
enum class Right : std::uint8_t;

// Deterministic command interleavings, following the feed hooks.
struct CommandEngineTestHook {
    enum class Point { kAfterSlotTablesListed, kBeforeSlotTableList, kAfterBackupAborted, kBeforeTxnBudgetReclaim };
    virtual ~CommandEngineTestHook() = default;
    virtual void Run(Point point, std::string_view table) = 0;
};

// A statement the logged-in user has no right to run: PERMISSION_DENIED.
class PermissionDeniedError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// The user a connection logged in as was dropped, perhaps created again:
// AUTH_REQUIRED, and the connection closes.
class UserDroppedError : public std::runtime_error {
  public:
    UserDroppedError() : std::runtime_error("the user of this connection was dropped; log in again") {}
};

struct EngineConfig {
    // Logins need a user and password of `users` (SCRAM-SHA-256). When false
    // (--auth none, for local development), HELLO 3 logs in without a user,
    // with every right.
    bool require_auth = true;
    // BACKUP destinations are relative to this directory; empty disables BACKUP.
    std::filesystem::path backup_dir{};
    std::shared_ptr<UserRegistry> users{};
    // Failed logins that close a connection; failed logins from one source
    // address that ban it (0: no bans), and for how long.
    std::size_t max_auth_failures = 5;
    std::size_t max_auth_failures_per_ip = 5;
    std::size_t auth_failure_delay_ms = 50;
    std::size_t auth_failure_ban_ms = 1000;
    // Logins and administrative statements, when set (--audit-log).
    std::shared_ptr<AuditLog> audit{};
    // Statements slower than this many ms are logged at WARN; 0: none.
    std::size_t slow_statement_ms = 0;
    // Reported by HELLO.
    std::string server_version = "unknown";
    std::size_t max_line_bytes = 65536;
    // Transactions (docs/design/TRANSACTIONS_DESIGN.md): how long one may stay
    // open, and the bytes of the private chunk copies of one and of all.
    std::chrono::milliseconds txn_max_duration{5000};
    std::size_t txn_max_bytes = 16ULL * 1024ULL * 1024ULL;
    std::size_t txn_total_bytes = 256ULL * 1024ULL * 1024ULL;
};

// What the audit and slow-statement logs say about a statement; the engine
// fills it only while either log is enabled.
struct StatementLogInfo {
    // The statement's verb, e.g. "SET BLOCK"; null until it parses.
    const char* verb = nullptr;
    // The audit event, null for statements the audit log leaves out.
    const char* audit_event = nullptr;
    std::string table;
    std::string target_user;
    std::string slot;
    std::string migration;
    // The chunks the statement read or wrote, where the engine knows.
    std::optional<std::size_t> chunks;
};

// Orders chunk coordinates (x, then y, then z).
struct ChunkCoordLess {
    bool operator()(const ChunkCoord& lhs, const ChunkCoord& rhs) const noexcept {
        return lhs < rhs;
    }
};

struct SessionTransaction;

// An engine's open transactions and the bytes of their private copies.
struct TxnBudget {
    // The bytes of the private copies of all open transactions
    // (--txn-total-bytes).
    std::atomic<std::size_t> bytes{0};
    // Guards `open` and each open transaction's `in_statement`.
    std::mutex mutex;
    std::list<SessionTransaction*> open;
};

// A connection's open transaction (docs/design/TRANSACTIONS_DESIGN.md).
// While `in_statement` is set only its session's statement uses it; while
// it is clear, the engine may end it under the budget's mutex once it is
// past --txn-max-duration-ms, so its bytes do not wait for the client.
struct SessionTransaction {
    // Registers the transaction in `budget`.
    SessionTransaction(TxnBudget* budget, std::chrono::steady_clock::time_point started);
    SessionTransaction(const SessionTransaction&) = delete;
    SessionTransaction& operator=(const SessionTransaction&) = delete;
    // Unregisters it and releases its bytes.
    ~SessionTransaction();

    TxnBudget* budget;
    // This transaction in budget->open.
    std::list<SessionTransaction*>::iterator entry{};
    // A statement of the session is using the transaction; guarded by
    // budget->mutex.
    bool in_statement = false;
    std::chrono::steady_clock::time_point started;
    // The table of the first statement and the snapshot it took; empty
    // before.
    std::string table{};
    std::unique_ptr<TxnSnapshot> snapshot{};
    // The chunks read, and the private copies of the chunks written.
    std::set<ChunkCoord, ChunkCoordLess> read_set{};
    std::map<ChunkCoord, ChunkState, ChunkCoordLess> writes{};
    // The bytes of `writes`, also counted in budget->bytes.
    std::size_t bytes = 0;
    // Set when a statement got CONFLICT: the snapshot and the writes are
    // gone, and every statement but COMMIT and ROLLBACK answers this reply,
    // so a statement sent after the conflict never runs outside the
    // transaction.
    std::optional<std::string> aborted{};

    // What Detach takes out of the transaction.
    struct Released {
        std::unique_ptr<TxnSnapshot> snapshot;
        std::map<ChunkCoord, ChunkState, ChunkCoordLess> writes;
        std::set<ChunkCoord, ChunkCoordLess> read_set;
    };

    // Ends the transaction, keeping `reply` as the answer and releasing its
    // bytes; the snapshot and the copies are returned, so a caller holding a
    // lock can destroy them after releasing it.
    [[nodiscard]] Released Detach(std::string reply) {
        Released released{std::move(snapshot), std::exchange(writes, {}), std::exchange(read_set, {})};
        budget->bytes.fetch_sub(bytes);
        bytes = 0;
        aborted = std::move(reply);
        return released;
    }
    // Drops the snapshot and the writes, keeping `reply` as the answer.
    void Abort(std::string reply) { (void)Detach(std::move(reply)); }
};

struct SessionHelloOptions {
    std::optional<std::string> client_name;
    std::optional<std::uint64_t> statement_timeout_ms;
    std::optional<std::string> default_tag;
    std::vector<std::string> unknown_options;
};

// A connection id unique within the process, from one counter for the server
// and the engine.
[[nodiscard]] std::uint64_t NewConnectionId();

struct SessionState {
    // Identifies the connection in HELLO replies and the audit log; unique
    // within the process (NewConnectionId).
    std::uint64_t connection_id = 0U;
    std::size_t idle_timeout_ms = 60000U;
    std::size_t max_watches = 64U;
    SessionHelloOptions hello_options;
    std::unique_ptr<FeedSubscription> watch;
    std::shared_ptr<SlotWatch> slot_watch;
    std::function<bool(std::shared_ptr<SlotWatch>)> register_slot_watch;
    FeedOptions watch_options;
    // Server shutdown interrupts backup work, independent of client lifetime.
    std::stop_token backup_cancelled;
    std::string remote_address;
    bool authenticated = false;
    std::size_t failed_auth_attempts = 0;
    bool close_after_reply = false;
    // HELLO succeeded; every other command needs it.
    bool greeted = false;
    // The logged-in user; empty when logins need no user.
    std::string user;
    // The incarnation of that user at login (UserRegistry::FindIncarnation).
    std::uint64_t user_incarnation = 0;
    // That user as of `user_generation` of the registry; null for good once
    // dropped, even when a user of that name is created again.
    std::shared_ptr<const User> user_rights;
    std::uint64_t user_generation = 0;
    // The registry generation at which the published rights view last
    // showed that user, at that incarnation.
    std::uint64_t user_present_generation = 0;
    // READ of that user on the table of the open watch, which the feed checks
    // while it streams; null without users.
    std::shared_ptr<ReadRightCheck> watch_right;
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
    // value of (kParameters), or to drain bounded opaque frames before
    // sending reject_response (kDiscard).
    enum class PayloadPlan { kNone, kParameters, kDiscard };
    struct PayloadRequest {
        PayloadPlan plan = PayloadPlan::kNone;
        std::vector<std::size_t> parameter_limits{};
        std::string reject_response{};
        std::size_t frame_count = 0U;
        // Unknown HELLO option values are bounded and discarded, but their
        // placeholder still participates in numeric parameter indexing.
        std::vector<bool> opaque_parameters{};
        // An oversized known HELLO option is drained, then rejected without
        // counting it as a failed authentication attempt.
        std::vector<std::string> parameter_rejections{};
        // SET BLOCK bounds actual frame lengths together. Other forms keep
        // their existing per-frame bound (a chunk may hold many blocks).
        std::size_t parameter_bytes_limit = std::numeric_limits<std::size_t>::max();
    };
    [[nodiscard]] PayloadRequest PlanPayload(SessionState& session, std::string_view line) const;

    // `parameters` are the frames kParameters asked for, std::nullopt for
    // `$-1`.
    [[nodiscard]] std::string Execute(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters = {});
    [[nodiscard]] std::string RejectPayload(SessionState& session, std::string_view line, std::string response);
    // The error that ends a watch whose user lost READ on `table`: NO_TABLE,
    // as a statement on a table the user cannot see gets, or AUTH_REQUIRED
    // once the user was dropped.
    [[nodiscard]] static std::string LostWatchReply(bool user_dropped, const std::string& table);
    [[nodiscard]] const std::shared_ptr<MetricsRegistry>& metrics() const noexcept {
        return metrics_;
    }
    // The users of the data directory; null without users.
    [[nodiscard]] const std::shared_ptr<UserRegistry>& users() const noexcept { return config_.users; }
    // Test-only visibility into the bounded auth-failure tracking table.
    [[nodiscard]] std::size_t AuthFailureTrackedSourcesForTests();
    void SetHookForTests(CommandEngineTestHook* hook) noexcept {
        hook_.store(hook, std::memory_order_release);
    }
    // What RELOAD TLS runs: the server's reload, which throws when the new
    // certificate or key is refused. Empty (the default, and without TLS):
    // RELOAD TLS is refused. Setting it waits for a reload in progress.
    void SetTlsReload(std::function<void()> reload) {
        std::lock_guard lock(tls_reload_mutex_);
        tls_reload_ = std::move(reload);
    }

  private:
    struct IpAuthFailureState {
        std::size_t failures = 0;
        // The part of `failures` per attempted user name; a login clears only
        // its own name's part. Failures of other names stay counted.
        std::vector<std::pair<std::string, std::size_t>> failures_by_user;
        std::chrono::steady_clock::time_point banned_until{};
        std::chrono::steady_clock::time_point last_update{};
    };

    EngineConfig config_;
    std::shared_ptr<TableCatalog> catalog_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::mutex auth_failures_mutex_;
    std::unordered_map<std::string, IpAuthFailureState> auth_failures_by_ip_;
    // The open transactions and their bytes.
    TxnBudget txn_budget_;
    std::atomic<CommandEngineTestHook*> hook_{nullptr};
    std::mutex tls_reload_mutex_;
    std::function<void()> tls_reload_;

    // The error reply for an exception a command threw.
    [[nodiscard]] static std::string ErrorReply(const std::exception& error);
    // A protocol 3 statement (engine_cql.cpp); sets `command_class` for the
    // metrics once the statement is known, and fills `info` when not null.
    [[nodiscard]] std::string ExecuteStatement(
        SessionState& session,
        std::string_view line,
        std::span<const std::optional<std::string>> parameters,
        MetricsRegistry::CommandClass& command_class,
        StatementLogInfo* info);
    // Writes the audit and slow-statement lines of a finished statement.
    void LogStatement(
        const SessionState& session,
        const StatementLogInfo& info,
        std::chrono::steady_clock::time_point started,
        const std::string& response) const;
    // Writes the audit line of a login attempt that ended with `response`.
    void AuditLogin(const SessionState& session, std::string_view user, bool known_user, bool succeeded,
                    const std::string& response) const;
    [[nodiscard]] PayloadRequest PlanParameters(SessionState& session, std::string_view line) const;
    // AUTH_REQUIRED for a session whose user was dropped, closing it after
    // the reply; empty while the user exists.
    [[nodiscard]] std::string LostUserReply(SessionState& session) const;
    // LostUserReply, recorded in the metrics and logs as a refused statement.
    [[nodiscard]] std::string AnswerLostUser(
        SessionState& session, std::string_view line, std::chrono::steady_clock::time_point started);
    // What the logs say about `line` (engine_cql.cpp); nothing when it does
    // not parse.
    static void DescribeLine(std::string_view line, StatementLogInfo& info);
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
    [[nodiscard]] std::string HelloReply(const SessionState& session, std::string_view server_signature) const;
    // The per-source failure tracking of logins: the reply for a banned
    // source (empty when not banned), a failed login (AUTH_FAILED), and a
    // successful one.
    [[nodiscard]] std::string AuthBanReply(SessionState& session);
    [[nodiscard]] std::string RecordAuthFailure(SessionState& session, std::string_view user);
    void RecordAuthSuccess(SessionState& session, std::string_view user);

    // Rights (engine_cql.cpp, docs/design/USERS_DESIGN.md). Each throws
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

    // Transactions (engine_cql.cpp, docs/design/TRANSACTIONS_DESIGN.md).
    [[nodiscard]] std::string TxnBegin(SessionState& session);
    [[nodiscard]] std::string TxnCommit(SessionState& session);
    // The conflict a transaction past --txn-max-duration-ms ends with.
    [[nodiscard]] TransactionConflictError TxnDurationConflict() const;
    // Whether the transaction is --txn-max-duration-ms old.
    [[nodiscard]] bool TxnPastDuration(
        const SessionTransaction& txn, std::chrono::steady_clock::time_point now) const;
    // Whether it holds a snapshot and private copies that ending it releases;
    // read under the budget's mutex unless its statement is running.
    [[nodiscard]] static bool TxnHoldsBudget(const SessionTransaction& txn);
    // Marks the session's transaction, if any, as used by the statement
    // starting, after ending it when it holds a snapshot and is past
    // --txn-max-duration-ms; returns whether there was one.
    // TxnStatementDone clears the mark when the statement left it open.
    [[nodiscard]] bool TxnStatementStart(SessionState& session);
    void TxnStatementDone(SessionState& session, bool started) noexcept;
    // Ends every transaction past --txn-max-duration-ms that holds a snapshot
    // and that no statement is using, releasing its private copies and
    // snapshot; its next statement answers CONFLICT reason=duration.
    void EndExpiredTransactions();
    // The table's lease for a statement inside the transaction. The first
    // statement binds the transaction to its table and takes the snapshot;
    // a later one on another table is refused, and a table altered or
    // dropped since ends the transaction (TransactionConflictError).
    [[nodiscard]] Table::Lease TxnLease(SessionState& session, const std::string& table);
    // Adds chunks to the transaction's read set, or throws past its limit
    // leaving it as it was.
    static void TxnRead(SessionTransaction& txn, std::span<const ChunkCoord> coords);
    // Throws as TxnWrite would for a change that leaves the chunk's private
    // copy at `after` bytes, before that change is built or the chunk read.
    void RequireTxnRoom(const SessionTransaction& txn, const ChunkCoord& coord, std::size_t after);
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
