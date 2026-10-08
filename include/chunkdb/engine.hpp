#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
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

struct EngineConfig {
    std::string auth_token;
    bool require_auth = true;
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
    [[nodiscard]] PayloadRequest PlanParameters(std::string_view line) const;
    // The table a statement names, leased for it; the session keeps the
    // table so the next statement on it skips the catalog.
    [[nodiscard]] Table::Lease AcquireNamedTable(SessionState& session, std::string_view name) const;
    // Checks `token` with the per-source failure tracking; an empty string
    // on success, the error reply otherwise.
    [[nodiscard]] std::string Authenticate(SessionState& session, std::string_view token);
    [[nodiscard]] std::string HandleHello(SessionState& session, std::string_view line);
    // Records a reply in the command metrics.
    void ObserveReply(
        MetricsRegistry::CommandClass command_class,
        std::chrono::steady_clock::time_point started,
        const std::string& response);
    [[nodiscard]] std::string HandleMetrics() const;
    [[nodiscard]] bool IsAuthRequired() const noexcept;
};

}  // namespace chunkdb
