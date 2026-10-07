#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/metrics.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

struct EngineConfig {
    std::string auth_token;
    bool require_auth = true;
    std::size_t max_auth_failures = 5;
    std::size_t max_auth_failures_per_ip = 5;
    std::size_t auth_failure_delay_ms = 50;
    std::size_t auth_failure_ban_ms = 1000;
};

struct SessionState {
    std::string remote_address;
    bool authenticated = false;
    std::size_t failed_auth_attempts = 0;
    bool close_after_reply = false;
    // The table this connection works on: `default` until USE selects
    // another (bound on first use). Kept after a drop, so the connection
    // gets NO_TABLE instead of silently reaching a new table of that name.
    std::shared_ptr<Table> table;
};

class CommandEngine {
  public:
    CommandEngine(
        EngineConfig config,
        std::shared_ptr<TableCatalog> catalog,
        std::shared_ptr<MetricsRegistry> metrics = nullptr);

    // Commands that carry a raw payload after the request line (CHUNKSETBIN)
    // are read in two phases. PlanPayload inspects the request line and tells
    // the connection whether to read `bytes` of payload before executing, or
    // to send `reject_response` and close because the declared length cannot
    // be trusted (unauthenticated session, malformed header, or a length
    // above what the configured geometry can ever need).
    enum class PayloadPlan { kNone, kRead, kReject };
    struct PayloadRequest {
        PayloadPlan plan = PayloadPlan::kNone;
        std::size_t bytes = 0;
        std::string reject_response;
    };
    [[nodiscard]] PayloadRequest PlanPayload(SessionState& session, std::string_view line) const;

    // `payload` is the raw bytes read according to PlanPayload; empty for
    // line-only commands.
    [[nodiscard]] std::string Execute(
        SessionState& session,
        std::string_view line,
        std::string_view payload = {});
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

    [[nodiscard]] std::string ExecuteInternal(
        SessionState& session,
        std::string_view line,
        std::string_view command_name,
        std::string_view payload);
    [[nodiscard]] std::string HandleAuth(SessionState& session, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleExists(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleGet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleSet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleUnset(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkExists(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunk(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkSet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkSetBinary(
        ChunkStore& store,
        const ParsedCommandView& command,
        std::string_view payload);
    [[nodiscard]] static std::size_t ParsePayloadLength(std::string_view token);
    [[nodiscard]] std::string HandleChunkBinary(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkBinaryCompressed(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleInfo(const Table& table, ChunkStore& store) const;
    [[nodiscard]] std::string HandleMSet(ChunkStore& store, std::string_view line);
    [[nodiscard]] std::string HandleMGet(ChunkStore& store, std::string_view line);
    [[nodiscard]] std::string HandleChunkScan(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkRange(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkRadius(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkVersion(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkCas(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkBatch(ChunkStore& store, std::string_view line);
    [[nodiscard]] std::string HandleWalFlush(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleMetrics() const;
    [[nodiscard]] std::string HandleTables(const ParsedCommandView& command) const;
    [[nodiscard]] std::string HandleTableInfo(const ParsedCommandView& command) const;
    [[nodiscard]] std::string HandleUse(SessionState& session, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleTableCreate(std::string_view line);
    [[nodiscard]] std::string HandleTableDrop(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleTableSet(std::string_view line);
    // The selected table (binding `default` first), leased for one command.
    // Throws TableNotFoundError when it does not exist or was dropped.
    [[nodiscard]] Table::Lease AcquireTable(SessionState& session) const;
    // Binds `default` when no table was selected yet; nullptr when absent.
    [[nodiscard]] const std::shared_ptr<Table>& SelectedTable(SessionState& session) const;

    static std::int64_t ParseInt64(std::string_view token);
    static std::uint64_t ParseUint64(std::string_view token);
    [[nodiscard]] bool IsAuthRequired() const noexcept;
};

}  // namespace chunkdb
