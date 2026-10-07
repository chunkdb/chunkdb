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

// The wire protocol this engine speaks (docs/PROTOCOL.md).
inline constexpr int kProtocolVersion = 2;

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
    // The table this connection works on, selected by HELLO (`default`, or
    // the one TABLE names) and changed only by USE; null when HELLO found no
    // `default`. Kept after a drop, so the connection gets NO_TABLE instead
    // of silently reaching a new table of that name.
    std::shared_ptr<Table> table;
};

class CommandEngine {
  public:
    CommandEngine(
        EngineConfig config,
        std::shared_ptr<TableCatalog> catalog,
        std::shared_ptr<MetricsRegistry> metrics = nullptr);

    // Commands that carry a raw payload after the request line (CHUNKPUT) are
    // read in two phases. PlanPayload inspects the request line and tells
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
    // Checks `token` with the per-source failure tracking; an empty string
    // on success, the error reply otherwise.
    [[nodiscard]] std::string Authenticate(SessionState& session, std::string_view token);
    [[nodiscard]] std::string HandleHello(SessionState& session, std::string_view line);
    [[nodiscard]] std::string HandleGet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleSet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleUnset(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkExists(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkGet(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkPut(
        ChunkStore& store,
        const ParsedCommandView& command,
        std::string_view payload);
    [[nodiscard]] std::string HandleInfo(const Table& table, ChunkStore& store) const;
    [[nodiscard]] std::string HandleMSet(ChunkStore& store, std::string_view line);
    [[nodiscard]] std::string HandleMGet(ChunkStore& store, std::string_view line);
    [[nodiscard]] std::string HandleChunkScan(ChunkStore& store, const ParsedCommandView& command);
    // CHUNKRANGE (radius false) and CHUNKRADIUS.
    [[nodiscard]] std::string HandleChunkArea(
        ChunkStore& store,
        const ParsedCommandView& command,
        bool radius);
    [[nodiscard]] std::string HandleChunkVersion(ChunkStore& store, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleChunkBatch(ChunkStore& store, std::string_view line);
    [[nodiscard]] static std::size_t ParsePayloadLength(std::string_view token);

    // The `[STATE] [ZRLE]` options of CHUNKGET, CHUNKPUT and the area reads.
    struct ChunkForm {
        bool state = false;
        bool zrle = false;
    };
    // Parses command.args[begin, end) as chunk options, each at most once.
    [[nodiscard]] static ChunkForm ParseChunkForm(
        const ParsedCommandView& command,
        std::size_t begin,
        std::size_t end,
        std::string_view command_name);
    // CHUNKPUT <cx> <cy> [STATE] [ZRLE] [IF <version>] <length>
    struct ChunkPutRequest {
        std::int64_t chunk_x = 0;
        std::int64_t chunk_y = 0;
        bool state = false;
        bool zrle = false;
        bool has_if = false;
        std::uint64_t if_version = 0;
        std::size_t length = 0;
    };
    [[nodiscard]] static ChunkPutRequest ParseChunkPut(const ParsedCommandView& command);
    // Records a reply in the command metrics.
    void ObserveReply(
        std::string_view command_name,
        std::chrono::steady_clock::time_point started,
        const std::string& response);
    [[nodiscard]] std::string HandleWalFlush(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleMetrics() const;
    [[nodiscard]] std::string HandleTables(const ParsedCommandView& command) const;
    [[nodiscard]] std::string HandleTableInfo(const ParsedCommandView& command) const;
    [[nodiscard]] std::string HandleUse(SessionState& session, const ParsedCommandView& command);
    [[nodiscard]] std::string HandleTableCreate(std::string_view line);
    [[nodiscard]] std::string HandleTableDrop(const ParsedCommandView& command);
    [[nodiscard]] std::string HandleTableSet(std::string_view line);
    // The selected table, leased for one command. Throws TableNotFoundError
    // when none is selected or it was dropped.
    [[nodiscard]] Table::Lease AcquireTable(SessionState& session) const;

    static std::int64_t ParseInt64(std::string_view token);
    static std::uint64_t ParseUint64(std::string_view token);
    [[nodiscard]] bool IsAuthRequired() const noexcept;
};

}  // namespace chunkdb
