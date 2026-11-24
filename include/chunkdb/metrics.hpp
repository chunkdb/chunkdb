#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// The store counters of one table, rendered with its name as the `table`
// label.
struct TableRuntimeStats {
    std::string table;
    StoreRuntimeStats stats{};
};

// Thread-safe runtime metrics with bounded label cardinality: every label
// value comes from the fixed enums below or names a table. Rendered in the
// Prometheus text exposition format; durations are reported in seconds.
class MetricsRegistry {
  public:
    enum class CommandClass : std::size_t {
        kAuth = 0,
        kPointRead,
        kPointWrite,
        kChunkRead,
        kChunkWrite,
        kScan,
        kRange,
        kConditional,
        kBarrier,
        kAdmin,
        kOther,
        kCount,
    };

    enum class ErrorClass : std::size_t {
        kInvalidArgument = 0,
        kOutOfRange,
        kAuthRequired,
        kAuthFailed,
        kUnknownCommand,
        kVersionMismatch,
        kInternal,
        kOther,
        kCount,
    };

    static constexpr std::size_t kBucketCount = 14;
    // Upper bounds in seconds; the final implicit bucket is +Inf.
    static constexpr std::array<double, kBucketCount> kBucketBoundsSeconds = {
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
        0.025,  0.05,    0.1,    0.25,  0.5,    1.0,   2.5,
    };

    void ObserveCommand(CommandClass command_class, double seconds, bool ok) noexcept;
    void CountError(ErrorClass error_class) noexcept;
    void IncAuthFailure() noexcept;

    // Connection lifecycle gauges and server-side failure counters for
    // outcomes that never reach CommandEngine::Execute (connection accept,
    // request framing, and admission control).
    void IncActiveConnections() noexcept;
    void DecActiveConnections() noexcept;
    void SetPendingConnections(std::size_t value) noexcept;
    void CountConnectionRejected() noexcept;
    void CountMalformedRequest() noexcept;

    [[nodiscard]] static ErrorClass ClassifyErrorCode(std::string_view error_code) noexcept;

    // A table as the table state gauges report it.
    struct TableState {
        std::string table;
        // Fenced (TableCatalog::Fences), also when out of service.
        bool fenced = false;
        // Table::SlotFrontierFrozen; std::nullopt for a table that is not
        // serving at that moment.
        std::optional<bool> slot_frontier_frozen;
    };

    // chunkdb_table_fenced for each of `tables` and chunkdb_slot_frontier_frozen
    // for each with a known frontier, labelled `table`, in the order of `tables`.
    [[nodiscard]] static std::string RenderTableStates(std::span<const TableState> tables);

    // `value` as a label value: backslash, double quote and line feed escaped.
    [[nodiscard]] static std::string EscapeLabelValue(std::string_view value);

    // Store counters come as one sample per table, labelled `table`, in the
    // order of `tables`.
    [[nodiscard]] std::string RenderPrometheus(
        std::span<const TableRuntimeStats> tables,
        std::size_t loaded_chunks, const StoreResources* resources = nullptr) const;

  private:
    struct Histogram {
        std::array<std::atomic<std::uint64_t>, kBucketCount + 1> buckets{};
        std::atomic<std::uint64_t> count{0};
        std::atomic<std::uint64_t> sum_micros{0};
    };

    static constexpr std::size_t kCommandClassCount =
        static_cast<std::size_t>(CommandClass::kCount);
    static constexpr std::size_t kErrorClassCount = static_cast<std::size_t>(ErrorClass::kCount);

    std::array<Histogram, kCommandClassCount> latency_{};
    std::array<std::atomic<std::uint64_t>, kCommandClassCount> commands_ok_{};
    std::array<std::atomic<std::uint64_t>, kCommandClassCount> commands_error_{};
    std::array<std::atomic<std::uint64_t>, kErrorClassCount> errors_{};
    std::atomic<std::uint64_t> auth_failures_{0};
    std::atomic<std::int64_t> active_connections_{0};
    std::atomic<std::int64_t> pending_connections_{0};
    std::atomic<std::uint64_t> connections_rejected_{0};
    std::atomic<std::uint64_t> malformed_requests_{0};
};

}  // namespace chunkdb
