#include "chunkdb/metrics.hpp"

#include <cmath>
#include <string_view>
#include <vector>


namespace chunkdb {

namespace {

[[nodiscard]] const char* CommandClassName(MetricsRegistry::CommandClass command_class) noexcept {
    switch (command_class) {
        case MetricsRegistry::CommandClass::kAuth:
            return "auth";
        case MetricsRegistry::CommandClass::kPointRead:
            return "point_read";
        case MetricsRegistry::CommandClass::kPointWrite:
            return "point_write";
        case MetricsRegistry::CommandClass::kChunkRead:
            return "chunk_read";
        case MetricsRegistry::CommandClass::kChunkWrite:
            return "chunk_write";
        case MetricsRegistry::CommandClass::kScan:
            return "scan";
        case MetricsRegistry::CommandClass::kRange:
            return "range";
        case MetricsRegistry::CommandClass::kConditional:
            return "conditional";
        case MetricsRegistry::CommandClass::kBarrier:
            return "barrier";
        case MetricsRegistry::CommandClass::kAdmin:
            return "admin";
        case MetricsRegistry::CommandClass::kOther:
        case MetricsRegistry::CommandClass::kCount:
            break;
    }
    return "other";
}

[[nodiscard]] const char* ErrorClassName(MetricsRegistry::ErrorClass error_class) noexcept {
    switch (error_class) {
        case MetricsRegistry::ErrorClass::kInvalidArgument:
            return "invalid_argument";
        case MetricsRegistry::ErrorClass::kOutOfRange:
            return "out_of_range";
        case MetricsRegistry::ErrorClass::kAuthRequired:
            return "auth_required";
        case MetricsRegistry::ErrorClass::kAuthFailed:
            return "auth_failed";
        case MetricsRegistry::ErrorClass::kUnknownCommand:
            return "unknown_command";
        case MetricsRegistry::ErrorClass::kVersionMismatch:
            return "version_mismatch";
        case MetricsRegistry::ErrorClass::kInternal:
            return "internal";
        case MetricsRegistry::ErrorClass::kOther:
        case MetricsRegistry::ErrorClass::kCount:
            break;
    }
    return "other";
}

// `micros` as seconds with six decimals, from integers only: the C library's
// floating-point formatting follows the process locale, and Prometheus text
// needs a '.' separator.
[[nodiscard]] std::string FormatMicrosAsSeconds(std::uint64_t micros) {
    std::string fraction = std::to_string(micros % 1'000'000U);
    fraction.insert(0, 6U - fraction.size(), '0');
    return std::to_string(micros / 1'000'000U) + "." + fraction;
}

[[nodiscard]] std::string FormatBucketBound(double bound) {
    std::string text = FormatMicrosAsSeconds(static_cast<std::uint64_t>(std::llround(bound * 1e6)));
    while (!text.empty() && text.back() == '0') {
        text.pop_back();
    }
    if (!text.empty() && text.back() == '.') {
        text.pop_back();
    }
    return text;
}

void AppendCounter(
    std::string* out,
    std::string_view name,
    std::string_view labels,
    std::uint64_t value) {
    out->append(name);
    if (!labels.empty()) {
        out->append("{");
        out->append(labels);
        out->append("}");
    }
    out->append(" ");
    out->append(std::to_string(value));
    out->append("\n");
}

}  // namespace

void MetricsRegistry::ObserveCommand(
    CommandClass command_class,
    double seconds,
    bool ok) noexcept {
    const auto index = static_cast<std::size_t>(command_class);
    if (index >= kCommandClassCount) {
        return;
    }

    if (ok) {
        commands_ok_[index].fetch_add(1, std::memory_order_relaxed);
    } else {
        commands_error_[index].fetch_add(1, std::memory_order_relaxed);
    }

    auto& histogram = latency_[index];
    std::size_t bucket = kBucketCount;
    for (std::size_t i = 0; i < kBucketCount; ++i) {
        if (seconds <= kBucketBoundsSeconds[i]) {
            bucket = i;
            break;
        }
    }
    histogram.buckets[bucket].fetch_add(1, std::memory_order_relaxed);
    histogram.count.fetch_add(1, std::memory_order_relaxed);
    const double micros = seconds * 1e6;
    const auto micros_clamped =
        micros <= 0.0 ? std::uint64_t{0} : static_cast<std::uint64_t>(std::llround(micros));
    histogram.sum_micros.fetch_add(micros_clamped, std::memory_order_relaxed);
}

void MetricsRegistry::CountError(ErrorClass error_class) noexcept {
    const auto index = static_cast<std::size_t>(error_class);
    if (index >= kErrorClassCount) {
        return;
    }
    errors_[index].fetch_add(1, std::memory_order_relaxed);
}

void MetricsRegistry::IncAuthFailure() noexcept {
    auth_failures_.fetch_add(1, std::memory_order_relaxed);
}

void MetricsRegistry::IncActiveConnections() noexcept {
    active_connections_.fetch_add(1, std::memory_order_relaxed);
}

void MetricsRegistry::DecActiveConnections() noexcept {
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
}

void MetricsRegistry::SetPendingConnections(std::size_t value) noexcept {
    pending_connections_.store(static_cast<std::int64_t>(value), std::memory_order_relaxed);
}

void MetricsRegistry::CountConnectionRejected() noexcept {
    connections_rejected_.fetch_add(1, std::memory_order_relaxed);
}

void MetricsRegistry::CountMalformedRequest() noexcept {
    malformed_requests_.fetch_add(1, std::memory_order_relaxed);
}

MetricsRegistry::ErrorClass MetricsRegistry::ClassifyErrorCode(
    std::string_view error_code) noexcept {
    if (error_code == "INVALID_ARGUMENT") {
        return ErrorClass::kInvalidArgument;
    }
    if (error_code == "OUT_OF_RANGE") {
        return ErrorClass::kOutOfRange;
    }
    if (error_code == "AUTH_REQUIRED") {
        return ErrorClass::kAuthRequired;
    }
    if (error_code == "AUTH_FAILED") {
        return ErrorClass::kAuthFailed;
    }
    if (error_code == "UNKNOWN_COMMAND") {
        return ErrorClass::kUnknownCommand;
    }
    if (error_code == "VERSION_MISMATCH") {
        return ErrorClass::kVersionMismatch;
    }
    if (error_code == "INTERNAL") {
        return ErrorClass::kInternal;
    }
    return ErrorClass::kOther;
}

std::string MetricsRegistry::EscapeLabelValue(std::string_view value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char c : value) {
        if (c == '\\') {
            escaped += "\\\\";
        } else if (c == '"') {
            escaped += "\\\"";
        } else if (c == '\n') {
            escaped += "\\n";
        } else {
            escaped += c;
        }
    }
    return escaped;
}

std::string MetricsRegistry::RenderPrometheus(
    std::span<const TableRuntimeStats> tables,
    std::size_t loaded_chunks, const StoreResources* resources, std::span<const TableExpiryMetrics> expiry) const {
    std::string out;
    out.reserve(8192);

    out += "# HELP chunkdb_commands_total Commands processed, by command class and outcome.\n";
    out += "# TYPE chunkdb_commands_total counter\n";
    for (std::size_t i = 0; i < kCommandClassCount; ++i) {
        const auto* name = CommandClassName(static_cast<CommandClass>(i));
        AppendCounter(
            &out,
            "chunkdb_commands_total",
            std::string("class=\"") + name + "\",outcome=\"ok\"",
            commands_ok_[i].load(std::memory_order_relaxed));
        AppendCounter(
            &out,
            "chunkdb_commands_total",
            std::string("class=\"") + name + "\",outcome=\"error\"",
            commands_error_[i].load(std::memory_order_relaxed));
    }

    out += "# HELP chunkdb_errors_total Command errors, by error class.\n";
    out += "# TYPE chunkdb_errors_total counter\n";
    for (std::size_t i = 0; i < kErrorClassCount; ++i) {
        AppendCounter(
            &out,
            "chunkdb_errors_total",
            std::string("code=\"") + ErrorClassName(static_cast<ErrorClass>(i)) + "\"",
            errors_[i].load(std::memory_order_relaxed));
    }

    out += "# HELP chunkdb_auth_failures_total Failed authentication attempts.\n";
    out += "# TYPE chunkdb_auth_failures_total counter\n";
    AppendCounter(&out, "chunkdb_auth_failures_total", "", auth_failures_.load(std::memory_order_relaxed));

    out += "# HELP chunkdb_command_duration_seconds Command latency in seconds, by command class.\n";
    out += "# TYPE chunkdb_command_duration_seconds histogram\n";
    for (std::size_t i = 0; i < kCommandClassCount; ++i) {
        const auto* name = CommandClassName(static_cast<CommandClass>(i));
        const auto& histogram = latency_[i];
        std::uint64_t cumulative = 0;
        for (std::size_t bucket = 0; bucket < kBucketCount; ++bucket) {
            cumulative += histogram.buckets[bucket].load(std::memory_order_relaxed);
            AppendCounter(
                &out,
                "chunkdb_command_duration_seconds_bucket",
                std::string("class=\"") + name + "\",le=\"" +
                    FormatBucketBound(kBucketBoundsSeconds[bucket]) + "\"",
                cumulative);
        }
        cumulative += histogram.buckets[kBucketCount].load(std::memory_order_relaxed);
        AppendCounter(
            &out,
            "chunkdb_command_duration_seconds_bucket",
            std::string("class=\"") + name + "\",le=\"+Inf\"",
            cumulative);
        out += "chunkdb_command_duration_seconds_sum{class=\"";
        out += name;
        out += "\"} ";
        out += FormatMicrosAsSeconds(histogram.sum_micros.load(std::memory_order_relaxed));
        out += "\n";
        AppendCounter(
            &out,
            "chunkdb_command_duration_seconds_count",
            std::string("class=\"") + name + "\"",
            histogram.count.load(std::memory_order_relaxed));
    }

    out += "# HELP chunkdb_loaded_chunks Currently loaded (cached) chunks.\n";
    out += "# TYPE chunkdb_loaded_chunks gauge\n";
    AppendCounter(&out, "chunkdb_loaded_chunks", "", loaded_chunks);

    out += "# HELP chunkdb_large_chunk_containers Live, constructing and retired large-chunk containers.\n";
    out += "# TYPE chunkdb_large_chunk_containers gauge\n";
    AppendCounter(&out, "chunkdb_large_chunk_containers", "", resources ? resources->LargeChunkCount() : 0U);
    out += "# HELP chunkdb_large_chunk_container_limit Shared large-chunk container capacity.\n";
    out += "# TYPE chunkdb_large_chunk_container_limit gauge\n";
    AppendCounter(&out, "chunkdb_large_chunk_container_limit", "", resources ? resources->max_large_chunks() : 0U);
    out += "# HELP chunkdb_large_chunk_refusals_total New containers refused at capacity.\n";
    out += "# TYPE chunkdb_large_chunk_refusals_total counter\n";
    AppendCounter(&out, "chunkdb_large_chunk_refusals_total", "", resources ? resources->LargeChunkRefusals() : 0U);

    const auto render_signed_gauge =
        [&out](std::string_view name, std::string_view help, std::int64_t value) {
            out += "# HELP ";
            out += name;
            out += " ";
            out += help;
            out += "\n# TYPE ";
            out += name;
            out += " gauge\n";
            out += name;
            out += " ";
            out += std::to_string(value);
            out += "\n";
        };
    render_signed_gauge(
        "chunkdb_active_connections",
        "Client connections currently being served.",
        active_connections_.load(std::memory_order_relaxed));
    render_signed_gauge(
        "chunkdb_pending_connections",
        "Accepted client connections waiting for a worker.",
        pending_connections_.load(std::memory_order_relaxed));

    out += "# HELP chunkdb_connections_rejected_total Connections rejected because the "
           "pending queue was full.\n";
    out += "# TYPE chunkdb_connections_rejected_total counter\n";
    AppendCounter(
        &out, "chunkdb_connections_rejected_total", "",
        connections_rejected_.load(std::memory_order_relaxed));

    out += "# HELP chunkdb_malformed_requests_total Requests rejected during framing before "
           "command execution.\n";
    out += "# TYPE chunkdb_malformed_requests_total counter\n";
    AppendCounter(
        &out, "chunkdb_malformed_requests_total", "",
        malformed_requests_.load(std::memory_order_relaxed));

    // Store counters, one sample per table.
    const struct {
        const char* name;
        const char* type;
        const char* help;
        std::uint64_t StoreRuntimeStats::*value;
    } store_families[] = {
        {"chunkdb_open_wal_streams", "gauge", "Currently open WAL append streams.",
         &StoreRuntimeStats::open_wal_streams},
        {"chunkdb_background_queue_depth", "gauge", "Queued background checkpoint requests.",
         &StoreRuntimeStats::background_queue_depth},
        {"chunkdb_wal_barrier_full_syncs_total", "counter",
         "WAL barriers that fell back to a full data-directory sync.",
         &StoreRuntimeStats::wal_barrier_full_syncs},
        {"chunkdb_compressed_checkpoint_images_total", "counter",
         "Checkpoint images written with zrle compression.",
         &StoreRuntimeStats::compressed_checkpoint_images},
        {"chunkdb_evictions_total", "counter", "Chunks evicted from the cache.",
         &StoreRuntimeStats::evictions},
        {"chunkdb_eviction_recency_skips_total", "counter",
         "Eviction candidates skipped because they were accessed recently.",
         &StoreRuntimeStats::eviction_recency_skips},
        {"chunkdb_eviction_snapshot_builds_total", "counter", "Eviction candidate snapshots built.",
         &StoreRuntimeStats::eviction_snapshot_builds},
        {"chunkdb_eviction_probes_total", "counter", "Eviction candidates probed.",
         &StoreRuntimeStats::eviction_probes},
        {"chunkdb_eviction_no_progress_cycles_total", "counter", "Eviction passes that evicted nothing.",
         &StoreRuntimeStats::eviction_no_progress_cycles},
        {"chunkdb_eviction_forced_wal_flushes_with_data_total", "counter",
         "WAL batches flushed by eviction that held writes.",
         &StoreRuntimeStats::eviction_forced_wal_flushes_with_data},
        {"chunkdb_eviction_forced_wal_flushes_empty_batch_total", "counter",
         "WAL flushes forced by eviction with an empty batch.",
         &StoreRuntimeStats::eviction_forced_wal_flushes_empty_batch},
        {"chunkdb_checkpoints_total", "counter", "Chunk checkpoints written.",
         &StoreRuntimeStats::checkpoints},
        {"chunkdb_background_checkpoints_total", "counter",
         "Checkpoints completed by the background maintenance thread.",
         &StoreRuntimeStats::background_checkpoints},
        {"chunkdb_background_checkpoint_failures_total", "counter",
         "Background checkpoint attempts that failed.",
         &StoreRuntimeStats::background_checkpoint_failures},
        {"chunkdb_background_queue_full_inline_total", "counter",
         "Checkpoints run inline because the background queue was full.",
         &StoreRuntimeStats::background_queue_full_inline},
        {"chunkdb_wal_batch_flushes_total", "counter", "WAL batch flushes.",
         &StoreRuntimeStats::wal_batch_flushes},
        {"chunkdb_wal_barriers_total", "counter", "Explicit WAL durability barriers completed.",
         &StoreRuntimeStats::wal_barriers},
        {"chunkdb_empty_chunk_gcs_total", "counter", "Empty-chunk storage reclamations.",
         &StoreRuntimeStats::empty_chunk_gcs},
        {"chunkdb_unique_loaded_chunks_total", "counter", "Chunk loads into the cache.",
         &StoreRuntimeStats::unique_loaded_chunks},
        {"chunkdb_txn_history_bytes", "gauge",
         "Bytes, with bookkeeping overhead, of the chunk states kept for open transaction snapshots.",
         &StoreRuntimeStats::txn_history_bytes},
        {"chunkdb_txn_history_copies_total", "counter",
         "Chunk states writes copied to keep open transaction snapshots.",
         &StoreRuntimeStats::txn_history_copies},
        {"chunkdb_txn_history_copied_bytes_total", "counter",
         "Bytes, with bookkeeping overhead, of the chunk states writes copied to keep open transaction snapshots.",
         &StoreRuntimeStats::txn_history_copied_bytes},
    };
    std::vector<std::string> table_labels;
    table_labels.reserve(tables.size());
    for (const auto& table : tables) {
        table_labels.push_back("table=\"" + EscapeLabelValue(table.table) + "\"");
    }
    for (const auto& family : store_families) {
        out += "# HELP ";
        out += family.name;
        out += " ";
        out += family.help;
        out += "\n# TYPE ";
        out += family.name;
        out += " ";
        out += family.type;
        out += "\n";
        for (std::size_t i = 0; i < tables.size(); ++i) {
            AppendCounter(&out, family.name, table_labels[i], tables[i].stats.*family.value);
        }
    }

    if (!expiry.empty()) {
        std::vector<std::string> expiry_labels;
        expiry_labels.reserve(expiry.size());
        for (const auto& table : expiry) expiry_labels.push_back("table=\"" + EscapeLabelValue(table.table) + "\"");
        out += "# HELP chunkdb_expiry_lag_seconds Time since the start of the last completed expiry sweep, "
               "by table with an expire_column.\n";
        out += "# TYPE chunkdb_expiry_lag_seconds gauge\n";
        for (std::size_t i = 0; i < expiry.size(); ++i) {
            out += "chunkdb_expiry_lag_seconds{";
            out += expiry_labels[i];
            out += "} ";
            out += FormatMicrosAsSeconds(expiry[i].lag_ms * 1000U);
            out += "\n";
        }
        out += "# HELP chunkdb_expired_blocks_total Blocks deleted by expiry, by table.\n";
        out += "# TYPE chunkdb_expired_blocks_total counter\n";
        for (std::size_t i = 0; i < expiry.size(); ++i)
            AppendCounter(&out, "chunkdb_expired_blocks_total", expiry_labels[i], expiry[i].expired_blocks);
        out += "# HELP chunkdb_expiry_failed_passes_total Expiry passes that failed, by table.\n";
        out += "# TYPE chunkdb_expiry_failed_passes_total counter\n";
        for (std::size_t i = 0; i < expiry.size(); ++i)
            AppendCounter(&out, "chunkdb_expiry_failed_passes_total", expiry_labels[i], expiry[i].failed_passes);
    }

    return out;
}

}  // namespace chunkdb
