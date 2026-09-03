#include "chunkdb/metrics.hpp"

#include <cmath>
#include <string_view>

#include "chunkdb/protocol.hpp"

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

[[nodiscard]] std::string FormatBucketBound(double bound) {
    std::string text = std::to_string(bound);
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

MetricsRegistry::CommandClass MetricsRegistry::ClassifyCommand(
    std::string_view command_name) noexcept {
    const auto equals = [command_name](std::string_view expected) {
        return Protocol::CommandEquals(command_name, expected);
    };
    if (equals("AUTH")) {
        return CommandClass::kAuth;
    }
    if (equals("GET") || equals("EXISTS") || equals("MGET")) {
        return CommandClass::kPointRead;
    }
    if (equals("SET") || equals("UNSET") || equals("MSET")) {
        return CommandClass::kPointWrite;
    }
    if (equals("CHUNK") || equals("CHUNKBIN") || equals("CHUNKBINC") ||
        equals("CHUNKEXISTS") || equals("CHUNKVER")) {
        return CommandClass::kChunkRead;
    }
    if (equals("CHUNKSET") || equals("CHUNKSETBIN")) {
        return CommandClass::kChunkWrite;
    }
    if (equals("CHUNKSCAN")) {
        return CommandClass::kScan;
    }
    if (equals("CHUNKRANGE") || equals("CHUNKRADIUS")) {
        return CommandClass::kRange;
    }
    if (equals("CHUNKCAS") || equals("CHUNKBATCH")) {
        return CommandClass::kConditional;
    }
    if (equals("WALFLUSH")) {
        return CommandClass::kBarrier;
    }
    if (equals("PING") || equals("INFO") || equals("METRICS") || equals("QUIT")) {
        return CommandClass::kAdmin;
    }
    return CommandClass::kOther;
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

std::string MetricsRegistry::RenderPrometheus(
    const StoreRuntimeStats& store_stats,
    std::size_t loaded_chunks) const {
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
        const double sum_seconds =
            static_cast<double>(histogram.sum_micros.load(std::memory_order_relaxed)) / 1e6;
        out += "chunkdb_command_duration_seconds_sum{class=\"";
        out += name;
        out += "\"} ";
        out += std::to_string(sum_seconds);
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

    out += "# HELP chunkdb_open_wal_streams Currently open WAL append streams.\n";
    out += "# TYPE chunkdb_open_wal_streams gauge\n";
    AppendCounter(&out, "chunkdb_open_wal_streams", "", store_stats.open_wal_streams);

    out += "# HELP chunkdb_background_queue_depth Queued background checkpoint requests.\n";
    out += "# TYPE chunkdb_background_queue_depth gauge\n";
    AppendCounter(&out, "chunkdb_background_queue_depth", "", store_stats.background_queue_depth);

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

    out += "# HELP chunkdb_wal_barrier_full_syncs_total WAL barriers that fell back to a full "
           "data-directory sync.\n";
    out += "# TYPE chunkdb_wal_barrier_full_syncs_total counter\n";
    AppendCounter(
        &out, "chunkdb_wal_barrier_full_syncs_total", "", store_stats.wal_barrier_full_syncs);

    out += "# HELP chunkdb_compressed_checkpoint_images_total Checkpoint images written with "
           "zrle compression.\n";
    out += "# TYPE chunkdb_compressed_checkpoint_images_total counter\n";
    AppendCounter(
        &out, "chunkdb_compressed_checkpoint_images_total", "",
        store_stats.compressed_checkpoint_images);

    const struct {
        const char* name;
        const char* help;
        std::uint64_t value;
    } counters[] = {
        {"chunkdb_evictions_total", "Chunks evicted from the cache.", store_stats.evictions},
        {"chunkdb_eviction_recency_skips_total",
         "Eviction candidates skipped because they were accessed recently.",
         store_stats.eviction_recency_skips},
        {"chunkdb_checkpoints_total", "Chunk checkpoints written.", store_stats.checkpoints},
        {"chunkdb_background_checkpoints_total",
         "Checkpoints completed by the background maintenance thread.",
         store_stats.background_checkpoints},
        {"chunkdb_background_checkpoint_failures_total",
         "Background checkpoint attempts that failed.",
         store_stats.background_checkpoint_failures},
        {"chunkdb_background_queue_full_inline_total",
         "Checkpoints run inline because the background queue was full.",
         store_stats.background_queue_full_inline},
        {"chunkdb_wal_batch_flushes_total", "WAL batch flushes.", store_stats.wal_batch_flushes},
        {"chunkdb_wal_barriers_total", "Explicit WAL durability barriers completed.",
         store_stats.wal_barriers},
        {"chunkdb_empty_chunk_gcs_total", "Empty-chunk storage reclamations.",
         store_stats.empty_chunk_gcs},
        {"chunkdb_unique_loaded_chunks_total", "Chunk loads into the cache.",
         store_stats.unique_loaded_chunks},
    };
    for (const auto& counter : counters) {
        out += "# HELP ";
        out += counter.name;
        out += " ";
        out += counter.help;
        out += "\n# TYPE ";
        out += counter.name;
        out += " counter\n";
        AppendCounter(&out, counter.name, "", counter.value);
    }

    return out;
}

}  // namespace chunkdb
