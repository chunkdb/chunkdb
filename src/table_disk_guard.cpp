// The free-disk floor (--min-free-disk-bytes) and the max_disk_bytes table
// option. A sampler thread measures both every disk_check_interval and
// publishes the result in atomics, so a write checks one atomic load
// (TableCatalog::RequireDiskRoom) instead of querying the file system.

#include <algorithm>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"

namespace chunkdb {

namespace {

// The bytes of the regular files below `root`. Files removed during the walk
// are not counted; std::nullopt when `root` cannot be listed.
[[nodiscard]] std::optional<std::uint64_t> TreeFileBytes(const std::filesystem::path& root) {
    namespace fs = std::filesystem;
    std::uint64_t bytes = 0U;
    std::vector<fs::path> pending{root};
    while (!pending.empty()) {
        const fs::path dir = std::move(pending.back());
        pending.pop_back();
        std::error_code error;
        fs::directory_iterator it(dir, error);
        if (error) {
            if (dir == root) return std::nullopt;
            continue;
        }
        for (const fs::directory_iterator end{}; !error && it != end; it.increment(error)) {
            std::error_code entry_error;
            const auto status = it->symlink_status(entry_error);
            if (entry_error) continue;
            if (fs::is_directory(status)) pending.push_back(it->path());
            else if (fs::is_regular_file(status)) {
                const auto size = fs::file_size(it->path(), entry_error);
                if (!entry_error) bytes += size;
            }
        }
    }
    return bytes;
}

[[nodiscard]] std::optional<std::uint64_t> AvailableDiskBytes(const std::filesystem::path& dir) {
    std::error_code error;
    const auto space = std::filesystem::space(dir, error);
    if (error) return std::nullopt;
    return static_cast<std::uint64_t>(space.available);
}

}  // namespace

void TableCatalog::SetDiskSpaceProbeForTests(
    std::function<std::optional<std::uint64_t>(const std::filesystem::path&)> probe) {
    std::lock_guard sample(disk_sample_mutex_);
    disk_space_probe_ = std::move(probe);
}

void TableCatalog::SampleDisk() {
    if (config_.access_mode != AccessMode::kReadWrite) return;
    std::lock_guard sample(disk_sample_mutex_);
    std::uint32_t refusals = 0U;
    if (config_.min_free_disk_bytes != 0U) {
        const auto available = disk_space_probe_ ? disk_space_probe_(config_.data_dir) : AvailableDiskBytes(config_.data_dir);
        if (available) {
            disk_probe_failure_logged_ = false;
            const bool low = *available < config_.min_free_disk_bytes;
            disk_available_bytes_.store(*available, std::memory_order_relaxed);
            if (low != disk_floor_reached_.load(std::memory_order_relaxed)) {
                LogMessage(low ? LogLevel::kWarn : LogLevel::kInfo, LogComponent::kStore,
                    low ? "free disk space is below --min-free-disk-bytes; data writes are refused with DISK_FULL"
                        : "free disk space is back above --min-free-disk-bytes; data writes are accepted",
                    {{"data_dir", config_.data_dir.string()}, {"available_bytes", std::to_string(*available)},
                        {"min_free_disk_bytes", std::to_string(config_.min_free_disk_bytes)}});
                disk_floor_reached_.store(low, std::memory_order_relaxed);
            }
        } else if (!disk_probe_failure_logged_) {
            // The last known state stands until a query succeeds again.
            disk_probe_failure_logged_ = true;
            LogMessage(LogLevel::kWarn, LogComponent::kStore, "cannot query free disk space; keeping the last result",
                {{"data_dir", config_.data_dir.string()}});
        }
        if (disk_floor_reached_.load(std::memory_order_relaxed)) ++refusals;
    }
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(tables_mutex_);
        tables.reserve(tables_.size());
        for (const auto& [_, table] : tables_) tables.push_back(table);
    }
    for (const auto& table : tables) {
        std::optional<std::size_t> limit;
        {
            std::lock_guard lock(table->mutex_);
            limit = table->options_.max_disk_bytes;
        }
        if (!limit) {
            table->disk_limit_reached_.store(false, std::memory_order_relaxed);
            continue;
        }
        // A table directory that cannot be listed keeps its last result.
        if (const auto used = TreeFileBytes(table->dir_)) {
            const bool reached = *used >= *limit;
            table->disk_used_bytes_.store(*used, std::memory_order_relaxed);
            table->disk_limit_bytes_.store(*limit, std::memory_order_relaxed);
            if (reached != table->disk_limit_reached_.load(std::memory_order_relaxed)) {
                LogMessage(reached ? LogLevel::kWarn : LogLevel::kInfo, LogComponent::kStore,
                    reached ? "table files reached max_disk_bytes; its data writes are refused with DISK_FULL"
                            : "table files are below max_disk_bytes again; its data writes are accepted",
                    {{"table", table->name_}, {"disk_bytes", std::to_string(*used)},
                        {"max_disk_bytes", std::to_string(*limit)}});
                table->disk_limit_reached_.store(reached, std::memory_order_relaxed);
            }
        }
        if (table->disk_limit_reached_.load(std::memory_order_relaxed)) ++refusals;
    }
    if (disk_refusals_.load(std::memory_order_relaxed) != refusals)
        disk_refusals_.store(refusals, std::memory_order_relaxed);
}

void TableCatalog::RefuseDiskWrite(const Table* table) const {
    if (disk_floor_reached_.load(std::memory_order_relaxed)) {
        const auto available = std::to_string(disk_available_bytes_.load(std::memory_order_relaxed));
        const auto limit = std::to_string(config_.min_free_disk_bytes);
        throw DiskFullError("min_free_disk", {{"available", available}, {"limit", limit}},
            "the data directory's file system has " + available + " bytes available, below --min-free-disk-bytes " +
                limit + "; free disk space before retrying");
    }
    if (table != nullptr && table->disk_limit_reached_.load(std::memory_order_relaxed)) {
        const auto used = std::to_string(table->disk_used_bytes_.load(std::memory_order_relaxed));
        const auto limit = std::to_string(table->disk_limit_bytes_.load(std::memory_order_relaxed));
        throw DiskFullError("max_disk_bytes", {{"table", table->name_}, {"used", used}, {"limit", limit}},
            "the files of table '" + table->name_ + "' take " + used + " bytes, at its max_disk_bytes " + limit +
                "; free space in the table or raise the limit with ALTER TABLE ... SET max_disk_bytes");
    }
}

void TableCatalog::WakeDiskSampler() {
    {
        std::lock_guard lock(disk_sampler_mutex_);
        disk_sample_requested_ = true;
    }
    disk_sampler_cv_.notify_all();
}

void TableCatalog::RunDiskSampler(std::stop_token stop) {
    while (!stop.stop_requested()) {
        {
            std::unique_lock lock(disk_sampler_mutex_);
            disk_sampler_cv_.wait_for(lock, stop, config_.disk_check_interval, [this] { return disk_sample_requested_; });
            disk_sample_requested_ = false;
        }
        if (stop.stop_requested()) return;
        try {
            SampleDisk();
        } catch (const std::exception& error) {
            // An allocation or logging failure: the next interval tries again.
            try {
                LogMessage(LogLevel::kWarn, LogComponent::kStore, "disk sample failed", {{"error", error.what()}});
            } catch (...) {
            }
        }
    }
}

}  // namespace chunkdb
