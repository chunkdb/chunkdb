#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace chunkdb {

// Continuous archiving of sealed WAL segments (docs/design/WAL_ARCHIVE.md).
// At most one of `directory` and `command` is set.
struct WalArchiveConfig {
    // --archive-dir: files are copied here.
    std::filesystem::path directory{};
    // --archive-command: run through the system shell for each file, with %p
    // replaced by its local path, %f by its archive-relative name and %% by %.
    std::string command{};
    // --archive-interval-ms: how often every table is sealed and shipped.
    std::chrono::milliseconds interval{60000};
    // --archive-retention-ms: for `directory`, how long seal records and the
    // segments only they need are kept; zero keeps everything.
    std::chrono::milliseconds retention{0};

    [[nodiscard]] bool enabled() const noexcept { return !directory.empty() || !command.empty(); }
};

// Throws std::invalid_argument for an unusable configuration.
void RequireValidWalArchiveConfig(const WalArchiveConfig& config);

struct WalArchiveTableStatus {
    std::string name;
    // The table's seal point in the last published seal record.
    std::uint64_t revision = 0;
};

struct WalArchiveStatus {
    // "dir" or "command".
    std::string mode;
    // Capture time, in Unix milliseconds, of the last published seal record;
    // zero before the first.
    std::uint64_t last_seal_ms = 0;
    // Why the last pass failed; std::nullopt after a pass that succeeded.
    std::optional<std::string> error{};
    std::vector<WalArchiveTableStatus> tables{};
    // Bytes of the files waiting in the data directory to be shipped.
    std::uint64_t staged_bytes = 0;
};

}  // namespace chunkdb
