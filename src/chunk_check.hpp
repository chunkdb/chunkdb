#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"
#include "populated_index_format.hpp"

namespace chunkdb {

class CheckTableShutdownError : public std::runtime_error {
  public:
    CheckTableShutdownError() : std::runtime_error("CHECK TABLE stopped: the server is shutting down") {}
};

// One finding about a chunk's files, with chunkdb_verify's codes.
struct ChunkFileFinding {
    bool error = false;
    std::string code;
    std::filesystem::path path;
    std::string detail;
};

struct ChunkFileCheck {
    // Files read: the image and the WAL, when present.
    std::uint64_t files = 0;
    std::vector<ChunkFileFinding> findings;
    // Revisions the files hold, for a clock rebuild.
    std::vector<std::uint64_t> revisions;
    // Whether the chunk's state was rebuilt from its files, and that state's
    // identity and populated-ness.
    bool complete = false;
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    bool populated = false;

    // The first error: damage a chunk load refuses. Warnings, such as a WAL
    // tail a crash left, are not damage.
    [[nodiscard]] const ChunkFileFinding* damage() const noexcept {
        for (const auto& finding : findings)
            if (finding.error) return &finding;
        return nullptr;
    }
};

// Checks the image and WAL of chunk `coord` in the table directory
// `table_dir` as chunkdb_verify checks them: the image is parsed, and the WAL
// is replayed over it, or over the retained empty boundary `retained` of the
// coordinate index. `wal_cut` is the committed WAL length a pending rollback
// intent names. `wal_bytes` bounds the WAL to a pinned prefix with the same
// crash-tail rules as the whole file. Without `check_wal`, only the image is
// checked. Never throws for damage; it reports a finding. A WAL read
// checkpoint may throw StatementStoppedError or CheckTableShutdownError.
[[nodiscard]] ChunkFileCheck CheckChunkFiles(const std::filesystem::path& table_dir, const Geometry& geometry,
    const StoreId& epoch, FeatureFlags features, const ChunkCoord& coord,
    const std::optional<ChunkIndexRecord>& retained, std::optional<std::uint64_t> wal_cut = std::nullopt,
    bool check_wal = true, std::optional<std::uint64_t> wal_bytes = std::nullopt,
    const std::function<void()>& read_checkpoint = {});

// The coordinates of the chunks with an image or a WAL in the table
// directory `table_dir`, in ascending order. Listing takes no lock.
[[nodiscard]] std::vector<ChunkCoord> ChunkFileCoordinates(const std::filesystem::path& table_dir, const Geometry& geometry,
    const std::function<void()>& checkpoint);

}  // namespace chunkdb
