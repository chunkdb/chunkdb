#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/geometry.hpp"
#include "verify.hpp"

// chunkdb_migrate: offline conversion of a data directory written by chunkdb
// 1.x, or by the unreleased storage format of the 2.0 development line, into
// a new 2.0 data directory whose `default` table holds the old store.
//
// Each chunk gets the state the read-write server of the 2.0 development line
// (which reads every 1.x artifact the way 1.x does) would load after startup
// recovery: the image, then the WAL up to its conditional rollback boundary,
// with a torn tail ignored. Persisted revisions are kept; chunks without one
// get fresh revisions at or above the source's version-clock ceiling.
namespace chunkdb::migrate {

struct MigrateOptions {
    std::filesystem::path from;
    std::filesystem::path to;
    // The geometry the source was written with; nothing in a 1.x directory
    // records the large-chunk size.
    GeometryConfig geometry;
    // Options of the new `default` table.
    TableOptions table_options;
    // Convert data the old server would have dropped or could not read,
    // dropping it the same way, instead of refusing.
    bool accept_loss = false;
};

struct MigrateSummary {
    std::uint64_t chunks = 0;           // chunks written
    std::uint64_t absent_chunks = 0;    // artifacts that load as an absent chunk
    std::uint64_t source_bytes = 0;     // image and WAL bytes read
    std::array<std::uint64_t, 6> images_by_version{};  // index: image version 1..5
    std::uint64_t wals_v2 = 0;
    std::uint64_t wals_v3 = 0;
    std::uint64_t wals_v4 = 0;
    std::uint64_t wals_headerless = 0;
    std::uint64_t wals_mixed = 0;       // 1.x records continued by v4 frames
    std::uint64_t torn_tails = 0;
    std::uint64_t rollback_intents = 0;
    std::uint64_t committed_intents = 0;
    std::uint64_t canonicalized_chunks = 0;
    std::uint64_t persisted_revisions = 0;
    std::uint64_t assigned_revisions = 0;
    std::uint64_t source_clock_ceiling = 0;
    // Data dropped under --accept-loss, one line per artifact.
    std::vector<std::string> losses;
    // Notes about the source that do not affect data.
    std::vector<std::string> notes;
    VerifyCounters verify;
};

// Thrown when the source cannot be converted as it is; `problems` lists
// every reason found, so one run reports them all.
class MigrateRefused : public std::runtime_error {
  public:
    MigrateRefused(const std::string& message, std::vector<std::string> problems)
        : std::runtime_error(message), problems_(std::move(problems)) {}
    [[nodiscard]] const std::vector<std::string>& problems() const noexcept {
        return problems_;
    }

  private:
    std::vector<std::string> problems_;
};

// Converts `options.from` into the new directory `options.to`, which must not
// exist or be empty. The source is never modified, and is locked against a
// server for the duration. The result appears at `to` only once complete and
// verified. Throws MigrateRefused, or std::runtime_error for I/O failures.
[[nodiscard]] MigrateSummary Migrate(const MigrateOptions& options);

}  // namespace chunkdb::migrate
