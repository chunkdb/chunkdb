#pragma once

#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/types.hpp"

namespace chunkdb {

// Gives `user` of the data directory a new password, offline: takes the
// writer lock, so it fails while a server has the directory open, and writes
// only the new verifier (docs/design/USERS_DESIGN.md). Throws std::invalid_argument
// for a directory without users, an unknown user or an empty password, and
// refuses a backup or incomplete restore directory before taking the lock.
void ResetPassword(const std::filesystem::path& data_dir, const std::string& user, std::string_view password);

// Revisions a rebuilt clock skips above the highest revision on disk, so a
// revision handed out but never written (a relaxed-durability write lost in
// a crash, a rolled-back frame) is not issued again.
inline constexpr std::uint64_t kRebuiltVersionClockMargin = std::uint64_t{1} << 32U;

struct RebuiltVersionClock {
    std::string table;
    // The highest revision the table's artifacts hold.
    std::uint64_t highest_revision = 0;
    // The ceiling of a readable chunkdb.version before the rebuild.
    std::optional<std::uint64_t> previous_ceiling;
    // The table's next revision after the rebuild.
    std::uint64_t ceiling = 0;
    // Whether chunkdb.version was written; false when the clock is kept.
    bool rewritten = false;
    // Whether .chunkdb.initialized was written, also beside a kept clock.
    bool marker_rewritten = false;
};

// Rebuilds each table's chunkdb.version and .chunkdb.initialized offline:
// takes the writer lock, so it fails while a server has the directory open,
// and runs the chunkdb_verify checks, writing their findings to `findings`.
// Any damage other than a missing or malformed clock or marker, or a feature
// a read-write open of this build refuses, refuses the whole directory before
// a file changes. A readable clock above every revision on disk is kept; any
// other gets the highest revision on disk or the directory's version floor,
// whichever is larger, plus kRebuiltVersionClockMargin, so a clock is never
// lowered.
[[nodiscard]] std::vector<RebuiltVersionClock> RebuildVersionClock(
    const std::filesystem::path& data_dir, std::ostream& findings);

struct QuarantinedChunk {
    // The entry that holds the chunk's files, relative to the data directory.
    std::filesystem::path entry;
    // The image and WAL names moved there.
    std::vector<std::string> files;
    // The chunkdb_verify code of the chunk's damage; empty for a forced
    // quarantine of sound files or a finished interrupted one.
    std::string damage;
    // Whether an interrupted quarantine of the chunk was finished.
    bool resumed = false;
    // Whether an earlier run had finished already; nothing changed.
    bool already_quarantined = false;
    // Slots marked so that their next watch starts with resync.
    std::size_t slots_marked = 0;
    // Errors the chunkdb_verify checks of the table still find.
    std::uint64_t table_errors = 0;
};

// Moves the image and WAL of chunk `coord` of `table` into a new entry of
// the table's `.chunkdb.quarantine/` directory with a synced record of them,
// offline, so the table opens without the chunk (docs/BACKUP.md). Takes the
// writer lock, so it fails while a server has the directory open. The
// chunk must fail the chunkdb_verify checks unless `force`. Refuses a chunk
// a pending recovery intent names. The files are hard-linked into the
// entry and synced, the record is published, the table's slots are marked
// to resync, and then the WAL is removed before the image, so a crash
// leaves the chunk whole, without its WAL, or gone; running the command
// again finishes it. Writes the checks' findings to `findings`.
[[nodiscard]] QuarantinedChunk QuarantineChunk(const std::filesystem::path& data_dir, const std::string& table,
    std::uint8_t dims, const ChunkCoord& coord, bool force, std::ostream& findings);

}  // namespace chunkdb
