#pragma once

#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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
    bool rewritten = false;
};

// Rebuilds each table's chunkdb.version and .chunkdb.initialized offline:
// takes the writer lock, so it fails while a server has the directory open,
// and runs the chunkdb_verify checks, writing their findings to `findings`.
// Any damage other than a missing or malformed clock or marker refuses the
// whole directory before a file changes. A readable clock above every
// revision on disk is kept; any other gets the highest revision on disk or
// the directory's version floor, whichever is larger, plus
// kRebuiltVersionClockMargin, so a clock is never lowered.
[[nodiscard]] std::vector<RebuiltVersionClock> RebuildVersionClock(
    const std::filesystem::path& data_dir, std::ostream& findings);

}  // namespace chunkdb
