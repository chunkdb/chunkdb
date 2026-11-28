#pragma once

#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <vector>

namespace chunkdb {

struct CommitOwnerScope;

struct VerifyCounters {
    std::uint64_t checked = 0;
    std::uint64_t warnings = 0;
    std::uint64_t errors = 0;
};

// Checks a catalog or standalone store offline without changing it, using
// the geometry each table manifest records. Writes one finding per line to `out`:
//   VERIFY <level> <code> <path> [detail]
// Paths and details are C-style quoted, escaped tokens. Throws when the
// directory cannot be inspected at all.
[[nodiscard]] VerifyCounters VerifyDataDirectory(
    const std::filesystem::path& data_dir,
    std::ostream& out, bool features = false);

// Checks one table directory as VerifyDataDirectory checks each table. Its
// manifest owner scope is checked only against itself, since the directory
// that binds a catalog table is not part of the check; a standalone store
// also has its canonical INACTIVE owner root checked.
[[nodiscard]] VerifyCounters VerifyTableDirectory(
    const std::filesystem::path& table_dir,
    std::ostream& out);

// Checks the commit group owner of a directory for an admitted scope without
// changing it. VerifyDataDirectory only admits the initial INACTIVE root.
// Journal records are checked as records only; member frames are not checked.
// Throws when the directory itself cannot be opened.
[[nodiscard]] VerifyCounters VerifyCommitGroupOwner(
    const std::filesystem::path& directory,
    const CommitOwnerScope& scope,
    std::ostream& out);

// The highest revision the checked artifacts of one table hold: images,
// WALs, archived WALs and bases, coordinate decisions, transaction intents,
// slot positions and the restore fork revision.
struct TableRevisions {
    std::filesystem::path table_dir;
    std::uint64_t highest = 0;
};

// Runs the checks of VerifyDataDirectory, writing its findings to `out`, and
// collects what a rebuilt version clock must stay above. `damage` counts its
// errors except a missing or malformed chunkdb.version or
// .chunkdb.initialized, which a rebuild replaces.
struct RevisionSurvey {
    VerifyCounters counters;
    std::uint64_t damage = 0;
    std::uint64_t version_floor = 0;
    std::vector<TableRevisions> tables;
};
[[nodiscard]] RevisionSurvey SurveyRevisions(
    const std::filesystem::path& data_dir,
    std::ostream& out);

}  // namespace chunkdb
