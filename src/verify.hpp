#pragma once

#include <cstdint>
#include <filesystem>
#include <iosfwd>

namespace chunkdb {

struct VerifyCounters {
    std::uint64_t checked = 0;
    std::uint64_t warnings = 0;
    std::uint64_t errors = 0;
};

// Checks a data directory offline without changing it: its manifest, the
// leftovers of interrupted table operations, and every table with the
// geometry its manifest records. Writes one finding per line to `out`:
//   VERIFY <level> <code> <path> [detail]
// Paths and details are C-style quoted, escaped tokens. Throws when the
// directory cannot be inspected at all.
[[nodiscard]] VerifyCounters VerifyDataDirectory(
    const std::filesystem::path& data_dir,
    std::ostream& out);

}  // namespace chunkdb
