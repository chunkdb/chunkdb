#pragma once

#include <cstdint>
#include <filesystem>
#include <system_error>
#include <vector>

namespace chunkdb {

void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync,
    bool enable_generic_failpoints);
std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path);
// Moves `tmp_path` to `target_path` only when `target_path` does not exist.
// Returns std::errc::file_exists, leaving both files untouched, when it does.
// On POSIX a failure to drop the temporary name after the target was created
// is returned as an error even though the target is already published.
std::error_code MovePathNoReplace(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path);

}  // namespace chunkdb
