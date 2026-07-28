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

}  // namespace chunkdb
