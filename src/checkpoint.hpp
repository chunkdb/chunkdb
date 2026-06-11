#pragma once

#include <filesystem>
#include <vector>

#include "chunk_store_internal.hpp"

namespace chunkdb {

void EnsureDirectoryPathExists(
    const std::filesystem::path& path,
    bool durable_sync);
void SyncFilePath(const std::filesystem::path& path);
void SyncDirectoryPath(const std::filesystem::path& path);
void CleanupAtomicTmpArtifacts(const std::filesystem::path& target_path);
void AtomicWrite(
    const std::filesystem::path& path,
    const std::vector<std::uint8_t>& bytes,
    bool fsync_file,
    bool fsync_directory);

}  // namespace chunkdb
