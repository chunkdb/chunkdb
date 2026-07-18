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
// Atomically replaces `path` with `bytes` via a temp file and rename.
// When `out_replaced` is non-null it is set to true as soon as the rename
// completes, so a caller can tell whether a post-rename failure (directory
// sync) left the target already updated. Throws on any failure; if
// `out_replaced` is false after a throw, the target file is unchanged.
void AtomicWrite(
    const std::filesystem::path& path,
    const std::vector<std::uint8_t>& bytes,
    bool fsync_file,
    bool fsync_directory,
    bool* out_replaced = nullptr,
    const char* after_rename_failpoint = nullptr,
    bool enable_generic_failpoints = true);

}  // namespace chunkdb
