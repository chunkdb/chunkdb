#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <system_error>
#include <vector>

namespace chunkdb {

// Sync the already admitted native generation without reopening its path.
// Labels are diagnostic and observer values, never I/O instructions.
#ifdef _WIN32
void SyncOwnerFileHandle(void* handle, const std::filesystem::path& label);
void SyncOwnerDirectoryHandle(void* handle, const std::filesystem::path& label);
#else
void SyncOwnerFileDescriptor(int fd, const std::filesystem::path& label);
void SyncOwnerDirectoryDescriptor(int fd, const std::filesystem::path& label);
#endif

using AtomicFileSink = std::function<void(std::span<const std::uint8_t>)>;
using AtomicFileProducer = std::function<void(const AtomicFileSink&)>;

// Creates files readable and writable by their owner alone; publication keeps
// the mode or protected Windows ACL.
void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync,
    bool enable_generic_failpoints);
// The producer streams bounded pieces through the same exclusive owner-only
// temporary-file handle; a producer or I/O failure removes the unpublished file.
// The producer calls its borrowed sink synchronously and does not retain it.
void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const AtomicFileProducer& produce,
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
// Renames directory `from` to `to` unless `to` exists, returning
// std::errc::file_exists then. Where the platform has no exclusive rename,
// the existence check and the rename are two steps: callers hold the data
// directory's writer lock, so no other creator can take the name between.
std::error_code MoveDirectoryNoReplace(
    const std::filesystem::path& from,
    const std::filesystem::path& to);

}  // namespace chunkdb
