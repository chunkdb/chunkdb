#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "crypto.hpp"

namespace chunkdb {

enum class ArchiveBaseKind : std::uint8_t { kNeverWritten = 0, kImage = 1, kRetainedEmpty = 2 };

struct ArchiveBaseDescriptor {
    std::uint8_t dims = 2;
    ArchiveBaseKind kind = ArchiveBaseKind::kNeverWritten;
    StoreId epoch{};
    ChunkCoord coord{};
    std::uint64_t first = 0;
    std::uint64_t base_revision = 0;
    std::uint64_t base_time_ms = 0;
    std::uint64_t base_schema = 0;
    std::uint64_t image_length = 0;
    crypto::Sha256Digest image_sha256{};
};

[[nodiscard]] constexpr std::size_t ArchiveBaseDescriptorBytes(std::uint8_t dims) noexcept {
    return 100U + 8U * dims;
}

[[nodiscard]] std::vector<std::uint8_t> SerializeArchiveBaseDescriptor(
    const ArchiveBaseDescriptor& descriptor, std::uint64_t max_image_bytes);
[[nodiscard]] ArchiveBaseDescriptor ParseArchiveBaseDescriptor(
    std::span<const std::uint8_t> bytes, std::uint8_t dims, const StoreId& epoch,
    const ChunkCoord& coord, std::uint64_t first, std::uint64_t max_image_bytes);

struct PinnedBaseImageIdentity {
    std::uint8_t dims = 2;
    StoreId epoch{};
    ChunkCoord coord{};
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
    std::uint64_t schema_version = 0;
};

// The complete image must first pass its own checked image codec. Bind its
// validated identity and all raw bytes to the descriptor; never substitute the
// current emptying image or synthesize a base from an absent filename.
void ValidatePinnedArchiveBaseImage(
    const ArchiveBaseDescriptor& descriptor, const PinnedBaseImageIdentity& image,
    std::span<const std::uint8_t> bytes, std::uint64_t max_image_bytes);

[[nodiscard]] std::string ArchiveBaseFileName(const ChunkCoord& coord, std::uint8_t dims, std::uint64_t first);
// Strict canonical numeric stems/revision, no filesystem access. Refusal leaves
// both output values unchanged.
[[nodiscard]] bool ParseArchiveBaseFileName(
    std::string_view name, std::uint8_t dims, ChunkCoord* coord, std::uint64_t* first);

}  // namespace chunkdb
