#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

inline constexpr std::uint32_t kChunkModelFormatBit = 32U;

// The table envelope must already have passed feature-registry admission.
// These primitives check identity and feature containment, not feature ownership.
struct ChunkFileContext {
    std::uint8_t dims = 2;
    StoreId epoch{};
    ChunkCoord coord{};
    FeatureFlags admitted_features{.incompat = kChunkModelFormatBit};
};

struct ChunkImageHeaderLimits {
    std::uint64_t max_file_bytes = 0;
    std::uint64_t max_section_stored_bytes = 0;
    std::uint64_t max_section_raw_bytes = 0;
    std::uint64_t max_total_raw_bytes = 0;
};

struct ChunkImageSectionDescriptor {
    std::uint16_t kind = 0;
    std::uint16_t flags = 0;
    std::uint32_t stored_size = 0;
    std::uint32_t raw_size = 0;
    std::uint32_t raw_crc32 = 0;
    friend bool operator==(const ChunkImageSectionDescriptor&, const ChunkImageSectionDescriptor&) = default;
};

struct ChunkImageHeaderInfo {
    FeatureFlags features{.incompat = kChunkModelFormatBit};
    std::uint64_t revision = 0;
    std::uint64_t commit_time_ms = 0;
};

struct CheckedChunkImageHeader {
    ChunkImageHeaderInfo info;
    std::vector<ChunkImageSectionDescriptor> sections;
    std::size_t header_bytes = 0;
    std::uint64_t file_bytes = 0;
};

[[nodiscard]] std::size_t ChunkImageFixedHeaderBytes(std::uint8_t dims);
[[nodiscard]] std::size_t ChunkImageHeaderBytes(std::uint8_t dims, std::uint16_t section_count);
[[nodiscard]] std::size_t ChunkWalHeaderBytes(std::uint8_t dims);

// Only exact header prefixes are accepted; complete_file_bytes is measured by
// the caller from the same stable file generation. All directory bounds pass
// before allocating the result entries. No bodies are read or allocated here.
// A full image decoder must still check registry ownership, required sections,
// schema, decompression and raw checksums before returning or publishing state.
[[nodiscard]] CheckedChunkImageHeader ParseChunkImageHeader(
    std::span<const std::uint8_t> header, std::uint64_t complete_file_bytes,
    const ChunkFileContext& context, const ChunkImageHeaderLimits& limits);
[[nodiscard]] std::vector<std::uint8_t> SerializeChunkImageHeader(
    const ChunkFileContext& context, const ChunkImageHeaderInfo& info,
    std::span<const ChunkImageSectionDescriptor> sections, const ChunkImageHeaderLimits& limits);

// FRM2 and all record/schema validation remain the full WAL reader's job.
[[nodiscard]] FeatureFlags ParseChunkWalHeader(
    std::span<const std::uint8_t> header, const ChunkFileContext& context);
[[nodiscard]] std::vector<std::uint8_t> SerializeChunkWalHeader(
    const ChunkFileContext& context, const FeatureFlags& features);

}  // namespace chunkdb
