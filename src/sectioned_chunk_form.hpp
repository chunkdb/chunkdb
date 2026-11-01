#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace chunkdb {

inline constexpr std::uint16_t kChunkFormProjected = 1;
inline constexpr std::uint16_t kChunkFormPrivate = 2;

enum class ChunkFormSectionKind : std::uint16_t {
    kPresence = 1,
    kFixedValues = 2,
    kNullBits = 3,
    kVariableValues = 4,
    kChunkColumns = 5,
    kBlockSpans = 6,
};

struct ChunkFormHeader {
    std::uint16_t flags = 0;
    std::uint64_t chunk_version = 0;
    std::uint64_t schema_version = 0;
    std::uint64_t commit_time_ms = 0;
};

struct ChunkFormSection {
    std::uint16_t kind = 0;
    std::span<const std::uint8_t> body;
};

struct ChunkFormSections {
    ChunkFormHeader header;
    std::vector<ChunkFormSection> sections;
};

struct ChunkFormBlockColumn {
    std::uint32_t column_id = 0;
    std::uint32_t width_bits = 0;
    std::span<const std::uint8_t> values;
    // Empty for nonnullable columns; otherwise one validity bit per block.
    std::span<const std::uint8_t> validity;
};

struct ChunkFormChunkColumn {
    std::uint32_t column_id = 0;
    bool is_null = false;
    std::span<const std::uint8_t> value;
};

enum class ChunkFormReadMode { kRead, kMutation };

// Validate the transport header and complete directory before returning any
// borrowed section. The backing bytes must outlive the returned spans. Known
// section bodies still require schema/geometry validation by the typed codec.
// Unknown read sections remain opaque; mutations refuse them.
[[nodiscard]] ChunkFormSections ParseChunkFormSections(
    std::span<const std::uint8_t> bytes, std::size_t max_bytes, ChunkFormReadMode mode);

// Validate sizes before allocating the output. The typed codec supplies bodies
// already validated against the schema and geometry.
[[nodiscard]] std::vector<std::uint8_t> SerializeChunkFormSections(
    const ChunkFormHeader& header, std::span<const ChunkFormSection> sections,
    std::size_t max_bytes);

// Validate presence, descriptors, array widths/padding and complete coverage of
// fixed/validity bodies without guessing column types. A typed codec must also
// match IDs, widths and nullability against the applicable schema/projection.
[[nodiscard]] std::vector<ChunkFormBlockColumn> ParseChunkFormBlockColumns(
    const ChunkFormSections& form, std::size_t block_count);

// Pack one fixed value, including an opaque future type, lowest bit first.
// Check validity/presence separately before interpreting the returned bytes.
[[nodiscard]] std::vector<std::uint8_t> ChunkFormRawBlockValue(
    const ChunkFormBlockColumn& column, std::size_t block_count, std::size_t block_index);

// Checked one-row directory; empty when omitted. The typed codec additionally
// requires exactly every described chunk column, with valid scalar encodings.
[[nodiscard]] std::vector<ChunkFormChunkColumn> ParseChunkFormChunkColumns(
    const ChunkFormSections& form);

}  // namespace chunkdb
