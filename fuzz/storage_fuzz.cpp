#include "fuzz_common.hpp"
#include "chunk_store_internal.hpp"
#include "feature_flags.hpp"
#include "wal_replay.hpp"

namespace {
using chunkdb::fuzz::Bytes;
void Parse(const Bytes& bytes, const chunkdb::Geometry& geometry) {
#if CHUNKDB_FUZZ_STORAGE == 1
    chunkdb::fuzz::Rejectable([&] {
        Bytes payload(geometry.ChunkPayloadBytes(), 0U);
        Bytes presence(chunkdb::ChunkPresenceBitmapBytes(geometry), 0U);
        chunkdb::ChunkVars vars;
        std::vector<chunkdb::WalFrameBoundary> boundaries;
        (void)chunkdb::ReplayWal(bytes, geometry, chunkdb::fuzz::kCoord,
            chunkdb::fuzz::kId, chunkdb::kKnownFeatures, 0U, 1U,
            &payload, &presence, &vars, &boundaries);
    });
#else
    chunkdb::fuzz::Rejectable([&] {
        (void)chunkdb::ParseChunkImage(bytes, geometry, chunkdb::fuzz::kCoord,
            chunkdb::fuzz::kId, chunkdb::kKnownFeatures);
    });
#endif
}
void RepairChecksums(Bytes& bytes) {
#if CHUNKDB_FUZZ_STORAGE == 1
    if (bytes.size() < chunkdb::kWalHeaderSize) return;
    chunkdb::fuzz::Put32(bytes, chunkdb::kWalHeaderSize - 4U,
        chunkdb::Crc32(bytes.data(), chunkdb::kWalHeaderSize - 4U));
    std::size_t at = chunkdb::kWalHeaderSize;
    while (bytes.size() - at >= chunkdb::kWalFrameFixedHeaderSize) {
        const auto tlv = chunkdb::ReadLe16(bytes, at + 22U);
        const auto body = chunkdb::ReadLe32(bytes, at + 28U);
        const std::size_t header = chunkdb::kWalFrameFixedHeaderSize + tlv;
        if (header + 4U > bytes.size() - at) return;
        chunkdb::fuzz::Put32(bytes, at + header,
            chunkdb::Crc32(bytes.data() + at + 4U, header - 4U));
        const std::size_t start = at + header + 4U;
        if (bytes.size() - start < 4U || body > bytes.size() - start - 4U) return;
        chunkdb::fuzz::Put32(bytes, start + body,
            chunkdb::Crc32(bytes.data() + start, body));
        at = start + body + 4U;
    }
#else
    if (bytes.size() < chunkdb::kImageFixedHeaderSize) return;
    const auto sections = chunkdb::ReadLe16(bytes, 10U);
    if (sections > chunkdb::kImageMaxSections) return;
    const auto end = chunkdb::kImageFixedHeaderSize + sections * chunkdb::kImageSectionEntrySize;
    if (end > bytes.size() || bytes.size() - end < 4U) return;
    chunkdb::fuzz::Put32(bytes, end, chunkdb::Crc32(bytes.data(), end));
    // Section CRCs are checked by the real decompressor/parser. Keeping them
    // intact makes mutations of sizes/flags reach their admission checks.
#endif
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    if (size == 0U || size > chunkdb::fuzz::kMaxInput) return 0;
    const auto& geometry = chunkdb::fuzz::Model(data[0]);
    Bytes bytes(data + 1U, data + size);
    Parse(bytes, geometry);
    RepairChecksums(bytes);
    Parse(bytes, geometry);
    return 0;
}
