#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

namespace chunkdb {

// `chunkdb_admin quarantine` moves a damaged chunk's files into an entry of
// this directory inside its table directory, with a record of what it moved
// (docs/STORAGE_FORMAT.md).
inline constexpr std::string_view kQuarantineDirName = ".chunkdb.quarantine";
inline constexpr std::string_view kQuarantineRecordName = "quarantine.record";

struct QuarantineFile {
    // The chunk's image or WAL name, such as C_1_0.chk.
    std::string name;
    std::uint64_t size = 0;
    std::uint32_t crc32 = 0;
};

struct QuarantineRecord {
    StoreId table_epoch{};
    std::uint8_t dims = 2;
    ChunkCoord coord{};
    std::uint64_t quarantined_at_ms = 0;
    // One or two files: the chunk's image, its WAL or both.
    std::vector<QuarantineFile> files;
};

[[nodiscard]] std::vector<std::uint8_t> SerializeQuarantineRecord(const QuarantineRecord& record);
// Throws std::runtime_error for a malformed record.
[[nodiscard]] QuarantineRecord ParseQuarantineRecord(const std::vector<std::uint8_t>& bytes);

}  // namespace chunkdb
