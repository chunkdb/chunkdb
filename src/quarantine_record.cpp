#include "quarantine_record.hpp"

#include <algorithm>
#include <array>
#include <stdexcept>

#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace chunkdb {
namespace {

constexpr std::array<std::uint8_t, 4U> kMagic{'C', 'K', 'Q', 'R'};
constexpr std::uint16_t kVersion = 1U;
constexpr std::size_t kMaxFileName = 256U;

void Validate(const QuarantineRecord& record) {
    if (record.dims != 2U && record.dims != 3U) throw std::runtime_error("quarantine record has invalid dimensions");
    if (record.dims == 2U && record.coord.z != 0) throw std::runtime_error("quarantine record has an inactive coordinate");
    if (record.files.empty() || record.files.size() > 2U) throw std::runtime_error("quarantine record must name one or two files");
    const auto stem = ChunkFileStem(record.coord, record.dims);
    for (const auto& file : record.files) {
        if (file.name != stem + ".chk" && file.name != stem + ".wal")
            throw std::runtime_error("quarantine record names a file of another chunk: " + file.name);
    }
    if (record.files.size() == 2U && record.files[0].name == record.files[1].name)
        throw std::runtime_error("quarantine record names a file twice");
}

}  // namespace

std::vector<std::uint8_t> SerializeQuarantineRecord(const QuarantineRecord& record) {
    Validate(record);
    std::vector<std::uint8_t> bytes(kMagic.begin(), kMagic.end());
    WriteLe16(bytes, kVersion);
    bytes.push_back(record.dims);
    bytes.push_back(0U);
    bytes.insert(bytes.end(), record.table_epoch.begin(), record.table_epoch.end());
    WriteLe64(bytes, static_cast<std::uint64_t>(record.coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(record.coord.y));
    if (record.dims == 3U) WriteLe64(bytes, static_cast<std::uint64_t>(record.coord.z));
    WriteLe64(bytes, record.quarantined_at_ms);
    WriteLe32(bytes, static_cast<std::uint32_t>(record.files.size()));
    for (const auto& file : record.files) {
        WriteLe32(bytes, static_cast<std::uint32_t>(file.name.size()));
        bytes.insert(bytes.end(), file.name.begin(), file.name.end());
        WriteLe64(bytes, file.size);
        WriteLe32(bytes, file.crc32);
    }
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}

QuarantineRecord ParseQuarantineRecord(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 4U + 2U + 2U + 16U + 16U + 8U + 4U + 4U || !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
        ReadLe32(bytes, bytes.size() - 4U) != Crc32(bytes.data(), bytes.size() - 4U))
        throw std::runtime_error("invalid quarantine record or checksum");
    const std::size_t end = bytes.size() - 4U;
    std::size_t at = 4U;
    const auto need = [&](std::size_t count) {
        if (count > end - at) throw std::runtime_error("truncated quarantine record");
    };
    const auto u64 = [&] { need(8U); const auto value = ReadLe64(bytes, at); at += 8U; return value; };
    const auto u32 = [&] { need(4U); const auto value = ReadLe32(bytes, at); at += 4U; return value; };
    if (ReadLe16(bytes, at) != kVersion || bytes[at + 3U] != 0U) throw std::runtime_error("unsupported quarantine record version");
    QuarantineRecord record;
    record.dims = bytes[at + 2U];
    at += 4U;
    need(16U);
    std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(at), record.table_epoch.size(), record.table_epoch.begin());
    at += record.table_epoch.size();
    record.coord.x = static_cast<std::int64_t>(u64());
    record.coord.y = static_cast<std::int64_t>(u64());
    if (record.dims == 3U) record.coord.z = static_cast<std::int64_t>(u64());
    record.quarantined_at_ms = u64();
    const auto count = u32();
    if (count == 0U || count > 2U) throw std::runtime_error("quarantine record must name one or two files");
    for (std::uint32_t i = 0; i < count; ++i) {
        QuarantineFile file;
        const auto length = u32();
        if (length == 0U || length > kMaxFileName) throw std::runtime_error("invalid quarantine file name");
        need(length);
        file.name.assign(bytes.begin() + static_cast<std::ptrdiff_t>(at), bytes.begin() + static_cast<std::ptrdiff_t>(at + length));
        at += length;
        file.size = u64();
        file.crc32 = u32();
        record.files.push_back(std::move(file));
    }
    if (at != end) throw std::runtime_error("quarantine record has trailing bytes");
    Validate(record);
    return record;
}

}  // namespace chunkdb
