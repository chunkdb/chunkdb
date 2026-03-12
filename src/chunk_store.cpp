#include "chunkdb/chunk_store.hpp"

#include <chrono>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <system_error>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

namespace chunkdb {

namespace {

constexpr std::uint16_t kFileVersion = 1;
constexpr std::uint8_t kChunkMagic[8] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
constexpr std::uint8_t kWalMagic[8] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '1'};

void WriteLe16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
}

void WriteLe32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    out.push_back(static_cast<std::uint8_t>(value & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 8U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 16U) & 0xFFU));
    out.push_back(static_cast<std::uint8_t>((value >> 24U) & 0xFFU));
}

void WriteLe64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFU));
    }
}

std::uint16_t ReadLe16(const std::vector<std::uint8_t>& data, std::size_t offset) {
    return static_cast<std::uint16_t>(
        static_cast<std::uint16_t>(data[offset]) |
        static_cast<std::uint16_t>(data[offset + 1] << 8U));
}

std::uint32_t ReadLe32(const std::vector<std::uint8_t>& data, std::size_t offset) {
    return static_cast<std::uint32_t>(
        static_cast<std::uint32_t>(data[offset]) |
        static_cast<std::uint32_t>(data[offset + 1] << 8U) |
        static_cast<std::uint32_t>(data[offset + 2] << 16U) |
        static_cast<std::uint32_t>(data[offset + 3] << 24U));
}

std::uint64_t ReadLe64(const std::vector<std::uint8_t>& data, std::size_t offset) {
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(data[offset + i]) << (8U * i);
    }
    return value;
}

std::vector<std::uint8_t> SerializeImage(
    const std::uint8_t magic[8],
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const std::vector<std::uint8_t>& payload) {
    if (payload.size() != geometry.ChunkPayloadBytes()) {
        throw std::invalid_argument("payload size does not match geometry");
    }

    std::vector<std::uint8_t> bytes;
    bytes.reserve(64U + payload.size());

    bytes.insert(bytes.end(), magic, magic + 8);
    WriteLe16(bytes, kFileVersion);
    WriteLe16(bytes, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(bytes, geometry.config().chunk_width_blocks);
    WriteLe32(bytes, geometry.config().chunk_height_blocks);
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));
    WriteLe32(bytes, static_cast<std::uint32_t>(payload.size()));
    WriteLe32(bytes, Crc32(payload));

    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto millis = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    WriteLe64(bytes, millis);

    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

std::vector<std::uint8_t> LoadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("failed to open file: " + path.string());
    }
    input.seekg(0, std::ios::end);
    const std::streamoff size = input.tellg();
    if (size < 0) {
        throw std::runtime_error("failed to read file size: " + path.string());
    }
    input.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()), size);
    }
    if (!input.good() && !input.eof()) {
        throw std::runtime_error("failed to read file: " + path.string());
    }
    return bytes;
}

void AtomicWrite(const std::filesystem::path& path, const std::vector<std::uint8_t>& bytes) {
    const auto parent = path.parent_path();
    std::filesystem::create_directories(parent);

    const auto unique_part = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const std::filesystem::path tmp_path = path.string() + ".tmp." + unique_part;

    {
        std::ofstream output(tmp_path, std::ios::binary | std::ios::trunc);
        if (!output) {
            throw std::runtime_error("failed to open temporary file: " + tmp_path.string());
        }
        if (!bytes.empty()) {
            output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        }
        output.flush();
        if (!output.good()) {
            throw std::runtime_error("failed to write temporary file: " + tmp_path.string());
        }
    }

    std::error_code ec;
    std::filesystem::rename(tmp_path, path, ec);
    if (ec) {
        std::filesystem::remove(path, ec);
        ec.clear();
        std::filesystem::rename(tmp_path, path, ec);
        if (ec) {
            std::filesystem::remove(tmp_path);
            throw std::runtime_error("failed to move temporary file into place: " + path.string());
        }
    }
}

std::vector<std::uint8_t> ParseImage(
    const std::vector<std::uint8_t>& bytes,
    const std::uint8_t expected_magic[8],
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    constexpr std::size_t kHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
    if (bytes.size() < kHeaderSize) {
        throw std::runtime_error("file too small");
    }

    if (std::memcmp(bytes.data(), expected_magic, 8U) != 0) {
        throw std::runtime_error("invalid magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kFileVersion) {
        throw std::runtime_error("unsupported version");
    }

    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);

    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("geometry mismatch");
    }

    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 20U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 28U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("chunk coordinate mismatch");
    }

    const std::uint32_t payload_size = ReadLe32(bytes, 36U);
    const std::uint32_t payload_crc = ReadLe32(bytes, 40U);

    if (payload_size != geometry.ChunkPayloadBytes()) {
        throw std::runtime_error("payload size mismatch");
    }

    if (bytes.size() != kHeaderSize + payload_size) {
        throw std::runtime_error("incomplete payload");
    }

    std::vector<std::uint8_t> payload(bytes.begin() + static_cast<std::ptrdiff_t>(kHeaderSize), bytes.end());
    if (Crc32(payload) != payload_crc) {
        throw std::runtime_error("payload checksum mismatch");
    }

    return payload;
}

}  // namespace

ChunkStore::ChunkStore(StoreConfig config)
    : geometry_(config.geometry), data_dir_(std::move(config.data_dir)) {
    if (data_dir_.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    std::filesystem::create_directories(data_dir_);
}

std::string ChunkStore::GetBlockBits(std::int64_t block_x, std::int64_t block_y) {
    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, bit_offset, geometry_.config().block_bits);
}

void ChunkStore::SetBlockBits(std::int64_t block_x, std::int64_t block_y, std::string_view bits) {
    if (bits.size() != geometry_.config().block_bits) {
        throw std::invalid_argument("bit string length does not match configured block_bits");
    }

    if (!BitCodec::IsBitString(bits)) {
        throw std::invalid_argument("bit string must contain only 0 and 1");
    }

    const ChunkCoord chunk_coord = geometry_.BlockToChunk(block_x, block_y);
    const auto [local_x, local_y] = geometry_.BlockToLocal(block_x, block_y);
    const std::size_t block_index = geometry_.LocalBlockIndex(local_x, local_y);
    const std::size_t bit_offset = block_index * geometry_.config().block_bits;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto previous_payload = regular_chunk->payload;
    BitCodec::WriteBits(regular_chunk->payload, bit_offset, bits);

    try {
        PersistChunkPayload(chunk_coord, regular_chunk->payload);
    } catch (...) {
        regular_chunk->payload = std::move(previous_payload);
        throw;
    }
}

std::string ChunkStore::GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits());
}

std::shared_ptr<ChunkStore::LargeChunk> ChunkStore::GetOrCreateLargeChunk(const LargeChunkCoord& large_coord) {
    std::lock_guard lock(large_chunks_mutex_);
    auto it = large_chunks_.find(large_coord);
    if (it != large_chunks_.end()) {
        return it->second;
    }

    auto created = std::make_shared<LargeChunk>();
    large_chunks_.emplace(large_coord, created);
    return created;
}

std::shared_ptr<ChunkStore::RegularChunk> ChunkStore::GetOrLoadRegularChunk(const ChunkCoord& chunk_coord) {
    const LargeChunkCoord large_coord = geometry_.ChunkToLarge(chunk_coord);
    const auto large_chunk = GetOrCreateLargeChunk(large_coord);

    {
        std::lock_guard lock(large_chunk->mutex);
        auto it = large_chunk->chunks.find(chunk_coord);
        if (it != large_chunk->chunks.end()) {
            return it->second;
        }
    }

    const auto loaded_payload = LoadChunkPayload(chunk_coord);
    auto created_chunk = std::make_shared<RegularChunk>(loaded_payload);

    std::lock_guard lock(large_chunk->mutex);
    auto [it, inserted] = large_chunk->chunks.emplace(chunk_coord, created_chunk);
    if (!inserted) {
        return it->second;
    }
    return created_chunk;
}

std::vector<std::uint8_t> ChunkStore::EmptyPayload() const {
    return std::vector<std::uint8_t>(geometry_.ChunkPayloadBytes(), 0U);
}

std::vector<std::uint8_t> ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord) {
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    if (std::filesystem::exists(wal_path)) {
        const auto wal_bytes = LoadFile(wal_path);
        const auto recovered_payload = ParseImage(wal_bytes, kWalMagic, geometry_, chunk_coord);
        const auto recovered_data = SerializeImage(kChunkMagic, geometry_, chunk_coord, recovered_payload);
        AtomicWrite(data_path, recovered_data);
        std::filesystem::remove(wal_path);
        return recovered_payload;
    }

    if (!std::filesystem::exists(data_path)) {
        return EmptyPayload();
    }

    const auto data_bytes = LoadFile(data_path);
    return ParseImage(data_bytes, kChunkMagic, geometry_, chunk_coord);
}

void ChunkStore::PersistChunkPayload(const ChunkCoord& chunk_coord, const std::vector<std::uint8_t>& payload) {
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    const auto wal_bytes = SerializeImage(kWalMagic, geometry_, chunk_coord, payload);
    AtomicWrite(wal_path, wal_bytes);

    const auto data_bytes = SerializeImage(kChunkMagic, geometry_, chunk_coord, payload);
    AtomicWrite(data_path, data_bytes);

    std::error_code ec;
    std::filesystem::remove(wal_path, ec);
}

}  // namespace chunkdb
