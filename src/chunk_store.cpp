#include "chunkdb/chunk_store.hpp"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>

#include "chunkdb/bit_codec.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace chunkdb {

namespace {

constexpr std::uint16_t kChunkFileVersion = 1;
constexpr std::uint16_t kWalFileVersion = 2;

constexpr std::uint8_t kChunkMagic[8] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
constexpr std::uint8_t kWalMagic[8] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
constexpr std::uint8_t kWalRecordMagic[4] = {'D', 'L', 'T', '1'};

constexpr std::size_t kChunkHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
constexpr std::size_t kWalHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U;
constexpr std::size_t kWalRecordHeaderSize = 4U + 4U + 2U + 4U;
constexpr std::uint64_t kWriterHeartbeatIntervalMs = 250;
constexpr std::uint64_t kWriterStaleThresholdMs = 5000;
constexpr int kAtomicWriteRetryCount = 6;

std::uint64_t CurrentProcessIdValue() {
#ifdef _WIN32
    return static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    return static_cast<std::uint64_t>(::getpid());
#endif
}

bool IsAtomicWriteTransientError(const std::error_code& ec) {
    if (!ec) {
        return false;
    }
#ifdef _WIN32
    const int code = ec.value();
    return code == ERROR_ACCESS_DENIED ||
           code == ERROR_SHARING_VIOLATION ||
           code == ERROR_LOCK_VIOLATION ||
           code == ERROR_FILE_EXISTS ||
           code == ERROR_ALREADY_EXISTS ||
           code == ERROR_BUSY;
#else
    const auto c = static_cast<std::errc>(ec.value());
    return c == std::errc::permission_denied ||
           c == std::errc::device_or_resource_busy ||
           c == std::errc::resource_unavailable_try_again ||
           c == std::errc::operation_not_permitted ||
           c == std::errc::text_file_busy ||
           c == std::errc::file_exists;
#endif
}

std::filesystem::path BuildAtomicTmpPath(const std::filesystem::path& path) {
    static std::atomic<std::uint64_t> seq{0};
    const auto now = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto tid = static_cast<std::uint64_t>(
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    const auto id = seq.fetch_add(1, std::memory_order_relaxed) + 1;
    return path.string() + ".tmp." + std::to_string(CurrentProcessIdValue()) +
           "." + std::to_string(tid) +
           "." + std::to_string(now) +
           "." + std::to_string(id);
}

std::uint64_t UnixMillisNow() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::unordered_map<std::string, std::string> ParseKv(const std::string& text) {
    std::unordered_map<std::string, std::string> out;
    std::istringstream input(text);
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos || eq == 0 || eq + 1 >= line.size()) {
            continue;
        }
        out.emplace(line.substr(0, eq), line.substr(eq + 1));
    }
    return out;
}

bool TryParseInt64(const std::string& text, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long long value = std::stoll(text, &consumed, 10);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::int64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool TryParseUint64(const std::string& text, std::uint64_t* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const auto value = std::stoull(text, &consumed, 10);
        if (consumed != text.size()) {
            return false;
        }
        *out = static_cast<std::uint64_t>(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool IsProcessAlive(std::int64_t pid) {
#ifdef _WIN32
    HANDLE process = OpenProcess(SYNCHRONIZE, FALSE, static_cast<DWORD>(pid));
    if (process == nullptr) {
        return false;
    }
    DWORD exit_code = 0;
    const bool alive = GetExitCodeProcess(process, &exit_code) != 0 && exit_code == STILL_ACTIVE;
    CloseHandle(process);
    return alive;
#else
    if (pid <= 0) {
        return false;
    }
    if (::kill(static_cast<pid_t>(pid), 0) == 0) {
        return true;
    }
    return errno == EPERM;
#endif
}

std::string LoadTextFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return "";
    }
    std::string out(
        (std::istreambuf_iterator<char>(input)),
        std::istreambuf_iterator<char>());
    return out;
}

bool MetadataLooksStale(const std::unordered_map<std::string, std::string>& meta) {
    const auto now_ms = UnixMillisNow();

    std::int64_t pid = -1;
    std::uint64_t heartbeat_ms = 0;
    const auto pid_it = meta.find("pid");
    const auto hb_it = meta.find("heartbeat_ms");

    if (pid_it == meta.end() || hb_it == meta.end()) {
        return true;
    }
    if (!TryParseInt64(pid_it->second, &pid) || !TryParseUint64(hb_it->second, &heartbeat_ms)) {
        return true;
    }

    const bool pid_alive = IsProcessAlive(pid);
    const bool heartbeat_stale = now_ms > heartbeat_ms && (now_ms - heartbeat_ms) > kWriterStaleThresholdMs;
    return !pid_alive || heartbeat_stale;
}

std::string GenerateSessionId() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<std::uint64_t> dist;
    std::ostringstream out;
    out << std::hex << UnixMillisNow() << "-" << dist(gen);
    return out.str();
}


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

void SyncFilePath(const std::filesystem::path& path) {
#ifdef _WIN32
    const int fd = _open(path.string().c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) {
        throw std::runtime_error("failed to open file for durability sync: " + path.string());
    }
    const int result = _commit(fd);
    _close(fd);
    if (result != 0) {
        throw std::runtime_error("failed to sync file: " + path.string());
    }
#else
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw std::runtime_error("failed to open file for durability sync: " + path.string());
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        throw std::runtime_error("failed to sync file: " + path.string());
    }
    ::close(fd);
#endif
}

void SyncDirectoryPath(const std::filesystem::path& path) {
#ifdef _WIN32
    (void)path;
#else
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return;
    }
    (void)::fsync(fd);
    ::close(fd);
#endif
}

std::vector<std::uint8_t> SerializeChunkImage(
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    const std::vector<std::uint8_t>& payload) {
    if (payload.size() != geometry.ChunkPayloadBytes()) {
        throw std::invalid_argument("payload size does not match geometry");
    }

    std::vector<std::uint8_t> bytes;
    bytes.reserve(64U + payload.size());

    bytes.insert(bytes.end(), kChunkMagic, kChunkMagic + 8);
    WriteLe16(bytes, kChunkFileVersion);
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

std::vector<std::uint8_t> BuildWalHeader(const Geometry& geometry, const ChunkCoord& chunk_coord) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(kWalHeaderSize);

    bytes.insert(bytes.end(), kWalMagic, kWalMagic + 8);
    WriteLe16(bytes, kWalFileVersion);
    WriteLe16(bytes, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(bytes, geometry.config().chunk_width_blocks);
    WriteLe32(bytes, geometry.config().chunk_height_blocks);
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.x));
    WriteLe64(bytes, static_cast<std::uint64_t>(chunk_coord.y));

    return bytes;
}

void ValidateWalHeader(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kWalHeaderSize) {
        throw std::runtime_error("WAL file too small");
    }
    if (std::memcmp(bytes.data(), kWalMagic, 8U) != 0) {
        throw std::runtime_error("invalid WAL magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kWalFileVersion) {
        throw std::runtime_error("unsupported WAL version");
    }

    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);
    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("WAL geometry mismatch");
    }

    const auto chunk_x = static_cast<std::int64_t>(ReadLe64(bytes, 20U));
    const auto chunk_y = static_cast<std::int64_t>(ReadLe64(bytes, 28U));
    if (chunk_x != expected_chunk_coord.x || chunk_y != expected_chunk_coord.y) {
        throw std::runtime_error("WAL chunk coordinate mismatch");
    }
}

std::vector<std::uint8_t> ParseChunkImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const ChunkCoord& expected_chunk_coord) {
    if (bytes.size() < kChunkHeaderSize) {
        throw std::runtime_error("chunk file too small");
    }

    if (std::memcmp(bytes.data(), kChunkMagic, 8U) != 0) {
        throw std::runtime_error("invalid chunk magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kChunkFileVersion) {
        throw std::runtime_error("unsupported chunk file version");
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

    if (bytes.size() != kChunkHeaderSize + payload_size) {
        throw std::runtime_error("incomplete payload");
    }

    std::vector<std::uint8_t> payload(bytes.begin() + static_cast<std::ptrdiff_t>(kChunkHeaderSize), bytes.end());
    if (Crc32(payload) != payload_crc) {
        throw std::runtime_error("payload checksum mismatch");
    }

    return payload;
}

void ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload) {
    if (payload == nullptr) {
        throw std::invalid_argument("payload must not be null");
    }

    if (wal_bytes.empty()) {
        return;
    }

    std::size_t cursor = 0;
    if (wal_bytes.size() >= kWalHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalMagic, 8U) == 0) {
        try {
            ValidateWalHeader(wal_bytes, geometry, chunk_coord);
            cursor = kWalHeaderSize;
        } catch (...) {
            return;
        }
    } else if (
        wal_bytes.size() >= kWalRecordHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalRecordMagic, 4U) == 0) {
        // Headerless WAL can appear if a writer recreated WAL and appended records
        // across a file replacement race; replay from record stream start.
        cursor = 0;
    } else {
        // Unknown leading bytes: treat as non-replayable WAL content.
        return;
    }

    while (cursor < wal_bytes.size()) {
        const std::size_t remaining = wal_bytes.size() - cursor;
        if (remaining < kWalRecordHeaderSize) {
            break;
        }

        // If a repeated WAL header is encountered mid-stream, skip it and continue.
        if (remaining >= kWalHeaderSize &&
            std::memcmp(wal_bytes.data() + cursor, kWalMagic, 8U) == 0) {
            const std::uint16_t version = ReadLe16(wal_bytes, cursor + 8U);
            const std::uint16_t block_bits = ReadLe16(wal_bytes, cursor + 10U);
            const std::uint32_t chunk_width = ReadLe32(wal_bytes, cursor + 12U);
            const std::uint32_t chunk_height = ReadLe32(wal_bytes, cursor + 16U);
            const auto header_chunk_x = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 20U));
            const auto header_chunk_y = static_cast<std::int64_t>(ReadLe64(wal_bytes, cursor + 28U));

            if (version == kWalFileVersion &&
                block_bits == geometry.config().block_bits &&
                chunk_width == geometry.config().chunk_width_blocks &&
                chunk_height == geometry.config().chunk_height_blocks &&
                header_chunk_x == chunk_coord.x &&
                header_chunk_y == chunk_coord.y) {
                cursor += kWalHeaderSize;
                continue;
            }

            break;
        }

        if (std::memcmp(wal_bytes.data() + cursor, kWalRecordMagic, 4U) != 0) {
            break;
        }

        const std::uint32_t byte_offset = ReadLe32(wal_bytes, cursor + 4U);
        const std::uint16_t data_size = ReadLe16(wal_bytes, cursor + 8U);
        const std::uint32_t record_crc = ReadLe32(wal_bytes, cursor + 10U);

        const std::size_t full_record_size = kWalRecordHeaderSize + data_size;
        if (remaining < full_record_size) {
            break;
        }

        const std::size_t payload_end = static_cast<std::size_t>(byte_offset) + data_size;
        if (payload_end > payload->size()) {
            break;
        }

        const std::uint8_t* record_data = wal_bytes.data() + cursor + kWalRecordHeaderSize;
        const std::uint32_t computed_crc = Crc32(record_data, data_size);
        if (computed_crc != record_crc) {
            break;
        }

        std::copy(
            record_data,
            record_data + data_size,
            payload->begin() + static_cast<std::ptrdiff_t>(byte_offset));

        cursor += full_record_size;
    }
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

void AtomicWrite(
    const std::filesystem::path& path,
    const std::vector<std::uint8_t>& bytes,
    bool fsync_file,
    bool fsync_directory) {
    const auto parent = path.parent_path();
    std::filesystem::create_directories(parent);

    const std::filesystem::path tmp_path = BuildAtomicTmpPath(path);

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

    std::error_code rename_ec;
    for (int attempt = 0; attempt < kAtomicWriteRetryCount; ++attempt) {
        rename_ec.clear();
        std::filesystem::rename(tmp_path, path, rename_ec);
        if (!rename_ec) {
            break;
        }

        std::error_code remove_ec;
        std::filesystem::remove(path, remove_ec);
        rename_ec.clear();
        std::filesystem::rename(tmp_path, path, rename_ec);
        if (!rename_ec) {
            break;
        }

        if (attempt + 1 < kAtomicWriteRetryCount && IsAtomicWriteTransientError(rename_ec)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2 * (attempt + 1)));
            continue;
        }

        break;
    }

    if (rename_ec) {
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw std::runtime_error(
            "failed to move temporary file into place: " + path.string() +
            " (tmp=" + tmp_path.string() +
            ", ec=" + std::to_string(rename_ec.value()) +
            ", msg='" + rename_ec.message() + "')");
    }

    if (fsync_file) {
        SyncFilePath(path);
    }
    if (fsync_directory) {
        SyncDirectoryPath(parent);
    }
}

}  // namespace

DurabilityMode ParseDurabilityMode(std::string_view text) {
    if (text == "relaxed") {
        return DurabilityMode::kRelaxed;
    }
    if (text == "fsync-wal") {
        return DurabilityMode::kFsyncWal;
    }
    if (text == "fsync-checkpoint") {
        return DurabilityMode::kFsyncCheckpoint;
    }
    throw std::invalid_argument(
        "invalid durability mode: " + std::string(text) +
        " (expected relaxed|fsync-wal|fsync-checkpoint)");
}

const char* DurabilityModeName(DurabilityMode mode) noexcept {
    switch (mode) {
        case DurabilityMode::kRelaxed:
            return "relaxed";
        case DurabilityMode::kFsyncWal:
            return "fsync-wal";
        case DurabilityMode::kFsyncCheckpoint:
            return "fsync-checkpoint";
    }
    return "unknown";
}

const char* AccessModeName(AccessMode mode) noexcept {
    switch (mode) {
        case AccessMode::kReadWrite:
            return "read-write";
        case AccessMode::kReadOnly:
            return "read-only";
    }
    return "unknown";
}

ChunkStore::ChunkStore(StoreConfig config)
    : geometry_(config.geometry),
      data_dir_(std::move(config.data_dir)),
      durability_mode_(config.durability_mode),
      access_mode_(config.access_mode),
      checkpoint_update_interval_(config.checkpoint_update_interval),
      checkpoint_wal_bytes_(config.checkpoint_wal_bytes),
      wal_group_commit_updates_(config.wal_group_commit_updates),
      max_loaded_chunks_(config.max_loaded_chunks) {
    if (data_dir_.empty()) {
        throw std::invalid_argument("data_dir must not be empty");
    }
    if (checkpoint_update_interval_ == 0) {
        throw std::invalid_argument("checkpoint_update_interval must be > 0");
    }
    if (checkpoint_wal_bytes_ == 0) {
        throw std::invalid_argument("checkpoint_wal_bytes must be > 0");
    }
    if (wal_group_commit_updates_ == 0) {
        throw std::invalid_argument("wal_group_commit_updates must be > 0");
    }
    if (max_loaded_chunks_ == 0) {
        throw std::invalid_argument("max_loaded_chunks must be > 0");
    }

    std::filesystem::create_directories(data_dir_);
    AcquireProcessLock(config.allow_multiple_processes);
}

ChunkStore::~ChunkStore() {
    FlushAllPendingWalBatches();
    ReleaseProcessLock();
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
    if (access_mode_ == AccessMode::kReadOnly) {
        throw std::invalid_argument("store is read-only");
    }
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

    const std::size_t begin_byte = bit_offset / 8U;
    const std::size_t end_byte = (bit_offset + bits.size() - 1U) / 8U;
    const std::size_t touched_bytes = end_byte - begin_byte + 1U;

    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::unique_lock lock(regular_chunk->mutex);

    auto& previous_bytes = regular_chunk->scratch_before;
    previous_bytes.resize(touched_bytes);
    std::copy_n(
        regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte),
        static_cast<std::ptrdiff_t>(touched_bytes),
        previous_bytes.begin());

    BitCodec::WriteBits(regular_chunk->payload, bit_offset, bits);

    bool changed = false;
    for (std::size_t i = 0; i < touched_bytes; ++i) {
        if (previous_bytes[i] != regular_chunk->payload[begin_byte + i]) {
            changed = true;
            break;
        }
    }

    if (!changed) {
        return;
    }

    try {
        std::size_t appended_bytes = 0;
        AppendWalDelta(
            chunk_coord,
            regular_chunk,
            static_cast<std::uint32_t>(begin_byte),
            regular_chunk->payload.data() + begin_byte,
            touched_bytes,
            &appended_bytes);

        regular_chunk->pending_updates += 1;
        regular_chunk->wal_bytes += appended_bytes;
        MaybeCheckpointChunk(chunk_coord, regular_chunk);
    } catch (...) {
        std::copy(
            previous_bytes.begin(),
            previous_bytes.end(),
            regular_chunk->payload.begin() + static_cast<std::ptrdiff_t>(begin_byte));
        throw;
    }
}

std::string ChunkStore::GetChunkBits(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return BitCodec::ExtractBits(regular_chunk->payload, 0, geometry_.ChunkPayloadBits());
}

std::vector<std::uint8_t> ChunkStore::GetChunkPayloadBytes(std::int64_t chunk_x, std::int64_t chunk_y) {
    const ChunkCoord chunk_coord{chunk_x, chunk_y};
    const auto regular_chunk = GetOrLoadRegularChunk(chunk_coord);
    std::shared_lock lock(regular_chunk->mutex);
    return regular_chunk->payload;
}

std::size_t ChunkStore::ApproxLoadedChunkCount() const {
    std::size_t loaded = 0;
    std::lock_guard global_lock(large_chunks_mutex_);
    for (const auto& [_, large_chunk] : large_chunks_) {
        std::lock_guard chunk_lock(large_chunk->mutex);
        loaded += large_chunk->chunks.size();
    }
    return loaded;
}

StoreRuntimeStats ChunkStore::RuntimeStats() const noexcept {
    return StoreRuntimeStats{
        .evictions = stats_evictions_.load(std::memory_order_relaxed),
        .checkpoints = stats_checkpoints_.load(std::memory_order_relaxed),
        .wal_batch_flushes = stats_wal_batch_flushes_.load(std::memory_order_relaxed),
        .unique_loaded_chunks = stats_unique_loaded_chunks_.load(std::memory_order_relaxed),
    };
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

    bool inserted = false;
    std::shared_ptr<RegularChunk> selected;
    {
        std::lock_guard lock(large_chunk->mutex);
        auto it = large_chunk->chunks.find(chunk_coord);
        if (it != large_chunk->chunks.end()) {
            selected = it->second;
        } else {
            const auto loaded_payload = LoadChunkPayload(chunk_coord);
            selected = std::make_shared<RegularChunk>(loaded_payload);
            large_chunk->chunks.emplace(chunk_coord, selected);
            inserted = true;
        }
    }

    TouchChunk(selected);
    if (inserted) {
        const auto loaded_now = loaded_chunk_count_.fetch_add(1, std::memory_order_relaxed) + 1;
        stats_unique_loaded_chunks_.fetch_add(1, std::memory_order_relaxed);
        if (loaded_now > max_loaded_chunks_) {
            MaybeEvictChunks();
        }
    }
    return selected;
}

std::vector<std::uint8_t> ChunkStore::EmptyPayload() const {
    return std::vector<std::uint8_t>(geometry_.ChunkPayloadBytes(), 0U);
}

std::vector<std::uint8_t> ChunkStore::LoadChunkPayload(const ChunkCoord& chunk_coord) {
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    std::vector<std::uint8_t> payload;
    if (std::filesystem::exists(data_path)) {
        try {
            const auto data_bytes = LoadFile(data_path);
            payload = ParseChunkImage(data_bytes, geometry_, chunk_coord);
        } catch (...) {
            // The image can be replaced concurrently by atomic checkpoint rename.
            // If it disappeared during open, fall back to empty payload.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            payload = EmptyPayload();
        }
    } else {
        payload = EmptyPayload();
    }

    if (std::filesystem::exists(wal_path)) {
        std::vector<std::uint8_t> wal_bytes;
        try {
            wal_bytes = LoadFile(wal_path);
        } catch (...) {
            // WAL can be removed concurrently by checkpoint cleanup.
            // If it no longer exists, treat as already checkpointed.
            if (std::filesystem::exists(wal_path)) {
                throw;
            }
            return payload;
        }

        ReplayWal(wal_bytes, geometry_, chunk_coord, &payload);

        const auto checkpoint_bytes = SerializeChunkImage(geometry_, chunk_coord, payload);
        AtomicWrite(
            data_path,
            checkpoint_bytes,
            durability_mode_ == DurabilityMode::kFsyncCheckpoint,
            durability_mode_ == DurabilityMode::kFsyncCheckpoint);
        stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);

        std::error_code ec;
        std::filesystem::remove(wal_path, ec);
        if (durability_mode_ == DurabilityMode::kFsyncCheckpoint) {
            SyncDirectoryPath(data_path.parent_path());
        }
    }

    return payload;
}

void ChunkStore::TouchChunk(const std::shared_ptr<RegularChunk>& chunk) noexcept {
    const std::uint64_t tick = access_clock_.fetch_add(1, std::memory_order_relaxed) + 1U;
    chunk->last_access_tick.store(tick, std::memory_order_relaxed);
}

void ChunkStore::MaybeEvictChunks() {
    const auto loaded_hint = loaded_chunk_count_.load(std::memory_order_relaxed);
    if (loaded_hint <= max_loaded_chunks_) {
        return;
    }

    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }

    struct Candidate {
        std::shared_ptr<LargeChunk> large_chunk;
        ChunkCoord chunk_coord;
        std::uint64_t tick;
    };

    std::size_t total_loaded = 0;
    std::vector<Candidate> candidates;

    for (const auto& large_chunk : large_chunks) {
        std::lock_guard lock(large_chunk->mutex);
        total_loaded += large_chunk->chunks.size();
        for (const auto& [coord, regular_chunk] : large_chunk->chunks) {
            if (regular_chunk.use_count() == 1) {
                candidates.push_back(Candidate{
                    .large_chunk = large_chunk,
                    .chunk_coord = coord,
                    .tick = regular_chunk->last_access_tick.load(std::memory_order_relaxed),
                });
            }
        }
    }

    if (total_loaded <= max_loaded_chunks_) {
        return;
    }

    const std::size_t required = total_loaded - max_loaded_chunks_;
    if (required < candidates.size()) {
        std::nth_element(
            candidates.begin(),
            candidates.begin() + static_cast<std::ptrdiff_t>(required),
            candidates.end(),
            [](const Candidate& lhs, const Candidate& rhs) { return lhs.tick < rhs.tick; });
        candidates.resize(required);
    }

    std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
        return lhs.tick < rhs.tick;
    });

    std::size_t removed = 0;

    for (const auto& candidate : candidates) {
        if (removed >= required) {
            break;
        }

        std::lock_guard lock(candidate.large_chunk->mutex);
        auto it = candidate.large_chunk->chunks.find(candidate.chunk_coord);
        if (it == candidate.large_chunk->chunks.end()) {
            continue;
        }

        const auto& regular_chunk = it->second;
        if (regular_chunk.use_count() != 1) {
            continue;
        }

        {
            std::unique_lock chunk_lock(regular_chunk->mutex);
            FlushWalBatch(
                candidate.chunk_coord,
                regular_chunk,
                durability_mode_ != DurabilityMode::kRelaxed);
        }

        if (regular_chunk.use_count() != 1) {
            continue;
        }

        candidate.large_chunk->chunks.erase(it);
        ++removed;
    }

    if (removed == 0) {
        return;
    }

    loaded_chunk_count_.fetch_sub(removed, std::memory_order_relaxed);
    stats_evictions_.fetch_add(removed, std::memory_order_relaxed);

    std::lock_guard global_lock(large_chunks_mutex_);
    for (auto it = large_chunks_.begin(); it != large_chunks_.end();) {
        std::lock_guard large_lock(it->second->mutex);
        if (it->second->chunks.empty()) {
            it = large_chunks_.erase(it);
        } else {
            ++it;
        }
    }
}

void ChunkStore::AppendWalDelta(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    std::uint32_t byte_offset,
    const std::uint8_t* payload_bytes,
    std::size_t payload_size,
    std::size_t* appended_record_bytes) {
    if (payload_bytes == nullptr || payload_size == 0) {
        throw std::invalid_argument("WAL delta payload must not be empty");
    }
    if (payload_size > std::numeric_limits<std::uint16_t>::max()) {
        throw std::invalid_argument("WAL delta payload too large");
    }

    const std::size_t record_size = kWalRecordHeaderSize + payload_size;

    auto& batch = chunk->wal_batch;
    batch.reserve(batch.size() + record_size);
    batch.insert(batch.end(), kWalRecordMagic, kWalRecordMagic + 4);
    WriteLe32(batch, byte_offset);
    WriteLe16(batch, static_cast<std::uint16_t>(payload_size));
    WriteLe32(batch, Crc32(payload_bytes, payload_size));
    batch.insert(batch.end(), payload_bytes, payload_bytes + payload_size);

    chunk->pending_wal_flush_updates += 1;

    const bool sync_required = durability_mode_ != DurabilityMode::kRelaxed;
    if (sync_required || chunk->pending_wal_flush_updates >= wal_group_commit_updates_) {
        FlushWalBatch(chunk_coord, chunk, sync_required);
    }

    if (appended_record_bytes != nullptr) {
        *appended_record_bytes = record_size;
    }
}

void ChunkStore::FlushWalBatch(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk,
    bool force_sync) {
    if (chunk->wal_batch.empty()) {
        return;
    }

    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);
    std::filesystem::create_directories(wal_path.parent_path());

    std::ofstream output(wal_path, std::ios::binary | std::ios::app);
    if (!output) {
        throw std::runtime_error("failed to open WAL file for append: " + wal_path.string());
    }

    std::error_code wal_size_ec;
    const auto existing_size = std::filesystem::file_size(wal_path, wal_size_ec);
    const bool needs_header = wal_size_ec || existing_size == 0U;
    if (needs_header) {
        const auto wal_header = BuildWalHeader(geometry_, chunk_coord);
        output.write(
            reinterpret_cast<const char*>(wal_header.data()),
            static_cast<std::streamsize>(wal_header.size()));
    }

    output.write(
        reinterpret_cast<const char*>(chunk->wal_batch.data()),
        static_cast<std::streamsize>(chunk->wal_batch.size()));
    output.flush();
    if (!output.good()) {
        throw std::runtime_error("failed to append WAL record batch: " + wal_path.string());
    }

    if (force_sync) {
        SyncFilePath(wal_path);
    }

    stats_wal_batch_flushes_.fetch_add(1, std::memory_order_relaxed);
    chunk->wal_batch.clear();
    chunk->pending_wal_flush_updates = 0;
}

void ChunkStore::FlushAllPendingWalBatches() noexcept {
    std::vector<std::shared_ptr<LargeChunk>> large_chunks;
    {
        std::lock_guard lock(large_chunks_mutex_);
        large_chunks.reserve(large_chunks_.size());
        for (const auto& [_, large_chunk] : large_chunks_) {
            large_chunks.push_back(large_chunk);
        }
    }

    for (const auto& large_chunk : large_chunks) {
        std::vector<std::pair<ChunkCoord, std::shared_ptr<RegularChunk>>> chunks;
        {
            std::lock_guard lock(large_chunk->mutex);
            chunks.reserve(large_chunk->chunks.size());
            for (const auto& [coord, chunk] : large_chunk->chunks) {
                chunks.emplace_back(coord, chunk);
            }
        }

        for (const auto& [coord, chunk] : chunks) {
            std::unique_lock chunk_lock(chunk->mutex);
            try {
                FlushWalBatch(
                    coord,
                    chunk,
                    durability_mode_ != DurabilityMode::kRelaxed);
            } catch (...) {
                // Destructor-time best effort: avoid throwing.
            }
        }
    }
}

void ChunkStore::MaybeCheckpointChunk(
    const ChunkCoord& chunk_coord,
    const std::shared_ptr<RegularChunk>& chunk) {
    if (chunk->pending_updates < checkpoint_update_interval_ && chunk->wal_bytes < checkpoint_wal_bytes_) {
        return;
    }
    CheckpointChunk(chunk_coord, chunk);
}

void ChunkStore::CheckpointChunk(const ChunkCoord& chunk_coord, const std::shared_ptr<RegularChunk>& chunk) {
    const auto data_path = ChunkDataPath(data_dir_, geometry_, chunk_coord);
    const auto wal_path = ChunkWalPath(data_dir_, geometry_, chunk_coord);

    const auto image = SerializeChunkImage(geometry_, chunk_coord, chunk->payload);
    const bool strict = durability_mode_ == DurabilityMode::kFsyncCheckpoint;
    AtomicWrite(data_path, image, strict, strict);

    std::error_code ec;
    std::filesystem::remove(wal_path, ec);
    if (strict) {
        SyncDirectoryPath(data_path.parent_path());
    }

    stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);
    chunk->pending_updates = 0;
    chunk->wal_bytes = 0;
    chunk->pending_wal_flush_updates = 0;
    chunk->wal_batch.clear();
}

std::string ChunkStore::BuildWriterMetadata() const {
#ifdef _WIN32
    const std::uint64_t pid = static_cast<std::uint64_t>(GetCurrentProcessId());
#else
    const std::uint64_t pid = static_cast<std::uint64_t>(::getpid());
#endif

    std::ostringstream out;
    out << "version=1\n";
    out << "mode=writer\n";
    out << "session_id=" << process_lock_session_id_ << "\n";
    out << "pid=" << pid << "\n";
    out << "heartbeat_ms=" << UnixMillisNow() << "\n";
    out << "stale_after_ms=" << kWriterStaleThresholdMs << "\n";
    return out.str();
}

void ChunkStore::WriteWriterMetadata() {
    if (process_lock_meta_path_.empty()) {
        return;
    }

    const std::string text = BuildWriterMetadata();
    const std::vector<std::uint8_t> bytes(text.begin(), text.end());

    std::lock_guard lock(process_lock_meta_mutex_);
    AtomicWrite(process_lock_meta_path_, bytes, false, false);
}

void ChunkStore::StartWriterHeartbeat() {
    process_lock_heartbeat_stop_.store(false, std::memory_order_release);
    process_lock_heartbeat_thread_ = std::thread([this]() {
        while (!process_lock_heartbeat_stop_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(kWriterHeartbeatIntervalMs));
            if (process_lock_heartbeat_stop_.load(std::memory_order_acquire)) {
                break;
            }
            try {
                WriteWriterMetadata();
            } catch (...) {
                // Heartbeat failures are best-effort; writer ownership still relies on OS file lock.
            }
        }
    });
}

void ChunkStore::StopWriterHeartbeat() noexcept {
    process_lock_heartbeat_stop_.store(true, std::memory_order_release);
    if (process_lock_heartbeat_thread_.joinable()) {
        process_lock_heartbeat_thread_.join();
    }
}

void ChunkStore::AcquireProcessLock(bool allow_multiple_processes) {
    if (allow_multiple_processes || access_mode_ == AccessMode::kReadOnly) {
        return;
    }

    process_lock_dir_ = data_dir_ / ".chunkdb.lock";
    process_lock_file_path_ = process_lock_dir_ / "writer.lock";
    process_lock_meta_path_ = process_lock_dir_ / "writer.meta";

    std::filesystem::create_directories(process_lock_dir_);

#ifdef _WIN32
    HANDLE handle = CreateFileA(
        process_lock_file_path_.string().c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0,
        nullptr,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw std::runtime_error(
            "failed to acquire writer lock file: " + process_lock_file_path_.string());
    }
    process_lock_handle_ = handle;
#else
    const int fd = ::open(process_lock_file_path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        throw std::runtime_error(
            "failed to open writer lock file: " + process_lock_file_path_.string());
    }

    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const std::string existing_meta = LoadTextFile(process_lock_meta_path_);
        const auto parsed = ParseKv(existing_meta);
        std::string detail;
        if (!parsed.empty()) {
            const auto sid = parsed.find("session_id");
            const auto pid = parsed.find("pid");
            if (sid != parsed.end()) {
                detail += " session=" + sid->second;
            }
            if (pid != parsed.end()) {
                detail += " pid=" + pid->second;
            }
        }
        ::close(fd);
        throw std::runtime_error(
            "data directory already has an active writer:" + detail +
            " (dir=" + data_dir_.string() + ")");
    }

    process_lock_fd_ = fd;
#endif

    const std::string existing_meta = LoadTextFile(process_lock_meta_path_);
    if (!existing_meta.empty()) {
        const auto parsed = ParseKv(existing_meta);
        if (MetadataLooksStale(parsed)) {
            const auto stale_path = process_lock_dir_ /
                                    ("writer.meta.stale." + std::to_string(UnixMillisNow()));
            std::error_code ec;
            std::filesystem::rename(process_lock_meta_path_, stale_path, ec);
        }
    }

    process_lock_session_id_ = GenerateSessionId();
    WriteWriterMetadata();
    StartWriterHeartbeat();
}

void ChunkStore::ReleaseProcessLock() noexcept {
    StopWriterHeartbeat();

    if (!process_lock_meta_path_.empty()) {
        std::error_code ec;
        std::filesystem::remove(process_lock_meta_path_, ec);
    }

#ifdef _WIN32
    if (process_lock_handle_ != nullptr) {
        const HANDLE handle = static_cast<HANDLE>(process_lock_handle_);
        CloseHandle(handle);
        process_lock_handle_ = nullptr;
    }
#else
    if (process_lock_fd_ >= 0) {
        (void)::flock(process_lock_fd_, LOCK_UN);
        (void)::close(process_lock_fd_);
        process_lock_fd_ = -1;
    }
#endif

    process_lock_session_id_.clear();
    process_lock_meta_path_.clear();
    process_lock_file_path_.clear();
    process_lock_dir_.clear();
}

}  // namespace chunkdb
