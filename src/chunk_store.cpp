#include "chunkdb/chunk_store.hpp"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
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
#include "chunkdb/logging.hpp"

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
constexpr std::uint16_t kRegionFileVersion = 1;

constexpr std::uint8_t kChunkMagic[8] = {'C', 'H', 'K', 'D', 'A', 'T', 'A', '1'};
constexpr std::uint8_t kWalMagic[8] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};
constexpr std::uint8_t kWalRecordMagic[4] = {'D', 'L', 'T', '1'};
constexpr std::uint8_t kRegionMagic[8] = {'C', 'H', 'K', 'R', 'G', 'N', '1', '0'};

constexpr std::size_t kChunkHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U + 4U + 4U + 8U;
constexpr std::size_t kWalHeaderSize = 8U + 2U + 2U + 4U + 4U + 8U + 8U;
constexpr std::size_t kWalRecordHeaderSize = 4U + 4U + 2U + 4U;
constexpr std::size_t kRegionHeaderSize = 8U + 2U + 2U + 4U + 4U + 4U + 8U + 8U + 4U + 4U;
constexpr std::uint64_t kWriterHeartbeatIntervalMs = 250;
constexpr std::uint64_t kWriterStaleThresholdMs = 5000;
constexpr std::uint64_t kAtomicTmpCurrentPidCleanupMinAgeMs = 500;
constexpr int kAtomicWriteRetryCount = 6;

struct StartupRecoveryScan {
    std::uint64_t wal_files = 0;
    std::uint64_t checkpoint_files = 0;
    std::uint64_t region_files = 0;
};

struct WalReplayResult {
    std::size_t applied_records = 0;
    bool replayable = false;
    bool tail_truncated_or_corrupt = false;
    std::string stop_reason;
};

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

StartupRecoveryScan ScanStartupRecovery(const std::filesystem::path& data_dir) {
    StartupRecoveryScan result;
    std::error_code exists_ec;
    if (!std::filesystem::exists(data_dir, exists_ec) || exists_ec) {
        return result;
    }

    const std::filesystem::recursive_directory_iterator end;
    std::error_code it_ec;
    for (std::filesystem::recursive_directory_iterator it(data_dir, it_ec);
         it != end && !it_ec;
         it.increment(it_ec)) {
        if (!it->is_regular_file()) {
            continue;
        }
        const auto ext = it->path().extension();
        if (ext == ".wal") {
            ++result.wal_files;
        } else if (ext == ".chk") {
            ++result.checkpoint_files;
        } else if (ext == ".rgn") {
            ++result.region_files;
        }
    }
    return result;
}

std::uint64_t UnixMillisNow() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::int64_t FloorDiv(std::int64_t value, std::int64_t divisor) {
    if (divisor <= 0) {
        throw std::invalid_argument("divisor must be > 0");
    }
    std::int64_t q = value / divisor;
    const std::int64_t r = value % divisor;
    if (r != 0 && ((r < 0) != (divisor < 0))) {
        --q;
    }
    return q;
}

bool ConsumeFailpointEnv(const char* key) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    (void)_putenv_s(key, "");
#else
    (void)unsetenv(key);
#endif
    return true;
}

[[nodiscard]] std::runtime_error BuildErrnoError(
    const std::string& action,
    const std::filesystem::path& path,
    int err) {
    return std::runtime_error(
        action + ": " + path.string() +
        " (errno=" + std::to_string(err) +
        ", msg='" + std::strerror(err) + "')");
}

#ifdef _WIN32
[[nodiscard]] std::runtime_error BuildWin32Error(
    const std::string& action,
    const std::filesystem::path& path,
    DWORD code) {
    return std::runtime_error(
        action + ": " + path.string() +
        " (win32=" + std::to_string(static_cast<unsigned long>(code)) +
        ", msg='" + std::system_category().message(static_cast<int>(code)) + "')");
}
#endif

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

struct RegionChunkAddress {
    std::int64_t region_x = 0;
    std::int64_t region_y = 0;
    std::uint32_t local_x = 0;
    std::uint32_t local_y = 0;
    std::uint32_t slot_index = 0;
};

RegionChunkAddress ComputeRegionChunkAddress(const ChunkCoord& chunk_coord, std::size_t span_chunks) {
    if (span_chunks == 0) {
        throw std::invalid_argument("experimental_region_span_chunks must be > 0");
    }
    const auto span = static_cast<std::int64_t>(span_chunks);
    const auto rx = FloorDiv(chunk_coord.x, span);
    const auto ry = FloorDiv(chunk_coord.y, span);
    const auto lx = static_cast<std::uint32_t>(chunk_coord.x - rx * span);
    const auto ly = static_cast<std::uint32_t>(chunk_coord.y - ry * span);
    const auto span_u32 = static_cast<std::uint32_t>(span_chunks);
    return RegionChunkAddress{
        .region_x = rx,
        .region_y = ry,
        .local_x = lx,
        .local_y = ly,
        .slot_index = ly * span_u32 + lx,
    };
}

std::filesystem::path RegionDataPath(
    const std::filesystem::path& data_dir,
    const ChunkCoord& chunk_coord,
    std::size_t span_chunks) {
    const auto addr = ComputeRegionChunkAddress(chunk_coord, span_chunks);
    return data_dir /
           ("R_" + std::to_string(addr.region_x) + "_" + std::to_string(addr.region_y) + ".rgn");
}

std::filesystem::path LayoutWalPath(
    const std::filesystem::path& data_dir,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    StorageLayoutMode mode) {
    switch (mode) {
        case StorageLayoutMode::kFsSplitV1:
        case StorageLayoutMode::kFsRegionV1Experimental:
            return ChunkWalPath(data_dir, geometry, chunk_coord);
    }
    return ChunkWalPath(data_dir, geometry, chunk_coord);
}

struct RegionFileImage {
    std::uint32_t span_chunks = 0;
    std::int64_t region_x = 0;
    std::int64_t region_y = 0;
    std::uint32_t slot_count = 0;
    std::uint32_t payload_bytes = 0;
    std::vector<std::uint8_t> present_bitmap;
    std::vector<std::uint32_t> slot_crc;
    std::vector<std::uint8_t> slot_payloads;
};

std::size_t RegionPresentBitmapBytes(std::uint32_t slot_count) {
    return (static_cast<std::size_t>(slot_count) + 7U) / 8U;
}

RegionFileImage BuildEmptyRegionFileImage(
    const Geometry& geometry,
    const RegionChunkAddress& addr,
    std::size_t span_chunks) {
    const auto span_u32 = static_cast<std::uint32_t>(span_chunks);
    const std::uint32_t slot_count = span_u32 * span_u32;
    const std::uint32_t payload_bytes = static_cast<std::uint32_t>(geometry.ChunkPayloadBytes());

    RegionFileImage image;
    image.span_chunks = span_u32;
    image.region_x = addr.region_x;
    image.region_y = addr.region_y;
    image.slot_count = slot_count;
    image.payload_bytes = payload_bytes;
    image.present_bitmap.assign(RegionPresentBitmapBytes(slot_count), 0U);
    image.slot_crc.assign(slot_count, 0U);
    image.slot_payloads.assign(static_cast<std::size_t>(slot_count) * payload_bytes, 0U);
    return image;
}

bool RegionSlotPresent(const RegionFileImage& image, std::uint32_t slot_index) {
    if (slot_index >= image.slot_count) {
        throw std::runtime_error("region slot index out of range");
    }
    const std::size_t byte_index = slot_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (slot_index % 8U));
    return (image.present_bitmap[byte_index] & bit_mask) != 0U;
}

void SetRegionSlotPresent(RegionFileImage* image, std::uint32_t slot_index, bool present) {
    if (image == nullptr || slot_index >= image->slot_count) {
        throw std::runtime_error("region slot index out of range");
    }
    const std::size_t byte_index = slot_index / 8U;
    const std::uint8_t bit_mask = static_cast<std::uint8_t>(1U << (slot_index % 8U));
    if (present) {
        image->present_bitmap[byte_index] |= bit_mask;
    } else {
        image->present_bitmap[byte_index] &= static_cast<std::uint8_t>(~bit_mask);
    }
}

std::vector<std::uint8_t> SerializeRegionFileImage(
    const Geometry& geometry,
    const RegionFileImage& image) {
    const std::uint32_t expected_payload_bytes = static_cast<std::uint32_t>(geometry.ChunkPayloadBytes());
    if (image.payload_bytes != expected_payload_bytes) {
        throw std::runtime_error("region payload bytes mismatch");
    }
    if (image.slot_crc.size() != image.slot_count) {
        throw std::runtime_error("region crc table size mismatch");
    }
    if (image.present_bitmap.size() != RegionPresentBitmapBytes(image.slot_count)) {
        throw std::runtime_error("region presence bitmap size mismatch");
    }
    if (image.slot_payloads.size() != static_cast<std::size_t>(image.slot_count) * image.payload_bytes) {
        throw std::runtime_error("region payload area size mismatch");
    }

    std::vector<std::uint8_t> out;
    out.reserve(
        kRegionHeaderSize +
        image.present_bitmap.size() +
        image.slot_crc.size() * 4U +
        image.slot_payloads.size());

    out.insert(out.end(), kRegionMagic, kRegionMagic + 8U);
    WriteLe16(out, kRegionFileVersion);
    WriteLe16(out, static_cast<std::uint16_t>(geometry.config().block_bits));
    WriteLe32(out, geometry.config().chunk_width_blocks);
    WriteLe32(out, geometry.config().chunk_height_blocks);
    WriteLe32(out, image.span_chunks);
    WriteLe64(out, static_cast<std::uint64_t>(image.region_x));
    WriteLe64(out, static_cast<std::uint64_t>(image.region_y));
    WriteLe32(out, image.payload_bytes);
    WriteLe32(out, image.slot_count);

    out.insert(out.end(), image.present_bitmap.begin(), image.present_bitmap.end());
    for (const auto crc : image.slot_crc) {
        WriteLe32(out, crc);
    }
    out.insert(out.end(), image.slot_payloads.begin(), image.slot_payloads.end());
    return out;
}

RegionFileImage ParseRegionFileImage(
    const std::vector<std::uint8_t>& bytes,
    const Geometry& geometry,
    const RegionChunkAddress& expected_addr,
    std::size_t expected_span_chunks) {
    if (bytes.size() < kRegionHeaderSize) {
        throw std::runtime_error("region file too small");
    }
    if (std::memcmp(bytes.data(), kRegionMagic, 8U) != 0) {
        throw std::runtime_error("invalid region file magic");
    }

    const std::uint16_t version = ReadLe16(bytes, 8U);
    if (version != kRegionFileVersion) {
        throw std::runtime_error("unsupported region file version");
    }
    const std::uint16_t block_bits = ReadLe16(bytes, 10U);
    const std::uint32_t chunk_width = ReadLe32(bytes, 12U);
    const std::uint32_t chunk_height = ReadLe32(bytes, 16U);
    if (block_bits != geometry.config().block_bits ||
        chunk_width != geometry.config().chunk_width_blocks ||
        chunk_height != geometry.config().chunk_height_blocks) {
        throw std::runtime_error("region geometry mismatch");
    }

    const std::uint32_t span_chunks = ReadLe32(bytes, 20U);
    const auto region_x = static_cast<std::int64_t>(ReadLe64(bytes, 24U));
    const auto region_y = static_cast<std::int64_t>(ReadLe64(bytes, 32U));
    const std::uint32_t payload_bytes = ReadLe32(bytes, 40U);
    const std::uint32_t slot_count = ReadLe32(bytes, 44U);

    const auto expected_span_u32 = static_cast<std::uint32_t>(expected_span_chunks);
    if (span_chunks != expected_span_u32) {
        throw std::runtime_error("region span mismatch");
    }
    if (region_x != expected_addr.region_x || region_y != expected_addr.region_y) {
        throw std::runtime_error("region coordinate mismatch");
    }
    if (payload_bytes != geometry.ChunkPayloadBytes()) {
        throw std::runtime_error("region payload bytes mismatch");
    }
    if (slot_count != expected_span_u32 * expected_span_u32) {
        throw std::runtime_error("region slot count mismatch");
    }

    const std::size_t bitmap_bytes = RegionPresentBitmapBytes(slot_count);
    const std::size_t crc_bytes = static_cast<std::size_t>(slot_count) * 4U;
    const std::size_t payload_area_bytes = static_cast<std::size_t>(slot_count) * payload_bytes;
    const std::size_t expected_size = kRegionHeaderSize + bitmap_bytes + crc_bytes + payload_area_bytes;
    if (bytes.size() != expected_size) {
        throw std::runtime_error("region file size mismatch");
    }

    RegionFileImage image;
    image.span_chunks = span_chunks;
    image.region_x = region_x;
    image.region_y = region_y;
    image.slot_count = slot_count;
    image.payload_bytes = payload_bytes;
    image.present_bitmap.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize),
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize + bitmap_bytes));

    image.slot_crc.resize(slot_count);
    std::size_t crc_cursor = kRegionHeaderSize + bitmap_bytes;
    for (std::size_t i = 0; i < slot_count; ++i) {
        image.slot_crc[i] = ReadLe32(bytes, crc_cursor);
        crc_cursor += 4U;
    }

    image.slot_payloads.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(kRegionHeaderSize + bitmap_bytes + crc_bytes),
        bytes.end());
    return image;
}

std::vector<std::uint8_t> ExtractRegionSlotPayload(
    const RegionFileImage& image,
    std::uint32_t slot_index) {
    if (!RegionSlotPresent(image, slot_index)) {
        return {};
    }

    const std::size_t slot_offset = static_cast<std::size_t>(slot_index) * image.payload_bytes;
    std::vector<std::uint8_t> payload(
        image.slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset),
        image.slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset + image.payload_bytes));
    if (Crc32(payload) != image.slot_crc[slot_index]) {
        throw std::runtime_error("region slot payload checksum mismatch");
    }
    return payload;
}

void WriteRegionSlotPayload(
    RegionFileImage* image,
    std::uint32_t slot_index,
    const std::vector<std::uint8_t>& payload) {
    if (image == nullptr || payload.size() != image->payload_bytes) {
        throw std::runtime_error("region slot payload size mismatch");
    }
    const std::size_t slot_offset = static_cast<std::size_t>(slot_index) * image->payload_bytes;
    std::copy(payload.begin(), payload.end(), image->slot_payloads.begin() + static_cast<std::ptrdiff_t>(slot_offset));
    image->slot_crc[slot_index] = Crc32(payload);
    SetRegionSlotPresent(image, slot_index, true);
}

std::mutex& RegionIoMutex() {
    static std::mutex mutex;
    return mutex;
}

std::uint64_t TmpArtifactAgeMs(const std::filesystem::path& tmp_path) {
    std::error_code ts_ec;
    const auto ts = std::filesystem::last_write_time(tmp_path, ts_ec);
    if (ts_ec) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    const auto now = std::filesystem::file_time_type::clock::now();
    if (now <= ts) {
        return 0;
    }
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now - ts).count());
}

bool ShouldCleanupTmpArtifactForCurrentProcess(
    const std::filesystem::path& target_path,
    const std::filesystem::path& tmp_path) {
    const std::string prefix = target_path.filename().string() + ".tmp.";
    const std::string name = tmp_path.filename().string();
    if (name.rfind(prefix, 0) != 0) {
        return false;
    }

    const std::string suffix = name.substr(prefix.size());
    const std::size_t dot = suffix.find('.');
    if (dot == std::string::npos || dot == 0) {
        return true;
    }

    std::int64_t pid = -1;
    if (!TryParseInt64(suffix.substr(0, dot), &pid) || pid <= 0) {
        return true;
    }

    const auto current_pid = static_cast<std::int64_t>(CurrentProcessIdValue());
    if (pid != current_pid) {
        return !IsProcessAlive(pid);
    }
    return TmpArtifactAgeMs(tmp_path) >= kAtomicTmpCurrentPidCleanupMinAgeMs;
}

void CleanupAtomicTmpArtifacts(const std::filesystem::path& target_path) {
    const auto parent = target_path.parent_path();
    std::error_code exists_ec;
    if (!std::filesystem::exists(parent, exists_ec)) {
        if (exists_ec) {
            throw std::runtime_error(
                "failed to inspect chunk directory for temporary artifacts: " + parent.string() +
                " (ec=" + std::to_string(exists_ec.value()) +
                ", msg='" + exists_ec.message() + "')");
        }
        return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        const auto& tmp_path = entry.path();
        if (!entry.is_regular_file()) {
            continue;
        }
        if (!ShouldCleanupTmpArtifactForCurrentProcess(target_path, tmp_path)) {
            continue;
        }
        std::error_code remove_ec;
        std::filesystem::remove(tmp_path, remove_ec);
        if (remove_ec) {
            throw std::runtime_error(
                "failed to remove stale temporary chunk artifact: " + tmp_path.string() +
                " (ec=" + std::to_string(remove_ec.value()) +
                ", msg='" + remove_ec.message() + "')");
        }
    }
}

#ifdef _WIN32
void CloseHandleChecked(HANDLE handle, const std::filesystem::path& path, const std::string& action) {
    if (CloseHandle(handle) == 0) {
        throw BuildWin32Error(action, path, GetLastError());
    }
}

void FlushHandleChecked(HANDLE handle, const std::filesystem::path& path, const std::string& action) {
    if (FlushFileBuffers(handle) == 0) {
        throw BuildWin32Error(action, path, GetLastError());
    }
}

void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync) {
    const std::wstring tmp_w = tmp_path.wstring();
    HANDLE handle = CreateFileW(
        tmp_w.c_str(),
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw BuildWin32Error("failed to open temporary file", tmp_path, GetLastError());
    }

    try {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const std::size_t remaining = bytes.size() - offset;
            const DWORD chunk = static_cast<DWORD>(std::min<std::size_t>(remaining, 1U << 20U));
            DWORD written = 0;
            const BOOL ok = WriteFile(
                handle,
                bytes.data() + offset,
                chunk,
                &written,
                nullptr);
            if (ok == 0 || written != chunk) {
                throw BuildWin32Error("failed to write temporary file", tmp_path, GetLastError());
            }
            offset += written;
        }

        if (durable_sync) {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected temporary file sync failure: " + tmp_path.string());
            }
            FlushHandleChecked(handle, tmp_path, "failed to flush temporary file");
        }

        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file close failure: " + tmp_path.string());
        }

        CloseHandleChecked(handle, tmp_path, "failed to close temporary file");
        handle = INVALID_HANDLE_VALUE;
    } catch (...) {
        if (handle != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(handle);
        }
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw;
    }
}

std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    const std::wstring tmp_w = tmp_path.wstring();
    HANDLE tmp_handle = CreateFileW(
        tmp_w.c_str(),
        DELETE | SYNCHRONIZE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr);
    if (tmp_handle == INVALID_HANDLE_VALUE) {
        return std::error_code(static_cast<int>(GetLastError()), std::system_category());
    }

    const std::wstring target_w = std::filesystem::absolute(target_path).wstring();
    std::vector<std::uint8_t> rename_bytes(
        sizeof(FILE_RENAME_INFO) + target_w.size() * sizeof(wchar_t));
    auto* rename_info = reinterpret_cast<FILE_RENAME_INFO*>(rename_bytes.data());
    rename_info->ReplaceIfExists = TRUE;
    rename_info->RootDirectory = nullptr;
    rename_info->FileNameLength = static_cast<DWORD>(target_w.size() * sizeof(wchar_t));
    std::memcpy(rename_info->FileName, target_w.data(), rename_info->FileNameLength);

    const BOOL rename_ok = SetFileInformationByHandle(
        tmp_handle,
        FileRenameInfo,
        rename_info,
        static_cast<DWORD>(rename_bytes.size()));
    const DWORD rename_error = rename_ok != 0 ? ERROR_SUCCESS : GetLastError();

    const BOOL close_ok = CloseHandle(tmp_handle);
    const DWORD close_error = close_ok != 0 ? ERROR_SUCCESS : GetLastError();

    if (rename_ok == 0) {
        return std::error_code(static_cast<int>(rename_error), std::system_category());
    }
    if (close_ok == 0) {
        return std::error_code(static_cast<int>(close_error), std::system_category());
    }
    return {};
}

void SyncFilePath(const std::filesystem::path& path) {
    const std::string path_u8 = path.string();
    const int fd = _open(path_u8.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) {
        throw std::runtime_error(
            "failed to open file for durability sync: " + path.string());
    }
    const int sync_rc = _commit(fd);
    const int close_rc = _close(fd);
    if (sync_rc != 0) {
        throw std::runtime_error("failed to sync file: " + path.string());
    }
    if (close_rc != 0) {
        throw std::runtime_error("failed to close synced file: " + path.string());
    }
}

void SyncDirectoryPath(const std::filesystem::path& path) {
    const std::wstring path_w = path.wstring();
    HANDLE handle = CreateFileW(
        path_w.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw BuildWin32Error("failed to open directory for durability sync", path, GetLastError());
    }
    try {
        if (FlushFileBuffers(handle) == 0) {
            const DWORD flush_error = GetLastError();
            // Some Windows filesystems/runtimes do not allow directory handle flush.
            // Treat known capability errors as best-effort degradation.
            if (flush_error != ERROR_ACCESS_DENIED &&
                flush_error != ERROR_INVALID_HANDLE &&
                flush_error != ERROR_INVALID_FUNCTION) {
                throw BuildWin32Error("failed to sync directory", path, flush_error);
            }
        }
        CloseHandleChecked(handle, path, "failed to close synced directory");
    } catch (...) {
        (void)CloseHandle(handle);
        throw;
    }
}

#else
void CloseFdChecked(int fd, const std::filesystem::path& path, const std::string& action) {
    while (::close(fd) != 0) {
        if (errno == EINTR) {
            continue;
        }
        throw BuildErrnoError(action, path, errno);
    }
}

void SyncFdDurably(int fd, const std::filesystem::path& path, bool full_sync) {
#if defined(__APPLE__)
    if (full_sync) {
        if (::fcntl(fd, F_FULLFSYNC, 0) == 0) {
            return;
        }
        const int fullsync_error = errno;
        // F_FULLFSYNC can be unsupported on some macOS filesystems/devices.
        // Fall back to fsync while surfacing non-capability errors.
        if (fullsync_error != EINVAL && fullsync_error != ENOTSUP && fullsync_error != ENOTTY) {
            throw BuildErrnoError("failed to F_FULLFSYNC file", path, fullsync_error);
        }
    }
    if (::fsync(fd) != 0) {
        throw BuildErrnoError("failed to fsync file", path, errno);
    }
#else
    if (::fdatasync(fd) != 0) {
        const int data_sync_error = errno;
        if (data_sync_error == EINVAL) {
            if (::fsync(fd) != 0) {
                throw BuildErrnoError("failed to fsync file", path, errno);
            }
            return;
        }
        throw BuildErrnoError("failed to fdatasync file", path, data_sync_error);
    }
#endif
}

void WriteAtomicTempFile(
    const std::filesystem::path& tmp_path,
    const std::vector<std::uint8_t>& bytes,
    bool durable_sync) {
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw BuildErrnoError("failed to open temporary file", tmp_path, errno);
    }

    try {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const std::size_t remaining = bytes.size() - offset;
            const ssize_t written = ::write(fd, bytes.data() + offset, remaining);
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw BuildErrnoError("failed to write temporary file", tmp_path, errno);
            }
            if (written == 0) {
                throw std::runtime_error(
                    "short write while writing temporary file: " + tmp_path.string());
            }
            offset += static_cast<std::size_t>(written);
        }

        if (durable_sync) {
            if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_SYNC_FAIL_ONCE")) {
                throw std::runtime_error(
                    "injected temporary file sync failure: " + tmp_path.string());
            }
            SyncFdDurably(fd, tmp_path, true);
        }

        if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_CLOSE_FAIL_ONCE")) {
            throw std::runtime_error(
                "injected temporary file close failure: " + tmp_path.string());
        }

        CloseFdChecked(fd, tmp_path, "failed to close temporary file");
    } catch (...) {
        (void)::close(fd);
        std::error_code cleanup_ec;
        std::filesystem::remove(tmp_path, cleanup_ec);
        throw;
    }
}

std::error_code ReplacePathAtomically(
    const std::filesystem::path& tmp_path,
    const std::filesystem::path& target_path) {
    std::error_code ec;
    std::filesystem::rename(tmp_path, target_path, ec);
    return ec;
}

void SyncFilePath(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        throw BuildErrnoError("failed to open file for durability sync", path, errno);
    }
    try {
        SyncFdDurably(fd, path, false);
        CloseFdChecked(fd, path, "failed to close synced file");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
}

void SyncDirectoryPath(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        throw BuildErrnoError("failed to open directory for durability sync", path, errno);
    }
    try {
        if (::fsync(fd) != 0) {
            throw BuildErrnoError("failed to sync directory", path, errno);
        }
        CloseFdChecked(fd, path, "failed to close synced directory");
    } catch (...) {
        (void)::close(fd);
        throw;
    }
}
#endif

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

WalReplayResult ReplayWal(
    const std::vector<std::uint8_t>& wal_bytes,
    const Geometry& geometry,
    const ChunkCoord& chunk_coord,
    std::vector<std::uint8_t>* payload) {
    WalReplayResult result;
    if (payload == nullptr) {
        throw std::invalid_argument("payload must not be null");
    }

    if (wal_bytes.empty()) {
        return result;
    }

    std::size_t cursor = 0;
    if (wal_bytes.size() >= kWalHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalMagic, 8U) == 0) {
        try {
            ValidateWalHeader(wal_bytes, geometry, chunk_coord);
            cursor = kWalHeaderSize;
            result.replayable = true;
        } catch (...) {
            result.stop_reason = "invalid_header";
            return result;
        }
    } else if (
        wal_bytes.size() >= kWalRecordHeaderSize &&
        std::memcmp(wal_bytes.data(), kWalRecordMagic, 4U) == 0) {
        // Headerless WAL can appear if a writer recreated WAL and appended records
        // across a file replacement race; replay from record stream start.
        cursor = 0;
        result.replayable = true;
    } else {
        // Unknown leading bytes: treat as non-replayable WAL content.
        result.stop_reason = "unknown_prefix";
        return result;
    }

    while (cursor < wal_bytes.size()) {
        const std::size_t remaining = wal_bytes.size() - cursor;
        if (remaining < kWalRecordHeaderSize) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_header";
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

            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "header_mismatch_midstream";
            break;
        }

        if (std::memcmp(wal_bytes.data() + cursor, kWalRecordMagic, 4U) != 0) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_magic_mismatch";
            break;
        }

        const std::uint32_t byte_offset = ReadLe32(wal_bytes, cursor + 4U);
        const std::uint16_t data_size = ReadLe16(wal_bytes, cursor + 8U);
        const std::uint32_t record_crc = ReadLe32(wal_bytes, cursor + 10U);

        const std::size_t full_record_size = kWalRecordHeaderSize + data_size;
        if (remaining < full_record_size) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "partial_record_payload";
            break;
        }

        const std::size_t payload_end = static_cast<std::size_t>(byte_offset) + data_size;
        if (payload_end > payload->size()) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_out_of_range";
            break;
        }

        const std::uint8_t* record_data = wal_bytes.data() + cursor + kWalRecordHeaderSize;
        const std::uint32_t computed_crc = Crc32(record_data, data_size);
        if (computed_crc != record_crc) {
            result.tail_truncated_or_corrupt = true;
            result.stop_reason = "record_crc_mismatch";
            break;
        }

        std::copy(
            record_data,
            record_data + data_size,
            payload->begin() + static_cast<std::ptrdiff_t>(byte_offset));

        cursor += full_record_size;
        result.applied_records += 1;
    }

    return result;
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

    WriteAtomicTempFile(tmp_path, bytes, fsync_file);

    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_TEMP_FLUSH_ONCE")) {
        throw std::runtime_error(
            "injected atomic write failure after temp flush and close before replace: " + tmp_path.string());
    }

    std::error_code rename_ec;
    for (int attempt = 0; attempt < kAtomicWriteRetryCount; ++attempt) {
        rename_ec = ReplacePathAtomically(tmp_path, path);
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

StorageLayoutMode ParseStorageLayoutMode(std::string_view text) {
    if (text == "fs_split_v1") {
        return StorageLayoutMode::kFsSplitV1;
    }
    if (text == "fs_region_v1") {
        return StorageLayoutMode::kFsRegionV1Experimental;
    }
    throw std::invalid_argument(
        "invalid storage layout mode: " + std::string(text) +
        " (expected fs_split_v1|fs_region_v1)");
}

const char* StorageLayoutModeName(StorageLayoutMode mode) noexcept {
    switch (mode) {
        case StorageLayoutMode::kFsSplitV1:
            return "fs_split_v1";
        case StorageLayoutMode::kFsRegionV1Experimental:
            return "fs_region_v1";
    }
    return "unknown";
}

ChunkStore::ChunkStore(StoreConfig config)
    : geometry_(config.geometry),
      data_dir_(std::move(config.data_dir)),
      durability_mode_(config.durability_mode),
      access_mode_(config.access_mode),
      storage_layout_mode_(config.storage_layout_mode),
      experimental_region_span_chunks_(config.experimental_region_span_chunks),
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
    if (experimental_region_span_chunks_ == 0) {
        throw std::invalid_argument("experimental_region_span_chunks must be > 0");
    }
    if (experimental_region_span_chunks_ > 64) {
        throw std::invalid_argument("experimental_region_span_chunks must be <= 64");
    }

    const auto recovery_start = std::chrono::steady_clock::now();
    const auto startup_scan = ScanStartupRecovery(data_dir_);

    std::filesystem::create_directories(data_dir_);
    AcquireProcessLock(config.allow_multiple_processes);

    const auto recovery_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - recovery_start);
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kRecovery,
        "startup recovery summary",
        {
            {"checkpoint_files", std::to_string(startup_scan.checkpoint_files)},
            {"region_files", std::to_string(startup_scan.region_files)},
            {"wal_files", std::to_string(startup_scan.wal_files)},
            {"replay_mode", "lazy-on-load"},
            {"elapsed_ms", std::to_string(recovery_elapsed_ms.count())},
        });
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kStore,
        "store initialized",
        {
            {"data_dir", data_dir_.string()},
            {"durability_mode", DurabilityModeName(durability_mode_)},
            {"access_mode", AccessModeName(access_mode_)},
            {"storage_layout_mode", StorageLayoutModeName(storage_layout_mode_)},
            {"max_loaded_chunks", std::to_string(max_loaded_chunks_)},
        });
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
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);

    auto persist_payload = [&](const std::vector<std::uint8_t>& payload_bytes) {
        const bool strict = durability_mode_ == DurabilityMode::kFsyncCheckpoint;
        if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
            const auto checkpoint_bytes = SerializeChunkImage(geometry_, chunk_coord, payload_bytes);
            AtomicWrite(data_path, checkpoint_bytes, strict, strict);
            return;
        }

        const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
        std::lock_guard region_lock(RegionIoMutex());
        RegionFileImage region_image = BuildEmptyRegionFileImage(geometry_, addr, experimental_region_span_chunks_);
        if (std::filesystem::exists(data_path)) {
            const auto region_bytes = LoadFile(data_path);
            region_image = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
        }
        WriteRegionSlotPayload(&region_image, addr.slot_index, payload_bytes);
        const auto serialized = SerializeRegionFileImage(geometry_, region_image);
        AtomicWrite(data_path, serialized, strict, strict);
    };

    std::vector<std::uint8_t> payload = EmptyPayload();
    CleanupAtomicTmpArtifacts(data_path);
    if (std::filesystem::exists(data_path)) {
        try {
            if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
                const auto data_bytes = LoadFile(data_path);
                payload = ParseChunkImage(data_bytes, geometry_, chunk_coord);
            } else {
                const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
                std::lock_guard region_lock(RegionIoMutex());
                const auto region_bytes = LoadFile(data_path);
                const auto region = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
                const auto slot_payload = ExtractRegionSlotPayload(region, addr.slot_index);
                if (!slot_payload.empty()) {
                    payload = slot_payload;
                }
            }
        } catch (...) {
            // The image can be replaced concurrently by atomic checkpoint rename.
            // If it disappeared during open, fall back to empty payload.
            if (std::filesystem::exists(data_path)) {
                throw;
            }
            payload = EmptyPayload();
        }
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

        const auto replay = ReplayWal(wal_bytes, geometry_, chunk_coord, &payload);
        if (!replay.replayable) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL skipped during chunk load",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "non_replayable" : replay.stop_reason},
                });
        } else if (replay.tail_truncated_or_corrupt) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kRecovery,
                "WAL replay stopped on tail corruption/truncation",
                {
                    {"chunk_x", std::to_string(chunk_coord.x)},
                    {"chunk_y", std::to_string(chunk_coord.y)},
                    {"reason", replay.stop_reason.empty() ? "tail_corruption" : replay.stop_reason},
                    {"applied_records", std::to_string(replay.applied_records)},
                });
        }
        persist_payload(payload);
        stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);

        std::error_code ec;
        std::filesystem::remove(wal_path, ec);
        if (durability_mode_ == DurabilityMode::kFsyncCheckpoint) {
            SyncDirectoryPath(data_path.parent_path());
            if (wal_path.parent_path() != data_path.parent_path()) {
                SyncDirectoryPath(wal_path.parent_path());
            }
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

    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);
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
    output.close();
    if (!output.good()) {
        throw std::runtime_error("failed to close WAL file after append: " + wal_path.string());
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
    const auto data_path =
        (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1)
            ? ChunkDataPath(data_dir_, geometry_, chunk_coord)
            : RegionDataPath(data_dir_, chunk_coord, experimental_region_span_chunks_);
    const auto wal_path = LayoutWalPath(data_dir_, geometry_, chunk_coord, storage_layout_mode_);

    try {
        const bool strict = durability_mode_ == DurabilityMode::kFsyncCheckpoint;
        if (storage_layout_mode_ == StorageLayoutMode::kFsSplitV1) {
            const auto image = SerializeChunkImage(geometry_, chunk_coord, chunk->payload);
            AtomicWrite(data_path, image, strict, strict);
        } else {
            const auto addr = ComputeRegionChunkAddress(chunk_coord, experimental_region_span_chunks_);
            std::lock_guard region_lock(RegionIoMutex());
            RegionFileImage region_image = BuildEmptyRegionFileImage(geometry_, addr, experimental_region_span_chunks_);
            if (std::filesystem::exists(data_path)) {
                const auto region_bytes = LoadFile(data_path);
                region_image = ParseRegionFileImage(region_bytes, geometry_, addr, experimental_region_span_chunks_);
            }
            WriteRegionSlotPayload(&region_image, addr.slot_index, chunk->payload);
            const auto serialized = SerializeRegionFileImage(geometry_, region_image);
            AtomicWrite(data_path, serialized, strict, strict);
        }

        std::error_code ec;
        std::filesystem::remove(wal_path, ec);
        if (strict) {
            SyncDirectoryPath(data_path.parent_path());
            if (wal_path.parent_path() != data_path.parent_path()) {
                SyncDirectoryPath(wal_path.parent_path());
            }
        }

        stats_checkpoints_.fetch_add(1, std::memory_order_relaxed);
        chunk->pending_updates = 0;
        chunk->wal_bytes = 0;
        chunk->pending_wal_flush_updates = 0;
        chunk->wal_batch.clear();
    } catch (const std::exception& e) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kRecovery,
            "checkpoint failed",
            {
                {"chunk_x", std::to_string(chunk_coord.x)},
                {"chunk_y", std::to_string(chunk_coord.y)},
                {"error", e.what()},
            });
        throw;
    }
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
        if (allow_multiple_processes) {
            LogMessage(
                LogLevel::kWarn,
                LogComponent::kLock,
                "single-writer guard disabled by configuration",
                {{"allow_multi_process", "on"}});
        }
        if (access_mode_ == AccessMode::kReadOnly) {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kLock,
                "read-only mode; writer lock not acquired",
                {{"access_mode", AccessModeName(access_mode_)}});
        }
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
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "failed to acquire writer lock file",
            {{"path", process_lock_file_path_.string()}});
        throw std::runtime_error(
            "failed to acquire writer lock file: " + process_lock_file_path_.string());
    }
    process_lock_handle_ = handle;
#else
    const int fd = ::open(process_lock_file_path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "failed to open writer lock file",
            {{"path", process_lock_file_path_.string()}});
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
        LogMessage(
            LogLevel::kError,
            LogComponent::kLock,
            "active writer already holds lock",
            {
                {"data_dir", data_dir_.string()},
                {"details", detail.empty() ? "none" : detail},
            });
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
            std::string stale_session = "unknown";
            std::string stale_pid = "unknown";
            const auto sid = parsed.find("session_id");
            const auto pid = parsed.find("pid");
            if (sid != parsed.end() && !sid->second.empty()) {
                stale_session = sid->second;
            }
            if (pid != parsed.end() && !pid->second.empty()) {
                stale_pid = pid->second;
            }
            const auto stale_path = process_lock_dir_ /
                                    ("writer.meta.stale." + std::to_string(UnixMillisNow()));
            std::error_code ec;
            std::filesystem::rename(process_lock_meta_path_, stale_path, ec);
            if (!ec) {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "stale writer metadata moved for takeover recovery",
                    {
                        {"from", process_lock_meta_path_.string()},
                        {"to", stale_path.string()},
                        {"stale_session", stale_session},
                        {"stale_pid", stale_pid},
                    });
            } else {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "stale writer metadata detected but could not be moved",
                    {
                        {"path", process_lock_meta_path_.string()},
                        {"error", ec.message()},
                    });
            }
        }
    }

    process_lock_session_id_ = GenerateSessionId();
    WriteWriterMetadata();
    StartWriterHeartbeat();
    LogMessage(
        LogLevel::kInfo,
        LogComponent::kLock,
        "lock acquired",
        {
            {"data_dir", data_dir_.string()},
            {"session_id", process_lock_session_id_},
        });
}

void ChunkStore::ReleaseProcessLock() noexcept {
    const std::string session_id = process_lock_session_id_;
    const bool had_lock =
        !session_id.empty() ||
#ifdef _WIN32
        process_lock_handle_ != nullptr;
#else
        process_lock_fd_ >= 0;
#endif

    StopWriterHeartbeat();

    if (!process_lock_meta_path_.empty()) {
        std::error_code ec;
        std::filesystem::remove(process_lock_meta_path_, ec);
        if (ec) {
            try {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "failed to remove writer metadata on shutdown",
                    {
                        {"path", process_lock_meta_path_.string()},
                        {"error", ec.message()},
                    });
            } catch (...) {
            }
        }
    }

#ifdef _WIN32
    if (process_lock_handle_ != nullptr) {
        const HANDLE handle = static_cast<HANDLE>(process_lock_handle_);
        if (CloseHandle(handle) == 0) {
            const DWORD code = GetLastError();
            try {
                LogMessage(
                    LogLevel::kWarn,
                    LogComponent::kLock,
                    "failed to release writer lock handle",
                    {{"error", std::system_category().message(static_cast<int>(code))}});
            } catch (...) {
            }
        }
        process_lock_handle_ = nullptr;
    }
#else
    if (process_lock_fd_ >= 0) {
        (void)::flock(process_lock_fd_, LOCK_UN);
        (void)::close(process_lock_fd_);
        process_lock_fd_ = -1;
    }
#endif

    if (had_lock) {
        try {
            LogMessage(
                LogLevel::kInfo,
                LogComponent::kLock,
                "lock release result",
                {
                    {"session_id", session_id.empty() ? "unknown" : session_id},
                    {"result", "released"},
                });
        } catch (...) {
        }
    }

    process_lock_session_id_.clear();
    process_lock_meta_path_.clear();
    process_lock_file_path_.clear();
    process_lock_dir_.clear();
}

}  // namespace chunkdb
