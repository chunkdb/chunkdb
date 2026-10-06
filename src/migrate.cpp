#include "migrate.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <system_error>
#include <utility>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "legacy_format.hpp"

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace chunkdb::migrate {

namespace {

// Names in a source data directory. They are frozen here, like the formats
// in legacy_format.cpp: engine changes must not change what is converted.
constexpr std::string_view kVersionClockFile = "chunkdb.version";
constexpr std::string_view kInitializedMarkerFile = ".chunkdb.initialized";
constexpr std::string_view kSnapshotGenerationFile = "chunkdb.snapshot";
constexpr std::string_view kIntentDir = ".chunkdb.intents";
constexpr std::string_view kLockDir = ".chunkdb.lock";
constexpr std::string_view kIntentSuffix = ".rollback";
constexpr std::size_t kWalHeaderSize = 36;
constexpr std::uint8_t kWalMagic[8] = {'C', 'H', 'K', 'W', 'A', 'L', '0', '2'};

using Coord = std::pair<std::int64_t, std::int64_t>;

// Parses a decimal integer exactly as chunkdb writes it (std::to_string).
[[nodiscard]] bool ParseCanonicalInt(const std::string& text, std::int64_t* out) {
    std::int64_t value = 0;
    if (!TryParseInt64(text, &value) || std::to_string(value) != text) {
        return false;
    }
    *out = value;
    return true;
}

// "<prefix><x>_<y><suffix>" with canonical integers.
[[nodiscard]] bool ParseCoordName(
    const std::string& name,
    std::string_view prefix,
    std::string_view suffix,
    std::int64_t* x,
    std::int64_t* y) {
    if (name.size() <= prefix.size() + suffix.size() || name.rfind(prefix, 0) != 0 ||
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }
    const std::string middle =
        name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
    // The x part may be negative, so split at the first '_' after its digits.
    const std::size_t separator = middle.find('_', middle.empty() || middle[0] != '-' ? 0U : 1U);
    if (separator == std::string::npos) {
        return false;
    }
    return ParseCanonicalInt(middle.substr(0, separator), x) &&
           ParseCanonicalInt(middle.substr(separator + 1), y);
}

[[nodiscard]] std::vector<std::uint8_t> ReadWholeFile(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("cannot open " + path.string());
    }
    std::vector<std::uint8_t> bytes(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (in.bad()) {
        throw std::runtime_error("cannot read " + path.string());
    }
    return bytes;
}

[[nodiscard]] bool IsTmpName(const std::string& name) {
    return name.find(".tmp.") != std::string::npos;
}

[[nodiscard]] std::string LargeDirName(std::int64_t lx, std::int64_t ly) {
    return "L_" + std::to_string(lx) + "_" + std::to_string(ly);
}

[[nodiscard]] std::string ChunkFileName(const Coord& coord, std::string_view extension) {
    return "C_" + std::to_string(coord.first) + "_" + std::to_string(coord.second) +
           std::string(extension);
}

// Holds the source's writer lock without writing to the source, so no
// server starts on it while it is read. A source without a lock file was
// never opened by a server that took one; it cannot be locked without
// creating the file.
class SourceLock {
  public:
    explicit SourceLock(const std::filesystem::path& source) {
        const auto path = source / std::string(kLockDir) / "writer.lock";
        std::error_code ec;
        if (!std::filesystem::exists(path, ec) || ec) {
            return;
        }
#ifdef _WIN32
        handle_ = CreateFileW(
            path.wstring().c_str(), GENERIC_READ, 0, nullptr, OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) {
            const DWORD error = GetLastError();
            if (error == ERROR_SHARING_VIOLATION || error == ERROR_LOCK_VIOLATION) {
                throw MigrateRefused(
                    "a server is using the source", {path.string() + ": held by a running chunkdb server; stop it first"});
            }
            throw std::runtime_error(
                "cannot open " + path.string() + " (Windows error " + std::to_string(error) + ")");
        }
#else
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("cannot open " + path.string() + ": " + std::strerror(errno));
        }
        if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            const int error = errno;
            ::close(fd_);
            fd_ = -1;
            if (error == EWOULDBLOCK) {
                throw MigrateRefused(
                    "a server is using the source", {path.string() + ": held by a running chunkdb server; stop it first"});
            }
            throw std::runtime_error("cannot lock " + path.string() + ": " + std::strerror(error));
        }
#endif
        locked_ = true;
    }
    ~SourceLock() {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(handle_);
        }
#else
        if (fd_ >= 0) {
            ::close(fd_);
        }
#endif
    }
    SourceLock(const SourceLock&) = delete;
    SourceLock& operator=(const SourceLock&) = delete;
    [[nodiscard]] bool locked() const noexcept { return locked_; }

  private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
    bool locked_ = false;
};

struct ChunkArtifacts {
    bool image = false;
    bool wal = false;
};

struct SourceScan {
    std::map<Coord, ChunkArtifacts> chunks;
    // Keyed by the intent's file name.
    std::map<std::string, legacy::LegacyConditionalIntent> intents;
    std::uint64_t clock_ceiling = 1;
    std::vector<std::string> problems;
    std::vector<std::string> notes;
};

void ReadVersionClock(const std::filesystem::path& source, SourceScan* scan) {
    const auto clock_path = source / std::string(kVersionClockFile);
    const auto marker_path = source / std::string(kInitializedMarkerFile);
    const bool has_clock = std::filesystem::exists(clock_path);
    const bool has_marker = std::filesystem::exists(marker_path);
    if (has_marker && !legacy::IsValidLegacyInitializedMarker(ReadWholeFile(marker_path))) {
        scan->problems.push_back(
            marker_path.string() + ": damaged initialized-store marker; the old server refused to start");
    }
    if (!has_clock) {
        if (has_marker) {
            scan->problems.push_back(
                clock_path.string() +
                ": missing although the store was initialized; the old server refused to start");
        }
        // A store without clock bookkeeping started its clock at 1.
        scan->clock_ceiling = 1;
        return;
    }
    const auto bytes = ReadWholeFile(clock_path);
    std::uint64_t ceiling = 0;
    if (legacy::TryParseLegacyVersionClockRecord(bytes, &ceiling) ||
        legacy::TryParseIntermediateVersionClockRecord(bytes, &ceiling)) {
        scan->clock_ceiling = ceiling;
        return;
    }
    scan->problems.push_back(
        clock_path.string() + ": damaged version clock; the old server refused to start");
}

void ReadIntents(const std::filesystem::path& source, SourceScan* scan) {
    const auto dir = source / std::string(kIntentDir);
    if (!std::filesystem::is_directory(dir)) {
        return;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        const auto name = entry.path().filename().string();
        // The old server only reads regular files named *.rollback.
        if (!entry.is_regular_file() || name.size() <= kIntentSuffix.size() ||
            name.compare(name.size() - kIntentSuffix.size(), kIntentSuffix.size(), kIntentSuffix) != 0) {
            continue;
        }
        legacy::LegacyConditionalIntent intent;
        if (!legacy::TryParseLegacyConditionalIntent(ReadWholeFile(entry.path()), &intent)) {
            scan->problems.push_back(
                entry.path().string() + ": damaged conditional intent; the old server refused to start");
            continue;
        }
        scan->intents.emplace(name, intent);
    }
}

void ScanLargeChunkDir(
    const std::filesystem::path& dir,
    std::int64_t lx,
    std::int64_t ly,
    const Geometry& geometry,
    SourceScan* scan) {
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        const auto name = entry.path().filename().string();
        if (IsTmpName(name)) {
            scan->notes.push_back(entry.path().string() + ": temporary file of an interrupted write; ignored");
            continue;
        }
        const bool image = name.size() > 4U && name.compare(name.size() - 4U, 4U, ".chk") == 0;
        const bool wal = name.size() > 4U && name.compare(name.size() - 4U, 4U, ".wal") == 0;
        if (!image && !wal) {
            scan->notes.push_back(entry.path().string() + ": not chunk data; ignored");
            continue;
        }
        std::int64_t cx = 0;
        std::int64_t cy = 0;
        if (!entry.is_regular_file() || !ParseCoordName(name, "C_", image ? ".chk" : ".wal", &cx, &cy)) {
            scan->problems.push_back(
                entry.path().string() + ": not a chunk file chunkdb writes (the old server never read it)");
            continue;
        }
        const auto large = geometry.ChunkToLarge(ChunkCoord{cx, cy});
        if (large.x != lx || large.y != ly) {
            scan->problems.push_back(
                entry.path().string() + ": belongs in " + LargeDirName(large.x, large.y) +
                " with the given --large-chunk-width/--large-chunk-height; check them (the "
                "old server reads only the canonical path)");
            continue;
        }
        auto& artifacts = scan->chunks[Coord{cx, cy}];
        (image ? artifacts.image : artifacts.wal) = true;
    }
}

[[nodiscard]] SourceScan ScanSource(const std::filesystem::path& source, const Geometry& geometry) {
    SourceScan scan;
    for (const auto& entry : std::filesystem::directory_iterator(source)) {
        const auto name = entry.path().filename().string();
        if (name == "chunkdb.manifest" || name == "tables") {
            scan.problems.push_back(
                entry.path().string() +
                ": the source is already a 2.0 data directory (or one of a 2.0 development "
                "build); there is nothing to convert");
            continue;
        }
        if (name == kVersionClockFile || name == kInitializedMarkerFile ||
            name == kSnapshotGenerationFile || name == kIntentDir || name == kLockDir) {
            continue;
        }
        if (IsTmpName(name)) {
            scan.notes.push_back(entry.path().string() + ": temporary file of an interrupted write; ignored");
            continue;
        }
        std::int64_t lx = 0;
        std::int64_t ly = 0;
        if (entry.is_directory() && ParseCoordName(name, "L_", "", &lx, &ly)) {
            ScanLargeChunkDir(entry.path(), lx, ly, geometry, &scan);
            continue;
        }
        if (name.rfind("R_", 0) == 0 && name.size() > 4U && name.compare(name.size() - 4U, 4U, ".rgn") == 0) {
            scan.problems.push_back(
                entry.path().string() +
                ": data of the experimental fs_region_v1 layout, which chunkdb_migrate does not convert");
            continue;
        }
        if (name.rfind('.', 0) == 0) {
            continue;  // OS metadata
        }
        scan.notes.push_back(entry.path().string() + ": not chunkdb data; ignored");
    }
    ReadVersionClock(source, &scan);
    ReadIntents(source, &scan);
    return scan;
}

// A WAL header, frame or record that starts somewhere after `from`: data a
// writer appended after the point where replay stopped, which is lost.
[[nodiscard]] bool HasStructureAfter(
    const std::vector<std::uint8_t>& wal,
    std::size_t from,
    const Geometry& geometry) {
    const std::size_t state_size = geometry.ChunkPayloadBytes() + ChunkPresenceBitmapBytes(geometry);
    for (std::size_t at = from + 1; at + 4U <= wal.size(); ++at) {
        const std::uint8_t* p = wal.data() + at;
        const std::size_t remaining = wal.size() - at;
        if (remaining >= kWalHeaderSize && std::memcmp(p, kWalMagic, sizeof(kWalMagic)) == 0) {
            return true;
        }
        if (remaining >= 22U && std::memcmp(p, "FRM1", 4U) == 0 &&
            Crc32(p + 4U, 14U) == ReadLe32(wal, at + 18U)) {
            return true;
        }
        if (remaining >= 14U && std::memcmp(p, "DLT1", 4U) == 0) {
            const std::uint32_t offset = ReadLe32(wal, at + 4U);
            const std::uint16_t size = ReadLe16(wal, at + 8U);
            if (size != 0U && remaining >= 14U + size &&
                static_cast<std::size_t>(offset) + size <= state_size &&
                Crc32(p + 14U, size) == ReadLe32(wal, at + 10U)) {
                return true;
            }
        }
    }
    return false;
}

// A WAL a crash left without data: a torn prefix of the header, or only zero
// bytes (a filesystem can extend a file with zeros before the data lands).
[[nodiscard]] bool HoldsNoData(const std::vector<std::uint8_t>& wal) {
    if (wal.size() < kWalHeaderSize &&
        std::memcmp(wal.data(), kWalMagic, std::min(wal.size(), sizeof(kWalMagic))) == 0) {
        return true;
    }
    return std::all_of(wal.begin(), wal.end(), [](std::uint8_t byte) { return byte == 0U; });
}

struct LoadedChunk {
    std::vector<std::uint8_t> payload;
    std::vector<std::uint8_t> presence;
    std::uint64_t revision = 0;
    bool present = false;
    bool canonicalized = false;
    // Data dropped as the old server did; the chunk cannot be read at all
    // when `unreadable`.
    std::vector<std::string> losses;
    bool unreadable = false;
    // Reasons the old server would not have started.
    std::vector<std::string> problems;
    // Artifacts whose header records another geometry than the given one,
    // and the number that record the given one: wrong flags when none
    // matches, damaged headers otherwise.
    std::vector<std::string> geometry_mismatches;
    std::uint64_t geometry_matches = 0;
};

// Loads one chunk as the read-write server did after startup recovery, and
// counts what it read into `summary`.
[[nodiscard]] LoadedChunk LoadChunk(
    const std::filesystem::path& source,
    const Geometry& geometry,
    const Coord& coord,
    const ChunkArtifacts& artifacts,
    const SourceScan& scan,
    MigrateSummary* summary) {
    LoadedChunk chunk;
    const ChunkCoord chunk_coord{coord.first, coord.second};
    const auto large = geometry.ChunkToLarge(chunk_coord);
    const std::string large_dir = LargeDirName(large.x, large.y);
    const auto image_path = source / large_dir / ChunkFileName(coord, ".chk");
    const auto wal_path = source / large_dir / ChunkFileName(coord, ".wal");

    chunk.payload.assign(geometry.ChunkPayloadBytes(), 0U);
    chunk.presence.assign(ChunkPresenceBitmapBytes(geometry), 0U);

    if (artifacts.image) {
        const auto bytes = ReadWholeFile(image_path);
        summary->source_bytes += bytes.size();
        try {
            auto image = legacy::ParseLegacyChunkImage(bytes, geometry, chunk_coord);
            summary->images_by_version.at(image.version) += 1;
            chunk.payload = std::move(image.payload);
            chunk.presence = std::move(image.presence_bitmap);
            chunk.revision = image.revision;
            ++chunk.geometry_matches;
        } catch (const std::exception& e) {
            // The old server failed every load of this chunk.
            const std::string what = e.what();
            if (what == "geometry mismatch" || what == "payload size mismatch") {
                chunk.geometry_mismatches.push_back(image_path.string());
            }
            chunk.losses.push_back(
                image_path.string() + ": unreadable image (" + what + "); the old server could not load the chunk");
            chunk.unreadable = true;
            return chunk;
        }
    }

    // Startup recovery cut the WAL to a rollback intent's boundary.
    const std::string intent_name =
        large_dir + "__" + ChunkFileName(coord, ".wal") + std::string(kIntentSuffix);
    const auto intent = scan.intents.find(intent_name);
    std::optional<std::uint64_t> boundary;
    if (intent != scan.intents.end()) {
        if (intent->second.rollback) {
            ++summary->rollback_intents;
            boundary = intent->second.boundary;
        } else {
            ++summary->committed_intents;
        }
    }

    // A missing WAL or one shorter than the boundary is refused by
    // CheckIntents before any chunk is loaded.
    if (artifacts.wal) {
        auto wal = ReadWholeFile(wal_path);
        summary->source_bytes += wal.size();
        if (boundary.has_value() && wal.size() >= *boundary) {
            wal.resize(static_cast<std::size_t>(*boundary));
        }
        bool geometry_mismatch = false;
        if (wal.size() >= kWalHeaderSize && std::memcmp(wal.data(), kWalMagic, sizeof(kWalMagic)) == 0) {
            try {
                legacy::ValidateLegacyWalHeader(wal, geometry, chunk_coord);
                ++chunk.geometry_matches;
            } catch (const std::exception& e) {
                geometry_mismatch = std::string(e.what()) == "WAL geometry mismatch";
            }
        }
        if (geometry_mismatch) {
            chunk.geometry_mismatches.push_back(wal_path.string());
            chunk.losses.push_back(
                wal_path.string() + ": header records another geometry; the old server dropped its " +
                std::to_string(wal.size()) + " bytes");
        } else if (!wal.empty()) {
            const auto replay =
                legacy::ReplayLegacyWal(wal, geometry, chunk_coord, &chunk.payload, &chunk.presence);
            if (!replay.replayable) {
                if (HoldsNoData(wal)) {
                    ++summary->torn_tails;
                } else {
                    chunk.losses.push_back(
                        wal_path.string() + ": not replayable (" + replay.stop_reason + "); its " +
                        std::to_string(wal.size()) + " bytes were dropped");
                }
            } else {
                switch (replay.wal_version) {
                    case 2: ++summary->wals_v2; break;
                    case 3: ++summary->wals_v3; break;
                    case 4: ++summary->wals_v4; break;
                    default: ++summary->wals_headerless; break;
                }
                if (replay.wal_version != 4 && replay.applied_frames > 0) {
                    ++summary->wals_mixed;
                }
                if (replay.tail_truncated_or_corrupt) {
                    // Structure after the stop is data a writer appended there.
                    if (HasStructureAfter(wal, replay.stop_offset, geometry)) {
                        chunk.losses.push_back(
                            wal_path.string() + ": replay stopped at byte " +
                            std::to_string(replay.stop_offset) + " (" + replay.stop_reason +
                            ") and the " + std::to_string(wal.size() - replay.stop_offset) +
                            " bytes after it, which hold more writes, were dropped");
                    } else {
                        ++summary->torn_tails;
                    }
                }
                if (replay.applied_frames > 0) {
                    chunk.revision = replay.revision;
                }
            }
        }
    }

    if (chunk.revision == std::numeric_limits<std::uint64_t>::max()) {
        chunk.problems.push_back(
            image_path.string() + ": revision at the maximum; the version clock is exhausted");
    }
    auto canonical_payload = chunk.payload;
    auto canonical_presence = chunk.presence;
    MaskUnusedPayloadBits(geometry, &canonical_payload);
    MaskUnusedPresenceBits(geometry, &canonical_presence);
    CanonicalizeAbsentBlocks(geometry, canonical_presence, &canonical_payload);
    chunk.present = ChunkPresent(canonical_presence);
    chunk.canonicalized =
        chunk.present && (canonical_payload != chunk.payload || canonical_presence != chunk.presence);
    chunk.payload = std::move(canonical_payload);
    chunk.presence = std::move(canonical_presence);
    return chunk;
}

// What pass 1 found for a chunk; pass 2 must find the same.
struct ChunkFingerprint {
    bool unreadable = false;
    bool present = false;
    std::uint64_t revision = 0;
    std::uint64_t state_hash = 0;
    std::size_t losses = 0;
    bool operator==(const ChunkFingerprint&) const = default;
};

// Startup recovery of the old server: every intent names a WAL by its path.
// A malformed name, a rollback whose WAL is missing (with data before the
// boundary) or shorter than the boundary made it refuse to start.
void CheckIntents(
    const std::filesystem::path& source,
    const Geometry& geometry,
    const SourceScan& scan,
    std::vector<std::string>* problems,
    std::vector<std::string>* notes) {
    for (const auto& [name, intent] : scan.intents) {
        const auto intent_path = source / std::string(kIntentDir) / name;
        const std::string flat = name.substr(0, name.size() - kIntentSuffix.size());
        std::filesystem::path wal_path = source;
        bool malformed = false;
        std::size_t start = 0;
        while (true) {
            const std::size_t separator = flat.find("__", start);
            const std::string component =
                separator == std::string::npos ? flat.substr(start) : flat.substr(start, separator - start);
            if (component.empty() || component == "." || component == ".." ||
                component.find('/') != std::string::npos || component.find('\\') != std::string::npos) {
                malformed = true;
                break;
            }
            wal_path /= component;
            if (separator == std::string::npos) {
                break;
            }
            start = separator + 2U;
        }
        if (malformed) {
            problems->push_back(
                intent_path.string() + ": malformed conditional intent name; the old server refused to start");
            continue;
        }
        if (intent.rollback) {
            std::error_code ec;
            const bool exists = std::filesystem::is_regular_file(wal_path, ec);
            if (exists && std::filesystem::file_size(wal_path) < intent.boundary) {
                problems->push_back(
                    wal_path.string() + ": shorter than its rollback intent's boundary; the old "
                    "server refused to start");
            } else if (!exists && intent.boundary != 0U) {
                problems->push_back(
                    wal_path.string() + ": missing although a rollback intent keeps " +
                    std::to_string(intent.boundary) + " bytes of it; the old server refused to start");
            }
        }
        std::int64_t lx = 0;
        std::int64_t ly = 0;
        std::int64_t cx = 0;
        std::int64_t cy = 0;
        const auto split = name.find("__");
        const bool chunk_intent =
            split != std::string::npos && ParseCoordName(name.substr(0, split), "L_", "", &lx, &ly) &&
            ParseCoordName(name.substr(split + 2), "C_", ".wal.rollback", &cx, &cy) &&
            geometry.ChunkToLarge(ChunkCoord{cx, cy}).x == lx &&
            geometry.ChunkToLarge(ChunkCoord{cx, cy}).y == ly;
        if (!chunk_intent) {
            notes->push_back(name + ": conditional intent for no chunk WAL; no data depends on it");
        }
    }
}

[[nodiscard]] std::uint64_t StateHash(
    const std::vector<std::uint8_t>& payload,
    const std::vector<std::uint8_t>& presence) {
    std::uint64_t hash = 1469598103934665603ULL;  // FNV-1a
    for (const auto* part : {&payload, &presence}) {
        for (const std::uint8_t byte : *part) {
            hash = (hash ^ byte) * 1099511628211ULL;
        }
    }
    return hash;
}

// Where the result is built: next to `to`, or inside it when it already
// exists (it is empty; it may be a mount point that cannot be replaced).
[[nodiscard]] ChunkFingerprint Fingerprint(const LoadedChunk& chunk) {
    return ChunkFingerprint{
        .unreadable = chunk.unreadable,
        .present = chunk.present,
        .revision = chunk.revision,
        .state_hash = StateHash(chunk.payload, chunk.presence),
        .losses = chunk.losses.size(),
    };
}

[[nodiscard]] std::filesystem::path StagingPath(const std::filesystem::path& to, bool to_exists) {
    std::random_device random;
    std::ostringstream suffix;
    suffix << std::hex << random() << random();
    if (to_exists) {
        return to / (".migrate-" + suffix.str());
    }
    return to.parent_path() / ("." + to.filename().string() + ".migrate-" + suffix.str());
}

// Moves the finished result into place. Into an existing (empty) `to`, the
// data-directory manifest moves last: an interrupted move leaves no manifest,
// and chunkdb refuses such a directory instead of reading part of it.
void PublishResult(const std::filesystem::path& staging, const std::filesystem::path& to, bool to_exists) {
    if (!to_exists) {
        std::filesystem::rename(staging, to);
        SyncDirectoryPath(to.parent_path().empty() ? std::filesystem::path(".") : to.parent_path());
        return;
    }
    const std::string manifest = "chunkdb.manifest";
    for (const auto& entry : std::filesystem::directory_iterator(staging)) {
        if (entry.path().filename() != manifest) {
            std::filesystem::rename(entry.path(), to / entry.path().filename());
        }
    }
    SyncDirectoryPath(to);
    std::filesystem::rename(staging / manifest, to / manifest);
    SyncDirectoryPath(to);
    std::filesystem::remove(staging);
}

void CheckDestination(const MigrateOptions& options) {
    std::error_code ec;
    const auto from = std::filesystem::weakly_canonical(options.from, ec);
    const auto to = std::filesystem::weakly_canonical(options.to, ec);
    if (ec) {
        throw std::runtime_error("cannot resolve " + options.to.string() + ": " + ec.message());
    }
    const auto inside = [](const std::filesystem::path& path, const std::filesystem::path& dir) {
        const auto relative = path.lexically_relative(dir);
        return !relative.empty() && *relative.begin() != "..";
    };
    if (from == to || inside(to, from) || inside(from, to)) {
        throw MigrateRefused("invalid destination", {"--to must be outside the source directory"});
    }
    if (std::filesystem::exists(options.to)) {
        if (!std::filesystem::is_directory(options.to) || !std::filesystem::is_empty(options.to)) {
            throw MigrateRefused(
                "invalid destination", {options.to.string() + ": exists and is not an empty directory"});
        }
    }
}

}  // namespace

MigrateSummary Migrate(const MigrateOptions& requested) {
    // "out/" names the directory "out".
    MigrateOptions options = requested;
    for (auto* path : {&options.from, &options.to}) {
        *path = path->lexically_normal();
        if (!path->has_filename() && path->has_parent_path()) {
            *path = path->parent_path();
        }
    }
    const Geometry geometry(options.geometry);
    if (!std::filesystem::is_directory(options.from)) {
        throw MigrateRefused("no source", {options.from.string() + ": not a directory"});
    }
    CheckDestination(options);
    const SourceLock source_lock(options.from);

    MigrateSummary summary;
    SourceScan scan = ScanSource(options.from, geometry);
    summary.notes = scan.notes;
    summary.source_clock_ceiling = scan.clock_ceiling;
    if (!source_lock.locked()) {
        summary.notes.push_back(
            options.from.string() + ": has no writer lock file, so it was not locked; no server may use it during the conversion");
    }

    // Pass 1: load every chunk and find every reason not to convert, before
    // anything is written. Each chunk's result is kept to check pass 2.
    std::vector<std::string> problems = scan.problems;
    CheckIntents(options.from, geometry, scan, &problems, &summary.notes);
    std::vector<std::string> losses;
    std::vector<std::string> geometry_mismatches;
    std::uint64_t geometry_matches = 0;
    std::uint64_t max_revision = 0;
    std::map<Coord, ChunkFingerprint> pass1;
    {
        MigrateSummary counts;
        for (const auto& [coord, artifacts] : scan.chunks) {
            const auto chunk = LoadChunk(options.from, geometry, coord, artifacts, scan, &counts);
            problems.insert(problems.end(), chunk.problems.begin(), chunk.problems.end());
            losses.insert(losses.end(), chunk.losses.begin(), chunk.losses.end());
            geometry_mismatches.insert(
                geometry_mismatches.end(), chunk.geometry_mismatches.begin(), chunk.geometry_mismatches.end());
            geometry_matches += chunk.geometry_matches;
            max_revision = std::max(max_revision, chunk.revision);
            pass1[coord] = Fingerprint(chunk);
        }
    }
    // No header agrees with the flags: they are wrong, not the headers.
    if (!geometry_mismatches.empty() && geometry_matches == 0U) {
        std::vector<std::string> mismatch_problems;
        for (const auto& path : geometry_mismatches) {
            mismatch_problems.push_back(path + ": written with another geometry than the given one");
        }
        problems.insert(problems.begin(), mismatch_problems.begin(), mismatch_problems.end());
    }
    if (!problems.empty()) {
        throw MigrateRefused("the source cannot be converted", problems);
    }
    if (!losses.empty() && !options.accept_loss) {
        throw MigrateRefused(
            "the old server lost data from this source (it dropped it, or could not read it); "
            "converting it loses the same data. Rerun with --accept-loss to accept that",
            losses);
    }

    // Pass 2: write the chunks into a staging directory.
    const bool to_exists = std::filesystem::exists(options.to);
    const auto staging = StagingPath(options.to, to_exists);
    std::map<Coord, std::pair<std::uint64_t, std::uint64_t>> expected;  // revision, state hash
    try {
        {
            CatalogConfig config;
            config.data_dir = staging;
            config.default_geometry = options.geometry;
            // Images are written unsynced and made durable by one barrier.
            config.default_options = options.table_options;
            config.default_options.durability_mode = DurabilityMode::kRelaxed;
            TableCatalog catalog(config);
            const auto table = catalog.Find("default");
            auto lease = table->Acquire();
            if (!lease.has_value()) {
                throw std::runtime_error("the new default table is unavailable");
            }
            // Every token the old server handed out is below its clock's
            // ceiling: the new clock starts there, and chunks that never had
            // a revision get theirs from there.
            lease->store().RaiseVersionClockFloor(scan.clock_ceiling);
            std::uint64_t next_revision = std::max(scan.clock_ceiling, max_revision + 1);
            for (const auto& [coord, artifacts] : scan.chunks) {
                auto chunk = LoadChunk(options.from, geometry, coord, artifacts, scan, &summary);
                if (!chunk.problems.empty() || !(Fingerprint(chunk) == pass1.at(coord))) {
                    throw std::runtime_error(
                        "chunk " + std::to_string(coord.first) + " " + std::to_string(coord.second) +
                        " changed in the source during the conversion; stop every process that uses it");
                }
                summary.losses.insert(summary.losses.end(), chunk.losses.begin(), chunk.losses.end());
                if (chunk.unreadable) {
                    continue;
                }
                if (!chunk.present) {
                    ++summary.absent_chunks;
                    continue;
                }
                if (chunk.revision != 0U) {
                    ++summary.persisted_revisions;
                } else {
                    chunk.revision = next_revision++;
                    ++summary.assigned_revisions;
                }
                if (chunk.canonicalized) {
                    ++summary.canonicalized_chunks;
                }
                expected[coord] = {chunk.revision, StateHash(chunk.payload, chunk.presence)};
                lease->store().ImportChunk(
                    coord.first, coord.second, std::move(chunk.payload), std::move(chunk.presence),
                    chunk.revision);
                ++summary.chunks;
            }
            lease.reset();
            catalog.WalBarrier();
            if (options.table_options.durability_mode != DurabilityMode::kRelaxed) {
                catalog.SetOptions("default", options.table_options);
            }
        }

        // Pass 3: read the result back from disk and verify it.
        {
            CatalogConfig config;
            config.data_dir = staging;
            config.access_mode = AccessMode::kReadOnly;
            config.default_geometry_fields = 0;  // the table's own geometry
            TableCatalog catalog(config);
            auto lease = catalog.Find("default")->Acquire();
            auto& store = lease->store();
            const auto scan_page = store.ScanPopulatedChunks(false, {}, expected.size() + 1U);
            if (scan_page.coords.size() != expected.size()) {
                throw std::runtime_error("the result holds another number of chunks than was written");
            }
            for (const auto& [coord, want] : expected) {
                const auto state = store.GetChunkStateBytes(coord.first, coord.second);
                const std::size_t payload_bytes = geometry.ChunkPayloadBytes();
                const std::vector<std::uint8_t> payload(state.begin(), state.begin() + static_cast<std::ptrdiff_t>(payload_bytes));
                const std::vector<std::uint8_t> presence(state.begin() + static_cast<std::ptrdiff_t>(payload_bytes), state.end());
                if (StateHash(payload, presence) != want.second ||
                    store.GetChunkVersion(coord.first, coord.second) != want.first) {
                    throw std::runtime_error(
                        "chunk " + std::to_string(coord.first) + " " + std::to_string(coord.second) +
                        " reads back differently from what was written");
                }
            }
        }
        std::ostringstream findings;
        summary.verify = VerifyDataDirectory(staging, findings);
        if (summary.verify.errors != 0U || summary.verify.warnings != 0U) {
            throw std::runtime_error("chunkdb_verify found problems in the result:\n" + findings.str());
        }

        PublishResult(staging, options.to, to_exists);
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove_all(staging, ec);
        throw;
    }
    return summary;
}

}  // namespace chunkdb::migrate
