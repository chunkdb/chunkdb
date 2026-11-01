#include "feed_archive.hpp"

#include <algorithm>
#include <bit>
#include <charconv>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "chunkdb/crc32.hpp"
#include "chunkdb/file_layout.hpp"
#include "feed_protocol.hpp"
#include "feed_prefix.hpp"
#include "feed_slot_records.hpp"
#include "feature_flags.hpp"
#include "snapshot_generation.hpp"
#include "txn_history.hpp"
#include "wal_replay.hpp"
#include "typed_chunk_form.hpp"
#include "chunk_coordinate_index.hpp"

namespace chunkdb {
namespace {

// Short-lived native handles allow checkpoint to rename the same live WAL.
// In particular, Windows requires delete sharing even for read-only opens.
class ReadFile {
  public:
    explicit ReadFile(const std::filesystem::path& path) : path_(path) {
#ifdef _WIN32
        file_ = CreateFileW(path.c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
            OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (file_ == INVALID_HANDLE_VALUE) Fail("open");
#else
        file_ = open(path.c_str(), O_RDONLY);
        if (file_ < 0) Fail("open");
#endif
    }
    ~ReadFile() {
#ifdef _WIN32
        if (file_ != INVALID_HANDLE_VALUE) CloseHandle(file_);
#else
        if (file_ >= 0) close(file_);
#endif
    }
    ReadFile(const ReadFile&) = delete;
    ReadFile& operator=(const ReadFile&) = delete;
    std::uint64_t Size() const {
#ifdef _WIN32
        LARGE_INTEGER size;
        if (!GetFileSizeEx(file_, &size) || size.QuadPart < 0) Fail("stat");
        return static_cast<std::uint64_t>(size.QuadPart);
#else
        struct stat status {};
        if (fstat(file_, &status) != 0 || status.st_size < 0) Fail("stat");
        return static_cast<std::uint64_t>(status.st_size);
#endif
    }
    std::vector<std::uint8_t> At(std::uint64_t offset, std::size_t count) {
        std::vector<std::uint8_t> bytes(count);
        std::size_t used = 0;
        while (used < count) {
#ifdef _WIN32
            if (offset + used > static_cast<std::uint64_t>(std::numeric_limits<LONGLONG>::max())) Fail("seek");
            LARGE_INTEGER position;
            position.QuadPart = static_cast<LONGLONG>(offset + used);
            if (!SetFilePointerEx(file_, position, nullptr, FILE_BEGIN)) Fail("seek");
            DWORD read = 0;
            const auto requested = static_cast<DWORD>(std::min<std::size_t>(count - used, MAXDWORD));
            if (!::ReadFile(file_, bytes.data() + used, requested, &read, nullptr)) Fail("read");
            if (read == 0U) break;
            used += read;
#else
            if (offset + used > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) Fail("seek");
            const auto read = pread(file_, bytes.data() + used, count - used, static_cast<off_t>(offset + used));
            if (read < 0) {
                if (errno == EINTR) continue;
                Fail("read");
            }
            if (read == 0) break;
            used += static_cast<std::size_t>(read);
#endif
        }
        bytes.resize(used);
        return bytes;
    }
    std::vector<std::uint8_t> Image(const Geometry& geometry, ChunkCoord coord,
        const StoreId& epoch, FeatureFlags features) {
        const auto size = Size();
        const auto prefix = At(0U, 12U);
        if (prefix.size() != 12U || ReadLe16(prefix, 10U) > kImageMaxSections)
            throw std::runtime_error("feed image directory is truncated or oversized");
        const auto header_size = ChunkImageHeaderBytes(geometry.dims(), ReadLe16(prefix, 10U));
        const auto header = At(0U, header_size);
        try {
            (void)ParseChunkImageHeader(header, size,
                {.dims = geometry.dims(), .epoch = epoch, .coord = coord, .admitted_features = features},
                {.max_file_bytes = kMaxChunkImageFileBytes, .max_section_stored_bytes = kMaxChunkImageFileBytes,
                    .max_section_raw_bytes = kMaxChunkImageRawBytes, .max_total_raw_bytes = kMaxChunkImageRawBytes});
        } catch (const std::invalid_argument& error) {
            throw std::runtime_error(error.what());
        }
        auto bytes = At(0U, static_cast<std::size_t>(size));
        if (bytes.size() != size) throw std::runtime_error("feed image is truncated");
        return bytes;
    }
  private:
    [[noreturn]] void Fail(std::string_view operation) const {
#ifdef _WIN32
        const std::error_code error(static_cast<int>(GetLastError()), std::system_category());
#else
        const std::error_code error(errno, std::generic_category());
#endif
        throw std::system_error(error, "feed archive cannot " + std::string(operation) + " " + path_.string());
    }
    std::filesystem::path path_;
#ifdef _WIN32
    HANDLE file_ = INVALID_HANDLE_VALUE;
#else
    int file_ = -1;
#endif
};

template <typename T>
bool Number(std::string_view text, T* value) {
    if (text.empty()) return false;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), *value);
    return ec == std::errc{} && end == text.data() + text.size();
}

struct ArchiveName {
    ChunkCoord coord;
    std::uint64_t first = 0;
    std::uint64_t last = 0;
};

std::optional<ArchiveName> ParseArchiveName(std::string_view name, std::uint8_t dims) {
    if (!name.ends_with(".wal")) return std::nullopt;
    name.remove_suffix(4U);
    const auto dot = name.find('.');
    if (dot == std::string_view::npos) return std::nullopt;
    const auto dash = name.find('-', dot + 1U);
    ArchiveName result;
    if (dash == std::string_view::npos || !ParseChunkFileStem(name.substr(0U, dot), dims, &result.coord) ||
        !Number(name.substr(dot + 1U, dash - dot - 1U), &result.first) ||
        !Number(name.substr(dash + 1U), &result.last) || result.first == 0U || result.last < result.first)
        throw std::runtime_error("invalid feed archive name " + std::string(name));
    return result;
}

std::optional<std::int64_t> Absolute(std::int64_t chunk, std::uint32_t width, std::size_t local) {
    const auto factor = static_cast<std::uint64_t>(width);
    const auto offset = static_cast<std::uint64_t>(local);
    if (chunk >= 0) {
        const auto limit = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) - offset;
        if (static_cast<std::uint64_t>(chunk) > limit / factor) return std::nullopt;
        return static_cast<std::int64_t>(static_cast<std::uint64_t>(chunk) * factor + offset);
    }
    const auto magnitude = 0ULL - static_cast<std::uint64_t>(chunk);
    if (magnitude > ((1ULL << 63U) + offset) / factor) return std::nullopt;
    return std::bit_cast<std::int64_t>(std::uint64_t{0} - (magnitude * factor - offset));
}

struct Cursor {
    ChunkCoord coord;
    std::filesystem::path path;
    std::uint64_t first = 0;
    std::uint64_t last = 0;  // Zero for the captured prefix of a live WAL.
    std::uint64_t limit = 0;
    std::uint64_t offset = 0U;
    std::uint64_t previous = 0;
    std::uint64_t next = 0;
    std::size_t frame_size = 0;
    bool done = false;
    bool initialized = false;
    bool had_current_base = false;
    bool movable = false;
    bool completed_prefix = false;
    bool loaded = false;
    std::optional<ChunkIndexRecord> retained_empty{};
    std::uint64_t schema_version = 0;
    ChunkState state;
};

// Only classify whether a captured live image predates this WAL. The header
// and its directory protect the revision; body/state validation remains lazy
// in ParseChunkImage when the image is actually used as a base.
bool HasOriginalBase(const std::filesystem::path& path, ChunkCoord coord, const StoreId& epoch,
                     FeatureFlags features, std::uint64_t first, std::uint8_t dims) {
    if (!std::filesystem::exists(path)) return false;
    ReadFile file(path);
    const auto fixed_size = ChunkImageFixedHeaderBytes(dims);
    const auto fixed = file.At(0U, fixed_size);
    if (fixed.size() != fixed_size || std::memcmp(fixed.data(), kImageMagic, kImageMagicSize) != 0 ||
        ReadLe16(fixed, 8U) != kImageFormatVersion || ReadLe16(fixed, 10U) > kImageMaxSections)
        throw std::runtime_error("feed archive current image header is invalid");
    const auto directory_end = fixed_size + static_cast<std::size_t>(ReadLe16(fixed, 10U)) * kImageSectionEntrySize;
    const auto header = file.At(0U, directory_end + 4U);
    try {
        const auto checked = ParseChunkImageHeader(header, file.Size(),
            ChunkFileContext{.dims = dims, .epoch = epoch, .coord = coord, .admitted_features = features},
            ChunkImageHeaderLimits{.max_file_bytes = kMaxChunkImageFileBytes,
                .max_section_stored_bytes = kMaxChunkImageFileBytes,
                .max_section_raw_bytes = kMaxChunkImageRawBytes,
                .max_total_raw_bytes = kMaxChunkImageRawBytes});
        return checked.info.revision < first;
    } catch (const std::invalid_argument& error) {
        throw std::runtime_error("feed archive current image metadata is invalid: " + std::string(error.what()));
    }
}

}  // namespace

std::vector<std::uint8_t> ReadFeedSnapshotFile(const std::filesystem::path& path) {
    ReadFile file(path);
    const auto size = file.Size();
    if (size > std::numeric_limits<std::size_t>::max())
        throw std::runtime_error("feed snapshot file is too large: " + path.string());
    return file.At(0U, static_cast<std::size_t>(size));
}

std::optional<ArchiveBaseDescriptor> ReadFeedArchiveBaseDescriptor(
    const std::filesystem::path& path, const Geometry& geometry, const StoreId& epoch,
    ChunkCoord coord, std::uint64_t first) {
    try {
        ReadFile file(path);
        const auto length = ArchiveBaseDescriptorBytes(geometry.dims());
        if (file.Size() != length) throw std::runtime_error("feed archive base descriptor has wrong length");
        try {
            auto descriptor = ParseArchiveBaseDescriptor(file.At(0U, length), geometry.dims(), epoch,
                coord, first, kMaxChunkImageFileBytes);
            (void)geometry.LayoutAt(descriptor.base_schema);
            return descriptor;
        } catch (const std::invalid_argument& error) {
            throw std::runtime_error("invalid feed archive base descriptor: " + std::string(error.what()));
        }
    } catch (const std::system_error& error) {
        if (error.code() == std::errc::no_such_file_or_directory) return std::nullopt;
        throw;
    }
}

ChunkStateImage ReadPinnedFeedArchiveBase(const std::filesystem::path& path,
    const Geometry& geometry, const StoreId& epoch, ChunkCoord coord, FeatureFlags features,
    const ArchiveBaseDescriptor& descriptor) {
    ReadFile file(path);
    const auto bytes = file.Image(geometry, coord, epoch, features);
    auto image = ParseChunkImage(bytes, geometry, coord, epoch, features);
    ValidatePinnedArchiveBaseImage(descriptor,
        {.dims = geometry.dims(), .epoch = epoch, .coord = coord, .revision = image.revision,
            .commit_time_ms = image.commit_time_ms, .schema_version = image.schema_version},
        bytes, kMaxChunkImageFileBytes);
    return image;
}

struct FeedArchiveReader::Impl {
    std::filesystem::path root;
    Geometry geometry;
    StoreId epoch;
    FeatureFlags features;
    FeedPosition position;
    std::uint64_t through;
    std::shared_ptr<void> pin;
    std::vector<Cursor> cursors;
    bool usable = true;
    bool started = false;
    std::optional<std::size_t> chunk_forms_budget;
    std::size_t entry_budget = kDefaultFeedBufferBytes;

    Impl(const std::filesystem::path& directory, const Geometry& shape, const StoreId& store_id,
         FeedPosition from, std::uint64_t durable, std::shared_ptr<void> retention, FeatureFlags flags,
         const std::vector<FeedWalPrefix>* prefixes = nullptr)
        : root(directory), geometry(shape), epoch(store_id), features(flags), position(from),
          through(durable), pin(std::move(retention)) {
        if (from.epoch != epoch) throw std::invalid_argument("feed archive position belongs to another table epoch");
        if (from.revision > through) throw std::invalid_argument("feed archive position exceeds the durable frontier");
        if (prefixes) {
            CaptureCompletedPrefixes(*prefixes);
            return;
        }
        const auto generation_path = root / "chunkdb.snapshot";
        const bool generation_required = std::filesystem::exists(generation_path);
        const auto generation = ReadSnapshotGenerationForScan(generation_path, generation_required);
        if ((generation.second & 1U) != 0U) throw std::runtime_error("feed archive snapshot is being modified");
        ChunkCoordinateIndex retained_index(root, geometry, epoch, true);

        std::vector<std::vector<std::uint8_t>> transactions;
        const auto intent_dir = ConditionalIntentDirectory(root);
        if (std::filesystem::exists(intent_dir)) for (const auto& item : std::filesystem::directory_iterator(intent_dir)) {
            const auto name = item.path().filename().string();
            if (IsTxnIntentFileName(name)) {
                auto bytes = LoadRecoveryTxnIntent(item.path());
                TxnIntent intent;
                if (!TryParseTxnIntent(bytes, &intent, geometry.dims(), epoch)) throw std::runtime_error("malformed feed transaction intent");
                if (intent.state == TxnIntentState::kRollback) through = std::min(through, intent.version - 1U);
                transactions.push_back(std::move(bytes));
            } else if (name.ends_with(".rollback")) {
                ConditionalIntentState state;
                std::uint64_t size = 0U;
                if (!TryParseConditionalIntent(LoadFile(item.path()), &state, &size))
                    throw std::runtime_error("feed archive contains a malformed conditional intent");
                if (state == ConditionalIntentState::kRollback && size != 0U &&
                    !std::filesystem::exists(WalPathForConditionalIntent(root, item.path())))
                    throw std::runtime_error("feed archive is missing a conditional WAL");
            }
        }
        // Validate even an intent whose chunk has no WAL in the listing.
        (void)TxnRollbackBoundaryForChunk(transactions, {}, geometry, epoch);

        const auto archives = root / kFeedArchiveDirName;
        if (std::filesystem::exists(archives)) for (const auto& item : std::filesystem::directory_iterator(archives)) {
            if (!item.is_regular_file()) continue;
            const auto name = ParseArchiveName(item.path().filename().string(), geometry.dims());
            if (!name || name->last <= from.revision || name->first > through) continue;
            Cursor cursor;
            cursor.coord = name->coord;
            cursor.path = item.path();
            cursor.first = name->first;
            cursor.last = name->last;
            cursor.limit = std::filesystem::file_size(cursor.path);
            // Archive names already give their next revision. Opening and
            // validating the file waits until the merge reaches this cursor.
            cursor.next = cursor.first;
            cursors.push_back(std::move(cursor));
        }
        std::sort(cursors.begin(), cursors.end(), [](const auto& a, const auto& b) {
            if (a.coord != b.coord) return a.coord < b.coord;
            return a.first < b.first;
        });
        for (std::size_t i = 1U; i < cursors.size(); ++i) if (cursors[i - 1U].coord == cursors[i].coord &&
            cursors[i].first <= cursors[i - 1U].last)
            throw std::runtime_error("feed archive revision ranges overlap for a chunk");
        for (const auto& large : std::filesystem::directory_iterator(root)) {
            const auto large_name = large.path().filename().string();
            if (!large_name.starts_with("L_") || !large.is_directory()) continue;
            for (const auto& item : std::filesystem::directory_iterator(large.path())) {
                const auto filename = item.path().filename().string();
                if (!filename.ends_with(".wal")) continue;
                Cursor cursor;
                if (!ParseChunkFileStem(std::string_view(filename).substr(0U, filename.size() - 4U), geometry.dims(), &cursor.coord)) continue;
                cursor.path = item.path();
                cursor.limit = std::filesystem::file_size(cursor.path);
                auto boundary = TxnRollbackBoundaryForChunk(transactions, cursor.coord, geometry, epoch);
                const auto intent_path = ConditionalIntentPathForWal(root, cursor.path);
                if (std::filesystem::exists(intent_path)) {
                    ConditionalIntentState state;
                    std::uint64_t size = 0;
                    if (!TryParseConditionalIntent(LoadFile(intent_path), &state, &size))
                        throw std::runtime_error("feed archive contains a malformed conditional intent");
                    if (state == ConditionalIntentState::kRollback) boundary = std::min(boundary.value_or(size), size);
                }
                if (boundary) {
                    if (cursor.limit < *boundary) throw std::runtime_error("feed archive WAL is shorter than its intent boundary");
                    cursor.limit = *boundary;
                }
                Initialize(cursor);
                if (cursor.done || cursor.next > through) continue;
                cursor.had_current_base = HasOriginalBase(ChunkDataPath(root, geometry, cursor.coord), cursor.coord,
                    epoch, features, cursor.first, geometry.dims());
                cursor.retained_empty = retained_index.EmptyBoundary(cursor.coord);
                const auto duplicate = std::find_if(cursors.begin(), cursors.end(), [&](const auto& archived) {
                    return archived.coord == cursor.coord && archived.first == cursor.first;
                });
                if (duplicate != cursors.end()) {
                    if (!std::filesystem::equivalent(cursor.path, duplicate->path))
                        throw std::runtime_error("feed archive and live WAL disagree on segment identity");
                } else {
                    cursors.push_back(std::move(cursor));
                }
            }
        }
        // Missing WALs named by nonzero rollback boundaries are damage.
        for (const auto& bytes : transactions) {
            TxnIntent intent;
            if (!TryParseTxnIntent(bytes, &intent, geometry.dims(), epoch)) throw std::runtime_error("malformed feed transaction intent");
            if (intent.state != TxnIntentState::kRollback) continue;
            for (const auto& entry : intent.entries) if (entry.wal_boundary != 0U &&
                !std::filesystem::exists(ChunkWalPath(root, geometry, entry.coord)))
                throw std::runtime_error("feed archive is missing a transaction WAL");
        }
        if (ReadSnapshotGenerationForScan(generation_path, generation_required) != generation)
            throw std::runtime_error("feed archive snapshot changed while it was captured");
    }

    void CaptureCompletedPrefixes(const std::vector<FeedWalPrefix>& prefixes) {
        const auto directory = root / kFeedArchiveDirName;
        if (std::filesystem::exists(directory)) for (const auto& item : std::filesystem::directory_iterator(directory)) {
            if (!item.is_regular_file()) continue;
            const auto name = ParseArchiveName(item.path().filename().string(), geometry.dims());
            if (!name || name->last <= position.revision || name->first > through) continue;
            Cursor cursor;
            cursor.coord = name->coord;
            cursor.path = item.path();
            cursor.first = name->first;
            cursor.last = name->last;
            cursor.limit = std::filesystem::file_size(cursor.path);
            cursor.next = cursor.first;
            cursors.push_back(std::move(cursor));
        }
        for (const auto& prefix : prefixes) {
            if (prefix.first == 0U || prefix.last < prefix.first || prefix.last > through ||
                prefix.limit <= ChunkWalHeaderBytes(geometry.dims()))
                throw std::invalid_argument("invalid completed feed WAL prefix");
            if (prefix.last <= position.revision) continue;
            const auto archived = std::find_if(cursors.begin(), cursors.end(), [&](const auto& cursor) {
                return cursor.coord == prefix.coord && cursor.first == prefix.first;
            });
            if (archived != cursors.end()) {
                if (archived->last < prefix.last || archived->limit < prefix.limit)
                    throw std::runtime_error("archive is shorter than its completed feed WAL prefix");
                archived->last = prefix.last;
                archived->limit = prefix.limit;
                archived->completed_prefix = true;
                archived->retained_empty = prefix.retained_empty;
                continue;
            }
            Cursor cursor;
            cursor.coord = prefix.coord;
            cursor.path = ChunkWalPath(root, geometry, prefix.coord);
            cursor.first = prefix.first;
            cursor.last = prefix.last;
            cursor.limit = prefix.limit;
            cursor.next = prefix.first;
            cursor.movable = true;
            cursor.completed_prefix = true;
            cursor.retained_empty = prefix.retained_empty;
            cursors.push_back(std::move(cursor));
        }
        std::sort(cursors.begin(), cursors.end(), [](const auto& a, const auto& b) {
            if (a.coord != b.coord) return a.coord < b.coord;
            return a.first < b.first;
        });
        for (std::size_t i = 1U; i < cursors.size(); ++i) if (cursors[i - 1U].coord == cursors[i].coord &&
            cursors[i].first <= cursors[i - 1U].last)
            throw std::runtime_error("feed archive revision ranges overlap for a chunk");
    }

    std::filesystem::path Resolve(const Cursor& cursor) const {
        if (cursor.last != 0U && !cursor.movable) return cursor.path;
        const auto directory = root / kFeedArchiveDirName;
        if (std::filesystem::exists(directory)) for (const auto& item : std::filesystem::directory_iterator(directory)) {
            const auto name = ParseArchiveName(item.path().filename().string(), geometry.dims());
            if (name && name->coord == cursor.coord && name->first == cursor.first) return item.path();
        }
        return cursor.path;
    }

    std::pair<std::filesystem::path, std::unique_ptr<ReadFile>> Open(const Cursor& cursor) const {
        auto path = Resolve(cursor);
        std::unique_ptr<ReadFile> file;
        try { file = std::make_unique<ReadFile>(path); }
        catch (const std::system_error& error) {
            if (error.code() != std::errc::no_such_file_or_directory || (cursor.last != 0U && !cursor.movable)) throw;
            const auto archived = Resolve(cursor);
            if (archived == path) throw;
            path = archived;
            file = std::make_unique<ReadFile>(path);
        }
        if (cursor.movable && path == cursor.path) {
            // Opening a reused live name implies the old segment was already
            // archived. Re-resolve before reading even a header; otherwise a
            // future writer's partial header could be mistaken for corruption.
            // If rename happens after this check, our handle still owns the old
            // segment. The retention pin keeps its archive name available.
            const auto archived = Resolve(cursor);
            if (archived != path) {
                path = archived;
                file = std::make_unique<ReadFile>(path);
            }
        }
        return {std::move(path), std::move(file)};
    }

    void Initialize(Cursor& cursor) {
        cursor.initialized = true;
        auto [path, file] = Open(cursor);
        (void)path;
        const auto header_size = ChunkWalHeaderBytes(geometry.dims());
        cursor.offset = header_size;
        const auto header = file->At(0U, static_cast<std::size_t>(std::min<std::uint64_t>(cursor.limit, header_size)));
        if (header.size() < header_size) {
            if (cursor.completed_prefix || cursor.last != 0U) throw std::runtime_error("completed feed WAL prefix has a partial header");
            const auto expected = BuildWalHeader(cursor.coord, epoch, features, geometry.dims());
            const bool zero = std::all_of(header.begin(), header.end(), [](auto byte) { return byte == 0U; });
            if (!zero && !std::equal(header.begin(), header.end(), expected.begin()))
                throw std::runtime_error("feed archive has a damaged partial WAL header");
            cursor.done = true;
            return;
        }
        ValidateWalHeader(header, cursor.coord, epoch, features, geometry.dims());
        Peek(cursor, *file);
        if (!cursor.done) {
            if (cursor.first != 0U && cursor.first != cursor.next)
                throw std::runtime_error("feed archive first revision disagrees with its name");
            cursor.first = cursor.next;
        }
    }

    void Peek(Cursor& cursor, ReadFile& file) {
        cursor.next = 0U;
        if (cursor.offset >= cursor.limit) {
            if (cursor.last != 0U && cursor.previous != cursor.last)
                throw std::runtime_error("feed archive last revision disagrees with its name");
            cursor.done = true;
            return;
        }
        const auto remaining = cursor.limit - cursor.offset;
        const auto fixed = file.At(cursor.offset, static_cast<std::size_t>(std::min<std::uint64_t>(remaining, kWalFrameFixedHeaderSize)));
        if (fixed.size() < kWalFrameFixedHeaderSize) {
            if (cursor.completed_prefix || cursor.last != 0U) throw std::runtime_error("completed feed WAL prefix has a partial frame header");
            cursor.done = true; return;
        }
        if (std::memcmp(fixed.data(), kWalFrameMagic, kWalFrameMagicSize) != 0)
            throw std::runtime_error("feed archive frame magic is damaged");
        const auto header_size = kWalFrameFixedHeaderSize + ReadLe16(fixed, 22U) + kWalFrameHeaderCrcSize;
        if (header_size > remaining) {
            if (cursor.completed_prefix || cursor.last != 0U) throw std::runtime_error("completed feed WAL prefix ends inside a frame header");
            cursor.done = true; return;
        }
        const auto header = file.At(cursor.offset, header_size);
        if (header.size() != header_size) throw std::runtime_error("feed archive WAL shortened while reading");
        if (ReadLe32(header, header_size - 4U) != Crc32(header.data() + kWalFrameMagicSize, header_size - 8U))
            throw std::runtime_error("feed archive frame header checksum mismatch");
        const auto frame_size = static_cast<std::uint64_t>(header_size) + ReadLe32(fixed, 28U) + kWalFrameTrailerSize;
        if (frame_size > kMaxWalFrameBytes)
            throw std::runtime_error("feed archive frame exceeds its byte limit");
        if (frame_size > remaining) {
            if (cursor.completed_prefix || cursor.last != 0U) throw std::runtime_error("completed feed WAL prefix ends inside a frame");
            cursor.done = true; return;
        }
        cursor.next = ReadLe64(fixed, 4U);
        if (cursor.next == 0U || cursor.next <= cursor.previous)
            throw std::runtime_error("feed archive frame revisions do not increase");
        if (cursor.completed_prefix && cursor.next > cursor.last)
            throw std::runtime_error("completed feed WAL prefix exceeds its declared last revision");
        if (cursor.next > through) { cursor.done = true; return; }
        cursor.frame_size = static_cast<std::size_t>(frame_size);
    }

    void LoadBase(Cursor& cursor, std::uint64_t first_schema, bool still_live) {
        if (cursor.loaded) return;
        const auto linked = root / kFeedArchiveDirName / (ChunkFileStem(cursor.coord, geometry.dims()) + "." + std::to_string(cursor.first) + ".chk");
        const auto descriptor_path = root / kFeedArchiveDirName / ArchiveBaseFileName(cursor.coord, geometry.dims(), cursor.first);
        const auto current = ChunkDataPath(root, geometry, cursor.coord);
        const auto read_descriptor = [&] {
            return ReadFeedArchiveBaseDescriptor(descriptor_path, geometry, epoch, cursor.coord, cursor.first);
        };
        const auto use_image = [&](ChunkStateImage image) {
            if (image.revision >= cursor.first || image.schema_version > first_schema)
                throw std::runtime_error("feed archive base overlaps its WAL");
            cursor.state = {.version = image.revision, .payload = std::move(image.payload),
                .presence_bitmap = std::move(image.presence_bitmap), .vars = std::move(image.vars),
                .written = true, .commit_time_ms = image.commit_time_ms, .chunk_columns = std::move(image.chunk_columns)};
            cursor.schema_version = image.schema_version;
            cursor.loaded = true;
        };
        const auto use_empty = [&](std::uint64_t schema, std::uint64_t revision, std::uint64_t time) {
            if (schema > first_schema || revision >= cursor.first)
                throw std::runtime_error("feed archive empty base overlaps its WAL");
            cursor.schema_version = schema;
            cursor.state.payload.assign(geometry.LayoutAt(schema).payload_bytes(), 0U);
            cursor.state.presence_bitmap.assign(ChunkPresenceBitmapBytes(geometry), 0U);
            cursor.state.version = revision;
            cursor.state.commit_time_ms = time;
            cursor.state.written = revision != 0U;
            if (cursor.state.written) cursor.state.chunk_columns = DefaultChunkColumnsAt(geometry, schema);
            cursor.loaded = true;
        };
        const auto use_descriptor = [&](const ArchiveBaseDescriptor& descriptor) {
            if (descriptor.kind == ArchiveBaseKind::kImage) {
                use_image(ReadPinnedFeedArchiveBase(linked, geometry, epoch, cursor.coord, features, descriptor));
            } else {
                if (std::filesystem::exists(linked)) throw std::runtime_error("non-image archive base has an unexpected pinned image");
                use_empty(descriptor.base_schema, descriptor.base_revision, descriptor.base_time_ms);
            }
        };
        if (const auto descriptor = read_descriptor()) { use_descriptor(*descriptor); return; }
        if (!still_live) throw std::runtime_error("feed archive base descriptor is missing");
        std::optional<ChunkStateImage> image;
        try {
            ReadFile file(current);
            // A descriptor is durable before replacing this filename. Check
            // after opening, before parsing a possibly newer schema's image.
            if (const auto descriptor = read_descriptor()) { use_descriptor(*descriptor); return; }
            image = ParseChunkImage(file.Image(geometry, cursor.coord, epoch, features), geometry, cursor.coord, epoch, features);
        } catch (const std::system_error& error) {
            if (error.code() != std::errc::no_such_file_or_directory) throw;
        }
        if (const auto descriptor = read_descriptor()) { use_descriptor(*descriptor); return; }
        auto retained = cursor.retained_empty;
        if (retained && retained->kind != ChunkIndexKind::kDeleted) retained.reset();
        if (retained && (retained->coord != cursor.coord || retained->revision == 0U || retained->schema_version == 0U))
            throw std::runtime_error("invalid retained feed archive base");
        if (retained && retained->revision < cursor.first &&
            (!image || image->revision >= cursor.first || retained->revision >= image->revision)) {
            use_empty(retained->schema_version, retained->revision, retained->commit_time_ms);
            return;
        }
        if (image && image->revision < cursor.first) { use_image(std::move(*image)); return; }
        if (image || (retained && retained->revision >= cursor.first))
            throw std::runtime_error("original live feed base is missing after its WAL was covered");
        use_empty(first_schema, 0U, 0U);
    }

    bool Apply(Cursor& cursor, FeedEntry* entry, std::size_t* retained_rows) {
        if (!cursor.initialized) {
            Initialize(cursor);
            if (cursor.done) return false;
        }
        auto [path, file] = Open(cursor);
        const auto bytes = file->At(cursor.offset, cursor.frame_size);
        if (bytes.size() != cursor.frame_size) throw std::runtime_error("feed archive WAL shortened while reading");
        const auto info = InspectFeedFrame(bytes, geometry, features);
        if (info.revision != cursor.next) throw std::runtime_error("feed archive WAL changed identity");
        LoadBase(cursor, info.schema_version, path == cursor.path && (cursor.last == 0U || cursor.movable));
        if (info.schema_version < cursor.schema_version) throw std::runtime_error("feed archive schema versions do not increase");
        while (cursor.schema_version < info.schema_version) {
            BringChunkColumnsToSchema(geometry, cursor.state.written ? cursor.schema_version : 0U,
                cursor.schema_version + 1U, &cursor.state.chunk_columns);
            TranslateChunk(geometry.LayoutAt(cursor.schema_version), geometry.LayoutAt(cursor.schema_version + 1U),
                cursor.state.presence_bitmap, &cursor.state.payload, &cursor.state.vars);
            ++cursor.schema_version;
        }
        const auto before = cursor.state;
        if (info.gc && ChunkPresent(before.presence_bitmap))
            throw std::runtime_error("feed archive collection frame would delete present blocks");
        (void)ReplayFeedFrame(bytes, geometry, &cursor.state, features);
        if (entry && !info.gc) {
            if (entry->schema_version != 0U && (entry->schema_version != info.schema_version ||
                entry->commit_time_ms != info.commit_time_ms || entry->user != info.user))
                throw std::runtime_error("frames of an archived feed change disagree");
            entry->schema_version = info.schema_version;
            entry->commit_time_ms = info.commit_time_ms;
            entry->user = info.user;
            entry->dims = geometry.dims();
            const auto& layout = geometry.LayoutAt(info.schema_version);
            std::optional<Geometry> historical_geometry;
            if (info.schema_version != geometry.layout().schema().version) {
                auto config = geometry.config();
                config.block_bits = FixedBitsPerBlock(layout.schema());
                historical_geometry.emplace(config, layout.schema(), SchemaAtVersion(geometry.chunk_schema(), info.schema_version));
            }
            const auto& frame_geometry = historical_geometry ? *historical_geometry : geometry;
            const auto& chunk_schema = frame_geometry.chunk_schema();
            if (!chunk_forms_budget && !chunk_schema.columns.empty()) {
                FeedChunkChange change{.chunk = cursor.coord, .before = {}, .after = {}};
                if (before.written) change.before = DecodeChunkColumns(chunk_schema, before.chunk_columns->bytes);
                if (cursor.state.written) change.after = DecodeChunkColumns(chunk_schema, cursor.state.chunk_columns->bytes);
                if (change.before.has_value() != change.after.has_value() ||
                    (change.before && !SameFeedValues(*change.before, *change.after))) {
                    const auto old_bytes = FeedRowRetainedBytes(change.before), new_bytes = FeedRowRetainedBytes(change.after);
                    const auto held = FeedEntryRetainedBytes(*entry);
                    if (held > entry_budget || old_bytes > entry_budget - held || new_bytes > entry_budget - held - old_bytes)
                        throw std::length_error("archived metadata exceeds the watch buffer");
                    *retained_rows += old_bytes + new_bytes;
                    entry->chunk_changes.push_back(std::move(change));
                    if (FeedEntryRetainedBytes(*entry) > entry_budget)
                        throw std::length_error("archived metadata exceeds the watch buffer");
                }
            }
            if (chunk_forms_budget) {
                const auto held = FeedEntryRetainedBytes(*entry);
                const bool populated = ChunkPopulated(frame_geometry, cursor.state.presence_bitmap, cursor.state.chunk_columns.get());
                const auto estimate = populated ? 256U + layout.fixed_columns().size() * 64U + cursor.state.presence_bitmap.size() +
                    cursor.state.payload.size() + cursor.state.vars.encoded_size() +
                    (cursor.state.chunk_columns ? cursor.state.chunk_columns->bytes.size() : 0U) : 128U;
                if (estimate > *chunk_forms_budget || held > *chunk_forms_budget - estimate)
                    throw std::length_error("archived chunks exceed the watch buffer");
                FeedChunkForm chunk{cursor.coord, info.revision, {}};
                if (populated) {
                    const auto form = EncodeSectionedChunkState(frame_geometry, cursor.state, kVarMaxChunkBytesLimit);
                    chunk.form = std::make_shared<const std::string>(form.begin(), form.end());
                }
                entry->chunk_forms.push_back(std::move(chunk));
                entry->chunk_forms_complete = true;
                if (FeedEntryRetainedBytes(*entry) > entry_budget)
                    throw std::length_error("archived chunks exceed the watch buffer");
            }
            const auto width = geometry.config().chunk_width_blocks;
            const auto height = geometry.config().chunk_height_blocks;
            const auto held = FeedEntryRetainedBytes(*entry);
            if (held > entry_budget || *retained_rows > held) throw std::length_error("archived change exceeds the watch buffer");
            std::size_t structural = held - *retained_rows;
            for (std::size_t b = 0U; !chunk_forms_budget && b < geometry.ChunkBlockCount(); ++b) {
                std::optional<std::vector<ColumnValue>> old_values, new_values;
                if (BlockPresent(before.presence_bitmap, b)) old_values = DecodeBlockColumns(layout, before.payload, before.vars, b);
                if (BlockPresent(cursor.state.presence_bitmap, b)) new_values = DecodeBlockColumns(layout, cursor.state.payload, cursor.state.vars, b);
                if (old_values.has_value() == new_values.has_value() &&
                    (!old_values || SameFeedValues(*old_values, *new_values))) continue;
                const auto old_bytes = FeedRowRetainedBytes(old_values), new_bytes = FeedRowRetainedBytes(new_values);
                const auto available = entry_budget - structural - *retained_rows;
                if (old_bytes > available || new_bytes > available - old_bytes)
                    throw std::length_error("archived rows exceed the watch buffer");
                *retained_rows += old_bytes + new_bytes;
                const auto local_y = (b / width) % height;
                const auto local_z = b / (static_cast<std::size_t>(width) * height);
                const auto capacity = entry->blocks.capacity();
                entry->blocks.push_back({cursor.coord,
                    Absolute(cursor.coord.x, width, b % width),
                    Absolute(cursor.coord.y, height, local_y),
                    std::move(old_values), std::move(new_values),
                    static_cast<std::uint32_t>(b % width), static_cast<std::uint32_t>(local_y),
                    geometry.dims() == 3U ? Absolute(cursor.coord.z, geometry.config().chunk_depth_blocks, local_z) : std::nullopt,
                    static_cast<std::uint32_t>(local_z)});
                const auto growth = entry->blocks.capacity() - capacity;
                if (growth > (entry_budget - structural) / sizeof(FeedBlockChange))
                    throw std::length_error("archived rows exceed the watch buffer");
                structural += growth * sizeof(FeedBlockChange);
                if (*retained_rows > entry_budget - structural) throw std::length_error("archived rows exceed the watch buffer");
            }
        }
        cursor.previous = info.revision;
        cursor.offset += cursor.frame_size;
        Peek(cursor, *file);
        if (cursor.done) cursor.state = {};
        return true;
    }

    std::shared_ptr<const FeedEntry> Next() {
        for (;;) {
            std::uint64_t revision = 0U;
            for (const auto& cursor : cursors) if (!cursor.done && (revision == 0U || cursor.next < revision)) revision = cursor.next;
            if (revision == 0U) return nullptr;
            auto entry = std::make_shared<FeedEntry>();
            entry->position = {epoch, revision};
            const bool deliver = revision > position.revision;
            bool consumed = false;
            std::size_t retained_rows = 0U;
            for (auto& cursor : cursors) if (!cursor.done && cursor.next == revision)
                consumed = Apply(cursor, deliver ? entry.get() : nullptr, &retained_rows) || consumed;
            if (!deliver) continue;
            if (entry->schema_version == 0U) {
                if (consumed) position.revision = revision;
                continue;  // Only the GC maintenance frame.
            }
            if (!chunk_forms_budget) entry->protocol_frame = std::make_shared<const std::string>(EncodeFeedEntry(*entry));
            if (FeedEntryRetainedBytes(*entry) > entry_budget) throw std::length_error("archived change exceeds the watch buffer");
            position.revision = revision;
            return entry;
        }
    }
};

FeedArchiveReader::FeedArchiveReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
FeedArchiveReader::~FeedArchiveReader() = default;
FeedArchiveReader::FeedArchiveReader(FeedArchiveReader&&) noexcept = default;
FeedArchiveReader& FeedArchiveReader::operator=(FeedArchiveReader&&) noexcept = default;
std::shared_ptr<const FeedEntry> FeedArchiveReader::Next() {
    if (!impl_) return nullptr;
    if (!impl_->usable) throw std::runtime_error("feed archive reader is unusable after a read failure");
    // Several file cursors can advance while merging one transaction. An
    // exception must not permit a later call to emit only the remaining part.
    impl_->started = true;
    impl_->usable = false;
    auto entry = impl_->Next();
    impl_->usable = true;
    return entry;
}
void FeedArchiveReader::EnableChunkForms(std::size_t budget) {
    if (!impl_ || impl_->started || budget == 0U) throw std::logic_error("chunk forms must be enabled before archive reading");
    impl_->chunk_forms_budget = budget;
    impl_->entry_budget = budget;
}
void FeedArchiveReader::SetEntryBudget(std::size_t budget) {
    if (!impl_ || impl_->started || budget == 0U) throw std::logic_error("entry budget must be set before archive reading");
    impl_->entry_budget = budget;
}
FeedPosition FeedArchiveReader::position() const noexcept { return impl_ ? impl_->position : FeedPosition{}; }
FeedPosition FeedArchiveReader::through() const noexcept { return impl_ ? FeedPosition{impl_->position.epoch, impl_->through} : FeedPosition{}; }

FeedArchiveReader FeedArchiveAccess::Create(const std::filesystem::path& root, const Geometry& geometry,
    const StoreId& epoch, FeedPosition from, std::uint64_t through_durable,
    std::shared_ptr<void> retention_pin, FeatureFlags features) {
    return FeedArchiveReader(std::make_unique<FeedArchiveReader::Impl>(root, geometry, epoch, from,
        through_durable, std::move(retention_pin), features));
}

FeedArchiveReader FeedArchiveAccess::CreateCompletedPrefix(const std::filesystem::path& root, const Geometry& geometry,
    const StoreId& epoch, FeedPosition from, std::uint64_t through_durable,
    std::shared_ptr<void> retention_pin, const std::vector<FeedWalPrefix>& prefixes, FeatureFlags features) {
    return FeedArchiveReader(std::make_unique<FeedArchiveReader::Impl>(root, geometry, epoch, from,
        through_durable, std::move(retention_pin), features, &prefixes));
}

}  // namespace chunkdb
