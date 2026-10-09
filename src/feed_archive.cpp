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
#define NOMINMAX
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

bool ChunkName(std::string_view name, ChunkCoord* coord) {
    if (!name.starts_with("C_")) return false;
    const auto separator = name.find('_', 2U);
    return separator != std::string_view::npos &&
        Number(name.substr(2U, separator - 2U), &coord->x) && Number(name.substr(separator + 1U), &coord->y);
}

struct ArchiveName {
    ChunkCoord coord;
    std::uint64_t first = 0;
    std::uint64_t last = 0;
};

std::optional<ArchiveName> ParseArchiveName(std::string_view name) {
    if (!name.ends_with(".wal")) return std::nullopt;
    name.remove_suffix(4U);
    const auto dot = name.find('.');
    if (dot == std::string_view::npos) return std::nullopt;
    const auto dash = name.find('-', dot + 1U);
    ArchiveName result;
    if (dash == std::string_view::npos || !ChunkName(name.substr(0U, dot), &result.coord) ||
        !Number(name.substr(dot + 1U, dash - dot - 1U), &result.first) ||
        !Number(name.substr(dash + 1U), &result.last) || result.first == 0U || result.last < result.first)
        throw std::runtime_error("invalid feed archive name " + std::string(name));
    return result;
}

std::string ChunkStem(ChunkCoord coord) {
    return "C_" + std::to_string(coord.x) + "_" + std::to_string(coord.y);
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
    std::uint64_t offset = kWalHeaderSize;
    std::uint64_t previous = 0;
    std::uint64_t next = 0;
    std::size_t frame_size = 0;
    bool done = false;
    bool initialized = false;
    bool had_current_base = false;
    bool movable = false;
    bool completed_prefix = false;
    bool loaded = false;
    std::uint64_t schema_version = 0;
    ChunkState state;
};

// Only classify whether a captured live image predates this WAL. The header
// and its directory protect the revision; body/state validation remains lazy
// in ParseChunkImage when the image is actually used as a base.
bool HasOriginalBase(const std::filesystem::path& path, ChunkCoord coord, const StoreId& epoch,
                     FeatureFlags features, std::uint64_t first) {
    if (!std::filesystem::exists(path)) return false;
    ReadFile file(path);
    const auto fixed = file.At(0U, kImageFixedHeaderSize);
    if (fixed.size() != kImageFixedHeaderSize || std::memcmp(fixed.data(), kImageMagic, kImageMagicSize) != 0 ||
        ReadLe16(fixed, 8U) != kImageFormatVersion || ReadLe16(fixed, 10U) > kImageMaxSections)
        throw std::runtime_error("feed archive current image header is invalid");
    const auto directory_end = kImageFixedHeaderSize + static_cast<std::size_t>(ReadLe16(fixed, 10U)) * kImageSectionEntrySize;
    const auto header = file.At(0U, directory_end + 4U);
    if (header.size() != directory_end + 4U || ReadLe32(header, directory_end) != Crc32(header.data(), directory_end))
        throw std::runtime_error("feed archive current image header checksum mismatch");
    const FeatureFlags image_features{ReadLe32(header, 12U), ReadLe32(header, 16U), ReadLe32(header, 20U)};
    if (!IsSubsetOf(image_features, features) || !std::equal(epoch.begin(), epoch.end(), header.begin() + 24U) ||
        std::bit_cast<std::int64_t>(ReadLe64(header, 40U)) != coord.x ||
        std::bit_cast<std::int64_t>(ReadLe64(header, 48U)) != coord.y || ReadLe64(header, 56U) == 0U)
        throw std::runtime_error("feed archive current image metadata is invalid");
    return ReadLe64(header, 56U) < first;
}

}  // namespace

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
        if ((generation & 1U) != 0U) throw std::runtime_error("feed archive snapshot is being modified");

        std::vector<std::vector<std::uint8_t>> transactions;
        const auto intent_dir = ConditionalIntentDirectory(root);
        if (std::filesystem::exists(intent_dir)) for (const auto& item : std::filesystem::directory_iterator(intent_dir)) {
            const auto name = item.path().filename().string();
            if (IsTxnIntentFileName(name)) {
                auto bytes = LoadFile(item.path());
                TxnIntent intent;
                if (!TryParseTxnIntent(bytes, &intent)) throw std::runtime_error("malformed feed transaction intent");
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
        (void)TxnRollbackBoundaryForChunk(transactions, {});

        const auto archives = root / kFeedArchiveDirName;
        if (std::filesystem::exists(archives)) for (const auto& item : std::filesystem::directory_iterator(archives)) {
            if (!item.is_regular_file()) continue;
            const auto name = ParseArchiveName(item.path().filename().string());
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
            if (a.coord.x != b.coord.x) return a.coord.x < b.coord.x;
            if (a.coord.y != b.coord.y) return a.coord.y < b.coord.y;
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
                if (!ChunkName(std::string_view(filename).substr(0U, filename.size() - 4U), &cursor.coord)) continue;
                cursor.path = item.path();
                cursor.limit = std::filesystem::file_size(cursor.path);
                auto boundary = TxnRollbackBoundaryForChunk(transactions, cursor.coord);
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
                    epoch, features, cursor.first);
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
            if (!TryParseTxnIntent(bytes, &intent)) throw std::runtime_error("malformed feed transaction intent");
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
            const auto name = ParseArchiveName(item.path().filename().string());
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
            if (prefix.first == 0U || prefix.last < prefix.first || prefix.last > through || prefix.limit <= kWalHeaderSize)
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
            cursors.push_back(std::move(cursor));
        }
        std::sort(cursors.begin(), cursors.end(), [](const auto& a, const auto& b) {
            if (a.coord.x != b.coord.x) return a.coord.x < b.coord.x;
            if (a.coord.y != b.coord.y) return a.coord.y < b.coord.y;
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
            const auto name = ParseArchiveName(item.path().filename().string());
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
        const auto header = file->At(0U, static_cast<std::size_t>(std::min<std::uint64_t>(cursor.limit, kWalHeaderSize)));
        if (header.size() < kWalHeaderSize) {
            if (cursor.completed_prefix || cursor.last != 0U) throw std::runtime_error("completed feed WAL prefix has a partial header");
            const auto expected = BuildWalHeader(cursor.coord, epoch, features);
            const bool zero = std::all_of(header.begin(), header.end(), [](auto byte) { return byte == 0U; });
            if (!zero && !std::equal(header.begin(), header.end(), expected.begin()))
                throw std::runtime_error("feed archive has a damaged partial WAL header");
            cursor.done = true;
            return;
        }
        ValidateWalHeader(header, cursor.coord, epoch, features);
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
        cursor.loaded = true;
        const auto linked = root / kFeedArchiveDirName / (ChunkStem(cursor.coord) + "." + std::to_string(cursor.first) + ".chk");
        const auto current = ChunkDataPath(root, geometry, cursor.coord);
        const auto read_image = [&](const std::filesystem::path& path, bool check_link = false) -> std::optional<ChunkStateImage> {
            try {
                ReadFile file(path);
                // A linked base is published before replacing the live image.
                // Recheck after opening: a handle to a newer schema's image
                // must not be parsed with this reader's captured geometry.
                if (check_link && std::filesystem::exists(linked)) return std::nullopt;
                const auto size = file.Size();
                return ParseChunkImage(file.At(0U, static_cast<std::size_t>(size)), geometry, cursor.coord, epoch, features);
            } catch (const std::system_error& error) {
                if (error.code() != std::errc::no_such_file_or_directory) throw;
                return std::nullopt;
            }
        };
        auto image = read_image(linked);
        bool has_link = image.has_value();
        if (!image && still_live && cursor.completed_prefix) {
            try { cursor.had_current_base = HasOriginalBase(current, cursor.coord, epoch, features, cursor.first); }
            catch (const std::system_error& error) {
                if (error.code() != std::errc::no_such_file_or_directory) throw;
            }
            image = read_image(linked);
            has_link = image.has_value();
        }
        if (!image && still_live && cursor.had_current_base) {
            image = read_image(current, true);
            if (!image || image->revision >= cursor.first) {
                // Checkpoint may have linked the base after our first lookup,
                // then published/replaced or collected the current image.
                image = read_image(linked);
                has_link = image.has_value();
            }
        }
        if (image) {
            if (has_link && image->revision >= cursor.first)
                throw std::runtime_error("feed archive base overlaps its WAL");
            cursor.state = {image->revision, std::move(image->payload), std::move(image->presence_bitmap), std::move(image->vars)};
            cursor.schema_version = image->schema_version;
            return;
        }
        // No original image existed: the WAL began with an empty chunk,
        // even if a crash left its newly published image at the live name.
        cursor.schema_version = first_schema;
        cursor.state.payload.assign(geometry.LayoutAt(first_schema).payload_bytes(), 0U);
        cursor.state.presence_bitmap.assign(ChunkPresenceBitmapBytes(geometry), 0U);
    }

    bool Apply(Cursor& cursor, FeedEntry* entry) {
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
            const auto& layout = geometry.LayoutAt(info.schema_version);
            const auto width = geometry.config().chunk_width_blocks;
            for (std::size_t b = 0U; b < geometry.ChunkBlockCount(); ++b) {
                std::optional<std::vector<ColumnValue>> old_values, new_values;
                if (BlockPresent(before.presence_bitmap, b)) old_values = DecodeBlockColumns(layout, before.payload, before.vars, b);
                if (BlockPresent(cursor.state.presence_bitmap, b)) new_values = DecodeBlockColumns(layout, cursor.state.payload, cursor.state.vars, b);
                if (old_values.has_value() == new_values.has_value() &&
                    (!old_values || SameFeedValues(*old_values, *new_values))) continue;
                entry->blocks.push_back({cursor.coord,
                    Absolute(cursor.coord.x, width, b % width),
                    Absolute(cursor.coord.y, geometry.config().chunk_height_blocks, b / width),
                    std::move(old_values), std::move(new_values),
                    static_cast<std::uint32_t>(b % width), static_cast<std::uint32_t>(b / width)});
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
            for (auto& cursor : cursors) if (!cursor.done && cursor.next == revision)
                consumed = Apply(cursor, deliver ? entry.get() : nullptr) || consumed;
            if (!deliver) continue;
            if (entry->schema_version == 0U) {
                if (consumed) position.revision = revision;
                continue;  // Only the GC maintenance frame.
            }
            entry->protocol_frame = std::make_shared<const std::string>(EncodeFeedEntry(*entry));
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
    impl_->usable = false;
    auto entry = impl_->Next();
    impl_->usable = true;
    return entry;
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
