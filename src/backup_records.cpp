#include "backup.hpp"
#include "migrations_records.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <cerrno>
#include <cstdio>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
#include <limits>
#include <map>
#include <set>

#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "feature_flags.hpp"
#include "feed_slot_records.hpp"
#include "store_manifest.hpp"
#include "wal_replay.hpp"

namespace chunkdb {
namespace {
constexpr std::array<std::uint8_t, 4> kMagic{'C', 'K', 'B', 'P'};
void Cancelled(const BackupCancel& cancelled) {
    if (cancelled.stop_requested()) throw std::runtime_error("backup cancelled");
}
void Crash(const char* point) noexcept { if (ConsumeFailpointEnv(point)) std::_Exit(86); }
bool Present(const std::filesystem::path& path) {
    const auto status = std::filesystem::symlink_status(path);
    return status.type() != std::filesystem::file_type::not_found;
}
void RequirePath(const std::filesystem::path& path) {
    if (path.native().find(typename std::filesystem::path::value_type{}) != std::filesystem::path::string_type::npos)
        throw std::invalid_argument("backup path contains a NUL byte");
}
void SafeDescendants(const std::filesystem::path& root, const std::filesystem::path& relative) {
    auto at = std::filesystem::weakly_canonical(root);
    for (const auto& part : relative) {
        at /= part;
        std::error_code error;
        const auto status = std::filesystem::symlink_status(at, error);
        if (error && error != std::errc::no_such_file_or_directory)
            throw std::filesystem::filesystem_error("cannot inspect backup path", at, error);
        if (status.type() == std::filesystem::file_type::symlink)
            throw std::invalid_argument("backup target must not contain symlinks below --backup-dir: " + at.string());
    }
}
bool Within(const std::filesystem::path& path, const std::filesystem::path& root) {
    auto p = path.begin();
    for (auto r = root.begin(); r != root.end(); ++r, ++p)
        if (p == path.end() || *p != *r) return false;
    return true;
}
void RequireRelative(const std::filesystem::path& path) {
    if (path.empty() || path.is_absolute() || path.has_root_name() || path != path.lexically_normal())
        throw std::runtime_error("invalid backup inventory path");
    for (const auto& part : path)
        if (part == "." || part == ".." || part.empty()) throw std::runtime_error("invalid backup inventory path");
    if (path.generic_string().find('\\') != std::string::npos || path.generic_string().find('\0') != std::string::npos)
        throw std::runtime_error("invalid backup inventory path");
}
void ValidateRecord(const BackupRecord& record) {
    std::set<std::string> names;
    for (const auto& table : record.tables) {
        if (!IsValidTableName(table.name) || !names.insert(table.name).second ||
            std::all_of(table.epoch.begin(), table.epoch.end(), [](auto b) { return b == 0U; }))
            throw std::runtime_error("invalid or repeated backup table cut");
    }
    std::set<std::string> paths;
    for (const auto& file : record.files) {
        RequireRelative(file.relative_path);
        if (!paths.insert(file.relative_path.generic_string()).second || file.relative_path == kBackupMarkerName ||
            file.relative_path == kBackupIncompleteName || file.relative_path == kRestoreIncompleteName)
            throw std::runtime_error("invalid or repeated backup inventory file");
    }
}
void PutString(std::vector<std::uint8_t>& out, const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) throw std::length_error("backup record string too long");
    WriteLe32(out, static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}
class Reader {
  public:
    Reader(const std::vector<std::uint8_t>& bytes, std::size_t end) : bytes_(bytes), end_(end) {}
    std::uint16_t U16() { Need(2U); auto v = ReadLe16(bytes_, at_); at_ += 2U; return v; }
    std::uint32_t U32() { Need(4U); auto v = ReadLe32(bytes_, at_); at_ += 4U; return v; }
    std::uint64_t U64() { Need(8U); auto v = ReadLe64(bytes_, at_); at_ += 8U; return v; }
    StoreId Id() { Need(16U); StoreId id{}; std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), id.size(), id.begin()); at_ += id.size(); return id; }
    std::string String() { const auto n = U32(); Need(n); std::string s(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), bytes_.begin() + static_cast<std::ptrdiff_t>(at_ + n)); at_ += n; return s; }
    std::size_t left() const { return end_ - at_; }
  private:
    void Need(std::size_t n) { if (n > left()) throw std::runtime_error("truncated backup record"); }
    const std::vector<std::uint8_t>& bytes_; std::size_t end_, at_ = 4U;
};
void RequireRegular(const std::filesystem::path& path) {
    if (std::filesystem::symlink_status(path).type() != std::filesystem::file_type::regular)
        throw std::runtime_error("backup inventory requires a regular file: " + path.string());
}
class ExclusiveOutput {
  public:
    explicit ExclusiveOutput(const std::filesystem::path& path) {
#ifdef _WIN32
        handle_ = CreateFileW(path.wstring().c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) throw std::runtime_error("cannot exclusively create backup file");
#else
        const auto absolute = std::filesystem::absolute(path).lexically_normal();
        int directory = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (directory < 0) throw std::runtime_error("cannot open backup path root");
        const auto relative = absolute.relative_path();
        for (auto it = relative.begin(); it != relative.end(); ++it) {
            const bool last = std::next(it) == relative.end();
            const auto part = it->string();
            const int next = ::openat(directory, part.c_str(), last ?
                O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC : O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC, 0600);
            ::close(directory);
            if (next < 0) throw std::runtime_error("cannot exclusively create safe backup file");
            if (last) { fd_ = next; break; }
            directory = next;
        }
#endif
    }
    ~ExclusiveOutput() {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
#else
        if (fd_ >= 0) ::close(fd_);
#endif
    }
    void Write(const std::uint8_t* bytes, std::size_t count) {
        while (count != 0U) {
#ifdef _WIN32
            DWORD written = 0U;
            if (!WriteFile(handle_, bytes, static_cast<DWORD>(count), &written, nullptr) || written == 0U)
                throw std::runtime_error("backup copy write failed");
#else
            const auto written = ::write(fd_, bytes, count);
            if (written < 0 && errno == EINTR) continue;
            if (written <= 0) throw std::runtime_error("backup copy write failed");
#endif
            bytes += written; count -= static_cast<std::size_t>(written);
        }
    }
    void Finish() {
#ifdef _WIN32
        if (!FlushFileBuffers(handle_)) throw std::runtime_error("backup copy sync failed");
        const auto handle = handle_; handle_ = INVALID_HANDLE_VALUE;
        if (!CloseHandle(handle)) throw std::runtime_error("backup copy close failed");
#else
        if (::fsync(fd_) != 0) throw std::runtime_error("backup copy sync failed");
#ifdef __APPLE__
        if (::fcntl(fd_, F_FULLFSYNC) != 0) throw std::runtime_error("backup copy full sync failed");
#endif
        const auto fd = fd_; fd_ = -1;
        if (::close(fd) != 0) throw std::runtime_error("backup copy close failed");
#endif
    }
  private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
};
// Staging links can outlive their live name. Windows must let checkpoints
// replace or remove that name while a backup is copying the linked inode.
class SharedInput {
  public:
    explicit SharedInput(const std::filesystem::path& path) {
#ifdef _WIN32
        handle_ = CreateFileW(path.wstring().c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
        if (handle_ == INVALID_HANDLE_VALUE) throw std::runtime_error("cannot open backup source");
#else
        input_.open(path, std::ios::binary);
        if (!input_) throw std::runtime_error("cannot open backup source");
#endif
    }
    ~SharedInput() {
#ifdef _WIN32
        if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
#endif
    }
    std::size_t Read(std::uint8_t* bytes, std::size_t count) {
#ifdef _WIN32
        DWORD read = 0U;
        if (!ReadFile(handle_, bytes, static_cast<DWORD>(count), &read, nullptr))
            throw std::runtime_error("backup file read failed");
        return read;
#else
        input_.read(reinterpret_cast<char*>(bytes), static_cast<std::streamsize>(count));
        if (!input_ && !input_.eof()) throw std::runtime_error("backup file read failed");
        return static_cast<std::size_t>(input_.gcount());
#endif
    }
  private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    std::ifstream input_;
#endif
};
void ValidateInventory(const std::filesystem::path& root, const BackupRecord& record, bool completing, const BackupCancel& cancelled = {}) {
    ValidateRecord(record);
    RequirePath(root);
    if (!std::filesystem::is_directory(root)) throw std::runtime_error("backup directory is missing");
    if (!completing && (Present(root / kRestoreIncompleteName) || Present(root / kBackupIncompleteName)))
        throw std::runtime_error("backup publication is incomplete");
    std::set<std::string> expected_files, expected_dirs{"tables"};
    for (const auto& table : record.tables) expected_dirs.insert("tables/" + table.name);
    for (const auto& file : record.files) {
        const auto relative = file.relative_path.generic_string();
        expected_files.insert(relative);
        auto parent = file.relative_path.parent_path();
        while (!parent.empty()) { expected_dirs.insert(parent.generic_string()); parent = parent.parent_path(); }
        const auto actual = InspectBackupFile(root, file.relative_path, cancelled);
        if (actual.size != file.size || actual.crc32 != file.crc32)
            throw std::runtime_error("backup inventory checksum or length mismatch: " + relative);
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        const auto relative = entry.path().lexically_relative(root).generic_string();
        const auto type = entry.symlink_status().type();
        if (type == std::filesystem::file_type::directory) {
            if (!expected_dirs.erase(relative)) throw std::runtime_error("unexpected backup directory: " + relative);
        } else if (type == std::filesystem::file_type::regular) {
            if (relative == kBackupMarkerName || (completing && (relative == kBackupIncompleteName || relative == kRestoreIncompleteName))) continue;
            if (!expected_files.erase(relative)) throw std::runtime_error("unexpected backup file: " + relative);
        } else throw std::runtime_error("unsafe backup entry: " + relative);
    }
    if (!expected_files.empty() || !expected_dirs.empty()) throw std::runtime_error("backup inventory entry is missing");
    if (!ReadDataDirManifest(root)) throw std::runtime_error("backup data-directory manifest is missing");
    const auto directory_manifest = *ReadDataDirManifest(root);
    RequireOpenableFeatures(directory_manifest.features, AccessMode::kReadOnly);
    if (Present(root / kUsersFileName)) (void)DecodeUsers(LoadFile(root / kUsersFileName));
    (void)ReadMigrationRecords(root);
    std::set<std::string> allowed{"chunkdb.manifest", "chunkdb.users", std::string(kMigrationsFileName)};
    for (const auto& cut : record.tables) {
        Cancelled(cancelled);
        const auto dir = root / "tables" / cut.name;
        const auto manifest = ReadStoreManifest(dir);
        if (!manifest || manifest->store_id != cut.epoch) throw std::runtime_error("backup table epoch mismatch: " + cut.name);
        RequireOpenableFeatures(manifest->features, AccessMode::kReadOnly);
        Geometry geometry(manifest->geometry, manifest->schema);
        std::uint64_t ceiling = 0U, generation = 0U;
        if (!TryParseVersionClockRecord(LoadFile(dir / "chunkdb.version"), &ceiling) || ceiling <= cut.revision)
            throw std::runtime_error("backup clock is not above its cut");
        if (!TryParseSnapshotGenerationRecord(LoadFile(dir / "chunkdb.snapshot"), &generation) || (generation & 1U) != 0U)
            throw std::runtime_error("backup snapshot generation is not stable");
        if (!IsValidInitializedStoreMarker(LoadFile(dir / ".chunkdb.initialized"))) throw std::runtime_error("invalid backup initialized marker");
        if (auto slots = ReadFeedSlotRecords(dir, cut.epoch)) {
            if ((manifest->features.incompat & kFeatureFeedSlots) == 0U || slots->durable_watermark > cut.revision)
                throw std::runtime_error("backup slot frontier exceeds cut or has no feature");
            for (const auto& slot : slots->slots) if (slot.written > cut.revision) throw std::runtime_error("backup slot exceeds cut");
        }
        const auto prefix = "tables/" + cut.name + "/";
        for (const auto* name : {"table.manifest", "chunkdb.version", "chunkdb.snapshot", ".chunkdb.initialized", "chunkdb.slots"})
            allowed.insert(prefix + name);
        for (const auto& file : record.files) {
            Cancelled(cancelled);
            const auto relative = file.relative_path.generic_string();
            if (relative.rfind(prefix, 0U) != 0U || allowed.contains(relative)) continue;
            const auto local = file.relative_path.lexically_relative(std::filesystem::path("tables") / cut.name);
            if (std::distance(local.begin(), local.end()) != 2) throw std::runtime_error("unknown backup table artifact: " + relative);
            const auto filename = local.filename().string();
            const auto stem = local.stem().string();
            std::int64_t x = 0, y = 0;
            const auto separator = stem.find('_', 2U);
            if (stem.rfind("C_", 0U) != 0U || separator == std::string::npos ||
                !TryParseInt64(stem.substr(2U, separator - 2U), &x) || !TryParseInt64(stem.substr(separator + 1U), &y))
                throw std::runtime_error("invalid backup chunk name: " + filename);
            if (stem != "C_" + std::to_string(x) + "_" + std::to_string(y)) throw std::runtime_error("noncanonical backup chunk name");
            const ChunkCoord coord{x, y};
            const auto large = geometry.ChunkToLarge(coord);
            if (local.parent_path().string() != "L_" + std::to_string(large.x) + "_" + std::to_string(large.y))
                throw std::runtime_error("misplaced backup chunk");
            const auto full = root / file.relative_path;
            if (local.extension() == ".chk") {
                auto image = ParseChunkImage(LoadFile(full), geometry, coord, cut.epoch, manifest->features);
                if (image.revision > cut.revision) throw std::runtime_error("backup image exceeds cut");
            } else if (local.extension() == ".wal") {
                std::vector<std::uint8_t> payload(geometry.ChunkPayloadBytes(), 0U), presence(ChunkPresenceBitmapBytes(geometry), 0U);
                ChunkVars vars; std::uint64_t base = 0U, schema = 0U;
                const auto image_path = full.parent_path() / (stem + ".chk");
                if (Present(image_path)) {
                    auto image = ParseChunkImage(LoadFile(image_path), geometry, coord, cut.epoch, manifest->features);
                    base = image.revision; schema = image.schema_version;
                    payload = std::move(image.payload); presence = std::move(image.presence_bitmap); vars = std::move(image.vars);
                }
                std::vector<WalFrameBoundary> boundaries;
                auto replay = ReplayWal(LoadFile(full), geometry, coord, cut.epoch, manifest->features, base, schema, &payload, &presence, &vars, &boundaries);
                if ((!replay.replayable && !replay.torn_creation) ||
                    (replay.tail_truncated_or_corrupt && !replay.stopped_at_crash_tail) || !replay.vars_problem.empty() ||
                    (!boundaries.empty() && boundaries.back().revision > cut.revision)) throw std::runtime_error("backup WAL is damaged or exceeds cut");
            } else throw std::runtime_error("unknown backup chunk artifact: " + relative);
            allowed.insert(relative);
        }
    }
    for (const auto& file : record.files)
        if (!allowed.contains(file.relative_path.generic_string())) throw std::runtime_error("unknown backup inventory artifact");
}
void SyncTreeImpl(const std::filesystem::path& root, const BackupCancel& cancelled) {
    const auto absolute = std::filesystem::absolute(root).lexically_normal();
    std::vector<std::filesystem::path> dirs{absolute};
    for (const auto& entry : std::filesystem::recursive_directory_iterator(absolute)) {
        Cancelled(cancelled);
        if (entry.is_directory()) dirs.push_back(entry.path());
        else { RequireRegular(entry.path()); SyncFilePath(entry.path()); }
    }
    for (auto it = dirs.rbegin(); it != dirs.rend(); ++it) { Cancelled(cancelled); SyncDirectoryPath(*it); }
    SyncDirectoryPath(absolute.parent_path());
}
} // namespace

void WriteBackupStagingOwner(const std::filesystem::path& staging, const StoreId& data_dir_id) {
    const auto name = staging.filename().string();
    if (name.size() != 32U || !std::all_of(name.begin(), name.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }) || std::all_of(data_dir_id.begin(), data_dir_id.end(), [](auto b) { return b == 0U; }))
        throw std::invalid_argument("invalid backup staging identity");
    std::vector<std::uint8_t> bytes{'C', 'K', 'B', 'S'};
    bytes.insert(bytes.end(), data_dir_id.begin(), data_dir_id.end());
    bytes.insert(bytes.end(), name.begin(), name.end());
    WriteLe32(bytes, Crc32(bytes));
    if (!PublishNewFile(staging / kBackupStagingOwnerName, bytes))
        throw std::runtime_error("backup staging directory is already owned");
    SyncDirectoryPath(staging.parent_path());
}

bool IsOwnedBackupStaging(const std::filesystem::path& staging, const StoreId& data_dir_id) {
    const auto name = staging.filename().string();
    if (name.size() != 32U || !std::all_of(name.begin(), name.end(), [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        })) return false;
    std::error_code error;
    const auto directory = std::filesystem::symlink_status(staging, error);
    if (error && error != std::errc::no_such_file_or_directory)
        throw std::filesystem::filesystem_error("cannot inspect backup staging", staging, error);
    if (directory.type() != std::filesystem::file_type::directory) return false;
    const auto guard = staging / kBackupStagingOwnerName;
    const auto status = std::filesystem::symlink_status(guard, error);
    if (error && error != std::errc::no_such_file_or_directory)
        throw std::filesystem::filesystem_error("cannot inspect backup staging owner", guard, error);
    if (status.type() != std::filesystem::file_type::regular) return false;
    if (std::filesystem::file_size(guard) != 56U) return false;
    const auto bytes = ReadBackupFile(guard, 56U);
    return std::equal(bytes.begin(), bytes.begin() + 4U, "CKBS") &&
        std::equal(data_dir_id.begin(), data_dir_id.end(), bytes.begin() + 4U) &&
        std::equal(name.begin(), name.end(), bytes.begin() + 20U) &&
        ReadLe32(bytes, 52U) == Crc32(bytes.data(), 52U);
}

std::filesystem::path ResolveBackupTarget(const std::filesystem::path& directory, const std::filesystem::path& requested) {
    if (directory.empty()) throw std::invalid_argument("BACKUP requires --backup-dir; set --backup-dir to a backup directory");
    RequirePath(directory); RequirePath(requested);
    if (requested.empty() || requested.is_absolute() || requested.has_root_path() ||
        requested.generic_string().find('\\') != std::string::npos)
        throw std::invalid_argument("BACKUP TO requires a relative name under --backup-dir");
    for (const auto& part : requested)
        if (part == "..") throw std::invalid_argument("BACKUP TO must not contain '..' components");
    const auto relative = requested.lexically_normal();
    if (relative == ".") throw std::invalid_argument("BACKUP TO requires a backup name");
    const auto root = std::filesystem::weakly_canonical(directory);
    SafeDescendants(root, relative);
    const auto target = std::filesystem::weakly_canonical(root / relative);
    if (!Within(target, root) || target == root) throw std::invalid_argument("backup destination escapes --backup-dir");
    return target;
}
void RequireBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target) {
    if (target.empty()) throw std::invalid_argument("backup target is empty");
    RequirePath(source); RequirePath(target);
    const auto source_path = std::filesystem::weakly_canonical(source), target_path = std::filesystem::weakly_canonical(target);
    if (Within(source_path, target_path) || Within(target_path, source_path)) throw std::invalid_argument("backup and source directories overlap");
    if (Present(target) && (!std::filesystem::is_directory(target) || !std::filesystem::is_empty(target)))
        throw std::invalid_argument("backup destination must be absent or empty");
}
void PrepareBackupTarget(const std::filesystem::path& source, const std::filesystem::path& target) {
    RequireBackupTarget(source, target);
    EnsureDirectoryPathExists(std::filesystem::absolute(target).parent_path(), true);
    std::filesystem::create_directory(target);
    if (!std::filesystem::is_empty(target)) throw std::invalid_argument("backup destination must be absent or empty");
    if (!PublishNewFile(target / kBackupIncompleteName, std::vector<std::uint8_t>{'C', 'K', 'B', 'I'}))
        throw std::invalid_argument("backup destination is already owned");
    SyncDirectoryPath(std::filesystem::absolute(target).parent_path());
}
void RequireNotBackupDirectory(const std::filesystem::path& path) {
    auto check = [](const auto& dir) {
        if (Present(dir / kBackupMarkerName) || Present(dir / kBackupIncompleteName) || Present(dir / kRestoreIncompleteName))
            throw std::runtime_error("backup or incomplete restore directory cannot be opened; use chunkdb_restore");
    };
    const auto absolute = std::filesystem::absolute(path).lexically_normal();
    check(absolute);
    if (absolute.parent_path().filename() == "tables") check(absolute.parent_path().parent_path());
}
BackupFileRecord InspectBackupFile(const std::filesystem::path& root, const std::filesystem::path& relative, const BackupCancel& cancelled) {
    RequirePath(root); RequireRelative(relative); SafeDescendants(root, relative); RequireRegular(root / relative);
    SharedInput input(root / relative);
    std::array<std::uint8_t, 65536U> buffer{}; std::uint64_t size = 0; std::uint32_t crc = 0;
    for (;;) {
        Cancelled(cancelled);
        const auto count = input.Read(buffer.data(), buffer.size());
        if (count == 0U) break;
        if (count > std::numeric_limits<std::uint64_t>::max() - size) throw std::length_error("backup file too large");
        size += count; crc = Crc32Extend(crc, buffer.data(), count);
    }
    return {relative, size, crc};
}
BackupFileRecord CopyBackupFile(const std::filesystem::path& source, const std::filesystem::path& root,
    const std::filesystem::path& relative, std::uint64_t size, const BackupCancel& cancelled) {
    RequirePath(source); RequirePath(root); RequireRelative(relative); RequireRegular(source); SafeDescendants(root, relative);
    const auto target = std::filesystem::weakly_canonical(root) / relative;
    EnsureDirectoryPathExists(target.parent_path(), true);
    if (Present(target)) throw std::runtime_error("backup copy would replace an entry");
    SharedInput input(source);
    ExclusiveOutput output(target);
    std::array<std::uint8_t, 65536U> buffer{}; auto remaining = size; std::uint32_t crc = 0;
    while (remaining != 0U) {
        Cancelled(cancelled); const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, buffer.size()));
        std::size_t read = 0U;
        while (read < count) {
            const auto n = input.Read(buffer.data() + read, count - read);
            if (n == 0U) throw std::runtime_error("backup required prefix was shortened");
            read += n;
        }
        output.Write(buffer.data(), count);
        remaining -= count; crc = Crc32Extend(crc, buffer.data(), count);
    }
    output.Finish();
    return {relative, size, crc};
}
std::vector<std::uint8_t> ReadBackupFile(const std::filesystem::path& source, std::uint64_t size, const BackupCancel& cancelled) {
    RequirePath(source); RequireRegular(source);
    if (size > std::numeric_limits<std::size_t>::max()) throw std::length_error("backup file too large");
    SharedInput input(source);
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(size));
    std::size_t at = 0U;
    while (at != bytes.size()) {
        Cancelled(cancelled);
        const auto count = std::min<std::size_t>(bytes.size() - at, 65536U);
        const auto read = input.Read(bytes.data() + at, count);
        if (read == 0U) throw std::runtime_error("backup required prefix was shortened");
        at += read;
    }
    return bytes;
}
std::vector<std::uint8_t> SerializeBackupRecord(const BackupRecord& record) {
    ValidateRecord(record);
    if (record.tables.size() > std::numeric_limits<std::uint32_t>::max() || record.files.size() > std::numeric_limits<std::uint32_t>::max())
        throw std::length_error("too many backup inventory records");
    std::vector<std::uint8_t> bytes(kMagic.begin(), kMagic.end()); WriteLe16(bytes, 1U); WriteLe16(bytes, 0U);
    WriteLe64(bytes, record.created_at_ms); WriteLe32(bytes, static_cast<std::uint32_t>(record.tables.size()));
    for (const auto& table : record.tables) { PutString(bytes, table.name); bytes.insert(bytes.end(), table.epoch.begin(), table.epoch.end()); WriteLe64(bytes, table.revision); }
    WriteLe32(bytes, static_cast<std::uint32_t>(record.files.size()));
    for (const auto& file : record.files) { PutString(bytes, file.relative_path.generic_string()); WriteLe64(bytes, file.size); WriteLe32(bytes, file.crc32); }
    WriteLe32(bytes, Crc32(bytes)); return bytes;
}
BackupRecord ParseBackupRecord(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 28U || !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) ||
        ReadLe32(bytes, bytes.size() - 4U) != Crc32(bytes.data(), bytes.size() - 4U)) throw std::runtime_error("invalid backup marker or checksum");
    Reader reader(bytes, bytes.size() - 4U); if (reader.U16() != 1U || reader.U16() != 0U) throw std::runtime_error("unsupported backup marker version");
    BackupRecord record; record.created_at_ms = reader.U64(); auto tables = reader.U32();
    if (tables > reader.left() / 28U) throw std::runtime_error("truncated backup tables");
    for (std::uint32_t i = 0; i < tables; ++i) { BackupTableCut cut; cut.name = reader.String(); cut.epoch = reader.Id(); cut.revision = reader.U64(); record.tables.push_back(std::move(cut)); }
    auto files = reader.U32(); if (files > reader.left() / 16U) throw std::runtime_error("truncated backup inventory");
    for (std::uint32_t i = 0; i < files; ++i) { BackupFileRecord file; file.relative_path = reader.String(); file.size = reader.U64(); file.crc32 = reader.U32(); record.files.push_back(std::move(file)); }
    if (reader.left() != 0U) throw std::runtime_error("backup marker has trailing bytes");
    ValidateRecord(record);
    return record;
}
BackupRecord ReadBackupRecord(const std::filesystem::path& root) {
    RequireRegular(root / kBackupMarkerName); return ParseBackupRecord(LoadFile(root / kBackupMarkerName));
}
void ValidateBackupContents(const std::filesystem::path& root, const BackupRecord& record) { ValidateInventory(root, record, true); }
void ValidateBackupInventory(const std::filesystem::path& root, const BackupRecord& record) { ValidateInventory(root, record, false); }
void SyncBackupTree(const std::filesystem::path& root, const BackupCancel& cancelled) { SyncTreeImpl(root, cancelled); }
void CompleteBackupGuard(const std::filesystem::path& root, std::string_view guard, std::string_view phase) {
    const auto path = root / guard;
    const auto failpoint = [&](std::string_view suffix) {
        return "CHUNKDB_FAILPOINT_" + std::string(phase) + "_" + std::string(suffix) + "_ONCE";
    };
    const auto remove_fail = failpoint("GUARD_REMOVE_FAIL");
    const auto sync_fail = failpoint("COMPLETE_SYNC_FAIL");
    const auto reinstate_fail = failpoint("GUARD_REINSTATE_FAIL");
    const auto crash_remove = "CHUNKDB_FAILPOINT_CRASH_" + std::string(phase) + "_AFTER_GUARD_REMOVE_ONCE";
    const auto crash_complete = "CHUNKDB_FAILPOINT_CRASH_" + std::string(phase) + "_AFTER_COMPLETE_ONCE";
    if (ConsumeFailpointEnv(remove_fail.c_str())) throw std::runtime_error("injected publication guard removal failure");
    if (!std::filesystem::remove(path)) throw std::runtime_error("publication guard disappeared");
    Crash(crash_remove.c_str());
    try {
        if (ConsumeFailpointEnv(sync_fail.c_str())) throw std::runtime_error("injected publication completion sync failure");
        SyncDirectoryPath(root);
    } catch (const std::exception& completion) {
        try {
            if (ConsumeFailpointEnv(reinstate_fail.c_str())) throw std::runtime_error("injected guard reinstatement failure");
            AtomicWrite(path, std::vector<std::uint8_t>{'C', 'K', 'I', 'N'}, true, true);
        } catch (const std::exception& reinstatement) {
            throw BackupPublicationUnknownError(std::string("publication outcome is unknown: ") + completion.what() + "; guard reinstatement failed: " + reinstatement.what());
        }
        throw;
    }
    Crash(crash_complete.c_str());
}
void CompleteBackup(const std::filesystem::path& root, const BackupRecord& record, const BackupCancel& cancelled) {
    if (!Present(root / kBackupIncompleteName)) throw std::runtime_error("backup incomplete guard is missing");
    ValidateInventory(root, record, true, cancelled); Cancelled(cancelled); SyncBackupTree(root, cancelled);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_BEFORE_MARKER_ONCE");
    AtomicWrite(root / kBackupMarkerName, SerializeBackupRecord(record), true, true);
    Crash("CHUNKDB_FAILPOINT_CRASH_BACKUP_AFTER_MARKER_ONCE");
    Cancelled(cancelled);
    CompleteBackupGuard(root, kBackupIncompleteName, "BACKUP");
}
} // namespace chunkdb
