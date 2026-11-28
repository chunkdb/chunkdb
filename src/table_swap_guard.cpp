#include "table_swap_guard.hpp"

#include <algorithm>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <vector>

#include "checkpoint.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/table_catalog.hpp"
#include "durability_io.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {

using Bytes = std::vector<std::uint8_t>;
constexpr std::array<std::uint8_t, 4U> kMagic{'C', 'K', 'S', 'W'};
constexpr std::size_t kNoNamesSize = 8U + 16U + 2U * (1U + 16U) + 8U + 4U;
constexpr std::size_t kMaxSize = kNoNamesSize + 2U * kMaxTableNameLength;

struct SwapGuard {
    StoreId directory{};
    std::array<std::string, 2U> names{};
    std::array<StoreId, 2U> ids{};
    std::array<std::uint8_t, 8U> suffix{};
};

std::filesystem::file_type EntryType(const std::filesystem::path& path) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(path, error);
    if (error == std::errc::no_such_file_or_directory) return std::filesystem::file_type::not_found;
    if (error) throw std::filesystem::filesystem_error("cannot inspect swap entry", path, error);
    return status.type();
}

[[noreturn]] void Bad(const std::filesystem::path& path, const std::string& why) {
    throw std::runtime_error("invalid swap guard " + path.string() + ": " + why);
}

void Validate(const std::filesystem::path& path, const SwapGuard& guard) {
    if (!IsValidTableName(guard.names[0]) || !IsValidTableName(guard.names[1]) || guard.names[0] >= guard.names[1])
        Bad(path, "table names or order");
    if (guard.directory == StoreId{} || guard.ids[0] == StoreId{} || guard.ids[1] == StoreId{} ||
        guard.ids[0] == guard.ids[1])
        Bad(path, "zero or repeated identity");
}

std::optional<SwapGuard> ReadGuard(const std::filesystem::path& path) {
    const auto type = EntryType(path);
    if (type == std::filesystem::file_type::not_found) return std::nullopt;
    if (type != std::filesystem::file_type::regular) Bad(path, "record is not a regular file");
    const auto size = std::filesystem::file_size(path);
    if (size < kNoNamesSize + 2U || size > kMaxSize) Bad(path, "record length");
    std::ifstream input(path, std::ios::binary);
    if (!input) Bad(path, "cannot read record");
    Bytes bytes(static_cast<std::size_t>(size));
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (!input || input.peek() != std::char_traits<char>::eof() || input.bad())
        Bad(path, "record changed while reading");
    if (!std::equal(kMagic.begin(), kMagic.end(), bytes.begin()) || ReadLe16(bytes, 4U) != 1U || ReadLe16(bytes, 6U) != 0U)
        Bad(path, "magic, version or reserved field");
    const auto end = bytes.size() - 4U;
    if (ReadLe32(bytes, end) != Crc32(bytes.data(), end)) Bad(path, "checksum");

    SwapGuard guard;
    std::size_t at = 8U;
    const auto read_bytes = [&](auto& field) {
        if (field.size() > end - at) Bad(path, "truncated field");
        std::copy_n(bytes.data() + at, field.size(), field.begin());
        at += field.size();
    };
    read_bytes(guard.directory);
    for (std::size_t i = 0U; i < guard.names.size(); ++i) {
        if (at == end) Bad(path, "missing table name");
        const auto length = bytes[at++];
        if (length > kMaxTableNameLength || length > end - at) Bad(path, "table name length");
        guard.names[i].assign(reinterpret_cast<const char*>(bytes.data() + at), length);
        at += length;
        read_bytes(guard.ids[i]);
    }
    read_bytes(guard.suffix);
    if (at != end) Bad(path, "trailing bytes");
    Validate(path, guard);
    return guard;
}

std::string ParkedName(const SwapGuard& guard) {
    constexpr std::string_view hex = "0123456789abcdef";
    std::string name = ".chunkdb.swap.";
    for (const auto byte : guard.suffix) {
        name += hex[byte >> 4U];
        name += hex[byte & 0x0fU];
    }
    return name;
}

struct SwapPaths {
    std::filesystem::path pending{}, tables{}, first{}, second{}, parked{};
};

SwapPaths Paths(const std::filesystem::path& root, const SwapGuard& guard) {
    const auto tables = root / "tables";
    return {root / kTableSwapPendingFileName, tables, tables / guard.names[0],
        tables / guard.names[1], tables / ParkedName(guard)};
}

std::optional<StoreId> TableIdentity(const std::filesystem::path& path) {
    const auto type = EntryType(path);
    if (type == std::filesystem::file_type::not_found) return std::nullopt;
    if (type != std::filesystem::file_type::directory)
        throw std::runtime_error("not a table directory: " + path.string());
    const auto manifest = ReadStoreManifest(path);
    if (!manifest) throw std::runtime_error("missing table manifest: " + StoreManifestPath(path).string());
    return manifest->store_id;
}

enum class SwapStep { kParkFirst, kMoveSecond, kMoveParked, kSync };

SwapStep NextStep(const std::filesystem::path& root, const SwapPaths& paths, const SwapGuard& guard) {
    try {
        const auto directory = ReadDataDirManifest(root);
        if (!directory || directory->data_dir_id != guard.directory)
            throw std::runtime_error("data directory identity changed");
        if (EntryType(paths.tables) != std::filesystem::file_type::directory)
            throw std::runtime_error("not a tables directory: " + paths.tables.string());
        const auto first = TableIdentity(paths.first);
        const auto second = TableIdentity(paths.second);
        const auto parked = TableIdentity(paths.parked);
        const auto& a = guard.ids[0];
        const auto& b = guard.ids[1];
        if (first == a && second == b && !parked) return SwapStep::kParkFirst;
        if (!first && second == b && parked == a) return SwapStep::kMoveSecond;
        if (first == b && !second && parked == a) return SwapStep::kMoveParked;
        if (first == b && second == a && !parked) return SwapStep::kSync;
        throw std::runtime_error("table identities do not match any swap step");
    } catch (const std::exception& error) {
        Bad(paths.pending, error.what());
    }
}

void Crash(const char* key) {
    if (ConsumeFailpointEnv(key)) ExitAbruptly(86);
}

void InjectFailure(const char* key, const char* action, const std::filesystem::path& path) {
    if (ConsumeFailpointEnv(key))
        throw std::runtime_error(std::string("injected failure ") + action + ": " + path.string());
}

void RemoveGuard(const SwapPaths& paths, const std::filesystem::path& root) {
    InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_REMOVE_GUARD_FAIL_ONCE", "removing swap guard", paths.pending);
    if (!std::filesystem::remove(paths.pending))
        throw std::runtime_error("swap guard disappeared: " + paths.pending.string());
    Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_UNLINK_ONCE");
    InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_REMOVE_SYNC_FAIL_ONCE", "syncing swap guard removal", paths.pending);
    SyncDirectoryPath(root);
    Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_REMOVE_ONCE");
}

std::error_code ParkFirst(const SwapPaths& paths) {
    if (ConsumeFailpointEnv("CHUNKDB_FAILPOINT_SWAP_TABLES_PARK_FIRST_FAIL_ONCE"))
        return std::make_error_code(std::errc::io_error);
    const auto moved = MoveDirectoryNoReplace(paths.first, paths.parked);
    if (!moved) Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_PARK_FIRST_ONCE");
    return moved;
}

void Complete(const std::filesystem::path& root, const SwapPaths& paths, SwapStep step) {
    switch (step) {
        case SwapStep::kParkFirst:
            if (const auto moved = ParkFirst(paths))
                throw std::filesystem::filesystem_error("cannot park first swap table", paths.first, paths.parked, moved);
            [[fallthrough]];
        case SwapStep::kMoveSecond:
            InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_MOVE_SECOND_FAIL_ONCE", "moving second swap table", paths.second);
            if (const auto moved = MoveDirectoryNoReplace(paths.second, paths.first))
                throw std::filesystem::filesystem_error("cannot move second swap table", paths.second, paths.first, moved);
            Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_MOVE_SECOND_ONCE");
            [[fallthrough]];
        case SwapStep::kMoveParked:
            InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_MOVE_PARKED_FAIL_ONCE", "moving parked swap table", paths.parked);
            if (const auto moved = MoveDirectoryNoReplace(paths.parked, paths.second))
                throw std::filesystem::filesystem_error("cannot move parked swap table", paths.parked, paths.second, moved);
            Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_MOVE_PARKED_ONCE");
            [[fallthrough]];
        case SwapStep::kSync:
            InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_SYNC_FAIL_ONCE", "syncing swapped tables", paths.tables);
            SyncDirectoryPath(paths.tables);
            Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_TABLES_SYNC_ONCE");
            RemoveGuard(paths, root);
            break;
    }
}

void Recover(const std::filesystem::path& root, const SwapGuard& guard) {
    const auto paths = Paths(root, guard);
    const auto step = NextStep(root, paths, guard);
    // A running swap can fail to sync publication while leaving the name
    // visible. Make the decision durable before moving anything in recovery.
    SyncDirectoryPath(root);
    Complete(root, paths, step);
}

#ifdef _WIN32
Bytes Encode(const SwapGuard& guard) {
    Bytes bytes(kMagic.begin(), kMagic.end());
    WriteLe16(bytes, 1U);
    WriteLe16(bytes, 0U);
    bytes.insert(bytes.end(), guard.directory.begin(), guard.directory.end());
    for (std::size_t i = 0U; i < guard.names.size(); ++i) {
        const auto& name = guard.names[i];
        bytes.push_back(static_cast<std::uint8_t>(name.size()));
        bytes.insert(bytes.end(), name.begin(), name.end());
        bytes.insert(bytes.end(), guard.ids[i].begin(), guard.ids[i].end());
    }
    bytes.insert(bytes.end(), guard.suffix.begin(), guard.suffix.end());
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}
#endif

}  // namespace

bool TableSwapPending(const std::filesystem::path& root) {
    return EntryType(root / kTableSwapPendingFileName) != std::filesystem::file_type::not_found;
}

void RequireNoPendingTableSwap(const std::filesystem::path& root) {
    if (TableSwapPending(root))
        throw std::runtime_error("pending swap needs recovery: " + (root / kTableSwapPendingFileName).string());
}

void RecoverPendingTableSwap(const std::filesystem::path& root) {
    const auto pending = root / kTableSwapPendingFileName;
    if (const auto guard = ReadGuard(pending)) {
        Recover(root, *guard);
    } else {
        // A process can exit after unlink but before its directory sync.
        // Settle the unlink before allowing the names to change again.
        SyncDirectoryPath(root);
    }
    CleanupAtomicTmpArtifacts(pending);
}

void RecoverFencedTableSwap(const std::filesystem::path& root, std::string_view name) {
    const auto guard = ReadGuard(root / kTableSwapPendingFileName);
    if (guard && (guard->names[0] == name || guard->names[1] == name)) {
        Recover(root, *guard);
    } else {
        // The record may already be gone when its removal or undo failed
        // to sync. Do not let either fenced name reopen until it is durable.
        SyncDirectoryPath(root);
    }
}

bool ValidatePendingTableSwap(const std::filesystem::path& root) {
    const auto guard = ReadGuard(root / kTableSwapPendingFileName);
    if (!guard) return false;
    (void)NextStep(root, Paths(root, *guard), *guard);
    return true;
}

#ifdef _WIN32
void SwapTableDirectoriesGuarded(const std::filesystem::path& root,
    const std::array<std::string, 2U>& names, const std::array<StoreId, 2U>& ids, bool& recovery_required) {
    const auto directory = ReadDataDirManifest(root);
    if (!directory) throw std::runtime_error("data directory manifest disappeared");
    SwapGuard guard{directory->data_dir_id, names, ids, {}};
    const auto random = NewStoreId();
    std::copy_n(random.begin(), guard.suffix.size(), guard.suffix.begin());
    const auto paths = Paths(root, guard);
    Validate(paths.pending, guard);
    if (NextStep(root, paths, guard) != SwapStep::kParkFirst)
        Bad(paths.pending, "table identities changed before publication");
    const auto bytes = Encode(guard);
    InjectFailure("CHUNKDB_FAILPOINT_SWAP_TABLES_PUBLISH_FAIL_ONCE", "publishing swap guard", paths.pending);
    recovery_required = true;
    try {
        if (!PublishNewFile(paths.pending, bytes, "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_BEFORE_GUARD_PUBLISH_ONCE",
                "CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_PUBLISH_ONCE"))
            throw std::runtime_error("swap guard already exists: " + paths.pending.string());
    } catch (const std::exception&) {
        recovery_required = TableSwapPending(root);
        throw;
    }
    Crash("CHUNKDB_FAILPOINT_CRASH_SWAP_TABLES_AFTER_GUARD_SYNC_ONCE");
    if (const auto moved = ParkFirst(paths)) {
        // The first rename failed without moving either table. Refuse only
        // after durably taking back the decision.
        RemoveGuard(paths, root);
        recovery_required = false;
        throw std::filesystem::filesystem_error("cannot park first swap table", paths.first, paths.parked, moved);
    }
    Complete(root, paths, SwapStep::kMoveSecond);
    recovery_required = false;
}
#endif

}  // namespace chunkdb
