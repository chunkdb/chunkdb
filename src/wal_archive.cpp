#include "wal_archive.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <system_error>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "durability_io.hpp"
#include "shared_file_input.hpp"
#include "store_manifest.hpp"

#ifndef _WIN32
#include <sys/wait.h>
#endif

namespace chunkdb {
namespace {

constexpr std::array<std::uint8_t, 4> kRecordMagic{'C', 'K', 'A', 'S'};
constexpr std::uint16_t kRecordFormat = 1U;
constexpr std::uint8_t kEntrySealed = 0x1U;
// A record names at most this many tables and files per table.
constexpr std::uint32_t kMaxRecordTables = 1U << 20U;
constexpr std::uint32_t kMaxRecordFiles = 1U << 24U;

class RecordReader {
  public:
    explicit RecordReader(const std::vector<std::uint8_t>& bytes) : bytes_(bytes) {}
    void Need(std::size_t count) const {
        if (count > bytes_.size() - at_) throw std::runtime_error("WAL archive seal record is truncated");
    }
    std::uint64_t Le(std::size_t width) {
        Need(width);
        std::uint64_t value = 0U;
        for (std::size_t i = 0U; i < width; ++i) value |= static_cast<std::uint64_t>(bytes_[at_ + i]) << (8U * i);
        at_ += width;
        return value;
    }
    std::string Text(std::size_t count) {
        Need(count);
        std::string text(bytes_.begin() + static_cast<std::ptrdiff_t>(at_),
            bytes_.begin() + static_cast<std::ptrdiff_t>(at_ + count));
        at_ += count;
        return text;
    }
    StoreId Id() {
        Need(16U);
        StoreId id{};
        std::copy_n(bytes_.begin() + static_cast<std::ptrdiff_t>(at_), id.size(), id.begin());
        at_ += id.size();
        return id;
    }
    [[nodiscard]] std::size_t at() const noexcept { return at_; }

  private:
    const std::vector<std::uint8_t>& bytes_;
    std::size_t at_ = 0U;
};

void AppendId(std::vector<std::uint8_t>& bytes, const StoreId& id) { bytes.insert(bytes.end(), id.begin(), id.end()); }

bool Digits(std::string_view text, std::uint64_t* value) {
    if (text.empty()) return false;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), *value);
    return error == std::errc{} && end == text.data() + text.size();
}

std::optional<StoreId> ParseHexId(std::string_view text) {
    if (text.size() != 32U) return std::nullopt;
    StoreId id{};
    for (std::size_t i = 0U; i < id.size(); ++i) {
        unsigned value = 0U;
        const auto [end, error] = std::from_chars(text.data() + 2U * i, text.data() + 2U * i + 2U, value, 16);
        if (error != std::errc{} || end != text.data() + 2U * i + 2U) return std::nullopt;
        id[i] = static_cast<std::uint8_t>(value);
    }
    // Only the lowercase form a writer produces names an archive directory.
    if (StoreIdHex(id) != text) return std::nullopt;
    return id;
}

// A name the archive and its staging use for a file; never a path.
void RequireFileName(const std::string& name) {
    if (name.empty() || name.size() > 255U || name.front() == '.' ||
        name.find_first_of("/\\:") != std::string::npos || name.find('\0') != std::string::npos)
        throw std::runtime_error("invalid WAL archive file name: " + name);
}

std::uint64_t UnixMs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

// Size and CRC32 of a whole file.
WalArchiveFile Inspect(const std::filesystem::path& path) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("cannot open WAL archive file: " + path.string());
    std::array<char, 65536> buffer{};
    WalArchiveFile file{path.filename().string(), 0U, 0U};
    for (;;) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (count <= 0) break;
        file.crc32 = Crc32Extend(file.crc32, reinterpret_cast<const std::uint8_t*>(buffer.data()), static_cast<std::size_t>(count));
        file.size += static_cast<std::uint64_t>(count);
        if (!input) break;
    }
    if (input.bad()) throw std::runtime_error("cannot read WAL archive file: " + path.string());
    return file;
}

// Copies `source` to `target`, which must not exist, and syncs it.
WalArchiveFile CopySynced(const std::filesystem::path& source, const std::filesystem::path& target) {
    SharedFileInput input(source);
    if (!input) throw std::runtime_error("cannot open WAL archive file: " + source.string());
    WalArchiveFile file{source.filename().string(), 0U, 0U};
    {
        std::ofstream output(target, std::ios::binary | std::ios::trunc);
        if (!output) throw std::runtime_error("cannot create WAL archive file: " + target.string());
        std::array<char, 65536> buffer{};
        for (;;) {
            input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const auto count = input.gcount();
            if (count <= 0) break;
            output.write(buffer.data(), count);
            if (!output) throw std::runtime_error("cannot write WAL archive file: " + target.string());
            file.crc32 = Crc32Extend(file.crc32, reinterpret_cast<const std::uint8_t*>(buffer.data()), static_cast<std::size_t>(count));
            file.size += static_cast<std::uint64_t>(count);
            if (!input) break;
        }
        if (input.bad()) throw std::runtime_error("cannot read WAL archive file: " + source.string());
        output.close();
        if (!output) throw std::runtime_error("cannot close WAL archive file: " + target.string());
    }
    SyncFilePath(target);
    return file;
}

bool CommandSucceeded(int status) {
#ifdef _WIN32
    return status == 0;
#else
    return status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 0;
#endif
}

bool Within(const std::filesystem::path& path, const std::filesystem::path& root) {
    const auto relative = path.lexically_relative(root);
    return !relative.empty() && relative.native().front() != '.';
}

std::vector<std::filesystem::path> SortedEntries(const std::filesystem::path& directory) {
    std::vector<std::filesystem::path> entries;
    std::error_code ec;
    if (!std::filesystem::exists(directory, ec)) {
        if (ec) throw std::filesystem::filesystem_error("cannot inspect WAL archive directory", directory, ec);
        return entries;
    }
    for (const auto& entry : std::filesystem::directory_iterator(directory)) entries.push_back(entry.path());
    std::sort(entries.begin(), entries.end());
    return entries;
}

}  // namespace

void RequireValidWalArchiveConfig(const WalArchiveConfig& config) {
    if (!config.directory.empty() && !config.command.empty())
        throw std::invalid_argument("--archive-dir and --archive-command exclude each other");
    if (!config.enabled()) return;
    if (config.interval < std::chrono::milliseconds(100) || config.interval > std::chrono::milliseconds(86400000))
        throw std::invalid_argument("--archive-interval-ms must be 100..86400000");
    if (config.retention.count() < 0) throw std::invalid_argument("--archive-retention-ms must not be negative");
    if (!config.command.empty()) {
        if (config.retention.count() != 0)
            throw std::invalid_argument("--archive-retention-ms applies to --archive-dir; the command's destination keeps its own retention");
        bool file = false;
        for (std::size_t i = 0U; i < config.command.size(); ++i) {
            if (config.command[i] != '%') continue;
            if (i + 1U == config.command.size() || (config.command[i + 1U] != 'p' && config.command[i + 1U] != 'f' &&
                                                     config.command[i + 1U] != '%'))
                throw std::invalid_argument("--archive-command allows only %p, %f and %%");
            file = file || config.command[i + 1U] == 'p';
            ++i;
        }
        if (!file) throw std::invalid_argument("--archive-command must name the file with %p");
    }
}

void RequireWalArchiveOutside(const WalArchiveConfig& config, const std::filesystem::path& data_dir) {
    RequireValidWalArchiveConfig(config);
    if (config.directory.empty()) return;
    const auto archive = std::filesystem::weakly_canonical(config.directory);
    const auto data = std::filesystem::weakly_canonical(data_dir);
    if (archive == data || Within(archive, data) || Within(data, archive))
        throw std::invalid_argument("--archive-dir and --data-dir must not contain each other");
}

std::string ExpandWalArchiveCommand(std::string_view command, std::string_view local, std::string_view relative) {
    std::string line;
    for (std::size_t i = 0U; i < command.size(); ++i) {
        if (command[i] != '%' || i + 1U == command.size()) { line.push_back(command[i]); continue; }
        const char next = command[++i];
        if (next == 'p') line.append(local);
        else if (next == 'f') line.append(relative);
        else line.push_back(next);
    }
    return line;
}

std::vector<std::uint8_t> SerializeWalArchiveRecord(const WalArchiveRecord& record) {
    if (record.tables.size() > kMaxRecordTables) throw std::length_error("too many tables in a WAL archive seal record");
    std::vector<std::uint8_t> bytes(kRecordMagic.begin(), kRecordMagic.end());
    WriteLe16(bytes, kRecordFormat);
    WriteLe16(bytes, 0U);
    AppendId(bytes, record.directory);
    WriteLe64(bytes, record.captured_ms);
    WriteLe32(bytes, static_cast<std::uint32_t>(record.tables.size()));
    for (const auto& table : record.tables) {
        if (table.name.size() > 255U) throw std::length_error("WAL archive table name is too long");
        if (table.files.size() > kMaxRecordFiles) throw std::length_error("too many files in a WAL archive seal record");
        bytes.push_back(table.revision ? kEntrySealed : std::uint8_t{0U});
        bytes.push_back(static_cast<std::uint8_t>(table.name.size()));
        bytes.insert(bytes.end(), table.name.begin(), table.name.end());
        AppendId(bytes, table.epoch);
        WriteLe64(bytes, table.revision.value_or(0U));
        WriteLe64(bytes, table.schema_version);
        WriteLe32(bytes, static_cast<std::uint32_t>(table.files.size()));
        for (const auto& file : table.files) {
            RequireFileName(file.name);
            WriteLe16(bytes, static_cast<std::uint16_t>(file.name.size()));
            bytes.insert(bytes.end(), file.name.begin(), file.name.end());
            WriteLe64(bytes, file.size);
            WriteLe32(bytes, file.crc32);
        }
    }
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}

WalArchiveRecord ParseWalArchiveRecord(const std::vector<std::uint8_t>& bytes) {
    if (bytes.size() < 4U + 4U || !std::equal(kRecordMagic.begin(), kRecordMagic.end(), bytes.begin()))
        throw std::runtime_error("not a WAL archive seal record");
    const std::vector<std::uint8_t> body(bytes.begin(), bytes.end() - 4);
    RecordReader crc(bytes);
    (void)crc.Text(body.size());
    if (static_cast<std::uint32_t>(crc.Le(4U)) != Crc32(body))
        throw std::runtime_error("WAL archive seal record checksum mismatch");
    RecordReader reader(body);
    (void)reader.Text(kRecordMagic.size());
    if (reader.Le(2U) != kRecordFormat || reader.Le(2U) != 0U)
        throw std::runtime_error("unknown WAL archive seal record format");
    WalArchiveRecord record;
    record.directory = reader.Id();
    record.captured_ms = reader.Le(8U);
    const auto tables = reader.Le(4U);
    if (tables > kMaxRecordTables) throw std::runtime_error("too many tables in a WAL archive seal record");
    std::set<StoreId> epochs;
    for (std::uint64_t t = 0U; t < tables; ++t) {
        WalArchiveTableEntry table;
        const auto flags = reader.Le(1U);
        if ((flags & ~static_cast<std::uint64_t>(kEntrySealed)) != 0U)
            throw std::runtime_error("unknown WAL archive seal record flags");
        table.name = reader.Text(static_cast<std::size_t>(reader.Le(1U)));
        table.epoch = reader.Id();
        if (!epochs.insert(table.epoch).second) throw std::runtime_error("WAL archive seal record repeats a table");
        const auto revision = reader.Le(8U);
        if ((flags & kEntrySealed) != 0U) table.revision = revision;
        else if (revision != 0U) throw std::runtime_error("WAL archive seal record has a revision without a seal point");
        table.schema_version = reader.Le(8U);
        const auto files = reader.Le(4U);
        if (files > kMaxRecordFiles) throw std::runtime_error("too many files in a WAL archive seal record");
        for (std::uint64_t f = 0U; f < files; ++f) {
            WalArchiveFile file;
            file.name = reader.Text(static_cast<std::size_t>(reader.Le(2U)));
            RequireFileName(file.name);
            file.size = reader.Le(8U);
            file.crc32 = static_cast<std::uint32_t>(reader.Le(4U));
            table.files.push_back(std::move(file));
        }
        record.tables.push_back(std::move(table));
    }
    if (reader.at() != body.size()) throw std::runtime_error("WAL archive seal record has trailing bytes");
    return record;
}

std::string WalArchiveTableDirName(const StoreId& epoch) { return "table." + StoreIdHex(epoch); }

std::string WalArchiveDirectoryDirName(const StoreId& directory) { return "directory." + StoreIdHex(directory); }

std::string WalArchiveRecordName(std::uint64_t captured_ms) {
    std::array<char, 32> digits{};
    std::snprintf(digits.data(), digits.size(), "%020llu", static_cast<unsigned long long>(captured_ms));
    return "seal." + std::string(digits.data());
}

std::optional<std::uint64_t> ParseWalArchiveRecordName(std::string_view name) {
    constexpr std::string_view prefix = "seal.";
    std::uint64_t captured = 0U;
    if (!name.starts_with(prefix) || name.size() != prefix.size() + 20U || !Digits(name.substr(prefix.size()), &captured))
        return std::nullopt;
    return captured;
}

std::optional<StoreId> ParseWalArchiveTableDirName(std::string_view name) {
    constexpr std::string_view prefix = "table.";
    if (!name.starts_with(prefix)) return std::nullopt;
    return ParseHexId(name.substr(prefix.size()));
}

std::optional<std::uint64_t> WalArchiveSegmentLast(std::string_view name) {
    if (!name.ends_with(".wal")) return std::nullopt;
    name.remove_suffix(4U);
    const auto dot = name.find('.');
    const auto dash = name.rfind('-');
    std::uint64_t last = 0U;
    if (dot == std::string_view::npos || dash == std::string_view::npos || dash < dot || !Digits(name.substr(dash + 1U), &last))
        return std::nullopt;
    return last;
}

WalArchiver::WalArchiver(TableCatalog& catalog, WalArchiveConfig config)
    : catalog_(catalog), config_(std::move(config)),
      directory_id_(ReadDataDirManifest(catalog.config_.data_dir).value().data_dir_id),
      staging_(catalog.config_.data_dir / std::string(kWalArchiveStagingName)) {
    RequireWalArchiveOutside(config_, catalog.config_.data_dir);
    std::lock_guard lock(status_mutex_);
    status_.mode = config_.directory.empty() ? "command" : "dir";
}

WalArchiver::~WalArchiver() { Stop(/*final_pass=*/false); }

void WalArchiver::Start() {
    (void)EnsureDirectoryPathExists(staging_, true);
    // An interrupted pass leaves temporary files and an unshipped seal
    // record. Staged table files stay: the next record lists them again.
    for (const auto& entry : SortedEntries(staging_)) {
        if (!ParseWalArchiveTableDirName(entry.filename().string()) || !std::filesystem::is_directory(entry)) {
            std::filesystem::remove_all(entry);
            continue;
        }
        for (const auto& file : SortedEntries(entry))
            if (file.filename().string().find(".tmp.") != std::string::npos) std::filesystem::remove(file);
    }
    worker_ = std::jthread([this](std::stop_token stop) { Run(std::move(stop)); });
}

void WalArchiver::Stop(bool final_pass) noexcept {
    if (worker_.joinable()) {
        worker_.request_stop();
        wait_cv_.notify_all();
        worker_.join();
    }
    if (final_pass) RunPassLogged();
}

void WalArchiver::Run(std::stop_token stop) {
    for (;;) {
        RunPassLogged();
        std::unique_lock lock(wait_mutex_);
        (void)wait_cv_.wait_until(lock, stop, std::chrono::steady_clock::now() + config_.interval, [] { return false; });
        if (stop.stop_requested()) return;
    }
}

void WalArchiver::RunPassLogged() noexcept {
    try {
        RunPass();
    } catch (const std::exception& error) {
        try {
            LogMessage(LogLevel::kWarn, LogComponent::kStore, "WAL archive pass failed; the next pass retries",
                {{"error", error.what()}});
        } catch (...) {
            std::fprintf(stderr, "WAL archive pass failed: %s\n", error.what());
        }
    }
}

void WalArchiver::RunPass() {
    std::lock_guard lock(pass_mutex_);
    try {
        Pass();
    } catch (const std::exception& error) {
        std::lock_guard status_lock(status_mutex_);
        status_.error = error.what();
        throw;
    }
}

WalArchiveStatus WalArchiver::Status() const {
    std::lock_guard lock(status_mutex_);
    return status_;
}

void WalArchiver::Pass() {
    // Each record of this process gets its own name.
    const auto captured = std::max(UnixMs(), last_captured_ + 1U);
    last_captured_ = captured;
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(catalog_.tables_mutex_);
        for (const auto& [_, table] : catalog_.tables_) tables.push_back(table);
    }
    std::map<StoreId, WalArchiveTableEntry> entries;
    std::optional<std::string> failure;
    for (const auto& table : tables) {
        try {
            StageTable(*table, entries);
        } catch (const std::exception& error) {
            if (!failure) failure = "table " + table->name() + ": " + error.what();
        }
    }
    // Everything staged, of these tables and of tables gone since.
    ShipStaged(entries);
    WalArchiveRecord record{directory_id_, captured, {}};
    for (auto& [_, entry] : entries)
        if (entry.revision || !entry.files.empty()) record.tables.push_back(std::move(entry));
    PublishRecord(record);
    RemoveShipped(record);
    RetainArchive(captured);
    {
        std::lock_guard lock(status_mutex_);
        status_.last_seal_ms = captured;
        status_.tables.clear();
        for (const auto& table : record.tables)
            if (table.revision) status_.tables.push_back({table.name, *table.revision});
        status_.error = failure;
    }
    if (failure) throw std::runtime_error(*failure);
}

void WalArchiver::StageTable(Table& table, std::map<StoreId, WalArchiveTableEntry>& entries) {
    auto lease = table.Acquire();
    if (!lease) return;
    auto& store = lease->store();
    const auto directory = staging_ / WalArchiveTableDirName(table.store_id());
    const auto revision = store.SealForArchive();
    (void)store.StageForArchive(directory);
    const auto schema = store.geometry().layout().schema().version;
    auto& staged = manifests_[table.store_id()];
    if (staged < schema) {
        // Named by schema version and content, so a manifest staged again
        // after a restart ships under the same name only with equal bytes.
        const auto bytes = LoadFile(StoreManifestPath(store.data_dir()));
        std::array<char, 16> crc{};
        std::snprintf(crc.data(), crc.size(), "%08x", static_cast<unsigned>(Crc32(bytes)));
        (void)EnsureDirectoryPathExists(directory, true);
        AtomicWrite(directory / ("table." + std::to_string(schema) + "." + crc.data() + ".manifest"), bytes, true, true);
        staged = schema;
    }
    auto& entry = entries[table.store_id()];
    entry.name = table.name();
    entry.epoch = table.store_id();
    entry.revision = revision;
    entry.schema_version = schema;
}

void WalArchiver::ShipStaged(std::map<StoreId, WalArchiveTableEntry>& entries) {
    for (const auto& directory : SortedEntries(staging_)) {
        const auto name = directory.filename().string();
        const auto epoch = ParseWalArchiveTableDirName(name);
        if (!epoch || !std::filesystem::is_directory(directory)) continue;
        auto& entry = entries[*epoch];
        entry.epoch = *epoch;
        for (const auto& file : SortedEntries(directory)) {
            const auto file_name = file.filename().string();
            if (file_name.find(".tmp.") != std::string::npos) continue;
            entry.files.push_back(Ship(file, name, file_name));
        }
    }
}

WalArchiveFile WalArchiver::Ship(const std::filesystem::path& source, const std::string& relative_dir,
    const std::string& name) {
    RequireFileName(name);
    if (!config_.command.empty()) {
        auto file = Inspect(source);
        file.name = name;
        const auto line = ExpandWalArchiveCommand(config_.command, source.string(), relative_dir + "/" + name);
        const int status = std::system(line.c_str());
        if (!CommandSucceeded(status))
            throw std::runtime_error("--archive-command failed for " + relative_dir + "/" + name + " (status " +
                std::to_string(status) + ")");
        return file;
    }
    const auto parent = config_.directory / relative_dir;
    const auto target = parent / name;
    (void)EnsureDirectoryPathExists(parent, true);
    const auto compare = [&](WalArchiveFile shipped) {
        const auto existing = Inspect(target);
        if (existing.size != shipped.size || existing.crc32 != shipped.crc32)
            throw std::runtime_error("the archive already holds a different " + target.string());
        shipped.name = name;
        return shipped;
    };
    if (std::filesystem::exists(target)) return compare(Inspect(source));
    const auto temporary = parent / ("." + name + ".copy");
    std::filesystem::remove(temporary);
    auto copied = CopySynced(source, temporary);
    const auto ec = MovePathNoReplace(temporary, target);
    if (ec == std::errc::file_exists) {
        std::filesystem::remove(temporary);
        return compare(copied);
    }
    if (ec) throw std::system_error(ec, "cannot publish archive file " + target.string());
    SyncDirectoryPath(parent);
    copied.name = name;
    return copied;
}

void WalArchiver::PublishRecord(const WalArchiveRecord& record) {
    const auto directory_name = WalArchiveDirectoryDirName(directory_id_);
    const auto name = WalArchiveRecordName(record.captured_ms);
    const auto local = staging_ / directory_name / name;
    (void)EnsureDirectoryPathExists(local.parent_path(), true);
    AtomicWrite(local, SerializeWalArchiveRecord(record), true, true);
    (void)Ship(local, directory_name, name);
    std::filesystem::remove(local);
}

void WalArchiver::RemoveShipped(const WalArchiveRecord& record) {
    for (const auto& table : record.tables) {
        const auto directory = staging_ / WalArchiveTableDirName(table.epoch);
        for (const auto& file : table.files) std::filesystem::remove(directory / file.name);
        std::error_code ec;
        if (std::filesystem::is_empty(directory, ec) && !ec) std::filesystem::remove(directory, ec);
    }
    SyncDirectoryPath(staging_);
}

void WalArchiver::RetainArchive(std::uint64_t now_ms) {
    if (config_.directory.empty() || config_.retention.count() == 0) return;
    const auto retention = static_cast<std::uint64_t>(config_.retention.count());
    if (now_ms <= retention) return;
    const auto bound = now_ms - retention;
    const auto records_dir = config_.directory / WalArchiveDirectoryDirName(directory_id_);
    std::vector<std::pair<std::uint64_t, std::filesystem::path>> records;
    for (const auto& path : SortedEntries(records_dir))
        if (const auto captured = ParseWalArchiveRecordName(path.filename().string())) records.emplace_back(*captured, path);
    // The newest record captured before the bound starts the kept archive.
    std::size_t boundary = records.size();
    for (std::size_t i = 0U; i < records.size(); ++i)
        if (records[i].first < bound) boundary = i;
    if (boundary == records.size() || boundary == 0U) return;
    const auto kept = ParseWalArchiveRecord(LoadFile(records[boundary].second));
    const auto newest = ParseWalArchiveRecord(LoadFile(records.back().second));
    const auto sealed = [](const WalArchiveRecord& in, const StoreId& epoch) -> std::optional<std::uint64_t> {
        for (const auto& table : in.tables) if (table.epoch == epoch) return table.revision;
        return std::nullopt;
    };
    const auto listed = [](const WalArchiveRecord& in, const StoreId& epoch) {
        return std::any_of(in.tables.begin(), in.tables.end(), [&](const auto& table) { return table.epoch == epoch; });
    };
    std::set<std::filesystem::path> touched;
    for (std::size_t j = 0U; j < boundary; ++j) {
        const auto record = ParseWalArchiveRecord(LoadFile(records[j].second));
        bool removable = true;
        for (const auto& table : record.tables) {
            if (const auto revision = sealed(kept, table.epoch)) {
                for (const auto& file : table.files) {
                    const auto last = WalArchiveSegmentLast(file.name);
                    if (last && *last > *revision) removable = false;
                }
            } else if (listed(kept, table.epoch) || listed(newest, table.epoch)) {
                // Sealed again later: the boundary does not cover this table yet.
                removable = false;
            }
        }
        if (!removable) break;
        // The record goes first: a crash then leaves only files no record needs.
        std::filesystem::remove(records[j].second);
        SyncDirectoryPath(records_dir);
        for (const auto& table : record.tables) {
            const auto directory = config_.directory / WalArchiveTableDirName(table.epoch);
            for (const auto& file : table.files) {
                // Manifests stay: a later record can list the same one again.
                if (file.name.ends_with(".manifest")) continue;
                std::filesystem::remove(directory / file.name);
                touched.insert(directory);
            }
        }
    }
    for (const auto& directory : touched) SyncDirectoryPath(directory);
}

}  // namespace chunkdb
