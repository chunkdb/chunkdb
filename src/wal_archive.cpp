#include "wal_archive.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <stdexcept>
#include <system_error>

#include "checkpoint.hpp"
#include "chunk_store_internal.hpp"
#include "chunkdb/crc32.hpp"
#include "chunkdb/logging.hpp"
#include "chunkdb/table_catalog.hpp"
#include "crypto.hpp"
#include "durability_io.hpp"
#include "migrations_records.hpp"
#include "shared_file_input.hpp"
#include "store_manifest.hpp"
#include "users.hpp"

#ifndef _WIN32
#include <cerrno>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
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

class ArchiveConflictError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Size and CRC32 of a whole file, optionally checking an existing copy.
WalArchiveFile Inspect(const std::filesystem::path& path, const std::filesystem::path* copy = nullptr) {
    SharedFileInput input(path);
    if (!input) throw std::runtime_error("cannot open WAL archive file: " + path.string());
    std::optional<SharedFileInput> existing;
    if (copy != nullptr && !existing.emplace(*copy))
        throw std::runtime_error("cannot open WAL archive file: " + copy->string());
    std::array<char, 65536> buffer{};
    std::array<char, 65536> compared{};
    WalArchiveFile file{path.filename().string(), 0U, 0U};
    for (;;) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (input.bad()) throw std::runtime_error("cannot read WAL archive file: " + path.string());
        if (existing) {
            existing->read(compared.data(), static_cast<std::streamsize>(compared.size()));
            if (existing->bad()) throw std::runtime_error("cannot read WAL archive file: " + copy->string());
            if (existing->gcount() != count || !std::equal(buffer.data(), buffer.data() + count, compared.data()))
                throw ArchiveConflictError("the archive already holds a different " + copy->string());
        }
        if (count <= 0) break;
        file.crc32 = Crc32Extend(file.crc32, reinterpret_cast<const std::uint8_t*>(buffer.data()), static_cast<std::size_t>(count));
        file.size += static_cast<std::uint64_t>(count);
        if (!input) break;
    }
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

// Runs `line` through the system shell and returns its wait status. Outside
// Windows the shell is spawned and waited for directly: std::system would
// ignore SIGINT and SIGQUIT for the whole server while the command runs.
int RunShellCommand(const std::string& line) {
#ifdef _WIN32
    return std::system(line.c_str());
#else
    std::string shell = "sh", option = "-c", text = line;
    char* argv[] = {shell.data(), option.data(), text.data(), nullptr};
    pid_t pid = 0;
    const int started = ::posix_spawn(&pid, "/bin/sh", nullptr, nullptr, argv, environ);
    if (started != 0) throw std::system_error(started, std::generic_category(), "cannot start --archive-command");
    int status = 0;
    while (::waitpid(pid, &status, 0) == -1) {
        if (errno != EINTR) throw std::system_error(errno, std::generic_category(), "cannot wait for --archive-command");
    }
    return status;
#endif
}

bool CommandSucceeded(int status) {
#ifdef _WIN32
    return status == 0;
#else
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
#endif
}

bool Within(const std::filesystem::path& path, const std::filesystem::path& root) {
    const auto relative = path.lexically_relative(root);
    // Inside `root` when the first component neither leaves it nor is it.
    return !relative.empty() && *relative.begin() != ".." && relative != ".";
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

namespace {

constexpr std::array<std::uint8_t, 4> kChainMagic{'C', 'K', 'A', 'C'};
constexpr std::uint16_t kChainFormat = 1U;
constexpr std::size_t kChainBytes = 36U;

// Stops the process at a deterministic archive step for crash tests.
void Crash(const char* point) {
    if (ConsumeFailpointEnv(point)) ExitAbruptly(86);
}

// `name` with a u8 length; empty allowed when `optional`.
void AppendShortName(std::vector<std::uint8_t>& bytes, const std::string& name, bool optional) {
    if (name.empty() && optional) { bytes.push_back(0U); return; }
    RequireFileName(name);
    bytes.push_back(static_cast<std::uint8_t>(name.size()));
    bytes.insert(bytes.end(), name.begin(), name.end());
}

std::string ReadShortName(RecordReader& reader, bool optional) {
    auto name = reader.Text(static_cast<std::size_t>(reader.Le(1U)));
    if (!(name.empty() && optional)) RequireFileName(name);
    return name;
}

}  // namespace

std::vector<std::uint8_t> SerializeWalArchiveRecord(const WalArchiveRecord& record) {
    if (record.tables.size() > kMaxRecordTables) throw std::length_error("too many tables in a WAL archive seal record");
    if (record.predecessor_ms >= record.captured_ms)
        throw std::invalid_argument("a WAL archive seal record follows its predecessor");
    std::vector<std::uint8_t> bytes(kRecordMagic.begin(), kRecordMagic.end());
    WriteLe16(bytes, kRecordFormat);
    WriteLe16(bytes, 0U);
    AppendId(bytes, record.directory);
    WriteLe64(bytes, record.captured_ms);
    WriteLe64(bytes, record.predecessor_ms);
    AppendShortName(bytes, record.users, true);
    AppendShortName(bytes, record.ledger, true);
    WriteLe32(bytes, static_cast<std::uint32_t>(record.tables.size()));
    for (const auto& table : record.tables) {
        if (table.name.size() > 255U) throw std::length_error("WAL archive table name is too long");
        if (table.files.size() > kMaxRecordFiles) throw std::length_error("too many files in a WAL archive seal record");
        bytes.push_back(table.revision ? kEntrySealed : std::uint8_t{0U});
        bytes.push_back(static_cast<std::uint8_t>(table.name.size()));
        bytes.insert(bytes.end(), table.name.begin(), table.name.end());
        AppendId(bytes, table.epoch);
        WriteLe64(bytes, table.revision.value_or(0U));
        WriteLe64(bytes, table.revision ? table.time_bound_ms : 0U);
        WriteLe64(bytes, table.schema_version);
        AppendShortName(bytes, table.manifest, true);
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
    record.predecessor_ms = reader.Le(8U);
    if (record.predecessor_ms >= record.captured_ms)
        throw std::runtime_error("WAL archive seal record does not follow its predecessor");
    record.users = ReadShortName(reader, true);
    record.ledger = ReadShortName(reader, true);
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
        const auto time_bound = reader.Le(8U);
        if ((flags & kEntrySealed) != 0U) {
            table.revision = revision;
            table.time_bound_ms = time_bound;
        } else if (revision != 0U || time_bound != 0U) {
            throw std::runtime_error("WAL archive seal record has a seal point on an unsealed table");
        }
        table.schema_version = reader.Le(8U);
        table.manifest = ReadShortName(reader, true);
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

std::vector<std::uint8_t> SerializeWalArchiveChain(const StoreId& directory, std::uint64_t captured_ms) {
    std::vector<std::uint8_t> bytes(kChainMagic.begin(), kChainMagic.end());
    WriteLe16(bytes, kChainFormat);
    WriteLe16(bytes, 0U);
    AppendId(bytes, directory);
    WriteLe64(bytes, captured_ms);
    WriteLe32(bytes, Crc32(bytes));
    return bytes;
}

std::uint64_t ParseWalArchiveChain(const std::vector<std::uint8_t>& bytes, const StoreId& directory) {
    if (bytes.size() != kChainBytes || !std::equal(kChainMagic.begin(), kChainMagic.end(), bytes.begin()))
        throw std::runtime_error("not a WAL archive chain file");
    const std::vector<std::uint8_t> body(bytes.begin(), bytes.end() - 4);
    RecordReader reader(bytes);
    (void)reader.Text(body.size());
    if (static_cast<std::uint32_t>(reader.Le(4U)) != Crc32(body)) throw std::runtime_error("WAL archive chain file checksum mismatch");
    RecordReader fields(body);
    (void)fields.Text(kChainMagic.size());
    if (fields.Le(2U) != kChainFormat || fields.Le(2U) != 0U) throw std::runtime_error("unknown WAL archive chain file format");
    if (fields.Id() != directory) throw std::runtime_error("WAL archive chain file belongs to another data directory");
    const auto captured = fields.Le(8U);
    if (captured == 0U) throw std::runtime_error("WAL archive chain file names no record");
    return captured;
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

std::string WalArchiveContentName(std::string_view prefix, const std::vector<std::uint8_t>& bytes) {
    constexpr char digits[] = "0123456789abcdef";
    const auto digest = crypto::Sha256Of(bytes);
    std::string name(prefix);
    name.reserve(prefix.size() + 1U + 2U * digest.size());
    name.push_back('.');
    for (const auto byte : digest) {
        name.push_back(digits[byte >> 4U]);
        name.push_back(digits[byte & 15U]);
    }
    return name;
}

void RemoveWalArchiveStaging(const std::filesystem::path& data_dir) {
    const auto staging = data_dir / std::string(kWalArchiveStagingName);
    std::error_code ec;
    const auto status = std::filesystem::symlink_status(staging, ec);
    if (ec == std::errc::no_such_file_or_directory || status.type() == std::filesystem::file_type::not_found) return;
    if (ec) throw std::filesystem::filesystem_error("cannot inspect WAL archive staging", staging, ec);
    std::filesystem::remove_all(staging);
    SyncDirectoryPath(data_dir);
    LogMessage(LogLevel::kInfo, LogComponent::kStore, "WAL archiving is off; the archive chain ends here",
        {{"data_dir", data_dir.string()}});
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
    const auto own = WalArchiveDirectoryDirName(directory_id_);
    // An interrupted pass leaves temporary files. Staged table, users and
    // ledger files and a staged seal record stay: the next pass ships them.
    for (const auto& entry : SortedEntries(staging_)) {
        const auto name = entry.filename().string();
        const bool directory = std::filesystem::is_directory(std::filesystem::symlink_status(entry));
        if (name == kWalArchiveChainName && std::filesystem::is_regular_file(std::filesystem::symlink_status(entry))) continue;
        if (!directory || (name != own && !ParseWalArchiveTableDirName(name))) {
            std::filesystem::remove_all(entry);
            continue;
        }
        for (const auto& file : SortedEntries(entry)) {
            const auto file_name = file.filename().string();
            if (file_name.front() == '.' || file_name.find(".tmp.") != std::string::npos) std::filesystem::remove_all(file);
        }
    }
    const auto chain = staging_ / std::string(kWalArchiveChainName);
    if (std::filesystem::exists(chain)) {
        try {
            predecessor_ms_ = ParseWalArchiveChain(LoadFile(chain), directory_id_);
        } catch (const std::exception& error) {
            // Without a trusted predecessor the next record starts a new chain.
            LogMessage(LogLevel::kWarn, LogComponent::kStore, "WAL archive chain file is unreadable; a new chain starts",
                {{"error", error.what()}});
            std::filesystem::remove(chain);
            predecessor_ms_ = 0U;
        }
    }
    SyncDirectoryPath(staging_);
    worker_ = std::jthread([this](std::stop_token stop) { Run(std::move(stop)); });
}

void WalArchiver::Stop(bool final_pass) noexcept {
    if (worker_.joinable()) {
        worker_.request_stop();
        wait_cv_.notify_all();
        worker_.join();
    }
    if (final_pass) RunPassLogged({});
}

void WalArchiver::Run(std::stop_token stop) {
    for (;;) {
        RunPassLogged(stop);
        std::unique_lock lock(wait_mutex_);
        (void)wait_cv_.wait_until(lock, stop, std::chrono::steady_clock::now() + config_.interval, [] { return false; });
        if (stop.stop_requested()) return;
    }
}

void WalArchiver::RunPassLogged(std::stop_token cancel) noexcept {
    try {
        RunPass(std::move(cancel));
    } catch (const std::exception& error) {
        try {
            LogMessage(LogLevel::kWarn, LogComponent::kStore, "WAL archive pass failed; the next pass retries",
                {{"error", error.what()}});
        } catch (...) {
            std::fprintf(stderr, "WAL archive pass failed: %s\n", error.what());
        }
    }
}

void WalArchiver::RunPass(std::stop_token cancel) {
    std::lock_guard lock(pass_mutex_);
    try {
        Pass(std::move(cancel));
    } catch (const std::exception& error) {
        std::lock_guard status_lock(status_mutex_);
        status_.error = error.what();
        try { status_.staged_bytes = StagedBytes(); } catch (const std::exception&) {}
        throw;
    }
}

WalArchiveStatus WalArchiver::Status() const {
    std::lock_guard lock(status_mutex_);
    return status_;
}

void WalArchiver::Pass(std::stop_token cancel) {
    const auto capture = CaptureCatalog(cancel);
    std::map<StoreId, WalArchiveTableEntry> entries;
    for (const auto& table : capture.tables) {
        auto& entry = entries[table.table->store_id()];
        entry.name = table.table->name();
        entry.epoch = table.table->store_id();
        entry.schema_version = table.schema_version;
        entry.manifest = table.manifest;
    }
    std::optional<std::string> failure;
    for (const auto& table : capture.tables) {
        try {
            StageTable(table, entries);
        } catch (const std::exception& error) {
            if (!failure) failure = "table " + table.table->name() + ": " + error.what();
        }
    }
    Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_AFTER_STAGE_ONCE");
    // A record staged before a crash or a failed pass is published before
    // this one, which names it as predecessor. Staging came first, so a
    // failure here leaves this pass's sealed segments staged.
    FinishStagedRecords();
    const auto captured = std::max({UnixMs(), predecessor_ms_ + 1U, last_attempt_ms_ + 1U});
    last_attempt_ms_ = captured;
    // Everything staged, of these tables and of tables gone since.
    ShipStaged(entries, capture);
    Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_AFTER_SHIP_ONCE");
    WalArchiveRecord record{directory_id_, captured, predecessor_ms_, capture.users, capture.ledger, {}};
    for (auto& [_, entry] : entries)
        if (!entry.manifest.empty() || !entry.files.empty()) record.tables.push_back(std::move(entry));
    PublishRecord(record);
    RemoveShipped(record);
    for (const auto& table : record.tables)
        if (!table.manifest.empty()) shipped_manifests_[table.epoch] = table.manifest;
    shipped_users_ = record.users;
    shipped_ledger_ = record.ledger;
    RetainArchive(captured);
    {
        std::lock_guard lock(status_mutex_);
        status_.last_seal_ms = captured;
        status_.tables.clear();
        for (const auto& table : record.tables)
            if (table.revision) status_.tables.push_back({table.name, *table.revision});
        status_.error = failure;
        status_.staged_bytes = StagedBytes();
    }
    if (failure) throw std::runtime_error(*failure);
}

void WalArchiver::FinishStagedRecords() {
    const auto directory_name = WalArchiveDirectoryDirName(directory_id_);
    for (const auto& path : SortedEntries(staging_ / directory_name)) {
        const auto name = path.filename().string();
        if (!ParseWalArchiveRecordName(name)) continue;
        const auto record = ParseWalArchiveRecord(LoadFile(path));
        if (record.directory != directory_id_) throw std::runtime_error("staged WAL archive seal record belongs to another data directory");
        // An earlier attempt may have published the intent's name before
        // its directory sync failed. Make it durable before shipping it.
        SyncDirectoryPath(path.parent_path());
        // Equal bytes count as shipped, so a record shipped before a crash
        // ships again unchanged. A different record under its name, of an
        // older chain, means this one was never published: it is dropped,
        // and the next record lists its files.
        try {
            (void)Ship(path, directory_name, name);
        } catch (const ArchiveConflictError&) {
            std::filesystem::remove(path);
            SyncDirectoryPath(path.parent_path());
            throw;
        }
        AtomicWrite(staging_ / std::string(kWalArchiveChainName), SerializeWalArchiveChain(directory_id_, record.captured_ms),
            true, true);
        predecessor_ms_ = record.captured_ms;
        RemoveShipped(record);
    }
}

WalArchiver::CatalogCapture WalArchiver::CaptureCatalog(std::stop_token cancel) {
    // Names, epochs, manifests, grants and the ledger as backup captures
    // them: under the metadata gate, which named migrations hold through
    // their decision and completion.
    auto& gate = catalog_.backup_metadata_gate_;
    if (!gate.lock(cancel)) throw std::runtime_error("WAL archive pass stopped");
    std::unique_lock metadata(gate, std::adopt_lock);
    catalog_.migration_health_->Check();
    CatalogCapture capture;
    std::vector<std::shared_ptr<Table>> tables;
    {
        std::shared_lock lock(catalog_.tables_mutex_);
        if (!catalog_.fenced_.empty())
            throw std::runtime_error("cannot capture WAL archive metadata for fenced table '" + catalog_.fenced_.begin()->first + "'");
        for (const auto& [_, table] : catalog_.tables_) tables.push_back(table);
    }
    for (const auto& table : tables) {
        // An exclusive operation may need the metadata gate to finish. If
        // its table cannot be captured now, leave the last record in place.
        const auto lease = table->TryAcquire();
        if (!lease) throw std::runtime_error("cannot capture WAL archive metadata for busy table '" + table->name() + "'");
        const auto bytes = LoadFile(StoreManifestPath(lease->store().data_dir()));
        const auto version = ParseStoreManifest(bytes).schema.version;
        CatalogCapture::TableCapture entry{table, version,
            WalArchiveContentName("table." + std::to_string(version), bytes) + ".manifest"};
        if (shipped_manifests_[table->store_id()] != entry.manifest)
            StageBytes(staging_ / WalArchiveTableDirName(table->store_id()), entry.manifest, bytes);
        capture.tables.push_back(std::move(entry));
    }
    const auto root = staging_ / WalArchiveDirectoryDirName(directory_id_);
    if (const auto users = ReadUsersFile(catalog_.config_.data_dir)) {
        const auto bytes = EncodeUsers(*users);
        capture.users = WalArchiveContentName("users", bytes);
        if (capture.users != shipped_users_) StageBytes(root, capture.users, bytes);
    }
    const auto ledger_path = catalog_.config_.data_dir / std::string(kMigrationsFileName);
    if (std::filesystem::exists(ledger_path)) {
        const auto bytes = LoadFile(ledger_path);
        capture.ledger = WalArchiveContentName("migrations", bytes);
        if (capture.ledger != shipped_ledger_) StageBytes(root, capture.ledger, bytes);
    }
    return capture;
}

void WalArchiver::StageBytes(const std::filesystem::path& directory, const std::string& name,
    const std::vector<std::uint8_t>& bytes) {
    RequireFileName(name);
    if (!EnsureDirectoryPathExists(directory, true)) SyncParentDirectory(directory);
    const auto path = directory / name;
    if (std::filesystem::exists(path)) {
        if (LoadFile(path) != bytes) throw std::runtime_error("staged WAL archive file has different bytes: " + path.string());
        SyncDirectoryPath(directory);
        return;
    }
    AtomicWrite(path, bytes, true, true);
}

void WalArchiver::StageTable(const CatalogCapture::TableCapture& capture, std::map<StoreId, WalArchiveTableEntry>& entries) {
    auto lease = capture.table->Acquire();
    if (!lease) return;
    auto& store = lease->store();
    // The table may have reopened after the metadata gate was released.
    // Seal it only against the manifest this record names.
    const auto bytes = LoadFile(StoreManifestPath(store.data_dir()));
    if (WalArchiveContentName("table." + std::to_string(capture.schema_version), bytes) + ".manifest" != capture.manifest)
        return;
    const auto seal = store.SealForArchive();
    (void)store.StageForArchive(staging_ / WalArchiveTableDirName(capture.table->store_id()));
    if (seal) {
        auto& entry = entries.at(capture.table->store_id());
        entry.revision = seal->revision;
        entry.time_bound_ms = seal->time_bound_ms;
    }
}

void WalArchiver::ShipStaged(std::map<StoreId, WalArchiveTableEntry>& entries, const CatalogCapture& capture) {
    for (const auto& directory : SortedEntries(staging_)) {
        const auto name = directory.filename().string();
        const auto epoch = ParseWalArchiveTableDirName(name);
        if (!epoch || !std::filesystem::is_directory(directory)) continue;
        auto& entry = entries[*epoch];
        entry.epoch = *epoch;
        for (const auto& file : SortedEntries(directory)) {
            const auto file_name = file.filename().string();
            if (file_name.front() == '.' || file_name.find(".tmp.") != std::string::npos) continue;
            entry.files.push_back(Ship(file, name, file_name));
        }
    }
    const auto root_name = WalArchiveDirectoryDirName(directory_id_);
    for (const auto& file : SortedEntries(staging_ / root_name)) {
        const auto file_name = file.filename().string();
        if (ParseWalArchiveRecordName(file_name) || file_name.front() == '.' || file_name.find(".tmp.") != std::string::npos)
            continue;
        if (file_name == capture.users || file_name == capture.ledger) (void)Ship(file, root_name, file_name);
        else std::filesystem::remove(file);  // superseded before it shipped
    }
}

WalArchiveFile WalArchiver::Ship(const std::filesystem::path& source, const std::string& relative_dir,
    const std::string& name) {
    RequireFileName(name);
    if (!config_.command.empty()) {
        auto file = Inspect(source);
        file.name = name;
        const auto line = ExpandWalArchiveCommand(config_.command, source.string(), relative_dir + "/" + name);
        const int status = RunShellCommand(line);
        if (!CommandSucceeded(status))
            throw std::runtime_error("--archive-command failed for " + relative_dir + "/" + name + " (status " +
                std::to_string(status) + ")");
        return file;
    }
    const auto parent = config_.directory / relative_dir;
    const auto target = parent / name;
    (void)EnsureDirectoryPathExists(parent, true);
    const auto finish_existing = [&] {
        auto shipped = Inspect(source, &target);
        // A previous publication may have made the name visible before its
        // directory sync succeeded. Repay that sync before recording the file.
        SyncFilePath(target);
        SyncDirectoryPath(parent);
        shipped.name = name;
        return shipped;
    };
    if (std::filesystem::exists(target)) return finish_existing();
    // A copy a crash interrupted is replaced by this one.
    const auto temporary = parent / ("." + name + ".copy");
    std::filesystem::remove(temporary);
    auto copied = CopySynced(source, temporary);
    const auto ec = MovePathNoReplace(temporary, target);
    if (ec == std::errc::file_exists) {
        std::filesystem::remove(temporary);
        return finish_existing();
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
    if (!EnsureDirectoryPathExists(local.parent_path(), true)) SyncParentDirectory(local.parent_path());
    // The staged record is the intent: a start that finds it ships it again
    // and makes it the chain's last before any newer record.
    AtomicWrite(local, SerializeWalArchiveRecord(record), true, true);
    Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_AFTER_RECORD_STAGE_ONCE");
    try {
        (void)Ship(local, directory_name, name);
    } catch (const ArchiveConflictError&) {
        std::filesystem::remove(local);
        SyncDirectoryPath(local.parent_path());
        throw;
    }
    Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_AFTER_RECORD_SHIP_ONCE");
    AtomicWrite(staging_ / std::string(kWalArchiveChainName), SerializeWalArchiveChain(directory_id_, record.captured_ms),
        true, true);
    predecessor_ms_ = record.captured_ms;
    Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_AFTER_CHAIN_ONCE");
}

void WalArchiver::RemoveShipped(const WalArchiveRecord& record) {
    for (const auto& table : record.tables) {
        const auto directory = staging_ / WalArchiveTableDirName(table.epoch);
        for (const auto& file : table.files) std::filesystem::remove(directory / file.name);
        std::error_code ec;
        if (std::filesystem::is_empty(directory, ec) && !ec) std::filesystem::remove(directory, ec);
    }
    const auto root = staging_ / WalArchiveDirectoryDirName(directory_id_);
    if (!record.users.empty()) std::filesystem::remove(root / record.users);
    if (!record.ledger.empty()) std::filesystem::remove(root / record.ledger);
    // The record goes last: until then a start finishes it again.
    std::filesystem::remove(root / WalArchiveRecordName(record.captured_ms));
    if (std::filesystem::exists(root)) SyncDirectoryPath(root);
    SyncDirectoryPath(staging_);
}

std::uint64_t WalArchiver::StagedBytes() const {
    std::uint64_t bytes = 0U;
    std::error_code ec;
    for (std::filesystem::recursive_directory_iterator it(staging_, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code entry_ec;
        if (!it->is_regular_file(entry_ec) || entry_ec) continue;
        const auto size = it->file_size(entry_ec);
        if (!entry_ec) bytes += size;
    }
    return bytes;
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
    // k, the newest record captured before the bound, starts the kept archive.
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
    std::set<std::filesystem::path> obsolete;
    std::size_t first_kept = 0U;
    for (; first_kept < boundary; ++first_kept) {
        const auto record = ParseWalArchiveRecord(LoadFile(records[first_kept].second));
        bool removable = true;
        for (const auto& table : record.tables) {
            if (const auto revision = sealed(kept, table.epoch)) {
                for (const auto& file : table.files) {
                    const auto last = WalArchiveSegmentLast(file.name);
                    if (last && *last > *revision) removable = false;
                }
            } else if (listed(kept, table.epoch) || listed(newest, table.epoch)) {
                // Not sealed by k: k does not cover this table yet.
                removable = false;
            }
        }
        if (!removable) break;
        for (const auto& table : record.tables) {
            const auto directory = config_.directory / WalArchiveTableDirName(table.epoch);
            for (const auto& file : table.files) {
                // Manifests stay: a later record can name the same one again.
                if (!file.name.ends_with(".manifest")) obsolete.insert(directory / file.name);
            }
        }
    }
    for (std::size_t j = first_kept; j < records.size() && !obsolete.empty(); ++j) {
        const auto record = ParseWalArchiveRecord(LoadFile(records[j].second));
        for (const auto& table : record.tables) {
            const auto directory = config_.directory / WalArchiveTableDirName(table.epoch);
            for (const auto& file : table.files) obsolete.erase(directory / file.name);
        }
    }
    // Remove every pruned record durably before unlinking any of its files.
    for (std::size_t j = 0U; j < first_kept; ++j) {
        std::filesystem::remove(records[j].second);
        SyncDirectoryPath(records_dir);
        Crash("CHUNKDB_FAILPOINT_CRASH_ARCHIVE_RETENTION_AFTER_RECORD_ONCE");
    }
    std::set<std::filesystem::path> touched;
    for (const auto& path : obsolete) {
        std::filesystem::remove(path);
        touched.insert(path.parent_path());
    }
    for (const auto& directory : touched) SyncDirectoryPath(directory);
}

}  // namespace chunkdb
