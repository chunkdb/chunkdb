#pragma once

#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "chunkdb/chunk_store.hpp"
#include "chunkdb/wal_archive.hpp"

namespace chunkdb {

class TableCatalog;
class Table;

// Sealed segments, base descriptors, manifests and seal records waiting to
// be shipped, as hard links or private copies (docs/design/WAL_ARCHIVE.md).
inline constexpr std::string_view kWalArchiveStagingName = ".chunkdb.archive";

struct WalArchiveFile {
    std::string name;
    std::uint64_t size = 0;
    std::uint32_t crc32 = 0;
    friend bool operator==(const WalArchiveFile&, const WalArchiveFile&) = default;
};

struct WalArchiveTableEntry {
    // Empty for a table whose files a later pass shipped after it was dropped.
    std::string name;
    StoreId epoch{};
    // The table's seal point; std::nullopt when this pass shipped its files
    // without sealing it.
    std::optional<std::uint64_t> revision{};
    std::uint64_t schema_version = 0;
    // Shipped since the previous record, in `table.<epoch>/`.
    std::vector<WalArchiveFile> files;
    friend bool operator==(const WalArchiveTableEntry&, const WalArchiveTableEntry&) = default;
};

struct WalArchiveRecord {
    StoreId directory{};
    std::uint64_t captured_ms = 0;
    std::vector<WalArchiveTableEntry> tables;
    friend bool operator==(const WalArchiveRecord&, const WalArchiveRecord&) = default;
};

[[nodiscard]] std::vector<std::uint8_t> SerializeWalArchiveRecord(const WalArchiveRecord& record);
[[nodiscard]] WalArchiveRecord ParseWalArchiveRecord(const std::vector<std::uint8_t>& bytes);
// `table.<epoch hex32>`, the directory of one table's archive files.
[[nodiscard]] std::string WalArchiveTableDirName(const StoreId& epoch);
// `directory.<data-directory id hex32>`, the directory of its seal records.
[[nodiscard]] std::string WalArchiveDirectoryDirName(const StoreId& directory);
// `seal.<capture time in Unix ms, 20 digits>`.
[[nodiscard]] std::string WalArchiveRecordName(std::uint64_t captured_ms);
// The capture time a seal record name holds; std::nullopt for another name.
[[nodiscard]] std::optional<std::uint64_t> ParseWalArchiveRecordName(std::string_view name);
// The epoch a `table.<epoch>` name holds; std::nullopt for another name.
[[nodiscard]] std::optional<StoreId> ParseWalArchiveTableDirName(std::string_view name);
// The last revision of a sealed segment name `C_<coordinates>.<first>-<last>.wal`.
[[nodiscard]] std::optional<std::uint64_t> WalArchiveSegmentLast(std::string_view name);
// Also refuses an archive directory that contains the data directory or lies in it.
void RequireWalArchiveOutside(const WalArchiveConfig& config, const std::filesystem::path& data_dir);
// The command line for one file: %p is `local`, %f is `relative`, %% is %.
[[nodiscard]] std::string ExpandWalArchiveCommand(std::string_view command, std::string_view local,
    std::string_view relative);

// The server's archiver: every interval it seals each table, stages its
// sealed segments, ships everything staged and publishes one seal record.
class WalArchiver {
  public:
    WalArchiver(TableCatalog& catalog, WalArchiveConfig config);
    ~WalArchiver();
    WalArchiver(const WalArchiver&) = delete;
    WalArchiver& operator=(const WalArchiver&) = delete;

    // Starts the worker; its first pass runs at once.
    void Start();
    // Stops the worker; with `final_pass`, runs one more pass. Failures are
    // logged.
    void Stop(bool final_pass) noexcept;
    // One pass now; rethrows its failure.
    void RunPass();
    [[nodiscard]] WalArchiveStatus Status() const;

  private:
    void Run(std::stop_token stop);
    void RunPassLogged() noexcept;
    void Pass();
    void StageTable(Table& table, std::map<StoreId, WalArchiveTableEntry>& entries);
    void ShipStaged(std::map<StoreId, WalArchiveTableEntry>& entries);
    void PublishRecord(const WalArchiveRecord& record);
    void RemoveShipped(const WalArchiveRecord& record);
    void RetainArchive(std::uint64_t now_ms);
    // Ships the local file `source` as `<relative_dir>/<name>`.
    [[nodiscard]] WalArchiveFile Ship(const std::filesystem::path& source, const std::string& relative_dir,
        const std::string& name);

    TableCatalog& catalog_;
    const WalArchiveConfig config_;
    const StoreId directory_id_;
    const std::filesystem::path staging_;
    // Serializes passes of the worker, RunPass and Stop.
    std::mutex pass_mutex_;
    // The newest manifest schema version staged per epoch; under pass_mutex_.
    std::map<StoreId, std::uint64_t> manifests_;
    // The capture time of this process's last pass; under pass_mutex_.
    std::uint64_t last_captured_ = 0U;
    mutable std::mutex status_mutex_;
    WalArchiveStatus status_;
    std::mutex wait_mutex_;
    std::condition_variable_any wait_cv_;
    std::jthread worker_;
};

}  // namespace chunkdb
