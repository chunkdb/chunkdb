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

// Sealed segments, base descriptors, manifests, users and ledger files and
// the seal record waiting to be shipped, as hard links or private copies,
// and the chain file (docs/design/WAL_ARCHIVE.md).
inline constexpr std::string_view kWalArchiveStagingName = ".chunkdb.archive";
inline constexpr std::string_view kWalArchiveChainName = "chain";

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
    // With `revision`: issued from the table's commit-time counter before the
    // seal point was sampled.
    std::uint64_t time_bound_ms = 0;
    std::uint64_t schema_version = 0;
    // `table.<schema version>.<CRC32>.manifest`; empty for a dropped table.
    std::string manifest;
    // Shipped since the predecessor, in `table.<epoch>/`.
    std::vector<WalArchiveFile> files;
    friend bool operator==(const WalArchiveTableEntry&, const WalArchiveTableEntry&) = default;
};

struct WalArchiveRecord {
    StoreId directory{};
    std::uint64_t captured_ms = 0;
    // The capture time of the previous record of the chain; zero at a chain start.
    std::uint64_t predecessor_ms = 0;
    // `users.<CRC32>` and `migrations.<CRC32>` in `directory.<id>/`; empty
    // when the data directory has no such file.
    std::string users;
    std::string ledger;
    std::vector<WalArchiveTableEntry> tables;
    friend bool operator==(const WalArchiveRecord&, const WalArchiveRecord&) = default;
};

[[nodiscard]] std::vector<std::uint8_t> SerializeWalArchiveRecord(const WalArchiveRecord& record);
[[nodiscard]] WalArchiveRecord ParseWalArchiveRecord(const std::vector<std::uint8_t>& bytes);
// The chain file: the data-directory id and the capture time of the last
// published record.
[[nodiscard]] std::vector<std::uint8_t> SerializeWalArchiveChain(const StoreId& directory, std::uint64_t captured_ms);
// The capture time a chain file of `directory` holds.
[[nodiscard]] std::uint64_t ParseWalArchiveChain(const std::vector<std::uint8_t>& bytes, const StoreId& directory);
// `table.<epoch hex32>`, the directory of one table's archive files.
[[nodiscard]] std::string WalArchiveTableDirName(const StoreId& epoch);
// `directory.<data-directory id hex32>`, the directory of its seal records.
[[nodiscard]] std::string WalArchiveDirectoryDirName(const StoreId& directory);
// `seal.<capture time in Unix ms, 20 digits>`.
[[nodiscard]] std::string WalArchiveRecordName(std::uint64_t captured_ms);
// `<prefix>.<CRC32 of bytes, 8 hex digits>`, named by content.
[[nodiscard]] std::string WalArchiveContentName(std::string_view prefix, const std::vector<std::uint8_t>& bytes);
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
// A writer started without archiving ends the chain: it removes the staging
// directory, chain file and staged record included, before tables open.
void RemoveWalArchiveStaging(const std::filesystem::path& data_dir);

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
    // One pass now; rethrows its failure. `cancel` stops a wait for the
    // backup metadata gate.
    void RunPass(std::stop_token cancel = {});
    [[nodiscard]] WalArchiveStatus Status() const;

  private:
    // What the pass captured under the backup metadata gate.
    struct CatalogCapture {
        struct TableCapture {
            std::shared_ptr<Table> table;
            std::uint64_t schema_version = 0;
            std::string manifest;
        };
        std::vector<TableCapture> tables;
        std::string users;
        std::string ledger;
    };

    void Run(std::stop_token stop);
    void RunPassLogged(std::stop_token cancel) noexcept;
    void Pass(std::stop_token cancel);
    // Ships a seal record staged before a crash and makes it the chain's last.
    void FinishStagedRecords();
    [[nodiscard]] CatalogCapture CaptureCatalog(std::stop_token cancel);
    // Stages `bytes` as `<directory>/<name>` unless that file is already staged.
    void StageBytes(const std::filesystem::path& directory, const std::string& name, const std::vector<std::uint8_t>& bytes);
    void StageTable(const CatalogCapture::TableCapture& capture, std::map<StoreId, WalArchiveTableEntry>& entries);
    // Also ships the current users and ledger files; superseded ones are dropped.
    void ShipStaged(std::map<StoreId, WalArchiveTableEntry>& entries, const CatalogCapture& capture);
    void PublishRecord(const WalArchiveRecord& record);
    void RemoveShipped(const WalArchiveRecord& record);
    void RetainArchive(std::uint64_t now_ms);
    [[nodiscard]] std::uint64_t StagedBytes() const;
    // Ships the local file `source` as `<relative_dir>/<name>`.
    [[nodiscard]] WalArchiveFile Ship(const std::filesystem::path& source, const std::string& relative_dir,
        const std::string& name);

    TableCatalog& catalog_;
    const WalArchiveConfig config_;
    const StoreId directory_id_;
    const std::filesystem::path staging_;
    // Serializes passes of the worker, RunPass and Stop.
    std::mutex pass_mutex_;
    // The capture time of the chain's last published record; zero before
    // the first of a new chain. Under pass_mutex_.
    std::uint64_t predecessor_ms_ = 0U;
    // The capture time of the last record this process tried to publish.
    std::uint64_t last_attempt_ms_ = 0U;
    // Names already shipped by this process, so unchanged manifests, users
    // and ledger files are not shipped again each pass. Under pass_mutex_.
    std::map<StoreId, std::string> shipped_manifests_;
    std::string shipped_users_;
    std::string shipped_ledger_;
    mutable std::mutex status_mutex_;
    WalArchiveStatus status_;
    std::mutex wait_mutex_;
    std::condition_variable_any wait_cv_;
    std::jthread worker_;
};

}  // namespace chunkdb
