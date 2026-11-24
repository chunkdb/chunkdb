#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include "chunkdb/chunk_store.hpp"

// The catalog revision of WATCH CATALOG and its durable record
// (docs/design/CATALOG_REVISION.md).
namespace chunkdb {

inline constexpr std::string_view kCatalogRevisionFileName = "chunkdb.catalog";
inline constexpr std::size_t kCatalogRevisionFileSize = 40U;

// `chunkdb.catalog`: the catalog epoch, the last catalog revision and whether
// a catalog statement may have run since that revision was recorded.
struct CatalogRevisionRecord {
    StoreId epoch{};
    std::uint64_t revision = 0U;
    bool in_progress = false;
    friend bool operator==(const CatalogRevisionRecord&, const CatalogRevisionRecord&) = default;
};
[[nodiscard]] std::vector<std::uint8_t> EncodeCatalogRevision(const CatalogRevisionRecord& record);
// Throws std::runtime_error naming the defect.
[[nodiscard]] CatalogRevisionRecord DecodeCatalogRevision(const std::vector<std::uint8_t>& bytes);
// std::nullopt only when the file does not exist; throws when it cannot be
// read or decoded.
[[nodiscard]] std::optional<CatalogRevisionRecord> ReadCatalogRevision(const std::filesystem::path& data_dir);

// Marks an existing record in progress, for a catalog change made without
// the server (chunkdb_admin reset-password): the next start moves past the
// revision a consumer may hold. A missing or unreadable record needs nothing,
// since the start then begins a new epoch.
void MarkCatalogRevision(const std::filesystem::path& data_dir);

// Gives catalog revisions. One revision names one catalog state: a revision
// is recorded before it is returned, a catalog statement is marked before it
// runs, and a start after a mark that was not cleared moves to a new revision.
class CatalogClock {
  public:
    enum class Mode {
        // In memory, with an epoch of this process: the directory lacks
        // catalog_revision, or the server opened it read-only.
        kMemory,
        // Recorded in chunkdb.catalog.
        kDurable,
        // Several processes may change the catalog: the record stays marked
        // in progress, so the next single-process start takes a new revision.
        kShared,
    };
    // kDurable and kShared read the record and write nothing yet; a missing or
    // unreadable record starts a new epoch.
    CatalogClock(std::filesystem::path data_dir, Mode mode);
    CatalogClock(const CatalogClock&) = delete;
    CatalogClock& operator=(const CatalogClock&) = delete;

    [[nodiscard]] Mode mode() const noexcept { return mode_; }
    [[nodiscard]] const StoreId& epoch() const noexcept { return epoch_; }
    [[nodiscard]] std::uint64_t revision() const;
    // Before a catalog statement runs. Throws when the mark cannot be
    // recorded: the statement must not run then.
    void Begin();
    // After that statement ended, finished or failed: its revision, recorded
    // before it returns. Throws when it cannot be recorded; the revision must
    // not be published then.
    [[nodiscard]] std::uint64_t End();
    // The revision for a read of the whole catalog: the current one while no
    // catalog statement runs, else a new one, recorded before it returns.
    [[nodiscard]] std::uint64_t Snapshot();
    // While held, the record stays marked in progress even with no statement
    // running: the published records miss tables a read could not read, so a
    // start must not let a consumer resume at a revision that misses them.
    void Hold(bool hold);
    // After a refresh at `revision` read every table: clears the mark when that
    // is still the current revision and no statement runs.
    void Settle(std::uint64_t revision);

  private:
    void Write(std::uint64_t revision, bool in_progress);

    const std::filesystem::path data_dir_;
    const Mode mode_;
    StoreId epoch_{};
    mutable std::mutex mutex_;
    std::uint64_t revision_ = 0U;
    std::size_t running_ = 0U;
    // What the record says: a statement may run, or ran after its revision.
    bool marked_ = false;
    // The revision the record holds; unknown after a failed write.
    std::optional<std::uint64_t> recorded_;
    bool holding_ = false;
};

}  // namespace chunkdb
