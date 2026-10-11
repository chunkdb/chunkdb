#pragma once

#include <atomic>
#include <stdexcept>
#include <string_view>

namespace chunkdb {

struct MigrationTestHook {
    enum class Point { kBeforeAdmission, kPrepared, kAfterDecision, kBeforeTableExclusive, kBeforeUserUpdate, kBeforeCatalogAdmission, kBeforeNarrowingScan };
    virtual ~MigrationTestHook() = default;
    virtual void Run(Point point, std::string_view name) = 0;
};

inline constexpr std::string_view kMigrationRecoveryRequiredMessage =
    "migration outcome is unknown; restart the server before retrying the same named step";

class MigrationRecoveryRequiredError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// A published migration decision must finish before any participant can change
// again. An I/O failure therefore closes the catalog until writer recovery.
struct MigrationHealth {
    std::atomic<bool> failed{false};
    std::atomic<MigrationTestHook*> hook{nullptr};
    void Check() const {
        if (failed.load(std::memory_order_acquire))
            throw MigrationRecoveryRequiredError("migration completion failed; restart the server to recover");
    }
};

class MigrationConflictError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

}  // namespace chunkdb
