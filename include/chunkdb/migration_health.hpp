#pragma once

#include <atomic>
#include <stdexcept>
#include <string_view>

namespace chunkdb {

struct MigrationTestHook {
    enum class Point { kBeforeAdmission, kPrepared, kAfterDecision, kBeforeTableExclusive, kBeforeUserUpdate, kBeforeCatalogAdmission, kBeforeNarrowingScan, kBeforeTxnLeaseWait, kBeforeTxnCohortPublication };
    virtual ~MigrationTestHook() = default;
    virtual void Run(Point point, std::string_view name) = 0;
};

inline constexpr std::string_view kMigrationRecoveryRequiredMessage =
    "migration outcome is unknown; restart the server before retrying the same named step";

class MigrationRecoveryRequiredError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// The command failed after its durable decision. Subsequent health-fenced
// commands use the base recovery error because they were never admitted.
class MigrationOutcomeUnknownError : public MigrationRecoveryRequiredError {
  public:
    using MigrationRecoveryRequiredError::MigrationRecoveryRequiredError;
};

class FeatureRecoveryRequiredError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// A published migration decision must finish before any participant can change
// again. An I/O failure therefore closes the catalog until writer recovery.
struct MigrationHealth {
    std::atomic<bool> failed{false};
    // Consulted only after the existing failed fast-path load.
    std::atomic<bool> feature_failed{false};
    std::atomic<MigrationTestHook*> hook{nullptr};
    void Check() const {
        if (failed.load(std::memory_order_acquire)) {
            if (feature_failed.load(std::memory_order_acquire))
                throw FeatureRecoveryRequiredError("feature activation completion failed; restart the server to recover");
            throw MigrationRecoveryRequiredError("migration completion failed; restart the server to recover");
        }
    }
};

class MigrationConflictError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

}  // namespace chunkdb
