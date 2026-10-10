#pragma once

#include <atomic>
#include <stdexcept>

namespace chunkdb {

// A published migration decision must finish before any participant can change
// again. An I/O failure therefore closes the catalog until writer recovery.
struct MigrationHealth {
    std::atomic<bool> failed{false};
    void Check() const {
        if (failed.load(std::memory_order_acquire))
            throw std::runtime_error("migration completion failed; restart the server to recover");
    }
};

class MigrationConflictError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

}  // namespace chunkdb
