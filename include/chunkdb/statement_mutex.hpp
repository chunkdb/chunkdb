#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace chunkdb {
// What a running statement reports done of its total work; SHOW CONNECTIONS
// lists it. RESTORE TABLE counts the files and bytes it copied.
struct StatementProgress {
    std::uint64_t files_done = 0U;
    std::uint64_t files_total = 0U;
    std::uint64_t bytes_done = 0U;
    std::uint64_t bytes_total = 0U;
};

// Serialized metadata admission wakes when its current statement is stopped.
class StatementMutex {
  public:
    void lock();
    void unlock() noexcept;
  private:
    std::mutex mutex_;
    std::condition_variable_any changed_;
    bool locked_ = false;
};
}  // namespace chunkdb
