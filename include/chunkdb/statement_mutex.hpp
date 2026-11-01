#pragma once

#include <condition_variable>
#include <mutex>

namespace chunkdb {
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
