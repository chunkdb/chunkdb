#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace chunkdb {
class MaintenancePool;

class MaintenanceTask {
  private:
    friend class MaintenancePool;
    explicit MaintenanceTask(std::function<bool()> step) : step_(std::move(step)) {}
    std::function<bool()> step_;
    std::shared_ptr<MaintenanceTask> next_;
    std::exception_ptr failure_;
    bool queued_ = false;
    bool running_ = false;
    bool again_ = false;
};

// A callback completes one bounded turn and returns whether work remains.
// Callers close their own admission before Wait and retain callback owners.
class MaintenancePool {
  public:
    explicit MaintenancePool(std::size_t workers);
    ~MaintenancePool();
    MaintenancePool(const MaintenancePool&) = delete;
    MaintenancePool& operator=(const MaintenancePool&) = delete;

    [[nodiscard]] std::shared_ptr<MaintenanceTask> MakeTask(std::function<bool()> step);
    [[nodiscard]] bool Notify(const std::shared_ptr<MaintenanceTask>& task);
    [[nodiscard]] std::exception_ptr Wait(const std::shared_ptr<MaintenanceTask>& task);

  private:
    void Enqueue(const std::shared_ptr<MaintenanceTask>& task);
    void Run();
    std::mutex mutex_;
    std::condition_variable cv_;
    std::shared_ptr<MaintenanceTask> ready_;
    MaintenanceTask* tail_ = nullptr;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};
}  // namespace chunkdb
