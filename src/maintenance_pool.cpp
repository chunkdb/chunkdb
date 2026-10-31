#include "maintenance_pool.hpp"
#include "chunk_store_internal.hpp"

#include <stdexcept>
#include <system_error>

namespace chunkdb {
MaintenancePool::MaintenancePool(std::size_t workers) {
    if (workers == 0U) throw std::invalid_argument("maintenance_workers must be positive");
    workers_.reserve(workers);
#ifdef CHUNKDB_FAILPOINTS
    const bool fail_allocation = ConsumeFailpointEnv("CHUNKDB_FAILPOINT_MAINTENANCE_START_ALLOC_FAIL_ONCE");
#endif
    try {
        for (std::size_t i = 0U; i < workers; ++i) {
#ifdef CHUNKDB_FAILPOINTS
            if (i == 1U && fail_allocation) throw std::bad_alloc();
#endif
            workers_.emplace_back([this] { Run(); });
        }
    } catch (const std::exception&) {
        { std::lock_guard lock(mutex_); stopping_ = true; }
        cv_.notify_all();
        for (auto& worker : workers_) worker.join();
        throw;
    }
}

MaintenancePool::~MaintenancePool() {
    { std::lock_guard lock(mutex_); stopping_ = true; }
    cv_.notify_all();
    for (auto& worker : workers_) worker.join();
}

std::shared_ptr<MaintenanceTask> MaintenancePool::MakeTask(std::function<bool()> step) {
    return std::shared_ptr<MaintenanceTask>(new MaintenanceTask(std::move(step)));
}

void MaintenancePool::Enqueue(const std::shared_ptr<MaintenanceTask>& task) {
    task->queued_ = true;
    if (tail_) tail_->next_ = task;
    else ready_ = task;
    tail_ = task.get();
}

bool MaintenancePool::Notify(const std::shared_ptr<MaintenanceTask>& task) {
    {
        std::lock_guard lock(mutex_);
        if (stopping_ || task->failure_) return false;
        if (task->running_) task->again_ = true;
        else if (!task->queued_) Enqueue(task);
    }
    cv_.notify_all();
    return true;
}

std::exception_ptr MaintenancePool::Wait(const std::shared_ptr<MaintenanceTask>& task) {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [&] { return !task->queued_ && !task->running_; });
    return task->failure_;
}

void MaintenancePool::Run() {
    for (;;) {
        std::shared_ptr<MaintenanceTask> task;
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [&] { return stopping_ || ready_; });
            if (!ready_) return;
            task = std::move(ready_);
            ready_ = std::move(task->next_);
            if (!ready_) tail_ = nullptr;
            task->queued_ = false;
            task->running_ = true;
            task->again_ = false;
        }
        bool more = false;
        std::exception_ptr failure;
        try { more = task->step_(); }
        catch (const std::exception&) { failure = std::current_exception(); }
        {
            std::lock_guard lock(mutex_);
            task->running_ = false;
            task->failure_ = failure;
            if (!failure && (more || task->again_)) Enqueue(task);
        }
        cv_.notify_all();
    }
}
}  // namespace chunkdb
