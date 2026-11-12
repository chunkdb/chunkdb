#include "maintenance_pool.hpp"
#include "txn_test_utils.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;

void FairTurnsAndCoalescing() {
    MaintenancePool pool(1);
    std::promise<void> entered, release;
    const auto released = release.get_future().share();
    std::vector<int> order;
    int turns = 0;
    auto busy = pool.MakeTask([&] {
        order.push_back(1);
        if (++turns == 1) { entered.set_value(); released.wait(); }
        return turns < 3;
    });
    auto second = pool.MakeTask([&] { order.push_back(2); return false; });
    auto third = pool.MakeTask([&] { order.push_back(3); return false; });
    assert(pool.Notify(busy));
    assert(entered.get_future().wait_for(5s) == std::future_status::ready);
    assert(pool.Notify(second));
    assert(pool.Notify(third));
    for (int i = 0; i < 100; ++i) assert(pool.Notify(busy));
    release.set_value();
    assert(!pool.Wait(busy) && !pool.Wait(second) && !pool.Wait(third));
    assert((order == std::vector<int>{1, 2, 3, 1, 1}));
}

void NotificationDuringFinalTurn() {
    MaintenancePool pool(1);
    std::promise<void> entered, release;
    const auto released = release.get_future().share();
    int turns = 0;
    auto task = pool.MakeTask([&] {
        if (++turns == 1) { entered.set_value(); released.wait(); }
        return false;
    });
    assert(pool.Notify(task));
    assert(entered.get_future().wait_for(5s) == std::future_status::ready);
    assert(pool.Notify(task));
    release.set_value();
    assert(!pool.Wait(task) && turns == 2);
}

void ParallelWorkersAndFailure() {
    MaintenancePool pool(2);
    std::promise<void> first_entered, second_entered, release;
    const auto released = release.get_future().share();
    auto first = pool.MakeTask([&] { first_entered.set_value(); released.wait(); return false; });
    auto second = pool.MakeTask([&] { second_entered.set_value(); released.wait(); throw std::runtime_error("second task failed"); return false; });
    assert(pool.Notify(first) && pool.Notify(second));
    assert(first_entered.get_future().wait_for(5s) == std::future_status::ready);
    assert(second_entered.get_future().wait_for(5s) == std::future_status::ready);
    release.set_value();
    assert(!pool.Wait(first));
    const auto failure = pool.Wait(second);
    assert(failure);
    try { std::rethrow_exception(failure); assert(false); }
    catch (const std::runtime_error& error) { assert(std::string(error.what()) == "second task failed"); }
    assert(!pool.Notify(second));
    std::atomic<bool> completed{false};
    auto next = pool.MakeTask([&] { completed.store(true); return false; });
    assert(pool.Notify(next) && !pool.Wait(next) && completed.load());
}

void ShutdownDrainsAcceptedTurns() {
    std::atomic<int> turns{0};
    {
        MaintenancePool pool(1);
        auto task = pool.MakeTask([&] { return ++turns < 100; });
        assert(pool.Notify(task));
    }
    assert(turns.load() == 100);
    bool refused = false;
    try { MaintenancePool pool(0); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused);
}

void PartialStartupAllocationFailure() {
    txn_test::ScopedEnv failure("CHUNKDB_FAILPOINT_MAINTENANCE_START_ALLOC_FAIL_ONCE", "1");
    bool refused = false;
    try { MaintenancePool pool(2); }
    catch (const std::bad_alloc&) { refused = true; }
    assert(refused);
    // A failed startup leaves no worker using a destroyed pool.
    MaintenancePool next(1);
    auto work = next.MakeTask([] { return false; });
    assert(next.Notify(work) && !next.Wait(work));
}
}

int main() {
    FairTurnsAndCoalescing();
    NotificationDuringFinalTurn();
    ParallelWorkersAndFailure();
    ShutdownDrainsAcceptedTurns();
    PartialStartupAllocationFailure();
}
