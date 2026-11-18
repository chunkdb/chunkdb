#include "user_registry.hpp"
#include "test_utils.hpp"
#include "txn_test_utils.hpp"

#include <atomic>
#include <barrier>
#include <cassert>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

namespace {
using namespace chunkdb;
using namespace std::chrono_literals;

scram::Verifier Verifier(std::string_view password = "secret") {
    return scram::MakeVerifier(std::string(password), std::vector<std::uint8_t>(16U, 7U), scram::kMinIterations);
}

std::shared_ptr<UserConnectionTicket> Connect(UserRegistry& users, const std::string& name) {
    const auto snapshot = users.AuthenticationSnapshot(name);
    assert(snapshot);
    auto admitted = users.AcquireConnection(*snapshot);
    assert(admitted.status == UserAdmissionStatus::kAllowed && admitted.ticket);
    return admitted.ticket;
}

void ConnectionCreditsAndIncarnations() {
    test::ScopedTempDir dir("chunkdb-user-credits");
    const auto verifier = Verifier();
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), verifier), {});
    users.Create("bot", verifier, false);
    users.Grant("bot", "world", Right::kWrite);
    users.SetLimits("bot", 2U, std::nullopt);
    auto first = Connect(users, "bot");
    auto second = Connect(users, "bot");
    const auto snapshot = *users.AuthenticationSnapshot("bot");
    assert(users.AcquireConnection(snapshot).status == UserAdmissionStatus::kConnectionLimit);
    users.SetLimits("bot", 1U, std::nullopt);
    first.reset();
    assert(users.AcquireConnection(snapshot).status == UserAdmissionStatus::kConnectionLimit);
    auto shared = second;
    second.reset();
    assert(users.AcquireConnection(snapshot).status == UserAdmissionStatus::kConnectionLimit);
    shared.reset();
    auto old = Connect(users, "bot");
    assert(users.FindForConnection("bot", old)->grants.at("world") == Right::kWrite);
    const auto incarnation = old->incarnation();
    users.Drop("bot");
    assert(!users.AuthenticationCurrent(snapshot));
    assert(snapshot.state->connections == 0U);
    users.Create("bot", verifier, false);
    assert(users.AcquireConnection(snapshot).status == UserAdmissionStatus::kStale);
    assert(!users.FindForConnection("bot", old));
    auto current = Connect(users, "bot");
    assert(current->incarnation() > incarnation);
    old.reset();
    assert(snapshot.state->connections == 0U);
}

void ConcurrentConnectionLimit() {
    test::ScopedTempDir dir("chunkdb-user-credit-race");
    const auto verifier = Verifier();
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), verifier), {});
    users.SetLimits("admin", 1U, std::nullopt);
    const auto snapshot = *users.AuthenticationSnapshot("admin");
    std::barrier start(9);
    std::vector<UserAdmissionResult> admissions(8);
    std::vector<std::thread> threads;
    for (std::size_t i = 0U; i < admissions.size(); ++i) threads.emplace_back([&, i] {
        start.arrive_and_wait();
        admissions[i] = users.AcquireConnection(snapshot);
    });
    start.arrive_and_wait();
    for (auto& thread : threads) thread.join();
    std::size_t admitted = 0U;
    for (const auto& result : admissions) admitted += result.status == UserAdmissionStatus::kAllowed;
    assert(admitted == 1U && snapshot.state->connections == 1U);
    admissions.clear();
    assert(snapshot.state->connections == 0U);
    assert(users.AcquireConnection(snapshot).status == UserAdmissionStatus::kAllowed);
}

void VerifierIdentityAndPublishedPair() {
    test::ScopedTempDir dir("chunkdb-user-verifier-identity");
    const auto original = Verifier();
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), original), {});
    const auto old = *users.AuthenticationSnapshot("admin");
    users.SetLimits("admin", 3U, 2U);
    users.Grant("admin", "world", Right::kRead);
    users.SetVerifier("admin", original);
    assert(users.AuthenticationCurrent(old));
    users.SetVerifier("admin", Verifier("replacement"));
    assert(!users.AuthenticationCurrent(old));
    const auto current = *users.AuthenticationSnapshot("admin");
    assert(current.verifier_generation > old.verifier_generation);
    // Reproduce the interval after runtime generation changes, before its
    // immutable users image is swapped into publication.
    {
        std::lock_guard lock(current.state->mutex);
        ++current.state->verifier_generation;
    }
    const auto during = *users.AuthenticationSnapshot("admin");
    assert(during.verifier_generation == current.verifier_generation);
    assert(during.user.verifier == current.user.verifier);
    assert(!users.AuthenticationCurrent(during));
    assert(users.AcquireConnection(during).status == UserAdmissionStatus::kStale);
}

void RollingRateWindowAndLowering() {
    test::ScopedTempDir dir("chunkdb-user-rate-window");
    std::atomic<std::int64_t> milliseconds{0};
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), Verifier()), {}, false,
        [&] { return UserAdmissionTime{} + std::chrono::milliseconds(milliseconds.load()); });
    users.SetLimits("admin", std::nullopt, 3U);
    auto ticket = Connect(users, "admin");
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    milliseconds.store(100);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    milliseconds.store(200);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    assert(users.AdmitStatement(ticket).retry_after_ms == 800U);
    milliseconds.store(300);
    users.SetLimits("admin", std::nullopt, 1U);
    auto denied = users.AdmitStatement(ticket);
    assert(denied.status == UserAdmissionStatus::kRateLimit && denied.retry_after_ms == 900U);
    milliseconds.store(1100);
    assert(users.AdmitStatement(ticket).retry_after_ms == 100U);
    milliseconds.store(1199);
    assert(users.AdmitStatement(ticket).retry_after_ms == 1U);
    milliseconds.store(1200);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    users.SetLimits("admin", std::nullopt, 5U);
    for (int i = 0; i < 4; ++i) assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kRateLimit);
    users.SetLimits("admin", std::nullopt, 0U);
    for (int i = 0; i < 10; ++i) assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    milliseconds.store(2200);
    users.SetLimits("admin", std::nullopt, 1U);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kRateLimit);
}

class PublicationGate : public MigrationTestHook {
  public:
    void Run(Point point, std::string_view) override {
        if (point != Point::kBeforeUserPublication) return;
        entered.set_value();
        release.get_future().wait();
    }
    std::promise<void> entered, release;
};

void GrowthRetainsAdmissionsDuringPublication() {
    test::ScopedTempDir dir("chunkdb-user-rate-growth");
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), Verifier()), {}, false,
        [] { return UserAdmissionTime{}; });
    users.SetLimits("admin", std::nullopt, 4U);
    auto ticket = Connect(users, "admin");
    for (int i = 0; i < 2; ++i) assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    PublicationGate gate;
    users.SetMigrationTestHook(&gate);
    auto update = std::async(std::launch::async, [&] { users.SetLimits("admin", std::nullopt, 8U); });
    assert(gate.entered.get_future().wait_for(5s) == std::future_status::ready);
    for (int i = 0; i < 2; ++i) assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kRateLimit);
    gate.release.set_value();
    assert(update.wait_for(5s) == std::future_status::ready);
    update.get();
    users.SetMigrationTestHook(nullptr);
    for (int i = 0; i < 4; ++i) assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kRateLimit);
}

void UnknownReplacementPublishesWholeImage() {
    test::ScopedTempDir dir("chunkdb-user-unknown-replacement");
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), Verifier()), {});
    const auto before = users.Snapshot();
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_ATOMICWRITE_AFTER_RENAME_BEFORE_DIR_SYNC_ONCE", "1");
        bool unknown = false;
        try { users.SetLimits("admin", 1U, 7U); }
        catch (const UsersOutcomeUnknownError&) { unknown = true; }
        assert(unknown);
    }
    assert(users.Snapshot() != before);
    const auto stored = ReadUsersFile(dir.path());
    assert(stored && *stored == users.Snapshot());
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_FULL_SYNC_DIRECTORY_FAIL_ONCE", "1");
        bool unknown = false;
        try { users.SetLimits("admin", 1U, 7U); }
        catch (const UsersOutcomeUnknownError&) { unknown = true; }
        assert(unknown); // An unchanged retry must confirm the outstanding rename.
    }
    users.SetLimits("admin", 1U, 7U);
    UserRegistry reopened(dir.path(), std::nullopt, {});
    assert(reopened.Snapshot() == users.Snapshot());
    auto ticket = Connect(users, "admin");
    assert(users.AcquireConnection(*users.AuthenticationSnapshot("admin")).status == UserAdmissionStatus::kConnectionLimit);
    const auto published = users.Snapshot();
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_ATOMICWRITE_TEMP_WRITE_FAIL_ONCE", "1");
        bool refused = false;
        try { users.SetLimits("admin", 2U, 8U); }
        catch (const std::exception&) { refused = true; }
        assert(refused);
    }
    assert(users.Snapshot() == published && ReadUsersFile(dir.path()) == published);
}

void HistoryAllocationPrecedesPublication() {
    test::ScopedTempDir dir("chunkdb-user-rate-allocation");
    UserRegistry users(dir.path(), std::make_pair(std::string("admin"), Verifier()), {}, false,
        [] { return UserAdmissionTime{}; });
    users.SetLimits("admin", std::nullopt, 1U);
    auto ticket = Connect(users, "admin");
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kAllowed);
    const auto before = users.Snapshot();
    {
        txn_test::ScopedEnv fail("CHUNKDB_FAILPOINT_USER_ADMISSION_HISTORY_ALLOC_FAIL_ONCE", "1");
        bool refused = false;
        try { users.SetLimits("admin", std::nullopt, 2U); }
        catch (const std::bad_alloc&) { refused = true; }
        assert(refused);
    }
    assert(users.Snapshot() == before && ReadUsersFile(dir.path()) == before);
    assert(users.AdmitStatement(ticket).status == UserAdmissionStatus::kRateLimit);
    bool refused = false;
    try { users.SetLimits("admin", std::nullopt, 65537U); }
    catch (const std::invalid_argument&) { refused = true; }
    assert(refused && users.Snapshot() == before && ReadUsersFile(dir.path()) == before);
}
}

int main(int argc, char** argv) {
    struct Case { const char* name; void (*run)(); };
    const Case cases[]{
        {"credits", ConnectionCreditsAndIncarnations}, {"race", ConcurrentConnectionLimit},
        {"verifier", VerifierIdentityAndPublishedPair}, {"rate", RollingRateWindowAndLowering},
        {"growth", GrowthRetainsAdmissionsDuringPublication}, {"unknown", UnknownReplacementPublishesWholeImage},
        {"allocation", HistoryAllocationPrecedesPublication},
    };
    bool selected = false;
    for (const auto& test : cases) if (argc == 1 || (argc == 2 && std::string_view(argv[1]) == test.name)) {
        selected = true;
        test.run();
    }
    return selected ? 0 : 2;
}
