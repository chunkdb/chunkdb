#include <atomic>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>

#include "backup.hpp"
#include "chunkdb/logging.hpp"
#include "feed_test_utils.hpp"
#include "file_permissions.hpp"
#include "test_utils.hpp"
#include "user_registry.hpp"

#ifdef _WIN32
#include <aclapi.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
using namespace chunkdb;

void AssertOwnerOnly(const std::filesystem::path& path) {
    const bool directory = std::filesystem::is_directory(path);
#ifdef _WIN32
    PSID owner = nullptr;
    PACL acl = nullptr;
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    auto name = path.wstring();
    assert(GetNamedSecurityInfoW(name.data(), SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
        &owner, nullptr, &acl, nullptr, &descriptor) == ERROR_SUCCESS);
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    assert(GetSecurityDescriptorControl(descriptor, &control, &revision));
    assert((control & SE_DACL_PROTECTED) != 0U && acl != nullptr && acl->AceCount == 1);
    void* raw_ace = nullptr;
    assert(GetAce(acl, 0, &raw_ace));
    const auto* ace = static_cast<const ACCESS_ALLOWED_ACE*>(raw_ace);
    assert(ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE);
    assert(ace->Header.AceFlags == (directory ? OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE : 0));
    assert(ace->Mask == FILE_ALL_ACCESS && EqualSid(owner, const_cast<DWORD*>(&ace->SidStart)));
    LocalFree(descriptor);
#else
    struct stat status {};
    assert(::lstat(path.c_str(), &status) == 0);
    assert((status.st_mode & 07777) == (directory ? 0700 : 0600));
#endif
}

void AssertTree(const std::filesystem::path& root) {
    AssertOwnerOnly(root);
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) AssertOwnerOnly(entry.path());
}

void TestCreatedStorageAndBackup() {
#ifndef _WIN32
    struct UmaskGuard {
        mode_t previous = ::umask(0);
        ~UmaskGuard() { ::umask(previous); }
    } umask_guard;
#endif
    test::ScopedTempDir temp("chunkdb-private-storage");
    const auto root = std::filesystem::canonical(temp.path());
    const auto source = root / "parents" / "source";
    const auto backup = root / "backups" / "snapshot";
    const auto restored = root / "restores" / "world";
    auto config = feed_test::Config(source);
    {
        TableCatalog catalog(config);
        auto table = feed_test::CreateDefault(catalog);
        (void)table->CreateFeedSlot("reader");
        {
            auto lease = table->Acquire();
            auto& store = lease->store();
            txn_test::WriteCounter(store, {0, 0}, 11);
            store.CheckpointForTests(0, 0);
            txn_test::WriteCounter(store, {1, 0}, 22);
        }
        (void)catalog.BackupTo(backup, {});
    }
    // Destruction joins the lock heartbeat before inspecting directory entries.
    AssertTree(source);
    AssertOwnerOnly(source.parent_path());
    AssertTree(backup);
    AssertOwnerOnly(backup.parent_path());
    bool saw_wal = false, saw_image = false, saw_slots = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(backup)) {
        saw_wal |= entry.path().extension() == ".wal";
        saw_image |= entry.path().extension() == ".chk";
        saw_slots |= entry.path().filename() == "chunkdb.slots";
    }
    assert(saw_wal && saw_image && saw_slots);
    RestoreBackup(backup, restored);
    AssertTree(restored);
    AssertOwnerOnly(restored.parent_path());
    TableCatalog catalog(feed_test::Config(restored));
    auto lease = catalog.Find("default")->Acquire();
    assert(txn_test::ReadCounter(lease->store(), {0, 0}) == 11);
    assert(txn_test::ReadCounter(lease->store(), {1, 0}) == 22);
}

void TestExistingUsersAreRestricted() {
    test::ScopedTempDir temp("chunkdb-private-users");
    const std::array<std::uint8_t, 32> secret{};
    {
        UserRegistry registry(temp.path(), std::make_pair(std::string("admin"),
            scram::MakeVerifier("secret", std::vector<std::uint8_t>(16, 1), scram::kMinIterations)), secret);
    }
    const auto path = temp.path() / kUsersFileName;
    const auto before = ReadUsersFile(temp.path());
    AssertOwnerOnly(path);
#ifdef _WIN32
    auto name = path.wstring();
    // A null DACL grants access to everyone; startup must remove that grant.
    assert(SetNamedSecurityInfoW(name.data(), SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
        nullptr, nullptr, nullptr, nullptr) == ERROR_SUCCESS);
#else
    assert(::chmod(path.c_str(), 0644) == 0);
#endif
    std::atomic<int> restrictions = 0;
    struct LogGuard {
        ~LogGuard() { ResetLogSinkForTests(); }
    } log_guard;
    SetLogSinkForTests([&](const std::string& message) {
        if (message.find("INFO") != std::string::npos &&
            message.find("restricted users file to owner-only access") != std::string::npos) ++restrictions;
    });
    UserRegistry reopened(temp.path(), std::nullopt, secret);
    AssertOwnerOnly(path);
    assert(ReadUsersFile(temp.path()) == before);
    assert(restrictions == 1);
    UserRegistry again(temp.path(), std::nullopt, secret);
    assert(restrictions == 1);
}

void TestUnsafePathIsRefused() {
#ifndef _WIN32
    test::ScopedTempDir temp("chunkdb-private-symlink");
    const auto target = temp.path() / "target";
    WriteUsersFile(temp.path(), Users{});
    std::filesystem::rename(temp.path() / kUsersFileName, target);
    assert(::chmod(target.c_str(), 0644) == 0);
    const auto alias = temp.path() / "alias";
    std::filesystem::create_symlink(target, alias);
    bool refused = false;
    try { (void)RestrictFileToOwner(alias); } catch (const std::system_error&) { refused = true; }
    assert(refused);
    struct stat status {};
    assert(::stat(target.c_str(), &status) == 0 && (status.st_mode & 0777) == 0644);
#endif
}

void TestConfiguredRootAlias() {
#ifndef _WIN32
    test::ScopedTempDir temp("chunkdb-private-root-alias");
    const auto real = temp.path() / "real";
    assert(CreateOwnerOnlyDirectory(real));
    const auto alias = temp.path() / "alias";
    std::filesystem::create_directory_symlink(real, alias);
    {
        TableCatalog catalog(feed_test::Config(alias));
        UserRegistry registry(alias, std::make_pair(std::string("admin"),
            scram::MakeVerifier("secret", std::vector<std::uint8_t>(16, 1), scram::kMinIterations)), {});
        auto lease = feed_test::CreateDefault(catalog)->Acquire();
        txn_test::WriteCounter(lease->store(), {0, 0}, 33);
    }
    AssertTree(real);
    assert(std::filesystem::is_symlink(alias));
#endif
}

void TestActualModeIsRequired() {
#ifndef _WIN32
    test::ScopedTempDir temp("chunkdb-private-mode-check");
    const auto file = temp.path() / "existing";
    const int fd = ::open(file.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    assert(fd >= 0 && ::fchmod(fd, 0644) == 0);
    bool refused = false;
    try { RequireOwnerOnlyDescriptor(fd, file); } catch (const std::runtime_error&) { refused = true; }
    assert(refused && ::close(fd) == 0);
    // Existing files are deliberately retained; newly created files must pass
    // the observed-mode check before append or atomic write can use them.
    const int existing = OpenOwnerOnlyFile(file, O_WRONLY | O_APPEND | O_NOFOLLOW | O_CLOEXEC);
    assert(existing >= 0 && ::close(existing) == 0);
    struct stat status {};
    assert(::stat(file.c_str(), &status) == 0 && (status.st_mode & 07777) == 0644);
    struct UmaskGuard {
        mode_t previous = ::umask(0777);
        ~UmaskGuard() { ::umask(previous); }
    } umask_guard;
    refused = false;
    const auto directory = temp.path() / "new-directory";
    try { (void)CreateOwnerOnlyDirectory(directory); } catch (const std::runtime_error&) { refused = true; }
    assert(refused && !std::filesystem::exists(directory));
    refused = false;
    const auto append = temp.path() / "new-append";
    try { (void)OpenOwnerOnlyFile(append, O_WRONLY | O_APPEND | O_NOFOLLOW | O_CLOEXEC); }
    catch (const std::runtime_error&) { refused = true; }
    assert(refused && !std::filesystem::exists(append));
    refused = false;
    try { WriteUsersFile(temp.path(), Users{}); } catch (const std::runtime_error&) { refused = true; }
    assert(refused && !std::filesystem::exists(temp.path() / kUsersFileName));
    assert(std::distance(std::filesystem::directory_iterator(temp.path()), {}) == 1);
#endif
}
}  // namespace

#ifndef _WIN32
// The read-only check accepts a mode without group or other bits and changes
// nothing; any group or other bit makes the file not owner-only.
void TestOwnerOnlyCheck() {
    test::ScopedTempDir temp("chunkdb-owner-only-check");
    const auto path = temp.path() / "file";
    { std::ofstream out(path); out << "x"; }
    for (const auto mode : {0400, 0600, 0700}) {
        assert(::chmod(path.c_str(), static_cast<mode_t>(mode)) == 0);
        assert(FileIsOwnerOnly(path));
    }
    for (const auto mode : {0640, 0604, 0610}) {
        assert(::chmod(path.c_str(), static_cast<mode_t>(mode)) == 0);
        assert(!FileIsOwnerOnly(path));
        struct stat status {};
        assert(::stat(path.c_str(), &status) == 0 && (status.st_mode & 07777) == static_cast<mode_t>(mode));
    }
    assert(::chmod(path.c_str(), 0600) == 0);
}
#endif

int main() {
    TestCreatedStorageAndBackup();
    TestExistingUsersAreRestricted();
#ifndef _WIN32
    TestOwnerOnlyCheck();
#endif
    TestUnsafePathIsRefused();
    TestConfiguredRootAlias();
    TestActualModeIsRequired();
}
