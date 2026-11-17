#include "commit_owner_artifact_io.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "durability_io.hpp"
#include "commit_owner_artifact_identity.hpp"
#include "file_permissions.hpp"
#include "process_lock.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winternl.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#endif

namespace chunkdb {
namespace {
constexpr const char* kObjects = "chunkdb.groups.objects";
constexpr const char* kRoot = "chunkdb.groups.owner";
constexpr const char* kMarker = ".scope";
constexpr const char* kTemporary = ".owner-stage-";

[[noreturn]] void Damage(const char* reason) {
    throw std::runtime_error(std::string("commit owner artifact: ") + reason);
}
std::string Hex(std::span<const std::uint8_t> bytes) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result; result.reserve(bytes.size() * 2U);
    for (const auto byte : bytes) { result.push_back(digits[byte >> 4U]); result.push_back(digits[byte & 15U]); }
    return result;
}
CommitOwnerObjectRef Reference(CommitOwnerObjectKind kind, std::span<const std::uint8_t> bytes) {
    return {kind, bytes.size(), crypto::Sha256Of(bytes)};
}
void CheckReference(const CommitOwnerObjectRef& reference, CommitOwnerObjectKind kind) {
    if (reference.kind != kind ||
        (kind == CommitOwnerObjectKind::kRoot && reference.bytes != kCommitOwnerRootBytes) ||
        (kind == CommitOwnerObjectKind::kPage &&
            (reference.bytes < kCommitOwnerEmptyPageBytes || reference.bytes > kMaxCommitOwnerPageBytes)) ||
        std::all_of(reference.sha256.begin(), reference.sha256.end(), [](auto b) { return b == 0U; })) {
        Damage("invalid root or page reference");
    }
}

#ifdef _WIN32
[[noreturn]] void NativeError(const char* operation, DWORD code = GetLastError()) {
    throw std::system_error(static_cast<int>(code), std::system_category(), operation);
}
using NativeHandle = HANDLE;
const NativeHandle kInvalid = INVALID_HANDLE_VALUE;
struct NativeApi {
    using Create = NTSTATUS (NTAPI*)(PHANDLE, ACCESS_MASK, POBJECT_ATTRIBUTES, PIO_STATUS_BLOCK,
        PLARGE_INTEGER, ULONG, ULONG, ULONG, ULONG, PVOID, ULONG);
    using Set = NTSTATUS (NTAPI*)(HANDLE, PIO_STATUS_BLOCK, PVOID, ULONG, FILE_INFORMATION_CLASS);
    using Error = ULONG (WINAPI*)(NTSTATUS);
    Create create;
    Set set;
    Error error;
    NativeApi() {
        const auto module = GetModuleHandleW(L"ntdll.dll");
        if (module == nullptr) NativeError("find native owner I/O module");
        const auto create_address = GetProcAddress(module, "NtCreateFile");
        const auto set_address = GetProcAddress(module, "NtSetInformationFile");
        const auto error_address = GetProcAddress(module, "RtlNtStatusToDosError");
        if (!create_address || !set_address || !error_address) Damage("native relative I/O capability unavailable");
        static_assert(sizeof(create) == sizeof(create_address));
        static_assert(sizeof(set) == sizeof(set_address));
        static_assert(sizeof(error) == sizeof(error_address));
        std::memcpy(&create, &create_address, sizeof(create));
        std::memcpy(&set, &set_address, sizeof(set));
        std::memcpy(&error, &error_address, sizeof(error));
    }
};
const NativeApi& Api() { static const NativeApi api; return api; }
#else
[[noreturn]] void NativeError(const char* operation, int code = errno) {
    throw std::system_error(code, std::generic_category(), operation);
}
using NativeHandle = int;
constexpr NativeHandle kInvalid = -1;
#endif

class Native {
  public:
    Native() = default;
    explicit Native(NativeHandle handle) noexcept : handle_(handle) {}
    ~Native() { Close(); }
    Native(const Native&) = delete;
    Native& operator=(const Native&) = delete;
    Native(Native&& other) noexcept : handle_(std::exchange(other.handle_, kInvalid)) {}
    Native& operator=(Native&& other) noexcept {
        if (this != &other) { Close(); handle_ = std::exchange(other.handle_, kInvalid); }
        return *this;
    }
    explicit operator bool() const noexcept { return handle_ != kInvalid; }
    NativeHandle get() const noexcept { return handle_; }
    void CloseChecked() {
        const auto old = std::exchange(handle_, kInvalid);
        if (old == kInvalid) return;
#ifdef _WIN32
        if (!CloseHandle(old)) NativeError("close owner object");
#else
        if (::close(old) != 0) NativeError("close owner object");
#endif
    }
  private:
    void Close() noexcept {
        if (handle_ == kInvalid) return;
#ifdef _WIN32
        (void)CloseHandle(handle_);
#else
        (void)::close(handle_);
#endif
        handle_ = kInvalid;
    }
    NativeHandle handle_ = kInvalid;
};

void CheckType(NativeHandle handle, bool directory) {
#ifdef _WIN32
    BY_HANDLE_FILE_INFORMATION information{};
    if (!GetFileInformationByHandle(handle, &information)) NativeError("inspect owner native object");
    if ((information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U ||
        ((information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0U) != directory ||
        GetFileType(handle) != FILE_TYPE_DISK) Damage("nonregular or redirected native object");
#else
    struct stat information{};
    if (::fstat(handle, &information) != 0) NativeError("inspect owner native object");
    if (!(directory ? S_ISDIR(information.st_mode) : S_ISREG(information.st_mode)))
        Damage("nonregular native object");
#endif
}
bool SameObject(NativeHandle left, NativeHandle right) {
#ifdef _WIN32
    return ReadCommitOwnerNativeIdentity(left) == ReadCommitOwnerNativeIdentity(right);
#else
    struct stat a{}, b{};
    if (::fstat(left, &a) != 0 || ::fstat(right, &b) != 0) NativeError("compare owner native objects");
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino;
#endif
}
Native OpenRoot(const std::filesystem::path& path, bool writable) {
#ifdef _WIN32
    (void)Api();
    Native result(CreateFileW(path.c_str(), GENERIC_READ | (writable ? GENERIC_WRITE : 0U),
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, nullptr));
#else
    (void)writable;
    Native result(::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC));
#endif
    if (!result) NativeError("open owner root directory");
    CheckType(result.get(), true); return result;
}
// Every name supplied here is fixed or locally derived, never persisted text.
Native OpenChild(const Native& parent, const std::string& name, bool directory, bool writable,
    bool create, bool missing_ok, const std::filesystem::path& label, bool metadata_only = false) {
    if (name.empty() || name.find_first_of("/\\") != std::string::npos || name == "." || name == "..")
        Damage("invalid internal leaf name");
#ifdef _WIN32
    const auto& api = Api();
    const std::wstring wide(name.begin(), name.end());
    if (wide.size() > std::numeric_limits<USHORT>::max() / sizeof(wchar_t)) Damage("oversized internal leaf");
    UNICODE_STRING leaf{};
    leaf.Length = static_cast<USHORT>(wide.size() * sizeof(wchar_t)); leaf.MaximumLength = leaf.Length;
    leaf.Buffer = const_cast<PWSTR>(wide.data());
    OBJECT_ATTRIBUTES attributes{}; attributes.Length = sizeof(attributes);
    attributes.RootDirectory = parent.get(); attributes.ObjectName = &leaf;
    attributes.Attributes = 0x40U;  // OBJ_CASE_INSENSITIVE.
    std::optional<OwnerOnlySecurity> security;
    if (create) { security.emplace(label, directory); attributes.SecurityDescriptor = security->attributes()->lpSecurityDescriptor; }
    ACCESS_MASK access = SYNCHRONIZE | FILE_READ_ATTRIBUTES;
    if (!metadata_only) access |= GENERIC_READ;
    if (writable) access |= GENERIC_WRITE;
    if (create && !directory) access |= DELETE;
    const ULONG share = FILE_SHARE_READ | FILE_SHARE_WRITE | (directory ? 0U : FILE_SHARE_DELETE);
    constexpr ULONG open_reparse = 0x00200000U, synchronous = 0x20U;
    HANDLE handle = INVALID_HANDLE_VALUE; IO_STATUS_BLOCK status{};
    const auto result = api.create(&handle, access, &attributes, &status, nullptr, FILE_ATTRIBUTE_NORMAL,
        share, create ? 2U : 1U, open_reparse | synchronous | (directory ? 1U : 0x40U), nullptr, 0U);
    if (result < 0) {
        const auto error = api.error(result);
        if (missing_ok && error == ERROR_FILE_NOT_FOUND) return {};
        NativeError("open relative owner object", error);
    }
    Native file(handle);
#else
    (void)metadata_only;
    int flags = (writable && !directory ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK;
    if (directory) flags |= O_DIRECTORY;
    if (create && directory) {
        if (::mkdirat(parent.get(), name.c_str(), 0700) != 0) NativeError("create relative owner directory");
    } else if (create) flags |= O_CREAT | O_EXCL;
    Native file(::openat(parent.get(), name.c_str(), flags, 0600));
    if (!file) {
        if (missing_ok && errno == ENOENT) return {};
        NativeError("open relative owner object");
    }
    if (create) RequireOwnerOnlyDescriptor(file.get(), label, directory);
#endif
    CheckType(file.get(), directory); return file;
}
void CheckCreationCapability(const Native& directory) {
#ifdef _WIN32
    DWORD flags = 0U;
    if (!GetVolumeInformationByHandleW(directory.get(), nullptr, 0U, nullptr, nullptr, &flags, nullptr, 0U))
        NativeError("inspect owner directory volume");
    if ((flags & FILE_PERSISTENT_ACLS) == 0U) Damage("owner-only native ACLs unavailable");
    (void)Api();
#else
    (void)directory;
#endif
}
std::uint64_t Extent(const Native& file) {
#ifdef _WIN32
    LARGE_INTEGER size{};
    if (!GetFileSizeEx(file.get(), &size)) NativeError("measure owner object");
    if (size.QuadPart < 0) Damage("negative object extent");
    return static_cast<std::uint64_t>(size.QuadPart);
#else
    struct stat status{};
    if (::fstat(file.get(), &status) != 0) NativeError("measure owner object");
    if (status.st_size < 0) Damage("negative object extent");
    return static_cast<std::uint64_t>(status.st_size);
#endif
}
void ReadAt(const Native& file, std::uint64_t offset, std::span<std::uint8_t> bytes) {
    while (!bytes.empty()) {
#ifdef _WIN32
        LARGE_INTEGER at{}; at.QuadPart = static_cast<LONGLONG>(offset);
        if (!SetFilePointerEx(file.get(), at, nullptr, FILE_BEGIN)) NativeError("seek owner object");
        DWORD read = 0U;
        if (!ReadFile(file.get(), bytes.data(), static_cast<DWORD>(bytes.size()), &read, nullptr))
            NativeError("read owner object");
        const auto amount = static_cast<std::size_t>(read);
#else
        const auto read = ::pread(file.get(), bytes.data(), bytes.size(), static_cast<off_t>(offset));
        if (read < 0) { if (errno == EINTR) continue; NativeError("read owner object"); }
        const auto amount = static_cast<std::size_t>(read);
#endif
        if (amount == 0U) Damage("short object read");
        offset += amount; bytes = bytes.subspan(amount);
    }
}
void WriteAll(const Native& file, std::span<const std::uint8_t> bytes) {
    while (!bytes.empty()) {
#ifdef _WIN32
        DWORD written = 0U;
        if (!WriteFile(file.get(), bytes.data(), static_cast<DWORD>(bytes.size()), &written, nullptr))
            NativeError("write owner object");
        const auto amount = static_cast<std::size_t>(written);
#else
        const auto written = ::write(file.get(), bytes.data(), bytes.size());
        if (written < 0) { if (errno == EINTR) continue; NativeError("write owner object"); }
        const auto amount = static_cast<std::size_t>(written);
#endif
        if (amount == 0U) Damage("zero object write");
        bytes = bytes.subspan(amount);
    }
}
void SyncFile(const Native& file, const std::filesystem::path& label) {
#ifdef _WIN32
    SyncOwnerFileHandle(file.get(), label);
#else
    SyncOwnerFileDescriptor(file.get(), label);
#endif
}
void SyncDirectory(const Native& directory, const std::filesystem::path& label) {
#ifdef _WIN32
    SyncOwnerDirectoryHandle(directory.get(), label);
#else
    SyncOwnerDirectoryDescriptor(directory.get(), label);
#endif
}
bool Publish(const Native& directory, const Native& source, const std::string& temporary, const std::string& leaf,
    const std::filesystem::path& label) {
#ifdef _WIN32
    (void)temporary;
    SyncFile(source, label);
    struct Rename { BOOLEAN replace; HANDLE root; ULONG bytes; WCHAR name[1]; };
    const std::wstring wide(leaf.begin(), leaf.end());
    std::vector<std::uint8_t> storage(offsetof(Rename, name) + wide.size() * sizeof(WCHAR));
    auto* rename = reinterpret_cast<Rename*>(storage.data());
    rename->replace = FALSE; rename->root = directory.get();
    rename->bytes = static_cast<ULONG>(wide.size() * sizeof(WCHAR));
    std::memcpy(rename->name, wide.data(), rename->bytes);
    IO_STATUS_BLOCK status{};
    const auto result = Api().set(source.get(), &status, rename, static_cast<ULONG>(storage.size()),
        static_cast<FILE_INFORMATION_CLASS>(10));
    if (result >= 0) return true;
    const auto error = Api().error(result);
    if (error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS) return false;
    NativeError("publish relative owner object", error);
#else
    const auto named = OpenChild(directory, temporary, false, false, false, false, label);
    if (!SameObject(named.get(), source.get())) Damage("temporary native generation changed");
    SyncFile(source, label);
#if defined(__APPLE__)
    if (::renameatx_np(directory.get(), temporary.c_str(), directory.get(), leaf.c_str(), RENAME_EXCL) == 0) return true;
    if (errno == EEXIST) return false;
    if (errno != ENOTSUP && errno != EINVAL && errno != ENOSYS) NativeError("publish relative owner object");
#elif defined(__linux__) && defined(SYS_renameat2)
    if (::syscall(SYS_renameat2, directory.get(), temporary.c_str(), directory.get(), leaf.c_str(), 1U) == 0) return true;
    if (errno == EEXIST) return false;
    if (errno != ENOTSUP && errno != EINVAL && errno != ENOSYS) NativeError("publish relative owner object");
#endif
    // Retain the private source link; unlink-after-identity-check is not atomic.
    if (::linkat(directory.get(), temporary.c_str(), directory.get(), leaf.c_str(), 0) == 0) return true;
    if (errno == EEXIST) return false;
    NativeError("link relative owner object");
#endif
}

// A markerless namespace may contain only unpublished private marker names.
void CheckUninitializedDirectory(const Native& directory) {
    const auto allowed = [](std::string_view name) {
        return name == "." || name == ".." || name.starts_with(".owner-marker-");
    };
#ifdef _WIN32
    alignas(8) std::array<std::uint8_t, 64U * 1024U> bytes{};
    bool first = true;
    for (;;) {
        if (!GetFileInformationByHandleEx(directory.get(), first ? FileIdBothDirectoryRestartInfo : FileIdBothDirectoryInfo,
                bytes.data(), static_cast<DWORD>(bytes.size()))) {
            const auto error = GetLastError();
            if (error == ERROR_NO_MORE_FILES) return;
            NativeError("enumerate pinned owner namespace", error);
        }
        first = false;
        std::size_t at = 0U;
        for (;;) {
            constexpr auto fixed = offsetof(FILE_ID_BOTH_DIR_INFO, FileName);
            if (at > bytes.size() - fixed) Damage("invalid native directory entry");
            const auto* entry = reinterpret_cast<const FILE_ID_BOTH_DIR_INFO*>(bytes.data() + at);
            const auto length = static_cast<std::size_t>(entry->FileNameLength);
            if (length % sizeof(WCHAR) != 0U || length > bytes.size() - at - fixed)
                Damage("invalid native directory name extent");
            std::string name;
            for (std::size_t i = 0U; i < length / sizeof(WCHAR); ++i) {
                if (entry->FileName[i] > 127U) Damage("foreign entry in markerless namespace");
                name.push_back(static_cast<char>(entry->FileName[i]));
            }
            if (!allowed(name)) Damage("foreign entry in markerless namespace");
            if (entry->NextEntryOffset == 0U) break;
            const auto next = static_cast<std::size_t>(entry->NextEntryOffset);
            if (next % 8U != 0U || next < fixed + length || next > bytes.size() - at - fixed)
                Damage("invalid native directory continuation");
            at += next;
        }
    }
#else
    const auto duplicate = ::fcntl(directory.get(), F_DUPFD_CLOEXEC, 0);
    if (duplicate < 0) NativeError("duplicate owner directory for enumeration");
    DIR* stream = ::fdopendir(duplicate);
    if (!stream) { const auto error = errno; (void)::close(duplicate); NativeError("enumerate owner directory", error); }
    struct CloseDirectory { void operator()(DIR* value) const noexcept { (void)::closedir(value); } };
    const std::unique_ptr<DIR, CloseDirectory> owned(stream);
    for (;;) {
        errno = 0;
        const auto* entry = ::readdir(stream);
        if (!entry) { if (errno != 0) NativeError("read owner directory"); return; }
        if (!allowed(entry->d_name)) Damage("foreign entry in markerless namespace");
    }
#endif
}

std::vector<std::uint8_t> ReadChecked(const Native& file, const CommitOwnerScope& scope,
    CommitOwnerObjectKind kind, const std::optional<CommitOwnerObjectRef>& reference = {},
    std::optional<std::uint64_t> maximum_generation = {}) {
    const auto extent = Extent(file);
    if ((kind == CommitOwnerObjectKind::kRoot && extent != kCommitOwnerRootBytes) ||
        (kind == CommitOwnerObjectKind::kPage &&
            (extent < kCommitOwnerEmptyPageBytes || extent > kMaxCommitOwnerPageBytes)) ||
        (reference && extent != reference->bytes)) Damage("object measured extent mismatch");
    std::array<std::uint8_t, kCommitOwnerHeaderBytes> header{}; ReadAt(file, 0U, header);
    const auto admitted = InspectCommitOwnerHeader(header, scope, kind, maximum_generation);
    if (admitted.bytes != extent) Damage("object declared extent mismatch");
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(extent));
    std::copy(header.begin(), header.end(), bytes.begin());
    ReadAt(file, header.size(), std::span(bytes).subspan(header.size()));
    if (Extent(file) != extent) Damage("object extent changed during read");
    if (reference && crypto::Sha256Of(bytes) != reference->sha256) Damage("object raw digest mismatch");
    return bytes;
}
}  // namespace

struct CommitOwnerArtifactIo::Impl {
    std::filesystem::path directory;
    CommitOwnerScope scope;
    Native root;
    bool writable = false;
    Native objects;

    Impl(const std::filesystem::path& path, const CommitOwnerScope& expected, bool write)
        : directory(path), scope(expected), root(OpenRoot(path, write)), writable(write) {
        (void)SerializeCommitOwnerScope(scope);
    }
    Native ObjectDirectory() const {
        auto opened = OpenChild(root, kObjects, true, writable, false, false, directory / kObjects);
        auto marker = OpenChild(opened, kMarker, false, false, false, false, directory / kObjects / kMarker);
        const auto bytes = ReadChecked(marker, scope, CommitOwnerObjectKind::kRoot);
        CommitOwnerRoot inactive; inactive.scope = scope;
        const auto expected = SerializeCommitOwnerRoot(inactive);
        if (!std::equal(bytes.begin(), bytes.end(), expected.begin(), expected.end())) Damage("namespace scope marker mismatch");
        return opened;
    }
    void EnsureNamespace() {
        if (objects) return;
        CheckCreationCapability(root);
        auto opened = OpenChild(root, kObjects, true, true, false, true, directory / kObjects);
        if (!opened) opened = OpenChild(root, kObjects, true, true, true, false, directory / kObjects);
        SyncDirectory(root, directory);
        CheckCreationCapability(opened);
        auto marker = OpenChild(opened, kMarker, false, true, false, true, directory / kObjects / kMarker);
        CommitOwnerRoot inactive; inactive.scope = scope;
        const auto bytes = SerializeCommitOwnerRoot(inactive);
        if (!marker) {
            CheckUninitializedDirectory(opened);
            const auto temporary = std::string(".owner-marker-") + Hex(crypto::RandomBytes(16U));
            auto source = OpenChild(opened, temporary, false, true, true, false, directory / kObjects / temporary);
            WriteAll(source, bytes);
            const auto installed = Publish(opened, source, temporary, kMarker, directory / kObjects / temporary);
            marker = OpenChild(opened, kMarker, false, true, false, false, directory / kObjects / kMarker);
            if (installed && !SameObject(source.get(), marker.get())) Damage("namespace marker generation substituted");
        }
        const auto actual = ReadChecked(marker, scope, CommitOwnerObjectKind::kRoot);
        if (!std::equal(actual.begin(), actual.end(), bytes.begin(), bytes.end())) Damage("namespace scope marker mismatch");
        SyncFile(marker, directory / kObjects / kMarker);
        SyncDirectory(opened, directory / kObjects);
        objects = std::move(opened);
    }
    CommitOwnerStagedArtifact Stage(CommitOwnerObjectKind kind, std::span<const std::uint8_t> bytes) {
        if (!writable) Damage("read-only artifact view cannot stage objects");
        const auto reference = Reference(kind, bytes);
        EnsureNamespace();
        const auto leaf = Hex(reference.sha256);
        auto existing = OpenChild(objects, leaf, false, true, false, true, directory / kObjects / leaf);
        bool installed = false;
        if (!existing) {
            const auto temporary = std::string(kTemporary) + Hex(crypto::RandomBytes(16U));
            auto source = OpenChild(objects, temporary, false, true, true, false, directory / kObjects / temporary);
            WriteAll(source, bytes);
            installed = Publish(objects, source, temporary, leaf, directory / kObjects / temporary);
            existing = OpenChild(objects, leaf, false, true, false, false, directory / kObjects / leaf);
            if (installed && !SameObject(source.get(), existing.get())) Damage("published native generation substituted");
            const auto actual = ReadChecked(existing, scope, kind, reference);
            if (!std::equal(actual.begin(), actual.end(), bytes.begin(), bytes.end())) Damage("published bytes mismatch");
            source.CloseChecked();
        } else {
            const auto actual = ReadChecked(existing, scope, kind, reference);
            if (!std::equal(actual.begin(), actual.end(), bytes.begin(), bytes.end())) Damage("existing object mismatch");
        }
        SyncFile(existing, directory / kObjects / leaf);
        existing.CloseChecked();
        SyncDirectory(objects, directory / kObjects);
        return {reference, installed};
    }
};

CommitOwnerArtifactIo::CommitOwnerArtifactIo(const std::filesystem::path& directory, const CommitOwnerScope& scope)
    : impl_(std::make_unique<Impl>(directory, scope, false)) {}

CommitOwnerArtifactIo::CommitOwnerArtifactIo(const std::filesystem::path& directory, const CommitOwnerScope& scope,
    const ProcessLock& lock, bool allow_multiple_processes) {
    if (allow_multiple_processes || std::filesystem::weakly_canonical(directory) != lock.data_dir_)
        Damage("incompatible exclusive writer scope");
    auto owned = std::make_unique<Impl>(directory, scope, true);
#ifdef _WIN32
    const auto original_root = reinterpret_cast<HANDLE>(lock.NativeRootForArtifacts());
#else
    const auto original_root = static_cast<int>(lock.NativeRootForArtifacts());
#endif
    if (original_root == kInvalid || !SameObject(owned->root.get(), original_root))
        Damage("exclusive writer root generation mismatch");
    auto lock_directory = OpenChild(owned->root, ".chunkdb.lock", true, false, false, false,
        directory / ".chunkdb.lock");
    auto lock_file = OpenChild(lock_directory, "writer.lock", false, false, false, false,
        directory / ".chunkdb.lock/writer.lock", true);
#ifdef _WIN32
    const auto native_lock = static_cast<HANDLE>(lock.lock_handle_);
#else
    const auto native_lock = lock.lock_fd_;
#endif
    if (native_lock == kInvalid || !SameObject(lock_file.get(), native_lock)) Damage("exclusive writer identity mismatch");
    CheckCreationCapability(owned->root);
    // Refuse an unsupported native directory durability contract before mkdir.
    SyncDirectory(owned->root, directory);
    impl_ = std::move(owned);
}
CommitOwnerArtifactIo::~CommitOwnerArtifactIo() = default;

std::optional<CommitOwnerRootArtifact> CommitOwnerArtifactIo::ReadCanonicalRoot() const {
    auto file = OpenChild(impl_->root, kRoot, false, false, false, true, impl_->directory / kRoot);
    if (!file) return {};
    const auto bytes = ReadChecked(file, impl_->scope, CommitOwnerObjectKind::kRoot);
    return CommitOwnerRootArtifact{Reference(CommitOwnerObjectKind::kRoot, bytes), ParseCommitOwnerRoot(bytes, impl_->scope)};
}
CommitOwnerRootArtifact CommitOwnerArtifactIo::ReadRoot(const CommitOwnerObjectRef& reference,
    std::optional<std::uint64_t> expected_generation) const {
    CheckReference(reference, CommitOwnerObjectKind::kRoot);
    const auto objects = impl_->ObjectDirectory();
    const auto file = OpenChild(objects, Hex(reference.sha256), false, false, false, false,
        impl_->directory / kObjects / Hex(reference.sha256));
    const auto bytes = ReadChecked(file, impl_->scope, reference.kind, reference, expected_generation);
    return {reference, ParseCommitOwnerRoot(bytes, impl_->scope, expected_generation)};
}
CommitOwnerPageArtifact CommitOwnerArtifactIo::ReadPage(const CommitOwnerObjectRef& reference,
    CommitOwnerTreeKind tree, std::uint64_t selecting_generation) const {
    CheckReference(reference, CommitOwnerObjectKind::kPage);
    const auto objects = impl_->ObjectDirectory();
    const auto file = OpenChild(objects, Hex(reference.sha256), false, false, false, false,
        impl_->directory / kObjects / Hex(reference.sha256));
    const auto bytes = ReadChecked(file, impl_->scope, reference.kind, reference, selecting_generation);
    return {reference, ParseCommitOwnerPage(bytes, impl_->scope, tree, selecting_generation)};
}
CommitOwnerStagedArtifact CommitOwnerArtifactIo::StageInactive() {
    CommitOwnerRoot inactive; inactive.scope = impl_->scope; return StageRoot(inactive);
}
CommitOwnerStagedArtifact CommitOwnerArtifactIo::StageRoot(const CommitOwnerRoot& value) {
    if (value.scope != impl_->scope) Damage("staged root scope mismatch");
    const auto bytes = SerializeCommitOwnerRoot(value);
    return impl_->Stage(CommitOwnerObjectKind::kRoot, bytes);
}
CommitOwnerStagedArtifact CommitOwnerArtifactIo::StagePage(const CommitOwnerPage& value) {
    if (value.scope != impl_->scope) Damage("staged page scope mismatch");
    const auto bytes = SerializeCommitOwnerPage(value);
    return impl_->Stage(CommitOwnerObjectKind::kPage, bytes);
}
}  // namespace chunkdb
