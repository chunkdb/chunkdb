#pragma once

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winioctl.h>
#endif

namespace chunkdb::test {

// A junction needs neither Developer Mode nor symbolic-link privileges.
// Both platforms exercise a directory alias that path checks must inspect.
inline void CreateDirectoryAlias(const std::filesystem::path& target, const std::filesystem::path& link) {
#ifdef _WIN32
    const auto native = std::filesystem::absolute(target).wstring();
    const auto substitute = L"\\??\\" + native;
    struct Header {
        DWORD tag;
        WORD data_length, reserved;
        WORD substitute_offset, substitute_length, print_offset, print_length;
    };
    static_assert(sizeof(Header) == 16U);
    const auto name_bytes = (substitute.size() + native.size() + 2U) * sizeof(wchar_t);
    const auto bytes = sizeof(Header) + name_bytes;
    if (bytes > MAXIMUM_REPARSE_DATA_BUFFER_SIZE) throw std::length_error("test junction target is too long");
    std::vector<DWORD> storage((bytes + sizeof(DWORD) - 1U) / sizeof(DWORD), 0U);
    const Header header{IO_REPARSE_TAG_MOUNT_POINT, static_cast<WORD>(bytes - 8U), 0U,
        0U, static_cast<WORD>(substitute.size() * sizeof(wchar_t)),
        static_cast<WORD>((substitute.size() + 1U) * sizeof(wchar_t)),
        static_cast<WORD>(native.size() * sizeof(wchar_t))};
    auto* data = reinterpret_cast<unsigned char*>(storage.data());
    std::memcpy(data, &header, sizeof(header));
    std::memcpy(data + sizeof(header), substitute.c_str(), (substitute.size() + 1U) * sizeof(wchar_t));
    std::memcpy(data + sizeof(header) + header.print_offset, native.c_str(), (native.size() + 1U) * sizeof(wchar_t));
    if (!std::filesystem::create_directory(link)) throw std::runtime_error("test junction path already exists");
    const auto handle = CreateFileW(link.c_str(), GENERIC_WRITE, 0U, nullptr, OPEN_EXISTING,
                                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        const auto error = GetLastError();
        std::filesystem::remove(link);
        throw std::system_error(static_cast<int>(error), std::system_category(), "open test junction");
    }
    DWORD returned = 0U;
    const auto success = DeviceIoControl(handle, FSCTL_SET_REPARSE_POINT, data, static_cast<DWORD>(bytes),
                                         nullptr, 0U, &returned, nullptr);
    const auto error = success ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!success) {
        std::filesystem::remove(link);
        throw std::system_error(static_cast<int>(error), std::system_category(), "create test junction");
    }
    const auto attributes = GetFileAttributesW(link.c_str());
    if (attributes == INVALID_FILE_ATTRIBUTES || (attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0U)
        throw std::runtime_error("test junction lacks reparse attributes");
#else
    std::filesystem::create_directory_symlink(target, link);
#endif
}

// Remove the alias before a generic temporary-directory cleanup can follow it.
class ScopedDirectoryAlias {
  public:
    ScopedDirectoryAlias(const std::filesystem::path& target, std::filesystem::path alias)
        : alias_(std::move(alias)) { CreateDirectoryAlias(target, alias_); }
    ScopedDirectoryAlias(const ScopedDirectoryAlias&) = delete;
    ScopedDirectoryAlias& operator=(const ScopedDirectoryAlias&) = delete;
    ~ScopedDirectoryAlias() {
        const auto error = Remove();
        if (error) {
            std::fprintf(stderr, "test directory alias cleanup failed: %s\n", error.message().c_str());
            std::abort();
        }
    }
    [[nodiscard]] std::error_code Remove() {
        if (alias_.empty()) return {};
        std::error_code error;
#ifdef _WIN32
        if (RemoveDirectoryW(alias_.c_str()) == 0) {
            const auto code = GetLastError();
            if (code != ERROR_FILE_NOT_FOUND && code != ERROR_PATH_NOT_FOUND)
                error = {static_cast<int>(code), std::system_category()};
        }
#else
        std::filesystem::remove(alias_, error);
#endif
        if (!error) alias_.clear();
        return error;
    }

  private:
    std::filesystem::path alias_;
};

}  // namespace chunkdb::test
