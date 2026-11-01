#include "shared_file_input.hpp"
#include "test_utils.hpp"

#include <array>
#include <cassert>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winioctl.h>
#else
#include <sys/stat.h>
#endif

namespace {
using chunkdb::SharedFileInput;

void Save(const std::filesystem::path& path, const std::string& contents) {
    std::ofstream output(path, std::ios::binary);
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    output.close();
    assert(output.good());
}
std::string Read(SharedFileInput& input, std::size_t bytes) {
    std::string result(bytes, '\0');
    input.read(result.data(), static_cast<std::streamsize>(result.size()));
    result.resize(static_cast<std::size_t>(input.gcount()));
    return result;
}
template <typename F> void Reject(F action) {
    bool refused = false;
    try { action(); } catch (const std::exception&) { refused = true; }
    assert(refused);
}

// Junction creation does not require symbolic-link privileges on Windows.
void DirectoryAlias(const std::filesystem::path& target, const std::filesystem::path& alias) {
#ifdef _WIN32
    const auto native = std::filesystem::absolute(target).wstring();
    const auto substitute = L"\\??\\" + native;
    struct Header { DWORD tag; WORD length, reserved, sub_offset, sub_length, print_offset, print_length; };
    static_assert(sizeof(Header) == 16U);
    const auto bytes = sizeof(Header) + (substitute.size() + native.size() + 2U) * sizeof(wchar_t);
    assert(bytes <= MAXIMUM_REPARSE_DATA_BUFFER_SIZE);
    std::vector<DWORD> storage((bytes + sizeof(DWORD) - 1U) / sizeof(DWORD), 0U);
    const Header header{IO_REPARSE_TAG_MOUNT_POINT, static_cast<WORD>(bytes - 8U), 0U, 0U,
        static_cast<WORD>(substitute.size() * sizeof(wchar_t)),
        static_cast<WORD>((substitute.size() + 1U) * sizeof(wchar_t)),
        static_cast<WORD>(native.size() * sizeof(wchar_t))};
    auto* data = reinterpret_cast<unsigned char*>(storage.data());
    std::memcpy(data, &header, sizeof(header));
    std::memcpy(data + sizeof(header), substitute.c_str(), (substitute.size() + 1U) * sizeof(wchar_t));
    std::memcpy(data + sizeof(header) + header.print_offset, native.c_str(), (native.size() + 1U) * sizeof(wchar_t));
    assert(std::filesystem::create_directory(alias));
    const auto handle = CreateFileW(alias.c_str(), GENERIC_WRITE, 0U, nullptr, OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, nullptr);
    assert(handle != INVALID_HANDLE_VALUE);
    DWORD returned = 0U;
    const auto success = DeviceIoControl(handle, FSCTL_SET_REPARSE_POINT, data, static_cast<DWORD>(bytes),
        nullptr, 0U, &returned, nullptr);
    CloseHandle(handle);
    assert(success);
#else
    std::filesystem::create_directory_symlink(target, alias);
#endif
}

void SeekAndShortRead(const std::filesystem::path& directory) {
    const auto path = directory / "buffered";
    std::string bytes(33001U, '\0');
    for (std::size_t i = 0U; i < bytes.size(); ++i) bytes[i] = static_cast<char>(i % 251U);
    Save(path, bytes);
    SharedFileInput input(path);
    assert(input.is_open() && input.tellg() == 0);
    assert(Read(input, 13U) == bytes.substr(0U, 13U));
    assert(input.tellg() == 13);
    input.seekg(8190, std::ios::beg);
    assert(Read(input, 11U) == bytes.substr(8190U, 11U));
    input.seekg(-7, std::ios::cur);
    assert(input.tellg() == 8194);
    assert(Read(input, 12000U) == bytes.substr(8194U, 12000U));
    input.seekg(-9, std::ios::end);
    assert(Read(input, 100U) == bytes.substr(bytes.size() - 9U));
    assert(input.eof() && input.gcount() == 9);
    input.clear();
    input.seekg(std::streampos(1));
    assert(Read(input, 4U) == bytes.substr(1U, 4U));
    const auto unchanged = input.tellg();
    input.seekg(-1, std::ios::beg);
    assert(input.fail());
    input.clear();
    assert(input.tellg() == unchanged);
    input.seekg(std::numeric_limits<std::streamoff>::min(), std::ios::cur);
    assert(input.fail());
    input.clear();
    input.seekg(std::numeric_limits<std::streamoff>::max(), std::ios::cur);
    assert(input.fail());
    input.clear();
    assert(input.tellg() == unchanged);
    input.close();
    assert(!input.is_open());
    input.close();
    input.seekg(0);
    assert(input.fail());
    input.clear();
    assert(Read(input, 1U).empty() && input.eof());
}

void SparseSeek(const std::filesystem::path& directory) {
    const auto path = directory / "sparse";
    constexpr std::streamoff position = (std::streamoff{1} << 32U) + 3U;
    Save(path, {});
#ifdef _WIN32
    const auto handle = CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    assert(handle != INVALID_HANDLE_VALUE);
    DWORD returned = 0U;
    const auto sparse = DeviceIoControl(handle, FSCTL_SET_SPARSE, nullptr, 0U, nullptr, 0U, &returned, nullptr);
    CloseHandle(handle);
    assert(sparse);
#endif
    {
        std::fstream output(path, std::ios::binary | std::ios::in | std::ios::out);
        output.seekp(position);
        output.write("tail", 4);
        output.close();
        assert(output.good());
    }
    SharedFileInput input(path);
    input.seekg(position);
    assert(input.tellg() == position && Read(input, 4U) == "tail");
    input.seekg(-4, std::ios::end);
    assert(input.tellg() == position && Read(input, 4U) == "tail");
    input.seekg(20, std::ios::end);
    assert(input && Read(input, 1U).empty() && input.eof());
}

void PinnedReplacement(const std::filesystem::path& directory) {
    const auto path = directory / "live";
    const auto moved = directory / "moved";
    Save(path, "old contents");
    SharedFileInput old(path);
    assert(Read(old, 2U) == "ol");
    std::filesystem::rename(path, moved);
    Save(path, "replacement");
    SharedFileInput replacement(path);
    assert(Read(replacement, 11U) == "replacement");
    std::filesystem::remove(moved);
    old.seekg(0);
    assert(Read(old, 12U) == "old contents");
    // A writer can append while the reader keeps the same file open.
    {
        std::ofstream append(path, std::ios::binary | std::ios::app);
        append << '!';
        append.close();
        assert(append.good());
    }
    replacement.seekg(-1, std::ios::end);
    assert(Read(replacement, 1U) == "!");
    std::filesystem::remove(path);
    replacement.seekg(0);
    assert(Read(replacement, 12U) == "replacement!");
}

void PlainLeafAdmission(const std::filesystem::path& directory) {
    Reject([&] { SharedFileInput missing(directory / "missing"); });
    Reject([&] { SharedFileInput folder(directory); });
    const auto owned = directory / "owned";
    std::filesystem::create_directory(owned);
    Save(owned / "value", "owned");
    const auto alias = directory / "alias";
    DirectoryAlias(owned, alias);
    Reject([&] { SharedFileInput leaf_alias(alias); });
    SharedFileInput parent_alias(alias / "value");
    assert(Read(parent_alias, 5U) == "owned");
    parent_alias.close();
    std::filesystem::remove(alias);
#ifndef _WIN32
    const auto link = directory / "file-link";
    std::filesystem::create_symlink(owned / "value", link);
    Reject([&] { SharedFileInput leaf_link(link); });
    const auto fifo = directory / "fifo";
    assert(mkfifo(fifo.c_str(), 0600) == 0);
    Reject([&] { SharedFileInput pipe(fifo); });
#endif
}
}  // namespace

int main() {
    chunkdb::test::ScopedTempDir directory("chunkdb-shared-file-input");
    SeekAndShortRead(directory.path());
    SparseSeek(directory.path());
    PinnedReplacement(directory.path());
    PlainLeafAdmission(directory.path());
}
