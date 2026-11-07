#pragma once

#include <filesystem>
#include <utility>
#include <vector>
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <memory>
#include <string_view>
#include <system_error>
#endif

namespace chunkdb::backup_detail {

#ifdef _WIN32
// The CRT directory API uses a MAX_PATH scratch buffer even for namespace
// paths; enumerate native names without changing the caller's pinned root.
class DirectoryEntry {
  public:
    DirectoryEntry() = default;
    DirectoryEntry(std::filesystem::path path, DWORD attributes)
        : path_(std::move(path)), attributes_(attributes) {}
    const std::filesystem::path& path() const noexcept { return path_; }
    std::filesystem::file_status symlink_status() const noexcept {
        using Type = std::filesystem::file_type;
        const auto type = (attributes_ & FILE_ATTRIBUTE_REPARSE_POINT) != 0U ? Type::symlink :
            (attributes_ & FILE_ATTRIBUTE_DIRECTORY) != 0U ? Type::directory :
            (attributes_ & FILE_ATTRIBUTE_DEVICE) != 0U ? Type::unknown : Type::regular;
        return std::filesystem::file_status(type);
    }
    bool is_directory() const noexcept { return std::filesystem::is_directory(symlink_status()); }
    bool is_regular_file() const noexcept { return std::filesystem::is_regular_file(symlink_status()); }
  private:
    std::filesystem::path path_;
    DWORD attributes_ = 0U;
};

class DirectoryEntries {
    struct State {
        struct CloseDirectory { void operator()(void* handle) const noexcept { CloseHandle(handle); } };
        explicit State(std::filesystem::path directory, bool no_follow) : root(std::move(directory)) {
            pinned.reset(CreateFileW(root.c_str(), FILE_READ_ATTRIBUTES | FILE_LIST_DIRECTORY,
                FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | (no_follow ? FILE_FLAG_OPEN_REPARSE_POINT : 0U), nullptr));
            if (pinned.get() == INVALID_HANDLE_VALUE) {
                pinned.release();
                throw std::filesystem::filesystem_error("cannot open backup directory", root,
                    std::error_code(static_cast<int>(GetLastError()), std::system_category()));
            }
            BY_HANDLE_FILE_INFORMATION information{};
            if (!GetFileInformationByHandle(pinned.get(), &information))
                throw std::filesystem::filesystem_error("cannot inspect backup directory", root,
                    std::error_code(static_cast<int>(GetLastError()), std::system_category()));
            if ((information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0U ||
                (no_follow && (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0U))
                throw std::filesystem::filesystem_error("backup entry is not a safe directory", root,
                    std::make_error_code(std::errc::not_a_directory));
            auto pattern = root.native();
            if (pattern.empty() || (pattern.back() != L'\\' && pattern.back() != L'/')) pattern += L'\\';
            pattern += L'*';
            search = FindFirstFileW(pattern.c_str(), &data);
            if (search == INVALID_HANDLE_VALUE) {
                const auto error = GetLastError();
                if (error == ERROR_FILE_NOT_FOUND) return;
                throw std::filesystem::filesystem_error("cannot list backup directory", root,
                    std::error_code(static_cast<int>(error), std::system_category()));
            }
        }
        ~State() { if (search != INVALID_HANDLE_VALUE) FindClose(search); }
        bool Next() {
            if (FindNextFileW(search, &data) != 0) return true;
            const auto error = GetLastError();
            if (error == ERROR_NO_MORE_FILES) return false;
            throw std::filesystem::filesystem_error("cannot continue backup directory listing", root,
                std::error_code(static_cast<int>(error), std::system_category()));
        }
        bool Select() {
            if (search == INVALID_HANDLE_VALUE) return false;
            while (std::wstring_view(data.cFileName) == L"." || std::wstring_view(data.cFileName) == L"..")
                if (!Next()) return false;
            current = DirectoryEntry(root / data.cFileName, data.dwFileAttributes);
            return true;
        }
        std::filesystem::path root;
        std::unique_ptr<void, CloseDirectory> pinned;
        HANDLE search = INVALID_HANDLE_VALUE;
        WIN32_FIND_DATAW data{};
        DirectoryEntry current;
    };
  public:
    DirectoryEntries() = default;
    explicit DirectoryEntries(const std::filesystem::path& root, bool no_follow = false)
        : state_(std::make_shared<State>(root, no_follow)) {
        if (!state_->Select()) state_.reset();
    }
    const DirectoryEntry& operator*() const noexcept { return state_->current; }
    const DirectoryEntry* operator->() const noexcept { return &state_->current; }
    DirectoryEntries& operator++() {
        if (!state_->Next() || !state_->Select()) state_.reset();
        return *this;
    }
    bool operator==(const DirectoryEntries& other) const noexcept { return state_ == other.state_; }
    DirectoryEntries begin() const { return *this; }
    DirectoryEntries end() const { return {}; }
  private:
    std::shared_ptr<State> state_;
};
#else
using DirectoryEntries = std::filesystem::directory_iterator;
#endif

inline DirectoryEntries OpenDirectory(const std::filesystem::path& root, bool no_follow) {
#ifdef _WIN32
    return DirectoryEntries(root, no_follow);
#else
    (void)no_follow;
    return DirectoryEntries(root);
#endif
}

// Callers inspect each entry before recursion, and links are never followed.
template <typename Visitor>
void WalkDirectory(const std::filesystem::path& root, const Visitor& visit) {
    struct Frame {
        explicit Frame(DirectoryEntries entries) : owner(entries), current(std::move(entries)) {}
        DirectoryEntries owner, current;
    };
    std::vector<Frame> stack;
    stack.emplace_back(OpenDirectory(root, false));
    while (!stack.empty()) {
        if (stack.back().current == DirectoryEntries{}) { stack.pop_back(); continue; }
        const auto entry = *stack.back().current;
        visit(entry);
        ++stack.back().current;
        if (std::filesystem::is_directory(entry.symlink_status()))
            stack.emplace_back(OpenDirectory(entry.path(), true));
    }
}

}  // namespace chunkdb::backup_detail
