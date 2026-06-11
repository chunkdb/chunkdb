#pragma once

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

namespace chunkdb::test {

class ScopedTempDir {
  public:
    explicit ScopedTempDir(const std::string& prefix) {
        const auto tick = static_cast<long long>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
                (prefix + "-" + std::to_string(tick));
        std::filesystem::create_directories(path_);
    }

    ScopedTempDir(const ScopedTempDir&) = delete;
    ScopedTempDir& operator=(const ScopedTempDir&) = delete;

    ScopedTempDir(ScopedTempDir&& other) noexcept
        : path_(std::move(other.path_)) {
        other.path_.clear();
    }

    ScopedTempDir& operator=(ScopedTempDir&& other) noexcept {
        if (this != &other) {
            Cleanup();
            path_ = std::move(other.path_);
            other.path_.clear();
        }
        return *this;
    }

    ~ScopedTempDir() { Cleanup(); }

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

  private:
    void Cleanup() noexcept {
        if (path_.empty()) {
            return;
        }
        for (int attempt = 0; attempt < 20; ++attempt) {
            std::error_code ec;
            std::filesystem::remove_all(path_, ec);
            if (!ec || !std::filesystem::exists(path_)) {
                path_.clear();
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }

    std::filesystem::path path_;
};

}  // namespace chunkdb::test
