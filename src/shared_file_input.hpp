#pragma once

#include <filesystem>
#include <istream>
#include <memory>

namespace chunkdb {

// Pins one regular file while its name may be replaced or removed.
// Construction rejects symbolic links and reparse points at the leaf.
// Hard links and parent directory aliases are allowed.
class SharedFileInput final : public std::istream {
  public:
    explicit SharedFileInput(const std::filesystem::path& path);
#ifndef _WIN32
    // Open the leaf relative to an already admitted directory descriptor.
    SharedFileInput(const std::filesystem::path& path, int parent_fd);
#endif
    ~SharedFileInput() override;
    SharedFileInput(const SharedFileInput&) = delete;
    SharedFileInput& operator=(const SharedFileInput&) = delete;
    SharedFileInput(SharedFileInput&&) = delete;
    SharedFileInput& operator=(SharedFileInput&&) = delete;
    [[nodiscard]] bool is_open() const noexcept;
    void close() noexcept;

  private:
    class Buffer;
    std::unique_ptr<Buffer> buffer_;
};

}  // namespace chunkdb
