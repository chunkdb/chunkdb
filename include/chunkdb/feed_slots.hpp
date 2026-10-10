#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include "chunkdb/change_feed.hpp"

namespace chunkdb {

class Table;
class FeedSlots;

inline constexpr std::size_t kDefaultSlotMaxBytes = 1024ULL * 1024ULL * 1024ULL;
inline constexpr std::chrono::milliseconds kDefaultSlotSyncInterval{100};

struct FeedSlot {
    std::string name;
    FeedPosition position;
    std::uint64_t durable_watermark = 0;
    std::uint64_t retained_bytes = 0;
    bool lost = false;
};

class FeedSlotBusyError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

class FeedSlotNotFoundError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

class FeedSlotLostError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

class FeedArchiveExpiredError : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// One reader, exact typed changes in revision order. Reading never repairs or
// trims storage. Revisions above the table's durable frontier are not returned.
// A read failure is terminal; open a fresh reader at the last returned position.
class FeedArchiveReader {
  public:
    ~FeedArchiveReader();
    FeedArchiveReader(FeedArchiveReader&&) noexcept;
    FeedArchiveReader& operator=(FeedArchiveReader&&) noexcept;
    FeedArchiveReader(const FeedArchiveReader&) = delete;
    FeedArchiveReader& operator=(const FeedArchiveReader&) = delete;
    [[nodiscard]] std::shared_ptr<const FeedEntry> Next();
    [[nodiscard]] FeedPosition position() const noexcept;
    [[nodiscard]] FeedPosition through() const noexcept;

  private:
    friend class Table;
    friend class FeedSlots;
    friend struct FeedArchiveAccess;
    struct Impl;
    explicit FeedArchiveReader(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace chunkdb
