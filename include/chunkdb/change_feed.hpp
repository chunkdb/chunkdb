#pragma once

#include <chrono>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "chunkdb/schema.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

class Table;
class ChangeFeed;
inline constexpr std::size_t kDefaultFeedBufferBytes = 64ULL * 1024ULL * 1024ULL;

struct FeedPosition {
    StoreId epoch{};
    std::uint64_t revision = 0;
    friend bool operator==(const FeedPosition&, const FeedPosition&) = default;
};

struct FeedArea {
    ChunkCoord first;
    ChunkCoord last;
};

struct FeedOptions {
    std::optional<FeedPosition> after{};
    std::optional<FeedArea> area{};
    // Set on the first subscription; subsequent subscriptions share this budget.
    std::optional<std::size_t> buffer_bytes{};
    // Publication/control wake notification: must not block, throw or re-enter.
    // Runs on the sender or exclusive table control, never ordinary writers.
    std::function<void()> notify{};
};

struct FeedBlockChange {
    ChunkCoord chunk;
    // Absolute block coordinates, when they fit int64. Chunk operations
    // also address chunks beyond the absolute block coordinate domain.
    std::optional<std::int64_t> x{};
    std::optional<std::int64_t> y{};
    std::optional<std::vector<ColumnValue>> before;
    std::optional<std::vector<ColumnValue>> after;
    std::uint32_t local_x = 0;
    std::uint32_t local_y = 0;
};

struct FeedEntry {
    enum class Kind { kChange, kSchema, kResync, kEnd };
    Kind kind = Kind::kChange;
    FeedPosition position;
    std::uint64_t commit_time_ms = 0;
    std::optional<std::string> user;
    std::uint64_t schema_version = 0;
    std::vector<FeedBlockChange> blocks;
    std::vector<Column> columns;
    // Immutable RESP3 bytes, charged to the ring and shared by whole-area watches.
    std::shared_ptr<const std::string> protocol_frame;
};

// A subscription has one reader. Different subscriptions are independent.
// Entries are immutable; keeping one does not pin the feed's ring. On resync,
// re-read state and apply later changes only above each chunk's read version.
// Create/destroy subscriptions and stop a feed outside any Table::Lease.
class FeedSubscription {
  public:
    ~FeedSubscription();
    FeedSubscription(const FeedSubscription&) = delete;
    FeedSubscription& operator=(const FeedSubscription&) = delete;
    [[nodiscard]] FeedPosition position() const noexcept { return position_; }
    [[nodiscard]] std::size_t buffer_bytes() const noexcept;
    // Skip unread entries up to the completed frontier.
    [[nodiscard]] FeedPosition Resync();
    // nullptr if nothing is available before timeout. kEnd is delivered once.
    [[nodiscard]] std::shared_ptr<const FeedEntry> Next(
        std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

  private:
    friend class ChangeFeed;
    FeedSubscription(std::shared_ptr<ChangeFeed> feed, std::weak_ptr<Table> table, const FeedOptions& options);
    std::shared_ptr<ChangeFeed> feed_;
    std::weak_ptr<Table> table_;
    FeedPosition position_;
    std::optional<FeedArea> area_;
    std::uint64_t reset_ = 0;
    bool invalid_ = false;
    bool ended_ = false;
    std::shared_ptr<const std::function<void()>> notify_;
};

// Identity of the current C++ write context. The engine scopes each statement
// to its authenticated user; an empty user means anonymous. Views must outlive
// the scope. Nested scopes restore the caller's context; no store API changes.
class ScopedWriteUser {
  public:
    explicit ScopedWriteUser(std::string_view user) noexcept;
    ~ScopedWriteUser();
    ScopedWriteUser(const ScopedWriteUser&) = delete;
    ScopedWriteUser& operator=(const ScopedWriteUser&) = delete;
  private:
    std::string_view previous_;
};

}  // namespace chunkdb
