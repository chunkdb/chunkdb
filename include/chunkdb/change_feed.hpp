#pragma once

#include <chrono>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <stdexcept>
#include <vector>

#include "chunkdb/schema.hpp"
#include "chunkdb/chunk_store.hpp"
#include "chunkdb/types.hpp"

namespace chunkdb {

class Table;
class ChangeFeed;
class FeedProjectionGroup;
inline constexpr std::string_view kFeedRecoveryRequiredMessage = "feed cleanup failed; run RECOVER TABLE or restart server before retrying";
class FeedRecoveryRequiredError : public std::runtime_error {
  public:
    FeedRecoveryRequiredError() : std::runtime_error(std::string(kFeedRecoveryRequiredMessage)) {}
};
inline constexpr std::size_t kDefaultFeedBufferBytes = 64ULL * 1024ULL * 1024ULL;
// The most chunk-coordinate areas one watch can give.
inline constexpr std::size_t kMaxFeedAreas = 64U;

struct FeedPosition {
    StoreId epoch{};
    std::uint64_t revision = 0;
    friend bool operator==(const FeedPosition&, const FeedPosition&) = default;
};

struct FeedArea {
    ChunkCoord first;
    ChunkCoord last;
    std::uint8_t dims = 2;
    friend bool operator==(const FeedArea&, const FeedArea&) = default;
};

struct FeedOptions {
    std::optional<FeedPosition> after{};
    // Empty for the whole table; otherwise a chunk in any of these areas is kept.
    std::vector<FeedArea> areas{};
    std::optional<std::vector<std::uint32_t>> column_ids{};
    bool no_before = false;
    bool chunks = false;
    // Set on the first subscription; subsequent subscriptions share this budget.
    std::optional<std::size_t> buffer_bytes{};
    std::chrono::milliseconds progress_interval{1000};
    std::optional<std::uint64_t> ack_timeout_ms{};
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
    std::optional<std::int64_t> z{};
    std::uint32_t local_z = 0;
};

struct FeedChunkForm {
    ChunkCoord chunk;
    std::uint64_t version = 0U;
    // Null denotes a committed deletion; bytes are the exact post-commit form.
    std::shared_ptr<const std::string> form;
};

struct FeedChunkChange {
    ChunkCoord chunk;
    std::optional<std::vector<ColumnValue>> before;
    std::optional<std::vector<ColumnValue>> after;
};

// An application message a write or transaction publishes in its commit
// (docs/design/MESSAGES.md): a channel name, [a-z_][a-z0-9_.]* of 1 to 63
// bytes, and at most kMaxFeedMessageBytes payload bytes.
struct FeedMessage {
    std::string channel;
    std::string payload;
    friend bool operator==(const FeedMessage&, const FeedMessage&) = default;
};
inline constexpr std::size_t kMaxFeedChannelBytes = 63U;
inline constexpr std::size_t kMaxFeedMessageBytes = 16384U;
// Whether `channel` is a valid channel name.
[[nodiscard]] bool IsValidFeedChannel(std::string_view channel) noexcept;

struct FeedEntry {
    enum class Kind { kChange, kChunks, kSchema, kResync, kProgress, kEnd };
    Kind kind = Kind::kChange;
    FeedPosition position;
    std::uint64_t commit_time_ms = 0;
    std::optional<std::string> user;
    std::uint64_t schema_version = 0;
    std::vector<FeedBlockChange> blocks;
    bool chunk_forms_complete = false;
    std::vector<FeedChunkForm> chunk_forms;
    std::vector<Column> columns;
    // Immutable RESP3 bytes, charged to the ring and shared by whole-area watches;
    // a filtered entry carries its own, shared by watches with the same filters.
    std::shared_ptr<const std::string> protocol_frame;
    std::uint8_t dims = 2U;
    std::vector<Column> chunk_columns;
    std::vector<FeedChunkChange> chunk_changes;
    // The messages the commit published, in kChange and kChunks entries; at
    // most one today.
    std::vector<FeedMessage> messages;
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
    [[nodiscard]] std::string ResyncFrame();
    // A drained sender cut; never passes an entry this reader has not consumed.
    [[nodiscard]] std::optional<FeedPosition> Progress();
    // nullptr if nothing is available before timeout. kEnd is delivered once.
    [[nodiscard]] std::shared_ptr<const FeedEntry> Next(
        std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

  private:
    friend class ChangeFeed;
    FeedSubscription(std::shared_ptr<ChangeFeed> feed, std::weak_ptr<Table> table, const FeedOptions& options);
    // Records a read through `revision` in the projection group.
    void Track(std::uint64_t revision);
    std::shared_ptr<ChangeFeed> feed_;
    std::weak_ptr<Table> table_;
    FeedPosition position_;
    std::vector<FeedArea> areas_;
    std::optional<std::vector<std::uint32_t>> column_ids_;
    bool no_before_ = false;
    bool chunks_ = false;
    // A slot watch's live reader, which projects each entry itself.
    bool slot_ = false;
    std::uint64_t reset_ = 0;
    bool invalid_ = false;
    bool ended_ = false;
    std::shared_ptr<const std::function<void()>> notify_;
    // Shared by subscriptions with the same filters; null without filters.
    std::shared_ptr<FeedProjectionGroup> group_;
    std::uint64_t group_position_ = 0;
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

// The message the writes of the current C++ write context publish; null for
// none. The engine scopes a single write, or a transaction's COMMIT, to the
// message of its statement. The message must outlive the scope; nested
// scopes restore the caller's.
class ScopedWriteMessage {
  public:
    explicit ScopedWriteMessage(const FeedMessage* message) noexcept;
    ~ScopedWriteMessage();
    ScopedWriteMessage(const ScopedWriteMessage&) = delete;
    ScopedWriteMessage& operator=(const ScopedWriteMessage&) = delete;
  private:
    const FeedMessage* previous_;
};

}  // namespace chunkdb
