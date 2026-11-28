#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include "catalog_revision.hpp"
#include "chunkdb/change_feed.hpp"
#include "chunkdb/chunk_store.hpp"

// WATCH CATALOG (docs/reference/FEED.md#watch-catalog): the tables, users and
// slots of the server as records, and every change to them as a catalog push.
namespace chunkdb {

enum class CatalogKind : std::uint8_t { kTable = 0, kUser = 1, kSlot = 2 };

struct CatalogKey {
    CatalogKind kind = CatalogKind::kTable;
    // The table of a table or slot record; empty for a user record.
    std::string table;
    // The user or slot name; empty for a table record.
    std::string name;
    friend bool operator==(const CatalogKey&, const CatalogKey&) = default;
    friend bool operator<(const CatalogKey& a, const CatalogKey& b) {
        return std::tie(a.kind, a.table, a.name) < std::tie(b.kind, b.table, b.name);
    }
};

struct CatalogValue {
    // The encoded value a watcher with MANAGES USERS receives.
    std::string full;
    // The value other watchers receive; std::nullopt hides the record from them.
    std::optional<std::string> limited;
    friend bool operator==(const CatalogValue&, const CatalogValue&) = default;
};

// What a statement can change: these tables and their slots, or every table,
// and always the users.
struct CatalogScope {
    std::vector<std::string> tables;
    bool all_tables = false;
};

// The records of a scope as they are now.
struct CatalogState {
    std::map<CatalogKey, CatalogValue> records;
    // Tables whose record cannot be read now (fenced before it ever served):
    // their published record stays.
    std::set<std::string, std::less<>> unknown_tables;
    // Tables whose slots cannot be listed now: their published slots stay.
    std::set<std::string, std::less<>> unknown_slots;
};

class CatalogSubscription;

// One per server process. Revisions come from its CatalogClock: each catalog
// statement takes one when it ends, and a read of the whole catalog while a
// statement runs takes one, so one revision names one catalog state; with
// catalog_revision they keep their meaning across restarts. Without subscriptions the feed
// keeps nothing and Refresh returns at once. Tables a read cannot read without
// waiting keep their published records and are read again by the next Refresh.
class CatalogFeed : public std::enable_shared_from_this<CatalogFeed> {
  public:
    // Reads every user and the tables and slots of the scope.
    using Reader = std::function<CatalogState(const CatalogScope&)>;
    CatalogFeed(Reader reader, std::size_t buffer_bytes, std::unique_ptr<CatalogClock> clock);
    CatalogFeed(const CatalogFeed&) = delete;
    CatalogFeed& operator=(const CatalogFeed&) = delete;

    [[nodiscard]] const StoreId& epoch() const noexcept { return clock_->epoch(); }
    // Before a statement that can change the catalog runs. Throws when its
    // mark cannot be recorded: the statement must not run then.
    void Begin();
    [[nodiscard]] std::size_t buffer_bytes() const noexcept { return buffer_bytes_; }
    // Runs after a statement of `user` (empty: none) that Begin started, finished
    // or failed: takes its revision and publishes under it the records of
    // `scope` that differ from the published ones. Without a decision it only
    // releases the mark. A write or read failure ends watches with INTERNAL.
    void Refresh(const CatalogScope& scope, std::string_view user, bool decided = true) noexcept;
    // Starts at the current revision; the first push holds the whole catalog.
    // With `after` of this epoch at a revision the history still holds, or at
    // the current one, it starts there instead; with another `after` the first
    // pushes are resync and the whole catalog.
    // `users`: user records and slot owners are visible. `notify` runs, without
    // the feed's locks, when a push is ready; it must not block or throw.
    [[nodiscard]] std::shared_ptr<CatalogSubscription> Subscribe(bool users, std::function<void()> notify,
        std::optional<FeedPosition> after = std::nullopt);

  private:
    friend class CatalogSubscription;
    struct Entry {
        std::uint64_t revision = 0U;
        std::shared_ptr<const std::string> full;
        // Null when no record of the push is visible without MANAGES USERS.
        std::shared_ptr<const std::string> limited;
        std::size_t bytes = 0U;
    };
    struct Event {
        const CatalogKey* key = nullptr;
        std::string_view action;
        // Null for a drop.
        const std::string* data = nullptr;
    };
    [[nodiscard]] std::string Push(std::uint64_t revision, std::string_view user, const std::vector<Event>& events) const;
    // The whole catalog at revision_; cached until the next push. Under mutex_.
    [[nodiscard]] std::shared_ptr<const std::string> Whole(bool users);
    // Drops the published records and the history; under mutex_.
    void ClearLocked() noexcept;
    // Drops entries every subscription has read, then the oldest while the
    // history exceeds buffer_bytes_; under mutex_.
    void TrimLocked() noexcept;
    void Unsubscribe(CatalogSubscription* subscription) noexcept;
    [[nodiscard]] std::vector<std::function<void()>> NotifiesLocked() const;
    // Tables `state` could not read, for the next Refresh to read again.
    void RetryLocked(const CatalogState& state);

    const Reader reader_;
    const std::size_t buffer_bytes_;
    const std::unique_ptr<CatalogClock> clock_;
    // Serializes Refresh and Subscribe: each reads the state and publishes it
    // as one step, so a later revision never shows an older state.
    std::mutex refresh_mutex_;
    // Under refresh_mutex_: tables the last read could not read.
    std::set<std::string, std::less<>> retry_;
    mutable std::mutex mutex_;
    std::set<CatalogSubscription*> subscriptions_;
    // The published records; valid while built_.
    bool built_ = false;
    std::map<CatalogKey, CatalogValue> records_;
    // The revision of the published records.
    std::uint64_t revision_ = 0U;
    // The last revision taken; above revision_ when later statements changed
    // nothing a watch can see. Watches receive it as progress.
    std::uint64_t known_ = 0U;
    std::deque<Entry> history_;
    std::size_t history_bytes_ = 0U;
    // Every entry up to this revision has left history_; entries can skip revisions.
    std::uint64_t dropped_through_ = 0U;
    std::uint64_t whole_revision_ = 0U;
    std::shared_ptr<const std::string> whole_full_, whole_limited_;
};

class CatalogSubscription {
  public:
    struct Frame {
        std::shared_ptr<const std::string> bytes;
        // The last frame: an error that ends the watch.
        bool close = false;
    };
    ~CatalogSubscription();
    CatalogSubscription(const CatalogSubscription&) = delete;
    CatalogSubscription& operator=(const CatalogSubscription&) = delete;

    [[nodiscard]] const StoreId& epoch() const noexcept { return feed_->epoch(); }
    // The revision the watch starts at.
    [[nodiscard]] std::uint64_t revision() const noexcept { return start_; }
    [[nodiscard]] std::size_t buffer_bytes() const noexcept { return feed_->buffer_bytes(); }
    // The next push, or std::nullopt until one is ready.
    [[nodiscard]] std::optional<Frame> Next();

  private:
    friend class CatalogFeed;
    CatalogSubscription(std::shared_ptr<CatalogFeed> feed, bool users, std::uint64_t start, std::function<void()> notify);

    const std::shared_ptr<CatalogFeed> feed_;
    const bool users_;
    const std::uint64_t start_;
    const std::function<void()> notify_;
    // Under the feed's mutex_.
    std::uint64_t next_ = 0U;
    bool whole_ = true;
    // AFTER could not be served: resync comes before the whole catalog.
    bool resync_ = false;
    // The last revision this watch was told: of a push or of progress.
    std::uint64_t told_ = 0U;
    bool failed_ = false;
    bool ended_ = false;
};

}  // namespace chunkdb
