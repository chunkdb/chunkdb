#include "catalog_feed.hpp"

#include <algorithm>
#include <chrono>
#include <new>
#include <utility>

#include "chunkdb/logging.hpp"
#include "chunkdb/protocol.hpp"
#include "store_manifest.hpp"

namespace chunkdb {
namespace {

[[nodiscard]] std::string_view KindName(CatalogKind kind) noexcept {
    switch (kind) {
        case CatalogKind::kTable: return "table";
        case CatalogKind::kUser: return "user";
        case CatalogKind::kSlot: return "slot";
    }
    return "?";
}

[[nodiscard]] std::uint64_t NowMs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

CatalogFeed::CatalogFeed(Reader reader, std::size_t buffer_bytes)
    : reader_(std::move(reader)), buffer_bytes_(buffer_bytes), epoch_(NewStoreId()) {}

std::string CatalogFeed::Push(std::uint64_t revision, std::string_view user, const std::vector<Event>& events) const {
    std::string out = ">7\r\n";
    Protocol::AppendBulk(out, "catalog");
    Protocol::AppendBulk(out, StoreIdHex(epoch_));
    Protocol::AppendInteger(out, revision);
    Protocol::AppendInteger(out, NowMs());
    if (user.empty()) Protocol::AppendNull(out);
    else Protocol::AppendBulk(out, user);
    Protocol::AppendArrayHeader(out, events.size());
    for (const auto& event : events) {
        Protocol::AppendMapHeader(out, 5U);
        Protocol::AppendBulk(out, "kind");
        Protocol::AppendBulk(out, KindName(event.key->kind));
        Protocol::AppendBulk(out, "action");
        Protocol::AppendBulk(out, event.action);
        Protocol::AppendBulk(out, "name");
        Protocol::AppendBulk(out, event.key->kind == CatalogKind::kTable ? event.key->table : event.key->name);
        Protocol::AppendBulk(out, "table");
        if (event.key->kind == CatalogKind::kSlot) Protocol::AppendBulk(out, event.key->table);
        else Protocol::AppendNull(out);
        Protocol::AppendBulk(out, "data");
        if (event.data != nullptr) out += *event.data;
        else Protocol::AppendNull(out);
    }
    // Catalog changes belong to no transaction.
    Protocol::AppendNull(out);
    return out;
}

std::shared_ptr<const std::string> CatalogFeed::Whole(bool users) {
    if (!whole_full_ || whole_revision_ != revision_) {
        std::vector<Event> full, limited;
        for (const auto& [key, value] : records_) {
            full.push_back(Event{.key = &key, .action = "create", .data = &value.full});
            if (value.limited) limited.push_back(Event{.key = &key, .action = "create", .data = &*value.limited});
        }
        whole_full_ = std::make_shared<const std::string>(Push(revision_, {}, full));
        whole_limited_ = std::make_shared<const std::string>(Push(revision_, {}, limited));
        whole_revision_ = revision_;
    }
    return users ? whole_full_ : whole_limited_;
}

void CatalogFeed::ClearLocked() noexcept {
    built_ = false;
    records_.clear();
    history_.clear();
    history_bytes_ = 0U;
    dropped_through_ = revision_;
    whole_full_.reset();
    whole_limited_.reset();
}

void CatalogFeed::TrimLocked() noexcept {
    std::uint64_t needed = revision_ + 1U;
    for (const auto* subscription : subscriptions_)
        if (!subscription->whole_ && !subscription->failed_) needed = std::min(needed, subscription->next_);
    while (!history_.empty() && (history_.front().revision < needed || history_bytes_ > buffer_bytes_)) {
        dropped_through_ = history_.front().revision;
        history_bytes_ -= history_.front().bytes;
        history_.pop_front();
    }
}

void CatalogFeed::RetryLocked(const CatalogState& state) {
    retry_.clear();
    retry_.insert(state.unknown_tables.begin(), state.unknown_tables.end());
    retry_.insert(state.unknown_slots.begin(), state.unknown_slots.end());
}

std::vector<std::function<void()>> CatalogFeed::NotifiesLocked() const {
    std::vector<std::function<void()>> notifies;
    notifies.reserve(subscriptions_.size());
    for (const auto* subscription : subscriptions_)
        if (subscription->notify_) notifies.push_back(subscription->notify_);
    return notifies;
}

void CatalogFeed::Refresh(const CatalogScope& scope, std::string_view user) noexcept {
    std::vector<std::function<void()>> notifies;
    {
        std::lock_guard refresh(refresh_mutex_);
        {
            std::lock_guard lock(mutex_);
            if (!built_) return;
        }
        try {
            // Tables a previous read could not read are read again.
            CatalogScope read = scope;
            if (!read.all_tables) read.tables.insert(read.tables.end(), retry_.begin(), retry_.end());
            CatalogState state = reader_(read);
            std::lock_guard lock(mutex_);
            // The last subscription left while the state was read.
            if (!built_) return;
            const std::set<std::string, std::less<>> tables(read.tables.begin(), read.tables.end());
            const auto in_scope = [&](const CatalogKey& key) {
                return key.kind == CatalogKind::kUser || read.all_tables || tables.contains(key.table);
            };
            std::vector<CatalogKey> removed;
            for (const auto& entry : records_) {
                const auto& key = entry.first;
                if (!in_scope(key) || state.records.contains(key)) continue;
                if (key.kind == CatalogKind::kTable && state.unknown_tables.contains(key.table)) continue;
                if (key.kind == CatalogKind::kSlot && state.unknown_slots.contains(key.table)) continue;
                removed.push_back(key);
            }
            std::vector<Event> full, limited;
            for (const auto& key : removed) {
                const auto at = records_.find(key);
                full.push_back(Event{.key = &at->first, .action = "drop", .data = nullptr});
                if (at->second.limited) limited.push_back(Event{.key = &at->first, .action = "drop", .data = nullptr});
            }
            for (const auto& [key, value] : state.records) {
                const auto at = records_.find(key);
                if (at == records_.end()) {
                    full.push_back(Event{.key = &key, .action = "create", .data = &value.full});
                    if (value.limited) limited.push_back(Event{.key = &key, .action = "create", .data = &*value.limited});
                    continue;
                }
                if (at->second.full != value.full) full.push_back(Event{.key = &key, .action = "alter", .data = &value.full});
                // A record's visibility follows its kind, so only its value can differ.
                if (value.limited && at->second.limited != value.limited)
                    limited.push_back(Event{.key = &key, .action = "alter", .data = &*value.limited});
            }
            RetryLocked(state);
            if (full.empty()) return;
            const std::uint64_t revision = revision_ + 1U;
            auto full_frame = std::make_shared<const std::string>(Push(revision, user, full));
            // Who ran a statement is user information: only MANAGES USERS sees it.
            std::shared_ptr<const std::string> limited_frame =
                limited.empty() ? nullptr : std::make_shared<const std::string>(Push(revision, {}, limited));
            const std::size_t bytes = full_frame->size() + (limited_frame ? limited_frame->size() : 0U);
            history_.push_back(Entry{.revision = revision, .full = std::move(full_frame),
                .limited = std::move(limited_frame), .bytes = bytes});
            history_bytes_ += bytes;
            for (const auto& key : removed) records_.erase(key);
            for (auto& [key, value] : state.records) records_.insert_or_assign(key, std::move(value));
            revision_ = revision;
            TrimLocked();
            notifies = NotifiesLocked();
        } catch (const std::exception& error) {
            // The published records may no longer match the catalog: every
            // watch ends with INTERNAL, and a new one starts from the whole
            // catalog again. Nothing may leave this noexcept function.
            try {
                {
                    std::lock_guard lock(mutex_);
                    for (auto* subscription : subscriptions_) subscription->failed_ = true;
                    ClearLocked();
                    retry_.clear();
                    notifies = NotifiesLocked();
                }
                LogMessage(LogLevel::kError, LogComponent::kServer, "catalog feed failed", {{"error", error.what()}});
            } catch (const std::exception&) {
                // The failure is marked first: the watches end at FeedIo's next
                // pass even without this log line or wake.
            }
        }
    }
    for (const auto& notify : notifies) notify();
}

std::shared_ptr<CatalogSubscription> CatalogFeed::Subscribe(bool users, std::function<void()> notify) {
    std::lock_guard refresh(refresh_mutex_);
    std::optional<CatalogState> state;
    {
        std::lock_guard lock(mutex_);
        // Meanwhile only the last Unsubscribe can change built_, to false.
        if (!built_) state.emplace();
    }
    if (state) *state = reader_(CatalogScope{.tables = {}, .all_tables = true});
    std::lock_guard lock(mutex_);
    if (state && !built_) {
        // A new revision: no other state was published under it.
        ++revision_;
        ClearLocked();
        records_ = std::move(state->records);
        RetryLocked(*state);
        built_ = true;
    }
    std::shared_ptr<CatalogSubscription> subscription(
        new CatalogSubscription(shared_from_this(), users, revision_, std::move(notify)));
    subscriptions_.insert(subscription.get());
    return subscription;
}

void CatalogFeed::Unsubscribe(CatalogSubscription* subscription) noexcept {
    std::lock_guard lock(mutex_);
    subscriptions_.erase(subscription);
    // Nothing is kept without a subscription; the next one reads the catalog again.
    if (subscriptions_.empty()) ClearLocked();
    else TrimLocked();
}

CatalogSubscription::CatalogSubscription(
    std::shared_ptr<CatalogFeed> feed, bool users, std::uint64_t start, std::function<void()> notify)
    : feed_(std::move(feed)), users_(users), start_(start), notify_(std::move(notify)), next_(start + 1U) {}

CatalogSubscription::~CatalogSubscription() { feed_->Unsubscribe(this); }

std::optional<CatalogSubscription::Frame> CatalogSubscription::Next() {
    std::lock_guard lock(feed_->mutex_);
    if (ended_) return std::nullopt;
    if (failed_) {
        ended_ = true;
        return Frame{.bytes = std::make_shared<const std::string>(
                         Protocol::Error("INTERNAL", "the catalog feed failed; watch the catalog again")),
            .close = true};
    }
    // The next change left the history: the consumer rebuilds its catalog
    // from the whole-catalog push that follows.
    if (!whole_ && next_ <= feed_->dropped_through_) {
        whole_ = true;
        std::string out = ">5\r\n";
        Protocol::AppendBulk(out, "resync");
        Protocol::AppendBulk(out, StoreIdHex(feed_->epoch_));
        Protocol::AppendInteger(out, feed_->revision_);
        Protocol::AppendNull(out);
        Protocol::AppendNull(out);
        return Frame{.bytes = std::make_shared<const std::string>(std::move(out)), .close = false};
    }
    if (whole_) {
        auto bytes = feed_->Whole(users_);
        whole_ = false;
        next_ = feed_->revision_ + 1U;
        feed_->TrimLocked();
        return Frame{.bytes = std::move(bytes), .close = false};
    }
    const auto& history = feed_->history_;
    // history_ holds every revision after dropped_through_, in order.
    if (history.empty() || next_ > history.back().revision) return std::nullopt;
    for (auto index = static_cast<std::size_t>(next_ - history.front().revision); index < history.size(); ++index) {
        const auto& entry = history[index];
        next_ = entry.revision + 1U;
        if (const auto& bytes = users_ ? entry.full : entry.limited) return Frame{.bytes = bytes, .close = false};
    }
    return std::nullopt;
}

}  // namespace chunkdb
