#include "slot_watch.hpp"

#include <algorithm>

#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"

namespace chunkdb {
std::shared_ptr<SlotWatch> SlotWatch::Create(std::shared_ptr<Table> table,
    std::string name, FeedOptions options) {
    if (options.area && (options.area->first.x > options.area->last.x || options.area->first.y > options.area->last.y))
        throw std::invalid_argument("feed area bounds are reversed");
    if (options.buffer_bytes && *options.buffer_bytes == 0U) throw std::invalid_argument("feed buffer must be positive");
    auto [slot, claim] = table->ClaimFeedSlot(name);
    auto start = slot.position;
    const bool resync = options.after && options.after->epoch != start.epoch;
    if (options.after && !resync) start.revision = std::max(start.revision, options.after->revision);
    if (start.revision > slot.durable_watermark) throw std::invalid_argument("AFTER exceeds the durable frontier");
    auto result = std::shared_ptr<SlotWatch>(new SlotWatch(std::move(table), std::move(claim), start, std::move(options), resync));
    result->acknowledged_ = slot.position.revision;
    result->written_ = slot.position.revision;
    return result;
}
SlotWatch::SlotWatch(std::shared_ptr<Table> table, std::shared_ptr<FeedSlotClaim> claim,
    FeedPosition start, FeedOptions options, bool resync)
    : table_(std::move(table)), claim_(std::move(claim)), start_(start), options_(std::move(options)),
      budget_(options_.buffer_bytes.value_or(kDefaultFeedBufferBytes)),
      quota_(budget_), sent_(start.revision), acknowledged_(start.revision), written_(start.revision), resync_(resync), cursor_(start) {}
SlotWatch::~SlotWatch() { Cancel(); }
void SlotWatch::SetQuota(std::size_t bytes) { std::lock_guard lock(mutex_); quota_ = bytes; }
std::optional<SlotWatch::Output> SlotWatch::Take(std::size_t room, std::size_t limit) {
    std::lock_guard lock(mutex_);
    if (output_.empty()) return std::nullopt;
    if (output_.front().bytes->size() > limit && !output_.front().close && !output_.front().resume) {
        auto error = std::make_shared<const std::string>(Protocol::Error("OUT_OF_RANGE", "slot change exceeds its watch buffer share"));
        if (error->size() > room) return std::nullopt;
        output_.clear(); bytes_ = 0U; cancelled_ = true;
        return Output{std::move(error), {}, true, false};
    }
    if (output_.front().bytes->size() > room) return std::nullopt;
    auto result = std::move(output_.front());
    output_.pop_front();
    bytes_ -= result.bytes->size();
    return result;
}
void SlotWatch::Sent(std::uint64_t revision) {
    std::lock_guard lock(mutex_);
    sent_ = std::max(sent_, revision);
}
void SlotWatch::Ack(std::uint64_t revision) {
    std::lock_guard lock(mutex_);
    if (finished_ || cancelled_ || unwatch_) throw std::invalid_argument("slot watch has ended");
    if (revision > sent_ || revision < acknowledged_) throw std::invalid_argument("ACK decreases or exceeds the last revision sent");
    acknowledged_ = revision;
}
void SlotWatch::Unwatch() { std::lock_guard lock(mutex_); unwatch_ = true; }
void SlotWatch::Cancel() noexcept {
    { std::lock_guard lock(mutex_); cancelled_ = true; }
    if (options_.notify) options_.notify();
}
void SlotWatch::Activate() { std::lock_guard lock(mutex_); active_ = true; }
bool SlotWatch::Finished() const { std::lock_guard lock(mutex_); return finished_; }
void SlotWatch::Finish(std::optional<Output> control) {
    // Archive/subscription destruction can enter table control; never do it
    // on the socket thread or while holding its queue lock.
    pending_.reset();
    archive_.reset();
    live_.reset();
    claim_.reset();
    {
        std::lock_guard lock(mutex_);
        if (cancelled_) { output_.clear(); bytes_ = 0U; }
        else if (control) {
            bytes_ += control->bytes->size();
            output_.push_back(std::move(*control));
        }
        finished_ = true;
    }
    if (options_.notify) options_.notify();
}
void SlotWatch::FlushAck(bool force) {
    const auto now = std::chrono::steady_clock::now();
    std::uint64_t acknowledged;
    { std::lock_guard lock(mutex_); acknowledged = acknowledged_; }
    if (acknowledged == written_ || (!force && now - flushed_ < std::chrono::milliseconds(100))) return;
    table_->AdvanceClaimedFeedSlot(claim_, {start_.epoch, acknowledged});
    written_ = acknowledged;
    flushed_ = now;
}
bool SlotWatch::Publish(const std::shared_ptr<const FeedEntry>& original) {
    { std::lock_guard lock(mutex_); if (!output_.empty()) return false; }
    auto entry = original;
    if (entry->kind == FeedEntry::Kind::kChange && options_.area) {
        auto filtered = std::make_shared<FeedEntry>(*entry);
        const auto& area = *options_.area;
        std::erase_if(filtered->blocks, [&](const auto& block) {
            return block.chunk.x < area.first.x || block.chunk.x > area.last.x ||
                   block.chunk.y < area.first.y || block.chunk.y > area.last.y;
        });
        if (filtered->blocks.empty()) return true;
        filtered->protocol_frame.reset();
        entry = std::move(filtered);
    }
    std::optional<Output> schema;
    if (entry->kind == FeedEntry::Kind::kChange && schema_ != entry->schema_version) {
        FeedEntry metadata;
        metadata.kind = FeedEntry::Kind::kSchema;
        metadata.position = entry->position;
        metadata.schema_version = entry->schema_version;
        metadata.columns = table_->geometry().LayoutAt(entry->schema_version).schema().columns;
        schema = Output{std::make_shared<const std::string>(EncodeFeedEntry(metadata))};
    }
    auto frame = entry->protocol_frame ? entry->protocol_frame : std::make_shared<const std::string>(EncodeFeedEntry(*entry));
    const auto size = frame->size() + (schema ? schema->bytes->size() : 0U);
    {
        std::lock_guard lock(mutex_);
        if (size > quota_) throw std::length_error("slot change exceeds the watch buffer share");
        if (cancelled_ || unwatch_) return false;
        if (schema) output_.push_back(std::move(*schema));
        const auto sent = entry->kind == FeedEntry::Kind::kSchema ? std::optional<std::uint64_t>{} : entry->position.revision;
        output_.push_back({std::move(frame), sent});
        bytes_ += size;
    }
    if (entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kSchema) schema_ = entry->schema_version;
    if (options_.notify) options_.notify();
    return true;
}
void SlotWatch::Work() {
    bool cancelled, unwatch;
    { std::lock_guard lock(mutex_); cancelled = cancelled_; unwatch = unwatch_; }
    if (cancelled) { Finish(std::nullopt); return; }
    if (unwatch) {
        FlushAck(true);
        Finish(Output{std::make_shared<const std::string>(Protocol::SimpleString("OK")), {}, false, true});
        return;
    }
    const auto slot = table_->ReadClaimedFeedSlot(claim_);
    FlushAck(false);
    if (resync_) {
        auto entry = std::make_shared<FeedEntry>();
        entry->kind = FeedEntry::Kind::kResync;
        entry->position = start_;
        if (!Publish(entry)) return;
        resync_ = false;
        return;
    }
    if (!live_) {
        FeedOptions options = options_;
        options.after.reset();
        options.area.reset();
        live_ = table_->SubscribeFeed(options);
        join_ = live_->position().revision;
        joined_ = cursor_.revision >= join_;
    }
    if (!joined_) {
        if (!archive_) {
            if (slot.durable_watermark <= cursor_.revision) return;
            archive_.emplace(table_->ReadFeedArchive(cursor_));
        }
        if (!pending_) pending_ = archive_->Next();
        if (pending_) {
            if (!Publish(pending_)) return;
            cursor_ = pending_->position;
            pending_.reset();
            return;
        }
        cursor_ = archive_->through();
        archive_.reset();
        joined_ = cursor_.revision >= join_;
        return;
    }
    if (!pending_) pending_ = live_->Next();
    if (!pending_) return;
    if (pending_->kind == FeedEntry::Kind::kEnd) throw TableNotFoundError("watched table was dropped");
    if (pending_->kind == FeedEntry::Kind::kResync) {
        join_ = pending_->position.revision;
        joined_ = false;
        pending_.reset();
        return;
    }
    if (pending_->position.revision <= cursor_.revision) { pending_.reset(); return; }
    if (pending_->position.revision > slot.durable_watermark) return;
    if (!Publish(pending_)) return;
    cursor_ = pending_->position;
    pending_.reset();
}
void SlotWatch::WorkStep() {
    { std::lock_guard lock(mutex_); if (finished_ || (!active_ && !cancelled_)) return; }
    const auto fail = [&](std::string_view code, const std::exception& error) {
        // A concurrent drop may have moved the files before publishing Gone.
        // Wait for that table control to distinguish it from storage damage.
        if (!table_->Acquire()) code = "NO_TABLE";
        Finish(Output{std::make_shared<const std::string>(Protocol::Error(code, error.what())), {}, true, false});
    };
    try { Work(); }
    catch (const TableNotFoundError& error) { fail("NO_TABLE", error); }
    catch (const FeedSlotLostError& error) { fail("SLOT_LOST", error); }
    catch (const FeedSlotNotFoundError& error) { fail("SLOT_LOST", error); }
    catch (const std::length_error& error) { fail("OUT_OF_RANGE", error); }
    catch (const std::logic_error& error) { fail("INTERNAL", error); }
    catch (const std::runtime_error& error) { fail("INTERNAL", error); }
}
}  // namespace chunkdb
