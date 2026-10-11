#include "slot_watch.hpp"

#include <algorithm>

#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/logging.hpp"

namespace chunkdb {
std::shared_ptr<SlotWatch> SlotWatch::Create(std::shared_ptr<Table> table,
    std::string name, FeedOptions options) {
    if (options.area && (options.area->first.x > options.area->last.x || options.area->first.y > options.area->last.y))
        throw std::invalid_argument("feed area bounds are reversed");
    if (options.progress_interval.count() <= 0 || options.progress_interval.count() > INT32_MAX)
        throw std::invalid_argument("feed progress interval must be between 1 and 2147483647 milliseconds");
    if (options.ack_timeout_ms && *options.ack_timeout_ms == 0U)
        throw std::invalid_argument("ACK TIMEOUT must be positive");
    if (options.buffer_bytes && *options.buffer_bytes == 0U) throw std::invalid_argument("feed buffer must be positive");
    auto [slot, claim] = table->ClaimFeedSlot(name);
    auto start = slot.position;
    const bool resync = options.after && (options.after->epoch != start.epoch || options.after->revision > claim->completed);
    if (options.after && !resync) start.revision = std::max(start.revision, options.after->revision);
    auto result = std::shared_ptr<SlotWatch>(new SlotWatch(std::move(table), std::move(claim), start, std::move(options), resync));
    result->acknowledged_ = slot.position.revision;
    result->written_ = slot.position.revision;
    FeedOptions live_options = result->options_;
    live_options.after.reset();
    live_options.ack_timeout_ms.reset();
    live_options.area.reset();
    result->live_ = result->table_->SubscribeFeed(live_options);
    result->join_ = result->live_->position().revision;
    result->joined_ = start.revision >= result->join_;
    return result;
}
SlotWatch::SlotWatch(std::shared_ptr<Table> table, std::shared_ptr<FeedSlotClaim> claim,
    FeedPosition start, FeedOptions options, bool resync)
    : table_(std::move(table)), claim_(std::move(claim)), table_bytes_(claim_->output_bytes), start_(start), options_(std::move(options)),
      quota_(budget()), sent_(start.revision), acknowledged_(start.revision), written_(start.revision), resync_(resync), cursor_(start), next_progress_(std::chrono::steady_clock::now() + options_.progress_interval) {}
SlotWatch::~SlotWatch() { table_bytes_->fetch_sub(unsent_ + receipts_.size() * kReceiptBytes, std::memory_order_acq_rel); }
void SlotWatch::SetQuota(std::size_t bytes) { std::lock_guard lock(mutex_); quota_ = bytes; }
std::optional<SlotWatch::Output> SlotWatch::Take(std::size_t room) {
    std::lock_guard lock(mutex_);
    if (output_.empty()) return std::nullopt;
    if (output_.front().bytes->size() > room) return std::nullopt;
    auto result = std::move(output_.front());
    output_.pop_front();
    bytes_ -= result.bytes->size();
    return result;
}
void SlotWatch::Sent(std::uint64_t revision) {
    SentAt(revision, std::chrono::steady_clock::now());
}
void SlotWatch::SentAt(std::uint64_t revision, std::chrono::steady_clock::time_point now) {
    std::lock_guard lock(mutex_);
    sent_ = std::max(sent_, revision);
    if (!options_.ack_timeout_ms) return;
    const auto receipt = std::find_if(receipts_.begin(), receipts_.end(),
        [&](const auto& item) { return item.revision == revision && !item.sent; });
    if (receipt != receipts_.end()) receipt->sent = now;
}
bool SlotWatch::ExpireAckDeadline(std::chrono::steady_clock::time_point now) {
    std::lock_guard lock(mutex_);
    if (cancelled_ || unwatch_) return false;
    return AckDeadlineExpiredLocked(now);
}
bool SlotWatch::AckDeadlineExpiredLocked(std::chrono::steady_clock::time_point now) {
    if (ack_expired_) return true;
    if (!options_.ack_timeout_ms || receipts_.empty() || !receipts_.front().sent) return false;
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - *receipts_.front().sent).count();
    if (elapsed < 0 || static_cast<std::uint64_t>(elapsed) < *options_.ack_timeout_ms) return false;
    ack_expired_ = true;  // Fence subsequent ACK validation before releasing the queue lock.
    return true;
}
void SlotWatch::ClearReceipts() noexcept {
    table_bytes_->fetch_sub(receipts_.size() * kReceiptBytes, std::memory_order_acq_rel);
    receipts_.clear();
}
void SlotWatch::Consumed(std::size_t bytes) {
    std::lock_guard lock(mutex_);
    if (bytes > unsent_) throw std::logic_error("slot output charge underflow");
    unsent_ -= bytes;
    table_bytes_->fetch_sub(bytes, std::memory_order_acq_rel);
}
SlotWatch::AckResult SlotWatch::Ack(std::uint64_t revision) {
    std::lock_guard lock(mutex_);
    if (ack_expired_ || (finished_ && !cancelled_ && !unwatch_)) return AckResult::kTerminal;
    if (finished_ || cancelled_ || unwatch_) throw std::invalid_argument("slot watch has ended");
    if (AckDeadlineExpiredLocked(std::chrono::steady_clock::now())) return AckResult::kTerminal;
    if (revision > sent_ || revision < acknowledged_) throw std::invalid_argument("ACK decreases or exceeds the last revision sent");
    for (const auto& receipt : receipts_) {
        if (receipt.revision > revision) break;
        if (!receipt.sent) throw std::logic_error("ACK crossed an unsent mutation receipt");
    }
    acknowledged_ = revision;
    while (!receipts_.empty() && receipts_.front().revision <= revision) {
        receipts_.pop_front();
        table_bytes_->fetch_sub(kReceiptBytes, std::memory_order_acq_rel);
    }
    return AckResult::kAccepted;
}
void SlotWatch::Unwatch() {
    std::lock_guard lock(mutex_);
    if (!AckDeadlineExpiredLocked(std::chrono::steady_clock::now())) unwatch_ = true;
}
void SlotWatch::Cancel() noexcept {
    { std::lock_guard lock(mutex_); cancelled_ = true; }
    if (options_.notify) options_.notify();
}
void SlotWatch::Activate() { std::lock_guard lock(mutex_); active_ = true; }
bool SlotWatch::Finished() const { std::lock_guard lock(mutex_); return finished_; }
bool SlotWatch::AckTimedOut() const { std::lock_guard lock(mutex_); return finished_ && ack_expired_; }
void SlotWatch::Finish(std::optional<Output> control) {
    // Archive/subscription destruction can enter table control; never do it
    // on the socket thread or while holding its queue lock.
    pending_.reset();
    archive_.reset();
    live_.reset();
    claim_.reset();
    {
        std::lock_guard lock(mutex_);
        if (cancelled_ || ack_expired_) {
            unsent_ -= bytes_;
            table_bytes_->fetch_sub(bytes_, std::memory_order_acq_rel);
            output_.clear(); bytes_ = 0U;
        }
        if (!cancelled_ && control) {
            bytes_ += control->bytes->size();
            unsent_ += control->bytes->size();
            table_bytes_->fetch_add(control->bytes->size(), std::memory_order_acq_rel);
            output_.push_back(std::move(*control));
        }
        ClearReceipts();
        finished_ = true;
    }
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterSlotFinish, 0U);
    if (options_.notify) options_.notify();
}
bool SlotWatch::FlushAck(bool force) {
    std::uint64_t acknowledged;
    { std::lock_guard lock(mutex_); acknowledged = acknowledged_; }
    if (!force && acknowledged <= written_) return true;
    table_->StageClaimedFeedSlotAck(claim_, {start_.epoch, acknowledged});
    if (force && table_->ReadClaimedFeedSlot(claim_).durable_watermark < acknowledged)
        table_->SyncClaimedFeedSlot(claim_);
    table_->FlushClaimedFeedSlotAcks(claim_, force);
    written_ = table_->ReadClaimedFeedSlot(claim_).position.revision;
    return acknowledged <= written_;
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
    std::list<MutationReceipt> receipt;
    if (options_.ack_timeout_ms && entry->kind == FeedEntry::Kind::kChange)
        receipt.push_back({entry->position.revision, {}});
    const auto receipt_bytes = receipt.size() * kReceiptBytes;
    std::deque<Output> next;
    if (schema) next.push_back(std::move(*schema));
    const bool eligible = entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kProgress;
    next.push_back({std::move(frame), eligible ? std::optional<std::uint64_t>(entry->position.revision) : std::nullopt});
    {
        std::lock_guard lock(mutex_);
        if (receipt_bytes > quota_ || size > quota_ - receipt_bytes)
            throw std::length_error("slot change exceeds the watch buffer share");
        if (cancelled_ || unwatch_ || ack_expired_) return false;
        const auto held = receipts_.size() * kReceiptBytes;
        if (held > quota_ - size - receipt_bytes || unsent_ > quota_ - size - receipt_bytes - held) return false;
        const auto table_budget = budget();
        if (receipt_bytes > table_budget || size > table_budget - receipt_bytes)
            throw std::length_error("slot change exceeds the table feed buffer");
        auto table_bytes = table_bytes_->load(std::memory_order_acquire);
        for (;;) {
            if (table_bytes > table_budget - size - receipt_bytes) return false;
            if (table_bytes_->compare_exchange_weak(table_bytes, table_bytes + size + receipt_bytes, std::memory_order_acq_rel)) break;
        }
        receipts_.splice(receipts_.end(), receipt);
        output_.swap(next);
        bytes_ += size;
        unsent_ += size;
    }
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterAdmission, size);
    if (entry->kind == FeedEntry::Kind::kProgress)
        if (auto* hook = hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kAfterProgressAdmission, size);
    if (entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kSchema) schema_ = entry->schema_version;
    if (entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kProgress)
        next_progress_ = std::chrono::steady_clock::now() + options_.progress_interval;
    if (options_.notify) options_.notify();
    return true;
}
void SlotWatch::Progress(const FeedSlot& slot) {
    const auto now = std::chrono::steady_clock::now();
    if (now < next_progress_) return;
    next_progress_ = now + options_.progress_interval;
    auto position = cursor_;
    if (joined_ && live_) {
        const auto live_position = live_->Progress();
        if (!live_position) return;
        position.revision = std::max(position.revision, live_position->revision);
    }
    position.revision = std::min(position.revision, slot.durable_watermark);
    auto entry = std::make_shared<FeedEntry>();
    entry->kind = FeedEntry::Kind::kProgress;
    entry->position = position;
    (void)Publish(entry);
}
void SlotWatch::Work() {
    bool cancelled, unwatch;
    { std::lock_guard lock(mutex_); cancelled = cancelled_; unwatch = unwatch_; }
    if (cancelled) {
        std::uint64_t acknowledged;
        { std::lock_guard lock(mutex_); acknowledged = acknowledged_; }
        if (acknowledged > written_)
            table_->StageClaimedFeedSlotAck(claim_, {start_.epoch, acknowledged});
        Finish(std::nullopt);
        return;
    }
    if (unwatch) {
        if (!FlushAck(true)) return;
        Finish(Output{std::make_shared<const std::string>(Protocol::SimpleString("OK")), {}, false, true});
        return;
    }
    if (ExpireAckDeadline(std::chrono::steady_clock::now())) {
        if (!FlushAck(true)) return;
        Finish(Output{std::make_shared<const std::string>(Protocol::Error("TIMEOUT", "oldest mutation was not acknowledged",
            {{"reason", "ack_timeout"}})), {}, true, false});
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
        options.ack_timeout_ms.reset();
        options.area.reset();
        live_ = table_->SubscribeFeed(options);
        join_ = live_->position().revision;
        joined_ = cursor_.revision >= join_;
    }
    if (!joined_) {
        if (!archive_) {
            if (slot.durable_watermark <= cursor_.revision) { Progress(slot); return; }
            archive_.emplace(table_->ReadClaimedFeedArchive(claim_, cursor_));
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
    if (!pending_) { Progress(slot); return; }
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
    const auto finish_failure = [&](std::string_view code, std::string_view message, const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer, "slot watch terminated", {{"error", error.what()}});
        bool cancelled;
        { std::lock_guard lock(mutex_); cancelled = cancelled_; }
        if (cancelled) { Finish(std::nullopt); return; }
        // One bounded terminal control must fit even a tiny data quota.
        Finish(Output{std::make_shared<const std::string>(Protocol::Error(code, message.substr(0U, 96U))), {}, true, false});
    };
    const auto fail = [&](std::string_view code, const std::exception& error) {
        // A concurrent drop may have moved the files before publishing Gone.
        // A fence encountered while checking that state is terminal locally.
        try { if (!table_->Acquire()) code = "NO_TABLE"; }
        catch (const MigrationRecoveryRequiredError& fenced) {
            finish_failure("INTERNAL", kMigrationRecoveryRequiredMessage, fenced);
            return;
        }
        finish_failure(code, error.what(), error);
    };
    try { Work(); }
    catch (const MigrationRecoveryRequiredError& error) { finish_failure("INTERNAL", kMigrationRecoveryRequiredMessage, error); }
    catch (const TableNotFoundError& error) { fail("NO_TABLE", error); }
    catch (const FeedSlotLostError& error) { fail("SLOT_LOST", error); }
    catch (const FeedSlotNotFoundError& error) { fail("SLOT_LOST", error); }
    catch (const std::length_error& error) { fail("OUT_OF_RANGE", error); }
    catch (const std::logic_error& error) { fail("INTERNAL", error); }
    catch (const std::runtime_error& error) { fail("INTERNAL", error); }
}
}  // namespace chunkdb
