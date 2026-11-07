#include "slot_watch.hpp"

#include <algorithm>
#include <utility>

#include "feed_protocol.hpp"
#include "chunkdb/protocol.hpp"
#include "chunkdb/logging.hpp"

namespace chunkdb {
namespace {
bool Charge(std::atomic<std::size_t>& counter, std::size_t bytes, std::size_t limit) {
    if (bytes == 0U) return true;
    if (bytes > limit) return false;
    auto used = counter.load(std::memory_order_acquire);
    for (;;) {
        if (used > limit - bytes) return false;
        if (counter.compare_exchange_weak(used, used + bytes, std::memory_order_acq_rel)) return true;
    }
}
}  // namespace
std::shared_ptr<SlotWatch> SlotWatch::Create(std::shared_ptr<Table> table,
    std::string name, FeedOptions options, bool takeover, std::string user,
    const std::function<void(std::string_view)>& authorize_owner) {
    if (options.area && (options.area->first.x > options.area->last.x || options.area->first.y > options.area->last.y))
        throw std::invalid_argument("feed area bounds are reversed");
    if (options.progress_interval.count() <= 0 || options.progress_interval.count() > INT32_MAX)
        throw std::invalid_argument("feed progress interval must be between 1 and 2147483647 milliseconds");
    if (options.ack_timeout_ms && *options.ack_timeout_ms == 0U)
        throw std::invalid_argument("ACK TIMEOUT must be positive");
    if (options.buffer_bytes && *options.buffer_bytes == 0U) throw std::invalid_argument("feed buffer must be positive");
    auto [slot, claim] = table->ClaimFeedSlot(name, options, takeover, std::move(user), authorize_owner);
    options.area = slot.filters.area; options.column_ids = slot.filters.column_ids;
    options.no_before = slot.filters.no_before; options.chunks = slot.filters.chunks;
    if (options.after && table->lineage() && options.after->epoch == table->lineage()->parent_epoch &&
        options.after->revision == table->lineage()->fork_revision) options.after->epoch = table->store_id();
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
    live_options.column_ids.reset(); live_options.no_before = false;
    result->live_ = result->table_->SubscribeFeed(live_options);
    result->join_ = result->live_->position().revision;
    result->joined_ = start.revision >= result->join_;
    { std::lock_guard lock(result->claim_->owner_mutex); result->claim_->owner = result; }
    return result;
}
SlotWatch::SlotWatch(std::shared_ptr<Table> table, std::shared_ptr<FeedSlotClaim> claim,
    FeedPosition start, FeedOptions options, bool resync)
    : table_(std::move(table)), claim_(std::move(claim)), generation_(claim_), table_bytes_(claim_->output_bytes), pool_(claim_->excess), start_(start), options_(std::move(options)),
      quota_(budget()), sent_(start.revision), acknowledged_(start.revision), written_(start.revision), resync_(resync), cursor_(start), next_progress_(std::chrono::steady_clock::now() + options_.progress_interval) {}
SlotWatch::~SlotWatch() {
    (void)LeaveExcessQueue();
    Release(unsent_ - excess_ + receipts_.size() * kReceiptBytes, excess_);
}
bool SlotWatch::LeaveExcessQueue() noexcept {
    if (!std::exchange(queued_, false)) return false;
    std::lock_guard lock(pool_->mutex);
    const bool head = pool_->waiters.front() == this;
    std::erase(pool_->waiters, this);
    return head;
}
void SlotWatch::Release(std::size_t shared, std::size_t excess) noexcept {
    if (shared != 0U) table_bytes_->fetch_sub(shared, std::memory_order_acq_rel);
    if (excess != 0U) pool_->bytes.fetch_sub(excess, std::memory_order_acq_rel);
}
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
    // Output is consumed in order, and excess output is its front.
    const auto excess = std::min(bytes, excess_);
    unsent_ -= bytes;
    excess_ -= excess;
    Release(bytes - excess, excess);
}
SlotWatch::AckResult SlotWatch::Ack(std::uint64_t revision) {
    std::lock_guard lock(mutex_);
    if (takeover_ || ack_expired_ || (finished_ && !cancelled_ && !unwatch_)) return AckResult::kTerminal;
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
    if (!takeover_ && !AckDeadlineExpiredLocked(std::chrono::steady_clock::now())) unwatch_ = true;
}
void SlotWatch::Cancel() noexcept {
    { std::lock_guard lock(mutex_); cancelled_ = true; }
    if (options_.notify) options_.notify();
}
bool SlotWatch::Activate() {
    std::lock_guard lock(mutex_);
    io_ready_.store(true, std::memory_order_release);
    return !std::exchange(active_, true);
}
void SlotWatch::RequestTakeoverAndWait() {
    std::unique_lock lock(mutex_);
    takeover_ = true; // Serialize with ACK validation; acknowledged_ is now frozen.
    lock.unlock();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterTakeoverFence, 0U);
    if (options_.notify) options_.notify();
    lock.lock();
    closed_cv_.wait(lock, [&] { return finished_ && socket_closed_; });
    if (takeover_error_) std::rethrow_exception(takeover_error_);
}
bool SlotWatch::ReleaseConnectionForReturn() {
    if (auto generation = generation_.lock()) {
        // Serialize ordinary UNWATCH ownership release with reservation of a
        // takeover. A pending handoff must close this socket, never return it.
        std::lock_guard lock(generation->owner_mutex);
        if (generation->takeover_pending.load(std::memory_order_acquire)) return false;
        io_ready_.store(false, std::memory_order_release);
    } else io_ready_.store(false, std::memory_order_release);
    return true;
}
bool SlotWatch::TakingOver() const { std::lock_guard lock(mutex_); return takeover_; }
void SlotWatch::SocketClosed() noexcept {
    { std::lock_guard lock(mutex_); socket_closed_ = true; }
    closed_cv_.notify_all();
}
bool SlotWatch::Finished() const { std::lock_guard lock(mutex_); return finished_; }
bool SlotWatch::AckTimedOut() const { std::lock_guard lock(mutex_); return finished_ && ack_expired_; }
void SlotWatch::Finish(std::optional<Output> control) {
    // Archive/subscription destruction can enter table control; never do it
    // on the socket thread or while holding its queue lock.
    pending_.reset();
    prepared_.reset();
    (void)LeaveExcessQueue();  // Finish wakes the worker for the next waiter.
    archive_.reset();
    live_.reset();
    claim_.reset();
    {
        std::lock_guard lock(mutex_);
        if (cancelled_ || ack_expired_ || takeover_) {
            // Untaken output is the back of the unsent output.
            const auto taken = unsent_ - bytes_;
            const auto excess = excess_ > taken ? excess_ - taken : 0U;
            unsent_ -= bytes_;
            excess_ -= excess;
            Release(bytes_ - excess, excess);
            output_.clear(); bytes_ = 0U;
        }
        if (!cancelled_ && !takeover_ && control) {
            bytes_ += control->bytes->size();
            unsent_ += control->bytes->size();
            table_bytes_->fetch_add(control->bytes->size(), std::memory_order_acq_rel);
            output_.push_back(std::move(*control));
        }
        ClearReceipts();
        finished_ = true;
    }
    closed_cv_.notify_all();
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

void SlotWatch::Build(Prepared& prepared) const {
    const auto& original = prepared.original;
    const bool mutation = original->kind == FeedEntry::Kind::kChange || original->kind == FeedEntry::Kind::kChunks;
    std::vector<Column> columns;
    if (original->kind == FeedEntry::Kind::kSchema && options_.column_ids) columns = original->columns;
    else if (mutation && (options_.column_ids || prepared.schema != original->schema_version))
        columns = table_->geometry().LayoutAt(original->schema_version).schema().columns;
    prepared.entry = options_.chunks && mutation
        ? ChunkFeedEntry(original, table_->geometry(), options_.area, options_.column_ids)
        : FilterFeedEntry(original, options_.area, options_.column_ids, options_.no_before, columns);
    prepared.built = true;
    const auto& entry = prepared.entry;
    prepared.send = entry != nullptr;
    if (!entry) return;
    prepared.kind = entry->kind;
    prepared.schema_version = entry->schema_version;
    const bool data = entry->kind == FeedEntry::Kind::kChange || entry->kind == FeedEntry::Kind::kChunks;
    if (data && prepared.schema != entry->schema_version) {
        FeedEntry metadata;
        metadata.kind = FeedEntry::Kind::kSchema;
        metadata.position = entry->position;
        metadata.schema_version = entry->schema_version;
        metadata.columns = columns;
        auto projected = FilterFeedEntry(std::make_shared<const FeedEntry>(std::move(metadata)), {}, options_.column_ids, false, columns);
        prepared.next.push_back(Output{std::make_shared<const std::string>(EncodeFeedEntry(*projected))});
    }
    auto frame = entry->protocol_frame ? entry->protocol_frame : std::make_shared<const std::string>(EncodeFeedEntry(*entry));
    const bool eligible = data || entry->kind == FeedEntry::Kind::kProgress;
    prepared.next.push_back({std::move(frame), eligible ? std::optional<std::uint64_t>(entry->position.revision) : std::nullopt});
    prepared.size = 0U;
    for (const auto& output : prepared.next) prepared.size += output.bytes->size();
    if (options_.ack_timeout_ms && data) prepared.receipt.push_back({entry->position.revision, {}});
    prepared.receipt_bytes = prepared.receipt.size() * kReceiptBytes;
}
SlotWatch::Admission SlotWatch::Admit(Prepared& prepared, bool& left_head) {
    std::lock_guard lock(mutex_);
    if (cancelled_ || unwatch_ || ack_expired_ || takeover_) return Admission::kWait;
    const auto size = prepared.size, receipt_bytes = prepared.receipt_bytes;
    const auto table_budget = budget();
    if (receipt_bytes > table_budget || size > table_budget - receipt_bytes)
        throw std::length_error("slot change exceeds the table feed buffer");
    // A watch waiting for anything but its pool turn gives up its place in
    // the queue, and joins at the back when it needs the pool again.
    const auto charge = size + receipt_bytes;
    const bool excess = charge > quota_;
    const bool data = prepared.kind == FeedEntry::Kind::kChange || prepared.kind == FeedEntry::Kind::kChunks;
    const auto wait_elsewhere = [&] {
        if (LeaveExcessQueue()) left_head = true;
        if (excess && data) prepared.Drop(); // Only the head keeps a change above the share built.
        return Admission::kWait;
    };
    // Unacknowledged receipts stay within the share; the first one always fits.
    const auto held = receipts_.size() * kReceiptBytes;
    if (held != 0U && (held > quota_ || receipt_bytes > quota_ - held)) return wait_elsewhere();
    if (!excess) {
        if (held > quota_ - charge || unsent_ > quota_ - charge - held) return wait_elsewhere();
        if (!prepared.built) return Admission::kBuild;
        if (!Charge(*table_bytes_, charge, table_budget)) return wait_elsewhere();
    } else if (!data) {
        // Progress, resync and schema frames are a few bytes and are rebuilt
        // for each attempt, so they never queue: they use the pool whenever
        // this watch has no unsent output and the pool has room.
        if (unsent_ != 0U || !Charge(pool_->bytes, size, table_budget)) return Admission::kWait;
        excess_ = size;
        for (auto& output : prepared.next) output.excess = true;
    } else {
        // A change larger than the share waits until this watch has no
        // unsent output, then for its turn in the table's pool queue. Only
        // the head keeps its built form, so the waiting changes held outside
        // the counters are at most one per table.
        if (unsent_ != 0U) { prepared.Drop(); return Admission::kWait; }
        std::lock_guard pool_lock(pool_->mutex);
        auto& waiters = pool_->waiters;
        if (!queued_) { waiters.push_back(this); queued_ = true; }
        if (waiters.front() != this) { prepared.Drop(); return Admission::kWait; }
        if (size > table_budget - std::min(table_budget, pool_->bytes.load(std::memory_order_acquire))) return Admission::kWait;
        if (!prepared.built) return Admission::kBuild;
        // Only the catch-up worker charges, so no admission saw a refund.
        if (!Charge(pool_->bytes, size, table_budget)) return Admission::kWait;
        if (!Charge(*table_bytes_, receipt_bytes, table_budget)) { Release(0U, size); return Admission::kWait; }
        waiters.pop_front(); queued_ = false;
        excess_ = size;
        for (auto& output : prepared.next) output.excess = true;
    }
    receipts_.splice(receipts_.end(), prepared.receipt);
    output_.swap(prepared.next);
    bytes_ += size;
    unsent_ += size;
    return Admission::kAdmitted;
}
bool SlotWatch::Publish(const std::shared_ptr<const FeedEntry>& original) {
    { std::lock_guard lock(mutex_); if (!output_.empty()) return false; }
    if (!prepared_ || prepared_->original != original || prepared_->schema != schema_) {
        // A different entry gives up this watch's place in the pool queue.
        if (LeaveExcessQueue() && options_.notify) options_.notify();
        prepared_.emplace();
        prepared_->original = original;
        prepared_->schema = schema_;
        Build(*prepared_);
    }
    if (!prepared_->send) { prepared_.reset(); return true; }
    for (;;) {
        bool left_head = false;
        const auto admission = Admit(*prepared_, left_head);
        if (left_head && options_.notify) options_.notify(); // The next waiter may proceed.
        if (admission == Admission::kAdmitted) break;
        if (admission == Admission::kWait) return false;
        Build(*prepared_);
    }
    // A frame that fits the share after all leaves the queue; success wakes the next waiter.
    if (queued_) (void)LeaveExcessQueue();
    const auto kind = prepared_->kind;
    const auto schema_version = prepared_->schema_version;
    const auto size = prepared_->size;
    prepared_.reset();
    if (auto* hook = hook_.load(std::memory_order_acquire))
        hook->Run(FeedDeliveryTestHook::Point::kAfterAdmission, size);
    if (kind == FeedEntry::Kind::kProgress)
        if (auto* hook = hook_.load(std::memory_order_acquire))
            hook->Run(FeedDeliveryTestHook::Point::kAfterProgressAdmission, size);
    if (kind == FeedEntry::Kind::kChange || kind == FeedEntry::Kind::kChunks || kind == FeedEntry::Kind::kSchema) schema_ = schema_version;
    if (kind == FeedEntry::Kind::kChange || kind == FeedEntry::Kind::kChunks || kind == FeedEntry::Kind::kProgress)
        next_progress_ = std::chrono::steady_clock::now() + options_.progress_interval;
    if (options_.notify) options_.notify();
    return true;
}
bool SlotWatch::Progress(const FeedSlot& slot) {
    const auto now = std::chrono::steady_clock::now();
    if (now < next_progress_) return false;
    next_progress_ = now + options_.progress_interval;
    auto position = cursor_;
    if (joined_ && live_) {
        const auto live_position = live_->Progress();
        if (!live_position) return false;
        position.revision = std::max(position.revision, live_position->revision);
    }
    position.revision = std::min(position.revision, slot.durable_watermark);
    auto entry = std::make_shared<FeedEntry>();
    entry->kind = FeedEntry::Kind::kProgress;
    entry->position = position;
    return Publish(entry);
}
bool SlotWatch::Work() {
    bool cancelled, unwatch, takeover;
    { std::lock_guard lock(mutex_); cancelled = cancelled_; unwatch = unwatch_; takeover = takeover_; }
    if (takeover) {
        if (!FlushAck(true)) return false;
        Finish(std::nullopt);
        return true;
    }
    if (cancelled) {
        std::uint64_t acknowledged;
        { std::lock_guard lock(mutex_); acknowledged = acknowledged_; }
        if (acknowledged > written_)
            table_->StageClaimedFeedSlotAck(claim_, {start_.epoch, acknowledged});
        Finish(std::nullopt);
        return true;
    }
    if (unwatch) {
        if (!FlushAck(true)) return false;
        Finish(Output{std::make_shared<const std::string>(Protocol::SimpleString("OK")), {}, false, true});
        return true;
    }
    if (ExpireAckDeadline(std::chrono::steady_clock::now())) {
        if (!FlushAck(true)) return false;
        Finish(Output{std::make_shared<const std::string>(Protocol::Error("TIMEOUT", "oldest mutation was not acknowledged",
            {{"reason", "ack_timeout"}})), {}, true, false});
        return true;
    }
    const auto slot = table_->ReadClaimedFeedSlot(claim_);
    FlushAck(false);
    if (resync_) {
        auto entry = std::make_shared<FeedEntry>();
        entry->kind = FeedEntry::Kind::kResync;
        entry->position = start_;
        entry->protocol_frame = std::make_shared<const std::string>(EncodeFeedResync(start_, table_->lineage()));
        if (!Publish(entry)) return false;
        resync_ = false;
        return true;
    }
    bool changed = false;
    if (!live_) {
        FeedOptions options = options_;
        options.after.reset();
        options.ack_timeout_ms.reset();
        options.area.reset();
        options.column_ids.reset(); options.no_before = false;
        live_ = table_->SubscribeFeed(options);
        join_ = live_->position().revision;
        joined_ = cursor_.revision >= join_;
        changed = true;
    }
    if (!joined_) {
        if (!archive_) {
            if (slot.durable_watermark <= cursor_.revision) return Progress(slot) || changed;
            archive_.emplace(table_->ReadClaimedFeedArchive(claim_, cursor_));
            if (options_.chunks) archive_->EnableChunkForms(budget());
            changed = true;
        }
        if (!pending_) pending_ = archive_->Next();
        if (pending_) {
            if (!Publish(pending_)) return changed;
            cursor_ = pending_->position;
            pending_.reset();
            return true;
        }
        cursor_ = archive_->through();
        archive_.reset();
        joined_ = cursor_.revision >= join_;
        return true;
    }
    if (!pending_) pending_ = live_->Next();
    if (!pending_) return Progress(slot) || changed;
    if (pending_->kind == FeedEntry::Kind::kEnd) throw TableNotFoundError("watched table was dropped");
    if (pending_->kind == FeedEntry::Kind::kResync) {
        join_ = pending_->position.revision;
        joined_ = false;
        pending_.reset();
        return true;
    }
    if (pending_->position.revision <= cursor_.revision) { pending_.reset(); return true; }
    if (pending_->position.revision > slot.durable_watermark) return changed;
    if (!Publish(pending_)) return changed;
    cursor_ = pending_->position;
    pending_.reset();
    return true;
}
bool SlotWatch::WorkStep() {
    { std::lock_guard lock(mutex_); if (finished_ || (!active_ && !cancelled_)) return false; }
    const auto finish_failure = [&](std::string_view code, std::string_view message, const std::exception& error) {
        LogMessage(LogLevel::kError, LogComponent::kServer, "slot watch terminated", {{"error", error.what()}});
        bool no_control;
        { std::lock_guard lock(mutex_); no_control = cancelled_ || takeover_;
          if (takeover_) takeover_error_ = std::current_exception(); }
        if (no_control) { Finish(std::nullopt); return; }
        // One bounded terminal control must fit even a tiny data quota.
        // A loss reason belongs to SLOT_LOST, not to a table that ended.
        const auto* lost = code == "SLOT_LOST" ? dynamic_cast<const FeedSlotLostError*>(&error) : nullptr;
        const auto* outcome = dynamic_cast<const WriteOutcomeUnknownError*>(&error);
        const auto reply = outcome ? Protocol::Error("OUTCOME_UNKNOWN", message.substr(0U, 96U), {{"stage", outcome->stage()}})
            : lost ? Protocol::Error(code, message.substr(0U, 96U), {{"reason", lost->reason()}})
                                : Protocol::Error(code, message.substr(0U, 96U));
        Finish(Output{std::make_shared<const std::string>(reply), {}, true, false});
    };
    const auto fail = [&](std::string_view code, const std::exception& error) {
        // A concurrent drop may have moved the files before publishing Gone.
        // A fence encountered while checking that state is terminal locally.
        // A table fenced or recovered is not dropped: say how to resume.
        try {
            if (!table_->Acquire()) {
                if (const char* ended = table_->ended_.load(std::memory_order_acquire)) {
                    finish_failure("INTERNAL", ended, error);
                    return;
                }
                code = "NO_TABLE";
            }
        }
        catch (const FeatureRecoveryRequiredError& fenced) {
            finish_failure("INTERNAL", fenced.what(), fenced);
            return;
        }
        catch (const MigrationRecoveryRequiredError& fenced) {
            finish_failure("INTERNAL", kMigrationRecoveryRequiredMessage, fenced);
            return;
        }
        catch (const FeedRecoveryRequiredError& fenced) {
            finish_failure("INTERNAL", kFeedRecoveryRequiredMessage, fenced);
            return;
        }
        finish_failure(code, error.what(), error);
    };
    try { return Work(); }
    catch (const FeatureRecoveryRequiredError& error) { finish_failure("INTERNAL", error.what(), error); }
    catch (const MigrationRecoveryRequiredError& error) { finish_failure("INTERNAL", kMigrationRecoveryRequiredMessage, error); }
    catch (const TableNotFoundError& error) { fail("NO_TABLE", error); }
    catch (const FeedSlotLostError& error) { fail("SLOT_LOST", error); }
    catch (const FeedSlotNotFoundError& error) { fail("SLOT_LOST", error); }
    catch (const WriteOutcomeUnknownError& error) { finish_failure("OUTCOME_UNKNOWN", error.what(), error); }
    catch (const std::length_error& error) { fail("OUT_OF_RANGE", error); }
    catch (const std::logic_error& error) { fail("INTERNAL", error); }
    catch (const std::runtime_error& error) { fail("INTERNAL", error); }
    return true;
}
}  // namespace chunkdb
